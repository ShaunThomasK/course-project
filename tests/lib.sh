# tests/lib.sh - a small test framework for the cluster. Source it from a
# test file, define functions named test_*, then call run_tests "$@".
#
# Every test gets a fresh scheduler on random ports and its own log
# directory. Workers and clients read their stdin from a FIFO, so a test
# can type into them at any time with `send`. Checks wait for a line to
# show up in a log instead of sleeping a fixed time.

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/osn_tests.XXXXXX")"
# Test programs are reached through a symlink with no spaces in its path, so
# "submit $BIN/hello ..." works even if the project lives in "Course Work/".
ln -s "$ROOT/tests/programs/bin" "$RUN_DIR/bin"
BIN="$RUN_DIR/bin"

TOTAL_PASS=0
TOTAL_FAIL=0
FAILED_CHECKS=()
declare -A PIDS=()
declare -A FDS=()
T=""          # this test's log directory
PORT=0        # scheduler TCP port for this test
DPORT=0       # discovery UDP port for this test
CUR_TEST=""
SETSID=$(command -v setsid || true)

if [ -t 1 ]; then
    GREEN=$'\e[32m' RED=$'\e[31m' YELLOW=$'\e[33m' OFF=$'\e[0m'
else
    GREEN="" RED="" YELLOW="" OFF=""
fi

# ---- results ---------------------------------------------------------------

pass() {
    TOTAL_PASS=$((TOTAL_PASS + 1))
    echo "  ${GREEN}PASS${OFF} $1"
}

# fail DESCRIPTION [LOGFILE]: shows the end of LOGFILE to help debugging.
fail() {
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
    FAILED_CHECKS+=("$CUR_TEST: $1")
    echo "  ${RED}FAIL${OFF} $1"
    if [ -n "${2:-}" ] && [ -f "$2" ]; then
        echo "       last lines of $2:"
        tail -n 12 "$2" | sed 's/^/       | /'
    fi
}

# wait_for FILE REGEX [TIMEOUT_SECONDS=10]: 0 once a line matches.
wait_for() {
    local file=$1 re=$2 tries=$(( ${3:-10} * 10 )) i
    for ((i = 0; i < tries; i++)); do
        grep -qE -- "$re" "$file" 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

# expect FILE REGEX DESCRIPTION [TIMEOUT]: passes once REGEX appears in FILE.
expect() {
    if wait_for "$1" "$2" "${4:-10}"; then pass "$3"; else fail "$3" "$1"; fi
}

# expect_not FILE REGEX DESCRIPTION: passes if REGEX is not in FILE right now.
expect_not() {
    if grep -qE -- "$2" "$1" 2>/dev/null; then fail "$3" "$1"; else pass "$3"; fi
}

# expect_count FILE REGEX N DESCRIPTION
expect_count() {
    local c
    c=$(grep -cE -- "$2" "$1" 2>/dev/null)
    if [ "${c:-0}" -eq "$3" ]; then pass "$4"; else fail "$4 (found $c, wanted $3)" "$1"; fi
}

# expect_count_eventually FILE REGEX N DESCRIPTION [TIMEOUT]: waits until
# REGEX has matched at least N lines.
expect_count_eventually() {
    local tries=$(( ${5:-10} * 10 )) i c=0
    for ((i = 0; i < tries; i++)); do
        c=$(grep -cE -- "$2" "$1" 2>/dev/null)
        [ "${c:-0}" -ge "$3" ] && { pass "$4"; return; }
        sleep 0.1
    done
    fail "$4 (found $c, wanted $3)" "$1"
}

# expect_order FILE REGEX_A REGEX_B DESCRIPTION: first A comes before first B.
expect_order() {
    local a b
    a=$(grep -nE -- "$2" "$1" | head -1 | cut -d: -f1)
    b=$(grep -nE -- "$3" "$1" | head -1 | cut -d: -f1)
    if [ -n "$a" ] && [ -n "$b" ] && [ "$a" -lt "$b" ]; then pass "$4"; else fail "$4" "$1"; fi
}

# check DESCRIPTION COMMAND...: passes if the command succeeds.
check() {
    local d=$1
    shift
    if "$@"; then pass "$d"; else fail "$d"; fi
}

# ---- processes ----------------------------------------------------------------

# open_fifo NAME: creates $T/NAME.in and keeps it open read-write in FDS[NAME].
open_fifo() {
    local fd
    [ -p "$T/$1.in" ] || mkfifo "$T/$1.in"
    if [ -z "${FDS[$1]:-}" ]; then
        exec {fd}<>"$T/$1.in"
        FDS[$1]=$fd
    fi
}

# start_scheduler ALGO: FIFO or PRIORITY.
start_scheduler() {
    "$ROOT/scheduler" "$PORT" "$1" -discovery-port "$DPORT" >> "$T/sched.log" 2>&1 &
    PIDS[sched]=$!
    wait_for "$T/sched.log" "Listening for incoming" 5 || fail "scheduler started" "$T/sched.log"
}

# start_worker NAME [VAR=value ...] [worker flags...]
# Defaults to the scheduler as fallback and this test's discovery port; later
# flags override (e.g. a different -discovery-port). Log: $T/NAME.log.
start_worker() {
    local name=$1 envs=()
    shift
    while [ $# -gt 0 ] && [[ $1 == *=* ]]; do envs+=("$1"); shift; done
    mkdir -p "$T/cache_$name"
    open_fifo "$name"
    # setsid: its own process group, like a worker started from its own
    # terminal, so a test can send it a "Ctrl+C" without hitting the tests.
    $SETSID env OSN_CACHE_DIR="$T/cache_$name" ${envs[@]+"${envs[@]}"} \
        "$ROOT/worker" 127.0.0.1 "$PORT" -discovery-port "$DPORT" "$@" \
        <&"${FDS[$name]}" >> "$T/$name.log" 2>&1 &
    PIDS[$name]=$!
    local n
    n=$(grep -c "Heartbeat active" "$T/$name.log" 2>/dev/null)
    local i
    for ((i = 0; i < 150; i++)); do
        [ "$(grep -c "Heartbeat active" "$T/$name.log")" -gt "${n:-0}" ] && return 0
        kill -0 "${PIDS[$name]}" 2>/dev/null || break
        sleep 0.1
    done
    fail "worker $name registered" "$T/$name.log"
}

# start_client NAME USER: logs in as USER (empty USER = answer the prompt
# yourself with send). Log: $T/NAME.log.
start_client() {
    local name=$1 user=$2
    open_fifo "$name"
    if [ -n "$user" ]; then
        "$ROOT/client" 127.0.0.1 "$PORT" -user "$user" -discovery-port "$DPORT" \
            <&"${FDS[$name]}" >> "$T/$name.log" 2>&1 &
        PIDS[$name]=$!
        wait_for "$T/$name.log" "Logged in as" 10 || fail "client $name logged in" "$T/$name.log"
    else
        "$ROOT/client" 127.0.0.1 "$PORT" -discovery-port "$DPORT" \
            <&"${FDS[$name]}" >> "$T/$name.log" 2>&1 &
        PIDS[$name]=$!
    fi
}

# send NAME TEXT: types TEXT and Enter into a client or worker.
send() {
    printf '%s\n' "$2" >&"${FDS[$1]}"
}

# submit CLIENT ARGS...: submits and prints the new job id ("" on failure).
submit() {
    local c=$1 f="$T/$1.log" before i
    shift
    before=$(grep -cE "Job ID assigned|Submit failed|queue is full|Missing required|Cannot find" "$f")
    send "$c" "submit $*"
    for ((i = 0; i < 100; i++)); do
        [ "$(grep -cE "Job ID assigned|Submit failed|queue is full|Missing required|Cannot find" "$f")" -gt "$before" ] && break
        sleep 0.1
    done
    tail -n 5 "$f" | grep -oE "Job ID assigned: [0-9]+" | tail -1 | grep -oE "[0-9]+$"
}

# signal_proc NAME SIGNAL: e.g. signal_proc w1 KILL
signal_proc() {
    kill -"$2" "${PIDS[$1]}" 2>/dev/null
}

# stop NAME: kills a process and forgets it (its FIFO stays for restarts).
stop() {
    kill -KILL "${PIDS[$1]}" 2>/dev/null
    wait "${PIDS[$1]}" 2>/dev/null
    unset "PIDS[$1]"
}

# running NAME: 0 if the process is still alive.
running() {
    kill -0 "${PIDS[$1]}" 2>/dev/null
}

# last_number FILE REGEX: the last number in the last line matching REGEX.
last_number() {
    grep -E -- "$2" "$1" | tail -1 | grep -oE "[0-9]+" | tail -1
}

end_test() {
    local name fd
    for name in "${!PIDS[@]}"; do
        kill -TERM "${PIDS[$name]}" 2>/dev/null
    done
    sleep 0.2
    for name in "${!PIDS[@]}"; do
        kill -KILL "${PIDS[$name]}" 2>/dev/null
        wait "${PIDS[$name]}" 2>/dev/null
    done
    for name in "${!FDS[@]}"; do
        fd=${FDS[$name]}
        exec {fd}>&-
    done
    PIDS=()
    FDS=()
}

# ---- runner -----------------------------------------------------------------

# run_tests [FILTER]: runs every test_* function of the calling file, in file
# order, whose name contains FILTER.
run_tests() {
    local filter=${1:-} fn file="${BASH_SOURCE[1]}"
    trap 'end_test; echo; echo "interrupted; logs in $RUN_DIR"; exit 130' INT TERM

    if [ ! -x "$ROOT/scheduler" ] || [ ! -x "$ROOT/tests/programs/bin/hello" ]; then
        echo "Build first: make all tests" >&2
        exit 2
    fi
    echo "Logs: $RUN_DIR"
    for fn in $(grep -oE '^test_[A-Za-z0-9_]+' "$file"); do
        [ -n "$filter" ] && [[ $fn != *"$filter"* ]] && continue
        CUR_TEST=$fn
        T="$RUN_DIR/$fn"
        mkdir -p "$T"
        PORT=$((20000 + RANDOM % 20000))
        DPORT=$((PORT + 1))
        echo "${YELLOW}== $fn${OFF}"
        "$fn"
        end_test
    done

    echo
    echo "$(basename "$file"): ${GREEN}$TOTAL_PASS passed${OFF}, ${RED}$TOTAL_FAIL failed${OFF}"
    for f in ${FAILED_CHECKS[@]+"${FAILED_CHECKS[@]}"}; do
        echo "  ${RED}x${OFF} $f"
    done
    if [ "$TOTAL_FAIL" -eq 0 ]; then
        rm -rf "$RUN_DIR"
    else
        echo "Logs kept in $RUN_DIR"
    fi
    [ "$TOTAL_FAIL" -eq 0 ]
}
