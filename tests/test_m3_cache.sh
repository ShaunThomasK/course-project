#!/usr/bin/env bash
# Milestone 3: executable caching (Section 13), following the spec's
# "How to check it" list.
#
#   tests/test_m3_cache.sh [name-filter]
source "$(dirname "$0")/lib.sh"

test_full_transfer_then_cache_hit() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    cp "$BIN/hello" "$T/prog"
    submit c1 "$T/prog -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1: sent .*prog \([0-9]+ bytes, sha256 [0-9a-f]{8}\.\.\.\) -> Worker 1001" "first run: full transfer"
    expect "$T/c1.log" "Job 1 COMPLETED" "job 1 done"
    submit c1 "$T/prog -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 2: .*prog already cached \(sha256 [0-9a-f]{8}\.\.\.\) on Worker 1001, not sending" "second run: cache hit"
    expect "$T/c1.log" "Job 2 COMPLETED" "runs from the cache"
    check "cache file is named <name>-<sha256>" bash -c "ls '$T/cache_w1' | grep -qE '^prog-[0-9a-f]{64}$'"

    printf 'x' >> "$T/prog"    # one byte different: new hash
    submit c1 "$T/prog -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 3: sent .*prog" "changed file: new transfer"
    local h1 h3
    h1=$(grep -oE "Job 1: sent .*sha256 [0-9a-f]{8}" "$T/sched.log" | grep -oE "[0-9a-f]{8}$")
    h3=$(grep -oE "Job 3: sent .*sha256 [0-9a-f]{8}" "$T/sched.log" | grep -oE "[0-9a-f]{8}$")
    check "with a new hash ($h1 vs $h3)" [ -n "$h1" -a "$h1" != "$h3" ]
    expect "$T/c1.log" "Job 3 COMPLETED" "changed program runs"
}

test_deleted_cache_file_is_resent() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "job 1 done"
    rm -f "$T/cache_w1"/hello-*
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/w1.log" "Job 2: cached sha256 [0-9a-f]{8}\.\.\. is missing, asking for the full file" "worker reports the missing file"
    expect "$T/sched.log" "Job 2: Worker 1001 no longer has sha256" "scheduler drops that hash"
    expect "$T/sched.log" "Job 2: sent .*hello" "and sends the whole file"
    expect "$T/c1.log" "Job 2 COMPLETED" "the job still completes"
    send c1 "status 2"
    expect "$T/c1.log" "Attempts: 1 of 3" "not counted as a failed attempt"
}

test_restarted_worker_reports_cache() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "job 1 done"
    stop w1
    start_worker w1 -cores 2 -ram 512    # same cache folder
    expect "$T/w1.log" "Cache: 1 program\(s\)" "restarted worker scans its cache"
    expect "$T/sched.log" "Worker 1002 connected.*1 cached program\(s\)" "and reports it when registering"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 2: .*already cached .* on Worker 1002" "no transfer needed after the restart"
}

test_path_command_is_not_cached() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "ls -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "PATH command runs"
    check "nothing was cached" bash -c "[ -z \"\$(ls '$T/cache_w1')\" ]"
}

test_prefers_worker_with_cached_binary() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_worker w2 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "first run"
    local first
    first=$(last_number "$T/c1.log" "Job 1 RUNNING on Worker")
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 2 -> Worker $first \(chosen over [0-9]+: binary cached vs not cached, both idle\)" "second run lands on the worker that has it"
    expect "$T/sched.log" "Job 2: .*already cached" "without a transfer"
}

test_idle_worker_beats_cached_busy_one() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_worker w2 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "1001 has big_a cached"
    submit c1 "$BIN/slow_print 10 -cores 1 -ram 64 -time 20 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 2 RUNNING on Worker 1001" "make 1001 busy"
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 3 -> Worker 1002 \(chosen over 1001: idle vs busy\)" "the idle worker wins even without the binary"
    expect "$T/sched.log" "Job 3: sent .*big_a" "so it gets a transfer"
}

test_lru_eviction() {
    # Spec: OSN_CACHE_SIZE_MB=2, three ~0.7 MB programs, submit A, B, A, C:
    # B is evicted (A was used more recently); B again needs a transfer.
    start_scheduler FIFO
    start_worker w1 OSN_CACHE_SIZE_MB=2 -cores 2 -ram 512
    start_client c1 kg
    expect "$T/w1.log" "limit 2MB" "cache limit read from OSN_CACHE_SIZE_MB"
    local p n=0
    for p in a b a c; do
        n=$((n + 1))
        submit c1 "$BIN/big_$p -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
        expect "$T/c1.log" "Job $n COMPLETED" "big_$p ran"
    done
    expect "$T/w1.log" "Cache: evicted big_b-" "storing C evicts B"
    expect_not "$T/w1.log" "Cache: evicted big_a-" "A, used more recently, stays"
    expect "$T/sched.log" "Worker 1001 cache now holds 2 program\(s\)" "worker tells the scheduler what it still has"
    submit c1 "$BIN/big_b -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 5: sent .*big_b" "B again needs a full transfer"
    expect "$T/c1.log" "Job 5 COMPLETED" "and runs"
}

test_large_binary() {
    # About the 30 MB of the spec's story; trailing bytes don't stop an ELF running.
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    cp "$BIN/hello" "$T/bigbin"
    head -c 30000000 /dev/urandom >> "$T/bigbin"
    local t0=$SECONDS
    submit c1 "$T/bigbin -cores 1 -ram 64 -time 30 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1: sent .*bigbin \(300[0-9]{5} bytes" "30 MB transferred" 20
    expect "$T/c1.log" "Job 1 COMPLETED" "and runs" 30
    check "in reasonable time ($((SECONDS - t0))s)" [ $((SECONDS - t0)) -lt 20 ]
    submit c1 "$T/bigbin -cores 1 -ram 64 -time 30 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 2: .*bigbin already cached" "second time: no transfer"
    send c1 "nodes"
    expect "$T/c1.log" "Cluster Status" "scheduler stayed responsive"
}

run_tests "$@"
