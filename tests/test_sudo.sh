#!/usr/bin/env bash
# Job isolation as root (Section 5A): the worker must drop jobs to the
# nobody user, so they can't write outside their own directory, and apply
# the CPU, memory and process limits. This is how Strict-TA tests.
#
#   make all tests          (as yourself, first)
#   sudo tests/test_sudo.sh [name-filter]
source "$(dirname "$0")/lib.sh"

if [ "$(id -u)" -ne 0 ]; then
    echo "This file must run as root: sudo $0" >&2
    exit 2
fi

test_jobs_run_as_nobody() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    expect "$T/w1.log" "Running as root; jobs will run as nobody" "worker says it will drop privileges"
    expect_not "$T/w1.log" "WARNING: not running as root" "no non-root warning"
    start_client c1 lordz
    submit c1 "id -un -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "id ran"
    send c1 "logs 1"
    expect "$T/c1.log" "^nobody$" "the job ran as nobody"
}

test_cannot_write_outside_job_dir() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/escape_attempt -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "escape attempt ran"
    send c1 "logs 1"
    expect "$T/c1.log" "write /etc/osn_pwned: Permission denied" "can't write system files"
    expect "$T/c1.log" "write /root/osn_pwned: Permission denied" "can't write root's home"
    expect "$T/c1.log" "write /home/.*/Desktop/osn_pwned: (Permission denied|No such file)" "can't write your Desktop"
    expect "$T/c1.log" "write \./inside_job_dir\.txt: ok" "can write inside its own job directory"
    expect_not "$T/c1.log" "SUCCEEDED" "no write outside succeeded"
    check "nothing was created in /etc" [ ! -e /etc/osn_pwned ]
    rm -f /etc/osn_pwned /root/osn_pwned "/home/${SUDO_USER:-nobody}/Desktop/osn_pwned"
}

test_job_dir_owned_by_nobody_and_cleaned() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 6 -cores 1 -ram 64 -time 20 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "running"
    sleep 0.5
    check "/tmp/osn_job_1 is owned by nobody" [ "$(stat -c %U /tmp/osn_job_1 2>/dev/null)" = nobody ]
    check "the copied binary is owned by nobody" [ "$(stat -c %U /tmp/osn_job_1/slow_print 2>/dev/null)" = nobody ]
    check "checkpoint/ is owned by nobody" [ "$(stat -c %U /tmp/osn_job_1/checkpoint 2>/dev/null)" = nobody ]
    check "the job process runs as nobody" [ "$(ps -o user= -C slow_print | head -1 | tr -d ' ')" = nobody ]
    expect "$T/c1.log" "Job 1 COMPLETED" "finished" 10
    sleep 0.5
    check "job directory removed afterwards" [ ! -d /tmp/osn_job_1 ]
}

test_fork_bomb_is_capped() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/forkbomb -cores 1 -ram 256 -time 15 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 (COMPLETED|FAILED)" "fork bomb job ended" 20
    send c1 "logs 1"
    expect "$T/c1.log" "forked [0-9]+ children before fork failed" "fork() started failing"
    local n
    n=$(last_number "$T/c1.log" "forked [0-9]+ children")
    check "capped by RLIMIT_NPROC=64 (got ${n:-?} children)" [ "${n:-999}" -lt 64 ]
}

test_cpu_limit_sigxcpu() {
    # 4 spinning threads burn 2 s of CPU in about 0.5 s of wall time, so
    # RLIMIT_CPU fires long before the worker's wall-clock timer.
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 512
    start_client c1 kg
    submit c1 "$BIN/cpu_hog -cores 4 -ram 256 -time 2 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 FAILED\. Reason: Wall-clock timeout exceeded \(2s limit\)\." "reported as a timeout, not a bare signal" 10
    expect_not "$T/w1.log" "wall-clock limit of 2s reached" "it was the kernel's CPU limit, not the worker's timer"
    send c1 "logs 1"
    expect "$T/c1.log" "spinning on 4 threads" "it really ran 4 threads"
}

test_memory_limit_as_nobody() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 2048
    start_client c1 kg
    submit c1 "$BIN/memleak -cores 1 -ram 100 -time 20 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 FAILED" "job over its RAM limit fails" 15
    send c1 "logs 1"
    expect "$T/c1.log" "malloc failed after [0-9]+ MB" "allocation refused at the limit"
}

test_checkpoint_and_migrate_as_nobody() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 legz
    submit c1 "$BIN/counter 12 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1: checkpoint #2 " "nobody can write its checkpoint" 6
    start_worker w2 -cores 2 -ram 512
    send c1 "migrate 1"
    expect "$T/c1.log" "Job 1 RUNNING on Worker 1002" "migrated"
    expect "$T/w2.log" "checkpoint \([0-9]+ bytes\) placed in /tmp/osn_job_1/checkpoint/state" "restored checkpoint placed"
    expect "$T/c1.log" "Job 1 COMPLETED" "finished" 15
    send c1 "logs 1"
    expect "$T/c1.log" "resuming after step" "resumed from the restored (chowned) checkpoint"
}

test_cache_works_as_root() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "first run"
    submit c1 "$BIN/big_a -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 2: .*already cached" "cache hit"
    expect "$T/c1.log" "Job 2 COMPLETED" "nobody can run the copy made from the cache"
}

run_tests "$@"
