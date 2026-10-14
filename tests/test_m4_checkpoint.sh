#!/usr/bin/env bash
# Milestone 4: checkpoints and live job migration (Section 14).
# tests/programs/counter.c counts steps and checkpoints after each one.
#
#   tests/test_m4_checkpoint.sh [name-filter]
source "$(dirname "$0")/lib.sh"

test_migrate_resumes_on_another_worker() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 legz
    submit c1 "$BIN/counter 14 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING on Worker 1001" "running on 1001"
    start_worker w2 -cores 2 -ram 512
    expect "$T/sched.log" "Job 1: checkpoint #3 " "scheduler collects checkpoints as they change" 5
    send c1 "status 1"
    expect "$T/c1.log" "Checkpoint: #[0-9]+, [0-9]+ bytes, [0-9.]+s old" "status shows number, size and age"
    send c1 "migrate 1"
    expect "$T/c1.log" "\[Client\] Migrating Job 1 off Worker 1001\. It will resume from checkpoint #[0-9]+\." "migrate reply"
    expect "$T/w1.log" "Job 1: migration requested, sending SIGTERM" "worker sends SIGTERM first"
    expect "$T/c1.log" "\[Event\] Job 1 stopped on Worker 1001 and moved back to QUEUED; it resumes from checkpoint #[0-9]+\." "stopped event"
    expect "$T/sched.log" "Job 1: excluding Worker 1001, which it was just migrated from" "the old worker is excluded"
    expect "$T/c1.log" "\[Event\] Job 1 RUNNING on Worker 1002\." "restarts on 1002"
    expect_order "$T/sched.log" "Job 1: sent checkpoint #[0-9]+ .* -> Worker 1002" "Job 1: sent .*counter .* -> Worker 1002" "checkpoint is sent before the binary"
    expect "$T/c1.log" "Job 1 COMPLETED" "finishes" 15
    send c1 "logs 1"
    expect "$T/c1.log" "SIGTERM: saved step [0-9]+, exiting" "job wrote a final checkpoint on SIGTERM"
    local saved resumed
    saved=$(last_number "$T/c1.log" "SIGTERM: saved step")
    resumed=$(last_number "$T/c1.log" "resuming after step")
    check "resumed exactly where it stopped (saved $saved, resumed after $resumed)" [ -n "$saved" -a "$saved" = "$resumed" ]
    expect_count "$T/c1.log" "starting from the beginning" 1 "started from scratch only once"
    send c1 "status 1"
    expect "$T/c1.log" "Attempts: 1 of 3" "migration didn't use an attempt"
}

test_migrate_returns_to_same_worker_if_only_one() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 legz
    submit c1 "$BIN/counter 10 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1: checkpoint #2 " "checkpointing" 5
    send c1 "migrate 1"
    expect "$T/c1.log" "Job 1 stopped on Worker 1001 and moved back to QUEUED" "stopped and requeued" 5
    expect_count_eventually "$T/c1.log" "Job 1 RUNNING on Worker 1001" 2 "with no other worker it restarts on the same one" 5
    expect "$T/c1.log" "Job 1 COMPLETED" "and completes" 15
    send c1 "logs 1"
    expect "$T/c1.log" "resuming after step" "from its checkpoint"
}

test_migrate_errors() {
    start_scheduler FIFO
    start_worker w1 -cores 1 -ram 512
    start_client c1 legz
    start_client c2 mdot
    send c1 "migrate 5"
    expect "$T/c1.log" "No such job: 5" "unknown job"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "job 1 done"
    send c1 "migrate 1"
    expect "$T/c1.log" "Job 1 is COMPLETED, not RUNNING" "finished job"
    submit c1 "$BIN/slow_print 30 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    send c1 "migrate 3"
    expect "$T/c1.log" "Job 3 is QUEUED, not RUNNING" "queued job"
    send c2 "migrate 2"
    expect "$T/c2.log" "belongs to legz" "someone else's job"
}

test_migrate_without_checkpoint_restarts() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 legz
    submit c1 "$BIN/slow_print 30 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "running"
    send c1 "migrate 1"
    expect "$T/c1.log" "It has no checkpoint yet, so it will restart from the beginning" "warns there's no checkpoint"
}

test_migrate_falls_back_to_sigkill() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 legz
    submit c1 "$BIN/ignore_term -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "running"
    sleep 1
    send c1 "migrate 1"
    local t0=$SECONDS
    expect "$T/w1.log" "Job 1: still running 2s after SIGTERM, sending SIGKILL" "SIGKILL 2s after an ignored SIGTERM" 5
    expect "$T/c1.log" "Job 1 stopped on Worker 1001 and moved back to QUEUED" "then requeued" 5
    check "took about 2s ($((SECONDS - t0))s)" [ $((SECONDS - t0)) -ge 1 -a $((SECONDS - t0)) -le 4 ]
}

test_resume_after_worker_death() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kaush
    submit c1 "$BIN/counter 16 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1: checkpoint #4 " "checkpointing on 1001" 6
    start_worker w2 -cores 2 -ram 512
    signal_proc w1 KILL
    expect "$T/c1.log" "Job 1 \(attempt 1\) moved back to QUEUED; it resumes from checkpoint #[0-9]+\." "requeued with its checkpoint" 12
    expect "$T/c1.log" "Job 1 \(attempt 2\) RUNNING on Worker 1002" "restarts on 1002"
    expect "$T/c1.log" "Job 1 COMPLETED" "finishes" 20
    send c1 "logs 1"
    expect "$T/c1.log" "resuming after step [4-9]|resuming after step 1[0-9]" "continued from its last checkpoint, not from zero"
}

test_resume_after_preemption() {
    start_scheduler PRIORITY
    start_worker w1 -cores 2 -ram 512
    start_client c1 mdot
    submit c1 "$BIN/counter 12 -cores 2 -ram 64 -time 60 -prio 3 -preemptible" > /dev/null
    expect "$T/sched.log" "Job 1: checkpoint #3 " "checkpointing" 6
    submit c1 "$BIN/hello vip -cores 2 -ram 64 -time 5 -prio 1" > /dev/null
    expect "$T/c1.log" "Job 1 preempted by priority 1 Job 2" "preempted"
    expect "$T/c1.log" "Job 2 COMPLETED" "priority 1 job done"
    expect "$T/c1.log" "Job 1 COMPLETED" "preempted job finishes" 15
    send c1 "logs 1"
    expect "$T/c1.log" "resuming after step [3-9]" "and resumed from its checkpoint"
}

run_tests "$@"
