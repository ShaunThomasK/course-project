#!/usr/bin/env bash
# Milestone 3 scheduling: dependencies and groups, preemption, fair-share
# quotas, cron jobs. Sections 8, 10, 11, 12.
#
#   tests/test_m3_scheduling.sh [name-filter]
source "$(dirname "$0")/lib.sh"

test_dependency_chain() {
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 1024
    start_client c1 lordz
    submit c1 "$BIN/slow_print 3 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "$BIN/hello step2 -cores 1 -ram 64 -time 5 -prio 2 -depends-on 1" > /dev/null
    expect "$T/c1.log" "\[Client\] Job submitted \(WAITING on Job 1\)\." "submit says it is WAITING"
    send c1 "status 2"
    expect "$T/c1.log" "Job 2 \[WAITING\]" "status shows WAITING"
    expect "$T/c1.log" "Depends on: Job 1 \((QUEUED|RUNNING)\)" "status lists the dependency"
    expect "$T/c1.log" "\[Event\] Job 2 moved from WAITING to QUEUED \(dependency satisfied\)\." "released when job 1 completes" 10
    expect_order "$T/c1.log" "Job 1 COMPLETED" "Job 2 moved from WAITING" "completion event comes first"
    expect "$T/c1.log" "Job 2 COMPLETED" "then it runs"
}

test_multiple_deps_and_group() {
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 1024
    start_client c1 lordz
    submit c1 "$BIN/slow_print 2 -cores 1 -ram 64 -time 10 -prio 2 -group 3" > /dev/null   # 1
    submit c1 "$BIN/slow_print 4 -cores 1 -ram 64 -time 10 -prio 2 -group 3" > /dev/null   # 2
    submit c1 "$BIN/hello all -cores 1 -ram 64 -time 5 -prio 2 -depends-on g3" > /dev/null  # 3
    expect "$T/c1.log" "WAITING on Jobs 1, 2" "g3 expands to the jobs in group 3"
    # Joins group 3 after job 3 was submitted: must not hold job 3 back.
    submit c1 "$BIN/slow_print 30 -cores 1 -ram 64 -time 60 -prio 2 -group 3" > /dev/null # 4
    submit c1 "$BIN/hello mix -cores 1 -ram 64 -time 5 -prio 2 -depends-on 1,2" > /dev/null # 5
    expect "$T/c1.log" "Job 3 COMPLETED" "job 3 runs once jobs 1 and 2 are done" 10
    send c1 "status 4"
    expect "$T/c1.log" "Job 4 \[RUNNING\]" "...even though later group member 4 is still running"
    expect "$T/c1.log" "Job 5 COMPLETED" "a comma list of job ids works"
}

test_dependency_errors() {
    start_scheduler FIFO
    start_client c1 lordz
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 42"
    expect "$T/c1.log" "Job 42 does not exist" "unknown job id rejected"
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on g9"
    expect "$T/c1.log" "Group 9 does not exist" "unknown group rejected"
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 1,x"
    expect "$T/c1.log" "Bad dependency 'x'" "garbage rejected"
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -group 0"
    expect "$T/c1.log" "-group needs a positive integer" "bad group id rejected"
}

test_failure_cascades() {
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 1024
    start_client c1 lordz
    submit c1 "$BIN/slow_print 2 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null          # 1
    submit c1 "$BIN/segfault -cores 1 -ram 64 -time 5 -prio 2 -depends-on 1" > /dev/null # 2 fails
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 2" > /dev/null    # 3
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 3,1" > /dev/null  # 4
    expect "$T/c1.log" "Job 3 WAITING -> CANCELLED \(dependency Job 2 FAILED\)" "dependent of a failed job is cancelled" 15
    expect "$T/c1.log" "Job 4 WAITING -> CANCELLED \(dependency Job 3 CANCELLED\)" "and so on down the chain"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 2" > /dev/null    # 5
    expect "$T/c1.log" "CANCELLED right away: Dependency Job 2 ended FAILED" "depending on an already failed job cancels at once"
    expect_not "$T/sched.log" "Job [345] QUEUED -> RUNNING" "none of them ran"
}

test_cancel_waiting_job() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 6 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 1" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -depends-on 2" > /dev/null
    send c1 "cancel 2"
    expect "$T/sched.log" "Job 2 WAITING -> CANCELLED \(requested by lordz\)" "WAITING job can be cancelled"
    expect "$T/sched.log" "Job 3 WAITING -> CANCELLED \(dependency Job 2 CANCELLED\)" "its dependents are cancelled too"
}

test_preemption_picks_lowest_priority_newest() {
    start_scheduler PRIORITY
    start_worker w1 -cores 4 -ram 1024
    start_client m mdot
    start_client l lordz
    submit m "$BIN/slow_print 60 -cores 1 -ram 64 -time 90 -prio 2 -preemptible" > /dev/null  # 1
    submit m "$BIN/slow_print 60 -cores 1 -ram 64 -time 90 -prio 3 -preemptible" > /dev/null  # 2
    submit m "$BIN/slow_print 60 -cores 1 -ram 64 -time 90 -prio 3 -preemptible" > /dev/null  # 3 newest prio 3
    submit m "$BIN/slow_print 60 -cores 1 -ram 64 -time 90 -prio 3" > /dev/null               # 4 not preemptible
    expect "$T/m.log" "Job 4 RUNNING" "worker is full"
    submit l "$BIN/slow_print 4 -cores 2 -ram 64 -time 30 -prio 1" > /dev/null                # 5 needs 2 cores
    expect "$T/sched.log" "Preempting Job 3 \(user mdot, priority 3\)" "first victim: lowest priority, newest"
    expect "$T/sched.log" "Preempting Job 2 \(user mdot, priority 3\)" "second victim: next newest of priority 3"
    expect "$T/sched.log" "Job 5 -> Worker 1001 \(made room by preempting 2 job\(s\)\)" "just enough victims"
    expect_not "$T/sched.log" "Preempting Job [14] " "priority 2 job and non-preemptible job untouched"
    expect "$T/m.log" "Job 3 preempted by priority 1 Job 5; moved back to QUEUED" "victim's owner is told"
    expect "$T/w1.log" "Job 3: kill requested \(preempt\), sending SIGKILL" "victims get SIGKILL"
    expect "$T/l.log" "Job 5 RUNNING on Worker 1001" "priority 1 job takes the room"
    expect "$T/l.log" "Job 5 COMPLETED" "and finishes" 10
    # 2 original starts + 2 restarts once job 5 is done.
    expect_count_eventually "$T/m.log" "Job [23] RUNNING on Worker 1001" 4 "victims run again afterwards" 8
    send m "status 3"
    expect "$T/m.log" "Attempts: 1 of 3" "preemption didn't use an attempt"
}

test_preemption_only_for_priority_1() {
    start_scheduler PRIORITY
    start_worker w1 -cores 1 -ram 512
    start_client c1 mdot
    submit c1 "$BIN/slow_print 20 -cores 1 -ram 64 -time 30 -prio 3 -preemptible" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "preemptible job running"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    sleep 2
    expect_not "$T/sched.log" "Preempting" "a priority 2 job does not preempt"
    submit c1 "$BIN/hello -cores 2 -ram 64 -time 5 -prio 1" > /dev/null
    sleep 2
    expect_not "$T/sched.log" "Preempting" "no preemption if even killing everything isn't enough"
}

test_quota_spec_example() {
    # Spec 11: one 4-core worker, mdot runs 3 cores and has a 2-core job
    # queued; kaush's 1-core job runs, under plain FIFO.
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 1024
    start_client m mdot
    start_client k kaush
    submit m "$BIN/slow_print 12 -cores 3 -ram 64 -time 30 -prio 2" > /dev/null
    expect "$T/m.log" "Job 1 RUNNING" "mdot holds 3 of 4 cores"
    submit m "$BIN/hello -cores 2 -ram 64 -time 5 -prio 1" > /dev/null
    submit k "$BIN/slow_print 2 -cores 1 -ram 64 -time 10 -prio 3" > /dev/null
    expect "$T/sched.log" "Skipping Job 2 \(user mdot, quota exceeded\) in favor of Job 3 \(user kaush\)" "skip is logged"
    expect "$T/k.log" "Job 3 RUNNING" "kaush's job runs ahead of mdot's higher-priority one"
    sleep 2.5
    expect_count "$T/sched.log" "Skipping Job 2" 1 "logged once, not on every tick"
}

test_quota_exactly_half_is_fine() {
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 1024
    start_client m mdot
    start_client k kaush
    submit m "$BIN/slow_print 12 -cores 2 -ram 64 -time 30 -prio 2" > /dev/null   # exactly 50%
    expect "$T/m.log" "Job 1 RUNNING" "mdot holds 2 of 4 cores"
    submit k "$BIN/slow_print 12 -cores 2 -ram 64 -time 30 -prio 2" > /dev/null   # can't fit yet
    submit m "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    sleep 2
    expect_not "$T/sched.log" "Skipping" "exactly 50% is not over quota"
}

test_cron_jobs() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 kg
    send c1 "submit $BIN/hello tick -cores 1 -ram 64 -time 5 -prio 2 -every 2 -group 5"
    expect "$T/c1.log" "Template ID assigned: 1" "-every registers a template"
    expect "$T/c1.log" "\[Event\] Cron 1 injected Job 1 \(run #1\)\." "first copy goes in at once" 3
    expect "$T/c1.log" "\[Event\] Cron 1 injected Job 2 \(run #2\)\." "second after the interval" 4
    expect "$T/c1.log" "\[Event\] Cron 1 injected Job 3 \(run #3\)\." "third" 4
    send c1 "crons"
    expect "$T/c1.log" "^1 +2s +[0-9]+ +kg +ACTIVE .*hello tick" "crons lists id, interval, runs, user, command"
    send c1 "status 2"
    expect "$T/c1.log" "Group: 5" "copies keep the template's flags"
    expect "$T/c1.log" "Spawned by: Cron 1" "status shows where it came from"
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -every 2 -depends-on 1"
    expect "$T/c1.log" "-every can't be combined with -depends-on" "-every with -depends-on rejected"
    send c1 "cancel-cron 1"
    expect "$T/c1.log" "Cron 1 cancelled" "cancel-cron"
    local runs
    runs=$(grep -c "Cron 1 injected" "$T/c1.log")
    sleep 3
    expect_count "$T/c1.log" "Cron 1 injected" "$runs" "no more copies after cancelling"
    send c1 "cancel-cron 1"
    expect "$T/c1.log" "Cron 1 is already cancelled" "cancelling again is an error"
    send c1 "cancel-cron 7"
    expect "$T/c1.log" "No such cron template: 7" "unknown template is an error"
    send c1 "crons"
    expect "$T/c1.log" "CANCELLED" "crons shows it as cancelled"
}

test_cron_runs_even_if_previous_still_running() {
    start_scheduler FIFO
    start_worker w1 -cores 4 -ram 512
    start_client c1 kg
    send c1 "submit $BIN/slow_print 20 -cores 1 -ram 64 -time 30 -prio 2 -every 2"
    expect "$T/c1.log" "Cron 1 injected Job 2" "second copy injected" 5
    send c1 "status 1"
    expect "$T/c1.log" "Job 1 \[RUNNING\]" "while the first copy is still running"
}

run_tests "$@"
