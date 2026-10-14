#!/usr/bin/env bash
# Milestone 1: starting the system, discovery, the client, scheduling
# algorithms, running/limiting jobs, output capture and logging.
# Sections 2, 3, 4A-B, 5A-B, 7.
#
#   tests/test_m1_core.sh [name-filter]
source "$(dirname "$0")/lib.sh"

test_startup_and_logging() {
    start_scheduler FIFO
    expect "$T/sched.log" "Started on port $PORT\. Algorithm: FIFO" "scheduler startup line"
    expect "$T/sched.log" "Discovery responder listening on UDP port $DPORT" "discovery responder line"
    expect "$T/sched.log" '^\[[0-9]{2}:[0-9]{2}:[0-9]{2}\] \[SCHEDULER\] ' "log lines are [HH:MM:SS] [SCHEDULER]"

    start_worker w1 -cores 2 -ram 512 -gpu 1
    expect "$T/w1.log" "Scheduler found at .*:$PORT" "worker found scheduler over UDP"
    expect "$T/w1.log" "Registered successfully\. ID: 1001\. Capacity: 2 Cores, 512MB RAM, 1 GPU" "worker registration line"
    expect "$T/w1.log" '^\[[0-9:]{8}\] \[WORKER\] Looking for scheduler' "worker logs without an ID before registering"
    expect "$T/w1.log" '^\[[0-9:]{8}\] \[WORKER 1001\] ' "worker logs with its ID after registering"
    expect "$T/sched.log" "Worker 1001 connected\. Cores: 2, RAM: 512MB, GPU: 1" "scheduler logs the new worker"
}

test_discovery_fallback() {
    start_scheduler FIFO
    # Nobody answers on this discovery port, so the worker must fall back.
    start_worker w1 -cores 1 -ram 64 -discovery-port $((DPORT + 7))
    expect "$T/w1.log" "Discovery attempt 3/3 got no reply" "three discovery attempts"
    expect "$T/w1.log" "No discovery reply, using fallback 127\.0\.0\.1:$PORT" "falls back to the command-line address"
    expect "$T/w1.log" "Registered successfully" "registers through the fallback"
}

test_discovery_no_fallback_exits() {
    timeout 20 "$ROOT/worker" -cores 1 -ram 64 -discovery-port $((DPORT + 9)) > "$T/w.log" 2>&1
    local rc=$?
    check "worker exits with an error when discovery fails without fallback" test "$rc" -ne 0 -a "$rc" -ne 124
    expect "$T/w.log" "No discovery reply and no fallback" "explains why"
}

test_discovery_port_reuse() {
    start_scheduler FIFO
    stop sched
    start_scheduler FIFO # immediately, same ports
    expect_count "$T/sched.log" "Discovery responder listening" 2 "restarted scheduler rebinds the UDP port at once"
    expect_not "$T/sched.log" "WARNING: discovery disabled" "no 'address already in use'"
}

test_capacity_clamping() {
    start_scheduler FIFO
    start_worker w1 -cores 9999 -ram 99999999 -gpu 50
    expect "$T/w1.log" "WARNING: -cores 9999 .*clamping to $(getconf _NPROCESSORS_ONLN)" "cores clamped with a warning"
    expect "$T/w1.log" "WARNING: -ram 99999999MB .*clamping" "RAM clamped with a warning"
    expect "$T/w1.log" "Capacity: .* 50 GPU" "GPUs are not clamped"
}

test_username_prompt() {
    start_scheduler FIFO
    start_client c1 ""
    expect "$T/c1.log" "Username:" "asks for a username"
    send c1 "bad name!"
    expect "$T/c1.log" "Invalid username" "rejects spaces and punctuation"
    send c1 "abcdefghijklmnopqrstuvwxyz0123456" # 33 chars
    expect_count "$T/c1.log" "Invalid username" 2 "rejects names over 31 characters"
    send c1 "lordz.K_2-x"
    expect "$T/c1.log" "Logged in as lordz\.K_2-x\." "accepts letters, digits, _ - and ."
    expect "$T/sched.log" "Client 'lordz\.K_2-x' logged in" "scheduler logs the login"

    start_client c2 "kaush"
    expect_not "$T/c2.log" "Username:" "-user skips the prompt"
}

test_job_with_args_and_logs() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    local id
    id=$(submit c1 "$BIN/hello foo \"bar baz\" -cores 1 -ram 64 -time 5 -prio 2")
    check "submit returns a job id" [ "$id" = 1 ]
    expect "$T/c1.log" "\[Client\] Job submitted\." "client confirms the submit"
    expect "$T/c1.log" "\[Event\] Job 1 COMPLETED successfully\." "completion event"
    expect "$T/sched.log" "Job 1 SUBMITTED by lordz" "scheduler logs who submitted"
    expect "$T/sched.log" "Job 1: sent .*hello \([0-9]+ bytes" "scheduler shipped the binary"
    expect "$T/w1.log" "Job 1 ASSIGNED, forking" "worker logs the assignment"
    send c1 "logs 1"
    expect "$T/c1.log" "arg 1: foo" "logs shows the output"
    expect "$T/c1.log" "arg 2: bar baz" "quoted arguments stay together"
    expect_not "$T/w1.log" "Hello from job" "job output never reaches the worker's terminal"
    sleep 0.3
    check "job directory removed after the job" [ ! -d /tmp/osn_job_1 ]
}

test_path_command() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "ls -la -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 COMPLETED" "a PATH command runs"
    expect "$T/sched.log" "Job 1 QUEUED -> RUNNING" "it was scheduled"
    expect_not "$T/sched.log" "Job 1: sent" "nothing is transferred for a PATH command"
    send c1 "logs 1"
    expect "$T/c1.log" "output\.log" "ran inside its job directory"
}

test_submit_validation() {
    start_scheduler FIFO
    start_client c1 lordz
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5"
    expect "$T/c1.log" "Missing required flag\(s\): -prio" "missing -prio is caught"
    send c1 "submit $BIN/hello -cores 1 -ram 64 -time 5 -prio 7"
    expect "$T/c1.log" "-prio 1-3" "priority out of range is caught"
    send c1 "submit ./definitely/not/here -cores 1 -ram 64 -time 5 -prio 1"
    expect "$T/c1.log" "Cannot find \./definitely/not/here" "missing file is caught"
    send c1 "frobnicate"
    expect "$T/c1.log" "Unknown command 'frobnicate'" "unknown command"
    send c1 "status 99"
    expect "$T/c1.log" "No such job: 99" "status of an unknown job"
    send c1 "help"
    expect "$T/c1.log" "cancel-cron" "help lists the commands"
}

test_crash_diagnosis() {
    start_scheduler FIFO
    start_worker w1 -cores 6 -ram 2048
    start_client c1 lordz
    submit c1 "$BIN/segfault -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "$BIN/divzero -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "no_such_command_xyz -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "$BIN/exit_code 42 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    submit c1 "$BIN/infinite_loop -cores 1 -ram 64 -time 2 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 FAILED\. Reason: Terminated by signal 11 \(SIGSEGV - Segmentation Fault\)\." "SIGSEGV in words" 15
    expect "$T/c1.log" "Job 2 FAILED\. Reason: Terminated by signal 8 \(SIGFPE - Floating Point Exception\)\." "SIGFPE in words" 15
    expect "$T/c1.log" "Job 3 FAILED\. Reason: Exited with code 127 \(command not found\)\." "exit 127" 15
    expect "$T/c1.log" "Job 4 FAILED\. Reason: Exited with code 42\." "non-zero exit code" 15
    expect "$T/c1.log" "Job 5 FAILED\. Reason: Wall-clock timeout exceeded \(2s limit\)\." "timeout reported in words" 15
    expect "$T/w1.log" "\[ERROR\] Job 1 Failed\. Diagnosis: Terminated by signal 11" "worker logs the diagnosis"
    expect "$T/sched.log" "Job 5 RUNNING -> FAILED \(Wall-clock timeout" "scheduler logs the transition"
}

test_sleeping_job_hits_wall_clock() {
    # A job that mostly sleeps never reaches RLIMIT_CPU; the worker's own
    # wall-clock deadline has to kill it.
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 100 -cores 1 -ram 64 -time 2 -prio 2" > /dev/null
    expect "$T/w1.log" "Job 1: wall-clock limit of 2s reached, sending SIGKILL" "worker enforces wall-clock time" 6
    expect "$T/c1.log" "Job 1 FAILED\. Reason: Wall-clock timeout exceeded \(2s limit\)" "reported as a timeout"
}

test_memory_limit() {
    # On Linux -ram is RLIMIT_AS: malloc fails once the job hits it.
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 2048
    start_client c1 lordz
    submit c1 "$BIN/memleak -cores 1 -ram 100 -time 20 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 FAILED" "job over its RAM limit fails" 15
    send c1 "logs 1"
    expect "$T/c1.log" "malloc failed after [0-9]+ MB" "allocation was refused by the limit"
    local mb
    mb=$(last_number "$T/c1.log" "malloc failed after")
    check "it got less than its 100 MB limit (got ${mb:-?} MB)" [ "${mb:-999}" -lt 100 ]
}

test_fifo_blocks_behind_oldest() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/hello -cores 4 -ram 64 -time 5 -prio 1" > /dev/null   # can never fit
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 1" > /dev/null   # would fit
    sleep 2.5
    send c1 "status 2"
    expect "$T/c1.log" "Job 2 \[QUEUED\]" "FIFO does not skip ahead to a smaller job"
    expect_not "$T/sched.log" "Job 2 QUEUED -> RUNNING" "the smaller job never started"
}

test_priority_order() {
    start_scheduler PRIORITY
    start_client c1 lordz
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 3" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 1" > /dev/null
    submit c1 "$BIN/hello -cores 9 -ram 64 -time 5 -prio 1" > /dev/null   # never fits
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 1" > /dev/null
    start_worker w1 -cores 1 -ram 512    # one slot: jobs run one at a time
    expect "$T/c1.log" "Job 1 COMPLETED" "all runnable jobs finish" 15
    expect_order "$T/sched.log" "Job 3 QUEUED -> RUNNING" "Job 5 QUEUED -> RUNNING" "priority 1 first, older first"
    expect_order "$T/sched.log" "Job 5 QUEUED -> RUNNING" "Job 2 QUEUED -> RUNNING" "then priority 2"
    expect_order "$T/sched.log" "Job 2 QUEUED -> RUNNING" "Job 1 QUEUED -> RUNNING" "then priority 3"
    expect_not "$T/sched.log" "Job 4 QUEUED -> RUNNING" "PRIORITY moves past a job that doesn't fit"
}

test_priority_aging() {
    start_scheduler PRIORITY
    start_client c1 lordz
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 3" > /dev/null
    expect "$T/sched.log" "Job 1 aged: effective priority 3 -> 2" "priority 3 becomes 2 after 10s" 13
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    start_worker w1 -cores 1 -ram 512
    expect "$T/c1.log" "Job 2 COMPLETED" "both run" 10
    expect_order "$T/sched.log" "Job 1 QUEUED -> RUNNING" "Job 2 QUEUED -> RUNNING" "aged job beats a newer job of the same effective priority"
}

test_queue_full() {
    start_scheduler FIFO
    start_client c1 kg
    local i
    for ((i = 0; i < 1026; i++)); do
        echo "submit ls -cores 1 -ram 1 -time 5 -prio 2"
    done >&"${FDS[c1]}"
    expect "$T/c1.log" "Job ID assigned: 1024" "1024 jobs fit" 60
    expect "$T/c1.log" "queue is full" "the 1025th gets QUEUE_FULL" 20
    expect_not "$T/c1.log" "Job ID assigned: 1025" "nothing past the capacity is accepted"
    expect "$T/sched.log" "rejected: QUEUE_FULL" "scheduler logs the rejection"
    send c1 "status 1"
    expect "$T/c1.log" "Job 1 \[QUEUED\]" "status still answers with a full queue" 5
}

test_nodes_status_and_load_choice() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_worker w2 -cores 4 -ram 1024
    start_client c1 lordz
    send c1 "nodes"
    expect "$T/c1.log" "1001 +ALIVE +2 / 2 +512 / 512" "nodes lists worker 1001"
    expect "$T/c1.log" "1002 +ALIVE +4 / 4 +1024 / 1024" "nodes lists worker 1002"
    submit c1 "$BIN/slow_print 10 -cores 2 -ram 100 -time 20 -prio 2" > /dev/null
    expect "$T/sched.log" "Job 1 -> Worker [0-9]+ \(chosen over [0-9]+: " "worker choice is logged with a reason"
    expect "$T/c1.log" "Job 1 RUNNING on Worker" "job starts"
    send c1 "status 1"
    expect "$T/c1.log" "Job 1 \[RUNNING\]" "status shows RUNNING"
    expect "$T/c1.log" "\[Job 1\] Assigned to: Worker 100[12]" "status shows the worker"
    send c1 "nodes"
    expect "$T/c1.log" "0 / 2 .* \[1\]|2 / 4 .* \[1\]" "nodes shows the cores in use"
}

test_gpu_accounting() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512 -gpu 0
    start_worker w2 -cores 2 -ram 512 -gpu 1
    start_client c1 lordz
    submit c1 "$BIN/slow_print 6 -cores 1 -ram 64 -time 20 -prio 2 -gpu 1" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING on Worker 1002\." "GPU job goes to the worker with a GPU"
    send c1 "nodes"
    expect "$T/c1.log" "1002 .* 0 / 1 " "its GPU is in use"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2 -gpu 2" > /dev/null
    sleep 1.5
    send c1 "status 2"
    expect "$T/c1.log" "Job 2 \[QUEUED\]" "no worker has 2 GPUs: stays queued"
}

run_tests "$@"
