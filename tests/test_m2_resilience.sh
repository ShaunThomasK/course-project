#!/usr/bin/env bash
# Milestone 2: heartbeats and fault tolerance, telemetry, cancellation,
# streaming logs, draining, events and reconnecting.
# Sections 3 (events), 4C-D, 5C-E, 6, 9.
#
#   tests/test_m2_resilience.sh [name-filter]
source "$(dirname "$0")/lib.sh"

test_events_while_idle() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 3 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null
    # From here on the client is never sent anything.
    expect "$T/c1.log" "\[Event\] Job 1 RUNNING on Worker 1001\." "RUNNING event arrives at an idle prompt"
    expect "$T/c1.log" "\[Event\] Job 1 COMPLETED successfully\." "COMPLETED event arrives at an idle prompt"
}

test_events_only_to_owner_and_same_name_sessions() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client a kg
    start_client b kg
    start_client c mdot
    expect "$T/sched.log" "Client 'kg' logged in \(1 other session" "second 'kg' login is accepted"
    submit a "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/a.log" "Job 1 COMPLETED" "submitting session gets the events"
    expect "$T/b.log" "Job 1 COMPLETED" "the other 'kg' session gets them too"
    expect_not "$T/c.log" "Job 1" "a different user gets nothing"
    send b "status 1"
    expect "$T/b.log" "User: kg" "status shows the owner"
}

test_logs_follow_streams_live() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 8 -cores 1 -ram 64 -time 20 -prio 2" > /dev/null
    send c1 "logs 1 -f"
    expect "$T/c1.log" "line 2 of 8" "output shows up while following"
    expect_not "$T/sched.log" "Job 1 RUNNING -> COMPLETED" "...while the job is still running"
    expect "$T/sched.log" "Client 'lordz' following Job 1 logs" "scheduler logs the follow"
    expect "$T/c1.log" "-- Job 1 finished \(COMPLETED\) --" "footer when the job ends" 10
    expect_order "$T/c1.log" "line 8 of 8" "-- Job 1 finished" "every line comes before the footer"
}

test_logs_follow_ctrl_c_returns_to_prompt() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 40 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    send c1 "logs 1 -f"
    expect "$T/c1.log" "line 2 of 40" "following"
    signal_proc c1 INT
    expect "$T/c1.log" "Stopped following Job 1" "Ctrl+C stops following"
    sleep 0.5
    check "the client did not exit" running c1
    send c1 "status 1"
    expect "$T/c1.log" "Job 1 \[RUNNING\]" "the prompt works again"
    signal_proc c1 INT
    expect "$T/c1.log" "type 'exit'" "Ctrl+C at the prompt explains how to quit"
    check "and doesn't exit either" running c1
}

test_logs_after_finish_and_truncation() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/spam 1000 -cores 1 -ram 64 -time 10 -prio 2" > /dev/null   # ~110 KB
    expect "$T/c1.log" "Job 1 COMPLETED" "spam job done"
    send c1 "logs 1"
    expect "$T/c1.log" "earlier bytes no longer kept" "says the start was dropped"
    expect "$T/c1.log" "spam line 1000 " "the last 64 KB are still there"
    expect_not "$T/c1.log" "spam line 1 \." "the first lines are gone"
    send c1 "logs 1 -f"
    expect_count "$T/c1.log" "-- Job 1 finished" 1 "logs -f on a finished job prints and returns"
}

test_ram_telemetry_climbs() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 4096
    start_client c1 lordz
    submit c1 "$BIN/memleak -cores 1 -ram 2000 -time 30 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "leaking job runs"
    sleep 2.5
    send c1 "status 1"
    wait_for "$T/c1.log" "Current RAM" 5
    local a b
    a=$(grep -oE "Current RAM: [0-9]+" "$T/c1.log" | tail -1 | grep -oE "[0-9]+$")
    sleep 2.5
    send c1 "status 1"
    sleep 0.5
    b=$(grep -oE "Current RAM: [0-9]+" "$T/c1.log" | tail -1 | grep -oE "[0-9]+$")
    check "Current RAM climbs (${a:-?}MB -> ${b:-?}MB)" [ "${b:-0}" -gt "${a:-0}" ]
    expect "$T/c1.log" "Current RAM: [0-9]+MB / 2000MB Limit" "shows the limit"
    expect "$T/c1.log" "Page Faults: [1-9][0-9]*" "page faults counted"
    expect "$T/c1.log" "CPU Time: [0-9.]+s" "CPU time shown"
    send c1 "cancel 1"
    expect "$T/c1.log" "CANCELLED" "cancelled"
    sleep 1
    send c1 "status 1"
    expect "$T/c1.log" "Peak RAM: [1-9][0-9]*MB / 2000MB Limit" "finished job shows its peak RAM"
}

test_cancel_queued_and_running() {
    start_scheduler FIFO
    start_worker w1 -cores 1 -ram 512
    start_client c1 lordz
    start_client c2 mdot
    submit c1 "$BIN/slow_print 60 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null   # waits for the core
    expect "$T/c1.log" "Job 1 RUNNING" "job 1 running"
    send c2 "cancel 1"
    expect "$T/c2.log" "belongs to lordz" "someone else can't cancel it"
    send c1 "cancel 2"
    expect "$T/sched.log" "Job 2 QUEUED -> CANCELLED \(requested by lordz\)" "QUEUED job cancelled"
    send c1 "cancel 1"
    expect "$T/sched.log" "Job 1 RUNNING -> CANCELLED \(kill sent, requested by lordz\)" "RUNNING job cancelled, kill sent"
    expect "$T/c1.log" "\[Event\] Job 1 RUNNING -> CANCELLED \(requested by lordz\)\." "owner gets the event"
    expect "$T/w1.log" "Job 1: kill requested \(cancel by lordz\), sending SIGKILL" "worker sends SIGKILL"
    expect "$T/w1.log" "Job 1 killed and reaped" "worker reaps it"
    sleep 0.5
    local zombies
    zombies=$(ps -o stat= --ppid "${PIDS[w1]}" 2>/dev/null | grep -c Z)
    check "no zombie children left" [ "${zombies:-0}" -eq 0 ]
    check "the job process is gone" bash -c "! pgrep -f '[s]low_print 60' > /dev/null"
    send c1 "nodes"
    expect "$T/c1.log" "1001 +ALIVE +1 / 1 " "its core is free again"
    send c1 "cancel 1"
    expect "$T/c1.log" "Job 1 is already CANCELLED" "cancelling twice is an error"
}

test_heartbeat_suspected_then_recovers() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 30 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "job running"
    signal_proc w1 STOP          # frozen: no heartbeats, but the connection stays
    expect "$T/sched.log" "Worker 1001 ALIVE -> SUSPECTED \(missed heartbeat\)" "SUSPECTED after a missed heartbeat" 5
    expect "$T/c1.log" "\[Event\] WARNING: Worker 1001 SUSPECTED \(missed heartbeat\)\." "the job's owner is warned"
    sleep 1
    signal_proc w1 CONT
    expect "$T/sched.log" "Worker 1001 SUSPECTED -> ALIVE \(heartbeat arrived\)" "back to ALIVE when a heartbeat arrives" 5
    expect_not "$T/sched.log" "DEAD" "never declared DEAD"
    send c1 "status 1"
    expect "$T/c1.log" "Job 1 \[RUNNING\]" "the job kept running"
}

test_worker_death_rescues_job() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 16 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING on Worker 1001" "job on worker 1001"
    start_worker w2 -cores 2 -ram 512
    signal_proc w1 KILL
    local t0=$SECONDS
    expect "$T/sched.log" "Worker 1001 ALIVE -> SUSPECTED" "SUSPECTED first" 5
    expect "$T/sched.log" "Worker 1001 SUSPECTED -> DEAD \(3 missed heartbeats\)" "then DEAD" 10
    local took=$((SECONDS - t0))
    check "declared DEAD after about 6s (took ${took}s)" [ "$took" -ge 4 -a "$took" -le 9 ]
    expect "$T/sched.log" "Job 1 RUNNING -> LOST -> QUEUED" "job goes RUNNING -> LOST -> QUEUED"
    expect "$T/c1.log" "\[Event\] WARNING: Worker 1001 DEAD\." "owner warned"
    expect "$T/c1.log" "\[Event\] Job 1 \(attempt 1\) moved back to QUEUED" "owner told it's requeued"
    expect "$T/c1.log" "\[Event\] Job 1 \(attempt 2\) RUNNING on Worker 1002\." "it runs again elsewhere"
    expect "$T/c1.log" "Job 1 COMPLETED" "and completes" 15
    sleep 0.3
    check "the old job process died with its worker" bash -c "! pgrep -f '[s]low_print 16' > /dev/null"
}

test_failed_permanent_after_three_deaths() {
    start_scheduler FIFO
    start_client c1 lordz
    start_worker w1 -cores 1 -ram 512
    submit c1 "$BIN/slow_print 100 -cores 1 -ram 64 -time 100 -prio 2" > /dev/null
    local attempt name
    for attempt in 1 2 3; do
        name=w$attempt
        expect "$T/c1.log" "Job 1 .*RUNNING on Worker" "attempt $attempt running" 15
        [ "$attempt" -lt 3 ] && start_worker w$((attempt + 1)) -cores 1 -ram 512
        signal_proc "$name" KILL
        if [ "$attempt" -lt 3 ]; then
            expect "$T/c1.log" "Job 1 \(attempt $attempt\) moved back to QUEUED" "attempt $attempt lost, requeued" 12
            expect "$T/c1.log" "Job 1 \(attempt $((attempt + 1))\) RUNNING" "attempt $((attempt + 1)) starts" 10
        fi
    done
    expect "$T/sched.log" "Job 1 RUNNING -> LOST -> FAILED_PERMANENT \(attempt 3 of 3\)" "third loss is FAILED_PERMANENT" 12
    expect "$T/c1.log" "Job 1 FAILED_PERMANENT" "owner is told"
}

test_dead_worker_comes_back_with_new_id() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    signal_proc w1 STOP
    expect "$T/sched.log" "Worker 1001 SUSPECTED -> DEAD" "frozen worker declared DEAD" 10
    signal_proc w1 CONT
    expect "$T/w1.log" "Lost connection to the scheduler" "it notices it was cut off" 5
    expect "$T/w1.log" "Registered successfully\. ID: 1002\." "and registers again with a new ID" 10
}

test_drain_from_stdin() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 8 -cores 1 -ram 64 -time 30 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING on Worker 1001" "job on worker 1001"
    start_worker w2 -cores 2 -ram 512
    send w1 "drain"
    expect "$T/w1.log" "Draining initiated locally" "worker starts draining"
    expect "$T/sched.log" "Worker 1001 ALIVE -> DRAINING" "scheduler marks it DRAINING"
    submit c1 "$BIN/hello -cores 1 -ram 64 -time 5 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 2 RUNNING on Worker 1002" "new jobs avoid the draining worker"
    expect "$T/c1.log" "Job 1 COMPLETED" "the running job finishes normally" 10
    expect "$T/w1.log" "Drain complete \(0 active jobs\)\. Exiting gracefully\." "then the worker exits" 5
    sleep 0.5
    check "worker process is gone" bash -c "! kill -0 ${PIDS[w1]} 2>/dev/null"
    expect "$T/sched.log" "Worker 1001 DRAINING -> DEAD \(drained, left the cluster\)" "logged as a clean exit"
    expect_not "$T/sched.log" "LOST" "nothing is rescued"
}

test_drain_with_ctrl_c_keeps_job_alive() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 6 -cores 1 -ram 64 -time 30 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "job running"
    # A real Ctrl+C goes to the terminal's whole foreground process group.
    # The worker has its own (setsid), so this is the worker plus anything
    # still in its group; jobs must have moved to their own groups.
    if [ -n "$SETSID" ]; then kill -INT -- "-${PIDS[w1]}"; else signal_proc w1 INT; fi
    expect "$T/w1.log" "Draining initiated locally" "Ctrl+C starts a drain"
    expect "$T/c1.log" "Job 1 COMPLETED" "the job was not killed by the Ctrl+C" 10
    expect "$T/w1.log" "Exiting gracefully" "worker exits after it"
}

test_scheduler_restart_everyone_reconnects() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 40 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    expect "$T/c1.log" "Job 1 RUNNING" "job running"
    stop sched
    expect "$T/w1.log" "Lost connection to the scheduler" "worker notices"
    expect "$T/w1.log" "Killing 1 running job" "worker kills its jobs"
    expect "$T/c1.log" "Lost connection to the scheduler\. Reconnecting" "client notices"
    start_scheduler FIFO
    expect "$T/w1.log" "Registered successfully\. ID: 1001" "worker registers with the new scheduler" 15
    expect "$T/c1.log" "Reconnected as lordz\." "client logs back in by itself" 15
    expect_count "$T/sched.log" "Client 'lordz' logged in" 2 "sends the same username again"
    sleep 0.3
    check "old job process is gone" bash -c "! pgrep -f '[s]low_print 40' > /dev/null"
}

test_client_reconnects_while_following() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    start_client c1 lordz
    submit c1 "$BIN/slow_print 40 -cores 1 -ram 64 -time 60 -prio 2" > /dev/null
    send c1 "logs 1 -f"
    expect "$T/c1.log" "line 2 of 40" "following"
    stop sched
    start_scheduler FIFO
    expect "$T/c1.log" "Reconnected as lordz\." "reconnects mid-follow" 15
    send c1 "nodes"
    expect "$T/c1.log" "Cluster Status" "and the prompt works"
}

test_tty_event_while_typing() {
    start_scheduler FIFO
    start_worker w1 -cores 2 -ram 512
    open_fifo tty
    python3 "$ROOT/tests/pty_run.py" "$T/tty.out" "$ROOT/client" 127.0.0.1 "$PORT" \
        -user lordz -discovery-port "$DPORT" <&"${FDS[tty]}" > /dev/null 2>&1 &
    PIDS[tty]=$!
    expect "$T/tty.out" "client> " "client prompt in a real terminal"
    send tty "LINE submit $BIN/slow_print 2 -cores 1 -ram 64 -time 10 -prio 2"
    expect "$T/tty.out" "Job ID assigned: 1" "submitted"
    send tty "TYPE stat"     # half-typed command when the event arrives
    expect "$T/tty.out" "Job 1 COMPLETED" "event arrives while typing" 10
    check "event line clears the prompt line first (\\r ESC[K)" grep -qaF $'\r\e[K[Event]' "$T/tty.out"
    send tty "LINE us 1"
    expect "$T/tty.out" "Job 1 \[COMPLETED\]" "the half-typed command still runs"
    send tty "CTRL_C"
    expect "$T/tty.out" "type 'exit'" "Ctrl+C in a terminal doesn't quit"
    send tty "QUIT"
}

run_tests "$@"
