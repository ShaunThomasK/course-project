# Distributed Compute Cluster

A scheduler, workers and clients in C over plain TCP, for the OSN course project.

## Building

```sh
make          # builds ./scheduler ./worker ./client
make tests    # builds the test programs into tests/programs/bin/
```

Works on Linux and macOS with any C11 compiler; there are no dependencies.

## Running

```sh
./scheduler 8080 PRIORITY                 # or FIFO; [-discovery-port <port>]
sudo ./worker -cores 4 -ram 2048 -gpu 1   # [<ip> <port>] fallback, [-discovery-port <port>]
./client -user lordz                      # [<ip> <port>] fallback, [-discovery-port <port>]
```

Without `sudo` the worker warns and runs jobs as you. It skips the `chown` and
the `setuid`, so you can develop without root.

Client commands: `nodes`, `submit <binary> [args...] -cores N -ram MB -time SEC
-prio 1-3 [-gpu N]`, `status <id>`, `logs <id> [-f]`, `cancel <id>`, `help`,
`exit`.

On the worker, type `drain` (or press Ctrl+C) to finish the current jobs and
leave the cluster. `jobs` lists what is running.

## Status

| Milestone | Sections | State |
|---|---|---|
| 1 Core pipeline | 2, 3, 4A-B, 5A-B, 7 | done |
| 2 Resilience & telemetry | 4C-D, 5C-E, 6, 9 | done |
| 3 Advanced scheduling | 8, 10, 11, 12, 13 | not started (state machine and fields already in place) |
| 4 Checkpoints & migration | 14 | not started (`checkpoint/` dir and `OSN_CHECKPOINT_DIR` already set up) |

## Design

### Layout

```
src/common/    common.c     state enums, log_line(), small helpers
               proto.c      message framing, buffered connections, sockets
               discovery.c  UDP discovery (client side and responder)
src/scheduler/ scheduler.c  event loop, connections, request handlers, heartbeats
               jobs.c       job state transitions, log buffers, status/nodes text
               pass.c       the scheduling pass and worker choice
src/worker/    worker.c     connection, heartbeats, drain, main loop
               jobrun.c     fork/limits/exec, output capture, telemetry, diagnosis
src/client/    client.c     prompt, commands, async events
```

### Scheduler concurrency (4A)

The scheduler uses the event-loop model. One thread `poll()`s the listening
socket, the UDP socket and every connection. Every socket is non-blocking,
and every connection has its own output buffer, so a slow or stuck peer can
never block the loop. The job table and worker table therefore need no locks.

`status` and `nodes` are answered as soon as they are read, and a scheduling
pass is a short in-memory loop, so a query never waits behind a decision. The
pass runs after anything relevant (submit, job end, register, heartbeat,
cancel) and at least once a second.

The queue is bounded at 1024 entries (QUEUED + WAITING). When it is full,
submit gets `QUEUE_FULL` straight away. Jobs the scheduler requeues itself
(rescues) bypass the limit.

### State changes

Every job state change goes through `job_set_state()` in `jobs.c`. It logs the
transition and keeps the worker's free cores, RAM and GPUs in step, so the
accounting can't drift. Worker transitions go through `worker_set_state()`.

### Failure detection (4C)

Workers heartbeat every 2 s. The scheduler counts the misses with 0.5 s of
slack: one miss makes a worker SUSPECTED, three make it DEAD.

A dropped TCP connection does not make the worker DEAD at once. It goes
through the same SUSPECTED → DEAD path, which matches the spec's example
output.

A DEAD worker's connection is cut, so if it comes back it has to register
again with a new ID. Its RUNNING jobs go LOST → QUEUED. On the third failed
attempt they go to FAILED_PERMANENT instead (`MAX_JOB_ATTEMPTS` in
`common.h`).

A DRAINING worker that disconnects with no jobs left is a clean exit, and
nothing is rescued.

### Job isolation (5A)

The child does the five steps in the order the spec gives. It also does three
extra things:

* **Own process group.** Ctrl+C in the worker's terminal (which starts a drain)
  must not reach the jobs, and a kill should reach a job's children too.
* **`PR_SET_PDEATHSIG` (Linux).** If the worker is SIGKILLed, its jobs die with
  it instead of running on as orphans. This is set after `setuid()`, because
  changing credentials clears it.
* **`RLIMIT_CORE = 0`.** Core dumps would fill the job directory and delay the
  crash report.

If any setup step fails, the child writes the reason to a close-on-exec pipe
and exits without running the job. The worker reports it as "Job setup failed
at setuid (...)" instead of a confusing exit code.

`RLIMIT_NPROC` counts every process of the user the job runs as. Under `sudo`
that is `nobody`, so all jobs on a worker share the budget of 64. Without
`sudo` it would be *your* user, which already has hundreds of processes, so a
job that forks will fail during development. Single-process jobs are
unaffected.

### Two clients with the same name

Both sessions are accepted. They count as the same user: both see that user's
jobs and both get the events.

### Cancel

Only the owner can cancel a job. A RUNNING job is marked CANCELLED and its
resources are released the moment the kill is sent, as in the spec's log
line `RUNNING -> CANCELLED (kill sent, ...)`. The worker kills the job and
reaps it with `waitpid()`.

## Protocol

Every message is one frame:

```
u32 total (big-endian) | u32 header_len | header | blob
```

The header is text. The first line is the message type, and every further line
is `key=value`, with `\` and newline in values escaped as `\\` and `\n`. The
blob is raw bytes: an executable, a chunk of output, a checkpoint.

| From → to | Type | Fields / blob |
|---|---|---|
| client → sched | `LOGIN` | `user` |
| sched → client | `LOGIN_OK` | `user` |
| client → sched | `SUBMIT` | `path argc arg0.. cores ram time prio gpu` |
| client → sched | `STATUS` / `CANCEL` | `job` |
| client → sched | `NODES` | |
| client → sched | `LOGS` | `job follow` |
| client → sched | `UNFOLLOW` | `job` |
| sched → client | `REPLY` | `ok text` (+ `job`, or `code=QUEUE_FULL`) |
| sched → client | `EVENT` | `text` |
| sched → client | `LOG_DATA` | `job [truncated]`, blob = output |
| sched → client | `LOG_END` | `job state` |
| worker → sched | `REGISTER` | `cores ram gpu host` |
| sched → worker | `REGISTER_OK` | `id` |
| worker → sched | `HEARTBEAT` | `load jobs tele=id:rss_kb:peak_kb:faults:cpu_ms;...` |
| sched → worker | `RUN_JOB` | `job path argc arg0.. cores ram time gpu [binary=1]`, blob = executable |
| sched → worker | `KILL_JOB` | `job reason by` |
| worker → sched | `JOB_OUTPUT` | `job`, blob = output chunk |
| worker → sched | `JOB_DONE` | `job result reason exit_code signal peak_kb faults cpu_ms` |
| worker → sched | `DRAIN` | |

Discovery is UDP. The asker sends `OSN_DISCOVER <nonce>` to 255.255.255.255,
to every interface's broadcast address, and to 127.0.0.1. The scheduler
replies `OSN_SCHEDULER <nonce> <tcp_port>`, and the source address of that
reply is the scheduler's IP. The asker makes 3 attempts with a 2 s wait each,
then falls back to the command-line address.

## Testing notes

Tested so far on Linux, without root:
* discovery and the fallback address
* binary transfer, PATH commands and program arguments
* streaming `logs -f`
* the timeout, SIGSEGV, SIGFPE and exit-127 diagnoses
* RAM telemetry climbing
* worker kill -9 → SUSPECTED → DEAD → rescue as attempt 2
* SIGSTOP → SUSPECTED → ALIVE
* drain
* cancelling a queued and a running job
* priority aging order
* QUEUE_FULL at 1024
* clients and workers reconnecting after a scheduler restart

Still to test: running under `sudo` (`escape_attempt` should get "Permission
denied" outside its job directory) and macOS.
