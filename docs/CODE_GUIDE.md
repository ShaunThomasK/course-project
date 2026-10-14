# Code Guide: the Distributed Compute Cluster, explained

This guide walks through every part of the code: what each file is for, what
the important functions do, how control flows between them, and why things
are done the way they are. File and function names match the source, so you
can keep the code open beside it.

**Contents**

1. [The big picture](#1-the-big-picture)
2. [One job, start to finish](#2-one-job-start-to-finish)
3. [Shared code (`src/common/`)](#3-shared-code-srccommon)
4. [The scheduler (`src/scheduler/`)](#4-the-scheduler-srcscheduler)
5. [The worker (`src/worker/`)](#5-the-worker-srcworker)
6. [The client (`src/client/`)](#6-the-client-srcclient)
7. [Protocol reference](#7-protocol-reference)
8. [Ideas that cut across files](#8-ideas-that-cut-across-files)
9. [Scenario walkthroughs](#9-scenario-walkthroughs)
10. [The test suite (`tests/`)](#10-the-test-suite-tests)
11. [Design choices and limitations](#11-design-choices-and-limitations)
12. [Questions you might be asked](#12-questions-you-might-be-asked)

---

## 1. The big picture

Three programs, built by `make` from C sources:

| Program | Role | Source |
|---|---|---|
| `scheduler` | Central server. Holds the job table and the worker table, decides which job runs where, keeps logs and checkpoints, pushes events to users. | `src/scheduler/*.c` |
| `worker` | One per machine. Registers its capacity, runs jobs as isolated child processes, streams their output, sends heartbeats. | `src/worker/*.c` |
| `client` | What a person types into. Sends commands, prints replies and `[Event]` lines. | `src/client/client.c` |

All three link the shared code in `src/common/`: logging, the wire protocol,
UDP discovery and SHA-256.

```
              submit/status/logs/...                RUN_JOB / KILL_JOB / RESTORE_CKPT
  client  ─────────────────────────────►  scheduler  ─────────────────────────────►  worker
          ◄─────────────────────────────            ◄─────────────────────────────
              REPLY / EVENT / LOG_DATA                HEARTBEAT / JOB_OUTPUT / JOB_DONE /
                                                      CHECKPOINT / CACHE_* / DRAIN
```

Everything travels over TCP, except discovery, which uses UDP broadcast to
find the scheduler in the first place.

### File map

```
src/common/    common.h/.c     constants, state enums, now_ms(), username rules, log_line()
               proto.h/.c      buf_t, msg_t, framing, conn_t, socket helpers
               discovery.h/.c  UDP discovery (both sides)
               sha256.h/.c     SHA-256, for identifying binaries by content
src/scheduler/ scheduler.h     all scheduler data structures + shared prototypes
               scheduler.c     main(), event loop, connections, request/message handlers,
                               heartbeat checking
               jobs.c          job lifecycle (state changes, rescue, dependencies),
                               log buffers, status/nodes text
               pass.c          the scheduling pass: order, hold, quota, match, choose,
                               preempt, dispatch, binary hashing, RUN_JOB building
               cron.c          recurring job templates
src/worker/    worker.h        wjob_t + prototypes for jobrun.c and cache.c
               worker.c        main(), main loop, scheduler connection, heartbeats,
                               RUN_JOB/KILL_JOB handling, drain, checkpoints
               jobrun.c        fork + isolation + limits + exec, telemetry, output,
                               reaping, crash diagnosis
               cache.c         executable cache folder with LRU eviction
src/client/    client.c        everything client side
tests/                         automated test suite (Section 10 of this guide)
```

### Build

`Makefile` targets:
* `make` / `make all` builds `scheduler`, `worker` and `client` with
  `-std=gnu11 -Wall -Wextra`. Each program is one `cc` command over its own
  files plus `COMMON`.
* `make tests` compiles every `tests/programs/*.c` into `tests/programs/bin/`
  (with `-pthread`, which `cpu_hog` needs). It also builds `big_a`, `big_b`
  and `big_c` from `bigprog.c` with different `-DVARIANT` values, giving
  three programs with different hashes.
* `make clean` removes the binaries.

---

## 2. One job, start to finish

Before diving into files, here is what happens when a user types
`submit ./hello world -cores 1 -ram 64 -time 5 -prio 2` into the client.
Every step names the function that does it.

1. **Client parses the line.**
   * `handle_stdin()` reads bytes into a line buffer.
   * `run_pending_lines()` takes one full line and calls
     `run_command()` → `tokenize()` → `cmd_submit()`.
   * `cmd_submit()` turns `./hello` into an absolute path with `realpath()`,
     because the scheduler runs in a different directory. It collects the
     flags, and treats `world` as a program argument (`arg0=world`).
   * It builds a `SUBMIT` message, sends it with `send_msg()`, and sets
     `g_wait = WAIT_SUBMIT`. The prompt now waits for the answer.
2. **Scheduler receives it.**
   * In the main loop, `poll()` reports the client socket readable, so
     `read_peer()` runs `conn_read()` and then `conn_next()`, which decodes
     the frame into a `msg_t`.
   * `peer_dispatch()` sees a `PEER_CLIENT` and calls `handle_client_msg()`,
     which calls `handle_submit()`.
   * `handle_submit()` validates (`submit_invalid()`), checks the queue isn't
     full (`queue_occupancy()`), creates the job with `job_create()` (state
     QUEUED, id = next number), logs `Job 1 SUBMITTED by lordz`, and replies
     with `REPLY ok=1 job=1 state=QUEUED`.
   * `job_create()` set `g_sched_dirty = 1`.
3. **Scheduling pass.**
   * At the end of the same loop iteration `g_sched_dirty` is set, so
     `schedule_pass()` runs.
   * It lists the QUEUED jobs, sorts them, and for each one checks the hold
     and the quota, then calls `choose_worker()`.
   * `choose_worker()` finds the workers with enough free resources and picks
     the best by the tiebreakers, logging why.
   * `dispatch()` updates attempts and the run number, and moves the job to
     RUNNING through `job_set_state()`. That also reserves the cores, RAM and
     GPUs on the worker.
   * `dispatch()` then calls `send_run_job()`. It sends the checkpoint first
     if the job has one, then `RUN_JOB`. The binary goes along as the blob,
     unless the worker already caches it.
   * The owner gets `[Event] Job 1 RUNNING on Worker 1001.`
4. **Worker starts it.**
   * The worker's loop decodes `RUN_JOB` and calls `handle_run_job()`.
   * `prepare_binary()` verifies the SHA-256 and stores the file in the cache.
   * `jobrun_start()` does `fork()`. The child runs `run_child()`: its own
     process group, the job directory, the binary copy, `output.log`,
     `chown`, `setuid(nobody)`, the rlimits, then `execvp()`.
5. **While it runs.**
   * Every 100 ms, `tick_jobs()` checks whether the job exited
     (`jobrun_reap()`) and whether its wall-clock limit has passed.
   * Every 500 ms it reads new output (`pump_output()` → `JOB_OUTPUT`),
     samples memory and CPU (`jobrun_sample()`), and looks for a new
     checkpoint (`pump_checkpoint()`).
   * Every 2 s, `send_heartbeat()` sends the CPU load and each job's
     telemetry.
   * On the scheduler, `JOB_OUTPUT` goes into the job's 64 KB buffer
     (`job_append_output()`) and on to anyone running `logs -f`.
6. **It ends.**
   * `jobrun_reap()` sees the exit, so `finish_job()` runs.
   * It sends the rest of the output, decides what happened
     (`jobrun_diagnose()`), sends `JOB_DONE result=COMPLETED`, and deletes
     `/tmp/osn_job_1`.
   * The scheduler's `handle_job_done()` sends
     `[Event] Job 1 COMPLETED successfully.` and calls
     `job_set_state(COMPLETED)`.
   * That frees the worker's resources, ends any `logs -f` followers, and
     re-checks jobs that depended on job 1.

Keep this flow in mind; the rest of the guide zooms into each step.

---

## 3. Shared code (`src/common/`)

### 3.1 `common.h` / `common.c`: constants, states, helpers, logging

**Tunables** (all `#define`s in `common.h`, so they're constants in the code,
as the spec asks for the retry limit):

| Constant | Value | Used for |
|---|---|---|
| `DEFAULT_DISCOVERY_PORT` | 9999 | UDP discovery port when `-discovery-port` isn't given |
| `DISCOVERY_ATTEMPTS`, `DISCOVERY_TIMEOUT_MS` | 3, 2000 | 3 tries × 2 s (2C) |
| `HEARTBEAT_INTERVAL_MS` | 2000 | worker heartbeat period (4C) |
| `DEAD_AFTER_MISSES` | 3 | 3 missed heartbeats → DEAD |
| `MAX_JOB_ATTEMPTS` | 3 | retry limit for worker failures (4C) |
| `JOB_QUEUE_CAPACITY` | 1024 | bounded queue (4A) |
| `AGING_INTERVAL_MS` | 10000 | PRIORITY aging step (4B) |
| `OUTPUT_KEEP_BYTES` | 64 KB | per-job log tail kept by the scheduler (6) |
| `OUTPUT_POLL_MS` | 500 | how often the worker reads output.log (5B) |
| `JOB_NPROC_LIMIT` | 64 | `RLIMIT_NPROC` for jobs (5A step 5) |
| `USERNAME_MAX` | 32 | 31 chars + NUL |
| `FIRST_WORKER_ID` | 1001 | first worker ID handed out |

**State enums.** `job_state_t` has all eight job states from the spec:
WAITING, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED, LOST,
FAILED_PERMANENT. `worker_state_t` has ALIVE, SUSPECTED, DEAD, DRAINING.
They were declared in full from day one, as the spec recommends, so later
milestones never had to change the data structures.

* `job_state_name()` / `worker_state_name()` turn a state into the text used
  in logs and messages.
* `job_state_is_terminal()` is true for COMPLETED, FAILED, CANCELLED and
  FAILED_PERMANENT, the states a job never leaves.

**Helpers**
* `now_ms()`: milliseconds from `CLOCK_MONOTONIC`. Monotonic means it never
  jumps when the wall clock is changed, so differences (timeouts, heartbeat
  gaps) are always right. Only differences are meaningful.
* `username_valid()`: 1–31 characters from `[A-Za-z0-9_.-]` (Section 3).
* `str_copy()`: a safe `strcpy` that truncates and always NUL-terminates.
* `parse_long()`: parses a *whole* string as a decimal number (`"12x"` fails),
  used for every numeric flag.

**Logging (Section 7)**
* `log_set_tag()` sets the bracketed tag: `SCHEDULER`, `WORKER`, or
  `WORKER 1001` once registered, or `Client`.
* `log_line()` is the single central logging function. It formats
  `[HH:MM:SS] [TAG] message` into one buffer and writes it with one `fputs`,
  so lines never interleave halfway.
* **The prompt hook.** `log_set_prompt("worker> ")` makes `log_line()` keep an
  input prompt at the bottom of the terminal. Before printing, it sends
  `\r\033[K` (go to start of line, erase it, which wipes the prompt). After
  printing, it redraws the prompt.
  * It only switches on when both stdin and stdout are terminals
    (`isatty`), so redirected logs never contain prompts.
  * `log_show_prompt()` redraws it, for example after an empty Enter.
  * An `atexit` handler clears the prompt line when the program exits.

### 3.2 `proto.h` / `proto.c`: the wire protocol

The code has three layers: a byte buffer, a message, and a connection.

#### `buf_t`, the growable byte buffer

```c
typedef struct { char *data; size_t off, len, cap; } buf_t;
```
* Valid bytes are `data[off .. len)`.
* `buf_append()` adds bytes. When it needs room, it first slides the unread
  part to the front (reclaiming consumed space), then doubles `cap`.
* `buf_consume(n)` drops n bytes from the front by moving `off`. If
  everything is consumed it resets to empty, without moving any memory.
* `buf_used()` returns `len - off`, the number of unread bytes.

Every connection has two of these: `in` (received, not yet decoded) and
`out` (encoded, not yet sent).

#### `msg_t`, one message

```c
typedef struct { char type[32]; int nfields, capfields; char **keys, **vals;
                 char *blob; size_t blob_len; } msg_t;
```
A message has a **type** (`"SUBMIT"`, `"HEARTBEAT"`, ...), a list of
**key=value string fields**, and an optional **blob** of raw bytes. The blob
carries an executable, a chunk of job output, or a checkpoint.

* `msg_new()`, `msg_free()`, `msg_clone()`. Cron keeps a clone of the
  original SUBMIT.
* `msg_set()` / `msg_setf()` / `msg_set_long()` set a field, replacing it if
  the key already exists.
* `msg_get()` returns NULL if a field is missing, `msg_get_str()` returns `""`,
  and `msg_get_long(m, key, def)` returns `def` if the field is missing or
  isn't a number.
* `msg_set_blob()` copies the data. `msg_take_blob()` takes ownership of a
  `malloc`ed buffer without copying, which matters for 30 MB binaries.
* `msg_is(m, "TYPE")` compares the type.

#### Frame format (`msg_encode()` / `msg_decode()`)

```
+--------------+---------------+--------------------------+-------------+
| u32 total    | u32 hdrlen    | header (hdrlen bytes)    | blob        |
+--------------+---------------+--------------------------+-------------+
  big-endian     big-endian      "TYPE\nkey=value\n..."     raw bytes
  total = 4 + hdrlen + bloblen
```
* Lengths are big-endian (`htonl`/`ntohl`), so machines of either byte order
  agree.
* The header is text. Inside values, `\` becomes `\\` and newline becomes
  `\n` (`append_escaped()` / `unescape()`), so a field can hold any string,
  even a multi-line `status` rendering. A raw newline therefore only ever
  separates lines.
* `msg_decode()` returns 0 if fewer than `4 + total` bytes have arrived. It
  doesn't need a full frame up front: it waits until the whole frame is in
  `in`. It returns -1 for a corrupt frame (`total` > `MSG_MAX_FRAME`, 512 MB,
  or `hdrlen` too big), and 1 with a decoded message otherwise. It then
  consumes exactly that frame from the buffer.

**Why length-prefixed?** TCP is a byte stream with no message boundaries: one
`recv()` can return half a message or three messages. The length prefix lets
the receiver know exactly where each message ends.

#### `conn_t`, a buffered connection

```c
typedef struct { int fd; buf_t in, out; } conn_t;
```
* `conn_send()` encodes the message onto `out`, then calls `conn_flush()`.
* `conn_flush()` calls `send()` until `out` is empty or the socket would
  block (`EAGAIN`). It never waits. Leftover bytes stay in `out`, and the
  scheduler's poll loop asks for `POLLOUT` until they're gone. It uses
  `MSG_NOSIGNAL` on Linux, so writing to a dead socket returns an error
  instead of killing the process with SIGPIPE.
* `conn_flush_all()` *does* wait (with `poll(POLLOUT)`) until everything is
  sent. Only the worker and the client use it; they talk to one peer and can
  afford to.
* `conn_read()` reads what's available into `in`. It returns >0 if alive
  (including "nothing right now", `EAGAIN` → 1), 0 on EOF (peer closed), and
  -1 on error.
* `conn_next()` pulls one decoded message out of `in`.

#### Socket helpers
* `set_nonblocking()` sets `O_NONBLOCK` with `fcntl`.
* `tcp_listen(port)`: `SO_REUSEADDR` (restart without "address in use"),
  bind to all interfaces, `listen(64)`, non-blocking.
* `tcp_connect(ip, port, timeout)`: a non-blocking `connect()`, then `poll`
  for writability with a timeout, then check `SO_ERROR`. This is how you
  connect with a timeout in C. It also sets `TCP_NODELAY` (small messages
  like heartbeats go out at once rather than being batched by Nagle's
  algorithm), and `SO_NOSIGPIPE` on macOS.

### 3.3 `discovery.h` / `discovery.c`: finding the scheduler (2C)

**Scheduler side**
* `discovery_open_responder(port)` creates a UDP socket with `SO_REUSEADDR`
  **and** `SO_REUSEPORT` (the spec asks for both, so a restarted scheduler
  can rebind immediately) and binds it to all interfaces.
* `discovery_handle_request(fd, tcp_port)` reads one datagram. If it is
  `OSN_DISCOVER <nonce>`, it replies `OSN_SCHEDULER <nonce> <tcp_port>` to
  the sender. Anything else is silently ignored.

**Asking side** (`discovery_resolve()`, used by the worker and the client)
1. Creates a UDP socket with `SO_BROADCAST`.
2. Builds a **nonce** of `<pid>-<now_ms>`. Replies must echo it, so a stray
   reply from an earlier attempt, or from another team's scheduler answering
   someone else, is ignored.
3. Makes up to 3 attempts (`discover_once()`). Each one:
   * sends the request with `send_requests()` to three kinds of destination:
     * `255.255.255.255`, the limited broadcast;
     * **each interface's directed broadcast address**, from `getifaddrs()`,
       e.g. `192.168.1.255`, since some systems don't route the limited
       broadcast;
     * **127.0.0.1**, because broadcast doesn't always loop back to a
       scheduler on the same machine;
   * then `poll`s for up to 2 s for a matching reply. The scheduler's IP is
     the **source address of the reply** (`recvfrom` fills `from`), and its
     port is in the payload.
4. If all attempts fail and a fallback IP and port were given, it uses them
   and logs `No discovery reply, using fallback ...`. Otherwise it returns -1,
   and the caller prints an error and exits.

Every outcome is logged, as Section 7 requires.

### 3.4 `sha256.h` / `sha256.c`

A standard SHA-256 written from the FIPS 180-4 description, so the project
needs no OpenSSL. It is checked against `sha256sum`.
* `sha256_init/update/final_hex` are the streaming interface.
* `sha256_hex(data, len, hex)` is the one-shot version and gives 64 lowercase
  hex characters.

Internally, `block()` processes 64-byte chunks with the 64 round constants
`K[]`. `sha256_final_hex()` adds the padding (`0x80`, zeros, then the 64-bit
bit length) before the last block. Section 13 uses it to identify programs
by content.

---

## 4. The scheduler (`src/scheduler/`)

### 4.1 Concurrency model: why an event loop

The spec offers two models: a single-threaded event loop, or a thread per
connection with mutexes. This project uses the **event loop**.

* **One thread** runs `poll()` over every socket: the TCP listener, the UDP
  discovery socket, and every client and worker connection.
* **All sockets are non-blocking**, and each connection has its own output
  buffer. A slow client that doesn't read its replies just accumulates bytes
  in its `out` buffer; nobody else waits.
* **No locks are needed.** Only one thread ever touches the job table and
  the worker table, so there can be no data races or deadlocks. The spec's
  "thread-safe bounded queue" requirement is met trivially.
* **`status` / `nodes` are never stuck behind scheduling.** A scheduling pass
  is a short in-memory computation that runs between handling messages. A
  `status` request is answered the moment it is read.

### 4.2 Data structures (`scheduler.h`)

#### `peer_t`, one TCP connection
| Field | Meaning |
|---|---|
| `conn` | the buffered connection |
| `kind` | `PEER_NEW` (hasn't said who it is), `PEER_CLIENT` or `PEER_WORKER` |
| `closing` | close once the output buffer drains (or immediately if it can't) |
| `user` | clients: the logged-in name |
| `follow_job` | clients: job whose `logs -f` this connection is following (0 = none) |
| `worker_id` | workers: which `worker_t` this connection belongs to |

#### `worker_t`, one registered worker
| Field | Meaning |
|---|---|
| `id`, `state`, `host` | ID from 1001 up, ALIVE/SUSPECTED/DEAD/DRAINING, hostname |
| `peer` | its connection, **NULL once disconnected** (the record survives so its jobs can be rescued) |
| `cores/free_cores`, `ram_mb/free_ram_mb`, `gpus/free_gpus` | advertised totals and what's unreserved right now |
| `cpu_load`, `reported_jobs` | from the last heartbeat (4D) |
| `running_jobs` | the scheduler's **own** count of RUNNING jobs there, updated instantly; used for "idle vs busy" |
| `last_heartbeat_ms`, `missed` | liveness tracking (4C) |
| `cached`, `ncached` | SHA-256s of binaries it has cached (13) |

#### `job_t`, one job (grouped by section)
* **Identity and request:** `id`, `user`, `path` (absolute path or bare
  command), `hash`, `argc/argv` (program arguments), `cores`, `ram_mb`,
  `time_s`, `prio` (1 highest … 3), `gpus`.
* **Lifecycle:**
  * `state` and `worker_id` (the current or last worker).
  * `submit_ms`, `queued_ms` (when it last entered QUEUED, the clock for
    aging), `start_ms`, `end_ms`.
  * `eff_prio_logged`, so aging changes are logged once.
  * `attempts`, which counts toward `MAX_JOB_ATTEMPTS`.
  * `run`, which goes up on every start (see 8.1).
  * `reason`, `exit_code`, `term_signal`.
* **Telemetry (5D):** `rss_kb`, `peak_rss_kb`, `page_faults`, `cpu_s`.
* **Output (6):** `out` (up to 64 KB), `out_len`, `out_total` (bytes ever
  received, to know if some were dropped).
* **Dependencies (8):** `deps`, `ndeps`, `group`.
* **Preemption (10):** `preemptible`; `held_for` (the job id this one was
  preempted for); `free_restart` (the next start costs no attempt).
* **Quota (11):** `quota_skip_logged`, so a skip is logged once.
* **Cron (12):** `cron_id`.
* **Checkpoints / migration (14):** `migrated_from`, `migrating`, `ckpt`,
  `ckpt_len`, `ckpt_num`, `ckpt_ms`.

#### Global state
`g_jobs` (array; `g_jobs[id-1]`, so `job_find()` is O(1)), `g_njobs`,
`g_workers`, `g_peers`, `g_algo` (FIFO or PRIORITY), and **`g_sched_dirty`**.
Anything that could let a job start sets that flag, so a pass runs at the
end of the current loop iteration.

Jobs are never deleted, so finished jobs stay queryable by `status` and
`logs`.

### 4.3 `main()` and the event loop (`scheduler.c`)

**Startup**
1. Parse `<port> <FIFO|PRIORITY> [-discovery-port <port>]`; anything else
   prints usage.
2. `tcp_listen(port)`, then log `Started on port 8080. Algorithm: PRIORITY`.
3. `discovery_open_responder(dport)`, then log
   `Discovery responder listening on UDP port 9999.` If it can't bind, it
   warns and carries on; discovery is off but the fallback address still
   works.
4. Log `Listening for incoming connections...`
5. `signal(SIGPIPE, SIG_IGN)`, so a dead client can't kill the scheduler.

**Each loop iteration**
1. Build the `pollfd` array: `[0]` is the listener, `[1]` the UDP socket,
   and `[2..]` one per peer. Each peer asks for `POLLIN`, plus `POLLOUT` only
   if it has unsent output, so the loop doesn't spin.
2. `poll(..., 200 ms)`. The 200 ms timeout ensures the periodic work below
   runs even when nothing arrives.
3. For each peer with events:
   * readable or hung up → `read_peer()`;
   * writable → `conn_flush()`.

   New peers accepted in this iteration are appended *after* the ones
   polled, so the indices still line up.
4. UDP readable → `discovery_handle_request()`.
5. Listener readable → `accept_peers()`.
6. `check_heartbeats()`, which handles 4C timing.
7. `cron_tick()`, which injects due cron copies.
8. If `g_sched_dirty`, or a second has passed since the last pass, run
   `schedule_pass()`. That's the spec's "whenever something relevant happens
   … and at least once per second". The once-a-second pass is also what
   makes aging take effect.
9. `sweep_peers()`: remove peers marked `closing`.

### 4.4 Connections

* **`accept_peers()`** accepts every pending connection (a loop until
  `accept` fails with `EAGAIN`), makes each non-blocking with `TCP_NODELAY`,
  and wraps it in a `peer_t` of kind `PEER_NEW`.
* **`read_peer()`** calls `conn_read()`.
  * On EOF or error: mark `closing` and throw away unsent output, since it
    can't be delivered.
  * Otherwise: decode every complete message (`conn_next()` in a loop) and
    hand each to `peer_dispatch()`.
  * A corrupt frame marks the peer `closing`.
* **`peer_dispatch()`** routes by `kind`: `PEER_NEW` to
  `handle_new_peer_msg()`, clients to `handle_client_msg()`, workers to
  `handle_worker_msg()`.
* **`handle_new_peer_msg()`**, the first message decides what the peer is:
  * `LOGIN user=...`: validates the name, then the peer becomes a client and
    gets `LOGIN_OK`.
    * Two sessions with the same name are allowed. They count as the same
      user, so both see that user's jobs and both receive the events.
    * The log mentions the other sessions.
  * `REGISTER cores ram gpu host cached`: creates a `worker_t` with the next
    ID, all resources free, `last_heartbeat_ms = now`, and its cached hashes
    from the list.
    * Logs `Worker 1001 connected. Cores: 4, RAM: 2048MB, GPU: 1`.
    * Replies `REGISTER_OK id=1001` and sets dirty, since a new worker might
      be able to start queued jobs.
  * Anything else: error, then close.
* **`sweep_peers()`** compacts `g_peers`, calling `peer_gone()` on each peer
  that is `closing` and either has no unsent output or can't take more.
* **`peer_gone()`**:
  * Client: just logs the disconnect.
  * Worker:
    * Sets `w->peer = NULL`.
    * If the worker was DRAINING and has no running jobs, this is a clean
      exit: it goes to DEAD with "drained, left the cluster", and nothing is
      rescued.
    * Otherwise it logs "connection lost; waiting on heartbeats". The
      heartbeat timer then takes it through SUSPECTED → DEAD and rescues its
      jobs. A dropped TCP connection isn't instantly fatal; it follows the
      same path as a silent worker, which matches the spec's example output.

### 4.5 Sending helpers
* `peer_send(p, m)` queues a message on a peer, marking it `closing` if the
  socket is dead. It ignores NULL or closing peers, so callers never need to
  check.
* `send_to_user(user, m)` sends to **every** client session with that name.
* `notify_user(user, fmt, ...)` builds an `EVENT text=...` message and sends
  it to that user. This is how `[Event]` lines are produced. Only the job's
  owner gets its events.
* `reply(p, ok, fmt, ...)` and `reply_buf(p, buf)` send a `REPLY` with
  `ok` and `text` fields to the peer that asked.

### 4.6 Client request handlers

| Request | Handler | What it does |
|---|---|---|
| `SUBMIT` | `handle_submit()` | see below |
| `STATUS job` | `handle_status()` | `job_render_status()` into a buffer, then `reply_buf()` |
| `NODES` | `handle_nodes()` | `render_nodes()`, then reply |
| `LOGS job follow` | `handle_logs()` | send the saved output; maybe start following |
| `UNFOLLOW` | inline | `p->follow_job = 0` |
| `CANCEL job` | `handle_cancel()` | see below |
| `MIGRATE job` | `handle_migrate()` | Section 14 |
| `CRONS` | inline | `render_crons()`, then reply |
| `CANCEL_CRON cron` | inline | `cron_cancel()` |

**`handle_submit()`**, step by step:
1. `submit_invalid()` checks the required fields: cores, RAM and time ≥ 1,
   priority 1–3, GPUs ≥ 0, the path not too long, and `-group` a positive
   integer.
2. **`-every`?** Then this is a cron template, not a job. It rejects
   `-every` together with `-depends-on`, calls `cron_register()`, replies
   with `cron=<template id>`, and stops there.
3. **`-depends-on`?** `parse_deps()` splits on commas.
   * `gN` is expanded *right now* into the IDs of all jobs currently in
     group N (via `group_exists()` and a scan).
   * A number must be an existing job.
   * Anything else is an error.
   * `add_dep()` skips duplicates.
4. **Queue full?** If `queue_occupancy()` (QUEUED + WAITING) ≥ 1024, it sends
   `reply_queue_full()` with `code=QUEUE_FULL`. Submit never blocks, and the
   job is never silently dropped: the user is told.
5. `job_create()` adds the job as QUEUED and the submit is logged.
6. If it has dependencies:
   * it switches to WAITING;
   * the reply lists `waiting_on`;
   * `job_check_deps()` runs once immediately, since dependencies might
     already be complete (it goes straight to QUEUED) or already failed (it
     is CANCELLED at once, and the reply includes a `note`).
7. The reply carries the `job` id and the final `state`.

**`handle_logs()`**:
1. Sends a `LOG_DATA` with the stored output (up to 64 KB). If more than that
   was ever produced, it adds `truncated=<bytes dropped>`.
2. If `follow` is set and the job isn't finished:
   * it sets `p->follow_job`;
   * from now on `job_append_output()` streams each new chunk to this peer;
   * `job_end_followers()` sends `LOG_END` when the job ends.
3. Otherwise it sends `LOG_END` straight away. Plain `logs` is "print what
   you have and return".

**`handle_cancel()`**:
* Only the owner can cancel. Finished jobs give an error ("already
  COMPLETED").
* A **RUNNING** job:
  * gets `KILL_JOB reason=cancel run=<run>` sent to its worker;
  * becomes CANCELLED immediately, with "kill sent, requested by X", as in
    the spec's log line;
  * has its resources released through `job_set_state()`.

  The worker's later `JOB_DONE` is ignored because the state is no longer
  RUNNING.
* A **QUEUED** or **WAITING** job becomes CANCELLED directly.
* The reply goes out and the owner's event is sent *before*
  `job_set_state()`. The state change may cascade to dependents, and their
  "CANCELLED" events should come after this one.

### 4.7 Worker message handlers (`handle_worker_msg()`)

First it checks that the message comes from the worker's **current**
connection (`w->peer == p`). A worker declared DEAD is cut off and can't
affect anything.

| Message | Effect |
|---|---|
| `HEARTBEAT load jobs tele` | `handle_heartbeat()`: refresh `last_heartbeat_ms`, reset `missed`, store CPU load. If SUSPECTED, back to ALIVE. Parse `tele` (`id:rss_kb:peak_kb:faults:cpu_ms;...`) into each RUNNING job on this worker. Sets dirty, since the load changed. |
| `JOB_OUTPUT job run` + blob | `job_append_output()`, but only if `run` matches the job's current run (8.1) |
| `JOB_DONE` | `handle_job_done()`, below |
| `CHECKPOINT job run` + blob | keep it as the job's latest checkpoint (`ckpt_num++`, `ckpt_ms = now`) if it's from this worker and the current run. QUEUED is accepted too: a preempted job may send its final checkpoint after being requeued. |
| `CACHE_STATE cached` | replace this worker's set of cached hashes (after evictions) |
| `CACHE_MISS job run hash` | forget that hash for this worker and `send_run_job(j, w, 1)`, i.e. resend with the full file, same run, no attempt used |
| `DRAIN` | ALIVE/SUSPECTED → DRAINING; tell affected users |

**`handle_job_done()`**:
1. Always records the final peak RAM, page faults and CPU time, even for a
   job already moved on (e.g. cancelled), so `status` can still show them.
2. If the job isn't RUNNING on this worker with this run number, logs
   "final report noted" and stops. That's the normal case after cancel or
   preempt.
3. `result=KILLED` while `migrating`: migration completed. Clear the reason,
   set `free_restart` (no attempt used) and `migrated_from = this worker`,
   tell the owner which checkpoint it will resume from, and move it to QUEUED.
4. `result=COMPLETED`: send the `COMPLETED successfully` event **first**,
   then move it to COMPLETED (so the owner sees "Job 1 COMPLETED" before
   "Job 2 moved from WAITING to QUEUED").
5. Otherwise, FAILED with the worker's diagnosis, plus
   `[ERROR] Job N Failed. Diagnosis: ...` in the log.

### 4.8 Heartbeats and failure detection (4C)

`check_heartbeats()` runs every loop iteration (at least every 200 ms). For
each worker that isn't DEAD:

```
elapsed = now - last_heartbeat_ms
missed  = elapsed > 500 ? (elapsed - 500) / 2000 : 0
```
The 500 ms of slack stops a heartbeat that's a little late from counting as
a miss. So:

* at about 2.5 s silent, `missed` = 1: ALIVE → SUSPECTED;
* at about 4.5 s it's 2 (still SUSPECTED);
* at about 6.5 s it's 3: DEAD.

`missed` is only acted on when it changes, so each transition is logged once.

* **→ SUSPECTED:** `worker_set_state()` logs it.
  `warn_users_of_worker()` sends `WARNING: Worker 1001 SUSPECTED (missed
  heartbeat).` to each user with a RUNNING job there, once per user. A
  SUSPECTED worker gets no new jobs (`worker_qualifies()` requires ALIVE).
* **Back to ALIVE:** `handle_heartbeat()` does this when a heartbeat arrives.
* **→ DEAD:** `worker_declare_dead()`:
  1. warns the users;
  2. sets DEAD;
  3. **cuts the connection** if it's still open (e.g. a frozen laptop). If it
     comes back it must register again with a new ID, as the spec requires;
  4. calls `job_rescue()` on every job RUNNING there.
* A DRAINING worker skips SUSPECTED: `missed >= 3` makes it DEAD (with
  rescue), and its state is otherwise left alone.

### 4.9 Job lifecycle (`jobs.c`)

**`job_set_state(j, to, why)`** is the single place a job's state changes
(except the two-step rescue, below). It:
1. Leaving RUNNING: `release()` gives the cores, RAM and GPUs back to the
   worker and decrements `running_jobs`.
2. Entering RUNNING: `reserve()` takes them, and `start_ms` is set.
3. Entering QUEUED: resets `queued_ms`, so aging starts over (and resets
   `eff_prio_logged`).
4. Entering a final state: sets `end_ms`.
5. Logs `Job 2 RUNNING -> FAILED (reason)`.
6. If the new state is final: `job_end_followers()` (ends every `logs -f`)
   and `release_dependents()` (re-checks waiting jobs).
7. Sets `g_sched_dirty`.

Because reserve and release happen *only* here, the worker's free counts
can't drift out of step with the jobs actually running there.

**`job_rescue(j)`** runs for each RUNNING job on a worker that went DEAD:
* It releases the resources directly.
* If `attempts >= MAX_JOB_ATTEMPTS`, the job goes **RUNNING → LOST →
  FAILED_PERMANENT**. The owner is told, followers are ended, and dependents
  are released (they'll be cancelled).
* Otherwise it goes **RUNNING → LOST → QUEUED**, logged as one line:
  `Job 2 RUNNING -> LOST -> QUEUED (attempt 1 of 3 failed)`. The event says
  which checkpoint it will resume from, if any.
* LOST is real in the state machine, but transient: it passes through within
  the same function call.

**`job_create(m, user)`** copies every field from a SUBMIT message: path,
args, resources, group, preemptible. It takes the next ID (IDs follow
submission order, which FIFO relies on), sets state QUEUED, appends to
`g_jobs`, and sets dirty. Both submit and cron use it.

**Dependencies (Section 8)**
* `job_check_deps(j)` applies only to WAITING jobs:
  * if any dependency is FAILED, FAILED_PERMANENT or CANCELLED, the job
    becomes CANCELLED ("dependency Job 5 FAILED"), with the event sent first.
    That `job_set_state()` reaches a final state and calls
    `release_dependents()`, so **the cancellation cascades** down the chain;
  * if all dependencies are COMPLETED, it goes to QUEUED, with the event
    "moved from WAITING to QUEUED";
  * otherwise it keeps waiting.
* `release_dependents(done)` re-checks every WAITING job whose dependency
  list contains `done`.
* WAITING jobs never appear in the scheduling pass, which only collects
  QUEUED jobs.

**Output buffer (Section 6)**: `job_append_output()`:
1. adds to `out_total`;
2. if the chunk itself is over 64 KB, keeps only its last 64 KB;
3. if the buffer would overflow, `memmove`s the oldest bytes out;
4. appends;
5. streams the chunk as `LOG_DATA` to every client whose `follow_job` is
   this job.

The result: the scheduler always holds the most recent 64 KB, so `logs` on a
finished job still shows something.

**Rendering**
* `bprintf()` is a `printf` that appends to a `buf_t`.
* `job_render_status()` writes the `status` text:
  * header `Job N [STATE]`;
  * user, command and arguments, requested resources, attempts;
  * group, preemptible, cron origin, preemption hold and dependencies (with
    each one's state), where relevant;
  * then, by state:
    * QUEUED: how long it has been waiting;
    * RUNNING: running time, **current RAM / limit, page faults, CPU time**
      (5D);
    * finished: **peak RAM**;
  * checkpoint (number, size, age, or "none"), migration in progress, the
    reason, total output;
  * last, `[Job N] Assigned to: Worker W`, `Ran on:`, or `Assigned to: None`.
* `render_nodes()` writes the `nodes` table: ID, state, free/total cores,
  RAM and GPUs, CPU%, job count, and the list of running job IDs.

### 4.10 The scheduling pass (`pass.c`)

`schedule_pass()` implements the spec's **Order of Scheduling Decisions**
exactly.

**Before looking at any job**
1. Collect **QUEUED jobs only**; WAITING ones are skipped.
2. **Aging (PRIORITY only).** `effective_prio()` is
   `prio - (time queued / 10 s)`, never below 1. When a job's effective
   priority changes, it's logged once (`Job 1 aged: effective priority
   3 -> 2`).
3. **Ordering.** `qsort` with `cmp_jobs()`: PRIORITY sorts by effective
   priority, then by ID; FIFO sorts by ID only. IDs are handed out in
   submission order, so "lower ID" means "older".

**Then for each job, in order**

(It first skips jobs that stopped being QUEUED earlier in this same pass,
e.g. preemption victims.)

1. **Preempted-job hold:** `job_is_held()`. If this job was preempted for job
   X and X is still QUEUED, skip it. If X has started (or was cancelled, or
   failed), the hold is cleared.
2. **User quota:** `quota_says_skip()`. If this job's user is over quota and
   another user *who isn't* has a QUEUED, unheld job, skip this job, even if
   it has higher priority.
3. **Match and choose:** `choose_worker()`.
4. **No match:** `try_preempt()`, which only helps priority-1 jobs.
5. Got a worker → `dispatch()`. Still nothing → under **FIFO, stop the whole
   pass** (the oldest job blocks the rest); under **PRIORITY, move on** to
   the next job.

A skip (from the hold or the quota) is *not* "no match": even under FIFO the
pass continues past it. That's how the spec's quota example works under
FIFO.

#### Matching and choosing: `choose_worker()`
* `worker_qualifies()`: the worker is ALIVE (not SUSPECTED, DEAD or
  DRAINING), still connected, and has enough **free** cores, RAM and GPUs.
* `job_binary_hash()` is called first, so the "already cached" tiebreaker
  knows the binary's hash.
* **Migration exclusion (14):** if the job was just migrated off worker W and
  *some other* worker qualifies, W is excluded. If nobody else qualifies, W
  can be used again.
* The loop keeps the best and second-best workers, using `cmp_workers()`,
  which applies the full tiebreaker order:
  1. **idle** (0 running jobs) beats busy;
  2. **binary cached** beats not cached (compared within the idle group and
     within the busy group, since idle is decided first);
  3. **lower CPU load**;
  4. **fewer running jobs**;
  5. lower ID, just to make the result deterministic.
* For logging, it compares the winner with the runner-up and writes the
  *first criterion that differed*, giving lines like
  `Job 5 -> Worker 1002 (chosen over 1001: CPU 8% vs 61%)` or
  `(chosen over 1001: binary cached vs not cached, both idle)`. With one
  candidate it logs `(only qualifying worker)`.

#### Preemption: `try_preempt()` and `pick_victims()` (Section 10)
* Only for jobs **submitted** with `-prio 1`; aging alone doesn't let a job
  preempt.
* For each ALIVE, connected worker, `pick_victims()`:
  * collects the RUNNING `-preemptible` jobs on it;
  * sorts them with `cmp_victims()`: **lowest priority first** (highest
    number), then **most recently submitted first** (highest ID);
  * walks that list, adding up what each kill would free on top of the free
    resources, and stops **as soon as it's enough**. It returns how many
    victims that took, or -1 if killing all of them wouldn't be enough.
* It picks the worker needing the **fewest victims**.
* For each victim:
  * sends `KILL_JOB reason=preempt run=<run>` (the worker sends SIGKILL);
  * sets `held_for` to the preempting job and `free_restart = 1` (it doesn't
    count as an attempt);
  * tells the victim's owner;
  * moves it RUNNING → QUEUED, which releases its resources.
* It returns that worker, and the priority-1 job is dispatched to it in the
  same pass, into the room just freed.

#### Quotas (Section 11)
* `cluster_cores()` adds up the cores of all workers that aren't DEAD.
* `user_running_cores()` adds up the cores of the user's RUNNING jobs. It's
  computed live, so jobs dispatched earlier in the same pass count.
* `user_over_quota()` is `2 × user_cores > total`. Exactly 50% is **not**
  over.
* `quota_says_skip()` only logs when the "in favour of" job changes
  (`quota_skip_logged`). That's "once per pair", so a stuck job doesn't spam
  the log every second.

#### Dispatching: `dispatch()` and `send_run_job()`
`dispatch(j, w)`:
1. If the job already has output (it ran before), appends a marker
   `--- restarted on Worker W (start #N) ---` so the logs of different runs
   don't blur together.
2. Sets `worker_id` and clears `migrated_from`.
3. **Attempts:** if `free_restart` is set (preempted or migrated), it's
   cleared and no attempt is used. Otherwise `attempts++`.
4. `run++`.
5. Resets telemetry, then `job_set_state(RUNNING)`, which reserves resources.
6. `send_run_job(j, w, 0)`.
7. Event: `Job 1 RUNNING on Worker 1001.`, or with `(attempt 2)` after a
   retry.

`send_run_job(j, w, force_full)` follows the spec's "after a worker is chosen"
order:
1. **Checkpoint first:** if the job has one, send `RESTORE_CKPT` with it as
   the blob. TCP keeps order, so the worker always has the checkpoint before
   the `RUN_JOB` arrives.
2. **Then `RUN_JOB`** with job, run, path, arguments and limits, plus the
   binary decision:
   * **No `/` in the path** (e.g. `ls`): a PATH command. Nothing is sent, and
     the worker runs it by name.
   * **The worker has it cached** (and this isn't `force_full`): send
     `binary=1 cached=1 hash=<sha256>`, and log
     `already cached (sha256 8628...) on Worker 1001, not sending`.
   * **Otherwise:** `read_binary()` the file, hash the bytes actually read
     (so the hash always matches what's sent), attach them as the blob, and
     record that the worker now has it (`worker_cache_add()`). Log
     `sent ./hello (33560 bytes, sha256 8628...)`.
   * **The scheduler can't read the file:** log it and let the worker try
     running it by path.

#### Hash memo: `job_binary_hash()`
Hashing 30 MB on every scheduling decision would be slow, and the event loop
must stay responsive. So `hash_memo_t` remembers, per path: device, inode,
size, modification time (with nanoseconds), and the hash.

* If `stat()` shows all of these unchanged, the stored hash is reused.
* If anything changed (e.g. the file was recompiled, or one byte was
  appended), the file is re-read and re-hashed.

`MTIME_NSEC` hides the Linux/macOS difference in the `stat` field name.

### 4.11 Cron (`cron.c`, Section 12)

`cron_t` holds: id, `every_s`, `runs`, `cancelled`, the owner, a
display-friendly `cmd`, **`tmpl`** (a `msg_clone()` of the original SUBMIT)
and `next_ms`.

* `cron_register()` creates the template with `next_ms = now`, so the first
  copy goes in on the next tick. Template IDs are numbered separately from
  job IDs, starting at 1.
* `cron_tick()` runs every loop iteration. For each active template that's
  due:
  1. Moves `next_ms` forward by the interval. If it fell behind, it schedules
     from now rather than injecting a burst to catch up.
  2. If the queue is full, it skips this run and tells the owner.
  3. Otherwise `job_create(tmpl, user)`. The copy behaves exactly like a
     normal submit: same command, resources, priority, GPUs, group and
     preemptible flag. It sets `cron_id`, increments `runs`, and logs and
     notifies `Cron 1 injected Job 7 (run #3).`

  Copies are injected whether or not the previous one is still running.
* `render_crons()` prints the template table: ID, interval, runs, user,
  status, command.
* `cron_cancel()` errors for an unknown ID, an already-cancelled template, or
  another user's template. Otherwise it marks it cancelled. Copies already
  queued or running are left alone.

---

## 5. The worker (`src/worker/`)

### 5.1 Data structures (`worker.h`)

`kill_reason_t` records **why the worker itself killed a job**. Section 5E
says to report that reason rather than a signal number:
`KILL_NONE, KILL_TIMEOUT, KILL_CANCEL, KILL_MEMORY, KILL_PREEMPT,
KILL_MIGRATE, KILL_SHUTDOWN`.

`wjob_t`, one running job on this worker:
| Field | Meaning |
|---|---|
| `id`, `run` | job id and the scheduler's run number |
| `next_run` | a newer `RUN_JOB` for the same job, held until this run is gone (8.1) |
| `pid`, `start_ms`, `time_s`, `ram_mb` | process and limits |
| `dir` | `/tmp/osn_job_<id>` |
| `hash` | SHA-256 of its cached binary, so it's never evicted while in use |
| `out_off` | how much of `output.log` has already been sent |
| `err_fd` | read end of the setup-error pipe (5.4) |
| `killed`, `killed_by` | why the worker killed it, and who asked (cancel) |
| `ckpt_mtime`, `ckpt_mtime_ns`, `ckpt_size`, `ckpt`, `ckpt_len` | the checkpoint file as last seen, and its content |
| `term_deadline` | migration: the time to send SIGKILL if SIGTERM didn't work |
| `rss_kb`, `peak_kb`, `faults`, `cpu_ms` | telemetry |
| `exited`, `status` | set once reaped |

Globals `g_is_root`, `g_nobody_uid`, `g_nobody_gid` say whether to drop
privileges, and to whom.

### 5.2 Startup (`worker.c` `main()`)

1. `log_set_tag("WORKER")` (no ID yet); ignore SIGPIPE.
2. Install a SIGINT handler **without `SA_RESTART`**. It only sets
   `g_sigint`, and because the flag is missing, Ctrl+C interrupts `poll()`
   and gets handled at once.
3. `parse_args()`:
   * an optional leading `<ip> <port>` is the fallback;
   * then `-cores` and `-ram` (required), `-gpu` and `-discovery-port`
     (optional).
4. `clamp_capacity()`: if `-cores` exceeds `sysconf(_SC_NPROCESSORS_ONLN)`,
   or `-ram` exceeds `_SC_PHYS_PAGES × _SC_PAGESIZE`, it clamps and warns.
   GPUs aren't clamped; the spec calls them just a counter.
5. `setup_privileges()`:
   * if `geteuid() != 0`, it warns that jobs will run without privilege
     dropping and carries on;
   * as root, it looks up `nobody` with `getpwnam()` and remembers its UID
     and GID.
6. `cache_init()` (5.8).
7. `connect_and_register()`:
   * runs `discovery_resolve()`, then `tcp_connect()`;
   * sends `REGISTER` with capacity, hostname, and `cached=` (after
     `cache_scan()`);
   * waits up to 5 s for `REGISTER_OK`;
   * then `log_set_tag("WORKER 1001")` and the two registration lines.

   If this first attempt fails, the worker prints an error and exits.
8. `log_set_prompt("worker> ")`. The prompt only shows in a real terminal.
9. Sends the first heartbeat immediately.

### 5.3 The main loop

Each iteration:
1. **Ctrl+C pending** → `start_drain()`.
2. **Draining and no jobs left** → log
   `Drain complete (0 active jobs). Exiting gracefully.`, flush, exit 0.
3. **Lost the scheduler** (`g_conn.fd < 0`):
   * log it and `kill_all_jobs()`; the spec says a disconnected worker kills
     its jobs and re-registers as a new worker;
   * if it was draining, just exit;
   * otherwise loop on `connect_and_register()` every 2 s, which runs
     discovery again first, as the spec requires. It gets a new ID.
4. **`poll()` stdin and the scheduler socket, 100 ms timeout (`TICK_MS`).**
   * Socket readable: decode every message.
     * `RUN_JOB` → `handle_run_job()`, which returns 1 if it kept the message
       for later.
     * `KILL_JOB` → `handle_kill_job()`.
     * `RESTORE_CKPT` → `handle_restore()`.
     * EOF or a corrupt frame closes the connection, and step 3 handles it on
       the next iteration.
   * stdin readable → `handle_stdin()`.
5. **`tick_jobs(sample)`**. `sample` is true every 500 ms
   (`OUTPUT_POLL_MS`).
6. **Heartbeat** every 2 s.

`send_heartbeat()`:
* CPU load is `getloadavg()`'s one-minute value ÷ core count × 100, capped at
  100, as the spec suggests. Being a one-minute average, it reacts slowly.
* `jobs` is the number of running jobs.
* `tele` is `id:rss_kb:peak_kb:faults:cpu_ms` for each job, joined by `;`.

`send_msg()` sends and **waits until written** (`conn_flush_all()`). If that
fails, the connection is closed and step 3 deals with it.

### 5.4 Starting a job

**`handle_run_job(m)`**
1. **Same job already here with a different run?** The old run is still
   being killed (preemption or migration brought the job straight back).
   Keep this message in `old->next_run` and start it from `finish_job()` once
   the old run is fully gone, which avoids two runs fighting over
   `/tmp/osn_job_<id>`. Returns 1 (message kept).
2. Log `Job 2 ASSIGNED, forking...`.
3. **`prepare_binary()`** (Section 13):
   * no `hash` field → a PATH command; nothing to do;
   * **blob attached** → compute its SHA-256; if it doesn't match `hash`,
     fail with "Binary arrived corrupted".
     * `cache_store()` saves it and sets `local_binary=<cache path>` in the
       message, then frees the blob (no need to keep 30 MB in memory).
     * If storing evicted anything, `send_cache_state()` tells the scheduler.
     * If storing failed, the job still runs from the blob.
   * **`cached=1`** → `cache_lookup()`. If the file is gone, send
     `CACHE_MISS` and **don't start**; the scheduler resends the full file.
     Otherwise set `local_binary`.
4. `take_restore(id)` collects the checkpoint that arrived in `RESTORE_CKPT`
   for this job, if any.
5. `jobrun_start(j, m, ckpt, len)`. If `fork()` fails, the job is reported
   failed.
6. Add it to `g_jobs` and log `Job 2 RUNNING (pid ..., dir ...)`.

**`jobrun_start()`** (`jobrun.c`), the parent side:
1. Fills `id`, `run`, `hash` (only if it came from the cache), the limits,
   and `dir`.
2. `jobrun_cleanup()` removes any leftover directory from an earlier attempt.
3. **Creates the error pipe** with `pipe()` and marks both ends
   `FD_CLOEXEC` (close on exec):
   * if `execvp()` succeeds, the child's write end closes automatically and
     the pipe stays empty;
   * if any setup step fails, the child writes `"step:error"` into it before
     exiting.

   The parent reads it after the job ends. That's how it can say "Job setup
   failed at setuid (Operation not permitted)" instead of showing a
   meaningless exit code.
4. `fork()`. The child calls `run_child()`; the parent continues.
5. Parent: closes the write end and calls `setpgid(pid, pid)`. The child does
   the same, so whichever runs first, the group exists before anyone signals
   it. It makes the read end non-blocking, and remembers the restored
   checkpoint's content so it isn't reported back as new.

### 5.5 Inside the child: isolation and limits (`run_child()`, Section 5A)

These run in the child, **before `execvp()`**:

**Preparation**
* `setpgid(0, 0)`: **its own process group.** Ctrl+C in the worker's
  terminal sends SIGINT to the terminal's foreground group. If the job were
  in it, Ctrl+C (which should only start a drain) would kill every job. A
  group also lets the worker kill a job's children with `kill(-pgid, ...)`.
* Reset SIGINT, SIGTERM, SIGPIPE and SIGCHLD to their defaults, and clear the
  signal mask. Ignored signals survive `exec`, and the worker ignores
  SIGPIPE, so without this the job would inherit that.

**Step 1: its own working directory**
1. `mkdir /tmp/osn_job_<id>`.
2. Put the program in it as `./<name>`: `copy_file()` from the cache, or
   `write_file()` from the blob.
3. `mkdir checkpoint/`. If resuming, write the restored checkpoint to
   `checkpoint/state`.
4. Create `output.log`.
5. As root, `chown` the directory, `checkpoint/`, `output.log`, `state` and
   the binary to `nobody`, so the job can use its own files.
6. `chdir` into it.
7. **Output capture (5B):** stdin is redirected from `/dev/null`;
   stdout and stderr go to `output.log` via `dup2()`. Nothing the job prints
   reaches the worker's terminal.

**Step 2: drop privileges** (root only)
* `setgroups(0, NULL)` drops supplementary groups (root's extra groups would
  otherwise survive).
* `setgid(nobody)` **then** `setuid(nobody)`. Group first, because after
  `setuid` the process no longer has permission to change its group.
* **Check:** UID and effective UID are now `nobody`, and `setuid(0)` must
  *fail*. If anything is off, `child_fail()` reports it and exits with 126,
  so the job never runs with the wrong identity.
* **Linux:** `prctl(PR_SET_PDEATHSIG, SIGKILL)`: if the worker dies, even
  from `kill -9`, the kernel kills the job too, so no orphan jobs keep
  running. It's set *after* `setuid` because changing credentials clears it.
  If the worker already died (`getppid() != parent`), the child exits.

**Step 3: CPU time.** `RLIMIT_CPU` with soft = `time`, hard = `time + 1`. At
the soft limit the kernel sends **SIGXCPU**; a second later, SIGKILL.

**Step 4: memory.** `RLIMIT_AS = ram × 1 MB` caps the virtual address space,
so `malloc` fails past the limit. macOS ignores it; `EINVAL` is tolerated
there, and the parent watches resident memory instead (5.6).

**Step 5: fork bombs.** `RLIMIT_NPROC = 64`. It counts **every process of the
user**, so all jobs (all `nobody`) on a worker share the budget.

**Extra:** `RLIMIT_CORE = 0`, so a crash doesn't write a core dump into the
job directory and delay the report.

**Environment:** `OSN_CHECKPOINT_DIR=/tmp/osn_job_<id>/checkpoint` (14) and
`OSN_JOB_ID`.

**`execvp()`**
* `argv[0]` is the program name, followed by the submitted arguments.
* A shipped program runs as `./<name>`; a PATH command runs by name.
* If `exec` fails, the error is printed into `output.log`, written to the
  pipe as `exec:<error>`, and the child exits with **127**, the conventional
  "command not found" code.

`child_fail()` writes `step:strerror(errno)` to the pipe and `_exit`s.
`_exit`, not `exit`, because the child must not run the parent's `atexit`
handlers or flush its copied stdio buffers.

### 5.6 Watching running jobs: `tick_jobs()`

Every 100 ms, for each job:
1. **`jobrun_reap()`:** `waitpid(pid, WNOHANG)`. If it exited, it saves the
   status, kills anything left in its process group, and calls
   `finish_job()`. Reaping is what prevents zombie processes.
2. **Wall-clock deadline:** if `now - start_ms ≥ time` and nothing else
   killed it, it calls `jobrun_kill(KILL_TIMEOUT)`, which sends SIGKILL to the
   group. This is needed because `RLIMIT_CPU` only counts CPU time, so a job
   that sleeps would never hit it.
3. **Migration deadline:** if `term_deadline` has passed (SIGTERM was ignored
   for 2 s), it sends SIGKILL.
4. **Every 500 ms:**
   * `jobrun_sample()` updates telemetry;
   * `pump_output(j, 0)` sends new output;
   * `pump_checkpoint()` sends a new checkpoint.

**`jobrun_sample()`** (5D)
* `getrusage(RUSAGE_CHILDREN)` only covers children that have already
  exited, so a running process's numbers are read from the OS directly.
* **Linux:**
  * `/proc/<pid>/stat`: parsed after the last `)`, because the command name
    may contain spaces or parentheses. Gives minor + major page faults, and
    `utime + stime` (in clock ticks, converted to ms with
    `sysconf(_SC_CLK_TCK)`).
  * `/proc/<pid>/statm`: resident pages × page size gives RSS.
  * `/proc/<pid>/status` `VmHWM`: the kernel's own **peak RSS**, more exact
    than our 500 ms samples.
* **macOS:** `proc_pidinfo(PROC_PIDTASKINFO)` gives resident size, faults and
  CPU time (converted with `mach_timebase_info`). **If RSS exceeds `-ram`, it
  calls `jobrun_kill(KILL_MEMORY)`**, since macOS ignores `RLIMIT_AS`.

**`jobrun_read_output()` / `pump_output()`**
* `pread()` from `output.log` at `out_off`, up to 256 KB at a time; the
  offset only moves forward.
* Each chunk becomes `JOB_OUTPUT job run` with the bytes as the blob.
  `pump_output(j, 1)` loops until everything is sent; it's used when a job
  ends.
* Because the file is read every 500 ms, output streams live (Section 6)
  rather than arriving all at the end.

**`jobrun_new_checkpoint()` / `pump_checkpoint()`** (14)
1. `stat()` `checkpoint/state`. If the mtime (with ns) and size are unchanged
   since last time, there's nothing to do; that's the spec's "comparing
   `stat()` timestamps".
2. Changed → read it (refusing files over 1 MB; checkpoints should be small).
3. If the content equals what we already sent or restored, skip it.
4. Otherwise remember it and send `CHECKPOINT job run` with it as the blob.

The spec has the job write `state.tmp` and then `rename()` it, so the worker
never reads a half-written checkpoint.

### 5.7 Ending, killing and diagnosing

**`finish_job(j)`**:
1. Sends the remaining output, `pump_output(j, 1)`.
2. Does a final `pump_checkpoint()`. This catches the checkpoint the job
   wrote in response to SIGTERM during a migration.
3. `jobrun_diagnose()` turns the outcome into words.
4. Logs a line: COMPLETED, killed and reaped, or `[ERROR] Job N Failed.
   Diagnosis: ...`.
5. Sends `JOB_DONE` with:
   * `result=COMPLETED`, `FAILED`, or `KILLED` (the scheduler asked:
     `killed=cancel|preempt|migrate`);
   * `reason`, the exit code or signal, peak RAM, faults and CPU time.
6. `jobrun_cleanup()` deletes `/tmp/osn_job_<id>` with `nftw()` (a
   depth-first walk that removes files before their directory). The spec
   asks for cleanup once all output has been sent, and it has.
7. If a newer run of the same job was held (`next_run`), it's started now.

**`jobrun_diagnose()`** (5E) answers "why did it end?":
1. Reads the error pipe; an empty pipe means `exec` succeeded.
2. **The worker killed it itself** → it reports the reason, not the signal:

   | `killed` | Reason text |
   |---|---|
   | `KILL_TIMEOUT` | `Wall-clock timeout exceeded (Ns limit)` |
   | `KILL_CANCEL` | `Cancelled by <user>` |
   | `KILL_MEMORY` | `Memory limit exceeded (NMB)` (macOS) |
   | `KILL_PREEMPT` | `Preempted by a priority 1 job` |
   | `KILL_MIGRATE` | `Stopped for migration` |
   | `KILL_SHUTDOWN` | `Worker lost its connection to the scheduler` |
3. **`WIFEXITED`**:
   * code 0 → completed;
   * the pipe has a setup error (not `exec:`) →
     `Job setup failed at <step> (<error>); the job was not run`;
   * 127 → `Exited with code 127 (command not found)`, or
     `(could not start program: <error>)` when exec failed for another
     reason, e.g. not enough memory under `RLIMIT_AS`;
   * 126 → `(command not executable)`;
   * anything else → `Exited with code N`.
4. **`WIFSIGNALED`**:
   * **SIGXCPU** is the CPU limit firing. Since `-time` sets both limits, it's
     reported as `Wall-clock timeout exceeded`, as the spec says;
   * others: `Terminated by signal 11 (SIGSEGV - Segmentation Fault)`, from
     the `signal_words()` table (SEGV, FPE, ABRT, BUS, ILL, KILL, TERM, INT,
     PIPE, XCPU, XFSZ, TRAP, SYS), falling back to `strsignal()`.

**Kill paths**
* `jobrun_kill(j, why, by)` records the first reason only (so a timeout
  isn't renamed by a later kill), then sends `kill(-pid, SIGKILL)` to the
  whole process group, falling back to the pid alone.
* `jobrun_terminate()` sends SIGTERM to the group and sets
  `term_deadline = now + 2 s`.
* `handle_kill_job()` ignores a `KILL_JOB` whose run number doesn't match.
  `reason=migrate` → `jobrun_terminate()`; `cancel` or `preempt` →
  `jobrun_kill()` (SIGKILL; a preempted job gets no chance to checkpoint, as
  the spec says).
* `kill_all_jobs()` runs when the scheduler connection is lost: it SIGKILLs
  each job, waits for it (blocking `waitpid`), cleans up, and empties the
  list.

### 5.8 The executable cache (`cache.c`, Section 13)

**Layout:** files named `<name>-<64-hex sha256>` in `$OSN_CACHE_DIR`
(default `cache/` in the worker's working directory, turned into an absolute
path with `realpath()`). Limit: `$OSN_CACHE_SIZE_MB`, default 512 MB.

The in-memory list `centry_t` holds `{name, hash, size, used_ns}`.
`used_ns` is the LRU clock: the last time a job started from the file.

* **`cache_init()`** reads the environment variables, creates the folder and
  makes the path absolute.
* **`cache_scan()`** runs at every (re)registration. It reads the folder,
  removes leftover `*.tmp` files (a store interrupted by a crash), and parses
  each `name-hash` file. The **mtime becomes `used_ns`**, so LRU order
  survives a worker restart. It logs `Cache: N program(s), X bytes`.
* **`cache_hash_list()`** returns all hashes comma-separated, for `REGISTER`
  and `CACHE_STATE`.
* **`cache_store()`**:
  1. writes to `<file>.tmp`, then `rename()`s it over the final name. Rename
     is atomic, so nobody ever sees a half-written program;
  2. `touch()` marks it used: `used_ns = now`, and `utimensat()` sets the
     file's mtime to now to persist that;
  3. `evict(hash)`.
* **`evict(keep)`**: while the total size is over the limit, deletes the
  entry with the smallest `used_ns`, skipping `keep` (the program just
  stored) and anything a running job uses (`cache_in_use()`, which lives in
  `worker.c` and checks every `wjob_t.hash`). Each eviction is logged. If
  everything left is in use, it logs that and stops.
* **`cache_lookup()`**: if the file vanished, the entry is dropped and it
  returns -1, which triggers `CACHE_MISS`. Otherwise it touches the file
  (marks it used) and returns its path.

The spec's example, with a 2 MB limit and three 0.7 MB programs A, B, A, C:
* storing C goes over the limit;
* the least recently used entry is **B** (A was used again after B);
* so B is evicted, and submitting B again needs a full transfer.

### 5.9 Draining (Section 9)

* **Starting it:** `drain` typed on stdin (`handle_stdin()`), or Ctrl+C
  (`g_sigint`, which behaves exactly like `drain`).
* `start_drain()` logs `Draining initiated locally.` and sends `DRAIN`. The
  scheduler marks the worker DRAINING; it never qualifies for new jobs.
* The worker keeps heartbeating and running its jobs. When the last one
  finishes, the main loop logs `Drain complete (0 active jobs). Exiting
  gracefully.` and exits.
* The scheduler sees the disconnect of a DRAINING worker with no jobs and
  records a clean exit: nothing is rescued.
* A second Ctrl+C just says "Already draining".
* Other stdin commands: `jobs` lists the running jobs, and an empty line
  redraws the prompt.

---

## 6. The client (`src/client/`)

### 6.1 State

`g_wait` says what the client is waiting for:
* `IDLE`: at the prompt;
* `WAIT_REPLY`: sent `nodes`, `status`, `cancel` ... and expects a `REPLY`;
* `WAIT_SUBMIT`: sent `SUBMIT`; the reply is formatted specially;
* `WAIT_LOGS`: printing `LOG_DATA` until `LOG_END`. `g_logs_job` and
  `g_logs_follow` say which job, and whether this is `-f`.

The prompt only comes back when the answer arrives. Events are printed at any
time.

### 6.2 Startup (`main()`)
1. A SIGINT handler (without `SA_RESTART`), and ignore SIGPIPE. `g_tty`
   records whether stdout is a terminal.
2. Parse `[ip port] [-user name] [-discovery-port p]`.
3. `connect_scheduler()`: discovery or fallback, then `tcp_connect()`.
   Prints `[Client] Connected to Scheduler.`
4. The username comes from `-user` if valid. Otherwise `ask_username()`
   prompts `Username:` until a valid name is typed.
   `setvbuf(stdin, NULL, _IONBF, 0)` makes stdin unbuffered first, so
   `fgets()` can't swallow lines meant for the main loop, which uses
   `read()`.
5. `login()`: `LOGIN`, then wait for `LOGIN_OK`. Prints
   `Logged in as lordz.`

### 6.3 Main loop

`poll()` over stdin and the scheduler socket, with no timeout.

* **Socket readable:** decode every message → `handle_msg()`. EOF or a bad
  frame → `reconnect()`. Afterwards `run_pending_lines()`, because an answer
  may have freed the client to run the next typed line.
* **stdin readable:** `handle_stdin()`.
* **SIGINT flag set:** `handle_sigint()`.

**Typing ahead.**
* `handle_stdin()` appends bytes to `g_line` (8 KB) and calls
  `run_pending_lines()`.
* That runs complete lines **only while `IDLE`**. Lines typed while an answer
  is pending wait in the buffer.
* While the buffer is full, stdin is left out of `poll()` (backpressure), so
  a long piped script is never cut short.
* At EOF (Ctrl+D, or the end of a piped script) the client exits once
  idle.

### 6.4 Commands (`run_command()`)

`tokenize()` splits on spaces and tabs; `"double quotes"` group words, which
is how to pass a path containing a space.

| Command | Sends | Then waits for |
|---|---|---|
| `nodes` | `NODES` | REPLY |
| `submit ...` | `SUBMIT` (built by `cmd_submit()`) | REPLY (submit format) |
| `status N` / `cancel N` / `migrate N` | `STATUS` / `CANCEL` / `MIGRATE` with `job=N` | REPLY |
| `logs N [-f]` | `LOGS job=N follow=0/1` | LOG_DATA … LOG_END |
| `crons` | `CRONS` | REPLY |
| `cancel-cron N` | `CANCEL_CRON cron=N` | REPLY |
| `help`, `exit`/`quit` | nothing | (local) |

**`cmd_submit()`**:
* Resolves the binary: a path containing `/` becomes absolute with
  `realpath()`; a missing file is an error at once ("Cannot find").
* Scans the remaining words:
  * `-cores -ram -time -prio -gpu` need a number;
  * `-depends-on -group -every` take a value, passed through as text;
  * `-preemptible` is a bare flag;
  * **any other word becomes a program argument** (`arg0`, `arg1`, ...).
* Checks the required flags and their ranges, listing every missing one
  together (`Missing required flag(s): -ram -prio`), then sends.

### 6.5 Handling messages (`handle_msg()`)
* **`EVENT`** → `print_async("[Event] ", text)`. When idle in a terminal, it
  clears the prompt line with `\r\033[K`, prints the event, and redraws the
  prompt. Whatever was half-typed is still in the terminal's input buffer,
  so pressing Enter still runs it. In logs mode it makes sure the event
  starts on its own line.
* **`LOG_DATA`**: ignored unless it's for the job we're showing (a chunk
  still in flight after Ctrl+C would otherwise leak). Prints a truncation
  note if needed, then writes the raw bytes. `g_logs_newline` tracks whether
  the output ended with a newline, for tidy formatting.
* **`LOG_END`**: in follow mode, prints the footer
  `-- Job 5 finished (COMPLETED) --`, then returns to the prompt.
* **`REPLY`**:
  * submit replies print `[Client] Job submitted.` /
    `Job submitted (WAITING on Job 1).` / cron template registered /
    queue full / `Submit failed: ...`, and `Job ID assigned: N`;
  * other replies print the text, prefixed `[Client] Error:` when
    `ok=0`.

### 6.6 Ctrl+C (`handle_sigint()`)
* While following logs: sends `UNFOLLOW`, prints
  `[Client] Stopped following Job N.`, and returns to the prompt. The spec:
  Ctrl+C "returns to the prompt, not exits the client".
* While waiting for another answer: stops waiting.
* At the prompt: prints `(type 'exit' or press Ctrl+D to quit)`.

### 6.7 Reconnecting (`reconnect()`)

When the connection drops:
1. Prints `[Client] Lost connection to the scheduler. Reconnecting...`
2. Loops: `connect_scheduler()` (which runs discovery again) then `login()`
   with the **same username, without prompting** (the spec asks for that),
   retrying every 2 s.
3. Returns to `IDLE` with `[Client] Reconnected as lordz.`

---

## 7. Protocol reference

All messages use the frame format in 3.2. Fields are text; "blob" means raw
bytes after the header.

**Client ↔ scheduler**
| Direction | Type | Fields | Blob |
|---|---|---|---|
| C → S | `LOGIN` | `user` | |
| S → C | `LOGIN_OK` | `user` | |
| C → S | `SUBMIT` | `path argc arg0.. cores ram time prio gpu` + optional `depends-on group preemptible every` | |
| C → S | `STATUS` / `CANCEL` / `MIGRATE` | `job` | |
| C → S | `NODES` / `CRONS` | | |
| C → S | `CANCEL_CRON` | `cron` | |
| C → S | `LOGS` | `job follow` | |
| C → S | `UNFOLLOW` | `job` | |
| S → C | `REPLY` | `ok text`; submit adds `job state [waiting_on] [note]` or `cron every`; full queue adds `code=QUEUE_FULL` | |
| S → C | `EVENT` | `text` | |
| S → C | `LOG_DATA` | `job [truncated]` | output |
| S → C | `LOG_END` | `job state` | |

**Worker ↔ scheduler**
| Direction | Type | Fields | Blob |
|---|---|---|---|
| W → S | `REGISTER` | `cores ram gpu host cached` | |
| S → W | `REGISTER_OK` | `id` | |
| W → S | `HEARTBEAT` | `load jobs tele` | |
| S → W | `RESTORE_CKPT` | `job run num` | checkpoint |
| S → W | `RUN_JOB` | `job run path argc arg0.. cores ram time gpu [binary hash cached]` | executable (unless cached or PATH command) |
| S → W | `KILL_JOB` | `job run reason by` | |
| W → S | `JOB_OUTPUT` | `job run` | output chunk |
| W → S | `CHECKPOINT` | `job run` | checkpoint |
| W → S | `JOB_DONE` | `job run result killed reason exit_code signal peak_kb faults cpu_ms` | |
| W → S | `CACHE_STATE` | `cached` | |
| W → S | `CACHE_MISS` | `job run hash` | |
| W → S | `DRAIN` | | |

**UDP discovery:** `OSN_DISCOVER <nonce>` → `OSN_SCHEDULER <nonce> <tcp_port>`.

---

## 8. Ideas that cut across files

### 8.1 Run numbers
* Every start of a job increments `job.run` (in `dispatch()`), and the run
  number goes in `RUN_JOB`.
* The worker stores it in `wjob_t.run` and puts it on everything it reports
  about that run: `JOB_OUTPUT`, `JOB_DONE`, `CHECKPOINT`, `CACHE_MISS`.
  `KILL_JOB` carries it too.

**Why:** preemption and migration put a job back in the queue while its old
process is still dying, and the next start can land on the *same* worker.
Without run numbers, the old run's "I was killed" `JOB_DONE` could be taken
as the new run finishing, or its late output mixed into the new run's.

With them:
* the scheduler ignores reports whose run isn't current;
* the worker holds a newer `RUN_JOB` (`next_run`) until the old run is gone.

### 8.2 Attempts vs `free_restart`
* `attempts` counts starts that *can fail because a worker died*.
  `job_rescue()` checks it against `MAX_JOB_ATTEMPTS`.
* Preemption and migration aren't failures, so they set `free_restart`, and
  the next `dispatch()` doesn't increment `attempts`.
* A cache miss doesn't even create a new start: it resends under the same
  run.

### 8.3 Ordering guarantees from TCP
TCP delivers bytes in order on one connection. The code relies on that:
* `RESTORE_CKPT` is sent before `RUN_JOB`, so the checkpoint is waiting when
  the job starts;
* the worker sends all `JOB_OUTPUT` before `JOB_DONE`, so followers see every
  line before the footer;
* `KILL_JOB` can't overtake the `RUN_JOB` it refers to.

### 8.4 Event order
Some state changes trigger other changes: a completion releases dependents,
and a cancel cascades. To keep the user's `[Event]` lines in causal order,
the handler sends **its own event first** and calls `job_set_state()`
afterwards (see `handle_job_done()`, `handle_cancel()`, `job_check_deps()`).

### 8.5 Resource accounting
The free cores, RAM and GPUs on a worker change **only** in `reserve()` and
`release()`, called from `job_set_state()` and `job_rescue()`. Every path
that starts or stops a job goes through them, so the numbers can't drift.

`running_jobs` is the scheduler's own instant count. The heartbeat's job
count arrives up to 2 s late, which is why "idle vs busy" uses the
scheduler's count.

### 8.6 Timing summary
| What | Where | Period / timeout |
|---|---|---|
| scheduler poll | `scheduler.c` main | 200 ms |
| scheduling pass | main loop | on any change, and at least every 1 s |
| worker tick | `worker.c` main | 100 ms |
| output + telemetry + checkpoint scan | `tick_jobs()` | 500 ms |
| heartbeat | `send_heartbeat()` | 2 s |
| SUSPECTED / DEAD | `check_heartbeats()` | ~2.5 s / ~6.5 s of silence |
| migration SIGKILL | `term_deadline` | 2 s after SIGTERM |
| aging | `effective_prio()` | one step per 10 s queued |
| discovery | `discovery_resolve()` | 3 × 2 s |
| connect / register | `tcp_connect()`, `recv_msg()` | 3 s / 5 s |

---

## 9. Scenario walkthroughs

### 9.1 A worker dies mid-job (`kill -9`)
1. The worker process vanishes. On Linux, `PR_SET_PDEATHSIG` makes the kernel
   kill its job too.
2. The scheduler gets EOF on its connection. `peer_gone()` sees the worker
   isn't a draining worker that's finished, so it just sets `w->peer = NULL`
   and logs "waiting on heartbeats".
3. About 2.5 s after the last heartbeat, `check_heartbeats()` moves it to
   SUSPECTED and warns the job owners.
4. At about 6.5 s it becomes DEAD: `worker_declare_dead()` runs
   `job_rescue()` on its jobs, and each goes RUNNING → LOST → QUEUED with
   `[Event] Job 2 (attempt 1) moved back to QUEUED.`
5. The next pass dispatches the job elsewhere as attempt 2, sending its last
   checkpoint first if it has one.
6. A third failure makes it FAILED_PERMANENT.

### 9.2 Preemption
1. The cluster is full; mdot's `-preemptible` jobs are running. lordz submits
   a `-prio 1` job.
2. In the pass, `choose_worker()` finds nothing, so `try_preempt()` runs.
3. It picks the worker needing the fewest kills, and the victims
   (lowest priority, then newest), just enough of them.
4. The victims get `KILL_JOB reason=preempt` and go to QUEUED
   (`held_for = lordz's job`, `free_restart = 1`).
5. Their resources are released immediately in the scheduler's accounting,
   so lordz's job is dispatched there in the same pass.
6. Next pass, the hold is cleared (lordz's job has started). The victims run
   again when there's room, resuming from their last checkpoint, still on the
   same attempt count.

### 9.3 Migration
1. `migrate 1` → `handle_migrate()` checks the job exists, is yours, is
   RUNNING, isn't already migrating, and that its worker is reachable.
2. It sends `KILL_JOB reason=migrate` and sets `migrating = 1`. The reply
   says which checkpoint the job will resume from.
3. The worker sends SIGTERM to the job's group. The job (e.g. `counter`)
   catches it, saves its progress to `checkpoint/state`, and exits.
4. `finish_job()` runs one last `pump_checkpoint()`, sending the fresh
   checkpoint, then `JOB_DONE result=KILLED killed=migrate`.
5. If the job ignores SIGTERM, `tick_jobs()` sends SIGKILL after 2 s.
6. The scheduler stores the checkpoint, then handles `JOB_DONE`: QUEUED,
   `free_restart`, and `migrated_from = old worker`.
7. Next pass, `choose_worker()` avoids the old worker if another qualifies.
   `send_run_job()` sends `RESTORE_CKPT` and then `RUN_JOB`.
8. The new worker writes `checkpoint/state` before `exec`. The job reads it
   and continues ("resuming after step 6").

### 9.4 Executable caching
* **First run:** the full file goes with its hash. The worker verifies and
  stores it, and the scheduler records that this worker has the hash.
* **Second run on that worker:** only `cached=1 hash=...` is sent. The worker
  finds the file and copies it into the job directory.
* **Changed binary:** a new mtime or size makes the memo re-hash it. The hash
  is new, so the file is sent in full.
* **Cache file deleted:** the worker answers `CACHE_MISS`, and the scheduler
  forgets the hash and resends the full file under the same run.
* **Worker restart:** `cache_scan()` then `REGISTER cached=...`, so the
  scheduler knows what's still there.

### 9.5 Dependency cascade
1. Jobs 2 → 3 → 4 each wait on the one before. Job 2 segfaults.
2. `job_set_state(2, FAILED)` → `release_dependents(2)` →
   `job_check_deps(3)`, which sees a FAILED dependency.
3. Job 3 gets its event, then `job_set_state(3, CANCELLED)` →
   `release_dependents(3)` → job 4 is cancelled in the same way.

### 9.6 Graceful drain
1. `drain` (or Ctrl+C) on the worker sends `DRAIN`; the scheduler marks it
   DRAINING.
2. New jobs go elsewhere. Running ones finish normally; Ctrl+C never reached
   them, because each job is in its own process group.
3. When it has no jobs left, the worker exits.
4. The scheduler sees a DRAINING worker with 0 jobs disconnect: DEAD
   ("drained, left the cluster"), and no rescue.

---

## 10. The test suite (`tests/`)

**Running it**
```sh
tests/run_all.sh                 # builds, runs every test file (no root needed)
tests/run_all.sh cache           # only tests whose name contains "cache"
sudo tests/test_sudo.sh          # privilege-dropping tests (build as yourself first)
```

**`lib.sh`, the framework**
* **Isolation:** every test gets its own log directory and random TCP and
  discovery ports. Test programs are reached through a symlink without
  spaces (`$BIN`), because the client splits commands on spaces.
* **Driving processes:**
  * `start_scheduler`, `start_worker NAME [ENV=..] flags`,
    `start_client NAME USER`;
  * workers and clients read stdin from a FIFO held open by the script, so
    `send NAME "text"` types into them at any moment;
  * workers run under `setsid` (their own process group, like a separate
    terminal), so a test can send a realistic Ctrl+C to one worker without
    hitting the test script.
* **Helpers:**
  * `submit` returns the new job ID;
  * `signal_proc NAME SIG`, `stop`, `running`, `last_number`.
* **Checks:**
  * `expect FILE REGEX DESC` *waits* (up to a timeout) for a matching log
    line instead of sleeping a fixed time;
  * there are also `expect_not`, `expect_count`, `expect_count_eventually`,
    `expect_order` and `check`;
  * a failure prints the end of the relevant log.
* **`run_tests`** runs every `test_*` function in file order, cleans up
  processes after each, and prints a summary. Logs are kept if anything
  failed.

**`pty_run.py`** runs the client in a pseudo-terminal, like a person at a
real terminal. It reads `TYPE` / `LINE` / `CTRL_C` / `QUIT` instructions, and
is used to check that an event arriving while you type is handled cleanly.

**Test files**

| File | Covers |
|---|---|
| `test_m1_core.sh` | startup, log format, discovery and fallback, UDP rebind, clamping, usernames, args and logs, PATH commands, validation, every crash diagnosis, time and memory limits, FIFO blocking, PRIORITY order and aging, QUEUE_FULL, nodes and status, GPUs |
| `test_m2_resilience.sh` | events, logs -f, Ctrl+C, 64 KB tail, telemetry, cancel (no zombies), SUSPECTED/DEAD/rescue, FAILED_PERMANENT, re-registering, drain, scheduler restart, reconnecting, the terminal test |
| `test_m3_scheduling.sh` | dependencies, groups, cascades, preemption, quotas, cron |
| `test_m3_cache.sh` | every Section 13 check, plus a 30 MB binary |
| `test_m4_checkpoint.sh` | migration, SIGKILL fallback, resume after kill -9 and after preemption |
| `test_sudo.sh` | runs as nobody, can't escape its directory, ownership, fork bomb capped, SIGXCPU, RLIMIT_AS, checkpoints and cache as nobody |

**Test programs** (`tests/programs/`):
* `hello` prints its arguments;
* `infinite_loop` and `cpu_hog` (4 threads, trips `RLIMIT_CPU`) burn CPU;
* `slow_print` mostly sleeps;
* `memleak` grows its memory;
* `segfault`, `divzero` and `exit_code` end badly on purpose;
* `spam` produces 100 KB of output;
* `counter` checkpoints its progress;
* `ignore_term` ignores SIGTERM;
* `forkbomb` is bounded;
* `escape_attempt` tries to write outside its directory;
* `bigprog.c` builds `big_a`, `big_b` and `big_c`, three ~0.7 MB programs
  with different hashes.

---

## 11. Design choices and limitations

**Choices where the spec leaves room**
* **Preemption:** only jobs *submitted* with priority 1 preempt; aging alone
  doesn't give that power. Among workers where preemption works, the one
  needing the fewest victims is chosen.
* **Ownership:** `cancel`, `migrate` and `cancel-cron` only work on your own
  jobs and templates.
* **Same username twice:** both sessions are accepted and share the user's
  jobs and events.
* **Cancel timing:** a RUNNING job becomes CANCELLED (and frees its
  resources) when the kill is *sent*, matching the spec's log line.
* **Aging restarts** when a job re-enters QUEUED (after a rescue, preemption
  or migration).
* **Cron and a full queue:** that run is skipped and the owner told; cron
  copies count as normal submits.
* **Disconnects** go through SUSPECTED → DEAD on the heartbeat timer, not
  straight to DEAD.

**Limitations**
* Scheduler state lives in memory only; a scheduler restart forgets every
  job, as the spec expects.
* Without root, `RLIMIT_NPROC = 64` counts *your* user's processes, which
  probably already number over 64, so jobs that fork fail during development.
  Under `sudo` it applies to `nobody` and works as intended.
* `PR_SET_PDEATHSIG` is Linux-only. On macOS, killing a worker with `kill -9`
  can leave its jobs running.
* Job directories of a worker killed with `kill -9` aren't cleaned up;
  nothing is alive to do it.
* The macOS-specific code (memory watch through `proc_pidinfo`) hasn't been
  compiled or tested on a Mac.
* Paths with spaces must be quoted in the client.

---

## 12. Questions you might be asked

**Why an event loop instead of threads?**
One thread owns all the state, so there are no locks, races or deadlocks.
Non-blocking sockets with per-connection output buffers mean nothing can
stall the loop. Status and nodes answer immediately because a pass is a
quick in-memory computation.

**How is the queue "bounded and thread-safe"?**
It's bounded by the 1024 check in `handle_submit()`, which answers
QUEUE_FULL. It's thread-safe because only one thread exists. Rescued jobs
re-enter regardless of the limit.

**Why `setgid` before `setuid`?**
After `setuid(nobody)` the process has lost the privilege it needs to change
its group.

**Why check that `setuid(0)` fails afterwards?**
To prove the drop is permanent. If it could switch back to root, the job
could too.

**Why is `PR_SET_PDEATHSIG` set after `setuid`?**
Changing credentials clears it.

**Why give each job its own process group?**
So the terminal's Ctrl+C (sent to the foreground group) doesn't kill jobs
during a drain, and so `kill(-pgid)` takes out a job's children too.

**What does the CLOEXEC pipe do?**
It lets the child report setup errors. A successful `exec` closes the pipe
with nothing written; a failed step writes the reason before exiting.

**`RLIMIT_CPU` vs wall-clock?**
RLIMIT_CPU counts CPU time. A sleeping job never hits it, so the worker also
keeps its own wall-clock deadline and sends SIGKILL. Both are reported as
"Wall-clock timeout exceeded".

**Why 127?**
It's the shell convention for "command not found", and the spec asks for it.

**How do you avoid zombie processes?**
Every job is reaped with `waitpid()` (every 100 ms, `WNOHANG`), including
cancelled ones. `kill_all_jobs()` waits for each job.

**How does the scheduler notice a dead worker that didn't close its socket?**
Heartbeats. Silence makes it SUSPECTED after about 2.5 s and DEAD after
about 6.5 s, whatever happened to the TCP connection.

**What stops an old run's messages from confusing things?**
Run numbers (8.1).

**How does caching identify programs?**
By SHA-256 of the content, so two programs with the same name are never
confused and a one-byte change is a different program. LRU order lives in
file mtimes.

**How is the checkpoint ordered relative to the binary?**
`RESTORE_CKPT` is sent before `RUN_JOB` on the same TCP connection, and TCP
preserves order.

**Where does FIFO vs PRIORITY differ?**
Only in the sort order, and in what happens when a job doesn't fit: FIFO
stops the pass, PRIORITY moves on.
