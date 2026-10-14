/*
 * common.h - shared constants, state enums and small helpers used by the
 * scheduler, the worker and the client.
 *
 * The full job and worker state machines are declared here up front (see the
 * "State Machines" section of the spec), even for states that only later
 * milestones use, so the data structures never have to change shape.
 */
#ifndef OSN_COMMON_H
#define OSN_COMMON_H

#include <stddef.h>
#include <stdint.h>

/* ---- tunables ---------------------------------------------------------- */

#define DEFAULT_DISCOVERY_PORT 9999
#define DISCOVERY_ATTEMPTS     3
#define DISCOVERY_TIMEOUT_MS   2000

#define HEARTBEAT_INTERVAL_MS  2000
#define DEAD_AFTER_MISSES      3      /* 3 missed heartbeats (~6 s) -> DEAD */
#define MAX_JOB_ATTEMPTS       3      /* retry limit for worker failures (4C) */

#define JOB_QUEUE_CAPACITY     1024   /* bounded queue (4A) */
#define AGING_INTERVAL_MS      10000  /* PRIORITY aging step (4B) */
#define OUTPUT_KEEP_BYTES      (64 * 1024) /* per-job log tail kept in memory (6) */
#define OUTPUT_POLL_MS         500    /* worker reads output.log this often (5B) */

#define JOB_NPROC_LIMIT        64     /* RLIMIT_NPROC for jobs (5A step 5) */

#define USERNAME_MAX           32     /* 31 chars + NUL */
#define FIRST_WORKER_ID        1001

/* ---- state machines ---------------------------------------------------- */

typedef enum {
    JOB_WAITING,          /* blocked on dependencies (Section 8) */
    JOB_QUEUED,           /* waiting for a worker */
    JOB_RUNNING,          /* executing on a worker */
    JOB_COMPLETED,        /* exit code 0 */
    JOB_FAILED,           /* non-zero exit, signal, timeout ... */
    JOB_CANCELLED,        /* by a user, or a dependency failed */
    JOB_LOST,             /* transient: its worker went DEAD (4C) */
    JOB_FAILED_PERMANENT  /* ran out of retry attempts (4C) */
} job_state_t;

typedef enum {
    WORKER_ALIVE,
    WORKER_SUSPECTED,
    WORKER_DEAD,
    WORKER_DRAINING
} worker_state_t;

const char *job_state_name(job_state_t s);
const char *worker_state_name(worker_state_t s);
int job_state_is_terminal(job_state_t s);

/* ---- helpers ----------------------------------------------------------- */

/* Milliseconds on a monotonic clock; only differences are meaningful. */
uint64_t now_ms(void);

/* 1-31 chars from [A-Za-z0-9_.-] (Section 3, Login). */
int username_valid(const char *name);

/* Copies at most dstsz-1 bytes and always NUL-terminates. */
void str_copy(char *dst, const char *src, size_t dstsz);

/* Parses a whole decimal string into *out; returns 0 on success. */
int parse_long(const char *s, long *out);

/* ---- logging (Section 7) ----------------------------------------------- */

/* Sets the component tag printed in brackets, e.g. "SCHEDULER", "WORKER 1001". */
void log_set_tag(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/* The single central logging function: "[HH:MM:SS] [TAG] message". */
void log_line(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/* An input prompt (e.g. "worker> ") that log_line() keeps at the bottom:
 * each log line clears it, prints, and draws it again. Only takes effect
 * when stdin and stdout are a terminal. The prompt is cleared at exit. */
void log_set_prompt(const char *prompt);
/* Redraws the prompt, e.g. after the user pressed Enter on an empty line. */
void log_show_prompt(void);

#endif
