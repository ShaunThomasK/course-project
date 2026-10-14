/*
 * worker.h - the worker's view of the jobs it runs.
 */
#ifndef OSN_WORKER_H
#define OSN_WORKER_H

#include "../common/common.h"
#include "../common/proto.h"

#include <sys/types.h>

/* Why the worker itself killed a job; it reports this instead of a signal (5E). */
typedef enum {
    KILL_NONE,
    KILL_TIMEOUT,   /* wall-clock deadline (5A) */
    KILL_CANCEL,    /* client cancel (5C) */
    KILL_MEMORY,    /* resident memory over -ram (macOS, 5A) */
    KILL_PREEMPT,   /* Section 10 */
    KILL_MIGRATE,   /* Section 14 */
    KILL_SHUTDOWN   /* lost the scheduler connection */
} kill_reason_t;

typedef struct {
    int      id;
    int      run;            /* scheduler's run number for this start */
    msg_t   *next_run;       /* RUN_JOB for a newer run, started once this one is gone */
    pid_t    pid;
    uint64_t start_ms;
    int      time_s;
    long     ram_mb;
    char     dir[64];        /* /tmp/osn_job_<id> */
    char     hash[65];       /* SHA-256 of its cached binary, "" if none (13) */
    off_t    out_off;        /* how much of output.log was already sent */
    int      err_fd;         /* setup/exec errors from the child (CLOEXEC pipe) */

    kill_reason_t killed;
    char          killed_by[USERNAME_MAX];

    /* Section 14: the checkpoint file as we last saw it. */
    time_t   ckpt_mtime;
    long     ckpt_mtime_ns;
    off_t    ckpt_size;
    char    *ckpt;           /* last content sent or restored */
    size_t   ckpt_len;
    uint64_t term_deadline;  /* migrate: SIGKILL at this time, 0 = none */

    /* Telemetry (5D). */
    long rss_kb, peak_kb, faults, cpu_ms;

    int exited;              /* reaped; status is valid */
    int status;
} wjob_t;

/* Privilege dropping target, filled in at startup. */
extern int   g_is_root;
extern uid_t g_nobody_uid;
extern gid_t g_nobody_gid;

/* Forks and starts the job described by a RUN_JOB message, placing the
 * checkpoint (if any) in its checkpoint dir first. 0 on success, -1 with
 * errno set if the fork failed. */
int  jobrun_start(wjob_t *j, const msg_t *run, const char *ckpt, size_t ckpt_len);
/* Kills the job's whole process group with SIGKILL and remembers why. */
void jobrun_kill(wjob_t *j, kill_reason_t why, const char *by);
/* SIGTERM now, SIGKILL 2 s later (migration, Section 14). */
void jobrun_terminate(wjob_t *j, kill_reason_t why);
/* Returns a new checkpoint (malloc()ed) if the job wrote one since the
 * last call, else NULL. */
char *jobrun_new_checkpoint(wjob_t *j, size_t *len);
/* Non-blocking reap. Returns 1 once the job has exited. */
int  jobrun_reap(wjob_t *j);
/* Updates rss/peak/faults/cpu from the OS. */
void jobrun_sample(wjob_t *j);
/* Reads output.log past what was already sent. Returns a malloc()ed
 * buffer (caller frees) and its length, or NULL if nothing is new. */
char *jobrun_read_output(wjob_t *j, size_t max, size_t *len);
/* Plain-words explanation of how the job ended. Returns 1 if it completed. */
int  jobrun_diagnose(wjob_t *j, char *reason, size_t len);
/* Removes /tmp/osn_job_<id>. */
void jobrun_cleanup(wjob_t *j);

/* ---- cache.c (Section 13) ------------------------------------------------ */

void  cache_init(void);
/* Re-reads the cache folder (at every (re)registration). */
void  cache_scan(void);
/* Comma-separated hashes of every cached program (malloc()ed). */
char *cache_hash_list(void);
/* Stores a program, evicting LRU entries to fit. Fills path_out. */
int   cache_store(const char *name, const char *hash, const char *data, size_t len,
                  char *path_out, size_t n, int *evicted);
/* Finds a cached program and marks it used. -1 if it is gone. */
int   cache_lookup(const char *hash, char *path_out, size_t n);
/* Implemented by worker.c: is a running job using this program? */
int   cache_in_use(const char *hash);

#endif
