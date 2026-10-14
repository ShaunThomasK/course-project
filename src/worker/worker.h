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
    pid_t    pid;
    uint64_t start_ms;
    int      time_s;
    long     ram_mb;
    char     dir[64];        /* /tmp/osn_job_<id> */
    off_t    out_off;        /* how much of output.log was already sent */
    int      err_fd;         /* setup/exec errors from the child (CLOEXEC pipe) */

    kill_reason_t killed;
    char          killed_by[USERNAME_MAX];

    /* Telemetry (5D). */
    long rss_kb, peak_kb, faults, cpu_ms;

    int exited;              /* reaped; status is valid */
    int status;
} wjob_t;

/* Privilege dropping target, filled in at startup. */
extern int   g_is_root;
extern uid_t g_nobody_uid;
extern gid_t g_nobody_gid;

/* Forks and starts the job described by a RUN_JOB message. 0 on success,
 * -1 with errno set if the fork failed. */
int  jobrun_start(wjob_t *j, const msg_t *run);
/* Kills the job's whole process group with SIGKILL and remembers why. */
void jobrun_kill(wjob_t *j, kill_reason_t why, const char *by);
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

#endif
