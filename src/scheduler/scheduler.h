/*
 * scheduler.h - shared state of the scheduler process.
 *
 * Concurrency model (Section 4A): a single-threaded event loop. One thread
 * poll()s the listening socket, the UDP discovery socket and every client
 * and worker connection; all sockets are non-blocking and every connection
 * has its own output buffer. Nothing can block the loop, so the job table
 * and worker table need no locks, and a status/nodes request is answered
 * as soon as it is read: it is never stuck behind a scheduling decision.
 * The scheduling pass runs on the same thread after any relevant event and
 * at least once a second.
 *
 * Fields used only by later milestones are already present so the
 * structures don't need to change shape later.
 */
#ifndef OSN_SCHEDULER_H
#define OSN_SCHEDULER_H

#include "../common/common.h"
#include "../common/proto.h"

#include <limits.h>
#include <stdint.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef enum { ALGO_FIFO, ALGO_PRIORITY } algo_t;

typedef enum { PEER_NEW, PEER_CLIENT, PEER_WORKER } peer_kind_t;

/* One TCP connection: a client, a worker, or not identified yet. */
typedef struct peer {
    conn_t      conn;
    peer_kind_t kind;
    int         closing;          /* close once the output buffer drains */
    char        user[USERNAME_MAX]; /* clients */
    int         follow_job;       /* clients: job whose logs are followed, 0 = none */
    int         worker_id;        /* workers */
} peer_t;

typedef struct worker {
    int            id;
    worker_state_t state;
    peer_t        *peer;          /* NULL once the connection is gone */
    char           host[64];

    int  cores, free_cores;
    long ram_mb, free_ram_mb;
    int  gpus, free_gpus;

    int      cpu_load;            /* 0-100, from heartbeats (4D) */
    int      reported_jobs;       /* job count from the last heartbeat */
    int      running_jobs;        /* scheduler's own count of RUNNING jobs here */
    uint64_t last_heartbeat_ms;
    int      missed;              /* consecutive missed heartbeats (4C) */

    /* Section 13: hashes of the binaries this worker has cached. */
    char   **cached;
    int      ncached;
} worker_t;

typedef struct job {
    int         id;
    char        user[USERNAME_MAX];
    char        path[PATH_MAX];   /* absolute path, or bare PATH command */
    int         argc;             /* program arguments, without argv[0] */
    char      **argv;

    int  cores;
    long ram_mb;
    int  time_s;
    int  prio;                    /* 1 (highest) .. 3 */
    int  gpus;

    job_state_t state;
    int         worker_id;        /* current (or last) worker, 0 = none */
    uint64_t    submit_ms;
    uint64_t    queued_ms;        /* when it last entered QUEUED (aging) */
    uint64_t    start_ms, end_ms;
    int         eff_prio_logged;  /* last effective priority we logged */
    int         attempts;         /* starts that count against MAX_JOB_ATTEMPTS */
    int         run;              /* bumped on every start; reports carry it, so a
                                     late report from an earlier run is ignored */
    char        reason[256];      /* why it failed / was cancelled */
    int         exit_code;        /* -1 if killed by a signal */
    int         term_signal;

    /* Section 5D: live telemetry from heartbeats. */
    long   rss_kb, peak_rss_kb;
    long   page_faults;
    double cpu_s;

    /* Section 6: last OUTPUT_KEEP_BYTES of stdout/stderr. */
    char  *out;
    size_t out_len;
    size_t out_total;             /* bytes ever received */

    /* Section 8: dependencies and groups. */
    int  *deps;
    int   ndeps;
    int   group;                  /* 0 = none */

    /* Section 10: preemption. */
    int   preemptible;
    int   held_for;               /* preempted for this job; 0 = no hold */

    /* Section 12: the cron template that spawned this job, 0 = none. */
    int   cron_id;

    /* Section 14: checkpoints and migration. */
    int      migrated_from;       /* worker to avoid on the next start */
    char    *ckpt;
    size_t   ckpt_len;
    int      ckpt_num;
    uint64_t ckpt_ms;
} job_t;

/* ---- global state (scheduler.c) ---------------------------------------- */

extern algo_t     g_algo;
extern job_t    **g_jobs;    /* g_jobs[id - 1] */
extern int        g_njobs;
extern worker_t **g_workers;
extern int        g_nworkers;
extern peer_t   **g_peers;
extern int        g_npeers;
extern int        g_sched_dirty; /* something changed; run a pass soon */

job_t    *job_find(int id);
worker_t *worker_find(int id);

/* Sends to every client connection logged in as user. */
void send_to_user(const char *user, const msg_t *m);
/* Pushes an "[Event] ..." line to the owner of a job. */
void notify_user(const char *user, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void peer_send(peer_t *p, const msg_t *m);

/* ---- jobs.c ------------------------------------------------------------ */

/* Number of entries occupying the bounded queue (QUEUED + WAITING). */
int  queue_occupancy(void);
/* Central job state transition: logs it and keeps worker accounting right. */
void job_set_state(job_t *j, job_state_t to, const char *why);
void job_append_output(job_t *j, const char *data, size_t len);
/* Ends `logs -f` for everyone following j. */
void job_end_followers(job_t *j);
void job_render_status(job_t *j, buf_t *out);
void render_nodes(buf_t *out);
/* Moves a job off a worker that died (RUNNING -> LOST -> QUEUED/FAILED_PERMANENT). */
void job_rescue(job_t *j);

/* ---- pass.c ------------------------------------------------------------ */

void schedule_pass(void);

#endif
