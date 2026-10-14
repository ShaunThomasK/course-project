/*
 * pass.c - one scheduling pass (Section 4B, and "Order of Scheduling
 * Decisions"):
 *
 *   1. Order the QUEUED jobs (FIFO: submission order; PRIORITY: effective
 *      priority after aging, then older first).
 *   2. Match: find the workers that qualify for a job.
 *   3. Choose one of them (idle > cached > lower CPU > fewer jobs).
 *   4. No match: FIFO stops the pass, PRIORITY moves to the next job.
 */
#include "scheduler.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Effective priority under aging: one step better per 10 s queued, min 1. */
static int effective_prio(const job_t *j, uint64_t now)
{
    if (g_algo != ALGO_PRIORITY)
        return j->prio;
    int boost = (int)((now - j->queued_ms) / AGING_INTERVAL_MS);
    int p = j->prio - boost;
    return p < 1 ? 1 : p;
}

static uint64_t g_pass_now;

static int cmp_jobs(const void *a, const void *b)
{
    const job_t *x = *(job_t *const *)a;
    const job_t *y = *(job_t *const *)b;
    if (g_algo == ALGO_PRIORITY) {
        int px = effective_prio(x, g_pass_now);
        int py = effective_prio(y, g_pass_now);
        if (px != py)
            return px - py;
    }
    return x->id - y->id; /* ids are handed out in submission order */
}

static int worker_qualifies(const worker_t *w, const job_t *j)
{
    return w->state == WORKER_ALIVE && w->peer != NULL &&
           w->free_cores >= j->cores && w->free_ram_mb >= j->ram_mb &&
           w->free_gpus >= j->gpus;
}

static int worker_has_cached(const worker_t *w, const job_t *j)
{
    (void)w;
    (void)j;
    return 0; /* Section 13 fills this in */
}

/*
 * Compares two qualifying workers for job j. Returns <0 if a is better.
 * When why != NULL, writes the deciding criterion, e.g. "CPU 8% vs 61%".
 */
static int cmp_workers(const worker_t *a, const worker_t *b, const job_t *j,
                       char *why, size_t whylen)
{
    int idle_a = a->running_jobs == 0, idle_b = b->running_jobs == 0;
    if (idle_a != idle_b) {
        if (why)
            snprintf(why, whylen, "%s", idle_a ? "idle vs busy" : "busy vs idle");
        return idle_b - idle_a;
    }
    int ca = worker_has_cached(a, j), cb = worker_has_cached(b, j);
    if (ca != cb) {
        if (why)
            snprintf(why, whylen, "binary %s, both %s",
                     ca ? "cached vs not cached" : "not cached vs cached",
                     idle_a ? "idle" : "busy");
        return cb - ca;
    }
    if (a->cpu_load != b->cpu_load) {
        if (why)
            snprintf(why, whylen, "CPU %d%% vs %d%%", a->cpu_load, b->cpu_load);
        return a->cpu_load - b->cpu_load;
    }
    if (a->running_jobs != b->running_jobs) {
        if (why)
            snprintf(why, whylen, "%d running jobs vs %d", a->running_jobs, b->running_jobs);
        return a->running_jobs - b->running_jobs;
    }
    if (why)
        snprintf(why, whylen, "tie on load and jobs, lower id");
    return a->id - b->id;
}

/* Steps 2 and 3: returns the chosen worker (and logs why), or NULL. */
static worker_t *choose_worker(job_t *j)
{
    worker_t *best = NULL, *second = NULL;
    for (int i = 0; i < g_nworkers; i++) {
        worker_t *w = g_workers[i];
        if (!worker_qualifies(w, j))
            continue;
        if (!best || cmp_workers(w, best, j, NULL, 0) < 0) {
            second = best;
            best = w;
        } else if (!second || cmp_workers(w, second, j, NULL, 0) < 0) {
            second = w;
        }
    }
    if (!best)
        return NULL;

    if (second) {
        char why[128];
        cmp_workers(best, second, j, why, sizeof(why));
        log_line("Job %d -> Worker %d (chosen over %d: %s)", j->id, best->id, second->id, why);
    } else {
        log_line("Job %d -> Worker %d (only qualifying worker)", j->id, best->id);
    }
    return best;
}

/* Reads the whole binary into memory; NULL if the scheduler can't read it. */
static char *read_binary(const char *path, size_t *len)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return NULL;
    struct stat st;
    if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode)) {
        close(fd);
        return NULL;
    }
    char *data = malloc(st.st_size > 0 ? (size_t)st.st_size : 1);
    if (!data) {
        close(fd);
        return NULL;
    }
    size_t got = 0;
    while (got < (size_t)st.st_size) {
        ssize_t n = read(fd, data + got, (size_t)st.st_size - got);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            break;
        got += (size_t)n;
    }
    close(fd);
    *len = got;
    return data;
}

static void dispatch(job_t *j, worker_t *w)
{
    msg_t *m = msg_new("RUN_JOB");
    msg_set_long(m, "job", j->id);
    msg_set(m, "path", j->path);
    msg_set_long(m, "argc", j->argc);
    for (int i = 0; i < j->argc; i++) {
        char key[16];
        snprintf(key, sizeof(key), "arg%d", i);
        msg_set(m, key, j->argv[i]);
    }
    msg_set_long(m, "cores", j->cores);
    msg_set_long(m, "ram", j->ram_mb);
    msg_set_long(m, "time", j->time_s);
    msg_set_long(m, "gpu", j->gpus);

    /* A path with a '/' is a file the scheduler ships to the worker. A bare
     * name (or a file we can't read) runs by path on the worker. */
    if (strchr(j->path, '/')) {
        size_t len = 0;
        char *data = read_binary(j->path, &len);
        if (data) {
            msg_set_long(m, "binary", 1);
            msg_take_blob(m, data, len);
            log_line("Job %d: sent %s (%zu bytes) -> Worker %d", j->id, j->path, len, w->id);
        } else {
            log_line("Job %d: can't read %s (%s), worker runs it by path",
                     j->id, j->path, strerror(errno));
        }
    }

    if (j->out_total > 0) {
        /* Earlier attempts' output stays; mark where this one starts. */
        char mark[96];
        int n = snprintf(mark, sizeof(mark), "\n--- restarted on Worker %d (start #%d) ---\n",
                         w->id, j->run + 1);
        job_append_output(j, mark, (size_t)n);
    }
    j->worker_id = w->id;
    if (j->free_restart)
        j->free_restart = 0; /* preempted or migrated: not a failure (10, 14) */
    else
        j->attempts++;
    j->run++;
    msg_set_long(m, "run", j->run);
    j->rss_kb = j->peak_rss_kb = j->page_faults = 0;
    j->cpu_s = 0;
    char why[64];
    snprintf(why, sizeof(why), "Worker %d, attempt %d", w->id, j->attempts);
    job_set_state(j, JOB_RUNNING, why);

    peer_send(w->peer, m);
    msg_free(m);

    if (j->attempts > 1)
        notify_user(j->user, "Job %d (attempt %d) RUNNING on Worker %d.", j->id, j->attempts, w->id);
    else
        notify_user(j->user, "Job %d RUNNING on Worker %d.", j->id, w->id);
}

/* ---- Section 11: fair-share quotas ------------------------------------- */

static int cluster_cores(void)
{
    int n = 0;
    for (int i = 0; i < g_nworkers; i++)
        if (g_workers[i]->state != WORKER_DEAD)
            n += g_workers[i]->cores;
    return n;
}

static int user_running_cores(const char *user)
{
    int n = 0;
    for (int i = 0; i < g_njobs; i++)
        if (g_jobs[i]->state == JOB_RUNNING && strcmp(g_jobs[i]->user, user) == 0)
            n += g_jobs[i]->cores;
    return n;
}

/* More than half the cluster's cores. Exactly 50% is not over. */
static int user_over_quota(const char *user)
{
    int total = cluster_cores();
    return total > 0 && 2 * user_running_cores(user) > total;
}

static int job_is_held(job_t *j);

/*
 * Step 2 of the pass: if j's user is over quota and another user, who
 * isn't, has a QUEUED job, j is skipped. Returns 1 to skip.
 */
static int quota_says_skip(job_t *j, job_t **list, int n)
{
    if (!user_over_quota(j->user))
        return 0;
    for (int i = 0; i < n; i++) {
        job_t *o = list[i];
        if (o->state != JOB_QUEUED || strcmp(o->user, j->user) == 0 || job_is_held(o))
            continue;
        if (user_over_quota(o->user))
            continue;
        if (j->quota_skip_logged != o->id) {
            log_line("Skipping Job %d (user %s, quota exceeded) in favor of Job %d (user %s)",
                     j->id, j->user, o->id, o->user);
            j->quota_skip_logged = o->id;
        }
        return 1;
    }
    return 0;
}

/* ---- Section 10: preemption --------------------------------------------- */

/* Step 1 of the pass: a preempted job waits until its preemptor started. */
static int job_is_held(job_t *j)
{
    if (!j->held_for)
        return 0;
    job_t *p = job_find(j->held_for);
    if (p && p->state == JOB_QUEUED)
        return 1;
    j->held_for = 0; /* started, or cancelled/failed before starting */
    return 0;
}

static int cmp_victims(const void *a, const void *b)
{
    const job_t *x = *(job_t *const *)a;
    const job_t *y = *(job_t *const *)b;
    if (x->prio != y->prio)
        return y->prio - x->prio; /* lowest priority (highest number) first */
    return y->id - x->id;         /* then the most recently submitted */
}

/*
 * Picks the victims on w that free enough room for j, in kill order.
 * Returns how many are needed (written to victims[]), or -1 if even
 * killing every preemptible job there isn't enough.
 */
static int pick_victims(worker_t *w, job_t *j, job_t **victims)
{
    int n = 0;
    for (int i = 0; i < g_njobs; i++) {
        job_t *v = g_jobs[i];
        if (v->state == JOB_RUNNING && v->worker_id == w->id && v->preemptible && v != j)
            victims[n++] = v;
    }
    qsort(victims, (size_t)n, sizeof(job_t *), cmp_victims);

    int cores = w->free_cores, gpus = w->free_gpus;
    long ram = w->free_ram_mb;
    for (int k = 0; k <= n; k++) {
        if (cores >= j->cores && ram >= j->ram_mb && gpus >= j->gpus)
            return k; /* just enough, and no more */
        if (k == n)
            break;
        cores += victims[k]->cores;
        ram += victims[k]->ram_mb;
        gpus += victims[k]->gpus;
    }
    return -1;
}

/* Step 5a: a priority 1 job with no qualifying worker makes room. */
static worker_t *try_preempt(job_t *j)
{
    if (j->prio != 1)
        return NULL;
    job_t **victims = malloc(sizeof(job_t *) * (size_t)(g_njobs + 1));
    job_t **best_v = malloc(sizeof(job_t *) * (size_t)(g_njobs + 1));
    worker_t *best = NULL;
    int best_k = -1;

    for (int i = 0; i < g_nworkers; i++) {
        worker_t *w = g_workers[i];
        if (w->state != WORKER_ALIVE || !w->peer)
            continue;
        int k = pick_victims(w, j, victims);
        if (k < 1)
            continue; /* can't make room here (k == 0 can't happen: no match) */
        if (!best || k < best_k) {
            best = w;
            best_k = k;
            memcpy(best_v, victims, sizeof(job_t *) * (size_t)k);
        }
    }

    for (int k = 0; best && k < best_k; k++) {
        job_t *v = best_v[k];
        log_line("Preempting Job %d (user %s, priority %d) on Worker %d for Job %d",
                 v->id, v->user, v->prio, best->id, j->id);
        msg_t *m = msg_new("KILL_JOB");
        msg_set_long(m, "job", v->id);
        msg_set_long(m, "run", v->run);
        msg_set(m, "reason", "preempt");
        peer_send(best->peer, m);
        msg_free(m);

        v->held_for = j->id;
        v->free_restart = 1;
        char why[64];
        snprintf(why, sizeof(why), "preempted by Job %d, no attempt used", j->id);
        notify_user(v->user, "Job %d preempted by priority 1 Job %d; moved back to QUEUED.",
                    v->id, j->id);
        job_set_state(v, JOB_QUEUED, why);
    }
    if (best)
        log_line("Job %d -> Worker %d (made room by preempting %d job(s))", j->id, best->id, best_k);
    free(victims);
    free(best_v);
    return best;
}

void schedule_pass(void)
{
    g_sched_dirty = 0;
    g_pass_now = now_ms();

    /* Collect QUEUED jobs only; WAITING jobs are not eligible yet. */
    job_t **list = malloc(sizeof(job_t *) * (size_t)(g_njobs + 1));
    if (!list)
        abort();
    int n = 0;
    for (int i = 0; i < g_njobs; i++) {
        job_t *j = g_jobs[i];
        if (j->state != JOB_QUEUED)
            continue;
        list[n++] = j;
        int eff = effective_prio(j, g_pass_now);
        if (g_algo == ALGO_PRIORITY && eff != j->eff_prio_logged) {
            log_line("Job %d aged: effective priority %d -> %d", j->id, j->eff_prio_logged, eff);
            j->eff_prio_logged = eff;
        }
    }
    qsort(list, (size_t)n, sizeof(job_t *), cmp_jobs);

    for (int i = 0; i < n; i++) {
        job_t *j = list[i];
        if (j->state != JOB_QUEUED)
            continue;
        if (job_is_held(j))                  /* 1. preempted-job hold (10) */
            continue;
        if (quota_says_skip(j, list, n))     /* 2. user quota (11) */
            continue;
        worker_t *w = choose_worker(j);      /* 3-4. match and choose */
        if (!w)
            w = try_preempt(j);              /* 5a. priority 1 preemption (10) */
        if (w) {
            dispatch(j, w);
            continue;
        }
        if (g_algo == ALGO_FIFO)
            break;                           /* 5b. the oldest job blocks the rest */
    }
    free(list);
}
