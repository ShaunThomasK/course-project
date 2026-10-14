/*
 * jobs.c - job lifecycle, log buffers and the text shown by status/nodes.
 */
#include "scheduler.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int queue_occupancy(void)
{
    int n = 0;
    for (int i = 0; i < g_njobs; i++)
        if (g_jobs[i]->state == JOB_QUEUED || g_jobs[i]->state == JOB_WAITING)
            n++;
    return n;
}

static void reserve(worker_t *w, job_t *j)
{
    w->free_cores -= j->cores;
    w->free_ram_mb -= j->ram_mb;
    w->free_gpus -= j->gpus;
    w->running_jobs++;
}

static void release(worker_t *w, job_t *j)
{
    w->free_cores += j->cores;
    w->free_ram_mb += j->ram_mb;
    w->free_gpus += j->gpus;
    w->running_jobs--;
}

void job_set_state(job_t *j, job_state_t to, const char *why)
{
    job_state_t from = j->state;
    if (from == to)
        return;

    worker_t *w = j->worker_id ? worker_find(j->worker_id) : NULL;
    if (from == JOB_RUNNING && w)
        release(w, j);
    if (to == JOB_RUNNING && w) {
        reserve(w, j);
        j->start_ms = now_ms();
    }
    if (to == JOB_QUEUED) {
        j->queued_ms = now_ms();
        j->eff_prio_logged = j->prio;
    }
    if (job_state_is_terminal(to))
        j->end_ms = now_ms();

    j->state = to;
    if (why && *why)
        log_line("Job %d %s -> %s (%s)", j->id, job_state_name(from), job_state_name(to), why);
    else
        log_line("Job %d %s -> %s", j->id, job_state_name(from), job_state_name(to));

    if (job_state_is_terminal(to))
        job_end_followers(j);
    g_sched_dirty = 1;
}

void job_rescue(job_t *j)
{
    worker_t *w = worker_find(j->worker_id);
    if (w)
        release(w, j);

    if (j->attempts >= MAX_JOB_ATTEMPTS) {
        j->state = JOB_LOST;
        snprintf(j->reason, sizeof(j->reason),
                 "Worker failed on all %d attempts", MAX_JOB_ATTEMPTS);
        j->end_ms = now_ms();
        log_line("Job %d RUNNING -> LOST -> FAILED_PERMANENT (attempt %d of %d)",
                 j->id, j->attempts, MAX_JOB_ATTEMPTS);
        j->state = JOB_FAILED_PERMANENT;
        notify_user(j->user, "Job %d FAILED_PERMANENT. Reason: worker died on all %d attempts.",
                    j->id, MAX_JOB_ATTEMPTS);
        job_end_followers(j);
    } else {
        log_line("Job %d RUNNING -> LOST -> QUEUED (attempt %d of %d failed)",
                 j->id, j->attempts, MAX_JOB_ATTEMPTS);
        j->state = JOB_QUEUED;
        j->queued_ms = now_ms();
        j->eff_prio_logged = j->prio;
        notify_user(j->user, "Job %d (attempt %d) moved back to QUEUED.", j->id, j->attempts);
    }
    g_sched_dirty = 1;
}

void job_append_output(job_t *j, const char *data, size_t len)
{
    j->out_total += len;
    if (len >= OUTPUT_KEEP_BYTES) {
        data += len - OUTPUT_KEEP_BYTES;
        len = OUTPUT_KEEP_BYTES;
    }
    if (!j->out) {
        j->out = malloc(OUTPUT_KEEP_BYTES);
        if (!j->out)
            abort();
    }
    if (j->out_len + len > OUTPUT_KEEP_BYTES) {
        size_t drop = j->out_len + len - OUTPUT_KEEP_BYTES;
        memmove(j->out, j->out + drop, j->out_len - drop);
        j->out_len -= drop;
    }
    memcpy(j->out + j->out_len, data, len);
    j->out_len += len;

    /* Stream to everyone following this job. */
    msg_t *m = msg_new("LOG_DATA");
    msg_set_long(m, "job", j->id);
    msg_set_blob(m, data, len);
    for (int i = 0; i < g_npeers; i++)
        if (g_peers[i]->kind == PEER_CLIENT && g_peers[i]->follow_job == j->id)
            peer_send(g_peers[i], m);
    msg_free(m);
}

void job_end_followers(job_t *j)
{
    msg_t *m = msg_new("LOG_END");
    msg_set_long(m, "job", j->id);
    msg_set(m, "state", job_state_name(j->state));
    for (int i = 0; i < g_npeers; i++) {
        if (g_peers[i]->kind == PEER_CLIENT && g_peers[i]->follow_job == j->id) {
            peer_send(g_peers[i], m);
            g_peers[i]->follow_job = 0;
        }
    }
    msg_free(m);
}

static void bprintf(buf_t *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void bprintf(buf_t *b, const char *fmt, ...)
{
    char tmp[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > (int)sizeof(tmp) - 1)
        n = (int)sizeof(tmp) - 1;
    buf_append(b, tmp, (size_t)n);
}

void job_render_status(job_t *j, buf_t *out)
{
    bprintf(out, "Job %d [%s]\n", j->id, job_state_name(j->state));
    bprintf(out, "  - User: %s\n", j->user);
    bprintf(out, "  - Command: %s", j->path);
    for (int i = 0; i < j->argc; i++)
        bprintf(out, " %s", j->argv[i]);
    bprintf(out, "\n");
    bprintf(out, "  - Requested: %d cores, %ldMB RAM, %d GPU, %ds time limit, priority %d\n",
            j->cores, j->ram_mb, j->gpus, j->time_s, j->prio);
    bprintf(out, "  - Attempts: %d of %d\n", j->attempts, MAX_JOB_ATTEMPTS);

    uint64_t now = now_ms();
    if (j->state == JOB_QUEUED) {
        bprintf(out, "  - Waiting in queue for %.1fs\n", (double)(now - j->queued_ms) / 1000.0);
    } else if (j->state == JOB_RUNNING) {
        bprintf(out, "  - Running for %.1fs\n", (double)(now - j->start_ms) / 1000.0);
        bprintf(out, "  - Current RAM: %ldMB / %ldMB Limit\n", j->rss_kb / 1024, j->ram_mb);
        bprintf(out, "  - Page Faults: %ld\n", j->page_faults);
        bprintf(out, "  - CPU Time: %.1fs\n", j->cpu_s);
    } else if (job_state_is_terminal(j->state) && j->start_ms) {
        bprintf(out, "  - Peak RAM: %ldMB / %ldMB Limit\n", j->peak_rss_kb / 1024, j->ram_mb);
        bprintf(out, "  - Page Faults: %ld\n", j->page_faults);
        bprintf(out, "  - CPU Time: %.1fs\n", j->cpu_s);
    }
    if (j->reason[0])
        bprintf(out, "  - Reason: %s\n", j->reason);
    bprintf(out, "  - Output: %zu bytes\n", j->out_total);

    if (j->worker_id && j->state == JOB_RUNNING)
        bprintf(out, "[Job %d] Assigned to: Worker %d", j->id, j->worker_id);
    else if (j->worker_id && j->state != JOB_QUEUED)
        bprintf(out, "[Job %d] Ran on: Worker %d", j->id, j->worker_id);
    else
        bprintf(out, "[Job %d] Assigned to: None", j->id);
}

void render_nodes(buf_t *out)
{
    bprintf(out, "--- Cluster Status ---\n");
    bprintf(out, "%-6s %-10s %-14s %-18s %-14s %-5s %s\n",
            "ID", "Status", "Cores(F/T)", "RAM MB(F/T)", "GPU(F/T)", "CPU%", "Jobs");
    for (int i = 0; i < g_nworkers; i++) {
        worker_t *w = g_workers[i];
        char cores[32], ram[48], gpu[32];
        snprintf(cores, sizeof(cores), "%d / %d", w->free_cores, w->cores);
        snprintf(ram, sizeof(ram), "%ld / %ld", w->free_ram_mb, w->ram_mb);
        snprintf(gpu, sizeof(gpu), "%d / %d", w->free_gpus, w->gpus);
        bprintf(out, "%-6d %-10s %-14s %-18s %-14s %-5d %d",
                w->id, worker_state_name(w->state), cores, ram, gpu, w->cpu_load,
                w->running_jobs);
        /* List the job ids too, it's handy when debugging. */
        int first = 1;
        for (int k = 0; k < g_njobs; k++) {
            job_t *j = g_jobs[k];
            if (j->state == JOB_RUNNING && j->worker_id == w->id) {
                bprintf(out, "%s%d", first ? " [" : ",", j->id);
                first = 0;
            }
        }
        bprintf(out, "%s\n", first ? "" : "]");
    }
    if (g_nworkers == 0)
        bprintf(out, "(no workers registered)\n");
    bprintf(out, "----------------------");
}
