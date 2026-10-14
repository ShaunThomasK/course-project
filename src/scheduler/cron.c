/*
 * cron.c - recurring jobs (Section 12).
 *
 * "submit ... -every N" registers a template instead of a job. A template
 * keeps a copy of the SUBMIT message; whenever it is due, the scheduler
 * builds a fresh job from that copy with job_create(), exactly like a
 * normal submit. The first copy is injected at once, then one every N
 * seconds, even if the previous copy is still running.
 */
#include "scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int      id;
    int      every_s;
    int      runs;
    int      cancelled;
    char     user[USERNAME_MAX];
    char     cmd[256];      /* for listing */
    msg_t   *tmpl;          /* the original SUBMIT */
    uint64_t next_ms;
} cron_t;

static cron_t **g_crons;
static int      g_ncrons;

int cron_register(const msg_t *submit, const char *user, int every_s)
{
    cron_t *c = calloc(1, sizeof(*c));
    if (!c)
        abort();
    c->id = g_ncrons + 1;
    c->every_s = every_s;
    str_copy(c->user, user, sizeof(c->user));
    c->tmpl = msg_clone(submit);
    c->next_ms = now_ms(); /* first copy goes in immediately */

    int n = snprintf(c->cmd, sizeof(c->cmd), "%s", msg_get_str(submit, "path"));
    long argc = msg_get_long(submit, "argc", 0);
    for (long i = 0; i < argc && n < (int)sizeof(c->cmd); i++) {
        char key[32];
        snprintf(key, sizeof(key), "arg%ld", i);
        n += snprintf(c->cmd + n, sizeof(c->cmd) - (size_t)n, " %s", msg_get_str(submit, key));
    }

    g_crons = realloc(g_crons, sizeof(cron_t *) * (size_t)(g_ncrons + 1));
    g_crons[g_ncrons++] = c;
    log_line("Cron %d REGISTERED by %s: every %ds, %s", c->id, user, every_s, c->cmd);
    return c->id;
}

void cron_tick(void)
{
    uint64_t now = now_ms();
    for (int i = 0; i < g_ncrons; i++) {
        cron_t *c = g_crons[i];
        if (c->cancelled || now < c->next_ms)
            continue;
        c->next_ms += (uint64_t)c->every_s * 1000;
        if (c->next_ms <= now)
            c->next_ms = now + (uint64_t)c->every_s * 1000; /* don't burst to catch up */

        if (queue_occupancy() >= JOB_QUEUE_CAPACITY) {
            log_line("Cron %d skipped a run: QUEUE_FULL", c->id);
            notify_user(c->user, "Cron %d skipped a run: the job queue is full.", c->id);
            continue;
        }
        job_t *j = job_create(c->tmpl, c->user);
        j->cron_id = c->id;
        c->runs++;
        log_line("Cron %d injected Job %d (run #%d)", c->id, j->id, c->runs);
        notify_user(c->user, "Cron %d injected Job %d (run #%d).", c->id, j->id, c->runs);
    }
}

void render_crons(buf_t *out)
{
    char line[512];
    int n = snprintf(line, sizeof(line), "%-4s %-8s %-6s %-12s %-10s %s\n",
                     "ID", "Every", "Runs", "User", "Status", "Command");
    buf_append(out, line, (size_t)n);
    for (int i = 0; i < g_ncrons; i++) {
        cron_t *c = g_crons[i];
        char every[16];
        snprintf(every, sizeof(every), "%ds", c->every_s);
        n = snprintf(line, sizeof(line), "%-4d %-8s %-6d %-12s %-10s %s\n", c->id, every,
                     c->runs, c->user, c->cancelled ? "CANCELLED" : "ACTIVE", c->cmd);
        buf_append(out, line, (size_t)n);
    }
    if (g_ncrons == 0) {
        n = snprintf(line, sizeof(line), "(no cron templates)\n");
        buf_append(out, line, (size_t)n);
    }
    if (out->len > out->off)
        out->len--; /* drop the last newline */
}

int cron_cancel(int id, const char *user, char *err, size_t errlen)
{
    if (id < 1 || id > g_ncrons) {
        snprintf(err, errlen, "No such cron template: %d", id);
        return -1;
    }
    cron_t *c = g_crons[id - 1];
    if (c->cancelled) {
        snprintf(err, errlen, "Cron %d is already cancelled.", id);
        return -1;
    }
    if (strcmp(c->user, user) != 0) {
        snprintf(err, errlen, "Cron %d belongs to %s; you can only cancel your own.", id, c->user);
        return -1;
    }
    c->cancelled = 1;
    log_line("Cron %d CANCELLED by %s after %d run(s); queued or running copies are left alone",
             id, user, c->runs);
    return 0;
}
