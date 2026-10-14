/*
 * scheduler.c - entry point, event loop, and message handlers.
 *
 *   ./scheduler <port> <FIFO|PRIORITY> [-discovery-port <port>]
 */
#include "scheduler.h"
#include "../common/discovery.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

algo_t     g_algo;
job_t    **g_jobs;
int        g_njobs;
worker_t **g_workers;
int        g_nworkers;
peer_t   **g_peers;
int        g_npeers;
int        g_sched_dirty;

static int g_next_worker_id = FIRST_WORKER_ID;

/* ---- lookups and sending ------------------------------------------------ */

job_t *job_find(int id)
{
    return (id >= 1 && id <= g_njobs) ? g_jobs[id - 1] : NULL;
}

worker_t *worker_find(int id)
{
    for (int i = 0; i < g_nworkers; i++)
        if (g_workers[i]->id == id)
            return g_workers[i];
    return NULL;
}

void peer_send(peer_t *p, const msg_t *m)
{
    if (!p || p->closing)
        return;
    if (conn_send(&p->conn, m) < 0)
        p->closing = 1;
}

void send_to_user(const char *user, const msg_t *m)
{
    for (int i = 0; i < g_npeers; i++)
        if (g_peers[i]->kind == PEER_CLIENT && strcmp(g_peers[i]->user, user) == 0)
            peer_send(g_peers[i], m);
}

void notify_user(const char *user, const char *fmt, ...)
{
    char text[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);

    msg_t *m = msg_new("EVENT");
    msg_set(m, "text", text);
    send_to_user(user, m);
    msg_free(m);
}

static void reply(peer_t *p, int ok, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static void reply(peer_t *p, int ok, const char *fmt, ...)
{
    char text[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);

    msg_t *m = msg_new("REPLY");
    msg_set_long(m, "ok", ok);
    msg_set(m, "text", text);
    peer_send(p, m);
    msg_free(m);
}

static void reply_buf(peer_t *p, buf_t *b)
{
    buf_append(b, "", 1); /* NUL-terminate */
    msg_t *m = msg_new("REPLY");
    msg_set_long(m, "ok", 1);
    msg_set(m, "text", b->data + b->off);
    peer_send(p, m);
    msg_free(m);
}

/* ---- worker liveness (4C) and state changes ----------------------------- */

/* Tells each user with a RUNNING job on w about w's state change. */
static void warn_users_of_worker(worker_t *w, const char *text)
{
    for (int i = 0; i < g_njobs; i++) {
        job_t *j = g_jobs[i];
        if (j->state != JOB_RUNNING || j->worker_id != w->id)
            continue;
        /* Only once per user: skip if an earlier job has the same owner. */
        int seen = 0;
        for (int k = 0; k < i && !seen; k++)
            seen = g_jobs[k]->state == JOB_RUNNING && g_jobs[k]->worker_id == w->id &&
                   strcmp(g_jobs[k]->user, j->user) == 0;
        if (!seen)
            notify_user(j->user, "WARNING: Worker %d %s", w->id, text);
    }
}

static void worker_set_state(worker_t *w, worker_state_t to, const char *why)
{
    if (w->state == to)
        return;
    log_line("Worker %d %s -> %s (%s)", w->id, worker_state_name(w->state),
             worker_state_name(to), why);
    w->state = to;
    g_sched_dirty = 1;
}

static void worker_declare_dead(worker_t *w, const char *why)
{
    warn_users_of_worker(w, "DEAD.");
    worker_set_state(w, WORKER_DEAD, why);
    if (w->peer) {
        /* If it was only unreachable, cut it off: on reconnect it must
         * register again as a new worker with a new ID. */
        w->peer->closing = 1;
        w->peer->worker_id = 0;
        w->peer = NULL;
    }
    for (int i = 0; i < g_njobs; i++)
        if (g_jobs[i]->state == JOB_RUNNING && g_jobs[i]->worker_id == w->id)
            job_rescue(g_jobs[i]);
}

static void check_heartbeats(void)
{
    uint64_t now = now_ms();
    for (int i = 0; i < g_nworkers; i++) {
        worker_t *w = g_workers[i];
        if (w->state == WORKER_DEAD)
            continue;
        uint64_t elapsed = now - w->last_heartbeat_ms;
        /* Half a second of slack so a slightly late heartbeat isn't a miss. */
        int missed = elapsed > 500 ? (int)((elapsed - 500) / HEARTBEAT_INTERVAL_MS) : 0;
        if (missed == w->missed)
            continue;
        w->missed = missed;
        if (missed >= DEAD_AFTER_MISSES) {
            char why[64];
            snprintf(why, sizeof(why), "%d missed heartbeats", missed);
            worker_declare_dead(w, why);
        } else if (missed >= 1 && w->state == WORKER_ALIVE) {
            worker_set_state(w, WORKER_SUSPECTED, "missed heartbeat");
            warn_users_of_worker(w, "SUSPECTED (missed heartbeat).");
        }
    }
}

/* ---- client requests ------------------------------------------------------ */

static void handle_submit(peer_t *p, msg_t *m)
{
    long cores = msg_get_long(m, "cores", -1), ram = msg_get_long(m, "ram", -1);
    long tlim = msg_get_long(m, "time", -1), prio = msg_get_long(m, "prio", -1);
    long gpus = msg_get_long(m, "gpu", 0);
    const char *path = msg_get_str(m, "path");

    if (!*path || cores < 1 || ram < 1 || tlim < 1 || prio < 1 || prio > 3 || gpus < 0) {
        reply(p, 0, "Invalid submit: need a binary, -cores >= 1, -ram >= 1, -time >= 1, -prio 1-3, -gpu >= 0.");
        return;
    }
    if (msg_get(m, "depends-on") || msg_get(m, "group") || msg_get(m, "every") ||
        msg_get(m, "preemptible")) {
        reply(p, 0, "-depends-on, -group, -preemptible and -every are not supported yet.");
        return;
    }
    if (strlen(path) >= PATH_MAX) {
        reply(p, 0, "Binary path is too long.");
        return;
    }
    if (queue_occupancy() >= JOB_QUEUE_CAPACITY) {
        log_line("Submit from %s rejected: QUEUE_FULL (%d entries)", p->user, JOB_QUEUE_CAPACITY);
        msg_t *r = msg_new("REPLY");
        msg_set_long(r, "ok", 0);
        msg_set(r, "code", "QUEUE_FULL");
        msg_setf(r, "text", "QUEUE_FULL: the job queue is full (%d jobs). Try again later.",
                 JOB_QUEUE_CAPACITY);
        peer_send(p, r);
        msg_free(r);
        return;
    }

    job_t *j = calloc(1, sizeof(*j));
    if (!j)
        abort();
    j->id = g_njobs + 1;
    str_copy(j->user, p->user, sizeof(j->user));
    str_copy(j->path, path, sizeof(j->path));
    j->argc = (int)msg_get_long(m, "argc", 0);
    if (j->argc < 0 || j->argc > 1024)
        j->argc = 0;
    j->argv = calloc((size_t)j->argc + 1, sizeof(char *));
    for (int i = 0; i < j->argc; i++) {
        char key[16];
        snprintf(key, sizeof(key), "arg%d", i);
        j->argv[i] = strdup(msg_get_str(m, key));
    }
    j->cores = (int)cores;
    j->ram_mb = ram;
    j->time_s = (int)tlim;
    j->prio = (int)prio;
    j->gpus = (int)gpus;
    j->submit_ms = j->queued_ms = now_ms();
    j->eff_prio_logged = j->prio;
    j->exit_code = -1;
    j->state = JOB_QUEUED;

    g_jobs = realloc(g_jobs, sizeof(job_t *) * (size_t)(g_njobs + 1));
    g_jobs[g_njobs++] = j;

    log_line("Job %d SUBMITTED by %s (%s, %d cores, %ldMB, %ds, prio %d, %d GPU)",
             j->id, j->user, j->path, j->cores, j->ram_mb, j->time_s, j->prio, j->gpus);

    msg_t *r = msg_new("REPLY");
    msg_set_long(r, "ok", 1);
    msg_set_long(r, "job", j->id);
    msg_set(r, "state", job_state_name(j->state));
    peer_send(p, r);
    msg_free(r);
    g_sched_dirty = 1;
}

static void handle_status(peer_t *p, msg_t *m)
{
    job_t *j = job_find((int)msg_get_long(m, "job", 0));
    if (!j) {
        reply(p, 0, "No such job: %s", msg_get_str(m, "job"));
        return;
    }
    buf_t b = {0};
    job_render_status(j, &b);
    reply_buf(p, &b);
    buf_free(&b);
}

static void handle_nodes(peer_t *p)
{
    buf_t b = {0};
    render_nodes(&b);
    reply_buf(p, &b);
    buf_free(&b);
}

static void handle_logs(peer_t *p, msg_t *m)
{
    job_t *j = job_find((int)msg_get_long(m, "job", 0));
    if (!j) {
        reply(p, 0, "No such job: %s", msg_get_str(m, "job"));
        return;
    }
    int follow = (int)msg_get_long(m, "follow", 0);

    msg_t *d = msg_new("LOG_DATA");
    msg_set_long(d, "job", j->id);
    if (j->out_total > j->out_len)
        msg_set_long(d, "truncated", (long long)(j->out_total - j->out_len));
    msg_set_blob(d, j->out, j->out_len);
    peer_send(p, d);
    msg_free(d);

    if (follow && !job_state_is_terminal(j->state)) {
        p->follow_job = j->id;
        log_line("Client '%s' following Job %d logs", p->user, j->id);
        return; /* LOG_END comes when the job finishes or the client stops */
    }
    msg_t *e = msg_new("LOG_END");
    msg_set_long(e, "job", j->id);
    msg_set(e, "state", job_state_name(j->state));
    peer_send(p, e);
    msg_free(e);
}

static void handle_cancel(peer_t *p, msg_t *m)
{
    job_t *j = job_find((int)msg_get_long(m, "job", 0));
    if (!j) {
        reply(p, 0, "No such job: %s", msg_get_str(m, "job"));
        return;
    }
    if (strcmp(j->user, p->user) != 0) {
        reply(p, 0, "Job %d belongs to %s; you can only cancel your own jobs.", j->id, j->user);
        return;
    }
    if (job_state_is_terminal(j->state)) {
        reply(p, 0, "Job %d is already %s.", j->id, job_state_name(j->state));
        return;
    }

    job_state_t from = j->state;
    char why[128];
    if (from == JOB_RUNNING) {
        worker_t *w = worker_find(j->worker_id);
        if (w && w->peer) {
            msg_t *k = msg_new("KILL_JOB");
            msg_set_long(k, "job", j->id);
            msg_set(k, "reason", "cancel");
            msg_set(k, "by", p->user);
            peer_send(w->peer, k);
            msg_free(k);
        }
        snprintf(why, sizeof(why), "kill sent, requested by %s", p->user);
    } else {
        snprintf(why, sizeof(why), "requested by %s", p->user);
    }
    snprintf(j->reason, sizeof(j->reason), "Cancelled by %s", p->user);
    job_set_state(j, JOB_CANCELLED, why);

    reply(p, 1, "[Client] Job %d cancelled.", j->id);
    notify_user(j->user, "Job %d %s -> CANCELLED (requested by %s).", j->id,
                job_state_name(from), p->user);
}

static void handle_client_msg(peer_t *p, msg_t *m)
{
    if (msg_is(m, "SUBMIT"))
        handle_submit(p, m);
    else if (msg_is(m, "STATUS"))
        handle_status(p, m);
    else if (msg_is(m, "NODES"))
        handle_nodes(p);
    else if (msg_is(m, "LOGS"))
        handle_logs(p, m);
    else if (msg_is(m, "UNFOLLOW"))
        p->follow_job = 0;
    else if (msg_is(m, "CANCEL"))
        handle_cancel(p, m);
    else
        reply(p, 0, "Unknown request '%s'.", m->type);
}

/* ---- worker messages ----------------------------------------------------- */

static void handle_heartbeat(worker_t *w, msg_t *m)
{
    w->last_heartbeat_ms = now_ms();
    w->missed = 0;
    w->cpu_load = (int)msg_get_long(m, "load", 0);
    w->reported_jobs = (int)msg_get_long(m, "jobs", 0);
    if (w->state == WORKER_SUSPECTED)
        worker_set_state(w, WORKER_ALIVE, "heartbeat arrived");

    /* Per-job telemetry: "id:rss_kb:peak_kb:faults:cpu_ms;..." (5D). */
    char *tele = strdup(msg_get_str(m, "tele"));
    char *save = NULL;
    for (char *t = strtok_r(tele, ";", &save); t; t = strtok_r(NULL, ";", &save)) {
        int id;
        long rss, peak, faults, cpu_ms;
        if (sscanf(t, "%d:%ld:%ld:%ld:%ld", &id, &rss, &peak, &faults, &cpu_ms) != 5)
            continue;
        job_t *j = job_find(id);
        if (!j || j->state != JOB_RUNNING || j->worker_id != w->id)
            continue;
        j->rss_kb = rss;
        if (peak > j->peak_rss_kb)
            j->peak_rss_kb = peak;
        j->page_faults = faults;
        j->cpu_s = (double)cpu_ms / 1000.0;
    }
    free(tele);
    g_sched_dirty = 1;
}

static void handle_job_done(worker_t *w, msg_t *m)
{
    job_t *j = job_find((int)msg_get_long(m, "job", 0));
    if (!j)
        return;

    /* Final numbers are useful even when we already moved the job on. */
    long peak = msg_get_long(m, "peak_kb", 0);
    if (peak > j->peak_rss_kb)
        j->peak_rss_kb = peak;
    j->page_faults = msg_get_long(m, "faults", j->page_faults);
    j->cpu_s = (double)msg_get_long(m, "cpu_ms", 0) / 1000.0;

    if (j->state != JOB_RUNNING || j->worker_id != w->id) {
        log_line("Job %d: final report from Worker %d noted (job is %s)", j->id, w->id,
                 job_state_name(j->state));
        return;
    }

    j->exit_code = (int)msg_get_long(m, "exit_code", -1);
    j->term_signal = (int)msg_get_long(m, "signal", 0);
    str_copy(j->reason, msg_get_str(m, "reason"), sizeof(j->reason));

    if (strcmp(msg_get_str(m, "result"), "COMPLETED") == 0) {
        job_set_state(j, JOB_COMPLETED, "exit code 0");
        notify_user(j->user, "Job %d COMPLETED successfully.", j->id);
    } else {
        job_set_state(j, JOB_FAILED, j->reason);
        log_line("[ERROR] Job %d Failed. Diagnosis: %s.", j->id, j->reason);
        notify_user(j->user, "Job %d FAILED. Reason: %s.", j->id, j->reason);
    }
}

static void handle_worker_msg(peer_t *p, msg_t *m)
{
    worker_t *w = worker_find(p->worker_id);
    if (!w || w->peer != p)
        return;

    if (msg_is(m, "HEARTBEAT")) {
        handle_heartbeat(w, m);
    } else if (msg_is(m, "JOB_OUTPUT")) {
        job_t *j = job_find((int)msg_get_long(m, "job", 0));
        if (j && m->blob_len)
            job_append_output(j, m->blob, m->blob_len);
    } else if (msg_is(m, "JOB_DONE")) {
        handle_job_done(w, m);
    } else if (msg_is(m, "DRAIN")) {
        if (w->state == WORKER_ALIVE || w->state == WORKER_SUSPECTED) {
            warn_users_of_worker(w, "DRAINING (finishing its jobs, then leaving).");
            worker_set_state(w, WORKER_DRAINING, "drain requested on the worker");
        }
    }
}

/* ---- connection setup -------------------------------------------------- */

static void handle_new_peer_msg(peer_t *p, msg_t *m)
{
    if (msg_is(m, "LOGIN")) {
        const char *user = msg_get_str(m, "user");
        if (!username_valid(user)) {
            reply(p, 0, "Invalid username.");
            return;
        }
        p->kind = PEER_CLIENT;
        str_copy(p->user, user, sizeof(p->user));
        int same = 0;
        for (int i = 0; i < g_npeers; i++)
            if (g_peers[i] != p && g_peers[i]->kind == PEER_CLIENT &&
                strcmp(g_peers[i]->user, user) == 0)
                same++;
        if (same)
            log_line("Client '%s' logged in (%d other session(s) with this name; they share jobs and events)",
                     user, same);
        else
            log_line("Client '%s' logged in", user);
        msg_t *r = msg_new("LOGIN_OK");
        msg_set(r, "user", user);
        peer_send(p, r);
        msg_free(r);
    } else if (msg_is(m, "REGISTER")) {
        worker_t *w = calloc(1, sizeof(*w));
        if (!w)
            abort();
        w->id = g_next_worker_id++;
        w->state = WORKER_ALIVE;
        w->peer = p;
        str_copy(w->host, msg_get_str(m, "host"), sizeof(w->host));
        w->cores = w->free_cores = (int)msg_get_long(m, "cores", 0);
        w->ram_mb = w->free_ram_mb = msg_get_long(m, "ram", 0);
        w->gpus = w->free_gpus = (int)msg_get_long(m, "gpu", 0);
        w->last_heartbeat_ms = now_ms();
        g_workers = realloc(g_workers, sizeof(worker_t *) * (size_t)(g_nworkers + 1));
        g_workers[g_nworkers++] = w;

        p->kind = PEER_WORKER;
        p->worker_id = w->id;
        log_line("Worker %d connected. Cores: %d, RAM: %ldMB, GPU: %d (host %s)",
                 w->id, w->cores, w->ram_mb, w->gpus, w->host);

        msg_t *r = msg_new("REGISTER_OK");
        msg_set_long(r, "id", w->id);
        peer_send(p, r);
        msg_free(r);
        g_sched_dirty = 1;
    } else {
        reply(p, 0, "Say LOGIN or REGISTER first.");
        p->closing = 1;
    }
}

static void peer_dispatch(peer_t *p, msg_t *m)
{
    switch (p->kind) {
    case PEER_NEW:    handle_new_peer_msg(p, m); break;
    case PEER_CLIENT: handle_client_msg(p, m); break;
    case PEER_WORKER: handle_worker_msg(p, m); break;
    }
}

static void peer_gone(peer_t *p)
{
    if (p->kind == PEER_CLIENT) {
        log_line("Client '%s' disconnected", p->user);
    } else if (p->kind == PEER_WORKER) {
        worker_t *w = worker_find(p->worker_id);
        if (w && w->peer == p) {
            w->peer = NULL;
            if (w->state == WORKER_DRAINING && w->running_jobs == 0) {
                worker_set_state(w, WORKER_DEAD, "drained, left the cluster");
            } else {
                /* Not a clean exit. The heartbeat timer takes it through
                 * SUSPECTED -> DEAD and rescues its jobs. */
                log_line("Worker %d connection lost; waiting on heartbeats", w->id);
            }
        }
    }
    conn_close(&p->conn);
    free(p);
}

static void accept_peers(int lfd)
{
    for (;;) {
        int fd = accept(lfd, NULL, NULL);
        if (fd < 0)
            return;
        set_nonblocking(fd);
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        peer_t *p = calloc(1, sizeof(*p));
        if (!p)
            abort();
        conn_init(&p->conn, fd);
        g_peers = realloc(g_peers, sizeof(peer_t *) * (size_t)(g_npeers + 1));
        g_peers[g_npeers++] = p;
    }
}

static void read_peer(peer_t *p)
{
    int r = conn_read(&p->conn);
    if (r <= 0) {
        p->closing = 1;
        buf_free(&p->conn.out); /* nothing more can be delivered */
        return;
    }
    msg_t *m;
    int got;
    while (!p->closing && (got = conn_next(&p->conn, &m)) == 1) {
        peer_dispatch(p, m);
        msg_free(m);
    }
    if (got < 0)
        p->closing = 1;
}

/* Closes peers marked closing whose output has drained (or can't). */
static void sweep_peers(void)
{
    int w = 0;
    for (int i = 0; i < g_npeers; i++) {
        peer_t *p = g_peers[i];
        if (p->closing && (!conn_has_output(&p->conn) || conn_flush(&p->conn) < 0 ||
                           !conn_has_output(&p->conn))) {
            peer_gone(p);
            continue;
        }
        g_peers[w++] = p;
    }
    g_npeers = w;
}

/* ---- main ---------------------------------------------------------------- */

static void usage(void)
{
    fprintf(stderr, "Usage: ./scheduler <port> <FIFO|PRIORITY> [-discovery-port <port>]\n");
    exit(1);
}

int main(int argc, char **argv)
{
    log_set_tag("SCHEDULER");
    signal(SIGPIPE, SIG_IGN);

    if (argc < 3)
        usage();
    long port, dport = DEFAULT_DISCOVERY_PORT;
    if (parse_long(argv[1], &port) < 0 || port < 1 || port > 65535)
        usage();
    if (strcmp(argv[2], "FIFO") == 0)
        g_algo = ALGO_FIFO;
    else if (strcmp(argv[2], "PRIORITY") == 0)
        g_algo = ALGO_PRIORITY;
    else
        usage();
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "-discovery-port") == 0 && i + 1 < argc) {
            if (parse_long(argv[++i], &dport) < 0 || dport < 1 || dport > 65535)
                usage();
        } else {
            usage();
        }
    }

    int lfd = tcp_listen((int)port);
    if (lfd < 0) {
        fprintf(stderr, "scheduler: cannot listen on port %ld: %s\n", port, strerror(errno));
        return 1;
    }
    log_line("Started on port %ld. Algorithm: %s", port, argv[2]);

    int ufd = discovery_open_responder((int)dport);
    if (ufd < 0)
        log_line("WARNING: discovery disabled, cannot bind UDP port %ld: %s", dport, strerror(errno));
    else
        log_line("Discovery responder listening on UDP port %ld.", dport);
    log_line("Listening for incoming connections...");

    uint64_t last_pass = 0;
    struct pollfd *pfds = NULL;

    for (;;) {
        int nfds = 2 + g_npeers;
        pfds = realloc(pfds, sizeof(struct pollfd) * (size_t)nfds);
        pfds[0] = (struct pollfd){.fd = lfd, .events = POLLIN};
        pfds[1] = (struct pollfd){.fd = ufd, .events = POLLIN};
        for (int i = 0; i < g_npeers; i++) {
            peer_t *p = g_peers[i];
            pfds[2 + i] = (struct pollfd){
                .fd = p->conn.fd,
                .events = (short)(POLLIN | (conn_has_output(&p->conn) ? POLLOUT : 0)),
            };
        }

        int r = poll(pfds, (nfds_t)nfds, 200);
        if (r < 0 && errno != EINTR) {
            perror("poll");
            return 1;
        }

        /* Peers first (their indices match pfds); new ones are appended. */
        int npeers = g_npeers;
        for (int i = 0; i < npeers && r > 0; i++) {
            peer_t *p = g_peers[i];
            short re = pfds[2 + i].revents;
            if (re & (POLLIN | POLLHUP | POLLERR))
                read_peer(p);
            if ((re & POLLOUT) && conn_flush(&p->conn) < 0)
                p->closing = 1;
        }
        if (r > 0 && (pfds[1].revents & POLLIN))
            discovery_handle_request(ufd, (int)port);
        if (r > 0 && (pfds[0].revents & POLLIN))
            accept_peers(lfd);

        check_heartbeats();

        uint64_t now = now_ms();
        if (g_sched_dirty || now - last_pass >= 1000) {
            schedule_pass();
            last_pass = now;
        }
        sweep_peers();
    }
}
