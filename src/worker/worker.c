/*
 * worker.c - entry point and main loop of a worker.
 *
 *   ./worker [<scheduler_ip> <scheduler_port>] -cores <num> -ram <MB>
 *            [-gpu <num>] [-discovery-port <port>]
 *
 * One poll() loop over the scheduler socket and stdin, with a 100 ms tick
 * that reaps finished jobs, enforces wall-clock deadlines, streams output
 * every 500 ms and sends a heartbeat every 2 s.
 */
#include "worker.h"
#include "../common/discovery.h"
#include "../common/sha256.h"

#include <errno.h>
#include <poll.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define TICK_MS 100

static int  g_cores, g_gpus;
static long g_ram_mb;
static int  g_discovery_port = DEFAULT_DISCOVERY_PORT;
static const char *g_fallback_ip;
static int  g_fallback_port;

static conn_t g_conn = {.fd = -1};
static int    g_id;

static wjob_t **g_jobs;
static int      g_njobs;

static volatile sig_atomic_t g_sigint;
static int g_draining;
static int g_stdin_open = 1;

static void on_sigint(int sig)
{
    (void)sig;
    g_sigint = 1;
}

/* ---- setup ------------------------------------------------------------- */

static void usage(void)
{
    fprintf(stderr, "Usage: ./worker [<scheduler_ip> <scheduler_port>] -cores <num> -ram <MB> "
                    "[-gpu <num>] [-discovery-port <port>]\n");
    exit(1);
}

static void parse_args(int argc, char **argv)
{
    long v;
    int i = 1;
    if (argc >= 3 && argv[1][0] != '-') {
        g_fallback_ip = argv[1];
        if (parse_long(argv[2], &v) < 0 || v < 1 || v > 65535)
            usage();
        g_fallback_port = (int)v;
        i = 3;
    }
    g_cores = -1;
    g_ram_mb = -1;
    for (; i < argc; i++) {
        if (i + 1 >= argc || parse_long(argv[i + 1], &v) < 0)
            usage();
        if (strcmp(argv[i], "-cores") == 0 && v >= 1)
            g_cores = (int)v;
        else if (strcmp(argv[i], "-ram") == 0 && v >= 1)
            g_ram_mb = v;
        else if (strcmp(argv[i], "-gpu") == 0 && v >= 0)
            g_gpus = (int)v;
        else if (strcmp(argv[i], "-discovery-port") == 0 && v >= 1 && v <= 65535)
            g_discovery_port = (int)v;
        else
            usage();
        i++;
    }
    if (g_cores < 0 || g_ram_mb < 0)
        usage();
}

/* Don't advertise more than the machine has (2B). GPUs are just a counter. */
static void clamp_capacity(void)
{
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu > 0 && g_cores > ncpu) {
        log_line("WARNING: -cores %d is more than this machine has; clamping to %ld.", g_cores, ncpu);
        g_cores = (int)ncpu;
    }
    long pages = sysconf(_SC_PHYS_PAGES), psize = sysconf(_SC_PAGESIZE);
    if (pages > 0 && psize > 0) {
        long phys_mb = (long)((long long)pages * psize / (1024 * 1024));
        if (g_ram_mb > phys_mb) {
            log_line("WARNING: -ram %ldMB is more than this machine has; clamping to %ldMB.",
                     g_ram_mb, phys_mb);
            g_ram_mb = phys_mb;
        }
    }
}

static void setup_privileges(void)
{
    g_is_root = geteuid() == 0;
    if (!g_is_root) {
        log_line("WARNING: not running as root. Jobs will run as the current user without "
                 "privilege dropping (chown and setuid are skipped). Use sudo for isolation.");
        return;
    }
    struct passwd *pw = getpwnam("nobody");
    if (!pw) {
        log_line("ERROR: running as root but there is no 'nobody' user to run jobs as.");
        exit(1);
    }
    g_nobody_uid = pw->pw_uid;
    g_nobody_gid = pw->pw_gid;
    log_line("Running as root; jobs will run as nobody (uid %d, gid %d).",
             (int)g_nobody_uid, (int)g_nobody_gid);
}

/* ---- talking to the scheduler -------------------------------------------- */

static void send_msg(msg_t *m)
{
    if (g_conn.fd >= 0 && (conn_send(&g_conn, m) < 0 || conn_flush_all(&g_conn) < 0)) {
        conn_close(&g_conn);
    }
    msg_free(m);
}

/* Waits up to timeout_ms for one message. */
static msg_t *recv_msg(int timeout_ms)
{
    uint64_t deadline = now_ms() + (uint64_t)timeout_ms;
    for (;;) {
        msg_t *m;
        int r = conn_next(&g_conn, &m);
        if (r == 1)
            return m;
        if (r < 0)
            return NULL;
        uint64_t now = now_ms();
        if (now >= deadline)
            return NULL;
        struct pollfd pfd = {.fd = g_conn.fd, .events = POLLIN};
        if (poll(&pfd, 1, (int)(deadline - now)) <= 0)
            continue;
        if (conn_read(&g_conn) <= 0)
            return NULL;
    }
}

/* Discovery, connect and register. Returns 0 once registered. */
static int connect_and_register(void)
{
    char ip[64];
    int port;
    if (discovery_resolve(g_discovery_port, g_fallback_ip, g_fallback_port, ip, sizeof(ip), &port) < 0)
        return -1;

    log_line("Connecting to scheduler at %s:%d...", ip, port);
    int fd = tcp_connect(ip, port, 3000);
    if (fd < 0) {
        log_line("Could not connect to %s:%d: %s", ip, port, strerror(errno));
        return -1;
    }
    conn_init(&g_conn, fd);

    char host[64] = "";
    gethostname(host, sizeof(host) - 1);
    msg_t *m = msg_new("REGISTER");
    msg_set_long(m, "cores", g_cores);
    msg_set_long(m, "ram", g_ram_mb);
    msg_set_long(m, "gpu", g_gpus);
    msg_set(m, "host", host);
    cache_scan(); /* tell the scheduler what we still have (13) */
    char *cached = cache_hash_list();
    msg_set(m, "cached", cached);
    free(cached);
    send_msg(m);

    msg_t *r = g_conn.fd >= 0 ? recv_msg(5000) : NULL;
    if (!r || !msg_is(r, "REGISTER_OK")) {
        log_line("Scheduler did not accept the registration.");
        msg_free(r);
        conn_close(&g_conn);
        return -1;
    }
    g_id = (int)msg_get_long(r, "id", 0);
    msg_free(r);

    log_set_tag("WORKER %d", g_id);
    log_line("Registered successfully. ID: %d. Capacity: %d Cores, %ldMB RAM, %d GPU.",
             g_id, g_cores, g_ram_mb, g_gpus);
    log_line("Heartbeat active. Waiting for jobs...");
    return 0;
}

static void send_heartbeat(void)
{
    double load[1] = {0};
    int cpu = 0;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (getloadavg(load, 1) == 1 && ncpu > 0) {
        cpu = (int)(load[0] / (double)ncpu * 100.0);
        if (cpu > 100)
            cpu = 100;
    }

    /* Per-job telemetry: "id:rss_kb:peak_kb:faults:cpu_ms;..." */
    buf_t tele = {0};
    for (int i = 0; i < g_njobs; i++) {
        wjob_t *j = g_jobs[i];
        char item[128];
        int n = snprintf(item, sizeof(item), "%s%d:%ld:%ld:%ld:%ld", i ? ";" : "",
                         j->id, j->rss_kb, j->peak_kb, j->faults, j->cpu_ms);
        buf_append(&tele, item, (size_t)n);
    }
    buf_append(&tele, "", 1);

    msg_t *m = msg_new("HEARTBEAT");
    msg_set_long(m, "load", cpu);
    msg_set_long(m, "jobs", g_njobs);
    msg_set(m, "tele", tele.data);
    buf_free(&tele);
    send_msg(m);
}

/* ---- job bookkeeping ------------------------------------------------------ */

static wjob_t *find_job(int id)
{
    for (int i = 0; i < g_njobs; i++)
        if (g_jobs[i]->id == id)
            return g_jobs[i];
    return NULL;
}

static void remove_job(wjob_t *j)
{
    for (int i = 0; i < g_njobs; i++) {
        if (g_jobs[i] == j) {
            g_jobs[i] = g_jobs[--g_njobs];
            break;
        }
    }
    free(j);
}

/* Sends whatever is new in output.log. With all=1 drains it completely. */
static void pump_output(wjob_t *j, int all)
{
    do {
        size_t len;
        char *data = jobrun_read_output(j, 256 * 1024, &len);
        if (!data)
            return;
        msg_t *m = msg_new("JOB_OUTPUT");
        msg_set_long(m, "job", j->id);
        msg_set_long(m, "run", j->run);
        msg_take_blob(m, data, len);
        send_msg(m);
    } while (all);
}

static void report_failure_to_start(int id, int run, const char *why);

int cache_in_use(const char *hash)
{
    for (int i = 0; i < g_njobs; i++)
        if (strcmp(g_jobs[i]->hash, hash) == 0)
            return 1;
    return 0;
}

static void send_cache_state(void)
{
    msg_t *m = msg_new("CACHE_STATE");
    char *list = cache_hash_list();
    msg_set(m, "cached", list);
    free(list);
    send_msg(m);
}

/*
 * Section 13: puts the job's program in the cache (or finds it there) and
 * points the RUN_JOB at the cached copy. Returns -1 if the job can't start
 * yet (cache miss reported, or a corrupted transfer).
 */
static int prepare_binary(msg_t *m, int id, int run)
{
    const char *hash = msg_get(m, "hash");
    if (!hash)
        return 0; /* a PATH command, nothing to cache */
    const char *path = msg_get_str(m, "path");
    const char *base = strrchr(path, '/') ? strrchr(path, '/') + 1 : path;
    char cpath[4096];

    if (m->blob) {
        char got[SHA256_HEX_LEN + 1];
        sha256_hex(m->blob, m->blob_len, got);
        if (strcmp(got, hash) != 0) {
            report_failure_to_start(id, run, "Binary arrived corrupted (SHA-256 mismatch)");
            return -1;
        }
        int evicted;
        if (cache_store(base, hash, m->blob, m->blob_len, cpath, sizeof(cpath), &evicted) == 0) {
            log_line("Job %d: cached %s (%zu bytes, sha256 %.8s...)", id, base, m->blob_len, hash);
            msg_set(m, "local_binary", cpath);
            msg_take_blob(m, NULL, 0);
            if (evicted)
                send_cache_state();
        }
        return 0; /* if storing failed the job still runs from the blob */
    }
    if (msg_get_long(m, "cached", 0)) {
        if (cache_lookup(hash, cpath, sizeof(cpath)) < 0) {
            log_line("Job %d: cached sha256 %.8s... is missing, asking for the full file", id, hash);
            msg_t *miss = msg_new("CACHE_MISS");
            msg_set_long(miss, "job", id);
            msg_set_long(miss, "run", run);
            msg_set(miss, "hash", hash);
            send_msg(miss);
            return -1;
        }
        log_line("Job %d: using cached %s", id, cpath);
        msg_set(m, "local_binary", cpath);
    }
    return 0;
}

static void report_failure_to_start(int id, int run, const char *why)
{
    log_line("[ERROR] Job %d Failed. Diagnosis: %s.", id, why);
    msg_t *m = msg_new("JOB_DONE");
    msg_set_long(m, "job", id);
    msg_set_long(m, "run", run);
    msg_set(m, "result", "FAILED");
    msg_set(m, "reason", why);
    send_msg(m);
}

/* Starts a job. Returns 1 if it kept m for later (caller must not free it). */
static int handle_run_job(msg_t *m)
{
    int id = (int)msg_get_long(m, "job", 0);
    int run = (int)msg_get_long(m, "run", 0);
    wjob_t *old = find_job(id);
    if (old && old->run != run && !old->next_run) {
        /* An earlier run of this job is still being killed (preemption or
         * migration brought it straight back here). Start once it's gone. */
        log_line("Job %d ASSIGNED again; waiting for the previous run to exit first", id);
        old->next_run = m;
        return 1;
    }
    log_line("Job %d ASSIGNED, forking... (%s, %lldMB, %llds)", id, msg_get_str(m, "path"),
             msg_get_long(m, "ram", 0), msg_get_long(m, "time", 0));
    if (old) {
        report_failure_to_start(id, run, "Worker is already running this job");
        return 0;
    }
    if (prepare_binary(m, id, run) < 0)
        return 0;
    wjob_t *j = calloc(1, sizeof(*j));
    if (!j)
        abort();
    if (jobrun_start(j, m) < 0) {
        char why[128];
        snprintf(why, sizeof(why), "Worker could not fork: %s", strerror(errno));
        free(j);
        report_failure_to_start(id, run, why);
        return 0;
    }
    g_jobs = realloc(g_jobs, sizeof(wjob_t *) * (size_t)(g_njobs + 1));
    g_jobs[g_njobs++] = j;
    log_line("Job %d RUNNING (pid %d, dir %s)", j->id, (int)j->pid, j->dir);
    return 0;
}

static void handle_kill_job(msg_t *m)
{
    wjob_t *j = find_job((int)msg_get_long(m, "job", 0));
    if (!j || msg_get_long(m, "run", j->run) != j->run)
        return; /* already finished */
    const char *reason = msg_get_str(m, "reason");
    kill_reason_t why = KILL_CANCEL;
    if (strcmp(reason, "preempt") == 0)
        why = KILL_PREEMPT;
    else if (strcmp(reason, "migrate") == 0)
        why = KILL_MIGRATE;
    log_line("Job %d: kill requested (%s%s%s), sending SIGKILL", j->id, reason,
             msg_get(m, "by") ? " by " : "", msg_get_str(m, "by"));
    jobrun_kill(j, why, msg_get(m, "by"));
}

/* Called for each job that has exited: final output, report, clean up. */
static void finish_job(wjob_t *j)
{
    pump_output(j, 1);
    char reason[300];
    int ok = jobrun_diagnose(j, reason, sizeof(reason));

    if (j->killed == KILL_CANCEL || j->killed == KILL_PREEMPT || j->killed == KILL_MIGRATE)
        log_line("Job %d killed and reaped (%s).", j->id, reason);
    else if (ok)
        log_line("Job %d COMPLETED (exit code 0, peak RAM %ldMB, CPU %.1fs)", j->id,
                 j->peak_kb / 1024, (double)j->cpu_ms / 1000.0);
    else
        log_line("[ERROR] Job %d Failed. Diagnosis: %s.", j->id, reason);

    static const char *kill_names[] = {
        [KILL_CANCEL] = "cancel", [KILL_PREEMPT] = "preempt", [KILL_MIGRATE] = "migrate",
    };
    int requested = j->killed == KILL_CANCEL || j->killed == KILL_PREEMPT ||
                    j->killed == KILL_MIGRATE;

    msg_t *m = msg_new("JOB_DONE");
    msg_set_long(m, "job", j->id);
    msg_set_long(m, "run", j->run);
    /* KILLED = stopped because the scheduler asked; the scheduler decides
     * what happens next (cancelled, requeued ...). */
    msg_set(m, "result", requested ? "KILLED" : ok ? "COMPLETED" : "FAILED");
    if (requested)
        msg_set(m, "killed", kill_names[j->killed]);
    msg_set(m, "reason", ok ? "" : reason);
    if (WIFEXITED(j->status))
        msg_set_long(m, "exit_code", WEXITSTATUS(j->status));
    if (WIFSIGNALED(j->status))
        msg_set_long(m, "signal", WTERMSIG(j->status));
    msg_set_long(m, "peak_kb", j->peak_kb);
    msg_set_long(m, "faults", j->faults);
    msg_set_long(m, "cpu_ms", j->cpu_ms);
    send_msg(m);

    /* Output and report are out; the job directory can go. */
    jobrun_cleanup(j);
    msg_t *next = j->next_run;
    remove_job(j);
    if (next && !handle_run_job(next))
        msg_free(next);
}

static void tick_jobs(int sample)
{
    uint64_t now = now_ms();
    for (int i = 0; i < g_njobs;) {
        wjob_t *j = g_jobs[i];
        if (jobrun_reap(j)) {
            finish_job(j); /* removes j; g_jobs[i] is now another job */
            continue;
        }
        if (j->killed == KILL_NONE && now - j->start_ms >= (uint64_t)j->time_s * 1000) {
            log_line("Job %d: wall-clock limit of %ds reached, sending SIGKILL", j->id, j->time_s);
            jobrun_kill(j, KILL_TIMEOUT, NULL);
        }
        if (sample) {
            jobrun_sample(j);
            pump_output(j, 0);
        }
        i++;
    }
}

/* Kills every job and waits for them (used when the scheduler is lost). */
static void kill_all_jobs(void)
{
    if (g_njobs)
        log_line("Killing %d running job(s).", g_njobs);
    while (g_njobs > 0) {
        wjob_t *j = g_jobs[0];
        jobrun_kill(j, KILL_SHUTDOWN, NULL);
        waitpid(j->pid, &j->status, 0);
        if (j->err_fd >= 0)
            close(j->err_fd);
        jobrun_cleanup(j);
        msg_free(j->next_run);
        remove_job(j);
    }
}

/* ---- local admin commands (Section 9) ------------------------------------- */

static void start_drain(void)
{
    if (g_draining) {
        log_line("Already draining; %d job(s) still running.", g_njobs);
        return;
    }
    g_draining = 1;
    log_line("Draining initiated locally. %d job(s) still running; no new jobs will be accepted.",
             g_njobs);
    send_msg(msg_new("DRAIN"));
}

static void handle_stdin(void)
{
    static char line[256];
    static size_t used;
    ssize_t n = read(STDIN_FILENO, line + used, sizeof(line) - 1 - used);
    if (n <= 0) {
        g_stdin_open = 0; /* e.g. started with </dev/null */
        return;
    }
    used += (size_t)n;
    char *nl;
    while ((nl = memchr(line, '\n', used)) != NULL) {
        *nl = '\0';
        char *cmd = line;
        while (*cmd == ' ' || *cmd == '\t')
            cmd++;
        if (strcmp(cmd, "drain") == 0) {
            start_drain();
        } else if (strcmp(cmd, "jobs") == 0) {
            log_line("%d job(s) running.", g_njobs);
            for (int i = 0; i < g_njobs; i++)
                log_line("  Job %d pid %d, %.1fs elapsed", g_jobs[i]->id, (int)g_jobs[i]->pid,
                         (double)(now_ms() - g_jobs[i]->start_ms) / 1000.0);
        } else if (*cmd) {
            log_line("Unknown command '%s'. Commands: drain, jobs", cmd);
        }
        size_t consumed = (size_t)(nl - line) + 1;
        memmove(line, nl + 1, used - consumed);
        used -= consumed;
    }
    if (used == sizeof(line) - 1)
        used = 0; /* overlong line, drop it */
}

/* ---- main ---------------------------------------------------------------- */

int main(int argc, char **argv)
{
    log_set_tag("WORKER");
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL); /* no SA_RESTART: interrupt poll() */

    parse_args(argc, argv);
    clamp_capacity();
    setup_privileges();
    cache_init();

    if (connect_and_register() < 0) {
        fprintf(stderr, "worker: could not find or reach a scheduler. Exiting.\n");
        return 1;
    }

    uint64_t last_hb = now_ms(), last_sample = now_ms();
    send_heartbeat();

    for (;;) {
        if (g_sigint) {
            g_sigint = 0;
            start_drain();
        }
        if (g_draining && g_njobs == 0) {
            log_line("Drain complete (0 active jobs). Exiting gracefully.");
            conn_flush_all(&g_conn);
            conn_close(&g_conn);
            return 0;
        }

        if (g_conn.fd < 0) {
            log_set_tag("WORKER");
            log_line("Lost connection to the scheduler.");
            kill_all_jobs();
            if (g_draining) {
                log_line("Was draining; exiting.");
                return 0;
            }
            while (connect_and_register() < 0) {
                if (g_sigint) {
                    log_line("Interrupted while reconnecting; exiting.");
                    return 0;
                }
                log_line("Retrying in 2 seconds...");
                sleep(2);
            }
            send_heartbeat();
            last_hb = now_ms();
        }

        struct pollfd pfds[2] = {
            {.fd = g_conn.fd, .events = POLLIN},
            {.fd = g_stdin_open ? STDIN_FILENO : -1, .events = POLLIN},
        };
        int r = poll(pfds, 2, TICK_MS);
        if (r > 0 && (pfds[0].revents & (POLLIN | POLLHUP | POLLERR))) {
            if (conn_read(&g_conn) <= 0) {
                conn_close(&g_conn);
            } else {
                msg_t *m;
                int got;
                while ((got = conn_next(&g_conn, &m)) == 1) {
                    if (msg_is(m, "RUN_JOB") && handle_run_job(m))
                        continue; /* kept for later */
                    if (msg_is(m, "KILL_JOB"))
                        handle_kill_job(m);
                    msg_free(m);
                }
                if (got < 0)
                    conn_close(&g_conn);
            }
        }
        if (r > 0 && (pfds[1].revents & (POLLIN | POLLHUP)))
            handle_stdin();

        uint64_t now = now_ms();
        int sample = now - last_sample >= OUTPUT_POLL_MS;
        if (sample)
            last_sample = now;
        tick_jobs(sample);

        if (g_conn.fd >= 0 && now - last_hb >= HEARTBEAT_INTERVAL_MS) {
            send_heartbeat();
            last_hb = now;
        }
    }
}
