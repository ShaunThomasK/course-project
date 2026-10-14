/*
 * client.c - the interactive client (Section 3).
 *
 *   ./client [<scheduler_ip> <scheduler_port>] [-user <username>]
 *            [-discovery-port <port>]
 *
 * One poll() loop over stdin and the scheduler socket, so [Event] lines
 * appear even while the user is idle at the prompt. Requests are answered
 * asynchronously; the prompt comes back once the answer arrives.
 */
#include "../common/common.h"
#include "../common/discovery.h"
#include "../common/proto.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROMPT "client> "
#define MAX_TOKENS 256

typedef enum {
    IDLE,          /* at the prompt */
    WAIT_REPLY,    /* sent a request, waiting for REPLY */
    WAIT_SUBMIT,   /* sent SUBMIT, waiting for REPLY */
    WAIT_LOGS      /* printing LOG_DATA until LOG_END */
} wait_t;

static conn_t g_conn = {.fd = -1};
static char   g_user[USERNAME_MAX];
static int    g_discovery_port = DEFAULT_DISCOVERY_PORT;
static const char *g_fallback_ip;
static int    g_fallback_port;

static wait_t g_wait = IDLE;
static int    g_logs_job;      /* job whose logs we're printing */
static int    g_logs_follow;
static int    g_logs_newline;  /* last log byte printed was '\n' */
static int    g_tty;

static volatile sig_atomic_t g_sigint;

static void on_sigint(int sig)
{
    (void)sig;
    g_sigint = 1;
}

static void prompt(void)
{
    fputs(PROMPT, stdout);
    fflush(stdout);
}

/* Prints a line that arrived on its own, without mangling the prompt. */
static void print_async(const char *prefix, const char *text)
{
    if (g_wait == IDLE && g_tty)
        fputs("\r\033[K", stdout);
    else if (g_wait == WAIT_LOGS && !g_logs_newline)
        fputc('\n', stdout), g_logs_newline = 1;
    printf("%s%s\n", prefix, text);
    if (g_wait == IDLE)
        prompt();
    else
        fflush(stdout);
}

static void back_to_prompt(void)
{
    g_wait = IDLE;
    g_logs_job = 0;
    g_logs_follow = 0;
    putchar('\n');
    prompt();
}

/* ---- connection ---------------------------------------------------------- */

static int send_msg(msg_t *m)
{
    int r = -1;
    if (g_conn.fd >= 0)
        r = (conn_send(&g_conn, m) < 0 || conn_flush_all(&g_conn) < 0) ? -1 : 0;
    msg_free(m);
    return r;
}

static msg_t *recv_msg(int timeout_ms)
{
    uint64_t deadline = now_ms() + (uint64_t)timeout_ms;
    for (;;) {
        msg_t *m;
        int r = conn_next(&g_conn, &m);
        if (r == 1)
            return m;
        if (r < 0 || now_ms() >= deadline)
            return NULL;
        struct pollfd pfd = {.fd = g_conn.fd, .events = POLLIN};
        if (poll(&pfd, 1, (int)(deadline - now_ms())) > 0 && conn_read(&g_conn) <= 0)
            return NULL;
    }
}

static int connect_scheduler(void)
{
    char ip[64];
    int port;
    if (discovery_resolve(g_discovery_port, g_fallback_ip, g_fallback_port, ip, sizeof(ip), &port) < 0)
        return -1;
    int fd = tcp_connect(ip, port, 3000);
    if (fd < 0) {
        printf("[Client] Could not connect to %s:%d: %s\n", ip, port, strerror(errno));
        return -1;
    }
    conn_init(&g_conn, fd);
    return 0;
}

static int login(void)
{
    msg_t *m = msg_new("LOGIN");
    msg_set(m, "user", g_user);
    if (send_msg(m) < 0)
        return -1;
    msg_t *r = recv_msg(5000);
    int ok = r && msg_is(r, "LOGIN_OK");
    if (r && !ok)
        printf("[Client] Login refused: %s\n", msg_get_str(r, "text"));
    msg_free(r);
    return ok ? 0 : -1;
}

static void ask_username(void)
{
    /* Unbuffered, so fgets() never swallows lines meant for the prompt
     * loop, which reads stdin with read(). */
    setvbuf(stdin, NULL, _IONBF, 0);
    char line[256];
    for (;;) {
        fputs("Username: ", stdout);
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) {
            putchar('\n');
            exit(0);
        }
        line[strcspn(line, "\r\n")] = '\0';
        if (username_valid(line)) {
            str_copy(g_user, line, sizeof(g_user));
            return;
        }
        printf("Invalid username: use 1-31 letters, digits, '_', '-' or '.'.\n");
    }
}

/* After a dropped connection: find the scheduler again and log back in. */
static void reconnect(void)
{
    conn_close(&g_conn);
    if (g_wait != IDLE && g_tty)
        putchar('\n');
    printf("[Client] Lost connection to the scheduler. Reconnecting...\n");
    for (;;) {
        if (connect_scheduler() == 0 && login() == 0)
            break;
        conn_close(&g_conn);
        printf("[Client] Scheduler unreachable, retrying in 2 seconds (Ctrl+C to quit)...\n");
        for (int i = 0; i < 20 && !g_sigint; i++)
            usleep(100000);
        if (g_sigint)
            exit(1);
    }
    printf("[Client] Reconnected as %s.\n", g_user);
    g_wait = IDLE;
    g_logs_job = 0;
    prompt();
}

/* ---- commands ------------------------------------------------------------- */

/* Splits a line on whitespace; "double quotes" group words. */
static int tokenize(char *line, char **tok, int max)
{
    int n = 0;
    char *p = line;
    while (*p && n < max) {
        while (*p == ' ' || *p == '\t')
            p++;
        if (!*p)
            break;
        if (*p == '"') {
            tok[n++] = ++p;
            while (*p && *p != '"')
                p++;
        } else {
            tok[n++] = p;
            while (*p && *p != ' ' && *p != '\t')
                p++;
        }
        if (*p)
            *p++ = '\0';
    }
    return n;
}

static int parse_job_id(const char *s, long *id)
{
    if (!s || parse_long(s, id) < 0 || *id < 1) {
        printf("[Client] Expected a job id (a positive number).\n");
        return -1;
    }
    return 0;
}

static void cmd_help(void)
{
    printf("Commands:\n"
           "  nodes                       list workers\n"
           "  submit <binary> [args...] -cores N -ram MB -time SEC -prio 1-3 [-gpu N]\n"
           "         [-depends-on 1,2,g3] [-group N] [-preemptible] [-every SEC]\n"
           "                              submit a job (-every registers a cron template)\n"
           "  status <job_id>             job state, worker and resource usage\n"
           "  logs <job_id> [-f]          job output (-f follows it; Ctrl+C stops)\n"
           "  cancel <job_id>             cancel a job\n"
           "  migrate <job_id>            move a running job to another worker\n"
           "  crons                       list cron templates\n"
           "  cancel-cron <template_id>   stop a cron template's future copies\n"
           "  help                        this text\n"
           "  exit | quit                 leave the client\n");
}

/* Returns 1 if a request was sent and we now wait for an answer. */
static int cmd_submit(char **tok, int n)
{
    if (n < 2) {
        printf("[Client] Usage: submit <binary> [args...] -cores N -ram MB -time SEC -prio 1-3 [-gpu N]\n");
        return 0;
    }
    /* The scheduler reads the file itself, from its own working directory,
     * so send an absolute path. A bare name is a PATH command. */
    char path[PATH_MAX];
    if (strchr(tok[1], '/')) {
        if (!realpath(tok[1], path)) {
            printf("[Client] Cannot find %s: %s\n", tok[1], strerror(errno));
            return 0;
        }
    } else {
        str_copy(path, tok[1], sizeof(path));
    }

    long cores = -1, ram = -1, tlim = -1, prio = -1, gpu = 0;
    struct { const char *flag; long *dst; } flags[] = {
        {"-cores", &cores}, {"-ram", &ram}, {"-time", &tlim}, {"-prio", &prio}, {"-gpu", &gpu},
    };
    msg_t *m = msg_new("SUBMIT");
    int argc = 0;
    for (int i = 2; i < n; i++) {
        int matched = 0;
        for (size_t f = 0; f < sizeof(flags) / sizeof(flags[0]); f++) {
            if (strcmp(tok[i], flags[f].flag) != 0)
                continue;
            matched = 1;
            if (i + 1 >= n || parse_long(tok[i + 1], flags[f].dst) < 0) {
                printf("[Client] %s needs a number.\n", tok[i]);
                msg_free(m);
                return 0;
            }
            i++;
        }
        if (matched)
            continue;
        if (strcmp(tok[i], "-depends-on") == 0 || strcmp(tok[i], "-group") == 0 ||
            strcmp(tok[i], "-every") == 0) {
            if (i + 1 >= n) {
                printf("[Client] %s needs a value.\n", tok[i]);
                msg_free(m);
                return 0;
            }
            msg_set(m, tok[i] + 1, tok[i + 1]);
            i++;
        } else if (strcmp(tok[i], "-preemptible") == 0) {
            msg_set(m, "preemptible", "1");
        } else {
            /* Anything else is an argument for the program. */
            char key[16];
            snprintf(key, sizeof(key), "arg%d", argc++);
            msg_set(m, key, tok[i]);
        }
    }

    char missing[64] = "";
    if (cores < 0) strcat(missing, " -cores");
    if (ram < 0)   strcat(missing, " -ram");
    if (tlim < 0)  strcat(missing, " -time");
    if (prio < 0)  strcat(missing, " -prio");
    if (*missing) {
        printf("[Client] Missing required flag(s):%s\n", missing);
        msg_free(m);
        return 0;
    }
    if (cores < 1 || ram < 1 || tlim < 1 || prio < 1 || prio > 3 || gpu < 0) {
        printf("[Client] Need -cores >= 1, -ram >= 1, -time >= 1, -prio 1-3, -gpu >= 0.\n");
        msg_free(m);
        return 0;
    }
    msg_set(m, "path", path);
    msg_set_long(m, "argc", argc);
    msg_set_long(m, "cores", cores);
    msg_set_long(m, "ram", ram);
    msg_set_long(m, "time", tlim);
    msg_set_long(m, "prio", prio);
    msg_set_long(m, "gpu", gpu);
    send_msg(m);
    g_wait = WAIT_SUBMIT;
    return 1;
}

static int run_command(char *line)
{
    char *tok[MAX_TOKENS];
    int n = tokenize(line, tok, MAX_TOKENS);
    if (n == 0)
        return 0;
    long id;

    if (strcmp(tok[0], "help") == 0) {
        cmd_help();
    } else if (strcmp(tok[0], "exit") == 0 || strcmp(tok[0], "quit") == 0) {
        exit(0);
    } else if (strcmp(tok[0], "nodes") == 0) {
        send_msg(msg_new("NODES"));
        g_wait = WAIT_REPLY;
        return 1;
    } else if (strcmp(tok[0], "submit") == 0) {
        return cmd_submit(tok, n);
    } else if (strcmp(tok[0], "status") == 0 || strcmp(tok[0], "cancel") == 0 ||
               strcmp(tok[0], "migrate") == 0) {
        if (parse_job_id(n > 1 ? tok[1] : NULL, &id) < 0)
            return 0;
        msg_t *m = msg_new(strcmp(tok[0], "status") == 0   ? "STATUS"
                           : strcmp(tok[0], "cancel") == 0 ? "CANCEL"
                                                           : "MIGRATE");
        msg_set_long(m, "job", id);
        send_msg(m);
        g_wait = WAIT_REPLY;
        return 1;
    } else if (strcmp(tok[0], "crons") == 0) {
        send_msg(msg_new("CRONS"));
        g_wait = WAIT_REPLY;
        return 1;
    } else if (strcmp(tok[0], "cancel-cron") == 0) {
        if (n < 2 || parse_long(tok[1], &id) < 0 || id < 1) {
            printf("[Client] Usage: cancel-cron <template_id>\n");
            return 0;
        }
        msg_t *m = msg_new("CANCEL_CRON");
        msg_set_long(m, "cron", id);
        send_msg(m);
        g_wait = WAIT_REPLY;
        return 1;
    } else if (strcmp(tok[0], "logs") == 0) {
        if (parse_job_id(n > 1 ? tok[1] : NULL, &id) < 0)
            return 0;
        int follow = n > 2 && strcmp(tok[2], "-f") == 0;
        msg_t *m = msg_new("LOGS");
        msg_set_long(m, "job", id);
        msg_set_long(m, "follow", follow);
        send_msg(m);
        g_wait = WAIT_LOGS;
        g_logs_job = (int)id;
        g_logs_follow = follow;
        g_logs_newline = 1;
        return 1;
    } else {
        printf("[Client] Unknown command '%s'. Type 'help' for the list.\n", tok[0]);
    }
    return 0;
}

/* ---- messages from the scheduler ------------------------------------------ */

static void handle_msg(msg_t *m)
{
    if (msg_is(m, "EVENT")) {
        print_async("[Event] ", msg_get_str(m, "text"));
        return;
    }

    if (msg_is(m, "LOG_DATA")) {
        if (g_wait != WAIT_LOGS || msg_get_long(m, "job", 0) != g_logs_job)
            return; /* in flight after we stopped following */
        long long trunc = msg_get_long(m, "truncated", 0);
        if (trunc)
            printf("[... %lld earlier bytes no longer kept ...]\n", trunc);
        if (m->blob_len) {
            fwrite(m->blob, 1, m->blob_len, stdout);
            g_logs_newline = m->blob[m->blob_len - 1] == '\n';
        }
        fflush(stdout);
        return;
    }

    if (msg_is(m, "LOG_END")) {
        if (g_wait != WAIT_LOGS || msg_get_long(m, "job", 0) != g_logs_job)
            return;
        if (!g_logs_newline)
            putchar('\n');
        if (g_logs_follow)
            printf("-- Job %d finished (%s) --\n", g_logs_job, msg_get_str(m, "state"));
        g_wait = IDLE;
        g_logs_job = 0;
        putchar('\n');
        prompt();
        return;
    }

    if (msg_is(m, "REPLY")) {
        int ok = (int)msg_get_long(m, "ok", 0);
        if (g_wait == WAIT_SUBMIT) {
            if (ok && msg_get(m, "cron")) {
                printf("[Client] Cron template registered (every %llds).\n", msg_get_long(m, "every", 0));
                printf("[Client] Template ID assigned: %lld\n", msg_get_long(m, "cron", 0));
            } else if (ok) {
                const char *state = msg_get_str(m, "state");
                if (strcmp(state, "WAITING") == 0)
                    printf("[Client] Job submitted (WAITING on %s).\n", msg_get_str(m, "waiting_on"));
                else if (strcmp(state, "CANCELLED") == 0)
                    printf("[Client] Job submitted, but CANCELLED right away: %s.\n",
                           msg_get_str(m, "note"));
                else
                    printf("[Client] Job submitted.\n");
                printf("[Client] Job ID assigned: %lld\n", msg_get_long(m, "job", 0));
            } else if (strcmp(msg_get_str(m, "code"), "QUEUE_FULL") == 0) {
                printf("[Client] The job queue is full; the job was not submitted. Try again later.\n");
            } else {
                printf("[Client] Submit failed: %s\n", msg_get_str(m, "text"));
            }
        } else if (g_wait == WAIT_REPLY || g_wait == WAIT_LOGS) {
            /* WAIT_LOGS gets a REPLY only when the request failed. */
            printf("%s%s\n", ok ? "" : "[Client] Error: ", msg_get_str(m, "text"));
        } else {
            return;
        }
        back_to_prompt();
    }
}

/* Lines read from stdin but not run yet. Typing ahead is fine: lines wait
 * here until the previous command has its answer. */
static char   g_line[8192];
static size_t g_line_used;
static int    g_stdin_eof;

static void run_pending_lines(void)
{
    char *nl;
    while (g_wait == IDLE && (nl = memchr(g_line, '\n', g_line_used)) != NULL) {
        *nl = '\0';
        size_t consumed = (size_t)(nl - g_line) + 1;
        char cmd[sizeof(g_line)];
        memcpy(cmd, g_line, consumed);
        memmove(g_line, nl + 1, g_line_used - consumed);
        g_line_used -= consumed;
        if (!run_command(cmd))
            prompt();
    }
    if (g_wait == IDLE && g_stdin_eof) {
        putchar('\n');
        exit(0); /* Ctrl+D, or the end of a piped script */
    }
}

static void handle_stdin(void)
{
    ssize_t n = read(STDIN_FILENO, g_line + g_line_used, sizeof(g_line) - 1 - g_line_used);
    if (n <= 0)
        g_stdin_eof = 1;
    else
        g_line_used += (size_t)n;
    if (g_line_used == sizeof(g_line) - 1 && !memchr(g_line, '\n', g_line_used))
        g_line_used = 0; /* overlong line, drop it */
    run_pending_lines();
}

static void handle_sigint(void)
{
    g_sigint = 0;
    if (g_wait == WAIT_LOGS && g_logs_follow) {
        msg_t *m = msg_new("UNFOLLOW");
        msg_set_long(m, "job", g_logs_job);
        send_msg(m);
        printf("%s[Client] Stopped following Job %d.\n", g_logs_newline ? "" : "\n", g_logs_job);
        g_wait = IDLE;
        g_logs_job = 0;
        prompt();
    } else if (g_wait != IDLE) {
        printf("\n[Client] Stopped waiting for the answer.\n");
        g_wait = IDLE;
        prompt();
    } else {
        printf("\n(type 'exit' or press Ctrl+D to quit)\n");
        prompt();
    }
    run_pending_lines();
}

/* ---- main ---------------------------------------------------------------- */

static void usage(void)
{
    fprintf(stderr, "Usage: ./client [<scheduler_ip> <scheduler_port>] [-user <username>] "
                    "[-discovery-port <port>]\n");
    exit(1);
}

int main(int argc, char **argv)
{
    log_set_tag("Client");
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL); /* no SA_RESTART: interrupt poll() */
    g_tty = isatty(STDOUT_FILENO);

    int i = 1;
    long v;
    if (argc >= 3 && argv[1][0] != '-') {
        g_fallback_ip = argv[1];
        if (parse_long(argv[2], &v) < 0 || v < 1 || v > 65535)
            usage();
        g_fallback_port = (int)v;
        i = 3;
    }
    const char *user_arg = NULL;
    for (; i < argc; i++) {
        if (strcmp(argv[i], "-user") == 0 && i + 1 < argc) {
            user_arg = argv[++i];
        } else if (strcmp(argv[i], "-discovery-port") == 0 && i + 1 < argc) {
            if (parse_long(argv[++i], &v) < 0 || v < 1 || v > 65535)
                usage();
            g_discovery_port = (int)v;
        } else {
            usage();
        }
    }

    if (connect_scheduler() < 0) {
        fprintf(stderr, "[Client] Could not find or reach a scheduler. Exiting.\n");
        return 1;
    }
    printf("[Client] Connected to Scheduler.\n");

    if (user_arg && username_valid(user_arg)) {
        str_copy(g_user, user_arg, sizeof(g_user));
    } else {
        if (user_arg)
            printf("Invalid username '%s': use 1-31 letters, digits, '_', '-' or '.'.\n", user_arg);
        ask_username();
    }
    if (login() < 0) {
        fprintf(stderr, "[Client] Login failed.\n");
        return 1;
    }
    printf("Logged in as %s.\n\n", g_user);
    prompt();

    for (;;) {
        struct pollfd pfds[2] = {
            /* A full line buffer stops reading until queued commands run. */
            {.fd = (g_stdin_eof || g_line_used >= sizeof(g_line) - 1) ? -1 : STDIN_FILENO,
             .events = POLLIN},
            {.fd = g_conn.fd, .events = POLLIN},
        };
        int r = poll(pfds, 2, -1);
        if (g_sigint)
            handle_sigint();
        if (r <= 0)
            continue;

        if (pfds[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            if (conn_read(&g_conn) <= 0) {
                reconnect();
                continue;
            }
            msg_t *m;
            int got;
            while ((got = conn_next(&g_conn, &m)) == 1) {
                handle_msg(m);
                msg_free(m);
            }
            if (got < 0) {
                reconnect();
                continue;
            }
            run_pending_lines();
        }
        if (pfds[0].revents & (POLLIN | POLLHUP))
            handle_stdin();
    }
}
