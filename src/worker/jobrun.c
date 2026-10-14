/*
 * jobrun.c - running one job as an isolated, limited child process
 * (Section 5).
 */
#define _GNU_SOURCE /* nftw() and strsignal() on glibc */
#include "worker.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/prctl.h>
#endif
#ifdef __APPLE__
#include <libproc.h>
#include <mach/mach_time.h>
#endif

int   g_is_root;
uid_t g_nobody_uid;
gid_t g_nobody_gid;

/* ---- the child side ------------------------------------------------------- */

/* Reports a failed setup step to the parent and exits without running the job. */
static void child_fail(int err_fd, const char *step, int code)
{
    char msg[256];
    int n = snprintf(msg, sizeof(msg), "%s:%s", step, strerror(errno));
    if (write(err_fd, msg, (size_t)n) < 0) {
        /* nothing else we can do */
    }
    _exit(code);
}

static int write_file(const char *path, const char *data, size_t len, mode_t mode)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0)
        return -1;
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, data + done, len - done);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0) {
            close(fd);
            return -1;
        }
        done += (size_t)n;
    }
    return close(fd);
}

static int copy_file(const char *from, const char *to)
{
    int in = open(from, O_RDONLY);
    if (in < 0)
        return -1;
    int out = open(to, O_WRONLY | O_CREAT | O_TRUNC, 0755);
    if (out < 0) {
        close(in);
        return -1;
    }
    char buf[65536];
    ssize_t n;
    int rc = 0;
    while ((n = read(in, buf, sizeof(buf))) > 0) {
        if (write(out, buf, (size_t)n) != n) {
            rc = -1;
            break;
        }
    }
    if (n < 0)
        rc = -1;
    close(in);
    if (close(out) < 0)
        rc = -1;
    return rc;
}

static void chown_or_fail(const char *path, int err_fd)
{
    if (chown(path, g_nobody_uid, g_nobody_gid) < 0)
        child_fail(err_fd, "chown", 126);
}

static void run_child(wjob_t *j, const msg_t *run, int err_fd, pid_t parent,
                      const char *ckpt_data, size_t ckpt_len)
{
    /* Own process group: Ctrl+C on the worker's terminal must not reach the
     * job (draining lets jobs finish), and a kill reaches its children. */
    setpgid(0, 0);
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    signal(SIGPIPE, SIG_DFL);
    signal(SIGCHLD, SIG_DFL);
    sigset_t none;
    sigemptyset(&none);
    sigprocmask(SIG_SETMASK, &none, NULL);

    const char *path = msg_get_str(run, "path");
    const char *base = strrchr(path, '/') ? strrchr(path, '/') + 1 : path;
    /* The program comes from the cache (13), or straight from the message
     * if it couldn't be cached. */
    const char *cached = msg_get(run, "local_binary");
    int shipped = cached || (msg_get_long(run, "binary", 0) && run->blob);

    /* 1. Own working directory, with the binary and output.log inside. */
    if (mkdir(j->dir, 0755) < 0)
        child_fail(err_fd, "mkdir", 126);
    char file[256];
    if (shipped) {
        snprintf(file, sizeof(file), "%s/%s", j->dir, base);
        if (cached ? copy_file(cached, file) < 0
                   : write_file(file, run->blob, run->blob_len, 0755) < 0)
            child_fail(err_fd, "copy binary", 126);
    }
    char ckpt[256];
    snprintf(ckpt, sizeof(ckpt), "%s/checkpoint", j->dir);
    if (mkdir(ckpt, 0755) < 0)
        child_fail(err_fd, "mkdir checkpoint", 126);
    char state[300];
    snprintf(state, sizeof(state), "%s/state", ckpt);
    if (ckpt_data) {
        /* Resuming: put the saved checkpoint where the job looks for it (14). */
        if (write_file(state, ckpt_data, ckpt_len, 0644) < 0)
            child_fail(err_fd, "restore checkpoint", 126);
    }
    char logf[256];
    snprintf(logf, sizeof(logf), "%s/output.log", j->dir);
    int out = open(logf, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (out < 0)
        child_fail(err_fd, "open output.log", 126);
    if (g_is_root) {
        chown_or_fail(j->dir, err_fd);
        chown_or_fail(ckpt, err_fd);
        chown_or_fail(logf, err_fd);
        if (ckpt_data)
            chown_or_fail(state, err_fd);
        if (shipped)
            chown_or_fail(file, err_fd);
    }
    if (chdir(j->dir) < 0)
        child_fail(err_fd, "chdir", 126);

    /* 5B: nothing the job prints reaches the worker's terminal. */
    int devnull = open("/dev/null", O_RDONLY);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        close(devnull);
    }
    dup2(out, STDOUT_FILENO);
    dup2(out, STDERR_FILENO);
    close(out);

    /* 2. Drop privileges: group first, then user. */
    if (g_is_root) {
        if (setgroups(0, NULL) < 0)
            child_fail(err_fd, "setgroups", 126);
        if (setgid(g_nobody_gid) < 0)
            child_fail(err_fd, "setgid", 126);
        if (setuid(g_nobody_uid) < 0)
            child_fail(err_fd, "setuid", 126);
        if (getuid() != g_nobody_uid || geteuid() != g_nobody_uid || setuid(0) == 0) {
            errno = EPERM;
            child_fail(err_fd, "privilege drop check", 126);
        }
    }
#ifdef __linux__
    /* Die with the worker even if it is SIGKILLed. Set after setuid(),
     * because changing credentials clears it. */
    prctl(PR_SET_PDEATHSIG, SIGKILL);
    if (getppid() != parent)
        _exit(126);
#else
    (void)parent;
#endif

    /* 3. CPU time. SIGXCPU at the soft limit, SIGKILL a second later. */
    struct rlimit rl;
    rl.rlim_cur = (rlim_t)j->time_s;
    rl.rlim_max = (rlim_t)j->time_s + 1;
    if (setrlimit(RLIMIT_CPU, &rl) < 0)
        child_fail(err_fd, "setrlimit(RLIMIT_CPU)", 126);

    /* 4. Address space (ignored by macOS; the parent watches RSS there). */
    rl.rlim_cur = rl.rlim_max = (rlim_t)j->ram_mb * 1024 * 1024;
    if (setrlimit(RLIMIT_AS, &rl) < 0 && errno != EINVAL)
        child_fail(err_fd, "setrlimit(RLIMIT_AS)", 126);

    /* 5. Fork bombs. Counts every process of the job user, see README. */
    rl.rlim_cur = rl.rlim_max = JOB_NPROC_LIMIT;
    if (setrlimit(RLIMIT_NPROC, &rl) < 0)
        child_fail(err_fd, "setrlimit(RLIMIT_NPROC)", 126);

    /* No core dumps: they fill the job directory and delay the report. */
    rl.rlim_cur = rl.rlim_max = 0;
    setrlimit(RLIMIT_CORE, &rl);

    setenv("OSN_CHECKPOINT_DIR", ckpt, 1);
    char idbuf[16];
    snprintf(idbuf, sizeof(idbuf), "%d", j->id);
    setenv("OSN_JOB_ID", idbuf, 1);

    int argc = (int)msg_get_long(run, "argc", 0);
    char **argv = calloc((size_t)argc + 2, sizeof(char *));
    char prog[300];
    if (shipped)
        snprintf(prog, sizeof(prog), "./%s", base);
    else
        snprintf(prog, sizeof(prog), "%s", path);
    argv[0] = (char *)base;
    for (int i = 0; i < argc; i++) {
        char key[16];
        snprintf(key, sizeof(key), "arg%d", i);
        argv[i + 1] = (char *)msg_get_str(run, key);
    }

    execvp(prog, argv);
    int e = errno;
    fprintf(stderr, "osn worker: cannot execute %s: %s\n", prog, strerror(e));
    errno = e;
    child_fail(err_fd, "exec", 127);
}

/* ---- the worker side ------------------------------------------------------ */

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

void jobrun_cleanup(wjob_t *j)
{
    nftw(j->dir, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
}

int jobrun_start(wjob_t *j, const msg_t *run, const char *ckpt, size_t ckpt_len)
{
    j->id = (int)msg_get_long(run, "job", 0);
    j->run = (int)msg_get_long(run, "run", 0);
    if (msg_get(run, "local_binary"))
        str_copy(j->hash, msg_get_str(run, "hash"), sizeof(j->hash));
    j->time_s = (int)msg_get_long(run, "time", 1);
    j->ram_mb = msg_get_long(run, "ram", 1);
    snprintf(j->dir, sizeof(j->dir), "/tmp/osn_job_%d", j->id);
    jobrun_cleanup(j); /* leftovers from an earlier attempt */

    int p[2];
    if (pipe(p) < 0)
        return -1;
    fcntl(p[0], F_SETFD, FD_CLOEXEC);
    fcntl(p[1], F_SETFD, FD_CLOEXEC);

    pid_t parent = getpid();
    pid_t pid = fork();
    if (pid < 0) {
        int e = errno;
        close(p[0]);
        close(p[1]);
        errno = e;
        return -1;
    }
    if (pid == 0) {
        close(p[0]);
        run_child(j, run, p[1], parent, ckpt, ckpt_len);
        _exit(127); /* not reached */
    }
    close(p[1]);
    setpgid(pid, pid); /* same as the child does; whoever runs first wins */
    fcntl(p[0], F_SETFL, O_NONBLOCK);
    if (ckpt) {
        /* Remember what we restored so it isn't reported as a new checkpoint. */
        j->ckpt = malloc(ckpt_len ? ckpt_len : 1);
        memcpy(j->ckpt, ckpt, ckpt_len);
        j->ckpt_len = ckpt_len;
    }
    j->pid = pid;
    j->err_fd = p[0];
    j->start_ms = now_ms();
    return 0;
}

void jobrun_kill(wjob_t *j, kill_reason_t why, const char *by)
{
    if (j->exited)
        return;
    if (j->killed == KILL_NONE) {
        j->killed = why;
        str_copy(j->killed_by, by ? by : "", sizeof(j->killed_by));
    }
    if (kill(-j->pid, SIGKILL) < 0)
        kill(j->pid, SIGKILL);
}

void jobrun_terminate(wjob_t *j, kill_reason_t why)
{
    if (j->exited)
        return;
    if (j->killed == KILL_NONE)
        j->killed = why;
    if (kill(-j->pid, SIGTERM) < 0)
        kill(j->pid, SIGTERM);
    j->term_deadline = now_ms() + 2000;
}

#define CHECKPOINT_MAX (1024 * 1024)

char *jobrun_new_checkpoint(wjob_t *j, size_t *len)
{
    char path[128];
    snprintf(path, sizeof(path), "%s/checkpoint/state", j->dir);
    struct stat st;
    if (stat(path, &st) < 0 || !S_ISREG(st.st_mode))
        return NULL;
#ifdef __APPLE__
    long ns = st.st_mtimespec.tv_nsec;
#else
    long ns = st.st_mtim.tv_nsec;
#endif
    if (st.st_mtime == j->ckpt_mtime && ns == j->ckpt_mtime_ns && st.st_size == j->ckpt_size)
        return NULL; /* unchanged since we last looked */
    j->ckpt_mtime = st.st_mtime;
    j->ckpt_mtime_ns = ns;
    j->ckpt_size = st.st_size;
    if (st.st_size > CHECKPOINT_MAX) {
        log_line("Job %d: checkpoint is %lld bytes, over the %d byte limit; not sent", j->id,
                 (long long)st.st_size, CHECKPOINT_MAX);
        return NULL;
    }

    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return NULL;
    char *data = malloc((size_t)st.st_size + 1);
    ssize_t n = data ? read(fd, data, (size_t)st.st_size) : -1;
    close(fd);
    if (n < 0) {
        free(data);
        return NULL;
    }
    if (j->ckpt && (size_t)n == j->ckpt_len && memcmp(data, j->ckpt, (size_t)n) == 0) {
        free(data); /* same content, e.g. the one we restored */
        return NULL;
    }
    free(j->ckpt);
    j->ckpt = malloc((size_t)n + 1);
    memcpy(j->ckpt, data, (size_t)n);
    j->ckpt_len = (size_t)n;
    *len = (size_t)n;
    return data;
}

int jobrun_reap(wjob_t *j)
{
    if (j->exited)
        return 1;
    int st;
    pid_t r = waitpid(j->pid, &st, WNOHANG);
    if (r == j->pid) {
        j->exited = 1;
        j->status = st;
        kill(-j->pid, SIGKILL); /* anything the job left behind */
        return 1;
    }
    return 0;
}

void jobrun_sample(wjob_t *j)
{
    if (j->exited)
        return;
#ifdef __linux__
    char path[64], buf[2048];
    snprintf(path, sizeof(path), "/proc/%d/stat", (int)j->pid);
    FILE *f = fopen(path, "r");
    if (f) {
        size_t n = fread(buf, 1, sizeof(buf) - 1, f);
        fclose(f);
        buf[n] = '\0';
        /* Fields after "(comm)": state ppid pgrp session tty tpgid flags
         * minflt cminflt majflt cmajflt utime stime ... */
        char *p = strrchr(buf, ')');
        if (p) {
            char st;
            long ppid, pgrp, sess, tty, tpgid;
            unsigned long flags, minflt, cminflt, majflt, cmajflt, ut, stime;
            if (sscanf(p + 2, "%c %ld %ld %ld %ld %ld %lu %lu %lu %lu %lu %lu %lu",
                       &st, &ppid, &pgrp, &sess, &tty, &tpgid, &flags, &minflt, &cminflt,
                       &majflt, &cmajflt, &ut, &stime) == 13) {
                long hz = sysconf(_SC_CLK_TCK);
                j->faults = (long)(minflt + majflt);
                j->cpu_ms = (long)((ut + stime) * 1000 / (unsigned long)hz);
            }
        }
    }
    snprintf(path, sizeof(path), "/proc/%d/statm", (int)j->pid);
    f = fopen(path, "r");
    if (f) {
        long size, resident;
        if (fscanf(f, "%ld %ld", &size, &resident) == 2)
            j->rss_kb = resident * (sysconf(_SC_PAGESIZE) / 1024);
        fclose(f);
    }
    /* VmHWM is the kernel's own peak RSS, better than our 500 ms samples. */
    snprintf(path, sizeof(path), "/proc/%d/status", (int)j->pid);
    f = fopen(path, "r");
    if (f) {
        while (fgets(buf, sizeof(buf), f)) {
            long kb;
            if (sscanf(buf, "VmHWM: %ld kB", &kb) == 1 && kb > j->peak_kb)
                j->peak_kb = kb;
        }
        fclose(f);
    }
#elif defined(__APPLE__)
    struct proc_taskinfo ti;
    if (proc_pidinfo(j->pid, PROC_PIDTASKINFO, 0, &ti, sizeof(ti)) == (int)sizeof(ti)) {
        static mach_timebase_info_data_t tb;
        if (tb.denom == 0)
            mach_timebase_info(&tb);
        uint64_t ns = (ti.pti_total_user + ti.pti_total_system) * tb.numer / tb.denom;
        j->rss_kb = (long)(ti.pti_resident_size / 1024);
        j->faults = (long)ti.pti_faults;
        j->cpu_ms = (long)(ns / 1000000);
        /* macOS ignores RLIMIT_AS, so -ram is enforced on resident memory. */
        if (j->rss_kb > j->ram_mb * 1024)
            jobrun_kill(j, KILL_MEMORY, NULL);
    }
#endif
    if (j->rss_kb > j->peak_kb)
        j->peak_kb = j->rss_kb;
}

char *jobrun_read_output(wjob_t *j, size_t max, size_t *len)
{
    char path[128];
    snprintf(path, sizeof(path), "%s/output.log", j->dir);
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return NULL; /* the child hasn't created it yet */
    char *data = malloc(max);
    ssize_t n = data ? pread(fd, data, max, j->out_off) : -1;
    close(fd);
    if (n <= 0) {
        free(data);
        return NULL;
    }
    j->out_off += n;
    *len = (size_t)n;
    return data;
}

static const char *signal_words(int sig)
{
    switch (sig) {
    case SIGSEGV: return "SIGSEGV - Segmentation Fault";
    case SIGFPE:  return "SIGFPE - Floating Point Exception";
    case SIGABRT: return "SIGABRT - Aborted";
    case SIGBUS:  return "SIGBUS - Bus Error";
    case SIGILL:  return "SIGILL - Illegal Instruction";
    case SIGKILL: return "SIGKILL - Killed";
    case SIGTERM: return "SIGTERM - Terminated";
    case SIGINT:  return "SIGINT - Interrupted";
    case SIGPIPE: return "SIGPIPE - Broken Pipe";
    case SIGXCPU: return "SIGXCPU - CPU Time Limit Exceeded";
    case SIGXFSZ: return "SIGXFSZ - File Size Limit Exceeded";
    case SIGTRAP: return "SIGTRAP - Trace/Breakpoint Trap";
    case SIGSYS:  return "SIGSYS - Bad System Call";
    default:      return NULL;
    }
}

int jobrun_diagnose(wjob_t *j, char *reason, size_t len)
{
    /* What the child told us before exec (setup failure or exec error). */
    char err[256] = "";
    ssize_t n = read(j->err_fd, err, sizeof(err) - 1);
    err[n > 0 ? n : 0] = '\0';
    close(j->err_fd);
    j->err_fd = -1;
    char *colon = strchr(err, ':');

    switch (j->killed) {
    case KILL_TIMEOUT:
        snprintf(reason, len, "Wall-clock timeout exceeded (%ds limit)", j->time_s);
        return 0;
    case KILL_CANCEL:
        snprintf(reason, len, "Cancelled by %s", j->killed_by[0] ? j->killed_by : "user");
        return 0;
    case KILL_MEMORY:
        snprintf(reason, len, "Memory limit exceeded (%ldMB)", j->ram_mb);
        return 0;
    case KILL_PREEMPT:
        snprintf(reason, len, "Preempted by a priority 1 job");
        return 0;
    case KILL_MIGRATE:
        snprintf(reason, len, "Stopped for migration");
        return 0;
    case KILL_SHUTDOWN:
        snprintf(reason, len, "Worker lost its connection to the scheduler");
        return 0;
    case KILL_NONE:
        break;
    }

    int st = j->status;
    if (WIFEXITED(st)) {
        int code = WEXITSTATUS(st);
        if (code == 0) {
            snprintf(reason, len, "Exited with code 0");
            return 1;
        }
        if (colon && strncmp(err, "exec:", 5) != 0) {
            snprintf(reason, len, "Job setup failed at %.*s (%s); the job was not run",
                     (int)(colon - err), err, colon + 1);
        } else if (code == 127) {
            if (colon && strcmp(colon + 1, strerror(ENOENT)) != 0)
                snprintf(reason, len, "Exited with code 127 (could not start program: %s)", colon + 1);
            else
                snprintf(reason, len, "Exited with code 127 (command not found)");
        } else if (code == 126) {
            snprintf(reason, len, "Exited with code 126 (command not executable)");
        } else {
            snprintf(reason, len, "Exited with code %d", code);
        }
        return 0;
    }
    if (WIFSIGNALED(st)) {
        int sig = WTERMSIG(st);
        if (sig == SIGXCPU) {
            /* The CPU limit and the wall-clock deadline are the same number. */
            snprintf(reason, len, "Wall-clock timeout exceeded (%ds limit)", j->time_s);
            return 0;
        }
        const char *w = signal_words(sig);
        if (w)
            snprintf(reason, len, "Terminated by signal %d (%s)", sig, w);
        else
            snprintf(reason, len, "Terminated by signal %d (%s)", sig, strsignal(sig));
        return 0;
    }
    snprintf(reason, len, "Ended with unknown wait status %d", st);
    return 0;
}
