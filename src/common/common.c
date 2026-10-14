#include "common.h"

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char *job_state_name(job_state_t s)
{
    switch (s) {
    case JOB_WAITING:          return "WAITING";
    case JOB_QUEUED:           return "QUEUED";
    case JOB_RUNNING:          return "RUNNING";
    case JOB_COMPLETED:        return "COMPLETED";
    case JOB_FAILED:           return "FAILED";
    case JOB_CANCELLED:        return "CANCELLED";
    case JOB_LOST:             return "LOST";
    case JOB_FAILED_PERMANENT: return "FAILED_PERMANENT";
    }
    return "?";
}

const char *worker_state_name(worker_state_t s)
{
    switch (s) {
    case WORKER_ALIVE:     return "ALIVE";
    case WORKER_SUSPECTED: return "SUSPECTED";
    case WORKER_DEAD:      return "DEAD";
    case WORKER_DRAINING:  return "DRAINING";
    }
    return "?";
}

int job_state_is_terminal(job_state_t s)
{
    return s == JOB_COMPLETED || s == JOB_FAILED || s == JOB_CANCELLED ||
           s == JOB_FAILED_PERMANENT;
}

uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

int username_valid(const char *name)
{
    size_t n = strlen(name);
    if (n < 1 || n >= USERNAME_MAX)
        return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!isalnum(c) && c != '_' && c != '-' && c != '.')
            return 0;
    }
    return 1;
}

void str_copy(char *dst, const char *src, size_t dstsz)
{
    if (dstsz == 0)
        return;
    size_t n = strlen(src);
    if (n >= dstsz)
        n = dstsz - 1;
    memcpy(dst, src, n);
    dst[n] = '\0';
}

int parse_long(const char *s, long *out)
{
    if (!s || !*s)
        return -1;
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno || *end)
        return -1;
    *out = v;
    return 0;
}

static char g_log_tag[64] = "?";

void log_set_tag(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_log_tag, sizeof(g_log_tag), fmt, ap);
    va_end(ap);
}

void log_line(const char *fmt, ...)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);

    /* Format into one buffer and write it in a single call so lines from
     * different sources never interleave mid-line. */
    char line[4096];
    int n = snprintf(line, sizeof(line), "[%02d:%02d:%02d] [%s] ",
                     tm.tm_hour, tm.tm_min, tm.tm_sec, g_log_tag);
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line + n, sizeof(line) - (size_t)n, fmt, ap);
    va_end(ap);

    fputs(line, stdout);
    fputc('\n', stdout);
    fflush(stdout);
}
