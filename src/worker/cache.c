/*
 * cache.c - the worker's executable cache (Section 13).
 *
 * Programs live in $OSN_CACHE_DIR (default "cache/" under the working
 * directory) as "<name>-<sha256>". The folder is capped at
 * $OSN_CACHE_SIZE_MB (default 512). Least-recently-used order is kept in
 * the files' mtimes, so it survives a worker restart: a file is touched
 * whenever it is stored or a job starts from it.
 */
#define _GNU_SOURCE
#include "worker.h"
#include "../common/sha256.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#ifdef __APPLE__
#define MTIME_NSEC(st) ((st).st_mtimespec.tv_nsec)
#else
#define MTIME_NSEC(st) ((st).st_mtim.tv_nsec)
#endif

typedef struct {
    char     name[256];
    char     hash[SHA256_HEX_LEN + 1];
    off_t    size;
    uint64_t used_ns; /* last time a job started from it */
} centry_t;

static centry_t *g_entries;
static int       g_nentries;
static char      g_dir[PATH_MAX] = "cache";
static long long g_limit = 512LL * 1024 * 1024;

static uint64_t wall_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void entry_path(const centry_t *e, char *out, size_t n)
{
    snprintf(out, n, "%s/%s", g_dir, e->name);
}

/* "<anything>-<64 hex>" -> fills hash. */
static int parse_name(const char *name, char *hash)
{
    const char *dash = strrchr(name, '-');
    if (!dash || strlen(dash + 1) != SHA256_HEX_LEN)
        return -1;
    for (const char *p = dash + 1; *p; p++)
        if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
            return -1;
    memcpy(hash, dash + 1, SHA256_HEX_LEN + 1);
    return 0;
}

void cache_init(void)
{
    const char *d = getenv("OSN_CACHE_DIR");
    if (d && *d)
        str_copy(g_dir, d, sizeof(g_dir));
    const char *mb = getenv("OSN_CACHE_SIZE_MB");
    long v;
    if (mb && parse_long(mb, &v) == 0 && v >= 1)
        g_limit = (long long)v * 1024 * 1024;
    if (mkdir(g_dir, 0755) < 0 && errno != EEXIST)
        log_line("WARNING: cannot create cache folder %s: %s", g_dir, strerror(errno));
    char abs[PATH_MAX];
    if (realpath(g_dir, abs))
        str_copy(g_dir, abs, sizeof(g_dir)); /* jobs copy from here after other chdirs */
}

void cache_scan(void)
{
    g_nentries = 0;
    DIR *dir = opendir(g_dir);
    if (!dir)
        return;
    struct dirent *de;
    long long total = 0;
    while ((de = readdir(dir)) != NULL) {
        char path[PATH_MAX + 512];
        snprintf(path, sizeof(path), "%s/%s", g_dir, de->d_name);
        if (strstr(de->d_name, ".tmp")) {
            unlink(path); /* half-written by a worker that died mid-store */
            continue;
        }
        char hash[SHA256_HEX_LEN + 1];
        struct stat st;
        if (parse_name(de->d_name, hash) < 0 || stat(path, &st) < 0 || !S_ISREG(st.st_mode))
            continue;
        g_entries = realloc(g_entries, sizeof(centry_t) * (size_t)(g_nentries + 1));
        centry_t *e = &g_entries[g_nentries++];
        str_copy(e->name, de->d_name, sizeof(e->name));
        memcpy(e->hash, hash, sizeof(hash));
        e->size = st.st_size;
        e->used_ns = (uint64_t)st.st_mtime * 1000000000ull + (uint64_t)MTIME_NSEC(st);
        total += st.st_size;
    }
    closedir(dir);
    log_line("Cache: %d program(s), %lld bytes in %s (limit %lldMB)", g_nentries, total, g_dir,
             g_limit / (1024 * 1024));
}

char *cache_hash_list(void)
{
    buf_t b = {0};
    for (int i = 0; i < g_nentries; i++) {
        if (i)
            buf_append(&b, ",", 1);
        buf_append(&b, g_entries[i].hash, SHA256_HEX_LEN);
    }
    buf_append(&b, "", 1);
    char *s = strdup(b.data);
    buf_free(&b);
    return s;
}

static centry_t *find(const char *hash)
{
    for (int i = 0; i < g_nentries; i++)
        if (strcmp(g_entries[i].hash, hash) == 0)
            return &g_entries[i];
    return NULL;
}

static void touch(centry_t *e)
{
    char path[PATH_MAX + 512];
    entry_path(e, path, sizeof(path));
    e->used_ns = wall_ns();
    utimensat(AT_FDCWD, path, NULL, 0); /* mtime = now, for the next restart */
}

/* Deletes least recently used programs until the folder fits. Never the
 * one just stored (keep) or one a running job uses. Returns how many. */
static int evict(const char *keep)
{
    int evicted = 0;
    for (;;) {
        long long total = 0;
        for (int i = 0; i < g_nentries; i++)
            total += g_entries[i].size;
        if (total <= g_limit)
            return evicted;

        int victim = -1;
        for (int i = 0; i < g_nentries; i++) {
            centry_t *e = &g_entries[i];
            if (strcmp(e->hash, keep) == 0 || cache_in_use(e->hash))
                continue;
            if (victim < 0 || e->used_ns < g_entries[victim].used_ns)
                victim = i;
        }
        if (victim < 0) {
            log_line("Cache: %lld bytes is over the %lldMB limit, but every program left is in use",
                     total, g_limit / (1024 * 1024));
            return evicted;
        }
        centry_t *e = &g_entries[victim];
        char path[PATH_MAX + 512];
        entry_path(e, path, sizeof(path));
        unlink(path);
        log_line("Cache: evicted %s (%lld bytes, least recently used)", e->name, (long long)e->size);
        g_entries[victim] = g_entries[--g_nentries];
        evicted++;
    }
}

int cache_store(const char *name, const char *hash, const char *data, size_t len,
                char *path_out, size_t n, int *evicted)
{
    *evicted = 0;
    centry_t *e = find(hash);
    if (!e) {
        g_entries = realloc(g_entries, sizeof(centry_t) * (size_t)(g_nentries + 1));
        e = &g_entries[g_nentries++];
        snprintf(e->name, sizeof(e->name), "%.150s-%s", name, hash);
        memcpy(e->hash, hash, SHA256_HEX_LEN + 1);
    }
    e->size = (off_t)len;

    char path[PATH_MAX + 512], tmp[PATH_MAX + 520];
    entry_path(e, path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0755);
    size_t done = 0;
    while (fd >= 0 && done < len) {
        ssize_t w = write(fd, data + done, len - done);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            break;
        done += (size_t)w;
    }
    /* Write then rename, so nobody ever sees a half-written program. */
    if (fd < 0 || done < len || close(fd) < 0 || rename(tmp, path) < 0) {
        log_line("Cache: could not store %s: %s", e->name, strerror(errno));
        if (fd >= 0)
            unlink(tmp);
        *e = g_entries[--g_nentries];
        return -1;
    }
    touch(e);
    str_copy(path_out, path, n);
    *evicted = evict(hash);
    return 0;
}

int cache_lookup(const char *hash, char *path_out, size_t n)
{
    centry_t *e = find(hash);
    if (!e)
        return -1;
    char path[PATH_MAX + 512];
    entry_path(e, path, sizeof(path));
    struct stat st;
    if (stat(path, &st) < 0) {
        *e = g_entries[--g_nentries]; /* someone deleted it */
        return -1;
    }
    touch(e);
    str_copy(path_out, path, n);
    return 0;
}
