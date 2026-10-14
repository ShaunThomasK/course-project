#include "proto.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void *xrealloc(void *p, size_t n)
{
    void *q = realloc(p, n);
    if (!q) {
        perror("realloc");
        abort();
    }
    return q;
}

static char *xstrdup(const char *s)
{
    char *d = strdup(s);
    if (!d) {
        perror("strdup");
        abort();
    }
    return d;
}

/* ---- buf_t -------------------------------------------------------------- */

void buf_append(buf_t *b, const void *p, size_t n)
{
    if (n == 0)
        return;
    /* Reclaim consumed space before growing. */
    if (b->off > 0 && b->len + n > b->cap) {
        memmove(b->data, b->data + b->off, b->len - b->off);
        b->len -= b->off;
        b->off = 0;
    }
    if (b->len + n > b->cap) {
        size_t cap = b->cap ? b->cap : 4096;
        while (cap < b->len + n)
            cap *= 2;
        b->data = xrealloc(b->data, cap);
        b->cap = cap;
    }
    memcpy(b->data + b->len, p, n);
    b->len += n;
}

void buf_consume(buf_t *b, size_t n)
{
    b->off += n;
    if (b->off >= b->len)
        b->off = b->len = 0;
}

size_t buf_used(const buf_t *b)
{
    return b->len - b->off;
}

void buf_free(buf_t *b)
{
    free(b->data);
    memset(b, 0, sizeof(*b));
}

/* ---- msg_t -------------------------------------------------------------- */

msg_t *msg_new(const char *type)
{
    msg_t *m = calloc(1, sizeof(*m));
    if (!m)
        abort();
    snprintf(m->type, sizeof(m->type), "%s", type);
    return m;
}

void msg_free(msg_t *m)
{
    if (!m)
        return;
    for (int i = 0; i < m->nfields; i++) {
        free(m->keys[i]);
        free(m->vals[i]);
    }
    free(m->keys);
    free(m->vals);
    free(m->blob);
    free(m);
}

msg_t *msg_clone(const msg_t *m)
{
    msg_t *c = msg_new(m->type);
    for (int i = 0; i < m->nfields; i++)
        msg_set(c, m->keys[i], m->vals[i]);
    msg_set_blob(c, m->blob, m->blob_len);
    return c;
}

void msg_set(msg_t *m, const char *key, const char *val)
{
    for (int i = 0; i < m->nfields; i++) {
        if (strcmp(m->keys[i], key) == 0) {
            free(m->vals[i]);
            m->vals[i] = xstrdup(val);
            return;
        }
    }
    if (m->nfields == m->capfields) {
        m->capfields = m->capfields ? m->capfields * 2 : 8;
        m->keys = xrealloc(m->keys, sizeof(char *) * (size_t)m->capfields);
        m->vals = xrealloc(m->vals, sizeof(char *) * (size_t)m->capfields);
    }
    m->keys[m->nfields] = xstrdup(key);
    m->vals[m->nfields] = xstrdup(val);
    m->nfields++;
}

void msg_setf(msg_t *m, const char *key, const char *fmt, ...)
{
    char tmp[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    msg_set(m, key, tmp);
}

void msg_set_long(msg_t *m, const char *key, long long v)
{
    msg_setf(m, key, "%lld", v);
}

void msg_set_blob(msg_t *m, const void *data, size_t len)
{
    char *copy = NULL;
    if (len) {
        copy = malloc(len);
        if (!copy)
            abort();
        memcpy(copy, data, len);
    }
    msg_take_blob(m, copy, len);
}

void msg_take_blob(msg_t *m, char *data, size_t len)
{
    free(m->blob);
    m->blob = data;
    m->blob_len = len;
}

const char *msg_get(const msg_t *m, const char *key)
{
    for (int i = 0; i < m->nfields; i++)
        if (strcmp(m->keys[i], key) == 0)
            return m->vals[i];
    return NULL;
}

const char *msg_get_str(const msg_t *m, const char *key)
{
    const char *v = msg_get(m, key);
    return v ? v : "";
}

long long msg_get_long(const msg_t *m, const char *key, long long def)
{
    const char *v = msg_get(m, key);
    if (!v || !*v)
        return def;
    char *end;
    long long r = strtoll(v, &end, 10);
    return *end ? def : r;
}

int msg_is(const msg_t *m, const char *type)
{
    return strcmp(m->type, type) == 0;
}

static void put_u32(buf_t *b, uint32_t v)
{
    uint32_t be = htonl(v);
    buf_append(b, &be, 4);
}

static void append_escaped(buf_t *b, const char *s)
{
    for (; *s; s++) {
        if (*s == '\\')
            buf_append(b, "\\\\", 2);
        else if (*s == '\n')
            buf_append(b, "\\n", 2);
        else
            buf_append(b, s, 1);
    }
}

void msg_encode(const msg_t *m, buf_t *out)
{
    buf_t hdr = {0};
    buf_append(&hdr, m->type, strlen(m->type));
    buf_append(&hdr, "\n", 1);
    for (int i = 0; i < m->nfields; i++) {
        buf_append(&hdr, m->keys[i], strlen(m->keys[i]));
        buf_append(&hdr, "=", 1);
        append_escaped(&hdr, m->vals[i]);
        buf_append(&hdr, "\n", 1);
    }
    size_t hlen = buf_used(&hdr);
    put_u32(out, (uint32_t)(4 + hlen + m->blob_len));
    put_u32(out, (uint32_t)hlen);
    buf_append(out, hdr.data, hlen);
    buf_append(out, m->blob, m->blob_len);
    buf_free(&hdr);
}

/* Unescapes s in place. */
static void unescape(char *s)
{
    char *w = s;
    for (char *r = s; *r; r++) {
        if (*r == '\\' && r[1] == 'n') {
            *w++ = '\n';
            r++;
        } else if (*r == '\\' && r[1] == '\\') {
            *w++ = '\\';
            r++;
        } else {
            *w++ = *r;
        }
    }
    *w = '\0';
}

int msg_decode(buf_t *in, msg_t **out)
{
    size_t avail = buf_used(in);
    if (avail < 8)
        return 0;
    const unsigned char *p = (const unsigned char *)in->data + in->off;
    uint32_t total, hlen;
    memcpy(&total, p, 4);
    memcpy(&hlen, p + 4, 4);
    total = ntohl(total);
    hlen = ntohl(hlen);
    if (total < 4 || total > MSG_MAX_FRAME || hlen > total - 4)
        return -1;
    if (avail < 4 + (size_t)total)
        return 0;

    /* Copy the header out so we can NUL-terminate and split it. */
    char *hdr = malloc((size_t)hlen + 1);
    if (!hdr)
        abort();
    memcpy(hdr, p + 8, hlen);
    hdr[hlen] = '\0';

    char *save = NULL;
    char *line = strtok_r(hdr, "\n", &save);
    if (!line) {
        free(hdr);
        return -1;
    }
    msg_t *m = msg_new(line);
    while ((line = strtok_r(NULL, "\n", &save)) != NULL) {
        char *eq = strchr(line, '=');
        if (!eq)
            continue;
        *eq = '\0';
        unescape(eq + 1);
        msg_set(m, line, eq + 1);
    }
    free(hdr);

    size_t blen = (size_t)total - 4 - hlen;
    if (blen)
        msg_set_blob(m, p + 8 + hlen, blen);

    buf_consume(in, 4 + (size_t)total);
    *out = m;
    return 1;
}

/* ---- conn_t ------------------------------------------------------------- */

void conn_init(conn_t *c, int fd)
{
    memset(c, 0, sizeof(*c));
    c->fd = fd;
}

void conn_close(conn_t *c)
{
    if (c->fd >= 0)
        close(c->fd);
    c->fd = -1;
    buf_free(&c->in);
    buf_free(&c->out);
}

int conn_flush(conn_t *c)
{
    while (buf_used(&c->out) > 0) {
        ssize_t n = send(c->fd, c->out.data + c->out.off, buf_used(&c->out),
#ifdef MSG_NOSIGNAL
                         MSG_NOSIGNAL
#else
                         0
#endif
        );
        if (n > 0) {
            buf_consume(&c->out, (size_t)n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

int conn_send(conn_t *c, const msg_t *m)
{
    if (c->fd < 0)
        return -1;
    msg_encode(m, &c->out);
    return conn_flush(c);
}

int conn_flush_all(conn_t *c)
{
    while (buf_used(&c->out) > 0) {
        if (conn_flush(c) < 0)
            return -1;
        if (buf_used(&c->out) == 0)
            break;
        struct pollfd pfd = {.fd = c->fd, .events = POLLOUT};
        if (poll(&pfd, 1, 5000) <= 0 && errno != EINTR)
            return -1;
    }
    return 0;
}

int conn_read(conn_t *c)
{
    char tmp[65536];
    for (;;) {
        ssize_t n = recv(c->fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            buf_append(&c->in, tmp, (size_t)n);
            return (int)n;
        }
        if (n == 0)
            return 0;
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 1; /* nothing right now, but still alive */
        return -1;
    }
}

int conn_next(conn_t *c, msg_t **out)
{
    return msg_decode(&c->in, out);
}

int conn_has_output(const conn_t *c)
{
    return buf_used(&c->out) > 0;
}

/* ---- sockets ------------------------------------------------------------ */

int set_nonblocking(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0)
        return -1;
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

int tcp_listen(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0 || listen(fd, 64) < 0 ||
        set_nonblocking(fd) < 0) {
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }
    return fd;
}

int tcp_connect(const char *ip, int port, int timeout_ms)
{
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1)
        return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    set_nonblocking(fd);

    int r = connect(fd, (struct sockaddr *)&a, sizeof(a));
    if (r < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    if (r < 0) {
        struct pollfd pfd = {.fd = fd, .events = POLLOUT};
        if (poll(&pfd, 1, timeout_ms) <= 0) {
            close(fd);
            return -1;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err) {
            close(fd);
            errno = err;
            return -1;
        }
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    return fd;
}
