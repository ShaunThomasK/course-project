/*
 * proto.h - the wire protocol and buffered TCP connections.
 *
 * Every message is one frame:
 *
 *     +-----------+-----------+------------------------+--------------+
 *     | u32 total | u32 hdrlen| header (hdrlen bytes)  | blob (rest)  |
 *     +-----------+-----------+------------------------+--------------+
 *
 * Both lengths are big-endian. total = 4 + hdrlen + bloblen.
 *
 * The header is text: the first line is the message type, every following
 * line is "key=value". Backslashes and newlines inside values are escaped
 * as "\\" and "\n", so any string survives the trip. The optional blob
 * carries raw bytes (an executable, a chunk of job output, a checkpoint)
 * without any escaping.
 *
 * Message types are listed in the Protocol section of README.md.
 */
#ifndef OSN_PROTO_H
#define OSN_PROTO_H

#include <stddef.h>
#include <stdint.h>

#define MSG_MAX_FRAME (512u * 1024 * 1024) /* refuse anything larger */

/* ---- growable byte buffer ---------------------------------------------- */

typedef struct {
    char  *data;
    size_t off;  /* bytes already consumed from the front */
    size_t len;  /* end of valid data */
    size_t cap;
} buf_t;

void   buf_append(buf_t *b, const void *p, size_t n);
void   buf_consume(buf_t *b, size_t n);
size_t buf_used(const buf_t *b);
void   buf_free(buf_t *b);

/* ---- messages ---------------------------------------------------------- */

typedef struct {
    char   type[32];
    int    nfields, capfields;
    char **keys;
    char **vals;
    char  *blob;
    size_t blob_len;
} msg_t;

msg_t      *msg_new(const char *type);
void        msg_free(msg_t *m);
msg_t      *msg_clone(const msg_t *m);
void        msg_set(msg_t *m, const char *key, const char *val);
void        msg_setf(msg_t *m, const char *key, const char *fmt, ...)
                __attribute__((format(printf, 3, 4)));
void        msg_set_long(msg_t *m, const char *key, long long v);
void        msg_set_blob(msg_t *m, const void *data, size_t len);
/* Takes ownership of a malloc()ed buffer instead of copying it. */
void        msg_take_blob(msg_t *m, char *data, size_t len);
const char *msg_get(const msg_t *m, const char *key); /* NULL if absent */
const char *msg_get_str(const msg_t *m, const char *key); /* "" if absent */
long long   msg_get_long(const msg_t *m, const char *key, long long def);
int         msg_is(const msg_t *m, const char *type);

void msg_encode(const msg_t *m, buf_t *out);
/* 1 = *out holds a message, 0 = need more bytes, -1 = malformed stream. */
int  msg_decode(buf_t *in, msg_t **out);

/* ---- connections ------------------------------------------------------- */

typedef struct {
    int   fd;
    buf_t in;
    buf_t out;
} conn_t;

void conn_init(conn_t *c, int fd);
void conn_close(conn_t *c);
/* Queues a message and writes as much as the socket takes right now. */
int  conn_send(conn_t *c, const msg_t *m);
/* Non-blocking write of pending output. -1 on a dead socket. */
int  conn_flush(conn_t *c);
/* Waits until all pending output is written (worker and client only). */
int  conn_flush_all(conn_t *c);
/* Reads what is available. >0 bytes read, 0 on EOF, -1 on error. */
int  conn_read(conn_t *c);
/* Pulls the next full message out of c->in (same returns as msg_decode). */
int  conn_next(conn_t *c, msg_t **out);
int  conn_has_output(const conn_t *c);

/* ---- sockets ----------------------------------------------------------- */

int set_nonblocking(int fd);
int tcp_listen(int port);
/* Connects with a timeout and returns a non-blocking socket, or -1. */
int tcp_connect(const char *ip, int port, int timeout_ms);

#endif
