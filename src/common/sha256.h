/*
 * sha256.h - SHA-256 (FIPS 180-4), used to identify executables by their
 * content for caching (Section 13).
 */
#ifndef OSN_SHA256_H
#define OSN_SHA256_H

#include <stddef.h>
#include <stdint.h>

#define SHA256_HEX_LEN 64

typedef struct {
    uint32_t state[8];
    uint64_t bits;
    uint8_t  buf[64];
    size_t   used;
} sha256_ctx;

void sha256_init(sha256_ctx *c);
void sha256_update(sha256_ctx *c, const void *data, size_t len);
/* Writes 64 lowercase hex digits plus a NUL into hex. */
void sha256_final_hex(sha256_ctx *c, char hex[SHA256_HEX_LEN + 1]);
/* One-shot helper. */
void sha256_hex(const void *data, size_t len, char hex[SHA256_HEX_LEN + 1]);

#endif
