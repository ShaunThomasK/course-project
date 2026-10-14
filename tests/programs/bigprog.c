/* A ~0.7 MB program for cache tests. "make tests" builds three variants,
 * big_a, big_b and big_c, whose contents (and so hashes) differ. */
#include <stdio.h>
#ifndef VARIANT
#define VARIANT "a"
#endif
static const char pad[700 * 1024] = {VARIANT[0]};
int main(void)
{
    printf("big program %s (%zu bytes of data, first byte %d)\n", VARIANT, sizeof(pad), pad[0]);
    return 0;
}
