/* Allocates and touches 10 MB every 0.5 s until something stops it. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main(void)
{
    for (int mb = 10;; mb += 10) {
        char *p = malloc(10 * 1024 * 1024);
        if (!p) {
            fprintf(stderr, "malloc failed after %d MB\n", mb - 10);
            return 1;
        }
        memset(p, 1, 10 * 1024 * 1024);
        printf("holding %d MB\n", mb);
        fflush(stdout);
        usleep(500000);
    }
}
