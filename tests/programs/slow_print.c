/* Prints a numbered line every 0.5 s, mostly sleeping (wall-clock, not CPU).
 * Usage: slow_print [count] */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
int main(int argc, char **argv)
{
    int n = argc > 1 ? atoi(argv[1]) : 20;
    for (int i = 1; i <= n; i++) {
        printf("line %d of %d\n", i, n);
        fflush(stdout);
        usleep(500000);
    }
    return 0;
}
