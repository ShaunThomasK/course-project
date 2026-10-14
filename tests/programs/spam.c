/* Prints N lines of ~100 bytes (default 1000, ~100 KB) to overflow the
 * scheduler's 64 KB log buffer. */
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char **argv)
{
    int n = argc > 1 ? atoi(argv[1]) : 1000;
    for (int i = 1; i <= n; i++)
        printf("spam line %d %s\n", i,
               "................................................................................");
    return 0;
}
