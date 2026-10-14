/* Prints a greeting and exits 0. */
#include <stdio.h>
int main(int argc, char **argv)
{
    printf("Hello from job! argc=%d\n", argc);
    for (int i = 1; i < argc; i++)
        printf("  arg %d: %s\n", i, argv[i]);
    return 0;
}
