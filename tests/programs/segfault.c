/* Dereferences NULL: SIGSEGV. */
#include <stdio.h>
int main(void)
{
    puts("about to crash");
    fflush(stdout);
    volatile int *p = 0;
    return *p;
}
