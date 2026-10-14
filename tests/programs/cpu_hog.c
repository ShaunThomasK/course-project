/* Spins 4 threads so CPU time grows ~4x faster than wall-clock time:
 * RLIMIT_CPU (SIGXCPU) fires well before the worker's wall-clock timer.
 * Falls back to one thread if threads can't be created. */
#include <pthread.h>
#include <stdio.h>
static void *spin(void *arg)
{
    (void)arg;
    volatile unsigned long x = 0;
    for (;;)
        x++;
    return NULL;
}
int main(void)
{
    pthread_t t[3];
    int made = 0;
    for (int i = 0; i < 3; i++)
        if (pthread_create(&t[i], NULL, spin, NULL) == 0)
            made++;
    printf("spinning on %d threads\n", made + 1);
    fflush(stdout);
    spin(NULL);
}
