/* Ignores SIGTERM and runs for a long time, so migration has to fall
 * back to SIGKILL after 2 seconds. */
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
int main(void)
{
    signal(SIGTERM, SIG_IGN);
    for (int i = 1; i <= 120; i++) {
        printf("still here %d\n", i);
        fflush(stdout);
        sleep(1);
    }
    return 0;
}
