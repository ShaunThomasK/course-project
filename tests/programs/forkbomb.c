/* A bounded "fork bomb" for checking RLIMIT_NPROC: forks up to 200
 * sleeping children, reports how many it got before fork() failed, then
 * kills them all. Only meant to run as a job under a sudo worker. */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
int main(void)
{
    pid_t kids[200];
    int n = 0;
    const char *why = "no failure";
    for (; n < 200; n++) {
        pid_t p = fork();
        if (p == 0) {
            sleep(10);
            _exit(0);
        }
        if (p < 0) {
            why = strerror(errno);
            break;
        }
        kids[n] = p;
    }
    printf("forked %d children before fork failed: %s\n", n, why);
    fflush(stdout);
    for (int i = 0; i < n; i++)
        kill(kids[i], SIGKILL);
    while (wait(NULL) > 0)
        ;
    return 0;
}
