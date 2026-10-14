/* Tries to write outside its job directory. Under sudo (running as
 * nobody) every attempt must fail with Permission denied. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(void)
{
    const char *targets[] = {"/etc/osn_pwned", "/root/osn_pwned", NULL};
    char home[512];
    const char *h = getenv("SUDO_USER");
    snprintf(home, sizeof(home), "/home/%s/Desktop/osn_pwned", h ? h : "nobody");
    for (int i = 0; i < 3; i++) {
        const char *t = targets[i] ? targets[i] : home;
        FILE *f = fopen(t, "w");
        printf("write %s: %s\n", t, f ? "SUCCEEDED (bad!)" : strerror(errno));
        if (f)
            fclose(f);
    }
    FILE *f = fopen("inside_job_dir.txt", "w");
    printf("write ./inside_job_dir.txt: %s\n", f ? "ok" : strerror(errno));
    if (f)
        fclose(f);
    return 0;
}
