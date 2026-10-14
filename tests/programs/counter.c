/*
 * A long job that checkpoints (Section 14). Counts from 1 to N (default
 * 30), one step every 0.5 s, saving the step in $OSN_CHECKPOINT_DIR/state
 * (written to a temp file, then renamed). On start it resumes from that
 * file if it exists. SIGTERM makes it save once more and exit.
 */
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static volatile sig_atomic_t stop;
static void on_term(int sig) { (void)sig; stop = 1; }

static void save(const char *dir, int step)
{
    if (!dir)
        return;
    char tmp[512], path[512];
    snprintf(tmp, sizeof(tmp), "%s/state.tmp", dir);
    snprintf(path, sizeof(path), "%s/state", dir);
    FILE *f = fopen(tmp, "w");
    if (!f)
        return;
    fprintf(f, "%d\n", step);
    fclose(f);
    rename(tmp, path);
}

int main(int argc, char **argv)
{
    int n = argc > 1 ? atoi(argv[1]) : 30;
    const char *dir = getenv("OSN_CHECKPOINT_DIR");
    signal(SIGTERM, on_term);

    int done = 0;
    char path[512];
    if (dir) {
        snprintf(path, sizeof(path), "%s/state", dir);
        FILE *f = fopen(path, "r");
        if (f) {
            if (fscanf(f, "%d", &done) == 1)
                printf("resuming after step %d\n", done);
            fclose(f);
        }
    }
    if (!done)
        printf("starting from the beginning\n");
    fflush(stdout);

    for (int i = done + 1; i <= n; i++) {
        usleep(500000);
        if (stop) {
            save(dir, i - 1);
            printf("SIGTERM: saved step %d, exiting\n", i - 1);
            return 0;
        }
        printf("step %d of %d\n", i, n);
        fflush(stdout);
        save(dir, i);
    }
    printf("done\n");
    return 0;
}
