/* Exits with the code given as the first argument (default 3). */
#include <stdlib.h>
int main(int argc, char **argv)
{
    return argc > 1 ? atoi(argv[1]) : 3;
}
