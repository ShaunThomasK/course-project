/* Burns CPU forever; should be killed by the time limit. */
int main(void)
{
    volatile unsigned long x = 0;
    for (;;)
        x++;
}
