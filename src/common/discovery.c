#include "discovery.h"
#include "common.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define REQ_PREFIX   "OSN_DISCOVER"
#define REPLY_PREFIX "OSN_SCHEDULER"

int discovery_open_responder(int udp_port)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)udp_port);
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }
    return fd;
}

void discovery_handle_request(int fd, int tcp_port)
{
    char buf[256];
    struct sockaddr_in from;
    socklen_t flen = sizeof(from);
    ssize_t n = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&from, &flen);
    if (n <= 0)
        return;
    buf[n] = '\0';

    char nonce[64];
    if (sscanf(buf, REQ_PREFIX " %63s", nonce) != 1)
        return; /* not ours; ignore silently */

    char reply[128];
    int len = snprintf(reply, sizeof(reply), REPLY_PREFIX " %s %d", nonce, tcp_port);
    sendto(fd, reply, (size_t)len, 0, (struct sockaddr *)&from, flen);
}

/* Sends the request to every place a scheduler might hear it: the limited
 * broadcast address, each interface's directed broadcast address, and
 * loopback (broadcast does not always reach a scheduler on the same host). */
static void send_requests(int fd, int udp_port, const char *req, size_t len)
{
    struct sockaddr_in to = {0};
    to.sin_family = AF_INET;
    to.sin_port = htons((uint16_t)udp_port);

    to.sin_addr.s_addr = htonl(INADDR_BROADCAST);
    sendto(fd, req, len, 0, (struct sockaddr *)&to, sizeof(to));

    struct ifaddrs *ifs = NULL;
    if (getifaddrs(&ifs) == 0) {
        for (struct ifaddrs *i = ifs; i; i = i->ifa_next) {
            if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET)
                continue;
            if (!(i->ifa_flags & IFF_BROADCAST) || !i->ifa_broadaddr)
                continue;
            to.sin_addr = ((struct sockaddr_in *)i->ifa_broadaddr)->sin_addr;
            sendto(fd, req, len, 0, (struct sockaddr *)&to, sizeof(to));
        }
        freeifaddrs(ifs);
    }

    to.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sendto(fd, req, len, 0, (struct sockaddr *)&to, sizeof(to));
}

static int discover_once(int fd, int udp_port, const char *nonce,
                         char *ip_out, int ip_out_len, int *port_out)
{
    char req[96];
    int rlen = snprintf(req, sizeof(req), REQ_PREFIX " %s", nonce);
    send_requests(fd, udp_port, req, (size_t)rlen);

    uint64_t deadline = now_ms() + DISCOVERY_TIMEOUT_MS;
    for (;;) {
        uint64_t now = now_ms();
        if (now >= deadline)
            return -1;
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        int r = poll(&pfd, 1, (int)(deadline - now));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return -1;

        char buf[256];
        struct sockaddr_in from;
        socklen_t flen = sizeof(from);
        ssize_t n = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&from, &flen);
        if (n <= 0)
            continue;
        buf[n] = '\0';

        char got_nonce[64];
        int port;
        if (sscanf(buf, REPLY_PREFIX " %63s %d", got_nonce, &port) != 2)
            continue;
        if (strcmp(got_nonce, nonce) != 0)
            continue; /* a stale reply from an earlier attempt or someone else */
        inet_ntop(AF_INET, &from.sin_addr, ip_out, (socklen_t)ip_out_len);
        *port_out = port;
        return 0;
    }
}

int discovery_resolve(int udp_port, const char *fallback_ip, int fallback_port,
                      char *ip_out, int ip_out_len, int *port_out)
{
    log_line("Looking for scheduler via UDP discovery (port %d)...", udp_port);

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));

        char nonce[32];
        snprintf(nonce, sizeof(nonce), "%d-%llu", (int)getpid(),
                 (unsigned long long)now_ms());

        for (int attempt = 1; attempt <= DISCOVERY_ATTEMPTS; attempt++) {
            if (discover_once(fd, udp_port, nonce, ip_out, ip_out_len, port_out) == 0) {
                close(fd);
                log_line("Scheduler found at %s:%d.", ip_out, *port_out);
                return 0;
            }
            log_line("Discovery attempt %d/%d got no reply.", attempt, DISCOVERY_ATTEMPTS);
        }
        close(fd);
    }

    if (fallback_ip) {
        str_copy(ip_out, fallback_ip, (size_t)ip_out_len);
        *port_out = fallback_port;
        log_line("No discovery reply, using fallback %s:%d.", fallback_ip, fallback_port);
        return 0;
    }
    log_line("No discovery reply and no fallback address given.");
    return -1;
}
