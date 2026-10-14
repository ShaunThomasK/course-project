/*
 * discovery.h - UDP scheduler discovery (Section 2C).
 *
 * A worker or client broadcasts "OSN_DISCOVER <nonce>" on the discovery
 * port. The scheduler answers the sender with
 * "OSN_SCHEDULER <nonce> <tcp_port>"; the source address of that reply is
 * the scheduler's IP.
 */
#ifndef OSN_DISCOVERY_H
#define OSN_DISCOVERY_H

/* Scheduler side: opens the responder socket (SO_REUSEADDR/SO_REUSEPORT). */
int discovery_open_responder(int udp_port);

/* Scheduler side: answers one pending request on fd, if it is valid. */
void discovery_handle_request(int fd, int tcp_port);

/*
 * Worker/client side: finds a scheduler, retrying DISCOVERY_ATTEMPTS times
 * with a DISCOVERY_TIMEOUT_MS wait each. If that fails and a fallback
 * address was given, uses it. Logs the outcome through log_line().
 * Returns 0 and fills ip/port on success, -1 if nothing is available.
 */
int discovery_resolve(int udp_port, const char *fallback_ip, int fallback_port,
                      char *ip_out, int ip_out_len, int *port_out);

#endif
