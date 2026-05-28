/*
 * GravelDB - Platform-agnostic I/O Multiplexing (epoll / kqueue / poll fallback)
 *
 * Provides a unified event interface for the server event loop.
 * Compile-time dispatch: Linux -> epoll, macOS/BSD -> kqueue, else -> poll.
 */

#ifndef GRAVELDB_IO_POLLER_H_
#define GRAVELDB_IO_POLLER_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Event flags */
#define IO_EVENT_READ   0x01
#define IO_EVENT_WRITE  0x02
#define IO_EVENT_ERROR  0x04
#define IO_EVENT_HUP    0x08

/* Fired event */
typedef struct {
    int       fd;
    uint32_t  events;    /* bitmask of IO_EVENT_* */
    void     *userdata;
} IOEvent;

/* Opaque poller handle */
typedef struct IOPoller IOPoller;

/* Create a new poller instance. Returns NULL on failure. */
IOPoller *io_poller_create(int max_events);

/* Destroy and free all resources. */
void io_poller_destroy(IOPoller *p);

/* Add a file descriptor to the poller.
 * events: bitmask of IO_EVENT_READ / IO_EVENT_WRITE.
 * userdata: opaque pointer returned in IOEvent on fire.
 * Returns 0 on success, -1 on error. */
int io_poller_add(IOPoller *p, int fd, uint32_t events, void *userdata);

/* Modify monitored events for an existing fd. Returns 0/-1. */
int io_poller_mod(IOPoller *p, int fd, uint32_t events, void *userdata);

/* Remove fd from the poller. Returns 0/-1. */
int io_poller_del(IOPoller *p, int fd);

/* Wait for events. Returns number of fired events (0 on timeout, -1 on error).
 * timeout_ms: -1 = block indefinitely, 0 = non-blocking, >0 = milliseconds. */
int io_poller_wait(IOPoller *p, IOEvent *out, int max_events, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_IO_POLLER_H_ */
