/*
 * GravelDB - I/O Poller Implementation
 *
 * Platform dispatch:
 *   - Linux:      epoll
 *   - macOS/BSD:  kqueue
 *   - Fallback:   poll
 */

#include "io_poller.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

/*
 * epoll backend (Linux)
 */
#if defined(__linux__)

#include <sys/epoll.h>

struct IOPoller {
    int epfd;
    int max_events;
    struct epoll_event *ep_events;  /* scratch buffer for epoll_wait */
};

IOPoller *io_poller_create(int max_events) {
    IOPoller *p = (IOPoller *)calloc(1, sizeof(IOPoller));
    if (!p) return NULL;

    p->epfd = epoll_create1(0);
    if (p->epfd < 0) { free(p); return NULL; }

    p->max_events = max_events > 0 ? max_events : 256;
    p->ep_events = (struct epoll_event *)calloc(p->max_events, sizeof(struct epoll_event));
    if (!p->ep_events) { close(p->epfd); free(p); return NULL; }

    return p;
}

void io_poller_destroy(IOPoller *p) {
    if (!p) return;
    close(p->epfd);
    free(p->ep_events);
    free(p);
}

static uint32_t to_epoll_events(uint32_t ev) {
    uint32_t e = 0;
    if (ev & IO_EVENT_READ)  e |= EPOLLIN;
    if (ev & IO_EVENT_WRITE) e |= EPOLLOUT;
    return e;
}

static uint32_t from_epoll_events(uint32_t ep) {
    uint32_t ev = 0;
    if (ep & EPOLLIN)   ev |= IO_EVENT_READ;
    if (ep & EPOLLOUT)  ev |= IO_EVENT_WRITE;
    if (ep & EPOLLERR)  ev |= IO_EVENT_ERROR;
    if (ep & EPOLLHUP)  ev |= IO_EVENT_HUP;
    return ev;
}

int io_poller_add(IOPoller *p, int fd, uint32_t events, void *userdata) {
    struct epoll_event ev;
    ev.events = to_epoll_events(events);
    ev.data.ptr = userdata;
    return epoll_ctl(p->epfd, EPOLL_CTL_ADD, fd, &ev);
}

int io_poller_mod(IOPoller *p, int fd, uint32_t events, void *userdata) {
    struct epoll_event ev;
    ev.events = to_epoll_events(events);
    ev.data.ptr = userdata;
    return epoll_ctl(p->epfd, EPOLL_CTL_MOD, fd, &ev);
}

int io_poller_del(IOPoller *p, int fd) {
    return epoll_ctl(p->epfd, EPOLL_CTL_DEL, fd, NULL);
}

int io_poller_wait(IOPoller *p, IOEvent *out, int max_events, int timeout_ms) {
    int cap = max_events < p->max_events ? max_events : p->max_events;
    int n = epoll_wait(p->epfd, p->ep_events, cap, timeout_ms);
    if (n < 0) return (errno == EINTR) ? 0 : -1;

    for (int i = 0; i < n; i++) {
        out[i].fd = -1;  /* fd not stored in epoll_event; use userdata */
        out[i].events = from_epoll_events(p->ep_events[i].events);
        out[i].userdata = p->ep_events[i].data.ptr;
    }
    return n;
}

/*
 * kqueue backend (macOS / FreeBSD / NetBSD / OpenBSD)
 */
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)

#include <sys/event.h>
#include <sys/time.h>

struct IOPoller {
    int kqfd;
    int max_events;
    struct kevent *kev_buf;  /* scratch buffer for kevent() */
};

IOPoller *io_poller_create(int max_events) {
    IOPoller *p = (IOPoller *)calloc(1, sizeof(IOPoller));
    if (!p) return NULL;

    p->kqfd = kqueue();
    if (p->kqfd < 0) { free(p); return NULL; }

    p->max_events = max_events > 0 ? max_events : 256;
    p->kev_buf = (struct kevent *)calloc(p->max_events, sizeof(struct kevent));
    if (!p->kev_buf) { close(p->kqfd); free(p); return NULL; }

    return p;
}

void io_poller_destroy(IOPoller *p) {
    if (!p) return;
    close(p->kqfd);
    free(p->kev_buf);
    free(p);
}

int io_poller_add(IOPoller *p, int fd, uint32_t events, void *userdata) {
    struct kevent changes[2];
    int nchanges = 0;

    if (events & IO_EVENT_READ) {
        EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, userdata);
        nchanges++;
    }
    if (events & IO_EVENT_WRITE) {
        EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, userdata);
        nchanges++;
    }

    if (nchanges == 0) return -1;
    return kevent(p->kqfd, changes, nchanges, NULL, 0, NULL) < 0 ? -1 : 0;
}

int io_poller_mod(IOPoller *p, int fd, uint32_t events, void *userdata) {
    /*
     * kqueue: delete old filters, then add new ones.
     *
     * IMPORTANT: We must split into two kevent() calls because if a
     * changelist entry fails (e.g. EV_DELETE on a non-existent filter),
     * kqueue stops processing subsequent entries in the same call.
     */
    struct kevent del_changes[2];
    EV_SET(&del_changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&del_changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    /* Ignore errors — filters might not exist */
    kevent(p->kqfd, &del_changes[0], 1, NULL, 0, NULL);
    kevent(p->kqfd, &del_changes[1], 1, NULL, 0, NULL);

    /* Re-add desired filters */
    struct kevent add_changes[2];
    int nchanges = 0;
    if (events & IO_EVENT_READ) {
        EV_SET(&add_changes[nchanges], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, userdata);
        nchanges++;
    }
    if (events & IO_EVENT_WRITE) {
        EV_SET(&add_changes[nchanges], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, userdata);
        nchanges++;
    }

    if (nchanges > 0) {
        if (kevent(p->kqfd, add_changes, nchanges, NULL, 0, NULL) < 0) {
            return -1;
        }
    }
    return 0;
}

int io_poller_del(IOPoller *p, int fd) {
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    /* Ignore errors (filter might not exist) */
    kevent(p->kqfd, changes, 2, NULL, 0, NULL);
    return 0;
}

int io_poller_wait(IOPoller *p, IOEvent *out, int max_events, int timeout_ms) {
    int cap = max_events < p->max_events ? max_events : p->max_events;

    struct timespec ts;
    struct timespec *tsp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        tsp = &ts;
    }

    int n = kevent(p->kqfd, NULL, 0, p->kev_buf, cap, tsp);
    if (n < 0) return (errno == EINTR) ? 0 : -1;

    for (int i = 0; i < n; i++) {
        out[i].fd = (int)p->kev_buf[i].ident;
        out[i].userdata = p->kev_buf[i].udata;
        out[i].events = 0;

        if (p->kev_buf[i].filter == EVFILT_READ)  out[i].events |= IO_EVENT_READ;
        if (p->kev_buf[i].filter == EVFILT_WRITE) out[i].events |= IO_EVENT_WRITE;
        if (p->kev_buf[i].flags & EV_EOF)         out[i].events |= IO_EVENT_HUP;
        if (p->kev_buf[i].flags & EV_ERROR)       out[i].events |= IO_EVENT_ERROR;
    }
    return n;
}

/*
 * poll fallback (portable)
 */
#else

#include <poll.h>

typedef struct {
    int       fd;
    uint32_t  events;
    void     *userdata;
} PollEntry;

struct IOPoller {
    PollEntry *entries;
    int        count;
    int        capacity;
};

IOPoller *io_poller_create(int max_events) {
    IOPoller *p = (IOPoller *)calloc(1, sizeof(IOPoller));
    if (!p) return NULL;

    p->capacity = max_events > 0 ? max_events : 256;
    p->entries = (PollEntry *)calloc(p->capacity, sizeof(PollEntry));
    if (!p->entries) { free(p); return NULL; }

    for (int i = 0; i < p->capacity; i++) p->entries[i].fd = -1;
    p->count = 0;
    return p;
}

void io_poller_destroy(IOPoller *p) {
    if (!p) return;
    free(p->entries);
    free(p);
}

static int find_entry(IOPoller *p, int fd) {
    for (int i = 0; i < p->capacity; i++) {
        if (p->entries[i].fd == fd) return i;
    }
    return -1;
}

int io_poller_add(IOPoller *p, int fd, uint32_t events, void *userdata) {
    for (int i = 0; i < p->capacity; i++) {
        if (p->entries[i].fd == -1) {
            p->entries[i].fd = fd;
            p->entries[i].events = events;
            p->entries[i].userdata = userdata;
            p->count++;
            return 0;
        }
    }
    return -1; /* full */
}

int io_poller_mod(IOPoller *p, int fd, uint32_t events, void *userdata) {
    int idx = find_entry(p, fd);
    if (idx < 0) return -1;
    p->entries[idx].events = events;
    p->entries[idx].userdata = userdata;
    return 0;
}

int io_poller_del(IOPoller *p, int fd) {
    int idx = find_entry(p, fd);
    if (idx < 0) return -1;
    p->entries[idx].fd = -1;
    p->entries[idx].userdata = NULL;
    p->count--;
    return 0;
}

int io_poller_wait(IOPoller *p, IOEvent *out, int max_events, int timeout_ms) {
    /* Build pollfd array */
    struct pollfd *pfds = (struct pollfd *)malloc(p->count * sizeof(struct pollfd));
    if (!pfds) return -1;

    int *idx_map = (int *)malloc(p->count * sizeof(int));
    if (!idx_map) { free(pfds); return -1; }

    int nfds = 0;
    for (int i = 0; i < p->capacity && nfds < p->count; i++) {
        if (p->entries[i].fd < 0) continue;
        pfds[nfds].fd = p->entries[i].fd;
        pfds[nfds].events = 0;
        if (p->entries[i].events & IO_EVENT_READ)  pfds[nfds].events |= POLLIN;
        if (p->entries[i].events & IO_EVENT_WRITE) pfds[nfds].events |= POLLOUT;
        pfds[nfds].revents = 0;
        idx_map[nfds] = i;
        nfds++;
    }

    int ready = poll(pfds, nfds, timeout_ms);
    if (ready <= 0) { free(pfds); free(idx_map); return ready == 0 ? 0 : -1; }

    int fired = 0;
    for (int i = 0; i < nfds && fired < max_events; i++) {
        if (pfds[i].revents == 0) continue;
        int ei = idx_map[i];
        out[fired].fd = p->entries[ei].fd;
        out[fired].userdata = p->entries[ei].userdata;
        out[fired].events = 0;
        if (pfds[i].revents & POLLIN)   out[fired].events |= IO_EVENT_READ;
        if (pfds[i].revents & POLLOUT)  out[fired].events |= IO_EVENT_WRITE;
        if (pfds[i].revents & POLLERR)  out[fired].events |= IO_EVENT_ERROR;
        if (pfds[i].revents & POLLHUP)  out[fired].events |= IO_EVENT_HUP;
        fired++;
    }

    free(pfds);
    free(idx_map);
    return fired;
}

#endif /* platform dispatch */
