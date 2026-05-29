/*
 * GravelDB Server - Implementation
 *
 * Single-threaded event loop with non-blocking I/O (poll-based).
 * Checkpoint scheduling delegated to CkptScheduler (cooperative tick).
 */

#include "server.h"
#include "../../src/checkpoint.h"
#include "io_poller.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>

#define MAX_CLIENTS     256
#define READ_BUF_SIZE   (16 * 1024 * 1024)

/* ===== Request-scoped Arena Allocator (nginx-style pool) =====
 *
 * Each connection owns a small arena. All temporary allocations during
 * request processing (e.g. the float buffer in handle_pull) come from
 * this arena. After the response is built the arena is reset to zero —
 * no individual free() calls needed.
 *
 * The arena starts with a single inline block; if a request needs more
 * space it chains additional blocks. On reset, extra blocks are freed
 * and only the first (inline) block is kept, so the common case is
 * zero malloc/free per request.
 */

#define ARENA_INLINE_SIZE (64 * 1024)  /* 64 KB inline block */

typedef struct ArenaBlock {
    struct ArenaBlock *next;
    size_t  cap;
    size_t  used;
    uint8_t *data;
} ArenaBlock;

typedef struct {
    uint8_t     inline_data[ARENA_INLINE_SIZE];
    ArenaBlock  first;       /* uses inline_data as its backing store */
    ArenaBlock *current;     /* current block we're allocating from */
    ArenaBlock *overflow;    /* linked list of overflow blocks */
    size_t      total_alloc; /* stats: bytes allocated this request */
} ReqArena;

static void arena_init(ReqArena *a) {
    a->first.next = NULL;
    a->first.cap  = ARENA_INLINE_SIZE;
    a->first.used = 0;
    a->first.data = a->inline_data;
    a->current    = &a->first;
    a->overflow   = NULL;
    a->total_alloc = 0;
}

static void arena_reset(ReqArena *a) {
    /* Free overflow blocks */
    ArenaBlock *blk = a->overflow;
    while (blk) {
        ArenaBlock *next = blk->next;
        free(blk->data);
        free(blk);
        blk = next;
    }
    a->overflow    = NULL;
    a->first.used  = 0;
    a->first.next  = NULL;
    a->current     = &a->first;
    a->total_alloc = 0;
}

static void arena_destroy(ReqArena *a) {
    arena_reset(a);  /* frees overflow blocks */
}

static void *arena_alloc(ReqArena *a, size_t size) {
    /* Align to 8 bytes */
    size = (size + 7) & ~(size_t)7;

    ArenaBlock *blk = a->current;
    if (blk->used + size <= blk->cap) {
        void *ptr = blk->data + blk->used;
        blk->used += size;
        a->total_alloc += size;
        return ptr;
    }

    /* Need a new overflow block — at least 'size' or double current block */
    size_t new_cap = blk->cap * 2;
    if (new_cap < size) new_cap = size;

    ArenaBlock *nb = (ArenaBlock *)malloc(sizeof(ArenaBlock));
    if (!nb) return NULL;
    nb->data = (uint8_t *)malloc(new_cap);
    if (!nb->data) { free(nb); return NULL; }
    nb->next = NULL;
    nb->cap  = new_cap;
    nb->used = size;

    /* Link into overflow list */
    nb->next = a->overflow;
    a->overflow = nb;
    a->current  = nb;
    a->total_alloc += size;
    return nb->data;
}

/* ===== Arena-backed GravelDBCtx adapter =====
 *
 * Wraps the per-connection ReqArena as a GravelDBCtx so that engine-internal
 * temporary allocations (order, key_batch, deferred) go through the arena.
 * dealloc is NULL → arena mode: engine never frees, we reset after response.
 */
static void *arena_ctx_alloc(void *opaque, size_t size) {
    return arena_alloc((ReqArena *)opaque, size);
}

static inline GravelDBCtx make_arena_ctx(ReqArena *arena) {
    GravelDBCtx ctx;
    ctx.opaque  = arena;
    ctx.alloc   = arena_ctx_alloc;
    ctx.dealloc = NULL;  /* arena mode: caller (server) manages lifetime */
    return ctx;
}

typedef struct {
    int       fd;
    uint8_t  *recv_buf;
    size_t    recv_len;
    size_t    recv_cap;
    uint8_t  *send_buf;
    size_t    send_len;
    size_t    send_off;
    size_t    send_cap;
    ReqArena  arena;      /* per-request scratch allocator */
} ClientConn;

struct GravelServer {
    GravelServerConfig config;
    GravelDB      *db;
    CkptScheduler  ckpt_sched;
    int            listen_fd;
    volatile int   running;

    ClientConn     clients[MAX_CLIENTS];
    int            num_clients;

    IOPoller      *poller;
    pthread_t      server_thread;
};

static uint64_t now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_tcp_nodelay(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

static ClientConn *find_free_slot(GravelServer *srv) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (srv->clients[i].fd == -1) return &srv->clients[i];
    }
    return NULL;
}

static void client_init(ClientConn *c, int fd) {
    c->fd = fd;
    c->recv_cap = READ_BUF_SIZE;
    c->recv_buf = (uint8_t *)malloc(c->recv_cap);
    c->recv_len = 0;
    c->send_cap = READ_BUF_SIZE;
    c->send_buf = (uint8_t *)malloc(c->send_cap);
    c->send_len = 0;
    c->send_off = 0;
    arena_init(&c->arena);
}

static void client_close(ClientConn *c) {
    if (c->fd >= 0) close(c->fd);
    free(c->recv_buf);
    free(c->send_buf);
    arena_destroy(&c->arena);
    c->fd = -1;
    c->recv_buf = NULL;
    c->send_buf = NULL;
    c->recv_len = 0;
    c->send_len = 0;
}

static void response_begin(ClientConn *c, graveldb_wire_status_t status) {
    c->send_len = 0;
    c->send_off = 0;

    uint32_t magic = GRAVELDB_WIRE_MAGIC;
    uint32_t st = (uint32_t)status;
    uint32_t body_len = 0;

    memcpy(c->send_buf, &magic, 4);
    memcpy(c->send_buf + 4, &st, 4);
    memcpy(c->send_buf + 8, &body_len, 4);
    c->send_len = GRAVELDB_WIRE_HEADER_SIZE;
}

static void response_append(ClientConn *c, const void *data, size_t len) {
    if (c->send_len + len > c->send_cap) {
        size_t new_cap = (c->send_len + len) * 2;
        uint8_t *tmp = (uint8_t *)realloc(c->send_buf, new_cap);
        if (!tmp) return;
        c->send_buf = tmp;
        c->send_cap = new_cap;
    }
    memcpy(c->send_buf + c->send_len, data, len);
    c->send_len += len;
}

static void response_finish(ClientConn *c) {
    uint32_t body_len = (uint32_t)(c->send_len - GRAVELDB_WIRE_HEADER_SIZE);
    memcpy(c->send_buf + 8, &body_len, 4);
}

static void handle_pull(GravelServer *srv, ClientConn *c, const uint8_t *body, uint32_t body_len) {
    if (body_len < 4) {
        response_begin(c, GRAVELDB_WIRE_INVALID);
        response_finish(c);
        return;
    }

    uint32_t count;
    memcpy(&count, body, 4);

    if (count > GRAVELDB_WIRE_MAX_BATCH || body_len < 4 + count * 8) {
        response_begin(c, GRAVELDB_WIRE_INVALID);
        response_finish(c);
        return;
    }

    GravelDBCtx ctx = make_arena_ctx(&c->arena);

    /* Parse all feat_ids upfront (8 bytes each, contiguous in wire) */
    const uint8_t *ptr = body + 4;
    uint64_t *feat_ids = (uint64_t *)arena_alloc(&c->arena, count * sizeof(uint64_t));
    if (!feat_ids) {
        response_begin(c, GRAVELDB_WIRE_ERR);
        response_finish(c);
        return;
    }
    for (uint32_t i = 0; i < count; i++) {
        memcpy(&feat_ids[i], ptr, 8);
        ptr += 8;
    }

    response_begin(c, GRAVELDB_WIRE_OK);
    response_append(c, &count, 4);

    /*
     * Process in chunks to keep scratch memory bounded (~256KB per chunk).
     * This preserves batch_get's cache-friendly probe loop while avoiding
     * arena over-allocation for very large batches.
     */
    #define PULL_CHUNK_SIZE 64
    #define PULL_MAX_DIM   1024

    /* Allocate fixed-size scratch for one chunk — reused across chunks */
    float  *chunk_scratch = (float *)arena_alloc(&c->arena, (size_t)PULL_CHUNK_SIZE * PULL_MAX_DIM * sizeof(float));
    float **chunk_bufs    = (float **)arena_alloc(&c->arena, PULL_CHUNK_SIZE * sizeof(float *));
    int    *chunk_dims    = (int *)arena_alloc(&c->arena, PULL_CHUNK_SIZE * sizeof(int));

    if (!chunk_scratch || !chunk_bufs || !chunk_dims) {
        response_begin(c, GRAVELDB_WIRE_ERR);
        response_finish(c);
        return;
    }

    for (uint32_t i = 0; i < (uint32_t)PULL_CHUNK_SIZE; i++) {
        chunk_bufs[i] = chunk_scratch + (size_t)i * PULL_MAX_DIM;
    }

    for (uint32_t off = 0; off < count; off += PULL_CHUNK_SIZE) {
        uint32_t chunk_n = count - off;
        if (chunk_n > PULL_CHUNK_SIZE) chunk_n = PULL_CHUNK_SIZE;

        /* Reset dims for this chunk */
        for (uint32_t i = 0; i < chunk_n; i++) {
            chunk_dims[i] = 0;
        }

        graveldb_batch_get(srv->db, &ctx, feat_ids + off, (int)chunk_n, chunk_bufs, chunk_dims);

        for (uint32_t i = 0; i < chunk_n; i++) {
            uint32_t d = (uint32_t)chunk_dims[i];
            response_append(c, &d, 4);
            if (d > 0) {
                response_append(c, chunk_bufs[i], d * sizeof(float));
            }
        }
    }

    #undef PULL_CHUNK_SIZE
    #undef PULL_MAX_DIM

    /* No free() — arena will be reset after process_message returns */
    response_finish(c);
}

static void handle_push(GravelServer *srv, ClientConn *c, const uint8_t *body, uint32_t body_len) {
    if (body_len < 4) {
        response_begin(c, GRAVELDB_WIRE_INVALID);
        response_finish(c);
        return;
    }

    uint32_t count;
    memcpy(&count, body, 4);

    if (count > GRAVELDB_WIRE_MAX_BATCH) {
        response_begin(c, GRAVELDB_WIRE_INVALID);
        response_finish(c);
        return;
    }

    GravelDBCtx ctx = make_arena_ctx(&c->arena);

    /* Allocate temporary arrays from arena — zero free() needed */
    uint64_t     *feat_ids   = (uint64_t *)arena_alloc(&c->arena, count * sizeof(uint64_t));
    int          *dims       = (int *)arena_alloc(&c->arena, count * sizeof(int));
    const float **embeddings = (const float **)arena_alloc(&c->arena, count * sizeof(const float *));

    if (!feat_ids || !dims || !embeddings) {
        response_begin(c, GRAVELDB_WIRE_ERR);
        response_finish(c);
        return;
    }

    const uint8_t *ptr = body + 4;
    const uint8_t *end = body + body_len;
    uint32_t actual = 0;

    for (uint32_t i = 0; i < count; i++) {
        if (ptr + 12 > end) break;

        uint64_t feat_id;
        uint32_t dim;
        memcpy(&feat_id, ptr, 8); ptr += 8;
        memcpy(&dim, ptr, 4); ptr += 4;

        if (ptr + dim * sizeof(float) > end) break;

        feat_ids[actual]   = feat_id;
        dims[actual]       = (int)dim;
        embeddings[actual] = (const float *)ptr;
        actual++;
        ptr += dim * sizeof(float);
    }

    if (actual > 0) {
        graveldb_batch_put(srv->db, &ctx, feat_ids, dims, embeddings, (int)actual);
    }

    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_delete(GravelServer *srv, ClientConn *c, const uint8_t *body, uint32_t body_len) {
    if (body_len < 4) {
        response_begin(c, GRAVELDB_WIRE_INVALID);
        response_finish(c);
        return;
    }

    uint32_t count;
    memcpy(&count, body, 4);
    const uint8_t *ptr = body + 4;

    for (uint32_t i = 0; i < count && ptr + 8 <= body + body_len; i++) {
        uint64_t feat_id;
        memcpy(&feat_id, ptr, 8); ptr += 8;
        graveldb_delete(srv->db, NULL, feat_id);
    }

    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_flush(GravelServer *srv, ClientConn *c) {
    ckpt_scheduler_force_flush(&srv->ckpt_sched);
    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_checkpoint(GravelServer *srv, ClientConn *c) {
    ckpt_scheduler_force_checkpoint(&srv->ckpt_sched);
    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_stats(GravelServer *srv, ClientConn *c) {
    GravelDBStats stats;
    graveldb_stats(srv->db, &stats);

    response_begin(c, GRAVELDB_WIRE_OK);
    response_append(c, &stats, sizeof(stats));
    response_finish(c);
}

static void handle_ping(GravelServer *srv, ClientConn *c) {
    (void)srv;
    response_begin(c, GRAVELDB_WIRE_OK);
    uint32_t pong = 0x504F4E47;
    response_append(c, &pong, 4);
    response_finish(c);
}

static int process_message(GravelServer *srv, ClientConn *c) {
    if (c->recv_len < GRAVELDB_WIRE_HEADER_SIZE) return 0;

    uint32_t magic, msg_type, body_len;
    memcpy(&magic, c->recv_buf, 4);
    memcpy(&msg_type, c->recv_buf + 4, 4);
    memcpy(&body_len, c->recv_buf + 8, 4);

    if (magic != GRAVELDB_WIRE_MAGIC) {
        return -1;
    }

    if (c->recv_len < GRAVELDB_WIRE_HEADER_SIZE + body_len) return 0;

    const uint8_t *body = c->recv_buf + GRAVELDB_WIRE_HEADER_SIZE;

    switch ((graveldb_msg_type_t)msg_type) {
        case GRAVELDB_MSG_PULL:       handle_pull(srv, c, body, body_len); break;
        case GRAVELDB_MSG_PUSH:       handle_push(srv, c, body, body_len); break;
        case GRAVELDB_MSG_DELETE:     handle_delete(srv, c, body, body_len); break;
        case GRAVELDB_MSG_FLUSH:     handle_flush(srv, c); break;
        case GRAVELDB_MSG_CHECKPOINT: handle_checkpoint(srv, c); break;
        case GRAVELDB_MSG_STATS:     handle_stats(srv, c); break;
        case GRAVELDB_MSG_PING:      handle_ping(srv, c); break;
        default:
            response_begin(c, GRAVELDB_WIRE_INVALID);
            response_finish(c);
            break;
    }

    /* Reset the per-request arena — all scratch memory is reclaimed in O(1) */
    arena_reset(&c->arena);

    size_t consumed = GRAVELDB_WIRE_HEADER_SIZE + body_len;
    size_t remaining = c->recv_len - consumed;
    if (remaining > 0) {
        memmove(c->recv_buf, c->recv_buf + consumed, remaining);
    }
    c->recv_len = remaining;

    return 1;
}

#define LISTEN_SENTINEL ((void *)(uintptr_t)0xDEAD0001)

static void *server_loop(void *arg) {
    GravelServer *srv = (GravelServer *)arg;
    IOEvent events[MAX_CLIENTS + 1];

    while (srv->running) {
        int n = io_poller_wait(srv->poller, events, MAX_CLIENTS + 1, 100);

        ckpt_scheduler_tick(&srv->ckpt_sched, now_ms());

        if (n <= 0) continue;

        for (int i = 0; i < n; i++) {
            IOEvent *ev = &events[i];
            if (ev->userdata == LISTEN_SENTINEL) {
                struct sockaddr_in addr;
                socklen_t addr_len = sizeof(addr);
                int client_fd = accept(srv->listen_fd, (struct sockaddr *)&addr, &addr_len);
                if (client_fd >= 0) {
                    set_nonblocking(client_fd);
                    set_tcp_nodelay(client_fd);
                    ClientConn *slot = find_free_slot(srv);
                    if (slot) {
                        client_init(slot, client_fd);
                        srv->num_clients++;
                        io_poller_add(srv->poller, client_fd, IO_EVENT_READ, slot);
                    } else {
                        close(client_fd);
                    }
                }
                continue;
            }
            ClientConn *c = (ClientConn *)ev->userdata;
            if (c->fd < 0) continue;

            /* Read */
            if (ev->events & (IO_EVENT_READ | IO_EVENT_HUP | IO_EVENT_ERROR)) {
                /* Ensure we have room to read */
                if (c->recv_len >= c->recv_cap) {
                    size_t new_cap = c->recv_cap * 2;
                    if (new_cap > (size_t)srv->config.max_request_size) {
                        new_cap = (size_t)srv->config.max_request_size;
                    }
                    if (new_cap <= c->recv_cap) {
                        /* Buffer at max, client sending too much */
                        io_poller_del(srv->poller, c->fd);
                        client_close(c);
                        srv->num_clients--;
                        continue;
                    }
                    uint8_t *tmp = (uint8_t *)realloc(c->recv_buf, new_cap);
                    if (!tmp) {
                        io_poller_del(srv->poller, c->fd);
                        client_close(c);
                        srv->num_clients--;
                        continue;
                    }
                    c->recv_buf = tmp;
                    c->recv_cap = new_cap;
                }

                ssize_t rd = read(c->fd, c->recv_buf + c->recv_len,
                                  c->recv_cap - c->recv_len);
                if (rd <= 0) {
                    io_poller_del(srv->poller, c->fd);
                    client_close(c);
                    srv->num_clients--;
                    continue;
                }
                c->recv_len += rd;

                int rc;
                while ((rc = process_message(srv, c)) > 0) {}
                if (rc < 0) {
                    io_poller_del(srv->poller, c->fd);
                    client_close(c);
                    srv->num_clients--;
                    continue;
                }
            }

            /* Write — try to flush pending response data immediately */
            if (c->send_len > c->send_off) {
                size_t to_send = c->send_len - c->send_off;
                ssize_t wr = write(c->fd, c->send_buf + c->send_off, to_send);
                if (wr > 0) {
                    c->send_off += wr;
                    if (c->send_off >= c->send_len) {
                        c->send_off = 0;
                        c->send_len = 0;
                    }
                } else if (wr < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    io_poller_del(srv->poller, c->fd);
                    client_close(c);
                    srv->num_clients--;
                    continue;
                }
            }

            if (c->fd >= 0) {
                uint32_t interest = IO_EVENT_READ;
                if (c->send_len > c->send_off) {
                    interest |= IO_EVENT_WRITE;
                }
                io_poller_mod(srv->poller, c->fd, interest, c);
            }
        }
    }

    return NULL;
}

graveldb_status_t gravel_server_create(GravelServer **out, const GravelServerConfig *config) {
    GravelServer *srv = (GravelServer *)calloc(1, sizeof(GravelServer));
    if (!srv) return GRAVELDB_ERR_OOM;

    srv->config = *config;
    srv->listen_fd = -1;
    srv->running = 0;

    for (int i = 0; i < MAX_CLIENTS; i++) {
        srv->clients[i].fd = -1;
    }

    srv->poller = io_poller_create(MAX_CLIENTS + 16);
    if (!srv->poller) {
        free(srv);
        return GRAVELDB_ERR_OOM;
    }

    graveldb_status_t rc = graveldb_open(&srv->db, &config->db_config);
    if (rc != GRAVELDB_OK) {
        io_poller_destroy(srv->poller);
        free(srv);
        return rc;
    }

    CkptConfig ckpt_cfg = {
        .flush_interval_ms     = config->auto_flush_interval_ms > 0
                                 ? (uint32_t)config->auto_flush_interval_ms : 1000,
        .flush_dirty_threshold = 4096,
        .checkpoint_interval_s = config->auto_checkpoint_interval_s > 0
                                 ? (uint32_t)config->auto_checkpoint_interval_s : 60,
        .full_cooldown_ms      = 60000,  /* 60s cooldown between full checkpoints */
        .auto_recover_on_open  = true,
    };
    rc = ckpt_scheduler_init(&srv->ckpt_sched, srv->db, &ckpt_cfg);
    if (rc != GRAVELDB_OK) {
        graveldb_close(srv->db);
        io_poller_destroy(srv->poller);
        free(srv);
        return rc;
    }

    *out = srv;
    return GRAVELDB_OK;
}

graveldb_status_t gravel_server_start(GravelServer *srv) {
    int port = srv->config.port > 0 ? srv->config.port : GRAVELDB_WIRE_PORT;
    int backlog = srv->config.backlog > 0 ? srv->config.backlog : 128;

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) return GRAVELDB_ERR_IO;

    int one = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY,
    };

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(srv->listen_fd);
        return GRAVELDB_ERR_IO;
    }

    if (listen(srv->listen_fd, backlog) < 0) {
        close(srv->listen_fd);
        return GRAVELDB_ERR_IO;
    }

    set_nonblocking(srv->listen_fd);

    io_poller_add(srv->poller, srv->listen_fd, IO_EVENT_READ, LISTEN_SENTINEL);

    srv->running = 1;

    pthread_create(&srv->server_thread, NULL, server_loop, srv);

    fprintf(stderr, "[GravelServer] Listening on port %d (using "
#if defined(__linux__)
            "epoll"
#elif defined(__APPLE__) || defined(__FreeBSD__)
            "kqueue"
#else
            "poll"
#endif
            ")\n", port);
    return GRAVELDB_OK;
}

void gravel_server_stop(GravelServer *srv) {
    if (!srv || !srv->running) return;

    srv->running = 0;
    pthread_join(srv->server_thread, NULL);

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (srv->clients[i].fd >= 0) {
            io_poller_del(srv->poller, srv->clients[i].fd);
            client_close(&srv->clients[i]);
        }
    }

    if (srv->listen_fd >= 0) {
        io_poller_del(srv->poller, srv->listen_fd);
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }

    ckpt_scheduler_force_flush(&srv->ckpt_sched);
    ckpt_scheduler_force_checkpoint(&srv->ckpt_sched);
}

void gravel_server_destroy(GravelServer *srv) {
    if (!srv) return;
    gravel_server_stop(srv);
    ckpt_scheduler_destroy(&srv->ckpt_sched);
    graveldb_close(srv->db);
    io_poller_destroy(srv->poller);
    free(srv);
}

GravelDB *gravel_server_get_db(GravelServer *srv) {
    return srv->db;
}
