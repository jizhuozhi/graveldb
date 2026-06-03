/*
 * GravelDB Server - Implementation
 *
 * Single-threaded event loop with non-blocking I/O (poll-based).
 * Checkpoint scheduling delegated to CkptScheduler (cooperative tick).
 */

#include "server.h"
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
#include <sys/uio.h>
#include <pthread.h>

#define MAX_CLIENTS     256
#define READ_BUF_SIZE   (16 * 1024 * 1024)

/* Request-scoped Arena Allocator (nginx-style pool)
 *
 * Each connection owns a single arena. All allocations during request
 * processing — scratch buffers, SendChunk structs, response body buffers —
 * come from this arena.
 *
 * Lifetime: the arena is NOT reset after process_message returns. Instead,
 * it stays alive until client_flush_send has written out all pending chunks
 * (i.e. the send queue is drained). At that point the arena is reset in O(1).
 * This ensures SendChunks remain valid through writev without needing any
 * per-chunk malloc/free or freelist.
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

/*
 * SendChunk: linked list of output buffers for scatter-write.
 * Each response occupies one chunk. On flush we writev() all pending
 * chunks, then release completed ones. No realloc, no memmove.
 */
#define SEND_CHUNK_INLINE 4096

typedef struct SendChunk {
    struct SendChunk *next;
    uint8_t *data;
    size_t   len;       /* bytes written into this chunk */
    size_t   cap;
    size_t   sent;      /* bytes already sent (partial write) */
    uint8_t  inline_buf[SEND_CHUNK_INLINE];
} SendChunk;

/*
 * Per-connection async IO state machine.
 *
 * When a request requires disk IO (pull/pull_stream), we submit the
 * io_uring reads and return control to the event loop instead of blocking.
 * On each event loop iteration, we poll all connections with in-flight IO.
 * Once IO completes, we resume building the response from where we left off.
 *
 * States:
 *   CONN_IDLE        - no request in progress, ready to receive
 *   CONN_ASYNC_PULL  - pull request with in-flight disk IO
 *   CONN_ASYNC_STREAM - streaming pull with in-flight disk IO
 */
typedef enum {
    CONN_IDLE = 0,
    CONN_ASYNC_PULL,
    CONN_ASYNC_STREAM,
} ConnAsyncState;

typedef struct {
    ConnAsyncState  state;
    GravelDBAsyncGet ag;       /* current in-flight async get */
    uint64_t   *feat_ids;      /* parsed feat_ids (in arena) */
    uint32_t    count;         /* total feature count */
    uint32_t    offset;        /* current chunk offset into feat_ids */
    float      *chunk_scratch; /* reusable chunk buffer (in arena) */
    float     **chunk_bufs;    /* per-entry buffer pointers (in arena) */
    int        *chunk_dims;    /* per-entry dim output (in arena) */
    uint32_t    chunk_n;       /* entries in current chunk */
    int         first_frame;   /* streaming: is this the first frame? */
    int         phase;         /* 0=submit, 1=poll/waiting */
} ConnAsyncCtx;

typedef struct {
    int       fd;
    uint8_t  *recv_buf;
    size_t    recv_len;
    size_t    recv_cap;
    SendChunk *send_head;    /* first pending chunk */
    SendChunk *send_tail;    /* last pending chunk (append here) */
    SendChunk *send_cur;     /* chunk currently being built by response_* */
    size_t     resp_hdr_off; /* offset of response header within send_cur */
    ReqArena  arena;         /* per-request arena; lifetime extended until flush completes */
    ConnAsyncCtx async;      /* async IO state machine */
} ClientConn;

/* ===== Read Worker (readonly mode multi-threaded reads) =====
 *
 * Each ReadWorker is an independent event loop thread that handles
 * only pull/ping/stats requests. Because the DB is never mutated in
 * readonly mode, all reads are lock-free — no synchronization needed.
 *
 * The main accept loop distributes new connections to workers via a
 * pipe (round-robin). Each worker has its own poller + client array.
 */
#define WORKER_MAX_CLIENTS  64
#define NOTIFY_SENTINEL     ((void *)(uintptr_t)0xDEAD0002)

typedef struct {
    GravelDB       *db;            /* shared, read-only reference */
    IOPoller       *poller;
    ClientConn      clients[WORKER_MAX_CLIENTS];
    int             num_clients;
    int             notify_fd;     /* read end of pipe for new fd delivery */
    volatile int   *running;       /* pointer to srv->running */
    bool            readonly;
    pthread_t       thread;
    int             worker_id;
} ReadWorker;

struct GravelServer {
    GravelServerConfig config;
    GravelDB      *db;
    uint64_t       last_checkpoint_ms;
    uint32_t       checkpoint_interval_ms;
    int            listen_fd;
    volatile int   running;
    bool           readonly;

    ClientConn     clients[MAX_CLIENTS];
    int            num_clients;

    IOPoller      *poller;
    pthread_t      server_thread;

    /* Async flush state: submitted immediately when flush_needed fires */
    GravelDBAsyncFlush async_flush;
    bool               flush_in_flight;

    /* Readonly mode: worker threads for lock-free concurrent reads */
    int            num_read_workers;
    ReadWorker    *workers;
    int           *worker_pipe_w;    /* write ends of pipes (one per worker) */
    int            rr_next;          /* round-robin counter for accept distribution */
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

/*
 * Allocate a SendChunk from the connection's arena. Since arena lifetime
 * is extended until flush completes, the chunk lives long enough for writev.
 * Zero malloc per request in steady state.
 */
static SendChunk *chunk_arena_new(ClientConn *c) {
    SendChunk *ch = (SendChunk *)arena_alloc(&c->arena, sizeof(SendChunk));
    if (!ch) return NULL;
    ch->next = NULL;
    ch->len  = 0;
    ch->sent = 0;
    ch->data = ch->inline_buf;
    ch->cap  = SEND_CHUNK_INLINE;
    return ch;
}

static void client_init(ClientConn *c, int fd) {
    c->fd = fd;
    c->recv_cap = READ_BUF_SIZE;
    c->recv_buf = (uint8_t *)malloc(c->recv_cap);
    c->recv_len = 0;
    c->send_head = NULL;
    c->send_tail = NULL;
    c->send_cur  = NULL;
    c->resp_hdr_off = 0;
    arena_init(&c->arena);
    memset(&c->async, 0, sizeof(c->async));
    c->async.state = CONN_IDLE;
}

static void client_close(ClientConn *c) {
    if (c->fd >= 0) close(c->fd);
    /* If async IO was in flight, drain it to free internal heap state */
    if (c->async.state != CONN_IDLE && c->async.ag.internal != NULL) {
        while (graveldb_batch_get_poll(&c->async.ag) == GRAVELDB_AGAIN) {}
    }
    c->async.state = CONN_IDLE;
    free(c->recv_buf);
    /* All SendChunks and their grown buffers live in the arena */
    arena_destroy(&c->arena);
    c->fd = -1;
    c->recv_buf = NULL;
    c->send_head = NULL;
    c->send_tail = NULL;
    c->send_cur  = NULL;
    c->recv_len = 0;
}

/* Returns true if there are pending chunks to send */
static inline bool client_has_pending_send(ClientConn *c) {
    return c->send_head != NULL;
}

/*
 * Flush pending send chunks via writev (no arena reset).
 * Returns:
 *   0  = all flushed
 *   1  = partial (EAGAIN), still has pending
 *  -1  = error (connection should be closed)
 */
static int client_flush_send_raw(ClientConn *c) {
    while (c->send_head) {
        /* Build iovec array from chunk list (up to IOV_MAX or 64) */
        struct iovec iov[64];
        int iovcnt = 0;
        SendChunk *ch = c->send_head;
        while (ch && iovcnt < 64) {
            iov[iovcnt].iov_base = ch->data + ch->sent;
            iov[iovcnt].iov_len  = ch->len - ch->sent;
            iovcnt++;
            ch = ch->next;
        }

        ssize_t wr = writev(c->fd, iov, iovcnt);
        if (wr <= 0) {
            if (wr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                return 1;  /* partial */
            return -1;     /* error */
        }

        /* Advance: consume written bytes from head chunks */
        size_t remaining = (size_t)wr;
        while (remaining > 0 && c->send_head) {
            SendChunk *head = c->send_head;
            size_t avail = head->len - head->sent;
            if (remaining >= avail) {
                remaining -= avail;
                c->send_head = head->next;
                if (!c->send_head) c->send_tail = NULL;
                /* chunk struct + data both in arena, no free needed */
            } else {
                head->sent += remaining;
                remaining = 0;
            }
        }
    }
    return 0;  /* all flushed */
}

/*
 * Flush pending send chunks via writev. Non-blocking: sends as much as
 * the socket allows, frees fully-sent chunks, returns:
 *   0  = all flushed (arena reset)
 *   1  = partial (EAGAIN), still has pending
 *  -1  = error (connection should be closed)
 */
static int client_flush_send(ClientConn *c) {
    int rc = client_flush_send_raw(c);
    if (rc == 0) {
        /* All chunks flushed — reset arena to reclaim all memory in O(1) */
        arena_reset(&c->arena);
    }
    return rc;
}

static void response_begin(ClientConn *c, graveldb_wire_status_t status) {
    /* Allocate chunk from arena (zero malloc) */
    SendChunk *ch = chunk_arena_new(c);
    if (!ch) return;
    c->send_cur = ch;
    c->resp_hdr_off = 0;

    uint32_t magic = GRAVELDB_WIRE_MAGIC;
    uint32_t st = (uint32_t)status;
    uint32_t body_len = 0;  /* placeholder, patched by response_finish */

    memcpy(ch->data, &magic, 4);
    memcpy(ch->data + 4, &st, 4);
    memcpy(ch->data + 8, &body_len, 4);
    ch->len = GRAVELDB_WIRE_HEADER_SIZE;
}

static void response_append(ClientConn *c, const void *data, size_t len) {
    SendChunk *ch = c->send_cur;
    if (!ch) return;
    /* Grow chunk if needed: allocate from arena (no individual free needed).
     * Arena doesn't support realloc, so we alloc a new buffer and copy.
     * This only happens for large responses (e.g. big pull batches). */
    if (ch->len + len > ch->cap) {
        size_t new_cap = (ch->len + len) * 2;
        uint8_t *new_buf = (uint8_t *)arena_alloc(&c->arena, new_cap);
        if (!new_buf) return;
        memcpy(new_buf, ch->data, ch->len);
        ch->data = new_buf;
        ch->cap = new_cap;
    }
    memcpy(ch->data + ch->len, data, len);
    ch->len += len;
}

static void response_finish(ClientConn *c) {
    SendChunk *ch = c->send_cur;
    if (!ch) return;

    /* Patch body_len in header */
    uint32_t body_len = (uint32_t)(ch->len - GRAVELDB_WIRE_HEADER_SIZE);
    memcpy(ch->data + 8, &body_len, 4);

    /* Enqueue chunk into send list */
    ch->next = NULL;
    if (c->send_tail) {
        c->send_tail->next = ch;
    } else {
        c->send_head = ch;
    }
    c->send_tail = ch;
    c->send_cur = NULL;
}

#define PULL_CHUNK_SIZE 64
#define PULL_MAX_DIM   1024

/*
 * Submit the next chunk of async IO for a pull request.
 * If all chunks are done, finalize the response and transition to CONN_IDLE.
 * Returns: 1 = IO submitted (connection is in async state)
 *          0 = all done (back to IDLE)
 */
static int pull_submit_next_chunk(GravelServer *srv, ClientConn *c) {
    ConnAsyncCtx *a = &c->async;

    if (a->offset >= a->count) {
        /* All chunks processed — finalize response */
        response_finish(c);
        a->state = CONN_IDLE;
        return 0;
    }

    uint32_t chunk_n = a->count - a->offset;
    if (chunk_n > PULL_CHUNK_SIZE) chunk_n = PULL_CHUNK_SIZE;
    a->chunk_n = chunk_n;

    for (uint32_t i = 0; i < chunk_n; i++) {
        a->chunk_dims[i] = 0;
    }

    GravelDBCtx ctx = make_arena_ctx(&c->arena);
    graveldb_batch_get_submit(srv->db, &ctx, a->feat_ids + a->offset,
                              (int)chunk_n, a->chunk_bufs, a->chunk_dims, &a->ag);

    /* Check if it completed immediately (all in memory) */
    if (graveldb_batch_get_poll(&a->ag) != GRAVELDB_AGAIN) {
        /* Append results, advance to next chunk */
        for (uint32_t i = 0; i < chunk_n; i++) {
            uint32_t d = (uint32_t)a->chunk_dims[i];
            response_append(c, &d, 4);
            if (d > 0) {
                response_append(c, a->chunk_bufs[i], d * sizeof(float));
            }
        }
        a->offset += chunk_n;
        /* Recurse to submit next (tail call) */
        return pull_submit_next_chunk(srv, c);
    }

    /* IO in flight — return to event loop */
    a->phase = 1;
    return 1;
}

/*
 * Resume an in-progress pull after async IO completes.
 * Returns: 1 = still in async state (more IO submitted)
 *          0 = all done (CONN_IDLE)
 */
static int pull_resume(GravelServer *srv, ClientConn *c) {
    ConnAsyncCtx *a = &c->async;

    /* Poll current in-flight IO */
    if (graveldb_batch_get_poll(&a->ag) == GRAVELDB_AGAIN) {
        return 1;  /* still waiting */
    }

    /* IO done — scatter results into response */
    for (uint32_t i = 0; i < a->chunk_n; i++) {
        uint32_t d = (uint32_t)a->chunk_dims[i];
        response_append(c, &d, 4);
        if (d > 0) {
            response_append(c, a->chunk_bufs[i], d * sizeof(float));
        }
    }
    a->offset += a->chunk_n;

    /* Submit next chunk or finalize */
    return pull_submit_next_chunk(srv, c);
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

    /* Parse all feat_ids upfront */
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

    /* Allocate scratch buffers (reused across chunks) */
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

    /* Begin response and set up async state */
    response_begin(c, GRAVELDB_WIRE_OK);
    response_append(c, &count, 4);

    ConnAsyncCtx *a = &c->async;
    a->state = CONN_ASYNC_PULL;
    a->feat_ids = feat_ids;
    a->count = count;
    a->offset = 0;
    a->chunk_scratch = chunk_scratch;
    a->chunk_bufs = chunk_bufs;
    a->chunk_dims = chunk_dims;
    a->phase = 0;

    /* Submit first chunk — may complete synchronously */
    pull_submit_next_chunk(srv, c);
}

/*
 * Streaming pull: sends response in chunked frames so that TCP can begin
 * transmitting data before the entire batch is fetched. Each frame carries
 * one chunk (up to 64 entries). Format:
 *
 *   Header:     [magic][OK][0xFFFFFFFF]  (sentinel body_len = streaming)
 *   Frame 0:    [4B frame_len][4B total_count][entries...]
 *   Frame 1..N: [4B frame_len][entries...]
 *   Terminator: [4B 0]
 *
 * Entry: [4B dim][dim*4B floats]
 *
 * Async state machine: submit one chunk's IO, return to event loop.
 * On resume, build frame from completed IO and submit next chunk.
 */

static void stream_send_terminator(ClientConn *c) {
    SendChunk *term = chunk_arena_new(c);
    if (term) {
        uint32_t zero = 0;
        memcpy(term->data, &zero, 4);
        term->len = 4;
        term->next = NULL;
        if (c->send_tail) c->send_tail->next = term;
        else c->send_head = term;
        c->send_tail = term;
    }
}

static void stream_build_frame(ClientConn *c) {
    ConnAsyncCtx *a = &c->async;
    uint32_t chunk_n = a->chunk_n;

    size_t payload_size = a->first_frame ? 4 : 0;
    for (uint32_t i = 0; i < chunk_n; i++) {
        payload_size += 4 + (size_t)a->chunk_dims[i] * sizeof(float);
    }

    size_t frame_size = 4 + payload_size;
    SendChunk *ch = chunk_arena_new(c);
    if (!ch) return;

    if (frame_size > ch->cap) {
        uint8_t *new_buf = (uint8_t *)arena_alloc(&c->arena, frame_size);
        if (!new_buf) return;
        ch->data = new_buf;
        ch->cap = frame_size;
    }

    uint8_t *wp = ch->data;
    uint32_t frame_len = (uint32_t)payload_size;
    memcpy(wp, &frame_len, 4); wp += 4;

    if (a->first_frame) {
        memcpy(wp, &a->count, 4); wp += 4;
        a->first_frame = 0;
    }

    for (uint32_t i = 0; i < chunk_n; i++) {
        uint32_t d = (uint32_t)a->chunk_dims[i];
        memcpy(wp, &d, 4); wp += 4;
        if (d > 0) {
            memcpy(wp, a->chunk_bufs[i], d * sizeof(float));
            wp += d * sizeof(float);
        }
    }
    ch->len = (size_t)(wp - ch->data);

    ch->next = NULL;
    if (c->send_tail) {
        c->send_tail->next = ch;
    } else {
        c->send_head = ch;
    }
    c->send_tail = ch;
}

/*
 * Submit next chunk IO for streaming pull.
 * Returns: 1 = IO in flight, 0 = done (IDLE)
 */
static int stream_submit_next_chunk(GravelServer *srv, ClientConn *c) {
    ConnAsyncCtx *a = &c->async;

    if (a->offset >= a->count) {
        stream_send_terminator(c);
        a->state = CONN_IDLE;
        return 0;
    }

    uint32_t chunk_n = a->count - a->offset;
    if (chunk_n > PULL_CHUNK_SIZE) chunk_n = PULL_CHUNK_SIZE;
    a->chunk_n = chunk_n;

    for (uint32_t i = 0; i < chunk_n; i++) {
        a->chunk_dims[i] = 0;
    }

    GravelDBCtx ctx = make_arena_ctx(&c->arena);
    graveldb_batch_get_submit(srv->db, &ctx, a->feat_ids + a->offset,
                              (int)chunk_n, a->chunk_bufs, a->chunk_dims, &a->ag);

    if (graveldb_batch_get_poll(&a->ag) != GRAVELDB_AGAIN) {
        /* Completed immediately */
        stream_build_frame(c);
        a->offset += chunk_n;
        return stream_submit_next_chunk(srv, c);
    }

    a->phase = 1;
    return 1;
}

/*
 * Resume streaming pull after IO completes.
 * Returns: 1 = still async, 0 = done
 */
static int stream_resume(GravelServer *srv, ClientConn *c) {
    ConnAsyncCtx *a = &c->async;

    if (graveldb_batch_get_poll(&a->ag) == GRAVELDB_AGAIN) {
        return 1;  /* still waiting */
    }

    stream_build_frame(c);
    a->offset += a->chunk_n;

    return stream_submit_next_chunk(srv, c);
}

static void handle_pull_stream(GravelServer *srv, ClientConn *c, const uint8_t *body, uint32_t body_len) {
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

    /* Parse all feat_ids */
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

    /* Send streaming header: body_len = sentinel */
    {
        SendChunk *ch = chunk_arena_new(c);
        if (!ch) return;
        uint32_t magic = GRAVELDB_WIRE_MAGIC;
        uint32_t st = (uint32_t)GRAVELDB_WIRE_OK;
        uint32_t sentinel = GRAVELDB_WIRE_STREAM_SENTINEL;
        memcpy(ch->data, &magic, 4);
        memcpy(ch->data + 4, &st, 4);
        memcpy(ch->data + 8, &sentinel, 4);
        ch->len = GRAVELDB_WIRE_HEADER_SIZE;
        ch->next = NULL;
        if (c->send_tail) {
            c->send_tail->next = ch;
        } else {
            c->send_head = ch;
        }
        c->send_tail = ch;
    }

    /* Allocate scratch buffers */
    float  *chunk_scratch = (float *)arena_alloc(&c->arena, (size_t)PULL_CHUNK_SIZE * PULL_MAX_DIM * sizeof(float));
    float **chunk_bufs    = (float **)arena_alloc(&c->arena, PULL_CHUNK_SIZE * sizeof(float *));
    int    *chunk_dims    = (int *)arena_alloc(&c->arena, PULL_CHUNK_SIZE * sizeof(int));

    if (!chunk_scratch || !chunk_bufs || !chunk_dims) {
        stream_send_terminator(c);
        return;
    }

    for (uint32_t i = 0; i < PULL_CHUNK_SIZE; i++) {
        chunk_bufs[i] = chunk_scratch + (size_t)i * PULL_MAX_DIM;
    }

    /* Set up async state */
    ConnAsyncCtx *a = &c->async;
    a->state = CONN_ASYNC_STREAM;
    a->feat_ids = feat_ids;
    a->count = count;
    a->offset = 0;
    a->chunk_scratch = chunk_scratch;
    a->chunk_bufs = chunk_bufs;
    a->chunk_dims = chunk_dims;
    a->first_frame = 1;
    a->phase = 0;

    /* Submit first chunk */
    stream_submit_next_chunk(srv, c);
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

        /* If water-level hit, submit flush NOW — don't wait for next tick */
        if (!srv->flush_in_flight && graveldb_flush_needed(srv->db)) {
            graveldb_flush_submit(srv->db, &srv->async_flush);
            srv->flush_in_flight = true;
        }
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

    /* Parse feat_ids into contiguous array for batch delete */
    uint64_t stack_ids[64];
    uint64_t *feat_ids = (count <= 64) ? stack_ids : (uint64_t *)malloc(count * sizeof(uint64_t));
    if (!feat_ids) { response_begin(c, GRAVELDB_WIRE_ERR); response_finish(c); return; }

    uint32_t actual = 0;
    for (uint32_t i = 0; i < count && ptr + 8 <= body + body_len; i++) {
        memcpy(&feat_ids[actual], ptr, 8); ptr += 8;
        actual++;
    }

    if (actual > 0) {
        graveldb_batch_delete(srv->db, NULL, feat_ids, (int)actual);
    }

    if (feat_ids != stack_ids) free(feat_ids);

    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_flush(GravelServer *srv, ClientConn *c) {
    graveldb_checkpoint(srv->db);
    response_begin(c, GRAVELDB_WIRE_OK);
    response_finish(c);
}

static void handle_checkpoint(GravelServer *srv, ClientConn *c) {
    graveldb_checkpoint(srv->db);
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

/*
 * Resume in-progress async IO on a connection.
 * Called from the event loop on every iteration for connections with
 * in-flight disk IO. This is the key mechanism that interleaves disk IO
 * with network IO across multiple connections.
 *
 * Returns:
 *   1 = still in async state (IO pending)
 *   0 = completed, connection is IDLE
 */
static int client_async_resume(GravelServer *srv, ClientConn *c) {
    switch (c->async.state) {
    case CONN_IDLE:
        return 0;
    case CONN_ASYNC_PULL:
        return pull_resume(srv, c);
    case CONN_ASYNC_STREAM:
        return stream_resume(srv, c);
    }
    return 0;
}

static int process_message(GravelServer *srv, ClientConn *c) {
    /* Don't accept new messages while async IO is in progress */
    if (c->async.state != CONN_IDLE) return 0;

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
        case GRAVELDB_MSG_PULL:        handle_pull(srv, c, body, body_len); break;
        case GRAVELDB_MSG_PULL_STREAM: handle_pull_stream(srv, c, body, body_len); break;
        case GRAVELDB_MSG_PUSH:
        case GRAVELDB_MSG_DELETE:
        case GRAVELDB_MSG_FLUSH:
        case GRAVELDB_MSG_CHECKPOINT:
            if (srv->readonly) {
                response_begin(c, GRAVELDB_WIRE_INVALID);
                response_finish(c);
                break;
            }
            if (msg_type == GRAVELDB_MSG_PUSH)       handle_push(srv, c, body, body_len);
            else if (msg_type == GRAVELDB_MSG_DELETE) handle_delete(srv, c, body, body_len);
            else if (msg_type == GRAVELDB_MSG_FLUSH)  handle_flush(srv, c);
            else                                      handle_checkpoint(srv, c);
            break;
        case GRAVELDB_MSG_STATS:     handle_stats(srv, c); break;
        case GRAVELDB_MSG_PING:      handle_ping(srv, c); break;
        default:
            response_begin(c, GRAVELDB_WIRE_INVALID);
            response_finish(c);
            break;
    }

    /* Arena reset is deferred — happens in client_flush_send after all
     * chunks are fully written. This keeps SendChunks alive through writev. */

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
        /*
         * Phase 0a: Poll in-flight async flush.
         * This runs the IO completion check — on Linux io_uring CQE,
         * on macOS it already completed in submit (fdatasync inline).
         */
        if (srv->flush_in_flight) {
            if (graveldb_flush_poll(&srv->async_flush) != GRAVELDB_AGAIN) {
                srv->flush_in_flight = false;
            }
        }

        /*
         * Phase 0b: Resume all connections with in-flight disk IO.
         * This is what makes disk IO and network IO truly interleaved:
         * after submitting disk IO for one client, we come back here on the
         * next iteration and can service other clients' network IO in between.
         */
        int has_async = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            ClientConn *c = &srv->clients[i];
            if (c->fd < 0 || c->async.state == CONN_IDLE) continue;

            has_async = 1;
            int still_busy = client_async_resume(srv, c);

            /* If async completed, try to flush + process queued messages */
            if (!still_busy) {
                if (client_has_pending_send(c)) {
                    int wr_rc = client_flush_send(c);
                    if (wr_rc < 0) {
                        io_poller_del(srv->poller, c->fd);
                        client_close(c);
                        srv->num_clients--;
                        continue;
                    }
                }
                /* Process any queued messages that were blocked */
                int rc;
                while ((rc = process_message(srv, c)) > 0) {}
                if (rc < 0) {
                    io_poller_del(srv->poller, c->fd);
                    client_close(c);
                    srv->num_clients--;
                    continue;
                }
            } else {
                /* Still busy with disk IO — flush network in the meantime */
                if (client_has_pending_send(c)) {
                    int wr_rc = client_flush_send_raw(c);
                    if (wr_rc < 0) {
                        io_poller_del(srv->poller, c->fd);
                        client_close(c);
                        srv->num_clients--;
                        continue;
                    }
                }
            }

            if (c->fd >= 0) {
                uint32_t interest = IO_EVENT_READ;
                if (client_has_pending_send(c)) {
                    interest |= IO_EVENT_WRITE;
                }
                io_poller_mod(srv->poller, c->fd, interest, c);
            }
        }

        /* Use zero timeout if any connection has async IO in flight,
         * or if a flush is in progress — don't block while IO is completing. */
        int timeout_ms = (has_async || srv->flush_in_flight) ? 0 : 100;
        int n = io_poller_wait(srv->poller, events, MAX_CLIENTS + 1, timeout_ms);

        if (!srv->readonly && srv->checkpoint_interval_ms > 0) {
            uint64_t now = now_ms();
            if (now - srv->last_checkpoint_ms >= srv->checkpoint_interval_ms) {
                graveldb_checkpoint(srv->db);
                srv->last_checkpoint_ms = now;
            }
        }

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

            /* Write — flush pending send chunks */
            if (client_has_pending_send(c)) {
                int wr_rc = client_flush_send(c);
                if (wr_rc < 0) {
                    io_poller_del(srv->poller, c->fd);
                    client_close(c);
                    srv->num_clients--;
                    continue;
                }
            }

            if (c->fd >= 0) {
                uint32_t interest = IO_EVENT_READ;
                if (client_has_pending_send(c)) {
                    interest |= IO_EVENT_WRITE;
                }
                io_poller_mod(srv->poller, c->fd, interest, c);
            }
        }
    }

    return NULL;
}

/* ===== ReadWorker event loop (readonly mode) =====
 *
 * Each worker is fully independent: its own poller, its own clients.
 * It receives new fds via the notify pipe and handles only reads.
 * No locks, no shared mutable state — the DB is frozen.
 */
static ClientConn *worker_find_free_slot(ReadWorker *w) {
    for (int i = 0; i < WORKER_MAX_CLIENTS; i++) {
        if (w->clients[i].fd == -1) return &w->clients[i];
    }
    return NULL;
}

static void *read_worker_loop(void *arg) {
    ReadWorker *w = (ReadWorker *)arg;
    IOEvent events[WORKER_MAX_CLIENTS + 1];

    /* Fake GravelServer for process_message (only db and readonly fields used).
     * Heap-allocated because GravelServer contains a large ClientConn array
     * (MAX_CLIENTS × 64KB arena) which would overflow the default thread stack. */
    GravelServer *fake_srv = (GravelServer *)calloc(1, sizeof(GravelServer));
    fake_srv->db = w->db;
    fake_srv->readonly = true;

    while (*w->running) {
        /* Phase 0: Resume async IO on all worker clients */
        int has_async = 0;
        for (int i = 0; i < WORKER_MAX_CLIENTS; i++) {
            ClientConn *c = &w->clients[i];
            if (c->fd < 0 || c->async.state == CONN_IDLE) continue;

            has_async = 1;
            int still_busy = client_async_resume(fake_srv, c);

            if (!still_busy) {
                if (client_has_pending_send(c)) {
                    int wr_rc = client_flush_send(c);
                    if (wr_rc < 0) {
                        io_poller_del(w->poller, c->fd);
                        client_close(c);
                        w->num_clients--;
                        continue;
                    }
                }
                int rc;
                while ((rc = process_message(fake_srv, c)) > 0) {}
                if (rc < 0) {
                    io_poller_del(w->poller, c->fd);
                    client_close(c);
                    w->num_clients--;
                    continue;
                }
            } else {
                if (client_has_pending_send(c)) {
                    int wr_rc = client_flush_send_raw(c);
                    if (wr_rc < 0) {
                        io_poller_del(w->poller, c->fd);
                        client_close(c);
                        w->num_clients--;
                        continue;
                    }
                }
            }

            if (c->fd >= 0) {
                uint32_t interest = IO_EVENT_READ;
                if (client_has_pending_send(c)) {
                    interest |= IO_EVENT_WRITE;
                }
                io_poller_mod(w->poller, c->fd, interest, c);
            }
        }

        int timeout_ms = has_async ? 0 : 100;
        int n = io_poller_wait(w->poller, events, WORKER_MAX_CLIENTS + 1, timeout_ms);
        if (n <= 0) continue;

        for (int i = 0; i < n; i++) {
            IOEvent *ev = &events[i];

            /* New fd delivered via notify pipe */
            if (ev->userdata == NOTIFY_SENTINEL) {
                int new_fd = -1;
                while (read(w->notify_fd, &new_fd, sizeof(new_fd)) == sizeof(new_fd)) {
                    if (new_fd < 0) continue;
                    set_nonblocking(new_fd);
                    set_tcp_nodelay(new_fd);
                    ClientConn *slot = worker_find_free_slot(w);
                    if (slot) {
                        client_init(slot, new_fd);
                        w->num_clients++;
                        io_poller_add(w->poller, new_fd, IO_EVENT_READ, slot);
                    } else {
                        close(new_fd);
                    }
                }
                continue;
            }

            ClientConn *c = (ClientConn *)ev->userdata;
            if (c->fd < 0) continue;

            /* Read */
            if (ev->events & (IO_EVENT_READ | IO_EVENT_HUP | IO_EVENT_ERROR)) {
                if (c->recv_len >= c->recv_cap) {
                    size_t new_cap = c->recv_cap * 2;
                    if (new_cap > 64 * 1024 * 1024) new_cap = 64 * 1024 * 1024;
                    if (new_cap <= c->recv_cap) {
                        io_poller_del(w->poller, c->fd);
                        client_close(c);
                        w->num_clients--;
                        continue;
                    }
                    uint8_t *tmp = (uint8_t *)realloc(c->recv_buf, new_cap);
                    if (!tmp) {
                        io_poller_del(w->poller, c->fd);
                        client_close(c);
                        w->num_clients--;
                        continue;
                    }
                    c->recv_buf = tmp;
                    c->recv_cap = new_cap;
                }

                ssize_t rd = read(c->fd, c->recv_buf + c->recv_len,
                                  c->recv_cap - c->recv_len);
                if (rd <= 0) {
                    io_poller_del(w->poller, c->fd);
                    client_close(c);
                    w->num_clients--;
                    continue;
                }
                c->recv_len += rd;

                int rc;
                while ((rc = process_message(fake_srv, c)) > 0) {}
                if (rc < 0) {
                    io_poller_del(w->poller, c->fd);
                    client_close(c);
                    w->num_clients--;
                    continue;
                }
            }

            /* Write */
            if (client_has_pending_send(c)) {
                int wr_rc = client_flush_send(c);
                if (wr_rc < 0) {
                    io_poller_del(w->poller, c->fd);
                    client_close(c);
                    w->num_clients--;
                    continue;
                }
            }

            if (c->fd >= 0) {
                uint32_t interest = IO_EVENT_READ;
                if (client_has_pending_send(c)) {
                    interest |= IO_EVENT_WRITE;
                }
                io_poller_mod(w->poller, c->fd, interest, c);
            }
        }
    }

    /* Cleanup worker clients */
    for (int i = 0; i < WORKER_MAX_CLIENTS; i++) {
        if (w->clients[i].fd >= 0) {
            io_poller_del(w->poller, w->clients[i].fd);
            client_close(&w->clients[i]);
        }
    }
    io_poller_destroy(w->poller);
    close(w->notify_fd);
    free(fake_srv);

    return NULL;
}

/* Accept loop for readonly mode: only accepts and distributes fds to workers */
static void *readonly_accept_loop(void *arg) {
    GravelServer *srv = (GravelServer *)arg;
    IOEvent events[4];

    while (srv->running) {
        int n = io_poller_wait(srv->poller, events, 4, 100);
        if (n <= 0) continue;

        for (int i = 0; i < n; i++) {
            struct sockaddr_in addr;
            socklen_t addr_len = sizeof(addr);
            int client_fd = accept(srv->listen_fd, (struct sockaddr *)&addr, &addr_len);
            if (client_fd < 0) continue;

            /* Round-robin distribute to workers */
            int target = srv->rr_next % srv->num_read_workers;
            srv->rr_next++;

            /* Send fd to worker via pipe */
            if (write(srv->worker_pipe_w[target], &client_fd, sizeof(client_fd)) != sizeof(client_fd)) {
                close(client_fd);  /* pipe full or error, drop connection */
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
    srv->readonly = config->readonly;
    srv->flush_in_flight = false;
    srv->num_read_workers = 0;
    srv->workers = NULL;
    srv->worker_pipe_w = NULL;
    srv->rr_next = 0;

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

    /* Setup checkpoint timing (only in read-write mode) */
    if (!srv->readonly) {
        uint32_t interval_s = config->auto_checkpoint_interval_s > 0
                              ? (uint32_t)config->auto_checkpoint_interval_s : 60;
        srv->checkpoint_interval_ms = interval_s * 1000;
        srv->last_checkpoint_ms = now_ms();
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

    if (srv->readonly && srv->config.num_read_workers > 0) {
        /* Readonly multi-worker mode: spin up N reader threads */
        int nw = srv->config.num_read_workers;
        srv->num_read_workers = nw;
        srv->workers = (ReadWorker *)calloc(nw, sizeof(ReadWorker));
        srv->worker_pipe_w = (int *)calloc(nw, sizeof(int));

        for (int i = 0; i < nw; i++) {
            int pipefd[2];
            if (pipe(pipefd) < 0) return GRAVELDB_ERR_IO;
            set_nonblocking(pipefd[0]);

            ReadWorker *w = &srv->workers[i];
            w->db = srv->db;
            w->poller = io_poller_create(WORKER_MAX_CLIENTS + 4);
            w->num_clients = 0;
            w->notify_fd = pipefd[0];
            w->running = &srv->running;
            w->readonly = true;
            w->worker_id = i;

            for (int j = 0; j < WORKER_MAX_CLIENTS; j++) {
                w->clients[j].fd = -1;
            }

            /* Register notify pipe in worker's poller */
            io_poller_add(w->poller, pipefd[0], IO_EVENT_READ, NOTIFY_SENTINEL);

            srv->worker_pipe_w[i] = pipefd[1];

            pthread_create(&w->thread, NULL, read_worker_loop, w);
        }

        /* Main thread becomes the accept-only loop */
        pthread_create(&srv->server_thread, NULL, readonly_accept_loop, srv);

        fprintf(stderr, "[GravelServer] Listening on port %d, READONLY mode"
                " with %d reader workers (lock-free)\n", port, nw);
    } else {
        /* Normal single-threaded event loop (read-write or readonly with 1 thread) */
        pthread_create(&srv->server_thread, NULL, server_loop, srv);

        fprintf(stderr, "[GravelServer] Listening on port %d%s (using "
#if defined(__linux__)
                "epoll"
#elif defined(__APPLE__) || defined(__FreeBSD__)
                "kqueue"
#else
                "poll"
#endif
                ")\n", port, srv->readonly ? " [READONLY]" : "");
    }

    return GRAVELDB_OK;
}

void gravel_server_stop(GravelServer *srv) {
    if (!srv || !srv->running) return;

    srv->running = 0;
    pthread_join(srv->server_thread, NULL);

    /* Join read workers (they check srv->running and exit) */
    for (int i = 0; i < srv->num_read_workers; i++) {
        pthread_join(srv->workers[i].thread, NULL);
        close(srv->worker_pipe_w[i]);
    }

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

    if (!srv->readonly) {
        graveldb_checkpoint(srv->db);
    }
}

void gravel_server_destroy(GravelServer *srv) {
    if (!srv) return;
    gravel_server_stop(srv);
    graveldb_close(srv->db);
    io_poller_destroy(srv->poller);
    free(srv->workers);
    free(srv->worker_pipe_w);
    free(srv);
}

GravelDB *gravel_server_get_db(GravelServer *srv) {
    return srv->db;
}
