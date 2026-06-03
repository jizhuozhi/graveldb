/*
 * GravelDB - io_uring batch I/O implementation (multi-fd)
 *
 * Single ring shared across all DimBin fds:
 *   - Writes + reads submitted to arbitrary fds
 *   - Per-fd fsync after all writes
 *   - One io_uring_enter() for everything
 *
 * On macOS / older Linux: transparent fallback to pwrite/pread loops.
 */

#include "io_uring_flush.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#if GRAVELDB_USE_IO_URING

#include <liburing.h>

int uring_io_init(uring_io_ctx_t *ctx) {
    memset(ctx, 0, sizeof(*ctx));

    struct io_uring *ring = (struct io_uring *)malloc(sizeof(struct io_uring));
    if (!ring) return -1;

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    int ret = io_uring_queue_init_params(URING_QUEUE_DEPTH, ring, &params);
    if (ret < 0) {
        free(ring);
        return -1;
    }

    ctx->ring = ring;
    ctx->initialized = true;
    ctx->fsync_fd_count = 0;
    return 0;
}

/* Track fd for later fsync (dedup) */
static void uring_io_track_fd(uring_io_ctx_t *ctx, int fd) {
    for (int i = 0; i < ctx->fsync_fd_count; i++) {
        if (ctx->fsync_fds[i] == fd) return;
    }
    if (ctx->fsync_fd_count < URING_MAX_FDS) {
        ctx->fsync_fds[ctx->fsync_fd_count++] = fd;
    }
}

static struct io_uring_sqe *uring_io_get_sqe(uring_io_ctx_t *ctx) {
    struct io_uring *ring = (struct io_uring *)ctx->ring;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

    if (!sqe) {
        /* SQ full: submit + drain some completions */
        int ret = io_uring_submit(ring);
        if (ret < 0) return NULL;

        struct io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(ring, &cqe);
        if (ret < 0) return NULL;

        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(ring, head, cqe) {
            if (cqe->res < 0) ctx->errors++;
            count++;
            ctx->pending--;
        }
        io_uring_cq_advance(ring, count);

        sqe = io_uring_get_sqe(ring);
    }
    return sqe;
}

int uring_io_submit_write(uring_io_ctx_t *ctx, int fd,
                          const void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    struct io_uring_sqe *sqe = uring_io_get_sqe(ctx);
    if (!sqe) return -1;

    io_uring_prep_write(sqe, fd, buf, (unsigned)len, offset);
    io_uring_sqe_set_data(sqe, NULL);
    ctx->pending++;

    uring_io_track_fd(ctx, fd);
    return 0;
}

int uring_io_submit_writev(uring_io_ctx_t *ctx, int fd,
                           const struct iovec *iov, int iovcnt, off_t offset) {
    if (!ctx->initialized) return -1;

    struct io_uring_sqe *sqe = uring_io_get_sqe(ctx);
    if (!sqe) return -1;

    io_uring_prep_writev(sqe, fd, iov, (unsigned)iovcnt, offset);
    io_uring_sqe_set_data(sqe, NULL);
    ctx->pending++;

    uring_io_track_fd(ctx, fd);
    return 0;
}

int uring_io_submit_read(uring_io_ctx_t *ctx, int fd,
                         void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    struct io_uring_sqe *sqe = uring_io_get_sqe(ctx);
    if (!sqe) return -1;

    io_uring_prep_read(sqe, fd, buf, (unsigned)len, offset);
    io_uring_sqe_set_data(sqe, NULL);
    ctx->pending++;
    return 0;
}

int uring_io_submit_fsyncs(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return -1;

    struct io_uring *ring = (struct io_uring *)ctx->ring;

    for (int i = 0; i < ctx->fsync_fd_count; i++) {
        struct io_uring_sqe *sqe = uring_io_get_sqe(ctx);
        if (!sqe) return -1;

        io_uring_prep_fsync(sqe, ctx->fsync_fds[i], IORING_FSYNC_DATASYNC);
        sqe->flags |= IOSQE_IO_DRAIN;
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)1);
        ctx->pending++;
    }

    return 0;
}

int uring_io_wait(uring_io_ctx_t *ctx) {
    if (!ctx->initialized || ctx->pending == 0) return ctx->errors;

    struct io_uring *ring = (struct io_uring *)ctx->ring;

    int ret = io_uring_submit(ring);
    if (ret < 0) {
        ctx->errors++;
        return ctx->errors;
    }

    while (ctx->pending > 0) {
        struct io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(ring, &cqe);
        if (ret < 0) {
            ctx->errors++;
            break;
        }

        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(ring, head, cqe) {
            if (cqe->res < 0) ctx->errors++;
            count++;
        }
        io_uring_cq_advance(ring, count);
        ctx->pending -= count;
    }

    ctx->pending = 0;
    return ctx->errors;
}

int uring_io_poll(uring_io_ctx_t *ctx) {
    if (!ctx->initialized || ctx->pending == 0) return 0;

    struct io_uring *ring = (struct io_uring *)ctx->ring;

    /* Ensure all SQEs are submitted to the kernel */
    io_uring_submit(ring);

    /* Non-blocking reap: peek completions without waiting */
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;
    io_uring_for_each_cqe(ring, head, cqe) {
        if (cqe->res < 0) ctx->errors++;
        count++;
    }
    if (count > 0) {
        io_uring_cq_advance(ring, count);
        ctx->pending -= count;
    }

    return (int)ctx->pending;
}

void uring_io_destroy(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return;
    struct io_uring *ring = (struct io_uring *)ctx->ring;
    io_uring_queue_exit(ring);
    free(ring);
    ctx->ring = NULL;
    ctx->initialized = false;
}

void uring_io_reset(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return;
    ctx->pending = 0;
    ctx->errors = 0;
    ctx->fsync_fd_count = 0;
}

bool uring_io_available(void) {
    struct io_uring ring;
    int ret = io_uring_queue_init(1, &ring, 0);
    if (ret < 0) return false;
    io_uring_queue_exit(&ring);
    return true;
}

#else /* !GRAVELDB_USE_IO_URING -- fallback for macOS / non-Linux */

int uring_io_init(uring_io_ctx_t *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->initialized = true;
    ctx->fsync_fd_count = 0;
    return 0;
}

static void uring_io_track_fd_fallback(uring_io_ctx_t *ctx, int fd) {
    for (int i = 0; i < ctx->fsync_fd_count; i++) {
        if (ctx->fsync_fds[i] == fd) return;
    }
    if (ctx->fsync_fd_count < URING_MAX_FDS) {
        ctx->fsync_fds[ctx->fsync_fd_count++] = fd;
    }
}

int uring_io_submit_write(uring_io_ctx_t *ctx, int fd,
                          const void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    ssize_t wr = pwrite(fd, buf, len, offset);
    if (wr < 0 || (size_t)wr != len) {
        ctx->errors++;
        return -1;
    }
    ctx->pending++;
    uring_io_track_fd_fallback(ctx, fd);
    return 0;
}

int uring_io_submit_writev(uring_io_ctx_t *ctx, int fd,
                           const struct iovec *iov, int iovcnt, off_t offset) {
    if (!ctx->initialized) return -1;

    ssize_t wr = pwritev(fd, iov, iovcnt, offset);
    size_t expected = 0;
    for (int i = 0; i < iovcnt; i++) expected += iov[i].iov_len;
    if (wr < 0 || (size_t)wr != expected) {
        ctx->errors++;
        return -1;
    }
    ctx->pending++;
    uring_io_track_fd_fallback(ctx, fd);
    return 0;
}

int uring_io_submit_read(uring_io_ctx_t *ctx, int fd,
                         void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    ssize_t rd = pread(fd, buf, len, offset);
    if (rd < 0) {
        ctx->errors++;
        return -1;
    }
    ctx->pending++;
    return 0;
}

int uring_io_submit_fsyncs(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return -1;

    for (int i = 0; i < ctx->fsync_fd_count; i++) {
        fdatasync(ctx->fsync_fds[i]);
    }
    return 0;
}

int uring_io_wait(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return (int)ctx->errors;
    /* In fallback mode, all I/O is already done synchronously */
    ctx->pending = 0;
    return (int)ctx->errors;
}

int uring_io_poll(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return 0;
    /* In fallback mode, all I/O completed synchronously during submit */
    ctx->pending = 0;
    return 0;
}

void uring_io_destroy(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return;
    ctx->initialized = false;
}

void uring_io_reset(uring_io_ctx_t *ctx) {
    if (!ctx->initialized) return;
    ctx->pending = 0;
    ctx->errors = 0;
    ctx->fsync_fd_count = 0;
}

bool uring_io_available(void) {
    return false;
}

#endif /* GRAVELDB_USE_IO_URING */
