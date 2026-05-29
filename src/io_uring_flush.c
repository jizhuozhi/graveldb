/*
 * GravelDB - io_uring batch flush implementation
 *
 * Strategy:
 *   1. All block writes are submitted to the SQ ring without syscalls
 *   2. A final FSYNC SQE is chained (IOSQE_IO_DRAIN ensures ordering)
 *   3. One io_uring_enter() submits everything + waits for all CQEs
 *
 * This turns a flush of N blocks from:
 *   N * pwrite() + 1 * fdatasync() = (N+1) syscalls
 * Into:
 *   1 * io_uring_enter() = 1 syscall (or 0 with SQPOLL)
 *
 * Fallback: on macOS or kernels without io_uring, all functions degrade
 * gracefully to pwrite + fdatasync.
 */

#include "io_uring_flush.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#if GRAVELDB_USE_IO_URING

#include <liburing.h>

int uring_flush_init(uring_flush_ctx_t *ctx, int fd) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->fd = fd;

    struct io_uring *ring = (struct io_uring *)malloc(sizeof(struct io_uring));
    if (!ring) return -1;

    /*
     * Queue depth = URING_FLUSH_QUEUE_DEPTH.
     * IORING_SETUP_SINGLE_ISSUER: hint that only one thread submits (our case).
     * We don't use SQPOLL to avoid needing CAP_SYS_ADMIN / elevated privs.
     */
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    int ret = io_uring_queue_init_params(URING_FLUSH_QUEUE_DEPTH, ring, &params);
    if (ret < 0) {
        /* io_uring not supported on this kernel */
        free(ring);
        return -1;
    }

    ctx->ring = ring;
    ctx->initialized = true;
    return 0;
}

int uring_flush_submit(uring_flush_ctx_t *ctx, const void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    struct io_uring *ring = (struct io_uring *)ctx->ring;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

    if (!sqe) {
        /*
         * SQ ring full -- need to submit current batch and wait,
         * then retry. For simplicity, submit what we have first.
         */
        int ret = io_uring_submit(ring);
        if (ret < 0) return -1;

        /* Wait for some completions to free SQ slots */
        struct io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(ring, &cqe);
        if (ret < 0) return -1;

        /* Consume completed entries */
        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(ring, head, cqe) {
            if (cqe->res < 0) ctx->errors++;
            count++;
            ctx->pending--;
        }
        io_uring_cq_advance(ring, count);

        /* Retry getting SQE */
        sqe = io_uring_get_sqe(ring);
        if (!sqe) return -1;
    }

    io_uring_prep_write(sqe, ctx->fd, buf, (unsigned)len, offset);
    io_uring_sqe_set_data(sqe, NULL);  /* no user data needed */
    ctx->pending++;

    return 0;
}

int uring_flush_submit_fsync(uring_flush_ctx_t *ctx) {
    if (!ctx->initialized) return -1;

    struct io_uring *ring = (struct io_uring *)ctx->ring;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;

    /*
     * IORING_FSYNC_DATASYNC = fdatasync semantics (metadata not synced
     * unless needed for data integrity). IO_DRAIN ensures all prior
     * writes complete before the fsync executes.
     */
    io_uring_prep_fsync(sqe, ctx->fd, IORING_FSYNC_DATASYNC);
    sqe->flags |= IOSQE_IO_DRAIN;
    io_uring_sqe_set_data(sqe, (void *)(uintptr_t)1);  /* tag: fsync */
    ctx->pending++;

    return 0;
}

int uring_flush_wait(uring_flush_ctx_t *ctx) {
    if (!ctx->initialized || ctx->pending == 0) return ctx->errors;

    struct io_uring *ring = (struct io_uring *)ctx->ring;

    /* Submit all queued SQEs */
    int ret = io_uring_submit(ring);
    if (ret < 0) {
        ctx->errors++;
        return ctx->errors;
    }

    /* Wait for all completions */
    while (ctx->pending > 0) {
        struct io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(ring, &cqe);
        if (ret < 0) {
            ctx->errors++;
            break;
        }

        /* Drain all available CQEs */
        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(ring, head, cqe) {
            if (cqe->res < 0) {
                ctx->errors++;
            }
            count++;
        }
        io_uring_cq_advance(ring, count);
        ctx->pending -= count;
    }

    ctx->pending = 0;
    return ctx->errors;
}

void uring_flush_destroy(uring_flush_ctx_t *ctx) {
    if (!ctx->initialized) return;

    struct io_uring *ring = (struct io_uring *)ctx->ring;
    io_uring_queue_exit(ring);
    free(ring);
    ctx->ring = NULL;
    ctx->initialized = false;
}

bool uring_flush_available(void) {
    /* Probe: try to init a minimal ring */
    struct io_uring ring;
    int ret = io_uring_queue_init(1, &ring, 0);
    if (ret < 0) return false;
    io_uring_queue_exit(&ring);
    return true;
}

#else /* !GRAVELDB_USE_IO_URING -- fallback for macOS / non-Linux */

/*
 * Fallback implementation: pwrite each block immediately on submit,
 * fdatasync on wait. Same API so dimbin_flush needs zero #ifdefs.
 *
 * On macOS there's no io_uring equivalent (kqueue doesn't do async disk I/O
 * for regular files). pwrite is the best we can do.
 */

int uring_flush_init(uring_flush_ctx_t *ctx, int fd) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->fd = fd;
    ctx->initialized = true;
    return 0;
}

int uring_flush_submit(uring_flush_ctx_t *ctx, const void *buf, size_t len, off_t offset) {
    if (!ctx->initialized) return -1;

    ssize_t wr = pwrite(ctx->fd, buf, len, offset);
    if (wr < 0 || (size_t)wr != len) {
        ctx->errors++;
        return -1;
    }
    ctx->pending++;
    return 0;
}

int uring_flush_submit_fsync(uring_flush_ctx_t *ctx) {
    (void)ctx;
    return 0;  /* fdatasync happens in wait() */
}

int uring_flush_wait(uring_flush_ctx_t *ctx) {
    if (!ctx->initialized) return (int)ctx->errors;

    if (ctx->pending > 0) {
        fdatasync(ctx->fd);
    }
    ctx->pending = 0;
    return (int)ctx->errors;
}

void uring_flush_destroy(uring_flush_ctx_t *ctx) {
    if (!ctx->initialized) return;
    ctx->initialized = false;
}

bool uring_flush_available(void) {
    return false;
}

#endif /* GRAVELDB_USE_IO_URING */
