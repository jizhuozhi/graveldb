/*
 * GravelDB - io_uring batch I/O (multi-fd writes + batch reads)
 *
 * Supports:
 *   - Multi-fd batch write: submit writes across different file descriptors
 *     in a single io_uring ring, with per-fd fsync at the end.
 *   - Batch read: submit multiple pread requests across fds, wait all.
 *
 * On macOS / older Linux: transparent fallback to pwrite/pread loops.
 */

#ifndef GRAVELDB_IO_URING_FLUSH_H_
#define GRAVELDB_IO_URING_FLUSH_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(GRAVELDB_USE_IO_URING)
  #if defined(__linux__)
    #define GRAVELDB_USE_IO_URING 1
  #else
    #define GRAVELDB_USE_IO_URING 0
  #endif
#endif

/* Maximum in-flight I/O requests per batch */
#define URING_QUEUE_DEPTH  1024

/* Maximum distinct fds that can receive fsync in one batch */
#define URING_MAX_FDS      64

/*
 * I/O context: supports batch writes and batch reads.
 * One ring shared across multiple file descriptors.
 */

typedef struct {
    uint32_t    pending;
    uint32_t    errors;
    bool        initialized;

    /* Track distinct fds that received writes (for per-fd fsync) */
    int         fsync_fds[URING_MAX_FDS];
    int         fsync_fd_count;

#if GRAVELDB_USE_IO_URING
    void       *ring;   /* struct io_uring* (opaque) */
#endif
} uring_io_ctx_t;

/*
 * Initialize the I/O context (not bound to any single fd).
 * Returns 0 on success, -1 on failure.
 */
int uring_io_init(uring_io_ctx_t *ctx);

/*
 * Submit a write request for a specific fd.
 * Data must remain valid until uring_io_wait().
 */
int uring_io_submit_write(uring_io_ctx_t *ctx, int fd,
                          const void *buf, size_t len, off_t offset);

/*
 * Submit a read request for a specific fd.
 * Buffer must remain valid until uring_io_wait().
 */
int uring_io_submit_read(uring_io_ctx_t *ctx, int fd,
                         void *buf, size_t len, off_t offset);

/*
 * Submit fsync for all fds that received writes.
 * Called after all writes are submitted, before wait.
 */
int uring_io_submit_fsyncs(uring_io_ctx_t *ctx);

/*
 * Wait for all submitted I/O (reads + writes + fsyncs) to complete.
 * Returns number of errors (0 = all OK).
 */
int uring_io_wait(uring_io_ctx_t *ctx);

/*
 * Destroy the I/O context.
 */
void uring_io_destroy(uring_io_ctx_t *ctx);

/*
 * Check if io_uring is available at runtime.
 */
bool uring_io_available(void);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_IO_URING_FLUSH_H_ */
