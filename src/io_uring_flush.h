/*
 * GravelDB - io_uring batch flush support
 *
 * On Linux with io_uring (kernel 5.1+), flush submits all block writes as a
 * single batch into the submission queue, achieving:
 *   - 0-1 syscalls for N writes (vs N pwrite syscalls)
 *   - Full NVMe queue depth utilization
 *   - Kernel-side I/O merging of adjacent submissions
 *
 * On macOS / older Linux: transparent fallback to pwrite loop.
 *
 * Usage:
 *   uring_flush_ctx_t ctx;
 *   uring_flush_init(&ctx, fd);
 *   uring_flush_submit(&ctx, buf, size, offset);  // repeat N times
 *   uring_flush_wait(&ctx);  // wait all completions + fsync
 *   uring_flush_destroy(&ctx);
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

/*
 * Detect io_uring availability at compile time.
 * Define GRAVELDB_USE_IO_URING=1 to force enable (for cross-compile).
 * It auto-enables on Linux by default.
 */
#if !defined(GRAVELDB_USE_IO_URING)
  #if defined(__linux__)
    #define GRAVELDB_USE_IO_URING 1
  #else
    #define GRAVELDB_USE_IO_URING 0
  #endif
#endif

/* Maximum in-flight I/O requests per flush batch */
#define URING_FLUSH_QUEUE_DEPTH  256

typedef struct {
    int         fd;
    uint32_t    pending;       /* number of submitted but not yet completed */
    uint32_t    errors;        /* count of failed completions */
    bool        initialized;

#if GRAVELDB_USE_IO_URING
    void       *ring;          /* struct io_uring* (opaque to avoid header leak) */
#endif
} uring_flush_ctx_t;

/*
 * Initialize the flush context for the given file descriptor.
 * Returns 0 on success, -1 on failure (caller should fallback to pwrite).
 */
int uring_flush_init(uring_flush_ctx_t *ctx, int fd);

/*
 * Submit a write request. The data must remain valid until uring_flush_wait().
 * Returns 0 on success, -1 if the ring is full (caller should wait first).
 */
int uring_flush_submit(uring_flush_ctx_t *ctx, const void *buf, size_t len, off_t offset);

/*
 * Submit an fsync/fdatasync request (appended after all writes).
 * Returns 0 on success.
 */
int uring_flush_submit_fsync(uring_flush_ctx_t *ctx);

/*
 * Wait for all submitted I/O to complete. Returns number of errors (0 = all OK).
 */
int uring_flush_wait(uring_flush_ctx_t *ctx);

/*
 * Destroy the flush context and free kernel resources.
 */
void uring_flush_destroy(uring_flush_ctx_t *ctx);

/*
 * Check if io_uring is available at runtime (may fail on old kernels even on Linux).
 */
bool uring_flush_available(void);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_IO_URING_FLUSH_H_ */
