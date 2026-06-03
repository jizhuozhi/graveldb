/*
 * GravelDB - Public API
 *
 * Minimal public header for embedding GravelDB as a library.
 * All internal structures are opaque; users interact only through
 * this API and the types defined here.
 *
 * Lifecycle control:
 *   - GravelDB instance is fully self-contained (no globals).
 *   - Multiple instances can coexist in the same process.
 */

#ifndef GRAVELDB_H_
#define GRAVELDB_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    GRAVELDB_OK            =  0,
    GRAVELDB_AGAIN         =  1,   /* async op not yet complete, call again later */
    GRAVELDB_FLUSH_NEEDED  =  2,   /* write buffer above water-level, flush recommended */
    GRAVELDB_ERR_IO        = -1,
    GRAVELDB_ERR_OOM       = -2,
    GRAVELDB_ERR_NOT_FOUND = -3,
    GRAVELDB_ERR_CORRUPT   = -4,
    GRAVELDB_ERR_FULL      = -5,
    GRAVELDB_ERR_INVALID   = -6,
    GRAVELDB_ERR_BUSY      = -7,   /* overlay at WCU limit; checkpoint must drain first */
    GRAVELDB_ERR_DIM_MISMATCH = -8, /* feat_id already exists with different dim; delete first */
} graveldb_status_t;

typedef struct GravelDB GravelDB;

typedef struct GravelDBCtx {
    /*
     * Request-scoped allocator (optional).
     *   - alloc != NULL, dealloc != NULL: custom alloc/dealloc pair.
     *   - alloc != NULL, dealloc == NULL: arena mode (no free).
     *   - alloc == NULL: fallback to malloc/free.
     */
    void   *opaque;
    void  *(*alloc)(void *opaque, size_t size);
    void   (*dealloc)(void *opaque, void *ptr, size_t size);
} GravelDBCtx;

typedef struct {
    const char *data_dir;

    /* Dimension list for pre-created bins.
     * Copied internally; caller may free after graveldb_open() returns.
     * May be NULL with num_dims=0 for fully dynamic mode. */
    const int  *dims;
    int         num_dims;

    /* Buffer size in bytes (per dim bin). Default: 256MB */
    size_t      buffer_size;

    /* Hash index initial capacity. Default: 1<<24 (16M) */
    uint32_t    index_capacity;

    /* Entry alignment (0 = no padding). e.g. 16 -> pad to 16B boundary */
    uint32_t    entry_align;

    /* Page size in bytes (0 = auto-detect from filesystem/device).
     * Must be a power of 2 if specified. Default: 4096 */
    uint32_t    page_size;

    /* Auto-create bins for unknown dims at put() time */
    bool        auto_create_bins;

    int         delta_chain_max;  /* 0 = default (10) */
    float       dirty_ratio_full; /* 0 = default (0.5) */

    /* Overlay memory budget per dim bin (bytes).
     * Max memory consumed by overlay during checkpoint. When hit, put()
     * returns GRAVELDB_ERR_BUSY — caller should accelerate checkpoint step.
     * 0 = unlimited (default). Recommended: 64MB-256MB depending on write QPS
     * and expected checkpoint duration. */
    size_t      overlay_budget;
} GravelDBConfig;

graveldb_status_t graveldb_open(GravelDB **db, const GravelDBConfig *config);
void              graveldb_close(GravelDB *db);

/* All operations are batch-oriented. Single-key convenience is batch with n=1. */
graveldb_status_t graveldb_batch_put(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids,
                                     const int *dims, const float *const *embeddings, int n);

graveldb_status_t graveldb_batch_delete(GravelDB *db, GravelDBCtx *ctx,
                                        const uint64_t *feat_ids, int n);

graveldb_status_t graveldb_batch_get(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids, int n,
                                     float **out_embeddings, int *out_dims);

/* Two-phase async batch get: submit disk IO, poll for completion.
 * submit resolves memory hits immediately; poll returns GRAVELDB_AGAIN
 * until all disk IO completes (or GRAVELDB_OK when done).
 * On macOS (no io_uring), IO completes synchronously in submit. */
typedef struct {
    void *internal;  /* opaque state (heap-allocated on disk miss) */
} GravelDBAsyncGet;

graveldb_status_t graveldb_batch_get_submit(GravelDB *db, GravelDBCtx *ctx,
                                            const uint64_t *feat_ids, int n,
                                            float **out_embeddings, int *out_dims,
                                            GravelDBAsyncGet *async_out);
graveldb_status_t graveldb_batch_get_poll(GravelDBAsyncGet *async_ctx);

/* Cancel an in-flight async get: release all resources without waiting.
 * Safe to call even if the operation has already completed (no-op).
 * Use this when a client disconnects mid-operation. */
void graveldb_batch_get_cancel(GravelDBAsyncGet *async_ctx);

/* Check if any write buffer is above water-level and flush is recommended.
 * Non-blocking, O(num_dims). Returns true if at least one bin needs flush.
 * The server event loop should call graveldb_flush_submit when this returns true. */
bool graveldb_flush_needed(GravelDB *db);

/* Two-phase async flush: keys flushed synchronously (crash ordering),
 * value pages submitted via io_uring. Poll until GRAVELDB_OK. */
typedef struct {
    void *internal;  /* opaque state */
} GravelDBAsyncFlush;

graveldb_status_t graveldb_flush_submit(GravelDB *db, GravelDBAsyncFlush *async_out);
graveldb_status_t graveldb_flush_poll(GravelDBAsyncFlush *async_ctx);

/* Synchronous flush (convenience; internally calls submit+poll loop). */
graveldb_status_t graveldb_flush(GravelDB *db);

/* Synchronous delta checkpoint (convenience; internally calls checkpoint_step loop).
 * Persists all data to disk with crash safety.
 * Safe to call at any time; concurrent reads/writes are isolated via overlay. */
graveldb_status_t graveldb_checkpoint(GravelDB *db);

/* Incremental checkpoint: call repeatedly from event loop.
 * Each call does bounded work (at most max_pages_per_step pages of I/O).
 * Returns GRAVELDB_AGAIN while work remains, GRAVELDB_OK when done.
 * Check graveldb_checkpoint_in_progress() to know if done. */
graveldb_status_t graveldb_checkpoint_step(GravelDB *db, uint32_t max_pages_per_step);
bool              graveldb_checkpoint_in_progress(const GravelDB *db);

/*
 * Unified Checkpoint Export/Import API
 *
 * Both full and delta checkpoints use the same entry-interleaved format:
 *   [Header][feat_id(8B)|embedding(native)]...[Trailer]
 *
 * Full: scans all valid entries. Delta: scans only dirty pages.
 * Both leverage overlay for SATB (snapshot-at-the-beginning) isolation.
 *
 * Export is progressive: begin → step (repeat) → end.
 * IO within export_step is non-blocking (io_uring on Linux; fallback on macOS).
 * The caller's event loop polls via export_poll between steps.
 *
 * Import uses a similar async pattern: begin → poll (repeat until done).
 */

#define GRAVELDB_CKPT_FULL  0
#define GRAVELDB_CKPT_DELTA 1

/* Opaque export context (allocated internally) */
typedef struct GravelDBCkptExport GravelDBCkptExport;

/*
 * Checkpoint Export API (zero-backpressure design)
 *
 * Export runs at full speed, writing to a local dump file. The host environment
 * never blocks the export pipeline — it reads the completed dump afterwards.
 *
 * Lifecycle:
 *   begin → step (repeat until GRAVELDB_OK) → end
 *   After end, the dump file is ready at the path returned by export_path().
 *   Host reads it at its own pace via export_read() or direct file access.
 *
 * The dump file is written sequentially to local storage. Even if the host is
 * slow (network send, compression, etc.), the export completes at full IO speed.
 */

/* Begin a checkpoint export session for a given dimension.
 * Requires that checkpoint_step has placed the bin in checkpoint mode (overlay active).
 * type: GRAVELDB_CKPT_FULL or GRAVELDB_CKPT_DELTA.
 * dim: dimension to export.
 * base_gen: for delta, the generation this is relative to. For full, pass 0.
 * batch_entries: entries to process per step call (0 = default 256).
 * flags: 0, or GRAVELDB_CKPT_ALSO_FULL to also produce a full dump alongside delta. */
graveldb_status_t graveldb_ckpt_export_begin(GravelDB *db, GravelDBCkptExport **out,
                                             int type, int dim, uint64_t base_gen,
                                             uint32_t batch_entries, uint32_t flags);

/* Advance export: submit IO for next batch, write completed entries to dump file.
 * Returns GRAVELDB_AGAIN if more work remains.
 * Returns GRAVELDB_OK when export is complete. */
graveldb_status_t graveldb_ckpt_export_step(GravelDBCkptExport *exp);

/* Non-blocking poll: reap completed IO from current step.
 * Returns GRAVELDB_AGAIN if IO still in flight.
 * Returns GRAVELDB_OK when current step's IO is done (call step again or end). */
graveldb_status_t graveldb_ckpt_export_poll(GravelDBCkptExport *exp);

/* Finalize export: write trailer, fsync dump file.
 * After this returns, the dump file is complete and readable. */
graveldb_status_t graveldb_ckpt_export_end(GravelDBCkptExport *exp);

/* Get the path to the dump file (valid after begin, stable until end).
 * The file grows as export_step progresses; after end it is complete. */
const char *graveldb_ckpt_export_path(const GravelDBCkptExport *exp);

/* Get total bytes written to dump so far. */
uint64_t graveldb_ckpt_export_size(const GravelDBCkptExport *exp);

/* Sequential read from the dump file (for host consumption after export_end).
 * Reads up to `len` bytes starting from the internal read cursor.
 * Returns bytes actually read (0 at EOF), or < 0 on error.
 * The host can call this in a loop at its own pace. */
ssize_t graveldb_ckpt_export_read(GravelDBCkptExport *exp, void *buf, size_t len);

/* Reset the read cursor to the beginning (re-read from start). */
void graveldb_ckpt_export_read_reset(GravelDBCkptExport *exp);

/* Release export context and close dump file.
 * Call after the host has finished reading the dump. */
void graveldb_ckpt_export_destroy(GravelDBCkptExport *exp);

/* Flag for export_begin: when exporting delta, also produce a full dump.
 * The full dump is generated internally in the same step loop.
 * Retrieve the full dump path via graveldb_ckpt_export_full_path(). */
#define GRAVELDB_CKPT_ALSO_FULL  (1u << 0)

/* Get the full dump path (only valid if GRAVELDB_CKPT_ALSO_FULL was set).
 * Returns NULL if no paired full was created. */
const char *graveldb_ckpt_export_full_path(const GravelDBCkptExport *exp);

/* Opaque import context */
typedef struct GravelDBCkptImport GravelDBCkptImport;

/* Begin a checkpoint import session from a dump file.
 * The dump file is read directly — no host IO involvement.
 * dim: dimension to import into (must match the dump's dimension).
 * path: path to the dump file (from export_path, or host-placed file). */
graveldb_status_t graveldb_ckpt_import_begin(GravelDB *db, GravelDBCkptImport **out,
                                             int dim, const char *path);

/* Advance import: read next batch and submit writes via io_uring.
 * Returns GRAVELDB_AGAIN if more data to process.
 * Returns GRAVELDB_OK when all entries have been written. */
graveldb_status_t graveldb_ckpt_import_step(GravelDBCkptImport *imp);

/* Non-blocking poll: reap completed IO from current import step.
 * Returns GRAVELDB_AGAIN if IO still in flight.
 * Returns GRAVELDB_OK when current step's IO is done. */
graveldb_status_t graveldb_ckpt_import_poll(GravelDBCkptImport *imp);

/* Finalize import and release resources. */
graveldb_status_t graveldb_ckpt_import_end(GravelDBCkptImport *imp);

/*
 * Checkpoint Dump Parser
 *
 * Standalone parser for dump files. Does not require a GravelDB instance.
 * Useful for: format conversion, filtering, validation, debugging tools,
 * or forwarding entries to a different system.
 *
 * The parser opens the file, validates the header, and emits entries
 * one-by-one via a listener callback. The caller controls the pace.
 */

typedef struct {
    uint64_t    feat_id;
    const float *embedding;   /* valid until next emit or parse returns */
    uint32_t    dim;
} GravelDBCkptEntry;

/* Listener callback: called for each parsed entry.
 * Return 0 to continue parsing, non-zero to stop early (value propagated as-is). */
typedef int (*graveldb_ckpt_entry_fn)(void *ctx, const GravelDBCkptEntry *entry);

typedef struct {
    uint64_t generation;
    uint64_t base_gen;
    uint64_t num_entries;     /* from header; 0 if unknown */
    uint32_t dim;
    uint32_t entry_size;      /* bytes per embedding (dim * sizeof(float)) */
    int      type;            /* GRAVELDB_CKPT_FULL or GRAVELDB_CKPT_DELTA */
} GravelDBCkptDumpHeader;

/* Parse a dump file, emitting each entry via the listener.
 * header_out: if non-NULL, filled with parsed header metadata.
 * Returns GRAVELDB_OK on successful complete parse.
 * Returns GRAVELDB_ERR_CORRUPT if format is invalid.
 * Returns GRAVELDB_ERR_IO on file open/read failure.
 * If emit_fn returns non-zero, parsing stops and that value is returned
 * (cast to graveldb_status_t; positive = early stop, not an error). */
graveldb_status_t graveldb_ckpt_parse(const char *path,
                                      GravelDBCkptDumpHeader *header_out,
                                      graveldb_ckpt_entry_fn emit_fn,
                                      void *emit_ctx);

typedef struct {
    uint64_t total_features;
    uint64_t total_entries;
    uint64_t buffer_hits;
    uint64_t buffer_misses;
    uint64_t buffer_evictions;
    uint64_t flush_bytes;
    uint64_t checkpoint_generation;
    float    dirty_ratio;
    float    cache_hit_ratio;
} GravelDBStats;

graveldb_status_t graveldb_stats(GravelDB *db, GravelDBStats *stats);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_H_ */
