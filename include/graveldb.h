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

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    GRAVELDB_OK            =  0,
    GRAVELDB_ERR_IO        = -1,
    GRAVELDB_ERR_OOM       = -2,
    GRAVELDB_ERR_NOT_FOUND = -3,
    GRAVELDB_ERR_CORRUPT   = -4,
    GRAVELDB_ERR_FULL      = -5,
    GRAVELDB_ERR_INVALID   = -6,
} graveldb_status_t;

typedef struct GravelDB GravelDB;

typedef struct GravelDBCtx {
    void *reserved;  /* unused, kept for ABI stability */
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

    int         delta_chain_max;     /* 0 = default (10) */
    float       dirty_ratio_full;    /* 0 = default (0.5) */
} GravelDBConfig;

graveldb_status_t graveldb_open(GravelDB **db, const GravelDBConfig *config);
void              graveldb_close(GravelDB *db);

/* feat_id must be non-zero (0 is reserved as internal sentinel). */
graveldb_status_t graveldb_put(GravelDB *db, GravelDBCtx *ctx,
                               uint64_t feat_id, int dim, const float *embedding);
graveldb_status_t graveldb_get(GravelDB *db, GravelDBCtx *ctx,
                               uint64_t feat_id, float *out_embedding, int *out_dim);
graveldb_status_t graveldb_delete(GravelDB *db, GravelDBCtx *ctx, uint64_t feat_id);

graveldb_status_t graveldb_batch_get(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids, int n,
                                     float **out_embeddings, int *out_dims);
graveldb_status_t graveldb_batch_put(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids,
                                     const int *dims, const float *const *embeddings, int n);


/* Persist all data to disk with crash safety (delta checkpoint).
 * Safe to call at any time; concurrent reads/writes are isolated via overlay. */
graveldb_status_t graveldb_checkpoint(GravelDB *db);

/* Incremental checkpoint: call repeatedly from event loop.
 * Each call does bounded work (at most max_pages_per_step pages of I/O).
 * Check graveldb_checkpoint_in_progress() to know if done. */
graveldb_status_t graveldb_checkpoint_step(GravelDB *db, uint32_t max_pages_per_step);
bool              graveldb_checkpoint_in_progress(const GravelDB *db);

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
