/*
 * GravelDB - Implementation Header (full GravelDB struct aggregate).
 * Only graveldb.c and checkpoint.c include this; sub-modules include their own .h.
 */

#ifndef GRAVELDB_IMPL_H_
#define GRAVELDB_IMPL_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>

#include "graveldb.h"
#include "dimbin.h"
#include "dim_registry.h"
#include "dirty_tracker.h"
#include "slab_alloc.h"
#include "checkpoint.h"
#include "io_uring_flush.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Number of slots migrated per operation during incremental rehash */
#define HASH_REHASH_BATCH  16

/*
 * Hash slot: stores one key-value mapping in an open-addressing table.
 * feat_id == 0 means the slot is empty (sentinel).
 * NOTE: feat_id=0 is reserved and cannot be used as a valid feature ID.
 */
typedef struct {
    uint64_t  feat_id;
    uint16_t  dim_idx;
    uint32_t  entry_idx;
} HashSlot;

/*
 * HashIndex: open-addressing with linear probing + incremental rehash.
 * During rehash: lookups probe new table first, then old table.
 */
typedef struct {
    HashSlot   *slots;          /* current (new) table */
    uint32_t    capacity;       /* current table capacity (power of 2) */
    uint32_t    count;          /* total live entries (across both tables) */
    uint32_t    mask;           /* capacity - 1 */

    /* Incremental rehash state (NULL when not rehashing) */
    HashSlot   *old_slots;
    uint32_t    old_capacity;
    uint32_t    old_mask;
    uint32_t    rehash_cursor;  /* next slot to migrate in old_slots */
} HashIndex;

/*
 * Iterator for scanning all entries in the hash table.
 * Handles iteration across both tables during rehash.
 */
typedef struct {
    const HashIndex *index;
    uint32_t         pos;
    bool             in_old;    /* true if currently iterating old_slots */
} HashIter;

/* Lifecycle */
graveldb_status_t hash_index_init(HashIndex *idx, uint32_t capacity);
void              hash_index_destroy(HashIndex *idx);

/* Point operations */
graveldb_status_t hash_index_put(HashIndex *idx, uint64_t feat_id,
                                 uint16_t dim_idx, uint32_t entry_idx);
graveldb_status_t hash_index_get(const HashIndex *idx, uint64_t feat_id,
                                 uint16_t *out_dim_idx, uint32_t *out_entry_idx);
graveldb_status_t hash_index_remove(HashIndex *idx, uint64_t feat_id);

/* Iteration */
void hash_iter_init(const HashIndex *idx, HashIter *it);
bool hash_iter_next(HashIter *it, uint64_t *feat_id,
                    uint16_t *dim_idx, uint32_t *entry_idx);

/* Force complete any in-progress rehash (useful before serialization) */
void hash_index_finish_rehash(HashIndex *idx);

#define GRAVELDB_TLAB_SIZE         1024
#define GRAVELDB_DELTA_CHAIN_MAX   10
#define GRAVELDB_DIRTY_RATIO_FULL  0.5f

#define GRAVELDB_MAGIC             0x47525644  /* "GRVD" */
#define GRAVELDB_DELTA_MAGIC       0x44454C54  /* "DELT" */
#define GRAVELDB_MAX_DIRTY         (1 << 20)   /* 1M dirty pages per scan */

typedef struct GravelDB {
    GravelDBConfig  config;
    DimRegistry     dim_reg;
    HashIndex       index;

    uint64_t        current_epoch;

    /* Slab allocator (owned; initialized in graveldb_open, destroyed in close) */
    SlabAllocator   allocator;

    /* Persistent io_uring ring (reused across flushes to avoid setup/teardown).
     * Initialized once in graveldb_open, destroyed in graveldb_close. */
    uring_io_ctx_t  io_ring;

    char           *data_dir;
    bool            auto_create_bins;

    /* Incremental checkpoint state */
    CkptProgress    ckpt_progress;
} GravelDB;

void ensure_dir(const char *path);
uint32_t detect_page_size(const char *path);

/*
 * Context-aware allocation: uses ctx->alloc if provided, else malloc/free.
 * Arena mode: ctx->alloc set but ctx->dealloc NULL → free is no-op.
 */
static inline void *ctx_alloc(const GravelDBCtx *ctx, size_t size) {
    if (ctx && ctx->alloc) {
        return ctx->alloc(ctx->opaque, size);
    }
    return malloc(size);
}

static inline void ctx_dealloc(const GravelDBCtx *ctx, void *ptr, size_t size) {
    if (!ptr) return;
    if (ctx && ctx->alloc) {
        if (ctx->dealloc) ctx->dealloc(ctx->opaque, ptr, size);
        return;
    }
    free(ptr);
}

/*
 * Flush all write buffers to disk (internal operation).
 * Writes dirty pages from all DimBin write buffers to their data files.
 * This does NOT guarantee crash safety -- use graveldb_checkpoint for that.
 */
graveldb_status_t graveldb_flush(GravelDB *db);

/*
 * Synchronous delta checkpoint (internal, used by scheduler).
 * Public API users should use graveldb_checkpoint_step() instead.
 */
graveldb_status_t graveldb_checkpoint(GravelDB *db);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_IMPL_H_ */
