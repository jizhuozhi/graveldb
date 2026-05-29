/*
 * GravelDB - Implementation Header (GravelDB aggregate struct)
 *
 * This is the ONLY file that pulls together all sub-modules into the
 * final GravelDB struct. It replaces the old "internal.h" god-header.
 *
 * Dependency policy:
 *   - Each sub-module (overlay, dimbin, dirty_tracker,
 *     checkpoint, etc.) is self-contained: it defines its own types and
 *     only includes what it truly needs.
 *   - This file is the single aggregation point. Only translation units
 *     that need the full GravelDB struct (graveldb.c, checkpoint.c)
 *     include this header.
 *   - Sub-module .c files include their own .h, NOT this file.
 *
 * Build cache benefit:
 *   Modifying a sub-module only rebuilds that .c and files that truly
 *   depend on it -- not all translation units.
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
 * HashIndex: open-addressing hash table with linear probing
 * and incremental rehash support.
 *
 * When rehashing:
 *   - old_slots holds the previous table being migrated
 *   - rehash_cursor tracks progress through old_slots
 *   - Lookups probe new table first, then old table if key not found
 *   - Each mutation operation migrates HASH_REHASH_BATCH slots
 */
typedef struct {
    HashSlot   *slots;          /* current (new) table */
    uint32_t    capacity;       /* current table capacity */
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
#define GRAVELDB_FOOTER_MAGIC      0x46545244  /* "FTRD" */
#define GRAVELDB_MAX_DIRTY         (1 << 20)   /* 1M dirty pages per scan */

typedef struct GravelDB {
    GravelDBConfig  config;
    DimRegistry     dim_reg;
    HashIndex       index;

    uint64_t        current_epoch;

    /* Slab allocator (owned; initialized in graveldb_open, destroyed in close) */
    SlabAllocator   allocator;

    char           *data_dir;
    bool            auto_create_bins;

    /* Incremental checkpoint state */
    CkptProgress    ckpt_progress;
} GravelDB;

void ensure_dir(const char *path);
uint32_t detect_page_size(const char *path);

/*
 * Context-aware allocation helpers.
 *
 * Rule: alloc and dealloc are ALWAYS resolved as a pair from the same source.
 *   - ctx->alloc != NULL  → use ctx->alloc for allocation
 *   - ctx->dealloc != NULL → use ctx->dealloc for deallocation
 *   - ctx->alloc != NULL && ctx->dealloc == NULL → arena mode (no-op free)
 *   - ctx == NULL || ctx->alloc == NULL → fallback to malloc/free
 *
 * The pair is determined ONCE and used consistently. Never mix sources.
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
        /* Allocation came from ctx->alloc, so dealloc must also go through ctx */
        if (ctx->dealloc) {
            ctx->dealloc(ctx->opaque, ptr, size);
        }
        /* else: arena mode — caller manages lifetime, no-op here */
        return;
    }
    /* Allocation came from malloc, free with stdlib */
    free(ptr);
}

/*
 * Flush all write buffers to disk (internal operation).
 * Writes dirty pages from all DimBin write buffers to their data files.
 * This does NOT guarantee crash safety -- use graveldb_checkpoint for that.
 */
graveldb_status_t graveldb_flush(GravelDB *db);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_IMPL_H_ */
