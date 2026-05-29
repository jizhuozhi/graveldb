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

/* Swiss Table group width (must match SIMD register width) */
#define HASH_GROUP_WIDTH  16

/* ctrl byte values */
#define HASH_CTRL_EMPTY    ((uint8_t)0xFF)
#define HASH_CTRL_DELETED  ((uint8_t)0x80)
/* Occupied: top 7 bits of h2 with high bit clear (0x00..0x7F) */

/*
 * Hash slot: stores one key-value mapping.
 * No sentinel needed in data — ctrl byte array handles empty/occupied state.
 */
typedef struct {
    uint64_t  feat_id;
    uint16_t  dim_idx;
    uint32_t  entry_idx;
} HashSlot;

/*
 * Swiss Table hash index.
 *
 * Layout: ctrl[] array (1 byte per slot, grouped in 16-byte groups)
 *         + slots[] array (HashSlot per slot).
 *
 * Probe: hash → h1 (position) + h2 (7-bit fingerprint in ctrl).
 * Each probe step checks 16 ctrl bytes at once (NEON/SSE or byte loop).
 * ctrl byte encoding:
 *   0x00..0x7F = occupied (h2 fingerprint, high bit = 0)
 *   0x80       = deleted (tombstone)
 *   0xFF       = empty
 *
 * Capacity is always a multiple of HASH_GROUP_WIDTH.
 * ctrl[] has GROUP_WIDTH extra "mirror" bytes at end for unaligned SIMD loads.
 */
typedef struct {
    uint8_t    *ctrl;           /* ctrl byte array [capacity + GROUP_WIDTH] */
    HashSlot   *slots;          /* data array [capacity] */
    uint32_t    capacity;       /* always multiple of GROUP_WIDTH */
    uint32_t    count;          /* live entries (across both tables) */
    uint32_t    growth_left;    /* remaining inserts before grow */

    /* Incremental rehash state */
    uint8_t    *old_ctrl;
    HashSlot   *old_slots;
    uint32_t    old_capacity;
    uint32_t    rehash_cursor;
} HashIndex;

/*
 * Iterator for scanning all entries in the hash table.
 */
typedef struct {
    const HashIndex *index;
    uint32_t         pos;
    bool             in_old;
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
