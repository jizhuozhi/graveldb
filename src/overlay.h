/*
 * GravelDB - Overlay Buffer for Checkpoint Isolation
 *
 * Self-contained module: defines OverlaySlot/OverlayBuffer and operations.
 * Depends only on slab_alloc.h for allocator types.
 *
 * Design:
 *   Open-addressing hash table (linear probing) with incremental rehash.
 *   Same rehash strategy as HashIndex -- unified approach for all table sizes.
 *   Key is uint32_t entry_id; sentinel is UINT32_MAX (not 0, since entry_id=0
 *   is valid).
 *
 *   Embedding data is allocated from a dedicated SlabPool and destroyed in
 *   bulk at checkpoint_end.
 */

#ifndef GRAVELDB_OVERLAY_H_
#define GRAVELDB_OVERLAY_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "graveldb.h"
#include "slab_alloc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Sentinel value for empty slots (entry_id space is uint32_t, all values valid) */
#define OVERLAY_EMPTY  UINT32_MAX

/* Rehash batch size (same philosophy as HashIndex) */
#define OVERLAY_REHASH_BATCH  16

/*
 * Overlay slot: one entry_id -> embedding mapping.
 * entry_id == OVERLAY_EMPTY means the slot is empty.
 */
typedef struct {
    uint32_t entry_id;
    float   *data;
} OverlaySlot;

typedef struct {
    OverlaySlot   *slots;        /* current (new) table */
    uint32_t       capacity;     /* current table capacity (power of 2) */
    uint32_t       count;        /* total live entries */
    uint32_t       mask;         /* capacity - 1 */

    /* Incremental rehash state */
    OverlaySlot   *old_slots;
    uint32_t       old_capacity;
    uint32_t       old_mask;
    uint32_t       rehash_cursor;

    /* Tombstone list (logical deletes, deferred until checkpoint_end) */
    uint32_t      *tombstones;
    uint32_t       tomb_count;
    uint32_t       tomb_capacity;

    size_t         memory_used;
    size_t         budget_bytes;  /* max memory; 0 = unlimited */

    SlabPool      *data_pool;
    SlabAllocator *allocator;
} OverlayBuffer;

/* Returns true when overlay memory usage has reached the budget limit. */
static inline bool overlay_full(const OverlayBuffer *ob) {
    if (ob->budget_bytes == 0) return false;
    return ob->memory_used >= ob->budget_bytes;
}

graveldb_status_t overlay_init(OverlayBuffer *ob, int dim);
void overlay_destroy(OverlayBuffer *ob);
graveldb_status_t overlay_put(OverlayBuffer *ob, uint32_t entry_id,
                               const float *data, int dim);
bool overlay_get(const OverlayBuffer *ob, uint32_t entry_id, float *buf, int dim);
bool overlay_contains(const OverlayBuffer *ob, uint32_t entry_id);
graveldb_status_t overlay_tombstone(OverlayBuffer *ob, uint32_t entry_id);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_OVERLAY_H_ */
