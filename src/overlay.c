/*
 * GravelDB - Overlay Buffer for checkpoint isolation
 *
 * Open-addressing hash table with incremental rehash.
 * Same strategy as HashIndex: when load factor > 70%, allocate 2x table and
 * migrate OVERLAY_REHASH_BATCH slots per operation. Unified approach regardless
 * of table size -- small tables finish rehash in one or two operations.
 *
 * Hot-path optimization: each overlay creates a dedicated SlabPool for its
 * known fixed-size embedding data (dim * sizeof(float)). All allocs during
 * checkpoint go to this pool; on checkpoint_end, the entire pool is destroyed
 * in one shot -- zero per-object free overhead.
 *
 * The allocator is passed via ob->allocator (set by the owning DimBin).
 * No global state; supports multiple DB instances.
 */

#include "overlay.h"
#include <stdlib.h>
#include <string.h>

#define OVERLAY_INITIAL_CAP    256
#define OVERLAY_TOMB_INITIAL   256

/*
 * Hash for overlay (32-bit integer hash)
 */

static inline uint32_t overlay_hash(uint32_t entry_id) {
    uint32_t h = entry_id;
    h ^= h >> 16;
    h *= 0x45d9f3b;
    h ^= h >> 16;
    return h;
}

/*
 * Internal: initialize a slot array with OVERLAY_EMPTY sentinels.
 */
static inline void init_slots(OverlaySlot *slots, uint32_t capacity) {
    for (uint32_t i = 0; i < capacity; i++) {
        slots[i].entry_id = OVERLAY_EMPTY;
        slots[i].data = NULL;
    }
}

/*
 * Internal: insert into a specific slot array (no rehash trigger).
 * Returns pointer to the slot where inserted (for data assignment).
 */
static inline OverlaySlot *insert_into_table(OverlaySlot *slots, uint32_t mask,
                                              uint32_t entry_id) {
    uint32_t h = overlay_hash(entry_id) & mask;
    while (slots[h].entry_id != OVERLAY_EMPTY) {
        h = (h + 1) & mask;
    }
    slots[h].entry_id = entry_id;
    return &slots[h];
}

/*
 * Internal: migrate a batch of slots from old table to new table.
 */
static void overlay_rehash_step(OverlayBuffer *ob) {
    if (!ob->old_slots) return;

    int migrated = 0;
    while (ob->rehash_cursor < ob->old_capacity && migrated < OVERLAY_REHASH_BATCH) {
        OverlaySlot *slot = &ob->old_slots[ob->rehash_cursor];
        if (slot->entry_id != OVERLAY_EMPTY) {
            OverlaySlot *dst = insert_into_table(ob->slots, ob->mask, slot->entry_id);
            dst->data = slot->data;  /* transfer data pointer (pool-owned) */
            slot->entry_id = OVERLAY_EMPTY;
            migrated++;
        }
        ob->rehash_cursor++;
    }

    /* Check if migration complete */
    if (ob->rehash_cursor >= ob->old_capacity) {
        free(ob->old_slots);
        ob->old_slots = NULL;
        ob->old_capacity = 0;
        ob->old_mask = 0;
        ob->rehash_cursor = 0;
    }
}

/*
 * Internal: begin incremental rehash.
 */
static graveldb_status_t overlay_begin_grow(OverlayBuffer *ob) {
    uint32_t new_cap = ob->capacity * 2;
    OverlaySlot *new_slots = (OverlaySlot *)malloc(new_cap * sizeof(OverlaySlot));
    if (!new_slots) return GRAVELDB_ERR_OOM;
    init_slots(new_slots, new_cap);

    /* Current table becomes old table */
    ob->old_slots = ob->slots;
    ob->old_capacity = ob->capacity;
    ob->old_mask = ob->mask;
    ob->rehash_cursor = 0;

    /* New table is now current */
    ob->slots = new_slots;
    ob->capacity = new_cap;
    ob->mask = new_cap - 1;

    return GRAVELDB_OK;
}

/*
 * Init/Destroy
 */

graveldb_status_t overlay_init(OverlayBuffer *ob, int dim) {
    uint32_t cap = OVERLAY_INITIAL_CAP;

    ob->slots = (OverlaySlot *)malloc(cap * sizeof(OverlaySlot));
    if (!ob->slots) return GRAVELDB_ERR_OOM;
    init_slots(ob->slots, cap);

    ob->capacity = cap;
    ob->count = 0;
    ob->mask = cap - 1;

    /* No rehash in progress */
    ob->old_slots = NULL;
    ob->old_capacity = 0;
    ob->old_mask = 0;
    ob->rehash_cursor = 0;

    ob->tomb_capacity = OVERLAY_TOMB_INITIAL;
    ob->tomb_count = 0;
    ob->tombstones = (uint32_t *)malloc(ob->tomb_capacity * sizeof(uint32_t));
    if (!ob->tombstones) {
        free(ob->slots);
        ob->slots = NULL;
        return GRAVELDB_ERR_OOM;
    }

    ob->memory_used = 0;
    ob->budget_bytes = 0;  /* unlimited until caller sets it */

    /* Create a dedicated slab pool for this overlay's embedding data.
     * All entries have the same size: dim * sizeof(float).
     * Pool is destroyed in bulk at checkpoint_end -- zero per-object free cost.
     *
     * Overlay requires a valid allocator and dim -- there is no malloc fallback. */
    if (!ob->allocator || dim <= 0) {
        free(ob->slots);
        free(ob->tombstones);
        memset(ob, 0, sizeof(*ob));
        return GRAVELDB_ERR_INVALID;
    }

    size_t emb_size = (size_t)dim * sizeof(float);
    ob->data_pool = slab_pool_create(ob->allocator, emb_size, SLAB_DEFAULT_ALIGNMENT);
    if (!ob->data_pool) {
        free(ob->slots);
        free(ob->tombstones);
        memset(ob, 0, sizeof(*ob));
        return GRAVELDB_ERR_OOM;
    }

    return GRAVELDB_OK;
}

void overlay_destroy(OverlayBuffer *ob) {
    /* Pool owns ALL embedding data -- destroy it in one shot (no per-object free) */
    if (ob->data_pool && ob->allocator) {
        slab_pool_destroy(ob->allocator, ob->data_pool);
        ob->data_pool = NULL;
    }

    free(ob->slots);
    free(ob->old_slots);
    free(ob->tombstones);
    memset(ob, 0, sizeof(*ob));
}

/*
 * Put
 */

graveldb_status_t overlay_put(OverlayBuffer *ob, uint32_t entry_id,
                              const float *data, int dim) {
    /* Advance incremental rehash */
    overlay_rehash_step(ob);

    /* Trigger rehash if load > 70% and not already rehashing */
    if (!ob->old_slots && ob->count * 10 > ob->capacity * 7) {
        graveldb_status_t st = overlay_begin_grow(ob);
        if (st != GRAVELDB_OK) return st;
    }

    size_t data_size = (size_t)dim * sizeof(float);

    /* Check if already exists in new table */
    uint32_t h = overlay_hash(entry_id) & ob->mask;
    uint32_t start = h;
    while (ob->slots[h].entry_id != OVERLAY_EMPTY) {
        if (ob->slots[h].entry_id == entry_id) {
            memcpy(ob->slots[h].data, data, data_size);
            return GRAVELDB_OK;
        }
        h = (h + 1) & ob->mask;
        if (h == start) break;
    }

    /* Check old table during rehash */
    if (ob->old_slots) {
        uint32_t oh = overlay_hash(entry_id) & ob->old_mask;
        uint32_t ostart = oh;
        while (ob->old_slots[oh].entry_id != OVERLAY_EMPTY) {
            if (ob->old_slots[oh].entry_id == entry_id) {
                memcpy(ob->old_slots[oh].data, data, data_size);
                return GRAVELDB_OK;
            }
            oh = (oh + 1) & ob->old_mask;
            if (oh == ostart) break;
        }
    }

    /* New entry: insert into new table at position h (found empty above) */
    ob->slots[h].entry_id = entry_id;
    ob->slots[h].data = (float *)slab_pool_alloc(ob->data_pool);
    if (!ob->slots[h].data) {
        ob->slots[h].entry_id = OVERLAY_EMPTY;
        return GRAVELDB_ERR_OOM;
    }
    memcpy(ob->slots[h].data, data, data_size);
    ob->count++;

    ob->memory_used += data_size + sizeof(OverlaySlot);
    return GRAVELDB_OK;
}

/*
 * Get
 */

bool overlay_get(const OverlayBuffer *ob, uint32_t entry_id, float *buf, int dim) {
    /* Advance incremental rehash */
    overlay_rehash_step((OverlayBuffer *)ob);

    size_t data_size = (size_t)dim * sizeof(float);

    /* Search new table */
    uint32_t h = overlay_hash(entry_id) & ob->mask;
    uint32_t start = h;
    while (ob->slots[h].entry_id != OVERLAY_EMPTY) {
        if (ob->slots[h].entry_id == entry_id) {
            memcpy(buf, ob->slots[h].data, data_size);
            return true;
        }
        h = (h + 1) & ob->mask;
        if (h == start) break;
    }

    /* Search old table */
    if (ob->old_slots) {
        h = overlay_hash(entry_id) & ob->old_mask;
        start = h;
        while (ob->old_slots[h].entry_id != OVERLAY_EMPTY) {
            if (ob->old_slots[h].entry_id == entry_id) {
                memcpy(buf, ob->old_slots[h].data, data_size);
                return true;
            }
            h = (h + 1) & ob->old_mask;
            if (h == start) break;
        }
    }

    return false;
}

bool overlay_contains(const OverlayBuffer *ob, uint32_t entry_id) {
    /* Advance incremental rehash */
    overlay_rehash_step((OverlayBuffer *)ob);

    /* Search new table */
    uint32_t h = overlay_hash(entry_id) & ob->mask;
    uint32_t start = h;
    while (ob->slots[h].entry_id != OVERLAY_EMPTY) {
        if (ob->slots[h].entry_id == entry_id) return true;
        h = (h + 1) & ob->mask;
        if (h == start) break;
    }

    /* Search old table */
    if (ob->old_slots) {
        h = overlay_hash(entry_id) & ob->old_mask;
        start = h;
        while (ob->old_slots[h].entry_id != OVERLAY_EMPTY) {
            if (ob->old_slots[h].entry_id == entry_id) return true;
            h = (h + 1) & ob->old_mask;
            if (h == start) break;
        }
    }

    return false;
}

/*
 * Tombstone
 */

graveldb_status_t overlay_tombstone(OverlayBuffer *ob, uint32_t entry_id) {
    if (ob->tomb_count >= ob->tomb_capacity) {
        ob->tomb_capacity *= 2;
        ob->tombstones = (uint32_t *)realloc(ob->tombstones,
                                             ob->tomb_capacity * sizeof(uint32_t));
        if (!ob->tombstones) return GRAVELDB_ERR_OOM;
    }
    ob->tombstones[ob->tomb_count++] = entry_id;
    return GRAVELDB_OK;
}
