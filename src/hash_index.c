/*
 * GravelDB - Hash Index Implementation
 *
 * Open-addressing hash table with linear probing and incremental rehash.
 * Slot type: { feat_id (64-bit key), dim_idx (16-bit), entry_idx (32-bit) }
 * Sentinel: feat_id == 0 (feat_id=0 is reserved and cannot be stored).
 */

#include "graveldb_impl.h"
#include <stdlib.h>
#include <string.h>

static inline uint32_t hash64(uint64_t key) {
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return (uint32_t)key;
}

static inline void insert_into_table(HashSlot *slots, uint32_t mask,
                                     uint64_t feat_id, uint16_t dim_idx,
                                     uint32_t entry_idx) {
    uint32_t h = hash64(feat_id) & mask;
    while (slots[h].feat_id != 0) {
        h = (h + 1) & mask;
    }
    slots[h].feat_id = feat_id;
    slots[h].dim_idx = dim_idx;
    slots[h].entry_idx = entry_idx;
}

static void rehash_step(HashIndex *idx) {
    if (!idx->old_slots) return;

    int migrated = 0;
    while (idx->rehash_cursor < idx->old_capacity && migrated < HASH_REHASH_BATCH) {
        HashSlot *slot = &idx->old_slots[idx->rehash_cursor];
        if (slot->feat_id != 0) {
            insert_into_table(idx->slots, idx->mask,
                              slot->feat_id, slot->dim_idx, slot->entry_idx);
            slot->feat_id = 0;
            migrated++;
        }
        idx->rehash_cursor++;
    }

    if (idx->rehash_cursor >= idx->old_capacity) {
        free(idx->old_slots);
        idx->old_slots = NULL;
        idx->old_capacity = 0;
        idx->old_mask = 0;
        idx->rehash_cursor = 0;
    }
}

static graveldb_status_t hash_index_begin_grow(HashIndex *idx) {
    uint32_t new_cap = idx->capacity * 2;
    HashSlot *new_slots = (HashSlot *)calloc(new_cap, sizeof(HashSlot));
    if (!new_slots) return GRAVELDB_ERR_OOM;

    idx->old_slots = idx->slots;
    idx->old_capacity = idx->capacity;
    idx->old_mask = idx->mask;
    idx->rehash_cursor = 0;

    idx->slots = new_slots;
    idx->capacity = new_cap;
    idx->mask = new_cap - 1;

    return GRAVELDB_OK;
}

graveldb_status_t hash_index_init(HashIndex *idx, uint32_t capacity) {
    uint32_t cap = 1;
    while (cap < capacity) cap <<= 1;

    idx->slots = (HashSlot *)calloc(cap, sizeof(HashSlot));
    if (!idx->slots) return GRAVELDB_ERR_OOM;

    idx->capacity = cap;
    idx->count = 0;
    idx->mask = cap - 1;

    idx->old_slots = NULL;
    idx->old_capacity = 0;
    idx->old_mask = 0;
    idx->rehash_cursor = 0;

    return GRAVELDB_OK;
}

void hash_index_destroy(HashIndex *idx) {
    free(idx->slots);
    free(idx->old_slots);
    idx->slots = NULL;
    idx->old_slots = NULL;
    idx->capacity = 0;
    idx->old_capacity = 0;
    idx->count = 0;
}

graveldb_status_t hash_index_put(HashIndex *idx, uint64_t feat_id,
                                 uint16_t dim_idx, uint32_t entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_INVALID;

    rehash_step(idx);

    if (!idx->old_slots && idx->count * 10 > idx->capacity * 7) {
        graveldb_status_t st = hash_index_begin_grow(idx);
        if (st != GRAVELDB_OK) return st;
    }

    uint32_t h = hash64(feat_id) & idx->mask;
    while (idx->slots[h].feat_id != 0) {
        if (idx->slots[h].feat_id == feat_id) {
            idx->slots[h].dim_idx = dim_idx;
            idx->slots[h].entry_idx = entry_idx;
            return GRAVELDB_OK;
        }
        h = (h + 1) & idx->mask;
    }

    if (idx->old_slots) {
        uint32_t oh = hash64(feat_id) & idx->old_mask;
        uint32_t start = oh;
        while (idx->old_slots[oh].feat_id != 0) {
            if (idx->old_slots[oh].feat_id == feat_id) {
                idx->old_slots[oh].dim_idx = dim_idx;
                idx->old_slots[oh].entry_idx = entry_idx;
                return GRAVELDB_OK;
            }
            oh = (oh + 1) & idx->old_mask;
            if (oh == start) break;
        }
    }

    idx->slots[h].feat_id = feat_id;
    idx->slots[h].dim_idx = dim_idx;
    idx->slots[h].entry_idx = entry_idx;
    idx->count++;
    return GRAVELDB_OK;
}

graveldb_status_t hash_index_get(const HashIndex *idx, uint64_t feat_id,
                                 uint16_t *out_dim_idx, uint32_t *out_entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;

    rehash_step((HashIndex *)idx);

    uint32_t h = hash64(feat_id) & idx->mask;
    uint32_t start = h;
    while (idx->slots[h].feat_id != 0) {
        if (idx->slots[h].feat_id == feat_id) {
            *out_dim_idx = idx->slots[h].dim_idx;
            *out_entry_idx = idx->slots[h].entry_idx;
            return GRAVELDB_OK;
        }
        h = (h + 1) & idx->mask;
        if (h == start) break;
    }

    if (idx->old_slots) {
        h = hash64(feat_id) & idx->old_mask;
        start = h;
        while (idx->old_slots[h].feat_id != 0) {
            if (idx->old_slots[h].feat_id == feat_id) {
                *out_dim_idx = idx->old_slots[h].dim_idx;
                *out_entry_idx = idx->old_slots[h].entry_idx;
                return GRAVELDB_OK;
            }
            h = (h + 1) & idx->old_mask;
            if (h == start) break;
        }
    }

    return GRAVELDB_ERR_NOT_FOUND;
}

graveldb_status_t hash_index_remove(HashIndex *idx, uint64_t feat_id) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;

    rehash_step(idx);

    uint32_t h = hash64(feat_id) & idx->mask;
    uint32_t start = h;
    while (idx->slots[h].feat_id != 0) {
        if (idx->slots[h].feat_id == feat_id) {
            idx->count--;

            uint32_t empty = h;
            uint32_t j = (empty + 1) & idx->mask;
            while (idx->slots[j].feat_id != 0) {
                uint32_t ideal = hash64(idx->slots[j].feat_id) & idx->mask;
                bool displaced;
                if (empty < j) {
                    displaced = (ideal <= empty || ideal > j);
                } else {
                    displaced = (ideal <= empty && ideal > j);
                }
                if (displaced) {
                    idx->slots[empty] = idx->slots[j];
                    empty = j;
                }
                j = (j + 1) & idx->mask;
                if (j == h) break;
            }
            idx->slots[empty].feat_id = 0;
            return GRAVELDB_OK;
        }
        h = (h + 1) & idx->mask;
        if (h == start) break;
    }

    if (idx->old_slots) {
        h = hash64(feat_id) & idx->old_mask;
        start = h;
        while (idx->old_slots[h].feat_id != 0) {
            if (idx->old_slots[h].feat_id == feat_id) {
                idx->old_slots[h].feat_id = 0;
                idx->count--;
                return GRAVELDB_OK;
            }
            h = (h + 1) & idx->old_mask;
            if (h == start) break;
        }
    }

    return GRAVELDB_ERR_NOT_FOUND;
}

void hash_iter_init(const HashIndex *idx, HashIter *it) {
    it->index = idx;
    it->pos = 0;
    it->in_old = false;
}

bool hash_iter_next(HashIter *it, uint64_t *feat_id, uint16_t *dim_idx, uint32_t *entry_idx) {
    const HashIndex *idx = it->index;

    if (!it->in_old) {
        while (it->pos < idx->capacity) {
            if (idx->slots[it->pos].feat_id != 0) {
                *feat_id = idx->slots[it->pos].feat_id;
                *dim_idx = idx->slots[it->pos].dim_idx;
                *entry_idx = idx->slots[it->pos].entry_idx;
                it->pos++;
                return true;
            }
            it->pos++;
        }
        if (idx->old_slots) {
            it->in_old = true;
            it->pos = 0;
        } else {
            return false;
        }
    }

    while (it->pos < idx->old_capacity) {
        if (idx->old_slots[it->pos].feat_id != 0) {
            *feat_id = idx->old_slots[it->pos].feat_id;
            *dim_idx = idx->old_slots[it->pos].dim_idx;
            *entry_idx = idx->old_slots[it->pos].entry_idx;
            it->pos++;
            return true;
        }
        it->pos++;
    }

    return false;
}

void hash_index_finish_rehash(HashIndex *idx) {
    while (idx->old_slots) {
        rehash_step(idx);
    }
}
