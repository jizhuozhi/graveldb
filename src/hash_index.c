/*
 * GravelDB - Swiss Table Hash Index
 *
 * Open-addressing hash table using separated ctrl metadata (1 byte/slot)
 * and data slots. Probes 16 ctrl bytes per step for fast SIMD-friendly
 * matching. Maintains incremental rehash for latency-bounded growth.
 */

#include "graveldb_impl.h"
#include <stdlib.h>
#include <string.h>

#ifdef __aarch64__
#include <arm_neon.h>
#define HASH_USE_NEON 1
#elif defined(__SSE2__)
#include <emmintrin.h>
#define HASH_USE_SSE2 1
#endif

static inline uint64_t hash64_full(uint64_t key) {
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return key;
}

/* h1: position hash (uses upper bits) */
static inline uint32_t h1(uint64_t hash) {
    return (uint32_t)(hash >> 7);
}

/* h2: 7-bit fingerprint stored in ctrl byte (0x00..0x7F) */
static inline uint8_t h2(uint64_t hash) {
    return (uint8_t)(hash & 0x7F);
}

/* Match mask: find all positions in a 16-byte group where ctrl == target */
static inline uint32_t group_match(const uint8_t *group, uint8_t target) {
#if HASH_USE_NEON
    uint8x16_t ctrl_vec = vld1q_u8(group);
    uint8x16_t match_vec = vceqq_u8(ctrl_vec, vdupq_n_u8(target));
    /* Convert byte mask to bit mask (one bit per byte) */
    static const uint8_t shift_tbl[16] = {1,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128};
    uint8x16_t shifted = vandq_u8(match_vec, vld1q_u8(shift_tbl));
    uint8x8_t lo = vget_low_u8(shifted);
    uint8x8_t hi = vget_high_u8(shifted);
    uint8_t lo_sum = vaddv_u8(lo);
    uint8_t hi_sum = vaddv_u8(hi);
    return (uint32_t)lo_sum | ((uint32_t)hi_sum << 8);
#elif HASH_USE_SSE2
    __m128i ctrl_vec = _mm_loadu_si128((const __m128i *)group);
    __m128i match_vec = _mm_cmpeq_epi8(ctrl_vec, _mm_set1_epi8((char)target));
    return (uint32_t)_mm_movemask_epi8(match_vec);
#else
    uint32_t mask = 0;
    for (int i = 0; i < 16; i++) {
        if (group[i] == target) mask |= (1u << i);
    }
    return mask;
#endif
}

/* Find any empty slot in group */
static inline uint32_t group_match_empty(const uint8_t *group) {
    return group_match(group, HASH_CTRL_EMPTY);
}

/* Find any empty or deleted slot in group */
static inline uint32_t group_match_empty_or_deleted(const uint8_t *group) {
#if HASH_USE_NEON
    uint8x16_t ctrl_vec = vld1q_u8(group);
    /* empty=0xFF, deleted=0x80: both have high bit set */
    uint8x16_t match_vec = vcgeq_u8(ctrl_vec, vdupq_n_u8(0x80));
    static const uint8_t shift_tbl[16] = {1,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128};
    uint8x16_t shifted = vandq_u8(match_vec, vld1q_u8(shift_tbl));
    uint8x8_t lo = vget_low_u8(shifted);
    uint8x8_t hi = vget_high_u8(shifted);
    return (uint32_t)vaddv_u8(lo) | ((uint32_t)vaddv_u8(hi) << 8);
#elif HASH_USE_SSE2
    __m128i ctrl_vec = _mm_loadu_si128((const __m128i *)group);
    /* Check high bit: both 0x80 and 0xFF have it set */
    return (uint32_t)_mm_movemask_epi8(ctrl_vec);
#else
    uint32_t mask = 0;
    for (int i = 0; i < 16; i++) {
        if (group[i] >= 0x80) mask |= (1u << i);
    }
    return mask;
#endif
}

static inline int ctz(uint32_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctz(x);
#else
    int n = 0;
    if (!(x & 0xFFFF)) { n += 16; x >>= 16; }
    if (!(x & 0xFF))   { n += 8;  x >>= 8; }
    if (!(x & 0xF))    { n += 4;  x >>= 4; }
    if (!(x & 0x3))    { n += 2;  x >>= 2; }
    if (!(x & 0x1))    { n += 1; }
    return n;
#endif
}

/* capacity must be multiple of GROUP_WIDTH, minimum GROUP_WIDTH */
static uint32_t normalize_capacity(uint32_t requested) {
    uint32_t cap = HASH_GROUP_WIDTH;
    while (cap < requested) cap <<= 1;
    return cap;
}

/* Max load before grow: ~7/8 of capacity (87.5%) */
static inline uint32_t capacity_to_growth(uint32_t capacity) {
    return capacity - capacity / 8;
}

static uint8_t *alloc_ctrl(uint32_t capacity) {
    /* Extra GROUP_WIDTH bytes for mirror (allows unaligned SIMD at end) */
    uint8_t *ctrl = (uint8_t *)malloc(capacity + HASH_GROUP_WIDTH);
    if (ctrl) memset(ctrl, HASH_CTRL_EMPTY, capacity + HASH_GROUP_WIDTH);
    return ctrl;
}

static void set_ctrl(uint8_t *ctrl, uint32_t capacity, uint32_t pos, uint8_t val) {
    ctrl[pos] = val;
    /* Mirror: if pos < GROUP_WIDTH, also write to tail mirror */
    if (pos < HASH_GROUP_WIDTH) {
        ctrl[capacity + pos] = val;
    }
}

static void rehash_step(HashIndex *idx) {
    if (!idx->old_ctrl) return;

    int migrated = 0;
    while (idx->rehash_cursor < idx->old_capacity && migrated < HASH_REHASH_BATCH) {
        uint8_t c = idx->old_ctrl[idx->rehash_cursor];
        if (c != HASH_CTRL_EMPTY && c != HASH_CTRL_DELETED) {
            /* Migrate this slot to new table */
            HashSlot *slot = &idx->old_slots[idx->rehash_cursor];
            uint64_t full_hash = hash64_full(slot->feat_id);
            uint32_t pos = h1(full_hash) % idx->capacity;
            uint8_t fingerprint = h2(full_hash);

            /* Find empty slot in new table (guaranteed to exist) */
            while (1) {
                uint32_t group_start = pos - (pos % HASH_GROUP_WIDTH);
                uint32_t empty_mask = group_match_empty(&idx->ctrl[group_start]);
                if (empty_mask) {
                    uint32_t offset = (uint32_t)ctz(empty_mask);
                    uint32_t target = group_start + offset;
                    set_ctrl(idx->ctrl, idx->capacity, target, fingerprint);
                    idx->slots[target] = *slot;
                    break;
                }
                pos = (group_start + HASH_GROUP_WIDTH) % idx->capacity;
            }
            migrated++;
        }
        idx->rehash_cursor++;
    }

    if (idx->rehash_cursor >= idx->old_capacity) {
        free(idx->old_ctrl);
        free(idx->old_slots);
        idx->old_ctrl = NULL;
        idx->old_slots = NULL;
        idx->old_capacity = 0;
        idx->rehash_cursor = 0;
    }
}

static graveldb_status_t hash_index_begin_grow(HashIndex *idx) {
    uint32_t new_cap = idx->capacity * 2;
    uint8_t *new_ctrl = alloc_ctrl(new_cap);
    if (!new_ctrl) return GRAVELDB_ERR_OOM;

    HashSlot *new_slots = (HashSlot *)malloc((size_t)new_cap * sizeof(HashSlot));
    if (!new_slots) { free(new_ctrl); return GRAVELDB_ERR_OOM; }

    idx->old_ctrl = idx->ctrl;
    idx->old_slots = idx->slots;
    idx->old_capacity = idx->capacity;
    idx->rehash_cursor = 0;

    idx->ctrl = new_ctrl;
    idx->slots = new_slots;
    idx->capacity = new_cap;
    idx->growth_left = capacity_to_growth(new_cap) - idx->count;

    return GRAVELDB_OK;
}

graveldb_status_t hash_index_init(HashIndex *idx, uint32_t capacity) {
    uint32_t cap = normalize_capacity(capacity);

    idx->ctrl = alloc_ctrl(cap);
    if (!idx->ctrl) return GRAVELDB_ERR_OOM;

    idx->slots = (HashSlot *)malloc((size_t)cap * sizeof(HashSlot));
    if (!idx->slots) { free(idx->ctrl); return GRAVELDB_ERR_OOM; }

    idx->capacity = cap;
    idx->count = 0;
    idx->growth_left = capacity_to_growth(cap);

    idx->old_ctrl = NULL;
    idx->old_slots = NULL;
    idx->old_capacity = 0;
    idx->rehash_cursor = 0;

    return GRAVELDB_OK;
}

void hash_index_destroy(HashIndex *idx) {
    free(idx->ctrl);
    free(idx->slots);
    free(idx->old_ctrl);
    free(idx->old_slots);
    idx->ctrl = NULL;
    idx->slots = NULL;
    idx->old_ctrl = NULL;
    idx->old_slots = NULL;
    idx->capacity = 0;
    idx->old_capacity = 0;
    idx->count = 0;
}

/* Find slot in a given table. Returns position or capacity if not found. */
static uint32_t find_in_table(const uint8_t *ctrl, const HashSlot *slots,
                              uint32_t capacity, uint64_t feat_id, uint64_t full_hash) {
    uint8_t fingerprint = h2(full_hash);
    uint32_t pos = h1(full_hash) % capacity;

    while (1) {
        uint32_t group_start = pos - (pos % HASH_GROUP_WIDTH);
        const uint8_t *group = &ctrl[group_start];

        /* Check for fingerprint matches */
        uint32_t match_mask = group_match(group, fingerprint);
        while (match_mask) {
            uint32_t offset = (uint32_t)ctz(match_mask);
            uint32_t candidate = group_start + offset;
            if (slots[candidate].feat_id == feat_id) {
                return candidate;
            }
            match_mask &= match_mask - 1; /* clear lowest bit */
        }

        /* If group has any empty slot, key is definitely not here */
        if (group_match_empty(group)) {
            return capacity; /* not found */
        }

        /* Advance to next group */
        pos = (group_start + HASH_GROUP_WIDTH) % capacity;
    }
}

graveldb_status_t hash_index_get(const HashIndex *idx, uint64_t feat_id,
                                 uint16_t *out_dim_idx, uint32_t *out_entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;

    rehash_step((HashIndex *)idx);

    uint64_t full_hash = hash64_full(feat_id);

    uint32_t pos = find_in_table(idx->ctrl, idx->slots, idx->capacity, feat_id, full_hash);
    if (pos < idx->capacity) {
        *out_dim_idx = idx->slots[pos].dim_idx;
        *out_entry_idx = idx->slots[pos].entry_idx;
        return GRAVELDB_OK;
    }

    if (idx->old_ctrl) {
        pos = find_in_table(idx->old_ctrl, idx->old_slots, idx->old_capacity, feat_id, full_hash);
        if (pos < idx->old_capacity) {
            *out_dim_idx = idx->old_slots[pos].dim_idx;
            *out_entry_idx = idx->old_slots[pos].entry_idx;
            return GRAVELDB_OK;
        }
    }

    return GRAVELDB_ERR_NOT_FOUND;
}

/* Find insert position: first empty or deleted slot along probe path */
static uint32_t find_insert_slot(const uint8_t *ctrl, uint32_t capacity, uint64_t full_hash) {
    uint32_t pos = h1(full_hash) % capacity;

    while (1) {
        uint32_t group_start = pos - (pos % HASH_GROUP_WIDTH);
        uint32_t mask = group_match_empty_or_deleted(&ctrl[group_start]);
        if (mask) {
            return group_start + (uint32_t)ctz(mask);
        }
        pos = (group_start + HASH_GROUP_WIDTH) % capacity;
    }
}

graveldb_status_t hash_index_put(HashIndex *idx, uint64_t feat_id,
                                 uint16_t dim_idx, uint32_t entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_INVALID;

    rehash_step(idx);

    /* Check if need to grow */
    if (!idx->old_ctrl && idx->growth_left == 0) {
        graveldb_status_t st = hash_index_begin_grow(idx);
        if (st != GRAVELDB_OK) return st;
    }

    uint64_t full_hash = hash64_full(feat_id);

    /* Check if already exists in new table */
    uint32_t pos = find_in_table(idx->ctrl, idx->slots, idx->capacity, feat_id, full_hash);
    if (pos < idx->capacity) {
        idx->slots[pos].dim_idx = dim_idx;
        idx->slots[pos].entry_idx = entry_idx;
        return GRAVELDB_OK;
    }

    /* Check old table during rehash */
    if (idx->old_ctrl) {
        pos = find_in_table(idx->old_ctrl, idx->old_slots, idx->old_capacity, feat_id, full_hash);
        if (pos < idx->old_capacity) {
            idx->old_slots[pos].dim_idx = dim_idx;
            idx->old_slots[pos].entry_idx = entry_idx;
            return GRAVELDB_OK;
        }
    }

    /* Insert new entry into current table */
    uint32_t target = find_insert_slot(idx->ctrl, idx->capacity, full_hash);
    set_ctrl(idx->ctrl, idx->capacity, target, h2(full_hash));
    idx->slots[target].feat_id = feat_id;
    idx->slots[target].dim_idx = dim_idx;
    idx->slots[target].entry_idx = entry_idx;
    idx->count++;
    if (idx->growth_left > 0) idx->growth_left--;
    return GRAVELDB_OK;
}

graveldb_status_t hash_index_remove(HashIndex *idx, uint64_t feat_id) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;

    rehash_step(idx);

    uint64_t full_hash = hash64_full(feat_id);

    /* Try new table */
    uint32_t pos = find_in_table(idx->ctrl, idx->slots, idx->capacity, feat_id, full_hash);
    if (pos < idx->capacity) {
        /* Use tombstone if next slot in group is occupied, else mark empty */
        uint32_t next = (pos + 1) % idx->capacity;
        uint8_t next_ctrl = idx->ctrl[next];
        if (next_ctrl != HASH_CTRL_EMPTY) {
            set_ctrl(idx->ctrl, idx->capacity, pos, HASH_CTRL_DELETED);
        } else {
            set_ctrl(idx->ctrl, idx->capacity, pos, HASH_CTRL_EMPTY);
            idx->growth_left++;
        }
        idx->count--;
        return GRAVELDB_OK;
    }

    /* Try old table */
    if (idx->old_ctrl) {
        pos = find_in_table(idx->old_ctrl, idx->old_slots, idx->old_capacity, feat_id, full_hash);
        if (pos < idx->old_capacity) {
            set_ctrl(idx->old_ctrl, idx->old_capacity, pos, HASH_CTRL_DELETED);
            idx->count--;
            return GRAVELDB_OK;
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
            uint8_t c = idx->ctrl[it->pos];
            if (c != HASH_CTRL_EMPTY && c != HASH_CTRL_DELETED) {
                *feat_id = idx->slots[it->pos].feat_id;
                *dim_idx = idx->slots[it->pos].dim_idx;
                *entry_idx = idx->slots[it->pos].entry_idx;
                it->pos++;
                return true;
            }
            it->pos++;
        }
        if (idx->old_ctrl) {
            it->in_old = true;
            it->pos = 0;
        } else {
            return false;
        }
    }

    while (it->pos < idx->old_capacity) {
        uint8_t c = idx->old_ctrl[it->pos];
        if (c != HASH_CTRL_EMPTY && c != HASH_CTRL_DELETED) {
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
    while (idx->old_ctrl) {
        rehash_step(idx);
    }
}
