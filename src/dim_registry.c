/*
 * DimRegistry - Adaptive dim->bin lookup implementation
 *
 * Mode transitions:
 *   LINEAR (<=8) -> SORTED (<=64) -> HASH (>64)
 *
 * Once upgraded, does NOT downgrade (dims are never removed in practice).
 */

#include "dim_registry.h"
#include "dimbin.h"
#include <stdlib.h>
#include <string.h>

static inline uint32_t dim_hash(int dim) {
    uint32_t x = (uint32_t)dim;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint16_t next_pow2_u16(uint16_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v++;
    return v < 16 ? 16 : v;
}

void dim_registry_init(DimRegistry *reg) {
    memset(reg, 0, sizeof(*reg));
    reg->mode = DIM_REG_LINEAR;

    reg->entries_cap = DIM_REG_THRESHOLD_LINEAR;
    reg->entries = (DimRegEntry *)calloc(reg->entries_cap, sizeof(DimRegEntry));

    reg->capacity = DIM_REG_THRESHOLD_LINEAR;
    reg->bins = (DimBin **)calloc(reg->capacity, sizeof(DimBin *));
}

void dim_registry_destroy(DimRegistry *reg) {
    free(reg->entries);
    free(reg->bins);
    free(reg->slots);
    memset(reg, 0, sizeof(*reg));
}

static int linear_find(const DimRegistry *reg, int dim) {
    for (uint16_t i = 0; i < reg->count; i++) {
        if (reg->entries[i].dim == dim) return (int)reg->entries[i].bin_idx;
    }
    return -1;
}

static int sorted_find(const DimRegistry *reg, int dim) {
    int lo = 0, hi = (int)reg->count - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        int mid_dim = reg->entries[mid].dim;
        if (mid_dim == dim) return (int)reg->entries[mid].bin_idx;
        if (mid_dim < dim) lo = mid + 1;
        else hi = mid - 1;
    }
    return -1;
}

static void sorted_insert(DimRegistry *reg, int dim, uint16_t bin_idx) {
    int pos = 0;
    while (pos < (int)reg->count && reg->entries[pos].dim < dim) pos++;

    if (reg->count > 0) {
        memmove(&reg->entries[pos + 1], &reg->entries[pos],
                ((int)reg->count - pos) * sizeof(DimRegEntry));
    }

    reg->entries[pos].dim = dim;
    reg->entries[pos].bin_idx = bin_idx;
}

static int hash_find(const DimRegistry *reg, int dim) {
    uint16_t mask = reg->hash_mask;
    uint16_t idx = (uint16_t)(dim_hash(dim) & mask);

    for (uint16_t probe = 0; probe <= mask; probe++, idx = (idx + 1) & mask) {
        if (reg->slots[idx].dim == 0 && reg->slots[idx].bin_idx == 0) {
            return -1;
        }
        if (reg->slots[idx].dim == dim) {
            return (int)reg->slots[idx].bin_idx;
        }
    }
    return -1;
}

static void hash_insert(DimRegHashSlot *slots, uint16_t mask,
                        int dim, uint16_t bin_idx) {
    uint16_t idx = (uint16_t)(dim_hash(dim) & mask);

    for (uint16_t probe = 0; probe <= mask; probe++, idx = (idx + 1) & mask) {
        if (slots[idx].dim == 0 && slots[idx].bin_idx == 0) {
            slots[idx].dim = dim;
            slots[idx].bin_idx = bin_idx;
            return;
        }
    }
}

static void upgrade_to_sorted(DimRegistry *reg) {
    for (int i = 1; i < (int)reg->count; i++) {
        DimRegEntry tmp = reg->entries[i];
        int j = i - 1;
        while (j >= 0 && reg->entries[j].dim > tmp.dim) {
            reg->entries[j + 1] = reg->entries[j];
            j--;
        }
        reg->entries[j + 1] = tmp;
    }

    if (reg->entries_cap < DIM_REG_THRESHOLD_SORTED) {
        reg->entries_cap = DIM_REG_THRESHOLD_SORTED;
        reg->entries = (DimRegEntry *)realloc(reg->entries,
                                              reg->entries_cap * sizeof(DimRegEntry));
    }

    reg->mode = DIM_REG_SORTED;
    reg->upgrades++;
}

static void upgrade_to_hash(DimRegistry *reg) {
    uint16_t new_cap = next_pow2_u16((uint16_t)(reg->count * 4));
    if (new_cap < 128) new_cap = 128;

    reg->slots = (DimRegHashSlot *)calloc(new_cap, sizeof(DimRegHashSlot));
    reg->hash_cap = new_cap;
    reg->hash_mask = new_cap - 1;

    for (uint16_t i = 0; i < reg->count; i++) {
        hash_insert(reg->slots, reg->hash_mask,
                   reg->entries[i].dim, reg->entries[i].bin_idx);
    }

    reg->mode = DIM_REG_HASH;
    reg->upgrades++;
}

static void hash_grow(DimRegistry *reg) {
    uint16_t old_cap = reg->hash_cap;
    uint16_t new_cap = old_cap * 2;

    DimRegHashSlot *new_slots = (DimRegHashSlot *)calloc(new_cap, sizeof(DimRegHashSlot));
    uint16_t new_mask = new_cap - 1;

    for (uint16_t i = 0; i < old_cap; i++) {
        if (reg->slots[i].dim != 0 || reg->slots[i].bin_idx != 0) {
            hash_insert(new_slots, new_mask,
                                 reg->slots[i].dim, reg->slots[i].bin_idx);
        }
    }

    free(reg->slots);
    reg->slots = new_slots;
    reg->hash_cap = new_cap;
    reg->hash_mask = new_mask;
}

int dim_registry_find(DimRegistry *reg, int dim) {
    reg->lookups++;

    switch (reg->mode) {
    case DIM_REG_LINEAR:
        return linear_find(reg, dim);
    case DIM_REG_SORTED:
        return sorted_find(reg, dim);
    case DIM_REG_HASH:
        return hash_find(reg, dim);
    }
    return -1;
}

int dim_registry_put(DimRegistry *reg, int dim, DimBin *bin) {
    int existing = dim_registry_find(reg, dim);
    if (existing >= 0) return existing;

    uint16_t bin_idx = reg->count;

    if (bin_idx >= reg->capacity) {
        uint16_t new_cap = reg->capacity * 2;
        if (new_cap < 16) new_cap = 16;
        DimBin **new_bins = (DimBin **)realloc(reg->bins, new_cap * sizeof(DimBin *));
        if (!new_bins) return -1;
        reg->bins = new_bins;
        reg->capacity = new_cap;
    }
    reg->bins[bin_idx] = bin;

    switch (reg->mode) {
    case DIM_REG_LINEAR:
        if (reg->count >= reg->entries_cap) {
            reg->entries_cap *= 2;
            reg->entries = (DimRegEntry *)realloc(reg->entries,
                                                  reg->entries_cap * sizeof(DimRegEntry));
            if (!reg->entries) return -1;
        }
        reg->entries[reg->count].dim = dim;
        reg->entries[reg->count].bin_idx = bin_idx;
        break;

    case DIM_REG_SORTED:
        if (reg->count >= reg->entries_cap) {
            reg->entries_cap *= 2;
            reg->entries = (DimRegEntry *)realloc(reg->entries,
                                                  reg->entries_cap * sizeof(DimRegEntry));
            if (!reg->entries) return -1;
        }
        sorted_insert(reg, dim, bin_idx);
        break;

    case DIM_REG_HASH:
        if ((uint32_t)(reg->count + 1) * 4 > (uint32_t)reg->hash_cap * 3) {
            hash_grow(reg);
        }
        hash_insert(reg->slots, reg->hash_mask, dim, bin_idx);
        if (reg->count >= reg->entries_cap) {
            reg->entries_cap *= 2;
            reg->entries = (DimRegEntry *)realloc(reg->entries,
                                                  reg->entries_cap * sizeof(DimRegEntry));
        }
        reg->entries[reg->count].dim = dim;
        reg->entries[reg->count].bin_idx = bin_idx;
        break;
    }

    reg->count++;

    if (reg->mode == DIM_REG_LINEAR && reg->count > DIM_REG_THRESHOLD_LINEAR) {
        upgrade_to_sorted(reg);
    } else if (reg->mode == DIM_REG_SORTED && reg->count > DIM_REG_THRESHOLD_SORTED) {
        upgrade_to_hash(reg);
    }

    return (int)bin_idx;
}
