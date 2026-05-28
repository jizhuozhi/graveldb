/*
 * DimRegistry - Adaptive dim->bin lookup with automatic structure upgrade
 *
 * Inspired by Java 8 HashMap's treeify: the internal data structure
 * automatically upgrades as the number of distinct dims grows:
 *
 *   n <= THRESHOLD_LINEAR (8):    Linear scan (cache-line friendly)
 *   n <= THRESHOLD_SORTED (64):   Sorted array + binary search O(log n)
 *   n > THRESHOLD_SORTED:         Open-addressing hash map O(1)
 *
 * Supports fully dynamic dims: new dims can be registered at runtime
 * without knowing them at open() time.
 */

#ifndef GRAVELDB_DIM_REGISTRY_H_
#define GRAVELDB_DIM_REGISTRY_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration - DimBin is fully defined in internal.h */
struct DimBin;

typedef enum {
    DIM_REG_LINEAR = 0,     /* n <= 8: linear scan */
    DIM_REG_SORTED = 1,     /* 8 < n <= 64: sorted + binary search */
    DIM_REG_HASH   = 2,     /* n > 64: open-addressing hash */
} DimRegMode;

#define DIM_REG_THRESHOLD_LINEAR   8
#define DIM_REG_THRESHOLD_SORTED   64

typedef struct {
    int      dim;     /* dimension value (key) */
    uint16_t bin_idx; /* index into bins[] pointer array */
} DimRegEntry;

typedef struct {
    int      dim; /* 0 = empty slot */
    uint16_t bin_idx;
    uint16_t _pad;
} DimRegHashSlot;

typedef struct {
    DimRegMode       mode;

    /* Shared state */
    uint16_t         count;          /* number of registered dims */
    uint16_t         capacity;       /* allocated capacity (for bins array) */

    /* Bin pointer array (dynamically grown) */
    struct DimBin **bins;           /* bins[bin_idx] -> DimBin* */

    /* LINEAR / SORTED mode storage */
    DimRegEntry     *entries;        /* count entries; sorted in SORTED mode */
    uint16_t         entries_cap;

    /* HASH mode storage */
    DimRegHashSlot   *slots;         /* open-addressing table */
    uint16_t          hash_cap;      /* power of 2 */
    uint16_t          hash_mask;     /* hash_cap - 1 */

    uint64_t          lookups;
    uint64_t          upgrades;       /* number of mode transitions */
} DimRegistry;

/*
 * Initialize an empty registry. Starts in LINEAR mode.
 */
void dim_registry_init(DimRegistry *reg);

/*
 * Destroy registry (free internal allocations).
 * Does NOT destroy the DimBins themselves (caller responsibility).
 */
void dim_registry_destroy(DimRegistry *reg);

/*
 * Lookup: find the bin index for a given dim.
 * Returns the bin_idx (>= 0) on success, -1 if dim not found.
 */
int dim_registry_find(DimRegistry *reg, int dim);

/*
 * Register a new dim -> bin mapping.
 * The caller has already created the DimBin; pass its pointer.
 * Returns the assigned bin_idx (>= 0) on success, -1 on error (OOM).
 *
 * If the dim already exists, returns existing bin_idx (no-op).
 * May trigger a mode upgrade (LINEAR->SORTED->HASH).
 */
int dim_registry_put(DimRegistry *reg, int dim, struct DimBin *bin);

/*
 * Get DimBin pointer by bin_idx (fast, unchecked).
 */
static inline struct DimBin *dim_registry_get_bin(const DimRegistry *reg, uint16_t bin_idx) {
    return reg->bins[bin_idx];
}

/*
 * Get current count of registered dims.
 */
static inline uint16_t dim_registry_count(const DimRegistry *reg) {
    return reg->count;
}

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_DIM_REGISTRY_H_ */
