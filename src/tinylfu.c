/*
 * GravelDB - TinyLFU implementation (Count-Min Sketch based)
 */

#include "tinylfu.h"
#include <stdlib.h>
#include <string.h>

/*
 * Hash functions for CMS
 */

static inline uint32_t hash_i(uint64_t key, int i) {
    key += (uint64_t)i * 0x9E3779B97F4A7C15ULL;
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return (uint32_t)key;
}

/*
 * Init/Destroy
 */

graveldb_status_t tinylfu_init(TinyLFU *lfu, uint32_t cms_width, uint8_t eviction_threshold) {
    lfu->cms_width = cms_width;
    lfu->total_accesses = 0;
    lfu->decay_threshold = (uint64_t)cms_width * 10; /* decay when we've seen 10x width accesses */
    lfu->eviction_threshold = eviction_threshold;

    for (int i = 0; i < 4; i++) {
        lfu->cms[i] = (uint8_t *)calloc(cms_width, sizeof(uint8_t));
        if (!lfu->cms[i]) {
            /* Free previously allocated rows */
            for (int j = 0; j < i; j++) {
                free(lfu->cms[j]);
                lfu->cms[j] = NULL;
            }
            return GRAVELDB_ERR_OOM;
        }
    }
    return GRAVELDB_OK;
}

void tinylfu_destroy(TinyLFU *lfu) {
    for (int i = 0; i < 4; i++) {
        free(lfu->cms[i]);
        lfu->cms[i] = NULL;
    }
}

/*
 * Decay (halve all counters)
 */

static void tinylfu_decay(TinyLFU *lfu) {
    for (int i = 0; i < 4; i++) {
        for (uint32_t j = 0; j < lfu->cms_width; j++) {
            lfu->cms[i][j] >>= 1;
        }
    }
    lfu->total_accesses = 0;
}

/*
 * Record Access
 */

void tinylfu_access(TinyLFU *lfu, uint64_t feat_id) {
    for (int i = 0; i < 4; i++) {
        uint32_t idx = hash_i(feat_id, i) % lfu->cms_width;
        if (lfu->cms[i][idx] < 15) lfu->cms[i][idx]++;  /* 4-bit saturate */
    }
    if (++lfu->total_accesses >= lfu->decay_threshold) {
        tinylfu_decay(lfu);
    }
}

/*
 * Estimate Frequency
 */

uint8_t tinylfu_estimate(const TinyLFU *lfu, uint64_t feat_id) {
    uint8_t min_val = 15;
    for (int i = 0; i < 4; i++) {
        uint32_t idx = hash_i(feat_id, i) % lfu->cms_width;
        if (lfu->cms[i][idx] < min_val) min_val = lfu->cms[i][idx];
    }
    return min_val;
}

/*
 * Promote (for resurrection)
 */

void tinylfu_promote(TinyLFU *lfu, uint64_t feat_id) {
    for (int i = 0; i < 4; i++) {
        uint32_t idx = hash_i(feat_id, i) % lfu->cms_width;
        lfu->cms[i][idx] = 15; /* max out */
    }
}
