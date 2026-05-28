/*
 * GravelDB - TinyLFU Admission Filter
 *
 * Self-contained module: defines the TinyLFU type and operations.
 * Count-Min Sketch based frequency estimation with periodic halving.
 */

#ifndef GRAVELDB_TINYLFU_H_
#define GRAVELDB_TINYLFU_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "graveldb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t  *cms[4];
    uint32_t  cms_width;
    uint64_t  total_accesses;
    uint64_t  decay_threshold;
    uint8_t   eviction_threshold;
} TinyLFU;

graveldb_status_t tinylfu_init(TinyLFU *lfu, uint32_t cms_width,
                                uint8_t eviction_threshold);
void tinylfu_destroy(TinyLFU *lfu);
void tinylfu_access(TinyLFU *lfu, uint64_t feat_id);
uint8_t tinylfu_estimate(const TinyLFU *lfu, uint64_t feat_id);
void tinylfu_promote(TinyLFU *lfu, uint64_t feat_id);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_TINYLFU_H_ */
