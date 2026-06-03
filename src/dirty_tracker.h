/*
 * GravelDB - Dirty Page Tracker
 *
 * Multi-level radix bitmap tree with configurable segment span.
 *
 * Architecture:
 *   Top-level: sorted array of segments (sparse, grows with actual usage)
 *   Per-segment: uniform DirtyNode tree, depth = log64(segment_span)
 *   Leaf:        DirtyNode.bits used directly as 64-page bitmap
 *
 * The segment span (pages per segment) is a configuration parameter that
 * determines tree height. Data scale growth only adds top-level segments,
 * never changes tree depth.
 *
 * Double-buffered for concurrent checkpoint support.
 */

#ifndef GRAVELDB_DIRTY_TRACKER_H_
#define GRAVELDB_DIRTY_TRACKER_H_

#include <stdint.h>
#include <stddef.h>
#include "graveldb.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Uniform tree node. At intermediate levels, 'bits' is a summary bitmap
 * indicating which children are non-NULL. At leaf level (depth == 0),
 * 'bits' is the actual page dirty bitmap (64 pages).
 */
typedef struct DirtyNode {
    uint64_t           bits;
    struct DirtyNode  *children[64];
} DirtyNode;

/*
 * Top-level segment: maps a segment_id to a sub-tree root.
 * Each segment covers segment_span pages:
 *   [segment_id * span, (segment_id+1) * span)
 */
typedef struct {
    uint32_t   segment_id;
    DirtyNode *root;
} DirtySegment;

/*
 * A single dirty tree with adaptive top-level.
 */
typedef struct {
    DirtySegment *segments;       /* sorted array of active segments */
    uint16_t      segment_count;
    uint16_t      segment_cap;
    uint32_t      segment_span;   /* pages per segment (power of 64, e.g. 262144) */
    uint8_t       depth;          /* tree depth per segment = log64(segment_span) */
} DirtyTree;

/*
 * Double-buffered dirty tracker.
 */
typedef struct {
    DirtyTree   active;
    DirtyTree   ckpt;
    uint32_t    capacity;       /* advisory max page count */
    uint64_t    generation;
} DirtyTracker;

/*
 * Configuration for dirty tracker initialization.
 *
 * estimated_pages: the expected total page count for this tracker.
 *   Used to auto-select tree depth so that segment count stays reasonable.
 *   If 0, defaults to 262144 (resulting in depth=3, span=262144).
 *
 * Internal mapping (automatic):
 *   estimated <= 4096   -> depth=2, span=4096
 *   estimated <= 262144 -> depth=3, span=262144
 *   estimated > 262144  -> depth=4, span=16777216
 */
typedef struct {
    uint32_t    estimated_pages;
} DirtyTrackerConfig;

graveldb_status_t dirty_tracker_init(DirtyTracker *dt, const DirtyTrackerConfig *cfg);
void              dirty_tracker_destroy(DirtyTracker *dt);

void dirty_tracker_mark(DirtyTracker *dt, uint32_t page_idx);
int  dirty_tracker_scan(const DirtyTracker *dt, uint32_t *out, int max_out);
int  dirty_tracker_scan_ckpt(const DirtyTracker *dt, uint32_t *out, int max_out);
void dirty_tracker_swap(DirtyTracker *dt);

graveldb_status_t dirty_tracker_resize(DirtyTracker *dt, uint32_t new_capacity);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_DIRTY_TRACKER_H_ */
