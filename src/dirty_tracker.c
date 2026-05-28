/*
 * GravelDB - Dirty Page Tracker Implementation
 *
 * Uniform radix bitmap tree with configurable top-level span.
 * All nodes are DirtyNode; depth parameter distinguishes leaf vs intermediate.
 * Top-level uses a sorted slot array -- allocation proportional to active
 * regions, not total address space.
 */

#include "dirty_tracker.h"
#include <stdlib.h>
#include <string.h>

static DirtyNode *node_create(void) {
    return (DirtyNode *)calloc(1, sizeof(DirtyNode));
}

static void node_destroy(DirtyNode *node, uint8_t depth) {
    if (!node) return;
    if (depth > 0) {
        uint64_t bits = node->bits;
        while (bits) {
            int idx = __builtin_ctzll(bits);
            bits &= bits - 1;
            node_destroy(node->children[idx], depth - 1);
        }
    }
    free(node);
}

/*
 * Mark a page within a sub-tree rooted at 'node'.
 * 'offset' is the page index relative to the sub-tree root.
 * At depth 0, node->bits is the leaf bitmap.
 * At depth > 0, each child covers 64^depth pages.
 */
static void node_mark(DirtyNode *node, uint32_t offset, uint8_t depth) {
    if (depth == 0) {
        node->bits |= (1ULL << (offset & 63));
        return;
    }
    /* Compute shift: each level covers 6 bits of the offset */
    uint32_t shift = depth * 6;
    uint32_t child_idx = (offset >> shift) & 63;

    if (!node->children[child_idx]) {
        node->children[child_idx] = node_create();
        if (!node->children[child_idx]) return; /* OOM, silent fail */
        node->bits |= (1ULL << child_idx);
    }
    node_mark(node->children[child_idx], offset, depth - 1);
}

/*
 * Scan all dirty pages in sub-tree rooted at 'node'.
 * 'base' is the page index offset for this sub-tree in global space.
 * Returns number of entries written to out[].
 */
static int node_scan(const DirtyNode *node, uint8_t depth,
                     uint32_t base, uint32_t max_pages,
                     uint32_t *out, int max_out, int count) {
    if (depth == 0) {
        uint64_t bits = node->bits;
        while (bits && count < max_out) {
            uint32_t bit_pos = __builtin_ctzll(bits);
            bits &= bits - 1;
            uint32_t page_idx = base + bit_pos;
            if (page_idx < max_pages) {
                out[count++] = page_idx;
            }
        }
        return count;
    }

    uint64_t cbits = node->bits;
    while (cbits && count < max_out) {
        uint32_t child_idx = __builtin_ctzll(cbits);
        cbits &= cbits - 1;

        DirtyNode *child = node->children[child_idx];
        if (!child) continue;

        uint32_t shift = depth * 6;
        uint32_t child_base = base + ((uint32_t)child_idx << shift);
        count = node_scan(child, depth - 1, child_base, max_pages,
                          out, max_out, count);
    }
    return count;
}

static uint8_t compute_depth(uint32_t span) {
    /* depth = log64(span): span=64 -> 1, span=4096 -> 2, span=262144 -> 3, etc. */
    uint8_t d = 0;
    uint32_t s = span;
    while (s > 64) {
        s /= 64;
        d++;
    }
    return d;
}

static void tree_init(DirtyTree *tree, uint32_t segment_span) {
    memset(tree, 0, sizeof(*tree));
    /* Default span: 64^3 = 262144 */
    if (segment_span == 0) segment_span = 262144;
    tree->segment_span = segment_span;
    tree->depth = compute_depth(segment_span);
}

static void tree_destroy(DirtyTree *tree) {
    for (uint16_t i = 0; i < tree->segment_count; i++) {
        node_destroy(tree->segments[i].root, tree->depth);
    }
    free(tree->segments);
    memset(tree, 0, sizeof(*tree));
}

/*
 * Binary search for segment_id in sorted segment array.
 * Returns index if found, or ~insertion_point if not found.
 */
static int segment_find(const DirtyTree *tree, uint32_t segment_id) {
    int lo = 0, hi = (int)tree->segment_count - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (tree->segments[mid].segment_id == segment_id) return mid;
        if (tree->segments[mid].segment_id < segment_id) lo = mid + 1;
        else hi = mid - 1;
    }
    return ~lo; /* insertion point, bitwise-inverted */
}

/*
 * Get or create a segment for the given segment_id.
 */
static DirtyNode *segment_get_or_create(DirtyTree *tree, uint32_t segment_id) {
    int idx = segment_find(tree, segment_id);
    if (idx >= 0) return tree->segments[idx].root;

    /* Need to insert */
    int insert_at = ~idx;

    /* Grow capacity if needed */
    if (tree->segment_count >= tree->segment_cap) {
        uint16_t new_cap = tree->segment_cap ? tree->segment_cap * 2 : 4;
        DirtySegment *new_segs = (DirtySegment *)realloc(
            tree->segments, (size_t)new_cap * sizeof(DirtySegment));
        if (!new_segs) return NULL;
        tree->segments = new_segs;
        tree->segment_cap = new_cap;
    }

    /* Shift right to make room */
    if (insert_at < (int)tree->segment_count) {
        memmove(&tree->segments[insert_at + 1],
                &tree->segments[insert_at],
                ((size_t)tree->segment_count - insert_at) * sizeof(DirtySegment));
    }

    /* Create new segment */
    DirtyNode *root = node_create();
    if (!root) return NULL;

    tree->segments[insert_at].segment_id = segment_id;
    tree->segments[insert_at].root = root;
    tree->segment_count++;

    return root;
}

static void tree_mark(DirtyTree *tree, uint32_t page_idx) {
    uint32_t segment_id = page_idx / tree->segment_span;
    uint32_t offset     = page_idx % tree->segment_span;

    DirtyNode *root = segment_get_or_create(tree, segment_id);
    if (!root) return;

    node_mark(root, offset, tree->depth);
}

static int tree_scan(const DirtyTree *tree, uint32_t max_pages,
                     uint32_t *out, int max_out) {
    int count = 0;
    for (uint16_t i = 0; i < tree->segment_count && count < max_out; i++) {
        uint32_t base = tree->segments[i].segment_id * tree->segment_span;
        count = node_scan(tree->segments[i].root, tree->depth,
                          base, max_pages, out, max_out, count);
    }
    return count;
}

/*
 * Auto-select segment_span from estimated page count.
 * Goal: keep segment count manageable while not over-deepening the tree.
 */
static uint32_t span_from_estimate(uint32_t estimated_pages) {
    if (estimated_pages == 0)      return 262144;    /* 64^3, depth 3 */
    if (estimated_pages <= 4096)   return 4096;      /* 64^2, depth 2 */
    if (estimated_pages <= 262144) return 262144;    /* 64^3, depth 3 */
    return 16777216;                                 /* 64^4, depth 4 */
}

graveldb_status_t dirty_tracker_init(DirtyTracker *dt,
                                     const DirtyTrackerConfig *cfg) {
    memset(dt, 0, sizeof(*dt));
    uint32_t est  = (cfg && cfg->estimated_pages) ? cfg->estimated_pages : 0;
    uint32_t span = span_from_estimate(est);
    uint32_t cap  = est > 0 ? est : 262144;

    tree_init(&dt->active, span);
    tree_init(&dt->ckpt, span);
    dt->capacity = cap;
    dt->generation = 0;
    return GRAVELDB_OK;
}

void dirty_tracker_destroy(DirtyTracker *dt) {
    tree_destroy(&dt->active);
    tree_destroy(&dt->ckpt);
    memset(dt, 0, sizeof(*dt));
}

void dirty_tracker_mark(DirtyTracker *dt, uint32_t page_idx) {
    tree_mark(&dt->active, page_idx);
}

int dirty_tracker_scan(const DirtyTracker *dt, uint32_t *out, int max_out) {
    return tree_scan(&dt->active, dt->capacity, out, max_out);
}

int dirty_tracker_scan_ckpt(const DirtyTracker *dt, uint32_t *out, int max_out) {
    return tree_scan(&dt->ckpt, dt->capacity, out, max_out);
}

void dirty_tracker_swap(DirtyTracker *dt) {
    tree_destroy(&dt->ckpt);
    dt->ckpt = dt->active;
    tree_init(&dt->active, dt->ckpt.segment_span);
    dt->generation++;
}

graveldb_status_t dirty_tracker_resize(DirtyTracker *dt, uint32_t new_capacity) {
    if (new_capacity <= dt->capacity) return GRAVELDB_OK;
    dt->capacity = new_capacity;
    return GRAVELDB_OK;
}
