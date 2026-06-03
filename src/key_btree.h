/*
 * key_btree.h - In-memory B-tree for key buffer (entry_idx → feat_id).
 *
 * Properties:
 *   - Sorted by entry_idx (ascending)
 *   - Deduplication: inserting same entry_idx overwrites feat_id
 *   - Ordered iteration for sequential flush (page coalescing)
 *   - Budget-based: caller gives a byte budget, arena fills up → flush
 *
 * Node order B=64: each node holds up to 2*B-1=127 keys.
 * Leaf-heavy: minimizes pointer chasing for sequential scans.
 */

#ifndef GRAVELDB_KEY_BTREE_H_
#define GRAVELDB_KEY_BTREE_H_

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KBT_ORDER 64   /* minimum degree t: node holds [t-1, 2t-1] keys */
#define KBT_MAX_KEYS (2 * KBT_ORDER - 1)  /* 127 */
#define KBT_MIN_KEYS (KBT_ORDER - 1)      /* 63 */

typedef struct KBTNode {
    uint32_t n;                          /* number of keys in this node */
    bool     leaf;
    uint32_t keys[KBT_MAX_KEYS];         /* entry_idx values */
    uint64_t vals[KBT_MAX_KEYS];         /* feat_id values */
    struct KBTNode *children[KBT_MAX_KEYS + 1];
} KBTNode;

typedef struct {
    KBTNode  *root;
    uint32_t  count;       /* total entries in tree */

    /* Fixed arena: one malloc at init, bump forward, reset on clear */
    KBTNode  *arena;
    uint32_t  arena_cap;   /* total nodes the arena can hold */
    uint32_t  arena_used;
} KeyBTree;

/* Init: pre-allocate fixed arena from a byte budget. Page-aligned. */
static inline void kbt_init(KeyBTree *t, size_t budget_bytes) {
    t->root = NULL;
    t->count = 0;
    budget_bytes = (budget_bytes + 4095) & ~(size_t)4095;
    uint32_t cap = (uint32_t)(budget_bytes / sizeof(KBTNode));
    if (cap < 4) cap = 4;  /* minimum sanity */
    size_t alloc = (size_t)cap * sizeof(KBTNode);
    t->arena = (KBTNode *)malloc(alloc);
    t->arena_cap = t->arena ? cap : 0;
    t->arena_used = 0;
}

/* Returns true when arena is nearly full (< 3 nodes left). Caller should flush. */
static inline bool kbt_full(const KeyBTree *t) {
    return t->arena_cap - t->arena_used < 3;
}

static inline KBTNode *kbt_alloc_node(KeyBTree *t, bool leaf) {
    if (t->arena_used >= t->arena_cap) return NULL;
    KBTNode *n = &t->arena[t->arena_used++];
    n->n = 0;
    n->leaf = leaf;
    memset(n->children, 0, sizeof(n->children));
    return n;
}

static inline void kbt_destroy(KeyBTree *t) {
    free(t->arena);
    t->arena = NULL;
    t->arena_cap = 0;
    t->arena_used = 0;
    t->root = NULL;
    t->count = 0;
}

/* Clear: reset bump pointer. O(1). Arena memory stays pinned. */
static inline void kbt_clear(KeyBTree *t) {
    t->root = NULL;
    t->count = 0;
    t->arena_used = 0;
}

/* Split child y of node x at index i */
static inline void kbt_split_child(KeyBTree *t, KBTNode *x, uint32_t i) {
    KBTNode *y = x->children[i];
    KBTNode *z = kbt_alloc_node(t, y->leaf);
    if (!z) return;  /* OOM: degrade gracefully */

    z->n = KBT_MIN_KEYS;

    /* Copy upper half of y's keys/vals to z */
    memcpy(z->keys, y->keys + KBT_ORDER, KBT_MIN_KEYS * sizeof(uint32_t));
    memcpy(z->vals, y->vals + KBT_ORDER, KBT_MIN_KEYS * sizeof(uint64_t));

    if (!y->leaf) {
        memcpy(z->children, y->children + KBT_ORDER, KBT_ORDER * sizeof(KBTNode *));
    }

    y->n = KBT_MIN_KEYS;

    /* Shift x's children and keys to make room */
    for (uint32_t j = x->n; j > i; j--)
        x->children[j + 1] = x->children[j];
    x->children[i + 1] = z;

    for (uint32_t j = x->n; j > i; j--) {
        x->keys[j] = x->keys[j - 1];
        x->vals[j] = x->vals[j - 1];
    }
    /* Promote median */
    x->keys[i] = y->keys[KBT_MIN_KEYS];
    x->vals[i] = y->vals[KBT_MIN_KEYS];
    x->n++;
}

/*
 * Insert or update entry_idx → feat_id.
 * Returns true if new entry inserted, false if existing entry updated.
 */
static inline bool kbt_insert_nonfull(KeyBTree *t, KBTNode *x,
                                       uint32_t entry_idx, uint64_t feat_id) {
    int i = (int)x->n - 1;

    if (x->leaf) {
        /* Binary search for position or existing key */
        int lo = 0, hi = (int)x->n - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (x->keys[mid] == entry_idx) {
                x->vals[mid] = feat_id;  /* update */
                return false;
            } else if (x->keys[mid] < entry_idx) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        /* Insert at position lo */
        for (i = (int)x->n - 1; i >= lo; i--) {
            x->keys[i + 1] = x->keys[i];
            x->vals[i + 1] = x->vals[i];
        }
        x->keys[lo] = entry_idx;
        x->vals[lo] = feat_id;
        x->n++;
        return true;
    }

    /* Internal node: find child */
    int lo = 0, hi = (int)x->n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (x->keys[mid] == entry_idx) {
            x->vals[mid] = feat_id;  /* update */
            return false;
        } else if (x->keys[mid] < entry_idx) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    /* Descend into child[lo] */
    i = lo;
    if (x->children[i]->n == KBT_MAX_KEYS) {
        kbt_split_child(t, x, (uint32_t)i);
        if (x->keys[i] == entry_idx) {
            x->vals[i] = feat_id;
            return false;
        }
        if (entry_idx > x->keys[i]) i++;
    }
    return kbt_insert_nonfull(t, x->children[i], entry_idx, feat_id);
}

static inline bool kbt_insert(KeyBTree *t, uint32_t entry_idx, uint64_t feat_id) {
    if (!t->root) {
        t->root = kbt_alloc_node(t, true);
        if (!t->root) return false;
        t->root->keys[0] = entry_idx;
        t->root->vals[0] = feat_id;
        t->root->n = 1;
        t->count = 1;
        return true;
    }

    /* Check if key already exists by trying insert directly */
    if (t->root->n == KBT_MAX_KEYS) {
        KBTNode *s = kbt_alloc_node(t, false);
        if (!s) return false;
        s->children[0] = t->root;
        t->root = s;
        kbt_split_child(t, s, 0);
    }

    bool inserted = kbt_insert_nonfull(t, t->root, entry_idx, feat_id);
    if (inserted) t->count++;
    return inserted;
}

/* In-order iteration callback */
typedef void (*kbt_iter_fn)(uint32_t entry_idx, uint64_t feat_id, void *ctx);

static inline void kbt_iterate_node(KBTNode *n, kbt_iter_fn fn, void *ctx) {
    if (!n) return;
    for (uint32_t i = 0; i < n->n; i++) {
        if (!n->leaf) kbt_iterate_node(n->children[i], fn, ctx);
        fn(n->keys[i], n->vals[i], ctx);
    }
    if (!n->leaf) kbt_iterate_node(n->children[n->n], fn, ctx);
}

static inline void kbt_iterate(KeyBTree *t, kbt_iter_fn fn, void *ctx) {
    kbt_iterate_node(t->root, fn, ctx);
}

/*
 * Collect all entries into a caller-provided array, in sorted order.
 * Returns number of entries written. Array must have room for t->count entries.
 * Uses KeyWriteEntry which has the same {entry_idx, feat_id} layout.
 */

static inline uint32_t kbt_collect_node(KBTNode *n, KeyWriteEntry *out, uint32_t pos) {
    if (!n) return pos;
    for (uint32_t i = 0; i < n->n; i++) {
        if (!n->leaf) pos = kbt_collect_node(n->children[i], out, pos);
        out[pos].entry_idx = n->keys[i];
        out[pos].feat_id = n->vals[i];
        pos++;
    }
    if (!n->leaf) pos = kbt_collect_node(n->children[n->n], out, pos);
    return pos;
}

static inline uint32_t kbt_collect(KeyBTree *t, KeyWriteEntry *out) {
    return kbt_collect_node(t->root, out, 0);
}

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_KEY_BTREE_H_ */
