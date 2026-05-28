/*
 * GravelDB Hash Table Strategy Benchmark
 *
 * Compares two hash table implementations:
 *   1. Open Addressing with Linear Probing (current GravelDB impl)
 *   2. Separate Chaining with linked list buckets
 *
 * Dimensions varied:
 *   - Data scale: 10K, 100K, 1M, 10M keys
 *   - Load factor: 50%, 70%, 90% (shows degradation behavior)
 *   - Key distribution: uniform, zipfian (hot set), clustered
 *   - Operation mix: pure lookup, mixed insert+lookup, delete-heavy
 *
 * Key metrics:
 *   - Lookup latency (ns/key)
 *   - Insert throughput (ns/key)
 *   - Memory usage (bytes per entry)
 *   - Cache miss behavior at scale
 *
 * Build:
 *   make bench-hash
 *
 * Usage:
 *   ./build/bench-hash
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <assert.h>

/* ═══════════════════════════════════════════════════════════════════════
 * Common Types
 * ═══════════════════════════════════════════════════════════════════════ */

typedef int graveldb_status_t;
#define GRAVELDB_OK          0
#define GRAVELDB_ERR_OOM    -1
#define GRAVELDB_ERR_NOT_FOUND -2
#define GRAVELDB_ERR_INVALID   -3

/* ═══════════════════════════════════════════════════════════════════════
 * Hash Function (shared between both implementations)
 * ═══════════════════════════════════════════════════════════════════════ */

static inline uint32_t hash64(uint64_t key) {
    /* splitmix64 finalizer — excellent distribution */
    key ^= key >> 30;
    key *= 0xbf58476d1ce4e5b9ULL;
    key ^= key >> 27;
    key *= 0x94d049bb133111ebULL;
    key ^= key >> 31;
    return (uint32_t)key;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Implementation 1: Open Addressing (Linear Probing)
 * ═══════════════════════════════════════════════════════════════════════ */

typedef struct {
    uint64_t  feat_id;    /* 0 = empty slot (sentinel) */
    uint16_t  dim_idx;
    uint32_t  entry_idx;
} OA_Slot;

typedef struct {
    OA_Slot  *slots;
    uint32_t   capacity;
    uint32_t   count;
    uint32_t   mask;
} OA_HashTable;

static graveldb_status_t oa_init(OA_HashTable *ht, uint32_t capacity) {
    uint32_t cap = 1;
    while (cap < capacity) cap <<= 1;
    ht->slots = (OA_Slot *)calloc(cap, sizeof(OA_Slot));
    if (!ht->slots) return GRAVELDB_ERR_OOM;
    ht->capacity = cap;
    ht->count = 0;
    ht->mask = cap - 1;
    return GRAVELDB_OK;
}

static void oa_destroy(OA_HashTable *ht) {
    free(ht->slots);
    ht->slots = NULL;
}

static graveldb_status_t oa_grow(OA_HashTable *ht) {
    uint32_t new_cap = ht->capacity * 2;
    OA_Slot *new_slots = (OA_Slot *)calloc(new_cap, sizeof(OA_Slot));
    if (!new_slots) return GRAVELDB_ERR_OOM;
    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < ht->capacity; i++) {
        if (ht->slots[i].feat_id == 0) continue;
        uint32_t h = hash64(ht->slots[i].feat_id) & new_mask;
        while (new_slots[h].feat_id != 0) h = (h + 1) & new_mask;
        new_slots[h] = ht->slots[i];
    }
    free(ht->slots);
    ht->slots = new_slots;
    ht->capacity = new_cap;
    ht->mask = new_mask;
    return GRAVELDB_OK;
}

static graveldb_status_t oa_put(OA_HashTable *ht, uint64_t feat_id,
                                uint16_t dim_idx, uint32_t entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_INVALID;
    if (ht->count * 10 > ht->capacity * 7) {
        graveldb_status_t st = oa_grow(ht);
        if (st != GRAVELDB_OK) return st;
    }
    uint32_t h = hash64(feat_id) & ht->mask;
    while (ht->slots[h].feat_id != 0) {
        if (ht->slots[h].feat_id == feat_id) {
            ht->slots[h].dim_idx = dim_idx;
            ht->slots[h].entry_idx = entry_idx;
            return GRAVELDB_OK;
        }
        h = (h + 1) & ht->mask;
    }
    ht->slots[h].feat_id = feat_id;
    ht->slots[h].dim_idx = dim_idx;
    ht->slots[h].entry_idx = entry_idx;
    ht->count++;
    return GRAVELDB_OK;
}

static graveldb_status_t oa_get(const OA_HashTable *ht, uint64_t feat_id,
                                uint16_t *out_dim_idx, uint32_t *out_entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;
    uint32_t h = hash64(feat_id) & ht->mask;
    uint32_t start = h;
    while (ht->slots[h].feat_id != 0) {
        if (ht->slots[h].feat_id == feat_id) {
            *out_dim_idx = ht->slots[h].dim_idx;
            *out_entry_idx = ht->slots[h].entry_idx;
            return GRAVELDB_OK;
        }
        h = (h + 1) & ht->mask;
        if (h == start) break;
    }
    return GRAVELDB_ERR_NOT_FOUND;
}

static graveldb_status_t oa_remove(OA_HashTable *ht, uint64_t feat_id) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;
    uint32_t h = hash64(feat_id) & ht->mask;
    uint32_t start = h;
    while (ht->slots[h].feat_id != 0) {
        if (ht->slots[h].feat_id == feat_id) {
            /* Backward shift deletion */
            ht->slots[h].feat_id = 0;
            ht->count--;
            uint32_t next = (h + 1) & ht->mask;
            while (ht->slots[next].feat_id != 0) {
                uint32_t ideal = hash64(ht->slots[next].feat_id) & ht->mask;
                /* Check if 'next' is displaced past 'h' */
                if ((next > h && (ideal <= h || ideal > next)) ||
                    (next < h && (ideal <= h && ideal > next))) {
                    ht->slots[h] = ht->slots[next];
                    ht->slots[next].feat_id = 0;
                    h = next;
                }
                next = (next + 1) & ht->mask;
            }
            return GRAVELDB_OK;
        }
        h = (h + 1) & ht->mask;
        if (h == start) break;
    }
    return GRAVELDB_ERR_NOT_FOUND;
}

static size_t oa_memory_bytes(const OA_HashTable *ht) {
    return (size_t)ht->capacity * sizeof(OA_Slot);
}

/* ═══════════════════════════════════════════════════════════════════════
 * Implementation 2: Separate Chaining
 * ═══════════════════════════════════════════════════════════════════════ */

typedef struct SC_Node {
    uint64_t       feat_id;
    uint16_t       dim_idx;
    uint32_t       entry_idx;
    struct SC_Node *next;
} SC_Node;

typedef struct {
    SC_Node  **buckets;     /* Array of bucket head pointers */
    uint32_t   capacity;
    uint32_t   count;
    uint32_t   mask;
} SC_HashTable;

/* Node pool for better allocation perf (avoids malloc per node) */
typedef struct SC_Pool {
    SC_Node       *nodes;
    uint32_t       capacity;
    uint32_t       used;
    struct SC_Pool *next_pool;  /* linked list of pools for overflow */
} SC_Pool;

static SC_Pool *pool_create(uint32_t capacity) {
    SC_Pool *p = (SC_Pool *)malloc(sizeof(SC_Pool));
    if (!p) return NULL;
    p->nodes = (SC_Node *)malloc(capacity * sizeof(SC_Node));
    if (!p->nodes) { free(p); return NULL; }
    p->capacity = capacity;
    p->used = 0;
    p->next_pool = NULL;
    return p;
}

static SC_Pool *g_pool = NULL;

static SC_Node *pool_alloc_node(void) {
    if (!g_pool || g_pool->used >= g_pool->capacity) {
        /* Allocate a new pool chunk */
        uint32_t new_cap = g_pool ? g_pool->capacity * 2 : 65536;
        SC_Pool *p = pool_create(new_cap);
        if (!p) return NULL;
        p->next_pool = g_pool;
        g_pool = p;
    }
    return &g_pool->nodes[g_pool->used++];
}

static void pool_destroy_all(void) {
    SC_Pool *p = g_pool;
    while (p) {
        SC_Pool *next = p->next_pool;
        free(p->nodes);
        free(p);
        p = next;
    }
    g_pool = NULL;
}

static graveldb_status_t sc_init(SC_HashTable *ht, uint32_t capacity) {
    uint32_t cap = 1;
    while (cap < capacity) cap <<= 1;
    ht->buckets = (SC_Node **)calloc(cap, sizeof(SC_Node *));
    if (!ht->buckets) return GRAVELDB_ERR_OOM;
    ht->capacity = cap;
    ht->count = 0;
    ht->mask = cap - 1;
    return GRAVELDB_OK;
}

static void sc_destroy(SC_HashTable *ht) {
    /* Nodes are pool-allocated, no need to free individually */
    free(ht->buckets);
    ht->buckets = NULL;
}

static graveldb_status_t sc_grow(SC_HashTable *ht) {
    uint32_t new_cap = ht->capacity * 2;
    SC_Node **new_buckets = (SC_Node **)calloc(new_cap, sizeof(SC_Node *));
    if (!new_buckets) return GRAVELDB_ERR_OOM;
    uint32_t new_mask = new_cap - 1;

    for (uint32_t i = 0; i < ht->capacity; i++) {
        SC_Node *node = ht->buckets[i];
        while (node) {
            SC_Node *next = node->next;
            uint32_t h = hash64(node->feat_id) & new_mask;
            node->next = new_buckets[h];
            new_buckets[h] = node;
            node = next;
        }
    }
    free(ht->buckets);
    ht->buckets = new_buckets;
    ht->capacity = new_cap;
    ht->mask = new_mask;
    return GRAVELDB_OK;
}

static graveldb_status_t sc_put(SC_HashTable *ht, uint64_t feat_id,
                                uint16_t dim_idx, uint32_t entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_INVALID;

    /* Grow if load factor > 1.0 (average chain length > 1) */
    if (ht->count >= ht->capacity) {
        graveldb_status_t st = sc_grow(ht);
        if (st != GRAVELDB_OK) return st;
    }

    uint32_t h = hash64(feat_id) & ht->mask;

    /* Check for existing key */
    SC_Node *node = ht->buckets[h];
    while (node) {
        if (node->feat_id == feat_id) {
            node->dim_idx = dim_idx;
            node->entry_idx = entry_idx;
            return GRAVELDB_OK;
        }
        node = node->next;
    }

    /* Insert new node at head */
    SC_Node *new_node = pool_alloc_node();
    if (!new_node) return GRAVELDB_ERR_OOM;
    new_node->feat_id = feat_id;
    new_node->dim_idx = dim_idx;
    new_node->entry_idx = entry_idx;
    new_node->next = ht->buckets[h];
    ht->buckets[h] = new_node;
    ht->count++;
    return GRAVELDB_OK;
}

static graveldb_status_t sc_get(const SC_HashTable *ht, uint64_t feat_id,
                                uint16_t *out_dim_idx, uint32_t *out_entry_idx) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;
    uint32_t h = hash64(feat_id) & ht->mask;
    SC_Node *node = ht->buckets[h];
    while (node) {
        if (node->feat_id == feat_id) {
            *out_dim_idx = node->dim_idx;
            *out_entry_idx = node->entry_idx;
            return GRAVELDB_OK;
        }
        node = node->next;
    }
    return GRAVELDB_ERR_NOT_FOUND;
}

static graveldb_status_t sc_remove(SC_HashTable *ht, uint64_t feat_id) {
    if (feat_id == 0) return GRAVELDB_ERR_NOT_FOUND;
    uint32_t h = hash64(feat_id) & ht->mask;
    SC_Node **prev_ptr = &ht->buckets[h];
    SC_Node *node = ht->buckets[h];
    while (node) {
        if (node->feat_id == feat_id) {
            *prev_ptr = node->next;
            /* Node is pool-allocated, mark as "free" by zeroing feat_id */
            node->feat_id = 0;
            ht->count--;
            return GRAVELDB_OK;
        }
        prev_ptr = &node->next;
        node = node->next;
    }
    return GRAVELDB_ERR_NOT_FOUND;
}

static size_t sc_memory_bytes(const SC_HashTable *ht) {
    /* Bucket array + all allocated nodes */
    size_t bucket_array = (size_t)ht->capacity * sizeof(SC_Node *);
    size_t node_mem = (size_t)ht->count * sizeof(SC_Node);
    return bucket_array + node_mem;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Random Number Generator (xoshiro256**)
 * ═══════════════════════════════════════════════════════════════════════ */

static uint64_t rng_state[4];

static inline uint64_t rotl(const uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static uint64_t rng_next(void) {
    const uint64_t result = rotl(rng_state[1] * 5, 7) * 9;
    const uint64_t t = rng_state[1] << 17;
    rng_state[2] ^= rng_state[0];
    rng_state[3] ^= rng_state[1];
    rng_state[1] ^= rng_state[2];
    rng_state[0] ^= rng_state[3];
    rng_state[2] ^= t;
    rng_state[3] = rotl(rng_state[3], 45);
    return result;
}

static void rng_seed(uint64_t seed) {
    for (int i = 0; i < 4; i++) {
        seed += 0x9e3779b97f4a7c15ULL;
        uint64_t z = seed;
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        rng_state[i] = z ^ (z >> 31);
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 * Key Distribution Generators
 * ═══════════════════════════════════════════════════════════════════════ */

typedef enum {
    DIST_UNIFORM,
    DIST_ZIPFIAN,
    DIST_CLUSTERED,
} Distribution;

static const char *dist_names[] = {"uniform", "zipfian", "clustered"};

static uint64_t gen_lookup_key(Distribution dist, uint64_t max_key) {
    uint64_t r = rng_next();
    switch (dist) {
    case DIST_UNIFORM:
        return (r % max_key) + 1;
    case DIST_ZIPFIAN: {
        double u = (double)(r >> 11) / (double)(1ULL << 53);
        if (u < 0.80) {
            uint64_t hot_range = max_key / 10;
            if (hot_range == 0) hot_range = 1;
            return (rng_next() % hot_range) + 1;
        }
        return (rng_next() % max_key) + 1;
    }
    case DIST_CLUSTERED: {
        uint64_t window = max_key / 1000;
        if (window < 64) window = 64;
        uint64_t center = (rng_next() % max_key) + 1;
        int64_t offset = (int64_t)(rng_next() % window) - (int64_t)(window / 2);
        int64_t result = (int64_t)center + offset;
        if (result < 1) result = 1;
        if ((uint64_t)result > max_key) result = (int64_t)max_key;
        return (uint64_t)result;
    }
    }
    return (r % max_key) + 1;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Timer Utilities
 * ═══════════════════════════════════════════════════════════════════════ */

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark Results
 * ═══════════════════════════════════════════════════════════════════════ */

typedef struct {
    double ns_per_op;
    double mops;
} BenchResult;

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Lookup Performance
 * ═══════════════════════════════════════════════════════════════════════ */

static BenchResult bench_oa_lookup(const OA_HashTable *ht, const uint64_t *keys,
                                   int total_keys) {
    uint16_t dim; uint32_t entry;
    volatile uint32_t sink = 0;

    uint64_t start = now_ns();
    for (int i = 0; i < total_keys; i++) {
        if (oa_get(ht, keys[i], &dim, &entry) == GRAVELDB_OK) {
            sink += entry;
        }
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)total_keys;
    r.mops = (double)total_keys / ((double)elapsed / 1e9) / 1e6;
    return r;
}

static BenchResult bench_sc_lookup(const SC_HashTable *ht, const uint64_t *keys,
                                   int total_keys) {
    uint16_t dim; uint32_t entry;
    volatile uint32_t sink = 0;

    uint64_t start = now_ns();
    for (int i = 0; i < total_keys; i++) {
        if (sc_get(ht, keys[i], &dim, &entry) == GRAVELDB_OK) {
            sink += entry;
        }
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)total_keys;
    r.mops = (double)total_keys / ((double)elapsed / 1e9) / 1e6;
    return r;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Insert Performance
 * ═══════════════════════════════════════════════════════════════════════ */

static BenchResult bench_oa_insert(uint64_t scale) {
    OA_HashTable ht;
    oa_init(&ht, 1024); /* Start small, let it grow organically */

    uint64_t start = now_ns();
    for (uint64_t i = 1; i <= scale; i++) {
        oa_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }
    uint64_t elapsed = now_ns() - start;

    oa_destroy(&ht);

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)scale;
    r.mops = (double)scale / ((double)elapsed / 1e9) / 1e6;
    return r;
}

static BenchResult bench_sc_insert(uint64_t scale) {
    SC_HashTable ht;
    sc_init(&ht, 1024);

    uint64_t start = now_ns();
    for (uint64_t i = 1; i <= scale; i++) {
        sc_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }
    uint64_t elapsed = now_ns() - start;

    sc_destroy(&ht);
    pool_destroy_all();

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)scale;
    r.mops = (double)scale / ((double)elapsed / 1e9) / 1e6;
    return r;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Delete Performance
 * ═══════════════════════════════════════════════════════════════════════ */

static BenchResult bench_oa_delete(uint64_t scale) {
    OA_HashTable ht;
    oa_init(&ht, (uint32_t)(scale * 2));
    for (uint64_t i = 1; i <= scale; i++) {
        oa_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }

    /* Delete random 50% of keys */
    rng_seed(12345);
    int delete_count = (int)(scale / 2);
    uint64_t *del_keys = (uint64_t *)malloc(delete_count * sizeof(uint64_t));
    for (int i = 0; i < delete_count; i++) {
        del_keys[i] = (rng_next() % scale) + 1;
    }

    uint64_t start = now_ns();
    for (int i = 0; i < delete_count; i++) {
        oa_remove(&ht, del_keys[i]);
    }
    uint64_t elapsed = now_ns() - start;

    free(del_keys);
    oa_destroy(&ht);

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)delete_count;
    r.mops = (double)delete_count / ((double)elapsed / 1e9) / 1e6;
    return r;
}

static BenchResult bench_sc_delete(uint64_t scale) {
    SC_HashTable ht;
    sc_init(&ht, (uint32_t)(scale * 2));
    for (uint64_t i = 1; i <= scale; i++) {
        sc_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }

    rng_seed(12345);
    int delete_count = (int)(scale / 2);
    uint64_t *del_keys = (uint64_t *)malloc(delete_count * sizeof(uint64_t));
    for (int i = 0; i < delete_count; i++) {
        del_keys[i] = (rng_next() % scale) + 1;
    }

    uint64_t start = now_ns();
    for (int i = 0; i < delete_count; i++) {
        sc_remove(&ht, del_keys[i]);
    }
    uint64_t elapsed = now_ns() - start;

    free(del_keys);
    sc_destroy(&ht);
    pool_destroy_all();

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)delete_count;
    r.mops = (double)delete_count / ((double)elapsed / 1e9) / 1e6;
    return r;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Lookup at Different Load Factors
 * ═══════════════════════════════════════════════════════════════════════ */

typedef struct {
    BenchResult oa_result;
    BenchResult sc_result;
    size_t oa_mem;
    size_t sc_mem;
} LoadFactorResult;

static LoadFactorResult bench_at_load_factor(uint64_t scale, double target_load,
                                             Distribution dist) {
    LoadFactorResult res;

    /* Open addressing: capacity = scale / target_load (rounded to power of 2) */
    uint32_t oa_cap_hint = (uint32_t)((double)scale / target_load);
    OA_HashTable oa;
    oa_init(&oa, oa_cap_hint);

    /* Separate chaining: capacity = scale / target_load */
    uint32_t sc_cap_hint = (uint32_t)((double)scale / target_load);
    SC_HashTable sc;
    sc_init(&sc, sc_cap_hint);

    /* Insert keys */
    for (uint64_t i = 1; i <= scale; i++) {
        oa_put(&oa, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
        sc_put(&sc, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }

    /* Generate lookup keys */
    int num_lookups = 1000000;
    if (scale < 100000) num_lookups = 500000;
    uint64_t *lookup_keys = (uint64_t *)malloc(num_lookups * sizeof(uint64_t));
    rng_seed(scale ^ (uint64_t)(target_load * 1000) ^ ((uint64_t)dist << 40));
    for (int i = 0; i < num_lookups; i++) {
        lookup_keys[i] = gen_lookup_key(dist, scale);
    }

    /* Warmup */
    uint16_t d; uint32_t e;
    for (int w = 0; w < 10000 && w < num_lookups; w++) {
        oa_get(&oa, lookup_keys[w], &d, &e);
        sc_get(&sc, lookup_keys[w], &d, &e);
    }

    /* Benchmark */
    res.oa_result = bench_oa_lookup(&oa, lookup_keys, num_lookups);
    res.sc_result = bench_sc_lookup(&sc, lookup_keys, num_lookups);
    res.oa_mem = oa_memory_bytes(&oa);
    res.sc_mem = sc_memory_bytes(&sc);

    free(lookup_keys);
    oa_destroy(&oa);
    sc_destroy(&sc);
    pool_destroy_all();

    return res;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Mixed Workload (80% lookup, 10% insert, 10% delete)
 * ═══════════════════════════════════════════════════════════════════════ */

static BenchResult bench_oa_mixed(uint64_t scale, Distribution dist) {
    OA_HashTable ht;
    oa_init(&ht, (uint32_t)(scale * 2));

    /* Pre-populate 80% */
    uint64_t init_count = scale * 8 / 10;
    for (uint64_t i = 1; i <= init_count; i++) {
        oa_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }

    int ops = 1000000;
    if (scale >= 10000000) ops = 500000;

    rng_seed(scale ^ 0xDEADCAFEULL ^ (uint64_t)dist);
    uint64_t next_insert_id = init_count + 1;
    volatile uint32_t sink = 0;
    uint16_t d; uint32_t e;

    uint64_t start = now_ns();
    for (int i = 0; i < ops; i++) {
        uint64_t r = rng_next() % 100;
        if (r < 80) {
            /* Lookup */
            uint64_t key = gen_lookup_key(dist, ht.count > 0 ? ht.count : 1);
            if (oa_get(&ht, key, &d, &e) == GRAVELDB_OK) sink += e;
        } else if (r < 90) {
            /* Insert */
            oa_put(&ht, next_insert_id, (uint16_t)(next_insert_id % 8),
                   (uint32_t)(next_insert_id * 7));
            next_insert_id++;
        } else {
            /* Delete */
            uint64_t key = gen_lookup_key(dist, ht.count > 0 ? ht.count : 1);
            oa_remove(&ht, key);
        }
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;

    oa_destroy(&ht);

    BenchResult res;
    res.ns_per_op = (double)elapsed / (double)ops;
    res.mops = (double)ops / ((double)elapsed / 1e9) / 1e6;
    return res;
}

static BenchResult bench_sc_mixed(uint64_t scale, Distribution dist) {
    SC_HashTable ht;
    sc_init(&ht, (uint32_t)(scale * 2));

    uint64_t init_count = scale * 8 / 10;
    for (uint64_t i = 1; i <= init_count; i++) {
        sc_put(&ht, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
    }

    int ops = 1000000;
    if (scale >= 10000000) ops = 500000;

    rng_seed(scale ^ 0xDEADCAFEULL ^ (uint64_t)dist);
    uint64_t next_insert_id = init_count + 1;
    volatile uint32_t sink = 0;
    uint16_t d; uint32_t e;

    uint64_t start = now_ns();
    for (int i = 0; i < ops; i++) {
        uint64_t r = rng_next() % 100;
        if (r < 80) {
            uint64_t key = gen_lookup_key(dist, ht.count > 0 ? ht.count : 1);
            if (sc_get(&ht, key, &d, &e) == GRAVELDB_OK) sink += e;
        } else if (r < 90) {
            sc_put(&ht, next_insert_id, (uint16_t)(next_insert_id % 8),
                   (uint32_t)(next_insert_id * 7));
            next_insert_id++;
        } else {
            uint64_t key = gen_lookup_key(dist, ht.count > 0 ? ht.count : 1);
            sc_remove(&ht, key);
        }
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;

    sc_destroy(&ht);
    pool_destroy_all();

    BenchResult res;
    res.ns_per_op = (double)elapsed / (double)ops;
    res.mops = (double)ops / ((double)elapsed / 1e9) / 1e6;
    return res;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Benchmark: Lookup Miss Rate (50% hit, 50% miss)
 * ═══════════════════════════════════════════════════════════════════════ */

static BenchResult bench_oa_miss(const OA_HashTable *ht, uint64_t scale) {
    int num_lookups = 1000000;
    uint64_t *keys = (uint64_t *)malloc(num_lookups * sizeof(uint64_t));
    rng_seed(0xBADC0FFEEULL ^ scale);
    for (int i = 0; i < num_lookups; i++) {
        if (rng_next() % 2 == 0) {
            keys[i] = (rng_next() % scale) + 1;          /* hit */
        } else {
            keys[i] = scale + (rng_next() % scale) + 1;  /* miss */
        }
    }

    uint16_t d; uint32_t e;
    volatile uint32_t sink = 0;

    uint64_t start = now_ns();
    for (int i = 0; i < num_lookups; i++) {
        if (oa_get(ht, keys[i], &d, &e) == GRAVELDB_OK) sink += e;
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;
    free(keys);

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)num_lookups;
    r.mops = (double)num_lookups / ((double)elapsed / 1e9) / 1e6;
    return r;
}

static BenchResult bench_sc_miss(const SC_HashTable *ht, uint64_t scale) {
    int num_lookups = 1000000;
    uint64_t *keys = (uint64_t *)malloc(num_lookups * sizeof(uint64_t));
    rng_seed(0xBADC0FFEEULL ^ scale);
    for (int i = 0; i < num_lookups; i++) {
        if (rng_next() % 2 == 0) {
            keys[i] = (rng_next() % scale) + 1;
        } else {
            keys[i] = scale + (rng_next() % scale) + 1;
        }
    }

    uint16_t d; uint32_t e;
    volatile uint32_t sink = 0;

    uint64_t start = now_ns();
    for (int i = 0; i < num_lookups; i++) {
        if (sc_get(ht, keys[i], &d, &e) == GRAVELDB_OK) sink += e;
    }
    uint64_t elapsed = now_ns() - start;
    (void)sink;
    free(keys);

    BenchResult r;
    r.ns_per_op = (double)elapsed / (double)num_lookups;
    r.mops = (double)num_lookups / ((double)elapsed / 1e9) / 1e6;
    return r;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Output Formatting
 * ═══════════════════════════════════════════════════════════════════════ */

static void print_divider(void) {
    printf("─────────────────────────────────────────────────────────────────────────────────────\n");
}

/* ═══════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("\n");
    printf("═════════════════════════════════════════════════════════════════════════════════════\n");
    printf("  Hash Table Strategy Benchmark: Open Addressing vs Separate Chaining\n");
    printf("═════════════════════════════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("  Open Addressing (OA): Linear probing, 70%% max load, backward-shift delete\n");
    printf("  Separate Chaining (SC): Linked list per bucket (pool alloc), load factor ~1.0\n");
    printf("  Hash function: splitmix64 finalizer (shared)\n");
    printf("\n");

    uint64_t scales[] = {10000, 100000, 1000000, 10000000};
    int num_scales = 4;

    /* ─── Section 1: Insert Performance ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 1: Insert Throughput (sequential keys, start from empty, auto-grow)      ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");
    printf("  %-10s │ %12s │ %12s │ %10s\n", "Scale", "OA (ns/key)", "SC (ns/key)", "OA/SC");
    print_divider();

    for (int s = 0; s < num_scales; s++) {
        BenchResult oa_ins = bench_oa_insert(scales[s]);
        BenchResult sc_ins = bench_sc_insert(scales[s]);
        double ratio = oa_ins.ns_per_op / sc_ins.ns_per_op;
        printf("  %-10llu │ %9.1f ns │ %9.1f ns │ %8.2fx\n",
               (unsigned long long)scales[s],
               oa_ins.ns_per_op, sc_ins.ns_per_op, ratio);
    }
    printf("\n");

    /* ─── Section 2: Lookup at Various Load Factors ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 2: Lookup Latency at Different Load Factors (uniform distribution)       ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");

    double load_factors[] = {0.50, 0.70, 0.90};
    int num_lf = 3;

    for (int s = 0; s < num_scales; s++) {
        printf("  Scale: %llu keys\n", (unsigned long long)scales[s]);
        printf("  %-6s │ %12s │ %12s │ %8s │ %10s │ %10s │ %10s\n",
               "Load", "OA (ns/key)", "SC (ns/key)", "OA/SC", "OA mem", "SC mem", "Mem ratio");
        print_divider();

        for (int lf = 0; lf < num_lf; lf++) {
            LoadFactorResult res = bench_at_load_factor(scales[s], load_factors[lf], DIST_UNIFORM);
            double ratio = res.oa_result.ns_per_op / res.sc_result.ns_per_op;
            double mem_ratio = (double)res.oa_mem / (double)res.sc_mem;
            printf("  %-6.0f%% │ %9.1f ns │ %9.1f ns │ %6.2fx  │ %7.1f MB │ %7.1f MB │ %8.2fx\n",
                   load_factors[lf] * 100,
                   res.oa_result.ns_per_op, res.sc_result.ns_per_op, ratio,
                   (double)res.oa_mem / (1024.0 * 1024.0),
                   (double)res.sc_mem / (1024.0 * 1024.0),
                   mem_ratio);
        }
        printf("\n");
    }

    /* ─── Section 3: Lookup with Different Distributions ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 3: Lookup by Key Distribution (load factor = 70%%)                        ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");

    Distribution dists[] = {DIST_UNIFORM, DIST_ZIPFIAN, DIST_CLUSTERED};
    int num_dists = 3;

    for (int s = 0; s < num_scales; s++) {
        printf("  Scale: %llu keys\n", (unsigned long long)scales[s]);
        printf("  %-12s │ %12s │ %12s │ %8s\n",
               "Dist", "OA (ns/key)", "SC (ns/key)", "OA/SC");
        print_divider();

        for (int d = 0; d < num_dists; d++) {
            LoadFactorResult res = bench_at_load_factor(scales[s], 0.70, dists[d]);
            double ratio = res.oa_result.ns_per_op / res.sc_result.ns_per_op;
            printf("  %-12s │ %9.1f ns │ %9.1f ns │ %6.2fx\n",
                   dist_names[d],
                   res.oa_result.ns_per_op, res.sc_result.ns_per_op, ratio);
        }
        printf("\n");
    }

    /* ─── Section 4: Lookup with Misses ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 4: Lookup with 50%% Miss Rate (load factor = 70%%)                        ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");
    printf("  %-10s │ %12s │ %12s │ %8s\n", "Scale", "OA (ns/key)", "SC (ns/key)", "OA/SC");
    print_divider();

    for (int s = 0; s < num_scales; s++) {
        OA_HashTable oa;
        SC_HashTable sc;
        oa_init(&oa, (uint32_t)(scales[s] * 2));
        sc_init(&sc, (uint32_t)(scales[s] * 2));

        for (uint64_t i = 1; i <= scales[s]; i++) {
            oa_put(&oa, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
            sc_put(&sc, i, (uint16_t)(i % 8), (uint32_t)(i * 7));
        }

        BenchResult oa_res = bench_oa_miss(&oa, scales[s]);
        BenchResult sc_res = bench_sc_miss(&sc, scales[s]);
        double ratio = oa_res.ns_per_op / sc_res.ns_per_op;
        printf("  %-10llu │ %9.1f ns │ %9.1f ns │ %6.2fx\n",
               (unsigned long long)scales[s],
               oa_res.ns_per_op, sc_res.ns_per_op, ratio);

        oa_destroy(&oa);
        sc_destroy(&sc);
        pool_destroy_all();
    }
    printf("\n");

    /* ─── Section 5: Delete Performance ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 5: Delete Performance (random 50%% of keys)                               ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");
    printf("  %-10s │ %12s │ %12s │ %8s\n", "Scale", "OA (ns/key)", "SC (ns/key)", "OA/SC");
    print_divider();

    for (int s = 0; s < num_scales; s++) {
        BenchResult oa_del = bench_oa_delete(scales[s]);
        BenchResult sc_del = bench_sc_delete(scales[s]);
        double ratio = oa_del.ns_per_op / sc_del.ns_per_op;
        printf("  %-10llu │ %9.1f ns │ %9.1f ns │ %6.2fx\n",
               (unsigned long long)scales[s],
               oa_del.ns_per_op, sc_del.ns_per_op, ratio);
    }
    printf("\n");

    /* ─── Section 6: Mixed Workload ─── */
    printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n");
    printf("┃  Section 6: Mixed Workload (80%% lookup, 10%% insert, 10%% delete)                  ┃\n");
    printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n");
    printf("\n");

    for (int s = 0; s < num_scales; s++) {
        printf("  Scale: %llu keys\n", (unsigned long long)scales[s]);
        printf("  %-12s │ %12s │ %12s │ %8s\n",
               "Dist", "OA (ns/op)", "SC (ns/op)", "OA/SC");
        print_divider();

        for (int d = 0; d < num_dists; d++) {
            BenchResult oa_mix = bench_oa_mixed(scales[s], dists[d]);
            BenchResult sc_mix = bench_sc_mixed(scales[s], dists[d]);
            double ratio = oa_mix.ns_per_op / sc_mix.ns_per_op;
            printf("  %-12s │ %9.1f ns │ %9.1f ns │ %6.2fx\n",
                   dist_names[d], oa_mix.ns_per_op, sc_mix.ns_per_op, ratio);
        }
        printf("\n");
    }

    /* Summary */
    printf("═════════════════════════════════════════════════════════════════════════════════════\n");
    printf("  Analysis\n");
    printf("═════════════════════════════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("  OA/SC ratio interpretation:\n");
    printf("    < 1.0 = Open Addressing FASTER (lower latency)\n");
    printf("    > 1.0 = Separate Chaining FASTER\n");
    printf("\n");
    printf("  Expected behavior:\n");
    printf("    - OA wins at low-medium load (50-70%%): better cache locality,\n");
    printf("      no pointer chasing, data inline in contiguous memory.\n");
    printf("    - SC wins at high load (>85%%): OA linear probing degrades as\n");
    printf("      cluster length grows; SC average chain stays ~1 at load=1.0.\n");
    printf("    - OA wins for lookup-heavy (sequential memory scan is prefetch-friendly).\n");
    printf("    - SC wins for delete-heavy (O(1) unlink vs backward-shift in OA).\n");
    printf("    - OA uses LESS memory at low load (no pointer overhead per entry).\n");
    printf("    - SC uses LESS memory at high load (OA needs spare capacity for probing).\n");
    printf("    - At large scale (10M+) with uniform access, TLB behavior dominates:\n");
    printf("      OA benefits from larger pages; SC scattered nodes hurt.\n");
    printf("\n");
    printf("  Key takeaway for GravelDB:\n");
    printf("    Open addressing is preferred because our workload is:\n");
    printf("      - Read-heavy (lookup >> insert/delete)\n");
    printf("      - Pre-sized (capacity known at init, load stays ~50-70%%)\n");
    printf("      - Latency-sensitive (inline data = fewer cache lines touched)\n");
    printf("\n");

    return 0;
}
