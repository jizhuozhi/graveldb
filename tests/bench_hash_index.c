/*
 * GravelDB - Hash Index Benchmark
 *
 * Measures:
 *   1. Raw insert throughput at various scales
 *   2. Lookup throughput (hit + miss) at various load factors
 *   3. Incremental rehash: overhead during online growth
 *   4. Delete (backward-shift) throughput
 *   5. Mixed workload: interleaved put/get/delete
 *   6. Recovery rebuild: bulk insert from sorted key file
 *
 * The hash index uses:
 *   - Open addressing, linear probing
 *   - splitmix64 hash function
 *   - Incremental rehash (16 slots per mutation)
 *   - Backward-shift deletion
 *   - 70% load factor trigger for growth
 */

#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* ─────────────────── Utilities ─────────────────── */

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static uint64_t splitmix64(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: Insert Throughput at Various Scales
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_insert_scaling(void) {
    printf("── Insert Throughput (scaling) ──\n\n");

    int counts[] = {10000, 100000, 500000, 1000000, 5000000};
    int num_counts = sizeof(counts) / sizeof(counts[0]);

    printf("  %-10s | %-12s | %-14s | %-12s | %-10s\n",
           "N", "Capacity", "Insert ops/s", "Final Load", "Memory MB");
    printf("  %-10s-+-%-12s-+-%-14s-+-%-12s-+-%-10s\n",
           "----------", "------------", "--------------", "------------", "----------");

    for (int c = 0; c < num_counts; c++) {
        int n = counts[c];
        uint32_t init_cap = 1024; /* start small to test rehash path */

        HashIndex idx;
        hash_index_init(&idx, init_cap);

        uint64_t rng = 0x12345ULL;

        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            uint64_t feat_id = splitmix64(&rng);
            if (feat_id == 0) feat_id = 1; /* sentinel is 0 */
            hash_index_put(&idx, feat_id, (uint16_t)(i % 6), (uint32_t)i);
        }
        double elapsed = now_sec() - t0;

        float load = (float)idx.count / (float)idx.capacity;
        float mem_mb = (float)(idx.capacity * sizeof(HashSlot)) / (1024.0f * 1024.0f);
        if (idx.old_slots) {
            mem_mb += (float)(idx.old_capacity * sizeof(HashSlot)) / (1024.0f * 1024.0f);
        }

        printf("  %-10d | %-12u | %14.0f | %12.2f | %10.2f\n",
               n, idx.capacity, n / elapsed, load, mem_mb);

        hash_index_destroy(&idx);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 2: Lookup Throughput (hit vs miss)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_lookup(void) {
    printf("── Lookup Throughput ──\n\n");

    int n = 1000000;
    uint32_t cap = n * 2; /* ~50% load */

    HashIndex idx;
    hash_index_init(&idx, cap);

    /* Insert N keys */
    uint64_t rng = 0xCAFEBABEULL;
    uint64_t *keys = (uint64_t *)malloc(n * sizeof(uint64_t));
    for (int i = 0; i < n; i++) {
        keys[i] = splitmix64(&rng);
        if (keys[i] == 0) keys[i] = 1;
        hash_index_put(&idx, keys[i], (uint16_t)(i % 6), (uint32_t)i);
    }

    /* Finish any pending rehash */
    hash_index_finish_rehash(&idx);

    printf("  %d keys, capacity=%u, load=%.2f\n\n",
           n, idx.capacity, (float)idx.count / idx.capacity);

    printf("  %-24s | %-14s\n", "Pattern", "Ops/s");
    printf("  %-24s-+-%-14s\n", "------------------------", "--------------");

    /* Sequential hit (all keys exist) */
    {
        uint16_t dim_idx;
        uint32_t entry_idx;
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            hash_index_get(&idx, keys[i], &dim_idx, &entry_idx);
        }
        double elapsed = now_sec() - t0;
        printf("  %-24s | %14.0f\n", "sequential hit", n / elapsed);
    }

    /* Random hit (shuffle keys) */
    {
        /* Fisher-Yates shuffle */
        uint64_t shuffle_rng = 0xDEAD1234ULL;
        for (int i = n - 1; i > 0; i--) {
            int j = (int)(xorshift64(&shuffle_rng) % (i + 1));
            uint64_t tmp = keys[i];
            keys[i] = keys[j];
            keys[j] = tmp;
        }

        uint16_t dim_idx;
        uint32_t entry_idx;
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            hash_index_get(&idx, keys[i], &dim_idx, &entry_idx);
        }
        double elapsed = now_sec() - t0;
        printf("  %-24s | %14.0f\n", "random hit", n / elapsed);
    }

    /* Miss (keys that don't exist) */
    {
        uint64_t miss_rng = 0xFEEDFACEULL;
        uint16_t dim_idx;
        uint32_t entry_idx;
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            uint64_t k = splitmix64(&miss_rng) | 0x8000000000000000ULL; /* high bit set → unlikely to collide */
            hash_index_get(&idx, k, &dim_idx, &entry_idx);
        }
        double elapsed = now_sec() - t0;
        printf("  %-24s | %14.0f\n", "miss (non-existent)", n / elapsed);
    }

    /* Mixed (50% hit, 50% miss) */
    {
        uint64_t mix_rng = 0xABCD5678ULL;
        uint16_t dim_idx;
        uint32_t entry_idx;
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            if (i % 2 == 0) {
                hash_index_get(&idx, keys[i / 2], &dim_idx, &entry_idx);
            } else {
                uint64_t k = splitmix64(&mix_rng) | 0x8000000000000000ULL;
                hash_index_get(&idx, k, &dim_idx, &entry_idx);
            }
        }
        double elapsed = now_sec() - t0;
        printf("  %-24s | %14.0f\n", "mixed (50%% hit/miss)", n / elapsed);
    }

    free(keys);
    hash_index_destroy(&idx);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 3: Incremental Rehash Overhead
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_incremental_rehash(void) {
    printf("── Incremental Rehash Overhead ──\n\n");

    /*
     * Measure: insert throughput during active rehash vs stable state.
     * Strategy:
     *   1. Pre-fill to 69% load (just below 70% trigger)
     *   2. Measure insert rate for next 10K ops (crosses threshold, triggers rehash)
     *   3. Compare with inserts that DON'T trigger rehash
     */

    uint32_t init_cap = 1 << 20; /* 1M slots */
    uint32_t pre_fill = (uint32_t)(init_cap * 0.69); /* just below trigger */
    int measure_ops = 50000;

    printf("  Initial capacity: %u, Pre-fill: %u (%.1f%%)\n",
           init_cap, pre_fill, (float)pre_fill / init_cap * 100.0f);
    printf("  Measuring next %d ops (crosses 70%% → triggers rehash)\n\n", measure_ops);

    HashIndex idx;
    hash_index_init(&idx, init_cap);

    uint64_t rng = 0x1234567890ABCDEFULL;

    /* Pre-fill */
    for (uint32_t i = 0; i < pre_fill; i++) {
        uint64_t feat_id = splitmix64(&rng);
        if (feat_id == 0) feat_id = 1;
        hash_index_put(&idx, feat_id, 0, i);
    }

    printf("  State before measurement: count=%u, capacity=%u, load=%.3f\n",
           idx.count, idx.capacity, (float)idx.count / idx.capacity);

    /* Measure: inserts that trigger + progress rehash */
    double t0 = now_sec();
    for (int i = 0; i < measure_ops; i++) {
        uint64_t feat_id = splitmix64(&rng);
        if (feat_id == 0) feat_id = 1;
        hash_index_put(&idx, feat_id, 0, (uint32_t)(pre_fill + i));
    }
    double rehash_elapsed = now_sec() - t0;

    bool rehash_triggered = (idx.capacity > init_cap) || (idx.old_slots != NULL);
    printf("  Rehash triggered: %s\n", rehash_triggered ? "YES" : "NO");
    printf("  State after: count=%u, capacity=%u, old_slots=%s\n",
           idx.count, idx.capacity, idx.old_slots ? "migrating" : "NULL");
    printf("  Insert ops/s (during rehash): %.0f\n\n", measure_ops / rehash_elapsed);

    /* Now measure stable state (no rehash) */
    hash_index_finish_rehash(&idx);
    printf("  After finish_rehash: count=%u, capacity=%u, load=%.3f\n",
           idx.count, idx.capacity, (float)idx.count / idx.capacity);

    t0 = now_sec();
    for (int i = 0; i < measure_ops; i++) {
        uint64_t feat_id = splitmix64(&rng);
        if (feat_id == 0) feat_id = 1;
        hash_index_put(&idx, feat_id, 0, (uint32_t)(pre_fill + measure_ops + i));
    }
    double stable_elapsed = now_sec() - t0;
    printf("  Insert ops/s (stable, no rehash): %.0f\n", measure_ops / stable_elapsed);
    printf("  Overhead ratio: %.2fx\n\n",
           (rehash_elapsed / measure_ops) / (stable_elapsed / measure_ops));

    hash_index_destroy(&idx);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 4: Delete (Backward-Shift) Throughput
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_delete(void) {
    printf("── Delete (Backward-Shift) Throughput ──\n\n");

    int sizes[] = {100000, 500000, 1000000};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    printf("  %-10s | %-12s | %-14s | %-14s\n",
           "N", "Load After", "Delete ops/s", "Lookup After");
    printf("  %-10s-+-%-12s-+-%-14s-+-%-14s\n",
           "----------", "------------", "--------------", "--------------");

    for (int s = 0; s < num_sizes; s++) {
        int n = sizes[s];
        HashIndex idx;
        hash_index_init(&idx, n * 2);

        uint64_t rng = 0xFEED1234ULL;
        uint64_t *keys = (uint64_t *)malloc(n * sizeof(uint64_t));

        /* Insert all */
        for (int i = 0; i < n; i++) {
            keys[i] = splitmix64(&rng);
            if (keys[i] == 0) keys[i] = 1;
            hash_index_put(&idx, keys[i], (uint16_t)(i % 6), (uint32_t)i);
        }
        hash_index_finish_rehash(&idx);

        /* Delete 50% (every other key) */
        int del_count = n / 2;
        double t0 = now_sec();
        for (int i = 0; i < del_count; i++) {
            hash_index_remove(&idx, keys[i * 2]);
        }
        double del_elapsed = now_sec() - t0;

        /* Verify remaining keys are still findable */
        uint16_t dim_idx;
        uint32_t entry_idx;
        int found = 0;
        t0 = now_sec();
        for (int i = 0; i < del_count; i++) {
            if (hash_index_get(&idx, keys[i * 2 + 1], &dim_idx, &entry_idx) == GRAVELDB_OK) {
                found++;
            }
        }
        double lookup_elapsed = now_sec() - t0;

        float load = (float)idx.count / (float)idx.capacity;
        printf("  %-10d | %12.3f | %14.0f | %14.0f (%d/%d ok)\n",
               n, load, del_count / del_elapsed, del_count / lookup_elapsed, found, del_count);

        free(keys);
        hash_index_destroy(&idx);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 5: Mixed Workload (YCSB-like)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_mixed_workload(void) {
    printf("── Mixed Workload (YCSB-style) ──\n\n");

    /*
     * Workload profiles:
     *   A: 50% read, 50% update (read-heavy)
     *   B: 95% read, 5% update
     *   C: Insert-heavy (50% insert, 25% read, 25% delete)
     */

    int n_initial = 500000; /* pre-fill */
    int n_ops = 1000000;

    HashIndex idx;
    hash_index_init(&idx, n_initial * 4);

    /* Pre-fill */
    uint64_t rng = 0xBEEFCAFE1234ULL;
    uint64_t *existing = (uint64_t *)malloc(n_initial * sizeof(uint64_t));
    for (int i = 0; i < n_initial; i++) {
        existing[i] = splitmix64(&rng);
        if (existing[i] == 0) existing[i] = 1;
        hash_index_put(&idx, existing[i], (uint16_t)(i % 6), (uint32_t)i);
    }
    hash_index_finish_rehash(&idx);

    printf("  Pre-filled: %d keys, capacity: %u\n\n", n_initial, idx.capacity);

    typedef struct {
        const char *name;
        int read_pct;
        int update_pct;
        int insert_pct;
        int delete_pct;
    } WorkloadProfile;

    WorkloadProfile profiles[] = {
        { "A: 50R/50U",        50, 50,  0,  0 },
        { "B: 95R/5U",         95,  5,  0,  0 },
        { "C: 25R/50I/25D",    25,  0, 50, 25 },
    };
    int num_profiles = sizeof(profiles) / sizeof(profiles[0]);

    printf("  %-20s | %-14s | %-10s\n", "Profile", "Ops/s", "Final Count");
    printf("  %-20s-+-%-14s-+-%-10s\n", "--------------------", "--------------", "----------");

    for (int p = 0; p < num_profiles; p++) {
        WorkloadProfile *wp = &profiles[p];

        /* Reset index for each profile */
        hash_index_destroy(&idx);
        hash_index_init(&idx, n_initial * 4);
        rng = 0xBEEFCAFE1234ULL;
        for (int i = 0; i < n_initial; i++) {
            existing[i] = splitmix64(&rng);
            if (existing[i] == 0) existing[i] = 1;
            hash_index_put(&idx, existing[i], (uint16_t)(i % 6), (uint32_t)i);
        }
        hash_index_finish_rehash(&idx);

        uint64_t op_rng = 0xDEAD5678ABCDEFULL;
        uint64_t key_rng = 0x9876FEDCBA12ULL;
        int existing_count = n_initial;
        int exist_cap = n_initial * 2;
        existing = (uint64_t *)realloc(existing, exist_cap * sizeof(uint64_t));

        double t0 = now_sec();
        for (int i = 0; i < n_ops; i++) {
            int roll = (int)(xorshift64(&op_rng) % 100);
            if (roll < wp->read_pct) {
                /* Read */
                uint16_t dim_idx;
                uint32_t entry_idx;
                int ki = (int)(xorshift64(&op_rng) % existing_count);
                hash_index_get(&idx, existing[ki], &dim_idx, &entry_idx);
            } else if (roll < wp->read_pct + wp->update_pct) {
                /* Update (put existing key with new value) */
                int ki = (int)(xorshift64(&op_rng) % existing_count);
                hash_index_put(&idx, existing[ki], (uint16_t)((i + 1) % 6), (uint32_t)(i + n_initial));
            } else if (roll < wp->read_pct + wp->update_pct + wp->insert_pct) {
                /* Insert new key */
                uint64_t new_key = splitmix64(&key_rng);
                if (new_key == 0) new_key = 1;
                hash_index_put(&idx, new_key, (uint16_t)(i % 6), (uint32_t)(i + n_initial));
                if (existing_count < exist_cap) {
                    existing[existing_count++] = new_key;
                }
            } else {
                /* Delete */
                if (existing_count > 1) {
                    int ki = (int)(xorshift64(&op_rng) % existing_count);
                    hash_index_remove(&idx, existing[ki]);
                    existing[ki] = existing[--existing_count]; /* swap-remove */
                }
            }
        }
        double elapsed = now_sec() - t0;

        printf("  %-20s | %14.0f | %10u\n", wp->name, n_ops / elapsed, idx.count);
    }

    free(existing);
    hash_index_destroy(&idx);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 6: Recovery Rebuild (Bulk Sequential Insert)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_recovery_rebuild(void) {
    printf("── Recovery Rebuild (bulk insert, simulates .keys replay) ──\n\n");

    int counts[] = {100000, 500000, 1000000, 5000000};
    int num_counts = sizeof(counts) / sizeof(counts[0]);

    printf("  %-10s | %-14s | %-12s\n", "N", "Rebuild ops/s", "Time");
    printf("  %-10s-+-%-14s-+-%-12s\n", "----------", "--------------", "------------");

    for (int c = 0; c < num_counts; c++) {
        int n = counts[c];

        /* Pre-generate keys (simulating reading from .keys file) */
        uint64_t *keys = (uint64_t *)malloc(n * sizeof(uint64_t));
        uint64_t rng = 0xAAAABBBBCCCCDDDDULL;
        for (int i = 0; i < n; i++) {
            keys[i] = splitmix64(&rng);
            if (keys[i] == 0) keys[i] = 1;
        }

        /* Build index from scratch (what recovery does) */
        HashIndex idx;
        hash_index_init(&idx, n * 2); /* pre-sized for expected count */

        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            hash_index_put(&idx, keys[i], 0, (uint32_t)i);
        }
        hash_index_finish_rehash(&idx);
        double elapsed = now_sec() - t0;

        printf("  %-10d | %14.0f | %10.3fs\n", n, n / elapsed, elapsed);

        free(keys);
        hash_index_destroy(&idx);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Hash Index Benchmark\n");
    printf("==============================\n\n");

    bench_insert_scaling();
    bench_lookup();
    bench_incremental_rehash();
    bench_delete();
    bench_mixed_workload();
    bench_recovery_rebuild();

    printf("Benchmark complete.\n");
    return 0;
}
