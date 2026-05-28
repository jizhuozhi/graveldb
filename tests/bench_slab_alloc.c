/*
 * GravelDB - Slab Allocator Benchmark
 *
 * Measures:
 *   1. Pool alloc/free throughput vs malloc (various object sizes)
 *   2. Bulk alloc/free throughput
 *   3. Aligned allocation (4KB page buffers) throughput
 *   4. Mixed alloc/free pattern (simulating DimBin page buffer churn)
 *   5. Pool growth (slab page creation) amortization
 *   6. Memory efficiency: overhead per allocation
 *
 * Key insight: slab allocator is on the hot path for every page buffer
 * allocation (read cache load, write buffer ensure). Its performance
 * directly impacts get/put latency.
 */

#include "slab_alloc.h"
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

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: Pool Alloc/Free vs malloc (various sizes)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_alloc_free_vs_malloc(void) {
    printf("── Pool Alloc/Free vs malloc ──\n\n");

    size_t sizes[] = {32, 64, 128, 256, 512, 1024, 2048, 4096};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);
    int num_ops = 1000000;

    printf("  %-8s | %-16s | %-16s | %-16s | %-16s | %-8s\n",
           "Size", "Slab alloc/s", "Slab free/s", "Malloc alloc/s", "Malloc free/s", "Speedup");
    printf("  %-8s-+-%-16s-+-%-16s-+-%-16s-+-%-16s-+-%-8s\n",
           "--------", "----------------", "----------------", "----------------", "----------------", "--------");

    for (int s = 0; s < num_sizes; s++) {
        size_t obj_size = sizes[s];
        void **ptrs = (void **)malloc(num_ops * sizeof(void *));

        SlabAllocator alloc;
        slab_allocator_init(&alloc);
        SlabPool *pool = slab_pool_create(&alloc, obj_size, 16);

        /* Slab alloc */
        double t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            ptrs[i] = slab_pool_alloc(pool);
        }
        double slab_alloc_elapsed = now_sec() - t0;

        /* Slab free */
        t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            slab_pool_free(pool, ptrs[i]);
        }
        double slab_free_elapsed = now_sec() - t0;

        /* malloc alloc */
        t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            ptrs[i] = malloc(obj_size);
        }
        double malloc_alloc_elapsed = now_sec() - t0;

        /* malloc free */
        t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            free(ptrs[i]);
        }
        double malloc_free_elapsed = now_sec() - t0;

        double speedup = (malloc_alloc_elapsed + malloc_free_elapsed) /
                         (slab_alloc_elapsed + slab_free_elapsed);

        printf("  %-8zu | %16.0f | %16.0f | %16.0f | %16.0f | %6.2fx\n",
               obj_size,
               num_ops / slab_alloc_elapsed,
               num_ops / slab_free_elapsed,
               num_ops / malloc_alloc_elapsed,
               num_ops / malloc_free_elapsed,
               speedup);

        slab_pool_destroy(&alloc, pool);
        slab_allocator_destroy(&alloc);
        free(ptrs);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 2: Bulk Alloc/Free
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_bulk_operations(void) {
    printf("── Bulk Alloc/Free ──\n\n");

    size_t obj_size = 512; /* typical embedding: dim=128, float=4 → 512B */
    int batch_sizes[] = {1, 4, 16, 64, 256, 1024};
    int num_batches = sizeof(batch_sizes) / sizeof(batch_sizes[0]);
    int total_ops = 500000;

    printf("  %-10s | %-16s | %-16s | %-14s\n",
           "Batch", "Bulk alloc/s", "Bulk free/s", "Total time");
    printf("  %-10s-+-%-16s-+-%-16s-+-%-14s\n",
           "----------", "----------------", "----------------", "--------------");

    for (int b = 0; b < num_batches; b++) {
        int batch = batch_sizes[b];
        int num_batches_run = total_ops / batch;
        int actual_total = num_batches_run * batch;

        SlabAllocator alloc;
        slab_allocator_init(&alloc);
        SlabPool *pool = slab_pool_create(&alloc, obj_size, 16);

        void **ptrs = (void **)malloc(batch * sizeof(void *));

        /* Bulk alloc */
        double t0 = now_sec();
        for (int i = 0; i < num_batches_run; i++) {
            slab_pool_alloc_bulk(pool, ptrs, batch);
            /* Don't free yet — accumulate */
            /* Actually we need to free in batches too, so alternate */
        }
        double alloc_elapsed = now_sec() - t0;

        /* Bulk free (free everything allocated) */
        /* Re-allocate and then bulk-free to measure free path */
        void **all_ptrs = (void **)malloc(actual_total * sizeof(void *));

        /* Re-init pool */
        slab_pool_destroy(&alloc, pool);
        slab_allocator_destroy(&alloc);
        slab_allocator_init(&alloc);
        pool = slab_pool_create(&alloc, obj_size, 16);

        for (int i = 0; i < actual_total; i++) {
            all_ptrs[i] = slab_pool_alloc(pool);
        }

        t0 = now_sec();
        for (int i = 0; i < num_batches_run; i++) {
            slab_pool_free_bulk(pool, all_ptrs + i * batch, batch);
        }
        double free_elapsed = now_sec() - t0;

        printf("  %-10d | %16.0f | %16.0f | %12.4fs\n",
               batch,
               actual_total / alloc_elapsed,
               actual_total / free_elapsed,
               alloc_elapsed + free_elapsed);

        free(ptrs);
        free(all_ptrs);
        slab_pool_destroy(&alloc, pool);
        slab_allocator_destroy(&alloc);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 3: Aligned Allocation (4KB page buffers)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_aligned_alloc(void) {
    printf("── Aligned Allocation (4KB, page buffers) ──\n\n");

    int counts[] = {1000, 10000, 50000};
    int num_counts = sizeof(counts) / sizeof(counts[0]);

    printf("  %-8s | %-16s | %-16s | %-16s | %-16s\n",
           "N", "Slab alloc/s", "Slab free/s", "posix_memalign/s", "free/s");
    printf("  %-8s-+-%-16s-+-%-16s-+-%-16s-+-%-16s\n",
           "--------", "----------------", "----------------", "----------------", "----------------");

    for (int c = 0; c < num_counts; c++) {
        int n = counts[c];
        void **ptrs = (void **)malloc(n * sizeof(void *));

        SlabAllocator alloc;
        slab_allocator_init(&alloc);

        /* Slab aligned alloc */
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            ptrs[i] = slab_alloc_aligned(&alloc, 4096, 4096);
        }
        double slab_alloc_time = now_sec() - t0;

        /* Slab aligned free */
        t0 = now_sec();
        for (int i = 0; i < n; i++) {
            slab_free_aligned(&alloc, ptrs[i], 4096);
        }
        double slab_free_time = now_sec() - t0;

        slab_allocator_destroy(&alloc);

        /* posix_memalign comparison */
        t0 = now_sec();
        for (int i = 0; i < n; i++) {
            posix_memalign(&ptrs[i], 4096, 4096);
        }
        double posix_alloc_time = now_sec() - t0;

        t0 = now_sec();
        for (int i = 0; i < n; i++) {
            free(ptrs[i]);
        }
        double posix_free_time = now_sec() - t0;

        printf("  %-8d | %16.0f | %16.0f | %16.0f | %16.0f\n",
               n,
               n / slab_alloc_time,
               n / slab_free_time,
               n / posix_alloc_time,
               n / posix_free_time);

        free(ptrs);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 4: Mixed Alloc/Free Pattern (Simulating Page Buffer Churn)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_churn_pattern(void) {
    printf("── Page Buffer Churn (alloc/free interleaved) ──\n\n");

    /*
     * Simulates read cache behavior:
     *   - Cache has max N pages
     *   - On miss: alloc page, on eviction: free page
     *   - Steady state: alloc rate ≈ free rate
     *
     * Pattern: maintain a pool of N live allocations, randomly
     * free one and immediately allocate a new one.
     */

    int cache_sizes[] = {256, 1024, 4096, 16384};
    int num_sizes = sizeof(cache_sizes) / sizeof(cache_sizes[0]);
    int churn_ops = 500000;

    printf("  %-10s | %-16s | %-16s | %-8s\n",
           "Cache Size", "Slab churn/s", "Malloc churn/s", "Speedup");
    printf("  %-10s-+-%-16s-+-%-16s-+-%-8s\n",
           "----------", "----------------", "----------------", "--------");

    for (int s = 0; s < num_sizes; s++) {
        int cache_size = cache_sizes[s];
        void **live = (void **)malloc(cache_size * sizeof(void *));
        uint64_t rng = 0xFEEDFACE1234ULL;

        /* Slab churn */
        SlabAllocator alloc;
        slab_allocator_init(&alloc);

        /* Pre-fill cache */
        for (int i = 0; i < cache_size; i++) {
            live[i] = slab_alloc_aligned(&alloc, 4096, 4096);
        }

        double t0 = now_sec();
        for (int i = 0; i < churn_ops; i++) {
            int victim = (int)(xorshift64(&rng) % cache_size);
            slab_free_aligned(&alloc, live[victim], 4096);
            live[victim] = slab_alloc_aligned(&alloc, 4096, 4096);
        }
        double slab_elapsed = now_sec() - t0;

        /* Cleanup slab */
        for (int i = 0; i < cache_size; i++) {
            slab_free_aligned(&alloc, live[i], 4096);
        }
        slab_allocator_destroy(&alloc);

        /* Malloc churn */
        for (int i = 0; i < cache_size; i++) {
            posix_memalign(&live[i], 4096, 4096);
        }

        rng = 0xFEEDFACE1234ULL; /* same sequence */
        t0 = now_sec();
        for (int i = 0; i < churn_ops; i++) {
            int victim = (int)(xorshift64(&rng) % cache_size);
            free(live[victim]);
            posix_memalign(&live[victim], 4096, 4096);
        }
        double malloc_elapsed = now_sec() - t0;

        for (int i = 0; i < cache_size; i++) {
            free(live[i]);
        }

        double speedup = malloc_elapsed / slab_elapsed;

        printf("  %-10d | %16.0f | %16.0f | %6.2fx\n",
               cache_size,
               churn_ops / slab_elapsed,
               churn_ops / malloc_elapsed,
               speedup);

        free(live);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 5: Pool Growth Amortization
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_pool_growth(void) {
    printf("── Pool Growth Amortization ──\n\n");

    /*
     * Start with a fresh pool (1 slab page = 64KB).
     * Measure alloc throughput as we exhaust pages and force growth.
     * Compare first N allocs (from pre-carved free list) vs later allocs
     * (that trigger page creation).
     */

    size_t obj_size = 128; /* 512 objects per 64KB page */
    int objects_per_page = 65536 / 128;
    int num_pages_to_fill = 20;
    int total_allocs = objects_per_page * num_pages_to_fill;

    SlabAllocator alloc;
    slab_allocator_init(&alloc);
    SlabPool *pool = slab_pool_create(&alloc, obj_size, 16);

    void **ptrs = (void **)malloc(total_allocs * sizeof(void *));

    printf("  Object size: %zu, Objects/page: %d, Total: %d allocs across %d pages\n\n",
           obj_size, objects_per_page, total_allocs, num_pages_to_fill);

    printf("  %-12s | %-16s | %-10s\n",
           "Phase", "Alloc ops/s", "Slow allocs");
    printf("  %-12s-+-%-16s-+-%-10s\n",
           "------------", "----------------", "----------");

    /* Measure per-page-worth of allocs */
    uint64_t prev_slow = pool->alloc_slow;
    for (int page = 0; page < num_pages_to_fill; page++) {
        int start = page * objects_per_page;
        int end = start + objects_per_page;

        double t0 = now_sec();
        for (int i = start; i < end; i++) {
            ptrs[i] = slab_pool_alloc(pool);
        }
        double elapsed = now_sec() - t0;

        uint64_t slow_this_page = pool->alloc_slow - prev_slow;
        prev_slow = pool->alloc_slow;

        if (page < 3 || page == num_pages_to_fill - 1) {
            char label[32];
            snprintf(label, sizeof(label), "Page %d", page);
            printf("  %-12s | %16.0f | %10llu\n",
                   label, objects_per_page / elapsed, (unsigned long long)slow_this_page);
        } else if (page == 3) {
            printf("  %-12s | %16s | %10s\n", "...", "...", "...");
        }
    }

    /* Overall stats */
    SlabAllocStats stats;
    slab_allocator_stats(&alloc, &stats);
    printf("\n  Total pages allocated: %llu, Total memory: %llu KB\n",
           (unsigned long long)stats.total_pages,
           (unsigned long long)stats.total_bytes_from_os / 1024);
    printf("  Fast allocs: %llu, Slow allocs: %llu (%.2f%% slow)\n",
           (unsigned long long)stats.pools[0].alloc_fast,
           (unsigned long long)stats.pools[0].alloc_slow,
           (double)stats.pools[0].alloc_slow / (stats.pools[0].alloc_fast + stats.pools[0].alloc_slow) * 100.0);

    free(ptrs);
    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 6: Convenience API Overhead (slab_alloc/slab_free with pool lookup)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_convenience_api(void) {
    printf("── Convenience API (slab_alloc/free with pool lookup) ──\n\n");

    int num_ops = 500000;

    /* Multiple sizes to test pool lookup overhead */
    size_t sizes[] = {64, 128, 256, 512};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    SlabAllocator alloc;
    slab_allocator_init(&alloc);

    printf("  %-8s | %-16s | %-16s | %-8s\n",
           "Size", "Direct pool/s", "Convenience/s", "Overhead");
    printf("  %-8s-+-%-16s-+-%-16s-+-%-8s\n",
           "--------", "----------------", "----------------", "--------");

    for (int s = 0; s < num_sizes; s++) {
        size_t obj_size = sizes[s];
        void **ptrs = (void **)malloc(num_ops * sizeof(void *));

        /* Direct pool API */
        SlabPool *pool = slab_pool_create(&alloc, obj_size, 16);

        double t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            ptrs[i] = slab_pool_alloc(pool);
        }
        for (int i = 0; i < num_ops; i++) {
            slab_pool_free(pool, ptrs[i]);
        }
        double direct_elapsed = now_sec() - t0;

        /* Convenience API (includes pool lookup) */
        t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            ptrs[i] = slab_alloc(&alloc, obj_size);
        }
        for (int i = 0; i < num_ops; i++) {
            slab_free(&alloc, ptrs[i], obj_size);
        }
        double convenience_elapsed = now_sec() - t0;

        double overhead = convenience_elapsed / direct_elapsed;

        printf("  %-8zu | %16.0f | %16.0f | %6.2fx\n",
               obj_size,
               num_ops * 2.0 / direct_elapsed,
               num_ops * 2.0 / convenience_elapsed,
               overhead);

        free(ptrs);
    }

    slab_allocator_destroy(&alloc);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Slab Allocator Benchmark\n");
    printf("==================================\n\n");

    bench_alloc_free_vs_malloc();
    bench_bulk_operations();
    bench_aligned_alloc();
    bench_churn_pattern();
    bench_pool_growth();
    bench_convenience_api();

    printf("Benchmark complete.\n");
    return 0;
}
