/*
 * GravelDB - Cache Subsystem Benchmark
 *
 * Measures performance of:
 *   1. WriteBuffer hashmap: insert/lookup/flush throughput
 *   2. ReadCache + TinyLFU: hit rate under various access patterns,
 *      admission effectiveness, eviction overhead
 *   3. TinyLFU standalone: access/estimate throughput, decay impact
 *   4. Page hashmap operations: raw insert/lookup/remove/grow
 *
 * Access patterns tested:
 *   - Zipfian (realistic skewed)
 *   - Uniform random (worst case for caching)
 *   - Sequential scan (scan pollution test)
 *   - Hotspot with scan interleave (TinyLFU's key advantage)
 */

#define _XOPEN_SOURCE 700   /* for mkdtemp */
#include "dimbin.h"
#include "tinylfu.h"
#include "slab_alloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>

/* ─── Local copies of pagemap helpers (static in dimbin.c) ─── */

static inline uint32_t bench_page_hash(uint32_t page_id, uint32_t mask) {
    return (page_id * 2654435761u) & mask;
}

static inline uint32_t bench_pagemap_find(const PageSlot *slots, uint32_t capacity,
                                           uint32_t page_id) {
    uint32_t mask = capacity - 1;
    uint32_t idx = bench_page_hash(page_id, mask);
    while (slots[idx].page_id != PAGE_SLOT_EMPTY && slots[idx].page_id != page_id) {
        idx = (idx + 1) & mask;
    }
    return idx;
}

static PageSlot *bench_pagemap_grow(PageSlot *old_slots, uint32_t old_cap,
                                     uint32_t new_cap) {
    PageSlot *new_slots = (PageSlot *)malloc(new_cap * sizeof(PageSlot));
    if (!new_slots) return NULL;
    for (uint32_t i = 0; i < new_cap; i++) {
        new_slots[i].page_id = PAGE_SLOT_EMPTY;
        new_slots[i].data = NULL;
    }
    if (old_slots) {
        for (uint32_t i = 0; i < old_cap; i++) {
            if (old_slots[i].page_id != PAGE_SLOT_EMPTY) {
                uint32_t idx = bench_pagemap_find(new_slots, new_cap, old_slots[i].page_id);
                new_slots[idx] = old_slots[i];
            }
        }
        free(old_slots);
    }
    return new_slots;
}

static void bench_pagemap_remove(PageSlot *slots, uint32_t capacity, uint32_t rm_idx) {
    uint32_t mask = capacity - 1;
    slots[rm_idx].page_id = PAGE_SLOT_EMPTY;
    slots[rm_idx].data = NULL;
    uint32_t idx = (rm_idx + 1) & mask;
    while (slots[idx].page_id != PAGE_SLOT_EMPTY) {
        PageSlot tmp = slots[idx];
        slots[idx].page_id = PAGE_SLOT_EMPTY;
        slots[idx].data = NULL;
        uint32_t new_idx = bench_pagemap_find(slots, capacity, tmp.page_id);
        slots[new_idx] = tmp;
        idx = (idx + 1) & mask;
    }
}

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

/*
 * Zipfian distribution generator (rejection method approximation).
 * theta=0.99 → highly skewed (typical DB workload).
 */
static uint32_t *gen_zipfian(int count, uint32_t range, double theta) {
    uint32_t *arr = (uint32_t *)malloc(count * sizeof(uint32_t));
    uint64_t rng = 0xCAFEBABE12345678ULL;

    /* Precompute harmonic numbers for rejection sampling */
    double c = 0.0;
    for (uint32_t i = 1; i <= range; i++) {
        c += 1.0 / pow((double)i, theta);
    }
    double inv_c = 1.0 / c;

    for (int i = 0; i < count; i++) {
        double u = (double)(xorshift64(&rng) & 0xFFFFFFFFULL) / (double)0xFFFFFFFFULL;
        double sum = 0.0;
        uint32_t val = 1;
        for (uint32_t k = 1; k <= range; k++) {
            sum += inv_c / pow((double)k, theta);
            if (sum >= u) {
                val = k;
                break;
            }
        }
        arr[i] = val - 1; /* 0-indexed page_id */
    }
    return arr;
}

static uint32_t *gen_uniform(int count, uint32_t range) {
    uint32_t *arr = (uint32_t *)malloc(count * sizeof(uint32_t));
    uint64_t rng = 0xDEADBEEF87654321ULL;
    for (int i = 0; i < count; i++) {
        arr[i] = (uint32_t)(xorshift64(&rng) % range);
    }
    return arr;
}

static uint32_t *gen_sequential(int count, uint32_t range) {
    uint32_t *arr = (uint32_t *)malloc(count * sizeof(uint32_t));
    for (int i = 0; i < count; i++) {
        arr[i] = (uint32_t)(i % range);
    }
    return arr;
}

/*
 * Hotspot with scan interleave:
 * 70% accesses hit top 10% of pages (hot working set).
 * 30% accesses do sequential scan (scan pollution).
 */
static uint32_t *gen_hotspot_scan(int count, uint32_t range) {
    uint32_t *arr = (uint32_t *)malloc(count * sizeof(uint32_t));
    uint64_t rng = 0xFEEDFACE00001234ULL;
    uint32_t hot_range = range / 10;
    if (hot_range == 0) hot_range = 1;
    uint32_t scan_cursor = 0;

    for (int i = 0; i < count; i++) {
        uint64_t r = xorshift64(&rng);
        if ((r % 100) < 70) {
            arr[i] = (uint32_t)(xorshift64(&rng) % hot_range);
        } else {
            arr[i] = scan_cursor++;
            if (scan_cursor >= range) scan_cursor = 0;
        }
    }
    return arr;
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: TinyLFU Standalone Throughput
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_tinylfu_throughput(void) {
    printf("── TinyLFU Throughput ──\n\n");

    uint32_t widths[] = {1024, 8192, 65536, 262144, 1048576};
    int num_widths = sizeof(widths) / sizeof(widths[0]);
    int num_ops = 1000000;

    printf("  %-12s | %-14s | %-14s | %-14s | %-10s\n",
           "CMS Width", "Access ops/s", "Estimate ops/s", "Decays", "Memory KB");
    printf("  %-12s-+-%-14s-+-%-14s-+-%-14s-+-%-10s\n",
           "------------", "--------------", "--------------", "--------------", "----------");

    for (int w = 0; w < num_widths; w++) {
        TinyLFU lfu;
        tinylfu_init(&lfu, widths[w], 1);

        uint64_t rng = 0x12345678ABCDULL;

        /* Access benchmark */
        double t0 = now_sec();
        for (int i = 0; i < num_ops; i++) {
            tinylfu_access(&lfu, xorshift64(&rng));
        }
        double access_elapsed = now_sec() - t0;

        /* Estimate benchmark */
        rng = 0xABCD12345678ULL;
        t0 = now_sec();
        volatile uint8_t sink = 0;
        for (int i = 0; i < num_ops; i++) {
            sink += tinylfu_estimate(&lfu, xorshift64(&rng));
        }
        double estimate_elapsed = now_sec() - t0;
        (void)sink;

        size_t mem_kb = (size_t)widths[w] * 4 / 1024; /* 4 rows */

        printf("  %-12u | %14.0f | %14.0f | %14llu | %10zu\n",
               widths[w],
               num_ops / access_elapsed,
               num_ops / estimate_elapsed,
               (unsigned long long)(num_ops / (lfu.decay_threshold > 0 ? lfu.decay_threshold : 1)),
               mem_kb);

        tinylfu_destroy(&lfu);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 2: Page Hashmap (raw operations)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_page_hashmap(void) {
    printf("── Page Hashmap (OA, Fibonacci hash) ──\n\n");

    int scales[] = {1000, 10000, 100000, 500000};
    int num_scales = sizeof(scales) / sizeof(scales[0]);

    printf("  %-8s | %-10s | %-14s | %-14s | %-14s | %-12s\n",
           "N", "Load Fac", "Insert ops/s", "Lookup ops/s", "Remove ops/s", "Grow time");
    printf("  %-8s-+-%-10s-+-%-14s-+-%-14s-+-%-14s-+-%-12s\n",
           "--------", "----------", "--------------", "--------------", "--------------", "------------");

    for (int s = 0; s < num_scales; s++) {
        int n = scales[s];
        uint32_t cap = 64;
        while (cap < (uint32_t)n * 2) cap *= 2;

        PageSlot *slots = (PageSlot *)malloc(cap * sizeof(PageSlot));
        for (uint32_t i = 0; i < cap; i++) {
            slots[i].page_id = PAGE_SLOT_EMPTY;
            slots[i].data = NULL;
        }

        /* Insert */
        double t0 = now_sec();
        for (int i = 0; i < n; i++) {
            uint32_t page_id = (uint32_t)(i * 7 + 3); /* scattered IDs */
            uint32_t idx = bench_pagemap_find(slots, cap, page_id);
            slots[idx].page_id = page_id;
            slots[idx].data = (uint8_t *)(uintptr_t)(i + 1);
        }
        double insert_elapsed = now_sec() - t0;

        /* Lookup (hit) */
        t0 = now_sec();
        volatile uintptr_t sink = 0;
        for (int i = 0; i < n; i++) {
            uint32_t page_id = (uint32_t)(i * 7 + 3);
            uint32_t idx = bench_pagemap_find(slots, cap, page_id);
            sink += (uintptr_t)slots[idx].data;
        }
        double lookup_elapsed = now_sec() - t0;
        (void)sink;

        /* Grow (rehash to 2x) */
        t0 = now_sec();
        uint32_t new_cap = cap * 2;
        PageSlot *new_slots = bench_pagemap_grow(slots, cap, new_cap);
        double grow_elapsed = now_sec() - t0;

        /* Remove */
        t0 = now_sec();
        for (int i = 0; i < n / 2; i++) {
            uint32_t page_id = (uint32_t)(i * 7 + 3);
            uint32_t idx = bench_pagemap_find(new_slots, new_cap, page_id);
            if (new_slots[idx].page_id == page_id) {
                bench_pagemap_remove(new_slots, new_cap, idx);
            }
        }
        double remove_elapsed = now_sec() - t0;

        float load_factor = (float)n / (float)cap;

        printf("  %-8d | %-10.2f | %14.0f | %14.0f | %14.0f | %10.4fs\n",
               n, load_factor,
               n / insert_elapsed,
               n / lookup_elapsed,
               (n / 2) / remove_elapsed,
               grow_elapsed);

        free(new_slots);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 3: ReadCache Hit Rate + Admission Effectiveness
 * ═══════════════════════════════════════════════════════════════════════════════ */

/*
 * Simulate read cache behavior without actual disk I/O.
 * We only care about: hit/miss/eviction counts and admission decisions.
 */
typedef struct {
    const char *name;
    uint32_t   *accesses;
    int         count;
    uint32_t    universe;   /* total number of distinct pages possible */
} CacheWorkload;

static void bench_read_cache_pattern(CacheWorkload *wl, uint32_t cache_pages) {
    /* Set up a DimBin with mock parameters just for cache testing */
    SlabAllocator alloc;
    slab_allocator_init(&alloc);

    /* We create a minimal DimBin to exercise the cache logic */
    /* Instead, we'll directly operate on ReadCache struct */
    ReadCache rc;
    memset(&rc, 0, sizeof(rc));
    rc.max_pages = cache_pages;
    rc.count = 0;
    uint32_t rc_cap = 64;
    while (rc_cap < cache_pages * 2) rc_cap *= 2;
    rc.capacity = rc_cap;
    rc.slots = (PageSlot *)malloc(rc_cap * sizeof(PageSlot));
    for (uint32_t i = 0; i < rc_cap; i++) {
        rc.slots[i].page_id = PAGE_SLOT_EMPTY;
        rc.slots[i].data = NULL;
    }
    tinylfu_init(&rc.lfu, rc_cap * 2, 1);
    rc.rng_state = 12345;
    rc.hits = 0;
    rc.misses = 0;
    rc.evictions = 0;

    uint64_t admissions_rejected = 0;

    double t0 = now_sec();

    for (int i = 0; i < wl->count; i++) {
        uint32_t page_id = wl->accesses[i];

        /* Check if in cache */
        uint32_t idx = bench_pagemap_find(rc.slots, rc.capacity, page_id);
        if (rc.slots[idx].page_id == page_id) {
            /* Hit */
            tinylfu_access(&rc.lfu, (uint64_t)page_id);
            rc.hits++;
            continue;
        }

        /* Miss */
        rc.misses++;
        tinylfu_access(&rc.lfu, (uint64_t)page_id);

        /* Admission check */
        bool admitted = true;
        if (rc.count >= rc.max_pages) {
            /* Find random victim */
            uint32_t mask = rc.capacity - 1;
            uint32_t victim_idx = UINT32_MAX;
            for (int attempts = 0; attempts < (int)rc.capacity; attempts++) {
                uint32_t x = rc.rng_state;
                x ^= x << 13; x ^= x >> 17; x ^= x << 5;
                rc.rng_state = x;
                uint32_t vi = x & mask;
                if (rc.slots[vi].page_id != PAGE_SLOT_EMPTY) {
                    victim_idx = vi;
                    break;
                }
            }
            if (victim_idx != UINT32_MAX) {
                uint8_t new_freq = tinylfu_estimate(&rc.lfu, (uint64_t)page_id);
                uint8_t vic_freq = tinylfu_estimate(&rc.lfu, (uint64_t)rc.slots[victim_idx].page_id);
                if (new_freq < vic_freq) {
                    admitted = false;
                    admissions_rejected++;
                }
            }
        }

        if (!admitted) continue;

        /* Evict if full */
        if (rc.count >= rc.max_pages) {
            /* Sample 5 slots, evict lowest freq */
            uint32_t best_idx = UINT32_MAX;
            uint8_t best_freq = UINT8_MAX;
            uint32_t mask = rc.capacity - 1;
            int found = 0;
            for (int attempts = 0; attempts < (int)rc.capacity && found < 5; attempts++) {
                uint32_t x = rc.rng_state;
                x ^= x << 13; x ^= x >> 17; x ^= x << 5;
                rc.rng_state = x;
                uint32_t vi = x & mask;
                if (rc.slots[vi].page_id == PAGE_SLOT_EMPTY) continue;
                uint8_t freq = tinylfu_estimate(&rc.lfu, (uint64_t)rc.slots[vi].page_id);
                if (freq < best_freq) {
                    best_freq = freq;
                    best_idx = vi;
                }
                found++;
            }
            if (best_idx != UINT32_MAX) {
                bench_pagemap_remove(rc.slots, rc.capacity, best_idx);
                rc.count--;
                rc.evictions++;
            }
        }

        /* Grow if needed */
        if (rc.count * 10 >= rc.capacity * 7) {
            uint32_t new_cap = rc.capacity * 2;
            PageSlot *new_slots = bench_pagemap_grow(rc.slots, rc.capacity, new_cap);
            if (new_slots) {
                rc.slots = new_slots;
                rc.capacity = new_cap;
            }
        }

        /* Insert */
        idx = bench_pagemap_find(rc.slots, rc.capacity, page_id);
        rc.slots[idx].page_id = page_id;
        rc.slots[idx].data = NULL; /* no actual data needed for simulation */
        rc.count++;
    }

    double elapsed = now_sec() - t0;
    double hit_rate = (double)rc.hits / (double)wl->count * 100.0;

    printf("    %-24s | ops/s: %10.0f | hit: %5.1f%% | miss: %8llu | "
           "evict: %8llu | rejected: %8llu\n",
           wl->name,
           wl->count / elapsed,
           hit_rate,
           (unsigned long long)rc.misses,
           (unsigned long long)rc.evictions,
           (unsigned long long)admissions_rejected);

    free(rc.slots);
    tinylfu_destroy(&rc.lfu);
    slab_allocator_destroy(&alloc);
}

static void bench_read_cache(void) {
    printf("── ReadCache Hit Rate & TinyLFU Admission ──\n\n");

    uint32_t cache_sizes[] = {256, 1024, 4096};
    int num_cache_sizes = sizeof(cache_sizes) / sizeof(cache_sizes[0]);
    int num_accesses = 500000;
    uint32_t universe = 100000; /* 100K distinct pages */

    for (int c = 0; c < num_cache_sizes; c++) {
        printf("  [Cache: %u pages, Universe: %u pages, Accesses: %d]\n",
               cache_sizes[c], universe, num_accesses);

        /* Zipfian (theta=0.99) */
        {
            uint32_t *acc = gen_zipfian(num_accesses, universe, 0.99);
            CacheWorkload wl = { "zipfian (θ=0.99)", acc, num_accesses, universe };
            bench_read_cache_pattern(&wl, cache_sizes[c]);
            free(acc);
        }

        /* Uniform random */
        {
            uint32_t *acc = gen_uniform(num_accesses, universe);
            CacheWorkload wl = { "uniform random", acc, num_accesses, universe };
            bench_read_cache_pattern(&wl, cache_sizes[c]);
            free(acc);
        }

        /* Sequential scan */
        {
            uint32_t *acc = gen_sequential(num_accesses, universe);
            CacheWorkload wl = { "sequential scan", acc, num_accesses, universe };
            bench_read_cache_pattern(&wl, cache_sizes[c]);
            free(acc);
        }

        /* Hotspot + scan interleave (TinyLFU advantage case) */
        {
            uint32_t *acc = gen_hotspot_scan(num_accesses, universe);
            CacheWorkload wl = { "hotspot+scan (70/30)", acc, num_accesses, universe };
            bench_read_cache_pattern(&wl, cache_sizes[c]);
            free(acc);
        }

        printf("\n");
    }
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 4: WriteBuffer Flush Path (Peephole Merge Effectiveness)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static int cmp_u32_fn(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    return (va > vb) - (va < vb);
}

static void bench_write_buffer_flush(void) {
    printf("── WriteBuffer Flush (Peephole Merge) ──\n\n");

    /*
     * Simulate the flush pattern: N dirty pages → sort → count merged runs.
     * We measure:
     *   - Sort time for different N
     *   - Number of I/O runs (merged vs unmerged)
     *   - Effective I/O amplification
     */

    typedef struct {
        const char *name;
        int count;
        uint32_t range;
        int locality; /* 0=random, 1=clustered, 2=sequential */
    } FlushScenario;

    FlushScenario scenarios[] = {
        { "sequential 1K",     1000,   1000,   2 },
        { "sequential 10K",    10000,  10000,  2 },
        { "clustered 1K",      1000,   100000, 1 },
        { "clustered 10K",     10000,  100000, 1 },
        { "random 1K",         1000,   100000, 0 },
        { "random 10K",        10000,  1000000, 0 },
        { "random 50K",        50000,  5000000, 0 },
    };
    int num_scenarios = sizeof(scenarios) / sizeof(scenarios[0]);

    printf("  %-24s | %-8s | %-10s | %-10s | %-14s | %-8s\n",
           "Scenario", "Pages", "Runs(raw)", "Runs(merge)", "Sort time", "Merge %");
    printf("  %-24s-+-%-8s-+-%-10s-+-%-10s-+-%-14s-+-%-8s\n",
           "------------------------", "--------", "----------", "----------",
           "--------------", "--------");

    uint64_t rng = 0xABCDEF0123456789ULL;

    for (int s = 0; s < num_scenarios; s++) {
        FlushScenario *sc = &scenarios[s];
        uint32_t *pages = (uint32_t *)malloc(sc->count * sizeof(uint32_t));

        /* Generate page IDs based on locality pattern */
        switch (sc->locality) {
            case 0: /* random */
                for (int i = 0; i < sc->count; i++) {
                    pages[i] = (uint32_t)(xorshift64(&rng) % sc->range);
                }
                break;
            case 1: /* clustered: 8 clusters */
                {
                    int cluster_size = sc->count / 8;
                    uint32_t cluster_stride = sc->range / 9;
                    for (int i = 0; i < sc->count; i++) {
                        int cluster = i / cluster_size;
                        if (cluster >= 8) cluster = 7;
                        uint32_t base = (cluster + 1) * cluster_stride;
                        pages[i] = base + (uint32_t)(xorshift64(&rng) % 64);
                    }
                }
                break;
            case 2: /* sequential */
                for (int i = 0; i < sc->count; i++) {
                    pages[i] = (uint32_t)i;
                }
                break;
        }

        /* Sort and measure time */
        double t0 = now_sec();
        qsort(pages, sc->count, sizeof(uint32_t), cmp_u32_fn);
        double sort_elapsed = now_sec() - t0;

        /* Count runs without peephole merge */
        int raw_runs = 1;
        for (int i = 1; i < sc->count; i++) {
            if (pages[i] != pages[i - 1] + 1) {
                raw_runs++;
            }
        }

        /* Count runs with peephole merge (gap <= 4) */
        int merged_runs = 1;
        for (int i = 1; i < sc->count; i++) {
            if (pages[i] > pages[i - 1] + 4 + 1) {
                merged_runs++;
            }
        }

        float merge_pct = (1.0f - (float)merged_runs / (float)raw_runs) * 100.0f;
        if (raw_runs == 0) merge_pct = 0;

        printf("  %-24s | %-8d | %-10d | %-10d | %12.5fs | %6.1f%%\n",
               sc->name, sc->count, raw_runs, merged_runs, sort_elapsed, merge_pct);

        free(pages);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 5: End-to-End DimBin Get/Put with Cache Warm-up
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_dimbin_e2e(void) {
    printf("── DimBin End-to-End (put → flush → get with cache) ──\n\n");

    int dims[] = {64, 128, 256};
    int num_dims = sizeof(dims) / sizeof(dims[0]);
    int num_entries = 50000;

    char dir_path[] = "/tmp/graveldb_bench_cache_XXXXXX";
    if (!mkdtemp(dir_path)) {
        fprintf(stderr, "  Failed to create temp dir\n");
        return;
    }

    printf("  %-6s | %-14s | %-14s | %-14s | %-14s | %-10s\n",
           "Dim", "Put ops/s", "Flush time", "SeqGet ops/s", "RandGet ops/s", "Hit Rate");
    printf("  %-6s-+-%-14s-+-%-14s-+-%-14s-+-%-14s-+-%-10s\n",
           "------", "--------------", "--------------", "--------------", "--------------", "----------");

    for (int d = 0; d < num_dims; d++) {
        int dim = dims[d];
        SlabAllocator alloc;
        slab_allocator_init(&alloc);

        char file_path[512];
        snprintf(file_path, sizeof(file_path), "%s/dim%d.emb", dir_path, dim);

        DimBin bin;
        dimbin_init(&bin, dim, file_path, 16 * 1024 * 1024, 16, 4096);
        bin.allocator = &alloc;

        float *emb = (float *)malloc(dim * sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        /* Put phase */
        double t0 = now_sec();
        for (int i = 0; i < num_entries; i++) {
            uint32_t entry_id = dimbin_alloc_entry(&bin);
            emb[0] = (float)i;
            dimbin_put(&bin, entry_id, emb);
        }
        double put_elapsed = now_sec() - t0;

        /* Flush */
        t0 = now_sec();
        dimbin_flush(&bin);
        double flush_elapsed = now_sec() - t0;

        /* Sequential get (after flush, cold cache) */
        float *out = (float *)malloc(dim * sizeof(float));
        t0 = now_sec();
        for (int i = 0; i < num_entries; i++) {
            dimbin_get(&bin, (uint32_t)i, out);
        }
        double seq_get_elapsed = now_sec() - t0;

        /* Random get (now cache is warm from seq scan) */
        uint64_t rng = 0xBEEF1234ULL;
        t0 = now_sec();
        for (int i = 0; i < num_entries; i++) {
            uint32_t entry_id = (uint32_t)(xorshift64(&rng) % num_entries);
            dimbin_get(&bin, entry_id, out);
        }
        double rand_get_elapsed = now_sec() - t0;

        float hit_rate = 0;
        uint64_t total_access = bin.read_cache.hits + bin.read_cache.misses;
        if (total_access > 0) {
            hit_rate = (float)bin.read_cache.hits / (float)total_access * 100.0f;
        }

        printf("  %-6d | %14.0f | %12.4fs | %14.0f | %14.0f | %8.1f%%\n",
               dim,
               num_entries / put_elapsed,
               flush_elapsed,
               num_entries / seq_get_elapsed,
               num_entries / rand_get_elapsed,
               hit_rate);

        free(emb);
        free(out);
        dimbin_destroy(&bin);
        slab_allocator_destroy(&alloc);
    }

    /* Cleanup */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
    system(cmd);

    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 6: TinyLFU vs No-Admission (scan resistance comparison)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_scan_resistance(void) {
    printf("── Scan Resistance: TinyLFU vs LRU (no admission) ──\n\n");

    /*
     * Workload: warm phase (build working set) → scan phase (sequential scan)
     *           → re-access phase (check how much working set survived).
     *
     * With TinyLFU admission: scan pages get rejected (low frequency).
     * Without admission: scan pages evict the working set.
     */

    uint32_t cache_size = 1024;
    uint32_t working_set = 800;  /* 800 pages = 78% of cache */
    uint32_t scan_length = 5000; /* sequential scan over 5K pages */
    int warm_accesses = 10;      /* access each working set page N times */

    printf("  Cache: %u pages, Working set: %u pages, Scan: %u pages\n",
           cache_size, working_set, scan_length);
    printf("  Warm: %d accesses per working-set page\n\n", warm_accesses);

    /* Generate workload */
    int total_ops = (int)working_set * warm_accesses + (int)scan_length + (int)working_set;
    uint32_t *accesses = (uint32_t *)malloc(total_ops * sizeof(uint32_t));
    int idx = 0;

    /* Phase 1: warm the working set */
    for (int rep = 0; rep < warm_accesses; rep++) {
        for (uint32_t p = 0; p < working_set; p++) {
            accesses[idx++] = p;
        }
    }

    /* Phase 2: sequential scan (pages are NOT in working set) */
    for (uint32_t p = 0; p < scan_length; p++) {
        accesses[idx++] = working_set + p; /* distinct from working set */
    }

    /* Phase 3: re-access working set */
    for (uint32_t p = 0; p < working_set; p++) {
        accesses[idx++] = p;
    }

    /* Run with TinyLFU admission */
    {
        CacheWorkload wl = { "with TinyLFU admission", accesses, total_ops, working_set + scan_length };
        printf("  ");
        bench_read_cache_pattern(&wl, cache_size);
    }

    /* Run without admission (simulate LRU: always admit, FIFO evict) */
    {
        /* Simple FIFO cache simulation */
        uint32_t *fifo = (uint32_t *)malloc(cache_size * sizeof(uint32_t));
        uint32_t fifo_head = 0;
        uint32_t fifo_count = 0;
        uint64_t hits = 0, misses = 0;

        double t0 = now_sec();
        for (int i = 0; i < total_ops; i++) {
            uint32_t page_id = accesses[i];
            /* Linear search (fine for benchmark, cache_size is small) */
            bool found = false;
            for (uint32_t j = 0; j < fifo_count; j++) {
                if (fifo[j] == page_id) {
                    found = true;
                    break;
                }
            }
            if (found) {
                hits++;
            } else {
                misses++;
                if (fifo_count < cache_size) {
                    fifo[fifo_count++] = page_id;
                } else {
                    fifo[fifo_head] = page_id;
                    fifo_head = (fifo_head + 1) % cache_size;
                }
            }
        }
        double elapsed = now_sec() - t0;

        printf("    %-24s | ops/s: %10.0f | hit: %5.1f%% | miss: %8llu | "
               "evict: %8s | rejected: %8s\n",
               "FIFO (no admission)",
               total_ops / elapsed,
               (double)hits / total_ops * 100.0,
               (unsigned long long)misses,
               "N/A", "0");

        free(fifo);
    }

    free(accesses);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Cache Subsystem Benchmark\n");
    printf("===================================\n\n");

    bench_tinylfu_throughput();
    bench_page_hashmap();
    bench_read_cache();
    bench_write_buffer_flush();
    bench_dimbin_e2e();
    bench_scan_resistance();

    printf("Benchmark complete.\n");
    return 0;
}
