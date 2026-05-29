/*
 * GravelDB - Cache Subsystem Benchmark
 *
 * Measures performance of:
 *   1. Page hashmap operations: raw insert/lookup/remove/grow
 *   2. WriteBuffer flush path (peephole merge)
 *   3. DimBin end-to-end put/flush/get
 */

#define _XOPEN_SOURCE 700   /* for mkdtemp */
#include "dimbin.h"
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

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: Page Hashmap (raw operations)
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
 * Benchmark 2: WriteBuffer Flush Path (Peephole Merge Effectiveness)
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
 * Benchmark 3: End-to-End DimBin Get/Put
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_dimbin_e2e(void) {
    printf("── DimBin End-to-End (put → flush → get) ──\n\n");

    int dims[] = {64, 128, 256};
    int num_dims = sizeof(dims) / sizeof(dims[0]);
    int num_entries = 50000;

    char dir_path[] = "/tmp/graveldb_bench_cache_XXXXXX";
    if (!mkdtemp(dir_path)) {
        fprintf(stderr, "  Failed to create temp dir\n");
        return;
    }

    printf("  %-6s | %-14s | %-14s | %-14s | %-14s\n",
           "Dim", "Put ops/s", "Flush time", "SeqGet ops/s", "RandGet ops/s");
    printf("  %-6s-+-%-14s-+-%-14s-+-%-14s-+-%-14s\n",
           "------", "--------------", "--------------", "--------------", "--------------");

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

        /* Sequential get */
        float *out = (float *)malloc(dim * sizeof(float));
        t0 = now_sec();
        for (int i = 0; i < num_entries; i++) {
            dimbin_get(&bin, (uint32_t)i, out);
        }
        double seq_get_elapsed = now_sec() - t0;

        /* Random get */
        uint64_t rng = 0xBEEF1234ULL;
        t0 = now_sec();
        for (int i = 0; i < num_entries; i++) {
            uint32_t entry_id = (uint32_t)(xorshift64(&rng) % num_entries);
            dimbin_get(&bin, entry_id, out);
        }
        double rand_get_elapsed = now_sec() - t0;

        printf("  %-6d | %14.0f | %12.4fs | %14.0f | %14.0f\n",
               dim,
               num_entries / put_elapsed,
               flush_elapsed,
               num_entries / seq_get_elapsed,
               num_entries / rand_get_elapsed);

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
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Cache Subsystem Benchmark\n");
    printf("===================================\n\n");

    bench_page_hashmap();
    bench_write_buffer_flush();
    bench_dimbin_e2e();

    printf("Benchmark complete.\n");
    return 0;
}
