/*
 * GravelDB - Dirty Tracker Benchmark
 *
 * Measures mark/scan performance across:
 *   - Different scales: 1K, 64K, 1M, 16M, 100M pages
 *   - Different distributions: sequential, clustered, uniform random,
 *     ultra-sparse, hotspot
 *
 * Metrics reported:
 *   - mark throughput (ops/s)
 *   - scan throughput (dirty pages scanned / s)
 *   - memory overhead per dirty page (bytes)
 */

#include "dirty_tracker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

/* ─────────────────── Benchmark Scenarios ─────────────────── */

typedef struct {
    const char *name;
    uint32_t    estimated_pages;  /* capacity / address space */
    uint32_t    num_marks;        /* total mark operations */
    uint32_t   *mark_indices;     /* pre-generated page indices to mark */
    uint32_t    expected_dirty;   /* expected unique dirty pages after all marks */
} BenchScenario;

/*
 * Generate sequential indices: 0, 1, 2, ..., n-1
 */
static uint32_t *gen_sequential(uint32_t n, uint32_t base) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    for (uint32_t i = 0; i < n; i++) {
        arr[i] = base + i;
    }
    return arr;
}

/*
 * Generate clustered indices: marks concentrated in a few regions.
 * num_clusters clusters, each cluster_size pages wide.
 * Total marks = n, distributed evenly across clusters.
 */
static uint32_t *gen_clustered(uint32_t n, uint32_t address_space,
                               int num_clusters, uint32_t cluster_size) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint64_t rng = 0xDEADBEEF12345678ULL;

    /* Pick cluster start positions spread across address space */
    uint32_t *cluster_starts = (uint32_t *)malloc(num_clusters * sizeof(uint32_t));
    uint32_t gap = address_space / (num_clusters + 1);
    for (int c = 0; c < num_clusters; c++) {
        cluster_starts[c] = gap * (c + 1);
    }

    uint32_t per_cluster = n / num_clusters;
    for (int c = 0; c < num_clusters; c++) {
        uint32_t start = cluster_starts[c];
        uint32_t count = (c == num_clusters - 1) ? (n - c * per_cluster) : per_cluster;
        for (uint32_t i = 0; i < count; i++) {
            uint32_t offset = (uint32_t)(xorshift64(&rng) % cluster_size);
            arr[c * per_cluster + i] = start + offset;
        }
    }

    free(cluster_starts);
    return arr;
}

/*
 * Generate uniform random indices across the address space.
 */
static uint32_t *gen_uniform_random(uint32_t n, uint32_t address_space) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint64_t rng = 0xCAFEBABE9876ULL;
    for (uint32_t i = 0; i < n; i++) {
        arr[i] = (uint32_t)(xorshift64(&rng) % address_space);
    }
    return arr;
}

/*
 * Generate ultra-sparse marks: very few marks across huge address space.
 * Marks are spread far apart.
 */
static uint32_t *gen_ultra_sparse(uint32_t n, uint32_t address_space) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint32_t stride = address_space / n;
    uint64_t rng = 0xFEEDFACEULL;
    for (uint32_t i = 0; i < n; i++) {
        /* Each mark in its own wide region, with small random jitter */
        uint32_t jitter = (uint32_t)(xorshift64(&rng) % (stride / 4));
        arr[i] = i * stride + jitter;
    }
    return arr;
}

/*
 * Generate hotspot pattern: 90% of marks hit 10% of the address space.
 * Simulates real-world workloads where popular items get updated repeatedly.
 */
static uint32_t *gen_hotspot(uint32_t n, uint32_t address_space) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint64_t rng = 0xABCD1234EF005678ULL;
    uint32_t hot_region = address_space / 10; /* 10% of space */

    for (uint32_t i = 0; i < n; i++) {
        uint64_t r = xorshift64(&rng);
        if ((r % 100) < 90) {
            /* 90% chance: hit the hot region */
            arr[i] = (uint32_t)(xorshift64(&rng) % hot_region);
        } else {
            /* 10% chance: anywhere in the full space */
            arr[i] = (uint32_t)(xorshift64(&rng) % address_space);
        }
    }
    return arr;
}

/*
 * Generate repeated marks: same pages hit multiple times (idempotent test).
 * n_unique unique pages, total n marks with repetitions.
 */
static uint32_t *gen_repeated(uint32_t n, uint32_t n_unique, uint32_t address_space) {
    uint32_t *arr = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint64_t rng = 0x1234ABCD5678EFULL;

    /* First generate unique target pages */
    uint32_t *targets = (uint32_t *)malloc(n_unique * sizeof(uint32_t));
    for (uint32_t i = 0; i < n_unique; i++) {
        targets[i] = (uint32_t)(xorshift64(&rng) % address_space);
    }

    /* Now fill marks by randomly picking from targets */
    for (uint32_t i = 0; i < n; i++) {
        arr[i] = targets[xorshift64(&rng) % n_unique];
    }

    free(targets);
    return arr;
}

/* ─────────────────── Run a Single Scenario ─────────────────── */

static void run_scenario(const BenchScenario *sc) {
    DirtyTrackerConfig cfg = { .estimated_pages = sc->estimated_pages };
    DirtyTracker dt;
    dirty_tracker_init(&dt, &cfg);

    /* ── Mark Phase ── */
    double t0 = now_sec();
    for (uint32_t i = 0; i < sc->num_marks; i++) {
        dirty_tracker_mark(&dt, sc->mark_indices[i]);
    }
    double mark_elapsed = now_sec() - t0;

    /* ── Scan Phase ── */
    uint32_t *scan_buf = (uint32_t *)malloc(sc->estimated_pages * sizeof(uint32_t));
    int max_out = (int)(sc->estimated_pages < 10000000 ? sc->estimated_pages : 10000000);

    t0 = now_sec();
    int dirty_count = dirty_tracker_scan(&dt, scan_buf, max_out);
    double scan_elapsed = now_sec() - t0;

    /* ── Swap + Scan Checkpoint Phase ── */
    dirty_tracker_swap(&dt);

    t0 = now_sec();
    int ckpt_count = dirty_tracker_scan_ckpt(&dt, scan_buf, max_out);
    double ckpt_scan_elapsed = now_sec() - t0;

    /* ── Report ── */
    printf("  %-28s | pages=%-10u marks=%-8u dirty=%-8d | "
           "mark: %9.0f ops/s | scan: %9.0f pages/s (%.4fs) | "
           "ckpt_scan: %.4fs\n",
           sc->name,
           sc->estimated_pages,
           sc->num_marks,
           dirty_count,
           sc->num_marks / mark_elapsed,
           dirty_count / (scan_elapsed > 0 ? scan_elapsed : 1e-9),
           scan_elapsed,
           ckpt_scan_elapsed);

    (void)ckpt_count;
    free(scan_buf);
    dirty_tracker_destroy(&dt);
}

/* ─────────────────── Scale × Distribution Matrix ─────────────────── */

typedef struct {
    const char *scale_name;
    uint32_t    estimated_pages;
    uint32_t    num_marks;      /* base marks for this scale */
} ScaleConfig;

static const ScaleConfig SCALES[] = {
    { "1K",   1024,        500 },
    { "64K",  65536,       5000 },
    { "1M",   1048576,     50000 },
    { "16M",  16777216,    200000 },
    { "100M", 100000000,   500000 },
};
static const int NUM_SCALES = sizeof(SCALES) / sizeof(SCALES[0]);

static void bench_scale_distribution_matrix(void) {
    printf("── Scale × Distribution Matrix ──\n\n");

    for (int s = 0; s < NUM_SCALES; s++) {
        const ScaleConfig *sc = &SCALES[s];
        printf("  [Scale: %s (%u pages, %u marks)]\n", sc->scale_name,
               sc->estimated_pages, sc->num_marks);

        /* 1. Sequential */
        {
            uint32_t *indices = gen_sequential(sc->num_marks, 0);
            BenchScenario bs = {
                .name = "sequential",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sc->num_marks,
                .mark_indices = indices,
                .expected_dirty = sc->num_marks,
            };
            run_scenario(&bs);
            free(indices);
        }

        /* 2. Clustered (8 clusters, each 1024 pages wide) */
        {
            uint32_t cluster_size = sc->estimated_pages < 1024 ? sc->estimated_pages / 8 : 1024;
            uint32_t *indices = gen_clustered(sc->num_marks, sc->estimated_pages, 8, cluster_size);
            BenchScenario bs = {
                .name = "clustered (8 regions)",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sc->num_marks,
                .mark_indices = indices,
                .expected_dirty = 0,
            };
            run_scenario(&bs);
            free(indices);
        }

        /* 3. Uniform random */
        {
            uint32_t *indices = gen_uniform_random(sc->num_marks, sc->estimated_pages);
            BenchScenario bs = {
                .name = "uniform random",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sc->num_marks,
                .mark_indices = indices,
                .expected_dirty = 0,
            };
            run_scenario(&bs);
            free(indices);
        }

        /* 4. Ultra-sparse (very few marks across entire space) */
        {
            uint32_t sparse_marks = sc->num_marks / 10;
            if (sparse_marks < 10) sparse_marks = 10;
            uint32_t *indices = gen_ultra_sparse(sparse_marks, sc->estimated_pages);
            BenchScenario bs = {
                .name = "ultra-sparse",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sparse_marks,
                .mark_indices = indices,
                .expected_dirty = sparse_marks,
            };
            run_scenario(&bs);
            free(indices);
        }

        /* 5. Hotspot (90% marks in 10% region) */
        {
            uint32_t *indices = gen_hotspot(sc->num_marks, sc->estimated_pages);
            BenchScenario bs = {
                .name = "hotspot (90/10)",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sc->num_marks,
                .mark_indices = indices,
                .expected_dirty = 0,
            };
            run_scenario(&bs);
            free(indices);
        }

        /* 6. Repeated (high idempotent rate: 10x marks on 1/10th unique pages) */
        {
            uint32_t n_unique = sc->num_marks / 10;
            if (n_unique < 10) n_unique = 10;
            uint32_t *indices = gen_repeated(sc->num_marks, n_unique, sc->estimated_pages);
            BenchScenario bs = {
                .name = "repeated (10x idempotent)",
                .estimated_pages = sc->estimated_pages,
                .num_marks = sc->num_marks,
                .mark_indices = indices,
                .expected_dirty = 0,
            };
            run_scenario(&bs);
            free(indices);
        }

        printf("\n");
    }
}

/* ─────────────────── Stress: Rapid Mark+Scan Cycles ─────────────────── */

static void bench_mark_scan_cycles(void) {
    printf("── Mark+Scan+Swap Cycles (simulating checkpoint cadence) ──\n\n");

    uint32_t estimated = 1048576; /* 1M pages */
    uint32_t marks_per_cycle = 10000;
    int num_cycles = 20;

    DirtyTrackerConfig cfg = { .estimated_pages = estimated };
    DirtyTracker dt;
    dirty_tracker_init(&dt, &cfg);

    uint64_t rng = 0xBEEFCAFE1234ULL;
    uint32_t *scan_buf = (uint32_t *)malloc(estimated * sizeof(uint32_t));

    printf("  %u pages, %u marks/cycle, %d cycles\n\n", estimated, marks_per_cycle, num_cycles);
    printf("  %-6s | %-12s | %-12s | %-8s | %-10s\n",
           "Cycle", "Mark (ops/s)", "Scan (pg/s)", "Dirty", "Scan time");
    printf("  %-6s-+-%-12s-+-%-12s-+-%-8s-+-%-10s\n",
           "------", "------------", "------------", "--------", "----------");

    for (int cycle = 0; cycle < num_cycles; cycle++) {
        /* Mark phase: clustered around a shifting hotspot */
        uint32_t hot_base = (uint32_t)((uint64_t)cycle * estimated / num_cycles);
        double t0 = now_sec();
        for (uint32_t i = 0; i < marks_per_cycle; i++) {
            uint32_t offset = (uint32_t)(xorshift64(&rng) % 4096);
            dirty_tracker_mark(&dt, hot_base + offset);
        }
        double mark_elapsed = now_sec() - t0;

        /* Scan phase */
        t0 = now_sec();
        int dirty = dirty_tracker_scan(&dt, scan_buf, (int)estimated);
        double scan_elapsed = now_sec() - t0;

        printf("  %-6d | %12.0f | %12.0f | %-8d | %.6fs\n",
               cycle,
               marks_per_cycle / mark_elapsed,
               dirty / (scan_elapsed > 0 ? scan_elapsed : 1e-9),
               dirty,
               scan_elapsed);

        /* Swap (simulate checkpoint) every 5 cycles */
        if ((cycle + 1) % 5 == 0) {
            dirty_tracker_swap(&dt);
            printf("  --- swap (checkpoint) ---\n");
        }
    }

    free(scan_buf);
    dirty_tracker_destroy(&dt);
}

/* ─────────────────── Depth Comparison ─────────────────── */

static void bench_depth_comparison(void) {
    printf("── Auto-Depth Selection Comparison ──\n\n");
    printf("  Different estimated_pages → different internal depths.\n");
    printf("  Same workload (50K random marks) to compare overhead.\n\n");

    uint32_t test_estimates[] = { 1024, 4096, 65536, 262144, 1048576, 16777216, 100000000 };
    int num_tests = sizeof(test_estimates) / sizeof(test_estimates[0]);
    uint32_t num_marks = 50000;

    printf("  %-12s | %-12s | %-12s | %-8s | %-10s\n",
           "Est. Pages", "Mark (ops/s)", "Scan (pg/s)", "Dirty", "Scan time");
    printf("  %-12s-+-%-12s-+-%-12s-+-%-8s-+-%-10s\n",
           "------------", "------------", "------------", "--------", "----------");

    for (int t = 0; t < num_tests; t++) {
        uint32_t est = test_estimates[t];
        uint32_t *indices = gen_uniform_random(num_marks, est);

        DirtyTrackerConfig cfg = { .estimated_pages = est };
        DirtyTracker dt;
        dirty_tracker_init(&dt, &cfg);

        /* Mark */
        double t0 = now_sec();
        for (uint32_t i = 0; i < num_marks; i++) {
            dirty_tracker_mark(&dt, indices[i]);
        }
        double mark_elapsed = now_sec() - t0;

        /* Scan */
        uint32_t *scan_buf = (uint32_t *)malloc(est * sizeof(uint32_t));
        int max_out = (int)(est < 10000000 ? est : 10000000);
        t0 = now_sec();
        int dirty = dirty_tracker_scan(&dt, scan_buf, max_out);
        double scan_elapsed = now_sec() - t0;

        printf("  %-12u | %12.0f | %12.0f | %-8d | %.6fs\n",
               est,
               num_marks / mark_elapsed,
               dirty / (scan_elapsed > 0 ? scan_elapsed : 1e-9),
               dirty,
               scan_elapsed);

        free(scan_buf);
        free(indices);
        dirty_tracker_destroy(&dt);
    }
}

/* ─────────────────── Main ─────────────────── */

int main(void) {
    printf("GravelDB Dirty Tracker Benchmark\n");
    printf("=================================\n\n");

    bench_scale_distribution_matrix();
    printf("\n");
    bench_mark_scan_cycles();
    printf("\n");
    bench_depth_comparison();

    printf("\nBenchmark complete.\n");
    return 0;
}
