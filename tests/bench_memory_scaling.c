/*
 * GravelDB - Memory Scaling Benchmark
 *
 * Measures the speedup ratio as available buffer memory (WriteBuffer)
 * increases relative to total data size. With read cache removed,
 * reads rely on OS page cache; this benchmark now primarily measures
 * the impact of write buffer size on mixed workloads.
 *
 * Test matrix:
 *   - Data sizes: 64MB, 256MB, 1GB (configurable)
 *   - Buffer ratios: 5%, 10%, 25%, 50%, 75%, 100% of data size
 *   - Access patterns: Zipfian (θ=0.99), Uniform, Hotspot (80/20)
 *   - Metrics: ops/s, avg latency (ns), p99 latency
 *
 * The benchmark creates real data on disk and exercises the full
 * dimbin_get path (write_buf forwarding → OS page cache / pread).
 */

#define _XOPEN_SOURCE 700
#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

/* ─────────────────── Config ─────────────────── */

#define BENCH_DIR "/tmp/graveldb_bench_memscale"

/* Default: moderate data size for quick runs. Override with env vars. */
#define DEFAULT_NUM_FEATURES   500000
#define DEFAULT_DIM            128
#define DEFAULT_NUM_QUERIES    200000

/* ─────────────────── Timing ─────────────────── */

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

#ifdef __APPLE__
#include <mach/mach_time.h>
static uint64_t now_ns(void) {
    static mach_timebase_info_data_t tb;
    if (tb.denom == 0) mach_timebase_info(&tb);
    return mach_absolute_time() * tb.numer / tb.denom;
}
#else
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
#endif

/* ─────────────────── RNG ─────────────────── */

static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

/* ─────────────────── Access Pattern Generators ─────────────────── */

/*
 * Zipfian: highly skewed. Top items get most accesses.
 * Uses approximation for speed: inverse-transform with precomputed CDF.
 */
static uint64_t *gen_zipfian_ids(int count, int num_features, double theta, uint64_t seed) {
    uint64_t *ids = (uint64_t *)malloc(count * sizeof(uint64_t));
    uint64_t rng = seed;

    /* Precompute cumulative distribution (truncated for large N) */
    int cdf_size = num_features < 100000 ? num_features : 100000;
    double *cdf = (double *)malloc((cdf_size + 1) * sizeof(double));
    cdf[0] = 0.0;
    for (int i = 1; i <= cdf_size; i++) {
        cdf[i] = cdf[i - 1] + 1.0 / pow((double)i, theta);
    }
    double total = cdf[cdf_size];

    /* Normalize */
    for (int i = 1; i <= cdf_size; i++) {
        cdf[i] /= total;
    }

    for (int i = 0; i < count; i++) {
        double u = (double)(xorshift64(&rng) & 0xFFFFFFFFULL) / (double)0xFFFFFFFFULL;
        /* Binary search in CDF */
        int lo = 1, hi = cdf_size;
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (cdf[mid] < u) lo = mid + 1;
            else hi = mid;
        }
        /* Map rank to feature_id (1-indexed) */
        ids[i] = (uint64_t)lo;
    }

    free(cdf);
    return ids;
}

static uint64_t *gen_uniform_ids(int count, int num_features, uint64_t seed) {
    uint64_t *ids = (uint64_t *)malloc(count * sizeof(uint64_t));
    uint64_t rng = seed;
    for (int i = 0; i < count; i++) {
        ids[i] = (xorshift64(&rng) % num_features) + 1;
    }
    return ids;
}

/*
 * Hotspot 80/20: 80% of accesses hit top 20% of features.
 */
static uint64_t *gen_hotspot_ids(int count, int num_features, uint64_t seed) {
    uint64_t *ids = (uint64_t *)malloc(count * sizeof(uint64_t));
    uint64_t rng = seed;
    int hot_range = num_features / 5;  /* top 20% */
    if (hot_range == 0) hot_range = 1;

    for (int i = 0; i < count; i++) {
        uint64_t r = xorshift64(&rng);
        if ((r % 100) < 80) {
            /* Hot: pick from top 20% */
            ids[i] = (xorshift64(&rng) % hot_range) + 1;
        } else {
            /* Cold: pick from remaining 80% */
            ids[i] = (xorshift64(&rng) % (num_features - hot_range)) + hot_range + 1;
        }
    }
    return ids;
}

/* ─────────────────── Latency Histogram ─────────────────── */

#define HIST_BUCKETS 1024
#define HIST_BUCKET_NS 100  /* 100ns per bucket → covers up to ~100µs */

typedef struct {
    uint64_t buckets[HIST_BUCKETS];
    uint64_t overflow;  /* > HIST_BUCKETS * HIST_BUCKET_NS */
    uint64_t count;
    uint64_t sum_ns;
} LatencyHist;

static void hist_init(LatencyHist *h) {
    memset(h, 0, sizeof(*h));
}

static void hist_record(LatencyHist *h, uint64_t ns) {
    h->count++;
    h->sum_ns += ns;
    uint64_t bucket = ns / HIST_BUCKET_NS;
    if (bucket < HIST_BUCKETS) {
        h->buckets[bucket]++;
    } else {
        h->overflow++;
    }
}

static double hist_avg_ns(const LatencyHist *h) {
    if (h->count == 0) return 0;
    return (double)h->sum_ns / (double)h->count;
}

static uint64_t hist_percentile_ns(const LatencyHist *h, double pct) {
    uint64_t target = (uint64_t)((double)h->count * pct / 100.0);
    uint64_t cum = 0;
    for (int i = 0; i < HIST_BUCKETS; i++) {
        cum += h->buckets[i];
        if (cum >= target) {
            return (uint64_t)(i + 1) * HIST_BUCKET_NS;
        }
    }
    return HIST_BUCKETS * HIST_BUCKET_NS;  /* overflow */
}

/* ─────────────────── Core Benchmark ─────────────────── */

typedef struct {
    double   ops_per_sec;
    double   avg_ns;
    uint64_t p50_ns;
    uint64_t p99_ns;
    float    hit_ratio;
    uint64_t total_hits;
    uint64_t total_misses;
} BenchResult;

static void cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCH_DIR);
    (void)system(cmd);
}

/*
 * Run a single configuration:
 *   - Populate num_features embeddings of given dim
 *   - Flush to disk (clear write buffer)
 *   - Drop read cache (reopen with specified buffer_size)
 *   - Run num_queries GET operations with given access pattern
 *   - Measure throughput, latency, cache hit ratio
 */
static BenchResult run_bench(int num_features, int dim, size_t buffer_size,
                             uint64_t *query_ids, int num_queries) {
    BenchResult result = {0};
    cleanup();

    /* Phase 1: Populate data */
    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 256 * 1024 * 1024; /* large buffer for fast population */
    config.index_capacity = (uint32_t)(num_features * 2);
    config.auto_create_bins = false;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "  Failed to open DB for population: %d\n", rc);
        return result;
    }

    /* Batch populate */
    float *emb = (float *)malloc((size_t)dim * sizeof(float));
    for (int i = 1; i <= num_features; i++) {
        for (int j = 0; j < dim; j++) {
            emb[j] = (float)(i * 1000 + j) * 0.001f;
        }
        graveldb_put(db, NULL, (uint64_t)i, dim, emb);
    }
    graveldb_flush(db);
    graveldb_close(db);

    /* Phase 2: Reopen with target buffer_size (controls WriteBuffer size) */
    config.buffer_size = buffer_size;
    rc = graveldb_open(&db, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "  Failed to reopen DB: %d\n", rc);
        free(emb);
        return result;
    }

    /* Phase 3: Run queries with latency measurement */
    float *out = (float *)malloc((size_t)dim * sizeof(float));
    int out_dim;
    LatencyHist hist;
    hist_init(&hist);

    /* Warm-up: run 10% of queries to warm OS page cache */
    int warmup_count = num_queries / 10;
    for (int i = 0; i < warmup_count; i++) {
        graveldb_get(db, NULL, query_ids[i % num_queries], out, &out_dim);
    }

    /* Measured run */
    double t0 = now_sec();
    for (int i = 0; i < num_queries; i++) {
        uint64_t start = now_ns();
        graveldb_get(db, NULL, query_ids[i], out, &out_dim);
        uint64_t elapsed_ns = now_ns() - start;
        hist_record(&hist, elapsed_ns);
    }
    double total_elapsed = now_sec() - t0;

    /* Collect results */
    result.ops_per_sec = (double)num_queries / total_elapsed;
    result.avg_ns = hist_avg_ns(&hist);
    result.p50_ns = hist_percentile_ns(&hist, 50.0);
    result.p99_ns = hist_percentile_ns(&hist, 99.0);

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    result.hit_ratio = stats.cache_hit_ratio;
    result.total_hits = stats.buffer_hits;
    result.total_misses = stats.buffer_misses;

    free(emb);
    free(out);
    graveldb_close(db);

    return result;
}

/* ─────────────────── Report ─────────────────── */

typedef struct {
    const char *name;
    uint64_t   *ids;
} PatternEntry;

static void run_scaling_suite(int num_features, int dim, int num_queries) {
    size_t entry_size = (size_t)dim * sizeof(float);
    /* Round up to 16-byte alignment (default entry_align) */
    entry_size = (entry_size + 15) & ~(size_t)15;
    size_t total_data_bytes = (size_t)num_features * entry_size;
    double total_data_mb = (double)total_data_bytes / (1024.0 * 1024.0);

    printf("═══════════════════════════════════════════════════════════════════\n");
    printf("  Data: %d features × dim=%d = %.1f MB on disk\n",
           num_features, dim, total_data_mb);
    printf("  Entry size: %zu bytes (aligned)\n", entry_size);
    printf("  Queries per run: %d (+ 10%% warmup)\n", num_queries);
    printf("═══════════════════════════════════════════════════════════════════\n\n");

    /* Cache size ratios to test */
    double ratios[] = {0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 1.00};
    int num_ratios = sizeof(ratios) / sizeof(ratios[0]);

    /* Access patterns */
    PatternEntry patterns[] = {
        { "zipfian (θ=0.99)", gen_zipfian_ids(num_queries, num_features, 0.99, 0xCAFEBABE1234ULL) },
        { "uniform random",   gen_uniform_ids(num_queries, num_features, 0xDEADBEEF5678ULL) },
        { "hotspot (80/20)",  gen_hotspot_ids(num_queries, num_features, 0xFEEDFACE9ABCULL) },
    };
    int num_patterns = sizeof(patterns) / sizeof(patterns[0]);

    /* Store baseline (smallest cache) ops/s per pattern for speedup calc */
    double baseline_ops[3] = {0};

    for (int p = 0; p < num_patterns; p++) {
        printf("── Pattern: %s ──\n\n", patterns[p].name);
        printf("  %-10s │ %-10s │ %10s │ %8s │ %8s │ %8s │ %8s │ %7s\n",
               "Cache MB", "Ratio", "ops/s", "avg(ns)", "p50(ns)", "p99(ns)", "hit%", "speedup");
        printf("  ──────────┼────────────┼────────────┼──────────┼──────────┼──────────┼──────────┼────────\n");

        for (int r = 0; r < num_ratios; r++) {
            /* buffer_size controls total page budget for WriteBuffer.
             * Reads rely on OS page cache. */
            size_t buffer_size = (size_t)(total_data_bytes * ratios[r]);
            if (buffer_size < 512 * 1024) buffer_size = 512 * 1024; /* min 512KB */
            double cache_mb = (double)buffer_size / (1024.0 * 1024.0);

            BenchResult res = run_bench(num_features, dim, buffer_size,
                                        patterns[p].ids, num_queries);

            if (r == 0) baseline_ops[p] = res.ops_per_sec;
            double speedup = baseline_ops[p] > 0 ? res.ops_per_sec / baseline_ops[p] : 1.0;

            printf("  %8.1f  │ %8.0f%%  │ %10.0f │ %8.0f │ %8llu │ %8llu │ %7.1f%% │ %6.2fx\n",
                   cache_mb,
                   ratios[r] * 100.0,
                   res.ops_per_sec,
                   res.avg_ns,
                   (unsigned long long)res.p50_ns,
                   (unsigned long long)res.p99_ns,
                   res.hit_ratio * 100.0,
                   speedup);
        }
        printf("\n");
    }

    /* Cleanup pattern data */
    for (int p = 0; p < num_patterns; p++) {
        free(patterns[p].ids);
    }
}

/* ─────────────────── Bonus: Fixed cache, vary data size ─────────────────── */

static void run_data_scaling_suite(int dim, int num_queries) {
    printf("\n══════════════════════════════════════════════════════════════════════\n");
    printf("  Fixed Cache (64MB), Varying Data Size (dim=%d)\n", dim);
    printf("  Shows how performance degrades as data outgrows cache.\n");
    printf("══════════════════════════════════════════════════════════════════════\n\n");

    size_t fixed_buffer = 64 * 1024 * 1024;  /* 64MB total buffer */
    double fixed_mb = (double)fixed_buffer / (1024.0 * 1024.0);

    int feature_counts[] = {50000, 100000, 200000, 500000, 1000000};
    int num_counts = sizeof(feature_counts) / sizeof(feature_counts[0]);

    printf("  %-10s │ %-10s │ %-10s │ %10s │ %8s │ %8s │ %7s\n",
           "Features", "Data MB", "Cache/Data", "ops/s", "avg(ns)", "p99(ns)", "hit%");
    printf("  ──────────┼────────────┼────────────┼────────────┼──────────┼──────────┼────────\n");

    for (int c = 0; c < num_counts; c++) {
        int nf = feature_counts[c];
        size_t entry_size = (size_t)dim * sizeof(float);
        entry_size = (entry_size + 15) & ~(size_t)15;
        double data_mb = (double)nf * entry_size / (1024.0 * 1024.0);
        double cache_ratio = fixed_mb / data_mb;

        /* Use Zipfian for this test (most realistic) */
        uint64_t *ids = gen_zipfian_ids(num_queries, nf, 0.99, 0xABCDEF01ULL);

        BenchResult res = run_bench(nf, dim, fixed_buffer, ids, num_queries);

        printf("  %8d  │ %8.1f   │ %8.1f%%  │ %10.0f │ %8.0f │ %8llu │ %6.1f%%\n",
               nf, data_mb, cache_ratio * 100.0,
               res.ops_per_sec,
               res.avg_ns,
               (unsigned long long)res.p99_ns,
               res.hit_ratio * 100.0);

        free(ids);
    }
    printf("\n");
}

/* ─────────────────── Main ─────────────────── */

int main(int argc, char **argv) {
    printf("GravelDB Memory Scaling Benchmark\n");
    printf("==================================\n\n");

    /* Allow overriding via env or args */
    int num_features = DEFAULT_NUM_FEATURES;
    int dim = DEFAULT_DIM;
    int num_queries = DEFAULT_NUM_QUERIES;

    if (argc >= 2) num_features = atoi(argv[1]);
    if (argc >= 3) dim = atoi(argv[2]);
    if (argc >= 4) num_queries = atoi(argv[3]);

    const char *env_features = getenv("GRAVELDB_BENCH_FEATURES");
    const char *env_dim = getenv("GRAVELDB_BENCH_DIM");
    const char *env_queries = getenv("GRAVELDB_BENCH_QUERIES");
    if (env_features) num_features = atoi(env_features);
    if (env_dim) dim = atoi(env_dim);
    if (env_queries) num_queries = atoi(env_queries);

    printf("Config: features=%d, dim=%d, queries=%d\n", num_features, dim, num_queries);
    printf("  (Override: ./bench-memory-scaling [features] [dim] [queries])\n");
    printf("  (Or env: GRAVELDB_BENCH_FEATURES, GRAVELDB_BENCH_DIM, GRAVELDB_BENCH_QUERIES)\n\n");

    /* Suite 1: Fixed data size, vary cache */
    run_scaling_suite(num_features, dim, num_queries);

    /* Suite 2: Fixed cache, vary data size */
    run_data_scaling_suite(dim, num_queries);

    cleanup();
    printf("Benchmark complete.\n");
    return 0;
}
