/*
 * GravelDB - End-to-End Latency Benchmark
 *
 * Measures latency distribution (p50/p99/p999/max) for:
 *   1. Single put (various dim, hot/cold cache)
 *   2. Single get (cache hit vs miss)
 *   3. Batch put (various batch sizes)
 *   4. Batch get (various batch sizes)
 *   5. Flush under different dirty-page counts
 *   6. Checkpoint (incremental step latency)
 *   7. Recovery (open time with various DB sizes)
 *
 * This complements the throughput-oriented bench.c by revealing
 * tail latency characteristics critical for real-time serving.
 */

#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>

#define BENCH_DIR "/tmp/graveldb_bench_latency"

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

static void cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCH_DIR);
    system(cmd);
}

/* ── Latency collector ── */

typedef struct {
    double *samples;
    int     count;
    int     capacity;
} LatencyCollector;

static void lat_init(LatencyCollector *lc, int capacity) {
    lc->samples = (double *)malloc(capacity * sizeof(double));
    lc->count = 0;
    lc->capacity = capacity;
}

static void lat_destroy(LatencyCollector *lc) {
    free(lc->samples);
    lc->samples = NULL;
    lc->count = 0;
}

static inline void lat_record(LatencyCollector *lc, double elapsed_us) {
    if (lc->count < lc->capacity) {
        lc->samples[lc->count++] = elapsed_us;
    }
}

static int cmp_double(const void *a, const void *b) {
    double va = *(const double *)a;
    double vb = *(const double *)b;
    return (va > vb) - (va < vb);
}

static void lat_report(LatencyCollector *lc, const char *label) {
    if (lc->count == 0) {
        printf("    %-30s | no samples\n", label);
        return;
    }

    qsort(lc->samples, lc->count, sizeof(double), cmp_double);

    double p50 = lc->samples[lc->count / 2];
    double p99 = lc->samples[(int)(lc->count * 0.99)];
    double p999 = lc->samples[(int)(lc->count * 0.999)];
    double max = lc->samples[lc->count - 1];
    double avg = 0;
    for (int i = 0; i < lc->count; i++) avg += lc->samples[i];
    avg /= lc->count;

    printf("    %-30s | avg: %8.1fµs | p50: %8.1fµs | p99: %8.1fµs | p999: %8.1fµs | max: %8.1fµs\n",
           label, avg, p50, p99, p999, max);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: Single Put Latency
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_put_latency(void) {
    printf("── Single Put Latency ──\n\n");

    int dims[] = {64, 128, 256};
    int num_dims = sizeof(dims) / sizeof(dims[0]);
    int num_ops = 100000;

    for (int d = 0; d < num_dims; d++) {
        int dim = dims[d];
        cleanup();

        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 64 * 1024 * 1024;
        config.index_capacity = num_ops * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        float *emb = (float *)malloc(dim * sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        LatencyCollector lc;
        lat_init(&lc, num_ops);

        for (int i = 1; i <= num_ops; i++) {
            emb[0] = (float)i;
            double t0 = now_sec();
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
            double elapsed = (now_sec() - t0) * 1e6; /* microseconds */
            lat_record(&lc, elapsed);
        }

        char label[64];
        snprintf(label, sizeof(label), "put dim=%d (%d ops)", dim, num_ops);
        lat_report(&lc, label);

        lat_destroy(&lc);
        free(emb);
        graveldb_close(db);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 2: Single Get Latency (hit vs miss)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_get_latency(void) {
    printf("── Single Get Latency ──\n\n");

    int dim = 128;
    int num_features = 100000;
    int num_reads = 100000;

    cleanup();
    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 32 * 1024 * 1024;
    config.index_capacity = num_features * 2;

    GravelDB *db = NULL;
    graveldb_open(&db, &config);
    if (!db) return;

    float *emb = (float *)malloc(dim * sizeof(float));
    for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

    /* Pre-fill */
    for (int i = 1; i <= num_features; i++) {
        emb[0] = (float)i;
        graveldb_put(db, NULL, (uint64_t)i, dim, emb);
    }
    graveldb_flush(db);

    float *out = (float *)malloc(dim * sizeof(float));
    int out_dim;

    /* Sequential get (cold start, cache fills up) */
    {
        LatencyCollector lc;
        lat_init(&lc, num_reads);

        for (int i = 1; i <= num_reads; i++) {
            double t0 = now_sec();
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            lat_record(&lc, (now_sec() - t0) * 1e6);
        }
        lat_report(&lc, "get: sequential (cold→warm)");
        lat_destroy(&lc);
    }

    /* Random get (cache warm from sequential pass) */
    {
        LatencyCollector lc;
        lat_init(&lc, num_reads);
        uint64_t rng = 0xBEEF1234ULL;

        for (int i = 0; i < num_reads; i++) {
            uint64_t feat_id = (xorshift64(&rng) % num_features) + 1;
            double t0 = now_sec();
            graveldb_get(db, NULL, feat_id, out, &out_dim);
            lat_record(&lc, (now_sec() - t0) * 1e6);
        }
        lat_report(&lc, "get: random (warm cache)");
        lat_destroy(&lc);
    }

    /* Get from write buffer (put → get without flush) */
    {
        /* Put some new entries */
        for (int i = num_features + 1; i <= num_features + 10000; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }

        LatencyCollector lc;
        lat_init(&lc, 10000);

        for (int i = num_features + 1; i <= num_features + 10000; i++) {
            double t0 = now_sec();
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            lat_record(&lc, (now_sec() - t0) * 1e6);
        }
        lat_report(&lc, "get: write-buffer forward");
        lat_destroy(&lc);
    }

    free(emb);
    free(out);
    graveldb_close(db);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 3: Batch Put Latency (per-batch)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_batch_put_latency(void) {
    printf("── Batch Put Latency ──\n\n");

    int dim = 128;
    int batch_sizes[] = {10, 50, 100, 500, 1000};
    int num_batch_sizes = sizeof(batch_sizes) / sizeof(batch_sizes[0]);
    int total_features = 100000;

    for (int b = 0; b < num_batch_sizes; b++) {
        int batch_size = batch_sizes[b];
        int num_batches = total_features / batch_size;
        cleanup();

        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 64 * 1024 * 1024;
        config.index_capacity = total_features * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        /* Prepare batch data */
        uint64_t *feat_ids = (uint64_t *)malloc(batch_size * sizeof(uint64_t));
        int *dims_arr = (int *)malloc(batch_size * sizeof(int));
        float **embeddings = (float **)malloc(batch_size * sizeof(float *));
        float *emb_data = (float *)malloc(batch_size * dim * sizeof(float));

        for (int i = 0; i < batch_size; i++) {
            dims_arr[i] = dim;
            embeddings[i] = emb_data + (size_t)i * dim;
            for (int j = 0; j < dim; j++) {
                embeddings[i][j] = (float)(i * 1000 + j) * 0.001f;
            }
        }

        LatencyCollector lc;
        lat_init(&lc, num_batches);

        for (int batch = 0; batch < num_batches; batch++) {
            for (int i = 0; i < batch_size; i++) {
                feat_ids[i] = (uint64_t)(batch * batch_size + i + 1);
            }

            double t0 = now_sec();
            graveldb_batch_put(db, NULL, feat_ids, dims_arr,
                              (const float *const *)embeddings, batch_size);
            lat_record(&lc, (now_sec() - t0) * 1e6);
        }

        char label[64];
        snprintf(label, sizeof(label), "batch_put (batch=%d)", batch_size);
        lat_report(&lc, label);

        lat_destroy(&lc);
        free(feat_ids);
        free(dims_arr);
        free(embeddings);
        free(emb_data);
        graveldb_close(db);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 4: Flush Latency vs Dirty Page Count
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_flush_latency(void) {
    printf("── Flush Latency vs Dirty Pages ──\n\n");

    int dim = 128;
    int dirty_counts[] = {100, 500, 1000, 5000, 10000, 50000};
    int num_dirty = sizeof(dirty_counts) / sizeof(dirty_counts[0]);
    int num_trials = 5;

    printf("  %-12s | %-12s | %-12s | %-12s | %-12s\n",
           "Dirty Pages", "Avg ms", "Min ms", "Max ms", "MB/s");
    printf("  %-12s-+-%-12s-+-%-12s-+-%-12s-+-%-12s\n",
           "------------", "------------", "------------", "------------", "------------");

    for (int d = 0; d < num_dirty; d++) {
        int dirty = dirty_counts[d];
        double times[5] = {0};

        for (int trial = 0; trial < num_trials; trial++) {
            cleanup();

            GravelDBConfig config = {0};
            config.data_dir = BENCH_DIR;
            config.dims = &dim;
            config.num_dims = 1;
            config.buffer_size = 256 * 1024 * 1024; /* large enough to hold all dirty */
            config.index_capacity = dirty * 2;

            GravelDB *db = NULL;
            graveldb_open(&db, &config);
            if (!db) continue;

            float *emb = (float *)malloc(dim * sizeof(float));
            for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

            /* Write `dirty` entries (each roughly one unique page) */
            for (int i = 1; i <= dirty; i++) {
                emb[0] = (float)i;
                graveldb_put(db, NULL, (uint64_t)i, dim, emb);
            }

            double t0 = now_sec();
            graveldb_flush(db);
            times[trial] = (now_sec() - t0) * 1000.0; /* ms */

            free(emb);
            graveldb_close(db);
        }

        double avg = 0, mn = times[0], mx = times[0];
        for (int t = 0; t < num_trials; t++) {
            avg += times[t];
            if (times[t] < mn) mn = times[t];
            if (times[t] > mx) mx = times[t];
        }
        avg /= num_trials;

        double data_mb = (double)dirty * 4096.0 / (1024.0 * 1024.0);
        double mbps = data_mb / (avg / 1000.0);

        printf("  %-12d | %10.2f | %10.2f | %10.2f | %10.1f\n",
               dirty, avg, mn, mx, mbps);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 5: Checkpoint Latency (Full + Step)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_checkpoint_latency(void) {
    printf("── Checkpoint Latency ──\n\n");

    int dim = 128;
    int num_features = 100000;
    int dirty_pcts[] = {1, 5, 10, 25, 50};
    int num_pcts = sizeof(dirty_pcts) / sizeof(dirty_pcts[0]);

    printf("  %-10s | %-12s | %-14s\n",
           "Dirty %", "Full ckpt ms", "Data MB");
    printf("  %-10s-+-%-12s-+-%-14s\n",
           "----------", "------------", "--------------");

    for (int p = 0; p < num_pcts; p++) {
        int dirty_pct = dirty_pcts[p];
        cleanup();

        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 128 * 1024 * 1024;
        config.index_capacity = num_features * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        float *emb = (float *)malloc(dim * sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        /* Pre-fill and flush (establish baseline) */
        for (int i = 1; i <= num_features; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        graveldb_flush(db);

        /* Dirty some percentage */
        int dirty_count = num_features * dirty_pct / 100;
        for (int i = 1; i <= dirty_count; i++) {
            emb[0] = (float)(i * 7);
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }

        /* Time full checkpoint */
        double t0 = now_sec();
        graveldb_checkpoint(db);
        double ckpt_elapsed = (now_sec() - t0) * 1000.0;

        double data_mb = (double)dirty_count * dim * sizeof(float) / (1024.0 * 1024.0);
        printf("  %-10d | %10.2f | %12.2f\n", dirty_pct, ckpt_elapsed, data_mb);

        free(emb);
        graveldb_close(db);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 6: Recovery (Open) Latency
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_recovery_latency(void) {
    printf("── Recovery (Open) Latency ──\n\n");

    int dim = 128;
    int sizes[] = {10000, 50000, 100000, 500000};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    printf("  %-10s | %-14s | %-14s\n",
           "Features", "Recovery ms", "Keys/s");
    printf("  %-10s-+-%-14s-+-%-14s\n",
           "----------", "--------------", "--------------");

    for (int s = 0; s < num_sizes; s++) {
        int n = sizes[s];
        cleanup();

        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 64 * 1024 * 1024;
        config.index_capacity = n * 2;

        /* Create and populate */
        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        float *emb = (float *)malloc(dim * sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        for (int i = 1; i <= n; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        graveldb_flush(db);
        graveldb_close(db);

        /* Measure recovery time */
        double t0 = now_sec();
        graveldb_open(&db, &config);
        double recovery_ms = (now_sec() - t0) * 1000.0;

        double keys_per_sec = n / (recovery_ms / 1000.0);

        printf("  %-10d | %12.2f | %14.0f\n", n, recovery_ms, keys_per_sec);

        free(emb);
        graveldb_close(db);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 7: Delete + GC Latency
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_delete_gc_latency(void) {
    printf("── Delete + GC Latency ──\n\n");

    int dim = 128;
    int num_features = 100000;
    int delete_pcts[] = {1, 5, 10, 25};
    int num_pcts = sizeof(delete_pcts) / sizeof(delete_pcts[0]);

    printf("  %-10s | %-14s | %-14s | %-14s\n",
           "Delete %", "Delete ops/s", "Avg del µs", "GC time ms");
    printf("  %-10s-+-%-14s-+-%-14s-+-%-14s\n",
           "----------", "--------------", "--------------", "--------------");

    for (int p = 0; p < num_pcts; p++) {
        int del_pct = delete_pcts[p];
        cleanup();

        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 64 * 1024 * 1024;
        config.index_capacity = num_features * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        float *emb = (float *)malloc(dim * sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        /* Pre-fill */
        for (int i = 1; i <= num_features; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        graveldb_flush(db);

        /* Delete */
        int del_count = num_features * del_pct / 100;
        uint64_t rng = 0xFEED9876ULL;

        LatencyCollector lc;
        lat_init(&lc, del_count);

        for (int i = 0; i < del_count; i++) {
            uint64_t feat_id = (xorshift64(&rng) % num_features) + 1;
            double t0 = now_sec();
            graveldb_delete(db, NULL, feat_id);
            lat_record(&lc, (now_sec() - t0) * 1e6);
        }

        double avg_del = 0;
        for (int i = 0; i < lc.count; i++) avg_del += lc.samples[i];
        if (lc.count > 0) avg_del /= lc.count;

        printf("  %-10d | %14.0f | %12.1f\n",
               del_pct,
               del_count / (avg_del * del_count / 1e6),
               avg_del);

        lat_destroy(&lc);
        free(emb);
        graveldb_close(db);
    }
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Latency Benchmark\n");
    printf("===========================\n\n");

    bench_put_latency();
    bench_get_latency();
    bench_batch_put_latency();
    bench_flush_latency();
    bench_checkpoint_latency();
    bench_recovery_latency();
    bench_delete_gc_latency();

    cleanup();
    printf("Benchmark complete.\n");
    return 0;
}
