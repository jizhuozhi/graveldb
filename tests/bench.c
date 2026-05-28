/*
 * GravelDB - Multi-Dim Benchmark
 *
 * Measures put/get/checkpoint performance across multiple dimensions.
 * Tests: dim = 16, 32, 64, 128, 256, 512
 */

#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define BENCH_DIR "/tmp/graveldb_bench"
#define NUM_FEATURES 100000

/* All dims to benchmark */
static const int BENCH_DIMS[] = {16, 32, 64, 128, 256, 512};
static const int NUM_BENCH_DIMS = sizeof(BENCH_DIMS) / sizeof(BENCH_DIMS[0]);

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

static void cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCH_DIR);
    system(cmd);
}

/* ─────────────────── Per-Dim Benchmark ─────────────────── */

static void bench_single_dim(int dim, int num_features) {
    cleanup();

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 128 * 1024 * 1024; /* 128MB */
    config.index_capacity = num_features * 2;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "  [dim=%d] Failed to open DB: %d\n", dim, rc);
        return;
    }

    size_t entry_bytes = (size_t)dim * sizeof(float);
    float *emb = (float *)malloc(entry_bytes);
    for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

    /* ── Sequential Put ── */
    double put_elapsed;
    {
        double t0 = now_sec();
        for (int i = 1; i <= num_features; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        put_elapsed = now_sec() - t0;
    }

    /* ── Flush ── */
    double flush_elapsed;
    {
        double t0 = now_sec();
        graveldb_flush(db);
        flush_elapsed = now_sec() - t0;
    }

    /* ── Sequential Get ── */
    double seq_get_elapsed;
    {
        float *out = (float *)malloc(entry_bytes);
        int out_dim;
        double t0 = now_sec();
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        }
        seq_get_elapsed = now_sec() - t0;
        free(out);
    }

    /* ── Random Get ── */
    double rand_get_elapsed;
    {
        float *out = (float *)malloc(entry_bytes);
        int out_dim;
        srand(42);
        double t0 = now_sec();
        for (int i = 0; i < num_features; i++) {
            uint64_t feat_id = (uint64_t)(rand() % num_features) + 1;
            graveldb_get(db, NULL, feat_id, out, &out_dim);
        }
        rand_get_elapsed = now_sec() - t0;
        free(out);
    }

    /* ── Checkpoint (10% dirty) ── */
    double ckpt_elapsed;
    {
        int dirty_count = num_features / 10;
        for (int i = 1; i <= dirty_count; i++) {
            emb[0] = (float)(i * 3);
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        double t0 = now_sec();
        graveldb_checkpoint(db);
        ckpt_elapsed = now_sec() - t0;
    }

    /* ── Print Results ── */
    double data_mb = (double)num_features * entry_bytes / (1024.0 * 1024.0);

    printf("  dim=%-4d | entry=%4zuB | put: %7.0f ops/s (%5.1f MB/s) | "
           "seq_get: %8.0f ops/s (%5.1f MB/s) | "
           "rand_get: %8.0f ops/s | "
           "flush: %.3fs | ckpt: %.3fs\n",
           dim, entry_bytes,
           num_features / put_elapsed,
           data_mb / put_elapsed,
           num_features / seq_get_elapsed,
           data_mb / seq_get_elapsed,
           num_features / rand_get_elapsed,
           flush_elapsed,
           ckpt_elapsed);

    free(emb);
    graveldb_close(db);
}

/* ─────────────────── Batch Put Benchmark (3 modes) ─────────────────── */

/*
 * Shuffle an array of uint64_t using Fisher-Yates.
 */
static void shuffle_u64(uint64_t *arr, int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        uint64_t tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

/*
 * Run a single batch-put trial with given feat_ids ordering.
 * Returns elapsed time in seconds.
 */
static double run_batch_put_trial(GravelDBConfig *config,
                                  uint64_t *feat_ids, float **embeddings,
                                  int *dims_arr, int num_features,
                                  int batch_size) {
    cleanup();
    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, config);
    if (rc != GRAVELDB_OK) return -1.0;

    double t0 = now_sec();
    for (int offset = 0; offset < num_features; offset += batch_size) {
        int n = batch_size;
        if (offset + n > num_features) n = num_features - offset;
        graveldb_batch_put(db, NULL, feat_ids + offset, dims_arr + offset,
                          (const float *const *)(embeddings + offset), n);
    }
    double elapsed = now_sec() - t0;

    graveldb_close(db);
    return elapsed;
}

static void bench_batch_put(void) {
    int dim = 128;
    int num = NUM_FEATURES;
    int batch_size = 1000;
    size_t entry_bytes = (size_t)dim * sizeof(float);
    double data_mb = (double)num * entry_bytes / (1024.0 * 1024.0);

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 128 * 1024 * 1024;
    config.index_capacity = num * 2;

    /* Allocate shared data arrays */
    uint64_t *feat_ids = (uint64_t *)malloc(num * sizeof(uint64_t));
    int *dims_arr = (int *)malloc(num * sizeof(int));
    float **embeddings = (float **)malloc(num * sizeof(float *));
    float *emb_data = (float *)malloc(num * entry_bytes);

    for (int i = 0; i < num; i++) {
        dims_arr[i] = dim;
        embeddings[i] = emb_data + (size_t)i * dim;
        for (int j = 0; j < dim; j++) {
            embeddings[i][j] = (float)(i * 1000 + j) * 0.001f;
        }
    }

    printf("\n  Batch Put Key-Coalescing (dim=%d, %d features, %.1f MB, batch=%d):\n",
           dim, num, data_mb, batch_size);
    printf("  %-22s %10s %10s %8s\n", "Mode", "ops/s", "MB/s", "time");
    printf("  %-22s %10s %10s %8s\n", "----", "-----", "----", "----");

    /* ── Single put baseline: contiguous (benefits from OS/mem-controller cache) ── */
    {
        cleanup();
        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        for (int i = 0; i < num; i++) feat_ids[i] = (uint64_t)(i + 1);

        double t0 = now_sec();
        for (int i = 0; i < num; i++) {
            graveldb_put(db, NULL, feat_ids[i], dim, embeddings[i]);
        }
        double elapsed = now_sec() - t0;
        graveldb_close(db);

        printf("  %-22s %10.0f %10.1f %7.3fs\n",
               "single: contiguous",
               num / elapsed, data_mb / elapsed, elapsed);
    }

    /* ── Single put: fully-random (no sequential cache benefit) ── */
    {
        cleanup();
        GravelDB *db = NULL;
        graveldb_open(&db, &config);

        /* Pre-fill to create a large address space, then delete random 10% */
        for (int i = 0; i < num; i++) {
            float *e = emb_data + (size_t)(i % num) * dim;
            graveldb_put(db, NULL, (uint64_t)(i + 1), dim, e);
        }

        srand(9999);
        int random_count = num / 10;
        uint64_t *del_ids = (uint64_t *)malloc(num * sizeof(uint64_t));
        for (int i = 0; i < num; i++) del_ids[i] = (uint64_t)(i + 1);
        shuffle_u64(del_ids, num);
        for (int i = 0; i < random_count; i++) {
            graveldb_delete(db, NULL, del_ids[i]);
        }
        free(del_ids);

        /* Now single-put into the random free slots */
        double t0 = now_sec();
        for (int i = 0; i < random_count; i++) {
            graveldb_put(db, NULL, (uint64_t)(num + i + 1), dim, embeddings[i % num]);
        }
        double elapsed = now_sec() - t0;
        graveldb_close(db);

        double rd_mb = (double)random_count * entry_bytes / (1024.0 * 1024.0);
        printf("  %-22s %10.0f %10.1f %7.3fs  (%d keys)\n",
               "single: fully-random",
               random_count / elapsed, rd_mb / elapsed, elapsed, random_count);
    }

    /*
     * ── Mode 1: Contiguous (best case) ──
     * feat_ids = 1,2,3,...,N — bump alloc produces perfectly sequential entry_ids.
     * All keys within each batch land in the same 1-2 pages → max coalescing.
     */
    {
        for (int i = 0; i < num; i++) feat_ids[i] = (uint64_t)(i + 1);
        double elapsed = run_batch_put_trial(&config, feat_ids, embeddings,
                                             dims_arr, num, batch_size);
        printf("  %-22s %10.0f %10.1f %7.3fs\n",
               "batch: contiguous",
               num / elapsed, data_mb / elapsed, elapsed);
    }

    /*
     * ── Mode 2: Scattered-pages (partial fill) ──
     * feat_ids chosen so that entries land in ~N/8 pages with ~8 keys per page.
     * Simulates a workload where features are semi-clustered (e.g. model shards
     * that got partially evicted and are being re-pushed in batch).
     *
     * Strategy: pre-fill DB with N features to advance the bump pointer,
     * then delete every 64th feature to create free-list holes spread across
     * many pages. Batch-put N/64 new features that will fill those scattered slots.
     */
    {
        cleanup();
        GravelDB *db = NULL;
        graveldb_open(&db, &config);

        /* Pre-fill to advance bump allocator */
        for (int i = 0; i < num; i++) {
            graveldb_put(db, NULL, (uint64_t)(i + 1), dim, embeddings[i % num]);
        }

        /* Delete every 64th → creates free slots spread across pages */
        int scattered_count = 0;
        for (int i = 0; i < num; i += 64) {
            graveldb_delete(db, NULL, (uint64_t)(i + 1));
            feat_ids[scattered_count] = (uint64_t)(num + scattered_count + 1); /* new feat_ids */
            scattered_count++;
        }

        /* Now batch-put into the scattered free slots */
        for (int i = 0; i < scattered_count; i++) dims_arr[i] = dim;

        double t0 = now_sec();
        for (int offset = 0; offset < scattered_count; offset += batch_size) {
            int n = batch_size;
            if (offset + n > scattered_count) n = scattered_count - offset;
            graveldb_batch_put(db, NULL, feat_ids + offset, dims_arr + offset,
                              (const float *const *)(embeddings + offset), n);
        }
        double elapsed = now_sec() - t0;
        graveldb_close(db);

        double sc_mb = (double)scattered_count * entry_bytes / (1024.0 * 1024.0);
        /* 512 keys/page, every 64th = 8 keys per page → density ~8 = above threshold */
        printf("  %-22s %10.0f %10.1f %7.3fs  (%d keys, ~8/page)\n",
               "batch: scattered-pages",
               scattered_count / elapsed, sc_mb / elapsed, elapsed, scattered_count);
    }

    /*
     * ── Mode 3: Fully random (worst case) ──
     * Pre-fill N features, delete a random 10% subset, then batch-put N/10
     * new features. The free-list order is random so entry_ids are fully
     * non-sequential → each key likely lands in a different page → no coalescing.
     */
    {
        cleanup();
        GravelDB *db = NULL;
        graveldb_open(&db, &config);

        /* Pre-fill */
        for (int i = 0; i < num; i++) {
            graveldb_put(db, NULL, (uint64_t)(i + 1), dim, embeddings[i % num]);
        }

        /* Random 10% deletion */
        srand(12345);
        int random_count = num / 10;
        /* Generate random unique indices to delete */
        uint64_t *del_ids = (uint64_t *)malloc(num * sizeof(uint64_t));
        for (int i = 0; i < num; i++) del_ids[i] = (uint64_t)(i + 1);
        shuffle_u64(del_ids, num);

        for (int i = 0; i < random_count; i++) {
            graveldb_delete(db, NULL, del_ids[i]);
            feat_ids[i] = (uint64_t)(num + i + 1); /* new feat_ids */
        }
        free(del_ids);

        for (int i = 0; i < random_count; i++) dims_arr[i] = dim;

        double t0 = now_sec();
        for (int offset = 0; offset < random_count; offset += batch_size) {
            int n = batch_size;
            if (offset + n > random_count) n = random_count - offset;
            graveldb_batch_put(db, NULL, feat_ids + offset, dims_arr + offset,
                              (const float *const *)(embeddings + offset), n);
        }
        double elapsed = now_sec() - t0;
        graveldb_close(db);

        double rd_mb = (double)random_count * entry_bytes / (1024.0 * 1024.0);
        printf("  %-22s %10.0f %10.1f %7.3fs  (%d keys, ~1-2/page)\n",
               "batch: fully-random",
               random_count / elapsed, rd_mb / elapsed, elapsed, random_count);
    }
    
    free(feat_ids);
    free(dims_arr);
    free(embeddings);
    free(emb_data);
}

/* ─────────────────── Multi-Dim Mixed Benchmark ─────────────────── */

static void bench_multi_dim_mixed(void) {
    cleanup();

    int dims[NUM_BENCH_DIMS];
    memcpy(dims, BENCH_DIMS, sizeof(BENCH_DIMS));

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = dims;
    config.num_dims = NUM_BENCH_DIMS;
    config.buffer_size = 256 * 1024 * 1024; /* 256MB */
    config.index_capacity = NUM_FEATURES * 2;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "  Failed to open multi-dim DB: %d\n", rc);
        return;
    }

    /* Allocate max-dim buffer */
    int max_dim = BENCH_DIMS[NUM_BENCH_DIMS - 1];
    float *emb = (float *)malloc((size_t)max_dim * sizeof(float));

    /* ── Mixed Put: features distributed across dims ── */
    double put_elapsed;
    {
        double t0 = now_sec();
        for (int i = 1; i <= NUM_FEATURES; i++) {
            int dim = BENCH_DIMS[i % NUM_BENCH_DIMS];
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 1000 + j) * 0.001f;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        put_elapsed = now_sec() - t0;
    }

    /* ── Flush ── */
    double flush_elapsed;
    {
        double t0 = now_sec();
        graveldb_flush(db);
        flush_elapsed = now_sec() - t0;
    }

    /* ── Mixed Random Get ── */
    double get_elapsed;
    {
        float *out = (float *)malloc((size_t)max_dim * sizeof(float));
        int out_dim;
        srand(123);
        double t0 = now_sec();
        for (int i = 0; i < NUM_FEATURES; i++) {
            uint64_t feat_id = (uint64_t)(rand() % NUM_FEATURES) + 1;
            graveldb_get(db, NULL, feat_id, out, &out_dim);
        }
        get_elapsed = now_sec() - t0;
        free(out);
    }

    /* ── Checkpoint ── */
    double ckpt_elapsed;
    {
        int dirty_count = NUM_FEATURES / 10;
        for (int i = 1; i <= dirty_count; i++) {
            int dim = BENCH_DIMS[i % NUM_BENCH_DIMS];
            emb[0] = (float)(i * 7);
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        double t0 = now_sec();
        graveldb_checkpoint(db);
        ckpt_elapsed = now_sec() - t0;
    }

    /* ── Recovery (close + reopen) ── */
    graveldb_close(db);

    double recovery_elapsed;
    {
        double t0 = now_sec();
        rc = graveldb_open(&db, &config);
        recovery_elapsed = now_sec() - t0;
    }

    if (rc == GRAVELDB_OK) {
        GravelDBStats stats;
        graveldb_stats(db, &stats);

        printf("\n  Mixed-dim results (%d features across %d dims):\n", NUM_FEATURES, NUM_BENCH_DIMS);
        printf("    Put:        %.0f ops/s\n", NUM_FEATURES / put_elapsed);
        printf("    Flush:      %.3f s\n", flush_elapsed);
        printf("    Random Get: %.0f ops/s\n", NUM_FEATURES / get_elapsed);
        printf("    Checkpoint: %.3f s (10%% dirty)\n", ckpt_elapsed);
        printf("    Recovery:   %.3f s (rebuild %llu keys from .keys files)\n",
               recovery_elapsed, (unsigned long long)stats.total_features);

        graveldb_close(db);
    }

    free(emb);
}

/* ─────────────────── Main ─────────────────── */

int main(void) {
    printf("GravelDB Multi-Dim Benchmark\n");
    printf("============================\n");
    printf("Features per dim: %d\n\n", NUM_FEATURES);

    /* Part 1: Individual dim benchmarks */
    printf("── Per-Dim Throughput (%d features each) ──\n\n", NUM_FEATURES);
    for (int i = 0; i < NUM_BENCH_DIMS; i++) {
        bench_single_dim(BENCH_DIMS[i], NUM_FEATURES);
    }

    /* Part 2: Batch put vs single put */
    printf("\n── Batch Put vs Single Put (key coalescing) ──\n");
    bench_batch_put();

    /* Part 3: Mixed multi-dim workload */
    printf("\n── Mixed Multi-Dim Workload ──\n");
    bench_multi_dim_mixed();

    cleanup();
    printf("\nBenchmark complete.\n");
    return 0;
}
