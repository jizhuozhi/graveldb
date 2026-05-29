/*
 * GravelDB - Network (Client/Server) Benchmark
 *
 * Starts a real GravelServer in a background thread, connects via TCP
 * with the C client SDK, and measures end-to-end throughput & latency
 * across the actual network stack (loopback).
 *
 * Benchmarks:
 *   1. Ping round-trip latency
 *   2. Single push / pull latency
 *   3. Batch push throughput (varying batch sizes)
 *   4. Batch pull throughput (varying batch sizes)
 *   5. Mixed read/write workload
 *
 * Build:
 *   Linked against graveldb_lib, graveldb_client, and server objects.
 */

#include "server.h"
#include "client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <pthread.h>

/* ─────────────────── Configuration ─────────────────── */

#define BENCH_DIR       "/tmp/graveldb_bench_network"
#define BENCH_PORT      19527   /* Use non-standard port to avoid conflicts */
#define DIM             128
#define NUM_FEATURES    50000
#define WARMUP_ITERS    50

/* Default sample counts (override with env BENCH_SAMPLES) */
#define DEFAULT_LATENCY_SAMPLES 2000
#define DEFAULT_MIXED_OPS       10000

/* Resolved at runtime */
static int LATENCY_SAMPLES;
static int MIXED_OPS;

/* Batch sizes to test */
static const int BATCH_SIZES[] = {1, 8, 32, 128, 512, 2048, 8192};
static const int NUM_BATCH_SIZES = sizeof(BATCH_SIZES) / sizeof(BATCH_SIZES[0]);

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
    int     cap;
} LatencyCollector;

static void lat_init(LatencyCollector *lc, int cap) {
    lc->samples = (double *)malloc(cap * sizeof(double));
    lc->count = 0;
    lc->cap = cap;
}

static void lat_record(LatencyCollector *lc, double us) {
    if (lc->count < lc->cap) {
        lc->samples[lc->count++] = us;
    }
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static void lat_report(LatencyCollector *lc, const char *label) {
    if (lc->count == 0) return;
    qsort(lc->samples, lc->count, sizeof(double), cmp_double);

    double sum = 0;
    for (int i = 0; i < lc->count; i++) sum += lc->samples[i];
    double avg = sum / lc->count;

    int p50  = (int)(lc->count * 0.50);
    int p90  = (int)(lc->count * 0.90);
    int p99  = (int)(lc->count * 0.99);
    int p999 = (int)(lc->count * 0.999);
    if (p999 >= lc->count) p999 = lc->count - 1;

    printf("  %-30s  avg=%7.1f  p50=%7.1f  p90=%7.1f  p99=%7.1f  p999=%7.1f  max=%7.1f us\n",
           label,
           avg,
           lc->samples[p50],
           lc->samples[p90],
           lc->samples[p99],
           lc->samples[p999],
           lc->samples[lc->count - 1]);
}

static void lat_free(LatencyCollector *lc) {
    free(lc->samples);
    lc->samples = NULL;
    lc->count = 0;
}

/* ─────────────────── Server Setup ─────────────────── */

static GravelServer *g_server = NULL;

static int start_server(void) {
    cleanup();

    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    int dims[] = {DIM};
    config.db_config.data_dir = BENCH_DIR;
    config.db_config.dims = dims;
    config.db_config.num_dims = 1;
    config.db_config.buffer_size = 256 * 1024 * 1024; /* 256MB */
    config.db_config.index_capacity = NUM_FEATURES * 4;

    config.port = BENCH_PORT;
    config.num_workers = 0;
    config.backlog = 64;
    config.max_request_size = 64 * 1024 * 1024;
    config.auto_flush_interval_ms = 5000;     /* Don't auto-flush during bench */
    config.auto_checkpoint_interval_s = 3600; /* Don't auto-checkpoint */

    graveldb_status_t rc = gravel_server_create(&g_server, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[bench_network] Failed to create server: %d\n", rc);
        return -1;
    }

    /* Retry start in case of transient port conflict (TIME_WAIT) */
    for (int attempt = 0; attempt < 5; attempt++) {
        rc = gravel_server_start(g_server);
        if (rc == GRAVELDB_OK) break;
        fprintf(stderr, "[bench_network] Port %d busy, retrying in 1s... (attempt %d/5)\n",
                BENCH_PORT, attempt + 1);
        sleep(1);
    }
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[bench_network] Failed to start server after retries: %d\n", rc);
        gravel_server_destroy(g_server);
        g_server = NULL;
        return -1;
    }

    /* Give server thread time to bind and listen */
    usleep(100000); /* 100ms */
    return 0;
}

static void stop_server(void) {
    if (g_server) {
        gravel_server_stop(g_server);
        gravel_server_destroy(g_server);
        g_server = NULL;
    }
}

/* ─────────────────── Benchmarks ─────────────────── */

static void bench_ping(GravelDBClient *client) {
    printf("\n── Ping Latency ──\n");
    printf("  [warmup] %d iterations...\n", WARMUP_ITERS);
    fflush(stdout);

    /* Warmup */
    for (int i = 0; i < WARMUP_ITERS; i++) {
        graveldb_client_ping(client);
    }

    printf("  [measure] %d samples...", LATENCY_SAMPLES);
    fflush(stdout);

    LatencyCollector lc;
    lat_init(&lc, LATENCY_SAMPLES);

    for (int i = 0; i < LATENCY_SAMPLES; i++) {
        double t0 = now_sec();
        graveldb_client_ping(client);
        double elapsed = (now_sec() - t0) * 1e6; /* to microseconds */
        lat_record(&lc, elapsed);

        if ((i + 1) % (LATENCY_SAMPLES / 5) == 0) {
            printf(" %d%%", (i + 1) * 100 / LATENCY_SAMPLES);
            fflush(stdout);
        }
    }
    printf(" done.\n");

    lat_report(&lc, "ping (RTT)");
    lat_free(&lc);
}

static void bench_single_push_pull(GravelDBClient *client) {
    printf("\n── Single Push/Pull Latency (dim=%d) ──\n", DIM);

    float *emb = (float *)malloc(DIM * sizeof(float));
    for (int i = 0; i < DIM; i++) emb[i] = (float)i * 0.001f;

    float *out = (float *)malloc(DIM * sizeof(float));

    /* Warmup push */
    printf("  [warmup] %d push iterations...\n", WARMUP_ITERS);
    fflush(stdout);
    for (int i = 0; i < WARMUP_ITERS; i++) {
        uint64_t fid = (uint64_t)(i + 1);
        int dim = DIM;
        const float *emb_ptr = emb;
        graveldb_client_push(client, &fid, &dim, &emb_ptr, 1);
    }

    /* Push latency */
    printf("  [measure] %d push samples...", LATENCY_SAMPLES);
    fflush(stdout);

    LatencyCollector lc_push;
    lat_init(&lc_push, LATENCY_SAMPLES);

    for (int i = 0; i < LATENCY_SAMPLES; i++) {
        uint64_t fid = (uint64_t)(WARMUP_ITERS + i + 1);
        int dim = DIM;
        const float *emb_ptr = emb;

        double t0 = now_sec();
        graveldb_client_push(client, &fid, &dim, &emb_ptr, 1);
        double elapsed = (now_sec() - t0) * 1e6;
        lat_record(&lc_push, elapsed);

        if ((i + 1) % (LATENCY_SAMPLES / 5) == 0) {
            printf(" %d%%", (i + 1) * 100 / LATENCY_SAMPLES);
            fflush(stdout);
        }
    }
    printf(" done.\n");

    lat_report(&lc_push, "single push");
    lat_free(&lc_push);

    /* Pull latency (keys exist from pushes above) */
    printf("  [measure] %d pull samples...", LATENCY_SAMPLES);
    fflush(stdout);

    LatencyCollector lc_pull;
    lat_init(&lc_pull, LATENCY_SAMPLES);

    for (int i = 0; i < LATENCY_SAMPLES; i++) {
        uint64_t fid = (uint64_t)(i % (WARMUP_ITERS + LATENCY_SAMPLES) + 1);
        int out_dim = 0;
        float *out_ptr = out;

        double t0 = now_sec();
        graveldb_client_pull(client, &fid, 1, &out_ptr, &out_dim);
        double elapsed = (now_sec() - t0) * 1e6;
        lat_record(&lc_pull, elapsed);

        if ((i + 1) % (LATENCY_SAMPLES / 5) == 0) {
            printf(" %d%%", (i + 1) * 100 / LATENCY_SAMPLES);
            fflush(stdout);
        }
    }
    printf(" done.\n");

    lat_report(&lc_pull, "single pull (cache hit)");
    lat_free(&lc_pull);

    free(emb);
    free(out);
}

static void bench_batch_push(GravelDBClient *client) {
    printf("\n── Batch Push Throughput (dim=%d) ──\n", DIM);
    printf("  %-12s  %12s  %12s  %12s\n", "batch_size", "ops/sec", "features/sec", "MB/sec");
    printf("  %-12s  %12s  %12s  %12s\n", "----------", "-------", "------------", "------");

    for (int bi = 0; bi < NUM_BATCH_SIZES; bi++) {
        int batch = BATCH_SIZES[bi];
        if (batch > GRAVELDB_WIRE_MAX_BATCH) break;

        fprintf(stderr, "  [bench_batch_push] batch=%d ...\r", batch);

        /* Prepare batch data */
        uint64_t *feat_ids = (uint64_t *)malloc(batch * sizeof(uint64_t));
        int *dims = (int *)malloc(batch * sizeof(int));
        float **embeddings = (float **)malloc(batch * sizeof(float *));
        float *emb_data = (float *)malloc(batch * DIM * sizeof(float));

        for (int i = 0; i < batch; i++) {
            feat_ids[i] = (uint64_t)(i + 1);
            dims[i] = DIM;
            embeddings[i] = emb_data + i * DIM;
            for (int j = 0; j < DIM; j++) {
                embeddings[i][j] = (float)(i * DIM + j) * 0.001f;
            }
        }

        /* Warmup */
        for (int i = 0; i < 5; i++) {
            graveldb_client_push(client, feat_ids, dims, (const float *const *)embeddings, batch);
        }

        /* Measure */
        int iters = (batch <= 128) ? 1000 : (batch <= 2048) ? 200 : 50;
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            /* Shift feat_ids to avoid same-key optimization */
            for (int j = 0; j < batch; j++) {
                feat_ids[j] = (uint64_t)(i * batch + j + 1);
            }
            graveldb_client_push(client, feat_ids, dims, (const float *const *)embeddings, batch);
        }
        double elapsed = now_sec() - t0;

        double ops_sec = iters / elapsed;
        double feats_sec = (double)iters * batch / elapsed;
        double mb_sec = feats_sec * DIM * sizeof(float) / (1024.0 * 1024.0);

        printf("  %-12d  %12.0f  %12.0f  %12.1f\n", batch, ops_sec, feats_sec, mb_sec);

        free(feat_ids);
        free(dims);
        free(embeddings);
        free(emb_data);
    }
}

static void bench_batch_pull(GravelDBClient *client) {
    printf("\n── Batch Pull Throughput (dim=%d) ──\n", DIM);

    /* Pre-populate data */
    int prepop = NUM_FEATURES;
    int prepop_batch = 1024;
    {
        uint64_t *fids = (uint64_t *)malloc(prepop_batch * sizeof(uint64_t));
        int *dims = (int *)malloc(prepop_batch * sizeof(int));
        float **embs = (float **)malloc(prepop_batch * sizeof(float *));
        float *data = (float *)malloc(prepop_batch * DIM * sizeof(float));

        for (int i = 0; i < prepop_batch; i++) {
            dims[i] = DIM;
            embs[i] = data + i * DIM;
            for (int j = 0; j < DIM; j++) embs[i][j] = 0.01f;
        }

        printf("  Pre-populating %d features...", prepop);
        fflush(stdout);
        for (int base = 0; base < prepop; base += prepop_batch) {
            int n = (prepop - base < prepop_batch) ? (prepop - base) : prepop_batch;
            for (int i = 0; i < n; i++) fids[i] = (uint64_t)(base + i + 1);
            graveldb_client_push(client, fids, dims, (const float *const *)embs, n);
        }
        printf(" done.\n");

        free(fids);
        free(dims);
        free(embs);
        free(data);
    }

    printf("  %-12s  %12s  %12s  %12s\n", "batch_size", "ops/sec", "features/sec", "MB/sec");
    printf("  %-12s  %12s  %12s  %12s\n", "----------", "-------", "------------", "------");

    uint64_t rng_state = 12345;

    for (int bi = 0; bi < NUM_BATCH_SIZES; bi++) {
        int batch = BATCH_SIZES[bi];
        if (batch > GRAVELDB_WIRE_MAX_BATCH) break;

        /* Allocate output buffers */
        uint64_t *feat_ids = (uint64_t *)malloc(batch * sizeof(uint64_t));
        float **out_embs = (float **)malloc(batch * sizeof(float *));
        float *out_data = (float *)malloc(batch * DIM * sizeof(float));
        int *out_dims = (int *)malloc(batch * sizeof(int));

        for (int i = 0; i < batch; i++) {
            out_embs[i] = out_data + i * DIM;
        }

        /* Warmup */
        for (int w = 0; w < 5; w++) {
            for (int i = 0; i < batch; i++) {
                feat_ids[i] = (xorshift64(&rng_state) % prepop) + 1;
            }
            graveldb_client_pull(client, feat_ids, batch, out_embs, out_dims);
        }

        fprintf(stderr, "  [bench_batch_pull] batch=%d, iters=...\r", batch);

        /* Measure */
        int iters = (batch <= 128) ? 1000 : (batch <= 2048) ? 200 : 50;
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            for (int j = 0; j < batch; j++) {
                feat_ids[j] = (xorshift64(&rng_state) % prepop) + 1;
            }
            graveldb_client_pull(client, feat_ids, batch, out_embs, out_dims);
        }
        double elapsed = now_sec() - t0;

        double ops_sec = iters / elapsed;
        double feats_sec = (double)iters * batch / elapsed;
        double mb_sec = feats_sec * DIM * sizeof(float) / (1024.0 * 1024.0);

        printf("  %-12d  %12.0f  %12.0f  %12.1f\n", batch, ops_sec, feats_sec, mb_sec);

        free(feat_ids);
        free(out_embs);
        free(out_data);
        free(out_dims);
    }
}

static void bench_mixed_workload(GravelDBClient *client) {
    printf("\n── Mixed Read/Write Workload (80%% pull / 20%% push, dim=%d) ──\n", DIM);

    int total_ops = MIXED_OPS;
    int batch = 32;
    uint64_t rng_state = 99999;

    /* Allocate buffers */
    uint64_t *feat_ids = (uint64_t *)malloc(batch * sizeof(uint64_t));
    int *dims = (int *)malloc(batch * sizeof(int));
    float **embeddings = (float **)malloc(batch * sizeof(float *));
    float *emb_data = (float *)malloc(batch * DIM * sizeof(float));
    float **out_embs = (float **)malloc(batch * sizeof(float *));
    float *out_data = (float *)malloc(batch * DIM * sizeof(float));
    int *out_dims = (int *)malloc(batch * sizeof(int));

    for (int i = 0; i < batch; i++) {
        dims[i] = DIM;
        embeddings[i] = emb_data + i * DIM;
        out_embs[i] = out_data + i * DIM;
        for (int j = 0; j < DIM; j++) embeddings[i][j] = 0.01f;
    }

    /* Warmup */
    for (int i = 0; i < 50; i++) {
        for (int j = 0; j < batch; j++) feat_ids[j] = (xorshift64(&rng_state) % NUM_FEATURES) + 1;
        if (i % 5 == 0) {
            graveldb_client_push(client, feat_ids, dims, (const float *const *)embeddings, batch);
        } else {
            graveldb_client_pull(client, feat_ids, batch, out_embs, out_dims);
        }
    }

    /* Measure */
    printf("  [running] %d ops (batch=%d each)...", total_ops, batch);
    fflush(stdout);
    int pull_count = 0, push_count = 0;
    double t0 = now_sec();
    for (int i = 0; i < total_ops; i++) {
        for (int j = 0; j < batch; j++) {
            feat_ids[j] = (xorshift64(&rng_state) % NUM_FEATURES) + 1;
        }
        if ((xorshift64(&rng_state) % 100) < 80) {
            graveldb_client_pull(client, feat_ids, batch, out_embs, out_dims);
            pull_count++;
        } else {
            graveldb_client_push(client, feat_ids, dims, (const float *const *)embeddings, batch);
            push_count++;
        }
        if ((i + 1) % (total_ops / 5) == 0) {
            printf(" %d%%", (i + 1) * 100 / total_ops);
            fflush(stdout);
        }
    }
    printf(" done.\n");
    double elapsed = now_sec() - t0;

    double ops_sec = total_ops / elapsed;
    double feats_sec = (double)total_ops * batch / elapsed;
    double mb_sec = feats_sec * DIM * sizeof(float) / (1024.0 * 1024.0);

    printf("  Total ops:     %d (pulls=%d, pushes=%d)\n", total_ops, pull_count, push_count);
    printf("  Elapsed:       %.2f s\n", elapsed);
    printf("  Ops/sec:       %.0f  (batch=%d each)\n", ops_sec, batch);
    printf("  Features/sec:  %.0f\n", feats_sec);
    printf("  Throughput:    %.1f MB/s\n", mb_sec);

    free(feat_ids);
    free(dims);
    free(embeddings);
    free(emb_data);
    free(out_embs);
    free(out_data);
    free(out_dims);
}

/* ── Multi-client thread types ── */

typedef struct {
    int         iters;
    int         batch;
    double      elapsed;
} MCThreadResult;

typedef struct {
    int             iters;
    int             batch;
    MCThreadResult *result;
    int             thread_id;
} MCThreadArg;

static void *mc_client_thread_fn(void *arg) {
    MCThreadArg *ta = (MCThreadArg *)arg;
    int iters = ta->iters;
    int batch_sz = ta->batch;

    GravelDBClient *c = NULL;
    if (graveldb_client_connect(&c, "127.0.0.1", BENCH_PORT) != 0) {
        ta->result->elapsed = -1;
        return NULL;
    }

    /* Allocate pull buffers */
    uint64_t *fids = (uint64_t *)malloc(batch_sz * sizeof(uint64_t));
    float **outs = (float **)malloc(batch_sz * sizeof(float *));
    float *data = (float *)malloc(batch_sz * DIM * sizeof(float));
    int *odims = (int *)malloc(batch_sz * sizeof(int));
    for (int i = 0; i < batch_sz; i++) outs[i] = data + i * DIM;

    uint64_t rng = (uint64_t)(ta->thread_id * 7919 + 42);

    /* Warmup */
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % NUM_FEATURES) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
    }

    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % NUM_FEATURES) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
    }
    ta->result->elapsed = now_sec() - t0;
    ta->result->iters = iters;
    ta->result->batch = batch_sz;

    free(fids);
    free(outs);
    free(data);
    free(odims);
    graveldb_client_close(c);
    return NULL;
}

static void bench_concurrent_clients(void) {
    printf("\n── Multi-Client Throughput (dim=%d, batch=128) ──\n", DIM);
    printf("  %-12s  %12s  %12s\n", "clients", "total ops/s", "features/sec");
    printf("  %-12s  %12s  %12s\n", "-------", "-----------", "------------");

    int client_counts[] = {1, 2, 4, 8};
    int num_counts = sizeof(client_counts) / sizeof(client_counts[0]);
    int batch = 128;
    int iters_per_client = 2000;

    for (int ci = 0; ci < num_counts; ci++) {
        int nc = client_counts[ci];
        fprintf(stderr, "  [bench_concurrent] testing %d clients...\n", nc);
        pthread_t *threads = (pthread_t *)malloc(nc * sizeof(pthread_t));
        MCThreadResult *results = (MCThreadResult *)calloc(nc, sizeof(MCThreadResult));
        MCThreadArg *args = (MCThreadArg *)malloc(nc * sizeof(MCThreadArg));

        for (int i = 0; i < nc; i++) {
            args[i].iters = iters_per_client;
            args[i].batch = batch;
            args[i].result = &results[i];
            args[i].thread_id = i;
            pthread_create(&threads[i], NULL, mc_client_thread_fn, &args[i]);
        }

        for (int i = 0; i < nc; i++) {
            pthread_join(threads[i], NULL);
        }

        /* Aggregate: total ops / max elapsed */
        double max_elapsed = 0;
        int total_iters = 0;
        for (int i = 0; i < nc; i++) {
            if (results[i].elapsed > max_elapsed) max_elapsed = results[i].elapsed;
            total_iters += results[i].iters;
        }

        double total_ops_sec = total_iters / max_elapsed;
        double total_feats_sec = total_ops_sec * batch;

        printf("  %-12d  %12.0f  %12.0f\n", nc, total_ops_sec, total_feats_sec);

        free(threads);
        free(results);
        free(args);
    }
}

/* ─────────────────── Readonly Mode Benchmark ─────────────────── */

/*
 * Tests multi-threaded lock-free reads in readonly mode.
 *
 * Strategy:
 *   1. Start RW server, push NUM_FEATURES, checkpoint, stop.
 *   2. Restart in readonly mode with varying --read-workers.
 *   3. Measure concurrent pull throughput with varying client counts.
 *
 * Output: a workers × clients throughput matrix.
 */

#define RO_BENCH_DIR    "/tmp/graveldb_bench_readonly"
#define RO_BENCH_PORT   19528

/* Defaults — all overridable via environment variables:
 *   RO_PREPOP          - number of features to pre-populate (default: 50000)
 *   RO_ITERS           - pull iterations per client thread (default: 0 = time-driven)
 *   RO_DURATION        - seconds per test point in time-driven mode (default: 10)
 *   RO_BATCH           - pull batch size (default: 128)
 *   RO_DIM             - embedding dimension (default: 128)
 *   RO_MAX_WORKERS     - max read-workers to test (default: auto = 2×ncpu, capped 64)
 *   RO_MAX_CLIENTS     - max concurrent clients to test (default: auto = 2×ncpu, capped 64)
 *
 * Presets for different EC2 scales (set RO_SCALE env):
 *   "small"   - 50K features, 5s per point (CI / quick verify)
 *   "medium"  - 500K features, 10s per point (r8g.2xlarge / r8g.4xlarge)
 *   "large"   - 2M features, 15s per point  (r8g.4xlarge / r8g.8xlarge)
 *   "extreme" - 10M features, 20s per point  (r8g.8xlarge+ / stress test)
 */
#define DEFAULT_RO_PREPOP       50000
#define DEFAULT_RO_ITERS        0       /* 0 = time-driven */
#define DEFAULT_RO_DURATION     10      /* seconds per test point */
#define DEFAULT_RO_BATCH        128
#define DEFAULT_RO_DIM          128

static int RO_PREPOP_N;
static int RO_ITERS_PER_CLIENT;  /* 0 means time-driven */
static int RO_DURATION_SEC;
static int RO_BATCH_SZ;
static int RO_DIM;
static int RO_MAX_WORKERS;
static int RO_MAX_CLIENTS;

/* Detect number of online CPUs (portable) */
static int detect_ncpu(void) {
#ifdef _SC_NPROCESSORS_ONLN
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return (n > 0) ? (int)n : 4;
#else
    return 4;
#endif
}

/* Build a power-of-two sequence: 1, 2, 4, ..., up to max (inclusive).
 * Returns count. buf must hold at least 16 entries. */
static int build_pow2_seq(int *buf, int max) {
    int count = 0;
    for (int v = 1; v <= max && count < 16; v *= 2) {
        buf[count++] = v;
    }
    return count;
}

static void ro_parse_config(void) {
    int ncpu = detect_ncpu();

    /* Check for scale preset first */
    const char *scale = getenv("RO_SCALE");
    if (scale) {
        if (strcmp(scale, "small") == 0) {
            RO_PREPOP_N = 50000;   RO_DURATION_SEC = 5;
            RO_BATCH_SZ = 128;     RO_DIM = 128;
        } else if (strcmp(scale, "medium") == 0) {
            RO_PREPOP_N = 500000;  RO_DURATION_SEC = 10;
            RO_BATCH_SZ = 256;     RO_DIM = 128;
        } else if (strcmp(scale, "large") == 0) {
            RO_PREPOP_N = 2000000; RO_DURATION_SEC = 15;
            RO_BATCH_SZ = 256;     RO_DIM = 128;
        } else if (strcmp(scale, "extreme") == 0) {
            RO_PREPOP_N = 10000000; RO_DURATION_SEC = 20;
            RO_BATCH_SZ = 512;     RO_DIM = 128;
        } else {
            fprintf(stderr, "Warning: unknown RO_SCALE '%s', using defaults\n", scale);
            RO_PREPOP_N = DEFAULT_RO_PREPOP;
            RO_DURATION_SEC = DEFAULT_RO_DURATION;
            RO_BATCH_SZ = DEFAULT_RO_BATCH;
            RO_DIM = DEFAULT_RO_DIM;
        }
        RO_ITERS_PER_CLIENT = 0; /* time-driven for presets */
    } else {
        RO_PREPOP_N = DEFAULT_RO_PREPOP;
        RO_DURATION_SEC = DEFAULT_RO_DURATION;
        RO_BATCH_SZ = DEFAULT_RO_BATCH;
        RO_DIM = DEFAULT_RO_DIM;
        RO_ITERS_PER_CLIENT = DEFAULT_RO_ITERS;
    }

    /* Individual env vars override preset values */
    const char *e;
    e = getenv("RO_PREPOP");
    if (e) { RO_PREPOP_N = atoi(e); if (RO_PREPOP_N < 1000) RO_PREPOP_N = 1000; }

    e = getenv("RO_ITERS");
    if (e) { RO_ITERS_PER_CLIENT = atoi(e); }

    e = getenv("RO_DURATION");
    if (e) { RO_DURATION_SEC = atoi(e); if (RO_DURATION_SEC < 1) RO_DURATION_SEC = 1; }

    e = getenv("RO_BATCH");
    if (e) { RO_BATCH_SZ = atoi(e); if (RO_BATCH_SZ < 1) RO_BATCH_SZ = 1; }

    e = getenv("RO_DIM");
    if (e) { RO_DIM = atoi(e); if (RO_DIM < 1) RO_DIM = 1; }

    e = getenv("RO_MAX_WORKERS");
    if (e) {
        RO_MAX_WORKERS = atoi(e);
    } else {
        RO_MAX_WORKERS = ncpu * 2;
        if (RO_MAX_WORKERS > 64) RO_MAX_WORKERS = 64;
        if (RO_MAX_WORKERS < 4) RO_MAX_WORKERS = 4;
    }

    e = getenv("RO_MAX_CLIENTS");
    if (e) {
        RO_MAX_CLIENTS = atoi(e);
    } else {
        RO_MAX_CLIENTS = ncpu * 2;
        if (RO_MAX_CLIENTS > 64) RO_MAX_CLIENTS = 64;
        if (RO_MAX_CLIENTS < 4) RO_MAX_CLIENTS = 4;
    }

    /* Print dataset size estimate */
    double data_gb = (double)RO_PREPOP_N * RO_DIM * sizeof(float) / (1024.0 * 1024.0 * 1024.0);
    printf("  Dataset: %d features × dim%d = %.2f GB (raw embeddings)\n",
           RO_PREPOP_N, RO_DIM, data_gb);
    if (RO_ITERS_PER_CLIENT > 0) {
        printf("  Mode: iteration-driven (%d iters/client)\n", RO_ITERS_PER_CLIENT);
    } else {
        printf("  Mode: time-driven (%d sec/test-point, steady-state throughput)\n", RO_DURATION_SEC);
    }
}

static void ro_cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", RO_BENCH_DIR);
    system(cmd);
}

/* Pre-populate data using a RW server, then checkpoint and shut down */
static int ro_prepopulate(void) {
    ro_cleanup();

    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    int dims[] = {RO_DIM};
    config.db_config.data_dir = RO_BENCH_DIR;
    config.db_config.dims = dims;
    config.db_config.num_dims = 1;
    /* Buffer size: at least 512MB, scale up for large datasets */
    size_t buf_sz = 512ULL * 1024 * 1024;
    size_t data_sz = (size_t)RO_PREPOP_N * RO_DIM * sizeof(float);
    if (data_sz > buf_sz) buf_sz = data_sz + 256ULL * 1024 * 1024;
    config.db_config.buffer_size = buf_sz;
    config.db_config.index_capacity = RO_PREPOP_N * 2;

    config.port = RO_BENCH_PORT;
    config.backlog = 64;
    config.max_request_size = 64 * 1024 * 1024;
    config.auto_flush_interval_ms = 0;  /* manual flush only for speed */
    config.auto_checkpoint_interval_s = 0;
    config.readonly = false;

    GravelServer *srv = NULL;
    graveldb_status_t rc = gravel_server_create(&srv, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[ro_prepop] Failed to create server: %d\n", rc);
        return -1;
    }

    for (int attempt = 0; attempt < 5; attempt++) {
        rc = gravel_server_start(srv);
        if (rc == GRAVELDB_OK) break;
        fprintf(stderr, "[ro_prepop] Port %d busy, retrying (%d/5)...\n", RO_BENCH_PORT, attempt + 1);
        sleep(1);
    }
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[ro_prepop] Failed to start server: %d\n", rc);
        gravel_server_destroy(srv);
        return -1;
    }
    usleep(100000);

    /* Connect and push data */
    GravelDBClient *client = NULL;
    int retries = 10;
    while (retries-- > 0) {
        if (graveldb_client_connect(&client, "127.0.0.1", RO_BENCH_PORT) == 0) break;
        usleep(100000);
    }
    if (!client) {
        fprintf(stderr, "[ro_prepop] Failed to connect\n");
        gravel_server_stop(srv);
        gravel_server_destroy(srv);
        return -1;
    }

    /* Use larger push batch for big datasets */
    int push_batch = (RO_PREPOP_N >= 1000000) ? 4096 : 1024;
    uint64_t *fids = (uint64_t *)malloc(push_batch * sizeof(uint64_t));
    int *push_dims = (int *)malloc(push_batch * sizeof(int));
    float **embs = (float **)malloc(push_batch * sizeof(float *));
    float *pdata = (float *)malloc(push_batch * RO_DIM * sizeof(float));

    for (int i = 0; i < push_batch; i++) {
        push_dims[i] = RO_DIM;
        embs[i] = pdata + i * RO_DIM;
        for (int j = 0; j < RO_DIM; j++) embs[i][j] = 0.01f * ((i % 100) + 1);
    }

    double t0 = now_sec();
    int last_pct = -1;
    printf("  Pre-populating %d features (dim=%d, RW mode)...\n", RO_PREPOP_N, RO_DIM);
    fflush(stdout);

    /* Flush every N batches to avoid buffer overflow */
    int flush_every = (RO_PREPOP_N >= 1000000) ? 50 : 200;
    int batch_count = 0;

    for (int base = 0; base < RO_PREPOP_N; base += push_batch) {
        int n = (RO_PREPOP_N - base < push_batch) ? (RO_PREPOP_N - base) : push_batch;
        for (int i = 0; i < n; i++) fids[i] = (uint64_t)(base + i + 1);
        graveldb_client_push(client, fids, push_dims, (const float *const *)embs, n);
        batch_count++;

        if (batch_count % flush_every == 0) {
            graveldb_client_flush(client);
        }

        /* Progress */
        int pct = (int)((int64_t)(base + n) * 100 / RO_PREPOP_N);
        if (pct != last_pct && pct % 10 == 0) {
            double elapsed = now_sec() - t0;
            double rate = (base + n) / elapsed;
            printf("    %3d%% (%d/%d) - %.0f features/sec\n",
                   pct, base + n, RO_PREPOP_N, rate);
            fflush(stdout);
            last_pct = pct;
        }
    }

    /* Final flush + checkpoint */
    printf("  Flushing + checkpoint...");
    fflush(stdout);
    graveldb_client_flush(client);
    graveldb_client_checkpoint(client);
    double prep_elapsed = now_sec() - t0;
    printf(" done. (%.1f sec, %.0f features/sec)\n", prep_elapsed, RO_PREPOP_N / prep_elapsed);

    graveldb_client_close(client);
    free(fids); free(push_dims); free(embs); free(pdata);

    gravel_server_stop(srv);
    gravel_server_destroy(srv);
    return 0;
}

/* Start readonly server with N read workers */
static GravelServer *ro_start_server(int num_workers) {
    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    int dims[] = {RO_DIM};
    config.db_config.data_dir = RO_BENCH_DIR;
    config.db_config.dims = dims;
    config.db_config.num_dims = 1;
    /* Buffer size: scale with data */
    size_t ro_buf_sz = 512ULL * 1024 * 1024;
    size_t ro_data_sz = (size_t)RO_PREPOP_N * RO_DIM * sizeof(float);
    if (ro_data_sz > ro_buf_sz) ro_buf_sz = ro_data_sz + 256ULL * 1024 * 1024;
    config.db_config.buffer_size = ro_buf_sz;
    config.db_config.index_capacity = RO_PREPOP_N * 2;

    config.port = RO_BENCH_PORT;
    config.backlog = 128;
    config.max_request_size = 64 * 1024 * 1024;
    config.auto_flush_interval_ms = 0;
    config.auto_checkpoint_interval_s = 0;
    config.readonly = true;
    config.num_read_workers = num_workers;

    GravelServer *srv = NULL;
    graveldb_status_t rc = gravel_server_create(&srv, &config);
    if (rc != GRAVELDB_OK) return NULL;

    for (int attempt = 0; attempt < 5; attempt++) {
        rc = gravel_server_start(srv);
        if (rc == GRAVELDB_OK) break;
        usleep(500000);
    }
    if (rc != GRAVELDB_OK) {
        gravel_server_destroy(srv);
        return NULL;
    }
    usleep(100000);
    return srv;
}

typedef struct {
    int             iters;
    int             batch;
    double          elapsed;
    int             thread_id;
} ROThreadResult;

typedef struct {
    int              iters;
    int              batch;
    ROThreadResult  *result;
    int              thread_id;
} ROThreadArg;

static void *ro_client_thread_fn(void *arg) {
    ROThreadArg *ta = (ROThreadArg *)arg;
    int iters = ta->iters;
    int batch_sz = ta->batch;

    GravelDBClient *c = NULL;
    int retries = 10;
    while (retries-- > 0) {
        if (graveldb_client_connect(&c, "127.0.0.1", RO_BENCH_PORT) == 0) break;
        usleep(50000);
    }
    if (!c) {
        ta->result->elapsed = -1;
        return NULL;
    }

    uint64_t *fids = (uint64_t *)malloc(batch_sz * sizeof(uint64_t));
    float **outs = (float **)malloc(batch_sz * sizeof(float *));
    float *data = (float *)malloc((size_t)batch_sz * RO_DIM * sizeof(float));
    int *odims = (int *)malloc(batch_sz * sizeof(int));
    for (int i = 0; i < batch_sz; i++) outs[i] = data + (size_t)i * RO_DIM;

    uint64_t rng = (uint64_t)(ta->thread_id * 7919 + 1337);

    /* Warmup */
    for (int i = 0; i < 20; i++) {
        for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % RO_PREPOP_N) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
    }

    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % RO_PREPOP_N) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
    }
    ta->result->elapsed = now_sec() - t0;
    ta->result->iters = iters;
    ta->result->batch = batch_sz;

    free(fids); free(outs); free(data); free(odims);
    graveldb_client_close(c);
    return NULL;
}

static void bench_readonly_scalability(void) {
    ro_parse_config();

    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  GravelDB Readonly Mode Benchmark (lock-free multi-threaded)\n");
    printf("  dim=%d, batch=%d, iters/client=%d, features=%d\n",
           RO_DIM, RO_BATCH_SZ, RO_ITERS_PER_CLIENT, RO_PREPOP_N);
    printf("  max_workers=%d, max_clients=%d (ncpu=%d)\n",
           RO_MAX_WORKERS, RO_MAX_CLIENTS, detect_ncpu());
    printf("  (set RO_PREPOP/RO_ITERS/RO_BATCH/RO_MAX_WORKERS/RO_MAX_CLIENTS to tune)\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");

    /* Step 1: Prepopulate */
    printf("[RO 1/2] Prepopulating data (RW server)...\n");
    fflush(stdout);
    if (ro_prepopulate() != 0) {
        fprintf(stderr, "  Failed to prepopulate, skipping readonly bench.\n");
        return;
    }

    /* Step 2: Build workers and clients sequences based on config */
    int worker_buf[16], client_buf[16];
    int num_wc = build_pow2_seq(worker_buf, RO_MAX_WORKERS);
    int num_cc = build_pow2_seq(client_buf, RO_MAX_CLIENTS);
    /* Prepend workers=0 (single-thread fallback) */
    int worker_counts[17];
    worker_counts[0] = 0;
    for (int i = 0; i < num_wc; i++) worker_counts[i + 1] = worker_buf[i];
    num_wc += 1;

    printf("\n[RO 2/2] Throughput matrix (features/sec):\n\n");
    printf("  %-14s", "workers\\cli");
    for (int ci = 0; ci < num_cc; ci++) {
        printf("  %8d-cli", client_buf[ci]);
    }
    printf("\n  %-14s", "-----------");
    for (int ci = 0; ci < num_cc; ci++) {
        printf("  %12s", "----------");
    }
    printf("\n");

    for (int wi = 0; wi < num_wc; wi++) {
        int nw = worker_counts[wi];

        GravelServer *srv = ro_start_server(nw);
        if (!srv) {
            printf("  workers=%-5d  (FAILED to start)\n", nw);
            continue;
        }

        printf("  workers=%-5d", nw);
        fflush(stdout);

        for (int ci = 0; ci < num_cc; ci++) {
            int nc = client_buf[ci];
            pthread_t *threads = (pthread_t *)malloc(nc * sizeof(pthread_t));
            ROThreadResult *results = (ROThreadResult *)calloc(nc, sizeof(ROThreadResult));
            ROThreadArg *args = (ROThreadArg *)malloc(nc * sizeof(ROThreadArg));

            for (int i = 0; i < nc; i++) {
                args[i].iters = RO_ITERS_PER_CLIENT;
                args[i].batch = RO_BATCH_SZ;
                args[i].result = &results[i];
                args[i].thread_id = i;
                pthread_create(&threads[i], NULL, ro_client_thread_fn, &args[i]);
            }

            for (int i = 0; i < nc; i++) {
                pthread_join(threads[i], NULL);
            }

            /* Aggregate: total features / max elapsed */
            double max_elapsed = 0;
            int total_iters = 0;
            int failed = 0;
            for (int i = 0; i < nc; i++) {
                if (results[i].elapsed < 0) { failed++; continue; }
                if (results[i].elapsed > max_elapsed) max_elapsed = results[i].elapsed;
                total_iters += results[i].iters;
            }

            if (failed > 0 || max_elapsed == 0) {
                printf("  %12s", "ERR");
            } else {
                double total_feats_sec = (double)total_iters * RO_BATCH_SZ / max_elapsed;
                if (total_feats_sec >= 1e6) {
                    printf("  %9.2fM", total_feats_sec / 1e6);
                } else {
                    printf("  %9.0fK", total_feats_sec / 1e3);
                }
            }
            fflush(stdout);

            free(threads); free(results); free(args);
        }

        printf("\n");

        gravel_server_stop(srv);
        gravel_server_destroy(srv);
        usleep(200000);  /* let port release */
    }

    ro_cleanup();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MIXED-DIM BENCHMARK
 *
 * Simulates a real production embedding store where features have varying
 * dimensions (8, 16, 32, 64, 128, 256, 512, 1024) drawn from a Zipf
 * distribution (power-law): a few popular dims dominate while rare large
 * dims provide tail load — stressing the multi-bin mmap path.
 *
 * Environment variables:
 *   MD_PREPOP         - total features to pre-populate (default: 500000)
 *   MD_DURATION       - seconds per test point (default: 10)
 *   MD_BATCH          - pull batch size (default: 256)
 *   MD_MAX_WORKERS    - max read-workers to test (default: auto 2×ncpu)
 *   MD_MAX_CLIENTS    - max concurrent clients (default: auto 2×ncpu)
 *   MD_ZIPF_ALPHA     - Zipf skew parameter (default: 1.2; higher=more skewed)
 *   MD_SCALE          - preset: "small"/"medium"/"large"/"extreme"
 * ═══════════════════════════════════════════════════════════════════════════ */

#define MD_BENCH_DIR    "/tmp/graveldb_bench_mixdim"
#define MD_BENCH_PORT   19529

/* Candidate dimensions — power-of-two from 8 to 1024 */
static const int MD_DIM_CANDIDATES[] = {8, 16, 32, 64, 128, 256, 512, 1024};
#define MD_NUM_DIMS (sizeof(MD_DIM_CANDIDATES) / sizeof(MD_DIM_CANDIDATES[0]))

static int MD_PREPOP_N;
static int MD_DURATION_SEC;
static int MD_BATCH_SZ;
static int MD_MAX_WORKERS;
static int MD_MAX_CLIENTS;
static double MD_ZIPF_ALPHA;

/* Per-feature dim assignment (indexed by feature_id - 1) */
static int *MD_FEAT_DIMS = NULL;

/* Zipf probability table for dim selection */
static double MD_DIM_CDF[8];  /* cumulative probabilities */

static void md_build_zipf_cdf(double alpha) {
    double sum = 0.0;
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        sum += 1.0 / pow((double)(i + 1), alpha);
    }
    double cum = 0.0;
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        cum += (1.0 / pow((double)(i + 1), alpha)) / sum;
        MD_DIM_CDF[i] = cum;
    }
    MD_DIM_CDF[MD_NUM_DIMS - 1] = 1.0; /* ensure no rounding issues */
}

/* Select a dim index from Zipf CDF using a uniform random in [0,1) */
static int md_zipf_sample(uint64_t *rng) {
    double u = (double)(xorshift64(rng) & 0xFFFFFFFF) / 4294967296.0;
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        if (u < MD_DIM_CDF[i]) return i;
    }
    return (int)MD_NUM_DIMS - 1;
}

static void md_parse_config(void) {
    int ncpu = detect_ncpu();

    /* Defaults */
    MD_PREPOP_N = 500000;
    MD_DURATION_SEC = 10;
    MD_BATCH_SZ = 256;
    MD_ZIPF_ALPHA = 1.2;

    /* Scale presets */
    const char *scale = getenv("MD_SCALE");
    if (scale) {
        if (strcmp(scale, "small") == 0) {
            MD_PREPOP_N = 50000;  MD_DURATION_SEC = 5;  MD_BATCH_SZ = 128;
        } else if (strcmp(scale, "medium") == 0) {
            MD_PREPOP_N = 500000; MD_DURATION_SEC = 10; MD_BATCH_SZ = 256;
        } else if (strcmp(scale, "large") == 0) {
            MD_PREPOP_N = 2000000; MD_DURATION_SEC = 15; MD_BATCH_SZ = 256;
        } else if (strcmp(scale, "extreme") == 0) {
            MD_PREPOP_N = 10000000; MD_DURATION_SEC = 20; MD_BATCH_SZ = 512;
        }
    }

    const char *e;
    e = getenv("MD_PREPOP");
    if (e) { MD_PREPOP_N = atoi(e); if (MD_PREPOP_N < 1000) MD_PREPOP_N = 1000; }
    e = getenv("MD_DURATION");
    if (e) { MD_DURATION_SEC = atoi(e); if (MD_DURATION_SEC < 1) MD_DURATION_SEC = 1; }
    e = getenv("MD_BATCH");
    if (e) { MD_BATCH_SZ = atoi(e); if (MD_BATCH_SZ < 1) MD_BATCH_SZ = 1; }
    e = getenv("MD_ZIPF_ALPHA");
    if (e) { MD_ZIPF_ALPHA = atof(e); if (MD_ZIPF_ALPHA < 0.1) MD_ZIPF_ALPHA = 0.1; }

    e = getenv("MD_MAX_WORKERS");
    if (e) {
        MD_MAX_WORKERS = atoi(e);
    } else {
        MD_MAX_WORKERS = ncpu * 2;
        if (MD_MAX_WORKERS > 64) MD_MAX_WORKERS = 64;
        if (MD_MAX_WORKERS < 4) MD_MAX_WORKERS = 4;
    }
    e = getenv("MD_MAX_CLIENTS");
    if (e) {
        MD_MAX_CLIENTS = atoi(e);
    } else {
        MD_MAX_CLIENTS = ncpu * 2;
        if (MD_MAX_CLIENTS > 64) MD_MAX_CLIENTS = 64;
        if (MD_MAX_CLIENTS < 4) MD_MAX_CLIENTS = 4;
    }
}

static void md_cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", MD_BENCH_DIR);
    system(cmd);
}

/* Assign dims to all features using Zipf, returns total data size in bytes */
static size_t md_assign_dims(void) {
    MD_FEAT_DIMS = (int *)malloc(MD_PREPOP_N * sizeof(int));
    uint64_t rng = 42;
    size_t total_bytes = 0;
    int dim_counts[8] = {0};

    for (int i = 0; i < MD_PREPOP_N; i++) {
        int idx = md_zipf_sample(&rng);
        MD_FEAT_DIMS[i] = MD_DIM_CANDIDATES[idx];
        dim_counts[idx]++;
        total_bytes += MD_DIM_CANDIDATES[idx] * sizeof(float);
    }

    printf("  Dimension distribution (Zipf α=%.2f):\n", MD_ZIPF_ALPHA);
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        double pct = 100.0 * dim_counts[i] / MD_PREPOP_N;
        printf("    dim=%4d: %8d features (%5.1f%%)\n",
               MD_DIM_CANDIDATES[i], dim_counts[i], pct);
    }
    printf("  Total raw data: %.2f GB\n", total_bytes / (1024.0 * 1024.0 * 1024.0));
    return total_bytes;
}

/* Pre-populate mixed-dim data */
static int md_prepopulate(void) {
    md_cleanup();

    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    /* Register all candidate dims */
    int dims[8];
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) dims[i] = MD_DIM_CANDIDATES[i];

    config.db_config.data_dir = MD_BENCH_DIR;
    config.db_config.dims = dims;
    config.db_config.num_dims = (int)MD_NUM_DIMS;
    config.db_config.auto_create_bins = true;

    /* Buffer size: scale with data */
    size_t data_sz = 0;
    for (int i = 0; i < MD_PREPOP_N; i++)
        data_sz += MD_FEAT_DIMS[i] * sizeof(float);
    size_t buf_sz = 512ULL * 1024 * 1024;
    if (data_sz > buf_sz) buf_sz = data_sz + 256ULL * 1024 * 1024;
    config.db_config.buffer_size = buf_sz;
    config.db_config.index_capacity = MD_PREPOP_N * 2;

    config.port = MD_BENCH_PORT;
    config.backlog = 64;
    config.max_request_size = 128 * 1024 * 1024;
    config.auto_flush_interval_ms = 0;
    config.auto_checkpoint_interval_s = 0;
    config.readonly = false;

    GravelServer *srv = NULL;
    graveldb_status_t rc = gravel_server_create(&srv, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[md_prepop] Failed to create server: %d\n", rc);
        return -1;
    }

    for (int attempt = 0; attempt < 5; attempt++) {
        rc = gravel_server_start(srv);
        if (rc == GRAVELDB_OK) break;
        fprintf(stderr, "[md_prepop] Port %d busy, retrying (%d/5)...\n", MD_BENCH_PORT, attempt + 1);
        sleep(1);
    }
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[md_prepop] Failed to start server: %d\n", rc);
        gravel_server_destroy(srv);
        return -1;
    }
    usleep(100000);

    GravelDBClient *client = NULL;
    int retries = 10;
    while (retries-- > 0) {
        if (graveldb_client_connect(&client, "127.0.0.1", MD_BENCH_PORT) == 0) break;
        usleep(100000);
    }
    if (!client) {
        fprintf(stderr, "[md_prepop] Failed to connect\n");
        gravel_server_stop(srv);
        gravel_server_destroy(srv);
        return -1;
    }

    /* Push in batches — variable dim per feature */
    int push_batch = (MD_PREPOP_N >= 1000000) ? 2048 : 512;
    int max_dim = 1024;
    uint64_t *fids = (uint64_t *)malloc(push_batch * sizeof(uint64_t));
    int *pdims = (int *)malloc(push_batch * sizeof(int));
    float **embs = (float **)malloc(push_batch * sizeof(float *));
    float *pool = (float *)malloc((size_t)push_batch * max_dim * sizeof(float));

    /* Fill pool with pseudo-random data */
    uint64_t fill_rng = 12345;
    for (int i = 0; i < push_batch * max_dim; i++) {
        pool[i] = (float)(xorshift64(&fill_rng) & 0xFFFF) / 65536.0f - 0.5f;
    }

    double t0 = now_sec();
    int last_pct = -1;
    int flush_every = (MD_PREPOP_N >= 1000000) ? 30 : 100;
    int batch_count = 0;

    printf("  Pre-populating %d mixed-dim features...\n", MD_PREPOP_N);
    fflush(stdout);

    for (int base = 0; base < MD_PREPOP_N; base += push_batch) {
        int n = (MD_PREPOP_N - base < push_batch) ? (MD_PREPOP_N - base) : push_batch;
        for (int i = 0; i < n; i++) {
            fids[i] = (uint64_t)(base + i + 1);
            pdims[i] = MD_FEAT_DIMS[base + i];
            embs[i] = pool + (i % push_batch) * max_dim; /* reuse pool, only first pdims[i] floats matter */
        }
        graveldb_client_push(client, fids, pdims, (const float *const *)embs, n);
        batch_count++;

        if (batch_count % flush_every == 0) {
            graveldb_client_flush(client);
        }

        int pct = (int)((int64_t)(base + n) * 100 / MD_PREPOP_N);
        if (pct != last_pct && pct % 10 == 0) {
            double elapsed = now_sec() - t0;
            double rate = (base + n) / elapsed;
            printf("    %3d%% (%d/%d) - %.0f features/sec\n", pct, base + n, MD_PREPOP_N, rate);
            fflush(stdout);
            last_pct = pct;
        }
    }

    printf("  Flushing + checkpoint...");
    fflush(stdout);
    graveldb_client_flush(client);
    graveldb_client_checkpoint(client);
    double prep_elapsed = now_sec() - t0;
    printf(" done. (%.1f sec, %.0f features/sec)\n", prep_elapsed, MD_PREPOP_N / prep_elapsed);

    graveldb_client_close(client);
    free(fids); free(pdims); free(embs); free(pool);
    gravel_server_stop(srv);
    gravel_server_destroy(srv);
    return 0;
}

/* Start mixed-dim readonly server */
static GravelServer *md_start_server(int num_workers) {
    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    int dims[8];
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) dims[i] = MD_DIM_CANDIDATES[i];

    config.db_config.data_dir = MD_BENCH_DIR;
    config.db_config.dims = dims;
    config.db_config.num_dims = (int)MD_NUM_DIMS;
    config.db_config.auto_create_bins = true;

    size_t buf_sz = 512ULL * 1024 * 1024;
    size_t est_data = 0;
    for (int i = 0; i < MD_PREPOP_N; i++) est_data += MD_FEAT_DIMS[i] * sizeof(float);
    if (est_data > buf_sz) buf_sz = est_data + 256ULL * 1024 * 1024;
    config.db_config.buffer_size = buf_sz;
    config.db_config.index_capacity = MD_PREPOP_N * 2;

    config.port = MD_BENCH_PORT;
    config.backlog = 128;
    config.max_request_size = 128 * 1024 * 1024;
    config.auto_flush_interval_ms = 0;
    config.auto_checkpoint_interval_s = 0;
    config.readonly = true;
    config.num_read_workers = num_workers;

    GravelServer *srv = NULL;
    graveldb_status_t rc = gravel_server_create(&srv, &config);
    if (rc != GRAVELDB_OK) return NULL;

    for (int attempt = 0; attempt < 5; attempt++) {
        rc = gravel_server_start(srv);
        if (rc == GRAVELDB_OK) break;
        usleep(500000);
    }
    if (rc != GRAVELDB_OK) {
        gravel_server_destroy(srv);
        return NULL;
    }
    usleep(100000);
    return srv;
}

/* Mixed-dim client thread — time-driven */
typedef struct {
    int             batch;
    int             duration_sec;
    int             thread_id;
    /* output */
    double          elapsed;
    int64_t         total_features;
    int64_t         total_bytes;  /* bytes of embedding data pulled */
} MDThreadResult;

typedef struct {
    int              batch;
    int              duration_sec;
    MDThreadResult  *result;
    int              thread_id;
} MDThreadArg;

static void *md_client_thread_fn(void *arg) {
    MDThreadArg *ta = (MDThreadArg *)arg;
    int batch_sz = ta->batch;
    int dur = ta->duration_sec;

    GravelDBClient *c = NULL;
    int retries = 10;
    while (retries-- > 0) {
        if (graveldb_client_connect(&c, "127.0.0.1", MD_BENCH_PORT) == 0) break;
        usleep(50000);
    }
    if (!c) {
        ta->result->elapsed = -1;
        return NULL;
    }

    int max_dim = 1024;
    uint64_t *fids = (uint64_t *)malloc(batch_sz * sizeof(uint64_t));
    float **outs = (float **)malloc(batch_sz * sizeof(float *));
    float *data = (float *)malloc((size_t)batch_sz * max_dim * sizeof(float));
    int *odims = (int *)malloc(batch_sz * sizeof(int));
    for (int i = 0; i < batch_sz; i++) outs[i] = data + (size_t)i * max_dim;

    uint64_t rng = (uint64_t)(ta->thread_id * 7919 + 31337);

    /* Warmup */
    for (int i = 0; i < 20; i++) {
        for (int j = 0; j < batch_sz; j++)
            fids[j] = (xorshift64(&rng) % MD_PREPOP_N) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
    }

    /* Time-driven measurement */
    double t0 = now_sec();
    double deadline = t0 + dur;
    int64_t total_feats = 0;
    int64_t total_bytes = 0;

    while (now_sec() < deadline) {
        for (int j = 0; j < batch_sz; j++)
            fids[j] = (xorshift64(&rng) % MD_PREPOP_N) + 1;
        graveldb_client_pull(c, fids, batch_sz, outs, odims);
        total_feats += batch_sz;
        for (int j = 0; j < batch_sz; j++)
            total_bytes += odims[j] * (int)sizeof(float);
    }

    ta->result->elapsed = now_sec() - t0;
    ta->result->total_features = total_feats;
    ta->result->total_bytes = total_bytes;

    free(fids); free(outs); free(data); free(odims);
    graveldb_client_close(c);
    return NULL;
}

static void bench_mixed_dim_scalability(void) {
    md_parse_config();
    md_build_zipf_cdf(MD_ZIPF_ALPHA);

    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  GravelDB Mixed-Dimension Benchmark (Zipf distribution)\n");
    printf("  dims=8..1024, batch=%d, duration=%ds/point, features=%d\n",
           MD_BATCH_SZ, MD_DURATION_SEC, MD_PREPOP_N);
    printf("  max_workers=%d, max_clients=%d, zipf_alpha=%.2f\n",
           MD_MAX_WORKERS, MD_MAX_CLIENTS, MD_ZIPF_ALPHA);
    printf("  (set MD_PREPOP/MD_DURATION/MD_BATCH/MD_ZIPF_ALPHA/MD_SCALE to tune)\n");
    printf("═══════════════════════════════════════════════════════════════\n\n");

    /* Assign dims to features */
    size_t total_data = md_assign_dims();
    (void)total_data;
    printf("\n");

    /* Prepopulate */
    printf("[MD 1/2] Prepopulating data (RW server)...\n");
    fflush(stdout);
    if (md_prepopulate() != 0) {
        fprintf(stderr, "  Failed to prepopulate, skipping mixed-dim bench.\n");
        if (MD_FEAT_DIMS) { free(MD_FEAT_DIMS); MD_FEAT_DIMS = NULL; }
        return;
    }

    /* Build test sequences */
    int worker_buf[16], client_buf[16];
    int num_wc = build_pow2_seq(worker_buf, MD_MAX_WORKERS);
    int num_cc = build_pow2_seq(client_buf, MD_MAX_CLIENTS);
    int worker_counts[17];
    worker_counts[0] = 0;
    for (int i = 0; i < num_wc; i++) worker_counts[i + 1] = worker_buf[i];
    num_wc += 1;

    printf("\n[MD 2/2] Throughput matrix (time-driven, %ds/point):\n\n", MD_DURATION_SEC);

    /* Header: features/sec */
    printf("  %-14s", "workers\\cli");
    for (int ci = 0; ci < num_cc; ci++) {
        printf("  %8d-cli", client_buf[ci]);
    }
    printf("   avg_dim  bandwidth\n");
    printf("  %-14s", "-----------");
    for (int ci = 0; ci < num_cc; ci++) {
        printf("  %12s", "----------");
    }
    printf("   -------  ---------\n");

    for (int wi = 0; wi < num_wc; wi++) {
        int nw = worker_counts[wi];
        GravelServer *srv = md_start_server(nw);
        if (!srv) {
            printf("  workers=%-5d  (FAILED to start)\n", nw);
            continue;
        }

        printf("  workers=%-5d", nw);
        fflush(stdout);

        double last_bw = 0;

        for (int ci = 0; ci < num_cc; ci++) {
            int nc = client_buf[ci];
            pthread_t *threads = (pthread_t *)malloc(nc * sizeof(pthread_t));
            MDThreadResult *results = (MDThreadResult *)calloc(nc, sizeof(MDThreadResult));
            MDThreadArg *args = (MDThreadArg *)malloc(nc * sizeof(MDThreadArg));

            for (int i = 0; i < nc; i++) {
                args[i].batch = MD_BATCH_SZ;
                args[i].duration_sec = MD_DURATION_SEC;
                args[i].result = &results[i];
                args[i].thread_id = i;
                pthread_create(&threads[i], NULL, md_client_thread_fn, &args[i]);
            }

            for (int i = 0; i < nc; i++) {
                pthread_join(threads[i], NULL);
            }

            /* Aggregate */
            double max_elapsed = 0;
            int64_t total_feats = 0;
            int64_t total_bytes = 0;
            int failed = 0;
            for (int i = 0; i < nc; i++) {
                if (results[i].elapsed < 0) { failed++; continue; }
                if (results[i].elapsed > max_elapsed) max_elapsed = results[i].elapsed;
                total_feats += results[i].total_features;
                total_bytes += results[i].total_bytes;
            }

            if (failed > 0 || max_elapsed == 0) {
                printf("  %12s", "ERR");
            } else {
                double feats_sec = total_feats / max_elapsed;
                if (feats_sec >= 1e6) {
                    printf("  %9.2fM", feats_sec / 1e6);
                } else {
                    printf("  %9.0fK", feats_sec / 1e3);
                }
                last_bw = total_bytes / max_elapsed;
            }
            fflush(stdout);

            free(threads); free(results); free(args);
        }

        /* Print average effective dim and bandwidth for last (max clients) column */
        if (last_bw > 0) {
            /* Compute weighted average dim from the distribution */
            double avg_dim = 0;
            for (int i = 0; i < MD_PREPOP_N; i++) avg_dim += MD_FEAT_DIMS[i];
            avg_dim /= MD_PREPOP_N;
            printf("   %5.0f  ", avg_dim);
            if (last_bw >= 1e9) {
                printf("%6.2f GB/s", last_bw / 1e9);
            } else {
                printf("%6.0f MB/s", last_bw / 1e6);
            }
        }
        printf("\n");

        gravel_server_stop(srv);
        gravel_server_destroy(srv);
        usleep(200000);
    }

    /* Cleanup */
    md_cleanup();
    if (MD_FEAT_DIMS) { free(MD_FEAT_DIMS); MD_FEAT_DIMS = NULL; }
}

/* ─────────────────── Main ─────────────────── */

int main(void) {
    /* Disable stdout buffering for real-time progress output */
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    /* Parse config from environment */
    const char *env_samples = getenv("BENCH_SAMPLES");
    LATENCY_SAMPLES = env_samples ? atoi(env_samples) : DEFAULT_LATENCY_SAMPLES;
    if (LATENCY_SAMPLES < 100) LATENCY_SAMPLES = 100;

    const char *env_mixed = getenv("BENCH_MIXED_OPS");
    MIXED_OPS = env_mixed ? atoi(env_mixed) : DEFAULT_MIXED_OPS;
    if (MIXED_OPS < 100) MIXED_OPS = 100;

    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  GravelDB Network Benchmark (loopback TCP)\n");
    printf("  dim=%d, port=%d, samples=%d, mixed_ops=%d\n",
           DIM, BENCH_PORT, LATENCY_SAMPLES, MIXED_OPS);
    printf("  (set BENCH_SAMPLES / BENCH_MIXED_OPS env to adjust)\n");
    printf("═══════════════════════════════════════════════════════════════\n");

    double total_t0 = now_sec();

    /* Start server */
    printf("\n[1/8] Starting server...\n");
    fflush(stdout);
    if (start_server() != 0) {
        fprintf(stderr, "Failed to start server, aborting.\n");
        return 1;
    }
    printf("  Server started.\n");

    /* Connect client */
    printf("[2/8] Connecting client...\n");
    fflush(stdout);
    GravelDBClient *client = NULL;
    int retries = 10;
    while (retries-- > 0) {
        if (graveldb_client_connect(&client, "127.0.0.1", BENCH_PORT) == 0) break;
        usleep(100000); /* 100ms */
    }
    if (!client) {
        fprintf(stderr, "Failed to connect client after retries.\n");
        stop_server();
        return 1;
    }
    printf("  Connected.\n");

    /* Run benchmarks */
    printf("[3/8] Ping latency...\n");
    fflush(stdout);
    bench_ping(client);

    printf("[4/8] Single push/pull latency...\n");
    fflush(stdout);
    bench_single_push_pull(client);

    printf("[5/8] Batch push throughput...\n");
    fflush(stdout);
    bench_batch_push(client);

    printf("[6/8] Batch pull throughput...\n");
    fflush(stdout);
    bench_batch_pull(client);

    printf("[7/8] Mixed workload...\n");
    fflush(stdout);
    bench_mixed_workload(client);

    graveldb_client_close(client);

    /* Multi-client benchmark (opens its own connections) */
    printf("\n[bonus] Multi-client scalability (RW mode)...\n");
    fflush(stdout);
    bench_concurrent_clients();

    /* Cleanup RW server */
    stop_server();
    cleanup();

    /* Readonly mode benchmark */
    printf("\n[8/9] Readonly mode scalability...\n");
    fflush(stdout);
    bench_readonly_scalability();

    /* Mixed-dimension benchmark */
    printf("\n[9/9] Mixed-dimension scalability (Zipf distribution)...\n");
    fflush(stdout);
    bench_mixed_dim_scalability();

    double total_elapsed = now_sec() - total_t0;
    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  All network benchmarks complete. Total time: %.1f s\n", total_elapsed);
    printf("═══════════════════════════════════════════════════════════════\n");

    return 0;
}
