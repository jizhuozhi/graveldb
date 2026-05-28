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
    printf("\n[1/7] Starting server...\n");
    fflush(stdout);
    if (start_server() != 0) {
        fprintf(stderr, "Failed to start server, aborting.\n");
        return 1;
    }
    printf("  Server started.\n");

    /* Connect client */
    printf("[2/7] Connecting client...\n");
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
    printf("[3/7] Ping latency...\n");
    fflush(stdout);
    bench_ping(client);

    printf("[4/7] Single push/pull latency...\n");
    fflush(stdout);
    bench_single_push_pull(client);

    printf("[5/7] Batch push throughput...\n");
    fflush(stdout);
    bench_batch_push(client);

    printf("[6/7] Batch pull throughput...\n");
    fflush(stdout);
    bench_batch_pull(client);

    printf("[7/7] Mixed workload...\n");
    fflush(stdout);
    bench_mixed_workload(client);

    graveldb_client_close(client);

    /* Multi-client benchmark (opens its own connections) */
    printf("\n[bonus] Multi-client scalability...\n");
    fflush(stdout);
    bench_concurrent_clients();

    /* Cleanup */
    stop_server();
    cleanup();

    double total_elapsed = now_sec() - total_t0;
    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  All network benchmarks complete. Total time: %.1f s\n", total_elapsed);
    printf("═══════════════════════════════════════════════════════════════\n");

    return 0;
}
