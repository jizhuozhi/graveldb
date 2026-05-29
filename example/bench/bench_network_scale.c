/*
 * GravelDB - Network Scalability Benchmark (separated from bench_network.c)
 *
 * Tests that take significant time (minutes) due to worker×client matrix:
 *   1. Readonly mode scalability (lock-free multi-threaded reads)
 *   2. Mixed-dimension scalability (Zipf distribution workload)
 *
 * Run separately from the quick IO correctness/perf tests in bench_network.c.
 *
 * Dependencies: client SDK (client.h) only. No src/ internals.
 * Server is started as a child process via fork/exec of graveldb-server.
 */

#include "client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <signal.h>
#include <math.h>
#include <pthread.h>

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

/* Locate the server binary */
static const char *find_server_bin(void) {
    static char path[1024];
    const char *env = getenv("GRAVELDB_SERVER_BIN");
    if (env && env[0]) return env;
    snprintf(path, sizeof(path), "./build/graveldb-server");
    if (access(path, X_OK) == 0) return path;
    snprintf(path, sizeof(path), "graveldb-server");
    return path;
}

/* Start a graveldb-server process with given config.
 * Returns pid on success, -1 on failure. */
static pid_t start_server_process(const char *data_dir, int port, const char *dims_str,
                                  int buffer_mb, int readonly, int read_workers) {
    const char *bin = find_server_bin();
    char port_str[16], buf_str[16], rw_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    snprintf(buf_str, sizeof(buf_str), "%d", buffer_mb);
    snprintf(rw_str, sizeof(rw_str), "%d", read_workers);

    pid_t pid = fork();
    if (pid < 0) {
        perror("[bench_scale] fork failed");
        return -1;
    }

    if (pid == 0) {
        /* Child: exec the server */
        if (readonly && read_workers > 0) {
            execlp(bin, bin,
                   "-d", data_dir,
                   "-p", port_str,
                   "-D", dims_str,
                   "-b", buf_str,
                   "--flush-ms", "0",
                   "--checkpoint-s", "0",
                   "--readonly",
                   "--read-workers", rw_str,
                   NULL);
        } else if (readonly) {
            execlp(bin, bin,
                   "-d", data_dir,
                   "-p", port_str,
                   "-D", dims_str,
                   "-b", buf_str,
                   "--flush-ms", "0",
                   "--checkpoint-s", "0",
                   "--readonly",
                   NULL);
        } else {
            execlp(bin, bin,
                   "-d", data_dir,
                   "-p", port_str,
                   "-D", dims_str,
                   "-b", buf_str,
                   "--flush-ms", "0",
                   "--checkpoint-s", "0",
                   NULL);
        }
        perror("[bench_scale] exec server failed");
        _exit(127);
    }

    /* Parent: wait for server to be ready (up to 10 seconds) */
    for (int i = 0; i < 100; i++) {
        usleep(100000); /* 100ms */
        GravelDBClient *probe = NULL;
        if (graveldb_client_connect(&probe, "127.0.0.1", port) == 0) {
            graveldb_client_close(probe);
            return pid;
        }
        /* Check if child died */
        int status;
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w > 0) {
            fprintf(stderr, "[bench_scale] Server process exited prematurely (status=%d)\n", status);
            return -1;
        }
    }

    fprintf(stderr, "[bench_scale] Server did not become ready in 10 seconds\n");
    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
    return -1;
}

static void stop_server_process(pid_t pid) {
    if (pid > 0) {
        kill(pid, SIGTERM);
        int status;
        waitpid(pid, &status, 0);
    }
}

/* Wait for client connection to succeed (with retries) */
static GravelDBClient *connect_with_retry(const char *host, int port, int max_retries) {
    GravelDBClient *c = NULL;
    for (int i = 0; i < max_retries; i++) {
        if (graveldb_client_connect(&c, host, port) == 0) return c;
        usleep(100000);
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * READONLY MODE BENCHMARK
 *
 * Tests multi-threaded lock-free reads in readonly mode.
 *
 * Strategy:
 *   1. Start RW server, push features, checkpoint, stop.
 *   2. Restart in readonly mode with varying --read-workers.
 *   3. Measure concurrent pull throughput with varying client counts.
 *
 * Output: a workers x clients throughput matrix.
 * ═══════════════════════════════════════════════════════════════════════════ */

#define RO_BENCH_DIR    "/tmp/graveldb_bench_readonly"
#define RO_BENCH_PORT   19528

#define DEFAULT_RO_PREPOP       50000
#define DEFAULT_RO_ITERS        0       /* 0 = time-driven */
#define DEFAULT_RO_DURATION     10      /* seconds per test point */
#define DEFAULT_RO_BATCH        128
#define DEFAULT_RO_DIM          128

static int RO_PREPOP_N;
static int RO_ITERS_PER_CLIENT;
static int RO_DURATION_SEC;
static int RO_BATCH_SZ;
static int RO_DIM;
static int RO_MAX_WORKERS;
static int RO_MAX_CLIENTS;

static void ro_parse_config(void) {
    int ncpu = detect_ncpu();

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
        RO_ITERS_PER_CLIENT = 0;
    } else {
        RO_PREPOP_N = DEFAULT_RO_PREPOP;
        RO_DURATION_SEC = DEFAULT_RO_DURATION;
        RO_BATCH_SZ = DEFAULT_RO_BATCH;
        RO_DIM = DEFAULT_RO_DIM;
        RO_ITERS_PER_CLIENT = DEFAULT_RO_ITERS;
    }

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

    double data_gb = (double)RO_PREPOP_N * RO_DIM * sizeof(float) / (1024.0 * 1024.0 * 1024.0);
    printf("  Dataset: %d features x dim%d = %.2f GB (raw embeddings)\n",
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

static int ro_prepopulate(void) {
    ro_cleanup();

    char dim_str[16];
    snprintf(dim_str, sizeof(dim_str), "%d", RO_DIM);

    /* Calculate buffer size in MB */
    size_t data_sz = (size_t)RO_PREPOP_N * RO_DIM * sizeof(float);
    int buf_mb = 512;
    if (data_sz > (size_t)buf_mb * 1024 * 1024)
        buf_mb = (int)((data_sz + 256ULL * 1024 * 1024) / (1024 * 1024));

    pid_t srv = start_server_process(RO_BENCH_DIR, RO_BENCH_PORT, dim_str, buf_mb, 0, 0);
    if (srv < 0) {
        fprintf(stderr, "[ro_prepop] Failed to start server\n");
        return -1;
    }

    GravelDBClient *client = connect_with_retry("127.0.0.1", RO_BENCH_PORT, 10);
    if (!client) {
        fprintf(stderr, "[ro_prepop] Failed to connect\n");
        stop_server_process(srv);
        return -1;
    }

    int push_batch = (RO_PREPOP_N >= 1000000) ? 4096 : 1024;
    uint64_t *fids = (uint64_t *)malloc(push_batch * sizeof(uint64_t));
    int *push_dims = (int *)malloc(push_batch * sizeof(int));
    float **embs = (float **)malloc(push_batch * sizeof(float *));
    float *pdata = (float *)malloc((size_t)push_batch * RO_DIM * sizeof(float));

    for (int i = 0; i < push_batch; i++) {
        push_dims[i] = RO_DIM;
        embs[i] = pdata + i * RO_DIM;
        for (int j = 0; j < RO_DIM; j++) embs[i][j] = 0.01f * ((i % 100) + 1);
    }

    double t0 = now_sec();
    int last_pct = -1;
    printf("  Pre-populating %d features (dim=%d, RW mode)...\n", RO_PREPOP_N, RO_DIM);
    fflush(stdout);

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

    printf("  Flushing + checkpoint...");
    fflush(stdout);
    graveldb_client_flush(client);
    graveldb_client_checkpoint(client);
    double prep_elapsed = now_sec() - t0;
    printf(" done. (%.1f sec, %.0f features/sec)\n", prep_elapsed, RO_PREPOP_N / prep_elapsed);

    graveldb_client_close(client);
    free(fids); free(push_dims); free(embs); free(pdata);

    stop_server_process(srv);
    usleep(200000); /* let port release */
    return 0;
}

static pid_t ro_start_server(int num_workers) {
    char dim_str[16];
    snprintf(dim_str, sizeof(dim_str), "%d", RO_DIM);

    size_t data_sz = (size_t)RO_PREPOP_N * RO_DIM * sizeof(float);
    int buf_mb = 512;
    if (data_sz > (size_t)buf_mb * 1024 * 1024)
        buf_mb = (int)((data_sz + 256ULL * 1024 * 1024) / (1024 * 1024));

    return start_server_process(RO_BENCH_DIR, RO_BENCH_PORT, dim_str, buf_mb, 1, num_workers);
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

    GravelDBClient *c = connect_with_retry("127.0.0.1", RO_BENCH_PORT, 10);
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

    if (iters > 0) {
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % RO_PREPOP_N) + 1;
            graveldb_client_pull(c, fids, batch_sz, outs, odims);
        }
        ta->result->elapsed = now_sec() - t0;
        ta->result->iters = iters;
    } else {
        int completed = 0;
        double t0 = now_sec();
        double deadline = t0 + RO_DURATION_SEC;
        while (now_sec() < deadline) {
            for (int j = 0; j < batch_sz; j++) fids[j] = (xorshift64(&rng) % RO_PREPOP_N) + 1;
            graveldb_client_pull(c, fids, batch_sz, outs, odims);
            completed++;
        }
        ta->result->elapsed = now_sec() - t0;
        ta->result->iters = completed;
    }
    ta->result->batch = batch_sz;

    free(fids); free(outs); free(data); free(odims);
    graveldb_client_close(c);
    return NULL;
}

static void bench_readonly_scalability(void) {
    ro_parse_config();

    printf("\n  GravelDB Readonly Mode Benchmark (lock-free multi-threaded)\n");
    printf("  dim=%d, batch=%d, iters/client=%d, features=%d\n",
           RO_DIM, RO_BATCH_SZ, RO_ITERS_PER_CLIENT, RO_PREPOP_N);
    printf("  max_workers=%d, max_clients=%d (ncpu=%d)\n",
           RO_MAX_WORKERS, RO_MAX_CLIENTS, detect_ncpu());
    printf("  (set RO_PREPOP/RO_ITERS/RO_BATCH/RO_MAX_WORKERS/RO_MAX_CLIENTS to tune)\n\n");

    printf("[RO 1/2] Prepopulating data (RW server)...\n");
    fflush(stdout);
    if (ro_prepopulate() != 0) {
        fprintf(stderr, "  Failed to prepopulate, skipping readonly bench.\n");
        return;
    }

    int worker_buf[16], client_buf[16];
    int num_wc = build_pow2_seq(worker_buf, RO_MAX_WORKERS);
    int num_cc = build_pow2_seq(client_buf, RO_MAX_CLIENTS);
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

        pid_t srv = ro_start_server(nw);
        if (srv < 0) {
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

        stop_server_process(srv);
        usleep(200000);
    }

    ro_cleanup();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MIXED-DIM BENCHMARK
 *
 * Simulates a real production embedding store where features have varying
 * dimensions (8, 16, 32, 64, 128, 256, 512, 1024) drawn from a Zipf
 * distribution.
 * ═══════════════════════════════════════════════════════════════════════════ */

#define MD_BENCH_DIR    "/tmp/graveldb_bench_mixdim"
#define MD_BENCH_PORT   19529

static const int MD_DIM_CANDIDATES[] = {8, 16, 32, 64, 128, 256, 512, 1024};
#define MD_NUM_DIMS (sizeof(MD_DIM_CANDIDATES) / sizeof(MD_DIM_CANDIDATES[0]))

static int MD_PREPOP_N;
static int MD_DURATION_SEC;
static int MD_BATCH_SZ;
static int MD_MAX_WORKERS;
static int MD_MAX_CLIENTS;
static double MD_ZIPF_ALPHA;

static int *MD_FEAT_DIMS = NULL;
static double MD_DIM_CDF[8];

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
    MD_DIM_CDF[MD_NUM_DIMS - 1] = 1.0;
}

static int md_zipf_sample(uint64_t *rng) {
    double u = (double)(xorshift64(rng) & 0xFFFFFFFF) / 4294967296.0;
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        if (u < MD_DIM_CDF[i]) return i;
    }
    return (int)MD_NUM_DIMS - 1;
}

static void md_parse_config(void) {
    int ncpu = detect_ncpu();

    MD_PREPOP_N = 500000;
    MD_DURATION_SEC = 10;
    MD_BATCH_SZ = 256;
    MD_ZIPF_ALPHA = 1.2;

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

    printf("  Dimension distribution (Zipf alpha=%.2f):\n", MD_ZIPF_ALPHA);
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        double pct = 100.0 * dim_counts[i] / MD_PREPOP_N;
        printf("    dim=%4d: %8d features (%5.1f%%)\n",
               MD_DIM_CANDIDATES[i], dim_counts[i], pct);
    }
    printf("  Total raw data: %.2f GB\n", total_bytes / (1024.0 * 1024.0 * 1024.0));
    return total_bytes;
}

/* Build comma-separated dims string for all MD_DIM_CANDIDATES */
static void md_build_dims_str(char *buf, size_t bufsz) {
    int pos = 0;
    for (int i = 0; i < (int)MD_NUM_DIMS; i++) {
        if (i > 0) pos += snprintf(buf + pos, bufsz - pos, ",");
        pos += snprintf(buf + pos, bufsz - pos, "%d", MD_DIM_CANDIDATES[i]);
    }
}

static int md_prepopulate(void) {
    md_cleanup();

    char dims_str[128];
    md_build_dims_str(dims_str, sizeof(dims_str));

    size_t data_sz = 0;
    for (int i = 0; i < MD_PREPOP_N; i++)
        data_sz += MD_FEAT_DIMS[i] * sizeof(float);
    int buf_mb = 512;
    if (data_sz > (size_t)buf_mb * 1024 * 1024)
        buf_mb = (int)((data_sz + 256ULL * 1024 * 1024) / (1024 * 1024));

    pid_t srv = start_server_process(MD_BENCH_DIR, MD_BENCH_PORT, dims_str, buf_mb, 0, 0);
    if (srv < 0) {
        fprintf(stderr, "[md_prepop] Failed to start server\n");
        return -1;
    }

    GravelDBClient *client = connect_with_retry("127.0.0.1", MD_BENCH_PORT, 10);
    if (!client) {
        fprintf(stderr, "[md_prepop] Failed to connect\n");
        stop_server_process(srv);
        return -1;
    }

    int push_batch = (MD_PREPOP_N >= 1000000) ? 2048 : 512;
    int max_dim = 1024;
    uint64_t *fids = (uint64_t *)malloc(push_batch * sizeof(uint64_t));
    int *pdims = (int *)malloc(push_batch * sizeof(int));
    float **embs = (float **)malloc(push_batch * sizeof(float *));
    float *pool = (float *)malloc((size_t)push_batch * max_dim * sizeof(float));

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
            embs[i] = pool + (i % push_batch) * max_dim;
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
    stop_server_process(srv);
    usleep(200000);
    return 0;
}

static pid_t md_start_server(int num_workers) {
    char dims_str[128];
    md_build_dims_str(dims_str, sizeof(dims_str));

    size_t est_data = 0;
    for (int i = 0; i < MD_PREPOP_N; i++) est_data += MD_FEAT_DIMS[i] * sizeof(float);
    int buf_mb = 512;
    if (est_data > (size_t)buf_mb * 1024 * 1024)
        buf_mb = (int)((est_data + 256ULL * 1024 * 1024) / (1024 * 1024));

    return start_server_process(MD_BENCH_DIR, MD_BENCH_PORT, dims_str, buf_mb, 1, num_workers);
}

typedef struct {
    int             batch;
    int             duration_sec;
    int             thread_id;
    double          elapsed;
    int64_t         total_features;
    int64_t         total_bytes;
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

    GravelDBClient *c = connect_with_retry("127.0.0.1", MD_BENCH_PORT, 10);
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

    printf("\n  GravelDB Mixed-Dimension Benchmark (Zipf distribution)\n");
    printf("  dims=8..1024, batch=%d, duration=%ds/point, features=%d\n",
           MD_BATCH_SZ, MD_DURATION_SEC, MD_PREPOP_N);
    printf("  max_workers=%d, max_clients=%d, zipf_alpha=%.2f\n",
           MD_MAX_WORKERS, MD_MAX_CLIENTS, MD_ZIPF_ALPHA);
    printf("  (set MD_PREPOP/MD_DURATION/MD_BATCH/MD_ZIPF_ALPHA/MD_SCALE to tune)\n\n");

    size_t total_data = md_assign_dims();
    (void)total_data;
    printf("\n");

    printf("[MD 1/2] Prepopulating data (RW server)...\n");
    fflush(stdout);
    if (md_prepopulate() != 0) {
        fprintf(stderr, "  Failed to prepopulate, skipping mixed-dim bench.\n");
        if (MD_FEAT_DIMS) { free(MD_FEAT_DIMS); MD_FEAT_DIMS = NULL; }
        return;
    }

    int worker_buf[16], client_buf[16];
    int num_wc = build_pow2_seq(worker_buf, MD_MAX_WORKERS);
    int num_cc = build_pow2_seq(client_buf, MD_MAX_CLIENTS);
    int worker_counts[17];
    worker_counts[0] = 0;
    for (int i = 0; i < num_wc; i++) worker_counts[i + 1] = worker_buf[i];
    num_wc += 1;

    printf("\n[MD 2/2] Throughput matrix (time-driven, %ds/point):\n\n", MD_DURATION_SEC);

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
        pid_t srv = md_start_server(nw);
        if (srv < 0) {
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

        if (last_bw > 0) {
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

        stop_server_process(srv);
        usleep(200000);
    }

    md_cleanup();
    if (MD_FEAT_DIMS) { free(MD_FEAT_DIMS); MD_FEAT_DIMS = NULL; }
}

/* Main */
int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    printf("GravelDB Network Scalability Benchmark\n");
    printf("=======================================\n");

    /* Allow selecting which bench to run via argv[1]: "ro", "md", or both (default) */
    int run_ro = 1, run_md = 1;
    if (argc > 1) {
        if (strcmp(argv[1], "ro") == 0 || strcmp(argv[1], "readonly") == 0) {
            run_md = 0;
        } else if (strcmp(argv[1], "md") == 0 || strcmp(argv[1], "mixdim") == 0) {
            run_ro = 0;
        }
    }

    double t0 = now_sec();

    if (run_ro) {
        printf("\n[1] Readonly mode scalability...\n");
        fflush(stdout);
        bench_readonly_scalability();
    }

    if (run_md) {
        printf("\n[2] Mixed-dimension scalability (Zipf distribution)...\n");
        fflush(stdout);
        bench_mixed_dim_scalability();
    }

    double elapsed = now_sec() - t0;
    printf("\n=======================================\n");
    printf("  Scalability benchmarks complete. Total time: %.1f s\n", elapsed);
    printf("=======================================\n");

    return 0;
}
