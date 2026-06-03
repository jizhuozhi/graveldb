/*
 * bench_read_latency - read latency distribution under multi-threaded load.
 *
 * Question answered: "If I serve reads from this engine, what does the tail
 * look like at steady state?"
 *
 * Workflow:
 *   1. Open db, preload N records of given dim, flush + checkpoint to disk.
 *   2. Spawn T reader threads. Each thread loops: pick random keys, run
 *      batch_get of size B, record per-call latency.
 *   3. After --duration seconds, stop and print combined histogram.
 *
 * Defaults are tuned to "show a useful result on a laptop in under 30s".
 */

#undef NDEBUG
#include "graveldb.h"
#include "bench_common.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

typedef struct {
    GravelDB    *db;
    uint64_t     n_records;
    int          dim;
    int          batch;
    int          duration_sec;
    volatile int stop;

    BenchHist    hist;
    uint64_t     ops;
} ReaderCtx;

static void *reader_thread(void *arg) {
    ReaderCtx *r = arg;

    uint64_t *ids       = malloc(r->batch * sizeof(uint64_t));
    float   **out_bufs  = malloc(r->batch * sizeof(float *));
    int      *out_dims  = malloc(r->batch * sizeof(int));
    for (int i = 0; i < r->batch; i++) out_bufs[i] = malloc(r->dim * sizeof(float));

    unsigned int seed = (unsigned int)(uintptr_t)arg ^ (unsigned int)bench_now_ns();

    while (!r->stop) {
        for (int i = 0; i < r->batch; i++) {
            ids[i] = (uint64_t)((rand_r(&seed) % r->n_records) + 1);
        }
        uint64_t t0 = bench_now_ns();
        graveldb_status_t rc = graveldb_batch_get(r->db, NULL, ids, r->batch,
                                                  out_bufs, out_dims);
        uint64_t t1 = bench_now_ns();
        if (rc != GRAVELDB_OK) continue;
        bench_hist_record(&r->hist, t1 - t0);
        r->ops++;
    }

    for (int i = 0; i < r->batch; i++) free(out_bufs[i]);
    free(out_bufs); free(out_dims); free(ids);
    return NULL;
}

static void preload(GravelDB *db, uint64_t n, int dim) {
    printf("[preload] writing %llu records, dim=%d ...\n",
           (unsigned long long)n, dim);
    uint64_t t0 = bench_now_ns();

    float *emb = malloc(dim * sizeof(float));
    for (uint64_t i = 1; i <= n; i++) {
        for (int j = 0; j < dim; j++) emb[j] = (float)i + (float)j * 0.001f;
        const float *eptr = emb;
        int d = dim;
        uint64_t fid = i;
        graveldb_status_t rc = graveldb_batch_put(db, NULL, &fid, &d, &eptr, 1);
        if (rc != GRAVELDB_OK) {
            fprintf(stderr, "preload put failed at i=%llu rc=%d\n",
                    (unsigned long long)i, rc);
            exit(1);
        }
        if (i % 100000 == 0) {
            printf("  ... %llu\n", (unsigned long long)i);
        }
    }
    free(emb);

    assert(graveldb_flush(db) == GRAVELDB_OK);
    assert(graveldb_checkpoint(db) == GRAVELDB_OK);

    uint64_t t1 = bench_now_ns();
    printf("[preload] done in %.2f s\n", (t1 - t0) / 1e9);
}

int main(int argc, char **argv) {
    const char *data_dir   = bench_arg_str (argc, argv, "--data-dir",  "/tmp/graveldb_bench_read");
    long        threads    = bench_arg_long(argc, argv, "--threads",   4);
    long        duration   = bench_arg_long(argc, argv, "--duration",  10);
    long        records    = bench_arg_long(argc, argv, "--records",   1000000);
    long        dim        = bench_arg_long(argc, argv, "--dim",       128);
    long        batch      = bench_arg_long(argc, argv, "--batch",     32);
    long        skip_load  = bench_arg_long(argc, argv, "--skip-load", 0);

    char rmcmd[512];
    if (!skip_load) {
        snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", data_dir);
        if (system(rmcmd) != 0) { /* tolerated */ }
    }

    GravelDBConfig cfg = {0};
    cfg.data_dir = data_dir;
    int dims[1] = { (int)dim };
    cfg.dims = dims;
    cfg.num_dims = 1;
    cfg.buffer_size = 256ull * 1024 * 1024;
    cfg.index_capacity = 1u << 22;
    GravelDB *db = NULL;
    assert(graveldb_open(&db, &cfg) == GRAVELDB_OK);

    if (!skip_load) preload(db, (uint64_t)records, (int)dim);

    printf("\n[bench_read_latency]\n");
    printf("  threads=%ld batch=%ld dim=%ld duration=%lds records=%ld\n\n",
           threads, batch, dim, duration, records);

    ReaderCtx *ctxs = calloc(threads, sizeof(ReaderCtx));
    pthread_t *tids = calloc(threads, sizeof(pthread_t));
    for (long i = 0; i < threads; i++) {
        ctxs[i].db = db;
        ctxs[i].n_records = (uint64_t)records;
        ctxs[i].dim = (int)dim;
        ctxs[i].batch = (int)batch;
        ctxs[i].duration_sec = (int)duration;
        bench_hist_init(&ctxs[i].hist);
        pthread_create(&tids[i], NULL, reader_thread, &ctxs[i]);
    }

    sleep((unsigned int)duration);
    for (long i = 0; i < threads; i++) ctxs[i].stop = 1;
    for (long i = 0; i < threads; i++) pthread_join(tids[i], NULL);

    BenchHist agg; bench_hist_init(&agg);
    uint64_t total_ops = 0;
    for (long i = 0; i < threads; i++) {
        bench_hist_merge(&agg, &ctxs[i].hist);
        total_ops += ctxs[i].ops;
    }

    printf("[result]\n");
    printf("  batch_get calls : %llu\n", (unsigned long long)total_ops);
    printf("  keys/sec        : %.0f\n",
           (double)(total_ops * (uint64_t)batch) / (double)duration);
    printf("  calls/sec       : %.0f\n", (double)total_ops / (double)duration);
    bench_print_latency_summary("batch_get latency", &agg);

    graveldb_close(db);
    free(ctxs); free(tids);
    return 0;
}
