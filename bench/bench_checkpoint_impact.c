/*
 * bench_checkpoint_impact - read-latency tail when checkpoint runs concurrently.
 *
 * Question answered: "If checkpoint kicks in while my service is serving
 * reads, how much does the tail latency move?"
 *
 * Workflow:
 *   1. Open db, preload N records, flush + initial checkpoint.
 *   2. Spawn T reader threads (continuous batch_get, latency recorded).
 *   3. Main thread alternates:
 *        - "quiet" window of --interval seconds (no checkpoint),
 *        - call graveldb_checkpoint() and time it,
 *      until --duration elapses.
 *   4. Each read's latency is bucketed into one of two histograms based
 *      on whether checkpoint was in progress when it landed.
 *   5. Print both histograms side by side -- the gap tells the story.
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
    volatile int stop;
    volatile int ckpt_active; /* set by main thread */

    BenchHist    quiet;
    BenchHist    during;
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
        int active_before = __atomic_load_n(&r->ckpt_active, __ATOMIC_ACQUIRE);
        uint64_t t0 = bench_now_ns();
        graveldb_status_t rc = graveldb_batch_get(r->db, NULL, ids, r->batch,
                                                  out_bufs, out_dims);
        uint64_t t1 = bench_now_ns();
        int active_after = __atomic_load_n(&r->ckpt_active, __ATOMIC_ACQUIRE);
        if (rc != GRAVELDB_OK) continue;
        /* Only count the sample if the bucket was unambiguous for the whole call. */
        if (active_before == active_after) {
            if (active_before) bench_hist_record(&r->during, t1 - t0);
            else               bench_hist_record(&r->quiet,  t1 - t0);
        }
    }

    for (int i = 0; i < r->batch; i++) free(out_bufs[i]);
    free(out_bufs); free(out_dims); free(ids);
    return NULL;
}

static void preload(GravelDB *db, uint64_t n, int dim) {
    printf("[preload] writing %llu records, dim=%d ...\n",
           (unsigned long long)n, dim);
    float *emb = malloc(dim * sizeof(float));
    for (uint64_t i = 1; i <= n; i++) {
        for (int j = 0; j < dim; j++) emb[j] = (float)i + (float)j * 0.001f;
        const float *eptr = emb;
        int d = dim;
        uint64_t fid = i;
        assert(graveldb_batch_put(db, NULL, &fid, &d, &eptr, 1) == GRAVELDB_OK);
    }
    free(emb);
    assert(graveldb_flush(db) == GRAVELDB_OK);
    assert(graveldb_checkpoint(db) == GRAVELDB_OK);
    printf("[preload] done\n");
}

int main(int argc, char **argv) {
    const char *data_dir = bench_arg_str (argc, argv, "--data-dir", "/tmp/graveldb_bench_ckpt");
    long        threads  = bench_arg_long(argc, argv, "--threads",  4);
    long        duration = bench_arg_long(argc, argv, "--duration", 30);
    long        records  = bench_arg_long(argc, argv, "--records",  500000);
    long        dim      = bench_arg_long(argc, argv, "--dim",      128);
    long        batch    = bench_arg_long(argc, argv, "--batch",    32);
    long        interval = bench_arg_long(argc, argv, "--interval", 5);

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", data_dir);
    if (system(rmcmd) != 0) { /* tolerated */ }

    GravelDBConfig cfg = {0};
    cfg.data_dir = data_dir;
    int dims[1] = { (int)dim };
    cfg.dims = dims;
    cfg.num_dims = 1;
    cfg.buffer_size = 256ull * 1024 * 1024;
    cfg.index_capacity = 1u << 22;
    GravelDB *db = NULL;
    assert(graveldb_open(&db, &cfg) == GRAVELDB_OK);

    preload(db, (uint64_t)records, (int)dim);

    /* Generate some dirty pages so the checkpoint has work to do. */
    {
        float *emb = malloc(dim * sizeof(float));
        for (uint64_t i = 1; i <= (uint64_t)records / 4; i++) {
            for (int j = 0; j < dim; j++) emb[j] = (float)(i + 1);
            const float *eptr = emb;
            int d = dim;
            uint64_t fid = i;
            assert(graveldb_batch_put(db, NULL, &fid, &d, &eptr, 1) == GRAVELDB_OK);
        }
        free(emb);
    }

    printf("\n[bench_checkpoint_impact]\n");
    printf("  threads=%ld batch=%ld dim=%ld duration=%lds interval=%lds records=%ld\n\n",
           threads, batch, dim, duration, interval, records);

    ReaderCtx *ctxs = calloc(threads, sizeof(ReaderCtx));
    pthread_t *tids = calloc(threads, sizeof(pthread_t));
    for (long i = 0; i < threads; i++) {
        ctxs[i].db = db;
        ctxs[i].n_records = (uint64_t)records;
        ctxs[i].dim = (int)dim;
        ctxs[i].batch = (int)batch;
        bench_hist_init(&ctxs[i].quiet);
        bench_hist_init(&ctxs[i].during);
        pthread_create(&tids[i], NULL, reader_thread, &ctxs[i]);
    }

    /* Main thread: alternate quiet -> checkpoint -> quiet -> checkpoint. */
    uint64_t t_start = bench_now_ns();
    uint64_t t_end   = t_start + (uint64_t)duration * 1000000000ull;
    int ckpt_count = 0;
    BenchHist ckpt_durations; bench_hist_init(&ckpt_durations);

    while (bench_now_ns() < t_end) {
        sleep((unsigned int)interval);
        if (bench_now_ns() >= t_end) break;

        /* Mark "checkpoint active" for all readers, run it, mark inactive. */
        for (long i = 0; i < threads; i++) {
            __atomic_store_n(&ctxs[i].ckpt_active, 1, __ATOMIC_RELEASE);
        }
        uint64_t c0 = bench_now_ns();
        graveldb_status_t rc = graveldb_checkpoint(db);
        uint64_t c1 = bench_now_ns();
        for (long i = 0; i < threads; i++) {
            __atomic_store_n(&ctxs[i].ckpt_active, 0, __ATOMIC_RELEASE);
        }
        if (rc == GRAVELDB_OK) {
            bench_hist_record(&ckpt_durations, c1 - c0);
            ckpt_count++;
            printf("  [ckpt #%d done in %.2f ms]\n",
                   ckpt_count, (c1 - c0) / 1e6);
        }

        /* Re-dirty some pages so the next checkpoint isn't a no-op. */
        float *emb = malloc(dim * sizeof(float));
        unsigned int seed = (unsigned int)c1;
        for (int k = 0; k < 50000; k++) {
            uint64_t fid = (uint64_t)((rand_r(&seed) % records) + 1);
            for (int j = 0; j < dim; j++) emb[j] = (float)(fid + k);
            const float *eptr = emb;
            int d = dim;
            graveldb_batch_put(db, NULL, &fid, &d, &eptr, 1);
        }
        free(emb);
    }

    for (long i = 0; i < threads; i++) ctxs[i].stop = 1;
    for (long i = 0; i < threads; i++) pthread_join(tids[i], NULL);

    BenchHist agg_quiet, agg_during;
    bench_hist_init(&agg_quiet); bench_hist_init(&agg_during);
    for (long i = 0; i < threads; i++) {
        bench_hist_merge(&agg_quiet,  &ctxs[i].quiet);
        bench_hist_merge(&agg_during, &ctxs[i].during);
    }

    printf("\n[result]\n");
    printf("  checkpoints run : %d\n", ckpt_count);
    bench_print_latency_summary("ckpt duration",     &ckpt_durations);
    bench_print_latency_summary("read (quiet)",      &agg_quiet);
    bench_print_latency_summary("read (during ckpt)", &agg_during);

    if (agg_quiet.total > 0 && agg_during.total > 0) {
        double q99  = (double)bench_hist_percentile(&agg_quiet,  0.99);
        double d99  = (double)bench_hist_percentile(&agg_during, 0.99);
        double q999 = (double)bench_hist_percentile(&agg_quiet,  0.999);
        double d999 = (double)bench_hist_percentile(&agg_during, 0.999);
        printf("\n  p99   tail-amplification (during/quiet): %.2fx\n", d99  / q99);
        printf("  p99.9 tail-amplification (during/quiet): %.2fx\n", d999 / q999);
    }

    graveldb_close(db);
    free(ctxs); free(tids);
    return 0;
}
