/*
 * bench_write_throughput - sustained single-writer throughput.
 *
 * Question answered: "How fast can I bulk-load this engine, and is the
 * write rate stable over time or does it collapse?"
 *
 * Workflow:
 *   1. Open empty db.
 *   2. Single writer thread (reflects the engine's single-writer contract)
 *      issues batch_put of size B in a tight loop.
 *   3. Sample throughput once per second to expose stalls / collapse.
 *   4. After --duration seconds, print per-second curve + overall summary.
 */

#undef NDEBUG
#include "graveldb.h"
#include "bench_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

int main(int argc, char **argv) {
    const char *data_dir = bench_arg_str (argc, argv, "--data-dir", "/tmp/graveldb_bench_write");
    long        duration = bench_arg_long(argc, argv, "--duration", 30);
    long        batch    = bench_arg_long(argc, argv, "--batch",    256);
    long        dim      = bench_arg_long(argc, argv, "--dim",      128);

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

    printf("[bench_write_throughput]\n");
    printf("  batch=%ld dim=%ld duration=%lds  bytes/record=%ld\n\n",
           batch, dim, duration, dim * (long)sizeof(float));

    /* Pre-allocate batch buffers; we recycle them every iteration. */
    uint64_t *ids   = malloc(batch * sizeof(uint64_t));
    int      *bdims = malloc(batch * sizeof(int));
    float   **embs  = malloc(batch * sizeof(float *));
    for (long i = 0; i < batch; i++) {
        embs[i] = malloc(dim * sizeof(float));
        bdims[i] = (int)dim;
        for (long j = 0; j < dim; j++) embs[i][j] = (float)j * 0.01f;
    }

    uint64_t next_id = 1;
    uint64_t t_start = bench_now_ns();
    uint64_t t_end   = t_start + (uint64_t)duration * 1000000000ull;

    /* Per-second counters. */
    long  sec_records[duration + 4];
    memset(sec_records, 0, sizeof(sec_records));
    int   cur_sec = 0;
    uint64_t sec_boundary = t_start + 1000000000ull;

    uint64_t total_records = 0;
    uint64_t total_ops = 0;

    while (1) {
        uint64_t now = bench_now_ns();
        if (now >= t_end) break;
        if (now >= sec_boundary && cur_sec < duration) {
            cur_sec++;
            sec_boundary += 1000000000ull;
        }

        for (long i = 0; i < batch; i++) ids[i] = next_id++;
        graveldb_status_t rc = graveldb_batch_put(db, NULL, ids, bdims,
                                                  (const float *const *)embs,
                                                  (int)batch);
        if (rc == GRAVELDB_OK) {
            total_records += (uint64_t)batch;
            total_ops++;
            if (cur_sec < duration) sec_records[cur_sec] += batch;
        } else if (rc == GRAVELDB_ERR_BUSY) {
            /* Overlay budget hit: drive checkpoint forward and retry. */
            graveldb_checkpoint_step(db, 256);
            next_id -= batch; /* don't burn ids on a failed put */
        } else if (rc == GRAVELDB_FLUSH_NEEDED) {
            graveldb_flush(db);
            next_id -= batch;
        } else {
            fprintf(stderr, "put failed rc=%d at id=%llu\n",
                    rc, (unsigned long long)next_id);
            break;
        }
    }

    uint64_t t_done = bench_now_ns();
    double elapsed = (t_done - t_start) / 1e9;
    double bytes_per_record = (double)dim * sizeof(float) + 8.0;

    printf("[per-second throughput]\n");
    printf("  %4s %14s %12s\n", "sec", "records", "MB/s");
    for (int s = 0; s < (int)duration && s < cur_sec + 1; s++) {
        double mbs = (double)sec_records[s] * bytes_per_record / (1024.0 * 1024.0);
        printf("  %4d %14ld %12.1f\n", s, sec_records[s], mbs);
    }

    printf("\n[overall]\n");
    printf("  elapsed         : %.2f s\n", elapsed);
    printf("  total records   : %llu\n", (unsigned long long)total_records);
    printf("  records/sec     : %.0f\n", (double)total_records / elapsed);
    printf("  put calls       : %llu\n", (unsigned long long)total_ops);
    printf("  throughput      : %.1f MB/s\n",
           (double)total_records * bytes_per_record / (1024.0 * 1024.0) / elapsed);

    for (long i = 0; i < batch; i++) free(embs[i]);
    free(embs); free(bdims); free(ids);
    graveldb_close(db);
    return 0;
}
