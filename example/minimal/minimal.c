/*
 * GravelDB Minimal Example
 *
 * Demonstrates the simplest possible usage:
 *   open → put → get → checkpoint → close
 *
 * Build:
 *   cc -o minimal minimal.c -lgraveldb -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "graveldb.h"

#define DIM 128
#define NUM_FEATURES 10

int main(void) {
    /* Configuration */
    int dims[] = {DIM};
    GravelDBConfig config = {
        .data_dir       = "/tmp/graveldb_minimal",
        .dims           = dims,
        .num_dims       = 1,
        .buffer_size    = 64 * 1024 * 1024,  /* 64MB */
        .index_capacity = 1 << 16,           /* 64K slots */
        .auto_create_bins = true,
    };

    /* Open */
    GravelDB *db = NULL;
    graveldb_status_t st = graveldb_open(&db, &config);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "open failed: %d\n", st);
        return 1;
    }
    printf("opened graveldb at %s\n", config.data_dir);

    /* Batch put: 10 features of dim=128 */
    uint64_t feat_ids[NUM_FEATURES];
    int put_dims[NUM_FEATURES];
    float embeddings[NUM_FEATURES][DIM];
    const float *emb_ptrs[NUM_FEATURES];

    for (int i = 0; i < NUM_FEATURES; i++) {
        feat_ids[i] = 1000 + i;
        put_dims[i] = DIM;
        for (int d = 0; d < DIM; d++)
            embeddings[i][d] = (float)(i * DIM + d) * 0.001f;
        emb_ptrs[i] = embeddings[i];
    }

    st = graveldb_batch_put(db, NULL, feat_ids, put_dims, emb_ptrs, NUM_FEATURES);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "batch_put failed: %d\n", st);
        goto cleanup;
    }
    printf("put %d features (dim=%d)\n", NUM_FEATURES, DIM);

    /* Batch get: retrieve them back */
    float *out_embeddings[NUM_FEATURES];
    int out_dims[NUM_FEATURES];
    memset(out_embeddings, 0, sizeof(out_embeddings));

    st = graveldb_batch_get(db, NULL, feat_ids, NUM_FEATURES, out_embeddings, out_dims);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "batch_get failed: %d\n", st);
        goto cleanup;
    }

    /* Verify first feature */
    printf("get feat_id=%lu dim=%d first_val=%.4f\n",
           (unsigned long)feat_ids[0], out_dims[0], out_embeddings[0][0]);

    /* Checkpoint: persist to disk */
    st = graveldb_checkpoint(db);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "checkpoint failed: %d\n", st);
        goto cleanup;
    }
    printf("checkpoint done\n");

    /* Stats */
    GravelDBStats stats;
    graveldb_stats(db, &stats);
    printf("total_features=%lu generation=%lu\n",
           (unsigned long)stats.total_features,
           (unsigned long)stats.checkpoint_generation);

cleanup:
    graveldb_close(db);
    printf("closed\n");
    return (st == GRAVELDB_OK) ? 0 : 1;
}
