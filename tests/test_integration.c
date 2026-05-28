/*
 * GravelDB - Integration Tests (advanced scenarios)
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#define TEST_DIR "/tmp/graveldb_integ_test"

static void cleanup_test_dir(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);
}

static GravelDB *open_db(const int *dims, int num_dims, uint32_t index_cap) {
    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    config.dims = dims;
    config.num_dims = num_dims;
    config.buffer_size = 16 * 1024 * 1024;
    config.index_capacity = index_cap;
    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);
    return db;
}

/* ─── Batch Operations ─── */

static void test_batch_put_get(void) {
    printf("  test_batch_put_get...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 1024);

    int n = 100;
    uint64_t *ids = (uint64_t *)malloc(n * sizeof(uint64_t));
    int *bdims = (int *)malloc(n * sizeof(int));
    float **embeddings = (float **)malloc(n * sizeof(float *));

    for (int i = 0; i < n; i++) {
        ids[i] = (uint64_t)(i + 1);
        bdims[i] = 32;
        embeddings[i] = (float *)malloc(32 * sizeof(float));
        for (int j = 0; j < 32; j++) embeddings[i][j] = (float)(i * 32 + j);
    }

    graveldb_status_t rc = graveldb_batch_put(db, NULL, ids, bdims, (const float *const *)embeddings, n);
    assert(rc == GRAVELDB_OK);

    float **out_bufs = (float **)malloc(n * sizeof(float *));
    int *out_dims_arr = (int *)malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        out_bufs[i] = (float *)malloc(32 * sizeof(float));
    }

    rc = graveldb_batch_get(db, NULL, ids, n, out_bufs, out_dims_arr);
    assert(rc == GRAVELDB_OK);

    for (int i = 0; i < n; i++) {
        assert(out_dims_arr[i] == 32);
        for (int j = 0; j < 32; j++) {
            assert(fabsf(out_bufs[i][j] - (float)(i * 32 + j)) < 1e-6f);
        }
    }

    for (int i = 0; i < n; i++) free(out_bufs[i]);
    free(out_bufs);
    free(out_dims_arr);
    for (int i = 0; i < n; i++) free(embeddings[i]);
    free(embeddings);
    free(bdims);
    free(ids);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_batch_put_mixed_dims(void) {
    printf("  test_batch_put_mixed_dims...");
    cleanup_test_dir();

    int dims[] = {32, 64, 128};
    GravelDB *db = open_db(dims, 3, 4096);

    int n = 300;
    uint64_t *ids = (uint64_t *)malloc(n * sizeof(uint64_t));
    int *bdims = (int *)malloc(n * sizeof(int));
    float **embeddings = (float **)malloc(n * sizeof(float *));

    int dim_choices[] = {32, 64, 128};
    for (int i = 0; i < n; i++) {
        ids[i] = (uint64_t)(i + 1);
        bdims[i] = dim_choices[i % 3];
        embeddings[i] = (float *)malloc(bdims[i] * sizeof(float));
        for (int j = 0; j < bdims[i]; j++) embeddings[i][j] = (float)(i + j);
    }

    graveldb_status_t rc = graveldb_batch_put(db, NULL, ids, bdims, (const float *const *)embeddings, n);
    assert(rc == GRAVELDB_OK);

    for (int i = 0; i < n; i++) {
        float out[128];
        int out_dim;
        rc = graveldb_get(db, NULL, ids[i], out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(out_dim == bdims[i]);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }

    for (int i = 0; i < n; i++) free(embeddings[i]);
    free(embeddings);
    free(bdims);
    free(ids);
    graveldb_close(db);
    printf(" PASS\n");
}

/* ─── Edge Cases ─── */

static void test_single_feature(void) {
    printf("  test_single_feature...");
    cleanup_test_dir();

    int dims[] = {4};
    GravelDB *db = open_db(dims, 1, 16);

    float emb[4] = {1.0f, 2.0f, 3.0f, 4.0f};
    assert(graveldb_put(db, NULL, 1, 4, emb) == GRAVELDB_OK);

    float out[4];
    int out_dim;
    assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_OK);
    assert(out_dim == 4);
    assert(fabsf(out[2] - 3.0f) < 1e-6f);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_large_dim(void) {
    printf("  test_large_dim...");
    cleanup_test_dir();

    int dims[] = {2048};
    GravelDB *db = open_db(dims, 1, 256);

    float *emb = (float *)malloc(2048 * sizeof(float));
    for (int i = 0; i < 2048; i++) emb[i] = (float)i * 0.001f;

    assert(graveldb_put(db, NULL, 42, 2048, emb) == GRAVELDB_OK);

    float *out = (float *)malloc(2048 * sizeof(float));
    int out_dim;
    assert(graveldb_get(db, NULL, 42, out, &out_dim) == GRAVELDB_OK);
    assert(out_dim == 2048);
    for (int i = 0; i < 2048; i++) {
        assert(fabsf(out[i] - emb[i]) < 1e-6f);
    }

    free(emb);
    free(out);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_rapid_update_same_key(void) {
    printf("  test_rapid_update_same_key...");
    cleanup_test_dir();

    int dims[] = {16};
    GravelDB *db = open_db(dims, 1, 256);

    for (int round = 0; round < 1000; round++) {
        float emb[16];
        for (int j = 0; j < 16; j++) emb[j] = (float)round;
        graveldb_put(db, NULL, 1, 16, emb);
    }

    float out[16];
    int out_dim;
    assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_OK);
    assert(fabsf(out[0] - 999.0f) < 1e-6f);

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == 1);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_delete_nonexistent(void) {
    printf("  test_delete_nonexistent...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 64);

    graveldb_status_t rc = graveldb_delete(db, NULL, 99999);
    assert(rc == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_put_delete_put_cycle(void) {
    printf("  test_put_delete_put_cycle...");
    cleanup_test_dir();

    int dims[] = {8};
    GravelDB *db = open_db(dims, 1, 512);

    for (int cycle = 0; cycle < 100; cycle++) {
        float emb[8];
        for (int j = 0; j < 8; j++) emb[j] = (float)cycle;
        assert(graveldb_put(db, NULL, 1, 8, emb) == GRAVELDB_OK);
        assert(graveldb_delete(db, NULL, 1) == GRAVELDB_OK);

        float out[8];
        int out_dim;
        assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_ERR_NOT_FOUND);
    }

    float emb[8] = {42.0f};
    graveldb_put(db, NULL, 1, 8, emb);
    float out[8];
    int out_dim;
    assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_OK);
    assert(fabsf(out[0] - 42.0f) < 1e-6f);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ─── Persistence & Recovery ─── */

static void test_checkpoint_then_recovery(void) {
    printf("  test_checkpoint_then_recovery...");
    cleanup_test_dir();

    int dims[] = {32, 64};
    int n = 500;

    {
        GravelDB *db = open_db(dims, 2, 4096);
        for (int i = 1; i <= n; i++) {
            int dim = (i % 2 == 0) ? 64 : 32;
            float *emb = (float *)calloc(dim, sizeof(float));
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 10 + j);
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
            free(emb);
        }
        graveldb_checkpoint(db);
        graveldb_close(db);
    }

    {
        GravelDB *db = open_db(dims, 2, 4096);
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == (uint64_t)n);

        for (int i = 1; i <= n; i++) {
            int dim = (i % 2 == 0) ? 64 : 32;
            float *out = (float *)calloc(dim, sizeof(float));
            int out_dim;
            graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_OK);
            assert(out_dim == dim);
            assert(fabsf(out[0] - (float)(i * 10)) < 1e-6f);
            free(out);
        }
        graveldb_close(db);
    }

    printf(" PASS\n");
}

static void test_multiple_checkpoints(void) {
    printf("  test_multiple_checkpoints...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 4096);

    for (int gen = 0; gen < 5; gen++) {
        for (int i = 1; i <= 100; i++) {
            uint64_t id = (uint64_t)(gen * 100 + i);
            float emb[32];
            for (int j = 0; j < 32; j++) emb[j] = (float)id;
            graveldb_put(db, NULL, id, 32, emb);
        }
        graveldb_status_t rc = graveldb_checkpoint(db);
        assert(rc == GRAVELDB_OK);
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == 500);
    assert(stats.checkpoint_generation == 5);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_flush_then_reopen(void) {
    printf("  test_flush_then_reopen...");
    cleanup_test_dir();

    int dims[] = {16};

    {
        GravelDB *db = open_db(dims, 1, 1024);
        for (int i = 1; i <= 200; i++) {
            float emb[16];
            for (int j = 0; j < 16; j++) emb[j] = (float)(i * 100 + j);
            graveldb_put(db, NULL, (uint64_t)i, 16, emb);
        }
        graveldb_flush(db);
        graveldb_close(db);
    }

    {
        GravelDB *db = open_db(dims, 1, 1024);
        for (int i = 1; i <= 200; i++) {
            float out[16];
            int out_dim;
            graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_OK);
            assert(out_dim == 16);
            assert(fabsf(out[0] - (float)(i * 100)) < 1e-6f);
            assert(fabsf(out[15] - (float)(i * 100 + 15)) < 1e-6f);
        }
        graveldb_close(db);
    }

    printf(" PASS\n");
}

/* ─── Stats ─── */

static void test_stats_accuracy(void) {
    printf("  test_stats_accuracy...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 1024);

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == 0);

    for (int i = 1; i <= 50; i++) {
        float emb[32] = {0};
        graveldb_put(db, NULL, (uint64_t)i, 32, emb);
    }
    graveldb_stats(db, &stats);
    assert(stats.total_features == 50);

    for (int i = 1; i <= 20; i++) {
        graveldb_delete(db, NULL, (uint64_t)i);
    }
    graveldb_stats(db, &stats);
    assert(stats.total_features == 30);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ─── Stress ─── */

static void test_many_features(void) {
    printf("  test_many_features...");
    cleanup_test_dir();

    int dims[] = {64};
    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 64 * 1024 * 1024;
    config.index_capacity = 65536;

    GravelDB *db = NULL;
    assert(graveldb_open(&db, &config) == GRAVELDB_OK);

    int n = 10000;
    for (int i = 1; i <= n; i++) {
        float emb[64];
        for (int j = 0; j < 64; j++) emb[j] = (float)(i + j);
        assert(graveldb_put(db, NULL, (uint64_t)i, 64, emb) == GRAVELDB_OK);
    }

    for (int i = 1; i <= n; i++) {
        float out[64];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
        assert(out_dim == 64);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)n);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_interleaved_put_delete(void) {
    printf("  test_interleaved_put_delete...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 4096);

    for (int i = 1; i <= 1000; i++) {
        float emb[32];
        for (int j = 0; j < 32; j++) emb[j] = (float)i;
        graveldb_put(db, NULL, (uint64_t)i, 32, emb);

        if (i > 100 && i % 3 == 0) {
            graveldb_delete(db, NULL, (uint64_t)(i - 100));
        }
    }

    int found = 0;
    for (int i = 1; i <= 1000; i++) {
        float out[32];
        int out_dim;
        if (graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK) {
            found++;
        }
    }
    assert(found > 0 && found < 1000);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ─── Special Values ─── */

static void test_zero_embedding(void) {
    printf("  test_zero_embedding...");
    cleanup_test_dir();

    int dims[] = {32};
    GravelDB *db = open_db(dims, 1, 64);

    float emb[32] = {0};
    assert(graveldb_put(db, NULL, 1, 32, emb) == GRAVELDB_OK);

    float out[32];
    int out_dim;
    assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_OK);
    for (int i = 0; i < 32; i++) {
        assert(out[i] == 0.0f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_negative_values(void) {
    printf("  test_negative_values...");
    cleanup_test_dir();

    int dims[] = {16};
    GravelDB *db = open_db(dims, 1, 64);

    float emb[16];
    for (int i = 0; i < 16; i++) emb[i] = -(float)(i + 1) * 0.5f;

    graveldb_put(db, NULL, 1, 16, emb);

    float out[16];
    int out_dim;
    graveldb_get(db, NULL, 1, out, &out_dim);
    for (int i = 0; i < 16; i++) {
        assert(fabsf(out[i] - emb[i]) < 1e-6f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_special_float_values(void) {
    printf("  test_special_float_values...");
    cleanup_test_dir();

    int dims[] = {8};
    GravelDB *db = open_db(dims, 1, 64);

    float emb[8] = {
        0.0f, -0.0f, 1e-38f, 1e38f,
        -1e38f, 1e-7f, 3.14159265f, -2.71828f
    };

    graveldb_put(db, NULL, 1, 8, emb);

    float out[8];
    int out_dim;
    graveldb_get(db, NULL, 1, out, &out_dim);
    for (int i = 0; i < 8; i++) {
        assert(fabsf(out[i] - emb[i]) < fabsf(emb[i]) * 1e-6f + 1e-38f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_large_feat_ids(void) {
    printf("  test_large_feat_ids...");
    cleanup_test_dir();

    int dims[] = {8};
    GravelDB *db = open_db(dims, 1, 64);

    uint64_t large_ids[] = {
        UINT64_MAX - 1, UINT64_MAX - 100,
        0x8000000000000000ULL, 0xFFFFFFFF00000000ULL,
        0x00000000FFFFFFFFULL, 1
    };
    int n = sizeof(large_ids) / sizeof(large_ids[0]);

    for (int i = 0; i < n; i++) {
        float emb[8];
        for (int j = 0; j < 8; j++) emb[j] = (float)(i * 8 + j);
        graveldb_put(db, NULL, large_ids[i], 8, emb);
    }

    for (int i = 0; i < n; i++) {
        float out[8];
        int out_dim;
        graveldb_status_t rc = graveldb_get(db, NULL, large_ids[i], out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - (float)(i * 8)) < 1e-6f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

/* ─── Re-open stability ─── */

static void test_reopen_empty_db(void) {
    printf("  test_reopen_empty_db...");
    cleanup_test_dir();

    int dims[] = {32};

    {
        GravelDB *db = open_db(dims, 1, 64);
        graveldb_close(db);
    }

    {
        GravelDB *db = open_db(dims, 1, 64);
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == 0);

        float out[32];
        int out_dim;
        assert(graveldb_get(db, NULL, 1, out, &out_dim) == GRAVELDB_ERR_NOT_FOUND);

        graveldb_close(db);
    }

    printf(" PASS\n");
}

static void test_multiple_reopen_cycles(void) {
    printf("  test_multiple_reopen_cycles...");
    cleanup_test_dir();

    int dims[] = {16};

    for (int cycle = 0; cycle < 5; cycle++) {
        GravelDB *db = open_db(dims, 1, 1024);

        for (int i = 1; i <= 50; i++) {
            uint64_t id = (uint64_t)(cycle * 50 + i);
            float emb[16];
            for (int j = 0; j < 16; j++) emb[j] = (float)id;
            graveldb_put(db, NULL, id, 16, emb);
        }
        graveldb_flush(db);
        graveldb_close(db);
    }

    {
        GravelDB *db = open_db(dims, 1, 1024);
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == 250);

        for (int i = 1; i <= 250; i++) {
            float out[16];
            int out_dim;
            graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_OK);
            assert(fabsf(out[0] - (float)i) < 1e-6f);
        }
        graveldb_close(db);
    }

    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB Integration Tests\n");
    printf("==========================\n\n");

    test_batch_put_get();
    test_batch_put_mixed_dims();
    test_single_feature();
    test_large_dim();
    test_rapid_update_same_key();
    test_delete_nonexistent();
    test_put_delete_put_cycle();
    test_checkpoint_then_recovery();
    test_multiple_checkpoints();
    test_flush_then_reopen();
    test_stats_accuracy();
    test_many_features();
    test_interleaved_put_delete();
    test_zero_embedding();
    test_negative_values();
    test_special_float_values();
    test_large_feat_ids();
    test_reopen_empty_db();
    test_multiple_reopen_cycles();

    printf("\n All integration tests PASSED!\n\n");
    cleanup_test_dir();
    return 0;
}
