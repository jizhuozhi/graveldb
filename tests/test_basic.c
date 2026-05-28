/*
 * GravelDB - Basic functionality test
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

#define TEST_DIR "/tmp/graveldb_test"
#define DIM 64

static void cleanup_test_dir(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);
}

static void test_basic_put_get(void) {
    printf("  test_basic_put_get...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 16 * 1024 * 1024; /* 16MB */
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);
    assert(db != NULL);

    /* Put */
    float emb[DIM];
    for (int i = 0; i < DIM; i++) emb[i] = (float)i * 0.1f;

    rc = graveldb_put(db, NULL, 1001, DIM, emb);
    assert(rc == GRAVELDB_OK);

    /* Get */
    float out[DIM];
    int out_dim = 0;
    rc = graveldb_get(db, NULL, 1001, out, &out_dim);
    assert(rc == GRAVELDB_OK);
    assert(out_dim == DIM);

    for (int i = 0; i < DIM; i++) {
        assert(fabsf(out[i] - emb[i]) < 1e-6f);
    }

    /* Not found */
    rc = graveldb_get(db, NULL, 9999, out, &out_dim);
    assert(rc == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_multiple_features(void) {
    printf("  test_multiple_features...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int cfg_dims[] = {32, 64, 128};
    config.dims = cfg_dims;
    config.num_dims = 3;
    config.buffer_size = 32 * 1024 * 1024;
    config.index_capacity = 4096;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    /* Insert features with different dims */
    int num_features = 1000;
    int dims[] = {32, 64, 128};

    for (int i = 1; i <= num_features; i++) {
        int dim = dims[i % 3];
        float *emb = (float *)calloc(dim, sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)(i * 100 + j);

        rc = graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        assert(rc == GRAVELDB_OK);
        free(emb);
    }

    /* Verify all features */
    for (int i = 1; i <= num_features; i++) {
        int dim = dims[i % 3];
        float *out = (float *)calloc(dim, sizeof(float));
        int out_dim;

        rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(out_dim == dim);

        for (int j = 0; j < dim; j++) {
            assert(fabsf(out[j] - (float)(i * 100 + j)) < 1e-6f);
        }
        free(out);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_delete(void) {
    printf("  test_delete...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    float emb[DIM];
    for (int i = 0; i < DIM; i++) emb[i] = 1.0f;

    rc = graveldb_put(db, NULL, 42, DIM, emb);
    assert(rc == GRAVELDB_OK);

    /* Delete */
    rc = graveldb_delete(db, NULL, 42);
    assert(rc == GRAVELDB_OK);

    /* Should not be found */
    float out[DIM];
    int out_dim;
    rc = graveldb_get(db, NULL, 42, out, &out_dim);
    assert(rc == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_update(void) {
    printf("  test_update...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims2[] = {DIM};
    config.dims = dims2;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    /* Initial write */
    float emb1[DIM];
    for (int i = 0; i < DIM; i++) emb1[i] = 1.0f;
    rc = graveldb_put(db, NULL, 100, DIM, emb1);
    assert(rc == GRAVELDB_OK);

    /* Update */
    float emb2[DIM];
    for (int i = 0; i < DIM; i++) emb2[i] = 2.0f;
    rc = graveldb_put(db, NULL, 100, DIM, emb2);
    assert(rc == GRAVELDB_OK);

    /* Verify updated value */
    float out[DIM];
    int out_dim;
    rc = graveldb_get(db, NULL, 100, out, &out_dim);
    assert(rc == GRAVELDB_OK);
    for (int i = 0; i < DIM; i++) {
        assert(fabsf(out[i] - 2.0f) < 1e-6f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_flush_and_reread(void) {
    printf("  test_flush_and_reread...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    /* Write data */
    for (int i = 1; i <= 100; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)(i + j);
        graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
    }

    /* Flush to SSD */
    rc = graveldb_flush(db);
    assert(rc == GRAVELDB_OK);

    /* Verify after flush */
    for (int i = 1; i <= 100; i++) {
        float out[DIM];
        int out_dim;
        rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(out_dim == DIM);
        assert(fabsf(out[0] - (float)(i)) < 1e-6f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_checkpoint(void) {
    printf("  test_checkpoint...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims2[] = {DIM};
    config.dims = dims2;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    /* Write initial data */
    for (int i = 1; i <= 50; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)i;
        graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
    }

    /* Checkpoint */
    rc = graveldb_checkpoint(db);
    assert(rc == GRAVELDB_OK);

    /* Write more data after checkpoint */
    for (int i = 51; i <= 100; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)i;
        graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
    }

    /* Verify all data is accessible */
    for (int i = 1; i <= 100; i++) {
        float out[DIM];
        int out_dim;
        rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }

    /* Stats */
    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == 100);
    assert(stats.checkpoint_generation == 1);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_stats(void) {
    printf("  test_stats...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    GravelDBStats stats;
    rc = graveldb_stats(db, &stats);
    assert(rc == GRAVELDB_OK);
    assert(stats.total_features == 0);

    /* Insert some features */
    for (int i = 1; i <= 10; i++) {
        float emb[DIM] = {0};
        graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
    }

    rc = graveldb_stats(db, &stats);
    assert(rc == GRAVELDB_OK);
    assert(stats.total_features == 10);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_restart_recovery(void) {
    printf("  test_restart_recovery...");

    cleanup_test_dir();

    int num_features = 500;

    /* Phase 1: Write data and close */
    {
        GravelDBConfig config = {0};
        config.data_dir = TEST_DIR;
        int dims[] = {32, 64, 128};
        config.dims = dims;
        config.num_dims = 3;
        config.buffer_size = 16 * 1024 * 1024;
        config.index_capacity = 4096;

        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        int dim_choices[] = {32, 64, 128};
        for (int i = 1; i <= num_features; i++) {
            int dim = dim_choices[i % 3];
            float *emb = (float *)calloc(dim, sizeof(float));
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 1000 + j);
            rc = graveldb_put(db, NULL, (uint64_t)i, dim, emb);
            assert(rc == GRAVELDB_OK);
            free(emb);
        }

        /* Flush to ensure data is on disk */
        rc = graveldb_flush(db);
        assert(rc == GRAVELDB_OK);

        graveldb_close(db);
    }

    /* Phase 2: Reopen and verify all data recovered via .keys rebuild */
    {
        GravelDBConfig config = {0};
        config.data_dir = TEST_DIR;
        int dims[] = {32, 64, 128};
        config.dims = dims;
        config.num_dims = 3;
        config.buffer_size = 16 * 1024 * 1024;
        config.index_capacity = 4096;

        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        /* Verify all features are accessible */
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == (uint64_t)num_features);

        int dim_choices[] = {32, 64, 128};
        for (int i = 1; i <= num_features; i++) {
            int dim = dim_choices[i % 3];
            float *out = (float *)calloc(dim, sizeof(float));
            int out_dim = 0;

            rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_OK);
            assert(out_dim == dim);

            /* Verify data integrity */
            for (int j = 0; j < dim; j++) {
                assert(fabsf(out[j] - (float)(i * 1000 + j)) < 1e-6f);
            }
            free(out);
        }

        /* Verify a non-existent key is not found */
        float tmp[128];
        int tmp_dim;
        rc = graveldb_get(db, NULL, 999999, tmp, &tmp_dim);
        assert(rc == GRAVELDB_ERR_NOT_FOUND);

        graveldb_close(db);
    }

    printf(" PASS\n");
}

static void test_delete_and_recovery(void) {
    printf("  test_delete_and_recovery...");

    cleanup_test_dir();

    /* Phase 1: Write and delete some keys */
    {
        GravelDBConfig config = {0};
        config.data_dir = TEST_DIR;
        int dims[] = {DIM};
        config.dims = dims;
        config.num_dims = 1;
        config.buffer_size = 8 * 1024 * 1024;
        config.index_capacity = 1024;

        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        for (int i = 1; i <= 100; i++) {
            float emb[DIM];
            for (int j = 0; j < DIM; j++) emb[j] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
        }

        /* Delete even keys */
        for (int i = 2; i <= 100; i += 2) {
            graveldb_delete(db, NULL, (uint64_t)i);
        }

        graveldb_flush(db);
        graveldb_close(db);
    }

    /* Phase 2: Reopen and verify */
    {
        GravelDBConfig config = {0};
        config.data_dir = TEST_DIR;
        int dims[] = {DIM};
        config.dims = dims;
        config.num_dims = 1;
        config.buffer_size = 8 * 1024 * 1024;
        config.index_capacity = 1024;

        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == 50); /* 100 - 50 deleted */

        /* Odd keys should exist */
        for (int i = 1; i <= 100; i += 2) {
            float out[DIM];
            int out_dim;
            rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_OK);
            assert(fabsf(out[0] - (float)i) < 1e-6f);
        }

        /* Even keys should not exist */
        for (int i = 2; i <= 100; i += 2) {
            float out[DIM];
            int out_dim;
            rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            assert(rc == GRAVELDB_ERR_NOT_FOUND);
        }

        graveldb_close(db);
    }

    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB Basic Tests\n");
    printf("====================\n\n");

    test_basic_put_get();
    test_multiple_features();
    test_delete();
    test_update();
    test_flush_and_reread();
    test_checkpoint();
    test_stats();
    test_restart_recovery();
    test_delete_and_recovery();

    printf("\n All tests PASSED!\n\n");
    cleanup_test_dir();
    return 0;
}
