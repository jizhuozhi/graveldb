/*
 * GravelDB - Public API tests
 *
 * The single source of truth for what the library promises to its callers.
 * Tests here exercise only the public surface in include/graveldb.h via the
 * thin convenience wrappers in test_helpers.h. No internal headers.
 *
 * If a test in this file goes red, an external user is going to feel it.
 * If you change an internal module and nothing here breaks, that change is
 * by definition safe from a contract point of view.
 */

#undef NDEBUG
#include "graveldb.h"
#include "test_helpers.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#define TEST_DIR "/tmp/graveldb_test_api"
#define DIM 128

static void cleanup_test_dir(void) {
    system("rm -rf " TEST_DIR);
}

static GravelDB *open_db_default(void) {
    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 16 * 1024 * 1024;
    config.index_capacity = 1024; /* small on purpose: forces rehash */
    GravelDB *db = NULL;
    assert(graveldb_open(&db, &config) == GRAVELDB_OK);
    return db;
}

static GravelDB *open_db_with(const int *dims, int num_dims,
                              uint32_t idx_cap, uint32_t buf_mb) {
    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    config.dims = dims;
    config.num_dims = num_dims;
    config.buffer_size = buf_mb * 1024 * 1024;
    config.index_capacity = idx_cap;
    GravelDB *db = NULL;
    assert(graveldb_open(&db, &config) == GRAVELDB_OK);
    return db;
}

/* ---- single-key round trip ---- */

static void test_put_get_basic(void) {
    printf("  test_put_get_basic...");
    cleanup_test_dir();

    GravelDB *db = open_db_default();

    float emb[DIM];
    for (int i = 0; i < DIM; i++) emb[i] = (float)i * 0.1f;

    assert(graveldb_put(db, NULL, 1001, DIM, emb) == GRAVELDB_OK);

    float out[DIM];
    int out_dim = 0;
    assert(graveldb_get(db, NULL, 1001, out, &out_dim) == GRAVELDB_OK);
    assert(out_dim == DIM);
    for (int i = 0; i < DIM; i++) {
        assert(fabsf(out[i] - emb[i]) < 1e-6f);
    }

    /* Missing key returns NOT_FOUND, not OK with garbage data. */
    assert(graveldb_get(db, NULL, 9999, out, &out_dim) == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- bulk insert at scale that forces rehash + flush ---- */

static void test_bulk_insert(void) {
    printf("  test_bulk_insert (50K)...");
    cleanup_test_dir();

    GravelDB *db = open_db_default();

    int n = 50000;
    for (int i = 1; i <= n; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)(i * 100 + j);
        assert(graveldb_put(db, NULL, (uint64_t)i, DIM, emb) == GRAVELDB_OK);
    }

    for (int i = 1; i <= n; i += 100) {
        float out[DIM];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
        assert(out_dim == DIM);
        assert(fabsf(out[0] - (float)(i * 100)) < 1e-6f);
        assert(fabsf(out[DIM-1] - (float)(i * 100 + DIM - 1)) < 1e-6f);
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)n);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- overwrite semantics: last write wins, count unchanged ---- */

static void test_overwrite(void) {
    printf("  test_overwrite (10K x3 rounds)...");
    cleanup_test_dir();

    GravelDB *db = open_db_default();

    int n = 10000;
    for (int round = 1; round <= 3; round++) {
        for (int i = 1; i <= n; i++) {
            float emb[DIM];
            for (int j = 0; j < DIM; j++) emb[j] = (float)round;
            assert(graveldb_put(db, NULL, (uint64_t)i, DIM, emb) == GRAVELDB_OK);
        }
    }

    for (int i = 1; i <= n; i += 500) {
        float out[DIM];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
        assert(fabsf(out[0] - 3.0f) < 1e-6f);
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)n);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- delete: existing -> OK, missing -> NOT_FOUND, count drops ---- */

static void test_delete(void) {
    printf("  test_delete (20K insert, even keys deleted)...");
    cleanup_test_dir();

    GravelDB *db = open_db_default();

    int n = 20000;
    for (int i = 1; i <= n; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)i;
        assert(graveldb_put(db, NULL, (uint64_t)i, DIM, emb) == GRAVELDB_OK);
    }

    for (int i = 2; i <= n; i += 2) {
        assert(graveldb_delete(db, NULL, (uint64_t)i) == GRAVELDB_OK);
    }

    for (int i = 1; i <= n; i++) {
        float out[DIM];
        int out_dim;
        graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        if (i % 2 == 1) {
            assert(rc == GRAVELDB_OK);
            assert(fabsf(out[0] - (float)i) < 1e-6f);
        } else {
            assert(rc == GRAVELDB_ERR_NOT_FOUND);
        }
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)(n / 2));

    /* Missing key delete is NOT_FOUND. */
    assert(graveldb_delete(db, NULL, 999999) == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- batch put/get round trip ---- */

static void test_batch(void) {
    printf("  test_batch (10K batch)...");
    cleanup_test_dir();

    int dims[] = {64};
    GravelDB *db = open_db_with(dims, 1, 1024, 32);

    int n = 10000;
    uint64_t *ids = malloc(n * sizeof(uint64_t));
    int      *bdims = malloc(n * sizeof(int));
    float   **embs  = malloc(n * sizeof(float *));
    for (int i = 0; i < n; i++) {
        ids[i] = (uint64_t)(i + 1);
        bdims[i] = 64;
        embs[i] = malloc(64 * sizeof(float));
        for (int j = 0; j < 64; j++) embs[i][j] = (float)(i * 64 + j);
    }
    assert(graveldb_batch_put(db, NULL, ids, bdims,
                              (const float *const *)embs, n) == GRAVELDB_OK);

    float **outs = malloc(n * sizeof(float *));
    int    *odims = malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) outs[i] = malloc(64 * sizeof(float));
    assert(graveldb_batch_get(db, NULL, ids, n, outs, odims) == GRAVELDB_OK);

    for (int i = 0; i < n; i += 500) {
        assert(odims[i] == 64);
        assert(fabsf(outs[i][0]  - (float)(i * 64))      < 1e-6f);
        assert(fabsf(outs[i][63] - (float)(i * 64 + 63)) < 1e-6f);
    }

    for (int i = 0; i < n; i++) { free(outs[i]); free(embs[i]); }
    free(outs); free(odims); free(embs); free(bdims); free(ids);
    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- multi-dim isolation: same key id never collides across dims ---- */

static void test_multi_dim(void) {
    printf("  test_multi_dim (30K across 3 dims)...");
    cleanup_test_dir();

    int dims[] = {32, 64, 128};
    GravelDB *db = open_db_with(dims, 3, 4096, 64);

    int n = 30000;
    for (int i = 0; i < n; i++) {
        int dim = dims[i % 3];
        float *emb = calloc(dim, sizeof(float));
        for (int j = 0; j < dim; j++) emb[j] = (float)(i + j);
        assert(graveldb_put(db, NULL, (uint64_t)(i + 1), dim, emb) == GRAVELDB_OK);
        free(emb);
    }

    for (int i = 0; i < n; i += 3000) {
        int dim = dims[i % 3];
        float out[128];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)(i + 1), out, &out_dim) == GRAVELDB_OK);
        assert(out_dim == dim);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }

    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)n);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- persistence: write -> close -> reopen -> data still there ----
 * This is the public-API view of "checkpoint works": we don't care about
 * delta files, generation numbers, or the scheduler -- only that close +
 * reopen preserves what the user wrote. */

static void test_persistence(void) {
    printf("  test_persistence (50K, multi-gen)...");
    cleanup_test_dir();

    int dims[] = {64};
    int n_per_gen = 10000;
    int gens = 5;

    /* Phase 1: write across multiple checkpoint generations, then close. */
    {
        GravelDB *db = open_db_with(dims, 1, 4096, 64);
        for (int g = 0; g < gens; g++) {
            int base = g * n_per_gen + 1;
            for (int i = base; i < base + n_per_gen; i++) {
                float emb[64];
                for (int j = 0; j < 64; j++) emb[j] = (float)(g * 1000 + i);
                assert(graveldb_put(db, NULL, (uint64_t)i, 64, emb) == GRAVELDB_OK);
            }
            assert(graveldb_flush(db) == GRAVELDB_OK);
            assert(graveldb_checkpoint(db) == GRAVELDB_OK);
        }
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == (uint64_t)(gens * n_per_gen));
        graveldb_close(db);
    }

    /* Phase 2: reopen -- every entry from every generation must survive. */
    {
        GravelDB *db = open_db_with(dims, 1, 4096, 64);
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == (uint64_t)(gens * n_per_gen));

        for (int g = 0; g < gens; g++) {
            int base = g * n_per_gen + 1;
            for (int i = base; i < base + n_per_gen; i += 2000) {
                float out[64];
                int out_dim;
                assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
                assert(out_dim == 64);
                assert(fabsf(out[0] - (float)(g * 1000 + i)) < 1e-6f);
            }
        }

        /* Out-of-range key still NOT_FOUND after recovery. */
        float out[64];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)(gens * n_per_gen + 1),
                            out, &out_dim) == GRAVELDB_ERR_NOT_FOUND);

        graveldb_close(db);
    }

    printf(" PASS\n");
}

/* ---- updates survive checkpoint+recovery: last write wins on disk too ---- */

static void test_update_then_recover(void) {
    printf("  test_update_then_recover (20K, 50%% updated)...");
    cleanup_test_dir();

    int dims[] = {64};
    int n = 20000;

    {
        GravelDB *db = open_db_with(dims, 1, 4096, 32);
        for (int i = 1; i <= n; i++) {
            float emb[64];
            for (int j = 0; j < 64; j++) emb[j] = (float)i;
            assert(graveldb_put(db, NULL, (uint64_t)i, 64, emb) == GRAVELDB_OK);
        }
        assert(graveldb_flush(db) == GRAVELDB_OK);
        assert(graveldb_checkpoint(db) == GRAVELDB_OK);

        for (int i = 1; i <= n; i += 2) {
            float emb[64];
            for (int j = 0; j < 64; j++) emb[j] = -1.0f * (float)i;
            assert(graveldb_put(db, NULL, (uint64_t)i, 64, emb) == GRAVELDB_OK);
        }
        assert(graveldb_flush(db) == GRAVELDB_OK);
        assert(graveldb_checkpoint(db) == GRAVELDB_OK);
        graveldb_close(db);
    }

    {
        GravelDB *db = open_db_with(dims, 1, 4096, 32);
        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == (uint64_t)n);

        for (int i = 1; i <= n; i += 1000) {
            float out[64];
            int out_dim;
            assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
            if (i % 2 == 1) {
                assert(out[0] < 0.0f);
                assert(fabsf(out[0] - (-1.0f * (float)i)) < 1e-6f);
            } else {
                assert(fabsf(out[0] - (float)i) < 1e-6f);
            }
        }
        graveldb_close(db);
    }

    printf(" PASS\n");
}

/* ---- checkpoint during concurrent reads/writes ----
 * Public-API view of "checkpoint doesn't break the world": while a writer
 * thread is putting and a reader thread is getting, we trigger a checkpoint
 * on the main thread; reads must keep returning consistent values, no errors
 * other than NOT_FOUND for not-yet-written keys. */

#define CKPT_N            20000
#define CKPT_DIM          64
#define CKPT_READ_THREADS 4

typedef struct {
    GravelDB *db;
    int       written_upper; /* atomic-ish high water mark of fully written keys */
    int       stop;
    int       read_errors;
} CkptShared;

static void *ckpt_reader(void *arg) {
    CkptShared *s = arg;
    float out[CKPT_DIM];
    int out_dim;
    while (!__atomic_load_n(&s->stop, __ATOMIC_ACQUIRE)) {
        int hi = __atomic_load_n(&s->written_upper, __ATOMIC_ACQUIRE);
        if (hi <= 0) continue;
        uint64_t k = (uint64_t)((rand() % hi) + 1);
        graveldb_status_t rc = graveldb_get(s->db, NULL, k, out, &out_dim);
        if (rc == GRAVELDB_OK) {
            /* Value was written as (float)i for key i; verify consistency. */
            if (fabsf(out[0] - (float)k) > 1e-6f) {
                __atomic_fetch_add(&s->read_errors, 1, __ATOMIC_RELAXED);
            }
        } else if (rc != GRAVELDB_ERR_NOT_FOUND) {
            __atomic_fetch_add(&s->read_errors, 1, __ATOMIC_RELAXED);
        }
    }
    return NULL;
}

static void test_checkpoint_during_writes(void) {
    printf("  test_checkpoint_during_writes (20K + concurrent reads)...");
    cleanup_test_dir();

    int dims[] = {CKPT_DIM};
    GravelDB *db = open_db_with(dims, 1, 4096, 32);

    CkptShared shared = { .db = db };

    pthread_t readers[CKPT_READ_THREADS];
    for (int i = 0; i < CKPT_READ_THREADS; i++) {
        pthread_create(&readers[i], NULL, ckpt_reader, &shared);
    }

    /* Writer + checkpoint trigger on main thread. */
    for (int i = 1; i <= CKPT_N; i++) {
        float emb[CKPT_DIM];
        for (int j = 0; j < CKPT_DIM; j++) emb[j] = (float)i;
        assert(graveldb_put(db, NULL, (uint64_t)i, CKPT_DIM, emb) == GRAVELDB_OK);
        __atomic_store_n(&shared.written_upper, i, __ATOMIC_RELEASE);

        /* Trigger a few checkpoints during the write stream. */
        if (i == CKPT_N / 4 || i == CKPT_N / 2 || i == (3 * CKPT_N) / 4) {
            assert(graveldb_checkpoint(db) == GRAVELDB_OK);
        }
    }

    __atomic_store_n(&shared.stop, 1, __ATOMIC_RELEASE);
    for (int i = 0; i < CKPT_READ_THREADS; i++) pthread_join(readers[i], NULL);

    assert(shared.read_errors == 0);

    /* All keys must be readable after the dust settles. */
    for (int i = 1; i <= CKPT_N; i += 500) {
        float out[CKPT_DIM];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }

    /* And they must survive a reopen too. */
    graveldb_close(db);
    db = open_db_with(dims, 1, 4096, 32);
    GravelDBStats stats;
    graveldb_stats(db, &stats);
    assert(stats.total_features == (uint64_t)CKPT_N);
    for (int i = 1; i <= CKPT_N; i += 500) {
        float out[CKPT_DIM];
        int out_dim;
        assert(graveldb_get(db, NULL, (uint64_t)i, out, &out_dim) == GRAVELDB_OK);
        assert(fabsf(out[0] - (float)i) < 1e-6f);
    }
    graveldb_close(db);

    printf(" PASS\n");
}

/* ---- error paths: invalid args, bad config ---- */

static void test_error_paths(void) {
    printf("  test_error_paths...");
    cleanup_test_dir();

    /* NULL config. */
    GravelDB *db = NULL;
    assert(graveldb_open(&db, NULL) != GRAVELDB_OK);
    assert(db == NULL);

    /* NULL data_dir. */
    GravelDBConfig bad = {0};
    assert(graveldb_open(&db, &bad) != GRAVELDB_OK);

    /* Valid open, then exercise typical not-found / mismatch errors. */
    db = open_db_default();

    float out[DIM];
    int out_dim;
    assert(graveldb_get(db, NULL, 123, out, &out_dim) == GRAVELDB_ERR_NOT_FOUND);
    assert(graveldb_delete(db, NULL, 123) == GRAVELDB_ERR_NOT_FOUND);

    graveldb_close(db);
    printf(" PASS\n");
}

/* ---- key-range edges: same id space across the full uint64 ---- */

static void test_key_range_extremes(void) {
    printf("  test_key_range_extremes...");
    cleanup_test_dir();

    int dims[] = {16};
    GravelDB *db = open_db_with(dims, 1, 256, 8);

    uint64_t ids[] = {
        1, 2, UINT64_MAX - 1, UINT64_MAX - 2,
        0x8000000000000000ULL, 0x8000000000000001ULL,
        0xFFFFFFFF00000000ULL, 0x00000000FFFFFFFFULL,
        0xDEADBEEFCAFEBABEULL, 0x0123456789ABCDEFULL,
    };
    int n = sizeof(ids) / sizeof(ids[0]);

    for (int i = 0; i < n; i++) {
        float emb[16];
        for (int j = 0; j < 16; j++) emb[j] = (float)(i * 16 + j);
        assert(graveldb_put(db, NULL, ids[i], 16, emb) == GRAVELDB_OK);
    }

    assert(graveldb_flush(db) == GRAVELDB_OK);
    graveldb_close(db);

    db = open_db_with(dims, 1, 256, 8);
    for (int i = 0; i < n; i++) {
        float out[16];
        int out_dim;
        assert(graveldb_get(db, NULL, ids[i], out, &out_dim) == GRAVELDB_OK);
        assert(out_dim == 16);
        assert(fabsf(out[0] - (float)(i * 16)) < 1e-6f);
    }
    graveldb_close(db);

    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB Public API Tests\n");
    printf("=========================\n\n");

    test_put_get_basic();
    test_bulk_insert();
    test_overwrite();
    test_delete();
    test_batch();
    test_multi_dim();
    test_persistence();
    test_update_then_recover();
    test_checkpoint_during_writes();
    test_error_paths();
    test_key_range_extremes();

    printf("\nAll tests PASSED.\n\n");
    cleanup_test_dir();
    return 0;
}
