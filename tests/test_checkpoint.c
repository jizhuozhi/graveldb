/*
 * GravelDB - Checkpoint Tests
 *
 * Tests for:
 *   - CkptScheduler lifecycle (init/destroy)
 *   - Scheduler tick (flush/checkpoint interval triggering)
 *   - Force flush / force checkpoint
 *   - Request full: singleflight batching
 *   - Request full: cooldown mechanism
 *   - Scheduler stats
 *   - Delta dump and replay
 *   - Footer persist and read (dual A/B)
 *   - Delta chain management (list/purge)
 *   - Recovery with delta replay
 *   - CkptFullResponse broadcast to multiple waiters
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "graveldb_impl.h"
#include "checkpoint.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <math.h>

#define TEST_DIR "/tmp/graveldb_test_ckpt"
#define DIM 32

static void cleanup_test_dir(void) {
    system("rm -rf " TEST_DIR);
}

/* ─── Helper: open a GravelDB for checkpoint tests ─── */
static GravelDB *open_test_db(void) {
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
    assert(db != NULL);
    return db;
}

/* ─── Helper: insert N features into db ─── */
static void insert_features(GravelDB *db, int start, int count) {
    for (int i = start; i < start + count; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = (float)(i * 100 + j);
        graveldb_status_t rc = graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
        assert(rc == GRAVELDB_OK);
    }
}

/* ─── Tests ─── */

static void test_scheduler_init_destroy(void) {
    printf("  test_scheduler_init_destroy...");

    GravelDB *db = open_test_db();

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 500,
        .flush_dirty_threshold = 1024,
        .checkpoint_interval_s = 30,
        .full_cooldown_ms = 10000,
        .auto_recover_on_open = true,
    };

    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);
    assert(sched.db == db);
    assert(sched.config.flush_interval_ms == 500);
    assert(sched.config.checkpoint_interval_s == 30);
    assert(sched.full_cooldown_ms == 10000);
    assert(sched.full_pending == false);
    assert(sched.full_waiters == NULL);

    ckpt_scheduler_destroy(&sched);

    /* NULL params */
    rc = ckpt_scheduler_init(NULL, db, &config);
    assert(rc == GRAVELDB_ERR_INVALID);
    rc = ckpt_scheduler_init(&sched, NULL, &config);
    assert(rc == GRAVELDB_ERR_INVALID);

    /* NULL config should use defaults */
    rc = ckpt_scheduler_init(&sched, db, NULL);
    assert(rc == GRAVELDB_OK);
    assert(sched.config.flush_interval_ms == 1000);
    assert(sched.config.checkpoint_interval_s == 60);
    assert(sched.full_cooldown_ms == 60000);
    ckpt_scheduler_destroy(&sched);

    /* Destroy NULL should be safe */
    ckpt_scheduler_destroy(NULL);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_scheduler_tick_flush(void) {
    printf("  test_scheduler_tick_flush...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 100,
        .flush_dirty_threshold = 4096,
        .checkpoint_interval_s = 0,  /* disable checkpoint by interval */
        .full_cooldown_ms = 60000,
        .auto_recover_on_open = false,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    /* Tick at t=0: should not flush (elapsed=0) */
    rc = ckpt_scheduler_tick(&sched, 0);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 0);

    /* Tick at t=99ms: still not enough */
    rc = ckpt_scheduler_tick(&sched, 99);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 0);

    /* Tick at t=100ms: should trigger flush */
    rc = ckpt_scheduler_tick(&sched, 100);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 1);

    /* Tick at t=199ms: not yet */
    rc = ckpt_scheduler_tick(&sched, 199);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 1);

    /* Tick at t=200ms: second flush */
    rc = ckpt_scheduler_tick(&sched, 200);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 2);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_scheduler_tick_checkpoint(void) {
    printf("  test_scheduler_tick_checkpoint...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 20);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,       /* disable auto-flush */
        .flush_dirty_threshold = 4096,
        .checkpoint_interval_s = 1,   /* 1 second = 1000ms */
        .full_cooldown_ms = 60000,
        .auto_recover_on_open = false,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    /* Tick at t=500ms: not yet */
    rc = ckpt_scheduler_tick(&sched, 500);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 0);

    /* Tick at t=1000ms: should trigger checkpoint */
    rc = ckpt_scheduler_tick(&sched, 1000);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 1);

    /* Tick at t=1500ms: not yet */
    rc = ckpt_scheduler_tick(&sched, 1500);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 1);

    /* Tick at t=2000ms: second checkpoint */
    rc = ckpt_scheduler_tick(&sched, 2000);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 2);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_scheduler_tick_invalid(void) {
    printf("  test_scheduler_tick_invalid...");

    graveldb_status_t rc = ckpt_scheduler_tick(NULL, 1000);
    assert(rc == GRAVELDB_ERR_INVALID);

    CkptScheduler sched;
    memset(&sched, 0, sizeof(sched));
    sched.db = NULL;
    rc = ckpt_scheduler_tick(&sched, 1000);
    assert(rc == GRAVELDB_ERR_INVALID);

    printf(" PASS\n");
}

static void test_force_flush(void) {
    printf("  test_force_flush...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 100);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 60000,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    rc = ckpt_scheduler_force_flush(&sched);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 1);

    rc = ckpt_scheduler_force_flush(&sched);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_flushes == 2);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_force_checkpoint(void) {
    printf("  test_force_checkpoint...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 100);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 60000,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    rc = ckpt_scheduler_force_checkpoint(&sched);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 1);
    assert(sched.delta_chain_length == 1);

    /* Second checkpoint */
    insert_features(db, 101, 50);
    graveldb_flush(db);
    rc = ckpt_scheduler_force_checkpoint(&sched);
    assert(rc == GRAVELDB_OK);
    assert(sched.total_checkpoints == 2);
    assert(sched.delta_chain_length == 2);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_request_full_basic(void) {
    printf("  test_request_full_basic...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 0,  /* no cooldown for this test */
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    CkptFullResponse response = {0};
    rc = ckpt_scheduler_request_full(&sched, &response);
    assert(rc == GRAVELDB_OK);
    assert(sched.full_pending == true);
    assert(response.completed == false);

    /* Execute the checkpoint (which will do the full at safepoint) */
    rc = ckpt_scheduler_force_checkpoint(&sched);
    assert(rc == GRAVELDB_OK);

    /* Response should be filled */
    assert(response.completed == true);
    assert(response.checkpoint_id > 0);
    assert(response.delta_base == response.checkpoint_id);
    assert(sched.total_full_checkpoints == 1);
    assert(sched.delta_chain_length == 0);  /* reset after full */

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_request_full_null_params(void) {
    printf("  test_request_full_null_params...");

    GravelDB *db = open_test_db();
    CkptScheduler sched;
    CkptConfig config = { .full_cooldown_ms = 0 };
    ckpt_scheduler_init(&sched, db, &config);

    graveldb_status_t rc = ckpt_scheduler_request_full(NULL, &(CkptFullResponse){0});
    assert(rc == GRAVELDB_ERR_INVALID);

    rc = ckpt_scheduler_request_full(&sched, NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_request_full_singleflight(void) {
    printf("  test_request_full_singleflight...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 0,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    /* Multiple clients request full — should be batched */
    CkptFullResponse resp1 = {0};
    CkptFullResponse resp2 = {0};
    CkptFullResponse resp3 = {0};

    rc = ckpt_scheduler_request_full(&sched, &resp1);
    assert(rc == GRAVELDB_OK);
    rc = ckpt_scheduler_request_full(&sched, &resp2);
    assert(rc == GRAVELDB_OK);
    rc = ckpt_scheduler_request_full(&sched, &resp3);
    assert(rc == GRAVELDB_OK);

    /* Only one full_pending */
    assert(sched.full_pending == true);

    /* Execute */
    rc = ckpt_scheduler_force_checkpoint(&sched);
    assert(rc == GRAVELDB_OK);

    /* All three responses should be completed with the same checkpoint_id */
    assert(resp1.completed == true);
    assert(resp2.completed == true);
    assert(resp3.completed == true);
    assert(resp1.checkpoint_id == resp2.checkpoint_id);
    assert(resp2.checkpoint_id == resp3.checkpoint_id);
    assert(resp1.checkpoint_id > 0);

    /* Only one full was executed */
    assert(sched.total_full_checkpoints == 1);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_request_full_cooldown(void) {
    printf("  test_request_full_cooldown...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 60000,  /* 60 second cooldown */
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    /*
     * The cooldown check uses sched->last_checkpoint_ms as "now".
     * force_checkpoint sets last_full_time_ms = last_checkpoint_ms.
     * We need last_checkpoint_ms to be non-zero so the cooldown condition
     * (last_full_time_ms > 0) is satisfied after the first full completes.
     *
     * Simulate time by directly setting last_checkpoint_ms before force_checkpoint.
     */
    sched.last_checkpoint_ms = 10000;  /* t=10s */

    /* First full request and execute */
    CkptFullResponse resp1 = {0};
    rc = ckpt_scheduler_request_full(&sched, &resp1);
    assert(rc == GRAVELDB_OK);
    assert(resp1.completed == false);

    rc = ckpt_scheduler_force_checkpoint(&sched);
    assert(rc == GRAVELDB_OK);
    assert(resp1.completed == true);
    uint64_t first_ckpt_id = resp1.checkpoint_id;
    assert(sched.last_full_time_ms == 10000);

    /* Second request within cooldown — should be immediately satisfied */
    CkptFullResponse resp2 = {0};
    rc = ckpt_scheduler_request_full(&sched, &resp2);
    assert(rc == GRAVELDB_OK);
    /* Cooldown: immediately resolved (last_checkpoint_ms - last_full_time_ms = 0 < 60000) */
    assert(resp2.completed == true);
    assert(resp2.checkpoint_id == first_ckpt_id);
    assert(resp2.delta_base == first_ckpt_id);

    /* No new full was triggered */
    assert(sched.total_full_checkpoints == 1);
    assert(sched.full_pending == false);

    /* Simulate time passing beyond cooldown */
    sched.last_checkpoint_ms = 80000;  /* t=80s, elapsed = 70s > 60s cooldown */

    CkptFullResponse resp3 = {0};
    rc = ckpt_scheduler_request_full(&sched, &resp3);
    assert(rc == GRAVELDB_OK);
    /* Cooldown expired: should NOT be immediately resolved */
    assert(resp3.completed == false);
    assert(sched.full_pending == true);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_scheduler_stats(void) {
    printf("  test_scheduler_stats...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);
    graveldb_flush(db);

    CkptScheduler sched;
    CkptConfig config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 0,
    };
    graveldb_status_t rc = ckpt_scheduler_init(&sched, db, &config);
    assert(rc == GRAVELDB_OK);

    CkptStats stats;
    ckpt_scheduler_stats(&sched, &stats);
    assert(stats.total_flushes == 0);
    assert(stats.total_checkpoints == 0);
    assert(stats.total_full_checkpoints == 0);
    assert(stats.current_delta_chain_length == 0);

    /* Do some operations */
    ckpt_scheduler_force_flush(&sched);
    ckpt_scheduler_force_flush(&sched);
    ckpt_scheduler_force_checkpoint(&sched);

    ckpt_scheduler_stats(&sched, &stats);
    assert(stats.total_flushes == 2);
    assert(stats.total_checkpoints == 1);
    assert(stats.current_delta_chain_length == 1);

    /* Full checkpoint */
    CkptFullResponse resp = {0};
    ckpt_scheduler_request_full(&sched, &resp);
    ckpt_scheduler_force_checkpoint(&sched);

    ckpt_scheduler_stats(&sched, &stats);
    assert(stats.total_checkpoints == 2);
    assert(stats.total_full_checkpoints == 1);
    assert(stats.current_delta_chain_length == 0);

    /* NULL params should not crash */
    ckpt_scheduler_stats(NULL, &stats);
    ckpt_scheduler_stats(&sched, NULL);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_delta_dump_and_replay(void) {
    printf("  test_delta_dump_and_replay...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    /* Phase 1: Write data, flush, checkpoint, modify, checkpoint again */
    GravelDB *db = NULL;
    graveldb_status_t rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    insert_features(db, 1, 100);
    rc = graveldb_flush(db);
    assert(rc == GRAVELDB_OK);

    rc = graveldb_checkpoint(db);
    assert(rc == GRAVELDB_OK);

    /* Modify some entries */
    for (int i = 1; i <= 10; i++) {
        float emb[DIM];
        for (int j = 0; j < DIM; j++) emb[j] = 999.0f;
        graveldb_put(db, NULL, (uint64_t)i, DIM, emb);
    }
    graveldb_flush(db);
    graveldb_checkpoint(db);

    graveldb_close(db);

    /* Phase 2: Reopen with recovery — deltas should be replayed */
    db = NULL;
    rc = graveldb_open(&db, &config);
    assert(rc == GRAVELDB_OK);

    /* Verify modified entries */
    for (int i = 1; i <= 10; i++) {
        float out[DIM];
        int out_dim;
        rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - 999.0f) < 1e-6f);
    }

    /* Verify unmodified entries */
    for (int i = 11; i <= 100; i++) {
        float out[DIM];
        int out_dim;
        rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - (float)(i * 100)) < 1e-6f);
    }

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_footer_persist_and_read(void) {
    printf("  test_footer_persist_and_read...");

    GravelDB *db = open_test_db();
    insert_features(db, 1, 50);
    graveldb_flush(db);

    /* Get the DimBin and persist footer */
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, 0);
    assert(bin != NULL);

    graveldb_status_t rc = ckpt_persist_footer(bin, 50, 1);
    assert(rc == GRAVELDB_OK);

    /* Read it back */
    FileFooter footer;
    rc = ckpt_read_footer(bin->fd, &footer);
    assert(rc == GRAVELDB_OK);
    assert(footer.magic == GRAVELDB_FOOTER_MAGIC);
    assert(footer.num_entries == 50);
    assert(footer.generation == 1);
    assert(footer.dim == (uint32_t)DIM);

    /* Persist a second generation (should use other slot) */
    rc = ckpt_persist_footer(bin, 75, 2);
    assert(rc == GRAVELDB_OK);

    /* Read again — should pick gen 2 (higher generation) */
    rc = ckpt_read_footer(bin->fd, &footer);
    assert(rc == GRAVELDB_OK);
    assert(footer.generation == 2);
    assert(footer.num_entries == 75);

    /* Persist gen 3 — overwrites the gen 1 slot (3 % 2 == 1, same as gen 1 slot) */
    rc = ckpt_persist_footer(bin, 100, 3);
    assert(rc == GRAVELDB_OK);

    rc = ckpt_read_footer(bin->fd, &footer);
    assert(rc == GRAVELDB_OK);
    assert(footer.generation == 3);
    assert(footer.num_entries == 100);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_footer_read_invalid(void) {
    printf("  test_footer_read_invalid...");

    FileFooter footer;

    /* Invalid fd */
    graveldb_status_t rc = ckpt_read_footer(-1, &footer);
    assert(rc == GRAVELDB_ERR_INVALID);

    /* NULL output */
    rc = ckpt_read_footer(0, NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    /* File too small for dual footer */
    cleanup_test_dir();
    mkdir(TEST_DIR, 0755);
    char path[256];
    snprintf(path, sizeof(path), "%s/tiny.bin", TEST_DIR);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert(fd >= 0);
    char dummy[32] = {0};
    write(fd, dummy, sizeof(dummy)); /* smaller than 2 * sizeof(FileFooter) */

    rc = ckpt_read_footer(fd, &footer);
    assert(rc == GRAVELDB_ERR_CORRUPT);

    close(fd);
    printf(" PASS\n");
}

static void test_delta_list_and_purge(void) {
    printf("  test_delta_list_and_purge...");

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

    /* Create multiple checkpoints to generate delta files */
    for (int epoch = 0; epoch < 5; epoch++) {
        insert_features(db, epoch * 10 + 1, 10);
        graveldb_flush(db);
        graveldb_checkpoint(db);
    }

    /* List deltas for dim=DIM */
    CkptDeltaInfo *deltas = NULL;
    int count = ckpt_list_deltas(TEST_DIR, DIM, &deltas);
    assert(count >= 1); /* should have multiple delta files */

    /* Verify they are sorted by generation */
    for (int i = 1; i < count; i++) {
        assert(deltas[i].generation >= deltas[i-1].generation);
    }

    /* All deltas should match our dim */
    for (int i = 0; i < count; i++) {
        assert(deltas[i].dim == DIM);
        assert(deltas[i].path != NULL);
    }

    /* Purge deltas up to generation of the latest */
    uint64_t purge_gen = deltas[count - 1].generation;
    ckpt_free_deltas(deltas, count);

    rc = ckpt_purge_old_deltas(TEST_DIR, DIM, purge_gen);
    assert(rc == GRAVELDB_OK);

    /* After purge, listing should return 0 (all purged) */
    count = ckpt_list_deltas(TEST_DIR, DIM, &deltas);
    assert(count == 0);
    ckpt_free_deltas(deltas, count);

    /* Edge case: list from nonexistent dir */
    count = ckpt_list_deltas("/tmp/nonexistent_ckpt_test_xyz", DIM, &deltas);
    assert(count == 0);

    /* NULL params */
    count = ckpt_list_deltas(NULL, DIM, &deltas);
    assert(count == 0);
    count = ckpt_list_deltas(TEST_DIR, DIM, NULL);
    assert(count == 0);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_delta_replay_invalid(void) {
    printf("  test_delta_replay_invalid...");

    GravelDB *db = open_test_db();
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, 0);
    assert(bin != NULL);

    /* NULL params */
    graveldb_status_t rc = ckpt_replay_delta(NULL, "/tmp/fake.bin");
    assert(rc == GRAVELDB_ERR_INVALID);
    rc = ckpt_replay_delta(bin, NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    /* Non-existent file */
    rc = ckpt_replay_delta(bin, "/tmp/nonexistent_delta_file_xyz.bin");
    assert(rc == GRAVELDB_ERR_IO);

    /* File with bad magic */
    char bad_path[256];
    snprintf(bad_path, sizeof(bad_path), "%s/bad_delta.bin", TEST_DIR);
    int fd = open(bad_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert(fd >= 0);
    DeltaHeader bad_hdr = { .magic = 0x12345678, .version = 1 };
    write(fd, &bad_hdr, sizeof(bad_hdr));
    close(fd);

    rc = ckpt_replay_delta(bin, bad_path);
    assert(rc == GRAVELDB_ERR_CORRUPT);

    /* File with wrong dim */
    char wrong_dim_path[256];
    snprintf(wrong_dim_path, sizeof(wrong_dim_path), "%s/wrong_dim.bin", TEST_DIR);
    fd = open(wrong_dim_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert(fd >= 0);
    DeltaHeader wrong_hdr = {
        .magic = GRAVELDB_DELTA_MAGIC,
        .version = 1,
        .dim = 999, /* wrong dim */
    };
    write(fd, &wrong_hdr, sizeof(wrong_hdr));
    close(fd);

    rc = ckpt_replay_delta(bin, wrong_dim_path);
    assert(rc == GRAVELDB_ERR_INVALID);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_dump_delta_invalid(void) {
    printf("  test_dump_delta_invalid...");

    graveldb_status_t rc = ckpt_dump_delta(NULL, "/tmp", NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    GravelDB *db = open_test_db();
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, 0);
    rc = ckpt_dump_delta(bin, NULL, NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_dump_full_invalid(void) {
    printf("  test_dump_full_invalid...");

    graveldb_status_t rc = ckpt_dump_full(NULL, "/tmp");
    assert(rc == GRAVELDB_ERR_INVALID);

    GravelDB *db = open_test_db();
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, 0);
    rc = ckpt_dump_full(bin, NULL);
    assert(rc == GRAVELDB_ERR_INVALID);

    graveldb_close(db);
    printf(" PASS\n");
}

static void test_persist_footer_invalid(void) {
    printf("  test_persist_footer_invalid...");

    graveldb_status_t rc = ckpt_persist_footer(NULL, 0, 0);
    assert(rc == GRAVELDB_ERR_INVALID);

    printf(" PASS\n");
}

static void test_full_checkpoint_purges_deltas(void) {
    printf("  test_full_checkpoint_purges_deltas...");

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

    /* Create some delta checkpoints */
    for (int i = 0; i < 3; i++) {
        insert_features(db, i * 20 + 1, 20);
        graveldb_flush(db);
        graveldb_checkpoint(db);
    }

    /* Verify deltas exist */
    CkptDeltaInfo *deltas = NULL;
    int count = ckpt_list_deltas(TEST_DIR, DIM, &deltas);
    assert(count >= 1);
    ckpt_free_deltas(deltas, count);

    /* Request and execute full checkpoint */
    CkptScheduler sched;
    CkptConfig ckpt_config = {
        .flush_interval_ms = 0,
        .checkpoint_interval_s = 0,
        .full_cooldown_ms = 0,
    };
    rc = ckpt_scheduler_init(&sched, db, &ckpt_config);
    assert(rc == GRAVELDB_OK);

    CkptFullResponse resp = {0};
    ckpt_scheduler_request_full(&sched, &resp);
    ckpt_scheduler_force_checkpoint(&sched);
    assert(resp.completed == true);

    /* After full, old deltas should be purged */
    count = ckpt_list_deltas(TEST_DIR, DIM, &deltas);
    assert(count == 0);
    ckpt_free_deltas(deltas, count);

    ckpt_scheduler_destroy(&sched);
    graveldb_close(db);
    printf(" PASS\n");
}

static void test_recovery_with_footer_and_deltas(void) {
    printf("  test_recovery_with_footer_and_deltas...");

    cleanup_test_dir();

    GravelDBConfig config = {0};
    config.data_dir = TEST_DIR;
    int dims[] = {DIM};
    config.dims = dims;
    config.num_dims = 1;
    config.buffer_size = 8 * 1024 * 1024;
    config.index_capacity = 1024;

    /* Phase 1: Create data with proper checkpoint + deltas */
    {
        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        /* Insert initial batch */
        insert_features(db, 1, 200);
        graveldb_flush(db);
        graveldb_checkpoint(db);

        /* More modifications */
        insert_features(db, 201, 100);
        graveldb_flush(db);
        graveldb_checkpoint(db);

        graveldb_close(db);
    }

    /* Phase 2: Reopen and verify recovery works */
    {
        GravelDB *db = NULL;
        graveldb_status_t rc = graveldb_open(&db, &config);
        assert(rc == GRAVELDB_OK);

        GravelDBStats stats;
        graveldb_stats(db, &stats);
        assert(stats.total_features == 300);

        /* Spot check some values */
        float out[DIM];
        int out_dim;
        rc = graveldb_get(db, NULL, 1, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - 100.0f) < 1e-6f);

        rc = graveldb_get(db, NULL, 250, out, &out_dim);
        assert(rc == GRAVELDB_OK);
        assert(fabsf(out[0] - 25000.0f) < 1e-6f);

        graveldb_close(db);
    }

    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB Checkpoint Tests\n");
    printf("==========================\n\n");

    test_scheduler_init_destroy();
    test_scheduler_tick_flush();
    test_scheduler_tick_checkpoint();
    test_scheduler_tick_invalid();
    test_force_flush();
    test_force_checkpoint();
    test_request_full_basic();
    test_request_full_null_params();
    test_request_full_singleflight();
    test_request_full_cooldown();
    test_scheduler_stats();
    test_delta_dump_and_replay();
    test_footer_persist_and_read();
    test_footer_read_invalid();
    test_delta_list_and_purge();
    test_delta_replay_invalid();
    test_dump_delta_invalid();
    test_dump_full_invalid();
    test_persist_footer_invalid();
    test_full_checkpoint_purges_deltas();
    test_recovery_with_footer_and_deltas();

    printf("\n All checkpoint tests PASSED!\n\n");
    cleanup_test_dir();
    return 0;
}
