/*
 * GravelDB Replica Example — Double Buffer + Delta Load + Lock-Free Reads
 *
 * Architecture:
 *   ┌──────────────┐       checkpoint dump file       ┌──────────────────────┐
 *   │   Master     │  ──────────────────────────────► │     Replica          │
 *   │  (writer)    │                                  │  db[0] / db[1]       │
 *   └──────────────┘                                  │  atomic active_idx   │
 *                                                     │  N reader threads    │
 *                                                     └──────────────────────┘
 *
 * The replica maintains two GravelDB instances (double buffer).
 * A background loader thread imports delta dumps into the standby buffer,
 * then atomically swaps the active index. Reader threads always read from
 * the active buffer with zero synchronization.
 *
 * This pattern enables:
 *   - Zero-downtime hot reload of new data
 *   - No read locks, no mutexes on the hot path
 *   - Bounded memory: at most 2x data size
 *
 * Build:
 *   cc -o replica replica.c -lgraveldb -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include "graveldb.h"

#define DIM             128
#define NUM_READERS     4
#define NUM_FEATURES    1000
#define RELOAD_INTERVAL_SEC 2

/* Shared state */
static GravelDB *g_db[2];
static _Atomic int g_active_idx = 0;
static _Atomic int g_running = 1;

/* Reader thread: continuously reads from active buffer, no locks */
static void *reader_thread(void *arg) {
    int tid = (int)(intptr_t)arg;
    uint64_t reads = 0;
    uint64_t hits = 0;

    while (atomic_load(&g_running)) {
        /* Read the active index — guaranteed to point to a fully-loaded db */
        int idx = atomic_load_explicit(&g_active_idx, memory_order_acquire);
        GravelDB *db = g_db[idx];

        /* Random read */
        uint64_t feat_id = (uint64_t)(rand() % NUM_FEATURES) + 1;
        float *out = NULL;
        int out_dim = 0;

        graveldb_status_t st = graveldb_batch_get(db, NULL, &feat_id, 1, &out, &out_dim);
        reads++;
        if (st == GRAVELDB_OK && out != NULL) {
            hits++;
        }

        /* Simulate some work */
        if (reads % 10000 == 0) {
            usleep(1000);  /* yield occasionally */
        }
    }

    printf("[reader %d] total_reads=%lu hits=%lu hit_rate=%.2f%%\n",
           tid, (unsigned long)reads, (unsigned long)hits,
           reads > 0 ? (double)hits / (double)reads * 100.0 : 0.0);
    return NULL;
}

/* Simulate master: write data and produce a checkpoint dump file */
static graveldb_status_t produce_checkpoint(const char *master_dir, const char **dump_path) {
    GravelDB *master = NULL;
    int dims[] = {DIM};
    GravelDBConfig config = {
        .data_dir       = master_dir,
        .dims           = dims,
        .num_dims       = 1,
        .buffer_size    = 64 * 1024 * 1024,
        .index_capacity = 1 << 16,
        .auto_create_bins = true,
    };

    graveldb_status_t st = graveldb_open(&master, &config);
    if (st != GRAVELDB_OK) return st;

    /* Write features */
    for (int batch = 0; batch < NUM_FEATURES; batch += 100) {
        int n = (NUM_FEATURES - batch) < 100 ? (NUM_FEATURES - batch) : 100;
        uint64_t ids[100];
        int d[100];
        float vecs[100][DIM];
        const float *ptrs[100];

        for (int i = 0; i < n; i++) {
            ids[i] = (uint64_t)(batch + i + 1);
            d[i] = DIM;
            for (int j = 0; j < DIM; j++)
                vecs[i][j] = (float)(batch + i) * 0.01f + (float)j * 0.001f;
            ptrs[i] = vecs[i];
        }
        st = graveldb_batch_put(master, NULL, ids, d, ptrs, n);
        if (st != GRAVELDB_OK) { graveldb_close(master); return st; }
    }

    /* Flush + begin checkpoint to enable export */
    st = graveldb_flush(master);
    if (st != GRAVELDB_OK) { graveldb_close(master); return st; }

    /* Start checkpoint (puts overlay in place) */
    while (graveldb_checkpoint_step(master, 64) == GRAVELDB_AGAIN) {}

    /* Export full checkpoint */
    GravelDBCkptExport *exp = NULL;
    st = graveldb_ckpt_export_begin(master, &exp, GRAVELDB_CKPT_FULL, DIM, 0, 256, 0);
    if (st != GRAVELDB_OK) { graveldb_close(master); return st; }

    while ((st = graveldb_ckpt_export_step(exp)) == GRAVELDB_AGAIN) {
        while (graveldb_ckpt_export_poll(exp) == GRAVELDB_AGAIN) {}
    }
    graveldb_ckpt_export_end(exp);

    *dump_path = strdup(graveldb_ckpt_export_path(exp));
    printf("[master] exported checkpoint: %s (%lu bytes)\n",
           *dump_path, (unsigned long)graveldb_ckpt_export_size(exp));

    graveldb_ckpt_export_destroy(exp);
    graveldb_close(master);
    return GRAVELDB_OK;
}

/* Load a checkpoint dump into a GravelDB instance */
static graveldb_status_t load_dump(GravelDB *db, const char *dump_path) {
    GravelDBCkptImport *imp = NULL;
    graveldb_status_t st = graveldb_ckpt_import_begin(db, &imp, DIM, dump_path);
    if (st != GRAVELDB_OK) return st;

    while ((st = graveldb_ckpt_import_step(imp)) == GRAVELDB_AGAIN) {
        while (graveldb_ckpt_import_poll(imp) == GRAVELDB_AGAIN) {}
    }

    graveldb_status_t end_st = graveldb_ckpt_import_end(imp);
    return (st == GRAVELDB_OK) ? end_st : st;
}

/* Open a replica GravelDB instance */
static graveldb_status_t open_replica(GravelDB **db, const char *dir) {
    int dims[] = {DIM};
    GravelDBConfig config = {
        .data_dir       = dir,
        .dims           = dims,
        .num_dims       = 1,
        .buffer_size    = 64 * 1024 * 1024,
        .index_capacity = 1 << 16,
        .auto_create_bins = true,
    };
    return graveldb_open(db, &config);
}

int main(void) {
    printf("=== GravelDB Replica: Double Buffer + Lock-Free Reads ===\n\n");

    /* Step 1: Produce a checkpoint from "master" */
    const char *dump_path = NULL;
    graveldb_status_t st = produce_checkpoint("/tmp/graveldb_master", &dump_path);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "failed to produce checkpoint: %d\n", st);
        return 1;
    }

    /* Step 2: Open two replica instances (double buffer) */
    st = open_replica(&g_db[0], "/tmp/graveldb_replica_0");
    if (st != GRAVELDB_OK) { fprintf(stderr, "open replica 0 failed: %d\n", st); return 1; }

    st = open_replica(&g_db[1], "/tmp/graveldb_replica_1");
    if (st != GRAVELDB_OK) { fprintf(stderr, "open replica 1 failed: %d\n", st); return 1; }

    /* Step 3: Load initial data into buffer 0 */
    printf("[loader] loading initial data into buffer 0...\n");
    st = load_dump(g_db[0], dump_path);
    if (st != GRAVELDB_OK) { fprintf(stderr, "initial load failed: %d\n", st); return 1; }
    printf("[loader] buffer 0 ready\n");

    /* Step 4: Start reader threads — they read from g_db[g_active_idx] */
    pthread_t readers[NUM_READERS];
    for (int i = 0; i < NUM_READERS; i++) {
        pthread_create(&readers[i], NULL, reader_thread, (void *)(intptr_t)i);
    }
    printf("[main] %d reader threads started\n\n", NUM_READERS);

    /* Step 5: Simulate a reload cycle — load new data into standby, then swap */
    sleep(RELOAD_INTERVAL_SEC);

    int standby_idx = 1 - atomic_load(&g_active_idx);
    printf("[loader] reloading into standby buffer %d...\n", standby_idx);

    st = load_dump(g_db[standby_idx], dump_path);
    if (st != GRAVELDB_OK) {
        fprintf(stderr, "reload failed: %d\n", st);
    } else {
        /* Atomic swap — readers seamlessly switch to new data */
        atomic_store_explicit(&g_active_idx, standby_idx, memory_order_release);
        printf("[loader] swapped active buffer to %d (zero-downtime reload complete)\n", standby_idx);
    }

    /* Let readers run a bit more, then stop */
    sleep(RELOAD_INTERVAL_SEC);
    atomic_store(&g_running, 0);

    for (int i = 0; i < NUM_READERS; i++) {
        pthread_join(readers[i], NULL);
    }

    /* Cleanup */
    graveldb_close(g_db[0]);
    graveldb_close(g_db[1]);
    free((void *)dump_path);

    printf("\ndone.\n");
    return 0;
}
