/*
 * GravelDB - Write Buffer Effectiveness Benchmark
 *
 * Compares performance of:
 *   1. Write-buffer hit (put → get, forwarding from buffer)
 *   2. Read-cache hit (flush → get, served from read cache)
 *   3. Disk read (cold, no cache/buffer hit)
 *   4. Direct pwrite baseline (no buffer, raw syscall)
 *
 * Also validates correctness: read-your-writes consistency across
 * all code paths.
 *
 * Key question answered: "How much does the write buffer actually help
 * compared to not having one (going to disk every time)?"
 */

#define _XOPEN_SOURCE 700
#include "graveldb_impl.h"
#include "dimbin.h"
#include "slab_alloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>

#define BENCH_DIR "/tmp/graveldb_bench_wbuf"

/* ─────────────────── Utilities ─────────────────── */

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

static void cleanup(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCH_DIR);
    (void)system(cmd);
}

/* ── Latency stats ── */
typedef struct {
    double *samples;
    int     count;
    int     capacity;
} LatBuf;

static void lat_init(LatBuf *lb, int cap) {
    lb->samples = (double *)malloc(cap * sizeof(double));
    lb->count = 0;
    lb->capacity = cap;
}
static void lat_destroy(LatBuf *lb) { free(lb->samples); }

static inline void lat_record(LatBuf *lb, double us) {
    if (lb->count < lb->capacity) lb->samples[lb->count++] = us;
}

static int cmp_double(const void *a, const void *b) {
    double va = *(const double *)a, vb = *(const double *)b;
    return (va > vb) - (va < vb);
}

static void lat_print(LatBuf *lb, const char *label) {
    if (lb->count == 0) { printf("    %-36s | no data\n", label); return; }
    qsort(lb->samples, lb->count, sizeof(double), cmp_double);
    double avg = 0;
    for (int i = 0; i < lb->count; i++) avg += lb->samples[i];
    avg /= lb->count;
    double p50 = lb->samples[lb->count / 2];
    double p99 = lb->samples[(int)(lb->count * 0.99)];
    double p999 = lb->samples[(int)(lb->count * 0.999)];
    double max = lb->samples[lb->count - 1];
    double ops_per_sec = lb->count / (avg * lb->count / 1e6);

    printf("    %-36s | %9.0f ops/s | avg %7.2fµs | p50 %7.2fµs | p99 %7.2fµs | p999 %7.2fµs | max %8.1fµs\n",
           label, ops_per_sec, avg, p50, p99, p999, max);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 1: Write-buffer GET path comparison
 *
 * Put N entries → measure get latency from write buffer (forwarding).
 * Then flush → measure get latency from read cache.
 * Then close/reopen (cold cache) → measure get latency from disk.
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_get_paths(void) {
    printf("── GET Path Latency Comparison ──\n\n");

    int dim = 128;
    int num_features = 50000;
    int num_reads = 50000;

    cleanup();

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 256 * 1024 * 1024;  /* large enough to hold all */
    config.index_capacity = num_features * 2;

    GravelDB *db = NULL;
    graveldb_open(&db, &config);
    if (!db) { fprintf(stderr, "Failed to open DB\n"); return; }

    float *emb = (float *)malloc(dim * sizeof(float));
    float *out = (float *)malloc(dim * sizeof(float));
    int out_dim;

    /* Populate */
    for (int i = 1; i <= num_features; i++) {
        for (int j = 0; j < dim; j++) emb[j] = (float)(i * 1000 + j) * 0.001f;
        graveldb_put(db, NULL, (uint64_t)i, dim, emb);
    }

    /* ── Path 1: Get from write buffer (forwarding) ── */
    {
        LatBuf lb;
        lat_init(&lb, num_reads);
        uint64_t rng = 0xCAFE1234ULL;

        for (int i = 0; i < num_reads; i++) {
            uint64_t fid = (xorshift64(&rng) % num_features) + 1;
            double t0 = now_sec();
            graveldb_get(db, NULL, fid, out, &out_dim);
            lat_record(&lb, (now_sec() - t0) * 1e6);
        }
        lat_print(&lb, "GET: write-buffer forward");
        lat_destroy(&lb);
    }

    /* Flush → data now on disk, read cache starts filling */
    graveldb_flush(db);

    /* ── Path 2: Get from read cache (warm) ── */
    /* First pass: warm the read cache */
    for (int i = 1; i <= num_features; i++) {
        graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
    }

    /* Second pass: measure read-cache hit */
    {
        LatBuf lb;
        lat_init(&lb, num_reads);
        uint64_t rng = 0xCAFE1234ULL;

        for (int i = 0; i < num_reads; i++) {
            uint64_t fid = (xorshift64(&rng) % num_features) + 1;
            double t0 = now_sec();
            graveldb_get(db, NULL, fid, out, &out_dim);
            lat_record(&lb, (now_sec() - t0) * 1e6);
        }
        lat_print(&lb, "GET: read-cache hit");
        lat_destroy(&lb);
    }

    /* ── Path 3: Disk read (cold cache) ── */
    /* Close and reopen to clear all in-memory state */
    graveldb_close(db);
    db = NULL;
    graveldb_open(&db, &config);
    if (!db) { fprintf(stderr, "Failed to reopen DB\n"); goto done; }

    {
        LatBuf lb;
        lat_init(&lb, num_reads);
        uint64_t rng = 0xCAFE1234ULL;

        for (int i = 0; i < num_reads; i++) {
            uint64_t fid = (xorshift64(&rng) % num_features) + 1;
            double t0 = now_sec();
            graveldb_get(db, NULL, fid, out, &out_dim);
            lat_record(&lb, (now_sec() - t0) * 1e6);
        }
        lat_print(&lb, "GET: disk (cold, no cache)");
        lat_destroy(&lb);
    }

done:
    free(emb);
    free(out);
    if (db) graveldb_close(db);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 2: PUT path — write buffer vs direct pwrite
 *
 * Measures:
 *   - PUT into write buffer (memcpy into buffer page)
 *   - Direct pwrite (simulating "no buffer" — write straight to disk)
 *
 * This shows the write amplification benefit of buffering.
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_put_paths(void) {
    printf("── PUT Path Latency Comparison ──\n\n");

    int dim = 128;
    int num_ops = 100000;
    size_t entry_bytes = dim * sizeof(float);

    cleanup();

    /* ── Path A: PUT via write buffer (normal graveldb_put) ── */
    {
        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 256 * 1024 * 1024;
        config.index_capacity = num_ops * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) return;

        float *emb = (float *)malloc(entry_bytes);
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        LatBuf lb;
        lat_init(&lb, num_ops);

        for (int i = 1; i <= num_ops; i++) {
            emb[0] = (float)i;
            double t0 = now_sec();
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
            double elapsed = (now_sec() - t0) * 1e6;
            lat_record(&lb, elapsed);
        }
        lat_print(&lb, "PUT: write-buffer (memcpy)");
        lat_destroy(&lb);
        free(emb);
        graveldb_close(db);
    }

    cleanup();

    /* ── Path B: Direct pwrite (no buffer, simulated) ── */
    /*
     * This simulates what would happen if every put went directly to disk:
     * open file, pwrite the embedding at the correct offset, fdatasync.
     * We skip fdatasync per-write (too extreme) but do a periodic fsync
     * to simulate WAL-less direct-write semantics.
     */
    {
        char fpath[256];
        snprintf(fpath, sizeof(fpath), "%s/direct_write.bin", BENCH_DIR);
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", BENCH_DIR);
        system(cmd);

        int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) { perror("open"); return; }

        /* Pre-allocate file to avoid extent-allocation overhead during bench */
        size_t file_size = (size_t)num_ops * entry_bytes;
        if (ftruncate(fd, file_size) < 0) { perror("ftruncate"); close(fd); return; }

        float *emb = (float *)malloc(entry_bytes);
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        LatBuf lb;
        lat_init(&lb, num_ops);

        for (int i = 0; i < num_ops; i++) {
            emb[0] = (float)(i + 1);
            off_t offset = (off_t)i * entry_bytes;
            double t0 = now_sec();
            pwrite(fd, emb, entry_bytes, offset);
            double elapsed = (now_sec() - t0) * 1e6;
            lat_record(&lb, elapsed);

            /* Periodic fsync every 64 writes (same cadence as proactive flush) */
            if ((i & 63) == 63) fdatasync(fd);
        }
        lat_print(&lb, "PUT: direct pwrite (no buffer)");
        lat_destroy(&lb);
        free(emb);
        close(fd);
    }

    cleanup();

    /* ── Path C: Direct pwrite + fdatasync every write ── */
    {
        char fpath[256];
        snprintf(fpath, sizeof(fpath), "%s/direct_sync.bin", BENCH_DIR);
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", BENCH_DIR);
        system(cmd);

        int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) { perror("open"); close(fd); return; }

        size_t file_size = (size_t)num_ops * entry_bytes;
        if (ftruncate(fd, file_size) < 0) { perror("ftruncate"); close(fd); return; }

        float *emb = (float *)malloc(entry_bytes);
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        /* Only measure 10K ops (fdatasync per write is extremely slow) */
        int sync_ops = 10000;
        LatBuf lb;
        lat_init(&lb, sync_ops);

        for (int i = 0; i < sync_ops; i++) {
            emb[0] = (float)(i + 1);
            off_t offset = (off_t)i * entry_bytes;
            double t0 = now_sec();
            pwrite(fd, emb, entry_bytes, offset);
            fdatasync(fd);
            double elapsed = (now_sec() - t0) * 1e6;
            lat_record(&lb, elapsed);
        }
        lat_print(&lb, "PUT: pwrite + fdatasync/write");
        lat_destroy(&lb);
        free(emb);
        close(fd);
    }

    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 3: Throughput comparison (ops/sec and MB/s)
 *
 * Measures aggregate throughput for:
 *   - Buffered write → buffered read (read-your-writes)
 *   - Buffered write → flush → read-cache read
 *   - Direct I/O baseline (pread/pwrite, page-aligned)
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_throughput_comparison(void) {
    printf("── Throughput: Buffered vs Direct I/O ──\n\n");

    int dims[] = {64, 128, 256, 512};
    int num_dims = sizeof(dims) / sizeof(dims[0]);
    int num_features = 100000;

    printf("  %-6s | %-16s | %-16s | %-16s | %-16s | %-16s\n",
           "Dim", "Buf PUT ops/s", "Buf GET(wb) ops", "Buf GET(rc) ops", "Direct WR ops/s", "Direct RD ops/s");
    printf("  %-6s-+-%-16s-+-%-16s-+-%-16s-+-%-16s-+-%-16s\n",
           "------", "----------------", "----------------",
           "----------------", "----------------", "----------------");

    for (int d = 0; d < num_dims; d++) {
        int dim = dims[d];
        size_t entry_bytes = dim * sizeof(float);

        cleanup();

        /* ── Buffered path ── */
        GravelDBConfig config = {0};
        config.data_dir = BENCH_DIR;
        config.dims = &dim;
        config.num_dims = 1;
        config.buffer_size = 256 * 1024 * 1024;
        config.index_capacity = num_features * 2;

        GravelDB *db = NULL;
        graveldb_open(&db, &config);
        if (!db) continue;

        float *emb = (float *)malloc(entry_bytes);
        float *out = (float *)malloc(entry_bytes);
        int out_dim;
        for (int j = 0; j < dim; j++) emb[j] = (float)j * 0.01f;

        /* Buffered PUT */
        double t0 = now_sec();
        for (int i = 1; i <= num_features; i++) {
            emb[0] = (float)i;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        double buf_put_ops = num_features / (now_sec() - t0);

        /* Buffered GET from write buffer */
        t0 = now_sec();
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        }
        double buf_get_wb_ops = num_features / (now_sec() - t0);

        /* Flush, then GET from read cache */
        graveldb_flush(db);
        /* warm cache */
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        }
        /* measure */
        t0 = now_sec();
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
        }
        double buf_get_rc_ops = num_features / (now_sec() - t0);

        graveldb_close(db);

        /* ── Direct I/O path ── */
        cleanup();
        char fpath[256];
        snprintf(fpath, sizeof(fpath), "%s/direct_%d.bin", BENCH_DIR, dim);
        char cmd2[256];
        snprintf(cmd2, sizeof(cmd2), "mkdir -p %s", BENCH_DIR);
        system(cmd2);

        int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) { free(emb); free(out); continue; }

        size_t file_size = (size_t)num_features * entry_bytes;
        ftruncate(fd, file_size);

        /* Direct write (pwrite, periodic fsync every 256) */
        t0 = now_sec();
        for (int i = 0; i < num_features; i++) {
            emb[0] = (float)(i + 1);
            pwrite(fd, emb, entry_bytes, (off_t)i * entry_bytes);
            if ((i & 255) == 255) fdatasync(fd);
        }
        fdatasync(fd);
        double direct_wr_ops = num_features / (now_sec() - t0);

        /* Direct read (pread, sequential) */
        t0 = now_sec();
        for (int i = 0; i < num_features; i++) {
            (void)pread(fd, out, entry_bytes, (off_t)i * entry_bytes);
        }
        double direct_rd_ops = num_features / (now_sec() - t0);

        close(fd);

        printf("  %-6d | %16.0f | %16.0f | %16.0f | %16.0f | %16.0f\n",
               dim, buf_put_ops, buf_get_wb_ops, buf_get_rc_ops,
               direct_wr_ops, direct_rd_ops);

        free(emb);
        free(out);
    }

    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 4: Correctness validation (read-your-writes)
 *
 * Verifies that:
 *   1. After put(), get() returns correct data (from write buffer)
 *   2. After flush(), get() still returns correct data (from read cache / disk)
 *   3. Overwrite semantics: latest write wins in all paths
 *   4. Interleaved put/get with random keys maintains consistency
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_correctness(void) {
    printf("── Correctness Validation (read-your-writes) ──\n\n");

    int dim = 64;
    int num_features = 10000;
    size_t entry_bytes = dim * sizeof(float);

    cleanup();

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 64 * 1024 * 1024;
    config.index_capacity = num_features * 4;

    GravelDB *db = NULL;
    graveldb_open(&db, &config);
    if (!db) { fprintf(stderr, "Failed to open DB\n"); return; }

    float *emb = (float *)malloc(entry_bytes);
    float *out = (float *)malloc(entry_bytes);
    int out_dim;
    int errors = 0;

    /* ── Test 1: put → get from write buffer ── */
    {
        int test_errors = 0;
        for (int i = 1; i <= num_features; i++) {
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 1000 + j) * 0.001f;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }

        for (int i = 1; i <= num_features; i++) {
            graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            if (rc != GRAVELDB_OK) { test_errors++; continue; }
            if (out_dim != dim) { test_errors++; continue; }
            /* Check first and last float */
            float expected_0 = (float)(i * 1000 + 0) * 0.001f;
            float expected_last = (float)(i * 1000 + dim - 1) * 0.001f;
            if (fabsf(out[0] - expected_0) > 1e-5f ||
                fabsf(out[dim-1] - expected_last) > 1e-5f) {
                test_errors++;
            }
        }
        printf("    Test 1: put→get (write-buffer)     : %s (%d errors)\n",
               test_errors == 0 ? "PASS" : "FAIL", test_errors);
        errors += test_errors;
    }

    /* ── Test 2: flush → get from read cache / disk ── */
    {
        int test_errors = 0;
        graveldb_flush(db);

        for (int i = 1; i <= num_features; i++) {
            graveldb_status_t rc = graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            if (rc != GRAVELDB_OK) { test_errors++; continue; }
            float expected_0 = (float)(i * 1000 + 0) * 0.001f;
            float expected_last = (float)(i * 1000 + dim - 1) * 0.001f;
            if (fabsf(out[0] - expected_0) > 1e-5f ||
                fabsf(out[dim-1] - expected_last) > 1e-5f) {
                test_errors++;
            }
        }
        printf("    Test 2: flush→get (read-cache/disk): %s (%d errors)\n",
               test_errors == 0 ? "PASS" : "FAIL", test_errors);
        errors += test_errors;
    }

    /* ── Test 3: overwrite → latest value wins ── */
    {
        int test_errors = 0;

        /* Overwrite all entries with new values */
        for (int i = 1; i <= num_features; i++) {
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 2000 + j) * 0.002f;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }

        /* Verify from write buffer */
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            float expected_0 = (float)(i * 2000 + 0) * 0.002f;
            if (fabsf(out[0] - expected_0) > 1e-5f) test_errors++;
        }

        /* Flush and verify from disk */
        graveldb_flush(db);
        for (int i = 1; i <= num_features; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            float expected_0 = (float)(i * 2000 + 0) * 0.002f;
            if (fabsf(out[0] - expected_0) > 1e-5f) test_errors++;
        }

        printf("    Test 3: overwrite (latest wins)    : %s (%d errors)\n",
               test_errors == 0 ? "PASS" : "FAIL", test_errors);
        errors += test_errors;
    }

    /* ── Test 4: interleaved put/get with random keys ── */
    {
        int test_errors = 0;
        int num_ops = 50000;
        uint64_t rng = 0xDEAD4321ULL;

        /* Track expected values */
        float *expected = (float *)calloc(num_features + 1, sizeof(float));

        for (int op = 0; op < num_ops; op++) {
            uint64_t fid = (xorshift64(&rng) % num_features) + 1;
            float val = (float)(op * 3 + 7) * 0.0011f;

            if (op % 3 != 0) {
                /* Write */
                for (int j = 0; j < dim; j++) emb[j] = val + (float)j * 0.0001f;
                graveldb_put(db, NULL, fid, dim, emb);
                expected[fid] = val;
            } else {
                /* Read and verify */
                if (expected[fid] != 0.0f) {
                    graveldb_get(db, NULL, fid, out, &out_dim);
                    float exp_0 = expected[fid] + 0.0f * 0.0001f;
                    if (fabsf(out[0] - exp_0) > 1e-4f) {
                        test_errors++;
                    }
                }
            }
        }

        printf("    Test 4: interleaved put/get random : %s (%d errors)\n",
               test_errors == 0 ? "PASS" : "FAIL", test_errors);
        errors += test_errors;
        free(expected);
    }

    /* ── Test 5: close/reopen persistence ── */
    {
        int test_errors = 0;

        /* Write known values, flush, close */
        for (int i = 1; i <= 1000; i++) {
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 5000 + j) * 0.005f;
            graveldb_put(db, NULL, (uint64_t)i, dim, emb);
        }
        graveldb_flush(db);
        graveldb_close(db);
        db = NULL;

        /* Reopen and verify */
        graveldb_open(&db, &config);
        if (!db) { printf("    Test 5: FAIL (reopen failed)\n"); errors++; goto done; }

        for (int i = 1; i <= 1000; i++) {
            graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
            float expected_0 = (float)(i * 5000 + 0) * 0.005f;
            float expected_last = (float)(i * 5000 + dim - 1) * 0.005f;
            if (fabsf(out[0] - expected_0) > 1e-4f ||
                fabsf(out[dim-1] - expected_last) > 1e-4f) {
                test_errors++;
            }
        }

        printf("    Test 5: persistence (close/reopen) : %s (%d errors)\n",
               test_errors == 0 ? "PASS" : "FAIL", test_errors);
        errors += test_errors;
    }

done:
    printf("\n    ──────────────────────────────────────────\n");
    printf("    TOTAL: %s (%d errors)\n", errors == 0 ? "ALL PASS ✓" : "FAILURES DETECTED", errors);

    free(emb);
    free(out);
    if (db) graveldb_close(db);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 5: Read-After-Write — the critical path comparison
 *
 * This is the core question: if every put() is immediately followed by a get()
 * for the same key, how much does the write buffer help compared to doing
 * pwrite() + pread() without a buffer?
 *
 * Three paths compared:
 *   A. Buffered: graveldb_put + graveldb_get (write buffer forwarding)
 *   B. Direct pwrite + pread (OS page cache, no fsync)
 *   C. Direct pwrite + fdatasync + pread (durable, worst case)
 *
 * Each iteration: write one entry → immediately read it back → verify correct.
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_read_after_write(void) {
    printf("── Read-After-Write: Buffered vs Direct I/O ──\n\n");

    int dims[] = {64, 128, 256, 512};
    int num_dims_arr = sizeof(dims) / sizeof(dims[0]);
    int num_ops = 100000;

    printf("  %-6s | %-26s | %-26s | %-26s | Buf/Direct | Buf/Sync\n",
           "Dim", "A: Buffered (put+get)", "B: pwrite+pread", "C: pwrite+sync+pread");
    printf("  %-6s-+-%-26s-+-%-26s-+-%-26s-+-%-10s-+-%-10s\n",
           "------", "--------------------------", "--------------------------",
           "--------------------------", "----------", "----------");

    for (int d = 0; d < num_dims_arr; d++) {
        int dim = dims[d];
        size_t entry_bytes = dim * sizeof(float);

        /* ── Path A: Buffered (graveldb_put → graveldb_get) ── */
        double buf_elapsed;
        {
            cleanup();

            GravelDBConfig config = {0};
            config.data_dir = BENCH_DIR;
            config.dims = &dim;
            config.num_dims = 1;
            config.buffer_size = 256 * 1024 * 1024;
            config.index_capacity = num_ops * 2;

            GravelDB *db = NULL;
            graveldb_open(&db, &config);
            if (!db) continue;

            float *emb = (float *)malloc(entry_bytes);
            float *out = (float *)malloc(entry_bytes);
            int out_dim;
            int errors = 0;

            double t0 = now_sec();
            for (int i = 1; i <= num_ops; i++) {
                /* write */
                for (int j = 0; j < dim; j++) emb[j] = (float)(i * 100 + j) * 0.001f;
                graveldb_put(db, NULL, (uint64_t)i, dim, emb);
                /* immediately read back */
                graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
                /* verify */
                if (out_dim != dim || fabsf(out[0] - emb[0]) > 1e-5f) errors++;
            }
            buf_elapsed = now_sec() - t0;

            if (errors > 0) printf("  [WARN] Path A: %d verification errors!\n", errors);

            free(emb);
            free(out);
            graveldb_close(db);
        }

        /* ── Path B: Direct pwrite + pread (OS page cache, no fsync) ── */
        double direct_elapsed;
        {
            cleanup();
            char fpath[256];
            snprintf(fpath, sizeof(fpath), "%s/raw_direct_%d.bin", BENCH_DIR, dim);
            char cmd[256];
            snprintf(cmd, sizeof(cmd), "mkdir -p %s", BENCH_DIR);
            system(cmd);

            int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) { perror("open"); continue; }

            /* Pre-allocate to avoid extent allocation during bench */
            ftruncate(fd, (off_t)num_ops * entry_bytes);

            float *emb = (float *)malloc(entry_bytes);
            float *out = (float *)malloc(entry_bytes);
            int errors = 0;

            double t0 = now_sec();
            for (int i = 0; i < num_ops; i++) {
                /* write */
                for (int j = 0; j < dim; j++) emb[j] = (float)((i+1) * 100 + j) * 0.001f;
                off_t offset = (off_t)i * entry_bytes;
                pwrite(fd, emb, entry_bytes, offset);
                /* immediately read back */
                pread(fd, out, entry_bytes, offset);
                /* verify */
                if (fabsf(out[0] - emb[0]) > 1e-5f) errors++;
            }
            direct_elapsed = now_sec() - t0;

            if (errors > 0) printf("  [WARN] Path B: %d verification errors!\n", errors);

            free(emb);
            free(out);
            close(fd);
        }

        /* ── Path C: Direct pwrite + fdatasync + pread (durable read-after-write) ── */
        double sync_elapsed;
        {
            cleanup();
            char fpath[256];
            snprintf(fpath, sizeof(fpath), "%s/raw_sync_%d.bin", BENCH_DIR, dim);
            char cmd[256];
            snprintf(cmd, sizeof(cmd), "mkdir -p %s", BENCH_DIR);
            system(cmd);

            int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) { perror("open"); continue; }

            /* Fewer ops for sync path (it's slow) */
            int sync_ops = 20000;
            ftruncate(fd, (off_t)sync_ops * entry_bytes);

            float *emb = (float *)malloc(entry_bytes);
            float *out = (float *)malloc(entry_bytes);
            int errors = 0;

            double t0 = now_sec();
            for (int i = 0; i < sync_ops; i++) {
                for (int j = 0; j < dim; j++) emb[j] = (float)((i+1) * 100 + j) * 0.001f;
                off_t offset = (off_t)i * entry_bytes;
                pwrite(fd, emb, entry_bytes, offset);
                fdatasync(fd);
                pread(fd, out, entry_bytes, offset);
                if (fabsf(out[0] - emb[0]) > 1e-5f) errors++;
            }
            sync_elapsed = now_sec() - t0;

            /* Normalize to same op count for fair ops/s comparison */
            sync_elapsed = sync_elapsed / sync_ops * num_ops;

            if (errors > 0) printf("  [WARN] Path C: %d verification errors!\n", errors);

            free(emb);
            free(out);
            close(fd);
        }

        double buf_ops   = num_ops / buf_elapsed;
        double dir_ops   = num_ops / direct_elapsed;
        double sync_ops_s = num_ops / sync_elapsed;

        printf("  %-6d | %12.0f ops/s %6.2fµs | %12.0f ops/s %6.2fµs | %12.0f ops/s %6.2fµs | %9.2fx | %9.2fx\n",
               dim,
               buf_ops,   buf_elapsed / num_ops * 1e6,
               dir_ops,   direct_elapsed / num_ops * 1e6,
               sync_ops_s, sync_elapsed / num_ops * 1e6,
               buf_ops / dir_ops,
               buf_ops / sync_ops_s);
    }

    printf("\n  Note: 'Buf/Direct' > 1 means buffer is faster; < 1 means direct I/O wins.\n");
    printf("        'Buf/Sync' shows advantage over durable (fsync'd) direct writes.\n");
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 6: Read After Write-More (Page Cache Eviction)
 *
 * The key scenario: you write key A, then continue writing MANY more keys.
 * The subsequent writes evict A's page from OS page cache (LRU pressure).
 * When you later read A:
 *   - Write buffer still has it in memory → O(1) hashmap hit
 *   - Direct I/O path: pread must go to disk (page cache miss)
 *
 * Protocol:
 *   1. Write N "target" entries (these are what we'll read later)
 *   2. Write M >> N "pollution" entries (evicts targets from page cache)
 *   3. Read back the original N target entries in random order
 *
 * Compares:
 *   A. Buffered: write buffer holds ALL entries → forwarding hit for targets
 *   B. Direct: pwrite targets, pwrite pollution (evicts targets), pread targets
 *
 * This is the CORE value proposition of the write buffer: it retains all
 * recent writes regardless of subsequent write volume, while OS page cache
 * is LRU-evicted by later writes.
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void shuffle_u32(uint32_t *arr, int n, uint64_t seed) {
    /* Fisher-Yates */
    uint64_t rng = seed;
    for (int i = n - 1; i > 0; i--) {
        uint64_t r = xorshift64(&rng);
        int j = r % (i + 1);
        uint32_t tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

static void bench_scattered_raw(void) {
    printf("── Read After Write-More (Page Cache Eviction) ──\n\n");
    printf("  Scenario: write N targets → write M pollution (evicts targets) → read targets\n");
    printf("  Buffer retains ALL; OS page cache LRU-evicts earlier pages.\n\n");

    int dim = 128;
    size_t entry_bytes = dim * sizeof(float);

    /*
     * N_target = entries we care about reading back
     * M_pollution = subsequent writes that push targets out of page cache
     * We scale pollution to show degradation of direct path.
     */
    int N_target = 50000;  /* 24MB of target data */
    int pollution_multiples[] = {2, 8, 16, 32};
    int num_cases = sizeof(pollution_multiples) / sizeof(pollution_multiples[0]);

    printf("  %-8s | %-12s | %-10s | %-26s | %-26s | Speedup\n",
           "Targets", "Pollution", "Total WS", "A: Buffered GET", "B: Direct pread");
    printf("  %-8s-+-%-12s-+-%-10s-+-%-26s-+-%-26s-+--------\n",
           "--------", "------------", "----------",
           "--------------------------", "--------------------------");

    float *emb = (float *)malloc(entry_bytes);
    float *out = (float *)malloc(entry_bytes);
    int out_dim;

    /* Shuffled read order for targets */
    uint32_t *read_order = (uint32_t *)malloc(N_target * sizeof(uint32_t));
    for (int i = 0; i < N_target; i++) read_order[i] = i;
    shuffle_u32(read_order, N_target, 0xFEED4321ULL);

    for (int c = 0; c < num_cases; c++) {
        int M_pollution = pollution_multiples[c] * N_target;
        int total = N_target + M_pollution;
        size_t total_mb = (size_t)total * entry_bytes / (1024 * 1024);
        size_t pollute_mb = (size_t)M_pollution * entry_bytes / (1024 * 1024);

        /* ── Path A: Buffered ──
         * Write buffer large enough to hold ALL (targets + pollution).
         * Targets remain in buffer → forwarding still works. */
        double buf_read_elapsed;
        {
            cleanup();

            GravelDBConfig config = {0};
            config.data_dir = BENCH_DIR;
            config.dims = &dim;
            config.num_dims = 1;
            config.buffer_size = (size_t)total * entry_bytes + 64 * 1024 * 1024;
            config.index_capacity = (uint32_t)(total * 2);

            GravelDB *db = NULL;
            graveldb_open(&db, &config);
            if (!db) continue;

            /* Phase 1: write target entries (IDs 1..N) */
            for (int i = 0; i < N_target; i++) {
                for (int j = 0; j < dim; j++) emb[j] = (float)((i+1) * 100 + j) * 0.001f;
                graveldb_put(db, NULL, (uint64_t)(i + 1), dim, emb);
            }

            /* Phase 2: write pollution entries (IDs N+1..N+M) — pushes page cache */
            for (int i = 0; i < M_pollution; i++) {
                for (int j = 0; j < dim; j++) emb[j] = (float)((i + N_target + 1) * 77 + j) * 0.002f;
                graveldb_put(db, NULL, (uint64_t)(N_target + i + 1), dim, emb);
            }

            /* Phase 3: read back TARGET entries (random order) — still in buffer */
            double t0 = now_sec();
            for (int i = 0; i < N_target; i++) {
                uint64_t fid = (uint64_t)(read_order[i] + 1);
                graveldb_get(db, NULL, fid, out, &out_dim);
            }
            buf_read_elapsed = now_sec() - t0;

            graveldb_close(db);
        }

        /* ── Path B: Direct I/O ──
         * Write targets to file, then write M pollution entries AFTER them.
         * The pollution writes fill page cache and push target pages out (LRU).
         * Then try to pread the target entries back — page cache miss. */
        double direct_read_elapsed;
        {
            cleanup();
            char cmd[256];
            snprintf(cmd, sizeof(cmd), "mkdir -p %s", BENCH_DIR);
            system(cmd);

            char fpath[256];
            snprintf(fpath, sizeof(fpath), "%s/eviction_test.bin", BENCH_DIR);

            int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) { perror("open"); continue; }

            /* File layout: [targets: 0..N-1] [pollution: N..N+M-1] */
            size_t total_size = (size_t)total * entry_bytes;
            ftruncate(fd, total_size);

            /* Phase 1: write target entries */
            for (int i = 0; i < N_target; i++) {
                for (int j = 0; j < dim; j++) emb[j] = (float)((i+1) * 100 + j) * 0.001f;
                pwrite(fd, emb, entry_bytes, (off_t)i * entry_bytes);
            }

            /* Phase 2: write pollution — this fills page cache with new pages,
             * creating LRU pressure that evicts the target pages */
            for (int i = 0; i < M_pollution; i++) {
                for (int j = 0; j < dim; j++) emb[j] = (float)((i + N_target + 1) * 77 + j) * 0.002f;
                pwrite(fd, emb, entry_bytes, (off_t)(N_target + i) * entry_bytes);
            }

            /* Flush and close to ensure data hits disk */
            fdatasync(fd);
            close(fd);

            /* Reopen with cache-bypass to simulate evicted state */
            fd = open(fpath, O_RDONLY);
            if (fd < 0) { perror("reopen"); continue; }
#ifdef __APPLE__
            fcntl(fd, F_NOCACHE, 1);  /* Bypass page cache on reads */
#elif defined(__linux__)
            /* Evict only the target region pages */
            posix_fadvise(fd, 0, (off_t)N_target * entry_bytes, POSIX_FADV_DONTNEED);
#endif

            /* Phase 3: random pread of target entries (page cache miss) */
            double t0 = now_sec();
            for (int i = 0; i < N_target; i++) {
                off_t offset = (off_t)read_order[i] * entry_bytes;
                pread(fd, out, entry_bytes, offset);
            }
            direct_read_elapsed = now_sec() - t0;

            close(fd);
        }

        double buf_ops = N_target / buf_read_elapsed;
        double dir_ops = N_target / direct_read_elapsed;
        double speedup = buf_ops / dir_ops;

        printf("  %6d  | %6d (%3zuMB) | %4zuMB     | %12.0f ops/s %6.2fµs | %12.0f ops/s %6.2fµs | %6.2fx\n",
               N_target, M_pollution, pollute_mb, total_mb,
               buf_ops, buf_read_elapsed / N_target * 1e6,
               dir_ops, direct_read_elapsed / N_target * 1e6,
               speedup);
    }

    printf("\n  Write buffer: flat hashmap retains ALL writes until explicit flush.\n");
    printf("  OS page cache: LRU eviction — subsequent writes push earlier pages out.\n");
    printf("  More pollution → worse pread latency; buffer stays O(1) constant.\n\n");

    free(emb);
    free(out);
    free(read_order);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Benchmark 7: Write buffer effectiveness under mixed workload
 *
 * Simulates realistic serving pattern:
 *   - 80% reads, 20% writes (hot key set)
 *   - Measures how write buffer absorbs write bursts and serves
 *     read-your-writes without going to disk
 * ═══════════════════════════════════════════════════════════════════════════════ */

static void bench_mixed_workload(void) {
    printf("── Mixed Workload (80%% read / 20%% write) ──\n\n");

    int dim = 128;
    int num_features = 100000;
    int num_ops = 500000;
    size_t entry_bytes = dim * sizeof(float);

    cleanup();

    GravelDBConfig config = {0};
    config.data_dir = BENCH_DIR;
    config.dims = &dim;
    config.num_dims = 1;
    config.buffer_size = 128 * 1024 * 1024;
    config.index_capacity = num_features * 2;

    GravelDB *db = NULL;
    graveldb_open(&db, &config);
    if (!db) return;

    float *emb = (float *)malloc(entry_bytes);
    float *out = (float *)malloc(entry_bytes);
    int out_dim;

    /* Pre-fill */
    for (int i = 1; i <= num_features; i++) {
        for (int j = 0; j < dim; j++) emb[j] = (float)(i * 100 + j) * 0.01f;
        graveldb_put(db, NULL, (uint64_t)i, dim, emb);
    }
    graveldb_flush(db);

    /* Warm read cache */
    for (int i = 1; i <= num_features; i++) {
        graveldb_get(db, NULL, (uint64_t)i, out, &out_dim);
    }

    /* Mixed workload */
    uint64_t rng = 0xBEEFCAFE5678ULL;
    uint64_t reads = 0, writes = 0;
    uint64_t read_wb_hits = 0; /* reads that got buffer-forwarded data */

    double t0 = now_sec();
    for (int i = 0; i < num_ops; i++) {
        uint64_t r = xorshift64(&rng);
        uint64_t fid = (r % num_features) + 1;

        if ((r >> 32) % 5 == 0) {
            /* Write (20%) — update a random feature */
            for (int j = 0; j < dim; j++) emb[j] = (float)(i * 7 + j) * 0.001f;
            graveldb_put(db, NULL, fid, dim, emb);
            writes++;
        } else {
            /* Read (80%) */
            graveldb_get(db, NULL, fid, out, &out_dim);
            reads++;
        }
    }
    double elapsed = now_sec() - t0;

    GravelDBStats stats;
    graveldb_stats(db, &stats);

    printf("  Operations: %d (%llu reads, %llu writes)\n",
           num_ops, (unsigned long long)reads, (unsigned long long)writes);
    printf("  Elapsed:    %.3f s\n", elapsed);
    printf("  Throughput: %.0f total ops/s (%.0f read ops/s, %.0f write ops/s)\n",
           num_ops / elapsed, reads / elapsed, writes / elapsed);
    printf("  Cache hit:  %.1f%%\n", stats.cache_hit_ratio * 100.0f);
    printf("  Buffer hits: %llu, Buffer misses: %llu\n",
           (unsigned long long)stats.buffer_hits,
           (unsigned long long)stats.buffer_misses);
    (void)read_wb_hits;

    free(emb);
    free(out);
    graveldb_close(db);
    printf("\n");
}

/* ═══════════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("GravelDB Write Buffer Effectiveness Benchmark\n");
    printf("==============================================\n\n");

    bench_correctness();
    bench_get_paths();
    bench_put_paths();
    bench_read_after_write();
    bench_scattered_raw();
    bench_throughput_comparison();
    bench_mixed_workload();

    cleanup();
    printf("Benchmark complete.\n");
    return 0;
}
