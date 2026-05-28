/*
 * GravelDB - Pool-based Slab Allocator Unit Test & Benchmark
 *
 * Tests the pool-based API: slab_pool_create / slab_pool_alloc / slab_pool_free
 * Also tests the convenience size-based API: slab_alloc / slab_free
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "slab_alloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>

/* ─────────────────── Timer ─────────────────── */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* ─────────────────── Correctness Tests ─────────────────── */

static void test_pool_basic(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* Create pool for 256B objects (dim=64 embedding) */
    SlabPool *pool = slab_pool_create(&alloc, 256, 16);
    assert(pool != NULL);
    assert(pool->obj_size == 256);

    /* Alloc / free */
    void *p1 = slab_pool_alloc(pool);
    assert(p1 != NULL);
    memset(p1, 0xAA, 256);

    void *p2 = slab_pool_alloc(pool);
    assert(p2 != NULL);
    assert(p2 != p1);
    memset(p2, 0xBB, 256);

    /* Verify data */
    for (int i = 0; i < 256; i++) assert(((uint8_t *)p1)[i] == 0xAA);
    for (int i = 0; i < 256; i++) assert(((uint8_t *)p2)[i] == 0xBB);

    slab_pool_free(pool, p1);
    slab_pool_free(pool, p2);

    /* Reuse: should get p2 back (LIFO) */
    void *p3 = slab_pool_alloc(pool);
    assert(p3 == p2);
    slab_pool_free(pool, p3);

    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
    printf("  test_pool_basic... PASS\n");
}

static void test_pool_bulk(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* Pool for 512B objects (dim=128) */
    SlabPool *pool = slab_pool_create(&alloc, 512, 16);
    assert(pool != NULL);

    void *ptrs[200];
    int got = slab_pool_alloc_bulk(pool, ptrs, 200);
    assert(got == 200);

    /* Verify all distinct and writable */
    for (int i = 0; i < 200; i++) {
        assert(ptrs[i] != NULL);
        memset(ptrs[i], (uint8_t)i, 512);
    }

    /* Verify no overlap */
    for (int i = 0; i < 200; i++) {
        for (int j = 0; j < 512; j++) {
            assert(((uint8_t *)ptrs[i])[j] == (uint8_t)i);
        }
    }

    slab_pool_free_bulk(pool, ptrs, 200);
    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
    printf("  test_pool_bulk... PASS\n");
}

static void test_pool_multiple_dims(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* Simulate multiple DimBins, each with its own pool */
    SlabPool *pool_d64  = slab_pool_create(&alloc, 64 * sizeof(float), 16);   /* 256B */
    SlabPool *pool_d128 = slab_pool_create(&alloc, 128 * sizeof(float), 16);  /* 512B */
    SlabPool *pool_d96  = slab_pool_create(&alloc, 96 * sizeof(float), 16);   /* 384B */

    assert(pool_d64 != NULL);
    assert(pool_d128 != NULL);
    assert(pool_d96 != NULL);

    /* Each pool has independent obj_size */
    assert(pool_d64->obj_size >= 256);
    assert(pool_d128->obj_size >= 512);
    assert(pool_d96->obj_size >= 384);

    /* Alloc from each */
    float *emb64 = (float *)slab_pool_alloc(pool_d64);
    float *emb128 = (float *)slab_pool_alloc(pool_d128);
    float *emb96 = (float *)slab_pool_alloc(pool_d96);

    assert(emb64 != NULL);
    assert(emb128 != NULL);
    assert(emb96 != NULL);

    /* Write embeddings */
    for (int i = 0; i < 64; i++) emb64[i] = (float)i;
    for (int i = 0; i < 128; i++) emb128[i] = (float)(i * 2);
    for (int i = 0; i < 96; i++) emb96[i] = (float)(i * 3);

    /* Verify */
    for (int i = 0; i < 64; i++) assert(emb64[i] == (float)i);
    for (int i = 0; i < 128; i++) assert(emb128[i] == (float)(i * 2));

    slab_pool_free(pool_d64, emb64);
    slab_pool_free(pool_d128, emb128);
    slab_pool_free(pool_d96, emb96);

    slab_pool_destroy(&alloc, pool_d64);
    slab_pool_destroy(&alloc, pool_d128);
    slab_pool_destroy(&alloc, pool_d96);
    slab_allocator_destroy(&alloc);
    printf("  test_pool_multiple_dims... PASS\n");
}

static void test_pool_destroy_bulk_free(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* Simulate overlay: create pool, alloc many, then destroy pool (bulk free) */
    SlabPool *overlay_pool = slab_pool_create(&alloc, 384, 16); /* dim=96 */
    assert(overlay_pool != NULL);

    /* Alloc 1000 overlay entries */
    void *ptrs[1000];
    for (int i = 0; i < 1000; i++) {
        ptrs[i] = slab_pool_alloc(overlay_pool);
        assert(ptrs[i] != NULL);
        memset(ptrs[i], 0xCD, 384);
    }

    /* Destroy pool → all 1000 objects freed in one shot (no per-object free) */
    slab_pool_destroy(&alloc, overlay_pool);

    /* Verify allocator is still functional */
    assert(alloc.num_pools == 0);
    slab_allocator_destroy(&alloc);
    printf("  test_pool_destroy_bulk_free... PASS\n");
}

static void test_convenience_api(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* Size-based convenience API (auto-creates pools) */
    void *p128 = slab_alloc(&alloc, 128);
    void *p256 = slab_alloc(&alloc, 256);
    void *p128_2 = slab_alloc(&alloc, 128);

    assert(p128 != NULL);
    assert(p256 != NULL);
    assert(p128_2 != NULL);

    memset(p128, 0xAA, 128);
    memset(p256, 0xBB, 256);
    memset(p128_2, 0xCC, 128);

    slab_free(&alloc, p128, 128);
    slab_free(&alloc, p256, 256);
    slab_free(&alloc, p128_2, 128);

    /* Should have created 2 pools (128B and 256B) */
    assert(alloc.num_pools == 2);

    slab_allocator_destroy(&alloc);
    printf("  test_convenience_api... PASS\n");
}

static void test_aligned_alloc(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    /* 4KB aligned for block buffers */
    void *blk = slab_alloc_aligned(&alloc, 4096, 4096);
    assert(blk != NULL);
    assert(((uintptr_t)blk & 0xFFF) == 0); /* 4KB aligned */
    memset(blk, 0xAB, 4096);

    slab_free_aligned(&alloc, blk, 4096);
    slab_allocator_destroy(&alloc);
    printf("  test_aligned_alloc... PASS\n");
}

static void test_stats(void) {
    SlabAllocator alloc;
    assert(slab_allocator_init(&alloc) == GRAVELDB_OK);

    SlabPool *pool = slab_pool_create(&alloc, 64, 16);
    assert(pool != NULL);

    for (int i = 0; i < 1000; i++) {
        void *p = slab_pool_alloc(pool);
        slab_pool_free(pool, p);
    }

    SlabAllocStats stats;
    slab_allocator_stats(&alloc, &stats);

    assert(stats.num_pools == 1);
    assert(stats.pools[0].total_allocated >= 1000);
    assert(stats.pools[0].total_freed >= 1000);
    assert(stats.pools[0].alloc_fast > 0);

    printf("  test_stats... PASS (alloc_fast=%llu, alloc_slow=%llu)\n",
           (unsigned long long)stats.pools[0].alloc_fast,
           (unsigned long long)stats.pools[0].alloc_slow);

    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
}

/* ─────────────────── Performance Benchmark ─────────────────── */

#define BENCH_ITERS   1000000
#define BENCH_SIZE    256       /* typical embedding: dim=64, 64*4=256B */

static void bench_malloc_free(void) {
    double t0 = now_sec();
    for (int i = 0; i < BENCH_ITERS; i++) {
        void *p = malloc(BENCH_SIZE);
        *(volatile char *)p = 0;
        free(p);
    }
    double dt = now_sec() - t0;
    printf("  malloc/free    (%d iters, %dB): %.3f ms  (%.0f ns/op)\n",
           BENCH_ITERS, BENCH_SIZE, dt * 1000.0, dt * 1e9 / BENCH_ITERS);
}

static void bench_pool_alloc_free(void) {
    SlabAllocator alloc;
    slab_allocator_init(&alloc);
    SlabPool *pool = slab_pool_create(&alloc, BENCH_SIZE, 16);

    double t0 = now_sec();
    for (int i = 0; i < BENCH_ITERS; i++) {
        void *p = slab_pool_alloc(pool);
        *(volatile char *)p = 0;
        slab_pool_free(pool, p);
    }
    double dt = now_sec() - t0;
    printf("  pool_alloc     (%d iters, %dB): %.3f ms  (%.0f ns/op)\n",
           BENCH_ITERS, BENCH_SIZE, dt * 1000.0, dt * 1e9 / BENCH_ITERS);

    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
}

static void bench_pool_bulk(void) {
    SlabAllocator alloc;
    slab_allocator_init(&alloc);
    SlabPool *pool = slab_pool_create(&alloc, BENCH_SIZE, 16);

    #define BULK_N 64
    void *ptrs[BULK_N];
    int iters = BENCH_ITERS / BULK_N;

    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        slab_pool_alloc_bulk(pool, ptrs, BULK_N);
        slab_pool_free_bulk(pool, ptrs, BULK_N);
    }
    double dt = now_sec() - t0;
    int total = iters * BULK_N;
    printf("  pool_bulk      (%d iters, %dB, batch=%d): %.3f ms  (%.0f ns/op)\n",
           total, BENCH_SIZE, BULK_N, dt * 1000.0, dt * 1e9 / total);

    slab_pool_destroy(&alloc, pool);
    slab_allocator_destroy(&alloc);
}

static void bench_overlay_sim(void) {
    /* Simulate overlay lifecycle: create pool → alloc 10K entries → destroy pool */
    SlabAllocator alloc;
    slab_allocator_init(&alloc);

    int num_cycles = 100;
    int entries_per_ckpt = 10000;

    double t0 = now_sec();
    for (int c = 0; c < num_cycles; c++) {
        SlabPool *pool = slab_pool_create(&alloc, 384, 16); /* dim=96 */
        void **ptrs = (void **)malloc(entries_per_ckpt * sizeof(void *));
        for (int i = 0; i < entries_per_ckpt; i++) {
            ptrs[i] = slab_pool_alloc(pool);
        }
        /* Destroy pool = bulk free all at once */
        slab_pool_destroy(&alloc, pool);
        free(ptrs);
    }
    double dt = now_sec() - t0;
    double per_ckpt = dt * 1000.0 / num_cycles;
    printf("  overlay_sim    (%d ckpts, %d entries/ckpt): %.3f ms total  (%.3f ms/ckpt)\n",
           num_cycles, entries_per_ckpt, dt * 1000.0, per_ckpt);

    slab_allocator_destroy(&alloc);
}

/* ─────────────────── Main ─────────────────── */

int main(void) {
    printf("\nPool-based Slab Allocator - Correctness Tests\n");
    printf("==============================================\n\n");
    test_pool_basic();
    test_pool_bulk();
    test_pool_multiple_dims();
    test_pool_destroy_bulk_free();
    test_convenience_api();
    test_aligned_alloc();
    test_stats();

    printf("\nPool-based Slab Allocator - Performance Benchmark\n");
    printf("==================================================\n\n");
    bench_malloc_free();
    bench_pool_alloc_free();
    bench_pool_bulk();
    bench_overlay_sim();

    printf("\n✓ All pool-based slab allocator tests passed!\n\n");
    return 0;
}
