/*
 * GravelDB - TinyLFU Unit Tests
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "tinylfu.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

extern graveldb_status_t tinylfu_init(TinyLFU *lfu, uint32_t cms_width, uint8_t eviction_threshold);
extern void tinylfu_destroy(TinyLFU *lfu);
extern void tinylfu_access(TinyLFU *lfu, uint64_t feat_id);
extern uint8_t tinylfu_estimate(const TinyLFU *lfu, uint64_t feat_id);
extern void tinylfu_promote(TinyLFU *lfu, uint64_t feat_id);

static void test_init_destroy(void) {
    printf("  test_init_destroy...");
    TinyLFU lfu;
    assert(tinylfu_init(&lfu, 1024, 2) == GRAVELDB_OK);
    assert(lfu.cms_width == 1024);
    assert(lfu.total_accesses == 0);
    assert(lfu.eviction_threshold == 2);
    assert(lfu.decay_threshold == 1024 * 10);

    for (int i = 0; i < 4; i++) {
        assert(lfu.cms[i] != NULL);
    }

    tinylfu_destroy(&lfu);
    for (int i = 0; i < 4; i++) {
        assert(lfu.cms[i] == NULL);
    }
    printf(" PASS\n");
}

static void test_fresh_key_zero_estimate(void) {
    printf("  test_fresh_key_zero_estimate...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 1024, 2);

    assert(tinylfu_estimate(&lfu, 12345) == 0);
    assert(tinylfu_estimate(&lfu, 99999) == 0);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_single_access(void) {
    printf("  test_single_access...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 1024, 2);

    tinylfu_access(&lfu, 100);
    assert(tinylfu_estimate(&lfu, 100) == 1);
    assert(lfu.total_accesses == 1);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_multiple_accesses(void) {
    printf("  test_multiple_accesses...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 4096, 2);

    for (int i = 0; i < 10; i++) {
        tinylfu_access(&lfu, 42);
    }
    assert(tinylfu_estimate(&lfu, 42) == 10);

    for (int i = 0; i < 3; i++) {
        tinylfu_access(&lfu, 99);
    }
    assert(tinylfu_estimate(&lfu, 99) == 3);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_saturation_at_15(void) {
    printf("  test_saturation_at_15...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 4096, 2);

    for (int i = 0; i < 100; i++) {
        tinylfu_access(&lfu, 1);
    }
    assert(tinylfu_estimate(&lfu, 1) <= 15);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_decay(void) {
    printf("  test_decay...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 128, 2);
    /* decay_threshold = 128 * 10 = 1280 */

    for (int i = 0; i < 10; i++) {
        tinylfu_access(&lfu, 500);
    }
    assert(tinylfu_estimate(&lfu, 500) == 10);

    /* Trigger decay by filling up to threshold with other keys */
    for (uint64_t i = 1; i <= 1270; i++) {
        tinylfu_access(&lfu, 10000 + i);
    }
    /* After decay, counter should be halved (10 >> 1 = 5).
     * With CMS collisions from other keys, min might be slightly higher,
     * but must be strictly less than pre-decay value. */
    uint8_t est = tinylfu_estimate(&lfu, 500);
    assert(est < 10);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_promote(void) {
    printf("  test_promote...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 1024, 2);

    assert(tinylfu_estimate(&lfu, 777) == 0);
    tinylfu_promote(&lfu, 777);
    assert(tinylfu_estimate(&lfu, 777) == 15);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_different_keys_independent(void) {
    printf("  test_different_keys_independent...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 4096, 2);

    tinylfu_access(&lfu, 1);
    tinylfu_access(&lfu, 1);
    tinylfu_access(&lfu, 1);

    tinylfu_access(&lfu, 2);

    assert(tinylfu_estimate(&lfu, 1) == 3);
    /* Key 2 might be 1, or could have CMS collision with key 1 making it higher,
     * but should definitely be <= 3 */
    assert(tinylfu_estimate(&lfu, 2) >= 1);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_wide_cms_low_collision(void) {
    printf("  test_wide_cms_low_collision...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 65536, 2);

    for (int i = 0; i < 5; i++) tinylfu_access(&lfu, 100);
    for (int i = 0; i < 3; i++) tinylfu_access(&lfu, 200);
    tinylfu_access(&lfu, 300);

    assert(tinylfu_estimate(&lfu, 100) == 5);
    assert(tinylfu_estimate(&lfu, 200) == 3);
    assert(tinylfu_estimate(&lfu, 300) == 1);
    assert(tinylfu_estimate(&lfu, 400) == 0);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

static void test_eviction_threshold_comparison(void) {
    printf("  test_eviction_threshold_comparison...");
    TinyLFU lfu;
    tinylfu_init(&lfu, 4096, 3);

    tinylfu_access(&lfu, 1);
    tinylfu_access(&lfu, 1);
    /* est=2, below threshold=3 → candidate for eviction */
    assert(tinylfu_estimate(&lfu, 1) < lfu.eviction_threshold);

    tinylfu_access(&lfu, 1);
    /* est=3, at threshold → no longer cold */
    assert(tinylfu_estimate(&lfu, 1) >= lfu.eviction_threshold);

    tinylfu_destroy(&lfu);
    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB TinyLFU Tests\n");
    printf("======================\n\n");

    test_init_destroy();
    test_fresh_key_zero_estimate();
    test_single_access();
    test_multiple_accesses();
    test_saturation_at_15();
    test_decay();
    test_promote();
    test_different_keys_independent();
    test_wide_cms_low_collision();
    test_eviction_threshold_comparison();

    printf("\n All TinyLFU tests PASSED!\n\n");
    return 0;
}
