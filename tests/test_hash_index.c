/*
 * GravelDB - Hash Index Unit Tests
 *
 * Tests the open-addressing hash table (linear probing).
 * Verifies correctness of put/get/remove/iterate/resize.
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "graveldb_impl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static void test_init_destroy(void) {
    printf("  test_init_destroy...");
    HashIndex idx;
    assert(hash_index_init(&idx, 16) == GRAVELDB_OK);
    assert(idx.count == 0);
    assert(idx.capacity >= 16);
    assert(idx.slots != NULL);
    hash_index_destroy(&idx);
    assert(idx.slots == NULL);
    printf(" PASS\n");
}

static void test_put_get_basic(void) {
    printf("  test_put_get_basic...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    assert(hash_index_put(&idx, 100, 0, 42) == GRAVELDB_OK);
    assert(idx.count == 1);

    uint16_t dim_idx;
    uint32_t entry_idx;
    assert(hash_index_get(&idx, 100, &dim_idx, &entry_idx) == GRAVELDB_OK);
    assert(dim_idx == 0);
    assert(entry_idx == 42);

    assert(hash_index_get(&idx, 999, &dim_idx, &entry_idx) == GRAVELDB_ERR_NOT_FOUND);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_zero_key_rejected(void) {
    printf("  test_zero_key_rejected...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    assert(hash_index_put(&idx, 0, 0, 0) == GRAVELDB_ERR_INVALID);
    assert(idx.count == 0);

    uint16_t d; uint32_t s;
    assert(hash_index_get(&idx, 0, &d, &s) == GRAVELDB_ERR_NOT_FOUND);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_update_existing(void) {
    printf("  test_update_existing...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    hash_index_put(&idx, 100, 0, 10);
    hash_index_put(&idx, 100, 1, 20);
    assert(idx.count == 1);

    uint16_t d; uint32_t s;
    hash_index_get(&idx, 100, &d, &s);
    assert(d == 1);
    assert(s == 20);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_remove_basic(void) {
    printf("  test_remove_basic...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    hash_index_put(&idx, 100, 0, 10);
    hash_index_put(&idx, 200, 1, 20);
    hash_index_put(&idx, 300, 2, 30);
    assert(idx.count == 3);

    assert(hash_index_remove(&idx, 200) == GRAVELDB_OK);
    assert(idx.count == 2);

    uint16_t d; uint32_t s;
    assert(hash_index_get(&idx, 200, &d, &s) == GRAVELDB_ERR_NOT_FOUND);
    assert(hash_index_get(&idx, 100, &d, &s) == GRAVELDB_OK);
    assert(hash_index_get(&idx, 300, &d, &s) == GRAVELDB_OK);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_remove_nonexistent(void) {
    printf("  test_remove_nonexistent...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    hash_index_put(&idx, 100, 0, 10);
    assert(hash_index_remove(&idx, 999) == GRAVELDB_ERR_NOT_FOUND);
    assert(idx.count == 1);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_remove_and_reinsert(void) {
    printf("  test_remove_and_reinsert...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    hash_index_put(&idx, 100, 0, 10);
    hash_index_remove(&idx, 100);
    assert(idx.count == 0);

    uint16_t d; uint32_t s;
    assert(hash_index_get(&idx, 100, &d, &s) == GRAVELDB_ERR_NOT_FOUND);

    hash_index_put(&idx, 100, 2, 50);
    assert(idx.count == 1);
    hash_index_get(&idx, 100, &d, &s);
    assert(d == 2 && s == 50);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_many_inserts_and_resize(void) {
    printf("  test_many_inserts_and_resize...");
    HashIndex idx;
    hash_index_init(&idx, 16);

    /* Insert 10K keys - enough to trigger multiple resizes */
    for (uint64_t i = 1; i <= 10000; i++) {
        assert(hash_index_put(&idx, i, (uint16_t)(i % 8), (uint32_t)i) == GRAVELDB_OK);
    }
    assert(idx.count == 10000);

    /* Verify all keys retrievable */
    for (uint64_t i = 1; i <= 10000; i++) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, i, &d, &s) == GRAVELDB_OK);
        assert(d == (uint16_t)(i % 8));
        assert(s == (uint32_t)i);
    }

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_remove_chain_integrity(void) {
    printf("  test_remove_chain_integrity...");
    HashIndex idx;
    hash_index_init(&idx, 32);

    for (uint64_t i = 1; i <= 20; i++) {
        hash_index_put(&idx, i, 0, (uint32_t)i);
    }

    /* Remove odd keys */
    for (uint64_t i = 1; i <= 20; i += 2) {
        hash_index_remove(&idx, i);
    }
    assert(idx.count == 10);

    /* Even keys still present */
    for (uint64_t i = 2; i <= 20; i += 2) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, i, &d, &s) == GRAVELDB_OK);
        assert(s == (uint32_t)i);
    }

    /* Odd keys gone */
    for (uint64_t i = 1; i <= 20; i += 2) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, i, &d, &s) == GRAVELDB_ERR_NOT_FOUND);
    }

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_large_keys(void) {
    printf("  test_large_keys...");
    HashIndex idx;
    hash_index_init(&idx, 128);

    uint64_t keys[] = {
        UINT64_MAX, UINT64_MAX - 1, 0x8000000000000000ULL,
        0xDEADBEEFCAFEBABEULL, 0x123456789ABCDEF0ULL,
        1, 2, 3
    };
    int n = sizeof(keys) / sizeof(keys[0]);

    for (int i = 0; i < n; i++) {
        hash_index_put(&idx, keys[i], (uint16_t)i, (uint32_t)(i * 100));
    }

    for (int i = 0; i < n; i++) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, keys[i], &d, &s) == GRAVELDB_OK);
        assert(d == (uint16_t)i);
        assert(s == (uint32_t)(i * 100));
    }

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_sequential_remove_all(void) {
    printf("  test_sequential_remove_all...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    for (uint64_t i = 1; i <= 50; i++) {
        hash_index_put(&idx, i, 0, (uint32_t)i);
    }

    for (uint64_t i = 1; i <= 50; i++) {
        assert(hash_index_remove(&idx, i) == GRAVELDB_OK);
    }
    assert(idx.count == 0);

    for (uint64_t i = 1; i <= 50; i++) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, i, &d, &s) == GRAVELDB_ERR_NOT_FOUND);
    }

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static void test_iteration(void) {
    printf("  test_iteration...");
    HashIndex idx;
    hash_index_init(&idx, 64);

    /* Insert 10 keys */
    for (uint64_t i = 1; i <= 10; i++) {
        hash_index_put(&idx, i * 10, (uint16_t)i, (uint32_t)(i * 100));
    }

    /* Iterate and count */
    HashIter it;
    hash_iter_init(&idx, &it);

    int count = 0;
    uint64_t feat_id;
    uint16_t dim_idx;
    uint32_t entry_idx;
    uint64_t seen_mask = 0; /* track which keys we've seen (keys are 10,20,...100) */
    while (hash_iter_next(&it, &feat_id, &dim_idx, &entry_idx)) {
        assert(feat_id >= 10 && feat_id <= 100);
        assert(feat_id % 10 == 0);
        int idx_pos = (int)(feat_id / 10);
        assert(dim_idx == (uint16_t)idx_pos);
        assert(entry_idx == (uint32_t)(idx_pos * 100));
        seen_mask |= (1ULL << idx_pos);
        count++;
    }
    assert(count == 10);
    /* All 10 keys seen (bits 1-10) */
    assert(seen_mask == 0x7FE);

    hash_index_destroy(&idx);
    printf(" PASS\n");
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static void test_random_insert_large(void) {
    printf("  test_random_insert_large (50K)...");
    HashIndex idx;
    hash_index_init(&idx, 1024);

    /* Use pseudo-random keys */
    uint64_t seed = 0xDEADBEEF12345678ULL;
    uint64_t n = 50000;

    uint64_t *keys = (uint64_t *)malloc(n * sizeof(uint64_t));
    for (uint64_t i = 0; i < n; i++) {
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        keys[i] = seed * 0x2545F4914F6CDD1DULL;
        if (keys[i] == 0) keys[i] = 1; /* avoid sentinel */
    }

    /* Remove duplicates (rare but possible) */
    qsort(keys, n, sizeof(uint64_t), cmp_u64);
    uint64_t unique = 1;
    for (uint64_t i = 1; i < n; i++) {
        if (keys[i] != keys[i - 1]) {
            keys[unique++] = keys[i];
        }
    }
    n = unique;

    /* Shuffle back for random insertion order */
    seed = 42;
    for (uint64_t i = n - 1; i > 0; i--) {
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        uint64_t j = (seed * 0x2545F4914F6CDD1DULL) % (i + 1);
        uint64_t tmp = keys[i];
        keys[i] = keys[j];
        keys[j] = tmp;
    }

    /* Insert all */
    for (uint64_t i = 0; i < n; i++) {
        assert(hash_index_put(&idx, keys[i], (uint16_t)(i % 16), (uint32_t)i) == GRAVELDB_OK);
    }
    assert(idx.count == (uint32_t)n);

    /* Verify all */
    for (uint64_t i = 0; i < n; i++) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, keys[i], &d, &s) == GRAVELDB_OK);
    }

    /* Remove half */
    for (uint64_t i = 0; i < n / 2; i++) {
        assert(hash_index_remove(&idx, keys[i]) == GRAVELDB_OK);
    }
    assert(idx.count == (uint32_t)(n - n / 2));

    /* Verify remaining */
    for (uint64_t i = n / 2; i < n; i++) {
        uint16_t d; uint32_t s;
        assert(hash_index_get(&idx, keys[i], &d, &s) == GRAVELDB_OK);
    }

    free(keys);
    hash_index_destroy(&idx);
    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB Hash Index Tests\n");
    printf("==========================\n\n");

    test_init_destroy();
    test_put_get_basic();
    test_zero_key_rejected();
    test_update_existing();
    test_remove_basic();
    test_remove_nonexistent();
    test_remove_and_reinsert();
    test_many_inserts_and_resize();
    test_remove_chain_integrity();
    test_large_keys();
    test_sequential_remove_all();
    test_iteration();
    test_random_insert_large();

    printf("\n All hash index tests PASSED!\n\n");
    return 0;
}
