/*
 * GravelDB - DimRegistry Unit Tests
 */

#undef NDEBUG  /* Ensure assert() is always active in tests */

#include "dim_registry.h"
#include "dimbin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static void test_init_destroy(void) {
    printf("  test_init_destroy...");
    DimRegistry reg;
    dim_registry_init(&reg);
    assert(reg.mode == DIM_REG_LINEAR);
    assert(reg.count == 0);
    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_single_registration(void) {
    printf("  test_single_registration...");
    DimRegistry reg;
    dim_registry_init(&reg);

    struct DimBin fake_bin;
    memset(&fake_bin, 0, sizeof(fake_bin));
    fake_bin.dim = 64;

    int idx = dim_registry_put(&reg, 64, &fake_bin);
    assert(idx >= 0);
    assert(reg.count == 1);
    assert(reg.mode == DIM_REG_LINEAR);

    int found = dim_registry_find(&reg, 64);
    assert(found == idx);

    struct DimBin *bin = dim_registry_get_bin(&reg, (uint16_t)idx);
    assert(bin == &fake_bin);

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_not_found(void) {
    printf("  test_not_found...");
    DimRegistry reg;
    dim_registry_init(&reg);

    assert(dim_registry_find(&reg, 128) == -1);

    struct DimBin fake;
    memset(&fake, 0, sizeof(fake));
    dim_registry_put(&reg, 64, &fake);
    assert(dim_registry_find(&reg, 128) == -1);
    assert(dim_registry_find(&reg, 32) == -1);

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_duplicate_put(void) {
    printf("  test_duplicate_put...");
    DimRegistry reg;
    dim_registry_init(&reg);

    struct DimBin bin1, bin2;
    memset(&bin1, 0, sizeof(bin1));
    memset(&bin2, 0, sizeof(bin2));

    int idx1 = dim_registry_put(&reg, 64, &bin1);
    int idx2 = dim_registry_put(&reg, 64, &bin2);
    assert(idx1 == idx2);
    assert(reg.count == 1);

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_linear_mode_max(void) {
    printf("  test_linear_mode_max...");
    DimRegistry reg;
    dim_registry_init(&reg);

    struct DimBin bins[DIM_REG_THRESHOLD_LINEAR];
    memset(bins, 0, sizeof(bins));

    for (int i = 0; i < DIM_REG_THRESHOLD_LINEAR; i++) {
        bins[i].dim = (i + 1) * 32;
        int idx = dim_registry_put(&reg, bins[i].dim, &bins[i]);
        assert(idx >= 0);
    }
    assert(reg.count == DIM_REG_THRESHOLD_LINEAR);
    assert(reg.mode == DIM_REG_LINEAR);

    for (int i = 0; i < DIM_REG_THRESHOLD_LINEAR; i++) {
        int found = dim_registry_find(&reg, (i + 1) * 32);
        assert(found >= 0);
        assert(dim_registry_get_bin(&reg, (uint16_t)found) == &bins[i]);
    }

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_upgrade_to_sorted(void) {
    printf("  test_upgrade_to_sorted...");
    DimRegistry reg;
    dim_registry_init(&reg);

    struct DimBin bins[DIM_REG_THRESHOLD_LINEAR + 1];
    memset(bins, 0, sizeof(bins));

    for (int i = 0; i <= DIM_REG_THRESHOLD_LINEAR; i++) {
        bins[i].dim = (i + 1) * 16;
        dim_registry_put(&reg, bins[i].dim, &bins[i]);
    }
    assert(reg.count == DIM_REG_THRESHOLD_LINEAR + 1);
    assert(reg.mode == DIM_REG_SORTED);

    for (int i = 0; i <= DIM_REG_THRESHOLD_LINEAR; i++) {
        int found = dim_registry_find(&reg, (i + 1) * 16);
        assert(found >= 0);
        assert(dim_registry_get_bin(&reg, (uint16_t)found) == &bins[i]);
    }

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_upgrade_to_hash(void) {
    printf("  test_upgrade_to_hash...");
    DimRegistry reg;
    dim_registry_init(&reg);

    struct DimBin *bins = (struct DimBin *)calloc(DIM_REG_THRESHOLD_SORTED + 1, sizeof(struct DimBin));

    for (int i = 0; i <= DIM_REG_THRESHOLD_SORTED; i++) {
        bins[i].dim = i + 1;
        dim_registry_put(&reg, i + 1, &bins[i]);
    }
    assert(reg.count == DIM_REG_THRESHOLD_SORTED + 1);
    assert(reg.mode == DIM_REG_HASH);

    for (int i = 0; i <= DIM_REG_THRESHOLD_SORTED; i++) {
        int found = dim_registry_find(&reg, i + 1);
        assert(found >= 0);
        assert(dim_registry_get_bin(&reg, (uint16_t)found) == &bins[i]);
    }

    assert(dim_registry_find(&reg, 99999) == -1);

    free(bins);
    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_lookup_performance_after_upgrade(void) {
    printf("  test_lookup_performance_after_upgrade...");
    DimRegistry reg;
    dim_registry_init(&reg);

    int n = 100;
    struct DimBin *bins = (struct DimBin *)calloc(n, sizeof(struct DimBin));

    for (int i = 0; i < n; i++) {
        bins[i].dim = (i + 1) * 8;
        dim_registry_put(&reg, (i + 1) * 8, &bins[i]);
    }
    assert(reg.mode == DIM_REG_HASH);

    for (int round = 0; round < 1000; round++) {
        for (int i = 0; i < n; i++) {
            int found = dim_registry_find(&reg, (i + 1) * 8);
            assert(found >= 0);
        }
    }

    free(bins);
    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

static void test_sparse_dims(void) {
    printf("  test_sparse_dims...");
    DimRegistry reg;
    dim_registry_init(&reg);

    int sparse_dims[] = {3, 768, 1024, 2048, 4096, 7, 13, 256, 512};
    int n = sizeof(sparse_dims) / sizeof(sparse_dims[0]);
    struct DimBin bins[9];
    memset(bins, 0, sizeof(bins));

    for (int i = 0; i < n; i++) {
        bins[i].dim = sparse_dims[i];
        dim_registry_put(&reg, sparse_dims[i], &bins[i]);
    }

    for (int i = 0; i < n; i++) {
        int found = dim_registry_find(&reg, sparse_dims[i]);
        assert(found >= 0);
        assert(dim_registry_get_bin(&reg, (uint16_t)found) == &bins[i]);
    }

    assert(dim_registry_find(&reg, 999) == -1);
    assert(dim_registry_find(&reg, 0) == -1);

    dim_registry_destroy(&reg);
    printf(" PASS\n");
}

int main(void) {
    printf("GravelDB DimRegistry Tests\n");
    printf("==========================\n\n");

    test_init_destroy();
    test_single_registration();
    test_not_found();
    test_duplicate_put();
    test_linear_mode_max();
    test_upgrade_to_sorted();
    test_upgrade_to_hash();
    test_lookup_performance_after_upgrade();
    test_sparse_dims();

    printf("\n All dim_registry tests PASSED!\n\n");
    return 0;
}
