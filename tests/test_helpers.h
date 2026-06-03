/*
 * Test helper: single-key convenience wrappers over batch API.
 * These are NOT part of the public API -- only used by tests/benchmarks
 * to avoid rewriting every test call site.
 */
#ifndef GRAVELDB_TEST_HELPERS_H_
#define GRAVELDB_TEST_HELPERS_H_

#include "graveldb.h"

static inline graveldb_status_t graveldb_put(GravelDB *db, GravelDBCtx *ctx,
                                             uint64_t feat_id, int dim, const float *embedding) {
    return graveldb_batch_put(db, ctx, &feat_id, &dim, &embedding, 1);
}

static inline graveldb_status_t graveldb_get(GravelDB *db, GravelDBCtx *ctx,
                                             uint64_t feat_id, float *out_embedding, int *out_dim) {
    float *out_ptrs[1] = { out_embedding };
    int out_dims[1] = { 0 };
    graveldb_status_t rc = graveldb_batch_get(db, ctx, &feat_id, 1, out_ptrs, out_dims);
    if (rc != GRAVELDB_OK) return rc;
    if (out_dims[0] == 0) return GRAVELDB_ERR_NOT_FOUND;
    if (out_dim) *out_dim = out_dims[0];
    return GRAVELDB_OK;
}

static inline graveldb_status_t graveldb_delete(GravelDB *db, GravelDBCtx *ctx, uint64_t feat_id) {
    /* Check existence first: batch_delete silently skips missing keys,
     * but single-key semantic expects NOT_FOUND for absent keys. */
    float *out_ptrs[1] = { NULL };
    int out_dims[1] = { 0 };
    graveldb_status_t rc = graveldb_batch_get(db, ctx, &feat_id, 1, out_ptrs, out_dims);
    if (rc != GRAVELDB_OK) return rc;
    if (out_dims[0] == 0) return GRAVELDB_ERR_NOT_FOUND;
    return graveldb_batch_delete(db, ctx, &feat_id, 1);
}

#endif /* GRAVELDB_TEST_HELPERS_H_ */
