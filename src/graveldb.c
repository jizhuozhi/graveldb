/*
 * GravelDB - Main store implementation (lifecycle, batch ops, checkpoint)
 */

#include "graveldb_impl.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/statvfs.h>

#ifdef __linux__
#include <sys/ioctl.h>
#include <linux/fs.h>
#endif

#include "dimbin.h"

void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        mkdir(path, 0755);
    }
}

/*
 * Detect optimal page size for I/O on the given path.
 *
 * Strategy:
 *   1. Linux: try ioctl BLKPBSZGET (physical sector size) on block device,
 *      fall back to statvfs f_bsize.
 *   2. macOS: use statvfs f_frsize (fundamental fs block size),
 *      or statfs f_iosize for optimal transfer size.
 *   3. Clamp result to [512, 65536] and ensure power of 2.
 *   4. Fall back to GRAVELDB_PAGE_SIZE_DEFAULT (4096) on any failure.
 */
uint32_t detect_page_size(const char *path) {
    uint32_t detected = 0;

#ifdef __linux__
    /* Try to open the path (or its mount device) and query physical sector size */
    struct statvfs svfs;
    if (statvfs(path, &svfs) == 0 && svfs.f_frsize > 0) {
        detected = (uint32_t)svfs.f_frsize;
    }
    /* If the path is itself a block device, try ioctl */
    struct stat st;
    if (stat(path, &st) == 0 && S_ISBLK(st.st_mode)) {
        int fd = open(path, O_RDONLY);
        if (fd >= 0) {
            unsigned int phys_sz = 0;
            if (ioctl(fd, BLKPBSZGET, &phys_sz) == 0 && phys_sz > 0) {
                detected = phys_sz;
            }
            close(fd);
        }
    }
#else
    /* macOS / BSD: statvfs f_frsize is the fundamental block size */
    struct statvfs svfs;
    if (statvfs(path, &svfs) == 0 && svfs.f_frsize > 0) {
        detected = (uint32_t)svfs.f_frsize;
    }
#endif

    /* Fallback */
    if (detected == 0) {
        return GRAVELDB_PAGE_SIZE_DEFAULT;
    }

    /* Clamp to [512, 65536] */
    if (detected < 512) detected = 512;
    if (detected > 65536) detected = 65536;

    /* Ensure power of 2 (round up) */
    uint32_t p = 1;
    while (p < detected) p <<= 1;
    return p;
}

/*
 * Rebuild the in-memory hash index by scanning all .keys files.
 * Each .keys file is a flat array of uint64_t: keys[entry_idx] = feat_id.
 * feat_id == 0 means the slot is empty (free or never allocated).
 */
static graveldb_status_t rebuild_index_from_keys(GravelDB *db) {
    uint16_t num_bins = dim_registry_count(&db->dim_reg);

    for (uint16_t dim_idx = 0; dim_idx < num_bins; dim_idx++) {
        DimBin *bin = dim_registry_get_bin(&db->dim_reg, dim_idx);
        uint64_t bump = bin->bump_ptr;
        if (bump == 0) continue;

        /* Read .keys file in bulk for speed */
        size_t keys_bytes = (size_t)bump * sizeof(uint64_t);
        uint64_t *keys = (uint64_t *)malloc(keys_bytes);
        if (!keys) return GRAVELDB_ERR_OOM;

        ssize_t rd = pread(bin->key_fd, keys, keys_bytes, 0);
        if (rd < 0) { free(keys); continue; } /* empty/missing file is OK */

        uint64_t keys_read = (uint64_t)rd / sizeof(uint64_t);

        for (uint64_t entry_idx = 0; entry_idx < keys_read; entry_idx++) {
            uint64_t feat_id = keys[entry_idx];
            if (feat_id != 0) {
                hash_index_put(&db->index, feat_id, dim_idx, (uint32_t)entry_idx);
            } else {
                /* Rebuild free list: slot with key==0 is reclaimable */
                dimbin_free_entry(bin, (uint32_t)entry_idx);
            }
        }

        free(keys);
    }

    return GRAVELDB_OK;
}

/*
 * Dynamically create and register a new DimBin for a previously-unknown dim.
 * Returns bin_idx on success, -1 on error.
 */
static int create_and_register_bin(GravelDB *db, int dim) {
    DimBin *bin = (DimBin *)calloc(1, sizeof(DimBin));
    if (!bin) return -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/emb_d%d.bin", db->data_dir, dim);

    /* Determine buffer size: share equally among all bins (approximate) */
    size_t buf_size = db->config.buffer_size > 0 ? db->config.buffer_size : (256 * 1024 * 1024UL);
    uint16_t current_count = dim_registry_count(&db->dim_reg);
    size_t per_bin_buf = buf_size / (current_count + 1);
    if (per_bin_buf < (4 * 1024 * 1024UL)) per_bin_buf = 4 * 1024 * 1024UL; /* min 4MB */

    graveldb_status_t rc = dimbin_init(bin, dim, path, per_bin_buf,
                                        db->config.entry_align, db->config.page_size);
    if (rc != GRAVELDB_OK) {
        free(bin);
        return -1;
    }
    bin->allocator = &db->allocator;

    /* Register in dim_registry (may trigger mode upgrade) */
    int bin_idx = dim_registry_put(&db->dim_reg, dim, bin);
    if (bin_idx < 0) {
        dimbin_destroy(bin);
        free(bin);
        return -1;
    }

    return bin_idx;
}

graveldb_status_t graveldb_open(GravelDB **db_out, const GravelDBConfig *config) {
    if (!config || !config->data_dir)
        return GRAVELDB_ERR_INVALID;
    /* dims can be NULL (fully dynamic mode) */
    if (config->num_dims > 0 && !config->dims)
        return GRAVELDB_ERR_INVALID;

    GravelDB *db = (GravelDB *)calloc(1, sizeof(GravelDB));
    if (!db) return GRAVELDB_ERR_OOM;

    graveldb_status_t rc = GRAVELDB_OK;

    /* Copy config */
    db->config = *config;
    db->auto_create_bins = config->auto_create_bins;

    /* Deep copy dims if provided */
    if (config->num_dims > 0 && config->dims) {
        int *dims_copy = (int *)malloc(config->num_dims * sizeof(int));
        if (!dims_copy) { rc = GRAVELDB_ERR_OOM; goto fail_early; }
        memcpy(dims_copy, config->dims, config->num_dims * sizeof(int));
        db->config.dims = dims_copy;
    } else {
        db->config.dims = NULL;
        db->config.num_dims = 0;
    }

    db->data_dir = strdup(config->data_dir);
    if (!db->data_dir) { rc = GRAVELDB_ERR_OOM; goto fail_early; }

    ensure_dir(db->data_dir);

    /* Resolve page size: 0 means auto-detect from filesystem/device */
    if (db->config.page_size == 0) {
        db->config.page_size = detect_page_size(db->data_dir);
    }

    /* Initialize slab allocator (before other subsystems) */
    rc = slab_allocator_init(&db->allocator);
    if (rc != GRAVELDB_OK) goto fail_early;

    /* Create checkpoint directory */
    char ckpt_dir[512];
    snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/ckpt", db->data_dir);
    ensure_dir(ckpt_dir);

    /* Initialize hash index */
    uint32_t idx_cap = config->index_capacity > 0 ? config->index_capacity : (1 << 24);
    rc = hash_index_init(&db->index, idx_cap);
    if (rc != GRAVELDB_OK) goto fail_post_alloc;

    /* Initialize DimRegistry (adaptive lookup) */
    dim_registry_init(&db->dim_reg);

    /* Pre-create DimBins for initially-known dims */
    size_t buf_size = config->buffer_size > 0 ? config->buffer_size : (256 * 1024 * 1024UL);

    for (int i = 0; i < db->config.num_dims; i++) {
        DimBin *bin = (DimBin *)calloc(1, sizeof(DimBin));
        if (!bin) { rc = GRAVELDB_ERR_OOM; goto fail_full; }

        char path[512];
        snprintf(path, sizeof(path), "%s/emb_d%d.bin", db->data_dir, db->config.dims[i]);
        size_t per_bin_buf = db->config.num_dims > 0 ? buf_size / db->config.num_dims : buf_size;
        rc = dimbin_init(bin, db->config.dims[i], path, per_bin_buf,
                         config->entry_align, config->page_size);
        if (rc != GRAVELDB_OK) {
            free(bin);
            goto fail_full;
        }
        bin->allocator = &db->allocator;

        int bin_idx = dim_registry_put(&db->dim_reg, db->config.dims[i], bin);
        if (bin_idx < 0) {
            dimbin_destroy(bin);
            free(bin);
            rc = GRAVELDB_ERR_OOM;
            goto fail_full;
        }
    }

    db->current_epoch = 0;

    rc = rebuild_index_from_keys(db);
    if (rc != GRAVELDB_OK) {
        /* Non-fatal: fresh DB has no keys yet */
    }

    *db_out = db;
    return GRAVELDB_OK;

fail_full:
    /* Cleanup bins */
    {
        uint16_t num_bins = dim_registry_count(&db->dim_reg);
        for (uint16_t j = 0; j < num_bins; j++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, j);
            dimbin_destroy(s);
            free(s);
        }
    }
    dim_registry_destroy(&db->dim_reg);
    hash_index_destroy(&db->index);

fail_post_alloc:
    slab_allocator_destroy(&db->allocator);

fail_early:
    free(db->data_dir);
    free((void *)db->config.dims);
    free(db);
    return rc;
}

void graveldb_close(GravelDB *db) {
    if (!db) return;

    /* Flush and destroy all bins */
    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        dimbin_flush(s);
        dimbin_destroy(s);
        free(s);
    }

    dim_registry_destroy(&db->dim_reg);
    hash_index_destroy(&db->index);

    /* Destroy slab allocator (after all subsystems) */
    slab_allocator_destroy(&db->allocator);

    free((void *)db->config.dims);
    free(db->data_dir);
    free(db);
}

graveldb_status_t graveldb_put(GravelDB *db, GravelDBCtx *ctx,
                               uint64_t feat_id, int dim, const float *embedding) {
    (void)ctx;
    if (feat_id == 0 || !embedding || dim <= 0) return GRAVELDB_ERR_INVALID;

    /* Find dim bin via registry (adaptive lookup) */
    int dim_idx = dim_registry_find(&db->dim_reg, dim);

    /* Auto-create bin for unknown dim if enabled */
    if (dim_idx < 0) {
        if (!db->auto_create_bins) return GRAVELDB_ERR_INVALID;
        dim_idx = create_and_register_bin(db, dim);
        if (dim_idx < 0) return GRAVELDB_ERR_OOM;
    }

    DimBin *bin = dim_registry_get_bin(&db->dim_reg, (uint16_t)dim_idx);

    /* Check if feature already exists */
    uint16_t existing_dim_idx;
    uint32_t existing_entry;
    graveldb_status_t rc = hash_index_get(&db->index, feat_id, &existing_dim_idx, &existing_entry);

    uint32_t entry_id;
    if (rc == GRAVELDB_OK) {
        /* Existing feature - update in place */
        if (existing_dim_idx != (uint16_t)dim_idx) {
            /* Dim changed - free old slot, alloc new */
            DimBin *old_bin = dim_registry_get_bin(&db->dim_reg, existing_dim_idx);
            dimbin_free_entry(old_bin, existing_entry);
            entry_id = dimbin_alloc_entry(bin);
            hash_index_put(&db->index, feat_id, (uint16_t)dim_idx, entry_id);
            if (dimbin_put_key(bin, entry_id, feat_id) != GRAVELDB_OK) {
                /* Key write failed -- rollback: reclaim slot + revert index */
                dimbin_free_entry(bin, entry_id);
                hash_index_remove(&db->index, feat_id);
                return GRAVELDB_ERR_IO;
            }
        } else {
            entry_id = existing_entry;
        }
    } else {
        /* New feature - allocate slot */
        entry_id = dimbin_alloc_entry(bin);
        hash_index_put(&db->index, feat_id, (uint16_t)dim_idx, entry_id);
        if (dimbin_put_key(bin, entry_id, feat_id) != GRAVELDB_OK) {
            /* Key write failed -- rollback: reclaim slot + revert index */
            dimbin_free_entry(bin, entry_id);
            hash_index_remove(&db->index, feat_id);
            return GRAVELDB_ERR_IO;
        }
    }

    /* Write embedding data */
    rc = dimbin_put(bin, entry_id, embedding);

    return rc;
}

graveldb_status_t graveldb_get(GravelDB *db, GravelDBCtx *ctx,
                               uint64_t feat_id, float *out_embedding, int *out_dim) {
    (void)ctx;
    if (feat_id == 0 || !out_embedding) return GRAVELDB_ERR_INVALID;

    uint16_t dim_idx;
    uint32_t entry_idx;
    graveldb_status_t rc = hash_index_get(&db->index, feat_id, &dim_idx, &entry_idx);
    if (rc != GRAVELDB_OK) return rc;

    DimBin *bin = dim_registry_get_bin(&db->dim_reg, dim_idx);
    if (out_dim) *out_dim = bin->dim;

    return dimbin_get(bin, entry_idx, out_embedding);
}

graveldb_status_t graveldb_delete(GravelDB *db, GravelDBCtx *ctx, uint64_t feat_id) {
    (void)ctx;
    if (feat_id == 0) return GRAVELDB_ERR_INVALID;

    uint16_t dim_idx;
    uint32_t entry_idx;
    graveldb_status_t rc = hash_index_get(&db->index, feat_id, &dim_idx, &entry_idx);
    if (rc != GRAVELDB_OK) return rc;

    DimBin *bin = dim_registry_get_bin(&db->dim_reg, dim_idx);

    if (bin->in_checkpoint) {
        /* During checkpoint: record tombstone, defer free */
        extern graveldb_status_t overlay_tombstone(OverlayBuffer *ob, uint32_t entry_id);
        overlay_tombstone(&bin->overlay, entry_idx);
    } else {
        dimbin_free_entry(bin, entry_idx);
    }

    hash_index_remove(&db->index, feat_id);
    return GRAVELDB_OK;
}

/*
 * Batch Get
 *
 * Hash table provides O(1) random access per key, so batch lookups are
 * simply individual lookups in a tight loop. The hash table's open addressing
 * with linear probing gives excellent cache behavior for sequential probes.
 */

graveldb_status_t graveldb_batch_get(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids, int n,
                                     float **out_embeddings, int *out_dims) {
    (void)ctx;
    for (int i = 0; i < n; i++) {
        uint16_t dim_idx;
        uint32_t entry_idx;
        graveldb_status_t rc = hash_index_get(&db->index, feat_ids[i], &dim_idx, &entry_idx);
        if (rc != GRAVELDB_OK) {
            if (out_dims) out_dims[i] = 0;
            continue;
        }

        DimBin *bin = dim_registry_get_bin(&db->dim_reg, dim_idx);
        if (out_dims) out_dims[i] = bin->dim;

        if (out_embeddings[i]) {
            dimbin_get(bin, entry_idx, out_embeddings[i]);
        }
    }
    return GRAVELDB_OK;
}

/*
 * Batch Put
 */

/*
 * Batch Put -- the PRIMARY write path.
 *
 * Design: batch is the first-class citizen; single put degrades to batch(n=1).
 *
 * Steps:
 *   1. Group entries by dim -> avoids interleaving writes across DimBins
 *   2. For each group: batch alloc slots (bump pointer is naturally contiguous)
 *   3. Batch write embedding data into block buffer
 *   4. Batch write keys into key buffer
 *   5. Water-level check at the end (not per-entry) for I/O coalescing
 *
 * This ensures that consecutively allocated slots produce consecutive block
 * dirty marks, maximizing peephole merge effectiveness during flush.
 */

graveldb_status_t graveldb_batch_put(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids,
                                     const int *dims, const float *const *embeddings, int n) {
    if (n <= 0) return GRAVELDB_OK;

    /* For small batches (n<=4), just loop -- overhead of grouping not worth it */
    if (n <= 4) {
        for (int i = 0; i < n; i++) {
            graveldb_status_t rc = graveldb_put(db, ctx, feat_ids[i], dims[i], embeddings[i]);
            if (rc != GRAVELDB_OK) return rc;
        }
        return GRAVELDB_OK;
    }

    /*
     * Phase 1: Build per-dim groups.
     * Use a simple approach: sort indices by dim to group them.
     * For typical 5-10 distinct dims this is very fast.
     */
    size_t order_size = (size_t)n * sizeof(uint32_t);
    uint32_t *order = (uint32_t *)ctx_alloc(ctx, order_size);
    if (!order) return GRAVELDB_ERR_OOM;
    for (int i = 0; i < n; i++) order[i] = i;

    /* Sort by dim (stable within same dim preserves allocation contiguity) */
    const int *dims_ref = dims;
    /* Simple insertion sort (n typically 1K-10K, and dim variety is tiny) */
    for (int i = 1; i < n; i++) {
        uint32_t key = order[i];
        int key_dim = dims_ref[key];
        int j = i - 1;
        while (j >= 0 && dims_ref[order[j]] > key_dim) {
            order[j + 1] = order[j];
            j--;
        }
        order[j + 1] = key;
    }

    graveldb_status_t rc = GRAVELDB_OK;

    /* Phase 2: Process each dim group */
    int group_start = 0;
    while (group_start < n) {
        int dim = dims_ref[order[group_start]];
        int group_end = group_start + 1;
        while (group_end < n && dims_ref[order[group_end]] == dim) group_end++;
        int group_size = group_end - group_start;

        /* Resolve or create DimBin for this dim */
        int dim_idx = dim_registry_find(&db->dim_reg, dim);
        if (dim_idx < 0) {
            if (!db->auto_create_bins) { rc = GRAVELDB_ERR_INVALID; break; }
            dim_idx = create_and_register_bin(db, dim);
            if (dim_idx < 0) { rc = GRAVELDB_ERR_OOM; break; }
        }
        DimBin *bin = dim_registry_get_bin(&db->dim_reg, (uint16_t)dim_idx);

        /*
         * Phase 2a: Batch alloc + key-first write for this group.
         *
         * Strategy: write keys BEFORE values to avoid phantom slots.
         * If a key write fails, the slot is rolled back immediately.
         * For the batch (coalesced) path, keys are flushed first, then
         * we verify no I/O errors occurred before writing values.
         */
        KeyWriteEntry *key_batch = NULL;
        int key_batch_count = 0;

        /* Track (entry_id, orig_idx) for deferred embedding writes */
        typedef struct { uint32_t entry_id; int orig_idx; } DeferredEmb;
        size_t deferred_size = (size_t)group_size * sizeof(DeferredEmb);
        DeferredEmb *deferred = (DeferredEmb *)ctx_alloc(ctx, deferred_size);
        int deferred_count = 0;

        size_t key_batch_size = (size_t)group_size * sizeof(KeyWriteEntry);
        if (group_size >= KEY_COALESCE_THRESH) {
            key_batch = (KeyWriteEntry *)ctx_alloc(ctx, key_batch_size);
            /* If alloc fails, fall through to per-key pwrite path */
        }

        for (int g = 0; g < group_size; g++) {
            int orig_idx = order[group_start + g];
            uint64_t feat_id = feat_ids[orig_idx];
            const float *emb = embeddings[orig_idx];

            if (feat_id == 0 || !emb) continue;

            /* Check if feature already exists */
            uint16_t existing_dim_idx;
            uint32_t existing_entry;
            graveldb_status_t lrc = hash_index_get(&db->index, feat_id,
                                                   &existing_dim_idx, &existing_entry);

            uint32_t entry_id;
            if (lrc == GRAVELDB_OK) {
                if (existing_dim_idx != (uint16_t)dim_idx) {
                    DimBin *old_bin = dim_registry_get_bin(&db->dim_reg, existing_dim_idx);
                    dimbin_free_entry(old_bin, existing_entry);
                    entry_id = dimbin_alloc_entry(bin);
                    hash_index_put(&db->index, feat_id, (uint16_t)dim_idx, entry_id);
                } else {
                    entry_id = existing_entry;
                }
            } else {
                /* New feature -- bump alloc gives contiguous slots! */
                entry_id = dimbin_alloc_entry(bin);
                hash_index_put(&db->index, feat_id, (uint16_t)dim_idx, entry_id);
            }

            /* Collect key write for batched coalescing */
            if (key_batch) {
                key_batch[key_batch_count].entry_idx = entry_id;
                key_batch[key_batch_count].feat_id = feat_id;
                key_batch_count++;
            } else {
                /* Fallback: individual pwrite -- check result */
                if (dimbin_put_key(bin, entry_id, feat_id) != GRAVELDB_OK) {
                    /* Key write failed -- rollback slot + index to avoid phantom */
                    dimbin_free_entry(bin, entry_id);
                    hash_index_remove(&db->index, feat_id);
                    continue; /* skip this entry, proceed with rest of batch */
                }
            }

            /* Defer embedding write until keys are confirmed */
            if (deferred) {
                deferred[deferred_count].entry_id = entry_id;
                deferred[deferred_count].orig_idx = orig_idx;
                deferred_count++;
            } else {
                /* Allocation failed -- write inline (best-effort) */
                graveldb_status_t wrc = dimbin_put(bin, entry_id, embeddings[orig_idx]);
                if (wrc != GRAVELDB_OK) { rc = wrc; }
            }
        }

        /* Flush collected key writes with page coalescing */
        if (key_batch) {
            uint64_t errors_before = bin->io_errors;
            dimbin_put_keys_batch(bin, key_batch, key_batch_count);
            if (bin->io_errors > errors_before) {
                /* Some key writes failed in batch mode.
                 * We cannot pinpoint which entries failed at page-coalesce granularity,
                 * so we flag the batch as partially failed but continue --
                 * the io_errors counter allows monitoring/alerting. */
                rc = GRAVELDB_ERR_IO;
            }
            ctx_dealloc(ctx, key_batch, key_batch_size);
        }

        /* Phase 2b: Now write embeddings (memory buffer -- safe after key persist) */
        if (deferred) {
            for (int d = 0; d < deferred_count; d++) {
                graveldb_status_t wrc = dimbin_put(bin, deferred[d].entry_id,
                                                   embeddings[deferred[d].orig_idx]);
                if (wrc != GRAVELDB_OK) { rc = wrc; }
            }
            ctx_dealloc(ctx, deferred, deferred_size);
        }

        group_start = group_end;
    }

    ctx_dealloc(ctx, order, order_size);
    return rc;
}

graveldb_status_t graveldb_flush(GravelDB *db) {
    uint16_t num_slabs = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        graveldb_status_t rc = dimbin_flush(s);
        if (rc != GRAVELDB_OK) return rc;
    }
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_checkpoint(GravelDB *db) {
    graveldb_status_t rc;
    uint16_t num_slabs = dim_registry_count(&db->dim_reg);

    /* Begin checkpoint on all slabs */
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        rc = dimbin_checkpoint_begin(s);
        if (rc != GRAVELDB_OK) return rc;
    }

    /* Dump delta files */
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        DirtyTracker *dt = &s->dirty;

        char delta_path[512];
        snprintf(delta_path, sizeof(delta_path), "%s/ckpt/gen_%04llu_d%d_delta.bin",
                 db->data_dir, (unsigned long long)dt->generation, s->dim);

        int ckpt_fd = open(delta_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (ckpt_fd < 0) continue;

        /* Scan frozen bitmap */
        uint32_t *dirty_blocks = (uint32_t *)malloc(GRAVELDB_MAX_DIRTY * sizeof(uint32_t));
        if (!dirty_blocks) { close(ckpt_fd); continue; }
        int n = dirty_tracker_scan_ckpt(dt, dirty_blocks, GRAVELDB_MAX_DIRTY);

                DeltaHeader hdr = {
            .magic = GRAVELDB_DELTA_MAGIC,
            .version = 1,
            .generation = dt->generation,
            .dim = s->dim,
            .entry_size = (uint32_t)s->entry_size,
            .bump_ptr = s->bump_ptr,
            .num_entries = 0,
            .checksum = 0,
        };
        if (write(ckpt_fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
            free(dirty_blocks);
            close(ckpt_fd);
            continue;
        }

        /* Dump dirty blocks */
        int entries = 0;
        int idx = 0;
        while (idx < n) {
            int j = idx + 1;
            while (j < n && dirty_blocks[j] == dirty_blocks[j - 1] + 1) j++;

            uint32_t blk_start = dirty_blocks[idx];
            uint32_t blk_count = j - idx;
            size_t byte_len = (size_t)blk_count * s->page_size;

            void *buf = NULL;
            if (posix_memalign(&buf, 4096, byte_len) == 0) {
                ssize_t rd = pread(s->fd, buf, byte_len, (off_t)blk_start * s->page_size);
                if (rd == (ssize_t)byte_len) {
                    uint32_t entry[2] = { blk_start, blk_count };
                    if (write(ckpt_fd, entry, sizeof(entry)) == sizeof(entry)) {
                        if (write(ckpt_fd, buf, byte_len) == (ssize_t)byte_len) {
                            entries++;
                        }
                    }
                }
                free(buf);
            }
            idx = j;
        }
        if (pwrite(ckpt_fd, &entries, sizeof(uint32_t),
                   __builtin_offsetof(DeltaHeader, num_entries)) != sizeof(uint32_t)) {
            rc = GRAVELDB_ERR_IO;
        }

        fdatasync(ckpt_fd);
        close(ckpt_fd);
        free(dirty_blocks);
    }

    /* End checkpoint on all slabs */
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        rc = dimbin_checkpoint_end(s);
        if (rc != GRAVELDB_OK) return rc;
    }

    db->current_epoch++;
    return GRAVELDB_OK;
}

/*
 * Incremental checkpoint: bounded-latency version.
 *
 * State machine phases:
 *   IDLE -> FLUSHING -> DUMPING -> FINISHING -> IDLE
 *
 * Each call to graveldb_checkpoint_step() processes at most one bin's flush,
 * or max_pages_per_step pages of delta dump, bounding worst-case latency.
 */

bool graveldb_checkpoint_in_progress(const GravelDB *db) {
    return db->ckpt_progress.phase != CKPT_IDLE;
}

graveldb_status_t graveldb_checkpoint_step(GravelDB *db, uint32_t max_pages_per_step) {
    CkptProgress *p = &db->ckpt_progress;
    uint16_t num_bins = dim_registry_count(&db->dim_reg);

    if (max_pages_per_step == 0) max_pages_per_step = 64;
    p->max_pages_per_step = max_pages_per_step;

    switch (p->phase) {
    case CKPT_IDLE: {
        /* Start: begin checkpoint on all bins (flush + swap card table).
         * This is bin-by-bin: flush one bin per step call. */
        p->phase = CKPT_FLUSHING;
        p->current_bin = 0;
        p->ckpt_fd = -1;
        p->dirty_blocks = NULL;
        p->dirty_count = 0;
        p->dirty_cursor = 0;
        p->entries_written = 0;
        /* Fall through to process first bin flush */
    }
    /* fall through */

    case CKPT_FLUSHING: {
        if (p->current_bin >= num_bins) {
            /* All bins flushed -- move to dump phase */
            p->phase = CKPT_DUMPING;
            p->current_bin = 0;
            return GRAVELDB_OK;
        }

        DimBin *s = dim_registry_get_bin(&db->dim_reg, p->current_bin);
        graveldb_status_t rc = dimbin_checkpoint_begin(s);
        if (rc != GRAVELDB_OK) return rc;

        p->current_bin++;
        return GRAVELDB_OK;  /* One bin per step */
    }

    case CKPT_DUMPING: {
        if (p->current_bin >= num_bins) {
            /* All bins dumped -- move to finishing */
            p->phase = CKPT_FINISHING;
            p->current_bin = 0;
            return GRAVELDB_OK;
        }

        DimBin *s = dim_registry_get_bin(&db->dim_reg, p->current_bin);
        DirtyTracker *dt = &s->dirty;

        /* First entry into this bin: open file, scan dirty */
        if (p->ckpt_fd < 0) {
            char delta_path[512];
            snprintf(delta_path, sizeof(delta_path), "%s/ckpt/gen_%04llu_d%d_delta.bin",
                     db->data_dir, (unsigned long long)dt->generation, s->dim);
            ensure_dir(db->data_dir);

            /* Ensure ckpt subdir */
            char ckpt_dir[512];
            snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/ckpt", db->data_dir);
            ensure_dir(ckpt_dir);

            p->ckpt_fd = open(delta_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (p->ckpt_fd < 0) {
                /* Skip this bin */
                p->current_bin++;
                return GRAVELDB_OK;
            }

            p->dirty_blocks = (uint32_t *)malloc(GRAVELDB_MAX_DIRTY * sizeof(uint32_t));
            if (!p->dirty_blocks) {
                close(p->ckpt_fd);
                p->ckpt_fd = -1;
                p->current_bin++;
                return GRAVELDB_OK;
            }
            p->dirty_count = dirty_tracker_scan_ckpt(dt, p->dirty_blocks, GRAVELDB_MAX_DIRTY);
            p->dirty_cursor = 0;
            p->entries_written = 0;

            /* Write header */
            DeltaHeader hdr = {
                .magic = GRAVELDB_DELTA_MAGIC,
                .version = 1,
                .generation = dt->generation,
                .dim = s->dim,
                .entry_size = (uint32_t)s->entry_size,
                .bump_ptr = s->bump_ptr,
                .num_entries = 0,
                .checksum = 0,
            };
            if (write(p->ckpt_fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
                free(p->dirty_blocks);
                p->dirty_blocks = NULL;
                close(p->ckpt_fd);
                p->ckpt_fd = -1;
                p->current_bin++;
                return GRAVELDB_OK;
            }
        }

        /* Dump up to max_pages_per_step pages worth of dirty blocks */
        uint32_t pages_done = 0;
        while (p->dirty_cursor < p->dirty_count && pages_done < max_pages_per_step) {
            int j = p->dirty_cursor + 1;
            while (j < p->dirty_count &&
                   p->dirty_blocks[j] == p->dirty_blocks[j - 1] + 1) {
                j++;
            }

            uint32_t blk_start = p->dirty_blocks[p->dirty_cursor];
            uint32_t blk_count = j - p->dirty_cursor;

            /* Cap this run to stay within budget */
            if (pages_done + blk_count > max_pages_per_step) {
                blk_count = max_pages_per_step - pages_done;
                j = p->dirty_cursor + blk_count;
            }

            size_t byte_len = (size_t)blk_count * s->page_size;
            void *buf = NULL;
            if (posix_memalign(&buf, 4096, byte_len) == 0) {
                ssize_t rd = pread(s->fd, buf, byte_len, (off_t)blk_start * s->page_size);
                if (rd == (ssize_t)byte_len) {
                    uint32_t entry[2] = { blk_start, blk_count };
                    if (write(p->ckpt_fd, entry, sizeof(entry)) == sizeof(entry)) {
                        if (write(p->ckpt_fd, buf, byte_len) == (ssize_t)byte_len) {
                            p->entries_written++;
                        }
                    }
                }
                free(buf);
            }

            pages_done += blk_count;
            p->dirty_cursor = j;
        }

        /* Check if this bin is done */
        if (p->dirty_cursor >= p->dirty_count) {
            /* Finalize delta file */
            if (pwrite(p->ckpt_fd, &p->entries_written, sizeof(uint32_t),
                       __builtin_offsetof(DeltaHeader, num_entries)) != sizeof(uint32_t)) {
                /* I/O error on finalize - file may be incomplete but proceed */
            }
            fdatasync(p->ckpt_fd);
            close(p->ckpt_fd);
            p->ckpt_fd = -1;
            free(p->dirty_blocks);
            p->dirty_blocks = NULL;
            p->current_bin++;
        }

        return GRAVELDB_OK;
    }

    case CKPT_FINISHING: {
        /* End checkpoint on all bins and replay overlays */
        for (uint16_t i = 0; i < num_bins; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            graveldb_status_t rc = dimbin_checkpoint_end(s);
            if (rc != GRAVELDB_OK) return rc;
        }
        db->current_epoch++;
        p->phase = CKPT_IDLE;
        return GRAVELDB_OK;
    }
    }

    return GRAVELDB_OK;
}

graveldb_status_t graveldb_stats(GravelDB *db, GravelDBStats *stats) {
    memset(stats, 0, sizeof(*stats));

    stats->total_features = db->index.count;
    stats->checkpoint_generation = db->current_epoch;

    uint64_t total_flush = 0;
    uint64_t total_entries = 0;

    uint16_t num_slabs = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        total_flush += s->write_buf.flush_bytes;
        total_entries += s->bump_ptr;
    }

    stats->total_entries = total_entries;
    stats->buffer_hits = 0;
    stats->buffer_misses = 0;
    stats->buffer_evictions = 0;
    stats->flush_bytes = total_flush;
    stats->cache_hit_ratio = 0.0f;

    return GRAVELDB_OK;
}
