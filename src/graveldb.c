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
#include "wire.h"

/* Peephole gap for flush merge (same value as dimbin.c) */
#define PEEPHOLE_GAP_MAX  4

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    return (va > vb) - (va < vb);
}

void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        mkdir(path, 0755);
    }
}

/*
 * Detect optimal page size for I/O on the given path.
 * Clamps to [512, 65536], ensures power of 2.
 * Falls back to GRAVELDB_PAGE_SIZE_DEFAULT (4096).
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

        /* Hint to kernel: we'll read this file sequentially */
#ifdef __linux__
        posix_fadvise(bin->key_fd, 0, (off_t)keys_bytes, POSIX_FADV_SEQUENTIAL);
#endif

        ssize_t rd = pread(bin->key_fd, keys, keys_bytes, 0);
        if (rd < 0) { free(keys); continue; } /* empty/missing file is OK */

        /* Restore normal access pattern after bulk read */
#ifdef __linux__
        posix_fadvise(bin->key_fd, 0, 0, POSIX_FADV_RANDOM);
#endif

        /* Data files are always native byte order (no wire conversion needed).
         * Cross-node access goes through checkpoint serialized files only. */

        uint64_t keys_read = (uint64_t)rd / sizeof(uint64_t);

        for (uint64_t entry_idx = 0; entry_idx < keys_read; entry_idx++) {
            uint64_t feat_id = keys[entry_idx];
            if (feat_id == 0) {
                dimbin_free_entry(bin, (uint32_t)entry_idx);
                continue;
            }

            /* Cross-bin conflict: incomplete dim-change crashed mid-way.
             * Discard from both bins (conservative). */
            uint16_t prev_dim_idx;
            uint32_t prev_entry_idx;
            graveldb_status_t lrc = hash_index_get(&db->index, feat_id,
                                                   &prev_dim_idx, &prev_entry_idx);
            if (lrc == GRAVELDB_OK && prev_dim_idx != dim_idx) {
                static const uint8_t zb[8] = {0};
                DimBin *prev_bin = dim_registry_get_bin(&db->dim_reg, prev_dim_idx);
                pwrite(prev_bin->key_fd, zb, 8, (off_t)prev_entry_idx * 8);
                dimbin_free_entry(prev_bin, prev_entry_idx);
                hash_index_remove(&db->index, feat_id);
                pwrite(bin->key_fd, zb, 8, (off_t)entry_idx * 8);
                dimbin_free_entry(bin, (uint32_t)entry_idx);
            } else {
                hash_index_put(&db->index, feat_id, dim_idx, (uint32_t)entry_idx);
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
    bin->overlay_budget = db->config.overlay_budget;

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

    /* Initialize persistent io_uring ring (reused across flushes) */
    if (uring_io_init(&db->io_ring) != 0) {
        /* Non-fatal: flush paths will detect uninitialized ring and fallback */
        db->io_ring.initialized = false;
    }

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
        bin->overlay_budget = config->overlay_budget;

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

    /* Restore dirty_tracker generation from .meta (if prior checkpoint exists).
     * DirtyTracker starts clean; downstream detects generation gap and reloads
     * from bin file (full snapshot) if needed. */
    {
        uint16_t num_bins_rc = dim_registry_count(&db->dim_reg);
        for (uint16_t i = 0; i < num_bins_rc; i++) {
            DimBin *bin = dim_registry_get_bin(&db->dim_reg, i);
            BinMeta meta;
            graveldb_status_t frc = ckpt_read_meta(db->data_dir, bin->dim, &meta);

            if (frc == GRAVELDB_OK) {
                bin->dirty.generation = meta.generation;
            }
        }
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
        dimbin_flush(s, true);
        dimbin_destroy(s);
        free(s);
    }

    dim_registry_destroy(&db->dim_reg);
    hash_index_destroy(&db->index);

    /* Destroy persistent io_uring ring */
    uring_io_destroy(&db->io_ring);

    /* Destroy slab allocator (after all subsystems) */
    slab_allocator_destroy(&db->allocator);

    free((void *)db->config.dims);
    free(db->data_dir);
    free(db);
}

/*
 * Batch Get -- resolve through hash index, check overlay/write buffer,
 * coalesce disk misses by page, submit as io_uring batch.
 */

/* Internal types for two-phase async batch get */
typedef struct {
    int       req_idx;
    DimBin   *bin;
    uint32_t  entry_id;
    uint32_t  page_id;
    uint32_t  offset_in_page;
} BatchGetDiskRead;

typedef struct {
    DimBin   *bin;
    uint32_t  page_id;
    uint8_t  *buf;
    int       first_idx;
    int       count;
} BatchGetPageRead;

typedef struct {
    float          **out_embeddings;
    BatchGetDiskRead *disk_reads;
    BatchGetPageRead *page_reads;
    uring_io_ctx_t    io_ctx;
    int               disk_count;
    int               page_count;
    int               use_uring;
    graveldb_status_t submit_rc;
    /* Ownership flags for heap-allocated arrays */
    int               disk_reads_on_heap;
    int               page_reads_on_heap;
} AsyncGetState;

graveldb_status_t graveldb_batch_get_submit(GravelDB *db, GravelDBCtx *ctx,
                                            const uint64_t *feat_ids, int n,
                                            float **out_embeddings, int *out_dims,
                                            GravelDBAsyncGet *async_out) {
    (void)ctx;
    async_out->internal = NULL;

    if (n <= 0) return GRAVELDB_OK;

    /* Phase 1: resolve + memory hits */
    BatchGetDiskRead stack_reads[64];
    BatchGetDiskRead *disk_reads = (n <= 64) ? stack_reads :
                           (BatchGetDiskRead *)malloc((size_t)n * sizeof(BatchGetDiskRead));
    if (!disk_reads) return GRAVELDB_ERR_OOM;
    int disk_reads_on_heap = (disk_reads != stack_reads);
    int disk_count = 0;

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
        if (!out_embeddings[i]) continue;

        /* Check overlay (checkpoint in progress) */
        if (bin->in_checkpoint && overlay_contains(&bin->overlay, entry_idx)) {
            overlay_get(&bin->overlay, entry_idx, out_embeddings[i], bin->dim);
            continue;
        }

        uint32_t page_id = entry_idx / bin->entries_per_page;
        uint32_t offset_in_page = (entry_idx % bin->entries_per_page) * bin->entry_size;

        /* Check write buffer (read-your-writes) */
        WriteBuffer *wb = &bin->write_buf;
        uint32_t wb_idx = pagemap_find(wb->slots, wb->capacity, page_id);
        if (wb->slots[wb_idx].page_id == page_id) {
            memcpy(out_embeddings[i], wb->slots[wb_idx].data + offset_in_page, bin->entry_size);
            continue;
        }

        /* Disk miss: collect for batch I/O */
        disk_reads[disk_count].req_idx = i;
        disk_reads[disk_count].bin = bin;
        disk_reads[disk_count].entry_id = entry_idx;
        disk_reads[disk_count].page_id = page_id;
        disk_reads[disk_count].offset_in_page = offset_in_page;
        disk_count++;
    }

    if (disk_count == 0) {
        if (disk_reads_on_heap) free(disk_reads);
        return GRAVELDB_OK;
    }

    /* Phase 2: Sort disk reads by (bin pointer, page_id) for coalescing */
    for (int i = 1; i < disk_count; i++) {
        BatchGetDiskRead key = disk_reads[i];
        int j = i - 1;
        while (j >= 0 && (disk_reads[j].bin > key.bin ||
               (disk_reads[j].bin == key.bin && disk_reads[j].page_id > key.page_id))) {
            disk_reads[j + 1] = disk_reads[j];
            j--;
        }
        disk_reads[j + 1] = key;
    }

    /* Phase 3: Deduplicate pages + submit IO (don't wait) */
    BatchGetPageRead stack_pages[64];
    int max_pages = disk_count;
    BatchGetPageRead *page_reads = (max_pages <= 64) ? stack_pages :
                           (BatchGetPageRead *)malloc((size_t)max_pages * sizeof(BatchGetPageRead));
    if (!page_reads) {
        if (disk_reads_on_heap) free(disk_reads);
        return GRAVELDB_ERR_OOM;
    }
    int page_reads_on_heap = (page_reads != stack_pages);
    int page_count = 0;

    /* Identify unique pages */
    int i = 0;
    while (i < disk_count) {
        DimBin *bin = disk_reads[i].bin;
        uint32_t pg = disk_reads[i].page_id;
        int j = i + 1;
        while (j < disk_count && disk_reads[j].bin == bin && disk_reads[j].page_id == pg) {
            j++;
        }
        page_reads[page_count].bin = bin;
        page_reads[page_count].page_id = pg;
        page_reads[page_count].first_idx = i;
        page_reads[page_count].count = j - i;
        page_reads[page_count].buf = NULL;
        page_count++;
        i = j;
    }

    /* Allocate state that persists between submit and complete */
    AsyncGetState *state = (AsyncGetState *)malloc(sizeof(AsyncGetState));
    if (!state) {
        if (page_reads_on_heap) free(page_reads);
        if (disk_reads_on_heap) free(disk_reads);
        return GRAVELDB_ERR_OOM;
    }

    /* If disk_reads/page_reads are on stack, copy to heap for persistence */
    if (!disk_reads_on_heap) {
        BatchGetDiskRead *heap_dr = (BatchGetDiskRead *)malloc((size_t)disk_count * sizeof(BatchGetDiskRead));
        if (!heap_dr) {
            if (page_reads_on_heap) free(page_reads);
            free(state);
            return GRAVELDB_ERR_OOM;
        }
        memcpy(heap_dr, disk_reads, (size_t)disk_count * sizeof(BatchGetDiskRead));
        disk_reads = heap_dr;
    }
    if (!page_reads_on_heap) {
        BatchGetPageRead *heap_pr = (BatchGetPageRead *)malloc((size_t)page_count * sizeof(BatchGetPageRead));
        if (!heap_pr) {
            free(disk_reads);
            free(state);
            return GRAVELDB_ERR_OOM;
        }
        memcpy(heap_pr, page_reads, (size_t)page_count * sizeof(BatchGetPageRead));
        page_reads = heap_pr;
    }

    state->out_embeddings = out_embeddings;
    state->disk_reads = disk_reads;
    state->page_reads = page_reads;
    state->disk_count = disk_count;
    state->page_count = page_count;
    state->disk_reads_on_heap = 1;
    state->page_reads_on_heap = 1;
    state->submit_rc = GRAVELDB_OK;

    /* Submit IO */
    state->use_uring = (uring_io_init(&state->io_ctx) == 0);

    for (int p = 0; p < page_count; p++) {
        DimBin *bin = page_reads[p].bin;
        uint32_t pg = page_reads[p].page_id;

        if (page_reads[p].count == 1 && bin->entries_per_page == 1) {
            int ri = page_reads[p].first_idx;
            page_reads[p].buf = (uint8_t *)out_embeddings[disk_reads[ri].req_idx];
            if (state->use_uring) {
                uring_io_submit_read(&state->io_ctx, bin->fd, page_reads[p].buf,
                                     bin->entry_size, (off_t)disk_reads[ri].entry_id * bin->entry_size);
            } else {
                ssize_t rd = pread(bin->fd, page_reads[p].buf, bin->entry_size,
                                   (off_t)disk_reads[ri].entry_id * bin->entry_size);
                if (rd < 0) state->submit_rc = GRAVELDB_ERR_IO;
            }
            continue;
        }

        uint8_t *pbuf = (uint8_t *)malloc(bin->page_size);
        if (!pbuf) { state->submit_rc = GRAVELDB_ERR_OOM; continue; }
        page_reads[p].buf = pbuf;

        if (state->use_uring) {
            uring_io_submit_read(&state->io_ctx, bin->fd, pbuf,
                                 bin->page_size, (off_t)pg * bin->page_size);
        } else {
            ssize_t rd = pread(bin->fd, pbuf, bin->page_size, (off_t)pg * bin->page_size);
            if (rd < 0) state->submit_rc = GRAVELDB_ERR_IO;
        }
    }

    async_out->internal = state;
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_batch_get_poll(GravelDBAsyncGet *async_ctx) {
    AsyncGetState *state = (AsyncGetState *)async_ctx->internal;
    if (!state) return GRAVELDB_OK;  /* All memory hits, nothing to wait for */

    /* Non-blocking check: are all IOs done? */
    if (state->use_uring) {
        int still_pending = uring_io_poll(&state->io_ctx);
        if (still_pending > 0) {
            return GRAVELDB_AGAIN;  /* Not done yet, caller should do other work */
        }
    }
    /* Fallback (macOS): IO was done synchronously in submit, always ready */

    graveldb_status_t rc = state->submit_rc;
    if (state->use_uring && state->io_ctx.errors > 0)
        rc = GRAVELDB_ERR_IO;
    uring_io_destroy(&state->io_ctx);

    /* Phase 4: Scatter data from page buffers to output */
    for (int p = 0; p < state->page_count; p++) {
        if (!state->page_reads[p].buf) continue;
        DimBin *bin = state->page_reads[p].bin;

        if (state->page_reads[p].count == 1 && bin->entries_per_page == 1) {
            /* Data was read directly into output buffer — native format, no conversion */
            continue;
        }

        /* Data files are native byte order — scatter directly */

        for (int k = 0; k < state->page_reads[p].count; k++) {
            int ri = state->page_reads[p].first_idx + k;
            int req_idx = state->disk_reads[ri].req_idx;
            uint32_t off = state->disk_reads[ri].offset_in_page;
            memcpy(state->out_embeddings[req_idx], state->page_reads[p].buf + off, bin->entry_size);
        }

        free(state->page_reads[p].buf);
    }

    free(state->page_reads);
    free(state->disk_reads);
    free(state);
    async_ctx->internal = NULL;
    return rc;
}

void graveldb_batch_get_cancel(GravelDBAsyncGet *async_ctx) {
    AsyncGetState *state = (AsyncGetState *)async_ctx->internal;
    if (!state) return;

    /* Wait for pending IO before freeing buffers io_uring may reference */
    if (state->use_uring) {
        if (state->io_ctx.pending > 0) uring_io_wait(&state->io_ctx);
        uring_io_destroy(&state->io_ctx);
    }

    /* Free page buffers (skip direct-to-output pages) */
    for (int p = 0; p < state->page_count; p++) {
        if (!state->page_reads[p].buf) continue;
        DimBin *bin = state->page_reads[p].bin;
        if (state->page_reads[p].count == 1 && bin->entries_per_page == 1) continue;
        free(state->page_reads[p].buf);
    }

    free(state->page_reads);
    free(state->disk_reads);
    free(state);
    async_ctx->internal = NULL;
}

/* Synchronous batch_get: submit + poll until done */
graveldb_status_t graveldb_batch_get(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids, int n,
                                     float **out_embeddings, int *out_dims) {
    GravelDBAsyncGet ag;
    graveldb_status_t rc = graveldb_batch_get_submit(db, ctx, feat_ids, n,
                                                     out_embeddings, out_dims, &ag);
    if (rc != GRAVELDB_OK) return rc;

    /* Spin-poll until done (for synchronous callers) */
    graveldb_status_t poll_rc;
    while ((poll_rc = graveldb_batch_get_poll(&ag)) == GRAVELDB_AGAIN) {
        /* busy wait — synchronous callers accept this */
    }
    return poll_rc;
}

/*
 * Batch Put -- group by dim, batch alloc, page-coalesced embedding write.
 * Single put degrades to batch(n=1).
 */

graveldb_status_t graveldb_batch_put(GravelDB *db, GravelDBCtx *ctx,
                                     const uint64_t *feat_ids,
                                     const int *dims, const float *const *embeddings, int n) {
    if (n <= 0) return GRAVELDB_OK;

    /*
     * Phase 1: Sort indices by dim to group them.
     */
    size_t order_size = (size_t)n * sizeof(uint32_t);
    uint32_t *order = (uint32_t *)ctx_alloc(ctx, order_size);
    if (!order) return GRAVELDB_ERR_OOM;
    for (int i = 0; i < n; i++) order[i] = i;

    /* Sort by dim (insertion sort -- dim variety is tiny) */
    const int *dims_ref = dims;
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

        /* Phase 2a: Alloc slots + buffer key writes for this group */

        /* Track (entry_id, orig_idx) for deferred embedding writes */
        typedef struct { uint32_t entry_id; int orig_idx; } DeferredEmb;
        size_t deferred_size = (size_t)group_size * sizeof(DeferredEmb);
        DeferredEmb *deferred = (DeferredEmb *)ctx_alloc(ctx, deferred_size);
        int deferred_count = 0;

        /* Pre-reserve file space to avoid ftruncate in the alloc loop */
        dimbin_reserve(bin, (uint32_t)group_size);

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
                    /* Dim change not allowed; caller must delete first */
                    rc = GRAVELDB_ERR_DIM_MISMATCH;
                    continue;
                }
                entry_id = existing_entry;
            } else {
                /* New feature -- bump alloc gives contiguous slots! */
                entry_id = dimbin_alloc_entry(bin);
                hash_index_put(&db->index, feat_id, (uint16_t)dim_idx, entry_id);
            }

            /* Buffer key write (will be flushed before values in dimbin_flush) */
            dimbin_buf_key(bin, entry_id, feat_id);

            /* Defer embedding write */
            if (deferred) {
                deferred[deferred_count].entry_id = entry_id;
                deferred[deferred_count].orig_idx = orig_idx;
                deferred_count++;
            } else {
                /* Allocation failed -- write inline via batch(1) */
                EmbWriteEntry single = { .entry_id = entry_id, .data = embeddings[orig_idx] };
                graveldb_status_t wrc = dimbin_put_batch(bin, &single, 1);
                if (wrc != GRAVELDB_OK) { rc = wrc; }
            }
        }

        /* Phase 2b: Batch write embeddings with page-level coalescing */
        if (deferred && deferred_count > 0) {
            /* Build EmbWriteEntry array from deferred list */
            size_t emb_batch_size = (size_t)deferred_count * sizeof(EmbWriteEntry);
            EmbWriteEntry *emb_batch = (EmbWriteEntry *)ctx_alloc(ctx, emb_batch_size);
            if (emb_batch) {
                for (int d = 0; d < deferred_count; d++) {
                    emb_batch[d].entry_id = deferred[d].entry_id;
                    emb_batch[d].data = embeddings[deferred[d].orig_idx];
                }
                graveldb_status_t wrc = dimbin_put_batch(bin, emb_batch, deferred_count);
                if (wrc != GRAVELDB_OK) { rc = wrc; }
                ctx_dealloc(ctx, emb_batch, emb_batch_size);
            } else {
                /* Fallback: individual batch(1) calls if alloc fails */
                for (int d = 0; d < deferred_count; d++) {
                    EmbWriteEntry single = { .entry_id = deferred[d].entry_id,
                                             .data = embeddings[deferred[d].orig_idx] };
                    graveldb_status_t wrc = dimbin_put_batch(bin, &single, 1);
                    if (wrc != GRAVELDB_OK) { rc = wrc; }
                }
            }
            ctx_dealloc(ctx, deferred, deferred_size);
        }

        group_start = group_end;
    }

    ctx_dealloc(ctx, order, order_size);
    return rc;
}

graveldb_status_t graveldb_batch_delete(GravelDB *db, GravelDBCtx *ctx,
                                        const uint64_t *feat_ids, int n) {
    (void)ctx;
    if (n <= 0) return GRAVELDB_OK;

    for (int i = 0; i < n; i++) {
        uint64_t feat_id = feat_ids[i];
        if (feat_id == 0) continue;

        uint16_t dim_idx;
        uint32_t entry_idx;
        graveldb_status_t rc = hash_index_get(&db->index, feat_id, &dim_idx, &entry_idx);
        if (rc != GRAVELDB_OK) continue;  /* not found — skip silently */

        DimBin *bin = dim_registry_get_bin(&db->dim_reg, dim_idx);

        if (bin->in_checkpoint) {
            overlay_tombstone(&bin->overlay, entry_idx);
        } else {
            dimbin_free_entry(bin, entry_idx);
        }

        hash_index_remove(&db->index, feat_id);
    }

    return GRAVELDB_OK;
}

/*
 * Cross-DimBin flush: keys first (crash ordering), then value pages
 * batched via io_uring across all data fds.
 */
graveldb_status_t graveldb_flush(GravelDB *db) {
    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    if (num_bins == 0) return GRAVELDB_OK;

    graveldb_status_t rc = GRAVELDB_OK;

    /* Phase 1: Flush key buffers (crash ordering) */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        if (s->key_buf.count == 0) continue;
        graveldb_status_t krc = dimbin_flush_keys(s);
        if (krc != GRAVELDB_OK) rc = krc;
        s->flush_needed = false;
    }

    /* Sync key files for durability */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        if (s->key_fd >= 0 && fdatasync(s->key_fd) != 0) {
            rc = GRAVELDB_ERR_IO;
        }
    }

    /* Phase 2: Submit value page writes (only dirty bins) */
    uring_io_ctx_t *io_ctx = &db->io_ring;
    int use_uring = io_ctx->initialized;
    if (use_uring) uring_io_reset(io_ctx);

    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        WriteBuffer *wb = &s->write_buf;
        if (wb->count == 0) continue;

        /* Collect page_ids from this bin's hashmap */
        uint32_t *dirty_pages = s->flush_dirty_buf;
        int n = 0;
        for (uint32_t j = 0; j < wb->capacity && n < GRAVELDB_MAX_FLUSH_BATCH; j++) {
            if (wb->slots[j].page_id != PAGE_SLOT_EMPTY) {
                dirty_pages[n++] = wb->slots[j].page_id;
            }
        }
        if (n == 0) continue;

        /* Sort for peephole merge */
        qsort(dirty_pages, n, sizeof(uint32_t), cmp_u32);

        /* Submit writes for this bin into shared ring */
        int pi = 0;
        while (pi < n) {
            int pj = pi + 1;
            while (pj < n && dirty_pages[pj] <= dirty_pages[pj - 1] + PEEPHOLE_GAP_MAX + 1) {
                pj++;
            }
            for (int k = pi; k < pj; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;

                /* Data files are native byte order — write directly */

                if (use_uring) {
                    if (uring_io_submit_write(io_ctx, s->fd,
                                             wb->slots[slot_idx].data,
                                             s->page_size,
                                             (off_t)pg * s->page_size) < 0) {
                        rc = GRAVELDB_ERR_IO;
                    }
                } else {
                    ssize_t wr = pwrite(s->fd, wb->slots[slot_idx].data,
                                        s->page_size, (off_t)pg * s->page_size);
                    if (wr != (ssize_t)s->page_size) rc = GRAVELDB_ERR_IO;
                }
            }
            wb->flush_bytes += (size_t)(pj - pi) * s->page_size;
            pi = pj;
        }
    }

    /* Submit per-fd fsyncs + wait for all I/O across all bins */
    if (use_uring) {
        uring_io_submit_fsyncs(io_ctx);
        int errors = uring_io_wait(io_ctx);
        if (errors > 0) rc = GRAVELDB_ERR_IO;
    } else {
        /* Fallback: pwrite is synchronous, fdatasync each data fd */
        for (uint16_t i = 0; i < num_bins; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            if (s->write_buf.count == 0) continue;
            if (fdatasync(s->fd) != 0) rc = GRAVELDB_ERR_IO;
        }
    }

    /* Phase 3: Free pages from flushed bins AFTER I/O confirmed complete */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        WriteBuffer *wb = &s->write_buf;
        if (wb->count == 0) continue;

        uint32_t *dirty_pages = s->flush_dirty_buf;
        int n = 0;
        for (uint32_t j = 0; j < wb->capacity && n < GRAVELDB_MAX_FLUSH_BATCH; j++) {
            if (wb->slots[j].page_id != PAGE_SLOT_EMPTY) {
                dirty_pages[n++] = wb->slots[j].page_id;
            }
        }
        if (n == (int)wb->count) {
            /* Full flush: recycle all pages then bulk-clear hashmap */
            for (int k = 0; k < n; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;
                write_buf_recycle_page(wb, wb->slots[slot_idx].data);
            }
            memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
            wb->count = 0;
        } else {
            /* Partial flush: memset + re-insert remaining */
            for (int k = 0; k < n; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;
                write_buf_recycle_page(wb, wb->slots[slot_idx].data);
                wb->slots[slot_idx].page_id = PAGE_SLOT_EMPTY;
                wb->slots[slot_idx].data = NULL;
            }
            uint32_t rem = wb->count - (uint32_t)n;
            PageSlot *tmp_slots = rem > 0 ? (PageSlot *)malloc(rem * sizeof(PageSlot)) : NULL;
            if (tmp_slots) {
                uint32_t ri = 0;
                for (uint32_t ci = 0; ci < wb->capacity && ri < rem; ci++) {
                    if (wb->slots[ci].page_id != PAGE_SLOT_EMPTY)
                        tmp_slots[ri++] = wb->slots[ci];
                }
                memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
                for (uint32_t ri2 = 0; ri2 < ri; ri2++) {
                    uint32_t idx = pagemap_find(wb->slots, wb->capacity, tmp_slots[ri2].page_id);
                    wb->slots[idx] = tmp_slots[ri2];
                }
                free(tmp_slots);
                wb->count = ri;
            } else {
                uint32_t cnt = 0;
                for (uint32_t ci = 0; ci < wb->capacity; ci++) {
                    if (wb->slots[ci].page_id == PAGE_SLOT_EMPTY) continue;
                    PageSlot t = wb->slots[ci];
                    wb->slots[ci].page_id = PAGE_SLOT_EMPTY;
                    wb->slots[ci].data = NULL;
                    uint32_t new_idx = pagemap_find(wb->slots, wb->capacity, t.page_id);
                    wb->slots[new_idx] = t;
                    cnt++;
                }
                wb->count = cnt;
            }
        }
    }

    return rc;
}

bool graveldb_flush_needed(GravelDB *db) {
    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        if (s->flush_needed) return true;
    }
    return false;
}

/*
 * Async flush state: persists between submit and poll.
 */
typedef struct {
    GravelDB       *db;
    int             use_uring;
    graveldb_status_t submit_rc;
} AsyncFlushState;

graveldb_status_t graveldb_flush_submit(GravelDB *db, GravelDBAsyncFlush *async_out) {
    async_out->internal = NULL;

    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    if (num_bins == 0) return GRAVELDB_OK;

    graveldb_status_t rc = GRAVELDB_OK;

    /* Phase 1: Flush key buffers synchronously (must complete before values) */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        if (!s->flush_needed && s->key_buf.count == 0) continue;
        graveldb_status_t krc = dimbin_flush_keys(s);
        if (krc != GRAVELDB_OK) rc = krc;
        s->flush_needed = false;
    }

    /* Sync key files for durability (crash ordering: keys before values) */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        if (s->key_fd >= 0 && fdatasync(s->key_fd) != 0) {
            rc = GRAVELDB_ERR_IO;
        }
    }

    /* Phase 2: Submit value page writes (non-blocking on Linux) */
    AsyncFlushState *state = (AsyncFlushState *)malloc(sizeof(AsyncFlushState));
    if (!state) return GRAVELDB_ERR_OOM;

    state->db = db;
    state->submit_rc = rc;
    state->use_uring = db->io_ring.initialized;
    if (state->use_uring) uring_io_reset(&db->io_ring);

    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        WriteBuffer *wb = &s->write_buf;
        if (wb->count == 0) continue;

        uint32_t *dirty_pages = s->flush_dirty_buf;
        int n = 0;
        for (uint32_t j = 0; j < wb->capacity && n < GRAVELDB_MAX_FLUSH_BATCH; j++) {
            if (wb->slots[j].page_id != PAGE_SLOT_EMPTY) {
                dirty_pages[n++] = wb->slots[j].page_id;
            }
        }
        if (n == 0) continue;

        qsort(dirty_pages, n, sizeof(uint32_t), cmp_u32);

        int pi = 0;
        while (pi < n) {
            int pj = pi + 1;
            while (pj < n && dirty_pages[pj] <= dirty_pages[pj - 1] + PEEPHOLE_GAP_MAX + 1) {
                pj++;
            }
            for (int k = pi; k < pj; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;

                /* Data files are native byte order — write directly */

                if (state->use_uring) {
                    if (uring_io_submit_write(&db->io_ring, s->fd,
                                             wb->slots[slot_idx].data,
                                             s->page_size,
                                             (off_t)pg * s->page_size) < 0) {
                        state->submit_rc = GRAVELDB_ERR_IO;
                    }
                } else {
                    ssize_t wr = pwrite(s->fd, wb->slots[slot_idx].data,
                                        s->page_size, (off_t)pg * s->page_size);
                    if (wr != (ssize_t)s->page_size) state->submit_rc = GRAVELDB_ERR_IO;
                }
            }
            wb->flush_bytes += (size_t)(pj - pi) * s->page_size;
            pi = pj;
        }
    }

    /* Submit fsyncs (on Linux these go into the ring; on macOS done inline) */
    if (state->use_uring) {
        uring_io_submit_fsyncs(&db->io_ring);
    } else {
        /* Fallback: fdatasync each data fd inline */
        for (uint16_t i = 0; i < num_bins; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            if (s->write_buf.count == 0) continue;
            if (fdatasync(s->fd) != 0) state->submit_rc = GRAVELDB_ERR_IO;
        }
    }

    async_out->internal = state;
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_flush_poll(GravelDBAsyncFlush *async_ctx) {
    AsyncFlushState *state = (AsyncFlushState *)async_ctx->internal;
    if (!state) return GRAVELDB_OK;

    GravelDB *db = state->db;

    /* Non-blocking poll: check if io_uring writes are done */
    if (state->use_uring) {
        int still_pending = uring_io_poll(&db->io_ring);
        if (still_pending > 0) {
            return GRAVELDB_AGAIN;
        }
        if (db->io_ring.errors > 0) state->submit_rc = GRAVELDB_ERR_IO;
    }

    graveldb_status_t rc = state->submit_rc;
    free(state);
    async_ctx->internal = NULL;

    /* Phase 3: Free page buffers now that I/O is confirmed complete */
    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        WriteBuffer *wb = &s->write_buf;
        if (wb->count == 0) continue;

        uint32_t *dirty_pages = s->flush_dirty_buf;
        int n = 0;
        for (uint32_t j = 0; j < wb->capacity && n < GRAVELDB_MAX_FLUSH_BATCH; j++) {
            if (wb->slots[j].page_id != PAGE_SLOT_EMPTY) {
                dirty_pages[n++] = wb->slots[j].page_id;
            }
        }
        if (n == (int)wb->count) {
            for (int k = 0; k < n; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;
                write_buf_recycle_page(wb, wb->slots[slot_idx].data);
            }
            memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
            wb->count = 0;
        } else {
            /* Partial flush: memset + re-insert remaining */
            for (int k = 0; k < n; k++) {
                uint32_t pg = dirty_pages[k];
                uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                if (wb->slots[slot_idx].page_id != pg) continue;
                write_buf_recycle_page(wb, wb->slots[slot_idx].data);
                wb->slots[slot_idx].page_id = PAGE_SLOT_EMPTY;
                wb->slots[slot_idx].data = NULL;
            }
            uint32_t rem = wb->count - (uint32_t)n;
            PageSlot *tmp_slots = rem > 0 ? (PageSlot *)malloc(rem * sizeof(PageSlot)) : NULL;
            if (tmp_slots) {
                uint32_t ri = 0;
                for (uint32_t ci = 0; ci < wb->capacity && ri < rem; ci++) {
                    if (wb->slots[ci].page_id != PAGE_SLOT_EMPTY)
                        tmp_slots[ri++] = wb->slots[ci];
                }
                memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
                for (uint32_t ri2 = 0; ri2 < ri; ri2++) {
                    uint32_t idx = pagemap_find(wb->slots, wb->capacity, tmp_slots[ri2].page_id);
                    wb->slots[idx] = tmp_slots[ri2];
                }
                free(tmp_slots);
                wb->count = ri;
            } else {
                uint32_t cnt = 0;
                for (uint32_t ci = 0; ci < wb->capacity; ci++) {
                    if (wb->slots[ci].page_id == PAGE_SLOT_EMPTY) continue;
                    PageSlot t = wb->slots[ci];
                    wb->slots[ci].page_id = PAGE_SLOT_EMPTY;
                    wb->slots[ci].data = NULL;
                    uint32_t new_idx = pagemap_find(wb->slots, wb->capacity, t.page_id);
                    wb->slots[new_idx] = t;
                    cnt++;
                }
                wb->count = cnt;
            }
        }
    }

    return rc;
}

graveldb_status_t graveldb_checkpoint(GravelDB *db) {
    graveldb_status_t rc;
    uint16_t num_bins = dim_registry_count(&db->dim_reg);

    /* Begin checkpoint on all bins */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        rc = dimbin_checkpoint_begin(s);
        if (rc != GRAVELDB_OK) return rc;
    }

    /* Dump delta files */
    for (uint16_t i = 0; i < num_bins; i++) {
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
        uint8_t hdr_buf[WIRE_DELTA_HDR_SIZE];
        wire_encode_delta_hdr(hdr_buf, hdr.magic, hdr.version, hdr.generation,
                              hdr.dim, hdr.entry_size, hdr.bump_ptr,
                              hdr.num_entries, hdr.checksum);
        if (write(ckpt_fd, hdr_buf, WIRE_DELTA_HDR_SIZE) != WIRE_DELTA_HDR_SIZE) {
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
                    uint8_t entry_buf[WIRE_DELTA_ENTRY_SIZE];
                    wire_encode_delta_entry(entry_buf, blk_start, blk_count);
                    if (write(ckpt_fd, entry_buf, WIRE_DELTA_ENTRY_SIZE) == WIRE_DELTA_ENTRY_SIZE) {
                        if (write(ckpt_fd, buf, byte_len) == (ssize_t)byte_len) {
                            entries++;
                        }
                    }
                }
                free(buf);
            }
            idx = j;
        }
        uint8_t ne_buf[4];
        wire_put_u32(ne_buf, (uint32_t)entries);
        if (pwrite(ckpt_fd, ne_buf, 4, 32) != 4) {
            rc = GRAVELDB_ERR_IO;
        }

        fdatasync(ckpt_fd);
        close(ckpt_fd);
        free(dirty_blocks);
    }

    /* End checkpoint on all bins */
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        rc = dimbin_checkpoint_end(s);
        if (rc != GRAVELDB_OK) return rc;
    }

    db->current_epoch++;
    return GRAVELDB_OK;
}

/*
 * Incremental checkpoint: bounded-latency, one bin per step.
 * State machine: IDLE -> FLUSHING -> DUMPING -> FINISHING -> IDLE
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
            uint8_t hdr_buf[WIRE_DELTA_HDR_SIZE];
            wire_encode_delta_hdr(hdr_buf, hdr.magic, hdr.version, hdr.generation,
                                  hdr.dim, hdr.entry_size, hdr.bump_ptr,
                                  hdr.num_entries, hdr.checksum);
            if (write(p->ckpt_fd, hdr_buf, WIRE_DELTA_HDR_SIZE) != WIRE_DELTA_HDR_SIZE) {
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
                    uint8_t entry_buf[WIRE_DELTA_ENTRY_SIZE];
                    wire_encode_delta_entry(entry_buf, blk_start, blk_count);
                    if (write(p->ckpt_fd, entry_buf, WIRE_DELTA_ENTRY_SIZE) == WIRE_DELTA_ENTRY_SIZE) {
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
            /* Finalize delta file: patch num_entries at wire offset 32 */
            uint8_t ne_buf[4];
            wire_put_u32(ne_buf, (uint32_t)p->entries_written);
            if (pwrite(p->ckpt_fd, ne_buf, 4, 32) != 4) {
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

    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_bins; i++) {
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
