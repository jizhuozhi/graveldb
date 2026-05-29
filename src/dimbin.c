/*
 * GravelDB - DimBin: per-dimension storage bin + page buffer + I/O
 *
 * All page buffer allocations go through the instance's slab allocator
 * (s->allocator) for 4KB aligned pages. No fallback to posix_memalign;
 * allocator is guaranteed valid before any page alloc/free occurs.
 * No global state; supports multiple DB instances.
 */

#include "dimbin.h"
#include "io_uring_flush.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#include "overlay.h"

static inline uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

/*
 * Grow hashmap to new_capacity. Rehash all entries.
 */
static PageSlot *pagemap_grow(PageSlot *old_slots, uint32_t old_cap,
                               uint32_t new_cap) {
    PageSlot *new_slots = (PageSlot *)malloc(new_cap * sizeof(PageSlot));
    if (!new_slots) return NULL;
    for (uint32_t i = 0; i < new_cap; i++) {
        new_slots[i].page_id = PAGE_SLOT_EMPTY;
        new_slots[i].data = NULL;
    }
    if (old_slots) {
        for (uint32_t i = 0; i < old_cap; i++) {
            if (old_slots[i].page_id != PAGE_SLOT_EMPTY) {
                uint32_t idx = pagemap_find(new_slots, new_cap, old_slots[i].page_id);
                new_slots[idx] = old_slots[i];
            }
        }
        free(old_slots);
    }
    return new_slots;
}

static inline size_t align_up(size_t val, size_t align) {
    if (align == 0) return val;
    return (val + align - 1) & ~(align - 1);
}

/* Forward declarations */
graveldb_status_t dimbin_flush(DimBin *s);
static inline bool dimbin_should_flush(DimBin *s);
static void dimbin_proactive_flush(DimBin *s);
static uint8_t *write_buf_ensure_page(DimBin *s, uint32_t page_id);

graveldb_status_t dimbin_init(DimBin *s, int dim, const char *file_path,
                               size_t buffer_size, uint32_t entry_align,
                               uint32_t page_size) {
    memset(s, 0, sizeof(*s));
    s->fd = -1;
    s->key_fd = -1;

    s->dim = dim;
    s->page_size = page_size ? page_size : GRAVELDB_PAGE_SIZE_DEFAULT;
    s->entry_size = align_up(dim * sizeof(float), entry_align);
    s->entries_per_page = s->page_size / s->entry_size;
    if (s->entries_per_page == 0) {
        /* Entry larger than page: bump page_size to fit at least one entry */
        s->entries_per_page = 1;
        s->page_size = (uint32_t)s->entry_size;
    }

    graveldb_status_t rc = GRAVELDB_OK;

    s->file_path = strdup(file_path);
    if (!s->file_path) { rc = GRAVELDB_ERR_OOM; goto fail; }

    size_t path_len = strlen(file_path);
    s->key_file_path = (char *)malloc(path_len + 2);
    if (!s->key_file_path) { rc = GRAVELDB_ERR_OOM; goto fail; }
    memcpy(s->key_file_path, file_path, path_len - 4);
    memcpy(s->key_file_path + path_len - 4, ".keys", 6);

    s->fd = open(file_path, O_RDWR | O_CREAT, 0644);
    if (s->fd < 0) { rc = GRAVELDB_ERR_IO; goto fail; }

    s->key_fd = open(s->key_file_path, O_RDWR | O_CREAT, 0644);
    if (s->key_fd < 0) { rc = GRAVELDB_ERR_IO; goto fail; }

    struct stat st;
    if (fstat(s->fd, &st) < 0) { rc = GRAVELDB_ERR_IO; goto fail; }
    s->data_file_size = (size_t)st.st_size;

    if (st.st_size > 0) {
        s->bump_ptr = st.st_size / s->entry_size;
        s->total_entries = st.st_size / s->entry_size;
    } else {
        s->bump_ptr = 0;
        s->total_entries = 0;
    }
    s->total_pages = (s->total_entries + s->entries_per_page - 1) / s->entries_per_page;

    /* Track key file size */
    struct stat kst;
    if (fstat(s->key_fd, &kst) == 0) {
        s->key_file_size = (size_t)kst.st_size;
    } else {
        s->key_file_size = 0;
    }

    s->free_capacity = 1024;
    s->free_list = (uint32_t *)malloc(s->free_capacity * sizeof(uint32_t));
    if (!s->free_list) { rc = GRAVELDB_ERR_OOM; goto fail; }
    s->free_count = 0;

    uint32_t initial_pages = s->total_pages > 0 ? s->total_pages : 1024;
    DirtyTrackerConfig dt_cfg = { .estimated_pages = initial_pages };
    rc = dirty_tracker_init(&s->dirty, &dt_cfg);
    if (rc != GRAVELDB_OK) goto fail;

    size_t max_pgs = buffer_size / s->page_size;
    if (max_pgs == 0) max_pgs = 1024;

    size_t write_max = max_pgs;
    if (write_max == 0) write_max = 64;

    /* Initialize WriteBuffer (hashmap) */
    s->write_buf.max_pages = write_max;
    s->write_buf.count = 0;
    s->write_buf.write_counter = 0;
    s->write_buf.rng_state = 67890;  /* deterministic seed */
    uint32_t wb_cap = 64;  /* initial capacity, will grow as needed */
    while (wb_cap < write_max * 2) wb_cap *= 2;
    s->write_buf.capacity = wb_cap;
    s->write_buf.slots = (PageSlot *)malloc(wb_cap * sizeof(PageSlot));
    if (!s->write_buf.slots) { rc = GRAVELDB_ERR_OOM; goto fail; }
    for (uint32_t i = 0; i < wb_cap; i++) {
        s->write_buf.slots[i].page_id = PAGE_SLOT_EMPTY;
        s->write_buf.slots[i].data = NULL;
    }

    s->flush_dirty_buf = (uint32_t *)malloc(GRAVELDB_MAX_FLUSH_BATCH * sizeof(uint32_t));
    if (!s->flush_dirty_buf) { rc = GRAVELDB_ERR_OOM; goto fail; }

    s->in_checkpoint = false;

    return GRAVELDB_OK;

fail:
    if (s->fd >= 0) close(s->fd);
    if (s->key_fd >= 0) close(s->key_fd);
    free(s->file_path);
    free(s->key_file_path);
    free(s->free_list);
    dirty_tracker_destroy(&s->dirty);
    free(s->write_buf.slots);
    free(s->flush_dirty_buf);
    memset(s, 0, sizeof(*s));
    s->fd = -1;
    s->key_fd = -1;
    return rc;
}

void dimbin_destroy(DimBin *s) {
    if (s->fd >= 0) close(s->fd);
    if (s->key_fd >= 0) close(s->key_fd);
    free(s->file_path);
    free(s->key_file_path);
    free(s->free_list);
    dirty_tracker_destroy(&s->dirty);

    /* Free WriteBuffer pages */
    if (s->write_buf.slots) {
        for (uint32_t i = 0; i < s->write_buf.capacity; i++) {
            if (s->write_buf.slots[i].page_id != PAGE_SLOT_EMPTY && s->write_buf.slots[i].data) {
                slab_free_aligned(s->allocator, s->write_buf.slots[i].data, s->page_size);
            }
        }
        free(s->write_buf.slots);
    }

    free(s->flush_dirty_buf);

    overlay_destroy(&s->overlay);
    memset(s, 0, sizeof(*s));
    s->fd = -1;
    s->key_fd = -1;
}

/* Pre-reserve file space for `count` new bump-alloc entries.
 * Call once before a batch loop so that individual dimbin_alloc_entry
 * calls never hit the ftruncate path. Only grows; never shrinks. */
void dimbin_reserve(DimBin *s, uint32_t count) {
    uint64_t future_bump = s->bump_ptr + count;
    size_t required_bytes = (size_t)future_bump * s->entry_size;
    if (s->data_file_size < required_bytes) {
        size_t new_size = s->data_file_size > 0 ? s->data_file_size * 2 : (size_t)s->page_size * 256;
        while (new_size < required_bytes) new_size *= 2;
        if (ftruncate(s->fd, new_size) == 0) {
            s->data_file_size = new_size;
        } else if (ftruncate(s->fd, required_bytes) == 0) {
            s->data_file_size = required_bytes;
        }
    }
    size_t key_required = (size_t)future_bump * sizeof(uint64_t);
    if (s->key_file_size < key_required) {
        size_t new_key_size = s->key_file_size > 0 ? s->key_file_size * 2 : 8192;
        while (new_key_size < key_required) new_key_size *= 2;
        if (ftruncate(s->key_fd, new_key_size) == 0) {
            s->key_file_size = new_key_size;
        }
    }
}

uint32_t dimbin_alloc_entry(DimBin *s) {
    if (s->free_count > 0) {
        return s->free_list[--s->free_count];
    }
    uint32_t slot = (uint32_t)(s->bump_ptr++);

    /* Grow data file if needed (no fstat -- use tracked size).
     * If dimbin_reserve() was called beforehand, this never triggers. */
    size_t required_bytes = (size_t)(slot + 1) * s->entry_size;
    if (__builtin_expect(s->data_file_size < required_bytes, 0)) {
        size_t new_size = s->data_file_size > 0 ? s->data_file_size * 2 : (size_t)s->page_size * 256;
        if (new_size < required_bytes) new_size = required_bytes * 2;
        if (ftruncate(s->fd, new_size) == 0) {
            s->data_file_size = new_size;
        } else if (ftruncate(s->fd, required_bytes) == 0) {
            s->data_file_size = required_bytes;
        }
    }

    /* Grow key file if needed */
    size_t key_required = (size_t)(slot + 1) * sizeof(uint64_t);
    if (__builtin_expect(s->key_file_size < key_required, 0)) {
        size_t new_key_size = s->key_file_size > 0 ? s->key_file_size * 2 : 8192;
        if (new_key_size < key_required) new_key_size = key_required * 2;
        if (ftruncate(s->key_fd, new_key_size) == 0) {
            s->key_file_size = new_key_size;
        }
    }

    if (slot >= s->total_entries) {
        s->total_entries = slot + 1;
        s->total_pages = (s->total_entries + s->entries_per_page - 1) / s->entries_per_page;
    }
    return slot;
}

/*
 * Key write: immediate pwrite to key file.
 * No buffering -- ensures crash consistency for add/remove operations.
 * Returns GRAVELDB_OK on success, GRAVELDB_ERR_IO on failure.
 * Caller must NOT proceed with value write if key write fails,
 * otherwise the slot becomes a phantom (occupied but unrecoverable after crash).
 */
graveldb_status_t dimbin_put_key(DimBin *s, uint32_t entry_idx, uint64_t feat_id) {
    off_t offset = (off_t)entry_idx * sizeof(uint64_t);
    ssize_t wr = pwrite(s->key_fd, &feat_id, sizeof(uint64_t), offset);
    if (wr != sizeof(uint64_t)) {
        s->io_errors++;
        return GRAVELDB_ERR_IO;
    }
    return GRAVELDB_OK;
}

/*
 * Batched key write with user-space page-level coalescing.
 *
 * Strategy:
 *   - Key file is laid out as uint64_t[entry_idx], i.e. 512 keys per 4KB page.
 *   - We bucket entries by key-page. For each page:
 *     * If density >= threshold (e.g. >=4 keys in same page), do read-modify-write:
 *       pread the whole 4KB page, patch all keys in-memory, pwrite it back.
 *       Cost: 2 syscalls regardless of how many keys land in that page.
 *     * Otherwise (sparse page), issue individual 8-byte pwrite per key.
 *       Cost: 1 syscall per key but avoids reading a full page for 1-3 patches.
 *
 * The threshold balances: 1 pread + 1 pwrite (2 syscalls for any density)
 * vs N individual pwrites (N syscalls). Break-even is N=2, but accounting for
 * the larger transfer size of a full-page read we use threshold=4 to also
 * amortize cache-line / SSD write-amplification effects.
 */

#define KEY_PAGE_SIZE       4096
#define KEYS_PER_PAGE       (KEY_PAGE_SIZE / sizeof(uint64_t))  /* 512 */
/* KEY_COALESCE_THRESH defined in dimbin.h */

static inline void flush_key_page(DimBin *s, uint8_t *page_buf,
                                   uint32_t page_id,
                                   const uint32_t *slots, const uint64_t *vals,
                                   int count) {
    if (count >= KEY_COALESCE_THRESH) {
        /* Read-modify-write: load page, patch slots, write back */
        off_t pg_off = (off_t)page_id * KEY_PAGE_SIZE;
        if ((size_t)pg_off < s->key_file_size) {
            ssize_t rd = pread(s->key_fd, page_buf, KEY_PAGE_SIZE, pg_off);
            if (rd < KEY_PAGE_SIZE) {
                if (rd < 0) rd = 0;
                memset(page_buf + rd, 0, KEY_PAGE_SIZE - (size_t)rd);
            }
        } else {
            memset(page_buf, 0, KEY_PAGE_SIZE);
        }
        uint64_t *kp = (uint64_t *)page_buf;
        for (int k = 0; k < count; k++) {
            kp[slots[k]] = vals[k];
        }
        if (pwrite(s->key_fd, page_buf, KEY_PAGE_SIZE, pg_off) != KEY_PAGE_SIZE) {
            s->io_errors++;
        }
    } else {
        /* Sparse: individual 8-byte writes */
        for (int k = 0; k < count; k++) {
            off_t off = (off_t)(page_id * KEYS_PER_PAGE + slots[k]) * sizeof(uint64_t);
            if (pwrite(s->key_fd, &vals[k], sizeof(uint64_t), off) != sizeof(uint64_t)) {
                s->io_errors++;
            }
        }
    }
}

void dimbin_put_keys_batch(DimBin *s, const KeyWriteEntry *entries, int count) {
    if (count == 0) return;

    /* For very small batches, just do individual writes */
    if (count < KEY_COALESCE_THRESH) {
        for (int i = 0; i < count; i++) {
            dimbin_put_key(s, entries[i].entry_idx, entries[i].feat_id);
        }
        return;
    }

    /*
     * 1-pass peephole: scan entries (which are roughly ordered due to bump alloc),
     * accumulate into a page buffer. When we see a page boundary transition,
     * flush the previous page's accumulated writes.
     *
     * We use a small stack buffer to collect keys for the "current" page.
     */
    uint8_t page_buf[KEY_PAGE_SIZE] __attribute__((aligned(8)));
    uint32_t cur_page = entries[0].entry_idx / KEYS_PER_PAGE;
    uint32_t page_keys[KEYS_PER_PAGE]; /* slot-within-page for each accumulated key */
    uint64_t page_vals[KEYS_PER_PAGE]; /* feat_id for each accumulated key */
    int page_count = 0;

    for (int i = 0; i < count; i++) {
        uint32_t pg = entries[i].entry_idx / KEYS_PER_PAGE;
        uint32_t slot = entries[i].entry_idx % KEYS_PER_PAGE;

        if (pg != cur_page) {
            flush_key_page(s, page_buf, cur_page, page_keys, page_vals, page_count);
            page_count = 0;
            cur_page = pg;
        }

        page_keys[page_count] = slot;
        page_vals[page_count] = entries[i].feat_id;
        page_count++;
    }

    /* Flush remaining */
    if (page_count > 0) {
        flush_key_page(s, page_buf, cur_page, page_keys, page_vals, page_count);
    }
}

void dimbin_free_entry(DimBin *s, uint32_t entry_idx) {
    if (s->free_count >= s->free_capacity) {
        uint32_t new_cap = s->free_capacity * 2;
        uint32_t *tmp = (uint32_t *)realloc(s->free_list, new_cap * sizeof(uint32_t));
        if (!tmp) return;
        s->free_list = tmp;
        s->free_capacity = new_cap;
    }
    s->free_list[s->free_count++] = entry_idx;

    /* Best-effort: zero the key. Failure is tolerable because the slot is on
     * free_list and will get a new key on next reuse via dimbin_put_key. */
    (void)dimbin_put_key(s, entry_idx, 0);
}

/*
 * WriteBuffer: ensure a page exists in the write buffer.
 * If the page doesn't exist yet, allocate it and optionally load from disk
 * to support partial-page writes.
 * Returns pointer to page buffer, or NULL on failure.
 */
static uint8_t *write_buf_ensure_page(DimBin *s, uint32_t page_id) {
    WriteBuffer *wb = &s->write_buf;

    /* Check if already in hashmap */
    uint32_t idx = pagemap_find(wb->slots, wb->capacity, page_id);
    if (wb->slots[idx].page_id == page_id) {
        return wb->slots[idx].data;
    }

    /* Need to insert -- check if we need to flush first */
    if (wb->count >= wb->max_pages) {
        dimbin_flush(s);
    }

    /* Grow hashmap if load factor too high (>70%) */
    if (wb->count * 10 >= wb->capacity * 7) {
        uint32_t new_cap = wb->capacity * 2;
        PageSlot *new_slots = pagemap_grow(wb->slots, wb->capacity, new_cap);
        if (!new_slots) return NULL;
        wb->slots = new_slots;
        wb->capacity = new_cap;
        /* Re-find insertion point after rehash */
        idx = pagemap_find(wb->slots, wb->capacity, page_id);
    }

    /* Allocate page */
    void *mem = slab_alloc_aligned(s->allocator, s->page_size, 4096);
    if (!mem) return NULL;

    /* Load existing data from disk for partial-page writes.
     * Optimization: if the page is beyond current file size (freshly bump-allocated),
     * skip pread entirely -- just zero-fill. This eliminates syscalls for the common
     * batch-insert path where all pages are new. */
    off_t page_offset = (off_t)page_id * s->page_size;
    if ((size_t)page_offset < s->data_file_size) {
        ssize_t rd = pread(s->fd, mem, s->page_size, page_offset);
        if (rd < (ssize_t)s->page_size) {
            if (rd < 0) rd = 0;
            memset((uint8_t *)mem + rd, 0, s->page_size - (size_t)rd);
        }
    } else {
        memset(mem, 0, s->page_size);
    }

    wb->slots[idx].page_id = page_id;
    wb->slots[idx].data = (uint8_t *)mem;
    wb->count++;

    return (uint8_t *)mem;
}

graveldb_status_t dimbin_get(DimBin *s, uint32_t entry_id, float *buf) {
    if (s->in_checkpoint) {
        if (overlay_contains(&s->overlay, entry_id)) {
            overlay_get(&s->overlay, entry_id, buf, s->dim);
            return GRAVELDB_OK;
        }
    }

    uint32_t page_id = entry_id / s->entries_per_page;
    uint32_t offset_in_page = (entry_id % s->entries_per_page) * s->entry_size;

    /* 1. Forwarding: check write buffer first (read-your-writes) */
    WriteBuffer *wb = &s->write_buf;
    uint32_t wb_idx = pagemap_find(wb->slots, wb->capacity, page_id);
    if (wb->slots[wb_idx].page_id == page_id) {
        memcpy(buf, wb->slots[wb_idx].data + offset_in_page, s->entry_size);
        return GRAVELDB_OK;
    }

    /* 2. Direct pread fallback */
    off_t offset = (off_t)entry_id * s->entry_size;
    ssize_t rd = pread(s->fd, buf, s->entry_size, offset);
    if (rd < 0) return GRAVELDB_ERR_IO;
    return GRAVELDB_OK;
}

graveldb_status_t dimbin_put(DimBin *s, uint32_t entry_id, const float *data) {
    if (s->in_checkpoint) {
        return overlay_put(&s->overlay, entry_id, data, s->dim);
    }

    uint32_t page_id = entry_id / s->entries_per_page;
    uint32_t offset_in_page = (entry_id % s->entries_per_page) * s->entry_size;

    if (page_id >= s->dirty.capacity) {
        uint32_t new_cap = page_id < 1024 ? 1024 : (page_id + 1) * 2;
        dirty_tracker_resize(&s->dirty, new_cap);
    }

    /* Write into WriteBuffer */
    uint8_t *page = write_buf_ensure_page(s, page_id);
    if (page) {
        memcpy(page + offset_in_page, data, s->entry_size);
        dirty_tracker_mark(&s->dirty, page_id);

        /* Water-level check */
        if (dimbin_should_flush(s)) {
            dimbin_flush(s);
        } else {
            /* Proactive flush: amortize I/O over time */
            s->write_buf.write_counter++;
            if (s->write_buf.write_counter >= WB_PROACTIVE_FLUSH_INTERVAL) {
                s->write_buf.write_counter = 0;
                dimbin_proactive_flush(s);
            }
        }
        return GRAVELDB_OK;
    }

    /* Fallback: direct pwrite */
    off_t offset = (off_t)entry_id * s->entry_size;
    ssize_t wr = pwrite(s->fd, data, s->entry_size, offset);
    if (wr < 0) return GRAVELDB_ERR_IO;
    dirty_tracker_mark(&s->dirty, page_id);
    return GRAVELDB_OK;
}

/*
 * Batch embedding write with page-level coalescing.
 *
 * Strategy:
 *   1. Sort entries by page_id -> entries on same page become adjacent
 *   2. For each unique page: call write_buf_ensure_page ONCE
 *   3. memcpy all entries for that page in a tight loop (L1 cache hot)
 *   4. NO water-level check per entry -- defer to END of entire batch
 *   5. Single water-level check + optional flush after all data is buffered
 *
 * This eliminates:
 *   - Redundant pagemap_find probes (one per page instead of per entry)
 *   - Mid-batch flush interruptions that break page locality
 *   - Per-entry branch overhead for flush/proactive-flush logic
 *
 * For typical batch of 1K entries with dim=128 (entry_size=512B, 8 entries/page),
 * this reduces ensure_page calls from 1000 to ~125 and avoids any mid-batch I/O.
 */
graveldb_status_t dimbin_put_batch(DimBin *s, const EmbWriteEntry *entries, int count) {
    if (count <= 0) return GRAVELDB_OK;

    /* Checkpoint redirect: all writes go to overlay */
    if (s->in_checkpoint) {
        for (int i = 0; i < count; i++) {
            graveldb_status_t rc = overlay_put(&s->overlay, entries[i].entry_id,
                                               entries[i].data, s->dim);
            if (rc != GRAVELDB_OK) return rc;
        }
        return GRAVELDB_OK;
    }

    /* Build sorted index by page_id for coalescing */
    uint32_t stack_order[256];
    uint32_t *order = (count <= 256) ? stack_order :
                      (uint32_t *)malloc((size_t)count * sizeof(uint32_t));
    if (!order) return GRAVELDB_ERR_OOM;

    for (int i = 0; i < count; i++) order[i] = (uint32_t)i;

    /* Sort by page_id (insertion sort -- fast for mostly-sorted bump alloc) */
    uint32_t epp = (uint32_t)s->entries_per_page;
    for (int i = 1; i < count; i++) {
        uint32_t key = order[i];
        uint32_t key_pg = entries[key].entry_id / epp;
        int j = i - 1;
        while (j >= 0 && (entries[order[j]].entry_id / epp) > key_pg) {
            order[j + 1] = order[j];
            j--;
        }
        order[j + 1] = key;
    }

    graveldb_status_t rc = GRAVELDB_OK;
    int gi = 0;

    while (gi < count) {
        uint32_t page_id = entries[order[gi]].entry_id / epp;

        /* Find extent of entries on this page */
        int gj = gi + 1;
        while (gj < count && (entries[order[gj]].entry_id / epp) == page_id) {
            gj++;
        }

        /* Ensure dirty tracker capacity */
        if (page_id >= s->dirty.capacity) {
            uint32_t new_cap = page_id < 1024 ? 1024 : (page_id + 1) * 2;
            dirty_tracker_resize(&s->dirty, new_cap);
        }

        /* Get or allocate the page buffer ONCE for all entries on this page */
        uint8_t *page = write_buf_ensure_page(s, page_id);
        if (page) {
            /* Batch memcpy all entries into this page */
            for (int k = gi; k < gj; k++) {
                uint32_t eid = entries[order[k]].entry_id;
                uint32_t offset_in_page = (eid % epp) * (uint32_t)s->entry_size;
                memcpy(page + offset_in_page, entries[order[k]].data, s->entry_size);
            }
            dirty_tracker_mark(&s->dirty, page_id);
        } else {
            /* Fallback: direct pwrite per entry */
            for (int k = gi; k < gj; k++) {
                uint32_t eid = entries[order[k]].entry_id;
                off_t offset = (off_t)eid * (off_t)s->entry_size;
                ssize_t wr = pwrite(s->fd, entries[order[k]].data, s->entry_size, offset);
                if (wr < 0) rc = GRAVELDB_ERR_IO;
                dirty_tracker_mark(&s->dirty, page_id);
            }
        }

        gi = gj;
    }

    /* Deferred water-level check: only AFTER entire batch is buffered */
    if (dimbin_should_flush(s)) {
        dimbin_flush(s);
    }

    if (order != stack_order) free(order);
    return rc;
}

/*
 * Peephole gap tolerance: allow up to PEEPHOLE_GAP_MAX pages of gap between
 * two dirty runs. If gap <= threshold, merge them into one large sequential
 * write (the gap pages are read from buffer or zeroed). This trades a small
 * amount of extra write bandwidth for dramatically better I/O continuity.
 *
 * Rationale: NVMe optimal I/O size is 64-128KB. A 1-block gap (4KB) costs 4KB
 * extra write but saves one syscall boundary + one queue head re-seek. The
 * amortized benefit is massive when dirty pages are semi-contiguous (which is
 * the common case for bump-pointer allocation).
 */
#define PEEPHOLE_GAP_MAX   4   /* merge runs separated by up to 4 pages */

/*
 * Water-level threshold: when write buffer dirty count exceeds this fraction
 * of max_pages, the buffer should be flushed eagerly.
 */
#define FLUSH_WATERMARK_NUMER  3
#define FLUSH_WATERMARK_DENOM  4  /* flush when dirty >= max_pages * 3/4 */

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    return (va > vb) - (va < vb);
}

/*
 * Flush WriteBuffer: collect all pages from hashmap -> sort by page_id
 * -> peephole merge -> pwrite large contiguous runs. After flush, all freed.
 *
 * Keys are no longer buffered (immediate pwrite on put_key), so no key flush.
 */
graveldb_status_t dimbin_flush(DimBin *s) {
    WriteBuffer *wb = &s->write_buf;

    if (wb->count == 0) return GRAVELDB_OK;

    /* Collect page_ids from hashmap into flush buffer */
    uint32_t *dirty_pages = s->flush_dirty_buf;
    int n = 0;

    for (uint32_t i = 0; i < wb->capacity && n < GRAVELDB_MAX_FLUSH_BATCH; i++) {
        if (wb->slots[i].page_id != PAGE_SLOT_EMPTY) {
            dirty_pages[n++] = wb->slots[i].page_id;
        }
    }

    if (n == 0) return GRAVELDB_OK;

    /* Sort for peephole merge */
    qsort(dirty_pages, n, sizeof(uint32_t), cmp_u32);

    /* Peephole merge + io_uring batch submit */
    uring_io_ctx_t io_ctx;
    int use_uring = (uring_io_init(&io_ctx) == 0);
    graveldb_status_t rc = GRAVELDB_OK;

    int i = 0;
    while (i < n) {
        int j = i + 1;

        /* Extend run with peephole gap tolerance */
        while (j < n && dirty_pages[j] <= dirty_pages[j - 1] + PEEPHOLE_GAP_MAX + 1) {
            j++;
        }

        /* Submit each dirty page in this run */
        for (int k = i; k < j; k++) {
            uint32_t pg = dirty_pages[k];
            uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
            if (wb->slots[slot_idx].page_id != pg) continue;

            if (use_uring) {
                if (uring_io_submit_write(&io_ctx, s->fd,
                                          wb->slots[slot_idx].data,
                                          s->page_size,
                                          (off_t)pg * s->page_size) < 0) {
                    rc = GRAVELDB_ERR_IO;
                }
            } else {
                ssize_t wr = pwrite(s->fd, wb->slots[slot_idx].data, s->page_size,
                                    (off_t)pg * s->page_size);
                if (wr != (ssize_t)s->page_size) rc = GRAVELDB_ERR_IO;
            }
        }

        wb->flush_bytes += (size_t)(j - i) * s->page_size;
        i = j;
    }

    /* Submit fsync + wait for all I/O to complete */
    if (use_uring) {
        uring_io_submit_fsyncs(&io_ctx);
        int errors = uring_io_wait(&io_ctx);
        if (errors > 0) rc = GRAVELDB_ERR_IO;
    }
    uring_io_destroy(&io_ctx);

    /*
     * Free pages AFTER all I/O is confirmed complete.
     * Critical for io_uring: buffers must remain valid until writes finish.
     */
    for (int k = 0; k < n; k++) {
        uint32_t pg = dirty_pages[k];
        uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
        if (wb->slots[slot_idx].page_id != pg) continue;
        slab_free_aligned(s->allocator, wb->slots[slot_idx].data, s->page_size);
        pagemap_remove(wb->slots, wb->capacity, slot_idx);
        wb->count--;
    }

    return rc;
}

/*
 * Check flush watermark on write buffer.
 */
static inline bool dimbin_should_flush(DimBin *s) {
    uint32_t threshold = (uint32_t)(s->write_buf.max_pages * FLUSH_WATERMARK_NUMER / FLUSH_WATERMARK_DENOM);
    return s->write_buf.count >= threshold;
}

/*
 * Proactive flush: randomly sample a few dirty pages from write buffer and
 * flush them to disk. This spreads I/O evenly across time instead of
 * concentrating it all at checkpoint or buffer-full moments.
 *
 * Strategy: random pick (not min-heap) -- O(1) per sample, zero overhead on
 * the write hot path. Trades perfect age-ordering for simplicity and no
 * structural overhead. Since all buffered pages are equally "pending flush",
 * random eviction is fair enough; the key goal is reducing the residual count
 * before checkpoint begins.
 */
static void dimbin_proactive_flush(DimBin *s) {
    WriteBuffer *wb = &s->write_buf;

    if (wb->count == 0) return;

    uint32_t to_flush = WB_PROACTIVE_FLUSH_BATCH;
    if (to_flush > wb->count) to_flush = wb->count;

    uint32_t mask = wb->capacity - 1;
    uint32_t flushed = 0;
    uint32_t max_attempts = wb->capacity;  /* prevent infinite loop */
    uint32_t attempts = 0;

    while (flushed < to_flush && attempts < max_attempts) {
        uint32_t idx = xorshift32(&wb->rng_state) & mask;
        attempts++;

        if (wb->slots[idx].page_id == PAGE_SLOT_EMPTY) continue;

        uint32_t page_id = wb->slots[idx].page_id;
        uint8_t *data = wb->slots[idx].data;

        /* Write page to disk */
        ssize_t wr = pwrite(s->fd, data, s->page_size, (off_t)page_id * s->page_size);
        if (wr == (ssize_t)s->page_size) {
            /* Success: free page and remove from hashmap */
            slab_free_aligned(s->allocator, data, s->page_size);
            pagemap_remove(wb->slots, wb->capacity, idx);
            wb->count--;
            wb->flush_bytes += s->page_size;
            flushed++;
        }
    }

    /* Single fdatasync to cover all proactively flushed pages */
    if (flushed > 0) {
        fdatasync(s->fd);
    }
}

graveldb_status_t dimbin_checkpoint_begin(DimBin *s) {
    /* Flush write buffer before freezing card table bitmap. */
    dimbin_flush(s);

    dirty_tracker_swap(&s->dirty);

    s->overlay.allocator = s->allocator;
    overlay_init(&s->overlay, s->dim);
    s->in_checkpoint = true;

    return GRAVELDB_OK;
}

graveldb_status_t dimbin_checkpoint_end(DimBin *s) {
    s->in_checkpoint = false;

    /* Replay overlay entries back into main storage.
     * OA layout: scan both tables (new + old if mid-rehash), skip empty slots. */
    for (uint32_t i = 0; i < s->overlay.capacity; i++) {
        if (s->overlay.slots[i].entry_id != OVERLAY_EMPTY) {
            dimbin_put(s, s->overlay.slots[i].entry_id, s->overlay.slots[i].data);
        }
    }
    if (s->overlay.old_slots) {
        for (uint32_t i = 0; i < s->overlay.old_capacity; i++) {
            if (s->overlay.old_slots[i].entry_id != OVERLAY_EMPTY) {
                dimbin_put(s, s->overlay.old_slots[i].entry_id, s->overlay.old_slots[i].data);
            }
        }
    }

    for (uint32_t i = 0; i < s->overlay.tomb_count; i++) {
        dimbin_free_entry(s, s->overlay.tombstones[i]);
    }

    overlay_destroy(&s->overlay);

    return GRAVELDB_OK;
}
