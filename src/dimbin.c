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
#include <sys/uio.h>

#ifdef __linux__
#include <linux/falloc.h>
#endif

/*
 * Platform-aware file space pre-allocation.
 * On Linux: uses fallocate() which allocates disk blocks immediately,
 * avoiding later block-allocation latency during pwrite.
 * Elsewhere: falls back to ftruncate (sparse file, blocks allocated on write).
 */
static inline int graveldb_preallocate(int fd, size_t new_size) {
#ifdef __linux__
    int ret = fallocate(fd, 0, 0, (off_t)new_size);
    if (ret == 0) return 0;
    /* EOPNOTSUPP on some filesystems (e.g. tmpfs): fall through to ftruncate */
    if (errno != EOPNOTSUPP && errno != ENOSYS) return -1;
#endif
    return ftruncate(fd, (off_t)new_size);
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
graveldb_status_t dimbin_flush(DimBin *s, bool sync);
static inline bool dimbin_should_flush(DimBin *s);
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

    /* Track key file size */
    struct stat kst;
    if (fstat(s->key_fd, &kst) == 0) {
        s->key_file_size = (size_t)kst.st_size;
    } else {
        s->key_file_size = 0;
    }

    /* Derive bump_ptr from key file: each slot is one uint64_t.
     * The key file is the authoritative source for slot count because
     * the data file may have been pre-extended (ftruncate for growth)
     * and its size doesn't reflect actual allocation count. */
    uint64_t key_slots = s->key_file_size / sizeof(uint64_t);
    uint64_t data_slots = (st.st_size > 0) ? (uint64_t)st.st_size / s->entry_size : 0;
    /* Use the minimum of key-derived and data-derived slot count.
     * This handles the case where data was pre-extended but keys weren't. */
    s->bump_ptr = (key_slots < data_slots) ? key_slots : data_slots;
    s->total_entries = s->bump_ptr;
    s->total_pages = (s->total_entries + s->entries_per_page - 1) / s->entries_per_page;

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

    /* Initialize KeyBTree with a fixed memory budget.
     * Budget = write_max pages × entries_per_page × ~40 bytes per entry overhead.
     * Capped between 256KB and 16MB. */
    {
        size_t budget = (size_t)write_max * s->entries_per_page * 40;
        if (budget < (256u << 10)) budget = (256u << 10);
        if (budget > (16u << 20)) budget = (16u << 20);
        kbt_init(&s->key_buf, budget);
    }

    s->in_checkpoint = false;
    s->flush_needed = false;

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
    kbt_destroy(&s->key_buf);
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

    /* Free page pool (recycled buffers not currently in use) */
    for (uint32_t i = 0; i < s->write_buf.pool_count; i++) {
        slab_free_aligned(s->allocator, s->write_buf.page_pool[i], s->page_size);
    }
    free(s->write_buf.page_pool);

    free(s->flush_dirty_buf);
    kbt_destroy(&s->key_buf);

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
        if (graveldb_preallocate(s->fd, new_size) == 0) {
            s->data_file_size = new_size;
        } else if (graveldb_preallocate(s->fd, required_bytes) == 0) {
            s->data_file_size = required_bytes;
        }
    }
    size_t key_required = (size_t)future_bump * sizeof(uint64_t);
    if (s->key_file_size < key_required) {
        size_t new_key_size = s->key_file_size > 0 ? s->key_file_size * 2 : 8192;
        while (new_key_size < key_required) new_key_size *= 2;
        if (graveldb_preallocate(s->key_fd, new_key_size) == 0) {
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
        if (graveldb_preallocate(s->fd, new_size) == 0) {
            s->data_file_size = new_size;
        } else if (graveldb_preallocate(s->fd, required_bytes) == 0) {
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
 * Key write: immediate pwrite to key file (no buffering).
 */
graveldb_status_t dimbin_put_key(DimBin *s, uint32_t entry_idx, uint64_t feat_id) {
    off_t offset = (off_t)entry_idx * sizeof(uint64_t);
    ssize_t wr = pwrite(s->key_fd, &feat_id, 8, offset);
    if (wr != 8) {
        s->io_errors++;
        return GRAVELDB_ERR_IO;
    }
    return GRAVELDB_OK;
}

/*
 * Buffer a key write in the B-tree (sorted + dedup).
 * When arena is nearly full, trigger a flush before inserting.
 */
void dimbin_buf_key(DimBin *s, uint32_t entry_idx, uint64_t feat_id) {
    KeyBTree *kb = &s->key_buf;

    /* Arena nearly full: flush before inserting */
    if (kbt_full(kb)) {
        dimbin_flush_keys(s);
    }

    kbt_insert(kb, entry_idx, feat_id);
}

/*
 * Flush buffered keys to disk with batched coalescing.
 * fdatasync is deferred to the caller (only needed at checkpoint/close).
 */
graveldb_status_t dimbin_flush_keys(DimBin *s) {
    KeyBTree *kb = &s->key_buf;
    if (kb->count == 0) return GRAVELDB_OK;

    /* Collect entries from B-tree in sorted order (no extra sort needed) */
    KeyWriteEntry *batch = (KeyWriteEntry *)malloc(kb->count * sizeof(KeyWriteEntry));
    if (!batch) return GRAVELDB_ERR_OOM;

    uint32_t n = kbt_collect(kb, batch);

    /* Batch write with page coalescing (entries already sorted by entry_idx) */
    dimbin_put_keys_batch(s, batch, (int)n);
    free(batch);

    kbt_clear(kb);
    return GRAVELDB_OK;
}

/*
 * Batched key write with page-level coalescing.
 * Dense pages (>=KEY_COALESCE_THRESH keys): read-modify-write full 4KB page.
 * Sparse pages: individual 8-byte pwrite per key.
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
        /* Data files are native byte order — patch directly */
        uint64_t *kp = (uint64_t *)page_buf;
        for (int k = 0; k < count; k++) {
            kp[slots[k]] = vals[k];
        }
        /* Write back native format directly */
        if (pwrite(s->key_fd, page_buf, KEY_PAGE_SIZE, pg_off) != KEY_PAGE_SIZE) {
            s->io_errors++;
        }
    } else {
        /* Sparse: individual 8-byte writes (native byte order) */
        for (int k = 0; k < count; k++) {
            off_t off = (off_t)(page_id * KEYS_PER_PAGE + slots[k]) * sizeof(uint64_t);
            if (pwrite(s->key_fd, &vals[k], 8, off) != 8) {
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

    /* Buffer key=0 write (goes through key buffer so ordering is preserved) */
    dimbin_buf_key(s, entry_idx, 0);
}

/*
 * Ensure a page exists in the write buffer; load from disk if needed.
 */
static uint8_t *write_buf_ensure_page(DimBin *s, uint32_t page_id) {
    WriteBuffer *wb = &s->write_buf;

    uint32_t idx = pagemap_find(wb->slots, wb->capacity, page_id);
    if (wb->slots[idx].page_id == page_id) {
        return wb->slots[idx].data;
    }

    /* Need to insert -- flush if at hard limit */
    if (wb->count >= wb->max_pages) {
        dimbin_flush(s, false);
        idx = pagemap_find(wb->slots, wb->capacity, page_id);
    }

    /* Grow hashmap if load factor too high (>70%) */
    if (wb->count * 10 >= wb->capacity * 7) {
        uint32_t new_cap = wb->capacity * 2;
        PageSlot *new_slots = pagemap_grow(wb->slots, wb->capacity, new_cap);
        if (!new_slots) return NULL;
        wb->slots = new_slots;
        wb->capacity = new_cap;
        idx = pagemap_find(wb->slots, wb->capacity, page_id);
    }

    /* Allocate page: prefer recycled buffer from pool */
    void *mem = write_buf_acquire_page(wb, s->allocator, s->page_size);
    if (!mem) return NULL;

    /* Load existing data from disk only if this page contains previously
     * committed entries. Pages beyond bump_ptr are virgin (even if the file
     * was preallocated/ftruncated larger), so memset is sufficient. */
    uint32_t first_committed_page = (uint32_t)(s->bump_ptr / (uint32_t)s->entries_per_page);
    off_t page_offset = (off_t)page_id * s->page_size;
    if (page_id < first_committed_page && (size_t)page_offset < s->data_file_size) {
        ssize_t rd = pread(s->fd, mem, s->page_size, page_offset);
        if (rd < (ssize_t)s->page_size) {
            if (rd < 0) rd = 0;
            memset((uint8_t *)mem + rd, 0, s->page_size - (size_t)rd);
        }
        /* Data files are native byte order — no conversion needed */
    } else {
        memset(mem, 0, s->page_size);
    }

    wb->slots[idx].page_id = page_id;
    wb->slots[idx].data = (uint8_t *)mem;
    wb->count++;

    return (uint8_t *)mem;
}

/*
 * Batch embedding write: sort by page, coalesce, defer flush to end.
 */
graveldb_status_t dimbin_put_batch(DimBin *s, const EmbWriteEntry *entries, int count) {
    if (count <= 0) return GRAVELDB_OK;

    /* During checkpoint: writes go to overlay with backpressure */
    if (s->in_checkpoint) {
        for (int i = 0; i < count; i++) {
            if (overlay_full(&s->overlay)) {
                return GRAVELDB_ERR_BUSY;
            }
            graveldb_status_t rc = overlay_put(&s->overlay, entries[i].entry_id,
                                               entries[i].data, s->dim);
            if (rc != GRAVELDB_OK) return rc;
        }
        return GRAVELDB_OK;
    }

    /* Sort entries by page_id for coalescing */
    uint32_t stack_order[256];
    uint32_t *order = (count <= 256) ? stack_order :
                      (uint32_t *)malloc((size_t)count * sizeof(uint32_t));
    if (!order) return GRAVELDB_ERR_OOM;

    for (int i = 0; i < count; i++) order[i] = (uint32_t)i;

    /* Insertion sort (fast for mostly-sorted bump alloc order) */
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
            /* Fallback: direct pwrite per entry (native byte order) */
            uint8_t tmp_entry[4096]; /* stack buffer, entry_size <= page_size */
            for (int k = gi; k < gj; k++) {
                uint32_t eid = entries[order[k]].entry_id;
                off_t offset = (off_t)eid * (off_t)s->entry_size;
                memcpy(tmp_entry, entries[order[k]].data, s->entry_size);
                ssize_t wr = pwrite(s->fd, tmp_entry, s->entry_size, offset);
                if (wr < 0) rc = GRAVELDB_ERR_IO;
                dirty_tracker_mark(&s->dirty, page_id);
            }
        }

        gi = gj;
    }

    /* Deferred water-level check: set flag instead of blocking */
    if (dimbin_should_flush(s)) {
        s->flush_needed = true;
    }

    if (order != stack_order) free(order);
    return rc;
}

/* Peephole gap: merge dirty runs separated by up to 4 pages */
#define PEEPHOLE_GAP_MAX   4

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
 * Flush: keys first, then value pages via io_uring.
 * When sync=true, fdatasync key_fd and data_fd after writes (checkpoint/close).
 * When sync=false, only write to page cache (buffer eviction).
 */
graveldb_status_t dimbin_flush(DimBin *s, bool sync) {
    WriteBuffer *wb = &s->write_buf;

    /* Phase 1: Flush buffered keys BEFORE value pages */
    graveldb_status_t key_rc = dimbin_flush_keys(s);
    if (key_rc != GRAVELDB_OK) return key_rc;

    /* Clear flush_needed flag since we're flushing now */
    s->flush_needed = false;

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

    /* Peephole merge + io_uring vectored batch submit */
    uring_io_ctx_t io_ctx;
    int use_uring = (uring_io_init(&io_ctx) == 0);
    graveldb_status_t rc = GRAVELDB_OK;

    /* For io_uring writev: iov arrays must survive until wait completes.
     * Collect heap-allocated iov pointers for deferred free. */
    struct iovec **iov_heap_ptrs = NULL;
    int iov_heap_count = 0;
    int iov_heap_cap = 0;

    /* Stack iovec buffer for synchronous (fallback) writev calls */
    struct iovec stack_iov[128];

    int i = 0;
    while (i < n) {
        int j = i + 1;

        /* Extend run with peephole gap tolerance */
        while (j < n && dirty_pages[j] <= dirty_pages[j - 1] + PEEPHOLE_GAP_MAX + 1) {
            j++;
        }

        /* Pages are native byte order — write directly to data file */
        for (int k = i; k < j; k++) {
            uint32_t pg = dirty_pages[k];
            uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
            if (wb->slots[slot_idx].page_id != pg) continue;
        }

        /* Build iovec for consecutive pages within this run.
         * A page is "consecutive" if its page_id == prev_page_id + 1.
         * Non-consecutive pages (gaps within the peephole run) start a new writev.
         * Each writev is capped at FLUSH_IOV_MAX to respect UIO_MAXIOV. */
        #define FLUSH_IOV_MAX 1024

        int ri = i;
        while (ri < j) {
            /* Find consecutive sub-run starting at ri */
            int rj = ri + 1;
            while (rj < j && dirty_pages[rj] == dirty_pages[rj - 1] + 1) {
                rj++;
            }

            /* Submit consecutive sub-run in chunks of FLUSH_IOV_MAX */
            int sub_pos = ri;
            while (sub_pos < rj) {
                int chunk_end = sub_pos + FLUSH_IOV_MAX;
                if (chunk_end > rj) chunk_end = rj;
                int chunk_len = chunk_end - sub_pos;

                /* For io_uring writev with iovcnt>1: always heap-alloc iov
                 * (must survive until uring_io_wait). For fallback: use stack. */
                struct iovec *iov;
                int iov_on_heap = 0;

                if (use_uring && chunk_len > 1) {
                    /* io_uring: must persist until wait */
                    iov = (struct iovec *)malloc((size_t)chunk_len * sizeof(struct iovec));
                    iov_on_heap = 1;
                } else if (chunk_len <= 128) {
                    iov = stack_iov;
                } else {
                    iov = (struct iovec *)malloc((size_t)chunk_len * sizeof(struct iovec));
                    iov_on_heap = 1;
                }
                if (!iov) { rc = GRAVELDB_ERR_OOM; sub_pos = rj; break; }

                int iovcnt = 0;
                off_t base_offset = (off_t)dirty_pages[sub_pos] * s->page_size;

                for (int k = sub_pos; k < chunk_end; k++) {
                    uint32_t pg = dirty_pages[k];
                    uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
                    if (wb->slots[slot_idx].page_id != pg) continue;
                    iov[iovcnt].iov_base = wb->slots[slot_idx].data;
                    iov[iovcnt].iov_len = s->page_size;
                    iovcnt++;
                }

                if (iovcnt > 0) {
                    if (use_uring) {
                        if (iovcnt == 1) {
                            if (uring_io_submit_write(&io_ctx, s->fd,
                                                      iov[0].iov_base, iov[0].iov_len,
                                                      base_offset) < 0) {
                                rc = GRAVELDB_ERR_IO;
                            }
                        } else {
                            if (uring_io_submit_writev(&io_ctx, s->fd,
                                                       iov, iovcnt, base_offset) < 0) {
                                rc = GRAVELDB_ERR_IO;
                            }
                        }
                    } else {
                        if (iovcnt == 1) {
                            ssize_t wr = pwrite(s->fd, iov[0].iov_base, iov[0].iov_len,
                                                base_offset);
                            if (wr != (ssize_t)iov[0].iov_len) rc = GRAVELDB_ERR_IO;
                        } else {
                            ssize_t wr = pwritev(s->fd, iov, iovcnt, base_offset);
                            size_t expected = (size_t)iovcnt * s->page_size;
                            if (wr < 0 || (size_t)wr != expected) rc = GRAVELDB_ERR_IO;
                        }
                    }
                }

                /* Defer free for io_uring writev iov; free immediately for fallback */
                if (iov_on_heap) {
                    if (use_uring && iovcnt > 1) {
                        /* Keep alive until uring_io_wait */
                        if (iov_heap_count >= iov_heap_cap) {
                            int new_cap = iov_heap_cap ? iov_heap_cap * 2 : 16;
                            struct iovec **tmp = (struct iovec **)realloc(
                                iov_heap_ptrs, (size_t)new_cap * sizeof(struct iovec *));
                            if (tmp) { iov_heap_ptrs = tmp; iov_heap_cap = new_cap; }
                        }
                        if (iov_heap_count < iov_heap_cap) {
                            iov_heap_ptrs[iov_heap_count++] = iov;
                        } else {
                            free(iov); /* fallback: can't track, free now (may corrupt io_uring) */
                        }
                    } else {
                        free(iov);
                    }
                }

                sub_pos = chunk_end;
            }

            ri = rj;
        }

        #undef FLUSH_IOV_MAX

        wb->flush_bytes += (size_t)(j - i) * s->page_size;
        i = j;
    }

    /* Wait for writes; optionally fdatasync */
    if (use_uring) {
        if (sync) uring_io_submit_fsyncs(&io_ctx);
        int errors = uring_io_wait(&io_ctx);
        if (errors > 0) rc = GRAVELDB_ERR_IO;
    } else if (sync) {
        /* Fallback: pwrite is synchronous, but fdatasync for durability */
        if (fdatasync(s->key_fd) != 0) rc = GRAVELDB_ERR_IO;
        if (fdatasync(s->fd) != 0) rc = GRAVELDB_ERR_IO;
    }
    uring_io_destroy(&io_ctx);

    /* Free iov arrays that were kept alive for io_uring writev */
    for (int k = 0; k < iov_heap_count; k++) {
        free(iov_heap_ptrs[k]);
    }
    free(iov_heap_ptrs);

    /*
     * Free pages AFTER I/O confirmed complete (io_uring buffers must stay valid).
     */
    if (n == (int)wb->count) {
        /* Full flush: recycle all, bulk-clear hashmap */
        for (int k = 0; k < n; k++) {
            uint32_t pg = dirty_pages[k];
            uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
            if (wb->slots[slot_idx].page_id != pg) continue;
            write_buf_recycle_page(wb, wb->slots[slot_idx].data);
        }
        memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
        wb->count = 0;
    } else {
        /* Partial flush: memset + re-insert remaining pages.
         * Much faster than scanning 262K slots for rehash. */

        /* Step 1: recycle flushed pages' buffers, collect remaining pages */
        /* We use a bitmap on the sorted dirty_pages for O(log n) membership test,
         * but simpler: mark flushed in slot, then sweep non-empty into temp array. */
        for (int k = 0; k < n; k++) {
            uint32_t pg = dirty_pages[k];
            uint32_t slot_idx = pagemap_find(wb->slots, wb->capacity, pg);
            if (wb->slots[slot_idx].page_id != pg) continue;
            write_buf_recycle_page(wb, wb->slots[slot_idx].data);
            wb->slots[slot_idx].page_id = PAGE_SLOT_EMPTY;
            wb->slots[slot_idx].data = NULL;
        }

        /* Step 2: collect remaining pages into a compact temp array */
        uint32_t remaining = wb->count - (uint32_t)n;
        PageSlot *tmp_slots = NULL;
        if (remaining > 0) {
            tmp_slots = (PageSlot *)malloc(remaining * sizeof(PageSlot));
        }
        if (tmp_slots) {
            uint32_t ri = 0;
            for (uint32_t ci = 0; ci < wb->capacity && ri < remaining; ci++) {
                if (wb->slots[ci].page_id != PAGE_SLOT_EMPTY) {
                    tmp_slots[ri++] = wb->slots[ci];
                }
            }
            remaining = ri;

            /* Step 3: bulk-clear hashmap and re-insert remaining */
            memset(wb->slots, 0xFF, wb->capacity * sizeof(PageSlot));
            for (uint32_t ri2 = 0; ri2 < remaining; ri2++) {
                uint32_t idx = pagemap_find(wb->slots, wb->capacity, tmp_slots[ri2].page_id);
                wb->slots[idx] = tmp_slots[ri2];
            }
            free(tmp_slots);
            wb->count = remaining;
        } else {
            /* OOM fallback: in-place rehash (slower but no alloc) */
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

    return rc;
}

/*
 * Check flush watermark on write buffer.
 */
static inline bool dimbin_should_flush(DimBin *s) {
    uint32_t threshold = (uint32_t)(s->write_buf.max_pages * FLUSH_WATERMARK_NUMER / FLUSH_WATERMARK_DENOM);
    return s->write_buf.count >= threshold;
}

graveldb_status_t dimbin_checkpoint_begin(DimBin *s) {
    /* Flush write buffer before freezing card table bitmap. */
    dimbin_flush(s, true);

    dirty_tracker_swap(&s->dirty);

    s->overlay.allocator = s->allocator;
    overlay_init(&s->overlay, s->dim);
    s->overlay.budget_bytes = s->overlay_budget;
    s->in_checkpoint = true;

    return GRAVELDB_OK;
}

graveldb_status_t dimbin_checkpoint_end(DimBin *s) {
    s->in_checkpoint = false;

    /* Replay overlay entries back into main storage via batched path */
    uint32_t total_overlay = s->overlay.count;
    if (total_overlay > 0) {
        EmbWriteEntry *replay_buf = (EmbWriteEntry *)malloc(total_overlay * sizeof(EmbWriteEntry));
        if (replay_buf) {
            uint32_t collected = 0;

            for (uint32_t i = 0; i < s->overlay.capacity && collected < total_overlay; i++) {
                if (s->overlay.slots[i].entry_id != OVERLAY_EMPTY) {
                    replay_buf[collected].entry_id = s->overlay.slots[i].entry_id;
                    replay_buf[collected].data = s->overlay.slots[i].data;
                    collected++;
                }
            }
            if (s->overlay.old_slots) {
                for (uint32_t i = 0; i < s->overlay.old_capacity && collected < total_overlay; i++) {
                    if (s->overlay.old_slots[i].entry_id != OVERLAY_EMPTY) {
                        replay_buf[collected].entry_id = s->overlay.old_slots[i].entry_id;
                        replay_buf[collected].data = s->overlay.old_slots[i].data;
                        collected++;
                    }
                }
            }

            /* Submit in chunks to avoid long pause (progressive drain) */
            #define OVERLAY_REPLAY_CHUNK  1024
            uint32_t pos = 0;
            while (pos < collected) {
                uint32_t chunk = collected - pos;
                if (chunk > OVERLAY_REPLAY_CHUNK) chunk = OVERLAY_REPLAY_CHUNK;
                dimbin_put_batch(s, &replay_buf[pos], (int)chunk);
                pos += chunk;
            }
            #undef OVERLAY_REPLAY_CHUNK
            free(replay_buf);
        } else {
            /* OOM fallback: replay one by one (correctness over performance) */
            for (uint32_t i = 0; i < s->overlay.capacity; i++) {
                if (s->overlay.slots[i].entry_id != OVERLAY_EMPTY) {
                    EmbWriteEntry e = { .entry_id = s->overlay.slots[i].entry_id,
                                        .data = s->overlay.slots[i].data };
                    dimbin_put_batch(s, &e, 1);
                }
            }
            if (s->overlay.old_slots) {
                for (uint32_t i = 0; i < s->overlay.old_capacity; i++) {
                    if (s->overlay.old_slots[i].entry_id != OVERLAY_EMPTY) {
                        EmbWriteEntry e = { .entry_id = s->overlay.old_slots[i].entry_id,
                                            .data = s->overlay.old_slots[i].data };
                        dimbin_put_batch(s, &e, 1);
                    }
                }
            }
        }
    }

    for (uint32_t i = 0; i < s->overlay.tomb_count; i++) {
        dimbin_free_entry(s, s->overlay.tombstones[i]);
    }

    overlay_destroy(&s->overlay);

    return GRAVELDB_OK;
}
