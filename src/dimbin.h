/*
 * GravelDB - DimBin (per-dimension storage bin)
 *
 * Self-contained module: defines WriteBuffer, DimBin and all
 * dimbin operations. Depends on its sub-components directly.
 */

#ifndef GRAVELDB_DIMBIN_H_
#define GRAVELDB_DIMBIN_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "graveldb.h"
#include "dirty_tracker.h"
#include "overlay.h"
#include "slab_alloc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define GRAVELDB_PAGE_SIZE_DEFAULT 4096
#define GRAVELDB_MAX_FLUSH_BATCH   (1 << 16)

/*
 * PageSlot: a single entry in the open-addressing page hashmap.
 * page_id == UINT32_MAX means the slot is empty.
 */
#define PAGE_SLOT_EMPTY UINT32_MAX

typedef struct {
    uint32_t  page_id;
    uint8_t  *data;
} PageSlot;

/*
 * WriteBuffer: open-addressing hashmap of dirty pages.
 * Memory proportional to actual dirty page count, not max page_id.
 * Flushes when count reaches max_pages.
 *
 * Proactive flush: every PROACTIVE_FLUSH_INTERVAL writes, randomly sample
 * a few pages from the buffer and flush them eagerly. This amortizes I/O
 * so that checkpoint-time flush is smaller and latency spikes are reduced.
 */
#define WB_PROACTIVE_FLUSH_INTERVAL  64   /* trigger proactive flush every N puts */
#define WB_PROACTIVE_FLUSH_BATCH      4   /* pages to flush per proactive trigger */

typedef struct {
    PageSlot   *slots;
    uint32_t    capacity;       /* hash table capacity (power of 2) */
    uint32_t    count;          /* number of occupied slots */
    size_t      max_pages;      /* flush threshold */
    uint64_t    flush_bytes;
    uint32_t    write_counter;  /* monotonic put counter for proactive flush */
    uint32_t    rng_state;      /* xorshift32 for random page sampling */
} WriteBuffer;

typedef struct DimBin {
    int         dim;
    uint32_t    page_size;
    size_t      entry_size;
    int         entries_per_page;
    int         fd;
    int         key_fd;
    char       *file_path;
    char       *key_file_path;
    uint64_t    bump_ptr;
    uint32_t   *free_list;
    uint32_t    free_count;
    uint32_t    free_capacity;
    DirtyTracker dirty;
    WriteBuffer write_buf;
    OverlayBuffer overlay;
    bool      in_checkpoint;
    uint32_t   *flush_dirty_buf;

    /* Slab allocator (non-owning pointer to GravelDB's allocator).
     * Eliminates global static pointer; supports multiple DB instances. */
    SlabAllocator *allocator;
    uint64_t    total_entries;
    uint64_t    total_pages;
    uint64_t    io_errors;       /* cumulative I/O error count (non-fatal) */
} DimBin;

/*
 * DimBin Operations
 */

graveldb_status_t dimbin_init(DimBin *s, int dim, const char *file_path,
                               size_t buffer_size, uint32_t entry_align,
                               uint32_t page_size);
void dimbin_destroy(DimBin *s);
uint32_t dimbin_alloc_entry(DimBin *s);
void dimbin_free_entry(DimBin *s, uint32_t entry_idx);
graveldb_status_t dimbin_get(DimBin *s, uint32_t entry_id, float *buf);
graveldb_status_t dimbin_put(DimBin *s, uint32_t entry_id, const float *data);
graveldb_status_t dimbin_put_key(DimBin *s, uint32_t entry_idx, uint64_t feat_id);

/*
 * Batched key write with user-space coalescing.
 * Accepts an array of (entry_idx, feat_id) pairs. Internally buckets them
 * by key-page and uses read-modify-write for dense pages, individual pwrite
 * for sparse pages. Reduces syscalls from N down to ~N/density.
 *
 * KEY_COALESCE_THRESH: minimum keys in one key-page to trigger
 * read-modify-write instead of individual pwrites.
 */
#define KEY_COALESCE_THRESH 4

typedef struct {
    uint32_t  entry_idx;
    uint64_t  feat_id;
} KeyWriteEntry;

void dimbin_put_keys_batch(DimBin *s, const KeyWriteEntry *entries, int count);

graveldb_status_t dimbin_flush(DimBin *s);
graveldb_status_t dimbin_checkpoint_begin(DimBin *s);
graveldb_status_t dimbin_checkpoint_end(DimBin *s);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_DIMBIN_H_ */
