/*
 * GravelDB - Pool-based Slab Allocator
 *
 * Design:
 *   - Single-threaded: no locks, no atomics, no CAS
 *   - Each pool manages a single fixed-size object class
 *   - Caller creates pools explicitly (no size-class lookup overhead)
 *   - Simple linked-list free list for hot-path alloc/free
 *   - 64KB slab pages for cache-friendly memory layout
 *   - Bulk alloc/free for batch operations (overlay create/destroy)
 *   - Aligned allocation support (4KB for page buffers)
 *
 * Usage pattern:
 *   SlabPool *pool = slab_pool_create(alloc, dim * sizeof(float), 16);
 *   void *p = slab_pool_alloc(pool);
 *   slab_pool_free(pool, p);
 *   slab_pool_destroy(pool);
 *
 * Why slab over malloc:
 *   - Known fixed-size objects created/destroyed in bulk (embeddings in overlay)
 *   - Zero fragmentation within pool
 *   - Simple free-list push/pop vs malloc's heap bookkeeping
 *   - Bulk free returns entire batch at once
 */

#ifndef GRAVELDB_SLAB_ALLOC_H_
#define GRAVELDB_SLAB_ALLOC_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SLAB_PAGE_SIZE         (64 * 1024)   /* 64KB per slab page */
#define SLAB_MAGAZINE_SIZE     32            /* objects per magazine / TLAB cache */
#define SLAB_MIN_OBJ_SIZE      32            /* min object size (for free-list ptr) */
#define SLAB_MAX_POOLS         32            /* max pools per allocator */
#define SLAB_DEFAULT_ALIGNMENT 16            /* default minimum alignment */

#include "graveldb.h"

/*
 * Slab Page
 */

typedef struct SlabPage {
    struct SlabPage *next;             /* linked list of pages in this pool */

    uint8_t        *base;              /* page memory start (aligned) */
    size_t          obj_size;          /* object size for this page */
    uint32_t        total_objects;     /* objects per page */
    uint32_t        allocated;         /* how many have been bump-allocated */
} SlabPage;

/*
 * Slab Pool (one fixed-size class)
 */

typedef struct SlabPool {
    size_t          obj_size;          /* fixed object size (>= SLAB_MIN_OBJ_SIZE) */
    size_t          alignment;         /* allocation alignment */
    size_t          page_size;         /* slab page size (64KB default) */

    /* Free list (simple linked list, single-threaded) */
    void           *free_head;
    uint64_t        free_count;

    /* Slab pages */
    SlabPage       *pages;

    uint32_t        page_count;
    uint64_t        total_allocated;
    uint64_t        total_freed;

    uint64_t        alloc_fast;       /* from free list */
    uint64_t        alloc_slow;       /* needed new page */
} SlabPool;

/*
 * Global Allocator (pool registry + large alloc tracking)
 */

typedef struct {
    SlabPool       *pools[SLAB_MAX_POOLS];  /* registered pools (may be NULL) */
    int             num_pools;
    bool            initialized;

    /* Global stats */
    uint64_t        total_pages;
    uint64_t        total_bytes;      /* total memory from OS */
    uint64_t        large_allocs;     /* oversized / aligned allocations via OS */
    uint64_t        large_bytes;
} SlabAllocator;

/*
 * Public API: Allocator Lifecycle
 */

graveldb_status_t slab_allocator_init(SlabAllocator *alloc);
void              slab_allocator_destroy(SlabAllocator *alloc);

/*
 * Public API: Pool Operations
 */

/*
 * Create a pool for objects of `obj_size` bytes with given `alignment`.
 * The pool is registered with the allocator for global stats/cleanup.
 * Returns NULL on failure.
 */
SlabPool *slab_pool_create(SlabAllocator *alloc, size_t obj_size, size_t alignment);

/* Destroy a pool, freeing all pages back to OS */
void slab_pool_destroy(SlabAllocator *alloc, SlabPool *pool);

/* Allocate one object from pool */
void *slab_pool_alloc(SlabPool *pool);

/* Free one object back to pool */
void slab_pool_free(SlabPool *pool, void *ptr);

/* Bulk allocate: fill `out` with up to `count` objects. Returns actual count. */
int slab_pool_alloc_bulk(SlabPool *pool, void **out, int count);

/* Bulk free: return `count` objects to pool */
void slab_pool_free_bulk(SlabPool *pool, void **ptrs, int count);

/*
 * Public API: Convenience (size-based, for one-off uses)
 */

/*
 * For callers that don't want to manage pools explicitly (e.g., page buffer).
 * These find-or-create an internal pool for the given size.
 * Slightly slower due to pool lookup, but still much faster than malloc for repeated sizes.
 */
void *slab_alloc(SlabAllocator *alloc, size_t size);
void  slab_free(SlabAllocator *alloc, void *ptr, size_t size);

/* Aligned allocation (uses OS posix_memalign, tracked for stats) */
void *slab_alloc_aligned(SlabAllocator *alloc, size_t size, size_t alignment);
void  slab_free_aligned(SlabAllocator *alloc, void *ptr, size_t size);

typedef struct {
    uint64_t total_pages;
    uint64_t total_bytes_from_os;
    uint64_t large_alloc_count;
    uint64_t large_alloc_bytes;
    int      num_pools;
    struct {
        size_t   obj_size;
        size_t   alignment;
        uint64_t total_allocated;
        uint64_t total_freed;
        uint64_t free_count;
        uint64_t alloc_fast;
        uint64_t alloc_slow;
        uint32_t page_count;
    } pools[SLAB_MAX_POOLS];
} SlabAllocStats;

void slab_allocator_stats(const SlabAllocator *alloc, SlabAllocStats *stats);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_SLAB_ALLOC_H_ */
