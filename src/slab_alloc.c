/*
 * GravelDB - Pool-based Slab Allocator Implementation
 *
 * Single-threaded design:
 *   - 64KB pages carved into fixed-size objects
 *   - Simple linked-list free list (push/pop, no CAS)
 *   - No size-class lookup (caller binds to pool at creation)
 *   - Bulk alloc/free for batch operations
 */

#include "slab_alloc.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

/*
 * Internal: Free-list node
 */

typedef struct FreeNode {
    struct FreeNode *next;
} FreeNode;

/*
 * Internal: Create a slab page
 */

static SlabPage *slab_page_create(size_t obj_size, size_t alignment, size_t page_size) {
    SlabPage *page = (SlabPage *)malloc(sizeof(SlabPage));
    if (!page) return NULL;

    /* Allocate page memory with requested alignment */
    void *mem = NULL;
    size_t align = alignment > SLAB_DEFAULT_ALIGNMENT ? alignment : SLAB_DEFAULT_ALIGNMENT;
    if (posix_memalign(&mem, align, page_size) != 0) {
        free(page);
        return NULL;
    }

    page->base = (uint8_t *)mem;
    page->obj_size = obj_size;
    page->total_objects = (uint32_t)(page_size / obj_size);
    page->allocated = 0;
    page->next = NULL;

    return page;
}

static void slab_page_destroy(SlabPage *page) {
    if (page) {
        free(page->base);
        free(page);
    }
}

/*
 * Internal: Grow pool (add a new page, push all objects onto free list)
 */

static void slab_pool_grow(SlabPool *pool) {
    SlabPage *page = slab_page_create(pool->obj_size, pool->alignment, pool->page_size);
    if (!page) return;

    /* Link page into pool */
    page->next = pool->pages;
    pool->pages = page;
    pool->page_count++;

    /* Carve all objects into the free list (simple push) */
    uint32_t n = page->total_objects;
    for (uint32_t i = 0; i < n; i++) {
        FreeNode *node = (FreeNode *)(page->base + i * pool->obj_size);
        node->next = (FreeNode *)pool->free_head;
        pool->free_head = node;
    }

    pool->free_count += n;
    page->allocated = n;
    pool->alloc_slow++;
}

/*
 * Allocator Init / Destroy
 */

graveldb_status_t slab_allocator_init(SlabAllocator *alloc) {
    if (!alloc) return GRAVELDB_ERR_INVALID;

    memset(alloc, 0, sizeof(*alloc));
    alloc->num_pools = 0;
    alloc->initialized = true;
    alloc->total_pages = 0;
    alloc->total_bytes = 0;
    alloc->large_allocs = 0;
    alloc->large_bytes = 0;

    return GRAVELDB_OK;
}

void slab_allocator_destroy(SlabAllocator *alloc) {
    if (!alloc) return;

    for (int i = 0; i < alloc->num_pools; i++) {
        if (alloc->pools[i]) {
            slab_pool_destroy(alloc, alloc->pools[i]);
            alloc->pools[i] = NULL;
        }
    }
    alloc->num_pools = 0;
    alloc->initialized = false;
}

/*
 * Pool Create / Destroy
 */

SlabPool *slab_pool_create(SlabAllocator *alloc, size_t obj_size, size_t alignment) {
    if (!alloc || alloc->num_pools >= SLAB_MAX_POOLS) return NULL;

    /* Enforce minimum object size (must fit a FreeNode pointer) */
    if (obj_size < SLAB_MIN_OBJ_SIZE) obj_size = SLAB_MIN_OBJ_SIZE;

    /* Round up obj_size to alignment boundary */
    if (alignment == 0) alignment = SLAB_DEFAULT_ALIGNMENT;
    obj_size = ((obj_size + alignment - 1) / alignment) * alignment;

    SlabPool *pool = (SlabPool *)calloc(1, sizeof(SlabPool));
    if (!pool) return NULL;

    pool->obj_size = obj_size;
    pool->alignment = alignment;
    pool->page_size = SLAB_PAGE_SIZE;
    pool->free_head = NULL;
    pool->free_count = 0;
    pool->page_count = 0;
    pool->total_allocated = 0;
    pool->total_freed = 0;
    pool->alloc_fast = 0;
    pool->alloc_slow = 0;
    pool->pages = NULL;

    /* Pre-allocate one page */
    slab_pool_grow(pool);

    /* Register with allocator */
    alloc->pools[alloc->num_pools++] = pool;
    alloc->total_pages += pool->page_count;
    alloc->total_bytes += (uint64_t)pool->page_count * SLAB_PAGE_SIZE;

    return pool;
}

void slab_pool_destroy(SlabAllocator *alloc, SlabPool *pool) {
    if (!pool) return;

    /* Free all pages */
    SlabPage *p = pool->pages;
    while (p) {
        SlabPage *next = p->next;
        if (alloc) {
            alloc->total_pages--;
            alloc->total_bytes -= SLAB_PAGE_SIZE;
        }
        slab_page_destroy(p);
        p = next;
    }

    /* Unregister from allocator */
    if (alloc) {
        for (int i = 0; i < alloc->num_pools; i++) {
            if (alloc->pools[i] == pool) {
                /* Swap with last */
                alloc->pools[i] = alloc->pools[--alloc->num_pools];
                break;
            }
        }
    }

    free(pool);
}

/*
 * Pool Alloc (simple free-list pop)
 */

void *slab_pool_alloc(SlabPool *pool) {
    if (!pool) return NULL;

    /* Fast path: pop from free list */
    FreeNode *node = (FreeNode *)pool->free_head;
    if (node) {
        pool->free_head = node->next;
        pool->free_count--;
        pool->total_allocated++;
        pool->alloc_fast++;
        return (void *)node;
    }

    /* Slow path: grow pool then retry */
    slab_pool_grow(pool);

    node = (FreeNode *)pool->free_head;
    if (!node) return NULL; /* OOM */

    pool->free_head = node->next;
    pool->free_count--;
    pool->total_allocated++;
    return (void *)node;
}

/*
 * Pool Free (simple free-list push)
 */

void slab_pool_free(SlabPool *pool, void *ptr) {
    if (!pool || !ptr) return;

    FreeNode *node = (FreeNode *)ptr;
    node->next = (FreeNode *)pool->free_head;
    pool->free_head = node;

    pool->free_count++;
    pool->total_freed++;
}

/*
 * Pool Bulk Alloc/Free
 */

int slab_pool_alloc_bulk(SlabPool *pool, void **out, int count) {
    if (!pool || !out || count <= 0) return 0;

    int got = 0;

    /* Pop from free list */
    while (got < count) {
        FreeNode *node = (FreeNode *)pool->free_head;
        if (!node) {
            /* Need more pages */
            slab_pool_grow(pool);
            node = (FreeNode *)pool->free_head;
            if (!node) break; /* OOM */
        }
        pool->free_head = node->next;
        pool->free_count--;
        out[got++] = (void *)node;
    }

    pool->total_allocated += got;
    pool->alloc_fast += got;
    return got;
}

void slab_pool_free_bulk(SlabPool *pool, void **ptrs, int count) {
    if (!pool || !ptrs) return;

    for (int i = 0; i < count; i++) {
        if (!ptrs[i]) continue;
        FreeNode *node = (FreeNode *)ptrs[i];
        node->next = (FreeNode *)pool->free_head;
        pool->free_head = node;
    }

    pool->free_count += count;
    pool->total_freed += count;
}

/*
 * Convenience: size-based alloc (find-or-create pool)
 */

static SlabPool *slab_find_or_create_pool(SlabAllocator *alloc, size_t size) {
    /* Round up size to alignment boundary */
    size_t aligned_size = ((size + SLAB_DEFAULT_ALIGNMENT - 1) / SLAB_DEFAULT_ALIGNMENT) * SLAB_DEFAULT_ALIGNMENT;
    if (aligned_size < SLAB_MIN_OBJ_SIZE) aligned_size = SLAB_MIN_OBJ_SIZE;

    /* Search existing pools */
    for (int i = 0; i < alloc->num_pools; i++) {
        if (alloc->pools[i] && alloc->pools[i]->obj_size == aligned_size) {
            return alloc->pools[i];
        }
    }

    /* Create new pool */
    return slab_pool_create(alloc, size, SLAB_DEFAULT_ALIGNMENT);
}

void *slab_alloc(SlabAllocator *alloc, size_t size) {
    if (!alloc || size == 0) return NULL;

    SlabPool *pool = slab_find_or_create_pool(alloc, size);
    if (!pool) {
        /* Fallback to malloc */
        void *ptr = malloc(size);
        if (ptr) {
            alloc->large_allocs++;
            alloc->large_bytes += size;
        }
        return ptr;
    }

    return slab_pool_alloc(pool);
}

void slab_free(SlabAllocator *alloc, void *ptr, size_t size) {
    if (!alloc || !ptr) return;

    /* Round up size same way as alloc */
    size_t aligned_size = ((size + SLAB_DEFAULT_ALIGNMENT - 1) / SLAB_DEFAULT_ALIGNMENT) * SLAB_DEFAULT_ALIGNMENT;
    if (aligned_size < SLAB_MIN_OBJ_SIZE) aligned_size = SLAB_MIN_OBJ_SIZE;

    for (int i = 0; i < alloc->num_pools; i++) {
        if (alloc->pools[i] && alloc->pools[i]->obj_size == aligned_size) {
            slab_pool_free(alloc->pools[i], ptr);
            return;
        }
    }

    /* No pool found: was allocated via malloc fallback */
    free(ptr);
    alloc->large_allocs--;
    alloc->large_bytes -= size;
}

/*
 * Aligned Alloc (OS-backed, for block buffers)
 */

void *slab_alloc_aligned(SlabAllocator *alloc, size_t size, size_t alignment) {
    if (!alloc) return NULL;

    /* Check if we have a pool that matches both size and alignment */
    size_t aligned_size = ((size + alignment - 1) / alignment) * alignment;
    if (aligned_size < SLAB_MIN_OBJ_SIZE) aligned_size = SLAB_MIN_OBJ_SIZE;

    for (int i = 0; i < alloc->num_pools; i++) {
        SlabPool *p = alloc->pools[i];
        if (p && p->obj_size == aligned_size && p->alignment >= alignment) {
            return slab_pool_alloc(p);
        }
    }

    /* No matching pool -- try to create one if alignment is reasonable */
    if (aligned_size <= SLAB_PAGE_SIZE / 4) {
        SlabPool *pool = slab_pool_create(alloc, size, alignment);
        if (pool) return slab_pool_alloc(pool);
    }

    /* Fallback: posix_memalign */
    void *ptr = NULL;
    if (posix_memalign(&ptr, alignment, size) != 0) return NULL;
    alloc->large_allocs++;
    alloc->large_bytes += size;
    return ptr;
}

void slab_free_aligned(SlabAllocator *alloc, void *ptr, size_t size) {
    if (!alloc || !ptr) return;

    /* Try to find a matching pool */
    for (int i = 0; i < alloc->num_pools; i++) {
        SlabPool *p = alloc->pools[i];
        if (p && p->obj_size >= size) {
            /* Check if ptr belongs to this pool's pages */
            SlabPage *page = p->pages;
            while (page) {
                if ((uint8_t *)ptr >= page->base &&
                    (uint8_t *)ptr < page->base + p->page_size) {
                    slab_pool_free(p, ptr);
                    return;
                }
                page = page->next;
            }
        }
    }

    /* Not from any pool -> was posix_memalign'd */
    free(ptr);
    alloc->large_allocs--;
    alloc->large_bytes -= size;
}

void slab_allocator_stats(const SlabAllocator *alloc, SlabAllocStats *stats) {
    if (!alloc || !stats) return;
    memset(stats, 0, sizeof(*stats));

    stats->total_pages = alloc->total_pages;
    stats->total_bytes_from_os = alloc->total_bytes;
    stats->large_alloc_count = alloc->large_allocs;
    stats->large_alloc_bytes = alloc->large_bytes;
    stats->num_pools = alloc->num_pools;

    for (int i = 0; i < alloc->num_pools; i++) {
        const SlabPool *pool = alloc->pools[i];
        if (!pool) continue;
        stats->pools[i].obj_size = pool->obj_size;
        stats->pools[i].alignment = pool->alignment;
        stats->pools[i].total_allocated = pool->total_allocated;
        stats->pools[i].total_freed = pool->total_freed;
        stats->pools[i].free_count = pool->free_count;
        stats->pools[i].alloc_fast = pool->alloc_fast;
        stats->pools[i].alloc_slow = pool->alloc_slow;
        stats->pools[i].page_count = pool->page_count;
    }
}
