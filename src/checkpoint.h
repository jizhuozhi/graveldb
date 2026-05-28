/*
 * GravelDB - Checkpoint Scheduler & Storage Format
 *
 * Self-contained module. Depends on dimbin.h.
 *
 * Responsibilities:
 *   1. Periodic checkpoint scheduling (interval-based)
 *   2. Delta file I/O: dump + recovery
 *   3. Footer persistence (dual-footer crash-safe)
 *   4. Full checkpoint lifecycle management
 *   5. Delta chain management (compaction to full)
 *
 * Architecture:
 *   CkptScheduler owns a timer and triggers checkpoint/flush on the
 *   GravelDB instance. It runs in the same single-threaded event loop
 *   (cooperative tick) -- no background threads, no locks.
 *
 * Storage format:
 *   Each DimBin file is self-contained:
 *     [Entry Data...][Index Block][FreeList Block][Footer A (64B)][Footer B (64B)]
 *   Delta files are separate:
 *     [DeltaHeader][Entry0][Entry1]...
 */

#ifndef GRAVELDB_CHECKPOINT_H_
#define GRAVELDB_CHECKPOINT_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "graveldb.h"
#include "dimbin.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t dim;
    uint32_t _pad0;
    uint64_t num_entries;
    uint64_t bump_ptr;
    uint64_t free_list_offset;
    uint64_t free_list_size;
    uint64_t generation;
    uint32_t checksum;
    uint32_t _pad1;
} __attribute__((packed)) FileFooter;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t generation;
    uint32_t dim;
    uint32_t entry_size;
    uint64_t bump_ptr;
    uint32_t num_entries;
    uint32_t checksum;
} __attribute__((packed)) DeltaHeader;

/*
 * Incremental checkpoint progress state machine.
 * Each call to graveldb_checkpoint_step() advances one bin or one batch of
 * dirty pages, bounding per-call latency for latency-sensitive callers.
 */
typedef enum {
    CKPT_IDLE = 0,
    CKPT_FLUSHING,      /* flush one bin per step */
    CKPT_DUMPING,       /* dump delta for one bin per step */
    CKPT_FINISHING,     /* end checkpoint on all bins */
} CkptPhase;

typedef struct {
    CkptPhase  phase;
    uint16_t   current_bin;        /* which bin we're currently processing */
    int        ckpt_fd;            /* open delta fd for current bin (during DUMPING) */
    uint32_t  *dirty_blocks;       /* dirty block list for current bin */
    int        dirty_count;        /* total dirty blocks */
    int        dirty_cursor;       /* how far we've written */
    int        entries_written;    /* entries written so far in current delta */
    uint32_t   max_pages_per_step; /* budget: max pages to dump per step */
} CkptProgress;

typedef struct {
    uint32_t flush_interval_ms;     /* 0 = manual only (default: 1000) */
    uint32_t flush_dirty_threshold; /* trigger flush if dirty_pages > N (default: 4096) */
    uint32_t checkpoint_interval_s; /* 0 = manual only (default: 60) */
    uint32_t full_cooldown_ms;      /* minimum ms between two full checkpoints (default: 60000) */
    bool     auto_recover_on_open;  /* attempt recovery at startup (default: true) */
} CkptConfig;

/*
 * Full checkpoint response -- written back by the scheduler when
 * the full checkpoint actually completes at the safepoint.
 * Multiple clients can register; they form a linked list (singleflight).
 */
typedef struct CkptFullResponse {
    bool     completed;     /* set to true when full checkpoint is done */
    uint64_t checkpoint_id; /* generation of the completed full checkpoint */
    uint64_t delta_base;    /* == checkpoint_id; subsequent deltas start from here */
    uint64_t timestamp_ms;  /* wall-clock time when full completed (informational) */
    struct CkptFullResponse *next; /* internal: linked list for singleflight */
} CkptFullResponse;

/* Forward declaration -- full definition in graveldb_impl.h */
struct GravelDB;

typedef struct {
    struct GravelDB  *db;
    CkptConfig        config;

    uint64_t          last_flush_ms;
    uint64_t          last_checkpoint_ms;

    bool              full_pending;
    CkptFullResponse *full_waiters;
    uint64_t          last_full_generation;
    uint64_t          last_full_time_ms;
    uint32_t          full_cooldown_ms;
    uint32_t          delta_chain_length;

    uint64_t          total_flushes;
    uint64_t          total_checkpoints;
    uint64_t          total_full_checkpoints;
    uint64_t          total_bytes_dumped;
} CkptScheduler;

graveldb_status_t ckpt_scheduler_init(CkptScheduler *sched, struct GravelDB *db,
                                      const CkptConfig *config);

void ckpt_scheduler_destroy(CkptScheduler *sched);

graveldb_status_t ckpt_scheduler_tick(CkptScheduler *sched, uint64_t now_ms);

/* Force immediate flush/checkpoint regardless of interval */
graveldb_status_t ckpt_scheduler_force_flush(CkptScheduler *sched);
graveldb_status_t ckpt_scheduler_force_checkpoint(CkptScheduler *sched);

/*
 * Request a full checkpoint. Does NOT execute immediately.
 * The full checkpoint will be performed at the next safepoint -- i.e. after the
 * next delta checkpoint completes and files are in a consistent state.
 *
 * Singleflight: if a full is already pending, the new request joins the existing
 * batch. All waiters receive the same checkpoint_id when it completes.
 *
 * Cooldown: if a full was completed less than full_cooldown_ms ago, the request
 * is immediately resolved with the last full's checkpoint_id (no redundant work).
 *
 * The caller provides a response struct that will be filled in upon completion.
 * Returns GRAVELDB_OK on success (request accepted or immediately satisfied).
 * Returns GRAVELDB_ERR_INVALID if sched or response is NULL.
 */
graveldb_status_t ckpt_scheduler_request_full(CkptScheduler *sched,
                                              CkptFullResponse *response);

graveldb_status_t ckpt_dump_delta(DimBin *bin, const char *data_dir,
                                  uint64_t *out_bytes);

graveldb_status_t ckpt_dump_full(DimBin *bin, const char *data_dir);

graveldb_status_t ckpt_persist_footer(DimBin *bin, uint64_t num_entries,
                                      uint64_t generation);

graveldb_status_t ckpt_read_footer(int fd, FileFooter *out_footer);

graveldb_status_t ckpt_recover(struct GravelDB *db);
graveldb_status_t ckpt_replay_delta(DimBin *bin, const char *delta_path);

typedef struct {
    char    *path;
    uint64_t generation;
    int      dim;
} CkptDeltaInfo;

int ckpt_list_deltas(const char *data_dir, int dim,
                     CkptDeltaInfo **out_deltas);

void ckpt_free_deltas(CkptDeltaInfo *deltas, int count);

graveldb_status_t ckpt_purge_old_deltas(const char *data_dir, int dim,
                                        uint64_t last_full_generation);

typedef struct {
    uint64_t total_flushes;
    uint64_t total_checkpoints;
    uint64_t total_full_checkpoints;
    uint64_t total_bytes_dumped;
    uint32_t current_delta_chain_length;
    uint64_t last_checkpoint_generation;
} CkptStats;

void ckpt_scheduler_stats(const CkptScheduler *sched, CkptStats *out);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_CHECKPOINT_H_ */
