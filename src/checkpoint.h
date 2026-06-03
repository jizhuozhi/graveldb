/*
 * GravelDB - Checkpoint Scheduler & Storage Format
 *
 * Self-contained module. Depends on dimbin.h.
 *
 * Responsibilities:
 *   1. Periodic checkpoint scheduling (interval-based)
 *   2. Delta file I/O: dump + recovery
 *   3. Meta persistence (atomic write to separate .meta file)
 *   4. Full checkpoint lifecycle management
 *   5. Delta chain management (compaction to full)
 *
 * Architecture:
 *   CkptScheduler owns a timer and triggers checkpoint/flush on the
 *   GravelDB instance. It runs in the same single-threaded event loop
 *   (cooperative tick) -- no background threads, no locks.
 *
 * Storage format:
 *   Each DimBin file is pure data: [Entry Data...]
 *   Metadata (generation) lives in a separate .meta file per bin.
 *   Delta files are separate: [DeltaHeader][Entry0][Entry1]...
 */

#ifndef GRAVELDB_CHECKPOINT_H_
#define GRAVELDB_CHECKPOINT_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "graveldb.h"
#include "dimbin.h"
#include "io_uring_flush.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * BinMeta: minimal metadata persisted in a separate .meta file.
 * Only contains generation (for delta chain continuity tracking).
 * Written atomically via write-to-temp + fsync + rename.
 *
 * On-disk format: 16 bytes, little-endian (see wire.h for encode/decode).
 * This struct is for in-memory use only — never written directly to disk.
 */
typedef struct {
    uint32_t magic;
    uint64_t generation;
    uint32_t checksum;
} BinMeta;

/*
 * DeltaHeader: header of each delta checkpoint file.
 *
 * On-disk format: 40 bytes, little-endian (see wire.h for encode/decode).
 * This struct is for in-memory use only — never written directly to disk.
 */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t generation;
    uint32_t dim;
    uint32_t entry_size;
    uint64_t bump_ptr;
    uint32_t num_entries;
    uint32_t checksum;
} DeltaHeader;

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

/*
 * Persist metadata to a separate .meta file using atomic write:
 *   write(tmp) → fsync(tmp) → rename(tmp, final) → fsync(dir)
 * This guarantees the .meta file is always valid or absent.
 */
graveldb_status_t ckpt_persist_meta(DimBin *bin, const char *data_dir,
                                    uint64_t generation);

/*
 * Read metadata from the .meta file for the given bin.
 * Returns GRAVELDB_OK on success, GRAVELDB_ERR_CORRUPT if file is
 * missing/invalid/checksum mismatch (treated as fresh DB).
 */
graveldb_status_t ckpt_read_meta(const char *data_dir, int dim,
                                 BinMeta *out_meta);

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

/*
 * Unified Checkpoint Export API (internal structures)
 *
 * Both full and delta checkpoints use the same entry-interleaved wire format:
 *   [CkptExportHeader][feat_id|embedding][feat_id|embedding]...
 *
 * Full: scans all valid entries. Serves as truncation point (discard prior deltas).
 * Delta: scans only dirty entries (from frozen bitmap).
 *
 * Both rely on overlay for SATB (snapshot-at-the-beginning) isolation:
 *   - checkpoint_begin() freezes state via overlay
 *   - Export reads from main file (which holds the frozen-time data)
 *   - Concurrent writes go into overlay, not main file
 *   - No flush needed during export
 *
 * IO is non-blocking: reads are submitted via io_uring (or fallback pread),
 * then polled in the event loop. This avoids blocking the host thread.
 *
 * Backpressure: if overlay reaches budget, writes get GRAVELDB_ERR_BUSY.
 */

/* Max entries to read in a single step (bounds io_uring SQ depth usage) */
#define CKPT_EXPORT_IO_BATCH  256

/*
 * Coalesced IO range: a contiguous run of valid entries in the data file.
 * Instead of issuing one pread per entry, adjacent entries are merged into
 * a single large read. This eliminates random-read amplification for
 * bump-allocated data where most entries are physically contiguous.
 */
typedef struct {
    uint32_t  start_idx;    /* first entry_idx in this run */
    uint32_t  count;        /* number of contiguous entries */
    uint8_t  *buf;          /* read target (points into read_buf_pool) */
} CkptCoalescedRange;

/*
 * Per-entry metadata: after key batch read, we record valid entries
 * so that poll can output them in order.
 */
typedef struct {
    uint64_t  feat_id;
    uint32_t  entry_idx;
    uint8_t  *buf;       /* pointer into read_buf_pool for this entry's data */
} CkptExportReadOp;

/*
 * Export pipeline phases:
 *   PHASE_KEY_READ   - key batch read submitted, waiting for completion
 *   PHASE_EMB_READ   - embedding coalesced reads submitted, waiting
 *   PHASE_OUTPUT     - all reads done, outputting via write_cb
 */
typedef enum {
    CKPT_EXPORT_PHASE_IDLE = 0,
    CKPT_EXPORT_PHASE_KEY_READ,
    CKPT_EXPORT_PHASE_EMB_READ,
    CKPT_EXPORT_PHASE_OUTPUT,
} CkptExportPhase;

/*
 * Export context: holds state for a progressive full/delta export.
 * One context per DimBin. Created by graveldb_ckpt_export_begin.
 *
 * Lifecycle: begin → (step + poll) loop → end.
 *
 * IO strategy (sequential scan + coalesced reads + pipeline):
 *   1. Keys are read in bulk (batch_entries * 8 bytes per step) via io_uring
 *   2. Valid entries are identified, adjacent ones coalesced into ranges
 *   3. Embedding reads are submitted as coalesced large pread ops
 *   4. Key IO and embedding IO can overlap across steps (pipeline)
 */
struct GravelDBCkptExport {
    GravelDB      *db;
    DimBin        *bin;
    uint32_t       cursor;         /* next entry_idx to scan (full) */
    uint32_t       snapshot_bump;  /* bump_ptr at begin time */
    uint64_t       generation;     /* checkpoint generation */
    uint64_t       base_gen;       /* base generation (for delta: prev gen; full: 0) */
    uint64_t       num_entries;    /* entries written so far */
    uint32_t       type;           /* GRAVELDB_CKPT_FULL or GRAVELDB_CKPT_DELTA */
    uint32_t       batch_entries;  /* entries per step */
    bool           header_written;
    bool           scan_done;      /* true when scan phase is complete */

    /* Zero-backpressure dump file: export writes here at full speed */
    int            dump_fd;        /* writable fd for the dump file */
    char           dump_path[512]; /* path to the dump file */
    uint64_t       dump_size;      /* total bytes written to dump so far */
    off_t          read_cursor;    /* host read position (for export_read) */

    /* Delta mode: sorted array of dirty page indices */
    uint32_t      *dirty_pages;
    int            dirty_page_count;
    int            dirty_page_cursor;

    /* Pipeline phase tracking */
    CkptExportPhase phase;

    /* Key batch read buffer: batch_entries * 8 bytes */
    uint64_t      *key_batch_buf;
    uint32_t       key_batch_start;  /* entry_idx of first key in batch */
    uint32_t       key_batch_count;  /* number of keys read this batch */

    /* Coalesced embedding IO ranges */
    CkptCoalescedRange *coalesced_ranges;
    int            range_count;
    int            range_capacity;

    /* Per-entry output metadata (valid entries from key scan) */
    CkptExportReadOp *read_ops;
    int            read_op_count;    /* valid entries this step */

    /* Async IO state */
    uring_io_ctx_t io_ctx;
    uint8_t       *read_buf_pool;    /* contiguous buffer for embedding reads */

    /* If GRAVELDB_CKPT_ALSO_FULL was set on a delta export, this points to
     * the internal full export context driven in lockstep. NULL otherwise. */
    struct GravelDBCkptExport *paired_full;
};

/*
 * Import context: holds state for progressive import with async writes.
 */
#define CKPT_IMPORT_BATCH  256

struct GravelDBCkptImport {
    GravelDB      *db;
    DimBin        *bin;
    int            import_fd;       /* read fd for the dump file */
    bool           header_read;
    bool           done;
    uint32_t       dim;
    uint32_t       entry_size;
    uint64_t       generation;

    /* Async IO state */
    uring_io_ctx_t io_ctx;
    int            pending_writes;
    uint8_t       *write_buf_pool;   /* batch buffer: CKPT_IMPORT_BATCH * entry_size */
};

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_CHECKPOINT_H_ */
