/*
 * GravelDB - Checkpoint Scheduler & Storage Format Implementation
 *
 * Module layering:
 *   checkpoint.c (this file)
 *     - CkptScheduler: timer-based cooperative tick
 *     - Delta I/O: dump_delta, dump_full, replay_delta
 *     - Footer I/O: persist_footer, read_footer (dual A/B crash-safe)
 *     - Recovery: footer-based restore + delta replay
 *     - Chain management: list/purge old deltas
 *
 *   Depends on: graveldb.h (DimBin, HashIndex, DeltaHeader, FileFooter, etc.)
 *   Does NOT depend on: server.h, client.h
 */

#include "graveldb_impl.h"
#include "checkpoint.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>

#include "dimbin.h"

static uint32_t crc32_simple(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return ~crc;
}

/* Streaming CRC32: call repeatedly, then finalize with ~crc */
static uint32_t crc32_update(uint32_t crc, const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return crc;
}

graveldb_status_t ckpt_scheduler_init(CkptScheduler *sched, GravelDB *db,
                                      const CkptConfig *config) {
    if (!sched || !db) return GRAVELDB_ERR_INVALID;

    memset(sched, 0, sizeof(*sched));
    sched->db = db;

    sched->config.flush_interval_ms     = config ? config->flush_interval_ms : 1000;
    sched->config.flush_dirty_threshold = config ? config->flush_dirty_threshold : 4096;
    sched->config.checkpoint_interval_s = config ? config->checkpoint_interval_s : 60;
    sched->config.auto_recover_on_open  = config ? config->auto_recover_on_open : true;

    sched->full_pending = false;
    sched->full_waiters = NULL;
    sched->last_full_generation = db->current_epoch;
    sched->last_full_time_ms = 0;
    sched->full_cooldown_ms = config ? config->full_cooldown_ms : 60000;
    sched->delta_chain_length = 0;

    return GRAVELDB_OK;
}

void ckpt_scheduler_destroy(CkptScheduler *sched) {
    if (!sched) return;
    memset(sched, 0, sizeof(*sched));
}

graveldb_status_t ckpt_scheduler_tick(CkptScheduler *sched, uint64_t now_ms) {
    if (!sched || !sched->db) return GRAVELDB_ERR_INVALID;

    graveldb_status_t rc = GRAVELDB_OK;
    if (sched->config.flush_interval_ms > 0) {
        uint64_t elapsed_flush = now_ms - sched->last_flush_ms;
        if (elapsed_flush >= sched->config.flush_interval_ms) {
            rc = ckpt_scheduler_force_flush(sched);
            sched->last_flush_ms = now_ms;
            if (rc != GRAVELDB_OK) return rc;
        }
    }
    if (sched->config.checkpoint_interval_s > 0) {
        uint64_t elapsed_ckpt = now_ms - sched->last_checkpoint_ms;
        uint64_t interval_ms = (uint64_t)sched->config.checkpoint_interval_s * 1000;
        if (elapsed_ckpt >= interval_ms) {
            rc = ckpt_scheduler_force_checkpoint(sched);
            sched->last_checkpoint_ms = now_ms;
            if (rc != GRAVELDB_OK) return rc;
        }
    }

    return GRAVELDB_OK;
}

graveldb_status_t ckpt_scheduler_force_flush(CkptScheduler *sched) {
    graveldb_status_t rc = graveldb_flush(sched->db);
    if (rc == GRAVELDB_OK) {
        sched->total_flushes++;
    }
    return rc;
}

graveldb_status_t ckpt_scheduler_force_checkpoint(CkptScheduler *sched) {
    GravelDB *db = sched->db;
    graveldb_status_t rc;

    /* Always do delta checkpoint first */
    rc = graveldb_checkpoint(db);
    if (rc != GRAVELDB_OK) return rc;

    uint16_t num_slabs = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        ckpt_persist_footer(s, db->index.count, s->dirty.generation);
    }
    sched->delta_chain_length++;
    sched->total_checkpoints++;

    /*
     * Safepoint: delta is flushed and files are consistent.
     * If a full checkpoint was requested, execute it now by copying files.
     */
    if (sched->full_pending) {
        graveldb_flush(db);

        for (uint16_t i = 0; i < num_slabs; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            ckpt_dump_full(s, db->data_dir);
        }

        sched->last_full_generation = db->current_epoch;
        sched->last_full_time_ms = sched->last_checkpoint_ms;
        sched->delta_chain_length = 0;
        sched->total_full_checkpoints++;
        sched->full_pending = false;

        /* Broadcast to all waiting clients (singleflight completion) */
        CkptFullResponse *waiter = sched->full_waiters;
        while (waiter) {
            waiter->checkpoint_id = db->current_epoch;
            waiter->delta_base = db->current_epoch;
            waiter->timestamp_ms = sched->last_full_time_ms;
            waiter->completed = true;
            waiter = waiter->next;
        }
        sched->full_waiters = NULL;

        /* Purge old deltas that are now covered by the full */
        for (uint16_t i = 0; i < num_slabs; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            ckpt_purge_old_deltas(db->data_dir, s->dim, sched->last_full_generation);
        }
    }

    return GRAVELDB_OK;
}

/*
 * Request a full checkpoint at the next safepoint.
 * Singleflight: multiple requests are batched into one execution.
 * Cooldown: if a full was done recently, immediately return that result.
 */
graveldb_status_t ckpt_scheduler_request_full(CkptScheduler *sched,
                                              CkptFullResponse *response) {
    if (!sched || !response) return GRAVELDB_ERR_INVALID;

    response->next = NULL;

    /*
     * Cooldown check: if the last full completed within cooldown window,
     * immediately satisfy this request with the existing checkpoint_id.
     * The caller sees it as "already done" -- no redundant full needed.
     */
    if (sched->last_full_time_ms > 0 && sched->full_cooldown_ms > 0) {
        uint64_t now_ms = sched->last_checkpoint_ms; /* best available monotonic */
        if (now_ms - sched->last_full_time_ms < sched->full_cooldown_ms) {
            response->completed = true;
            response->checkpoint_id = sched->last_full_generation;
            response->delta_base = sched->last_full_generation;
            response->timestamp_ms = sched->last_full_time_ms;
            return GRAVELDB_OK;
        }
    }

    /* Singleflight: append to the waiters list */
    response->completed = false;
    response->checkpoint_id = 0;
    response->delta_base = 0;
    response->timestamp_ms = 0;
    response->next = sched->full_waiters;
    sched->full_waiters = response;

    sched->full_pending = true;
    return GRAVELDB_OK;
}

void ckpt_scheduler_stats(const CkptScheduler *sched, CkptStats *out) {
    if (!sched || !out) return;
    out->total_flushes = sched->total_flushes;
    out->total_checkpoints = sched->total_checkpoints;
    out->total_full_checkpoints = sched->total_full_checkpoints;
    out->total_bytes_dumped = sched->total_bytes_dumped;
    out->current_delta_chain_length = sched->delta_chain_length;
    out->last_checkpoint_generation = sched->db ? sched->db->current_epoch : 0;
}

graveldb_status_t ckpt_dump_delta(DimBin *bin, const char *data_dir,
                                  uint64_t *out_bytes) {
    if (!bin || !data_dir) return GRAVELDB_ERR_INVALID;

    DirtyTracker *dt = &bin->dirty;

    char ckpt_dir[512];
    snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/ckpt", data_dir);
    ensure_dir(ckpt_dir);

    char delta_path[512];
    snprintf(delta_path, sizeof(delta_path), "%s/ckpt/gen_%04llu_d%d_delta.bin",
             data_dir, (unsigned long long)dt->generation, bin->dim);

    int fd = open(delta_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return GRAVELDB_ERR_IO;

    uint32_t *dirty_pages = (uint32_t *)malloc(GRAVELDB_MAX_DIRTY * sizeof(uint32_t));
    if (!dirty_pages) { close(fd); return GRAVELDB_ERR_OOM; }
    int n = dirty_tracker_scan_ckpt(dt, dirty_pages, GRAVELDB_MAX_DIRTY);

    DeltaHeader hdr = {
        .magic       = GRAVELDB_DELTA_MAGIC,
        .version     = 1,
        .generation  = dt->generation,
        .dim         = (uint32_t)bin->dim,
        .entry_size  = (uint32_t)bin->entry_size,
        .bump_ptr    = bin->bump_ptr,
        .num_entries = 0,
        .checksum    = 0,
    };

    /* Write header placeholder (will patch num_entries + checksum at end) */
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        free(dirty_pages);
        close(fd);
        return GRAVELDB_ERR_IO;
    }

    /* Streaming CRC: accumulate over all data written after header */
    uint32_t running_crc = 0xFFFFFFFF;
    uint64_t bytes_written = sizeof(hdr);

    uint32_t entries = 0;
    int idx = 0;
    while (idx < n) {
        int j = idx + 1;
        while (j < n && dirty_pages[j] == dirty_pages[j - 1] + 1) j++;

        uint32_t pg_start = dirty_pages[idx];
        uint32_t pg_count = (uint32_t)(j - idx);
        size_t byte_len = (size_t)pg_count * bin->page_size;
        uint32_t entry[2] = { pg_start, pg_count };
        void *buf = NULL;
        if (posix_memalign(&buf, 4096, byte_len) == 0) {
            ssize_t rd = pread(bin->fd, buf, byte_len, (off_t)pg_start * bin->page_size);
            if (rd == (ssize_t)byte_len) {
                if (write(fd, entry, sizeof(entry)) == sizeof(entry)) {
                    if (write(fd, buf, byte_len) == (ssize_t)byte_len) {
                        /* Update streaming CRC with entry header + data */
                        running_crc = crc32_update(running_crc, entry, sizeof(entry));
                        running_crc = crc32_update(running_crc, buf, byte_len);
                        bytes_written += sizeof(entry) + byte_len;
                        entries++;
                    }
                }
            }
            free(buf);
        }
        idx = j;
    }

    /* Patch num_entries in header */
    if (pwrite(fd, &entries, sizeof(uint32_t),
               __builtin_offsetof(DeltaHeader, num_entries)) != sizeof(uint32_t)) {
        close(fd);
        free(dirty_pages);
        return GRAVELDB_ERR_IO;
    }

    /* Finalize CRC: include header fields (magic..bump_ptr + num_entries) in checksum */
    hdr.num_entries = entries;
    uint32_t hdr_crc = crc32_simple(&hdr, sizeof(hdr) - sizeof(uint32_t));
    /* Combine header CRC with body CRC for a single file-wide checksum */
    uint32_t body_crc = ~running_crc;
    uint32_t combined_crc = hdr_crc ^ body_crc;
    if (pwrite(fd, &combined_crc, sizeof(uint32_t),
               __builtin_offsetof(DeltaHeader, checksum)) != sizeof(uint32_t)) {
        close(fd);
        free(dirty_pages);
        return GRAVELDB_ERR_IO;
    }

    fdatasync(fd);
    close(fd);

    free(dirty_pages);
    if (out_bytes) *out_bytes = bytes_written;
    return GRAVELDB_OK;
}

graveldb_status_t ckpt_dump_full(DimBin *bin, const char *data_dir) {
    if (!bin || !data_dir) return GRAVELDB_ERR_INVALID;

    DirtyTracker *dt = &bin->dirty;
    char full_dir[512];
    snprintf(full_dir, sizeof(full_dir), "%s/ckpt/gen_%04llu_full",
             data_dir, (unsigned long long)dt->generation);
    ensure_dir(full_dir);
    char dst_path[512];
    snprintf(dst_path, sizeof(dst_path), "%s/emb_d%d.bin", full_dir, bin->dim);

    int dst_fd = open(dst_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dst_fd < 0) return GRAVELDB_ERR_IO;
    struct stat st;
    if (fstat(bin->fd, &st) < 0) {
        close(dst_fd);
        return GRAVELDB_ERR_IO;
    }

    size_t remaining = (size_t)st.st_size;
    off_t offset = 0;
    size_t buf_size = 4 * 1024 * 1024;
    void *buf = NULL;

    if (posix_memalign(&buf, 4096, buf_size) != 0) {
        close(dst_fd);
        return GRAVELDB_ERR_OOM;
    }

    while (remaining > 0) {
        size_t chunk = remaining < buf_size ? remaining : buf_size;
        ssize_t rd = pread(bin->fd, buf, chunk, offset);
        if (rd <= 0) break;

        ssize_t wr = write(dst_fd, buf, (size_t)rd);
        if (wr != rd) break;

        offset += rd;
        remaining -= (size_t)rd;
    }

    free(buf);
    fdatasync(dst_fd);
    close(dst_fd);

    return (remaining == 0) ? GRAVELDB_OK : GRAVELDB_ERR_IO;
}

/*
 * Footer layout at end of file:
 *   [Footer A (64B)][Footer B (64B)]
 *
 * We alternate writing A/B. On read, pick the one with valid checksum
 * and higher generation.
 */

#define FOOTER_SIZE   sizeof(FileFooter)
#define DUAL_FOOTER   (2 * FOOTER_SIZE)

graveldb_status_t ckpt_persist_footer(DimBin *bin, uint64_t num_entries,
                                      uint64_t generation) {
    if (!bin) return GRAVELDB_ERR_INVALID;
    uint64_t data_end = bin->bump_ptr * bin->entry_size;
    uint64_t free_list_offset = data_end;
    uint64_t free_list_size = (uint64_t)bin->free_count * sizeof(uint32_t);

    if (bin->free_count > 0) {
        if (pwrite(bin->fd, bin->free_list, (size_t)free_list_size,
                   (off_t)free_list_offset) < 0) {
            return GRAVELDB_ERR_IO;
        }
    }
    FileFooter footer = {
        .magic            = GRAVELDB_FOOTER_MAGIC,
        .version          = 2,
        .dim              = (uint32_t)bin->dim,
        ._pad0            = 0,
        .num_entries      = num_entries,
        .bump_ptr         = bin->bump_ptr,
        .free_list_offset = free_list_offset,
        .free_list_size   = free_list_size,
        .generation       = generation,
        .checksum         = 0,
        ._pad1            = 0,
    };

    footer.checksum = crc32_simple(&footer,
                                   __builtin_offsetof(FileFooter, checksum));
    off_t footer_base = (off_t)(free_list_offset + free_list_size);
    off_t slot_offset = (generation % 2 == 0)
                        ? footer_base
                        : footer_base + (off_t)FOOTER_SIZE;

    if (pwrite(bin->fd, &footer, FOOTER_SIZE, slot_offset) < 0) {
        return GRAVELDB_ERR_IO;
    }
    off_t required_end = footer_base + (off_t)DUAL_FOOTER;
    if (ftruncate(bin->fd, required_end) < 0) {
        return GRAVELDB_ERR_IO;
    }
    fsync(bin->fd);
    fsync(bin->key_fd);

    return GRAVELDB_OK;
}

graveldb_status_t ckpt_read_footer(int fd, FileFooter *out_footer) {
    if (fd < 0 || !out_footer) return GRAVELDB_ERR_INVALID;
    struct stat st;
    if (fstat(fd, &st) < 0) return GRAVELDB_ERR_IO;

    if ((size_t)st.st_size < DUAL_FOOTER) {
        return GRAVELDB_ERR_CORRUPT;
    }

    off_t footer_start = st.st_size - (off_t)DUAL_FOOTER;
    FileFooter footers[2];

    ssize_t rd = pread(fd, footers, DUAL_FOOTER, footer_start);
    if (rd != (ssize_t)DUAL_FOOTER) return GRAVELDB_ERR_IO;
    bool valid[2] = { false, false };
    for (int i = 0; i < 2; i++) {
        if (footers[i].magic != GRAVELDB_FOOTER_MAGIC) continue;

        uint32_t saved_crc = footers[i].checksum;
        uint32_t computed_crc = crc32_simple(&footers[i],
                                             __builtin_offsetof(FileFooter, checksum));
        if (saved_crc == computed_crc) {
            valid[i] = true;
        }
    }
    if (valid[0] && valid[1]) {
        *out_footer = (footers[0].generation >= footers[1].generation)
                      ? footers[0] : footers[1];
    } else if (valid[0]) {
        *out_footer = footers[0];
    } else if (valid[1]) {
        *out_footer = footers[1];
    } else {
        return GRAVELDB_ERR_CORRUPT;
    }

    return GRAVELDB_OK;
}

graveldb_status_t ckpt_replay_delta(DimBin *bin, const char *delta_path) {
    if (!bin || !delta_path) return GRAVELDB_ERR_INVALID;

    int fd = open(delta_path, O_RDONLY);
    if (fd < 0) return GRAVELDB_ERR_IO;
    DeltaHeader hdr;
    if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }
    if (hdr.magic != GRAVELDB_DELTA_MAGIC || hdr.version != 1) {
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }

    if ((int)hdr.dim != bin->dim) {
        close(fd);
        return GRAVELDB_ERR_INVALID;
    }
    for (uint32_t e = 0; e < hdr.num_entries; e++) {
        uint32_t entry[2];
        if (read(fd, entry, sizeof(entry)) != sizeof(entry)) break;

        uint32_t pg_start = entry[0];
        uint32_t pg_count = entry[1];
        size_t byte_len = (size_t)pg_count * bin->page_size;

        void *buf = NULL;
        if (posix_memalign(&buf, 4096, byte_len) != 0) break;

        ssize_t rd = read(fd, buf, byte_len);
        if (rd != (ssize_t)byte_len) {
            free(buf);
            break;
        }
        ssize_t wr = pwrite(bin->fd, buf, byte_len, (off_t)pg_start * bin->page_size);
        free(buf);
        if (wr != (ssize_t)byte_len) {
            close(fd);
            return GRAVELDB_ERR_IO;
        }
    }
    if (hdr.bump_ptr > bin->bump_ptr) {
        bin->bump_ptr = hdr.bump_ptr;
        bin->total_entries = hdr.bump_ptr;
        bin->total_pages = (bin->total_entries + bin->entries_per_page - 1)
                           / bin->entries_per_page;
    }

    close(fd);
    return GRAVELDB_OK;
}

graveldb_status_t ckpt_recover(GravelDB *db) {
    if (!db) return GRAVELDB_ERR_INVALID;

    /*
     * Recovery strategy:
     *   1. Each DimBin file may have a valid footer -> restore bump_ptr + free list
     *   2. Scan ckpt/ for delta files with generation > footer.generation
     *   3. Replay in generation order
     *   4. Hash index is rebuilt from .keys files by graveldb_open (not here)
     */

    uint16_t num_slabs = dim_registry_count(&db->dim_reg);

    for (uint16_t i = 0; i < num_slabs; i++) {
        DimBin *bin = dim_registry_get_bin(&db->dim_reg, i);
        FileFooter footer;
        graveldb_status_t rc = ckpt_read_footer(bin->fd, &footer);

        uint64_t base_generation = 0;
        if (rc == GRAVELDB_OK) {
            bin->bump_ptr = footer.bump_ptr;
            bin->total_entries = footer.bump_ptr;
            bin->total_pages = (bin->total_entries + bin->entries_per_page - 1)
                               / bin->entries_per_page;
            base_generation = footer.generation;
            if (footer.free_list_size > 0) {
                uint32_t free_count = (uint32_t)(footer.free_list_size / sizeof(uint32_t));
                if (free_count > bin->free_capacity) {
                    uint32_t *tmp = (uint32_t *)realloc(bin->free_list,
                                                        free_count * sizeof(uint32_t));
                    if (!tmp) continue;
                    bin->free_list = tmp;
                    bin->free_capacity = free_count;
                }
                ssize_t frd = pread(bin->fd, bin->free_list, (size_t)footer.free_list_size,
                                    (off_t)footer.free_list_offset);
                if (frd == (ssize_t)footer.free_list_size) {
                    bin->free_count = free_count;
                }
                /* On short read: leave free_count at 0 (entries leaked but safe) */
            }
        }
        CkptDeltaInfo *deltas = NULL;
        int num_deltas = ckpt_list_deltas(db->data_dir, bin->dim, &deltas);

        for (int d = 0; d < num_deltas; d++) {
            if (deltas[d].generation > base_generation) {
                ckpt_replay_delta(bin, deltas[d].path);
            }
        }

        ckpt_free_deltas(deltas, num_deltas);
    }

    return GRAVELDB_OK;
}

/*
 * Parse delta filename: gen_NNNN_dDDD_delta.bin
 * Returns 1 on success, 0 on parse failure.
 */
static int parse_delta_filename(const char *name, uint64_t *gen, int *dim) {
    unsigned long long g;
    int d;
    if (sscanf(name, "gen_%llu_d%d_delta.bin", &g, &d) == 2) {
        *gen = g;
        *dim = d;
        return 1;
    }
    return 0;
}

static int cmp_delta_info(const void *a, const void *b) {
    const CkptDeltaInfo *da = (const CkptDeltaInfo *)a;
    const CkptDeltaInfo *db_info = (const CkptDeltaInfo *)b;
    if (da->generation < db_info->generation) return -1;
    if (da->generation > db_info->generation) return 1;
    return 0;
}

int ckpt_list_deltas(const char *data_dir, int dim, CkptDeltaInfo **out_deltas) {
    if (!data_dir || !out_deltas) return 0;

    char ckpt_dir[512];
    snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/ckpt", data_dir);

    DIR *dir = opendir(ckpt_dir);
    if (!dir) {
        *out_deltas = NULL;
        return 0;
    }
    int count = 0;
    int capacity = 32;
    CkptDeltaInfo *deltas = (CkptDeltaInfo *)malloc(capacity * sizeof(CkptDeltaInfo));
    if (!deltas) {
        closedir(dir);
        *out_deltas = NULL;
        return 0;
    }

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        uint64_t gen;
        int d;
        if (!parse_delta_filename(ent->d_name, &gen, &d)) continue;
        if (d != dim) continue;
        if (count >= capacity) {
            capacity *= 2;
            deltas = (CkptDeltaInfo *)realloc(deltas, capacity * sizeof(CkptDeltaInfo));
            if (!deltas) {
                closedir(dir);
                *out_deltas = NULL;
                return 0;
            }
        }
        char full_path[1024];
        snprintf(full_path, sizeof(full_path), "%s/%s", ckpt_dir, ent->d_name);

        deltas[count].path = strdup(full_path);
        deltas[count].generation = gen;
        deltas[count].dim = d;
        count++;
    }

    closedir(dir);
    if (count > 1) {
        qsort(deltas, count, sizeof(CkptDeltaInfo), cmp_delta_info);
    }

    *out_deltas = deltas;
    return count;
}

void ckpt_free_deltas(CkptDeltaInfo *deltas, int count) {
    if (!deltas) return;
    for (int i = 0; i < count; i++) {
        free(deltas[i].path);
    }
    free(deltas);
}

graveldb_status_t ckpt_purge_old_deltas(const char *data_dir, int dim,
                                        uint64_t last_full_generation) {
    CkptDeltaInfo *deltas = NULL;
    int count = ckpt_list_deltas(data_dir, dim, &deltas);

    for (int i = 0; i < count; i++) {
        if (deltas[i].generation <= last_full_generation) {
            unlink(deltas[i].path);
        }
    }

    ckpt_free_deltas(deltas, count);
    return GRAVELDB_OK;
}
