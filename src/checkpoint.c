/*
 * GravelDB - Checkpoint: scheduler, delta I/O, meta persistence, recovery.
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
#include "wire.h"

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

    uint16_t num_bins = dim_registry_count(&db->dim_reg);
    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
        ckpt_persist_meta(s, db->data_dir, s->dirty.generation);
    }
    sched->delta_chain_length++;
    sched->total_checkpoints++;

    /* Full checkpoint requested: copy files at this safepoint */
    if (sched->full_pending) {
        graveldb_flush(db);

        for (uint16_t i = 0; i < num_bins; i++) {
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
        for (uint16_t i = 0; i < num_bins; i++) {
            DimBin *s = dim_registry_get_bin(&db->dim_reg, i);
            ckpt_purge_old_deltas(db->data_dir, s->dim, sched->last_full_generation);
        }
    }

    return GRAVELDB_OK;
}

/*
 * Request a full checkpoint (singleflight with cooldown).
 */
graveldb_status_t ckpt_scheduler_request_full(CkptScheduler *sched,
                                              CkptFullResponse *response) {
    if (!sched || !response) return GRAVELDB_ERR_INVALID;

    response->next = NULL;

    /* Cooldown: return recent full result if within window */
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

    /* Encode header to wire format and write placeholder */
    uint8_t hdr_buf[WIRE_DELTA_HDR_SIZE];
    wire_encode_delta_hdr(hdr_buf, hdr.magic, hdr.version, hdr.generation,
                          hdr.dim, hdr.entry_size, hdr.bump_ptr,
                          hdr.num_entries, hdr.checksum);
    if (write(fd, hdr_buf, WIRE_DELTA_HDR_SIZE) != WIRE_DELTA_HDR_SIZE) {
        free(dirty_pages);
        close(fd);
        return GRAVELDB_ERR_IO;
    }

    /* Streaming CRC: accumulate over all data written after header */
    uint32_t running_crc = 0xFFFFFFFF;
    uint64_t bytes_written = WIRE_DELTA_HDR_SIZE;

    uint32_t entries = 0;
    int idx = 0;
    while (idx < n) {
        int j = idx + 1;
        while (j < n && dirty_pages[j] == dirty_pages[j - 1] + 1) j++;

        uint32_t pg_start = dirty_pages[idx];
        uint32_t pg_count = (uint32_t)(j - idx);
        size_t byte_len = (size_t)pg_count * bin->page_size;
        uint8_t entry_buf[WIRE_DELTA_ENTRY_SIZE];
        wire_encode_delta_entry(entry_buf, pg_start, pg_count);
        void *buf = NULL;
        if (posix_memalign(&buf, 4096, byte_len) == 0) {
            ssize_t rd = pread(bin->fd, buf, byte_len, (off_t)pg_start * bin->page_size);
            if (rd == (ssize_t)byte_len) {
                if (write(fd, entry_buf, WIRE_DELTA_ENTRY_SIZE) == WIRE_DELTA_ENTRY_SIZE) {
                    if (write(fd, buf, byte_len) == (ssize_t)byte_len) {
                        /* Update streaming CRC with entry header + data */
                        running_crc = crc32_update(running_crc, entry_buf, WIRE_DELTA_ENTRY_SIZE);
                        running_crc = crc32_update(running_crc, buf, byte_len);
                        bytes_written += WIRE_DELTA_ENTRY_SIZE + byte_len;
                        entries++;
                    }
                }
            }
            free(buf);
        }
        idx = j;
    }

    /* Patch num_entries in header (at wire offset 32) */
    uint8_t ne_buf[4];
    wire_put_u32(ne_buf, entries);
    if (pwrite(fd, ne_buf, 4, 32) != 4) {
        close(fd);
        free(dirty_pages);
        return GRAVELDB_ERR_IO;
    }

    /* Finalize CRC: include header fields in checksum */
    hdr.num_entries = entries;
    uint8_t full_hdr_buf[WIRE_DELTA_HDR_SIZE];
    wire_encode_delta_hdr(full_hdr_buf, hdr.magic, hdr.version, hdr.generation,
                          hdr.dim, hdr.entry_size, hdr.bump_ptr,
                          hdr.num_entries, 0);
    /* CRC of header (excluding checksum field = first 36 bytes) */
    uint32_t hdr_crc = crc32_simple(full_hdr_buf, WIRE_DELTA_HDR_SIZE - 4);
    /* Combine header CRC with body CRC for a single file-wide checksum */
    uint32_t body_crc = ~running_crc;
    uint32_t combined_crc = hdr_crc ^ body_crc;
    uint8_t crc_buf[4];
    wire_put_u32(crc_buf, combined_crc);
    if (pwrite(fd, crc_buf, 4, 36) != 4) {
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
 * Meta file: atomic persist via write+fsync+rename.
 * Path: <data_dir>/emb_d<dim>.meta (16 bytes: magic + generation + checksum)
 */

#define META_MAGIC  0x4D455441  /* "META" */

static void meta_file_path(char *buf, size_t bufsize, const char *data_dir, int dim) {
    snprintf(buf, bufsize, "%s/emb_d%d.meta", data_dir, dim);
}

static void meta_tmp_path(char *buf, size_t bufsize, const char *data_dir, int dim) {
    snprintf(buf, bufsize, "%s/emb_d%d.meta.tmp", data_dir, dim);
}

graveldb_status_t ckpt_persist_meta(DimBin *bin, const char *data_dir,
                                    uint64_t generation) {
    if (!bin || !data_dir) return GRAVELDB_ERR_INVALID;

    /* Encode to wire format then compute CRC over magic + generation (12 bytes) */
    uint8_t buf[WIRE_META_SIZE];
    wire_put_u32(buf + 0, META_MAGIC);
    wire_put_u64(buf + 4, generation);
    uint32_t checksum = crc32_simple(buf, 12); /* CRC of first 12 bytes */
    wire_put_u32(buf + 12, checksum);

    char tmp_path[512];
    char final_path[512];
    meta_tmp_path(tmp_path, sizeof(tmp_path), data_dir, bin->dim);
    meta_file_path(final_path, sizeof(final_path), data_dir, bin->dim);

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return GRAVELDB_ERR_IO;

    ssize_t wr = write(fd, buf, WIRE_META_SIZE);
    if (wr != WIRE_META_SIZE) {
        close(fd);
        unlink(tmp_path);
        return GRAVELDB_ERR_IO;
    }

    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp_path);
        return GRAVELDB_ERR_IO;
    }
    close(fd);

    /* Atomic rename: guarantees .meta is always valid or absent */
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return GRAVELDB_ERR_IO;
    }

    /* fsync the directory to ensure rename is durable */
    int dir_fd = open(data_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return GRAVELDB_OK;
}

graveldb_status_t ckpt_read_meta(const char *data_dir, int dim,
                                 BinMeta *out_meta) {
    if (!data_dir || !out_meta) return GRAVELDB_ERR_INVALID;

    char path[512];
    meta_file_path(path, sizeof(path), data_dir, dim);

    int fd = open(path, O_RDONLY);
    if (fd < 0) return GRAVELDB_ERR_CORRUPT;  /* file missing = fresh DB */

    uint8_t buf[WIRE_META_SIZE];
    ssize_t rd = read(fd, buf, WIRE_META_SIZE);
    close(fd);

    if (rd != WIRE_META_SIZE) return GRAVELDB_ERR_CORRUPT;

    uint32_t magic, checksum;
    uint64_t generation;
    wire_decode_meta(buf, &magic, &generation, &checksum);

    if (magic != META_MAGIC) {
        /* Detect endianness mismatch: if the magic matches byte-swapped,
         * this file was likely created on a platform with opposite endianness. */
        if (magic == wire_bswap32(META_MAGIC)) {
            fprintf(stderr, "graveldb: meta file endianness mismatch — "
                    "data was written on a big-endian platform and cannot be "
                    "read on this little-endian host (or vice versa)\n");
        }
        return GRAVELDB_ERR_CORRUPT;
    }

    /* Verify CRC over first 12 bytes (magic + generation) */
    uint32_t expected_crc = crc32_simple(buf, 12);
    if (checksum != expected_crc) return GRAVELDB_ERR_CORRUPT;

    out_meta->magic = magic;
    out_meta->generation = generation;
    out_meta->checksum = checksum;
    return GRAVELDB_OK;
}

graveldb_status_t ckpt_replay_delta(DimBin *bin, const char *delta_path) {
    if (!bin || !delta_path) return GRAVELDB_ERR_INVALID;

    int fd = open(delta_path, O_RDONLY);
    if (fd < 0) return GRAVELDB_ERR_IO;

    uint8_t hdr_buf[WIRE_DELTA_HDR_SIZE];
    if (read(fd, hdr_buf, WIRE_DELTA_HDR_SIZE) != WIRE_DELTA_HDR_SIZE) {
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }

    DeltaHeader hdr;
    wire_decode_delta_hdr(hdr_buf, &hdr.magic, &hdr.version, &hdr.generation,
                          &hdr.dim, &hdr.entry_size, &hdr.bump_ptr,
                          &hdr.num_entries, &hdr.checksum);

    if (hdr.magic != GRAVELDB_DELTA_MAGIC || hdr.version != 1) {
        if (hdr.magic == wire_bswap32(GRAVELDB_DELTA_MAGIC)) {
            fprintf(stderr, "graveldb: delta file endianness mismatch — "
                    "data was written on a platform with opposite byte order\n");
        }
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }

    if ((int)hdr.dim != bin->dim) {
        close(fd);
        return GRAVELDB_ERR_INVALID;
    }
    for (uint32_t e = 0; e < hdr.num_entries; e++) {
        uint8_t entry_buf[WIRE_DELTA_ENTRY_SIZE];
        if (read(fd, entry_buf, WIRE_DELTA_ENTRY_SIZE) != WIRE_DELTA_ENTRY_SIZE) break;

        uint32_t pg_start, pg_count;
        wire_decode_delta_entry(entry_buf, &pg_start, &pg_count);
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

    /* Replay delta files with generation > base (from .meta) */

    uint16_t num_bins = dim_registry_count(&db->dim_reg);

    for (uint16_t i = 0; i < num_bins; i++) {
        DimBin *bin = dim_registry_get_bin(&db->dim_reg, i);
        BinMeta meta;
        graveldb_status_t rc = ckpt_read_meta(db->data_dir, bin->dim, &meta);

        uint64_t base_generation = 0;
        if (rc == GRAVELDB_OK) {
            base_generation = meta.generation;
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

/*
 * Unified Checkpoint Export Implementation (io_uring async)
 *
 * Both full and delta use the same entry-interleaved format.
 * SATB isolation via overlay: main file holds frozen-time data,
 * concurrent writes go to overlay (not seen by export scan).
 *
 * IO strategy (sequential scan + coalesced reads + pipeline):
 *   Phase 1 (KEY_READ): batch-read keys file sequentially via io_uring
 *   Phase 2 (EMB_READ): coalesce adjacent valid entries, submit merged
 *                        embedding reads (eliminates random IO amplification)
 *   Phase 3 (OUTPUT):   write completed entries via write_cb
 *
 * Key reads and embedding reads can overlap across steps in the event loop,
 * achieving pipeline parallelism without blocking the host thread.
 */

/*
 * Write the export header to the dump file.
 */
static graveldb_status_t export_write_header(struct GravelDBCkptExport *exp) {
    uint8_t hdr[WIRE_CKPT_EXPORT_HDR_SIZE];
    /* Wire type: FULL=1, DELTA=2; public API: FULL=0, DELTA=1 → wire = api + 1 */
    wire_encode_ckpt_export_hdr(hdr,
        GRAVELDB_CKPT_EXPORT_MAGIC, 1, exp->type + 1,
        exp->generation, (uint32_t)exp->bin->dim,
        (uint32_t)exp->bin->entry_size,
        0,  /* num_entries: placeholder */
        exp->base_gen, 0);

    ssize_t wr = write(exp->dump_fd, hdr, WIRE_CKPT_EXPORT_HDR_SIZE);
    if (wr != WIRE_CKPT_EXPORT_HDR_SIZE) return GRAVELDB_ERR_IO;
    exp->dump_size += WIRE_CKPT_EXPORT_HDR_SIZE;
    exp->header_written = true;
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_ckpt_export_begin(GravelDB *db, GravelDBCkptExport **out,
                                             int type, int dim, uint64_t base_gen,
                                             uint32_t batch_entries, uint32_t flags) {
    if (!db || !out) return GRAVELDB_ERR_INVALID;
    if (type != GRAVELDB_CKPT_FULL && type != GRAVELDB_CKPT_DELTA)
        return GRAVELDB_ERR_INVALID;

    int dim_idx = dim_registry_find(&db->dim_reg, dim);
    if (dim_idx < 0) return GRAVELDB_ERR_NOT_FOUND;
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, (uint16_t)dim_idx);

    if (batch_entries == 0) batch_entries = CKPT_EXPORT_IO_BATCH;

    struct GravelDBCkptExport *exp = (struct GravelDBCkptExport *)calloc(1, sizeof(*exp));
    if (!exp) return GRAVELDB_ERR_OOM;

    exp->db = db;
    exp->bin = bin;
    exp->cursor = 0;
    exp->snapshot_bump = (uint32_t)bin->bump_ptr;
    exp->generation = bin->dirty.generation;
    exp->base_gen = base_gen;
    exp->num_entries = 0;
    exp->type = (uint32_t)type;
    exp->batch_entries = batch_entries;
    exp->header_written = false;
    exp->scan_done = false;
    exp->dirty_pages = NULL;
    exp->dirty_page_count = 0;
    exp->dirty_page_cursor = 0;
    exp->phase = CKPT_EXPORT_PHASE_IDLE;
    exp->key_batch_buf = NULL;
    exp->key_batch_start = 0;
    exp->key_batch_count = 0;
    exp->coalesced_ranges = NULL;
    exp->range_count = 0;
    exp->range_capacity = 0;
    exp->read_ops = NULL;
    exp->read_op_count = 0;
    exp->read_buf_pool = NULL;
    exp->dump_fd = -1;
    exp->dump_size = 0;
    exp->read_cursor = 0;

    /* Create dump file in the ckpt directory */
    char ckpt_dir[512];
    snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/ckpt", db->data_dir);
    ensure_dir(ckpt_dir);

    const char *type_str = (type == GRAVELDB_CKPT_FULL) ? "full" : "delta";
    snprintf(exp->dump_path, sizeof(exp->dump_path),
             "%s/ckpt/export_gen%04llu_d%d_%s.dump",
             db->data_dir, (unsigned long long)bin->dirty.generation,
             bin->dim, type_str);

    exp->dump_fd = open(exp->dump_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (exp->dump_fd < 0) {
        free(exp);
        return GRAVELDB_ERR_IO;
    }

    /* Initialize io_uring for reads */
    if (uring_io_init(&exp->io_ctx) != 0) {
        exp->io_ctx.initialized = false;
    }

    /* Key batch buffer: batch_entries keys (uint64_t each) */
    exp->key_batch_buf = (uint64_t *)malloc((size_t)batch_entries * sizeof(uint64_t));
    if (!exp->key_batch_buf) {
        close(exp->dump_fd);
        uring_io_destroy(&exp->io_ctx);
        free(exp);
        return GRAVELDB_ERR_OOM;
    }

    /* Per-entry read ops array */
    exp->read_ops = (CkptExportReadOp *)malloc(batch_entries * sizeof(CkptExportReadOp));
    if (!exp->read_ops) {
        free(exp->key_batch_buf);
        close(exp->dump_fd);
        uring_io_destroy(&exp->io_ctx);
        free(exp);
        return GRAVELDB_ERR_OOM;
    }

    /* Embedding read buffer pool */
    exp->read_buf_pool = (uint8_t *)malloc((size_t)batch_entries * bin->entry_size);
    if (!exp->read_buf_pool) {
        free(exp->read_ops);
        free(exp->key_batch_buf);
        close(exp->dump_fd);
        uring_io_destroy(&exp->io_ctx);
        free(exp);
        return GRAVELDB_ERR_OOM;
    }

    /* Coalesced ranges: worst case is batch_entries ranges (no merging) */
    exp->range_capacity = (int)batch_entries;
    exp->coalesced_ranges = (CkptCoalescedRange *)malloc(
        (size_t)exp->range_capacity * sizeof(CkptCoalescedRange));
    if (!exp->coalesced_ranges) {
        free(exp->read_buf_pool);
        free(exp->read_ops);
        free(exp->key_batch_buf);
        close(exp->dump_fd);
        uring_io_destroy(&exp->io_ctx);
        free(exp);
        return GRAVELDB_ERR_OOM;
    }

    /* Delta mode: pre-scan frozen dirty bitmap */
    if (type == GRAVELDB_CKPT_DELTA) {
        exp->dirty_pages = (uint32_t *)malloc(GRAVELDB_MAX_DIRTY * sizeof(uint32_t));
        if (!exp->dirty_pages) {
            free(exp->coalesced_ranges);
            free(exp->read_buf_pool);
            free(exp->read_ops);
            free(exp->key_batch_buf);
            close(exp->dump_fd);
            uring_io_destroy(&exp->io_ctx);
            free(exp);
            return GRAVELDB_ERR_OOM;
        }
        exp->dirty_page_count = dirty_tracker_scan_ckpt(&bin->dirty,
                                    exp->dirty_pages, GRAVELDB_MAX_DIRTY);
    }

    /* Handle ALSO_FULL: create an internal full export driven in lockstep */
    exp->paired_full = NULL;
    if ((flags & GRAVELDB_CKPT_ALSO_FULL) && type == GRAVELDB_CKPT_DELTA) {
        graveldb_status_t rc = graveldb_ckpt_export_begin(db, &exp->paired_full,
                                   GRAVELDB_CKPT_FULL, dim, 0, batch_entries, 0);
        if (rc != GRAVELDB_OK) {
            /* Cleanup delta resources on failure */
            free(exp->dirty_pages);
            free(exp->coalesced_ranges);
            free(exp->read_buf_pool);
            free(exp->read_ops);
            free(exp->key_batch_buf);
            close(exp->dump_fd);
            uring_io_destroy(&exp->io_ctx);
            free(exp);
            return rc;
        }
    }

    *out = exp;
    return GRAVELDB_OK;
}

/*
 * Build coalesced ranges from valid entry indices.
 * Adjacent entries (entry_idx diff == 1) are merged into a single IO range.
 * This converts O(N) random reads into O(R) sequential reads where R << N.
 */
static void export_build_coalesced_ranges(struct GravelDBCkptExport *exp) {
    exp->range_count = 0;
    if (exp->read_op_count == 0) return;

    DimBin *bin = exp->bin;
    size_t entry_size = bin->entry_size;

    /* First entry starts a new range */
    CkptExportReadOp *ops = exp->read_ops;
    int rc = 0;
    exp->coalesced_ranges[0].start_idx = ops[0].entry_idx;
    exp->coalesced_ranges[0].count = 1;
    exp->coalesced_ranges[0].buf = exp->read_buf_pool;
    ops[0].buf = exp->read_buf_pool;
    rc = 1;

    for (int i = 1; i < exp->read_op_count; i++) {
        CkptCoalescedRange *cur = &exp->coalesced_ranges[rc - 1];
        uint32_t expected_next = cur->start_idx + cur->count;

        if (ops[i].entry_idx == expected_next) {
            /* Contiguous: extend current range */
            cur->count++;
        } else {
            /* Gap: start new range */
            rc++;
            exp->coalesced_ranges[rc - 1].start_idx = ops[i].entry_idx;
            exp->coalesced_ranges[rc - 1].count = 1;
            exp->coalesced_ranges[rc - 1].buf = exp->read_buf_pool +
                (size_t)i * entry_size;
        }
        ops[i].buf = exp->read_buf_pool + (size_t)i * entry_size;
    }
    exp->range_count = rc;
}

static graveldb_status_t export_step_single(struct GravelDBCkptExport *exp) {
    if (!exp || !exp->bin) return GRAVELDB_ERR_INVALID;
    if (exp->scan_done && exp->phase == CKPT_EXPORT_PHASE_IDLE) return GRAVELDB_OK;

    /* Write header on first step */
    if (!exp->header_written) {
        graveldb_status_t rc = export_write_header(exp);
        if (rc != GRAVELDB_OK) return rc;
    }

    DimBin *bin = exp->bin;
    uint32_t batch = exp->batch_entries;

    switch (exp->phase) {
    case CKPT_EXPORT_PHASE_IDLE: {
        if (exp->scan_done) return GRAVELDB_OK;

        /* Phase 1: Submit bulk key read via io_uring */
        uring_io_reset(&exp->io_ctx);
        exp->read_op_count = 0;
        exp->range_count = 0;

        if (exp->type == GRAVELDB_CKPT_FULL) {
            /* Full: read a batch of keys sequentially */
            uint32_t end = exp->cursor + batch;
            if (end > exp->snapshot_bump) end = exp->snapshot_bump;
            uint32_t count = end - exp->cursor;

            exp->key_batch_start = exp->cursor;
            exp->key_batch_count = count;

            /* Clamp read to actual key file size */
            off_t key_off = (off_t)exp->cursor * 8;
            size_t key_read_len = (size_t)count * 8;
            if ((size_t)key_off + key_read_len > bin->key_file_size) {
                if ((size_t)key_off >= bin->key_file_size) {
                    key_read_len = 0;
                } else {
                    key_read_len = bin->key_file_size - (size_t)key_off;
                }
                /* Zero out the rest */
                memset((uint8_t *)exp->key_batch_buf + key_read_len, 0,
                       (size_t)count * 8 - key_read_len);
            }

            if (key_read_len > 0) {
                uring_io_submit_read(&exp->io_ctx, bin->key_fd,
                                     exp->key_batch_buf, key_read_len, key_off);
            }

            exp->cursor = end;
            if (exp->cursor >= exp->snapshot_bump) {
                exp->scan_done = true;
            }
        } else {
            /* Delta: gather entries from dirty pages, read keys in batch.
             * Collect entry indices covered by dirty pages, then read their keys. */
            uint32_t entries_per_page = (uint32_t)bin->entries_per_page;
            uint32_t collected = 0;

            /* We need contiguous key read, so track min/max entry_idx range */
            uint32_t range_start = UINT32_MAX;
            uint32_t range_end = 0;

            while (exp->dirty_page_cursor < exp->dirty_page_count &&
                   collected < batch) {
                uint32_t pg = exp->dirty_pages[exp->dirty_page_cursor];
                uint32_t base_entry = pg * entries_per_page;
                uint32_t page_end = base_entry + entries_per_page;
                if (page_end > exp->snapshot_bump) page_end = exp->snapshot_bump;

                if (base_entry < range_start) range_start = base_entry;
                if (page_end > range_end) range_end = page_end;
                collected += (page_end - base_entry);
                exp->dirty_page_cursor++;

                /* Stop if we'd exceed batch */
                if (collected >= batch) break;
            }

            if (range_start == UINT32_MAX) {
                exp->scan_done = true;
                return GRAVELDB_OK;
            }

            /* Clamp to batch size */
            uint32_t count = range_end - range_start;
            if (count > batch) count = batch;

            exp->key_batch_start = range_start;
            exp->key_batch_count = count;

            off_t key_off = (off_t)range_start * 8;
            size_t key_read_len = (size_t)count * 8;
            if ((size_t)key_off + key_read_len > bin->key_file_size) {
                if ((size_t)key_off >= bin->key_file_size) {
                    key_read_len = 0;
                } else {
                    key_read_len = bin->key_file_size - (size_t)key_off;
                }
                memset((uint8_t *)exp->key_batch_buf + key_read_len, 0,
                       (size_t)count * 8 - key_read_len);
            }

            if (key_read_len > 0) {
                uring_io_submit_read(&exp->io_ctx, bin->key_fd,
                                     exp->key_batch_buf, key_read_len, key_off);
            }

            if (exp->dirty_page_cursor >= exp->dirty_page_count) {
                exp->scan_done = true;
            }
        }

        exp->phase = CKPT_EXPORT_PHASE_KEY_READ;
        return GRAVELDB_AGAIN;
    }

    case CKPT_EXPORT_PHASE_KEY_READ: {
        /* Poll key read completion */
        int still = uring_io_poll(&exp->io_ctx);
        if (still > 0) return GRAVELDB_AGAIN;

        /* Key read complete. Scan keys to identify valid entries. */
        int op_idx = 0;
        for (uint32_t i = 0; i < exp->key_batch_count && op_idx < (int)batch; i++) {
            uint64_t feat_id = exp->key_batch_buf[i];
            if (feat_id == 0) continue;

            CkptExportReadOp *op = &exp->read_ops[op_idx];
            op->feat_id = feat_id;
            op->entry_idx = exp->key_batch_start + i;
            op_idx++;
        }
        exp->read_op_count = op_idx;

        if (op_idx == 0) {
            /* No valid entries in this batch, go back to idle for next batch */
            exp->phase = CKPT_EXPORT_PHASE_IDLE;
            return exp->scan_done ? GRAVELDB_OK : GRAVELDB_AGAIN;
        }

        /* Build coalesced ranges and submit merged embedding reads */
        export_build_coalesced_ranges(exp);

        uring_io_reset(&exp->io_ctx);
        size_t entry_size = bin->entry_size;

        for (int r = 0; r < exp->range_count; r++) {
            CkptCoalescedRange *rng = &exp->coalesced_ranges[r];
            size_t read_len = (size_t)rng->count * entry_size;
            off_t offset = (off_t)rng->start_idx * (off_t)entry_size;
            uring_io_submit_read(&exp->io_ctx, bin->fd,
                                 rng->buf, read_len, offset);
        }

        exp->phase = CKPT_EXPORT_PHASE_EMB_READ;
        return GRAVELDB_AGAIN;
    }

    case CKPT_EXPORT_PHASE_EMB_READ: {
        /* Poll embedding read completion */
        int still = uring_io_poll(&exp->io_ctx);
        if (still > 0) return GRAVELDB_AGAIN;

        /* All embedding reads complete, transition to output */
        exp->phase = CKPT_EXPORT_PHASE_OUTPUT;
        return GRAVELDB_AGAIN;
    }

    case CKPT_EXPORT_PHASE_OUTPUT: {
        /* Output all entries to dump file (local IO, no backpressure) */
        uint8_t feat_wire[8];

        for (int i = 0; i < exp->read_op_count; i++) {
            CkptExportReadOp *op = &exp->read_ops[i];

            wire_put_u64(feat_wire, op->feat_id);
            ssize_t w1 = write(exp->dump_fd, feat_wire, 8);
            if (w1 != 8) return GRAVELDB_ERR_IO;
            ssize_t w2 = write(exp->dump_fd, op->buf, bin->entry_size);
            if (w2 != (ssize_t)bin->entry_size) return GRAVELDB_ERR_IO;

            exp->dump_size += 8 + bin->entry_size;
            exp->num_entries++;
        }

        exp->read_op_count = 0;
        exp->range_count = 0;
        exp->phase = CKPT_EXPORT_PHASE_IDLE;
        return exp->scan_done ? GRAVELDB_OK : GRAVELDB_AGAIN;
    }
    }

    return GRAVELDB_ERR_INVALID;
}

graveldb_status_t graveldb_ckpt_export_step(GravelDBCkptExport *exp) {
    if (!exp) return GRAVELDB_ERR_INVALID;

    graveldb_status_t rc = export_step_single(exp);
    if (rc < 0) return rc;

    /* Drive paired full export in lockstep */
    if (exp->paired_full) {
        graveldb_status_t rc_full = export_step_single(exp->paired_full);
        if (rc_full < 0) return rc_full;
        /* AGAIN if either still has work */
        if (rc == GRAVELDB_OK && rc_full == GRAVELDB_OK)
            return GRAVELDB_OK;
        return GRAVELDB_AGAIN;
    }

    return rc;
}

graveldb_status_t graveldb_ckpt_export_poll(GravelDBCkptExport *exp) {
    if (!exp) return GRAVELDB_ERR_INVALID;
    /* Pipeline phases are now driven entirely by step().
     * poll() advances the state machine (same as step) for backward compat. */
    return graveldb_ckpt_export_step(exp);
}

graveldb_status_t graveldb_ckpt_export_end(GravelDBCkptExport *exp) {
    if (!exp) return GRAVELDB_ERR_INVALID;

    /* Write trailer: magic + final entry count */
    uint8_t trailer[12];
    wire_put_u32(trailer + 0, GRAVELDB_CKPT_EXPORT_MAGIC);
    wire_put_u64(trailer + 4, exp->num_entries);
    ssize_t wr = write(exp->dump_fd, trailer, 12);
    exp->dump_size += 12;

    /* fsync to guarantee durability */
    fdatasync(exp->dump_fd);

    /* Cleanup internal buffers (keep dump_fd open for reads) */
    free(exp->dirty_pages);
    free(exp->coalesced_ranges);
    free(exp->read_ops);
    free(exp->read_buf_pool);
    free(exp->key_batch_buf);
    uring_io_destroy(&exp->io_ctx);

    exp->dirty_pages = NULL;
    exp->coalesced_ranges = NULL;
    exp->read_ops = NULL;
    exp->read_buf_pool = NULL;
    exp->key_batch_buf = NULL;

    /* End paired full export if present */
    if (exp->paired_full) {
        graveldb_status_t rc_full = graveldb_ckpt_export_end(exp->paired_full);
        if (wr == 12 && rc_full != GRAVELDB_OK) return rc_full;
    }

    return (wr == 12) ? GRAVELDB_OK : GRAVELDB_ERR_IO;
}

const char *graveldb_ckpt_export_path(const GravelDBCkptExport *exp) {
    if (!exp) return NULL;
    return exp->dump_path;
}

const char *graveldb_ckpt_export_full_path(const GravelDBCkptExport *exp) {
    if (!exp || !exp->paired_full) return NULL;
    return exp->paired_full->dump_path;
}

uint64_t graveldb_ckpt_export_size(const GravelDBCkptExport *exp) {
    if (!exp) return 0;
    return exp->dump_size;
}

ssize_t graveldb_ckpt_export_read(GravelDBCkptExport *exp, void *buf, size_t len) {
    if (!exp || !buf || len == 0) return -1;

    /* Open a read fd if first call (dump_fd is write-only) */
    ssize_t rd = pread(exp->dump_fd, buf, len, exp->read_cursor);
    if (rd < 0) return -1;
    exp->read_cursor += rd;
    return rd;
}

void graveldb_ckpt_export_read_reset(GravelDBCkptExport *exp) {
    if (!exp) return;
    exp->read_cursor = 0;
}

/* Release export context and close dump file. Call after done reading. */
void graveldb_ckpt_export_destroy(GravelDBCkptExport *exp) {
    if (!exp) return;
    if (exp->paired_full) {
        graveldb_ckpt_export_destroy(exp->paired_full);
        exp->paired_full = NULL;
    }
    if (exp->dump_fd >= 0) {
        close(exp->dump_fd);
        exp->dump_fd = -1;
    }
    free(exp);
}

/*
 * Checkpoint Import (io_uring async writes)
 */

graveldb_status_t graveldb_ckpt_import_begin(GravelDB *db, GravelDBCkptImport **out,
                                             int dim, const char *path) {
    if (!db || !out || !path) return GRAVELDB_ERR_INVALID;

    int dim_idx = dim_registry_find(&db->dim_reg, dim);
    if (dim_idx < 0) return GRAVELDB_ERR_NOT_FOUND;
    DimBin *bin = dim_registry_get_bin(&db->dim_reg, (uint16_t)dim_idx);

    struct GravelDBCkptImport *imp = (struct GravelDBCkptImport *)calloc(1, sizeof(*imp));
    if (!imp) return GRAVELDB_ERR_OOM;

    imp->db = db;
    imp->bin = bin;
    imp->header_read = false;
    imp->done = false;
    imp->dim = (uint32_t)dim;
    imp->entry_size = (uint32_t)bin->entry_size;
    imp->generation = 0;
    imp->pending_writes = 0;

    /* Open dump file for reading */
    imp->import_fd = open(path, O_RDONLY);
    if (imp->import_fd < 0) {
        free(imp);
        return GRAVELDB_ERR_IO;
    }

    if (uring_io_init(&imp->io_ctx) != 0) {
        imp->io_ctx.initialized = false;
    }

    /* Pre-allocate batch buffer for async writes */
    imp->write_buf_pool = (uint8_t *)malloc((size_t)CKPT_IMPORT_BATCH * bin->entry_size);
    if (!imp->write_buf_pool) {
        close(imp->import_fd);
        uring_io_destroy(&imp->io_ctx);
        free(imp);
        return GRAVELDB_ERR_OOM;
    }

    *out = imp;
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_ckpt_import_step(GravelDBCkptImport *imp) {
    if (!imp) return GRAVELDB_ERR_INVALID;
    if (imp->done) return GRAVELDB_OK;

    DimBin *bin = imp->bin;

    /* Read header on first call */
    if (!imp->header_read) {
        uint8_t hdr_buf[WIRE_CKPT_EXPORT_HDR_SIZE];
        ssize_t rd = read(imp->import_fd, hdr_buf, WIRE_CKPT_EXPORT_HDR_SIZE);
        if (rd != WIRE_CKPT_EXPORT_HDR_SIZE) return GRAVELDB_ERR_CORRUPT;

        uint32_t magic, version, type, dim, entry_size, checksum;
        uint64_t generation, num_entries, base_gen;
        wire_decode_ckpt_export_hdr(hdr_buf, &magic, &version, &type,
                                    &generation, &dim, &entry_size,
                                    &num_entries, &base_gen, &checksum);

        if (magic != GRAVELDB_CKPT_EXPORT_MAGIC || version != 1)
            return GRAVELDB_ERR_CORRUPT;
        if ((int)dim != bin->dim || entry_size != (uint32_t)bin->entry_size)
            return GRAVELDB_ERR_INVALID;

        imp->generation = generation;
        imp->header_read = true;
    }

    /* Reset io context for this step */
    uring_io_reset(&imp->io_ctx);
    imp->pending_writes = 0;

    /* Read and apply a batch of entries from dump file */
    uint8_t feat_wire[8];
    uint32_t entry_size = imp->entry_size;
    int count = 0;

    while (count < CKPT_IMPORT_BATCH) {
        ssize_t r1 = read(imp->import_fd, feat_wire, 8);
        if (r1 < 8) {
            imp->done = true;
            break;
        }

        /* Check for trailer magic */
        uint32_t maybe_magic = wire_get_u32(feat_wire);
        if (maybe_magic == GRAVELDB_CKPT_EXPORT_MAGIC) {
            uint8_t trail_rest[4];
            read(imp->import_fd, trail_rest, 4);
            imp->done = true;
            break;
        }

        uint64_t feat_id = wire_get_u64(feat_wire);

        /* Read embedding directly into the batch buffer pool slot */
        uint8_t *embed_buf = imp->write_buf_pool + (size_t)count * entry_size;
        ssize_t r2 = read(imp->import_fd, embed_buf, entry_size);
        if (r2 != (ssize_t)entry_size) {
            imp->done = true;
            break;
        }

        /* Allocate entry and submit async write */
        uint32_t entry_idx = dimbin_alloc_entry(bin);
        if (entry_idx == UINT32_MAX) {
            return GRAVELDB_ERR_FULL;
        }

        off_t data_off = (off_t)entry_idx * (off_t)entry_size;
        uring_io_submit_write(&imp->io_ctx, bin->fd, embed_buf, entry_size, data_off);
        imp->pending_writes++;

        /* Write key synchronously (8 bytes, negligible) */
        off_t key_off = (off_t)entry_idx * 8;
        pwrite(bin->key_fd, &feat_id, 8, key_off);

        /* Update hash index */
        int dim_idx = dim_registry_find(&imp->db->dim_reg, bin->dim);
        if (dim_idx >= 0) {
            hash_index_put(&imp->db->index, feat_id, (uint16_t)dim_idx, entry_idx);
        }

        count++;
    }

    /* Submit fsyncs for writes in this batch */
    if (imp->pending_writes > 0) {
        uring_io_submit_fsyncs(&imp->io_ctx);
    }

    return imp->done ? GRAVELDB_OK : GRAVELDB_AGAIN;
}

graveldb_status_t graveldb_ckpt_import_poll(GravelDBCkptImport *imp) {
    if (!imp) return GRAVELDB_ERR_INVALID;
    if (imp->pending_writes == 0) return GRAVELDB_OK;

    int still_pending = uring_io_poll(&imp->io_ctx);
    if (still_pending > 0) return GRAVELDB_AGAIN;

    if (imp->io_ctx.errors > 0) return GRAVELDB_ERR_IO;
    imp->pending_writes = 0;
    return GRAVELDB_OK;
}

graveldb_status_t graveldb_ckpt_import_end(GravelDBCkptImport *imp) {
    if (!imp) return GRAVELDB_ERR_INVALID;

    /* Wait for any remaining IO */
    if (imp->pending_writes > 0) {
        uring_io_wait(&imp->io_ctx);
    }

    if (imp->import_fd >= 0) {
        close(imp->import_fd);
        imp->import_fd = -1;
    }

    uring_io_destroy(&imp->io_ctx);
    free(imp->write_buf_pool);
    free(imp);
    return GRAVELDB_OK;
}

/*
 * Checkpoint Dump Parser — standalone, no GravelDB instance needed.
 * Opens a dump file, validates header, emits entries via callback.
 */
graveldb_status_t graveldb_ckpt_parse(const char *path,
                                      GravelDBCkptDumpHeader *header_out,
                                      graveldb_ckpt_entry_fn emit_fn,
                                      void *emit_ctx) {
    if (!path || !emit_fn) return GRAVELDB_ERR_INVALID;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return GRAVELDB_ERR_IO;

    /* Read and validate header */
    uint8_t hdr_buf[WIRE_CKPT_EXPORT_HDR_SIZE];
    ssize_t rd = read(fd, hdr_buf, WIRE_CKPT_EXPORT_HDR_SIZE);
    if (rd != WIRE_CKPT_EXPORT_HDR_SIZE) {
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }

    uint32_t magic, version, type_wire, dim, entry_size, checksum;
    uint64_t generation, num_entries, base_gen;
    wire_decode_ckpt_export_hdr(hdr_buf, &magic, &version, &type_wire,
                                &generation, &dim, &entry_size,
                                &num_entries, &base_gen, &checksum);

    if (magic != GRAVELDB_CKPT_EXPORT_MAGIC || version != 1) {
        close(fd);
        return GRAVELDB_ERR_CORRUPT;
    }

    /* Wire type: 1=FULL, 2=DELTA → API type: 0=FULL, 1=DELTA */
    int api_type = (int)type_wire - 1;

    if (header_out) {
        header_out->generation = generation;
        header_out->base_gen = base_gen;
        header_out->num_entries = num_entries;
        header_out->dim = dim;
        header_out->entry_size = entry_size;
        header_out->type = api_type;
    }

    /* Allocate read buffer for one entry's embedding */
    uint8_t *embed_buf = (uint8_t *)malloc(entry_size);
    if (!embed_buf) {
        close(fd);
        return GRAVELDB_ERR_OOM;
    }

    graveldb_status_t result = GRAVELDB_OK;
    uint8_t feat_wire[8];

    for (;;) {
        ssize_t r1 = read(fd, feat_wire, 8);
        if (r1 < 8) {
            /* Short read at entry boundary: could be EOF or truncation */
            if (r1 == 0) {
                /* Clean EOF (no trailer — tolerate) */
                break;
            }
            result = GRAVELDB_ERR_CORRUPT;
            break;
        }

        /* Check for trailer magic */
        uint32_t maybe_magic = wire_get_u32(feat_wire);
        if (maybe_magic == GRAVELDB_CKPT_EXPORT_MAGIC) {
            /* Trailer found — done */
            break;
        }

        uint64_t feat_id = wire_get_u64(feat_wire);

        /* Read embedding */
        ssize_t r2 = read(fd, embed_buf, entry_size);
        if (r2 != (ssize_t)entry_size) {
            result = GRAVELDB_ERR_CORRUPT;
            break;
        }

        /* Emit to listener */
        GravelDBCkptEntry entry;
        entry.feat_id = feat_id;
        entry.embedding = (const float *)embed_buf;
        entry.dim = dim;

        int cb_ret = emit_fn(emit_ctx, &entry);
        if (cb_ret != 0) {
            /* Early stop requested by caller */
            result = (graveldb_status_t)cb_ret;
            break;
        }
    }

    free(embed_buf);
    close(fd);
    return result;
}
