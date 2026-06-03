/*
 * GravelDB - Wire Format for Checkpoint Serialization
 *
 * Design principle:
 *   Data files (.bin, .keys) are ALWAYS native byte order — no conversion
 *   on read/write/flush paths. They are local-only and never transferred
 *   across nodes directly.
 *
 *   Cross-node data access goes exclusively through checkpoint files
 *   (delta + meta), whose HEADERS use explicit little-endian encoding
 *   to ensure portability. The delta body (raw page data) is native.
 *
 * This module provides:
 *   - LE encode/decode for structured checkpoint headers (BinMeta, DeltaHeader)
 *   - Magic byte-swap detection for endianness mismatch diagnostics
 */

#ifndef GRAVELDB_WIRE_H_
#define GRAVELDB_WIRE_H_

#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Endianness detection at compile time.
 * On little-endian hosts, wire_put/get become plain memcpy (compiler optimizes
 * to a single MOV — true zero-cost). On big-endian hosts, we fall back to
 * explicit byte-shift encoding.
 */
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define GRAVELDB_WIRE_LE 1
#elif defined(__LITTLE_ENDIAN__) || defined(_LITTLE_ENDIAN) || \
      defined(__ARMEL__) || defined(__AARCH64EL__) || \
      defined(__x86_64__) || defined(__i386__) || defined(_M_IX86) || defined(_M_X64)
#define GRAVELDB_WIRE_LE 1
#else
#define GRAVELDB_WIRE_LE 0
#endif

#if GRAVELDB_WIRE_LE

/* Little-endian: wire format == native, memcpy is a no-op (single MOV) */

static inline void wire_put_u32(uint8_t *buf, uint32_t v) { memcpy(buf, &v, 4); }
static inline void wire_put_u64(uint8_t *buf, uint64_t v) { memcpy(buf, &v, 8); }

static inline uint32_t wire_get_u32(const uint8_t *buf) {
    uint32_t v; memcpy(&v, buf, 4); return v;
}
static inline uint64_t wire_get_u64(const uint8_t *buf) {
    uint64_t v; memcpy(&v, buf, 8); return v;
}

#else

/* Big-endian fallback: explicit byte-shift encode/decode */

static inline void wire_put_u32(uint8_t *buf, uint32_t v) {
    buf[0] = (uint8_t)(v);
    buf[1] = (uint8_t)(v >> 8);
    buf[2] = (uint8_t)(v >> 16);
    buf[3] = (uint8_t)(v >> 24);
}

static inline void wire_put_u64(uint8_t *buf, uint64_t v) {
    buf[0] = (uint8_t)(v);
    buf[1] = (uint8_t)(v >> 8);
    buf[2] = (uint8_t)(v >> 16);
    buf[3] = (uint8_t)(v >> 24);
    buf[4] = (uint8_t)(v >> 32);
    buf[5] = (uint8_t)(v >> 40);
    buf[6] = (uint8_t)(v >> 48);
    buf[7] = (uint8_t)(v >> 56);
}

static inline uint32_t wire_get_u32(const uint8_t *buf) {
    return (uint32_t)buf[0]
         | ((uint32_t)buf[1] << 8)
         | ((uint32_t)buf[2] << 16)
         | ((uint32_t)buf[3] << 24);
}

static inline uint64_t wire_get_u64(const uint8_t *buf) {
    return (uint64_t)buf[0]
         | ((uint64_t)buf[1] << 8)
         | ((uint64_t)buf[2] << 16)
         | ((uint64_t)buf[3] << 24)
         | ((uint64_t)buf[4] << 32)
         | ((uint64_t)buf[5] << 40)
         | ((uint64_t)buf[6] << 48)
         | ((uint64_t)buf[7] << 56);
}

#endif /* GRAVELDB_WIRE_LE */

/* Byte-swap helper: detect if a magic was written on opposite endianness */
static inline uint32_t wire_bswap32(uint32_t v) {
    return ((v >> 24) & 0xFF)
         | ((v >>  8) & 0xFF00)
         | ((v <<  8) & 0xFF0000)
         | ((v << 24) & 0xFF000000u);
}

/*
 * BinMeta wire format (16 bytes):
 *   [0..3]   magic      (u32 LE)
 *   [4..11]  generation (u64 LE)
 *   [12..15] checksum   (u32 LE)  — CRC32 of bytes [0..11]
 */
#define WIRE_META_SIZE 16

static inline void wire_encode_meta(uint8_t buf[WIRE_META_SIZE],
                                    uint32_t magic, uint64_t generation,
                                    uint32_t checksum) {
    wire_put_u32(buf + 0, magic);
    wire_put_u64(buf + 4, generation);
    wire_put_u32(buf + 12, checksum);
}

static inline void wire_decode_meta(const uint8_t buf[WIRE_META_SIZE],
                                    uint32_t *magic, uint64_t *generation,
                                    uint32_t *checksum) {
    *magic      = wire_get_u32(buf + 0);
    *generation = wire_get_u64(buf + 4);
    *checksum   = wire_get_u32(buf + 12);
}

/*
 * DeltaHeader wire format (40 bytes):
 *   [0..3]   magic       (u32 LE)
 *   [4..7]   version     (u32 LE)
 *   [8..15]  generation  (u64 LE)
 *   [16..19] dim         (u32 LE)
 *   [20..23] entry_size  (u32 LE)
 *   [24..31] bump_ptr    (u64 LE)
 *   [32..35] num_entries (u32 LE)
 *   [36..39] checksum    (u32 LE)
 */
#define WIRE_DELTA_HDR_SIZE 40

static inline void wire_encode_delta_hdr(uint8_t buf[WIRE_DELTA_HDR_SIZE],
                                         uint32_t magic, uint32_t version,
                                         uint64_t generation,
                                         uint32_t dim, uint32_t entry_size,
                                         uint64_t bump_ptr,
                                         uint32_t num_entries,
                                         uint32_t checksum) {
    wire_put_u32(buf + 0,  magic);
    wire_put_u32(buf + 4,  version);
    wire_put_u64(buf + 8,  generation);
    wire_put_u32(buf + 16, dim);
    wire_put_u32(buf + 20, entry_size);
    wire_put_u64(buf + 24, bump_ptr);
    wire_put_u32(buf + 32, num_entries);
    wire_put_u32(buf + 36, checksum);
}

static inline void wire_decode_delta_hdr(const uint8_t buf[WIRE_DELTA_HDR_SIZE],
                                         uint32_t *magic, uint32_t *version,
                                         uint64_t *generation,
                                         uint32_t *dim, uint32_t *entry_size,
                                         uint64_t *bump_ptr,
                                         uint32_t *num_entries,
                                         uint32_t *checksum) {
    *magic       = wire_get_u32(buf + 0);
    *version     = wire_get_u32(buf + 4);
    *generation  = wire_get_u64(buf + 8);
    *dim         = wire_get_u32(buf + 16);
    *entry_size  = wire_get_u32(buf + 20);
    *bump_ptr    = wire_get_u64(buf + 24);
    *num_entries = wire_get_u32(buf + 32);
    *checksum    = wire_get_u32(buf + 36);
}

/*
 * Delta entry header (8 bytes per dirty block run):
 *   [0..3] pg_start (u32 LE)
 *   [4..7] pg_count (u32 LE)
 */
#define WIRE_DELTA_ENTRY_SIZE 8

static inline void wire_encode_delta_entry(uint8_t buf[WIRE_DELTA_ENTRY_SIZE],
                                           uint32_t pg_start, uint32_t pg_count) {
    wire_put_u32(buf + 0, pg_start);
    wire_put_u32(buf + 4, pg_count);
}

static inline void wire_decode_delta_entry(const uint8_t buf[WIRE_DELTA_ENTRY_SIZE],
                                           uint32_t *pg_start, uint32_t *pg_count) {
    *pg_start = wire_get_u32(buf + 0);
    *pg_count = wire_get_u32(buf + 4);
}

/*
 * Bulk data byte-swap: REMOVED.
 *
 * Data files (.bin, .keys) are always stored in native byte order.
 * Cross-node data transfer goes exclusively through checkpoint files
 * (delta/meta), which use the wire_encode/decode functions above for
 * their headers. This eliminates per-page byte-swap overhead on the
 * hot read/write/flush paths entirely.
 *
 * Portability note: a checkpoint file written on one host can be replayed
 * on any host because the delta header fields use explicit LE encoding.
 * The delta *body* (raw page data) is written verbatim from the source
 * host's native format. If cross-architecture replay is ever needed,
 * add a "source_endian" field to DeltaHeader and swap on mismatch.
 */

/*
 * Unified Checkpoint Export Wire Format (full and delta use the same format).
 *
 * Stream layout:
 *   [CkptExportHeader: 48 bytes, LE]
 *   [Entry 0: feat_id(8B LE) | embedding(entry_size bytes, native)]
 *   [Entry 1: feat_id(8B LE) | embedding(entry_size bytes, native)]
 *   ...
 *
 * Full: all valid entries (feat_id != 0). Truncation point for delta chain.
 * Delta: only entries dirtied since last checkpoint.
 * Consumer: sequential read, upsert each (feat_id, embedding). Later entries
 *   with the same feat_id override earlier ones (enables dirty compensation).
 *
 * CkptExportHeader (48 bytes):
 *   [0..3]   magic        "CKEX" = 0x434B4558
 *   [4..7]   version      1
 *   [8..11]  type         CKPT_EXPORT_FULL=1, CKPT_EXPORT_DELTA=2
 *   [12..19] generation   u64
 *   [20..23] dim          u32
 *   [24..27] entry_size   u32 (dim * sizeof(float), no padding)
 *   [28..35] num_entries  u64 (total entries in stream)
 *   [36..43] base_gen     u64 (delta: base generation; full: 0)
 *   [44..47] checksum     u32 (CRC32 of [0..43])
 */

#define GRAVELDB_CKPT_EXPORT_MAGIC  0x434B4558  /* "CKEX" */
#define CKPT_EXPORT_FULL   1
#define CKPT_EXPORT_DELTA  2

#define WIRE_CKPT_EXPORT_HDR_SIZE  48

static inline void wire_encode_ckpt_export_hdr(uint8_t buf[WIRE_CKPT_EXPORT_HDR_SIZE],
                                               uint32_t magic, uint32_t version,
                                               uint32_t type, uint64_t generation,
                                               uint32_t dim, uint32_t entry_size,
                                               uint64_t num_entries, uint64_t base_gen,
                                               uint32_t checksum) {
    wire_put_u32(buf + 0,  magic);
    wire_put_u32(buf + 4,  version);
    wire_put_u32(buf + 8,  type);
    wire_put_u64(buf + 12, generation);
    wire_put_u32(buf + 20, dim);
    wire_put_u32(buf + 24, entry_size);
    wire_put_u64(buf + 28, num_entries);
    wire_put_u64(buf + 36, base_gen);
    wire_put_u32(buf + 44, checksum);
}

static inline void wire_decode_ckpt_export_hdr(const uint8_t buf[WIRE_CKPT_EXPORT_HDR_SIZE],
                                               uint32_t *magic, uint32_t *version,
                                               uint32_t *type, uint64_t *generation,
                                               uint32_t *dim, uint32_t *entry_size,
                                               uint64_t *num_entries, uint64_t *base_gen,
                                               uint32_t *checksum) {
    *magic       = wire_get_u32(buf + 0);
    *version     = wire_get_u32(buf + 4);
    *type        = wire_get_u32(buf + 8);
    *generation  = wire_get_u64(buf + 12);
    *dim         = wire_get_u32(buf + 20);
    *entry_size  = wire_get_u32(buf + 24);
    *num_entries = wire_get_u64(buf + 28);
    *base_gen    = wire_get_u64(buf + 36);
    *checksum    = wire_get_u32(buf + 44);
}

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_WIRE_H_ */
