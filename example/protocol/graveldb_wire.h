/*
 * GravelDB - Wire Protocol Definition
 *
 * Shared between client SDK and server.
 * Binary request/response over TCP.
 *
 * Request format:
 *   [4B magic][4B msg_type][4B body_len][body...]
 *
 * Response format:
 *   [4B magic][4B status][4B body_len][body...]
 */

#ifndef GRAVELDB_WIRE_H_
#define GRAVELDB_WIRE_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GRAVELDB_WIRE_MAGIC       0x47565242   /* "GVRB" */
#define GRAVELDB_WIRE_PORT        9527
#define GRAVELDB_WIRE_MAX_BATCH   65536
#define GRAVELDB_WIRE_HEADER_SIZE 12

typedef enum {
    GRAVELDB_MSG_PULL       = 1,
    GRAVELDB_MSG_PUSH       = 2,
    GRAVELDB_MSG_DELETE     = 3,
    GRAVELDB_MSG_FLUSH      = 4,
    GRAVELDB_MSG_CHECKPOINT = 5,
    GRAVELDB_MSG_STATS      = 6,
    GRAVELDB_MSG_PULL_STREAM = 7,  /* streaming pull: chunked response */

    /* Administrative / health-check (high number range) */
    GRAVELDB_MSG_PING       = 255,
} graveldb_msg_type_t;

typedef enum {
    GRAVELDB_WIRE_OK         = 0,
    GRAVELDB_WIRE_ERR        = 1,
    GRAVELDB_WIRE_NOT_FOUND  = 2,
    GRAVELDB_WIRE_INVALID    = 3,
} graveldb_wire_status_t;

/* Pull request body:
 *   [4B count][8B feat_id_0][8B feat_id_1]...
 *
 * Pull response body:
 *   [4B count]
 *   for each:
 *     [4B dim][dim * 4B floats]  (dim=0 if not found)
 */

/* Pull Stream (GRAVELDB_MSG_PULL_STREAM):
 *   Request: same as PULL
 *   Response: chunked streaming
 *     Header: [magic][status][body_len=0xFFFFFFFF]
 *     Frames: [4B frame_len][frame_data...] repeated
 *       frame_data: [4B dim][dim*4B floats] for each entry in the chunk
 *     Terminator: [4B frame_len=0]
 *
 *   The first frame begins with [4B total_count] before the entries.
 */
#define GRAVELDB_WIRE_STREAM_SENTINEL 0xFFFFFFFFu

/* Push request body:
 *   [4B count]
 *   for each:
 *     [8B feat_id][4B dim][dim * 4B floats]
 */

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_WIRE_H_ */
