/*
 * GravelDB Client SDK - C client for the parameter server
 *
 * Usage:
 *   GravelDBClient *client = NULL;
 *   graveldb_client_connect(&client, "127.0.0.1", 9527);
 *   
 *   float emb[128] = { ... };
 *   graveldb_client_push(client, &feat_id, &dim, &emb_ptr, 1);
 *   
 *   float out[128];
 *   int out_dim;
 *   graveldb_client_pull(client, &feat_id, 1, &out_ptr, &out_dim);
 *   
 *   graveldb_client_close(client);
 */

#ifndef GRAVELDB_CLIENT_H_
#define GRAVELDB_CLIENT_H_

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct GravelDBClient GravelDBClient;

/* Connect to parameter server */
int graveldb_client_connect(GravelDBClient **client, const char *host, int port);

/* Close connection */
void graveldb_client_close(GravelDBClient *client);

/* Pull (batch get) embeddings
 * out_embeddings: array of float* (pre-allocated by caller, or NULL for skip)
 * out_dims: array of int, receives actual dim (0 if not found) */
int graveldb_client_pull(GravelDBClient *client,
                         const uint64_t *feat_ids, int n,
                         float **out_embeddings, int *out_dims);

/* Push (batch put) embeddings */
int graveldb_client_push(GravelDBClient *client,
                         const uint64_t *feat_ids,
                         const int *dims,
                         const float *const *embeddings, int n);

/* Push async: send request without waiting for response.
 * Call graveldb_client_await() later to drain pending responses. */
int graveldb_client_push_async(GravelDBClient *client,
                               const uint64_t *feat_ids,
                               const int *dims,
                               const float *const *embeddings, int n);

/* Drain up to max_drain pending responses (0 = drain all).
 * Returns number of errors, or -1 on connection failure. */
int graveldb_client_await(GravelDBClient *client, int max_drain);

/* Number of pending (un-drained) async responses */
int graveldb_client_pending(GravelDBClient *client);

/* Delete features */
int graveldb_client_delete(GravelDBClient *client,
                           const uint64_t *feat_ids, int n);

/* Administrative commands */
int graveldb_client_flush(GravelDBClient *client);
int graveldb_client_checkpoint(GravelDBClient *client);
int graveldb_client_ping(GravelDBClient *client);

/* Stats */
typedef struct {
    uint64_t total_features;
    uint64_t total_slots;
    uint64_t buffer_hits;
    uint64_t buffer_misses;
    uint64_t buffer_evictions;
    uint64_t flush_bytes;
    uint64_t checkpoint_generation;
    float    dirty_ratio;
    float    cache_hit_ratio;
} GravelDBClientStats;

int graveldb_client_stats(GravelDBClient *client, GravelDBClientStats *stats);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_CLIENT_H_ */
