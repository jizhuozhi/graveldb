/*
 * GravelDB Server
 *
 * A lightweight TCP-based server providing batch embedding I/O
 * for distributed training/inference workloads.
 */

#ifndef GRAVELDB_SERVER_H_
#define GRAVELDB_SERVER_H_

#include "graveldb.h"
#include "graveldb_wire.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    GravelDBConfig  db_config;
    int             port;
    int             num_workers;
    int             backlog;
    size_t          max_request_size;
    int             auto_flush_interval_ms;
    int             auto_checkpoint_interval_s;
    bool            readonly;           /* readonly mode: no writes, no flush/ckpt */
    int             num_read_workers;   /* readonly mode: number of reader threads (0=auto) */
} GravelServerConfig;

typedef struct GravelServer GravelServer;

graveldb_status_t gravel_server_create(GravelServer **srv, const GravelServerConfig *config);
graveldb_status_t gravel_server_start(GravelServer *srv);
void              gravel_server_stop(GravelServer *srv);
void              gravel_server_destroy(GravelServer *srv);
GravelDB *gravel_server_get_db(GravelServer *srv);

#ifdef __cplusplus
}
#endif

#endif /* GRAVELDB_SERVER_H_ */
