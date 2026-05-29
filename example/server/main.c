/*
 * GravelDB Server - main entry point
 *
 * Usage:
 *   graveldb-server [options]
 *
 * Options:
 *   -d <dir>      Data directory (default: ./graveldb_data)
 *   -p <port>     Listen port (default: 9527)
 *   -b <size>     Buffer size in MB (default: 256)
 *   -D <dims>     Comma-separated dim list (default: 32,64,128)
 *   --flush-ms <n>       Auto flush interval in ms (default: 1000)
 *   --checkpoint-s <n>   Auto checkpoint interval in seconds (default: 300)
 *   -h                   Print help
 */

#include "server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

static volatile int g_running = 1;
static GravelServer *g_server = NULL;

static void signal_handler(int sig) {
    (void)sig;
    g_running = 0;
    if (g_server) {
        gravel_server_stop(g_server);
    }
}

static void print_usage(const char *prog) {
    fprintf(stderr,
        "GravelDB Server v1.0\n"
        "\n"
        "Usage: %s [options]\n"
        "\n"
        "Options:\n"
        "  -d <dir>           Data directory (default: ./graveldb_data)\n"
        "  -p <port>          Listen port (default: 9527)\n"
        "  -b <size_mb>       Buffer size per bin in MB (default: 256)\n"
        "  -D <dims>          Comma-separated dim list (default: 32,64,128)\n"
        "  --flush-ms <n>     Auto flush interval in ms (default: 1000)\n"
        "  --checkpoint-s <n> Auto checkpoint interval in seconds (default: 300)\n"
        "  --readonly         Read-only mode: load from checkpoint, reject writes\n"
        "  --read-workers <n> Number of reader threads in readonly mode (default: auto)\n"
        "  -h                 Print this help\n"
        "\n"
        "Example:\n"
        "  %s -d /data/embeddings -p 9527 -D 64,128,256 -b 512\n"
        "  %s -d /data/embeddings -p 9527 --readonly --read-workers 4\n"
        "\n",
        prog, prog, prog);
}

#define MAX_CLI_DIMS 64

static int parse_dims(const char *str, int *dims, int max_dims) {
    int count = 0;
    char *dup = strdup(str);
    char *tok = strtok(dup, ",");
    while (tok && count < max_dims) {
        dims[count++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(dup);
    return count;
}

int main(int argc, char **argv) {
    GravelServerConfig config;
    memset(&config, 0, sizeof(config));

    /* Defaults */
    const char *data_dir = "./graveldb_data";
    int port = GRAVELDB_WIRE_PORT;
    size_t buffer_mb = 256;
    int dims[MAX_CLI_DIMS] = {32, 64, 128};
    int num_dims = 3;
    int flush_ms = 1000;
    int ckpt_s = 300;
    bool readonly = false;
    int read_workers = 0;

    /* Parse arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            buffer_mb = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-D") == 0 && i + 1 < argc) {
            num_dims = parse_dims(argv[++i], dims, MAX_CLI_DIMS);
        } else if (strcmp(argv[i], "--flush-ms") == 0 && i + 1 < argc) {
            flush_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--checkpoint-s") == 0 && i + 1 < argc) {
            ckpt_s = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--readonly") == 0) {
            readonly = true;
        } else if (strcmp(argv[i], "--read-workers") == 0 && i + 1 < argc) {
            read_workers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    /* Fill config */
    config.db_config.data_dir = data_dir;
    config.db_config.num_dims = num_dims;
    config.db_config.dims = dims;
    config.db_config.buffer_size = buffer_mb * 1024 * 1024;
    config.db_config.index_capacity = 1 << 24; /* 16M */

    config.port = port;
    config.num_workers = 0;
    config.backlog = 128;
    config.max_request_size = 64 * 1024 * 1024; /* 64MB */
    config.auto_flush_interval_ms = flush_ms;
    config.auto_checkpoint_interval_s = ckpt_s;
    config.readonly = readonly;
    config.num_read_workers = read_workers;

    /* Setup signals */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    /* Print config */
    fprintf(stderr, "[GravelServer] Configuration:\n");
    fprintf(stderr, "  Data dir:   %s\n", data_dir);
    fprintf(stderr, "  Port:       %d\n", port);
    fprintf(stderr, "  Buffer:     %zu MB per bin\n", buffer_mb / num_dims);
    fprintf(stderr, "  Dims:       ");
    for (int i = 0; i < num_dims; i++) fprintf(stderr, "%d ", dims[i]);
    fprintf(stderr, "\n");
    if (readonly) {
        fprintf(stderr, "  Mode:       READONLY (writes rejected, lock-free reads)\n");
        if (read_workers > 0) {
            fprintf(stderr, "  Workers:    %d reader threads\n", read_workers);
        }
    } else {
        fprintf(stderr, "  Mode:       READ-WRITE\n");
        fprintf(stderr, "  Flush:      every %d ms\n", flush_ms);
        fprintf(stderr, "  Checkpoint: every %d s\n", ckpt_s);
    }
    fprintf(stderr, "\n");

    /* Create and start server */
    GravelServer *srv = NULL;
    graveldb_status_t rc = gravel_server_create(&srv, &config);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[GravelServer] Failed to create server: %d\n", rc);
        return 1;
    }

    g_server = srv;

    rc = gravel_server_start(srv);
    if (rc != GRAVELDB_OK) {
        fprintf(stderr, "[GravelServer] Failed to start server: %d\n", rc);
        gravel_server_destroy(srv);
        return 1;
    }

    fprintf(stderr, "[GravelServer] Started. Press Ctrl+C to stop.\n");

    /* Wait for signal */
    while (g_running) {
        sleep(1);
    }

    fprintf(stderr, "\n[GravelServer] Shutting down...\n");
    gravel_server_destroy(srv);
    fprintf(stderr, "[GravelServer] Done.\n");

    return 0;
}
