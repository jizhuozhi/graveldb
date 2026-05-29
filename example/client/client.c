/*
 * GravelDB Client SDK - Implementation
 */

#include "client.h"
#include "../protocol/graveldb_wire.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

struct GravelDBClient {
    int       fd;
    uint8_t  *send_buf;
    size_t    send_cap;
    uint8_t  *recv_buf;
    size_t    recv_cap;
    int       pending;    /* number of responses not yet consumed */
};

static int send_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        ssize_t wr = write(fd, p, len);
        if (wr <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += wr;
        len -= wr;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    while (len > 0) {
        ssize_t rd = read(fd, p, len);
        if (rd <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += rd;
        len -= rd;
    }
    return 0;
}

static int send_request(GravelDBClient *c, graveldb_msg_type_t type,
                        const void *body, uint32_t body_len) {
    uint8_t header[GRAVELDB_WIRE_HEADER_SIZE];
    uint32_t magic = GRAVELDB_WIRE_MAGIC;
    uint32_t msg_type = (uint32_t)type;

    memcpy(header, &magic, 4);
    memcpy(header + 4, &msg_type, 4);
    memcpy(header + 8, &body_len, 4);

    if (send_all(c->fd, header, GRAVELDB_WIRE_HEADER_SIZE) < 0) return -1;
    if (body_len > 0 && send_all(c->fd, body, body_len) < 0) return -1;
    return 0;
}

static int recv_response(GravelDBClient *c, uint32_t *status, uint8_t **body, uint32_t *body_len) {
    uint8_t header[GRAVELDB_WIRE_HEADER_SIZE];
    if (recv_all(c->fd, header, GRAVELDB_WIRE_HEADER_SIZE) < 0) return -1;

    uint32_t magic;
    memcpy(&magic, header, 4);
    if (magic != GRAVELDB_WIRE_MAGIC) return -1;

    memcpy(status, header + 4, 4);
    memcpy(body_len, header + 8, 4);

    if (*body_len > 0) {
        if (*body_len > c->recv_cap) {
            c->recv_cap = *body_len;
            c->recv_buf = (uint8_t *)realloc(c->recv_buf, c->recv_cap);
            if (!c->recv_buf) return -1;
        }
        if (recv_all(c->fd, c->recv_buf, *body_len) < 0) return -1;
        *body = c->recv_buf;
    } else {
        *body = NULL;
    }
    return 0;
}

int graveldb_client_connect(GravelDBClient **out, const char *host, int port) {
    GravelDBClient *c = (GravelDBClient *)calloc(1, sizeof(GravelDBClient));
    if (!c) return -1;

    c->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (c->fd < 0) { free(c); return -1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        struct hostent *he = gethostbyname(host);
        if (!he) { close(c->fd); free(c); return -1; }
        memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
    }

    if (connect(c->fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(c->fd);
        free(c);
        return -1;
    }

    int one = 1;
    setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    c->send_cap = 1024 * 1024;
    c->send_buf = (uint8_t *)malloc(c->send_cap);
    c->recv_cap = 1024 * 1024;
    c->recv_buf = (uint8_t *)malloc(c->recv_cap);

    *out = c;
    return 0;
}

void graveldb_client_close(GravelDBClient *c) {
    if (!c) return;
    if (c->fd >= 0) close(c->fd);
    free(c->send_buf);
    free(c->recv_buf);
    free(c);
}

int graveldb_client_pull(GravelDBClient *c, const uint64_t *feat_ids, int n,
                         float **out_embeddings, int *out_dims) {
    uint32_t body_len = 4 + n * 8;
    if (body_len > c->send_cap) {
        c->send_cap = body_len;
        c->send_buf = (uint8_t *)realloc(c->send_buf, c->send_cap);
    }

    uint32_t count = (uint32_t)n;
    memcpy(c->send_buf, &count, 4);
    memcpy(c->send_buf + 4, feat_ids, n * 8);

    if (send_request(c, GRAVELDB_MSG_PULL, c->send_buf, body_len) < 0) return -1;

    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    if (status != GRAVELDB_WIRE_OK) return -1;

    if (resp_len < 4) return -1;
    uint32_t resp_count;
    memcpy(&resp_count, body, 4);

    const uint8_t *ptr = body + 4;
    const uint8_t *end = body + resp_len;

    for (uint32_t i = 0; i < resp_count && ptr < end; i++) {
        uint32_t dim;
        memcpy(&dim, ptr, 4); ptr += 4;

        if (out_dims) out_dims[i] = (int)dim;

        if (dim > 0 && out_embeddings && out_embeddings[i]) {
            size_t bytes = dim * sizeof(float);
            if (ptr + bytes <= end) {
                memcpy(out_embeddings[i], ptr, bytes);
            }
            ptr += bytes;
        }
    }
    return 0;
}

/* Serialize a push body into send_buf. Returns body length, or 0 on realloc failure. */
static size_t push_serialize(GravelDBClient *c, const uint64_t *feat_ids,
                             const int *dims, const float *const *embeddings, int n) {
    size_t body_len = 4;
    for (int i = 0; i < n; i++) {
        body_len += 8 + 4 + dims[i] * sizeof(float);
    }

    if (body_len > c->send_cap) {
        c->send_cap = body_len;
        uint8_t *tmp = (uint8_t *)realloc(c->send_buf, c->send_cap);
        if (!tmp) return 0;
        c->send_buf = tmp;
    }

    uint8_t *ptr = c->send_buf;
    uint32_t count = (uint32_t)n;
    memcpy(ptr, &count, 4); ptr += 4;

    for (int i = 0; i < n; i++) {
        memcpy(ptr, &feat_ids[i], 8); ptr += 8;
        uint32_t d = (uint32_t)dims[i];
        memcpy(ptr, &d, 4); ptr += 4;
        memcpy(ptr, embeddings[i], dims[i] * sizeof(float));
        ptr += dims[i] * sizeof(float);
    }
    return body_len;
}

int graveldb_client_push(GravelDBClient *c, const uint64_t *feat_ids,
                         const int *dims, const float *const *embeddings, int n) {
    size_t body_len = push_serialize(c, feat_ids, dims, embeddings, n);
    if (body_len == 0) return -1;

    if (send_request(c, GRAVELDB_MSG_PUSH, c->send_buf, (uint32_t)body_len) < 0) return -1;

    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    return (status == GRAVELDB_WIRE_OK) ? 0 : -1;
}

int graveldb_client_push_async(GravelDBClient *c, const uint64_t *feat_ids,
                               const int *dims, const float *const *embeddings, int n) {
    size_t body_len = push_serialize(c, feat_ids, dims, embeddings, n);
    if (body_len == 0) return -1;

    if (send_request(c, GRAVELDB_MSG_PUSH, c->send_buf, (uint32_t)body_len) < 0) return -1;
    c->pending++;
    return 0;
}

int graveldb_client_pull_send(GravelDBClient *c, const uint64_t *feat_ids, int n) {
    uint32_t body_len = 4 + n * 8;
    if (body_len > c->send_cap) {
        c->send_cap = body_len;
        uint8_t *tmp = (uint8_t *)realloc(c->send_buf, c->send_cap);
        if (!tmp) return -1;
        c->send_buf = tmp;
    }

    uint32_t count = (uint32_t)n;
    memcpy(c->send_buf, &count, 4);
    memcpy(c->send_buf + 4, feat_ids, n * 8);

    if (send_request(c, GRAVELDB_MSG_PULL, c->send_buf, body_len) < 0) return -1;
    c->pending++;
    return 0;
}

int graveldb_client_pull_recv(GravelDBClient *c, float **out_embeddings, int *out_dims, int n) {
    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    if (status != GRAVELDB_WIRE_OK) { c->pending--; return -1; }

    if (resp_len < 4) { c->pending--; return -1; }
    uint32_t resp_count;
    memcpy(&resp_count, body, 4);

    const uint8_t *ptr = body + 4;
    const uint8_t *end = body + resp_len;

    for (uint32_t i = 0; i < resp_count && (int)i < n && ptr < end; i++) {
        uint32_t dim;
        memcpy(&dim, ptr, 4); ptr += 4;

        if (out_dims) out_dims[i] = (int)dim;

        if (dim > 0 && out_embeddings && out_embeddings[i]) {
            size_t bytes = dim * sizeof(float);
            if (ptr + bytes <= end) {
                memcpy(out_embeddings[i], ptr, bytes);
            }
            ptr += bytes;
        }
    }
    c->pending--;
    return 0;
}

int graveldb_client_pull_stream(GravelDBClient *c, const uint64_t *feat_ids, int n,
                                float **out_embeddings, int *out_dims) {
    /* Send request (same body as normal PULL, different msg type) */
    uint32_t body_len = 4 + n * 8;
    if (body_len > c->send_cap) {
        c->send_cap = body_len;
        c->send_buf = (uint8_t *)realloc(c->send_buf, c->send_cap);
        if (!c->send_buf) return -1;
    }

    uint32_t count = (uint32_t)n;
    memcpy(c->send_buf, &count, 4);
    memcpy(c->send_buf + 4, feat_ids, n * 8);

    if (send_request(c, GRAVELDB_MSG_PULL_STREAM, c->send_buf, body_len) < 0) return -1;

    /* Receive streaming header */
    uint8_t header[GRAVELDB_WIRE_HEADER_SIZE];
    if (recv_all(c->fd, header, GRAVELDB_WIRE_HEADER_SIZE) < 0) return -1;

    uint32_t magic;
    memcpy(&magic, header, 4);
    if (magic != GRAVELDB_WIRE_MAGIC) return -1;

    uint32_t status;
    memcpy(&status, header + 4, 4);
    if (status != GRAVELDB_WIRE_OK) return -1;

    uint32_t sentinel;
    memcpy(&sentinel, header + 8, 4);
    if (sentinel != GRAVELDB_WIRE_STREAM_SENTINEL) {
        /* Server responded with non-streaming response (old server?).
         * Fall back: treat as normal response body. */
        uint32_t resp_len = sentinel;
        if (resp_len > c->recv_cap) {
            c->recv_cap = resp_len;
            c->recv_buf = (uint8_t *)realloc(c->recv_buf, c->recv_cap);
            if (!c->recv_buf) return -1;
        }
        if (resp_len > 0 && recv_all(c->fd, c->recv_buf, resp_len) < 0) return -1;
        /* Parse as normal pull response */
        if (resp_len < 4) return -1;
        uint32_t resp_count;
        memcpy(&resp_count, c->recv_buf, 4);
        const uint8_t *ptr = c->recv_buf + 4;
        const uint8_t *end = c->recv_buf + resp_len;
        for (uint32_t i = 0; i < resp_count && (int)i < n && ptr < end; i++) {
            uint32_t dim;
            memcpy(&dim, ptr, 4); ptr += 4;
            if (out_dims) out_dims[i] = (int)dim;
            if (dim > 0 && out_embeddings && out_embeddings[i]) {
                size_t bytes = dim * sizeof(float);
                if (ptr + bytes <= end) memcpy(out_embeddings[i], ptr, bytes);
                ptr += bytes;
            }
        }
        return 0;
    }

    /* Streaming mode: read frames until terminator (frame_len=0) */
    uint32_t entry_idx = 0;
    int first_frame = 1;

    for (;;) {
        uint32_t frame_len;
        if (recv_all(c->fd, &frame_len, 4) < 0) return -1;
        if (frame_len == 0) break;  /* terminator */

        /* Ensure recv_buf can hold frame payload */
        if (frame_len > c->recv_cap) {
            c->recv_cap = frame_len;
            c->recv_buf = (uint8_t *)realloc(c->recv_buf, c->recv_cap);
            if (!c->recv_buf) return -1;
        }
        if (recv_all(c->fd, c->recv_buf, frame_len) < 0) return -1;

        const uint8_t *ptr = c->recv_buf;
        const uint8_t *end = c->recv_buf + frame_len;

        /* First frame has total_count prefix */
        if (first_frame) {
            if (frame_len < 4) return -1;
            /* skip total_count (we already know n) */
            ptr += 4;
            first_frame = 0;
        }

        /* Parse entries: [4B dim][dim*4B floats] */
        while (ptr < end && (int)entry_idx < n) {
            uint32_t dim;
            memcpy(&dim, ptr, 4); ptr += 4;

            if (out_dims) out_dims[entry_idx] = (int)dim;

            if (dim > 0 && out_embeddings && out_embeddings[entry_idx]) {
                size_t bytes = dim * sizeof(float);
                if (ptr + bytes <= end) {
                    memcpy(out_embeddings[entry_idx], ptr, bytes);
                }
                ptr += bytes;
            } else if (dim > 0) {
                ptr += dim * sizeof(float);
            }
            entry_idx++;
        }
    }
    return 0;
}

int graveldb_client_await(GravelDBClient *c, int max_drain) {
    int drained = 0;
    int errors = 0;
    if (max_drain <= 0) max_drain = c->pending;

    while (drained < max_drain && c->pending > 0) {
        uint32_t status, resp_len;
        uint8_t *body;
        if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
        if (status != GRAVELDB_WIRE_OK) errors++;
        c->pending--;
        drained++;
    }
    return errors;
}

int graveldb_client_pending(GravelDBClient *c) {
    return c->pending;
}

int graveldb_client_delete(GravelDBClient *c, const uint64_t *feat_ids, int n) {
    uint32_t body_len = 4 + n * 8;
    if (body_len > c->send_cap) {
        c->send_cap = body_len;
        c->send_buf = (uint8_t *)realloc(c->send_buf, c->send_cap);
    }

    uint32_t count = (uint32_t)n;
    memcpy(c->send_buf, &count, 4);
    memcpy(c->send_buf + 4, feat_ids, n * 8);

    if (send_request(c, GRAVELDB_MSG_DELETE, c->send_buf, body_len) < 0) return -1;

    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    return (status == GRAVELDB_WIRE_OK) ? 0 : -1;
}

int graveldb_client_flush(GravelDBClient *c) {
    if (send_request(c, GRAVELDB_MSG_FLUSH, NULL, 0) < 0) return -1;
    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    return (status == GRAVELDB_WIRE_OK) ? 0 : -1;
}

int graveldb_client_checkpoint(GravelDBClient *c) {
    if (send_request(c, GRAVELDB_MSG_CHECKPOINT, NULL, 0) < 0) return -1;
    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    return (status == GRAVELDB_WIRE_OK) ? 0 : -1;
}

int graveldb_client_ping(GravelDBClient *c) {
    if (send_request(c, GRAVELDB_MSG_PING, NULL, 0) < 0) return -1;
    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    return (status == GRAVELDB_WIRE_OK) ? 0 : -1;
}

int graveldb_client_stats(GravelDBClient *c, GravelDBClientStats *stats) {
    if (send_request(c, GRAVELDB_MSG_STATS, NULL, 0) < 0) return -1;
    uint32_t status, resp_len;
    uint8_t *body;
    if (recv_response(c, &status, &body, &resp_len) < 0) return -1;
    if (status != GRAVELDB_WIRE_OK || resp_len < sizeof(GravelDBClientStats)) return -1;
    memcpy(stats, body, sizeof(GravelDBClientStats));
    return 0;
}
