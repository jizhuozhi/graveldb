# 06 — Server: Wire Protocol & Event Loop

## Problem

GravelDB 作为嵌入式库提供极高性能的本地 API。但在分布式训练/推理场景中，多个 worker 需要共享同一份 embedding 存储。需要一个轻量级网络层将库 API 暴露为服务。

## Design Choice

单线程 event loop + 二进制 TCP 协议。类 Redis 架构，无多线程，不做集群/分片——横向扩展交给进程级分片 + 外部路由。

## Actual Implementation

### Server Architecture

```
┌────────────────────────────────────────────────┐
│  GravelServer                                   │
│                                                 │
│  ┌─ IOPoller ─────────────────────────────────┐│
│  │  Linux: epoll    macOS: kqueue             ││
│  │  max 256 clients + 1 listen fd             ││
│  └─────────────────────────────────────────────┘│
│  ┌─ GravelDB ─────────────────────────────────┐│
│  │  (embedded engine, full ownership)          ││
│  └─────────────────────────────────────────────┘│
│  ┌─ CkptScheduler ───────────────────────────┐│
│  │  cooperative tick on each poll return       ││
│  │  interval-based flush / checkpoint          ││
│  └─────────────────────────────────────────────┘│
│                                                 │
│  Event loop:                                    │
│    while (running):                             │
│      events = io_poller_wait(timeout_ms=100)    │
│      for each event:                            │
│        if listen_fd → accept_client()           │
│        if client_fd → handle_client(fd)         │
│      ckpt_scheduler_tick()                      │
└────────────────────────────────────────────────┘
```

### Wire Protocol

**固定 12 字节头**：
```
[4B magic = 0x47565242 "GVRB"] [4B msg_type] [4B body_len]
```

**消息类型**：

| Type | Code | Request Body | Response Body |
|------|------|-------------|---------------|
| PULL | 1 | `[4B count][8B feat_id × count]` | `[4B count][per-entry: 4B dim + dim×4B floats]` (miss → dim=0) |
| PUSH | 2 | `[4B count][per-entry: 8B feat_id + 4B dim + dim×4B floats]` | `[status]` |
| DELETE | 3 | `[4B count][8B feat_id × count]` | `[status]` |
| FLUSH | 4 | (empty) | `[status]` |
| CHECKPOINT | 5 | (empty) | `[status]` |
| STATS | 6 | (empty) | `[GravelDBStats struct]` |
| PING | 255 | (empty) | `[status=OK]` |

**Response 格式**：
```
[4B magic = 0x47565242] [4B status] [4B body_len] [body...]
```

### IO Poller

抽象层，统一 epoll/kqueue/poll：

```c
typedef struct IOPoller IOPoller;

IOPoller *io_poller_create(int max_events);
int       io_poller_add(IOPoller *p, int fd, uint32_t events, void *data);
int       io_poller_del(IOPoller *p, int fd);
int       io_poller_wait(IOPoller *p, IOEvent *events, int max, int timeout_ms);
```

- Linux: epoll_create1 + epoll_ctl + epoll_wait
- macOS: kqueue + kevent
- Fallback: poll()

### CkptScheduler Integration

```c
// 服务器启动时配置
GravelServerConfig {
    .data_dir = "/data/embeddings",
    .port = 9527,
    .dims = {64, 128, 256},
    .buffer_size_mb = 512,
    .flush_interval_ms = 1000,    // 每秒自动 flush
    .checkpoint_interval_s = 300,  // 每 5 分钟自动 checkpoint
};
```

Scheduler 在每次 poll 返回后 tick，检查是否该触发 flush/checkpoint。所有操作在主线程内完成（单线程无锁）。

### Client SDK

```c
// 极简 C client
GravelDBClient *client;
graveldb_client_connect(&client, "127.0.0.1", 9527);

// Batch pull
graveldb_client_pull(client, feat_ids, count, out_embeddings, out_dims);

// Batch push
graveldb_client_push(client, feat_ids, dims, embeddings, count);

graveldb_client_close(client);
```

内部：连接池 / 重连由调用方负责（SDK 只做单连接）。

### Scaling Strategy

```
单机多实例（类 Redis）:
  Instance 0: port=9527, data=/ssd0/shard0, core 0-7
  Instance 1: port=9528, data=/ssd1/shard1, core 8-15
  ...

部署方式:
  numactl --cpunodebind=0 --membind=0 ./graveldb-server -p 9527 -d /ssd0/s0
  numactl --cpunodebind=1 --membind=1 ./graveldb-server -p 9528 -d /ssd1/s1

路由：client-side hash(feat_id) % num_shards → 选择实例
```

**注意**：当前实现不含 Facade 路由层或集群管理。集群/分片是部署层面的工作，存储引擎保持 shared-nothing 简洁性。

### 与设计文档的差异

| 设计文档假设 | 实际实现 |
|---|---|
| Facade 路由层 + 蓝绿集群切换 | **无（单节点 server，集群由部署层解决）** |
| Unix socket + TCP dual | **仅 TCP** |
| 1B opcode + 4B count 协议 | **12B header (4B magic + 4B type + 4B body_len)** |
| Shard 间零通信哲学 | **正确，每个实例独立自洽** |
