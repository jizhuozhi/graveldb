# GravelDB

A high-performance SSD-native embedding storage engine designed for large-scale recommendation systems (CTR/CVR). Serves as both an embedded library and a standalone parameter server.

**Single-threaded** · **~2500 LOC pure C** · **Zero external dependencies** · **io_uring optional**

## Key Features

- **Per-dimension slab storage** — zero fragmentation, O(1) offset addressing, sequential I/O friendly
- **Separated WriteBuffer + ReadCache** — decoupled write path (OA page hashmap) and read path (TinyLFU-admitted cache)
- **TinyLFU admission control** — scan-resistant, frequency-aware eviction via Count-Min Sketch
- **Incremental checkpoint** — overlay-based isolation, dirty-page-only delta dumps, event-loop compatible (`checkpoint_step`)
- **io_uring batch flush** — kernel-side I/O parallelism on Linux; automatic pwrite fallback on macOS
- **Horizontal scaling** — shared-nothing process sharding, no locks/atomics/CAS in hot path

## Performance

**Environment**: 100K features, dim=128 (512 B/entry), Apple M-series SSD, single thread.

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Put (write buffer) | 1.5–3 M ops/s | avg ~0.3 µs, p99 < 1 µs |
| Get (buffer forward) | 2–4 M ops/s | avg ~0.3 µs, p99 < 1 µs |
| Get (read cache hit) | 1.5–3 M ops/s | avg ~0.4 µs |
| Get (cold disk) | 80–150 K ops/s | avg ~8 µs, p99 ~20 µs |
| Batch put (1000/batch) | 2–3 M entries/s | per-batch ~400 µs |
| Flush (50K dirty pages) | ~300 MB/s | — |
| Checkpoint (10% dirty, 100K features) | — | < 50 ms |
| Recovery (100K keys rebuild) | 3–5 M keys/s | < 30 ms |

### Memory Scaling (ReadCache)

Varying cache-to-data ratio under different access patterns (100K features, dim=128, 100K queries):

| Access Pattern | 1% Cache | 25% Cache | 100% Cache | Max Speedup |
|----------------|----------|-----------|------------|-------------|
| Zipfian (θ=0.99) | 4.0 M ops/s | 5.1 M ops/s | 5.1 M ops/s | 1.5× |
| Uniform random | 1.6 M ops/s | 2.5 M ops/s | 3.3 M ops/s | 2.1× |
| Hotspot (80/20) | 1.9 M ops/s | 4.7 M ops/s | 4.7 M ops/s | 2.7× |

Key observations:
- TinyLFU admission achieves **56–80% hit rate** at just 1% cache ratio under Zipfian workloads
- Hotspot patterns see a **phase transition** when cache covers the hot working set (~20–25%)
- Scan pollution survival: TinyLFU > 90% vs. FIFO ~20%

### Write Buffer vs Direct I/O

- Sustained random reads after write pressure: buffer forwarding **30–60×** faster than pread (after OS page cache LRU thrashing)
- Per-write fsync scenario: batched buffer amortization **3–5×** faster than per-write fdatasync

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Client (C SDK / Python via ctypes)             │
│  pull · push · delete · checkpoint · gc         │
└─────────────────────┬───────────────────────────┘
                      │ TCP binary protocol (12B header)
┌─────────────────────┴───────────────────────────┐
│  Parameter Server (single-thread event loop)    │
│  epoll/kqueue/poll · max 256 clients            │
│  CkptScheduler · auto flush/checkpoint          │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────┐
│  GravelDB Engine                                │
│                                                 │
│  ┌─ HashIndex ─────────────────────────────────┐│
│  │  Open addressing + linear probe + incr rehash│
│  │  feat_id → (dim_idx, entry_idx)             ││
│  └─────────────────────────────────────────────┘│
│  ┌─ DimRegistry ──────────────────────────────┐ │
│  │  LINEAR(≤8) → SORTED(≤64) → HASH(>64)     │ │
│  └─────────────────────────────────────────────┘│
│  ┌─ DimBin[] (per-dim slab) ──────────────────┐│
│  │  WriteBuffer (OA page hashmap)              ││
│  │  ReadCache (OA hashmap + TinyLFU)           ││
│  │  DirtyTracker (radix bitmap tree, 2-buf)    ││
│  │  OverlayBuffer (checkpoint isolation)       ││
│  │  SlabAllocator (bump + free list)           ││
│  └─────────────────────────────────────────────┘│
│  ┌─ TinyLFU (CMS 4-row, 4-bit counters) ─────┐│
│  │  GC eviction + read cache admission         ││
│  └─────────────────────────────────────────────┘│
└─────────────────────┬───────────────────────────┘
                      │ pread/pwrite (io_uring batch on Linux)
┌─────────────────────┴───────────────────────────┐
│  SSD: dim_{N}.bin · dim_{N}.keys · ckpt/        │
└─────────────────────────────────────────────────┘
```

## Design Decisions

GravelDB's design is driven by the specific constraints of embedding serving: fixed-length values, overwrite-dominant workload, extreme read fan-out, and data volumes (10–100 GB) that far exceed available RAM. Each section below frames a **classic storage engine problem**, contrasts **common approaches**, and explains **GravelDB's choice**.

---

### 1. Concurrency Model

| Approach | Pros | Cons |
|----------|------|------|
| Multi-threaded + fine-grained locks | Scales CPU-bound workloads | Lock contention on shared index; complex correctness; cache-line bouncing |
| Lock-free (CAS/atomics) | No blocking | ABA problems; memory reclamation (EBR/HP) complexity; limited to simple structures |
| **Single-threaded event loop** | Zero synchronization overhead; deterministic latency; simple reasoning | CPU-bound tasks block the loop |

**GravelDB's choice**: Single-threaded (Redis-style).

**Rationale**: The bottleneck is SSD I/O latency (~10 µs/op), not CPU. A single core saturates the I/O path without contention. I/O parallelism is offloaded to io_uring (kernel-side completion queues). Horizontal scaling is achieved via process-level sharding (shared-nothing), avoiding all cross-thread coordination.

---

### 2. Storage Layout

| Approach | Pros | Cons |
|----------|------|------|
| LSM-tree (RocksDB, LevelDB) | Good write amplification for small KVs; range scans | Compaction storms; read amplification; unaware of fixed-length structure |
| B+ tree (InnoDB, WiredTiger) | Balanced read/write; range queries | Page splits; variable-length overhead; fragmentation under update-heavy workloads |
| Heap file + index | Simple | Fragmentation; no locality guarantees |
| **Per-dimension fixed-size slab** | Zero fragmentation; O(1) addressing; sequential batch I/O | Only works for fixed-length values; one file per dimension |

**GravelDB's choice**: Per-dim slab files (`dim_128.bin`) with bump-pointer allocation + free list.

**Rationale**: All embeddings of the same dimension are byte-identical in size. This eliminates the need for variable-length record management, page splits, or compaction. Addressing is a single multiply (`offset = entry_idx × slot_size`), and batch writes to the same dimension are naturally sequential on disk.

---

### 3. Read Path — Buffer & Cache Strategy

| Approach | Pros | Cons |
|----------|------|------|
| OS page cache (mmap) | Zero-copy; transparent | Uncontrollable eviction; TLB shootdowns; no frequency awareness; incompatible with io_uring |
| Unified buffer pool (InnoDB-style) | Full control; pin/unpin semantics | Complex LRU/clock management; dirty page writeback on eviction |
| **Separated WriteBuffer + ReadCache** | Write path never pollutes read cache; read cache always clean (no writeback); independent sizing | Two structures to manage |

**GravelDB's choice**: Decoupled WriteBuffer (absorbs writes) + ReadCache (serves reads, TinyLFU-admitted).

**Rationale**: Embedding workloads are heavily read-biased (training pull ≫ push). Separating the paths means:
- WriteBuffer acts as a coalescing layer — multiple updates to the same page merge before flush
- ReadCache pages are **always clean** — eviction is free (no writeback I/O)
- TinyLFU admission rejects scan/bulk-load pollution, maintaining hit rate for the true working set

**WriteBuffer flush policy** (multi-level, avoids latency spikes):
1. Proactive: every 64 writes, randomly flush 4 pages (amortized background drain)
2. Water-level: triggered at 3/4 capacity (moderate pressure)
3. Full: sorted page list + peephole gap merge (≤4-page gaps coalesced into single I/O)

**ReadCache eviction**: Sampled-LFU (5 random candidates → evict lowest frequency). Admission gate: new page rejected if frequency < random existing victim's frequency.

---

### 4. Persistence & Crash Recovery

| Approach | Pros | Cons |
|----------|------|------|
| WAL (write-ahead log) | Point-in-time recovery; transaction support | 50K emb × 512 B = 25 MB/batch write amplification; intermediate states redundant under overwrite semantics |
| fork + COW snapshot (Redis RDB) | Simple isolation | 12 GB+ COW page storms; incompatible with io_uring; page-granularity only |
| Full checkpoint (stop-the-world) | Simple implementation | Blocks all I/O during dump; latency spike proportional to data size |
| **Incremental delta checkpoint + overlay isolation** | Bounded I/O per step; no blocking; minimal write amplification | Overlay adds read indirection during checkpoint window |

**GravelDB's choice**: Overlay-based incremental checkpoint with DirtyTracker.

**Rationale**: Embedding updates are pure overwrites — there's no transaction log to replay. WAL would write every intermediate value that gets overwritten within the same flush interval. Instead:

```
checkpoint_begin:
  1. Flush write buffer → SSD
  2. dirty_tracker_swap()    // O(1) pointer swap; new writes mark fresh tree
  3. overlay_init()          // Checkpoint mode: writes go to overlay

During checkpoint (foreground unblocked):
  Write → overlay (in-memory OA hashmap + SlabPool)
  Read  → check overlay first → miss falls through to normal path

Dump (bounded work per step):
  Scan checkpoint dirty tree → sort pages → batch pread → write delta file → fdatasync

checkpoint_end:
  Drain overlay back to main storage → destroy SlabPool → resume normal
```

Two modes: blocking (`graveldb_checkpoint`) and event-loop-friendly incremental (`graveldb_checkpoint_step` with configurable pages-per-step).

---

### 5. I/O Submission Strategy

| Approach | Pros | Cons |
|----------|------|------|
| Synchronous pread/pwrite | Simple; portable | One syscall per I/O; kernel round-trip overhead |
| Thread pool + blocking I/O | Parallelism without async API | Thread management; context switches; memory overhead |
| AIO (libaio/POSIX aio) | Async without threads | Limited batching; poor API ergonomics; alignment restrictions |
| **io_uring (with pwrite fallback)** | True zero-copy batching; kernel-side parallelism; single syscall for N ops | Linux 5.1+ only |

**GravelDB's choice**: io_uring on Linux; automatic fallback to sorted pwrite + fdatasync on macOS/older kernels.

**Rationale**: Flush and checkpoint dump dozens to thousands of pages per batch. io_uring submits the entire batch in a single `io_uring_enter` syscall, and the kernel completes them in parallel across NVMe queues. On non-Linux platforms, the fallback sorts pages by offset for sequential I/O and uses a single fdatasync to amortize the fsync cost.

---

> **Deep dive**: See [`docs/`](docs/) for detailed per-subsystem design documents covering implementation internals, data structures, and algorithms.

## File Layout

```
data_dir/
├── dim_128.bin        # Embedding data (bump-pointer append)
├── dim_128.keys       # uint64_t feat_id array (indexed by entry_idx)
├── dim_64.bin
├── dim_64.keys
└── ckpt/
    ├── delta_000001.bin   # [header][blk_start, blk_count, data]...
    └── delta_000002.bin
```

- `.bin` — embedding data, bump-pointer allocated
- `.keys` — flat feat_id array; sequentially scanned at startup to rebuild HashIndex
- `delta_*.bin` — incremental snapshots, replayed in generation order during recovery

## Wire Protocol

```
Header: [4B magic=0x50535256] [4B msg_type] [4B body_len]
```

| Message | Type | Description |
|---------|------|-------------|
| PULL | 1 | Batch read embeddings |
| PUSH | 2 | Batch write/update embeddings |
| DELETE | 3 | Delete features |
| FLUSH | 4 | Force flush to disk |
| CHECKPOINT | 5 | Trigger incremental checkpoint |
| STATS | 6 | Retrieve runtime statistics |
| GC | 7 | Trigger garbage collection |
| PING | 8 | Health check |

## Building

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

**Requirements**: C11 compiler (GCC 7+ / Clang 10+ / MSVC 2019+).

**Linux io_uring support**: `apt install liburing-dev` (optional; falls back to pwrite+fdatasync).

**macOS**: Fully supported via kqueue + pwrite fallback.

## Testing

```bash
cd build && ctest --output-on-failure
```

## Benchmarks

```bash
./build/bench-main              # Multi-dim throughput + batch put
./build/bench-latency           # p50/p99/p999 latency distribution
./build/bench-write-buffer      # Write buffer vs direct I/O comparison
./build/bench-cache             # TinyLFU hit rate + scan resistance
./build/bench-memory-scaling    # ReadCache speedup vs memory allocation
./build/bench-hash-index        # Hash table grow/lookup/delete
./build/bench-slab-alloc        # Slab allocator vs malloc
./build/bench-dirty-tracker     # Radix tree mark/scan throughput
```

### bench-memory-scaling

Measures throughput scaling as a function of cache-to-data ratio under Zipfian, uniform, and hotspot access patterns.

```bash
./build/bench-memory-scaling                    # Default: 500K features, dim=128, 200K queries
./build/bench-memory-scaling 100000 128 100000  # Custom: num_features dim num_queries
```

## Usage

### Parameter Server

```bash
./build/graveldb-server \
  -d /data/embeddings \
  -p 9527 \
  -D 64,128,256 \
  -b 512 \
  --flush-ms 1000 \
  --checkpoint-s 300 \
  --enable-gc
```

### C Client SDK

```c
#include "client.h"

GravelDBClient *client = NULL;
graveldb_client_connect(&client, "127.0.0.1", 9527);

// Batch push
uint64_t ids[] = {1001, 1002};
int dims[] = {128, 128};
float emb1[128], emb2[128];
const float *embeddings[] = {emb1, emb2};
graveldb_client_push(client, ids, dims, embeddings, 2);

// Batch pull
float out1[128], out2[128];
float *outputs[] = {out1, out2};
int out_dims[2];
graveldb_client_pull(client, ids, 2, outputs, out_dims);

graveldb_client_close(client);
```

### Embedded Library (No Network)

```c
#include "graveldb.h"

GravelDBConfig config = {
    .data_dir = "/data/embeddings",
    .dims = (int[]){64, 128, 256},
    .num_dims = 3,
    .buffer_size = 256 * 1024 * 1024,  // 256 MB per dim bin
    .index_capacity = 1 << 24,         // 16M slots
};

GravelDB *db;
graveldb_open(&db, &config);

float emb[128];
graveldb_put(db, feat_id, 128, emb);
graveldb_get(db, feat_id, out_buf, &dim);

// Incremental checkpoint (event-loop friendly)
graveldb_checkpoint_step(db, 256);
while (graveldb_checkpoint_in_progress(db)) {
    graveldb_checkpoint_step(db, 256);
    // ... handle other events ...
}

graveldb_close(db);
```

## License

MIT
