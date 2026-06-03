# GravelDB Examples

Practical examples demonstrating how to embed and use libgraveldb in various scenarios.

## Overview

| Example | Language | Key Concepts |
|---------|----------|--------------|
| [minimal/](minimal/) | C | Simplest possible usage: open → put → get → checkpoint → close |
| [replica/](replica/) | C | Double-buffer hot reload + lock-free multi-threaded reads |
| [cgo/](cgo/) | Go | CGo FFI + dedicated goroutine + write-through LRU cache + HTTP service |
| [rust-tokio/](rust-tokio/) | Rust | Tokio async + dedicated thread + write-through cache (moka) + Axum HTTP |

## minimal/

**30 seconds to understand the API.** Opens a GravelDB instance, writes 10 embeddings, reads them back, checkpoints to disk.

```bash
cc -o minimal minimal/minimal.c -I../include -L../build -lgraveldb_lib -lpthread
./minimal
```

## replica/

**Core production pattern.** Demonstrates how a read-only replica service uses two GravelDB instances (double buffer) to hot-reload checkpoint dumps without blocking readers:

1. Master exports a checkpoint dump file
2. Loader thread imports dump into the standby buffer
3. Atomic pointer swap makes new data visible
4. N reader threads query the active buffer with zero locks

```bash
cc -o replica replica/replica.c -I../include -L../build -lgraveldb_lib -lpthread
./replica
```

## cgo/

**Go embedding pattern with write-through cache.** Shows how to safely use libgraveldb from Go:

- `graveldb.go` — Raw CGo bindings
- `engine.go` — Safe async wrapper: dedicated goroutine with `runtime.LockOSThread()`, channel-based command dispatch
- `cached_engine.go` — Write-through LRU cache layer (`golang-lru/v2`): cache hit = ns, miss = μs + auto-populate
- `main.go` — HTTP server (`GET /get?id=123`) with cache-accelerated reads + `/stats` hit rate monitoring

Key design decisions:
- Write-through: persist to GravelDB first, update cache second — crash-safe, no stale reads
- LRU eviction: memory-bounded, hot embeddings stay cached
- Cache warmup on startup to avoid cold-start penalty
- Cache is disposable — restart just means temporary cold reads, no data loss

```bash
cd cgo/
CGO_CFLAGS="-I../../include" CGO_LDFLAGS="-L../../build -lgraveldb_lib" go build .
./cgo
```

## rust-tokio/

**Rust async embedding pattern with write-through cache.** Shows how to embed libgraveldb in a Tokio async service:

- `src/ffi.rs` — Raw unsafe FFI declarations
- `src/engine.rs` — Safe async Engine: dedicated OS thread + mpsc/oneshot channels
- `src/cached_engine.rs` — Write-through cache layer (moka): lock-free concurrent LRU, async-friendly
- `src/main.rs` — Axum HTTP server + cache warmup + `/stats` hit rate

Key design decisions:
- Write-through: `engine.put()` first (channel → writer thread), then `cache.insert()` — crash-safe
- moka cache: lock-free reads (no Mutex on hot path), built-in LRU eviction
- Cache warmup pre-loads hot IDs on startup
- `/stats` endpoint exposes real-time hit rate for production monitoring

```bash
cd rust-tokio/
cargo build --release
./target/release/graveldb-tokio-example
```

## Architecture Pattern (shared by CGo and Rust)

```
┌──────────────────────────────────────────────────────┐
│  Async Runtime (goroutines / tokio tasks)             │
│                                                      │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                │
│  │ reader  │ │ reader  │ │ reader  │                 │
│  │ task    │ │ task    │ │ task    │                 │
│  └────┬────┘ └────┬────┘ └────┬────┘                │
│       │            │           │                     │
│       └────────────┼───────────┘                     │
│                    ▼                                 │
│           ┌────────────────┐                         │
│           │  LRU Cache     │  hit → respond (ns)     │
│           └───────┬────────┘                         │
│                   │ miss                             │
│                   ▼                                  │
│           graveldb_batch_get()   (μs, direct FFI)    │
│           → populate cache → respond                 │
│                                                      │
│  ┌─────────┐ ┌──────────────┐                        │
│  │ write   │ │ checkpoint   │                        │
│  │ task    │ │ timer task   │                        │
│  └────┬────┘ └──────┬───────┘                        │
│       └──────┬───────┘                               │
│              │ (mpsc channel)                        │
└──────────────│───────────────────────────────────────┘
               ▼
┌──────────────────────────────────┐
│  Dedicated Writer Thread          │
│  put → success → cache.Add → ack │
│  (write-through: persist first)   │
└──────────────────────────────────┘
```

Why this pattern:
1. **Reads are cache-accelerated** — hot embeddings return in nanoseconds (hash lookup + memcpy)
2. **Cache misses fall through to GravelDB** — still fast (μs, overlay isolation), and auto-populate cache
3. **Write-through** — persist before ack, then update cache. Crash-safe, no stale reads.
4. **Cache is disposable** — restart = cold start, not data loss. Can rebuild from DB anytime.
5. **Checkpoint never blocks reads** — overlay isolation means reads work during checkpoint.
6. **Write path uses channel as serialization** — no mutex needed, single-writer correctness.
