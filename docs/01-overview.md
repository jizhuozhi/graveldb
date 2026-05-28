# 01 — Overview: Goals & Architecture

## Design Goals

为大规模推荐系统（CTR/CVR）的 Sparse Embedding 提供轻量级 SSD 存储引擎：

- **10 亿+ feature** 的 embedding 存储（百 GB~TB 级）
- **批量读写为主**：训练 batch lookup / gradient update，推理 batch serving
- **增量 checkpoint**：有界延迟，不阻塞前台读写
- **成本极低**：SSD 比 RAM 便宜 50–100×
- **极简实现**：~2500 行 C，零外部依赖（io_uring 可选）

## Non-Goals

- ✗ ACID 事务
- ✗ WAL（ML 场景容忍丢失最近几个 mini-batch）
- ✗ LSM-Tree / B-Tree / Compaction
- ✗ 变长 value 支持（embedding 是定长 float 向量）
- ✗ 多线程并发控制（单线程 event loop + io_uring 内核并行）

## Overall Architecture

```
┌─────────────────────────────────────────────────┐
│  Client (C SDK / Python via ctypes)             │
│  pull · push · delete · checkpoint · gc         │
└─────────────────────┬───────────────────────────┘
                      │ TCP binary protocol (12B header)
┌─────────────────────┴───────────────────────────┐
│  Parameter Server (single-thread event loop)    │
│  epoll/kqueue · CkptScheduler · cooperative tick│
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────┐
│  GravelDB Engine                                │
│                                                 │
│  ┌─ HashIndex ─────────────────────────────────┐│
│  │  OA + linear probe + incremental rehash      ││
│  │  feat_id → (dim_idx, entry_idx)             ││
│  └─────────────────────────────────────────────┘│
│  ┌─ DimRegistry ──────────────────────────────┐ │
│  │  LINEAR(≤8) → SORTED(≤64) → HASH(>64)     │ │
│  └─────────────────────────────────────────────┘│
│  ┌─ DimBin[] (per-dim slab) ──────────────────┐│
│  │  WriteBuffer (OA page hashmap)              ││
│  │  ReadCache (OA hashmap + TinyLFU admission) ││
│  │  DirtyTracker (radix bitmap tree, 2-buf)    ││
│  │  OverlayBuffer (checkpoint isolation)       ││
│  │  bump_ptr + free_list allocator             ││
│  └─────────────────────────────────────────────┘│
│  ┌─ TinyLFU (CMS 4-row, 4-bit counters) ─────┐│
│  │  ReadCache admission + GC cold detection    ││
│  └─────────────────────────────────────────────┘│
│  ┌─ SlabAllocator (pool-based) ───────────────┐│
│  │  64KB slab pages, per-size free lists       ││
│  └─────────────────────────────────────────────┘│
│  ┌─ CkptScheduler + CkptProgress ────────────┐│
│  │  Cooperative tick, bounded-latency steps    ││
│  └─────────────────────────────────────────────┘│
└─────────────────────┬───────────────────────────┘
                      │ pread/pwrite (io_uring batch on Linux)
┌─────────────────────┴───────────────────────────┐
│  SSD: dim_{N}.bin · dim_{N}.keys · ckpt/        │
└─────────────────────────────────────────────────┘
```

## Core Principles

1. **单线程 event loop**：类 Redis，无 mutex/atomic/CAS。I/O 并行靠 io_uring 内核侧完成。
2. **按 Dim 分文件，定长 slab**：相同维度 embedding 等长，offset = entry_idx × slot_size。
3. **WriteBuffer + ReadCache 分离**：写路径不污染读缓存，ReadCache 页始终 clean。
4. **Overlay 隔离的增量 checkpoint**：前台不阻塞，增量只 dump 脏页。
5. **TinyLFU 驱动**：ReadCache 准入控制 + GC 冷键判定。
6. **Shared-nothing 横向扩展**：进程级分片，每个实例独立自洽。

## Data Flow

### Put Path
```
graveldb_put(feat_id, dim, embedding)
  → dim_registry_find(dim)         // O(1) 查找 bin
  → hash_index_get(feat_id)        // 检查是否已存在
  → dimbin_put(bin, entry_idx, embedding)
      checkpoint_mode? → overlay_put()
      normal_mode?     → write_buf_ensure_page() → memcpy → mark_dirty
                       → proactive_flush (每 64 写随机刷 4 页)
                       → water_level_flush (3/4 满时)
```

### Get Path
```
graveldb_get(feat_id)
  → hash_index_get(feat_id)
  → dimbin_get(bin, entry_idx)
      1. overlay (checkpoint mode only)
      2. write_buf forward (命中 → memcpy，最新数据)
      3. read_cache (TinyLFU-admitted pages)
      4. pread from SSD → attempt read_cache_load
  → tinylfu_access()
```
