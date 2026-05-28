# 02 — Storage Layout: Per-Dim Slabs & Allocator

## Problem

Embedding 存储的特殊约束：
- 同维度的 embedding 字节长度完全相同（dim × 4B）
- 维度种类极少（通常 5–10 种：16, 32, 64, 128, 256）
- 数据量巨大（百 GB–TB），远超 RAM
- 访问模式：大量 overwrite，极少 insert/delete

需要一种存储布局能利用"定长"特性，避免通用 KV 引擎的碎片和写放大。

## Alternatives Considered

| 方案 | 优势 | 劣势 |
|------|------|------|
| LSM-Tree (RocksDB) | 通用、写吞吐高 | 读放大；compaction 写放大；无法利用定长 |
| B+ Tree (InnoDB) | 读写平衡 | Page split；变长管理复杂 |
| 单一大文件 + 变长分配 | 灵活 | 碎片；寻址需索引 |
| **Per-dim 定长 slab 文件** | 零碎片；O(1) 寻址；批量写顺序 | 每个 dim 一对文件；不支持变长 |

## Actual Implementation

### File Layout

每个维度一对文件：

```
data_dir/
├── dim_128.bin      # embedding 数据，bump-pointer 追加
├── dim_128.keys     # uint64_t feat_id 平坦数组 (indexed by entry_idx)
├── dim_64.bin
├── dim_64.keys
└── ckpt/            # checkpoint delta/full files
```

**Key 与 Value 分离的理由**：
- embedding 需要 cache line / SIMD 对齐，混入 8B key 破坏对齐或浪费 padding
- `.keys` 文件极小（10 亿 key × 8B = 8 GB vs embedding 数百 GB）
- 启动时顺序扫描 `.keys` 重建内存 HashIndex（3–5 M keys/s）

### Entry Addressing

```
slot_size = ALIGN_UP(dim * sizeof(float), entry_align)  // 可配置对齐
offset    = entry_idx × slot_size                        // O(1) 直接定位
```

### Allocator: Bump Pointer + Free List

```c
// DimBin 内部的分配器
struct {
    uint32_t  bump_ptr;        // 下一个未使用位置
    uint32_t *free_list;       // 回收的 entry_idx 数组
    uint32_t  free_count;
    uint32_t  free_capacity;
};

// 分配：优先复用回收的 slot
static uint32_t dimbin_alloc_entry(DimBin *s) {
    if (s->free_count > 0)
        return s->free_list[--s->free_count];
    return s->bump_ptr++;
}

// 释放：加入 free list
static void dimbin_free_entry(DimBin *s, uint32_t entry_idx) {
    // grow free_list if needed (realloc 2x)
    s->free_list[s->free_count++] = entry_idx;
}
```

**注意**：单线程设计，无需 TLAB / atomic。

### SlabAllocator (内存分配)

GravelDB 内部有一个 pool-based `SlabAllocator` 用于管理**内存中的辅助结构**（overlay pages、read cache pages 等）：

- 每个 `SlabPool` 管理一种固定大小的对象
- 64 KB slab pages，切分成等大对象，free list 管理
- 最多 32 个 pool，按 size 注册
- 对齐分配支持 SIMD-friendly allocation
- Bulk alloc/free 支持批量操作
- Overlay 使用专用 SlabPool，checkpoint_end 时整体 destroy（零逐对象释放）

### DimRegistry: 自适应 dim → bin 查找

运行时维度可动态添加，查找结构随规模自动升级：

```
n ≤ 8:    LINEAR   — O(n) 线性扫描（≤1 cache line，实测最快）
8 < n ≤ 64: SORTED — O(log n) 二分查找（L1 cache 内）
n > 64:   HASH     — O(1) 开放寻址（25% load factor）
```

升级触发：`dim_registry_add()` 时 count 超过阈值自动升级。不降级（维度不会被删除）。

### HashIndex: feat_id → (dim_idx, entry_idx)

全局唯一索引，将 feature ID 映射到物理位置：

- 开放寻址 + 线性探测（cache-friendly）
- **增量 rehash**：load > 70% 时分配 2× 新表，每次 put/get/remove 搬迁 16 个 slot。大表数千次操作搬完，单次操作最坏 O(16) 可控
- `feat_id = 0` 作为空标记（sentinel），API 约束 feat_id 必须非零
- 每个 slot：`{ uint64_t feat_id, uint16_t dim_idx, uint32_t entry_idx }`

### Batch Put 优化

`graveldb_batch_put()` 针对大 batch 做了特别优化：

1. 按 dim 分组（insertion sort）→ 连续 bump alloc
2. `dimbin_put_keys_batch()`：按 key-page 分桶，密度 ≥4 则 read-modify-write 整页（减少 pwrite 次数），否则单独 pwrite
3. Batch 内所有 entry 共享一次 flush 决策

### Footer (Crash Safety)

每个 `.bin` 文件末尾有双 A/B footer（各 64B），交替写入：

```
Footer: {
    magic, version, dim, num_entries, bump_ptr,
    free_list_offset, free_list_size,
    generation, crc32
}
```

重启时读两个 footer，取 generation 大且 CRC 合法的为准。免 WAL 的持久化安全保证。
