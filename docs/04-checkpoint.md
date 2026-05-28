# 04 — Checkpoint: Overlay Isolation, DirtyTracker, Delta Files

## Problem

数据量百 GB 级，需要定期持久化保证崩溃恢复能力。但：
- 全量 dump 512 GB → 170s，不可接受
- WAL 在 overwrite 语义下产生 25 MB/batch 冗余写放大
- fork COW 在 12 GB+ 进程中触发页面风暴
- 持久化过程**不能阻塞前台读写**

需要一种增量持久化方案：只 dump 脏页，前台零阻塞。

## Alternatives Considered

| 方案 | 优势 | 劣势 |
|------|------|------|
| WAL | 精确 PITR | 写放大（所有中间态都记录）；embedding overwrite 下冗余 |
| fork + COW (Redis RDB) | 隔离简单 | COW 风暴；io_uring 不兼容；大进程慢 |
| Stop-the-world full dump | 实现简单 | 延迟正比于数据量 |
| **Overlay + DirtyTracker 增量 delta** | 有界 I/O；前台不阻塞；最小写放大 | Overlay 增加 checkpoint 期间的读间接层 |

## Actual Implementation

### State Machine (CkptProgress)

增量 checkpoint 通过状态机实现，每次 `graveldb_checkpoint_step()` 做有界工作：

```
CKPT_IDLE → CKPT_FLUSHING → CKPT_DUMPING → CKPT_FINISHING → CKPT_IDLE
```

| 状态 | 工作内容 | 每步上界 |
|------|---------|---------|
| FLUSHING | 逐 bin flush WriteBuffer + dirty_tracker_swap() | 1 bin/step |
| DUMPING | 扫描冻结 bitmap → pread 脏页 → 写 delta 文件 | max_pages_per_step |
| FINISHING | overlay drain + overlay destroy + epoch++ | 1 bin/step |

也支持阻塞式 `graveldb_checkpoint()` 一次完成。

### DirtyTracker: Radix Bitmap Tree (双缓冲)

**不是**设计文档中的扁平多层位图，而是**段化 radix tree**：

```
顶层：sorted array of DirtySegment（二分查找定位段）
每段：DirtyNode tree
  ├── 深度由数据量决定：
  │   estimated_pages ≤ 4096      → depth=2, span=4096
  │   estimated_pages ≤ 262144    → depth=3, span=262144
  │   estimated_pages > 262144    → depth=4, span=16777216
  ├── 中间节点：bits(64-bit summary) + children[64]
  └── 叶节点：bits 直接作为 64 页的 dirty bitmap
```

**优势 vs 扁平位图**：
- 按需分配内存（稀疏数据不浪费）
- 中间节点 summary 支持快速跳过 clean 区域
- 段化设计适应多 DimBin 共享一个 tracker 的场景

**双缓冲**：
```c
dirty_tracker_swap():
    // O(1) 指针交换
    old_active = tracker->active;
    tracker->active = new_empty_tree();
    tracker->ckpt = old_active;
    // 新写入标记到 active，dump 读 ckpt
```

### Overlay Buffer

Checkpoint 期间前台的写入不能落到正在 dump 的文件上，需要临时缓冲：

- **开放寻址哈希表**（线性探测），sentinel = UINT32_MAX
- **增量 rehash**：load > 70% 触发，每次操作迁移 16 个 slot
- **SlabPool 分配 embedding 数据**：per-dim size 的专用 pool
  - checkpoint_end 时整个 SlabPool destroy（零逐对象释放，O(1) 批量回收）
- **Tombstone 列表**：checkpoint 期间的 delete 操作暂存，end 时 replay

**前台路径（checkpoint 模式）**：
```
Write → overlay_put(entry_idx, data)
Read  → overlay first → miss → write_buf → read_cache → SSD
Delete → overlay_tombstone(entry_idx)
```

### Checkpoint 完整流程

```
checkpoint_begin (per DimBin):
  1. write_buffer_flush_all()    // 确保脏数据落 SSD
  2. dirty_tracker_swap()        // O(1) 冻结当前脏集
  3. overlay_init(dim)           // 创建 overlay + SlabPool
  4. 进入 checkpoint 模式

dump (bounded per step):
  5. dirty_tracker_scan(ckpt_tree)  // 收集冻结的脏页列表
  6. sort pages by offset            // 顺序化 I/O
  7. batch pread dirty pages from SSD
  8. write delta file: [header][pg_start, pg_count, data]...
  9. fdatasync(delta_fd)

checkpoint_end (per DimBin):
  10. overlay drain: 写回 overlay 中的数据到 SSD
  11. replay tombstones → dimbin_free_entry()
  12. overlay_destroy() → SlabPool 整体销毁
  13. 恢复正常模式
```

### Delta File Format

```
┌─────────────────────────────────────────┐
│ DeltaHeader (64B)                        │
│   magic:        0x44454C54 ("DELT")      │
│   version:      1                        │
│   generation:   checkpoint 代数          │
│   dim:          embedding 维度           │
│   slot_size:    字节数                   │
│   bump_ptr:     分配水位                 │
│   num_entries:  脏页条目数               │
│   crc32:        payload 校验             │
├─────────────────────────────────────────┤
│ Entry 0: { pg_start(u32), pg_count(u32) }│
│   data: pg_count × page_size bytes       │
├─────────────────────────────────────────┤
│ Entry 1: ...                             │
└─────────────────────────────────────────┘
```

### Recovery

启动时：
1. 读每个 `.bin` 文件的双 A/B footer，取 generation 高且 CRC 合法的
2. 如果有 delta chain → 按 generation 顺序 replay
3. 顺序扫描 `.keys` 文件重建内存 HashIndex
4. 性能：10 亿 key → ~3s（SSD 顺序读 8 GB）

### CkptScheduler

协作式调度器（cooperative tick），嵌入 event loop：

```c
// 每次 poll 返回后调用
ckpt_scheduler_tick(scheduler):
    if (time_since_last_flush > flush_interval_ms)
        → trigger flush
    if (time_since_last_ckpt > checkpoint_interval_s)
        → trigger checkpoint (incremental step)
```

**Full checkpoint singleflight**：多个并发 checkpoint 请求合并为一次执行 + cooldown 去重。

**Delta chain 管理**：当 chain 长度 ≥ `delta_chain_max`（默认 10）或脏率 > `dirty_ratio_full`（默认 0.5）时触发 full checkpoint，并 purge 旧 delta。

### 与设计文档的差异

| 设计文档假设 | 实际实现 |
|---|---|
| 扁平多层位图 (L0/L1/L2) | **段化 Radix Bitmap Tree**（动态深度，按需分配） |
| atomic_store(&in_checkpoint, true) | **CkptProgress 状态机** + per-bin overlay |
| 单一 dump 线程 | **协作式 step（event loop 内调用，无额外线程）** |
| 单文件自包含 (data+index+footer) | **每 dim 独立 .bin + .keys + delta 文件** |
