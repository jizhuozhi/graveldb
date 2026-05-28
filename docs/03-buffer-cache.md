# 03 — Buffer & Cache: WriteBuffer, ReadCache, TinyLFU Admission

## Problem

数据量远超 RAM，但访问模式有明显热度分层（推荐系统 Zipfian 分布）。需要：
1. 写入路径低延迟（不能每次 pwrite+fsync）
2. 读路径利用热度局部性（避免所有读都打到 SSD）
3. 抗扫描污染（bulk load / scan 不应该挤掉热数据）

## Alternatives Considered

| 方案 | 优势 | 劣势 |
|------|------|------|
| OS page cache (mmap) | 零拷贝；透明 | 不可控驱逐；TLB shootdown；无频率感知 |
| 统一 buffer pool (InnoDB) | 完全控制；pin/unpin | LRU 驱逐时脏页需 writeback |
| Read-through cache | 简单 | 写入无加速 |
| **WriteBuffer + ReadCache 分离** | 写不污染读；read cache clean（驱逐无 I/O） | 两个结构 |

## Actual Implementation

### WriteBuffer

**数据结构**：开放寻址哈希表（Fibonacci hash + 线性探测），key = `page_id`，value = 指向 4KB page data 的指针。

```
page_id = entry_idx / entries_per_page
page 内 offset = (entry_idx % entries_per_page) * slot_size
```

**写入路径**：
1. `write_buf_ensure_page(page_id)`：如果 page 不在 buffer → pread 整页到内存
2. `memcpy(page + offset, embedding, slot_size)` — 纯内存操作，~5 ns
3. `dirty_tracker_mark(page_id)` — 标记脏

**三级 Flush 策略**（避免延迟尖刺）：

| 级别 | 触发条件 | 行为 |
|------|---------|------|
| Proactive | 每 64 次 put | 随机采样 4 个脏页，pwrite+fdatasync |
| Water-level | count ≥ max_pages × 3/4 | 全量 flush |
| Full | count ≥ max_pages | 强制 flush（在 ensure_page 中） |

**Flush 实现**：
1. 收集所有脏 page_id
2. `qsort` 排序（使后续写入顺序化）
3. **Peephole gap merge**：扫描排序后的 page 序列，如果两个 run 之间间隔 ≤4 pages，合并为单次大 I/O
4. 通过 `io_uring_flush_pages()` 批量提交所有写入 + 一次 fdatasync
5. 清除 page entries，释放 page 内存

### ReadCache

**数据结构**：开放寻址哈希表，key = `page_id`，value = 指向 4KB read-only page data 的指针。

**关键性质**：ReadCache 中的 page **始终 clean**（从 SSD 读入后不被修改）。驱逐时直接丢弃，零 writeback I/O。

**操作**：

1. **Load**（`read_cache_load`）：从 SSD pread 整页 → 尝试放入 cache
2. **Admission 门控**：
   - 如果 cache 未满 → 直接放入
   - 如果 cache 已满 → 比较新页频率 vs 随机采样 victim 的频率
   - `new_freq < victim_freq` → **拒绝 admission**（scan resistance）
   - 否则 → 驱逐 victim，放入新页
3. **Eviction**（`read_cache_evict`）：Sampled-LFU
   - 随机采样 `EVICT_SAMPLE_COUNT=5` 个占用 slot
   - 取频率最低的驱逐
4. **Invalidation**：`dimbin_put()` 时如果 ReadCache 中有该 page → 立即删除（写 invalidate）

### TinyLFU — 频率估计器

ReadCache 的 admission 和 GC 的冷键判定共用一个全局 TinyLFU 实例。

**实现细节**：
- **Count-Min Sketch**：4 行 × W 列，每 cell 8-bit（saturate at 15）
- **Hash functions**：4 个独立 hash（不同 seed 的 fibonacci/multiply hash）
- **Access**：`tinylfu_access(feat_id)` — 4 行对应位置 +1（saturate）
- **Estimate**：`tinylfu_estimate(feat_id)` — 4 行取 min
- **Decay**：每 `cms_width × 10` 次 access 后，全部 counter 右移 1 位（指数遗忘，隐式时间衰减）
- **Promote**：`tinylfu_promote(feat_id)` — 强制置 max（GC 复活时使用）

**参数选择**：
- `cms_width` 默认 = `index_capacity / 4`（越大越精确，内存 = 4 × W bytes）
- Decay threshold = `cms_width × 10`（平均每个 counter 被 access 10 次后全局衰减）

### 与设计文档的差异

| 设计文档假设 | 实际实现 |
|---|---|
| Block Buffer with Clock eviction | **WriteBuffer(OA hashmap) + ReadCache(OA hashmap + Sampled-LFU)** |
| 统一读写 buffer | **读写分离** |
| Clock reference bit | **随机采样 5 候选取 min-freq** |
| 驱逐时需 writeback | **ReadCache 始终 clean，零 writeback** |

### Memory Scaling Benchmark Results

固定 100K features × dim=128 (48.8 MB 数据)，不同 cache/data ratio：

| Access Pattern | 1% cache | 25% cache | 100% cache | Speedup |
|---|---|---|---|---|
| Zipfian (θ=0.99) | 4.0 M ops/s | 5.1 M ops/s | 5.1 M ops/s | 1.5× |
| Uniform random | 1.6 M ops/s | 2.5 M ops/s | 3.3 M ops/s | 2.1× |
| Hotspot (80/20) | 1.9 M ops/s | 4.7 M ops/s | 4.7 M ops/s | 2.7× |

TinyLFU admission 在 1% cache ratio 下仍达 56% hit rate（Zipfian），说明频率感知准入有效过滤了低频噪声。
