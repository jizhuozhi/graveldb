package main

/*
 * CachedEngine — Write-Through LRU Cache over Engine
 *
 * Pattern:
 *
 *   GET path:
 *     cache.Get(id) → hit (ns) → return
 *                   → miss     → engine.Get(id) (μs) → cache.Add → return
 *
 *   PUT path:
 *     engine.Put(ids, vecs) → success → cache.Add(each) → ack
 *     (persist first, cache second — crash-safe)
 *
 * Why this works well with GravelDB:
 *   - GravelDB reads are already fast (μs, in-memory overlay)
 *   - But for hot embeddings accessed 1000x/sec, even μs adds up
 *   - LRU cache gives ns-level hit for hot set (typically top 10-20% IDs)
 *   - Write-through keeps cache consistent — no stale reads, no dirty tracking
 *   - Cache is disposable — crash/restart just means cold-start, no data loss
 */

import (
	"context"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

type CacheStats struct {
	Hits      uint64  `json:"hits"`
	Misses    uint64  `json:"misses"`
	Size      int     `json:"size"`
	Capacity  int     `json:"capacity"`
	HitRate   float64 `json:"hit_rate"`
}

type cachedVec struct {
	embedding []float32
}

type CachedEngine struct {
	engine *Engine
	cache  *lru.Cache[uint64, cachedVec]
	hits   atomic.Uint64
	misses atomic.Uint64
}

// WrapWithCache adds a write-through LRU cache in front of the engine.
// capacity = max entries. Memory budget ≈ capacity × dim × 4 bytes.
func WrapWithCache(engine *Engine, capacity int) (*CachedEngine, error) {
	cache, err := lru.New[uint64, cachedVec](capacity)
	if err != nil {
		return nil, err
	}
	return &CachedEngine{engine: engine, cache: cache}, nil
}

// Get retrieves embeddings: cache first, engine on miss.
func (ce *CachedEngine) Get(ctx context.Context, featIDs []uint64) ([][]float32, error) {
	results := make([][]float32, len(featIDs))
	var missIDs []uint64
	var missIdx []int

	// Phase 1: probe cache
	for i, id := range featIDs {
		if val, ok := ce.cache.Get(id); ok {
			results[i] = val.embedding
			ce.hits.Add(1)
		} else {
			missIDs = append(missIDs, id)
			missIdx = append(missIdx, i)
			ce.misses.Add(1)
		}
	}

	if len(missIDs) == 0 {
		return results, nil
	}

	// Phase 2: batch fetch misses from engine (direct FFI, thread-safe)
	fetched, err := ce.engine.Get(ctx, missIDs)
	if err != nil {
		return nil, err
	}

	// Phase 3: populate cache
	for j, emb := range fetched {
		results[missIdx[j]] = emb
		if emb != nil {
			ce.cache.Add(missIDs[j], cachedVec{embedding: emb})
		}
	}

	return results, nil
}

// Put writes to engine first (persist), then updates cache (accelerate future reads).
func (ce *CachedEngine) Put(ctx context.Context, featIDs []uint64, dims []int, embeddings [][]float32) error {
	if err := ce.engine.Put(ctx, featIDs, dims, embeddings); err != nil {
		return err
	}
	// Write-through: only cache after confirmed persistence
	for i, id := range featIDs {
		ce.cache.Add(id, cachedVec{embedding: embeddings[i]})
	}
	return nil
}

// Warmup pre-loads hot IDs into cache. Call on startup to avoid cold-start penalty.
func (ce *CachedEngine) Warmup(ctx context.Context, hotIDs []uint64) error {
	results, err := ce.engine.Get(ctx, hotIDs)
	if err != nil {
		return err
	}
	for i, emb := range results {
		if emb != nil {
			ce.cache.Add(hotIDs[i], cachedVec{embedding: emb})
		}
	}
	return nil
}

func (ce *CachedEngine) Stats() CacheStats {
	hits := ce.hits.Load()
	misses := ce.misses.Load()
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return CacheStats{
		Hits:     hits,
		Misses:   misses,
		Size:     ce.cache.Len(),
		Capacity: ce.cache.Cap(),
		HitRate:  hitRate,
	}
}

func (ce *CachedEngine) Checkpoint(ctx context.Context) error {
	return ce.engine.Checkpoint(ctx)
}

func (ce *CachedEngine) Flush(ctx context.Context) error {
	return ce.engine.Flush(ctx)
}

func (ce *CachedEngine) Close() error {
	return ce.engine.Close()
}
