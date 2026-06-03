//! Write-Through LRU Cache over Engine
//!
//! Pattern:
//!
//!   GET:  cache.get(id) → hit (ns) → return
//!                       → miss     → engine.get(id) (μs) → cache.insert → return
//!
//!   PUT:  engine.put(ids, vecs) → ok → cache.insert(each) → ack
//!         (persist first, cache second — crash-safe)
//!
//! Why moka:
//!   - Lock-free concurrent cache (no Mutex on the hot read path)
//!   - Async-friendly (moka::future::Cache)
//!   - Built-in LRU eviction with configurable max_capacity
//!   - Used in production by many Rust services
//!
//! Why write-through (not write-back):
//!   - GravelDB already buffers writes internally
//!   - Persist-before-ack = crash-safe, zero data loss
//!   - Cache is disposable — restart = cold start, not data loss

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use moka::future::Cache;

use crate::engine::{self, Engine, Result};

#[derive(Clone)]
pub struct CachedEngine {
    engine: Engine,
    cache: Cache<u64, Arc<Vec<f32>>>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

#[derive(Clone, serde::Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: u64,
    pub capacity: u64,
    pub hit_rate: f64,
}

impl CachedEngine {
    /// Wrap an engine with a write-through LRU cache.
    /// capacity = max entries. Memory ≈ capacity × dim × 4 bytes.
    pub fn new(engine: Engine, capacity: u64) -> Self {
        let cache = Cache::new(capacity);
        CachedEngine {
            engine,
            cache,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get embeddings: cache first, engine on miss.
    pub async fn get(&self, feat_ids: &[u64]) -> Result<Vec<Option<Vec<f32>>>> {
        let mut results: Vec<Option<Vec<f32>>> = vec![None; feat_ids.len()];
        let mut miss_ids: Vec<u64> = Vec::new();
        let mut miss_idx: Vec<usize> = Vec::new();

        // Phase 1: probe cache
        for (i, &id) in feat_ids.iter().enumerate() {
            if let Some(vec) = self.cache.get(&id).await {
                results[i] = Some(vec.as_ref().clone());
                self.hits.fetch_add(1, Ordering::Relaxed);
            } else {
                miss_ids.push(id);
                miss_idx.push(i);
                self.misses.fetch_add(1, Ordering::Relaxed);
            }
        }

        if miss_ids.is_empty() {
            return Ok(results);
        }

        // Phase 2: batch fetch misses from engine (direct FFI, thread-safe)
        let fetched = self.engine.get(&miss_ids)?;

        // Phase 3: populate cache
        for (j, emb_opt) in fetched.into_iter().enumerate() {
            if let Some(emb) = emb_opt {
                let arc_emb = Arc::new(emb.clone());
                self.cache.insert(miss_ids[j], arc_emb).await;
                results[miss_idx[j]] = Some(emb);
            }
        }

        Ok(results)
    }

    /// Put: persist to engine first, then update cache.
    pub async fn put(
        &self,
        feat_ids: Vec<u64>,
        dims: Vec<i32>,
        embeddings: Vec<Vec<f32>>,
    ) -> Result<()> {
        // Step 1: persist (write-through guarantee)
        self.engine.put(feat_ids.clone(), dims, embeddings.clone()).await?;

        // Step 2: update cache (only after confirmed persistence)
        for (i, id) in feat_ids.iter().enumerate() {
            self.cache.insert(*id, Arc::new(embeddings[i].clone())).await;
        }

        Ok(())
    }

    /// Warmup: pre-load hot IDs into cache on startup.
    pub async fn warmup(&self, hot_ids: &[u64]) -> Result<()> {
        let results = self.engine.get(hot_ids)?;
        for (i, emb_opt) in results.into_iter().enumerate() {
            if let Some(emb) = emb_opt {
                self.cache.insert(hot_ids[i], Arc::new(emb)).await;
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
        CacheStats {
            hits,
            misses,
            size: self.cache.entry_count(),
            capacity: self.cache.policy().max_capacity().unwrap_or(0),
            hit_rate,
        }
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.engine.checkpoint().await
    }

    pub async fn flush(&self) -> Result<()> {
        self.engine.flush().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.engine.shutdown().await
    }
}
