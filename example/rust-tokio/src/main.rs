//! GravelDB Rust Tokio Example — Write-Through Cache + Async Service
//!
//! Demonstrates:
//!   1. Embedding libgraveldb in a Tokio async runtime
//!   2. Write-through LRU cache (moka) for hot embedding acceleration
//!   3. Cache hit = nanoseconds, miss = microseconds + auto-populate
//!   4. Axum HTTP /stats endpoint shows cache hit rate in real-time
//!
//! Architecture:
//!
//!   [HTTP request]
//!        │
//!        ▼
//!   ┌────────────────┐  hit (ns, lock-free)
//!   │  moka LRU      │ ──────────────────→ respond
//!   └───────┬────────┘
//!           │ miss
//!           ▼
//!   ┌────────────────┐  (μs, direct FFI)
//!   │  Engine.get()  │ ─→ cache.insert → respond
//!   └────────────────┘
//!
//!   [PUT request]
//!        │
//!        ▼
//!   ┌────────────────┐  persist first (channel → writer thread)
//!   │  Engine.put()  │ ─→ success → cache.insert → ack
//!   └────────────────┘

mod cached_engine;
mod engine;
mod ffi;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::signal;

use cached_engine::CachedEngine;

const DIM: i32 = 128;
const NUM_FEATURES: u64 = 10_000;
const CACHE_CAPACITY: u64 = 5_000; // cache top 50% — demonstrates partial coverage

struct AppState {
    cached: CachedEngine,
}

#[derive(Deserialize)]
struct GetParams {
    id: u64,
}

#[derive(Serialize)]
struct GetResponse {
    id: u64,
    dim: usize,
    preview: Vec<f32>,
}

async fn handle_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<GetParams>,
) -> std::result::Result<Json<GetResponse>, StatusCode> {
    // Goes through cache: hit = ns (moka lock-free), miss = μs (FFI) + populate
    let results = state
        .cached
        .get(&[params.id])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let embedding = results
        .into_iter()
        .next()
        .flatten()
        .ok_or(StatusCode::NOT_FOUND)?;

    let preview: Vec<f32> = embedding.iter().take(4).copied().collect();
    Ok(Json(GetResponse {
        id: params.id,
        dim: embedding.len(),
        preview,
    }))
}

async fn handle_stats(
    State(state): State<Arc<AppState>>,
) -> Json<cached_engine::CacheStats> {
    Json(state.cached.stats())
}

#[tokio::main]
async fn main() {
    println!("=== GravelDB Rust Tokio Example (Write-Through Cache) ===\n");

    // Open engine (spawns dedicated writer thread internally)
    let engine = engine::Engine::open(engine::EngineConfig {
        data_dir: "/tmp/graveldb_rust_tokio".into(),
        dims: vec![DIM],
        buffer_size: 128 * 1024 * 1024,
        index_capacity: 1 << 18,
        auto_create_bins: true,
    })
    .await
    .expect("failed to open engine");

    // Wrap with write-through cache
    let cached = CachedEngine::new(engine, CACHE_CAPACITY);

    // Seed data (writes go through cache — auto-populated)
    println!("seeding {} features (dim={})...", NUM_FEATURES, DIM);
    let batch_size = 500u64;
    let mut start = 0u64;
    while start < NUM_FEATURES {
        let end = (start + batch_size).min(NUM_FEATURES);
        let n = (end - start) as usize;

        let ids: Vec<u64> = (start + 1..=end).collect();
        let dims = vec![DIM; n];
        let embeddings: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                (0..DIM as usize)
                    .map(|d| ((start as usize + i) * DIM as usize + d) as f32 * 0.0001)
                    .collect()
            })
            .collect();

        cached.put(ids, dims, embeddings).await.expect("put failed");
        start = end;
    }
    println!("seed complete");

    // Checkpoint
    cached.checkpoint().await.expect("initial checkpoint failed");
    println!("initial checkpoint done");

    // Warmup: pre-load hot IDs (simulating real workload's hot set)
    println!("warming up cache with top {} IDs...", CACHE_CAPACITY);
    let hot_ids: Vec<u64> = (1..=CACHE_CAPACITY).collect();
    cached.warmup(&hot_ids).await.expect("warmup failed");
    println!("warmup complete\n");

    let state = Arc::new(AppState {
        cached: cached.clone(),
    });

    // Background checkpoint every 60s
    let bg = cached.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let t = tokio::time::Instant::now();
            match bg.checkpoint().await {
                Ok(()) => println!("[bg] checkpoint done in {:?}", t.elapsed()),
                Err(e) => eprintln!("[bg] checkpoint error: {}", e),
            }
        }
    });

    // Build HTTP router
    let app = Router::new()
        .route("/get", get(handle_get))
        .route("/stats", get(handle_stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("failed to bind");

    println!("listening on :8080");
    println!("  GET /get?id=123  — retrieve embedding (cache-accelerated)");
    println!("  GET /stats       — cache hit rate + counters\n");

    axum::serve(listener, app)
        .with_graceful_shutdown(async { signal::ctrl_c().await.unwrap() })
        .await
        .unwrap();

    println!("\nshutting down...");
    cached.shutdown().await.unwrap();
    println!("done.");
}
