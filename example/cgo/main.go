package main

/*
 * GravelDB CGo Example — Write-Through Cache + Async Pattern
 *
 * Demonstrates:
 *   1. Opening GravelDB via CGo with a dedicated write thread
 *   2. Write-through LRU cache for hot embedding acceleration
 *   3. Concurrent HTTP handlers: cache hit = ns, miss = μs
 *   4. Cache warmup on startup, /stats shows hit rate
 *
 * Architecture:
 *
 *   [HTTP request]
 *        │
 *        ▼
 *   ┌─────────────┐  hit (ns)
 *   │  LRU Cache  │ ─────────→ respond
 *   └──────┬──────┘
 *          │ miss
 *          ▼
 *   ┌─────────────┐  (μs, direct FFI)
 *   │  Engine.Get │ ─→ populate cache → respond
 *   └─────────────┘
 *
 *   [PUT request]
 *        │
 *        ▼
 *   ┌─────────────┐  persist first
 *   │ Engine.Put  │ ─→ success → cache.Add → ack
 *   └─────────────┘
 *
 * Build:
 *   cd example/cgo
 *   CGO_ENABLED=1 go build -o graveldb-go .
 */

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	dim         = 128
	numFeatures = 10000
	cacheSize   = 5000 // cache top 50% — demonstrates partial coverage
)

func main() {
	log.Println("starting graveldb-go example (write-through cache)")

	// Open engine (dedicated cgo write thread starts here)
	engine, err := Open(&Config{
		DataDir:        "/tmp/graveldb_cgo_example",
		Dims:           []int{dim},
		BufferSize:     128 * 1024 * 1024,
		IndexCapacity:  1 << 18,
		AutoCreateBins: true,
	})
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}

	// Wrap engine with write-through LRU cache
	cached, err := WrapWithCache(engine, cacheSize)
	if err != nil {
		log.Fatalf("cache init failed: %v", err)
	}
	defer cached.Close()

	// Seed data (writes go through cache — cache gets populated automatically)
	log.Printf("seeding %d features (dim=%d)...", numFeatures, dim)
	ctx := context.Background()
	batchSize := 500
	for start := 0; start < numFeatures; start += batchSize {
		end := start + batchSize
		if end > numFeatures {
			end = numFeatures
		}
		n := end - start

		ids := make([]uint64, n)
		dims := make([]int, n)
		embs := make([][]float32, n)
		for i := 0; i < n; i++ {
			ids[i] = uint64(start + i + 1)
			dims[i] = dim
			embs[i] = make([]float32, dim)
			for d := 0; d < dim; d++ {
				embs[i][d] = rand.Float32()
			}
		}

		if err := cached.Put(ctx, ids, dims, embs); err != nil {
			log.Fatalf("put failed: %v", err)
		}
	}
	log.Println("seed complete")

	// Warmup: pre-load the most popular IDs (simulating hot set)
	log.Printf("warming up cache with top %d IDs...", cacheSize)
	hotIDs := make([]uint64, cacheSize)
	for i := range hotIDs {
		hotIDs[i] = uint64(i + 1)
	}
	if err := cached.Warmup(ctx, hotIDs); err != nil {
		log.Printf("warmup warning: %v", err)
	}
	log.Println("warmup complete")

	// Background checkpoint every 30s
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			start := time.Now()
			if err := cached.Checkpoint(ctx); err != nil {
				log.Printf("checkpoint error: %v", err)
			} else {
				log.Printf("checkpoint done in %v", time.Since(start))
			}
		}
	}()

	// HTTP handler: GET /get?id=123
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		idStr := r.URL.Query().Get("id")
		var id uint64
		fmt.Sscanf(idStr, "%d", &id)
		if id == 0 {
			http.Error(w, "missing id param", 400)
			return
		}

		// Goes through cache layer: hit = ns, miss = μs + cache populate
		embs, err := cached.Get(r.Context(), []uint64{id})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if embs[0] == nil {
			http.Error(w, "not found", 404)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":  id,
			"dim": len(embs[0]),
			"vec": embs[0][:4],
		})
	})

	// HTTP handler: GET /stats — includes cache hit rate
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cached.Stats())
	})

	// Graceful shutdown
	srv := &http.Server{Addr: ":8080"}
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		srv.Shutdown(context.Background())
	}()

	log.Println("listening on :8080")
	log.Println("  GET /get?id=123  — retrieve embedding (cache-accelerated)")
	log.Println("  GET /stats       — cache hit rate + counters")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
