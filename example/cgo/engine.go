package graveldb

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

/*
 * Engine wraps a GravelDB instance with split read/write paths.
 *
 * Thread safety model:
 *   - READS (batch_get) are thread-safe in GravelDB (overlay isolation).
 *     Go goroutines call cgo directly — no channel overhead. Since batch_get
 *     is pure memory lookup (microseconds), the brief OS thread pin from cgo
 *     is acceptable and won't exhaust Go's thread pool.
 *   - WRITES (put/delete/flush/checkpoint) are single-writer. They must be
 *     serialized onto a dedicated goroutine locked to one OS thread.
 *
 * This gives us:
 *   - Read latency = raw cgo call (no channel round-trip, no goroutine switch)
 *   - Write correctness = serial execution on dedicated thread
 *   - Checkpoint never blocks readers
 */

var ErrEngineClosed = errors.New("engine closed")

type cmdType int

const (
	cmdPut cmdType = iota
	cmdCheckpoint
	cmdFlush
	cmdClose
)

type command struct {
	typ        cmdType
	featIDs    []uint64
	dims       []int
	embeddings [][]float32
	reply      chan<- result
}

type result struct {
	err error
}

type Engine struct {
	db    *rawDB       // shared across goroutines for reads (thread-safe)
	cmdCh chan command // write commands → dedicated writer goroutine
	done  chan struct{}
	once  sync.Once
}

// Open creates a new Engine. Reads are served directly; writes go to a dedicated thread.
func Open(cfg *Config) (*Engine, error) {
	// Open DB on current goroutine (safe — no concurrent access yet)
	db, err := openRaw(cfg)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		db:    db,
		cmdCh: make(chan command, 256),
		done:  make(chan struct{}),
	}

	go e.writerLoop()
	return e, nil
}

// writerLoop runs on a dedicated OS thread, handling all write operations.
func (e *Engine) writerLoop() {
	// Lock to OS thread: GravelDB writes are not thread-safe,
	// and we want cache locality for the write path.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(e.done)

	for cmd := range e.cmdCh {
		switch cmd.typ {
		case cmdPut:
			err := e.db.batchPut(cmd.featIDs, cmd.dims, cmd.embeddings)
			cmd.reply <- result{err: err}

		case cmdCheckpoint:
			err := e.db.checkpoint()
			cmd.reply <- result{err: err}

		case cmdFlush:
			err := e.db.flush()
			cmd.reply <- result{err: err}

		case cmdClose:
			cmd.reply <- result{}
			return
		}
	}
}

// Get retrieves embeddings by feature IDs.
// FAST PATH: calls cgo directly on the calling goroutine.
// GravelDB reads are thread-safe (overlay isolation), and batch_get is
// pure memory lookup (microseconds), so the brief cgo thread pin is fine.
func (e *Engine) Get(ctx context.Context, featIDs []uint64) ([][]float32, error) {
	select {
	case <-e.done:
		return nil, ErrEngineClosed
	default:
	}
	return e.db.batchGet(featIDs)
}

// Put inserts embeddings. Routed to the dedicated writer thread.
func (e *Engine) Put(ctx context.Context, featIDs []uint64, dims []int, embeddings [][]float32) error {
	reply := make(chan result, 1)
	select {
	case e.cmdCh <- command{typ: cmdPut, featIDs: featIDs, dims: dims, embeddings: embeddings, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrEngineClosed
	}

	select {
	case r := <-reply:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Checkpoint persists data to disk. Routed to the dedicated writer thread.
func (e *Engine) Checkpoint(ctx context.Context) error {
	reply := make(chan result, 1)
	select {
	case e.cmdCh <- command{typ: cmdCheckpoint, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrEngineClosed
	}

	select {
	case r := <-reply:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush flushes write buffer to disk. Routed to the dedicated writer thread.
func (e *Engine) Flush(ctx context.Context) error {
	reply := make(chan result, 1)
	select {
	case e.cmdCh <- command{typ: cmdFlush, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrEngineClosed
	}

	select {
	case r := <-reply:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close shuts down the engine and releases all resources.
func (e *Engine) Close() error {
	var err error
	e.once.Do(func() {
		reply := make(chan result, 1)
		e.cmdCh <- command{typ: cmdClose, reply: reply}
		<-reply
		close(e.cmdCh)
		e.db.close()
	})
	return err
}
