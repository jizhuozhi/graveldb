package graveldb

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
#cgo LDFLAGS: -L${SRCDIR}/../../build -lgraveldb_lib -lpthread
#include "graveldb.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type Status int

const (
	OK           Status = 0
	Again        Status = 1
	ErrIO        Status = -1
	ErrOOM       Status = -2
	ErrNotFound  Status = -3
	ErrCorrupt   Status = -4
	ErrFull      Status = -5
	ErrInvalid   Status = -6
	ErrBusy      Status = -7
)

func (s Status) Error() string {
	switch s {
	case OK:
		return "ok"
	case Again:
		return "again"
	case ErrIO:
		return "io error"
	case ErrOOM:
		return "out of memory"
	case ErrNotFound:
		return "not found"
	case ErrCorrupt:
		return "corrupt"
	case ErrFull:
		return "full"
	case ErrInvalid:
		return "invalid"
	case ErrBusy:
		return "busy"
	default:
		return fmt.Sprintf("unknown status %d", int(s))
	}
}

type Config struct {
	DataDir        string
	Dims           []int
	BufferSize     uint64
	IndexCapacity  uint32
	AutoCreateBins bool
}

// rawDB wraps the C pointer. Not exported — users interact through Engine.
type rawDB struct {
	ptr *C.GravelDB
}

func openRaw(cfg *Config) (*rawDB, error) {
	cdir := C.CString(cfg.DataDir)
	defer C.free(unsafe.Pointer(cdir))

	var ccfg C.GravelDBConfig
	ccfg.data_dir = cdir

	var cdims []C.int
	if len(cfg.Dims) > 0 {
		cdims = make([]C.int, len(cfg.Dims))
		for i, d := range cfg.Dims {
			cdims[i] = C.int(d)
		}
		ccfg.dims = &cdims[0]
		ccfg.num_dims = C.int(len(cfg.Dims))
	}

	ccfg.buffer_size = C.size_t(cfg.BufferSize)
	ccfg.index_capacity = C.uint32_t(cfg.IndexCapacity)
	ccfg.auto_create_bins = C.bool(cfg.AutoCreateBins)

	var db *C.GravelDB
	st := C.graveldb_open(&db, &ccfg)
	if st != C.GRAVELDB_OK {
		return nil, Status(st)
	}
	return &rawDB{ptr: db}, nil
}

func (r *rawDB) batchPut(featIDs []uint64, dims []int, embeddings [][]float32) error {
	n := len(featIDs)
	cids := (*C.uint64_t)(unsafe.Pointer(&featIDs[0]))

	cdims := make([]C.int, n)
	for i, d := range dims {
		cdims[i] = C.int(d)
	}

	cptrs := make([]*C.float, n)
	for i := range embeddings {
		cptrs[i] = (*C.float)(unsafe.Pointer(&embeddings[i][0]))
	}

	st := C.graveldb_batch_put(r.ptr, nil, cids, &cdims[0], &cptrs[0], C.int(n))
	if st != C.GRAVELDB_OK {
		return Status(st)
	}
	return nil
}

func (r *rawDB) batchGet(featIDs []uint64) ([][]float32, error) {
	n := len(featIDs)
	cids := (*C.uint64_t)(unsafe.Pointer(&featIDs[0]))

	outPtrs := make([]*C.float, n)
	outDims := make([]C.int, n)

	st := C.graveldb_batch_get(r.ptr, nil, cids, C.int(n), &outPtrs[0], &outDims[0])
	if st != C.GRAVELDB_OK {
		return nil, Status(st)
	}

	results := make([][]float32, n)
	for i := 0; i < n; i++ {
		if outPtrs[i] == nil {
			continue
		}
		dim := int(outDims[i])
		// Copy from C memory to Go slice
		src := unsafe.Slice((*float32)(unsafe.Pointer(outPtrs[i])), dim)
		results[i] = make([]float32, dim)
		copy(results[i], src)
	}
	return results, nil
}

func (r *rawDB) checkpoint() error {
	st := C.graveldb_checkpoint(r.ptr)
	if st != C.GRAVELDB_OK {
		return Status(st)
	}
	return nil
}

func (r *rawDB) flush() error {
	st := C.graveldb_flush(r.ptr)
	if st != C.GRAVELDB_OK {
		return Status(st)
	}
	return nil
}

func (r *rawDB) close() {
	if r.ptr != nil {
		C.graveldb_close(r.ptr)
		r.ptr = nil
	}
}
