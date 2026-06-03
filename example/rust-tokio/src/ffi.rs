//! Raw FFI bindings to libgraveldb.
//!
//! These are unsafe C function declarations. Users should interact
//! through the safe `Engine` wrapper in engine.rs.

#![allow(non_camel_case_types, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub const GRAVELDB_OK: i32 = 0;
pub const GRAVELDB_AGAIN: i32 = 1;
pub const GRAVELDB_ERR_IO: i32 = -1;
pub const GRAVELDB_ERR_OOM: i32 = -2;
pub const GRAVELDB_ERR_NOT_FOUND: i32 = -3;
pub const GRAVELDB_ERR_BUSY: i32 = -7;

#[repr(C)]
pub struct GravelDB {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct GravelDBCtx {
    pub opaque: *mut c_void,
    pub alloc: Option<unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void>,
    pub dealloc: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize)>,
}

#[repr(C)]
pub struct GravelDBConfig {
    pub data_dir: *const c_char,
    pub dims: *const c_int,
    pub num_dims: c_int,
    pub buffer_size: usize,
    pub index_capacity: u32,
    pub entry_align: u32,
    pub page_size: u32,
    pub auto_create_bins: bool,
    pub delta_chain_max: c_int,
    pub dirty_ratio_full: f32,
    pub overlay_budget: usize,
}

#[repr(C)]
pub struct GravelDBStats {
    pub total_features: u64,
    pub total_entries: u64,
    pub buffer_hits: u64,
    pub buffer_misses: u64,
    pub buffer_evictions: u64,
    pub flush_bytes: u64,
    pub checkpoint_generation: u64,
    pub dirty_ratio: f32,
    pub cache_hit_ratio: f32,
}

extern "C" {
    pub fn graveldb_open(db: *mut *mut GravelDB, config: *const GravelDBConfig) -> i32;
    pub fn graveldb_close(db: *mut GravelDB);

    pub fn graveldb_batch_put(
        db: *mut GravelDB,
        ctx: *mut GravelDBCtx,
        feat_ids: *const u64,
        dims: *const c_int,
        embeddings: *const *const f32,
        n: c_int,
    ) -> i32;

    pub fn graveldb_batch_get(
        db: *mut GravelDB,
        ctx: *mut GravelDBCtx,
        feat_ids: *const u64,
        n: c_int,
        out_embeddings: *mut *mut f32,
        out_dims: *mut c_int,
    ) -> i32;

    pub fn graveldb_checkpoint(db: *mut GravelDB) -> i32;
    pub fn graveldb_flush(db: *mut GravelDB) -> i32;
    pub fn graveldb_stats(db: *mut GravelDB, stats: *mut GravelDBStats) -> i32;
}
