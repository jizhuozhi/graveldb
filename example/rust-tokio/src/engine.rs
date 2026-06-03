//! Safe async wrapper around GravelDB FFI.
//!
//! Thread safety model:
//!   - READS (batch_get) are thread-safe — GravelDB uses overlay isolation
//!     so concurrent reads are safe even during checkpoint. Reads go directly
//!     from any tokio worker thread, no channel overhead.
//!   - WRITES (put/delete/flush/checkpoint) are single-writer — must be
//!     serialized. These go through a dedicated OS thread via channel.
//!
//! This split gives us:
//!   - Read latency = raw FFI call time (microseconds, no channel round-trip)
//!   - Write correctness = serial execution on dedicated thread
//!   - Checkpoint never blocks reads or tokio scheduler

use std::ffi::CString;
use std::os::raw::c_int;
use std::ptr;
use tokio::sync::{mpsc, oneshot};

use crate::ffi;

#[derive(Debug)]
pub enum Error {
    Status(i32),
    Closed,
    Channel,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Status(code) => write!(f, "graveldb error code: {}", code),
            Error::Closed => write!(f, "engine shut down"),
            Error::Channel => write!(f, "internal channel error"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

enum WriteCommand {
    Put {
        feat_ids: Vec<u64>,
        dims: Vec<i32>,
        embeddings: Vec<Vec<f32>>,
        reply: oneshot::Sender<Result<()>>,
    },
    Flush {
        reply: oneshot::Sender<Result<()>>,
    },
    Checkpoint {
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Configuration for opening a GravelDB engine.
pub struct EngineConfig {
    pub data_dir: String,
    pub dims: Vec<i32>,
    pub buffer_size: usize,
    pub index_capacity: u32,
    pub auto_create_bins: bool,
}

/// Thread-safe handle to the raw db pointer.
/// Safety: GravelDB reads are thread-safe (overlay isolation).
/// Writes are serialized externally by the WriteCommand channel.
struct DbHandle {
    ptr: *mut ffi::GravelDB,
}

// batch_get is safe to call from any thread concurrently
unsafe impl Send for DbHandle {}
unsafe impl Sync for DbHandle {}

/// Async handle to a GravelDB instance.
///
/// - Reads go directly (zero overhead, any tokio task can read concurrently)
/// - Writes are serialized through a dedicated OS thread
#[derive(Clone)]
pub struct Engine {
    db: std::sync::Arc<DbHandle>,
    write_tx: mpsc::UnboundedSender<WriteCommand>,
}

impl Engine {
    /// Open a GravelDB instance.
    ///
    /// Spawns a dedicated writer thread for put/flush/checkpoint.
    /// Reads are served directly from any thread.
    pub async fn open(cfg: EngineConfig) -> Result<Self> {
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        // Open DB on dedicated thread (also serves as the writer thread)
        std::thread::Builder::new()
            .name("graveldb-writer".into())
            .spawn(move || {
                writer_thread(cfg, write_rx, ready_tx);
            })
            .map_err(|_| Error::Channel)?;

        // Wait for open result + get the db pointer
        let db_ptr = ready_rx.await.map_err(|_| Error::Channel)??;

        Ok(Engine {
            db: std::sync::Arc::new(DbHandle { ptr: db_ptr }),
            write_tx,
        })
    }

    /// Retrieve embeddings by feature IDs.
    ///
    /// FAST PATH: calls FFI directly on the current tokio worker thread.
    /// No channel, no context switch. GravelDB reads are thread-safe.
    pub fn get(&self, feat_ids: &[u64]) -> Result<Vec<Option<Vec<f32>>>> {
        do_get(self.db.ptr, feat_ids)
    }

    /// Async get (same as get, but returns a future for API consistency).
    /// Since batch_get is pure memory lookup (microseconds), we don't
    /// spawn_blocking — the cost of scheduling outweighs the call itself.
    pub async fn get_async(&self, feat_ids: Vec<u64>) -> Result<Vec<Option<Vec<f32>>>> {
        // For very large batches that might take >1ms, use spawn_blocking:
        if feat_ids.len() > 10_000 {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || do_get(db.ptr, &feat_ids))
                .await
                .map_err(|_| Error::Channel)?
        } else {
            do_get(self.db.ptr, &feat_ids)
        }
    }

    /// Insert embeddings (routed to dedicated writer thread).
    pub async fn put(&self, feat_ids: Vec<u64>, dims: Vec<i32>, embeddings: Vec<Vec<f32>>) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.write_tx
            .send(WriteCommand::Put { feat_ids, dims, embeddings, reply: reply_tx })
            .map_err(|_| Error::Closed)?;
        reply_rx.await.map_err(|_| Error::Channel)?
    }

    /// Trigger checkpoint (routed to dedicated writer thread — slow, doesn't block reads).
    pub async fn checkpoint(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.write_tx
            .send(WriteCommand::Checkpoint { reply: reply_tx })
            .map_err(|_| Error::Closed)?;
        reply_rx.await.map_err(|_| Error::Channel)?
    }

    /// Flush write buffer (routed to dedicated writer thread).
    pub async fn flush(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.write_tx
            .send(WriteCommand::Flush { reply: reply_tx })
            .map_err(|_| Error::Closed)?;
        reply_rx.await.map_err(|_| Error::Channel)?
    }

    /// Gracefully shut down the engine.
    pub async fn shutdown(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.write_tx.send(WriteCommand::Shutdown { reply: reply_tx });
        let _ = reply_rx.await;
        Ok(())
    }
}

/// Dedicated writer thread: only handles put/flush/checkpoint.
fn writer_thread(
    cfg: EngineConfig,
    mut rx: mpsc::UnboundedReceiver<WriteCommand>,
    ready_tx: oneshot::Sender<Result<*mut ffi::GravelDB>>,
) {
    let c_dir = match CString::new(cfg.data_dir.clone()) {
        Ok(s) => s,
        Err(_) => {
            let _ = ready_tx.send(Err(Error::Status(ffi::GRAVELDB_ERR_IO)));
            return;
        }
    };

    let c_dims: Vec<c_int> = cfg.dims.iter().map(|&d| d as c_int).collect();

    let ffi_cfg = ffi::GravelDBConfig {
        data_dir: c_dir.as_ptr(),
        dims: if c_dims.is_empty() { ptr::null() } else { c_dims.as_ptr() },
        num_dims: c_dims.len() as c_int,
        buffer_size: cfg.buffer_size,
        index_capacity: cfg.index_capacity,
        entry_align: 0,
        page_size: 0,
        auto_create_bins: cfg.auto_create_bins,
        delta_chain_max: 0,
        dirty_ratio_full: 0.0,
        overlay_budget: 0,
    };

    let mut db: *mut ffi::GravelDB = ptr::null_mut();
    let st = unsafe { ffi::graveldb_open(&mut db, &ffi_cfg) };
    if st != ffi::GRAVELDB_OK {
        let _ = ready_tx.send(Err(Error::Status(st)));
        return;
    }

    // Send db pointer back — reads will use it directly from any thread
    let _ = ready_tx.send(Ok(db));

    // Writer command loop
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            WriteCommand::Put { feat_ids, dims, embeddings, reply } => {
                let result = do_put(db, &feat_ids, &dims, &embeddings);
                let _ = reply.send(result);
            }
            WriteCommand::Flush { reply } => {
                let st = unsafe { ffi::graveldb_flush(db) };
                let result = if st == ffi::GRAVELDB_OK { Ok(()) } else { Err(Error::Status(st)) };
                let _ = reply.send(result);
            }
            WriteCommand::Checkpoint { reply } => {
                let st = unsafe { ffi::graveldb_checkpoint(db) };
                let result = if st == ffi::GRAVELDB_OK { Ok(()) } else { Err(Error::Status(st)) };
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown { reply } => {
                let _ = reply.send(());
                break;
            }
        }
    }

    // Cleanup — at this point no more reads should be in flight
    // (caller is responsible for draining before shutdown)
    unsafe { ffi::graveldb_close(db) };
}

fn do_put(db: *mut ffi::GravelDB, feat_ids: &[u64], dims: &[i32], embeddings: &[Vec<f32>]) -> Result<()> {
    let n = feat_ids.len();
    let c_dims: Vec<c_int> = dims.iter().map(|&d| d as c_int).collect();
    let ptrs: Vec<*const f32> = embeddings.iter().map(|v| v.as_ptr()).collect();

    let st = unsafe {
        ffi::graveldb_batch_put(
            db,
            ptr::null_mut(),
            feat_ids.as_ptr(),
            c_dims.as_ptr(),
            ptrs.as_ptr(),
            n as c_int,
        )
    };

    if st == ffi::GRAVELDB_OK { Ok(()) } else { Err(Error::Status(st)) }
}

fn do_get(db: *mut ffi::GravelDB, feat_ids: &[u64]) -> Result<Vec<Option<Vec<f32>>>> {
    let n = feat_ids.len();
    let mut out_ptrs: Vec<*mut f32> = vec![ptr::null_mut(); n];
    let mut out_dims: Vec<c_int> = vec![0; n];

    let st = unsafe {
        ffi::graveldb_batch_get(
            db,
            ptr::null_mut(),
            feat_ids.as_ptr(),
            n as c_int,
            out_ptrs.as_mut_ptr(),
            out_dims.as_mut_ptr(),
        )
    };

    if st != ffi::GRAVELDB_OK {
        return Err(Error::Status(st));
    }

    let results: Vec<Option<Vec<f32>>> = (0..n)
        .map(|i| {
            if out_ptrs[i].is_null() {
                None
            } else {
                let dim = out_dims[i] as usize;
                let slice = unsafe { std::slice::from_raw_parts(out_ptrs[i], dim) };
                Some(slice.to_vec())
            }
        })
        .collect();

    Ok(results)
}
