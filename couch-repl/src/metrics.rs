//! Process-wide replicator counters, surfaced by couch-http's
//! /_node/{node}/_prometheus endpoint under the same names CouchDB's
//! couch_prometheus uses (couchdb_couch_replicator_*).
//!
//! Globals rather than per-job `Stats` because Prometheus counters must be
//! monotonic for the life of the process, while a job's Stats die with it.

use std::sync::atomic::{AtomicU64, Ordering};

/// HTTP requests made by the replicator.
pub static REQUESTS: AtomicU64 = AtomicU64::new(0);
/// Successful (2xx) responses received.
pub static RESPONSES: AtomicU64 = AtomicU64::new(0);
/// Network errors and non-2xx responses.
pub static RESPONSE_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Checkpoints recorded.
pub static CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
/// Checkpoint writes that failed.
pub static CHECKPOINT_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Changes feed reads that failed (including continuous-feed reconnects).
pub static CHANGES_READ_FAILURES: AtomicU64 = AtomicU64::new(0);

pub fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn get(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
