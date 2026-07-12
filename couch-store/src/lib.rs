//! Native Rust storage engine for CouchDB `.couch` files.
//! See README.md for format coverage and verification.

/// Run `f`, growing the stack first if less than 64 KiB remains. Rev trees
/// nest one level per revision and delete/recreate churn produces trees tens
/// of thousands of levels deep, so every depth-recursive code path (ETF
/// codec, tree conversion/walks/merges, and the manual Clone/PartialEq/Drop
/// impls that replace the derived recursive glue) goes through this guard —
/// otherwise a deep production tree overflows tokio's 2 MiB worker stacks.
pub(crate) fn maybe_grow<T>(f: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(64 * 1024, 4 * 1024 * 1024, f)
}

pub mod btree;
pub mod compact;
pub mod compress;
pub mod db;
pub mod doc;
pub mod ejson;
pub mod error;
pub mod etf;
pub mod file;
pub mod header;
pub mod revtree;
pub mod writer;
