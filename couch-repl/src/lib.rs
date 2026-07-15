//! couch-repl as a library: the replication pipeline, endpoints and job
//! supervisor, embeddable in other binaries (couch-http's _replicator).

pub mod attachments;
pub mod changes;
pub mod checkpoint;
pub mod cli;
pub mod client;
pub mod error;
pub mod fetch;
pub mod gen;
pub mod ids;
pub mod metrics;
pub mod pipeline;
pub mod retry;
pub mod revs_diff;
pub mod seq;
pub mod server;
pub mod stats;
pub mod util;
pub mod write;
