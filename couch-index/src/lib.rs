//! couch-index as a library: Mango JSON indexes, planner and _find
//! execution, embeddable in other binaries (couch-http's /db/_find).

pub mod find;
pub mod index;
pub mod keys;
pub mod planner;
