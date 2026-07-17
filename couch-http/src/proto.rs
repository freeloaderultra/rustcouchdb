//! Proto-blob awareness: the `_schemas` database and the Mango augmenter.
//!
//! `_schemas` is an ordinary database (it replicates like any other, so a
//! fleet can ship descriptors through the same sync pipeline as data). Every
//! attachment of every non-design doc in it is treated as an encoded
//! `FileDescriptorSet`; a doc body may carry a `"doctypes"` object mapping
//! `db.DocType` strings to fully-qualified message names for types that
//! don't follow the snake_case-of-short-name convention.
//!
//! The registry rebuilds lazily whenever `_schemas` changes (its snapshot
//! update_seq is the cache key). With no `_schemas` db — or an empty one —
//! everything behaves exactly as before.

use couch_proto::Registry;
use couch_store::db::Db;
use serde_json::Value;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

pub const SCHEMAS_DB: &str = "_schemas";

/// Decoding happens in memory; blobs larger than this are left opaque.
pub const MAX_DECODE_BYTES: u64 = 64 * 1024 * 1024;

/// Descriptor sets are small; cap reads defensively.
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;

/// Build a registry from a `_schemas` snapshot. Returns None when nothing
/// usable is registered. Problems are logged, never fatal — a bad descriptor
/// doc must not take Mango down.
pub fn build_registry(snap: &Db) -> Option<Arc<Registry>> {
    let mut sets: Vec<Vec<u8>> = Vec::new();
    let mut overrides: HashMap<String, String> = HashMap::new();
    let walk = snap.fold_docs(|fdi| {
        if fdi.deleted || fdi.id.starts_with(b"_design/") {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(w) = fdi.rev_tree.winner() else {
            return Ok(ControlFlow::Continue(()));
        };
        let doc = snap.doc_json(&fdi, &w, &Default::default())?;
        if let Some(map) = doc.get("doctypes").and_then(|d| d.as_object()) {
            for (doctype, full) in map {
                if let Some(full) = full.as_str() {
                    overrides.insert(doctype.clone(), full.to_string());
                }
            }
        }
        if let Some(atts) = doc.get("_attachments").and_then(|a| a.as_object()) {
            for name in atts.keys() {
                match snap.att_bytes(&fdi.id, name, MAX_DESCRIPTOR_BYTES) {
                    Ok(Some(bytes)) => sets.push(bytes),
                    Ok(None) => tracing::warn!(
                        "_schemas/{}: attachment {name} missing or over {MAX_DESCRIPTOR_BYTES} bytes, skipped",
                        String::from_utf8_lossy(&fdi.id)
                    ),
                    Err(e) => tracing::warn!(
                        "_schemas/{}: cannot read attachment {name}: {e}",
                        String::from_utf8_lossy(&fdi.id)
                    ),
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    });
    if let Err(e) = walk {
        tracing::warn!("cannot scan _schemas: {e}");
        return None;
    }
    if sets.is_empty() {
        return None;
    }
    let (reg, problems) = Registry::build(&sets, &overrides);
    for p in &problems {
        tracing::warn!("_schemas: {p}");
    }
    if reg.is_empty() {
        return None;
    }
    tracing::info!("proto registry loaded: {} doctypes", reg.len());
    Some(Arc::new(reg))
}

/// The Mango augmenter (see `couch_index::find::Augmenter`): resolves a
/// doc's protobuf blob attachment through the registry and returns the
/// decoded-and-overlaid view. Any failure (unknown type, oversized, decode
/// error) leaves the doc opaque, exactly as if no schema were registered.
pub fn augmenter(reg: Arc<Registry>) -> impl Fn(&Db, &Value) -> Option<Value> {
    move |db, doc| {
        let (att_name, doctype, len) = couch_proto::blob_candidate(doc)?;
        if len > MAX_DECODE_BYTES {
            return None;
        }
        reg.resolve(doctype)?;
        let id = doc.get("_id")?.as_str()?;
        let bytes = match db.att_bytes(id.as_bytes(), att_name, MAX_DECODE_BYTES) {
            Ok(Some(b)) => b,
            Ok(None) => return None,
            Err(e) => {
                tracing::debug!("proto augment: attachment read for {id} failed: {e}");
                return None;
            }
        };
        match reg.decode_doc(doctype, &bytes) {
            Ok(decoded) => Some(couch_proto::overlay(decoded, doc)),
            Err(e) => {
                tracing::debug!("proto augment: {id}: {e}");
                None
            }
        }
    }
}
