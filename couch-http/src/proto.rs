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

/// Build a registry from a `_schemas` snapshot. Ok(None) means nothing is
/// registered. Every structural problem — an unreadable or oversized
/// attachment, bytes that aren't a FileDescriptorSet, unresolvable
/// dependencies — is an error that fails the query which needed the
/// registry: a registry silently built from part of its inputs would mask
/// exactly the misconfiguration the operator needs to see. Only genuine
/// advisories (doctype naming collisions, an override naming a message that
/// isn't registered *yet*) are logged and tolerated.
pub fn build_registry(snap: &Db) -> couch_store::error::Result<Option<Arc<Registry>>> {
    let mut sets: Vec<Vec<u8>> = Vec::new();
    let mut overrides: HashMap<String, String> = HashMap::new();
    snap.fold_docs(|fdi| {
        if fdi.deleted || fdi.id.starts_with(b"_design/") {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(w) = fdi.rev_tree.winner() else {
            return Ok(ControlFlow::Continue(()));
        };
        let id = String::from_utf8_lossy(&fdi.id).into_owned();
        let doc = snap.doc_json(&fdi, &w, &Default::default())?;
        if let Some(dt) = doc.get("doctypes") {
            let map = dt.as_object().ok_or_else(|| {
                couch_store::error::Error::Unsupported(format!(
                    "_schemas/{id}: \"doctypes\" must be an object of doctype -> message name"
                ))
            })?;
            for (doctype, full) in map {
                let full = full.as_str().ok_or_else(|| {
                    couch_store::error::Error::Unsupported(format!(
                        "_schemas/{id}: doctypes[{doctype:?}] must be a string message name"
                    ))
                })?;
                overrides.insert(doctype.clone(), full.to_string());
            }
        }
        if let Some(atts) = doc.get("_attachments").and_then(|a| a.as_object()) {
            for name in atts.keys() {
                let att = snap.att_info(&fdi.id, name)?.ok_or_else(|| {
                    couch_store::error::Error::Corrupt(format!(
                        "_schemas/{id}: attachment {name} in stubs but not in summary"
                    ))
                })?;
                if att.att_len > MAX_DESCRIPTOR_BYTES {
                    return Err(couch_store::error::Error::Unsupported(format!(
                        "_schemas/{id}: attachment {name} is {} bytes, over the \
                         {MAX_DESCRIPTOR_BYTES}-byte descriptor limit",
                        att.att_len
                    )));
                }
                let bytes = snap.att_bytes(&fdi.id, name, MAX_DESCRIPTOR_BYTES)?.ok_or_else(
                    || {
                        couch_store::error::Error::Corrupt(format!(
                            "_schemas/{id}: attachment {name} vanished from snapshot"
                        ))
                    },
                )?;
                sets.push(bytes);
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;
    if sets.is_empty() {
        return Ok(None);
    }
    let (reg, advisories) = Registry::build(&sets, &overrides)
        .map_err(|e| couch_store::error::Error::Unsupported(format!("_schemas: {e}")))?;
    for a in &advisories {
        tracing::warn!("_schemas: {a}");
    }
    if reg.is_empty() {
        return Ok(None);
    }
    tracing::info!("proto registry loaded: {} doctypes", reg.len());
    Ok(Some(Arc::new(reg)))
}

/// Write-time gate for `PUT /_schemas/<doc>/<att>`: reject bytes that are
/// not a FileDescriptorSet before they are stored, instead of discovering
/// them when a query later needs the registry.
pub fn validate_schemas_attachment(spool: &crate::spool::Spool) -> crate::error::ApiResult<()> {
    use std::io::{Read, Seek, SeekFrom};
    if spool.len > MAX_DESCRIPTOR_BYTES {
        return Err(crate::error::ApiError::bad_request(format!(
            "descriptor set is {} bytes, over the {MAX_DESCRIPTOR_BYTES}-byte limit",
            spool.len
        )));
    }
    let mut f = &spool.file;
    f.seek(SeekFrom::Start(0))
        .map_err(|e| crate::error::ApiError::bad_request(format!("spool seek: {e}")))?;
    let mut buf = Vec::with_capacity(spool.len as usize);
    f.read_to_end(&mut buf)
        .map_err(|e| crate::error::ApiError::bad_request(format!("spool read: {e}")))?;
    f.seek(SeekFrom::Start(0))
        .map_err(|e| crate::error::ApiError::bad_request(format!("spool seek: {e}")))?;
    couch_proto::validate_descriptor_set(&buf).map_err(crate::error::ApiError::bad_request)
}

/// Write-time gate for interactive `PUT /_schemas/<doc>`: the `doctypes`
/// mapping must be an object of strings, and inline base64 attachments must
/// be FileDescriptorSets. (Replicated writes skip this — the source already
/// validated, and a strict registry build backstops everything else.)
pub fn validate_schemas_doc(doc: &Value) -> crate::error::ApiResult<()> {
    use base64::Engine as _;
    if let Some(dt) = doc.get("doctypes") {
        let map = dt.as_object().ok_or_else(|| {
            crate::error::ApiError::bad_request(
                "\"doctypes\" must be an object mapping doctype to full message name",
            )
        })?;
        for (doctype, full) in map {
            if !full.is_string() {
                return Err(crate::error::ApiError::bad_request(format!(
                    "doctypes[{doctype:?}] must be a string message name"
                )));
            }
        }
    }
    if let Some(atts) = doc.get("_attachments").and_then(|a| a.as_object()) {
        for (name, spec) in atts {
            if let Some(data) = spec.get("data").and_then(|d| d.as_str()) {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|e| {
                        crate::error::ApiError::bad_request(format!("attachment {name}: bad base64: {e}"))
                    })?;
                couch_proto::validate_descriptor_set(&bytes).map_err(|e| {
                    crate::error::ApiError::bad_request(format!("attachment {name}: {e}"))
                })?;
            }
        }
    }
    Ok(())
}

/// The Mango augmenter (see `couch_index::find::Augmenter`): resolves a
/// doc's protobuf blob attachment through the registry and returns the
/// decoded-and-overlaid view.
///
/// `Ok(None)` is reserved for docs that are simply not decodable blob
/// documents by contract: no proto attachment, or a doctype with no
/// registered schema. Everything failure-shaped — corrupt wire data, an
/// attachment the snapshot's metadata promises but can't serve, a
/// registered blob too large to decode — is an error and fails the
/// operation. No fallbacks: a problem surfaces where it happens.
///
/// Two deterministic routes exist, decided from metadata BEFORE any data is
/// touched (dispatch, not failure recovery): callers that name the paths
/// they need get selective wire extraction (skipping unwanted fields and,
/// with chunked storage, whole disk chunks) when the paths are navigable
/// and the attachment is identity-encoded; otherwise the message is decoded
/// in full. Both routes produce identical values for the same paths.
pub fn augmenter(
    reg: Arc<Registry>,
) -> impl Fn(&Db, &Value, Option<&[String]>) -> couch_store::error::Result<Option<Value>> {
    move |db, doc, wanted| {
        // Proto-native body: the envelope carries the bytes; the view is
        // decoded (or path-extracted) directly from them. Unlike legacy
        // blob attachments, an unrenderable proto body is an error — in a
        // proto-native db every application doc must decode.
        if is_envelope(doc) {
            let (type_name, bytes) = envelope_parts(doc)?;
            let id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or("?");
            let desc = reg.resolve_full(&type_name).ok_or_else(|| {
                couch_store::error::Error::Unsupported(format!(
                    "{id}: no schema registered for message {type_name:?}"
                ))
            })?;
            if let Some(paths) = wanted {
                if let Some(trie) = couch_proto::PathTrie::compile(&desc, paths) {
                    let partial = trie
                        .extract(&mut couch_proto::SliceReader(&bytes), bytes.len() as u64)
                        .map_err(|e| {
                            corrupt(format!("{id}: extracting {paths:?} from {type_name}: {e}"))
                        })?;
                    return Ok(Some(meta_over(partial, doc)));
                }
            }
            let decoded = reg
                .decode_message(&type_name, &bytes)
                .map_err(|e| corrupt(format!("{id}: {e}")))?;
            return Ok(Some(meta_over(decoded, doc)));
        }

        let Some((att_name, doctype, _len)) = couch_proto::blob_candidate(doc) else {
            return Ok(None);
        };
        if reg.resolve(doctype).is_none() {
            return Ok(None); // unregistered types stay opaque by contract
        }
        let id = doc
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| corrupt("blob doc without _id"))?
            .to_string();
        let att = db
            .att_info(id.as_bytes(), att_name)?
            .ok_or_else(|| corrupt(format!("{id}: attachment {att_name} in stubs but not in summary")))?;

        // Route 1: selective extraction — paths known, navigable, and the
        // stored stream is skippable (identity encoding).
        if let Some(paths) = wanted {
            let desc = reg.resolve(doctype).unwrap();
            if att.encoding == "identity" {
                if let Some(trie) = couch_proto::PathTrie::compile(desc, paths) {
                    let mut reader = ChunkSkipRead(couch_store::doc::AttReader::new(&db.file, &att));
                    let partial = trie.extract(&mut reader, att.att_len).map_err(|e| {
                        corrupt(format!("{id}: extracting {paths:?} from {doctype}: {e}"))
                    })?;
                    return Ok(Some(couch_proto::overlay(partial, doc)));
                }
            }
        }

        // Route 2: full decode (arbitrary selectors, array-index paths,
        // gzip-stored attachments). Decoding materializes the message in
        // memory, hence the size bound — exceeding it is an error, not a
        // silently unqueryable doc.
        if att.att_len > MAX_DECODE_BYTES {
            return Err(couch_store::error::Error::Unsupported(format!(
                "{id}: {doctype} blob is {} bytes, over the {MAX_DECODE_BYTES}-byte full-decode limit",
                att.att_len
            )));
        }
        let bytes = db
            .att_bytes(id.as_bytes(), att_name, MAX_DECODE_BYTES)?
            .ok_or_else(|| corrupt(format!("{id}: attachment {att_name} vanished from snapshot")))?;
        let decoded = reg
            .decode_doc(doctype, &bytes)
            .map_err(|e| corrupt(format!("{id}: {e}")))?;
        Ok(Some(couch_proto::overlay(decoded, doc)))
    }
}

fn corrupt(msg: impl Into<String>) -> couch_store::error::Error {
    couch_store::error::Error::Corrupt(msg.into())
}

// ---- proto-native document bodies ------------------------------------

/// Envelope keys couch-store emits for proto bodies (`doc::body_json`).
pub const PB_TYPE: &str = "$pb_type";
pub const PB_BODY: &str = "$pb_body";

pub fn is_envelope(doc: &Value) -> bool {
    doc.get(PB_TYPE).is_some()
}

/// Application docs are proto in a proto-native db; design/local docs are
/// server bookkeeping and stay JSON.
pub fn is_app_doc_id(id: &str) -> bool {
    !id.starts_with("_design/") && !id.starts_with("_local/")
}

pub fn envelope_parts(doc: &Value) -> Result<(String, Vec<u8>), couch_store::error::Error> {
    use base64::Engine as _;
    let id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or("?");
    let t = doc
        .get(PB_TYPE)
        .and_then(|v| v.as_str())
        .ok_or_else(|| corrupt(format!("{id}: $pb_type is not a string")))?;
    let b64 = doc
        .get(PB_BODY)
        .and_then(|v| v.as_str())
        .ok_or_else(|| corrupt(format!("{id}: proto envelope without $pb_body")))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| corrupt(format!("{id}: bad $pb_body base64: {e}")))?;
    Ok((t.to_string(), bytes))
}

/// Render a proto envelope as its domain view: the decoded message with the
/// doc's `_*` metadata carried over and `$pb_*` stripped. A proto document
/// that cannot be rendered (no registry, unknown type, undecodable bytes)
/// is an error — in a proto-native world that is corruption, not a doc that
/// quietly loses its fields.
pub fn render_view(
    reg: Option<&Registry>,
    doc: &Value,
) -> Result<Value, couch_store::error::Error> {
    let (type_name, bytes) = envelope_parts(doc)?;
    let id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or("?");
    let reg = reg.ok_or_else(|| {
        couch_store::error::Error::Unsupported(format!(
            "{id}: proto document but no schemas are registered in _schemas"
        ))
    })?;
    let decoded = reg
        .decode_message(&type_name, &bytes)
        .map_err(|e| corrupt(format!("{id}: {e}")))?;
    if !decoded.is_object() {
        return Err(corrupt(format!("{id}: decoded {type_name} is not an object")));
    }
    Ok(meta_over(decoded, doc))
}

/// Render when the doc is a proto envelope; pass JSON docs through.
pub fn render_if_envelope(
    reg: Option<&Registry>,
    doc: Value,
) -> Result<Value, couch_store::error::Error> {
    if is_envelope(&doc) {
        render_view(reg, &doc)
    } else {
        Ok(doc)
    }
}

/// Query-result body negotiation. A proto-aware client sends
/// `X-Proto-Bodies: true` to receive documents as the stored `$pb`
/// envelope (raw message bytes, base64 in the JSON result) and decode them
/// itself — no server-side proto→JSON re-render, no client-side protojson.
/// Absent (curl, Fauxton, stock-style consumers), results render to the
/// readable JSON view. Single-doc GET negotiates the same choice through
/// `Accept: application/protobuf`.
pub fn wants_proto_bodies(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("x-proto-bodies")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

/// A query-result doc for the given body-format choice.
///
/// Without proto-bodies: envelopes render to the readable JSON view.
///
/// With proto-bodies: the stored envelope is returned verbatim (raw message
/// bytes in `$pb_body`) so the client decodes it directly — but the small
/// `db` sub-message is extracted from the wire bytes (O(db), skipping the
/// heavy payload) and attached, so the client's map-based resolution, ACL
/// and dedup logic keeps working without decoding the whole body. Non-proto
/// docs pass through unchanged.
pub fn present_doc(
    reg: Option<&Registry>,
    doc: Value,
    proto_bodies: bool,
) -> Result<Value, couch_store::error::Error> {
    if !proto_bodies {
        return render_if_envelope(reg, doc);
    }
    if !is_envelope(&doc) {
        return Ok(doc);
    }
    let (type_name, bytes) = envelope_parts(&doc)?;
    let Some(reg) = reg else {
        return Err(couch_store::error::Error::Unsupported(format!(
            "proto document {type_name} but no schemas registered in _schemas"
        )));
    };
    let desc = reg.resolve_full(&type_name).ok_or_else(|| {
        couch_store::error::Error::Unsupported(format!(
            "no schema registered for message {type_name:?}"
        ))
    })?;
    let Value::Object(mut obj) = doc else {
        return Err(corrupt("envelope is not an object"));
    };
    // Attach the extracted `db` sub-message (cheap: skips the payload).
    if let Some(trie) = couch_proto::PathTrie::compile(&desc, &["db".to_string()]) {
        let extracted = trie
            .extract(&mut couch_proto::SliceReader(&bytes), bytes.len() as u64)
            .map_err(|e| corrupt(format!("extract db from {type_name}: {e}")))?;
        if let Value::Object(m) = extracted {
            if let Some(dbval) = m.get("db") {
                obj.insert("db".to_string(), dbval.clone());
            }
        }
    }
    Ok(Value::Object(obj))
}

/// Decoded (or extracted) message fields plus the doc's `_*` metadata —
/// `$pb_*` deliberately not carried over.
fn meta_over(decoded: Value, doc: &Value) -> Value {
    let Value::Object(mut view) = decoded else {
        return decoded;
    };
    if let Some(obj) = doc.as_object() {
        for (k, v) in obj {
            if k.starts_with('_') {
                view.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(view)
}

/// Wrap the db's validator so it sees decoded views instead of envelopes:
/// old proto parents are rendered before the inner hook runs. Rendering
/// failures reject the write with the real reason.
pub fn wrap_validator<'a>(
    reg: Option<Arc<Registry>>,
    inner: couch_store::writer::Validator<'a>,
) -> impl Fn(&Value, Option<&Value>) -> Result<(), String> + 'a {
    move |new: &Value, old: Option<&Value>| {
        let decoded_old;
        let old = match old {
            Some(o) if is_envelope(o) => {
                decoded_old = render_view(reg.as_deref(), o).map_err(|e| e.to_string())?;
                Some(&decoded_old)
            }
            other => other,
        };
        inner(new, old)
    }
}

/// Adapts couch-store's chunk-aware attachment reader to the extractor's
/// reader contract (skips over known-length chunks never touch the disk).
struct ChunkSkipRead<'a>(couch_store::doc::AttReader<'a>);

impl couch_proto::SkipRead for ChunkSkipRead<'_> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), String> {
        self.0.read_exact(buf).map_err(|e| e.to_string())
    }

    fn skip(&mut self, n: u64) -> Result<(), String> {
        self.0.skip(n).map_err(|e| e.to_string())
    }
}
