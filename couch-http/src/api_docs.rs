//! Document endpoints: GET/PUT/DELETE docs (interactive and replicated),
//! POST /db, attachments, _local docs.

use crate::error::{ApiError, ApiResult};
use crate::spool::{parse_multipart_stream, spool_body};
use crate::state::{blocking, App};
use crate::util::{parse_json, qbool, Q};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use couch_store::db::DocOpts;
use couch_store::writer::{DocUpdate, SaveOutcome, SpooledAtt};
use couch_store::{doc as docmod};
use serde_json::{json, Map, Value};

/// JSON request bodies keep the pre-streaming cap; attachment bodies and
/// multipart attachment parts are spooled to disk and unbounded (stock's
/// max_attachment_size defaults to infinity).
const JSON_BODY_LIMIT: usize = 1024 * 1024 * 1024;

fn etag(rev: &str) -> header::HeaderValue {
    header::HeaderValue::from_str(&format!("\"{rev}\"")).expect("valid etag")
}

fn doc_opts(q: &Q) -> DocOpts {
    DocOpts {
        revs: qbool(q, "revs", false),
        conflicts: qbool(q, "conflicts", false) || qbool(q, "meta", false),
        attachments: qbool(q, "attachments", false),
    }
}

// ---------------------------------------------------------------- GET doc

pub async fn doc_get(
    State(state): State<App>,
    Path((db, docid)): Path<(String, String)>,
    Query(q): Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    if let Some(local) = docid.strip_prefix("_local/") {
        return local_get_inner(state, db, local.to_string()).await;
    }
    let accept_mixed = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("multipart/mixed"))
        .unwrap_or(false);
    let dbh = state.db(&db)?;
    let docid2 = docid.clone();
    blocking(move || {
        let snap = dbh.snapshot();
        let opts = doc_opts(&q);

        // open_revs: array response of {ok}/{missing}, or multipart/mixed
        // for the Erlang replicator.
        if let Some(or) = q.get("open_revs") {
            let Some(fdi) = snap.open_doc_info(docid2.as_bytes())? else {
                return Err(ApiError::missing());
            };
            let leaves = fdi.rev_tree.leaves();
            let wanted: Vec<(u64, Vec<u8>)> = if or == "all" {
                leaves.iter().map(|l| (l.pos, l.path[0].to_vec())).collect()
            } else {
                let revs: Vec<String> = serde_json::from_str(or)
                    .map_err(|e| ApiError::bad_request(format!("invalid open_revs: {e}")))?;
                revs.iter()
                    .map(|r| docmod::parse_rev(r))
                    .collect::<couch_store::error::Result<_>>()?
            };
            let latest = qbool(&q, "latest", false);
            let find_leaf = |pos: u64, revid: &[u8]| {
                if latest {
                    fdi.rev_tree
                        .descendant_leaves(pos, revid)
                        .into_iter()
                        .max_by(|a, b| (a.pos, a.path[0]).cmp(&(b.pos, b.path[0])))
                } else {
                    fdi.rev_tree
                        .leaves()
                        .into_iter()
                        .find(|l| l.pos == pos && l.path[0] == revid)
                }
            };
            if accept_mixed {
                return open_revs_multipart(&snap, &fdi, &wanted, &q, &opts, find_leaf);
            }
            let mut out = Vec::new();
            for (pos, revid) in wanted {
                match find_leaf(pos, &revid) {
                    Some(leaf) => {
                        crate::metrics::bump(&crate::metrics::DATABASE_READS);
                        out.push(json!({"ok": snap.doc_json(&fdi, &leaf, &opts)?}))
                    }
                    None => out.push(json!({"missing": docmod::rev_str(pos, &revid)})),
                }
            }
            return Ok(Json(Value::Array(out)).into_response());
        }

        // Single doc.
        let rev = q.get("rev").map(|s| s.as_str());
        let doc = if qbool(&q, "latest", false) && rev.is_some() {
            let (pos, revid) = docmod::parse_rev(rev.unwrap())?;
            let Some(fdi) = snap.open_doc_info(docid2.as_bytes())? else {
                return Err(ApiError::missing());
            };
            fdi.rev_tree
                .descendant_leaves(pos, &revid)
                .into_iter()
                .max_by(|a, b| (a.pos, a.path[0]).cmp(&(b.pos, b.path[0])))
                .map(|leaf| snap.doc_json(&fdi, &leaf, &opts))
                .transpose()?
        } else {
            snap.open_doc(docid2.as_bytes(), rev, &opts)?
        };
        let Some(doc) = doc else {
            return Err(ApiError::missing());
        };
        crate::metrics::bump(&crate::metrics::DATABASE_READS);
        if rev.is_none() && doc.get("_deleted") == Some(&Value::Bool(true)) {
            return Err(ApiError::deleted());
        }
        let rev_str = doc["_rev"].as_str().unwrap_or("").to_string();
        let mut resp = Json(doc).into_response();
        resp.headers_mut().insert(header::ETAG, etag(&rev_str));
        Ok(resp)
    })
}

/// multipart/mixed open_revs response — what couch_replicator_api_wrap's
/// mp_parse_mixed expects; format ported from chttpd_db:send_docs_multipart
/// and couch_httpd_multipart:encode_multipart_stream.
fn open_revs_multipart<'a>(
    snap: &'a couch_store::db::Db,
    fdi: &'a couch_store::db::FullDocInfo,
    wanted: &[(u64, Vec<u8>)],
    q: &Q,
    opts: &DocOpts,
    find_leaf: impl Fn(u64, &[u8]) -> Option<couch_store::revtree::LeafPath<'a>>,
) -> ApiResult<Response> {
    // atts_since: attachments at or below the highest ancestor listed there
    // are sent as stubs (couch_db:apply_open_options). Absent → every
    // attachment is included.
    let atts_since: Option<Vec<(u64, Vec<u8>)>> = match q.get("atts_since") {
        None => None,
        Some(s) => Some(
            serde_json::from_str::<Vec<String>>(s)
                .map_err(|e| ApiError::bad_request(format!("invalid atts_since: {e}")))?
                .iter()
                .map(|r| docmod::parse_rev(r))
                .collect::<couch_store::error::Result<_>>()?,
        ),
    };

    let outer = crate::state::gen_uuid();
    let inner = crate::state::gen_uuid();
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(format!("--{outer}").as_bytes());

    for (pos, revid) in wanted {
        let Some(leaf) = find_leaf(*pos, revid) else {
            let json = json!({"missing": docmod::rev_str(*pos, revid)});
            body.extend_from_slice(
                b"\r\nContent-Type: application/json; error=\"true\"\r\n\r\n",
            );
            body.extend_from_slice(json.to_string().as_bytes());
            body.extend_from_slice(format!("\r\n--{outer}").as_bytes());
            continue;
        };
        // The ancestor cutoff for this leaf's path (None: include all).
        let cutoff = atts_since.as_ref().map(|since| {
            since
                .iter()
                .filter(|(apos, arev)| {
                    leaf.pos >= *apos
                        && ((leaf.pos - apos) as usize) < leaf.path.len()
                        && leaf.path[(leaf.pos - apos) as usize] == arev.as_slice()
                })
                .map(|(apos, _)| *apos)
                .max()
                .unwrap_or(0)
        });

        crate::metrics::bump(&crate::metrics::DATABASE_READS);
        let mut doc = snap.doc_json(fdi, &leaf, opts)?;
        // Attachment bytes for the parts, in _attachments order.
        let mut parts: Vec<(String, String, Vec<u8>)> = Vec::new();
        let couch_store::revtree::RevVal::Leaf(lv) = leaf.leaf else {
            unreachable!("find_leaf only returns stored leaves")
        };
        if let (Some(ptr), Some(Value::Object(atts))) =
            (lv.ptr, doc.get_mut("_attachments").map(|a| a.take()))
        {
            let summary = docmod::read_summary(&snap.file, ptr)?;
            let mut new_atts = Map::new();
            for (name, mut spec) in atts {
                let ai = summary.atts.iter().find(|a| a.name == name);
                let follows = ai
                    .map(|a| cutoff.map(|c| a.revpos > c).unwrap_or(true))
                    .unwrap_or(false);
                if let (true, Some(ai)) = (follows, ai) {
                    let obj = spec.as_object_mut().unwrap();
                    obj.remove("stub");
                    obj.insert("follows".into(), Value::Bool(true));
                    let data = docmod::read_att_data(&snap.file, ai)?;
                    parts.push((name.clone(), ai.content_type.clone(), data));
                }
                new_atts.insert(name, spec);
            }
            doc["_attachments"] = Value::Object(new_atts);
        }
        let json_bytes = doc.to_string();
        if parts.is_empty() {
            body.extend_from_slice(b"\r\nContent-Type: application/json\r\n\r\n");
            body.extend_from_slice(json_bytes.as_bytes());
        } else {
            body.extend_from_slice(
                format!("\r\nContent-Type: multipart/related; boundary=\"{inner}\"\r\n\r\n")
                    .as_bytes(),
            );
            body.extend_from_slice(
                format!("--{inner}\r\nContent-Type: application/json\r\n\r\n").as_bytes(),
            );
            body.extend_from_slice(json_bytes.as_bytes());
            body.extend_from_slice(format!("\r\n--{inner}").as_bytes());
            for (name, ct, data) in parts {
                body.extend_from_slice(
                    format!(
                        "\r\nContent-Disposition: attachment; filename=\"{name}\"\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\r\n",
                        data.len()
                    )
                    .as_bytes(),
                );
                body.extend_from_slice(&data);
                body.extend_from_slice(format!("\r\n--{inner}").as_bytes());
            }
            body.extend_from_slice(b"--");
        }
        body.extend_from_slice(format!("\r\n--{outer}").as_bytes());
    }
    body.extend_from_slice(b"--");

    let mut resp = Response::new(axum::body::Body::from(body));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_str(&format!("multipart/mixed; boundary=\"{outer}\""))
            .expect("valid header"),
    );
    Ok(resp)
}

// ---------------------------------------------------------------- PUT/POST

/// Shared write path for PUT doc / POST db.
fn write_doc(state: &App, db: &str, doc: Value, q: &Q, headers: &HeaderMap) -> ApiResult<Response> {
    write_doc_with_spools(state, db, doc, Vec::new(), q, headers)
}

fn write_doc_with_spools(
    state: &App,
    db: &str,
    mut doc: Value,
    spools: Vec<SpooledAtt>,
    q: &Q,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    let dbh = state.db(db)?;
    let obj = doc
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("Document must be a JSON object"))?;

    if let Some(rev) = q.get("rev") {
        match obj.get("_rev") {
            Some(Value::String(r)) if r != rev => {
                return Err(ApiError::bad_request(
                    "Document rev from request body and query string have different values",
                ));
            }
            _ => {
                obj.insert("_rev".into(), Value::String(rev.clone()));
            }
        }
    } else if let Some(im) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        if !obj.contains_key("_rev") {
            obj.insert("_rev".into(), Value::String(im.trim_matches('"').to_string()));
        }
    }

    let id = obj
        .get("_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("doc missing _id"))?
        .to_string();
    if let Some(local) = id.strip_prefix("_local/") {
        let local = local.to_string();
        let rev = blocking(|| dbh.with_writer(|w| w.update_local(format!("_local/{local}").as_bytes(), Some(&doc))))?;
        return Ok(created(&format!("_local/{local}"), &rev));
    }

    let new_edits = qbool(q, "new_edits", true);
    if new_edits {
        let validator = state.validator_for(db);
        let deleted = doc.get("_deleted") == Some(&Value::Bool(true));
        let outcome =
            blocking(|| dbh.with_writer(|w| w.save_doc_with_spools(&doc, validator, spools)))?;
        match outcome {
            SaveOutcome::Ok { rev } => {
                if db == "_replicator" {
                    state.repl.poke();
                }
                // CouchDB answers 200 for deletions, 201 for other writes.
                let status = if deleted { StatusCode::OK } else { StatusCode::CREATED };
                Ok(ok_status(status, &id, &rev))
            }
            SaveOutcome::Error { error, reason } => Err(ApiError::from_save(&error, &reason)),
        }
    } else {
        let upd = DocUpdate::from_json_with_spools(doc, spools)?;
        let rev = docmod::rev_str(upd.rev_path.0, &upd.rev_path.1[0]);
        blocking(|| dbh.with_writer(|w| w.update_docs(vec![upd])))?;
        if db == "_replicator" {
            state.repl.poke();
        }
        Ok(created(&id, &rev))
    }
}

fn created(id: &str, rev: &str) -> Response {
    ok_status(StatusCode::CREATED, id, rev)
}

fn ok_status(status: StatusCode, id: &str, rev: &str) -> Response {
    let mut resp = (status, Json(json!({"ok": true, "id": id, "rev": rev}))).into_response();
    resp.headers_mut().insert(header::ETAG, etag(rev));
    resp
}

pub async fn doc_put(
    State(state): State<App>,
    Path((db, docid)): Path<(String, String)>,
    Query(q): Query<Q>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<Response> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let (mut doc, spools) = if ct.starts_with("multipart/related") {
        // Streamed: attachment parts spool to disk, only the JSON part
        // (and a boundary-sized carry) ever lives in memory.
        let parsed = parse_multipart_stream(&ct, request.into_body(), &state.dir).await?;
        let spools = match_parts_to_atts(&parsed.doc, parsed.parts)?;
        (parsed.doc, spools)
    } else {
        let body = axum::body::to_bytes(request.into_body(), JSON_BODY_LIMIT)
            .await
            .map_err(|e| ApiError::bad_request(format!("body read failed: {e}")))?;
        (parse_json(&body)?, Vec::new())
    };
    if let Some(obj) = doc.as_object_mut() {
        obj.insert("_id".into(), Value::String(docid.clone()));
    }
    write_doc_with_spools(&state, &db, doc, spools, &q, &headers)
}

/// Zip multipart body parts with the doc's `follows` attachments in JSON
/// order — the couch_doc wire contract couch-repl and the Erlang replicator
/// both emit (serde_json preserves object order here).
fn match_parts_to_atts(
    doc: &Value,
    parts: Vec<crate::spool::Spool>,
) -> ApiResult<Vec<SpooledAtt>> {
    let follows: Vec<(String, String)> = doc
        .get("_attachments")
        .and_then(|a| a.as_object())
        .map(|atts| {
            atts.iter()
                .filter(|(_, spec)| spec.get("follows") == Some(&Value::Bool(true)))
                .map(|(name, spec)| {
                    (
                        name.clone(),
                        spec.get("content_type")
                            .and_then(|c| c.as_str())
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if follows.len() != parts.len() {
        return Err(ApiError::bad_request(format!(
            "multipart part count mismatch: {} follows attachments, {} body parts",
            follows.len(),
            parts.len()
        )));
    }
    Ok(follows
        .into_iter()
        .zip(parts)
        .map(|((name, ct), spool)| spool.into_att(name, ct))
        .collect())
}

pub async fn doc_post(
    State(state): State<App>,
    Path(db): Path<String>,
    Query(q): Query<Q>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let mut doc = parse_json(&body)?;
    if let Some(obj) = doc.as_object_mut() {
        if !obj.contains_key("_id") {
            obj.insert("_id".into(), Value::String(state.next_uuid()));
        }
    }
    write_doc(&state, &db, doc, &q, &headers)
}

pub async fn doc_delete(
    State(state): State<App>,
    Path((db, docid)): Path<(String, String)>,
    Query(q): Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    if let Some(local) = docid.strip_prefix("_local/") {
        return local_delete_inner(state, db, local.to_string()).await;
    }
    let rev = q
        .get("rev")
        .cloned()
        .or_else(|| {
            headers
                .get(header::IF_MATCH)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string())
        })
        .ok_or_else(ApiError::conflict)?;
    let doc = json!({"_id": docid, "_rev": rev, "_deleted": true});
    let dbh = state.db(&db)?;
    let validator = state.validator_for(&db);
    let outcome = blocking(|| dbh.with_writer(|w| w.save_doc(&doc, validator)))?;
    match outcome {
        SaveOutcome::Ok { rev } => {
            if db == "_replicator" {
                state.repl.poke();
            }
            let mut resp = (
                StatusCode::OK,
                Json(json!({"ok": true, "id": docid, "rev": rev})),
            )
                .into_response();
            resp.headers_mut().insert(header::ETAG, etag(&rev));
            Ok(resp)
        }
        SaveOutcome::Error { error, reason } => Err(ApiError::from_save(&error, &reason)),
    }
}

// Design-doc routes delegate to the same handlers with the full id.

pub async fn design_get(
    state: State<App>,
    Path((db, name)): Path<(String, String)>,
    q: Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    doc_get(state, Path((db, format!("_design/{name}"))), q, headers).await
}

pub async fn design_put(
    state: State<App>,
    Path((db, name)): Path<(String, String)>,
    q: Query<Q>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<Response> {
    doc_put(state, Path((db, format!("_design/{name}"))), q, headers, request).await
}

pub async fn design_delete(
    state: State<App>,
    Path((db, name)): Path<(String, String)>,
    q: Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    doc_delete(state, Path((db, format!("_design/{name}"))), q, headers).await
}

pub async fn design_att_get(
    state: State<App>,
    Path((db, name, att)): Path<(String, String, String)>,
    q: Query<Q>,
) -> ApiResult<Response> {
    att_get(state, Path((db, format!("_design/{name}"), att)), q).await
}

pub async fn design_att_put(
    state: State<App>,
    Path((db, name, att)): Path<(String, String, String)>,
    q: Query<Q>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<Response> {
    att_put(state, Path((db, format!("_design/{name}"), att)), q, headers, request).await
}

pub async fn design_att_delete(
    state: State<App>,
    Path((db, name, att)): Path<(String, String, String)>,
    q: Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    att_delete(state, Path((db, format!("_design/{name}"), att)), q, headers).await
}

// ---------------------------------------------------------------- attachments

pub async fn att_get(
    State(state): State<App>,
    Path((db, docid, att)): Path<(String, String, String)>,
    Query(q): Query<Q>,
) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let Some(fdi) = snap.open_doc_info(docid.as_bytes())? else {
            return Err(ApiError::missing());
        };
        let leaf = match q.get("rev") {
            Some(r) => {
                let (pos, revid) = docmod::parse_rev(r)?;
                fdi.rev_tree.rev_path(pos, &revid)
            }
            None => fdi.rev_tree.winner(),
        };
        let Some(leaf) = leaf else {
            return Err(ApiError::missing());
        };
        let couch_store::revtree::RevVal::Leaf(lv) = leaf.leaf else {
            return Err(ApiError::missing());
        };
        let Some(ptr) = lv.ptr else {
            return Err(ApiError::missing());
        };
        let rev_str = docmod::rev_str(leaf.pos, leaf.path[0]);
        let summary = docmod::read_summary(&snap.file, ptr)?;
        let Some(ai) = summary.atts.iter().find(|a| a.name == att) else {
            return Err(ApiError::not_found("Document is missing attachment"));
        };

        // Stream the attachment chunk-by-chunk: peak memory is one stored
        // chunk, not the attachment. The snapshot Arc keeps the file open
        // even if the db swaps snapshots or compacts mid-download.
        let ai = ai.clone();
        let gzip_encoded = ai.encoding == "gzip";
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
        let pump_snap = snap.clone();
        tokio::task::spawn_blocking(move || {
            let send = |data: Vec<u8>| tx.blocking_send(Ok(bytes::Bytes::from(data))).is_ok();
            let fail = |msg: String| {
                let _ = tx.blocking_send(Err(std::io::Error::other(msg)));
            };
            if gzip_encoded {
                // Stored gzip (stock-written files): decode as it streams.
                let mut dec = flate2::write::GzDecoder::new(ChannelWriter {
                    tx: tx.clone(),
                    failed: false,
                });
                use std::io::Write;
                for (pos, _len) in &ai.chunks {
                    match pump_snap.file.read_chunk(*pos) {
                        Ok(data) => {
                            if dec.write_all(&data).is_err() {
                                return fail("gunzip attachment failed".into());
                            }
                        }
                        Err(e) => return fail(format!("attachment read failed: {e}")),
                    }
                }
                let _ = dec.finish();
            } else {
                for (pos, _len) in &ai.chunks {
                    match pump_snap.file.read_chunk(*pos) {
                        Ok(data) => {
                            if !send(data) {
                                return; // client went away
                            }
                        }
                        Err(e) => return fail(format!("attachment read failed: {e}")),
                    }
                }
            }
        });

        let mut resp = Response::new(axum::body::Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ));
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_str(&ai.content_type)
                .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
        );
        if !gzip_encoded {
            // Identity: the stored length is the wire length.
            resp.headers_mut()
                .insert(header::CONTENT_LENGTH, header::HeaderValue::from(ai.att_len));
        }
        resp.headers_mut().insert(header::ETAG, etag(&rev_str));
        Ok(resp)
    })
}

/// io::Write adapter feeding decoded bytes into the response channel.
struct ChannelWriter {
    tx: tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    failed: bool,
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.failed
            || self
                .tx
                .blocking_send(Ok(bytes::Bytes::copy_from_slice(buf)))
                .is_err()
        {
            self.failed = true;
            return Err(std::io::Error::other("client went away"));
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Rebuild the doc with existing attachments as stubs, applying one change
/// (upsert or delete of a named attachment), then save interactively.
fn modify_atts(
    state: &App,
    db: &str,
    docid: &str,
    rev: Option<&str>,
    success: StatusCode,
    spools: Vec<SpooledAtt>,
    change: impl FnOnce(&mut Map<String, Value>) -> ApiResult<()>,
) -> ApiResult<Response> {
    let dbh = state.db(db)?;
    let snap = dbh.snapshot();
    let mut doc = match snap.open_doc(docid.as_bytes(), rev, &Default::default())? {
        Some(d) if d.get("_deleted") != Some(&Value::Bool(true)) => {
            crate::metrics::bump(&crate::metrics::DATABASE_READS);
            d
        }
        Some(_) | None => {
            if rev.is_some() {
                return Err(ApiError::conflict());
            }
            json!({"_id": docid})
        }
    };
    let obj = doc.as_object_mut().unwrap();
    if !obj.contains_key("_attachments") {
        obj.insert("_attachments".into(), json!({}));
    }
    {
        let atts = obj
            .get_mut("_attachments")
            .and_then(|a| a.as_object_mut())
            .unwrap();
        change(atts)?;
    }
    let validator = state.validator_for(db);
    let outcome = dbh.with_writer(|w| w.save_doc_with_spools(&doc, validator, spools))?;
    match outcome {
        SaveOutcome::Ok { rev } => Ok(ok_status(success, docid, &rev)),
        SaveOutcome::Error { error, reason } => Err(ApiError::from_save(&error, &reason)),
    }
}

pub async fn att_put(
    State(state): State<App>,
    Path((db, docid, att)): Path<(String, String, String)>,
    Query(q): Query<Q>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<Response> {
    let rev = q.get("rev").cloned().or_else(|| {
        headers
            .get(header::IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
    });
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    // Stream the body to a spool — attachment size never touches RAM.
    let spool = spool_body(request.into_body(), &state.dir).await?;
    let spooled = spool.into_att(att.clone(), ct.clone());
    blocking(move || {
        modify_atts(
            &state,
            &db,
            &docid,
            rev.as_deref(),
            StatusCode::CREATED,
            vec![spooled],
            move |atts| {
                atts.insert(att, json!({ "content_type": ct }));
                Ok(())
            },
        )
    })
}

pub async fn att_delete(
    State(state): State<App>,
    Path((db, docid, att)): Path<(String, String, String)>,
    Query(q): Query<Q>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let rev = q.get("rev").cloned().or_else(|| {
        headers
            .get(header::IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
    });
    blocking(move || {
        modify_atts(&state, &db, &docid, rev.as_deref(), StatusCode::OK, Vec::new(), move |atts| {
            if atts.remove(&att).is_none() {
                return Err(ApiError::not_found("Document is missing attachment"));
            }
            Ok(())
        })
    })
}

// ---------------------------------------------------------------- _local

async fn local_get_inner(state: App, db: String, name: String) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let id = format!("_local/{name}");
        match snap.open_local(id.as_bytes())? {
            Some(doc) => Ok(Json(doc).into_response()),
            None => Err(ApiError::missing()),
        }
    })
}

pub async fn local_get(
    State(state): State<App>,
    Path((db, name)): Path<(String, String)>,
) -> ApiResult<Response> {
    local_get_inner(state, db, name).await
}

pub async fn local_put(
    State(state): State<App>,
    Path((db, name)): Path<(String, String)>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let doc = parse_json(&body)?;
    let dbh = state.db(&db)?;
    let id = format!("_local/{name}");
    let rev = blocking(|| dbh.with_writer(|w| w.update_local(id.as_bytes(), Some(&doc))))?;
    Ok(created(&id, &rev))
}

async fn local_delete_inner(state: App, db: String, name: String) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    let id = format!("_local/{name}");
    blocking(|| dbh.with_writer(|w| w.update_local(id.as_bytes(), None)))?;
    Ok((
        StatusCode::OK,
        Json(json!({"ok": true, "id": id, "rev": "0-0"})),
    )
        .into_response())
}

pub async fn local_delete(
    State(state): State<App>,
    Path((db, name)): Path<(String, String)>,
) -> ApiResult<Response> {
    local_delete_inner(state, db, name).await
}
