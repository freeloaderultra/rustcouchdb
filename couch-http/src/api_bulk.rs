//! Batch endpoints: _bulk_docs, _bulk_get, _revs_diff, _missing_revs,
//! _all_docs.

use crate::error::{ApiError, ApiResult};
use crate::state::{blocking, App};
use crate::util::{parse_json, qbool, qjson, qu64, winner_rev, Q};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use couch_store::db::DocOpts;
use couch_store::doc as docmod;
use couch_store::etf::Term;
use couch_store::writer::{DocUpdate, SaveOutcome};
use serde_json::{json, Map, Value};
use std::ops::ControlFlow;

// ---------------------------------------------------------------- _bulk_docs

pub async fn bulk_docs(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let docs = v
        .get("docs")
        .and_then(|d| d.as_array())
        .ok_or_else(|| ApiError::bad_request("POST body must include `docs` parameter."))?
        .clone();
    let new_edits = !matches!(v.get("new_edits"), Some(Value::Bool(false)));
    let dbh = state.db(&db)?;
    let validator = state.validator_for(&db);

    let results = blocking(move || -> ApiResult<Vec<Value>> {
        let proto_db = dbh.snapshot().proto_native();
        // Proto-native enforcement, batch-wide and up front: the whole
        // request fails if any doc violates the world's rules — a partial
        // batch masking bad docs is exactly the kind of fallback we refuse.
        for doc in &docs {
            let id = doc.get("_id").and_then(|i| i.as_str()).unwrap_or("");
            let is_env = crate::proto::is_envelope(doc);
            if crate::proto::is_app_doc_id(id) && !id.is_empty() {
                if proto_db && !is_env {
                    return Err(ApiError::bad_request(format!(
                        "{id}: proto-native database: application documents must be protobuf"
                    )));
                }
                if !proto_db && is_env {
                    return Err(ApiError::bad_request(format!(
                        "{id}: proto documents require a proto-native database"
                    )));
                }
                if is_env && new_edits {
                    return Err(ApiError::bad_request(format!(
                        "{id}: $pb envelopes are a replication transport (new_edits:false)"
                    )));
                }
            } else if is_env {
                return Err(ApiError::bad_request(format!(
                    "{id}: reserved document ids cannot hold proto documents"
                )));
            }
        }
        let mut results = Vec::with_capacity(docs.len());
        if new_edits {
            dbh.with_writer(|w| {
                for mut doc in docs {
                    if let Some(obj) = doc.as_object_mut() {
                        if !obj.contains_key("_id") {
                            obj.insert("_id".into(), Value::String(crate::state::gen_uuid()));
                        }
                    }
                    let id = doc.get("_id").and_then(|i| i.as_str()).unwrap_or("").to_string();
                    if id.starts_with("_local/") {
                        let rev = w.update_local(id.as_bytes(), Some(&doc))?;
                        results.push(json!({"ok": true, "id": id, "rev": rev}));
                        continue;
                    }
                    match w.save_doc(&doc, validator) {
                        Ok(SaveOutcome::Ok { rev }) => {
                            results.push(json!({"ok": true, "id": id, "rev": rev}))
                        }
                        Ok(SaveOutcome::Error { error, reason }) => {
                            results.push(json!({"id": id, "error": error, "reason": reason}))
                        }
                        Err(e) => results.push(json!({
                            "id": id, "error": "bad_request", "reason": e.to_string()
                        })),
                    }
                }
                Ok(())
            })?;
        } else {
            // Replicated changes: batch through the rev-tree merge. Errors
            // are reported per doc; successes are silent (like CouchDB).
            let mut updates = Vec::new();
            dbh.with_writer(|w| {
                for doc in docs {
                    let id = doc.get("_id").and_then(|i| i.as_str()).unwrap_or("").to_string();
                    if id.starts_with("_local/") {
                        w.update_local(id.as_bytes(), Some(&doc))?;
                        continue;
                    }
                    let parsed = if crate::proto::is_envelope(&doc) {
                        crate::proto::envelope_parts(&doc)
                            .map_err(couch_store::error::Error::from)
                            .and_then(|(t, bytes)| DocUpdate::from_proto_envelope(&doc, t, bytes))
                    } else {
                        DocUpdate::from_json(doc)
                    };
                    match parsed {
                        Ok(u) => updates.push(u),
                        Err(e) => results.push(json!({
                            "id": id, "error": "bad_request", "reason": e.to_string()
                        })),
                    }
                }
                w.update_docs(updates)?;
                Ok(())
            })?;
        }
        Ok(results)
    })?;

    if db == "_replicator" {
        state.repl.poke();
    }
    Ok((StatusCode::CREATED, Json(Value::Array(results))).into_response())
}

// ---------------------------------------------------------------- _bulk_get

pub async fn bulk_get(
    State(state): State<App>,
    Path(db): Path<String>,
    Query(q): Query<Q>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let docs = v
        .get("docs")
        .and_then(|d| d.as_array())
        .ok_or_else(|| ApiError::bad_request("Missing JSON list of 'docs'."))?
        .clone();
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let opts = DocOpts {
            revs: qbool(&q, "revs", false),
            conflicts: false,
            attachments: qbool(&q, "attachments", false),
        };
        let latest = qbool(&q, "latest", false);
        let mut results = Vec::with_capacity(docs.len());
        for want in docs {
            let id = want.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
            let rev = want.get("rev").and_then(|r| r.as_str()).filter(|r| !r.is_empty());
            let mut out_docs = Vec::new();
            if id.is_empty() {
                out_docs.push(json!({"error": {
                    "id": null, "rev": null, "error": "illegal_docid",
                    "reason": "Document id must not be empty",
                }}));
                results.push(json!({"id": id, "docs": out_docs}));
                continue;
            }
            let fdi = snap.open_doc_info(id.as_bytes())?;
            let not_found = |rev: Option<&str>| {
                json!({"error": {
                    "id": id, "rev": rev.unwrap_or("undefined"),
                    "error": "not_found", "reason": "missing",
                }})
            };
            match (&fdi, rev) {
                (None, _) => out_docs.push(not_found(rev)),
                (Some(f), None) => match f.rev_tree.winner() {
                    Some(w) if !matches!(w.leaf, couch_store::revtree::RevVal::Leaf(l) if l.deleted) => {
                        crate::metrics::bump(&crate::metrics::DATABASE_READS);
                        out_docs.push(json!({"ok": snap.doc_json(f, &w, &opts)?}))
                    }
                    _ => out_docs.push(not_found(None)),
                },
                (Some(f), Some(r)) => {
                    let (pos, revid) = docmod::parse_rev(r)?;
                    let leaves = if latest {
                        let mut d = f.rev_tree.descendant_leaves(pos, &revid);
                        d.sort_by(|a, b| (b.pos, b.path[0]).cmp(&(a.pos, a.path[0])));
                        d
                    } else {
                        f.rev_tree.rev_path(pos, &revid).into_iter().collect()
                    };
                    if leaves.is_empty() {
                        out_docs.push(not_found(Some(r)));
                    } else {
                        for leaf in leaves {
                            crate::metrics::bump(&crate::metrics::DATABASE_READS);
                            out_docs.push(json!({"ok": snap.doc_json(f, &leaf, &opts)?}));
                        }
                    }
                }
            }
            results.push(json!({"id": id, "docs": out_docs}));
        }
        Ok(Json(json!({"results": results})).into_response())
    })
}

// ---------------------------------------------------------------- _revs_diff

pub async fn revs_diff(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let Value::Object(map) = v else {
        return Err(ApiError::bad_request("Request body must be a JSON object"));
    };
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let mut out = Map::new();
        for (id, revs) in map {
            let revs: Vec<String> = revs
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|r| r.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let fdi = snap.open_doc_info(id.as_bytes())?;
            let (known, leaf_revs): (Vec<(u64, Vec<u8>)>, Vec<(u64, Vec<u8>)>) = match &fdi {
                Some(f) => (
                    f.rev_tree
                        .all_revs()
                        .into_iter()
                        .map(|(p, r)| (p, r.to_vec()))
                        .collect(),
                    f.rev_tree
                        .leaves()
                        .iter()
                        .map(|l| (l.pos, l.path[0].to_vec()))
                        .collect(),
                ),
                None => (vec![], vec![]),
            };
            let mut missing = Vec::new();
            let mut max_missing_pos = 0u64;
            for r in &revs {
                let Ok((pos, revid)) = docmod::parse_rev(r) else {
                    missing.push(r.clone());
                    continue;
                };
                if !known.iter().any(|(p, rv)| *p == pos && rv == &revid) {
                    max_missing_pos = max_missing_pos.max(pos);
                    missing.push(r.clone());
                }
            }
            if missing.is_empty() {
                continue;
            }
            let mut entry = Map::new();
            entry.insert("missing".into(), json!(missing));
            // Leafs below some missing rev can seed the fetch
            // (couch_key_tree:possible_ancestors).
            let pa: Vec<String> = leaf_revs
                .iter()
                .filter(|(p, _)| *p < max_missing_pos)
                .map(|(p, r)| docmod::rev_str(*p, r))
                .collect();
            if !pa.is_empty() {
                entry.insert("possible_ancestors".into(), json!(pa));
            }
            out.insert(id, Value::Object(entry));
        }
        Ok(Json(Value::Object(out)).into_response())
    })
}

/// The pre-1.2 variant some clients still call.
pub async fn missing_revs(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let Value::Object(map) = v else {
        return Err(ApiError::bad_request("Request body must be a JSON object"));
    };
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let mut out = Map::new();
        for (id, revs) in map {
            let revs: Vec<String> = revs
                .as_array()
                .map(|a| a.iter().filter_map(|r| r.as_str().map(String::from)).collect())
                .unwrap_or_default();
            let fdi = snap.open_doc_info(id.as_bytes())?;
            let known: Vec<(u64, Vec<u8>)> = match &fdi {
                Some(f) => f
                    .rev_tree
                    .all_revs()
                    .into_iter()
                    .map(|(p, r)| (p, r.to_vec()))
                    .collect(),
                None => vec![],
            };
            let missing: Vec<String> = revs
                .into_iter()
                .filter(|r| match docmod::parse_rev(r) {
                    Ok((pos, revid)) => !known.iter().any(|(p, rv)| *p == pos && rv == &revid),
                    Err(_) => true,
                })
                .collect();
            if !missing.is_empty() {
                out.insert(id, json!(missing));
            }
        }
        Ok(Json(json!({"missing_revs": out})).into_response())
    })
}

// ---------------------------------------------------------------- _all_docs

pub async fn all_docs_get(
    state: State<App>,
    path: Path<String>,
    Query(q): Query<Q>,
) -> ApiResult<Response> {
    all_docs_inner(state, path, q, None).await
}

pub async fn all_docs_post(
    state: State<App>,
    path: Path<String>,
    Query(q): Query<Q>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    all_docs_inner(state, path, q, Some(v)).await
}

async fn all_docs_inner(
    State(state): State<App>,
    Path(db): Path<String>,
    q: Q,
    body: Option<Value>,
) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    let proto_bodies = qbool(&q, "proto_bodies", false);
    let registry = if qbool(&q, "include_docs", false) {
        blocking(|| state.proto_registry())?
    } else {
        None
    };
    blocking(move || {
        let snap = dbh.snapshot();
        let include_docs = qbool(&q, "include_docs", false);
        let descending = qbool(&q, "descending", false);
        let inclusive_end = qbool(&q, "inclusive_end", true);
        let limit = qu64(&q, "limit").unwrap_or(u64::MAX);
        let mut skip = qu64(&q, "skip").unwrap_or(0);
        let opts = DocOpts {
            attachments: qbool(&q, "attachments", false),
            conflicts: qbool(&q, "conflicts", false),
            ..Default::default()
        };

        let (live, _, _, _) = snap.doc_counts()?;

        // keys= mode: point lookups, deleted and missing rows included.
        let keys = match &body {
            Some(v) => v.get("keys").and_then(|k| k.as_array()).cloned(),
            None => qjson(&q, "keys")?.and_then(|v| v.as_array().cloned()),
        };
        if let Some(keys) = keys {
            let mut rows = Vec::new();
            for k in keys {
                let Some(id) = k.as_str() else {
                    rows.push(json!({"key": k, "error": "not_found"}));
                    continue;
                };
                match snap.open_doc_info(id.as_bytes())? {
                    None => rows.push(json!({"key": id, "error": "not_found"})),
                    Some(fdi) => {
                        let rev = winner_rev(&fdi).unwrap_or_default();
                        let mut value = json!({"rev": rev});
                        if fdi.deleted {
                            value["deleted"] = json!(true);
                        }
                        let mut row = json!({"id": id, "key": id, "value": value});
                        if include_docs {
                            row["doc"] = if fdi.deleted {
                                Value::Null
                            } else {
                                crate::metrics::bump(&crate::metrics::DATABASE_READS);
                                match snap.open_doc(id.as_bytes(), None, &opts)? {
                                    Some(d) => crate::proto::present_doc(registry.as_deref(), d, proto_bodies)?,
                                    None => Value::Null,
                                }
                            };
                        }
                        rows.push(row);
                    }
                }
            }
            return Ok(Json(json!({
                "total_rows": live, "offset": null, "rows": rows,
            }))
            .into_response());
        }

        // Range scan over the id tree.
        let key = qjson(&q, "key")?;
        let (startkey, endkey) = if let Some(k) = &key {
            (Some(k.clone()), Some(k.clone()))
        } else {
            (
                qjson(&q, "startkey")?.or(qjson(&q, "start_key")?),
                qjson(&q, "endkey")?.or(qjson(&q, "end_key")?),
            )
        };
        let skey = startkey.as_ref().and_then(|v| v.as_str().map(|s| s.as_bytes().to_vec()));
        let ekey = endkey.as_ref().and_then(|v| v.as_str().map(|s| s.as_bytes().to_vec()));

        let mut rows = Vec::new();
        let mut emitted = 0u64;
        let mut visit = |fdi: couch_store::db::FullDocInfo| -> couch_store::error::Result<ControlFlow<()>> {
            if fdi.deleted {
                return Ok(ControlFlow::Continue(()));
            }
            // Range checks (fold streams from the start key onward).
            if descending {
                if let Some(e) = &ekey {
                    let cmp = fdi.id.as_slice().cmp(e.as_slice());
                    if cmp == std::cmp::Ordering::Less
                        || (!inclusive_end && cmp == std::cmp::Ordering::Equal)
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            } else if let Some(e) = &ekey {
                let cmp = fdi.id.as_slice().cmp(e.as_slice());
                if cmp == std::cmp::Ordering::Greater
                    || (!inclusive_end && cmp == std::cmp::Ordering::Equal)
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            if skip > 0 {
                skip -= 1;
                return Ok(ControlFlow::Continue(()));
            }
            if emitted >= limit {
                return Ok(ControlFlow::Break(()));
            }
            let id = String::from_utf8_lossy(&fdi.id).into_owned();
            let rev = winner_rev(&fdi).unwrap_or_default();
            let mut row = json!({"id": id, "key": id, "value": {"rev": rev}});
            if include_docs {
                if let Some(w) = fdi.rev_tree.winner() {
                    crate::metrics::bump(&crate::metrics::DATABASE_READS);
                    let raw = snap.doc_json(&fdi, &w, &opts)?;
                    row["doc"] = crate::proto::present_doc(registry.as_deref(), raw, proto_bodies)?;
                }
            }
            rows.push(row);
            emitted += 1;
            Ok(ControlFlow::Continue(()))
        };

        if descending {
            let start = skey.clone().map(|k| Term::Bin(k));
            couch_store::btree::fold_rev(
                &snap.file,
                &snap.id_root,
                start.as_ref(),
                &mut |k, v| visit(couch_store::db::Db::fdi_from_id_kv(k, v)?),
            )?;
        } else {
            let start = skey.clone().map(|k| Term::Bin(k));
            couch_store::btree::fold(
                &snap.file,
                &snap.id_root,
                start.as_ref(),
                &mut |k, v| visit(couch_store::db::Db::fdi_from_id_kv(k, v)?),
            )?;
        }
        Ok(Json(json!({
            "total_rows": live, "offset": null, "rows": rows,
        }))
        .into_response())
    })
}
