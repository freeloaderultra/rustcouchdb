//! Mango over HTTP: _index management, _find and _explain, backed by
//! couch-index files living next to the .couch database.

use crate::error::{ApiError, ApiResult};
use crate::state::{blocking, App};
use crate::util::parse_json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use couch_index::find::{self, FindQuery};
use couch_index::index::{self, IndexDef, IndexKind};
use serde_json::{json, Value};

/// Normalize the `fields` of an index definition: ["a"], [{"a":"asc"}].
fn normalize_fields(v: &Value) -> ApiResult<Vec<String>> {
    let arr = v
        .as_array()
        .ok_or_else(|| ApiError::bad_request("index fields must be an array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for f in arr {
        match f {
            Value::String(s) => out.push(s.clone()),
            Value::Object(o) if o.len() == 1 => {
                let (name, dir) = o.iter().next().unwrap();
                if dir.as_str() == Some("desc") {
                    return Err(ApiError::bad_request(
                        "descending index fields are not supported (indexes serve both directions)",
                    ));
                }
                out.push(name.clone());
            }
            other => {
                return Err(ApiError::bad_request(format!("bad index field: {other}")));
            }
        }
    }
    if out.is_empty() {
        return Err(ApiError::bad_request("index must have at least one field"));
    }
    Ok(out)
}

pub async fn index_create(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let idx = v
        .get("index")
        .ok_or_else(|| ApiError::bad_request("Missing required key: index"))?;
    // "type" is accepted both as a sibling of "index" (CouchDB's shape)
    // and inside it — client libraries like kivik only let callers set
    // the index object, not the request envelope.
    let kind = match v
        .get("type")
        .or_else(|| idx.get("type"))
        .and_then(|t| t.as_str())
    {
        None | Some("json") => IndexKind::Json,
        Some("spatial") => IndexKind::Spatial,
        Some(t) => {
            return Err(ApiError::bad_request(format!(
                "unsupported index type `{t}` (only \"json\" or \"spatial\")"
            )));
        }
    };
    let fields = normalize_fields(
        idx.get("fields")
            .ok_or_else(|| ApiError::bad_request("Missing required key: fields"))?,
    )?;
    if kind == IndexKind::Spatial && fields.len() != 4 {
        return Err(ApiError::bad_request(
            "a spatial index needs exactly 4 fields: the west, south, east, north paths",
        ));
    }
    let pfs = idx.get("partial_filter_selector").cloned().filter(|s| {
        !matches!(s, Value::Object(o) if o.is_empty()) && !s.is_null()
    });
    let mut def = IndexDef {
        name: String::new(),
        fields,
        partial_filter_selector: pfs,
        kind,
    };
    def.name = v
        .get("name")
        .and_then(|n| n.as_str())
        .map(String::from)
        .unwrap_or_else(|| def.auto_name());

    let dbh = state.db(&db)?;
    let lock_db = dbh.clone();
    let _guard = lock_db.index_lock.lock().await;
    blocking(move || {
        let dir = index::index_dir(&dbh.path);
        let snap = dbh.snapshot();
        // Same name or same definition → "exists".
        for e in index::discover(&dir, &snap)? {
            if e.def.name == def.name || e.def.def_json() == def.def_json() {
                let id = e
                    .ddoc_id
                    .unwrap_or_else(|| format!("_design/{}", e.def.name));
                return Ok(Json(json!({
                    "result": "exists",
                    "id": id,
                    "name": e.def.name,
                }))
                .into_response());
            }
        }
        // The definition lives in a design doc (CouchDB-compatible), so it
        // survives file-level data migration and replicates with the data.
        // The .fidx materialization is built lazily by the first _find.
        let name = def.name.clone();
        let ddoc_id = format!("_design/{name}");
        let body = couch_index::ddoc::ddoc_body(&ddoc_id, &def);
        let outcome = dbh.with_writer(|w| w.save_doc(&body, None))?;
        match outcome {
            couch_store::writer::SaveOutcome::Ok { .. } => Ok((
                StatusCode::OK,
                Json(json!({
                    "result": "created",
                    "id": ddoc_id,
                    "name": name,
                })),
            )
                .into_response()),
            couch_store::writer::SaveOutcome::Error { error, reason } => {
                Err(ApiError::from_save(&error, &reason))
            }
        }
    })
}

pub async fn index_list(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let dir = index::index_dir(&dbh.path);
        let mut indexes = vec![json!({
            "ddoc": null,
            "name": "_all_docs",
            "type": "special",
            "def": {"fields": [{"_id": "asc"}]},
        })];
        let snap = dbh.snapshot();
        for d in index::discover(&dir, &snap)? {
            // json defs list CouchDB-style [{field: "asc"}]; spatial defs
            // keep the plain 4-path array (order is the definition).
            let mut def = match d.def.kind {
                IndexKind::Json => json!({
                    "fields": d.def.fields.iter()
                        .map(|f| json!({f.clone(): "asc"}))
                        .collect::<Vec<Value>>()
                }),
                IndexKind::Spatial => json!({"fields": d.def.fields}),
            };
            if let Some(pfs) = &d.def.partial_filter_selector {
                def["partial_filter_selector"] = pfs.clone();
            }
            indexes.push(json!({
                "ddoc": d.ddoc_id
                    .unwrap_or_else(|| format!("_design/{}", d.def.name)),
                "name": d.def.name,
                "type": match d.def.kind {
                    IndexKind::Json => "json",
                    IndexKind::Spatial => "spatial",
                },
                "def": def,
            }));
        }
        Ok(Json(json!({"total_rows": indexes.len(), "indexes": indexes})).into_response())
    })
}

pub async fn index_delete(
    State(state): State<App>,
    Path((db, ddoc, name)): Path<(String, String, String)>,
) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    let lock_db = dbh.clone();
    let _guard = lock_db.index_lock.lock().await;
    blocking(move || {
        let dir = index::index_dir(&dbh.path);
        let snap = dbh.snapshot();
        let want_ddoc = if ddoc.starts_with("_design/") {
            ddoc.clone()
        } else {
            format!("_design/{ddoc}")
        };
        let found = index::discover(&dir, &snap)?.into_iter().find(|d| {
            d.def.name == name
                && d.ddoc_id
                    .as_deref()
                    .map(|id| id == want_ddoc)
                    // legacy standalone .fidx: listed under _design/<name>
                    .unwrap_or(want_ddoc == format!("_design/{name}"))
        });
        let Some(d) = found else {
            return Err(ApiError::not_found(format!("no such index: {name}")));
        };
        if let Some(id) = &d.ddoc_id {
            // tombstone the defining design doc
            if let Some(doc) = snap.open_doc(id.as_bytes(), None, &Default::default())? {
                let del = json!({
                    "_id": id,
                    "_rev": doc["_rev"],
                    "_deleted": true,
                });
                dbh.with_writer(|w| w.save_doc(&del, None))?;
            }
        }
        if let Some(idx) = &d.index {
            std::fs::remove_file(&idx.path).map_err(couch_store::error::Error::Io)?;
        }
        Ok(Json(json!({"ok": true})).into_response())
    })
}

pub async fn find(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let t0 = std::time::Instant::now();
    let v = parse_json(&body)?;
    let want_stats = matches!(v.get("execution_stats"), Some(Value::Bool(true)));
    let dbh = state.db(&db)?;
    let fq = FindQuery::parse(&v)?;
    let selector = couch_mango::Selector::compile(&fq.selector)
        .map_err(|e| ApiError::bad_request(format!("invalid selector: {e}")))?;

    // Proto-blob awareness: with schemas registered, selectors, index keys
    // and projections can reach fields inside protobuf blob attachments.
    let registry = blocking(|| state.proto_registry())?;
    let aug_fn = registry.clone().map(crate::proto::augmenter);

    // Serialize index-file writes: choosing, materializing and updating the
    // index happen under the per-db lock. The scan itself runs after the
    // lock is dropped — index reads are preads against an append-only file,
    // so a concurrent updater can't disturb the tree this query walks, and
    // one slow full scan no longer blocks every other Mango request.
    let guard = dbh.index_lock.lock().await;
    let dbh1 = dbh.clone();
    let aug1 = aug_fn.as_ref();
    let (snap, chosen) = blocking(|| -> ApiResult<_> {
        let snap = dbh1.snapshot();
        let dir = index::index_dir(&dbh1.path);
        let mut defined = index::discover(&dir, &snap)?;
        let mut chosen = find::choose(&mut defined, &fq)?;
        let picked = match chosen.defined.as_deref_mut() {
            Some(d) => {
                let mut idx = match d.index.take() {
                    Some(i) => i,
                    // defined by a design doc but never built: build it now
                    None => index::materialize(&dir, &d.def, &snap.header.uuid_str())?,
                };
                idx.update(&snap, aug1.map(|f| f as _))?;
                let plan = chosen.plan.take().ok_or_else(|| {
                    ApiError::bad_request("chosen index without a plan")
                })?;
                Some((idx, plan))
            }
            None => None,
        };
        Ok((snap, picked))
    })?;
    drop(guard);

    blocking(move || {
        let mut docs = Vec::new();
        let stats = find::execute(
            &snap,
            chosen.as_ref().map(|(i, p)| (i, p)),
            &fq,
            &selector,
            aug_fn.as_ref().map(|f| f as _),
            &mut |doc| {
                // Bare proto-native results come back as the stored $pb
                // envelope; render them to the domain view (projected
                // results already went through the augmenter's view).
                docs.push(crate::proto::render_if_envelope(registry.as_deref(), doc)?);
                Ok(())
            },
        )?;
        let mut resp = json!({"docs": docs, "bookmark": "nil"});
        if chosen.is_none() {
            resp["warning"] = json!(
                "No matching index found, create an index to optimize query time."
            );
        }
        if want_stats {
            resp["execution_stats"] = json!({
                "total_keys_examined": stats.scanned,
                "total_docs_examined": stats.docs_examined,
                "total_quorum_docs_examined": 0,
                "results_returned": stats.results,
                "execution_time_ms": t0.elapsed().as_secs_f64() * 1000.0,
            });
        }
        Ok(Json(resp).into_response())
    })
}

pub async fn explain(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let dbh = state.db(&db)?;
    let lock_db = dbh.clone();
    let _guard = lock_db.index_lock.lock().await;
    blocking(move || {
        let fq = FindQuery::parse(&v)?;
        let snap = dbh.snapshot();
        let dir = index::index_dir(&dbh.path);
        let mut defined = index::discover(&dir, &snap)?;
        let chosen = find::choose(&mut defined, &fq)?;
        let mut out = find::explain(&dbh.path, &chosen, &fq);
        out["dbname"] = json!(dbh.name);
        Ok(Json(out).into_response())
    })
}
