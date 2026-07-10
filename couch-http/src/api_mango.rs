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
use couch_index::index::{self, IndexDef};
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
    if let Some(t) = v.get("type").and_then(|t| t.as_str()) {
        if t != "json" {
            return Err(ApiError::bad_request(format!(
                "unsupported index type `{t}` (only \"json\")"
            )));
        }
    }
    let fields = normalize_fields(
        idx.get("fields")
            .ok_or_else(|| ApiError::bad_request("Missing required key: fields"))?,
    )?;
    let pfs = idx.get("partial_filter_selector").cloned().filter(|s| {
        !matches!(s, Value::Object(o) if o.is_empty()) && !s.is_null()
    });
    let mut def = IndexDef {
        name: String::new(),
        fields,
        partial_filter_selector: pfs,
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
        let existing = index::list(&dir)?;
        // Same name or same definition → "exists".
        for e in &existing {
            if e.def.name == def.name
                || (e.def.fields == def.fields
                    && e.def.partial_filter_selector == def.partial_filter_selector)
            {
                return Ok(Json(json!({
                    "result": "exists",
                    "id": format!("_design/{}", e.def.name),
                    "name": e.def.name,
                }))
                .into_response());
            }
        }
        let snap = dbh.snapshot();
        let name = def.name.clone();
        index::Index::create(&dir, def, &snap.header.uuid_str())?;
        Ok((
            StatusCode::OK,
            Json(json!({
                "result": "created",
                "id": format!("_design/{name}"),
                "name": name,
            })),
        )
            .into_response())
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
        for idx in index::list(&dir)? {
            let fields: Vec<Value> = idx
                .def
                .fields
                .iter()
                .map(|f| json!({f.clone(): "asc"}))
                .collect();
            let mut def = json!({"fields": fields});
            if let Some(pfs) = &idx.def.partial_filter_selector {
                def["partial_filter_selector"] = pfs.clone();
            }
            indexes.push(json!({
                "ddoc": format!("_design/{}", idx.def.name),
                "name": idx.def.name,
                "type": "json",
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
        let _ = ddoc;
        let path = index::index_dir(&dbh.path).join(format!("{name}.fidx"));
        if !path.exists() {
            return Err(ApiError::not_found(format!("no such index: {name}")));
        }
        std::fs::remove_file(&path).map_err(couch_store::error::Error::Io)?;
        Ok(Json(json!({"ok": true})).into_response())
    })
}

pub async fn find(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let v = parse_json(&body)?;
    let want_stats = matches!(v.get("execution_stats"), Some(Value::Bool(true)));
    let dbh = state.db(&db)?;
    // Serialize against other _find/_index calls: index files are opened RW
    // and updated before the query.
    let lock_db = dbh.clone();
    let _guard = lock_db.index_lock.lock().await;
    blocking(move || {
        let fq = FindQuery::parse(&v)?;
        let selector = couch_mango::Selector::compile(&fq.selector)
            .map_err(|e| ApiError::bad_request(format!("invalid selector: {e}")))?;
        let snap = dbh.snapshot();
        let dir = index::index_dir(&dbh.path);
        let mut indexes = index::list(&dir)?;
        let mut chosen = find::choose(&mut indexes, &fq)?;
        if let Some(idx) = chosen.index.as_deref_mut() {
            idx.update(&snap)?;
        }
        let mut docs = Vec::new();
        let stats = find::execute(&snap, &chosen, &fq, &selector, &mut |doc| {
            docs.push(doc);
            Ok(())
        })?;
        let mut resp = json!({"docs": docs, "bookmark": "nil"});
        if chosen.index.is_none() {
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
                "execution_time_ms": 0.0,
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
        let dir = index::index_dir(&dbh.path);
        let mut indexes = index::list(&dir)?;
        let chosen = find::choose(&mut indexes, &fq)?;
        let mut out = find::explain(&dbh.path, &chosen, &fq);
        out["dbname"] = json!(dbh.name);
        Ok(Json(out).into_response())
    })
}
