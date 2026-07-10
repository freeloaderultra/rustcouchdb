//! Database-level endpoints: info, create, delete, _security, _compact,
//! _ensure_full_commit and friends.

use crate::error::{ApiError, ApiResult};
use crate::state::{blocking, App};
use crate::util::parse_json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

pub async fn db_info(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let info = snap.info()?;
        Ok(Json(json!({
            "db_name": dbh.name,
            "doc_count": info["doc_count"],
            "doc_del_count": info["doc_del_count"],
            "update_seq": info["update_seq"].as_u64().unwrap_or(0).to_string(),
            "purge_seq": info["purge_seq"].as_u64().unwrap_or(0).to_string(),
            "compact_running": dbh.compacting.load(std::sync::atomic::Ordering::SeqCst),
            "disk_format_version": info["disk_format_version"],
            "sizes": info["sizes"],
            "props": {},
            "cluster": {"q": 1, "n": 1, "w": 1, "r": 1},
            "instance_start_time": "0",
        }))
        .into_response())
    })
}

pub async fn db_create(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    blocking(|| state.create_db(&db))?;
    Ok((StatusCode::CREATED, Json(json!({"ok": true}))).into_response())
}

pub async fn db_delete(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    blocking(|| state.delete_db(&db))?;
    if db == "_replicator" {
        state.repl.poke();
    }
    Ok(Json(json!({"ok": true})).into_response())
}

pub async fn security_get(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        Ok(Json(snap.security()?).into_response())
    })
}

pub async fn security_put(
    State(state): State<App>,
    Path(db): Path<String>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let doc = parse_json(&body)?;
    if !doc.is_object() {
        return Err(ApiError::bad_request("security must be a JSON object"));
    }
    let dbh = state.db(&db)?;
    blocking(|| dbh.with_writer(|w| w.set_security(&doc)))?;
    Ok(Json(json!({"ok": true})).into_response())
}

pub async fn compact_db(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    tokio::task::spawn_blocking(move || {
        if let Err(e) = dbh.compact() {
            tracing::error!("compaction of {} failed: {} {}", dbh.name, e.error, e.reason);
        } else {
            tracing::info!("compaction of {} finished", dbh.name);
        }
    });
    Ok((StatusCode::ACCEPTED, Json(json!({"ok": true}))).into_response())
}

pub async fn ensure_full_commit(
    State(state): State<App>,
    Path(db): Path<String>,
) -> ApiResult<Response> {
    // Every write already commits; report like 3.x.
    state.db(&db)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"ok": true, "instance_start_time": "0"})),
    )
        .into_response())
}

/// POST /{db}/_compact/{ddoc} and /{db}/_view_cleanup — accepted no-ops.
pub async fn accepted_noop(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    state.db(&db)?;
    Ok((StatusCode::ACCEPTED, Json(json!({"ok": true}))).into_response())
}

pub async fn view_cleanup(state: State<App>, Path((db, _rest)): Path<(String, String)>) -> ApiResult<Response> {
    accepted_noop(state, Path(db)).await
}

/// GET /{db}/_design_docs — minimal support (nxguide checks its ddoc).
pub async fn design_docs(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    blocking(move || {
        let snap = dbh.snapshot();
        let mut rows = Vec::new();
        snap.fold_docs(|fdi| {
            if !fdi.id.starts_with(b"_design/") {
                return Ok(std::ops::ControlFlow::Continue(()));
            }
            if !fdi.deleted {
                if let Some(rev) = crate::util::winner_rev(&fdi) {
                    let id = String::from_utf8_lossy(&fdi.id).into_owned();
                    rows.push(json!({"id": id, "key": id, "value": {"rev": rev}}));
                }
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(Json(json!({"total_rows": rows.len(), "offset": 0, "rows": rows})).into_response())
    })
}

/// GET /{db}/_shards — single-shard answer for tooling that asks.
pub async fn shards(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    state.db(&db)?;
    Ok(Json(json!({"shards": {"00000000-ffffffff": ["nonode@nohost"]}})).into_response())
}

pub async fn revs_limit_get(State(state): State<App>, Path(db): Path<String>) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    let snap = dbh.snapshot();
    Ok(Json(Value::from(snap.header.revs_limit)).into_response())
}
