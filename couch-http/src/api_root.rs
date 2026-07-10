//! Server-level endpoints: welcome, _up, _all_dbs, _uuids, _active_tasks,
//! _scheduler/*.

use crate::error::{ApiError, ApiResult};
use crate::state::{blocking, App};
use crate::util::{qu64, Q};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

pub const VERSION: &str = "3.5.1";

pub async fn welcome(State(state): State<App>) -> Json<Value> {
    Json(json!({
        "couchdb": "Welcome",
        "version": VERSION,
        "git_sha": "rustcouchdb",
        "uuid": state.server_uuid,
        "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler"],
        "vendor": {"name": "rustcouchdb", "variant": "rust"},
    }))
}

pub async fn up() -> Json<Value> {
    Json(json!({"status": "ok", "seeds": {}}))
}

pub async fn all_dbs(State(state): State<App>) -> Json<Value> {
    Json(json!(state.all_db_names()))
}

pub async fn uuids(State(state): State<App>, Query(q): Query<Q>) -> Json<Value> {
    let count = qu64(&q, "count").unwrap_or(1).min(1000);
    let uuids: Vec<String> = (0..count).map(|_| state.next_uuid()).collect();
    Json(json!({"uuids": uuids}))
}

pub async fn active_tasks(State(state): State<App>) -> Json<Value> {
    let mut tasks: Vec<Value> = state
        .repl
        .snapshot_jobs()
        .iter()
        .filter_map(|j| j.active_task())
        .collect();
    // Compactions.
    let dbs: Vec<_> = state.dbs.read().unwrap().values().cloned().collect();
    for db in dbs {
        if db.compacting.load(std::sync::atomic::Ordering::SeqCst) {
            tasks.push(json!({
                "type": "database_compaction",
                "database": db.name,
                "progress": 0,
                "started_on": crate::state::now_secs(),
                "updated_on": crate::state::now_secs(),
                "pid": "<0.0.0>",
                "node": "nonode@nohost",
            }));
        }
    }
    Json(Value::Array(tasks))
}

/// Everything _scheduler/docs shows: live jobs plus doc-only entries for
/// docs without one (terminal states from before a restart).
fn scheduler_entries(state: &App) -> ApiResult<Vec<Value>> {
    let jobs = state.repl.snapshot_jobs();
    let mut out: Vec<Value> = jobs.iter().map(|j| j.scheduler_doc()).collect();
    let seen: std::collections::HashSet<String> =
        jobs.iter().map(|j| j.doc_id.clone()).collect();
    if let Ok(dbh) = state.db("_replicator") {
        let snap = dbh.snapshot();
        snap.fold_docs(|fdi| {
            if fdi.deleted || fdi.id.starts_with(b"_design/") {
                return Ok(std::ops::ControlFlow::Continue(()));
            }
            let id = String::from_utf8_lossy(&fdi.id).into_owned();
            if !seen.contains(&id) {
                if let Some(w) = fdi.rev_tree.winner() {
                    let doc = snap.doc_json(&fdi, &w, &Default::default())?;
                    out.push(crate::repl::doc_only_scheduler_entry(&doc));
                }
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
    }
    out.sort_by(|a, b| {
        a["doc_id"]
            .as_str()
            .unwrap_or("")
            .cmp(b["doc_id"].as_str().unwrap_or(""))
    });
    Ok(out)
}

pub async fn scheduler_docs(State(state): State<App>) -> ApiResult<Response> {
    blocking(move || {
        let docs = scheduler_entries(&state)?;
        Ok(Json(json!({"total_rows": docs.len(), "offset": 0, "docs": docs})).into_response())
    })
}

pub async fn scheduler_doc(
    State(state): State<App>,
    Path(docid): Path<String>,
) -> ApiResult<Response> {
    blocking(move || {
        let docs = scheduler_entries(&state)?;
        for d in docs {
            if d["doc_id"].as_str() == Some(docid.as_str()) {
                return Ok(Json(d).into_response());
            }
        }
        Err(ApiError::missing())
    })
}

pub async fn scheduler_jobs(State(state): State<App>) -> ApiResult<Response> {
    let jobs: Vec<Value> = state
        .repl
        .snapshot_jobs()
        .iter()
        .filter(|j| !matches!(j.scheduler_state(), "completed" | "failed"))
        .map(|j| {
            json!({
                "id": format!("{}+{}", j.rep_id, if j.continuous { "continuous" } else { "normal" }),
                "database": "_replicator",
                "doc_id": j.doc_id,
                "node": "nonode@nohost",
                "pid": "<0.0.0>",
                "source": j.source,
                "target": j.target,
                "user": null,
                "info": j.info_json(),
                "start_time": crate::state::iso8601(j.started),
                "history": [],
            })
        })
        .collect();
    Ok(Json(json!({"total_rows": jobs.len(), "offset": 0, "jobs": jobs})).into_response())
}
