//! The _changes feed: normal, longpoll and continuous, with style=main_only,
//! _doc_ids and _selector filters — everything couch-repl and the CouchDB
//! replicator ask of a source.

use crate::error::{ApiError, ApiResult};
use crate::state::{blocking, parse_seq, seq_json, App, Database};
use crate::util::{parse_json, qbool, qjson, qu64, Q};
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::Response;
use bytes::Bytes;
use couch_store::db::{Db, DocOpts, FullDocInfo};
use couch_store::doc as docmod;
use couch_store::revtree::RevVal;
use serde_json::{json, Value};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone)]
struct ChangesOpts {
    style_main_only: bool,
    include_docs: bool,
    attachments: bool,
    conflicts: bool,
    doc_ids: Option<Vec<String>>,
    selector: Option<Arc<couch_mango::Selector>>,
    limit: u64,
}

fn parse_opts(q: &Q, body: &Value) -> ApiResult<ChangesOpts> {
    // CouchDB default style is main_only; the replicator asks for all_docs.
    let style_main_only = q.get("style").map(|s| s != "all_docs").unwrap_or(true);
    let mut doc_ids = None;
    let mut selector = None;
    match q.get("filter").map(|s| s.as_str()) {
        None => {}
        Some("_doc_ids") => {
            let ids = body
                .get("doc_ids")
                .cloned()
                .or(qjson(q, "doc_ids")?)
                .and_then(|v| v.as_array().cloned())
                .ok_or_else(|| ApiError::bad_request("filter=_doc_ids requires `doc_ids`"))?;
            doc_ids = Some(
                ids.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>(),
            );
        }
        Some("_selector") => {
            let sel = body
                .get("selector")
                .cloned()
                .ok_or_else(|| ApiError::bad_request("filter=_selector requires a `selector` in the body"))?;
            let sel = couch_mango::Selector::compile(&sel)
                .map_err(|e| ApiError::bad_request(format!("invalid selector: {e}")))?;
            selector = Some(Arc::new(sel));
        }
        Some("_design") | Some("_view") => {
            return Err(ApiError::bad_request("filter not supported"));
        }
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "JavaScript filters are not supported (got `{other}`); use filter=_selector"
            )));
        }
    }
    Ok(ChangesOpts {
        style_main_only,
        include_docs: qbool(q, "include_docs", false),
        attachments: qbool(q, "attachments", false),
        conflicts: qbool(q, "conflicts", false),
        doc_ids,
        selector,
        limit: qu64(q, "limit").unwrap_or(u64::MAX).max(1),
    })
}

/// One changes row for a doc, or None when filtered out.
fn change_row(snap: &Db, fdi: &FullDocInfo, opts: &ChangesOpts) -> couch_store::error::Result<Option<Value>> {
    if let Some(ids) = &opts.doc_ids {
        let id = String::from_utf8_lossy(&fdi.id);
        if !ids.iter().any(|x| x == id.as_ref()) {
            return Ok(None);
        }
    }
    let winner = match fdi.rev_tree.winner() {
        Some(w) => w,
        None => return Ok(None),
    };
    let winner_rev = docmod::rev_str(winner.pos, winner.path[0]);

    // The doc is needed for _selector filtering even without include_docs.
    let doc = if opts.include_docs || opts.selector.is_some() {
        let dopts = DocOpts {
            attachments: opts.attachments,
            conflicts: opts.conflicts,
            ..Default::default()
        };
        crate::metrics::bump(&crate::metrics::DATABASE_READS);
        Some(snap.doc_json(fdi, &winner, &dopts)?)
    } else {
        None
    };
    if let Some(sel) = &opts.selector {
        if !sel.matches(doc.as_ref().unwrap()) {
            return Ok(None);
        }
    }

    let mut changes = Vec::new();
    if opts.style_main_only {
        changes.push(json!({"rev": winner_rev}));
    } else {
        let mut leaves = fdi.rev_tree.leaves();
        // Winner first, like couch_doc:to_doc_info_path.
        leaves.sort_by(|a, b| {
            let da = matches!(a.leaf, RevVal::Leaf(l) if l.deleted);
            let db = matches!(b.leaf, RevVal::Leaf(l) if l.deleted);
            (!db, b.pos, b.path[0]).cmp(&(!da, a.pos, a.path[0]))
        });
        for l in leaves {
            if matches!(l.leaf, RevVal::Missing) {
                continue;
            }
            changes.push(json!({"rev": docmod::rev_str(l.pos, l.path[0])}));
        }
    }

    let mut row = json!({
        "seq": seq_json(fdi.update_seq),
        "id": String::from_utf8_lossy(&fdi.id).into_owned(),
        "changes": changes,
    });
    if fdi.deleted {
        row["deleted"] = json!(true);
    }
    if opts.include_docs {
        row["doc"] = doc.unwrap();
    }
    Ok(Some(row))
}

/// Scan the seq tree from `since`, returning up to `limit` rows.
fn scan(
    snap: &Db,
    since: u64,
    opts: &ChangesOpts,
    limit: u64,
) -> couch_store::error::Result<(Vec<Value>, u64)> {
    let mut rows = Vec::new();
    let mut last_seq = since;
    snap.fold_changes(since, |fdi| {
        last_seq = fdi.update_seq;
        if let Some(row) = change_row(snap, &fdi, opts)? {
            rows.push(row);
            if rows.len() as u64 >= limit {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok((rows, last_seq))
}

pub async fn changes_get(
    state: State<App>,
    path: Path<String>,
    Query(q): Query<Q>,
) -> ApiResult<Response> {
    changes_inner(state, path, q, Value::Object(Default::default())).await
}

pub async fn changes_post(
    state: State<App>,
    path: Path<String>,
    Query(q): Query<Q>,
    body: bytes::Bytes,
) -> ApiResult<Response> {
    let body = parse_json(&body)?;
    changes_inner(state, path, q, body).await
}

async fn changes_inner(
    State(state): State<App>,
    Path(db): Path<String>,
    q: Q,
    body: Value,
) -> ApiResult<Response> {
    let dbh = state.db(&db)?;
    let opts = parse_opts(&q, &body)?;
    let update_seq = dbh.snapshot().header.update_seq;
    let since = parse_seq(q.get("since").map(|s| s.as_str()).unwrap_or("0"), update_seq);
    let feed = q.get("feed").map(|s| s.as_str()).unwrap_or("normal").to_string();
    let heartbeat: Option<u64> = match q.get("heartbeat").map(|s| s.as_str()) {
        Some("true") => Some(60000),
        Some(ms) => ms.parse().ok(),
        None => None,
    };
    let timeout = qu64(&q, "timeout").unwrap_or(60000);

    match feed.as_str() {
        "continuous" => Ok(continuous(dbh, since, opts, heartbeat, timeout)),
        "longpoll" => {
            // Probe for one matching row; the full response is streamed so a
            // huge backlog never materialises in memory.
            let probe = {
                let dbh = dbh.clone();
                let opts = opts.clone();
                blocking(move || scan(&dbh.snapshot(), since, &opts, 1))?
            };
            if !probe.0.is_empty() {
                return Ok(stream_normal(dbh, since, opts, update_seq));
            }
            // Wait for a write past `since`, then rescan once. Check the
            // current seq BEFORE waiting each round: a write that lands
            // between the scan and the wait must not be lost until the next
            // unrelated write.
            let mut seq_rx = dbh.seq_rx.clone();
            let deadline = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(deadline);
            loop {
                if *seq_rx.borrow_and_update() > since {
                    break;
                }
                tokio::select! {
                    _ = &mut deadline => break,
                    r = seq_rx.changed() => {
                        if r.is_err() {
                            break;
                        }
                    }
                }
            }
            let update_seq = dbh.snapshot().header.update_seq;
            Ok(stream_normal(dbh, since, opts, update_seq))
        }
        _ => Ok(stream_normal(dbh, since, opts, update_seq)),
    }
}

/// feed=normal (and the longpoll response): stream the rows as they come off
/// the seq tree. A `since=0` scan of a million-change db must never buffer
/// the whole result — that OOMs small hosts (CouchDB streams here too).
fn stream_normal(dbh: Arc<Database>, since: u64, opts: ChangesOpts, update_seq: u64) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(8);
    tokio::task::spawn_blocking(move || {
        let snap = dbh.snapshot();
        let mut last_seq = since;
        let mut sent = 0u64;
        let mut first = true;
        let mut client_gone = false;
        let mut buf = String::with_capacity(96 * 1024);
        buf.push_str("{\"results\":[\n");
        let res = snap.fold_changes(since, |fdi| {
            last_seq = fdi.update_seq;
            if let Some(row) = change_row(&snap, &fdi, &opts)? {
                if !first {
                    buf.push_str(",\n");
                }
                first = false;
                buf.push_str(&row.to_string());
                sent += 1;
                if buf.len() >= 64 * 1024
                    && tx
                        .blocking_send(Ok(Bytes::from(std::mem::take(&mut buf))))
                        .is_err()
                {
                    client_gone = true;
                    return Ok(ControlFlow::Break(()));
                }
                if sent >= opts.limit {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        });
        if client_gone {
            return;
        }
        match res {
            Ok(()) => {
                let pending = update_seq.saturating_sub(last_seq);
                buf.push_str(&format!(
                    "\n],\n\"last_seq\":{},\"pending\":{}}}\n",
                    seq_json(last_seq),
                    pending
                ));
                let _ = tx.blocking_send(Ok(Bytes::from(buf)));
            }
            Err(e) => {
                // The 200 and part of the body are already on the wire; all
                // we can do is truncate so the client's JSON parse fails.
                tracing::error!("_changes scan failed mid-stream: {e:?}");
            }
        }
    });
    let mut resp = Response::new(axum::body::Body::from_stream(ReceiverStream::new(rx)));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    resp
}

/// feed=continuous: newline-delimited rows until the client goes away
/// (or `limit` is reached). Heartbeats are bare newlines.
fn continuous(
    dbh: Arc<Database>,
    since: u64,
    opts: ChangesOpts,
    heartbeat: Option<u64>,
    timeout: u64,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(64);
    tokio::spawn(async move {
        // Backlog drains in bounded batches so a since=0 feed over millions
        // of changes never buffers the whole result (that OOMs small hosts).
        const DRAIN_BATCH: u64 = 4096;
        let mut since = since;
        let mut remaining = opts.limit;
        let mut seq_rx = dbh.seq_rx.clone();
        loop {
            // Drain what's there.
            let batch = remaining.min(DRAIN_BATCH);
            let (rows, last_seq) = {
                let dbh = dbh.clone();
                let opts = opts.clone();
                match tokio::task::spawn_blocking(move || {
                    scan(&dbh.snapshot(), since, &opts, batch)
                })
                .await
                {
                    Ok(Ok(x)) => x,
                    _ => break,
                }
            };
            since = since.max(last_seq);
            let more_backlog = rows.len() as u64 >= batch;
            for row in rows {
                if remaining == 0 {
                    break;
                }
                remaining -= 1;
                let mut line = row.to_string();
                line.push('\n');
                if tx.send(Ok(Bytes::from(line))).await.is_err() {
                    return; // client gone
                }
            }
            if remaining == 0 {
                break;
            }
            if more_backlog {
                continue; // keep draining before waiting for new writes
            }
            // A write may have landed between the scan and now — rescan
            // immediately instead of waiting (else the event is stuck until
            // the next unrelated write).
            if *seq_rx.borrow_and_update() > since {
                continue;
            }
            let hb = Duration::from_millis(heartbeat.unwrap_or(timeout));
            let wait_result = tokio::select! {
                r = seq_rx.changed() => Some(r),
                _ = tokio::time::sleep(hb) => None,
            };
            match wait_result {
                Some(Err(_)) => break, // db deleted
                Some(Ok(())) => continue,
                None => {
                    if heartbeat.is_some() {
                        if tx.send(Ok(Bytes::from("\n"))).await.is_err() {
                            return;
                        }
                    } else {
                        break; // `timeout` elapsed with no heartbeat configured
                    }
                }
            }
        }
        let fin = json!({"last_seq": seq_json(since), "pending": 0});
        let _ = tx.send(Ok(Bytes::from(format!("{fin}\n")))).await;
    });
    let mut resp = Response::new(axum::body::Body::from_stream(ReceiverStream::new(rx)));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    resp
}
