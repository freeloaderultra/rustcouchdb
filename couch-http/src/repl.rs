//! The _replicator database manager: docs in _replicator become embedded
//! couch-repl jobs, surfaced through _scheduler/docs, _scheduler/jobs and
//! _active_tasks the way nxguide (and CouchDB tooling) expects.

use crate::state::{iso8601, now_secs, App};
use couch_repl::client::Endpoint;
use couch_repl::ids::{self, Filter};
use couch_repl::pipeline::{self, RepOptions};
use couch_repl::retry::RetryPolicy;
use couch_repl::stats::Stats;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Clone, Debug, PartialEq)]
pub enum Phase {
    Running,
    Crashing(String),
    Completed,
    Failed(String),
}

pub struct Job {
    pub doc_id: String,
    pub spec_sig: String,
    pub rep_id: String,
    pub source: String,
    pub target: String,
    pub continuous: bool,
    pub stats: Arc<Stats>,
    pub phase: Mutex<Phase>,
    pub error_count: AtomicU32,
    pub started: u64,
    cancel: CancellationToken,
}

impl Job {
    fn terminal(&self) -> bool {
        matches!(*self.phase.lock().unwrap(), Phase::Completed | Phase::Failed(_))
    }

    pub fn scheduler_state(&self) -> &'static str {
        match &*self.phase.lock().unwrap() {
            Phase::Running => "running",
            Phase::Crashing(_) => "crashing",
            Phase::Completed => "completed",
            Phase::Failed(_) => "failed",
        }
    }

    pub fn info_json(&self) -> Value {
        let mut info = self.stats_json();
        match &*self.phase.lock().unwrap() {
            Phase::Crashing(e) | Phase::Failed(e) => {
                info["error"] = json!(e);
            }
            _ => {}
        }
        info
    }

    fn stats_json(&self) -> Value {
        let s = &self.stats;
        let read = s.get(&s.changes_read);
        let written = s.get(&s.docs_written);
        json!({
            "changes_pending": read.saturating_sub(s.get(&s.docs_read)),
            "docs_read": s.get(&s.docs_read),
            "docs_written": written,
            "doc_write_failures": s.get(&s.doc_write_failures),
            "missing_revisions_found": s.get(&s.missing_found),
            "revisions_checked": s.get(&s.missing_checked),
            "bytes_written": s.get(&s.bytes_written),
        })
    }

    pub fn scheduler_doc(&self) -> Value {
        json!({
            "database": "_replicator",
            "doc_id": self.doc_id,
            "id": self.rep_id,
            "node": "nonode@nohost",
            "source": self.source,
            "target": self.target,
            "state": self.scheduler_state(),
            "info": self.info_json(),
            "error_count": self.error_count.load(Ordering::Relaxed),
            "start_time": iso8601(self.started),
            "last_updated": iso8601(now_secs()),
        })
    }

    pub fn active_task(&self) -> Option<Value> {
        if self.terminal() {
            return None;
        }
        let mut t = self.stats_json();
        let o = t.as_object_mut().unwrap();
        o.insert("type".into(), json!("replication"));
        o.insert("replication_id".into(), json!(format!("{}+{}", self.rep_id, if self.continuous { "continuous" } else { "normal" })));
        o.insert("doc_id".into(), json!(self.doc_id));
        o.insert("database".into(), json!("_replicator"));
        o.insert("continuous".into(), json!(self.continuous));
        o.insert("source".into(), json!(self.source));
        o.insert("target".into(), json!(self.target));
        o.insert("started_on".into(), json!(self.started));
        o.insert("updated_on".into(), json!(now_secs()));
        let read = self.stats.get(&self.stats.docs_read);
        let written = self.stats.get(&self.stats.docs_written);
        let progress = if read == 0 { 0 } else { (written.min(read) * 100) / read };
        o.insert("progress".into(), json!(progress));
        o.insert("user".into(), Value::Null);
        o.insert("pid".into(), json!("<0.0.0>"));
        o.insert("node".into(), json!("nonode@nohost"));
        Some(t)
    }
}

#[derive(Default)]
pub struct ReplManager {
    pub jobs: Mutex<HashMap<String, Arc<Job>>>,
    notify: tokio::sync::Notify,
}

impl ReplManager {
    /// Wake the reconcile loop (call after any _replicator write).
    pub fn poke(&self) {
        self.notify.notify_one();
    }

    pub fn snapshot_jobs(&self) -> Vec<Arc<Job>> {
        let mut v: Vec<Arc<Job>> = self.jobs.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.doc_id.cmp(&b.doc_id));
        v
    }
}

/// Manager loop: reconcile _replicator docs with running jobs.
pub async fn run(app: App) {
    loop {
        if let Err(e) = reconcile(&app).await {
            error!("_replicator reconcile failed: {e}");
        }
        tokio::select! {
            _ = app.repl.notify.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
        }
    }
}

async fn reconcile(app: &App) -> Result<(), String> {
    let Ok(dbh) = app.db("_replicator") else {
        // No _replicator db: cancel everything.
        for job in app.repl.snapshot_jobs() {
            job.cancel.cancel();
        }
        app.repl.jobs.lock().unwrap().clear();
        return Ok(());
    };
    let docs: Vec<Value> = {
        let dbh = dbh.clone();
        tokio::task::spawn_blocking(move || -> couch_store::error::Result<Vec<Value>> {
            let snap = dbh.snapshot();
            let mut docs = Vec::new();
            snap.fold_docs(|fdi| {
                if fdi.deleted || fdi.id.starts_with(b"_design/") {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
                if let Some(w) = fdi.rev_tree.winner() {
                    docs.push(snap.doc_json(&fdi, &w, &Default::default())?);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
            Ok(docs)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
    };

    let mut desired: HashMap<String, Value> = HashMap::new();
    for doc in docs {
        if let Some(id) = doc.get("_id").and_then(|i| i.as_str()) {
            desired.insert(id.to_string(), doc);
        }
    }

    // Cancel jobs whose docs are gone.
    {
        let mut jobs = app.repl.jobs.lock().unwrap();
        jobs.retain(|doc_id, job| {
            if desired.contains_key(doc_id) {
                true
            } else {
                info!("replication doc {doc_id} removed; canceling job");
                job.cancel.cancel();
                false
            }
        });
    }

    for (doc_id, doc) in desired {
        let state_field = doc.get("_replication_state").and_then(|s| s.as_str());
        if matches!(state_field, Some("completed") | Some("failed")) {
            continue;
        }
        // Identity is the replication spec, not the doc rev — the manager's
        // own _replication_id/state writes must not restart the job.
        let sig = spec_sig(&doc);
        {
            let jobs = app.repl.jobs.lock().unwrap();
            if let Some(job) = jobs.get(&doc_id) {
                if job.spec_sig == sig {
                    continue; // unchanged spec: leave it alone in any phase
                }
                info!("replication doc {doc_id} spec changed; restarting job");
                job.cancel.cancel();
            }
        }
        start_job(app.clone(), doc_id, sig, doc).await;
    }
    Ok(())
}

/// Digest of the spec-relevant fields of a _replicator doc.
fn spec_sig(doc: &Value) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    for field in [
        "source", "target", "continuous", "create_target", "selector", "doc_ids",
        "winning_revs_only", "since_seq", "use_checkpoints", "filter",
    ] {
        h.update(field.as_bytes());
        h.update(doc.get(field).map(|v| v.to_string()).unwrap_or_default());
    }
    crate::state::hex(&h.finalize())
}

fn resolve_endpoint_url(app: &App, v: &Value) -> Result<String, String> {
    let url = match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or("endpoint object without url")?
            .to_string(),
        _ => return Err("endpoint must be a string or {url}".into()),
    };
    if url.contains("://") {
        return Ok(url);
    }
    // Local db name: talk to ourselves over loopback with admin creds.
    let base = app.base_url.read().unwrap().clone();
    match &app.admin {
        Some((u, p)) => {
            let base = base.strip_prefix("http://").unwrap_or(&base);
            Ok(format!("http://{u}:{p}@{base}/{url}"))
        }
        None => Ok(format!("http://{base}/{url}")),
    }
}

fn strip_creds(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            if at < rest.find('/').unwrap_or(rest.len()) {
                return format!("{}{}", &url[..scheme_end + 3], &rest[at + 1..]);
            }
        }
    }
    url.to_string()
}

async fn start_job(app: App, doc_id: String, spec_sig: String, doc: Value) {
    let fail_doc = |app: App, doc_id: String, reason: String| async move {
        warn!("replication doc {doc_id} rejected: {reason}");
        write_doc_fields(
            &app,
            &doc_id,
            json!({
                "_replication_state": "failed",
                "_replication_state_reason": reason,
                "_replication_state_time": iso8601(now_secs()),
            }),
        )
        .await;
    };

    let source_url = match doc.get("source").map(|s| resolve_endpoint_url(&app, s)) {
        Some(Ok(u)) => u,
        Some(Err(e)) => return fail_doc(app, doc_id, e).await,
        None => return fail_doc(app, doc_id, "missing source".into()).await,
    };
    let target_url = match doc.get("target").map(|t| resolve_endpoint_url(&app, t)) {
        Some(Ok(u)) => u,
        Some(Err(e)) => return fail_doc(app, doc_id, e).await,
        None => return fail_doc(app, doc_id, "missing target".into()).await,
    };
    let continuous = doc.get("continuous").and_then(|c| c.as_bool()).unwrap_or(false);
    let winning_revs_only = doc
        .get("winning_revs_only")
        .and_then(|c| c.as_bool())
        .unwrap_or(false);
    let create_target = doc.get("create_target").and_then(|c| c.as_bool()).unwrap_or(false);
    let filter = Filter {
        doc_ids: doc.get("doc_ids").and_then(|d| d.as_array()).map(|a| {
            a.iter().filter_map(|v| v.as_str().map(String::from)).collect()
        }),
        selector: doc.get("selector").cloned().filter(|s| !s.is_null()),
    };
    if doc.get("filter").is_some() {
        return fail_doc(
            app,
            doc_id,
            "JavaScript filters are not supported; use a selector".into(),
        )
        .await;
    }
    let since = doc
        .get("since_seq")
        .map(|s| match s {
            Value::String(x) => x.clone(),
            other => other.to_string(),
        });

    let retry = RetryPolicy::default();
    let mk = |label: &'static str, url: &str| Endpoint::new(label, url, &[], false, 60, retry);
    let (source, target) = match (mk("source", &source_url), mk("target", &target_url)) {
        (Ok(s), Ok(t)) => (s, t),
        (Err(e), _) | (_, Err(e)) => return fail_doc(app, doc_id, e.to_string()).await,
    };

    let rep_id = ids::replication_id(
        &source.normalized_url(),
        &target.normalized_url(),
        &filter,
        continuous,
        winning_revs_only,
    );
    let opts = RepOptions {
        continuous,
        winning_revs_only,
        create_target,
        since,
        filter,
        fetch_concurrency: 32,
        write_concurrency: 8,
        att_concurrency: 16,
        batch_size: 500,
        max_batch_bytes: 4 * 1024 * 1024,
        inline_att_threshold: 65536,
        checkpoint_interval: Duration::from_millis(30000),
        use_checkpoints: doc
            .get("use_checkpoints")
            .and_then(|c| c.as_bool())
            .unwrap_or(true),
        use_bulk_get: true,
        continue_on_error: false,
        changes_limit: 10000,
        stats_interval: Duration::from_secs(5),
        progress: false,
    };

    let job = Arc::new(Job {
        doc_id: doc_id.clone(),
        spec_sig,
        rep_id: rep_id.clone(),
        source: strip_creds(&source_url),
        target: strip_creds(&target_url),
        continuous,
        stats: Arc::new(Stats::default()),
        phase: Mutex::new(Phase::Running),
        error_count: AtomicU32::new(0),
        started: now_secs(),
        cancel: CancellationToken::new(),
    });
    app.repl
        .jobs
        .lock()
        .unwrap()
        .insert(doc_id.clone(), job.clone());
    info!("replication job {doc_id} started ({} -> {})", job.source, job.target);

    // Stamp the replication id (like the Erlang manager).
    write_doc_fields(&app, &doc_id, json!({"_replication_id": rep_id})).await;

    tokio::spawn(supervise(app, job, source_url, target_url, opts));
}

async fn supervise(app: App, job: Arc<Job>, source_url: String, target_url: String, opts: RepOptions) {
    let mut attempt: u32 = 0;
    loop {
        if job.cancel.is_cancelled() {
            return;
        }
        *job.phase.lock().unwrap() = Phase::Running;
        let retry = RetryPolicy::default();
        let run = async {
            let source = Endpoint::new("source", &source_url, &[], false, 60, retry)?;
            let target = Endpoint::new("target", &target_url, &[], false, 60, retry)?;
            pipeline::replicate(source, target, opts.clone(), job.stats.clone(), job.cancel.clone()).await
        };
        match run.await {
            Ok(_) if job.cancel.is_cancelled() => return,
            Ok(_) => {
                if opts.continuous {
                    // A continuous job returning cleanly without cancel means
                    // the feed ended; restart it.
                    attempt += 1;
                } else {
                    *job.phase.lock().unwrap() = Phase::Completed;
                    let s = job.stats_json();
                    write_doc_fields(
                        &app,
                        &job.doc_id,
                        json!({
                            "_replication_state": "completed",
                            "_replication_state_time": iso8601(now_secs()),
                            "_replication_stats": s,
                        }),
                    )
                    .await;
                    info!("replication job {} completed", job.doc_id);
                    return;
                }
            }
            Err(e) => {
                if job.cancel.is_cancelled() {
                    return;
                }
                attempt += 1;
                job.error_count.fetch_add(1, Ordering::Relaxed);
                let permanent = matches!(e, couch_repl::error::Error::Url(_));
                if permanent || attempt > 8 {
                    *job.phase.lock().unwrap() = Phase::Failed(e.to_string());
                    write_doc_fields(
                        &app,
                        &job.doc_id,
                        json!({
                            "_replication_state": "failed",
                            "_replication_state_reason": e.to_string(),
                            "_replication_state_time": iso8601(now_secs()),
                        }),
                    )
                    .await;
                    error!("replication job {} failed permanently: {e}", job.doc_id);
                    return;
                }
                *job.phase.lock().unwrap() = Phase::Crashing(e.to_string());
                warn!("replication job {} crashed ({e}); attempt {attempt}", job.doc_id);
            }
        }
        let delay = Duration::from_secs((5u64 << attempt.min(6)).min(300));
        tokio::select! {
            _ = job.cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Merge fields into a _replicator doc (best effort, conflict-retried).
async fn write_doc_fields(app: &App, doc_id: &str, fields: Value) {
    for _ in 0..3 {
        let Ok(dbh) = app.db("_replicator") else { return };
        let doc_id2 = doc_id.to_string();
        let fields2 = fields.clone();
        let result = {
            let dbh = dbh.clone();
            tokio::task::spawn_blocking(move || -> Result<bool, String> {
                let snap = dbh.snapshot();
                let Some(mut doc) = snap
                    .open_doc(doc_id2.as_bytes(), None, &Default::default())
                    .map_err(|e| e.to_string())?
                else {
                    return Ok(true); // doc gone; nothing to update
                };
                if doc.get("_deleted") == Some(&Value::Bool(true)) {
                    return Ok(true);
                }
                let obj = doc.as_object_mut().unwrap();
                let mut same = true;
                if let Value::Object(f) = &fields2 {
                    for (k, v) in f {
                        if obj.get(k) != Some(v) {
                            same = false;
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
                if same {
                    return Ok(true);
                }
                let out = dbh.with_writer(|w| w.save_doc(&doc, None));
                match out {
                    Ok(couch_store::writer::SaveOutcome::Ok { .. }) => Ok(true),
                    Ok(couch_store::writer::SaveOutcome::Error { error, .. }) => {
                        Ok(error != "conflict") // retry conflicts
                    }
                    Err(e) => Err(format!("{}: {}", e.error, e.reason)),
                }
            })
            .await
        };
        match result {
            Ok(Ok(true)) => return,
            Ok(Ok(false)) => continue,
            Ok(Err(e)) => {
                error!("cannot update _replicator/{doc_id}: {e}");
                return;
            }
            Err(e) => {
                error!("cannot update _replicator/{doc_id}: {e}");
                return;
            }
        }
    }
    warn!("giving up updating _replicator/{doc_id} after conflicts");
}

/// Fields the scheduler shows for docs with no live job (e.g. completed
/// before a restart).
pub fn doc_only_scheduler_entry(doc: &Value) -> Value {
    let state = doc
        .get("_replication_state")
        .and_then(|s| s.as_str())
        .unwrap_or("initializing");
    let mut m = Map::new();
    m.insert("database".into(), json!("_replicator"));
    m.insert("doc_id".into(), doc.get("_id").cloned().unwrap_or(Value::Null));
    m.insert("id".into(), doc.get("_replication_id").cloned().unwrap_or(Value::Null));
    m.insert("node".into(), json!("nonode@nohost"));
    m.insert("source".into(), doc.get("source").cloned().unwrap_or(Value::Null));
    m.insert("target".into(), doc.get("target").cloned().unwrap_or(Value::Null));
    m.insert("state".into(), json!(state));
    let mut info = doc.get("_replication_stats").cloned().unwrap_or(json!({}));
    if let Some(reason) = doc.get("_replication_state_reason") {
        info["error"] = reason.clone();
    }
    m.insert("info".into(), info);
    m.insert("error_count".into(), json!(if state == "failed" { 1 } else { 0 }));
    m.insert(
        "start_time".into(),
        doc.get("_replication_state_time").cloned().unwrap_or(json!(iso8601(0))),
    );
    m.insert(
        "last_updated".into(),
        doc.get("_replication_state_time").cloned().unwrap_or(json!(iso8601(0))),
    );
    Value::Object(m)
}
