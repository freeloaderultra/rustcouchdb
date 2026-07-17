use crate::cli::ServeArgs;
use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::ids::Filter;
use crate::pipeline::{self, RepOptions};
use crate::retry::RetryPolicy;
use crate::stats::Stats;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// One replication job as accepted in the config file and on POST /_jobs.
#[derive(Deserialize, Serialize, Clone)]
pub struct JobSpec {
    #[serde(default)]
    pub name: Option<String>,
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub continuous: bool,
    #[serde(default)]
    pub winning_revs_only: bool,
    #[serde(default)]
    pub create_target: bool,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub doc_ids: Option<Vec<String>>,
    #[serde(default)]
    pub selector: Option<serde_json::Value>,
    #[serde(default = "d_fetch")]
    pub fetch_concurrency: usize,
    #[serde(default = "d_write")]
    pub write_concurrency: usize,
    #[serde(default = "d_att")]
    pub att_concurrency: usize,
    #[serde(default = "d_batch")]
    pub batch_size: usize,
    #[serde(default = "d_batch_bytes")]
    pub max_batch_bytes: usize,
    #[serde(default = "d_inline")]
    pub inline_att_threshold: u64,
    #[serde(default = "d_ckpt")]
    pub checkpoint_interval_ms: u64,
    #[serde(default)]
    pub no_checkpoints: bool,
    #[serde(default)]
    pub no_bulk_get: bool,
    #[serde(default = "d_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "d_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub continue_on_error: bool,
    #[serde(default)]
    pub insecure: bool,
    #[serde(default = "d_changes_limit")]
    pub changes_limit: usize,
}

fn d_fetch() -> usize { 32 }
fn d_write() -> usize { 8 }
fn d_att() -> usize { 16 }
fn d_batch() -> usize { 500 }
fn d_batch_bytes() -> usize { 4 * 1024 * 1024 }
fn d_inline() -> u64 { 65536 }
fn d_ckpt() -> u64 { 30000 }
fn d_timeout() -> u64 { 60 }
fn d_retries() -> u32 { 10 }
fn d_changes_limit() -> usize { 10000 }

impl JobSpec {
    fn endpoints(&self) -> Result<(Endpoint, Endpoint)> {
        let retry = RetryPolicy {
            max_retries: self.max_retries,
            ..RetryPolicy::default()
        };
        let source = Endpoint::new(
            "source",
            &self.source,
            &[],
            self.insecure,
            self.timeout_secs,
            retry,
        )?;
        let target = Endpoint::new(
            "target",
            &self.target,
            &[],
            self.insecure,
            self.timeout_secs,
            retry,
        )?;
        Ok((source, target))
    }

    fn options(&self) -> RepOptions {
        RepOptions {
            continuous: self.continuous,
            winning_revs_only: self.winning_revs_only,
            create_target: self.create_target,
            since: self.since.clone(),
            filter: Filter {
                doc_ids: self.doc_ids.clone(),
                selector: self.selector.clone(),
            },
            fetch_concurrency: self.fetch_concurrency.max(1),
            write_concurrency: self.write_concurrency.max(1),
            att_concurrency: self.att_concurrency.max(1),
            batch_size: self.batch_size.max(1),
            max_batch_bytes: self.max_batch_bytes.max(64 * 1024),
            inline_att_threshold: self.inline_att_threshold,
            checkpoint_interval: Duration::from_millis(self.checkpoint_interval_ms.max(1000)),
            use_checkpoints: !self.no_checkpoints,
            use_bulk_get: !self.no_bulk_get,
            request_gzip: true,
            continue_on_error: self.continue_on_error,
            changes_limit: self.changes_limit.max(100),
            stats_interval: Duration::from_secs(5),
            progress: false,
        }
    }
}

#[derive(Serialize, Clone)]
#[serde(tag = "state", rename_all = "snake_case")]
enum JobState {
    Running { attempt: u32 },
    Retrying { attempt: u32, retry_in_secs: u64, last_error: String },
    Completed { exit: i32 },
    Failed { error: String },
    Cancelled,
}

impl JobState {
    fn terminal(&self) -> bool {
        matches!(
            self,
            JobState::Completed { .. } | JobState::Failed { .. } | JobState::Cancelled
        )
    }
}

struct JobEntry {
    spec: JobSpec,
    display_source: String,
    display_target: String,
    stats: Arc<Stats>,
    state: Mutex<JobState>,
    cancel: CancellationToken,
    started: Instant,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl JobEntry {
    fn as_json(&self, name: &str) -> serde_json::Value {
        let state = self.state.lock().unwrap().clone();
        serde_json::json!({
            "name": name,
            "source": self.display_source,
            "target": self.display_target,
            "continuous": self.spec.continuous,
            "uptime_secs": self.started.elapsed().as_secs(),
            "status": state,
            "stats": self.stats.snapshot(),
        })
    }
}

#[derive(Default)]
struct ServerState {
    jobs: Mutex<HashMap<String, Arc<JobEntry>>>,
}

impl ServerState {
    /// Validate the spec, register the job, and spawn its supervised runner.
    fn add_job(&self, mut spec: JobSpec) -> Result<String> {
        let (source, target) = spec.endpoints()?;
        let filter = Filter {
            doc_ids: spec.doc_ids.clone(),
            selector: spec.selector.clone(),
        };
        let rep_id = crate::ids::replication_id(
            &source.normalized_url(),
            &target.normalized_url(),
            &filter,
            spec.winning_revs_only,
        );
        let name = spec
            .name
            .clone()
            .unwrap_or_else(|| format!("{}-{}", source.db_name(), &rep_id[..8]));
        spec.name = Some(name.clone());

        let entry = Arc::new(JobEntry {
            display_source: source.normalized_url(),
            display_target: target.normalized_url(),
            spec,
            stats: Arc::new(Stats::default()),
            state: Mutex::new(JobState::Running { attempt: 0 }),
            cancel: CancellationToken::new(),
            started: Instant::now(),
            handle: Mutex::new(None),
        });

        {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(existing) = jobs.get(&name) {
                if !existing.state.lock().unwrap().terminal() {
                    return Err(Error::Protocol(format!("job \"{name}\" already running")));
                }
            }
            jobs.insert(name.clone(), entry.clone());
        }

        let handle = tokio::spawn(run_job(entry.clone()));
        *entry.handle.lock().unwrap() = Some(handle);
        info!("job \"{name}\" registered");
        Ok(name)
    }

    fn cancel_all(&self) {
        for entry in self.jobs.lock().unwrap().values() {
            entry.cancel.cancel();
        }
    }

    fn drain_handles(&self) -> Vec<JoinHandle<()>> {
        self.jobs
            .lock()
            .unwrap()
            .values()
            .filter_map(|e| e.handle.lock().unwrap().take())
            .collect()
    }
}

/// Supervised runner: restart with exponential backoff until the job either
/// completes (one-shot) or is canceled.
async fn run_job(entry: Arc<JobEntry>) {
    let name = entry.spec.name.clone().unwrap_or_default();
    let mut attempt: u32 = 0;
    loop {
        if entry.cancel.is_cancelled() {
            *entry.state.lock().unwrap() = JobState::Cancelled;
            return;
        }
        *entry.state.lock().unwrap() = JobState::Running { attempt };
        let run = async {
            let (source, target) = entry.spec.endpoints()?;
            pipeline::replicate(
                source,
                target,
                entry.spec.options(),
                entry.stats.clone(),
                entry.cancel.clone(),
            )
            .await
        };
        match run.await {
            Ok(code) => {
                if entry.cancel.is_cancelled() {
                    *entry.state.lock().unwrap() = JobState::Cancelled;
                    info!("job \"{name}\" canceled");
                } else {
                    *entry.state.lock().unwrap() = JobState::Completed { exit: code };
                    info!("job \"{name}\" completed (exit {code})");
                }
                return;
            }
            Err(e @ Error::Url(_)) => {
                // Config problems never fix themselves.
                *entry.state.lock().unwrap() = JobState::Failed { error: e.to_string() };
                error!("job \"{name}\" failed permanently: {e}");
                return;
            }
            Err(e) => {
                if entry.cancel.is_cancelled() {
                    *entry.state.lock().unwrap() = JobState::Cancelled;
                    return;
                }
                attempt += 1;
                let delay = Duration::from_secs((30u64 << attempt.min(4)).min(300));
                warn!("job \"{name}\" failed ({e}); restarting in {delay:?}");
                *entry.state.lock().unwrap() = JobState::Retrying {
                    attempt,
                    retry_in_secs: delay.as_secs(),
                    last_error: e.to_string(),
                };
                tokio::select! {
                    _ = entry.cancel.cancelled() => {
                        *entry.state.lock().unwrap() = JobState::Cancelled;
                        return;
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

async fn up(State(st): State<Arc<ServerState>>) -> Json<serde_json::Value> {
    let jobs = st.jobs.lock().unwrap().len();
    Json(serde_json::json!({"status": "ok", "jobs": jobs}))
}

async fn list_jobs(State(st): State<Arc<ServerState>>) -> Json<serde_json::Value> {
    let jobs = st.jobs.lock().unwrap();
    let mut out: Vec<serde_json::Value> =
        jobs.iter().map(|(name, e)| e.as_json(name)).collect();
    out.sort_by_key(|j| j["name"].as_str().unwrap_or("").to_string());
    Json(serde_json::Value::Array(out))
}

async fn add_job(
    State(st): State<Arc<ServerState>>,
    Json(spec): Json<JobSpec>,
) -> Response {
    match st.add_job(spec) {
        Ok(name) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"ok": true, "name": name})),
        )
            .into_response(),
        Err(e) => {
            let code = if e.to_string().contains("already running") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            (code, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn get_job(
    State(st): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Response {
    let jobs = st.jobs.lock().unwrap();
    match jobs.get(&name) {
        Some(e) => Json(e.as_json(&name)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no such job"})),
        )
            .into_response(),
    }
}

async fn delete_job(
    State(st): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Response {
    let jobs = st.jobs.lock().unwrap();
    match jobs.get(&name) {
        Some(e) => {
            e.cancel.cancel();
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({"ok": true, "name": name, "canceling": true})),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no such job"})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped { jobs: Vec<JobSpec> },
    Bare(Vec<JobSpec>),
}

pub async fn run(args: ServeArgs) -> Result<()> {
    let state = Arc::new(ServerState::default());

    if let Some(path) = &args.config {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| Error::Protocol(format!("cannot read {}: {e}", path.display())))?;
        let cfg: ConfigFile = serde_json::from_str(&raw)
            .map_err(|e| Error::Protocol(format!("bad config {}: {e}", path.display())))?;
        let specs = match cfg {
            ConfigFile::Wrapped { jobs } => jobs,
            ConfigFile::Bare(jobs) => jobs,
        };
        for spec in specs {
            state.add_job(spec)?;
        }
    }

    let app = Router::new()
        .route("/_up", get(up))
        .route("/_jobs", get(list_jobs).post(add_job))
        .route("/_jobs/{name}", get(get_job).delete(delete_job))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .map_err(|e| Error::Protocol(format!("cannot bind {}: {e}", args.listen)))?;
    info!("couch-repl server listening on http://{}", args.listen);

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        let state = state.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("interrupt: canceling all jobs and shutting down");
                state.cancel_all();
                shutdown.cancel();
            }
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        });
    }

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .await
        .map_err(|e| Error::Protocol(format!("http server error: {e}")))?;

    // Let jobs drain and write their final checkpoints.
    for handle in state.drain_handles() {
        let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;
    }
    info!("server stopped");
    Ok(())
}
