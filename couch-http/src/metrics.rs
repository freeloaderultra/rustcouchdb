//! Process-wide server metrics behind GET /_node/{node}/_prometheus.
//!
//! Metric names mirror CouchDB's couch_prometheus renderer so existing
//! Prometheus scrape configs and Grafana dashboards work unchanged against
//! rustcouchdb. Counters are globals rather than ServerState fields for the
//! same reason couch_stats is global in CouchDB: Prometheus counters must be
//! monotonic for the life of the process, not of any one state object.

use crate::state::App;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub static HTTPD_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static BULK_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static ABORTED_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static DATABASE_READS: AtomicU64 = AtomicU64::new(0);
pub static DATABASE_WRITES: AtomicU64 = AtomicU64::new(0);
pub static DATABASE_PURGES: AtomicU64 = AtomicU64::new(0);

const METHODS: [&str; 7] = ["COPY", "DELETE", "GET", "HEAD", "OPTIONS", "POST", "PUT"];
static METHOD_COUNTS: [AtomicU64; 7] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

static REQ_TIME_SUM_MICROS: AtomicU64 = AtomicU64::new(0);
static REQ_TIME_COUNT: AtomicU64 = AtomicU64::new(0);

/// Latency samples from the last `WINDOW_SECS`, for the summary quantiles.
/// Upstream's folsom histograms are time-windowed the same way: quantiles
/// reflect recent traffic while _sum/_count are lifetime totals.
const WINDOW_SECS: u64 = 60;
const WINDOW_CAP: usize = 32768;
static WINDOW: Mutex<VecDeque<(Instant, u64)>> = Mutex::new(VecDeque::new());

static START: OnceLock<Instant> = OnceLock::new();

/// Called from ServerState::new so uptime counts from boot.
pub fn init_start() {
    let _ = START.set(Instant::now());
}

pub fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

struct AbortGuard;

impl Drop for AbortGuard {
    fn drop(&mut self) {
        bump(&ABORTED_REQUESTS);
    }
}

/// Outermost router layer: counts every request (auth rejections included)
/// and records latency. If the client goes away mid-request hyper drops the
/// handler future, so the armed guard's Drop fires and the request counts
/// as aborted — couch_stats' httpd.aborted_requests semantics.
pub async fn track(req: Request, next: Next) -> Response {
    let start = Instant::now();
    bump(&HTTPD_REQUESTS);
    if let Some(i) = METHODS.iter().position(|m| *m == req.method().as_str()) {
        bump(&METHOD_COUNTS[i]);
    }
    if req.uri().path().ends_with("/_bulk_docs") {
        bump(&BULK_REQUESTS);
    }
    let armed = AbortGuard;
    let resp = next.run(req).await;
    std::mem::forget(armed);
    let micros = start.elapsed().as_micros() as u64;
    REQ_TIME_SUM_MICROS.fetch_add(micros, Ordering::Relaxed);
    REQ_TIME_COUNT.fetch_add(1, Ordering::Relaxed);
    let mut w = WINDOW.lock().unwrap();
    w.push_back((Instant::now(), micros));
    while w.len() > WINDOW_CAP {
        w.pop_front();
    }
    drop(w);
    resp
}

/// Latencies (seconds) at the summary quantiles over the sliding window.
fn window_quantiles() -> Vec<(&'static str, f64)> {
    let now = Instant::now();
    let mut w = WINDOW.lock().unwrap();
    while let Some((t, _)) = w.front() {
        if now.duration_since(*t).as_secs() >= WINDOW_SECS {
            w.pop_front();
        } else {
            break;
        }
    }
    let mut sorted: Vec<u64> = w.iter().map(|(_, m)| *m).collect();
    drop(w);
    sorted.sort_unstable();
    let pick = |q: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
        sorted[idx] as f64 / 1_000_000.0
    };
    vec![
        ("0.5", pick(0.5)),
        ("0.75", pick(0.75)),
        ("0.9", pick(0.9)),
        ("0.95", pick(0.95)),
        ("0.99", pick(0.99)),
        ("0.999", pick(0.999)),
    ]
}

/// Resident set size; 0 where /proc is unavailable (macOS dev builds).
fn rss_bytes() -> u64 {
    let status = match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

fn head(out: &mut String, name: &str, typ: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {typ}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    head(out, name, "counter", help);
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    head(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {value}");
}

fn get(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

pub fn render(app: &App) -> String {
    let mut out = String::with_capacity(4096);

    counter(
        &mut out,
        "couchdb_uptime_seconds",
        "couchdb uptime",
        START.get().map(|s| s.elapsed().as_secs()).unwrap_or(0),
    );

    // HTTP layer
    counter(
        &mut out,
        "couchdb_httpd_requests_total",
        "number of HTTP requests",
        get(&HTTPD_REQUESTS),
    );
    counter(
        &mut out,
        "couchdb_httpd_bulk_requests_total",
        "number of bulk requests",
        get(&BULK_REQUESTS),
    );
    counter(
        &mut out,
        "couchdb_httpd_aborted_requests_total",
        "number of aborted requests",
        get(&ABORTED_REQUESTS),
    );
    head(
        &mut out,
        "couchdb_httpd_request_methods",
        "counter",
        "number of HTTP requests by method",
    );
    for (i, m) in METHODS.iter().enumerate() {
        let _ = writeln!(
            out,
            "couchdb_httpd_request_methods{{method=\"{m}\"}} {}",
            get(&METHOD_COUNTS[i])
        );
    }

    // Request latency summary: quantiles over the sliding window,
    // _sum/_count cumulative (proper Prometheus summary semantics).
    head(
        &mut out,
        "couchdb_request_time_seconds",
        "summary",
        "length of a request inside CouchDB without MochiWeb",
    );
    for (q, v) in window_quantiles() {
        let _ = writeln!(out, "couchdb_request_time_seconds{{quantile=\"{q}\"}} {v}");
    }
    let _ = writeln!(
        out,
        "couchdb_request_time_seconds_sum {}",
        get(&REQ_TIME_SUM_MICROS) as f64 / 1_000_000.0
    );
    let _ = writeln!(out, "couchdb_request_time_seconds_count {}", get(&REQ_TIME_COUNT));

    // Database ops
    counter(
        &mut out,
        "couchdb_database_reads_total",
        "number of times a document was read from a database",
        get(&DATABASE_READS),
    );
    counter(
        &mut out,
        "couchdb_database_writes_total",
        "number of times a database was changed",
        get(&DATABASE_WRITES),
    );
    counter(
        &mut out,
        "couchdb_database_purges_total",
        "number of times a database was purged",
        get(&DATABASE_PURGES),
    );
    gauge(
        &mut out,
        "couchdb_open_databases",
        "number of open databases",
        app.dbs.read().unwrap().len() as u64,
    );

    // Memory. No Erlang VM here, but the nx dashboards sum() this metric as
    // "CouchDB memory", so report process RSS under the upstream name (and
    // the honest name alongside for when the dashboards migrate).
    let rss = rss_bytes();
    head(
        &mut out,
        "couchdb_erlang_memory_bytes",
        "gauge",
        "process resident set size (no Erlang VM; kept for dashboard compatibility)",
    );
    let _ = writeln!(out, "couchdb_erlang_memory_bytes{{memory_type=\"total\"}} {rss}");
    gauge(
        &mut out,
        "couchdb_memory_bytes",
        "process resident set size in bytes",
        rss,
    );

    // Replicator scheduler gauges, from live job state.
    let jobs = app.repl.snapshot_all();
    let count_state = |s: &str| jobs.iter().filter(|j| j.scheduler_state() == s).count() as u64;
    gauge(
        &mut out,
        "couchdb_couch_replicator_jobs_running",
        "replicator scheduler running jobs",
        count_state("running"),
    );
    // Jobs spawn as soon as the reconcile loop sees their doc; nothing queues.
    gauge(
        &mut out,
        "couchdb_couch_replicator_jobs_pending",
        "replicator scheduler pending jobs",
        0,
    );
    gauge(
        &mut out,
        "couchdb_couch_replicator_jobs_crashed",
        "replicator scheduler crashed jobs",
        count_state("crashing") + count_state("failed"),
    );
    gauge(
        &mut out,
        "couchdb_couch_replicator_jobs_total",
        "total number of replicator scheduler jobs",
        jobs.len() as u64,
    );

    // Replicator counters (process-wide, from couch-repl).
    use couch_repl::metrics as rm;
    counter(
        &mut out,
        "couchdb_couch_replicator_requests_total",
        "number of HTTP requests made by the replicator",
        rm::get(&rm::REQUESTS),
    );
    counter(
        &mut out,
        "couchdb_couch_replicator_responses_total",
        "number of HTTP responses received by the replicator",
        rm::get(&rm::RESPONSES) + rm::get(&rm::RESPONSE_FAILURES),
    );
    counter(
        &mut out,
        "couchdb_couch_replicator_responses_failure_total",
        "number of failed HTTP responses received by the replicator",
        rm::get(&rm::RESPONSE_FAILURES),
    );
    counter(
        &mut out,
        "couchdb_couch_replicator_checkpoints_total",
        "number of checkpoint saves",
        rm::get(&rm::CHECKPOINTS) + rm::get(&rm::CHECKPOINT_FAILURES),
    );
    counter(
        &mut out,
        "couchdb_couch_replicator_checkpoints_failure_total",
        "number of failed checkpoint saves",
        rm::get(&rm::CHECKPOINT_FAILURES),
    );
    counter(
        &mut out,
        "couchdb_couch_replicator_changes_read_failures_total",
        "number of failed replicator changes read failures",
        rm::get(&rm::CHANGES_READ_FAILURES),
    );

    out
}
