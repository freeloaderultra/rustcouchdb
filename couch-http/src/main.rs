//! rustcouchdb server: the CouchDB HTTP API on couch-store + couch-index,
//! with couch-repl embedded for _replicator jobs. No Erlang, no JavaScript.

use clap::Parser;
use couch_http::state::{App, ServerState};
use couch_http::repl;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "couch-http",
    about = "rustcouchdb: a CouchDB-compatible server in pure Rust — couch-store shard files, couch-index Mango queries, couch-repl replication. No Erlang, no JavaScript."
)]
struct Cli {
    /// Data directory holding <db>.couch files
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Listen address
    #[arg(long, default_value = "127.0.0.1:5984")]
    listen: String,

    /// Admin credentials as user:password (default: open, like admin party)
    #[arg(long, env = "COUCH_HTTP_ADMIN")]
    admin: Option<String>,

    /// Enable the native nxguide soft-delete validator on all non-system dbs
    #[arg(long)]
    soft_delete_validator: bool,

    /// Auto-compact databases when file size exceeds this multiple of live
    /// data (0 disables; smoosh-style background compaction)
    #[arg(long, default_value_t = 2.0)]
    auto_compact_ratio: f64,

    /// Minimum file size in MB before auto-compaction considers a db
    #[arg(long, default_value_t = 4)]
    auto_compact_min_mb: u64,
}

/// smoosh, in one function: compact when the file has grown well past its
/// live data.
async fn auto_compactor(app: App, ratio: f64, min_bytes: u64) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let dbs: Vec<_> = app.dbs.read().unwrap().values().cloned().collect();
        for db in dbs {
            let db2 = db.clone();
            let check = tokio::task::spawn_blocking(move || -> Option<(u64, u64)> {
                let snap = db2.snapshot();
                let info = snap.info().ok()?;
                let file = info["sizes"]["file"].as_u64()?;
                let active = info["sizes"]["active"].as_u64()?;
                Some((file, active))
            })
            .await;
            if let Ok(Some((file, active))) = check {
                if file >= min_bytes && (file as f64) > (active.max(1) as f64) * ratio {
                    info!(
                        "auto-compacting {} (file {} B, active {} B)",
                        db.name, file, active
                    );
                    let db3 = db.clone();
                    let _ = tokio::task::spawn_blocking(move || db3.compact()).await;
                }
            }
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
    let cli = Cli::parse();

    let admin = cli.admin.as_ref().map(|s| {
        let (u, p) = s.split_once(':').unwrap_or((s.as_str(), ""));
        (u.to_string(), p.to_string())
    });
    if admin.is_none() {
        tracing::warn!("no --admin given: running in admin-party mode");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = rt.block_on(async move {
        let app: App = Arc::new(ServerState::new(
            cli.data_dir.clone(),
            admin,
            cli.soft_delete_validator,
        ));
        if let Err(e) = app.open_all() {
            error!("cannot open data dir: {} {}", e.error, e.reason);
            return 1;
        }
        // _replicator always exists.
        if app.db("_replicator").is_err() {
            if let Err(e) = app.create_db("_replicator") {
                error!("cannot create _replicator: {} {}", e.error, e.reason);
                return 1;
            }
        }

        let listener = match tokio::net::TcpListener::bind(&cli.listen).await {
            Ok(l) => l,
            Err(e) => {
                error!("cannot bind {}: {e}", cli.listen);
                return 1;
            }
        };
        let addr = listener.local_addr().map(|a| a.to_string()).unwrap_or(cli.listen.clone());
        *app.base_url.write().unwrap() = addr.replace("0.0.0.0", "127.0.0.1");
        info!(
            "rustcouchdb listening on http://{addr} ({} databases)",
            app.dbs.read().unwrap().len()
        );

        tokio::spawn(repl::run(app.clone()));
        if cli.auto_compact_ratio > 0.0 {
            tokio::spawn(auto_compactor(
                app.clone(),
                cli.auto_compact_ratio,
                cli.auto_compact_min_mb * 1024 * 1024,
            ));
        }

        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
            info!("shutting down");
        };
        if let Err(e) = couch_http::serve(listener, app.clone(), shutdown).await {
            error!("server error: {e}");
            return 1;
        }
        0
    });
    std::process::exit(code);
}
