use clap::Parser;
use couch_repl::cli::{self, parse_headers, Cli, Cmd};
use couch_repl::client::Endpoint;
use couch_repl::error::Error;
use couch_repl::ids::{self, Filter};
use couch_repl::retry::RetryPolicy;
use couch_repl::{gen, pipeline, server, stats};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let code = match cli.cmd {
        Cmd::Replicate(args) => rt.block_on(replicate(args)),
        Cmd::Id(args) => print_id(args),
        Cmd::Gen(args) => rt.block_on(async {
            match gen::run(args).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }),
        Cmd::Serve(args) => rt.block_on(async {
            match server::run(args).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }),
    };
    std::process::exit(code);
}

fn parse_filter(
    doc_ids: Option<Vec<String>>,
    selector: Option<String>,
) -> Result<Filter, Error> {
    let selector = match selector {
        Some(s) => Some(
            serde_json::from_str(&s)
                .map_err(|e| Error::Protocol(format!("invalid --selector JSON: {e}")))?,
        ),
        None => None,
    };
    Ok(Filter { doc_ids, selector })
}

fn print_id(args: cli::IdArgs) -> i32 {
    let filter = match parse_filter(args.doc_ids, args.selector) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };
    // Normalize the same way replicate does, via Endpoint.
    let mk = |label, url: &str| {
        Endpoint::new(label, url, &[], false, 60, RetryPolicy::default())
            .map(|e| e.normalized_url())
    };
    match (mk("source", &args.source), mk("target", &args.target)) {
        (Ok(s), Ok(t)) => {
            let id = ids::replication_id(&s, &t, &filter, args.winning_revs_only);
            println!("replication id:  {}", ids::task_id(&id, args.continuous));
            println!("checkpoint doc:  {}", ids::checkpoint_doc_id(&id));
            0
        }
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("error: {e}");
            1
        }
    }
}

async fn replicate(args: cli::ReplicateArgs) -> i32 {
    let run = async {
        let retry = RetryPolicy {
            max_retries: args.max_retries,
            ..RetryPolicy::default()
        };
        let src_headers = parse_headers(&args.source_header).map_err(Error::Protocol)?;
        let tgt_headers = parse_headers(&args.target_header).map_err(Error::Protocol)?;
        let source = Endpoint::new(
            "source",
            &args.source,
            &src_headers,
            args.insecure,
            args.timeout,
            retry,
        )?;
        let target = Endpoint::new(
            "target",
            &args.target,
            &tgt_headers,
            args.insecure,
            args.timeout,
            retry,
        )?;
        let filter = parse_filter(args.doc_ids.clone(), args.selector.clone())?;
        let opts = pipeline::RepOptions {
            continuous: args.continuous,
            winning_revs_only: args.winning_revs_only,
            create_target: args.create_target,
            since: args.since.clone(),
            filter,
            fetch_concurrency: args.fetch_concurrency.max(1),
            write_concurrency: args.write_concurrency.max(1),
            att_concurrency: args.att_concurrency.max(1),
            batch_size: args.batch_size.max(1),
            max_batch_bytes: args.max_batch_bytes.max(64 * 1024),
            inline_att_threshold: args.inline_att_threshold,
            checkpoint_interval: Duration::from_millis(args.checkpoint_interval.max(1000)),
            use_checkpoints: !args.no_checkpoints,
            use_bulk_get: !args.no_bulk_get,
            continue_on_error: args.continue_on_error,
            changes_limit: args.changes_limit.max(100),
            stats_interval: Duration::from_secs(args.stats_interval.max(1)),
            progress: true,
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        // First ctrl-c drains and checkpoints; second aborts hard.
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    tracing::warn!(
                        "interrupt: draining in-flight work, writing final checkpoint (ctrl-c again to abort)"
                    );
                    cancel.cancel();
                }
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            });
        }
        let stats = std::sync::Arc::new(stats::Stats::default());
        pipeline::replicate(source, target, opts, stats, cancel).await
    };
    match run.await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            1
        }
    }
}
