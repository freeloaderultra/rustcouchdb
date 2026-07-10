use crate::attachments::{AttLane, AttachmentDoc};
use crate::changes::{ChangesReader, TaggedChange};
use crate::checkpoint::Checkpointer;
use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::fetch::Fetcher;
use crate::ids::{checkpoint_doc_id, replication_id, Filter};
use crate::revs_diff::{MissingDoc, RevsDiff};
use crate::seq::SeqLedger;
use crate::stats::{Progress, Stats};
use crate::write::{FetchedDoc, Writer};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct RepOptions {
    pub continuous: bool,
    /// Replicate only winning revisions (CouchDB's winning_revs_only;
    /// changes feed uses style=main_only).
    pub winning_revs_only: bool,
    pub create_target: bool,
    pub since: Option<String>,
    pub filter: Filter,
    pub fetch_concurrency: usize,
    pub write_concurrency: usize,
    pub att_concurrency: usize,
    pub batch_size: usize,
    pub max_batch_bytes: usize,
    pub inline_att_threshold: u64,
    pub checkpoint_interval: Duration,
    pub use_checkpoints: bool,
    pub use_bulk_get: bool,
    pub continue_on_error: bool,
    pub changes_limit: usize,
    pub stats_interval: Duration,
    /// Emit periodic progress log lines (CLI mode).
    pub progress: bool,
}

/// Run one replication job to completion (or until `cancel` fires). Returns
/// 0 on clean completion, 2 when it completed but some docs were skipped.
pub async fn replicate(
    source: Endpoint,
    target: Endpoint,
    opts: RepOptions,
    stats: Arc<Stats>,
    cancel: CancellationToken,
) -> Result<i32> {
    let started = Instant::now();
    let src_info = source.db_info().await?;
    let _tgt_info = target.ensure_db(opts.create_target).await?;
    info!(
        "replicating {} -> {} ({} docs at source, update_seq {})",
        source.db_name(),
        target.db_name(),
        src_info.doc_count,
        crate::util::seq_to_string(&src_info.update_seq)
            .chars()
            .take(24)
            .collect::<String>()
    );

    let rep_id = replication_id(
        &source.normalized_url(),
        &target.normalized_url(),
        &opts.filter,
        opts.continuous,
        opts.winning_revs_only,
    );
    info!("replication id: {rep_id}");

    // Selector filtering always runs natively in couch-repl, applied to each
    // fetched leaf revision after the parallel _bulk_get: the source never
    // evaluates a filter (no couchjs, no per-row mango matching), and the
    // changes feed stays lean.
    let selector = opts
        .filter
        .selector
        .as_ref()
        .map(|s| {
            couch_mango::Selector::compile(s)
                .map_err(|e| Error::Protocol(format!("invalid selector: {e}")))
        })
        .transpose()?;
    if selector.is_some() {
        info!("selector filter: evaluating natively in couch-repl");
    }

    let ledger = Arc::new(SeqLedger::new());

    let (checkpointer, ckpt_seq) = if opts.use_checkpoints {
        let (c, seq) = Checkpointer::load(
            source.clone(),
            target.clone(),
            checkpoint_doc_id(&rep_id),
            ledger.clone(),
            stats.clone(),
            opts.checkpoint_interval,
        )
        .await?;
        (Some(Arc::new(c)), seq)
    } else {
        (None, "0".to_string())
    };
    let start_seq = opts.since.clone().unwrap_or(ckpt_seq);
    info!("starting from source seq {start_seq}");

    let background = CancellationToken::new(); // checkpoint + progress tasks

    let (changes_tx, changes_rx) = mpsc::channel::<TaggedChange>(8192);
    let (missing_tx, missing_rx) = mpsc::channel::<MissingDoc>(4096);
    let (fetched_tx, fetched_rx) = mpsc::channel::<FetchedDoc>(2048);
    let (att_tx, att_rx) = mpsc::channel::<AttachmentDoc>(256);

    let reader = ChangesReader {
        source: source.clone(),
        filter: opts.filter.clone(),
        ledger: ledger.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
        page_size: opts.changes_limit,
        winning_revs_only: opts.winning_revs_only,
    };
    let continuous = opts.continuous;
    let reader_start = start_seq.clone();
    let reader_handle = tokio::spawn(async move {
        if continuous {
            reader.run_continuous(reader_start, changes_tx).await
        } else {
            reader.run_normal(reader_start, changes_tx).await
        }
    });

    let revs_diff = Arc::new(RevsDiff {
        target: target.clone(),
        ledger: ledger.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
        group_size: opts.batch_size.max(64),
        concurrency: 4,
    });
    let revs_diff_handle = tokio::spawn(revs_diff.run(changes_rx, missing_tx));

    let fetcher = Arc::new(Fetcher {
        source: source.clone(),
        ledger: ledger.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
        batch_size: opts.batch_size,
        concurrency: opts.fetch_concurrency,
        use_bulk_get: AtomicBool::new(opts.use_bulk_get),
        continue_on_error: opts.continue_on_error,
        selector,
    });
    let fetcher_handle = tokio::spawn(fetcher.run(missing_rx, fetched_tx.clone(), att_tx));

    let att_lane = Arc::new(AttLane {
        source: source.clone(),
        target: target.clone(),
        ledger: ledger.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
        concurrency: opts.att_concurrency,
        inline_threshold: opts.inline_att_threshold,
        continue_on_error: opts.continue_on_error,
    });
    let att_handle = tokio::spawn(att_lane.run(att_rx, fetched_tx));

    let writer = Arc::new(Writer {
        target: target.clone(),
        ledger: ledger.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
        batch_docs: opts.batch_size,
        batch_bytes: opts.max_batch_bytes,
        concurrency: opts.write_concurrency,
        continue_on_error: opts.continue_on_error,
    });
    let writer_handle = tokio::spawn(writer.run(fetched_rx));

    if let Some(ckpt) = checkpointer.clone() {
        let bg = background.clone();
        tokio::spawn(async move { ckpt.run(bg).await });
    }
    if opts.progress {
        let stats = stats.clone();
        let ledger = ledger.clone();
        let bg = background.clone();
        let interval = opts.stats_interval;
        tokio::spawn(async move {
            let mut progress = Progress::new();
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = bg.cancelled() => return,
                    _ = tick.tick() => {
                        info!("{}", progress.line(&stats, ledger.in_flight(), &ledger.committable()));
                    }
                }
            }
        });
    }

    // Stages finish in pipeline order once the reader closes its channel.
    let mut first_err: Option<Error> = None;
    for (name, handle) in [
        ("changes reader", reader_handle),
        ("revs_diff", revs_diff_handle),
        ("fetcher", fetcher_handle),
        ("attachment lane", att_handle),
        ("writer", writer_handle),
    ] {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("{name} failed: {e}");
                cancel.cancel();
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(join_err) => {
                cancel.cancel();
                if first_err.is_none() {
                    first_err = Some(Error::Protocol(format!("{name} panicked: {join_err}")));
                }
            }
        }
    }
    background.cancel();

    if let Some(ckpt) = &checkpointer {
        if let Err(e) = ckpt.checkpoint().await {
            warn!("final checkpoint failed: {e}");
        }
    }

    info!("{}", stats.summary(started.elapsed()));

    if let Some(e) = first_err {
        return Err(e);
    }
    let skipped = stats.get(&stats.doc_write_failures);
    if skipped > 0 {
        warn!("{skipped} documents were skipped");
        return Ok(2);
    }
    Ok(0)
}
