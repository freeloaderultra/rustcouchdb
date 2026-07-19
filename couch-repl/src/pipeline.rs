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
    /// Gzip request bodies to servers that are known to inflate them
    /// (welcome-probe negotiated; unknown/old servers keep identity bodies).
    pub request_gzip: bool,
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
    if opts.request_gzip {
        tokio::join!(source.detect_gzip_support(), target.detect_gzip_support());
    }
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
        opts.winning_revs_only,
    );
    info!("replication id: {}", crate::ids::task_id(&rep_id, opts.continuous));

    // Selector filtering runs SERVER-SIDE via the source's proto-aware
    // _changes?filter=_selector (see ChangesReader): the source renders each
    // proto doc's domain view and matches the Mango selector — identical to
    // _find — so db.* fields inside the proto body are reachable. couch-repl
    // no longer matches client-side (it only ever saw the raw $pb envelope,
    // which has no top-level db, so every proto doc was wrongly filtered out).
    if opts.filter.selector.is_some() {
        // Validate early; the source compiles and applies it authoritatively.
        couch_mango::Selector::compile(opts.filter.selector.as_ref().unwrap())
            .map_err(|e| Error::Protocol(format!("invalid selector: {e}")))?;
        info!("selector filter: evaluated server-side (proto-aware, like _find)");
    }

    let ledger = Arc::new(SeqLedger::new());
    ledger.attach_stats(stats.clone());

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
        // Filtering is server-side now; the fetcher never matches client-side.
        selector: None,
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
