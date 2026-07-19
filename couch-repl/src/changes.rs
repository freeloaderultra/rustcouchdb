use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::ids::Filter;
use crate::seq::SeqLedger;
use crate::stats::Stats;
use crate::util::seq_to_string;
use bytes::BytesMut;
use futures::StreamExt;
use reqwest::Method;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct Change {
    pub seq: String,
    pub id: String,
    pub revs: Vec<String>,
}

#[derive(Debug)]
pub struct TaggedChange {
    pub ord: u64,
    pub change: Change,
}

#[derive(Deserialize)]
struct ChangeRow {
    seq: serde_json::Value,
    id: String,
    changes: Vec<RevEntry>,
}

#[derive(Deserialize)]
struct RevEntry {
    rev: String,
}

#[derive(Deserialize)]
struct ChangesPage {
    results: Vec<ChangeRow>,
    last_seq: serde_json::Value,
    pending: Option<i64>,
}

pub struct ChangesReader {
    pub source: Endpoint,
    pub filter: Filter,
    pub ledger: Arc<SeqLedger>,
    pub stats: Arc<Stats>,
    pub cancel: CancellationToken,
    pub page_size: usize,
    /// CouchDB's winning_revs_only: replicate only each doc's winning
    /// branch (style=main_only) instead of all leaf revisions.
    pub winning_revs_only: bool,
}

impl ChangesReader {
    fn base_query(&self, since: &str) -> Vec<(&'static str, String)> {
        let style = if self.winning_revs_only { "main_only" } else { "all_docs" };
        let mut q = vec![
            ("style", style.to_string()),
            ("since", since.to_string()),
        ];
        // Proto-aware selector filtering runs SERVER-SIDE, the same way _find
        // does: the source renders each proto doc's domain view (present_doc)
        // and matches the Mango selector, so db.* fields living inside the
        // proto body are reachable — a client-side match against the raw $pb
        // envelope never sees them. _doc_ids is a plain id-list restriction.
        // Selector takes precedence when both are set.
        if self.filter.selector.is_some() {
            q.push(("filter", "_selector".into()));
        } else if self.filter.doc_ids.is_some() {
            q.push(("filter", "_doc_ids".into()));
        }
        q
    }

    fn filter_body(&self) -> Option<serde_json::Value> {
        if let Some(sel) = &self.filter.selector {
            return Some(serde_json::json!({ "selector": sel }));
        }
        self.filter
            .doc_ids
            .as_ref()
            .map(|ids| serde_json::json!({ "doc_ids": ids }))
    }

    async fn fetch_page(&self, since: &str) -> Result<ChangesPage> {
        let mut q = self.base_query(since);
        q.push(("feed", "normal".into()));
        q.push(("limit", self.page_size.to_string()));
        if let Some(body) = self.filter_body() {
            self.source.post_json(&["_changes"], &q, &body).await
        } else {
            self.source.get_json(&["_changes"], &q).await
        }
    }

    async fn emit(&self, tx: &Sender<TaggedChange>, row: ChangeRow) -> Result<()> {
        let change = Change {
            seq: seq_to_string(&row.seq),
            id: row.id,
            revs: row.changes.into_iter().map(|r| r.rev).collect(),
        };
        let ord = self.ledger.register(change.seq.clone());
        self.stats.add(&self.stats.changes_read, 1);
        tx.send(TaggedChange { ord, change })
            .await
            .map_err(|_| Error::Canceled)
    }

    /// One-shot mode: page through the changes feed until `pending == 0`.
    pub async fn run_normal(&self, mut since: String, tx: Sender<TaggedChange>) -> Result<()> {
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            let page = tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                p = self.fetch_page(&since) => p?,
            };
            let n = page.results.len();
            for row in page.results {
                tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    r = self.emit(&tx, row) => r?,
                }
            }
            let last_seq = seq_to_string(&page.last_seq);
            // Barrier: once everything up to here is written, the checkpoint
            // may advance all the way to this page's last_seq.
            self.ledger.register_done(last_seq.clone());
            debug!("changes page: {n} rows, last_seq={last_seq}");
            if page.pending == Some(0) || n < self.page_size {
                info!("changes feed complete at seq {last_seq}");
                return Ok(());
            }
            since = last_seq;
        }
    }

    /// Continuous mode: stream until canceled, reconnecting on errors.
    pub async fn run_continuous(&self, since: String, tx: Sender<TaggedChange>) -> Result<()> {
        let mut since = since;
        let mut failures: u32 = 0;
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            match self.stream_once(&mut since, &tx).await {
                Ok(()) => return Ok(()), // canceled
                Err(Error::Canceled) => return Ok(()),
                Err(e) if e.retryable() || e.status().is_some() => {
                    crate::metrics::bump(&crate::metrics::CHANGES_READ_FAILURES);
                    failures += 1;
                    let delay = Duration::from_millis(500)
                        .saturating_mul(2u32.saturating_pow(failures.min(6)));
                    warn!("continuous feed dropped ({e}); reconnecting in {delay:?}");
                    tokio::select! {
                        _ = self.cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(delay.min(Duration::from_secs(30))) => {}
                    }
                }
                Err(e) => {
                    crate::metrics::bump(&crate::metrics::CHANGES_READ_FAILURES);
                    return Err(e);
                }
            }
        }
    }

    async fn stream_once(&self, since: &mut String, tx: &Sender<TaggedChange>) -> Result<()> {
        let mut q = self.base_query(since);
        q.push(("feed", "continuous".into()));
        q.push(("heartbeat", "10000".into()));
        let url = self.source.url(&["_changes"]);
        let rb = if let Some(body) = self.filter_body() {
            self.source.request(Method::POST, url).query(&q).json(&body)
        } else {
            self.source.request(Method::GET, url).query(&q)
        };
        // No overall timeout: this connection is expected to live forever.
        let resp = self.source.send(rb).await?;
        let mut stream = resp.bytes_stream();
        let mut buf = BytesMut::new();
        loop {
            let chunk = tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                // Heartbeats arrive every 10s; 45s of silence means the
                // connection is dead even if TCP hasn't noticed.
                c = tokio::time::timeout(Duration::from_secs(45), stream.next()) => match c {
                    Err(_) => return Err(Error::Protocol("continuous feed stalled".into())),
                    Ok(None) => return Err(Error::Protocol("continuous feed closed by server".into())),
                    Ok(Some(item)) => item.map_err(Error::Net)?,
                }
            };
            buf.extend_from_slice(&chunk);
            while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                let line = buf.split_to(pos + 1);
                let line = &line[..line.len() - 1];
                if line.is_empty() || line == b"\r" {
                    continue; // heartbeat
                }
                if let Ok(row) = serde_json::from_slice::<ChangeRow>(line) {
                    let seq = seq_to_string(&row.seq);
                    self.emit(tx, row).await?;
                    *since = seq;
                } else if let Ok(ls) = serde_json::from_slice::<LastSeqLine>(line) {
                    let seq = seq_to_string(&ls.last_seq);
                    self.ledger.register_done(seq.clone());
                    *since = seq;
                } else {
                    warn!(
                        "unparseable changes line: {}",
                        String::from_utf8_lossy(&line[..line.len().min(200)])
                    );
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct LastSeqLine {
    last_seq: serde_json::Value,
}
