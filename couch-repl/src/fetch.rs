use crate::attachments::AttachmentDoc;
use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::revs_diff::MissingDoc;
use crate::seq::SeqLedger;
use crate::stats::Stats;
use crate::util::batch_recv;
use crate::write::FetchedDoc;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// One (doc id, rev) pair that must be copied; carries its change's ordinal.
#[derive(Debug, Clone)]
struct FetchUnit {
    ord: u64,
    id: String,
    rev: String,
}

#[derive(Deserialize)]
struct BulkGetResp {
    results: Vec<BulkGetItem>,
}

#[derive(Deserialize)]
struct BulkGetItem {
    id: String,
    docs: Vec<BulkGetDoc>,
}

#[derive(Deserialize)]
struct BulkGetDoc {
    ok: Option<Box<RawValue>>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

pub struct Fetcher {
    pub source: Endpoint,
    pub ledger: Arc<SeqLedger>,
    pub stats: Arc<Stats>,
    pub cancel: CancellationToken,
    pub batch_size: usize,
    pub concurrency: usize,
    pub use_bulk_get: AtomicBool,
    pub continue_on_error: bool,
    /// Native selector filtering: applied to every fetched leaf revision;
    /// non-matching revisions complete their ledger unit and are dropped.
    /// Filtering here (after the parallel _bulk_get) keeps the changes feed
    /// lean and spreads body reads over all fetch connections.
    pub selector: Option<crate::mango::Selector>,
}

impl Fetcher {
    pub async fn run(
        self: Arc<Self>,
        mut rx: Receiver<MissingDoc>,
        doc_tx: Sender<FetchedDoc>,
        att_tx: Sender<AttachmentDoc>,
    ) -> Result<()> {
        // Explode each missing change into per-rev fetch units, telling the
        // ledger how many completions the change now needs.
        let (unit_tx, mut unit_rx) = tokio::sync::mpsc::channel::<FetchUnit>(8192);
        let ledger = self.ledger.clone();
        let exploder = tokio::spawn(async move {
            while let Some(md) = rx.recv().await {
                ledger.expand(md.ord, md.revs.len() as u32);
                for rev in md.revs {
                    let unit = FetchUnit {
                        ord: md.ord,
                        id: md.id.clone(),
                        rev,
                    };
                    if unit_tx.send(unit).await.is_err() {
                        return;
                    }
                }
            }
        });

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<FetchUnit>>(8);
        let batch_size = self.batch_size;
        let batcher = tokio::spawn(async move {
            while let Some(batch) =
                batch_recv(&mut unit_rx, batch_size, Duration::from_millis(200)).await
            {
                if batch_tx.send(batch).await.is_err() {
                    break;
                }
            }
        });

        let first_err: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        {
            let stage = self.clone();
            let first_err = first_err.clone();
            ReceiverStream::new(batch_rx)
                .for_each_concurrent(self.concurrency, |batch| {
                    let stage = stage.clone();
                    let doc_tx = doc_tx.clone();
                    let att_tx = att_tx.clone();
                    let first_err = first_err.clone();
                    async move {
                        if stage.cancel.is_cancelled() {
                            return;
                        }
                        if let Err(e) = stage.fetch_batch(batch, &doc_tx, &att_tx).await {
                            let mut g = first_err.lock().unwrap();
                            if g.is_none() {
                                *g = Some(e);
                            }
                            stage.cancel.cancel();
                        }
                    }
                })
                .await;
        }
        let _ = exploder.await;
        let _ = batcher.await;
        let err = first_err.lock().unwrap().take();
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn fetch_batch(
        &self,
        batch: Vec<FetchUnit>,
        doc_tx: &Sender<FetchedDoc>,
        att_tx: &Sender<AttachmentDoc>,
    ) -> Result<()> {
        if self.use_bulk_get.load(Ordering::Relaxed) {
            match self.bulk_get(&batch).await {
                Ok(results) => {
                    for (unit, doc) in results {
                        match doc {
                            Some(raw) => self.route(unit, raw, doc_tx, att_tx).await?,
                            None => self.fetch_single(unit, doc_tx, att_tx).await?,
                        }
                    }
                    return Ok(());
                }
                Err(e) if matches!(e.status(), Some(404) | Some(405) | Some(400)) => {
                    warn!("source does not support _bulk_get ({e}); falling back to per-doc fetches");
                    self.use_bulk_get.store(false, Ordering::Relaxed);
                }
                Err(e) => return Err(e),
            }
        }
        for unit in batch {
            self.fetch_single(unit, doc_tx, att_tx).await?;
        }
        Ok(())
    }

    /// Returns the raw doc for each unit, or None when that unit needs the
    /// per-doc fallback.
    async fn bulk_get(
        &self,
        batch: &[FetchUnit],
    ) -> Result<Vec<(FetchUnit, Option<Box<RawValue>>)>> {
        let docs: Vec<serde_json::Value> = batch
            .iter()
            .map(|u| serde_json::json!({"id": u.id, "rev": u.rev}))
            .collect();
        let body = serde_json::to_vec(&serde_json::json!({ "docs": docs })).unwrap();
        let q = [
            ("revs", "true".to_string()),
            ("latest", "true".to_string()),
        ];
        let raw: bytes::Bytes = {
            // post_raw parses JSON via serde into T; we need the raw bytes to
            // keep RawValue slices, so issue the request manually with retry.
            let url = self.source.url(&["_bulk_get"]);
            crate::retry::with_retry(&self.source.retry, "POST source/_bulk_get", || async {
                let rb = self
                    .source
                    .request(reqwest::Method::POST, url.clone())
                    .query(&q)
                    .header(reqwest::header::ACCEPT, "application/json")
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body.clone())
                    .timeout(self.source.request_timeout().max(Duration::from_secs(120)));
                let resp = self.source.send(rb).await?;
                resp.bytes().await.map_err(Error::Net)
            })
            .await?
        };
        let parsed: BulkGetResp = serde_json::from_slice(&raw)
            .map_err(|e| Error::Protocol(format!("bad _bulk_get response: {e}")))?;
        if parsed.results.len() != batch.len() {
            return Err(Error::Protocol(format!(
                "_bulk_get returned {} results for {} requests",
                parsed.results.len(),
                batch.len()
            )));
        }
        let mut out = Vec::with_capacity(batch.len());
        for (unit, item) in batch.iter().zip(parsed.results) {
            if item.id != unit.id {
                return Err(Error::Protocol(format!(
                    "_bulk_get result order mismatch: expected {}, got {}",
                    unit.id, item.id
                )));
            }
            let mut docs = item.docs.into_iter();
            match docs.next() {
                Some(BulkGetDoc { ok: Some(raw), .. }) => out.push((unit.clone(), Some(raw))),
                Some(BulkGetDoc { error, .. }) => {
                    debug!(
                        "bulk_get miss for {}@{}: {:?}; will fetch individually",
                        unit.id, unit.rev, error
                    );
                    out.push((unit.clone(), None));
                }
                None => out.push((unit.clone(), None)),
            }
        }
        Ok(out)
    }

    async fn fetch_single(
        &self,
        unit: FetchUnit,
        doc_tx: &Sender<FetchedDoc>,
        att_tx: &Sender<AttachmentDoc>,
    ) -> Result<()> {
        let q = [
            ("rev", unit.rev.clone()),
            ("revs", "true".to_string()),
        ];
        match self.source.get_bytes(&[&unit.id], &q).await {
            Ok(bytes) => {
                let raw: Box<RawValue> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Protocol(format!("bad doc body for {}: {e}", unit.id)))?;
                self.route(unit, raw, doc_tx, att_tx).await
            }
            Err(e) => self.doc_failed(unit, e),
        }
    }

    fn doc_failed(&self, unit: FetchUnit, e: Error) -> Result<()> {
        if self.continue_on_error {
            warn!("skipping unreadable doc {}@{}: {e}", unit.id, unit.rev);
            self.stats.add(&self.stats.doc_write_failures, 1);
            self.ledger.complete(unit.ord);
            Ok(())
        } else {
            Err(Error::Doc {
                id: unit.id,
                reason: format!("cannot read rev {}: {e}", unit.rev),
            })
        }
    }

    async fn route(
        &self,
        unit: FetchUnit,
        raw: Box<RawValue>,
        doc_tx: &Sender<FetchedDoc>,
        att_tx: &Sender<AttachmentDoc>,
    ) -> Result<()> {
        self.stats.add(&self.stats.docs_read, 1);
        let s = raw.get();
        let has_atts = s.contains("\"_attachments\"");
        // Parse only when something needs to look inside the body; matching
        // documents still ride to the target as the raw slice.
        let val: Option<serde_json::Value> = if self.selector.is_some() || has_atts {
            Some(serde_json::from_str(s).map_err(|e| {
                Error::Protocol(format!("bad doc JSON for {}: {e}", unit.id))
            })?)
        } else {
            None
        };
        if let Some(sel) = &self.selector {
            if !sel.matches(val.as_ref().unwrap()) {
                self.stats.add(&self.stats.docs_filtered, 1);
                self.ledger.complete(unit.ord);
                return Ok(());
            }
        }
        if has_atts {
            let val = val.unwrap();
            let rev = val
                .get("_rev")
                .and_then(|r| r.as_str())
                .unwrap_or(&unit.rev)
                .to_string();
            let total: u64 = val
                .get("_attachments")
                .and_then(|a| a.as_object())
                .map(|atts| {
                    atts.values()
                        .map(|a| a.get("length").and_then(|l| l.as_u64()).unwrap_or(u64::MAX / 1024))
                        .sum()
                })
                .unwrap_or(0);
            let msg = AttachmentDoc {
                ord: unit.ord,
                id: unit.id,
                rev,
                total_len: total,
            };
            att_tx.send(msg).await.map_err(|_| Error::Canceled)
        } else {
            let msg = FetchedDoc {
                ord: unit.ord,
                id: unit.id,
                rev: unit.rev,
                body: raw,
            };
            doc_tx.send(msg).await.map_err(|_| Error::Canceled)
        }
    }
}
