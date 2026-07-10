use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::seq::SeqLedger;
use crate::stats::Stats;
use crate::util::batch_recv_weighted;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::value::RawValue;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// A document body ready for the target, passed through verbatim from the
/// source response (no re-serialization).
#[derive(Debug)]
pub struct FetchedDoc {
    pub ord: u64,
    pub id: String,
    pub rev: String,
    pub body: Box<RawValue>,
}

pub struct Writer {
    pub target: Endpoint,
    pub ledger: Arc<SeqLedger>,
    pub stats: Arc<Stats>,
    pub cancel: CancellationToken,
    pub batch_docs: usize,
    pub batch_bytes: usize,
    pub concurrency: usize,
    pub continue_on_error: bool,
}

#[derive(serde::Deserialize)]
struct WriteError {
    id: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

impl Writer {
    pub async fn run(self: Arc<Self>, mut rx: Receiver<FetchedDoc>) -> Result<()> {
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<FetchedDoc>>(4);
        let max_docs = self.batch_docs;
        let max_bytes = self.batch_bytes;
        let batcher = tokio::spawn(async move {
            while let Some(batch) = batch_recv_weighted(
                &mut rx,
                max_docs,
                max_bytes,
                |d| d.body.get().len(),
                Duration::from_secs(1),
            )
            .await
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
                    let first_err = first_err.clone();
                    async move {
                        if stage.cancel.is_cancelled() {
                            // Do not write after cancel, but keep draining so
                            // upstream senders are not wedged.
                            return;
                        }
                        if let Err(e) = stage.flush(batch).await {
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
        let _ = batcher.await;
        let err = first_err.lock().unwrap().take();
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn flush(&self, batch: Vec<FetchedDoc>) -> Result<()> {
        let mut body = Vec::with_capacity(
            batch.iter().map(|d| d.body.get().len() + 1).sum::<usize>() + 32,
        );
        body.extend_from_slice(b"{\"new_edits\":false,\"docs\":[");
        for (i, doc) in batch.iter().enumerate() {
            if i > 0 {
                body.push(b',');
            }
            body.extend_from_slice(doc.body.get().as_bytes());
        }
        body.extend_from_slice(b"]}");
        let payload_len = body.len() as u64;

        let resp: Vec<serde_json::Value> = self
            .target
            .post_raw(
                &["_bulk_docs"],
                &[],
                Bytes::from(body),
                self.target.request_timeout().max(Duration::from_secs(120)),
            )
            .await?;

        // With new_edits:false CouchDB only reports failures.
        let mut failed_ids: HashSet<String> = HashSet::new();
        for entry in resp {
            if entry.get("error").is_some() {
                if let Ok(we) = serde_json::from_value::<WriteError>(entry.clone()) {
                    warn!(
                        "target rejected doc {}: {} ({})",
                        we.id,
                        we.error.as_deref().unwrap_or("?"),
                        we.reason.as_deref().unwrap_or("")
                    );
                    failed_ids.insert(we.id);
                }
            }
        }

        let mut written = 0u64;
        for doc in &batch {
            if failed_ids.contains(&doc.id) {
                if !self.continue_on_error {
                    return Err(Error::Doc {
                        id: doc.id.clone(),
                        reason: format!("rev {} rejected by target _bulk_docs", doc.rev),
                    });
                }
                self.stats.add(&self.stats.doc_write_failures, 1);
            } else {
                written += 1;
            }
            self.ledger.complete(doc.ord);
        }
        self.stats.add(&self.stats.docs_written, written);
        self.stats.add(&self.stats.bytes_written, payload_len);
        Ok(())
    }
}
