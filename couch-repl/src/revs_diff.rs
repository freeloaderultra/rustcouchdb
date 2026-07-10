use crate::changes::TaggedChange;
use crate::client::Endpoint;
use crate::error::Result;
use crate::seq::SeqLedger;
use crate::stats::Stats;
use crate::util::batch_recv;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// A change whose listed revs are (partially) missing on the target.
#[derive(Debug)]
pub struct MissingDoc {
    pub ord: u64,
    pub id: String,
    pub revs: Vec<String>,
}

#[derive(Deserialize)]
struct MissingInfo {
    #[serde(default)]
    missing: Vec<String>,
}

pub struct RevsDiff {
    pub target: Endpoint,
    pub ledger: Arc<SeqLedger>,
    pub stats: Arc<Stats>,
    pub cancel: CancellationToken,
    pub group_size: usize,
    pub concurrency: usize,
}

impl RevsDiff {
    pub async fn run(
        self: Arc<Self>,
        mut rx: Receiver<TaggedChange>,
        tx: Sender<MissingDoc>,
    ) -> Result<()> {
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<TaggedChange>>(4);
        let cancel = self.cancel.clone();
        let group = self.group_size;
        let batcher = tokio::spawn(async move {
            while let Some(batch) = batch_recv(&mut rx, group, Duration::from_millis(500)).await {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    r = batch_tx.send(batch) => { if r.is_err() { break } }
                }
            }
        });

        let first_err: Arc<Mutex<Option<crate::error::Error>>> = Arc::new(Mutex::new(None));
        {
            let stage = self.clone();
            let first_err = first_err.clone();
            ReceiverStream::new(batch_rx)
                .for_each_concurrent(self.concurrency, |batch| {
                    let stage = stage.clone();
                    let tx = tx.clone();
                    let first_err = first_err.clone();
                    async move {
                        if stage.cancel.is_cancelled() {
                            return;
                        }
                        if let Err(e) = stage.check_batch(batch, &tx).await {
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

    async fn check_batch(&self, batch: Vec<TaggedChange>, tx: &Sender<MissingDoc>) -> Result<()> {
        // Union of revs per doc id: the same doc may appear more than once in
        // a continuous feed batch.
        let mut request: HashMap<&str, Vec<String>> = HashMap::with_capacity(batch.len());
        let mut checked = 0u64;
        for tc in &batch {
            let entry = request.entry(tc.change.id.as_str()).or_default();
            for rev in &tc.change.revs {
                if !entry.contains(rev) {
                    entry.push(rev.clone());
                    checked += 1;
                }
            }
        }
        self.stats.add(&self.stats.missing_checked, checked);

        let resp: HashMap<String, MissingInfo> = self
            .target
            .post_json(&["_revs_diff"], &[], &request)
            .await?;

        let mut found = 0u64;
        for tc in batch {
            let missing: Vec<String> = match resp.get(&tc.change.id) {
                None => Vec::new(),
                Some(info) => tc
                    .change
                    .revs
                    .iter()
                    .filter(|r| info.missing.contains(r))
                    .cloned()
                    .collect(),
            };
            if missing.is_empty() {
                // Everything already on the target: this change is done.
                self.ledger.complete(tc.ord);
            } else {
                found += missing.len() as u64;
                let msg = MissingDoc {
                    ord: tc.ord,
                    id: tc.change.id,
                    revs: missing,
                };
                if tx.send(msg).await.is_err() {
                    return Err(crate::error::Error::Canceled);
                }
            }
        }
        self.stats.add(&self.stats.missing_found, found);
        Ok(())
    }
}
