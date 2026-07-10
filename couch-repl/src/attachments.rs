use crate::client::Endpoint;
use crate::error::{Error, Result};
use crate::retry::with_retry;
use crate::seq::SeqLedger;
use crate::stats::Stats;
use crate::write::FetchedDoc;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::value::RawValue;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// A document rev that carries attachments and needs special handling.
#[derive(Debug)]
pub struct AttachmentDoc {
    pub ord: u64,
    pub id: String,
    pub rev: String,
    pub total_len: u64,
}

pub struct AttLane {
    pub source: Endpoint,
    pub target: Endpoint,
    pub ledger: Arc<SeqLedger>,
    pub stats: Arc<Stats>,
    pub cancel: CancellationToken,
    pub concurrency: usize,
    pub inline_threshold: u64,
    pub continue_on_error: bool,
}

impl AttLane {
    pub async fn run(
        self: Arc<Self>,
        rx: Receiver<AttachmentDoc>,
        doc_tx: Sender<FetchedDoc>,
    ) -> Result<()> {
        let first_err: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        {
            let stage = self.clone();
            let first_err = first_err.clone();
            ReceiverStream::new(rx)
                .for_each_concurrent(self.concurrency, |ad| {
                    let stage = stage.clone();
                    let doc_tx = doc_tx.clone();
                    let first_err = first_err.clone();
                    async move {
                        if stage.cancel.is_cancelled() {
                            return;
                        }
                        if let Err(e) = stage.process(ad, &doc_tx).await {
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
        let err = first_err.lock().unwrap().take();
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn process(&self, ad: AttachmentDoc, doc_tx: &Sender<FetchedDoc>) -> Result<()> {
        let result = if ad.total_len <= self.inline_threshold {
            self.inline(&ad, doc_tx).await
        } else {
            self.stream_copy(&ad).await
        };
        match result {
            Ok(()) => Ok(()),
            Err(Error::Canceled) => Err(Error::Canceled),
            Err(e) if self.continue_on_error => {
                warn!("skipping attachment doc {}@{}: {e}", ad.id, ad.rev);
                self.stats.add(&self.stats.doc_write_failures, 1);
                self.ledger.complete(ad.ord);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Small attachments: refetch the doc with base64 attachment data inline
    /// and let it ride the ordinary _bulk_docs path.
    async fn inline(&self, ad: &AttachmentDoc, doc_tx: &Sender<FetchedDoc>) -> Result<()> {
        let q = [
            ("rev", ad.rev.clone()),
            ("revs", "true".to_string()),
            ("attachments", "true".to_string()),
        ];
        let bytes = self.source.get_bytes(&[&ad.id], &q).await?;
        let raw: Box<RawValue> = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Protocol(format!("bad doc body for {}: {e}", ad.id)))?;
        let msg = FetchedDoc {
            ord: ad.ord,
            id: ad.id.clone(),
            rev: ad.rev.clone(),
            body: raw,
        };
        doc_tx.send(msg).await.map_err(|_| Error::Canceled)
    }

    /// Large attachments: stream source -> target without buffering any
    /// attachment fully in memory, as one multipart/related PUT.
    async fn stream_copy(&self, ad: &AttachmentDoc) -> Result<()> {
        with_retry(
            &self.target.retry,
            &format!("stream doc {}@{}", ad.id, ad.rev),
            || self.stream_attempt(ad),
        )
        .await?;
        self.stats.add(&self.stats.docs_written, 1);
        self.stats.add(&self.stats.bytes_written, ad.total_len);
        self.ledger.complete(ad.ord);
        Ok(())
    }

    async fn stream_attempt(&self, ad: &AttachmentDoc) -> Result<()> {
        // 1. Fetch the doc with attachment stubs (+ encoding info so we know
        //    which digests would be invalidated by re-encoding).
        let q = [
            ("rev", ad.rev.clone()),
            ("revs", "true".to_string()),
            ("att_encoding_info", "true".to_string()),
        ];
        let stub_bytes = self.source.get_bytes(&[&ad.id], &q).await?;
        let mut doc: serde_json::Value = serde_json::from_slice(&stub_bytes)
            .map_err(|e| Error::Protocol(format!("bad doc body for {}: {e}", ad.id)))?;

        // 2. Rewrite attachment stubs into multipart "follows" references.
        let mut atts: Vec<(String, u64)> = Vec::new();
        if let Some(map) = doc
            .get_mut("_attachments")
            .and_then(|a| a.as_object_mut())
        {
            for (name, att) in map.iter_mut() {
                let obj = att
                    .as_object_mut()
                    .ok_or_else(|| Error::Protocol(format!("bad attachment stub in {}", ad.id)))?;
                let length = obj.get("length").and_then(|l| l.as_u64()).ok_or_else(|| {
                    Error::Protocol(format!("attachment {name} in {} has no length", ad.id))
                })?;
                obj.remove("stub");
                obj.insert("follows".into(), serde_json::Value::Bool(true));
                // We always send identity bytes; drop metadata describing the
                // source's on-disk encoding, and the digest with it (it hashes
                // the encoded form, which the target will never see).
                if obj.remove("encoding").is_some() {
                    obj.remove("encoded_length");
                    obj.remove("digest");
                }
                atts.push((name.clone(), length));
            }
        }
        if atts.is_empty() {
            // A false-positive routing (e.g. "_attachments" inside a string
            // value): write it as a plain doc.
            let raw: Box<RawValue> = serde_json::from_slice(&stub_bytes).unwrap();
            return self.put_plain(ad, raw).await;
        }

        // serde_json's map serializes in sorted key order; iterate the same
        // way so multipart parts line up with the JSON.
        atts.sort_by(|a, b| a.0.cmp(&b.0));
        let json_bytes = serde_json::to_vec(&doc).unwrap();

        // 3. Assemble the multipart/related body as a stream:
        //    --B\r\ncontent-type: application/json\r\n\r\n{doc}
        //    then per attachment: \r\n--B\r\n\r\n<bytes>
        //    then: \r\n--B--
        // (Exactly the framing couch_doc:len_doc_to_multi_part_stream emits.)
        let boundary = uuid::Uuid::new_v4().simple().to_string();
        let (body_tx, body_rx) =
            tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(8);

        let source = self.source.clone();
        let doc_id = ad.id.clone();
        let rev = ad.rev.clone();
        let b = boundary.clone();
        let att_list = atts.clone();
        let pump = tokio::spawn(async move {
            let send = |data: Bytes| {
                let tx = body_tx.clone();
                async move { tx.send(Ok(data)).await.is_ok() }
            };
            let preamble = format!("--{b}\r\ncontent-type: application/json\r\n\r\n");
            if !send(Bytes::from(preamble)).await {
                return;
            }
            if !send(Bytes::from(json_bytes)).await {
                return;
            }
            for (name, _len) in att_list {
                if !send(Bytes::from(format!("\r\n--{b}\r\n\r\n"))).await {
                    return;
                }
                let mut segs: Vec<&str> = vec![doc_id.as_str()];
                segs.extend(name.split('/'));
                let url = source.url(&segs);
                let rb = source
                    .request(reqwest::Method::GET, url)
                    .query(&[("rev", rev.clone())]);
                let resp = match source.send(rb).await {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = body_tx.send(Err(std::io::Error::other(e.to_string()))).await;
                        return;
                    }
                };
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(c) => {
                            if body_tx.send(Ok(c)).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ =
                                body_tx.send(Err(std::io::Error::other(e.to_string()))).await;
                            return;
                        }
                    }
                }
            }
            let _ = body_tx.send(Ok(Bytes::from(format!("\r\n--{b}--")))).await;
        });

        // 4. PUT it. No overall timeout: attachment size is unbounded.
        let url = self.target.url(&[&ad.id]);
        let rb = self
            .target
            .request(reqwest::Method::PUT, url)
            .query(&[("new_edits", "false")])
            .header(
                reqwest::header::CONTENT_TYPE,
                format!("multipart/related; boundary=\"{boundary}\""),
            )
            .body(reqwest::Body::wrap_stream(ReceiverStream::new(body_rx)));
        let result = self.target.send(rb).await;
        pump.abort();
        let resp = result?;
        let _ = resp.bytes().await;
        debug!("streamed doc {}@{} ({} bytes of attachments)", ad.id, ad.rev, ad.total_len);
        Ok(())
    }

    async fn put_plain(&self, ad: &AttachmentDoc, raw: Box<RawValue>) -> Result<()> {
        let url = self.target.url(&[&ad.id]);
        let rb = self
            .target
            .request(reqwest::Method::PUT, url)
            .query(&[("new_edits", "false")])
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(raw.get().to_string())
            .timeout(self.target.request_timeout());
        let resp = self.target.send(rb).await?;
        let _ = resp.bytes().await;
        Ok(())
    }
}
