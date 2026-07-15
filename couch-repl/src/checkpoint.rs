use crate::client::Endpoint;
use crate::error::Result;
use crate::seq::SeqLedger;
use crate::stats::Stats;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HistoryEntry {
    pub session_id: String,
    pub start_time: String,
    pub end_time: String,
    pub start_last_seq: serde_json::Value,
    pub end_last_seq: serde_json::Value,
    pub recorded_seq: serde_json::Value,
    #[serde(default)]
    pub missing_checked: u64,
    #[serde(default)]
    pub missing_found: u64,
    #[serde(default)]
    pub docs_read: u64,
    #[serde(default)]
    pub docs_written: u64,
    #[serde(default)]
    pub doc_write_failures: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CheckpointDoc {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_rev", skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub source_last_seq: serde_json::Value,
    #[serde(default)]
    pub replicator: String,
    /// CouchDB writes the integer 4 here and consumers (nxguide's checkpoint
    /// reader among them) decode it as a number — the distinct "couch-repl"
    /// `replicator` field carries our provenance instead.
    #[serde(default)]
    pub replication_id_version: u32,
    #[serde(default)]
    pub history: Vec<HistoryEntry>,
}

const MAX_HISTORY: usize = 50;

struct CkptState {
    src_rev: Option<String>,
    tgt_rev: Option<String>,
    history: Vec<HistoryEntry>,
    last_written_seq: Option<String>,
    source_writable: bool,
}

pub struct Checkpointer {
    source: Endpoint,
    target: Endpoint,
    doc_id: String,
    session_id: String,
    start_time: String,
    start_seq: String,
    ledger: Arc<SeqLedger>,
    stats: Arc<Stats>,
    interval: Duration,
    state: Mutex<CkptState>,
}

impl Checkpointer {
    /// Read both checkpoint docs and work out where to resume.
    /// Returns (checkpointer, start_seq).
    pub async fn load(
        source: Endpoint,
        target: Endpoint,
        doc_id: String,
        ledger: Arc<SeqLedger>,
        stats: Arc<Stats>,
        interval: Duration,
    ) -> Result<(Checkpointer, String)> {
        let src_doc = get_local(&source, &doc_id).await?;
        let tgt_doc = get_local(&target, &doc_id).await?;

        let (start_seq, history) = match (&src_doc, &tgt_doc) {
            (Some(s), Some(t)) => match common_seq(s, t) {
                Some(seq) => {
                    info!("resuming from checkpointed seq {seq}");
                    (seq, s.history.clone())
                }
                None => {
                    warn!("checkpoint docs exist but share no session; starting over");
                    ("0".to_string(), Vec::new())
                }
            },
            (None, Some(t)) => {
                let seq = crate::util::seq_to_string(&t.source_last_seq);
                warn!(
                    "checkpoint missing on source but present on target; \
                     trusting target checkpoint (seq {seq})"
                );
                (seq, t.history.clone())
            }
            _ => ("0".to_string(), Vec::new()),
        };

        let state = CkptState {
            src_rev: src_doc.and_then(|d| d.rev),
            tgt_rev: tgt_doc.and_then(|d| d.rev),
            history,
            last_written_seq: None,
            source_writable: true,
        };
        let ckpt = Checkpointer {
            source,
            target,
            doc_id,
            session_id: uuid::Uuid::new_v4().simple().to_string(),
            start_time: httpdate::fmt_http_date(SystemTime::now()),
            start_seq: start_seq.clone(),
            ledger,
            stats,
            interval,
            state: Mutex::new(state),
        };
        Ok((ckpt, start_seq))
    }

    /// Periodic checkpoint loop; runs until canceled.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // discard the immediate first tick
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    if let Err(e) = self.checkpoint().await {
                        warn!("checkpoint failed (will retry next interval): {e}");
                    }
                }
            }
        }
    }

    /// Write a checkpoint if the committable seq advanced. Also used for the
    /// final checkpoint at shutdown.
    pub async fn checkpoint(&self) -> Result<()> {
        let Some(seq) = self.ledger.committable() else {
            return Ok(());
        };
        {
            let g = self.state.lock().unwrap();
            if g.last_written_seq.as_deref() == Some(seq.as_str()) {
                return Ok(());
            }
        }
        let entry = HistoryEntry {
            session_id: self.session_id.clone(),
            start_time: self.start_time.clone(),
            end_time: httpdate::fmt_http_date(SystemTime::now()),
            start_last_seq: serde_json::Value::String(self.start_seq.clone()),
            end_last_seq: serde_json::Value::String(seq.clone()),
            recorded_seq: serde_json::Value::String(seq.clone()),
            missing_checked: self.stats.get(&self.stats.missing_checked),
            missing_found: self.stats.get(&self.stats.missing_found),
            docs_read: self.stats.get(&self.stats.docs_read),
            docs_written: self.stats.get(&self.stats.docs_written),
            doc_write_failures: self.stats.get(&self.stats.doc_write_failures),
        };

        let (history, src_rev, tgt_rev, source_writable) = {
            let g = self.state.lock().unwrap();
            // Replace this session's previous entry rather than growing the
            // history on every interval.
            let mut h: Vec<HistoryEntry> = g
                .history
                .iter()
                .filter(|e| e.session_id != self.session_id)
                .cloned()
                .collect();
            h.insert(0, entry);
            h.truncate(MAX_HISTORY);
            (h, g.src_rev.clone(), g.tgt_rev.clone(), g.source_writable)
        };

        let doc = CheckpointDoc {
            id: self.doc_id.clone(),
            rev: None,
            session_id: self.session_id.clone(),
            source_last_seq: serde_json::Value::String(seq.clone()),
            replicator: "couch-repl".into(),
            replication_id_version: 4,
            history: history.clone(),
        };

        // Target first: a target checkpoint without a matching source one is
        // recovered via the target-side fallback in load().
        let new_tgt_rev = match put_local(&self.target, &doc, tgt_rev).await {
            Ok(rev) => rev,
            Err(e) => {
                crate::metrics::bump(&crate::metrics::CHECKPOINT_FAILURES);
                return Err(e);
            }
        };
        let new_src_rev = if source_writable {
            match put_local(&self.source, &doc, src_rev).await {
                Ok(rev) => Some(rev),
                Err(e) if matches!(e.status(), Some(401) | Some(403)) => {
                    warn!("source refuses checkpoint writes ({e}); continuing with target-only checkpoints");
                    let mut g = self.state.lock().unwrap();
                    g.source_writable = false;
                    None
                }
                Err(e) => {
                    crate::metrics::bump(&crate::metrics::CHECKPOINT_FAILURES);
                    return Err(e);
                }
            }
        } else {
            None
        };

        let mut g = self.state.lock().unwrap();
        g.tgt_rev = Some(new_tgt_rev);
        if let Some(rev) = new_src_rev {
            g.src_rev = Some(rev);
        }
        g.history = history;
        g.last_written_seq = Some(seq.clone());
        crate::metrics::bump(&crate::metrics::CHECKPOINTS);
        info!("recorded checkpoint at seq {seq}");
        Ok(())
    }
}

async fn get_local(ep: &Endpoint, doc_id: &str) -> Result<Option<CheckpointDoc>> {
    let segments: Vec<&str> = doc_id.split('/').collect();
    match ep.get_json::<CheckpointDoc>(&segments, &[]).await {
        Ok(doc) => Ok(Some(doc)),
        Err(e) if e.status() == Some(404) => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Deserialize)]
struct PutResp {
    rev: String,
}

async fn put_local(ep: &Endpoint, doc: &CheckpointDoc, rev: Option<String>) -> Result<String> {
    let segments: Vec<&str> = doc.id.split('/').collect();
    let mut attempt_doc = doc.clone();
    attempt_doc.rev = rev;
    match ep.put_json::<_, PutResp>(&segments, &[], &attempt_doc).await {
        Ok(r) => Ok(r.rev),
        Err(e) if e.status() == Some(409) => {
            // Somebody else wrote our checkpoint doc; take their rev and retry once.
            warn!("checkpoint conflict on {}; retrying with fresh rev", ep.label);
            let fresh = get_local(ep, &doc.id).await?;
            attempt_doc.rev = fresh.and_then(|d| d.rev);
            let r: PutResp = ep.put_json(&segments, &[], &attempt_doc).await?;
            Ok(r.rev)
        }
        Err(e) => Err(e),
    }
}

/// Find the newest sequence recorded under a session id both sides know about.
fn common_seq(src: &CheckpointDoc, tgt: &CheckpointDoc) -> Option<String> {
    let mut tgt_sessions: Vec<&str> = vec![tgt.session_id.as_str()];
    tgt_sessions.extend(tgt.history.iter().map(|h| h.session_id.as_str()));

    // Newest-first source-side list of (session, recorded seq).
    let mut src_entries: Vec<(&str, String)> = vec![(
        src.session_id.as_str(),
        crate::util::seq_to_string(&src.source_last_seq),
    )];
    src_entries.extend(
        src.history
            .iter()
            .map(|h| (h.session_id.as_str(), crate::util::seq_to_string(&h.recorded_seq))),
    );

    src_entries
        .into_iter()
        .find(|(session, _)| !session.is_empty() && tgt_sessions.contains(session))
        .map(|(_, seq)| seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(session: &str, seq: &str, history: Vec<(&str, &str)>) -> CheckpointDoc {
        CheckpointDoc {
            id: "_local/x".into(),
            rev: None,
            session_id: session.into(),
            source_last_seq: serde_json::Value::String(seq.into()),
            replicator: "couch-repl".into(),
            replication_id_version: 4,
            history: history
                .into_iter()
                .map(|(s, q)| HistoryEntry {
                    session_id: s.into(),
                    start_time: String::new(),
                    end_time: String::new(),
                    start_last_seq: serde_json::Value::Null,
                    end_last_seq: serde_json::Value::Null,
                    recorded_seq: serde_json::Value::String(q.into()),
                    missing_checked: 0,
                    missing_found: 0,
                    docs_read: 0,
                    docs_written: 0,
                    doc_write_failures: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn matching_current_sessions() {
        let s = doc("A", "100", vec![]);
        let t = doc("A", "100", vec![]);
        assert_eq!(common_seq(&s, &t), Some("100".into()));
    }

    #[test]
    fn common_ancestor_in_history() {
        let s = doc("C", "300", vec![("B", "200"), ("A", "100")]);
        let t = doc("B", "200", vec![("A", "100")]);
        assert_eq!(common_seq(&s, &t), Some("200".into()));
    }

    #[test]
    fn no_common_session() {
        let s = doc("A", "100", vec![]);
        let t = doc("B", "200", vec![]);
        assert_eq!(common_seq(&s, &t), None);
    }
}
