use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{timeout_at, Instant};

/// Collect up to `max` items from `rx`, waiting at most `linger` after the
/// first item arrives. Returns None when the channel is closed and drained.
pub async fn batch_recv<T>(rx: &mut Receiver<T>, max: usize, linger: Duration) -> Option<Vec<T>> {
    let first = rx.recv().await?;
    let mut batch = Vec::with_capacity(max.min(1024));
    batch.push(first);
    let deadline = Instant::now() + linger;
    while batch.len() < max {
        match timeout_at(deadline, rx.recv()).await {
            Ok(Some(item)) => batch.push(item),
            Ok(None) | Err(_) => break,
        }
    }
    Some(batch)
}

/// Like `batch_recv`, but also bounded by a total weight (e.g. bytes).
pub async fn batch_recv_weighted<T>(
    rx: &mut Receiver<T>,
    max: usize,
    max_weight: usize,
    weigh: impl Fn(&T) -> usize,
    linger: Duration,
) -> Option<Vec<T>> {
    let first = rx.recv().await?;
    let mut weight = weigh(&first);
    let mut batch = vec![first];
    let deadline = Instant::now() + linger;
    while batch.len() < max && weight < max_weight {
        match timeout_at(deadline, rx.recv()).await {
            Ok(Some(item)) => {
                weight += weigh(&item);
                batch.push(item);
            }
            Ok(None) | Err(_) => break,
        }
    }
    Some(batch)
}

/// Render an opaque sequence value (string in 2.x+, integer in very old
/// servers) as the string we track and send back in `since=`.
pub fn seq_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
