use crate::error::{Error, Result};
use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tracing::warn;

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base: Duration,
    pub cap: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 10,
            base: Duration::from_millis(250),
            cap: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Exponential backoff with full jitter.
    fn delay(&self, attempt: u32) -> Duration {
        let exp = self.base.saturating_mul(2u32.saturating_pow(attempt));
        let cap = exp.min(self.cap);
        let jittered = rand::rng().random_range(0..=cap.as_millis() as u64);
        Duration::from_millis(jittered.max(50))
    }
}

/// Run `op` until it succeeds, fails permanently, or retries are exhausted.
/// `what` is used only for log messages.
pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, what: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if e.retryable() && attempt < policy.max_retries => {
                let delay = if e.status() == Some(429) {
                    // Prefer server guidance when rate limited.
                    retry_after(&e).unwrap_or_else(|| self_delay(policy, attempt))
                } else {
                    self_delay(policy, attempt)
                };
                warn!(
                    "{what}: {e}; retry {}/{} in {:?}",
                    attempt + 1,
                    policy.max_retries,
                    delay
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

fn self_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    policy.delay(attempt)
}

/// We stash a Retry-After hint in the error body as "retry-after:<secs>" when
/// the client sees the header; parse it back out here.
fn retry_after(e: &Error) -> Option<Duration> {
    if let Error::Http { body, .. } = e {
        if let Some(rest) = body.strip_prefix("retry-after:") {
            if let Some((secs, _)) = rest.split_once(';') {
                if let Ok(s) = secs.trim().parse::<u64>() {
                    return Some(Duration::from_secs(s.min(300)));
                }
            }
        }
    }
    None
}
