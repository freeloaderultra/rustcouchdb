use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Default)]
pub struct Stats {
    pub changes_read: AtomicU64,
    pub docs_filtered: AtomicU64,
    pub missing_checked: AtomicU64,
    pub missing_found: AtomicU64,
    pub docs_read: AtomicU64,
    pub docs_written: AtomicU64,
    pub doc_write_failures: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl Stats {
    pub fn add(&self, counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "changes_read": self.changes_read.load(Ordering::Relaxed),
            "docs_filtered": self.docs_filtered.load(Ordering::Relaxed),
            "missing_checked": self.missing_checked.load(Ordering::Relaxed),
            "missing_found": self.missing_found.load(Ordering::Relaxed),
            "docs_read": self.docs_read.load(Ordering::Relaxed),
            "docs_written": self.docs_written.load(Ordering::Relaxed),
            "doc_write_failures": self.doc_write_failures.load(Ordering::Relaxed),
            "bytes_written": self.bytes_written.load(Ordering::Relaxed),
        })
    }

    pub fn summary(&self, elapsed: std::time::Duration) -> String {
        let written = self.docs_written.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64().max(0.001);
        format!(
            "changes_read={} filtered={} missing_checked={} missing_found={} docs_read={} docs_written={} write_failures={} throughput={:.0} docs/s ({:.1} MB/s) elapsed={:.1}s",
            self.changes_read.load(Ordering::Relaxed),
            self.docs_filtered.load(Ordering::Relaxed),
            self.missing_checked.load(Ordering::Relaxed),
            self.missing_found.load(Ordering::Relaxed),
            self.docs_read.load(Ordering::Relaxed),
            written,
            self.doc_write_failures.load(Ordering::Relaxed),
            written as f64 / secs,
            self.bytes_written.load(Ordering::Relaxed) as f64 / 1_048_576.0 / secs,
            secs,
        )
    }
}

/// Periodic one-line progress reporter.
pub struct Progress {
    start: Instant,
    last_written: u64,
    last_at: Instant,
}

impl Progress {
    pub fn new() -> Self {
        let now = Instant::now();
        Progress {
            start: now,
            last_written: 0,
            last_at: now,
        }
    }

    pub fn line(&mut self, stats: &Stats, in_flight: usize, seq: &Option<String>) -> String {
        let now = Instant::now();
        let written = stats.get(&stats.docs_written);
        let window = now.duration_since(self.last_at).as_secs_f64().max(0.001);
        let rate = (written - self.last_written) as f64 / window;
        self.last_written = written;
        self.last_at = now;
        let seq_disp = seq
            .as_deref()
            .map(|s| s.chars().take(24).collect::<String>())
            .unwrap_or_else(|| "-".into());
        format!(
            "[{:>7.1}s] written={} ({:.0}/s) read={} in_flight={} checkpointable_seq={}",
            self.start.elapsed().as_secs_f64(),
            written,
            rate,
            stats.get(&stats.docs_read),
            in_flight,
            seq_disp,
        )
    }
}

impl Default for Progress {
    fn default() -> Self {
        Self::new()
    }
}
