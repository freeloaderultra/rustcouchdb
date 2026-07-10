use std::collections::BTreeMap;
use std::sync::Mutex;

/// Tracks which source-sequence prefixes are fully replicated.
///
/// Cluster sequences are opaque strings and pipeline stages complete out of
/// order, so every change is tagged with a monotonically increasing ordinal at
/// read time. `source_last_seq` may only advance to the seq of the highest
/// *contiguous* completed ordinal — that is the only value it is ever safe to
/// checkpoint.
pub struct SeqLedger {
    inner: Mutex<Inner>,
}

struct Inner {
    next_ord: u64,
    /// ord -> (seq, outstanding work units). 0 outstanding = complete, but the
    /// entry stays until everything before it is complete too.
    entries: BTreeMap<u64, Entry>,
    committable: Option<String>,
}

struct Entry {
    seq: String,
    pending: u32,
}

impl SeqLedger {
    pub fn new() -> Self {
        SeqLedger {
            inner: Mutex::new(Inner {
                next_ord: 0,
                entries: BTreeMap::new(),
                committable: None,
            }),
        }
    }

    /// Register a change with one outstanding unit of work; returns its ordinal.
    pub fn register(&self, seq: String) -> u64 {
        let mut g = self.inner.lock().unwrap();
        let ord = g.next_ord;
        g.next_ord += 1;
        g.entries.insert(ord, Entry { seq, pending: 1 });
        ord
    }

    /// Register a barrier that is already complete (e.g. a page's last_seq),
    /// so the checkpoint can reach it once all preceding work is done.
    pub fn register_done(&self, seq: String) {
        let mut g = self.inner.lock().unwrap();
        let ord = g.next_ord;
        g.next_ord += 1;
        g.entries.insert(ord, Entry { seq, pending: 0 });
        g.advance();
    }

    /// A change turned out to need `units` units of work (one per missing rev).
    /// Must be called at most once per ordinal, before its first `complete`.
    pub fn expand(&self, ord: u64, units: u32) {
        assert!(units >= 1);
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.entries.get_mut(&ord) {
            debug_assert_eq!(e.pending, 1, "expand() after work started");
            e.pending = units;
        }
    }

    /// One unit of work for this ordinal finished (written, verified present,
    /// or permanently skipped).
    pub fn complete(&self, ord: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.entries.get_mut(&ord) {
            debug_assert!(e.pending > 0, "complete() beyond registered work");
            e.pending = e.pending.saturating_sub(1);
        }
        g.advance();
    }

    /// Highest seq safe to record in a checkpoint right now.
    pub fn committable(&self) -> Option<String> {
        self.inner.lock().unwrap().committable.clone()
    }

    /// Number of changes still in flight (for progress display).
    pub fn in_flight(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }
}

impl Inner {
    fn advance(&mut self) {
        while let Some((&ord, entry)) = self.entries.iter().next() {
            if entry.pending > 0 {
                break;
            }
            self.committable = Some(entry.seq.clone());
            self.entries.remove(&ord);
        }
    }
}

impl Default for SeqLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_completion() {
        let l = SeqLedger::new();
        let a = l.register("1-x".into());
        let b = l.register("2-x".into());
        let c = l.register("3-x".into());
        assert_eq!(l.committable(), None);
        l.complete(b);
        assert_eq!(l.committable(), None); // 1 still pending
        l.complete(a);
        assert_eq!(l.committable(), Some("2-x".into()));
        l.complete(c);
        assert_eq!(l.committable(), Some("3-x".into()));
    }

    #[test]
    fn expanded_work_units() {
        let l = SeqLedger::new();
        let a = l.register("1".into());
        l.expand(a, 3);
        l.complete(a);
        l.complete(a);
        assert_eq!(l.committable(), None);
        l.complete(a);
        assert_eq!(l.committable(), Some("1".into()));
    }

    #[test]
    fn barrier_advances_when_clear() {
        let l = SeqLedger::new();
        let a = l.register("5".into());
        l.register_done("10".into());
        assert_eq!(l.committable(), None);
        l.complete(a);
        assert_eq!(l.committable(), Some("10".into()));
    }

    #[test]
    fn empty_page_barrier() {
        let l = SeqLedger::new();
        l.register_done("42".into());
        assert_eq!(l.committable(), Some("42".into()));
    }
}
