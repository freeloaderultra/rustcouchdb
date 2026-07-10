use md5::{Digest, Md5};
use serde_json::json;

/// Filter options that change which documents flow through a replication.
/// Changing any of these must produce a different replication id (and thus a
/// fresh checkpoint lineage), same as the Erlang replicator's rules.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub doc_ids: Option<Vec<String>>,
    pub selector: Option<serde_json::Value>,
}

/// Deterministic base replication id, scheme "rust-1". Not interoperable with
/// the Erlang replicator's ids by design; stable across couch-repl runs and
/// hosts. Like CouchDB, the base id keys the `_local` checkpoint documents,
/// while displayed ids (scheduler, _active_tasks, _local_id) append option
/// suffixes — see [`task_id`]. Consumers rely on that split: nxguide derives
/// the checkpoint doc id by truncating the scheduler id at the first '+'.
pub fn replication_id(
    source_url: &str,
    target_url: &str,
    filter: &Filter,
    winning_revs_only: bool,
) -> String {
    let doc_ids = filter.doc_ids.as_ref().map(|ids| {
        let mut sorted = ids.clone();
        sorted.sort();
        sorted
    });
    // serde_json serializes maps with sorted keys, so this is canonical.
    let canonical = json!({
        "v": "rust-1",
        "source": source_url,
        "target": target_url,
        "doc_ids": doc_ids,
        "selector": filter.selector,
        "winning_revs_only": winning_revs_only,
    });
    let mut hasher = Md5::new();
    hasher.update(canonical.to_string().as_bytes());
    hex(&hasher.finalize())
}

/// The id shown in _scheduler / _active_tasks / _replicate responses:
/// base id plus option suffixes (CouchDB's BaseId ++ ExtId).
pub fn task_id(base_id: &str, continuous: bool) -> String {
    if continuous {
        format!("{base_id}+continuous")
    } else {
        base_id.to_string()
    }
}

/// Checkpoint documents are keyed by the BASE id only (no option suffixes),
/// exactly like CouchDB's `_local/<BaseId>`.
pub fn checkpoint_doc_id(rep_id: &str) -> String {
    format!("_local/{rep_id}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_sensitive() {
        let f = Filter::default();
        let a = replication_id("http://h/db1", "http://h/db2", &f, false);
        let b = replication_id("http://h/db1", "http://h/db2", &f, false);
        assert_eq!(a, b);
        let c = replication_id("http://h/db1", "http://h/db3", &f, false);
        assert_ne!(a, c);
        // The base id has no option suffixes; task_id adds them for display,
        // and the checkpoint doc id must stay suffix-free.
        assert!(!a.contains('+'));
        assert_eq!(task_id(&a, true), format!("{a}+continuous"));
        assert_eq!(task_id(&a, false), a);
        assert_eq!(checkpoint_doc_id(&a), format!("_local/{a}"));
    }

    #[test]
    fn doc_ids_order_insensitive() {
        let f1 = Filter {
            doc_ids: Some(vec!["b".into(), "a".into()]),
            selector: None,
        };
        let f2 = Filter {
            doc_ids: Some(vec!["a".into(), "b".into()]),
            selector: None,
        };
        assert_eq!(
            replication_id("http://h/x", "http://h/y", &f1, false),
            replication_id("http://h/x", "http://h/y", &f2, false)
        );
    }
}
