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

/// Deterministic replication id, scheme "rust-1". Not interoperable with the
/// Erlang replicator's ids by design; stable across couch-repl runs and hosts.
pub fn replication_id(
    source_url: &str,
    target_url: &str,
    filter: &Filter,
    continuous: bool,
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
    });
    let mut hasher = Md5::new();
    hasher.update(canonical.to_string().as_bytes());
    let digest = hasher.finalize();
    let mut id = hex(&digest);
    if continuous {
        id.push_str("+continuous");
    }
    id
}

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
        let d = replication_id("http://h/db1", "http://h/db2", &f, true);
        assert_eq!(d, format!("{a}+continuous"));
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
