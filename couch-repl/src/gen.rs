use crate::cli::GenArgs;
use crate::client::Endpoint;
use crate::error::Result;
use crate::retry::RetryPolicy;
use base64::Engine;
use futures::StreamExt;
use rand::distr::Alphanumeric;
use rand::Rng;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Fill a database with synthetic documents for benchmarking.
pub async fn run(args: GenArgs) -> Result<()> {
    let ep = Endpoint::new(
        "gen",
        &args.db,
        &[],
        args.insecure,
        120,
        RetryPolicy::default(),
    )?;
    ep.ensure_db(true).await?;

    let started = Instant::now();
    let inserted = Arc::new(AtomicU64::new(0));
    let ranges: Vec<(u64, u64)> = (args.start..args.start + args.docs)
        .step_by(args.batch.max(1))
        .map(|lo| (lo, (lo + args.batch as u64).min(args.start + args.docs)))
        .collect();

    let errors = futures::stream::iter(ranges)
        .map(|(lo, hi)| {
            let ep = ep.clone();
            let args_prefix = args.prefix.clone();
            let inserted = inserted.clone();
            let doc_kb = args.doc_kb;
            let atts = args.atts;
            let att_kb = args.att_kb;
            let total = args.docs;
            async move {
                let docs: Vec<serde_json::Value> = (lo..hi)
                    .map(|i| make_doc(&args_prefix, i, doc_kb, atts, att_kb))
                    .collect();
                let body = json!({ "docs": docs });
                let r: std::result::Result<Vec<serde_json::Value>, _> =
                    ep.post_json(&["_bulk_docs"], &[], &body).await;
                match r {
                    Ok(_) => {
                        let n = inserted.fetch_add(hi - lo, Ordering::Relaxed) + (hi - lo);
                        if n % 50_000 < (hi - lo) {
                            eprintln!("generated {n}/{total} docs");
                        }
                        None
                    }
                    Err(e) => Some(e),
                }
            }
        })
        .buffer_unordered(args.concurrency)
        .filter_map(|e| async { e })
        .collect::<Vec<_>>()
        .await;

    if let Some(e) = errors.into_iter().next() {
        return Err(e);
    }
    let n = inserted.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    eprintln!("generated {n} docs in {secs:.1}s ({:.0} docs/s)", n as f64 / secs);
    Ok(())
}

fn make_doc(prefix: &str, i: u64, doc_kb: usize, atts: usize, att_kb: usize) -> serde_json::Value {
    let data: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(doc_kb * 1024)
        .map(char::from)
        .collect();
    let mut doc = json!({
        "_id": format!("{prefix}{i:010}"),
        "n": i,
        "group": i % 100,
        "data": data,
    });
    if atts > 0 {
        let mut attachments = serde_json::Map::new();
        for k in 0..atts {
            let mut bytes = vec![0u8; att_kb * 1024];
            rand::rng().fill(&mut bytes[..]);
            attachments.insert(
                format!("att{k}.bin"),
                json!({
                    "content_type": "application/octet-stream",
                    "data": base64::engine::general_purpose::STANDARD.encode(&bytes),
                }),
            );
        }
        doc.as_object_mut()
            .unwrap()
            .insert("_attachments".into(), serde_json::Value::Object(attachments));
    }
    doc
}
