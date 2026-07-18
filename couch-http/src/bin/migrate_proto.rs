//! One-off migration of a legacy nxguide database into a proto-native
//! rustcouchdb database.
//!
//! The old world stores two doc shapes: "blob documents" (a JSON head plus a
//! `blob.data` protobuf attachment holding the whole message) and plain
//! protojson documents (e.g. `field`). The new world stores every
//! application document as raw protobuf bytes with its message type.
//!
//! This tool reads the source `.couch` file, and for each winning,
//! non-deleted document writes one proto-native document into a fresh target
//! `.couch`:
//!   - blob docs      → the attachment bytes ARE the message; stored verbatim
//!   - protojson docs → protojson decoded against the schema, re-encoded
//!   - `_design/*`    → copied as-is (index definitions stay JSON control-plane)
//!
//! Tombstones are dropped (all other peers are wiped and resync from the
//! migrated database, so 1.7M deletion markers are dead weight). `_local`
//! checkpoints are dropped for the same reason. No fallbacks: any document
//! that cannot be resolved to a schema or re-encoded aborts the migration
//! with the offending id — a silently skipped document is data loss.

use couch_proto::Registry;
use couch_store::db::Db;
use couch_store::writer::{BodyInput, DbWriter, DocUpdate, SaveOutcome};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::process::exit;

const BLOB_ATT: &str = "blob.data";

fn arg(name: &str) -> Option<String> {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if let Some(v) = a.strip_prefix(&format!("--{name}=")) {
            return Some(v.to_string());
        }
        if a == format!("--{name}") {
            return it.next();
        }
    }
    None
}

fn main() {
    if let Err(e) = run() {
        eprintln!("migration failed: {e}");
        exit(1);
    }
}

fn run() -> Result<(), String> {
    let census = arg("census").is_some();
    let source = arg("source").ok_or("--source <old .couch> required")?;
    let target = if census {
        String::new()
    } else {
        arg("target").ok_or("--target <new .couch> required")?
    };
    let descriptor = arg("descriptor").ok_or("--descriptor <FileDescriptorSet .pb> required")?;
    let doctypes_path = arg("doctypes");

    let set = std::fs::read(&descriptor).map_err(|e| format!("read {descriptor}: {e}"))?;
    let overrides: HashMap<String, String> = match &doctypes_path {
        Some(p) => {
            let s = std::fs::read_to_string(p).map_err(|e| format!("read {p}: {e}"))?;
            serde_json::from_str(&s).map_err(|e| format!("parse doctypes {p}: {e}"))?
        }
        None => HashMap::new(),
    };
    let (reg, advisories) = Registry::build(&[set], &overrides)?;
    for a in &advisories {
        eprintln!("note: {a}");
    }

    let src = Db::open(&source).map_err(|e| e.to_string())?;
    // A throwaway writer target for census mode (never committed / kept).
    let target = if census {
        format!("{source}.census-scratch")
    } else {
        if std::path::Path::new(&target).exists() {
            return Err(format!("target {target} already exists; refusing to overwrite"));
        }
        target
    };
    let _ = std::fs::remove_file(&target);
    let mut w = DbWriter::create(&target).map_err(|e| e.to_string())?;
    w.mark_proto_native().map_err(|e| e.to_string())?;

    let mut stats = Stats::default();
    // Collect ids first (the fold borrows the file; writes need it mutably).
    let mut work: Vec<Vec<u8>> = Vec::new();
    src.fold_docs(|fdi| {
        if fdi.deleted {
            stats.tombstones += 1;
            return Ok(ControlFlow::Continue(()));
        }
        if fdi.rev_tree.winner().is_some() {
            work.push(fdi.id.clone());
        }
        Ok(ControlFlow::Continue(()))
    })
    .map_err(|e| e.to_string())?;

    // A pre-flight census (--census): resolve every doc's type without
    // keeping output, so an unmigratable doctype (schema moved/removed) is
    // reported in full rather than aborting on the first one.
    for id_bytes in &work {
        let id = String::from_utf8_lossy(id_bytes).into_owned();
        migrate_one(&src, &mut w, &reg, id_bytes, &id, &mut stats, census)
            .map_err(|e| format!("{id}: {e}"))?;
        if !census && (stats.ddocs + stats.blob + stats.json) % 5000 == 0 {
            w.commit().map_err(|e| e.to_string())?;
            eprintln!(
                "  … {} docs ({} blob, {} json, {} ddoc)",
                stats.blob + stats.json + stats.ddocs,
                stats.blob,
                stats.json,
                stats.ddocs
            );
        }
    }
    if !census {
        w.commit().map_err(|e| e.to_string())?;
    }

    // Report dropped legacy fields (matches the backend's DiscardUnknown).
    if !stats.dropped.is_empty() {
        eprintln!("\nlegacy fields dropped (backend already discards these on read):");
        let mut dts: Vec<_> = stats.dropped.keys().cloned().collect();
        dts.sort();
        for dt in dts {
            let fields = &stats.dropped[&dt];
            let mut fs: Vec<_> = fields.iter().collect();
            fs.sort_by(|a, b| b.1.cmp(a.1));
            let list: Vec<String> = fs.iter().map(|(k, c)| format!("{k}×{c}")).collect();
            eprintln!("  {dt}: {}", list.join(", "));
        }
    }

    // Blobs whose stored bytes no longer decode against the current schema.
    if !stats.undecodable.is_empty() {
        eprintln!("\nUNDECODABLE blobs (stored bytes wire-incompatible with current schema):");
        let mut dts: Vec<_> = stats.undecodable.iter().collect();
        dts.sort_by(|a, b| b.1 .0.cmp(&a.1 .0));
        for (dt, (n, sample)) in dts {
            eprintln!("  {dt}: {n} docs (e.g. {sample})");
        }
        if !census && !skip_undecodable() {
            return Err(
                "some blobs do not decode against the current schema; re-run with \
                 --skip-undecodable to drop them, or fix the schema/data first"
                    .into(),
            );
        }
    }

    // Unresolvable doctypes are a hard stop — their documents cannot be
    // migrated until the doctypes mapping names their current message.
    if !stats.unresolved.is_empty() {
        eprintln!("\nUNMIGRATABLE — doctypes with no message in the schema:");
        let mut dts: Vec<_> = stats.unresolved.iter().collect();
        dts.sort_by(|a, b| b.1.cmp(a.1));
        for (dt, n) in dts {
            eprintln!("  {dt}: {n} docs");
        }
        return Err(
            "some documents' types are not in the provided schema; supply correct \
             --doctypes mappings (or an updated descriptor set) and re-run"
                .into(),
        );
    }

    if census {
        let _ = std::fs::remove_file(&target);
        println!(
            "census OK: {} docs resolve ({} blob, {} json, {} ddoc); {} tombstones would be dropped",
            stats.blob + stats.json + stats.ddocs,
            stats.blob,
            stats.json,
            stats.ddocs,
            stats.tombstones
        );
        return Ok(());
    }

    println!(
        "migrated {} documents into {target}\n  blob→proto: {}\n  json→proto: {}\n  design docs kept: {}\n  tombstones dropped: {}",
        stats.blob + stats.json + stats.ddocs,
        stats.blob,
        stats.json,
        stats.ddocs,
        stats.tombstones
    );
    Ok(())
}

#[derive(Default)]
struct Stats {
    blob: u64,
    json: u64,
    ddocs: u64,
    tombstones: u64,
    /// doctype -> dropped legacy field name -> count (mirrors the backend's
    /// DiscardUnknown read; reported so nothing is silently lost).
    dropped: HashMap<String, HashMap<String, u64>>,
    /// doctype -> count of documents whose message type isn't in the schema
    /// (unmigratable; aborts the run after the full census).
    unresolved: HashMap<String, u64>,
    /// doctype -> (count, one sample id) of blobs whose stored bytes don't
    /// decode against the current schema (a wire-incompatible field-type
    /// change over the data's lifetime).
    undecodable: HashMap<String, (u64, String)>,
}

fn migrate_one(
    src: &Db,
    w: &mut DbWriter,
    reg: &Registry,
    id_bytes: &[u8],
    id: &str,
    stats: &mut Stats,
    census: bool,
) -> Result<(), String> {
    // Design docs (index definitions) stay JSON control-plane.
    if id.starts_with("_design/") {
        if census {
            stats.ddocs += 1;
            return Ok(());
        }
        let doc = src
            .open_doc(id_bytes, None, &Default::default())
            .map_err(|e| e.to_string())?
            .ok_or("design doc vanished")?;
        match w.save_doc(&doc, None).map_err(|e| e.to_string())? {
            SaveOutcome::Ok { .. } => {
                stats.ddocs += 1;
                Ok(())
            }
            SaveOutcome::Error { error, reason } => Err(format!("{error}: {reason}")),
        }
    } else {
        // Open with attachment stubs so we can spot a blob attachment.
        let doc = src
            .open_doc(id_bytes, None, &Default::default())
            .map_err(|e| e.to_string())?
            .ok_or("doc vanished")?;
        let doctype = doc
            .get("db")
            .and_then(|d| d.get("DocType"))
            .and_then(|v| v.as_str())
            .ok_or("no db.DocType; cannot resolve message type")?
            .to_string();
        let desc = match reg.resolve(&doctype) {
            Some(d) => d,
            None => {
                // Not fatal per-doc: census the full extent of the problem,
                // then abort with the complete list.
                *stats.unresolved.entry(doctype).or_default() += 1;
                return Ok(());
            }
        };
        let full_name = desc.full_name().to_string();

        let is_blob = doc
            .get("_attachments")
            .and_then(|a| a.get(BLOB_ATT))
            .is_some();
        let bytes = if is_blob {
            // The attachment bytes are already the complete marshaled message.
            src.att_bytes(id_bytes, BLOB_ATT, u64::MAX)
                .map_err(|e| e.to_string())?
                .ok_or("blob.data attachment missing")?
        } else {
            // protojson document: strip metadata, re-encode against schema
            // with the backend's DiscardUnknown semantics; record any legacy
            // keys dropped.
            let mut view = doc.clone();
            if let Some(obj) = view.as_object_mut() {
                obj.retain(|k, _| !k.starts_with('_'));
            }
            let (bytes, dropped) = reg.encode_message_lenient(&full_name, &view)?;
            if !dropped.is_empty() {
                let per = stats.dropped.entry(doctype.clone()).or_default();
                for k in dropped {
                    *per.entry(k).or_default() += 1;
                }
            }
            bytes
        };

        // Verify the bytes decode against the current schema. A failure
        // means the stored bytes are wire-incompatible with today's message
        // (a field's type changed) — collect the full extent rather than
        // abort on the first, and (outside --skip-undecodable) refuse to
        // write such a document rather than store one that fails at read.
        if let Err(e) = reg.can_decode(&full_name, &bytes) {
            let entry = stats
                .undecodable
                .entry(doctype.clone())
                .or_insert((0, id.to_string()));
            entry.0 += 1;
            if census || skip_undecodable() {
                return Ok(());
            }
            return Err(format!("blob does not decode against {full_name}: {e}"));
        }

        if !census {
            let upd = DocUpdate {
                id: id_bytes.to_vec(),
                // Fresh single rev: peers are wiped and resync, so rev
                // history need not be preserved — only the id (references).
                rev_path: (1, vec![gen_revid(id, &full_name, &bytes)]),
                deleted: false,
                body: BodyInput::Proto {
                    type_name: full_name,
                    bytes,
                },
                atts: Vec::new(),
            };
            w.update_docs(vec![upd]).map_err(|e| e.to_string())?;
        }
        if is_blob {
            stats.blob += 1;
        } else {
            stats.json += 1;
        }
        Ok(())
    }
}

fn skip_undecodable() -> bool {
    arg("skip-undecodable").is_some()
}

fn gen_revid(id: &str, type_name: &str, bytes: &[u8]) -> Vec<u8> {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(id.as_bytes());
    h.update(type_name.as_bytes());
    h.update(bytes);
    h.finalize().to_vec()
}
