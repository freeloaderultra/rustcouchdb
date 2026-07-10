use clap::{Parser, Subcommand};
use couch_store::{compact, db, doc, ejson, error, revtree, writer};
use db::{Db, DocOpts};
use error::Result;
use revtree::RevVal;
use serde_json::json;
use std::io::{BufRead, Write};
use std::ops::ControlFlow;

#[derive(Parser)]
#[command(
    name = "couch-store",
    about = "Native Rust engine for CouchDB .couch files — read, verify and write shard files without Erlang"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print database info (doc counts, seqs, sizes) as JSON
    Info { file: String },
    /// Dump all docs as NDJSON (winner revs)
    Dump {
        file: String,
        /// Include deleted docs (tombstone stubs)
        #[arg(long)]
        deleted: bool,
        /// Include _conflicts / _deleted_conflicts
        #[arg(long)]
        conflicts: bool,
        /// Include _revisions
        #[arg(long)]
        revs: bool,
        /// Inline attachment data as base64 instead of stubs
        #[arg(long)]
        attachments: bool,
    },
    /// Print one document
    Get {
        file: String,
        id: String,
        /// Specific revision (default: winner)
        #[arg(long)]
        rev: Option<String>,
        #[arg(long)]
        revs: bool,
        #[arg(long)]
        conflicts: bool,
        #[arg(long)]
        attachments: bool,
    },
    /// List changes since a sequence as NDJSON
    Changes {
        file: String,
        #[arg(long, default_value_t = 0)]
        since: u64,
    },
    /// Dump _local documents as NDJSON
    Local { file: String },
    /// Print the security object
    Security { file: String },
    /// Write one attachment's raw bytes to stdout
    Att {
        file: String,
        id: String,
        name: String,
    },
    /// Read every doc, revision, summary and attachment; verify checksums
    Verify { file: String },
    /// Compact: rewrite the file keeping only live data (atomic swap)
    Compact { file: String },
    /// Create a new .couch file from NDJSON docs (one JSON doc per line;
    /// honors _rev/_revisions/_deleted/_attachments-with-data)
    Create {
        file: String,
        /// NDJSON input path, or - for stdin
        #[arg(long, default_value = "-")]
        from: String,
        #[arg(long, default_value_t = 1000)]
        batch: usize,
    },
    /// Append NDJSON docs to an existing .couch file (new_edits:false merge)
    Append {
        file: String,
        /// NDJSON input path, or - for stdin
        #[arg(long, default_value = "-")]
        from: String,
        #[arg(long, default_value_t = 1000)]
        batch: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli.cmd) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn println_json(v: &serde_json::Value) {
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, v).ok();
    out.write_all(b"\n").ok();
}

fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Info { file } => {
            let db = Db::open(&file)?;
            println_json(&db.info()?);
        }
        Cmd::Dump {
            file,
            deleted,
            conflicts,
            revs,
            attachments,
        } => {
            let db = Db::open(&file)?;
            let opts = DocOpts {
                revs,
                conflicts,
                attachments,
            };
            db.fold_docs(|fdi| {
                if fdi.deleted && !deleted {
                    return Ok(ControlFlow::Continue(()));
                }
                if let Some(w) = fdi.rev_tree.winner() {
                    println_json(&db.doc_json(&fdi, &w, &opts)?);
                }
                Ok(ControlFlow::Continue(()))
            })?;
        }
        Cmd::Get {
            file,
            id,
            rev,
            revs,
            conflicts,
            attachments,
        } => {
            let db = Db::open(&file)?;
            let opts = DocOpts {
                revs,
                conflicts,
                attachments,
            };
            match db.open_doc(id.as_bytes(), rev.as_deref(), &opts)? {
                Some(doc) => println_json(&doc),
                None => {
                    eprintln!("not found");
                    std::process::exit(2);
                }
            }
        }
        Cmd::Changes { file, since } => {
            let db = Db::open(&file)?;
            db.fold_changes(since, |fdi| {
                let winner = fdi.rev_tree.winner();
                let rev = winner
                    .as_ref()
                    .map(|w| doc::rev_str(w.pos, w.path[0]))
                    .unwrap_or_default();
                let mut row = json!({
                    "seq": fdi.update_seq,
                    "id": String::from_utf8_lossy(&fdi.id),
                    "rev": rev,
                });
                if fdi.deleted {
                    row["deleted"] = json!(true);
                }
                println_json(&row);
                Ok(ControlFlow::Continue(()))
            })?;
        }
        Cmd::Local { file } => {
            let db = Db::open(&file)?;
            db.fold_local_docs(|d| {
                println_json(&d);
                Ok(ControlFlow::Continue(()))
            })?;
        }
        Cmd::Security { file } => {
            let db = Db::open(&file)?;
            println_json(&db.security()?);
        }
        Cmd::Att { file, id, name } => {
            let db = Db::open(&file)?;
            let fdi = db
                .open_doc_info(id.as_bytes())?
                .ok_or_else(|| error::Error::BadRequest(format!("no such doc: {id}")))?;
            let winner = fdi
                .rev_tree
                .winner()
                .ok_or_else(|| error::Error::BadRequest("doc has no revisions".into()))?;
            let RevVal::Leaf(lv) = winner.leaf else {
                return Err(error::Error::BadRequest("winner rev is missing".into()));
            };
            let ptr = lv
                .ptr
                .ok_or_else(|| error::Error::BadRequest("rev has no body".into()))?;
            let summary = doc::read_summary(&db.file, ptr)?;
            let att = db
                .find_att(&summary, &name)
                .ok_or_else(|| error::Error::BadRequest(format!("no such attachment: {name}")))?;
            let data = doc::read_att_data_decoded(&db.file, att)?;
            std::io::stdout().lock().write_all(&data)?;
        }
        Cmd::Verify { file } => {
            let db = Db::open(&file)?;
            verify(&db)?;
        }
        Cmd::Compact { file } => {
            println_json(&compact::compact(&file)?);
        }
        Cmd::Create { file, from, batch } => {
            let w = writer::DbWriter::create(&file)?;
            load_ndjson(w, &file, &from, batch)?;
        }
        Cmd::Append { file, from, batch } => {
            let w = writer::DbWriter::open(&file)?;
            load_ndjson(w, &file, &from, batch)?;
        }
    }
    Ok(())
}

fn verify(db: &Db) -> Result<()> {
    use md5::{Digest, Md5};
    let mut docs = 0u64;
    let mut revs = 0u64;
    let mut atts = 0u64;
    let mut att_bytes = 0u64;
    let mut body_bytes = 0u64;
    db.fold_docs(|fdi| {
        docs += 1;
        for leaf in fdi.rev_tree.leaves() {
            if let RevVal::Leaf(lv) = leaf.leaf {
                revs += 1;
                if let Some(ptr) = lv.ptr {
                    let summary = doc::read_summary(&db.file, ptr)?;
                    body_bytes += ejson::external_size(&ejson::to_json(&summary.body)?) as u64;
                    for att in &summary.atts {
                        let data = doc::read_att_data(&db.file, att)?;
                        atts += 1;
                        att_bytes += data.len() as u64;
                        if !att.md5.is_empty() && att.md5 != Md5::digest(&data).to_vec() {
                            return Err(error::corrupt(format!(
                                "attachment digest mismatch: {}/{}",
                                String::from_utf8_lossy(&fdi.id),
                                att.name
                            )));
                        }
                    }
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;
    // deleted docs too
    let mut changes = 0u64;
    db.fold_changes(0, |_| {
        changes += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    let mut locals = 0u64;
    db.fold_local_docs(|_| {
        locals += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    let (live, del, _, _) = db.doc_counts()?;
    if changes != live + del {
        return Err(error::corrupt(format!(
            "seq tree has {changes} entries but id tree reduces to {live}+{del}"
        )));
    }
    println_json(&json!({
        "ok": true,
        "live_docs_walked": docs,
        "doc_count": live,
        "doc_del_count": del,
        "leaf_revs_read": revs,
        "changes_rows": changes,
        "local_docs": locals,
        "attachments_verified": atts,
        "attachment_bytes": att_bytes,
        "body_json_bytes": body_bytes,
    }));
    Ok(())
}

fn load_ndjson(mut w: writer::DbWriter, file: &str, from: &str, batch_size: usize) -> Result<()> {
    let reader: Box<dyn BufRead> = if from == "-" {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::io::BufReader::new(std::fs::File::open(from)?))
    };
    let mut batch: Vec<writer::DocUpdate> = Vec::with_capacity(batch_size);
    let mut total = 0usize;
    let mut written = 0usize;
    let mut locals: Vec<(Vec<u8>, serde_json::Value)> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| error::Error::BadRequest(format!("bad JSON line: {e}")))?;
        if v.get("_id").and_then(|i| i.as_str()).is_some_and(|id| id.starts_with("_local/")) {
            let id = v["_id"].as_str().unwrap().as_bytes().to_vec();
            locals.push((id, v));
            continue;
        }
        batch.push(writer::DocUpdate::from_json(v)?);
        total += 1;
        if batch.len() >= batch_size {
            written += w.update_docs(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        written += w.update_docs(batch)?;
    }
    for (id, v) in &locals {
        w.update_local(id, Some(v))?;
    }
    w.commit()?;
    println_json(&json!({
        "ok": true,
        "docs_in": total,
        "docs_written": written,
        "local_docs": locals.len(),
        "update_seq": w.update_seq(),
        "file": file,
    }));
    Ok(())
}
