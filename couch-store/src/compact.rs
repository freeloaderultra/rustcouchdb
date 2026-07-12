//! Compaction: rewrite a .couch file keeping only live data, exactly like
//! couch_bt_engine_compactor — new file gets the same uuid/epochs, doc
//! summaries and attachment streams are copied (with stream pointers
//! rewritten), and `compacted_seq` records the seq the compaction saw.

use crate::btree::{self, Reducer};
use crate::compress;
use crate::db::Db;
use crate::doc;
use crate::error::Result;
use crate::etf::Term;
use crate::file::CouchFile;
use crate::header::{DbHeader, TreeState};
use crate::revtree::{RevTree, RevVal};
use serde_json::json;
use std::ops::ControlFlow;

const BATCH: usize = 2000;

pub fn compact(path: &str) -> Result<serde_json::Value> {
    let src = Db::open(path)?;
    let compact_path = format!("{path}.compact");
    let _ = std::fs::remove_file(&compact_path);
    let mut dst = CouchFile::create(&compact_path)?;

    // New header inherits identity; trees start empty.
    let mut header = DbHeader::new(String::new());
    header.uuid = src.header.uuid.clone();
    header.epochs = src.header.epochs.clone();
    header.revs_limit = src.header.revs_limit;
    header.purge_infos_limit = src.header.purge_infos_limit;
    header.update_seq = src.header.update_seq;
    header.compacted_seq = Term::Int(src.header.update_seq as i64);
    // Pointer-valued header fields must be re-appended, never copied — the
    // old offsets are meaningless in the new file.
    let mut copy_ptr = |field: &Term, dst: &mut CouchFile| -> Result<Term> {
        match field {
            Term::Int(ptr) => {
                let term = src.file.read_term(*ptr as u64)?;
                let (new_ptr, _) = dst.append_term(&term)?;
                Ok(Term::Int(new_ptr as i64))
            }
            other => Ok(other.clone()),
        }
    };
    header.security_ptr = copy_ptr(&src.header.security_ptr, &mut dst)?;
    header.props_ptr = copy_ptr(&src.header.props_ptr, &mut dst)?;
    header.time_seq_ptr = copy_ptr(&src.header.time_seq_ptr, &mut dst)?;

    let mut id_root: Option<TreeState> = None;
    let mut seq_root: Option<TreeState> = None;
    let mut docs = 0u64;
    let mut atts_copied = 0u64;

    let mut id_batch: Vec<(Term, Term)> = Vec::with_capacity(BATCH);

    // The fold below runs in doc-id order, so id-tree inserts only ever touch
    // the rightmost path — cheap. The same docs' update_seqs are effectively
    // random, and inserting them into the seq tree as we go would rewrite
    // thousands of interior nodes per batch (measured: it doubles the file).
    // Like couch_emsort in couch_bt_engine_compactor, seq entries are spilled
    // to sorted runs in a scratch file and bulk-inserted in seq order after
    // the fold.
    let meta_path = format!("{path}.compact.meta");
    let _ = std::fs::remove_file(&meta_path);
    let mut seq_spill = SeqSpill::create(&meta_path)?;

    src.fold_docs(|fdi| {
        docs += 1;
        // Copy every stored leaf's summary (and its attachments).
        let mut tree = fdi.rev_tree.clone();
        copy_tree_bodies(&src.file, &mut dst, &mut tree, &mut atts_copied)?;

        let tree_term = tree.to_term();
        let sizes = Term::Tuple(vec![Term::Int(fdi.sizes.0), Term::Int(fdi.sizes.1)]);
        id_batch.push((
            Term::Bin(fdi.id.clone()),
            Term::Tuple(vec![
                Term::Int(fdi.update_seq as i64),
                Term::Int(fdi.deleted as i64),
                sizes.clone(),
                tree_term.clone(),
            ]),
        ));
        seq_spill.push(
            fdi.update_seq,
            &Term::Tuple(vec![
                Term::Bin(fdi.id.clone()),
                Term::Int(fdi.deleted as i64),
                sizes,
                tree_term,
            ]),
        )?;
        if id_batch.len() >= BATCH {
            id_root = btree::add_remove(
                &mut dst,
                &id_root,
                Reducer::IdTree,
                std::mem::take(&mut id_batch),
                vec![],
            )?;
        }
        Ok(ControlFlow::Continue(()))
    })?;
    if !id_batch.is_empty() {
        id_root = btree::add_remove(
            &mut dst,
            &id_root,
            Reducer::IdTree,
            std::mem::take(&mut id_batch),
            vec![],
        )?;
    }

    // Merge the sorted runs and build the seq tree in ascending order.
    let mut merge = seq_spill.into_merge()?;
    let mut seq_batch: Vec<(Term, Term)> = Vec::with_capacity(BATCH);
    while let Some((seq, val)) = merge.next()? {
        seq_batch.push((Term::Int(seq as i64), val));
        if seq_batch.len() >= BATCH {
            seq_root = btree::add_remove(
                &mut dst,
                &seq_root,
                Reducer::SeqTree,
                std::mem::take(&mut seq_batch),
                vec![],
            )?;
        }
    }
    if !seq_batch.is_empty() {
        seq_root = btree::add_remove(
            &mut dst,
            &seq_root,
            Reducer::SeqTree,
            std::mem::take(&mut seq_batch),
            vec![],
        )?;
    }
    drop(merge);
    let _ = std::fs::remove_file(&meta_path);

    // Local docs come over verbatim.
    let mut local_inserts: Vec<(Term, Term)> = Vec::new();
    btree::fold(&src.file, &src.local_root, None, &mut |k, v| {
        local_inserts.push((k.clone(), v.clone()));
        Ok(ControlFlow::Continue(()))
    })?;
    let locals = local_inserts.len();
    let local_root = btree::add_remove(&mut dst, &None, Reducer::None, local_inserts, vec![])?;

    header.id_tree_state = TreeState::to_term(&id_root);
    header.seq_tree_state = TreeState::to_term(&seq_root);
    header.local_tree_state = TreeState::to_term(&local_root);
    dst.sync()?;
    dst.write_header(&header.to_term())?;
    dst.sync()?;

    let old_size = src.file.eof;
    let new_size = dst.eof;
    drop(dst);
    std::fs::rename(&compact_path, path)?;

    Ok(json!({
        "ok": true,
        "docs": docs,
        "local_docs": locals,
        "attachments_copied": atts_copied,
        "size_before": old_size,
        "size_after": new_size,
    }))
}

/// External sort for seq-tree entries during compaction (couch_emsort's
/// job): entries accumulate in memory, get written out as sorted runs of
/// RUN_LEN to a scratch file, and come back as one ascending stream via a
/// k-way merge. Values are stored ETF-encoded; frame format per entry is
/// `[seq: u64-be][len: u32-be][etf bytes]`.
struct SeqSpill {
    path: String,
    file: std::fs::File,
    buf: Vec<(u64, Vec<u8>)>,
    runs: Vec<(u64, u64)>, // (start offset, entry count)
    pos: u64,
}

const RUN_LEN: usize = 65536;

impl SeqSpill {
    fn create(path: &str) -> Result<SeqSpill> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(SeqSpill {
            path: path.to_string(),
            file,
            buf: Vec::with_capacity(RUN_LEN),
            runs: Vec::new(),
            pos: 0,
        })
    }

    fn push(&mut self, seq: u64, val: &Term) -> Result<()> {
        self.buf.push((seq, crate::etf::encode(val)));
        if self.buf.len() >= RUN_LEN {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        use std::io::Write;
        if self.buf.is_empty() {
            return Ok(());
        }
        let mut entries = std::mem::take(&mut self.buf);
        entries.sort_by_key(|(seq, _)| *seq);
        let start = self.pos;
        let mut out = Vec::new();
        for (seq, bytes) in &entries {
            out.extend_from_slice(&seq.to_be_bytes());
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        self.file.write_all(&out)?;
        self.pos += out.len() as u64;
        self.runs.push((start, entries.len() as u64));
        self.buf = Vec::with_capacity(RUN_LEN);
        Ok(())
    }

    fn into_merge(mut self) -> Result<SeqMerge> {
        self.spill()?;
        use std::io::{Seek, SeekFrom, Write};
        self.file.flush()?;
        let mut heads = std::collections::BinaryHeap::new();
        let mut readers = Vec::new();
        for (i, (start, count)) in self.runs.iter().enumerate() {
            // Independent handle per run: try_clone would share one cursor.
            let mut f = std::fs::File::open(&self.path)?;
            f.seek(SeekFrom::Start(*start))?;
            let mut r = RunReader {
                file: std::io::BufReader::with_capacity(1 << 20, f),
                left: *count,
            };
            if let Some((seq, val)) = r.next()? {
                heads.push(std::cmp::Reverse((seq, i, val)));
            }
            readers.push(r);
        }
        Ok(SeqMerge { readers, heads })
    }
}

struct RunReader {
    file: std::io::BufReader<std::fs::File>,
    left: u64,
}

impl RunReader {
    fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        use std::io::Read;
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let mut hdr = [0u8; 12];
        self.file.read_exact(&mut hdr)?;
        let seq = u64::from_be_bytes(hdr[..8].try_into().unwrap());
        let len = u32::from_be_bytes(hdr[8..].try_into().unwrap()) as usize;
        let mut bytes = vec![0u8; len];
        self.file.read_exact(&mut bytes)?;
        Ok(Some((seq, bytes)))
    }
}

struct SeqMerge {
    readers: Vec<RunReader>,
    heads: std::collections::BinaryHeap<std::cmp::Reverse<(u64, usize, Vec<u8>)>>,
}

impl SeqMerge {
    fn next(&mut self) -> Result<Option<(u64, Term)>> {
        let Some(std::cmp::Reverse((seq, i, bytes))) = self.heads.pop() else {
            return Ok(None);
        };
        if let Some((nseq, nbytes)) = self.readers[i].next()? {
            self.heads.push(std::cmp::Reverse((nseq, i, nbytes)));
        }
        Ok(Some((seq, crate::etf::decode(&bytes)?)))
    }
}

/// Rewrite every stored leaf in the tree: copy attachment chunks, rebuild
/// the summary with new stream pointers, append it to `dst`, and point the
/// leaf (and its att size info) at the new locations.
fn copy_tree_bodies(
    src: &CouchFile,
    dst: &mut CouchFile,
    tree: &mut RevTree,
    atts_copied: &mut u64,
) -> Result<()> {
    let roots = std::mem::take(&mut tree.0);
    tree.0 = roots
        .into_iter()
        .map(|(start, mut node)| {
            copy_node(src, dst, &mut node, atts_copied)?;
            Ok((start, node))
        })
        .collect::<Result<_>>()?;
    Ok(())
}

fn copy_node(
    src: &CouchFile,
    dst: &mut CouchFile,
    node: &mut crate::revtree::RevNode,
    atts_copied: &mut u64,
) -> Result<()> {
    crate::maybe_grow(|| copy_node_inner(src, dst, node, atts_copied))
}

fn copy_node_inner(
    src: &CouchFile,
    dst: &mut CouchFile,
    node: &mut crate::revtree::RevNode,
    atts_copied: &mut u64,
) -> Result<()> {
    // couch_db_updater's compactor maps branch (interior) nodes to
    // ?REV_MISSING: only leaf revisions keep their bodies. Copying interior
    // summaries too can more than double the file (delete/recreate-churned
    // docs carry thousands of stored interior revisions).
    if !node.children.is_empty() {
        node.val = RevVal::Missing;
    }
    if let RevVal::Leaf(leaf) = &mut node.val {
        if let Some(ptr) = leaf.ptr {
            let summary = doc::read_summary(src, ptr)?;
            // Copy attachments, building new disk terms.
            let mut att_terms = Vec::new();
            let mut leaf_att_sizes = Vec::new();
            for att in &summary.atts {
                let data = doc::read_att_data(src, att)?;
                let (new_pos, _) = dst.append_chunk(&data)?;
                *atts_copied += 1;
                let sp = Term::List(vec![Term::Tuple(vec![
                    Term::Int(new_pos as i64),
                    Term::Int(data.len() as i64),
                ])]);
                att_terms.push(Term::Tuple(vec![
                    Term::Bin(att.name.as_bytes().to_vec()),
                    Term::Bin(att.content_type.as_bytes().to_vec()),
                    sp.clone(),
                    Term::Int(att.att_len as i64),
                    Term::Int(att.disk_len as i64),
                    Term::Int(att.revpos as i64),
                    Term::Bin(att.md5.clone()),
                    Term::Atom(att.encoding.clone()),
                ]));
                leaf_att_sizes.push(Term::Tuple(vec![sp, Term::Int(att.att_len as i64)]));
            }
            let body_bin = compress::compress(&summary.body);
            let atts_bin = compress::compress(&Term::List(att_terms));
            let summary_bin = crate::etf::encode(&Term::Tuple(vec![
                Term::Bin(body_bin),
                Term::Bin(atts_bin),
            ]));
            let (new_ptr, written) = dst.append_chunk_checksummed(&summary_bin)?;
            leaf.ptr = Some(new_ptr);
            leaf.sizes.0 = written as i64;
            leaf.atts = Term::List(leaf_att_sizes);
        }
    }
    for child in &mut node.children {
        copy_node(src, dst, child, atts_copied)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::Db;
    use crate::writer::{DbWriter, DocUpdate};
    use serde_json::json;

    #[test]
    fn compact_roundtrip() {
        let dir = std::env::temp_dir().join(format!("couch-store-c-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.couch");
        let _ = std::fs::remove_file(&path);
        let path = path.to_string_lossy().into_owned();

        let mut w = DbWriter::create(&path).unwrap();
        // churn: write everything twice so half the data is garbage
        for round in 0..2 {
            let docs: Vec<DocUpdate> = (0..300)
                .map(|i| {
                    DocUpdate::from_json(json!({
                        "_id": format!("d{i:04}"),
                        "_revisions": {"start": round + 1, "ids":
                            (0..=round).rev().map(|r| format!("{:032x}", i * 10 + r)).collect::<Vec<_>>()},
                        "round": round,
                        "pad": "x".repeat(500),
                    }))
                    .unwrap()
                })
                .collect();
            w.update_docs(docs).unwrap();
        }
        use base64::Engine;
        w.update_docs(vec![DocUpdate::from_json(json!({
            "_id": "att", "x": 1,
            "_attachments": {"a.bin": {"content_type": "application/octet-stream",
                "data": base64::engine::general_purpose::STANDARD.encode(vec![9u8; 50_000])}},
        }))
        .unwrap()])
        .unwrap();
        w.update_local(b"_local/ck", Some(&json!({"seq": 601}))).unwrap();
        w.commit().unwrap();
        let size_before = std::fs::metadata(&path).unwrap().len();
        let uuid_before = Db::open(&path).unwrap().header.uuid_str();

        let stats = super::compact(&path).unwrap();
        assert_eq!(stats["docs"], json!(301));
        assert_eq!(stats["local_docs"], json!(1));
        assert_eq!(stats["attachments_copied"], json!(1));

        let db = Db::open(&path).unwrap();
        assert!((db.file.eof) < size_before, "file should shrink");
        assert_eq!(db.header.uuid_str(), uuid_before);
        let (live, del, _, _) = db.doc_counts().unwrap();
        assert_eq!((live, del), (301, 0));
        let d = db.open_doc(b"d0042", None, &Default::default()).unwrap().unwrap();
        assert_eq!(d["round"], json!(1));
        assert!(d["_rev"].as_str().unwrap().starts_with("2-"));
        let a = db.open_doc(b"att", None, &crate::db::DocOpts { attachments: true, ..Default::default() })
            .unwrap().unwrap();
        assert_eq!(a["_attachments"]["a.bin"]["data"].as_str().unwrap().len(), 66668);
        let mut locals = 0;
        db.fold_local_docs(|_| { locals += 1; Ok(std::ops::ControlFlow::Continue(())) }).unwrap();
        assert_eq!(locals, 1);
        // changes intact
        let mut n = 0;
        db.fold_changes(0, |_| { n += 1; Ok(std::ops::ControlFlow::Continue(())) }).unwrap();
        assert_eq!(n, 301);
    }
}
