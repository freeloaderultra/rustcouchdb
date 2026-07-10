//! Write-side engine: create or append to a .couch file the way
//! couch_db_updater + couch_bt_engine do. Writes are replicated-changes
//! style (new_edits:false): each incoming doc carries its full rev path,
//! which is merged into the existing rev tree.

use crate::btree::{self, Reducer};
use crate::db::Db;
use crate::doc;
use crate::ejson;
use crate::error::{Error, Result};
use crate::etf::Term;
use crate::file::CouchFile;
use crate::header::{DbHeader, TreeState};
use crate::revtree::{path_to_tree, LeafVal, RevTree, RevVal};
use md5::{Digest, Md5};
use serde_json::Value;
use std::collections::BTreeMap;

pub struct NewAtt {
    pub name: String,
    pub content_type: String,
    pub data: Vec<u8>,
}

pub struct DocUpdate {
    pub id: Vec<u8>,
    /// (pos, [leaf_revid, parent_revid, ...])
    pub rev_path: (u64, Vec<Vec<u8>>),
    pub deleted: bool,
    pub body: Value,
    pub atts: Vec<NewAtt>,
}

impl DocUpdate {
    /// Build a DocUpdate from a full JSON doc (`_id`, optional `_rev` /
    /// `_revisions` / `_deleted` / `_attachments` with base64 `data`).
    /// Docs without a rev get a deterministic generated 1-rev.
    pub fn from_json(mut v: Value) -> Result<DocUpdate> {
        let obj = v
            .as_object_mut()
            .ok_or_else(|| Error::BadRequest("doc is not an object".into()))?;
        let id = match obj.remove("_id") {
            Some(Value::String(s)) => s.into_bytes(),
            _ => return Err(Error::BadRequest("doc missing _id".into())),
        };
        let deleted = matches!(obj.remove("_deleted"), Some(Value::Bool(true)));
        let revisions = obj.remove("_revisions");
        let rev = obj.remove("_rev");
        let atts_json = obj.remove("_attachments");
        // drop other underscore metadata that isn't body content
        obj.remove("_conflicts");
        obj.remove("_deleted_conflicts");
        obj.remove("_revs_info");
        obj.remove("_local_seq");

        let mut atts = Vec::new();
        if let Some(Value::Object(am)) = atts_json {
            use base64::Engine;
            for (name, spec) in am {
                let data = spec
                    .get("data")
                    .and_then(|d| d.as_str())
                    .ok_or_else(|| {
                        Error::BadRequest(format!(
                            "attachment {name} has no inline data (stubs not supported)"
                        ))
                    })?;
                let data = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|e| Error::BadRequest(format!("attachment {name}: bad base64: {e}")))?;
                let content_type = spec
                    .get("content_type")
                    .and_then(|c| c.as_str())
                    .unwrap_or("application/octet-stream")
                    .to_string();
                atts.push(NewAtt {
                    name,
                    content_type,
                    data,
                });
            }
        }

        let rev_path = if let Some(revs) = revisions {
            let start = revs
                .get("start")
                .and_then(|s| s.as_u64())
                .ok_or_else(|| Error::BadRequest("bad _revisions.start".into()))?;
            let ids = revs
                .get("ids")
                .and_then(|i| i.as_array())
                .ok_or_else(|| Error::BadRequest("bad _revisions.ids".into()))?;
            let path: Vec<Vec<u8>> = ids
                .iter()
                .map(|r| {
                    r.as_str()
                        .map(doc::parse_revid)
                        .ok_or_else(|| Error::BadRequest("bad _revisions id".into()))
                })
                .collect::<Result<_>>()?;
            if path.is_empty() {
                return Err(Error::BadRequest("_revisions.ids is empty".into()));
            }
            (start, path)
        } else if let Some(Value::String(r)) = rev {
            let (pos, revid) = doc::parse_rev(&r)?;
            (pos, vec![revid])
        } else {
            // Deterministic generated first rev: md5 over id + body + deleted.
            let mut h = Md5::new();
            h.update(&id);
            h.update(serde_json::to_string(&v).unwrap_or_default());
            h.update([deleted as u8]);
            (1, vec![h.finalize().to_vec()])
        };

        Ok(DocUpdate {
            id,
            rev_path,
            deleted,
            body: v,
            atts,
        })
    }
}

pub struct DbWriter {
    pub file: CouchFile,
    pub header: DbHeader,
    id_root: Option<TreeState>,
    seq_root: Option<TreeState>,
    local_root: Option<TreeState>,
    update_seq: u64,
}

impl DbWriter {
    pub fn create(path: &str) -> Result<DbWriter> {
        let mut file = CouchFile::create(path)?;
        let header = DbHeader::new(gen_uuid());
        file.write_header(&header.to_term())?;
        Ok(DbWriter {
            file,
            header,
            id_root: None,
            seq_root: None,
            local_root: None,
            update_seq: 0,
        })
    }

    pub fn open(path: &str) -> Result<DbWriter> {
        let file = CouchFile::open_rw(path)?;
        let header = DbHeader::from_term(&file.read_header()?)?;
        Ok(DbWriter {
            id_root: TreeState::from_term(&header.id_tree_state)?,
            seq_root: TreeState::from_term(&header.seq_tree_state)?,
            local_root: TreeState::from_term(&header.local_tree_state)?,
            update_seq: header.update_seq,
            file,
            header,
        })
    }

    pub fn update_seq(&self) -> u64 {
        self.update_seq
    }

    /// Apply a batch of replicated-changes updates. Returns the number of
    /// docs whose rev tree actually changed.
    pub fn update_docs(&mut self, updates: Vec<DocUpdate>) -> Result<usize> {
        // Latest state per id within this batch (docs may repeat).
        let mut by_id: BTreeMap<Vec<u8>, (Option<u64>, RevTree)> = BTreeMap::new();
        let mut changed_ids: BTreeMap<Vec<u8>, bool> = BTreeMap::new();

        for upd in updates {
            let entry = match by_id.get(&upd.id) {
                Some(_) => None,
                None => {
                    // First touch of this id in the batch: load from disk.
                    let key = Term::Bin(upd.id.clone());
                    let found =
                        btree::lookup(&self.file, &self.id_root, std::slice::from_ref(&key))?
                            .pop()
                            .flatten();
                    Some(match found {
                        Some(v) => {
                            let fdi = Db::fdi_from_id_kv(&key, &v)?;
                            (Some(fdi.update_seq), fdi.rev_tree)
                        }
                        None => (None, RevTree::default()),
                    })
                }
            };
            if let Some(e) = entry {
                by_id.insert(upd.id.clone(), e);
            }

            let (pos, path) = &upd.rev_path;
            if *pos < path.len() as u64 {
                return Err(Error::BadRequest(format!(
                    "rev position {} shorter than history {}",
                    pos,
                    path.len()
                )));
            }

            // Write attachments, then the summary.
            self.update_seq += 1;
            let seq = self.update_seq;
            let mut att_terms = Vec::new();
            let mut leaf_att_sizes = Vec::new();
            for att in &upd.atts {
                let (att_pos, _) = self.file.append_chunk(&att.data)?;
                let sp = Term::List(vec![Term::Tuple(vec![
                    Term::Int(att_pos as i64),
                    Term::Int(att.data.len() as i64),
                ])]);
                let md5 = Md5::digest(&att.data).to_vec();
                att_terms.push(Term::Tuple(vec![
                    Term::Bin(att.name.as_bytes().to_vec()),
                    Term::Bin(att.content_type.as_bytes().to_vec()),
                    sp.clone(),
                    Term::Int(att.data.len() as i64),
                    Term::Int(att.data.len() as i64),
                    Term::Int(*pos as i64),
                    Term::Bin(md5),
                    Term::atom("identity"),
                ]));
                leaf_att_sizes.push(Term::Tuple(vec![sp, Term::Int(att.data.len() as i64)]));
            }
            let body_term = ejson::from_json(&upd.body);
            let body_bin = crate::compress::compress(&body_term);
            let atts_bin = crate::compress::compress(&Term::List(att_terms));
            let summary = crate::etf::encode(&Term::Tuple(vec![
                Term::Bin(body_bin),
                Term::Bin(atts_bin),
            ]));
            let (ptr, written) = self.file.append_chunk_checksummed(&summary)?;

            let leaf = LeafVal {
                deleted: upd.deleted,
                ptr: Some(ptr),
                seq,
                sizes: (written as i64, ejson::external_size(&upd.body) as i64),
                atts: Term::List(leaf_att_sizes),
            };
            let (start, chain) = path_to_tree(*pos, path, leaf);
            let (_, tree) = by_id.get_mut(&upd.id).expect("entry inserted above");
            let changed = tree.merge_path(start, chain);
            tree.stem(self.header.revs_limit);
            *changed_ids.entry(upd.id.clone()).or_insert(false) |= changed;
        }

        // Build btree operations.
        let mut id_inserts = Vec::new();
        let mut seq_inserts = Vec::new();
        let mut seq_removes = Vec::new();
        let mut n_changed = 0usize;
        for (id, (old_seq, tree)) in by_id {
            if !changed_ids.get(&id).copied().unwrap_or(false) {
                continue;
            }
            n_changed += 1;
            let fdi_val = fdi_value(&tree)?;
            let update_seq = fdi_val.update_seq;
            seq_inserts.push((Term::Int(update_seq as i64), seq_value(&id, &fdi_val)));
            id_inserts.push((Term::Bin(id.clone()), fdi_val.id_term));
            if let Some(s) = old_seq {
                seq_removes.push(Term::Int(s as i64));
            }
        }
        self.id_root = btree::add_remove(
            &mut self.file,
            &self.id_root,
            Reducer::IdTree,
            id_inserts,
            vec![],
        )?;
        self.seq_root = btree::add_remove(
            &mut self.file,
            &self.seq_root,
            Reducer::SeqTree,
            seq_inserts,
            seq_removes,
        )?;
        Ok(n_changed)
    }

    /// Write (or delete) a `_local/...` doc.
    pub fn update_local(&mut self, id: &[u8], body: Option<&Value>) -> Result<()> {
        match body {
            None => {
                self.local_root = btree::add_remove(
                    &mut self.file,
                    &self.local_root,
                    Reducer::None,
                    vec![],
                    vec![Term::Bin(id.to_vec())],
                )?;
            }
            Some(v) => {
                let mut v = v.clone();
                let mut rev: i64 = 0;
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("_id");
                    if let Some(Value::String(r)) = obj.remove("_rev") {
                        // local revs look like "0-N"
                        if let Some((_, n)) = r.split_once('-') {
                            rev = n.parse().unwrap_or(0);
                        }
                    }
                }
                let val = Term::Tuple(vec![Term::Int(rev + 1), ejson::from_json(&v)]);
                self.local_root = btree::add_remove(
                    &mut self.file,
                    &self.local_root,
                    Reducer::None,
                    vec![(Term::Bin(id.to_vec()), val)],
                    vec![],
                )?;
            }
        }
        Ok(())
    }

    /// Write the header and fsync — the durable commit point.
    pub fn commit(&mut self) -> Result<()> {
        self.header.update_seq = self.update_seq;
        self.header.id_tree_state = TreeState::to_term(&self.id_root);
        self.header.seq_tree_state = TreeState::to_term(&self.seq_root);
        self.header.local_tree_state = TreeState::to_term(&self.local_root);
        self.file.sync()?;
        self.file.write_header(&self.header.to_term())?;
        self.file.sync()?;
        Ok(())
    }
}

struct FdiValue {
    id_term: Term,
    update_seq: u64,
    deleted: bool,
    sizes: (i64, i64),
    tree_term: Term,
}

/// couch_bt_engine:id_tree_split — derive the FDI btree value from the tree.
fn fdi_value(tree: &RevTree) -> Result<FdiValue> {
    let leaves = tree.leaves();
    let mut max_seq = 0u64;
    let (mut active, mut external) = (0i64, 0i64);
    let mut att_sizes: Vec<(Term, i64)> = Vec::new();
    for l in &leaves {
        if let RevVal::Leaf(lv) = l.leaf {
            max_seq = max_seq.max(lv.seq);
            active += lv.sizes.0;
            external += lv.sizes.1;
            for a in lv.atts.as_list().unwrap_or(&[]) {
                if let Ok(pair) = a.tuple_n(2) {
                    let sz = pair[1].as_i64().unwrap_or(0);
                    if !att_sizes.iter().any(|(sp, _)| *sp == pair[0]) {
                        att_sizes.push((pair[0].clone(), sz));
                    }
                }
            }
        }
    }
    let total_atts: i64 = att_sizes.iter().map(|(_, s)| *s).sum();
    let deleted = match tree.winner() {
        Some(w) => matches!(w.leaf, RevVal::Leaf(l) if l.deleted),
        None => true,
    };
    let sizes = (active + total_atts, external + total_atts);
    let tree_term = tree.to_term();
    Ok(FdiValue {
        id_term: Term::Tuple(vec![
            Term::Int(max_seq as i64),
            Term::Int(deleted as i64),
            Term::Tuple(vec![Term::Int(sizes.0), Term::Int(sizes.1)]),
            tree_term.clone(),
        ]),
        update_seq: max_seq,
        deleted,
        sizes,
        tree_term,
    })
}

/// couch_bt_engine:seq_tree_split value.
fn seq_value(id: &[u8], fdi: &FdiValue) -> Term {
    Term::Tuple(vec![
        Term::Bin(id.to_vec()),
        Term::Int(fdi.deleted as i64),
        Term::Tuple(vec![Term::Int(fdi.sizes.0), Term::Int(fdi.sizes.1)]),
        fdi.tree_term.clone(),
    ])
}

fn gen_uuid() -> String {
    let mut h = Md5::new();
    h.update(std::process::id().to_le_bytes());
    h.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    let d = h.finalize();
    d.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmppath(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("couch-store-w-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(name);
        let _ = std::fs::remove_file(&p);
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn write_read_roundtrip() {
        let path = tmppath("rt.couch");
        {
            let mut w = DbWriter::create(&path).unwrap();
            let docs: Vec<DocUpdate> = (0..500)
                .map(|i| {
                    DocUpdate::from_json(json!({
                        "_id": format!("doc-{i:04}"),
                        "value": i,
                        "nested": {"list": [1, 2, 3], "s": "täxt"},
                    }))
                    .unwrap()
                })
                .collect();
            let n = w.update_docs(docs).unwrap();
            assert_eq!(n, 500);
            w.update_local(b"_local/ckpt", Some(&json!({"seq": 500})))
                .unwrap();
            w.commit().unwrap();
        }
        let db = Db::open(&path).unwrap();
        let (live, del, _, _) = db.doc_counts().unwrap();
        assert_eq!((live, del), (500, 0));
        assert_eq!(db.header.update_seq, 500);

        let doc = db
            .open_doc(b"doc-0042", None, &Default::default())
            .unwrap()
            .unwrap();
        assert_eq!(doc["value"], json!(42));
        assert_eq!(doc["nested"]["s"], json!("täxt"));
        assert!(doc["_rev"].as_str().unwrap().starts_with("1-"));

        let mut locals = Vec::new();
        db.fold_local_docs(|d| {
            locals.push(d);
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
        assert_eq!(locals.len(), 1);
        assert_eq!(locals[0]["seq"], json!(500));

        // changes fold sees every doc once
        let mut n = 0;
        db.fold_changes(0, |_| {
            n += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
        assert_eq!(n, 500);
    }

    #[test]
    fn update_delete_conflict() {
        let path = tmppath("udc.couch");
        let mut w = DbWriter::create(&path).unwrap();
        w.update_docs(vec![DocUpdate::from_json(
            json!({"_id": "a", "_rev": "1-aaa", "v": 1}),
        )
        .unwrap()])
        .unwrap();
        // update to 2-bbb with history
        w.update_docs(vec![DocUpdate::from_json(json!({
            "_id": "a", "v": 2,
            "_revisions": {"start": 2, "ids": ["bbb", "aaa"]},
        }))
        .unwrap()])
        .unwrap();
        // conflicting branch 2-ccc
        w.update_docs(vec![DocUpdate::from_json(json!({
            "_id": "a", "v": 3,
            "_revisions": {"start": 2, "ids": ["ccc", "aaa"]},
        }))
        .unwrap()])
        .unwrap();
        // duplicate write is a no-op
        let n = w
            .update_docs(vec![DocUpdate::from_json(json!({
                "_id": "a", "v": 3,
                "_revisions": {"start": 2, "ids": ["ccc", "aaa"]},
            }))
            .unwrap()])
            .unwrap();
        assert_eq!(n, 0);
        // delete winner branch
        w.update_docs(vec![DocUpdate::from_json(json!({
            "_id": "a", "_deleted": true,
            "_revisions": {"start": 3, "ids": ["ddd", "ccc"]},
        }))
        .unwrap()])
        .unwrap();
        w.commit().unwrap();

        let db = Db::open(&path).unwrap();
        let (live, del, _, _) = db.doc_counts().unwrap();
        assert_eq!((live, del), (1, 0)); // still one live branch (2-bbb)
        let doc = db
            .open_doc(
                b"a",
                None,
                &crate::db::DocOpts {
                    conflicts: true,
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(doc["_rev"], json!("2-bbb"));
        assert_eq!(doc["v"], json!(2));
        assert_eq!(doc["_deleted_conflicts"], json!(["3-ddd"]));
    }

    #[test]
    fn attachments_roundtrip() {
        let path = tmppath("att.couch");
        let data = vec![42u8; 100_000];
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
        let mut w = DbWriter::create(&path).unwrap();
        w.update_docs(vec![DocUpdate::from_json(json!({
            "_id": "withatt",
            "field": true,
            "_attachments": {"blob.bin": {"content_type": "application/x-blob", "data": b64}},
        }))
        .unwrap()])
        .unwrap();
        w.commit().unwrap();

        let db = Db::open(&path).unwrap();
        let doc = db
            .open_doc(b"withatt", None, &Default::default())
            .unwrap()
            .unwrap();
        let att = &doc["_attachments"]["blob.bin"];
        assert_eq!(att["length"], json!(100_000));
        assert_eq!(att["stub"], json!(true));

        // read the data back
        let fdi = db.open_doc_info(b"withatt").unwrap().unwrap();
        let tree = fdi.rev_tree.clone();
        let w = tree.winner().unwrap();
        let crate::revtree::RevVal::Leaf(lv) = w.leaf else {
            panic!()
        };
        let summary = crate::doc::read_summary(&db.file, lv.ptr.unwrap()).unwrap();
        let att = db.find_att(&summary, "blob.bin").unwrap();
        let back = crate::doc::read_att_data(&db.file, att).unwrap();
        assert_eq!(back, data);
        assert_eq!(att.md5, Md5::digest(&data).to_vec());
    }
}
