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
    /// Explicit attachment revpos (replicated docs carry one; absent means
    /// 0 for replicated writes, per couch_doc). None → the doc's rev pos.
    pub revpos: Option<u64>,
}

/// Placeholder leaf seq until the batch assigns per-doc seqs.
const PENDING_SEQ: u64 = u64::MAX;

fn assign_pending_seqs(tree: &mut RevTree, seq: u64) {
    fn walk(node: &mut crate::revtree::RevNode, seq: u64) {
        crate::maybe_grow(|| {
            if let RevVal::Leaf(l) = &mut node.val {
                if l.seq == PENDING_SEQ {
                    l.seq = seq;
                }
            }
            for c in &mut node.children {
                walk(c, seq);
            }
        })
    }
    for (_, root) in &mut tree.0 {
        walk(root, seq);
    }
}

/// An attachment carried by a doc update: fresh bytes to write, or a stub
/// pointing at data already in this file (interactive updates keep parent
/// attachments without rewriting them).
pub enum AttInput {
    Inline(NewAtt),
    Existing {
        /// The full attachment disk term, reused verbatim.
        term: Term,
        /// Its stream pointer and disk length, for the leaf att-size list.
        sp: Term,
        disk_len: i64,
    },
}

pub struct DocUpdate {
    pub id: Vec<u8>,
    /// (pos, [leaf_revid, parent_revid, ...])
    pub rev_path: (u64, Vec<Vec<u8>>),
    pub deleted: bool,
    pub body: Value,
    pub atts: Vec<AttInput>,
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
                let revpos = spec.get("revpos").and_then(|r| r.as_u64());
                atts.push(AttInput::Inline(NewAtt {
                    name,
                    content_type,
                    data,
                    revpos: Some(revpos.unwrap_or(0)),
                }));
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

/// Validation hook for interactive saves: (newDoc, oldWinnerDoc) → Err(reason)
/// rejects the write with 403 forbidden (the validate_doc_update contract).
pub type Validator<'a> = &'a dyn Fn(&Value, Option<&Value>) -> std::result::Result<(), String>;

/// Outcome of one interactive save: the new rev, or a per-doc error that maps
/// onto CouchDB's HTTP statuses (conflict/forbidden/missing_stub).
#[derive(Debug, Clone)]
pub enum SaveOutcome {
    Ok { rev: String },
    Error { error: String, reason: String },
}

impl SaveOutcome {
    pub fn conflict() -> SaveOutcome {
        SaveOutcome::error("conflict", "Document update conflict.")
    }
    pub fn error(error: impl Into<String>, reason: impl Into<String>) -> SaveOutcome {
        SaveOutcome::Error {
            error: error.into(),
            reason: reason.into(),
        }
    }
}

/// Rebuild the 8-tuple attachment disk term from parsed AttInfo (used to
/// carry a parent revision's attachment forward unchanged).
fn att_disk_term(a: &doc::AttInfo) -> Term {
    Term::Tuple(vec![
        Term::Bin(a.name.as_bytes().to_vec()),
        Term::Bin(a.content_type.as_bytes().to_vec()),
        sp_term(&a.chunks),
        Term::Int(a.att_len as i64),
        Term::Int(a.disk_len as i64),
        Term::Int(a.revpos as i64),
        Term::Bin(a.md5.clone()),
        Term::atom(&a.encoding),
    ])
}

fn sp_term(chunks: &[(u64, Option<u64>)]) -> Term {
    Term::List(
        chunks
            .iter()
            .map(|(pos, len)| match len {
                Some(l) => Term::Tuple(vec![Term::Int(*pos as i64), Term::Int(*l as i64)]),
                None => Term::Int(*pos as i64),
            })
            .collect(),
    )
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
        let mut id_order: Vec<Vec<u8>> = Vec::new();

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
                id_order.push(upd.id.clone());
            }

            let (pos, path) = &upd.rev_path;
            if *pos < path.len() as u64 {
                return Err(Error::BadRequest(format!(
                    "rev position {} shorter than history {}",
                    pos,
                    path.len()
                )));
            }

            // Write attachments, then the summary. The leaf gets a
            // placeholder seq; the real one is assigned per *doc* (not per
            // revision) once the whole batch is merged — couch_db_updater
            // gives every new leaf of a doc the same new update seq.
            let mut att_terms = Vec::new();
            let mut leaf_att_sizes = Vec::new();
            for att in &upd.atts {
                match att {
                    AttInput::Inline(att) => {
                        let (att_pos, _) = self.file.append_chunk(&att.data)?;
                        let sp = Term::List(vec![Term::Tuple(vec![
                            Term::Int(att_pos as i64),
                            Term::Int(att.data.len() as i64),
                        ])]);
                        let md5 = Md5::digest(&att.data).to_vec();
                        let revpos = att.revpos.unwrap_or(*pos);
                        att_terms.push(Term::Tuple(vec![
                            Term::Bin(att.name.as_bytes().to_vec()),
                            Term::Bin(att.content_type.as_bytes().to_vec()),
                            sp.clone(),
                            Term::Int(att.data.len() as i64),
                            Term::Int(att.data.len() as i64),
                            Term::Int(revpos as i64),
                            Term::Bin(md5),
                            Term::atom("identity"),
                        ]));
                        leaf_att_sizes
                            .push(Term::Tuple(vec![sp, Term::Int(att.data.len() as i64)]));
                    }
                    AttInput::Existing { term, sp, disk_len } => {
                        att_terms.push(term.clone());
                        leaf_att_sizes
                            .push(Term::Tuple(vec![sp.clone(), Term::Int(*disk_len)]));
                    }
                }
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
                seq: PENDING_SEQ,
                sizes: (written as i64, ejson::external_size(&upd.body) as i64),
                atts: Term::List(leaf_att_sizes),
            };
            let (start, chain) = path_to_tree(*pos, path, leaf);
            let (_, tree) = by_id.get_mut(&upd.id).expect("entry inserted above");
            let changed = tree.merge_path(start, chain);
            tree.stem(self.header.revs_limit);
            *changed_ids.entry(upd.id.clone()).or_insert(false) |= changed;
        }

        // Assign one new seq per changed doc (in batch first-touch order),
        // then build the btree operations.
        let mut id_inserts = Vec::new();
        let mut seq_inserts = Vec::new();
        let mut seq_removes = Vec::new();
        let mut n_changed = 0usize;
        for id in &id_order {
            let (old_seq, tree) = by_id.get_mut(id).expect("entry inserted above");
            let (old_seq, id) = (*old_seq, id.clone());
            if !changed_ids.get(&id).copied().unwrap_or(false) {
                continue;
            }
            n_changed += 1;
            self.update_seq += 1;
            assign_pending_seqs(tree, self.update_seq);
            let fdi_val = fdi_value(tree)?;
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

    /// Interactive (new_edits:true) save — the PUT/POST/DELETE doc semantics
    /// of couch_db:update_doc. Checks the given `_rev` against the current
    /// leaves, generates the next revid, inherits attachment stubs from the
    /// parent revision, and runs an optional validation hook against the old
    /// winner. Never creates conflicts.
    pub fn save_doc(&mut self, doc: &Value, validate: Option<Validator<'_>>) -> Result<SaveOutcome> {
        let obj = doc
            .as_object()
            .ok_or_else(|| Error::BadRequest("doc is not an object".into()))?;
        let id = match obj.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err(Error::BadRequest("_id must be a string".into())),
            None => return Err(Error::BadRequest("doc missing _id".into())),
        };
        if id.starts_with('_') && !id.starts_with("_design/") {
            return Ok(SaveOutcome::error(
                "bad_request",
                "Only reserved document ids may start with underscore.",
            ));
        }
        let rev = match obj.get("_rev") {
            Some(Value::String(r)) => Some(doc::parse_rev(r)?),
            Some(_) => return Err(Error::BadRequest("_rev must be a string".into())),
            None => None,
        };
        let deleted = matches!(obj.get("_deleted"), Some(Value::Bool(true)));

        // Current disk state and the parent leaf this edit extends.
        let key = Term::Bin(id.as_bytes().to_vec());
        let found = btree::lookup(&self.file, &self.id_root, std::slice::from_ref(&key))?
            .pop()
            .flatten();
        let fdi = match &found {
            Some(v) => Some(Db::fdi_from_id_kv(&key, v)?),
            None => None,
        };
        // (pos, revid, full ancestor path, leaf ptr, leaf deleted)
        let parent: Option<(u64, Vec<Vec<u8>>, Option<u64>, bool)> = match (&fdi, &rev) {
            (None, None) => None,
            (None, Some(_)) => return Ok(SaveOutcome::conflict()),
            (Some(f), maybe_rev) => {
                let leaves = f.rev_tree.leaves();
                let chosen = match maybe_rev {
                    Some((pos, revid)) => leaves
                        .into_iter()
                        .find(|l| l.pos == *pos && l.path[0] == revid.as_slice()),
                    None => {
                        // No rev given: only legal when the winner is a
                        // tombstone (recreate) — otherwise it's a conflict.
                        match f.rev_tree.winner() {
                            Some(w) if matches!(w.leaf, RevVal::Leaf(l) if l.deleted) => Some(w),
                            _ => return Ok(SaveOutcome::conflict()),
                        }
                    }
                };
                match chosen {
                    None => return Ok(SaveOutcome::conflict()),
                    Some(l) => {
                        let (ptr, was_deleted) = match l.leaf {
                            RevVal::Leaf(lv) => (lv.ptr, lv.deleted),
                            RevVal::Missing => (None, false),
                        };
                        Some((
                            l.pos,
                            l.path.iter().map(|r| r.to_vec()).collect(),
                            ptr,
                            was_deleted,
                        ))
                    }
                }
            }
        };

        // Validation runs against the parent revision's body (oldDoc).
        if let Some(validate) = validate {
            let old = match &parent {
                Some((pos, path, Some(ptr), was_deleted)) if !was_deleted => {
                    let summary = doc::read_summary(&self.file, *ptr)?;
                    let mut m = match ejson::to_json(&summary.body)? {
                        Value::Object(o) => o,
                        _ => Default::default(),
                    };
                    m.insert("_id".into(), Value::String(id.clone()));
                    m.insert("_rev".into(), Value::String(doc::rev_str(*pos, &path[0])));
                    Some(Value::Object(m))
                }
                _ => None,
            };
            if let Err(reason) = validate(doc, old.as_ref()) {
                return Ok(SaveOutcome::error("forbidden", reason));
            }
        }

        // Strip metadata; what remains is the stored body.
        let mut body = doc.clone();
        let bobj = body.as_object_mut().expect("checked above");
        for meta in [
            "_id", "_rev", "_revisions", "_deleted", "_attachments", "_conflicts",
            "_deleted_conflicts", "_revs_info", "_local_seq",
        ] {
            bobj.remove(meta);
        }

        // Resolve attachments: inline data is written fresh, stubs inherit
        // the parent revision's disk term.
        let parent_summary = match &parent {
            Some((_, _, Some(ptr), _)) => Some(doc::read_summary(&self.file, *ptr)?),
            _ => None,
        };
        let mut atts = Vec::new();
        let mut att_digest = Md5::new();
        if let Some(Value::Object(am)) = obj.get("_attachments") {
            for (name, spec) in am {
                let is_stub = matches!(spec.get("stub"), Some(Value::Bool(true)))
                    || matches!(spec.get("follows"), Some(Value::Bool(true)));
                if is_stub {
                    let Some(pa) = parent_summary
                        .as_ref()
                        .and_then(|s| s.atts.iter().find(|a| a.name == *name))
                    else {
                        return Ok(SaveOutcome::error(
                            "missing_stub",
                            format!("Attachment stub {name} referenced before inclusion"),
                        ));
                    };
                    att_digest.update(name.as_bytes());
                    att_digest.update(&pa.md5);
                    atts.push(AttInput::Existing {
                        term: att_disk_term(pa),
                        sp: sp_term(&pa.chunks),
                        disk_len: pa.disk_len as i64,
                    });
                } else {
                    use base64::Engine;
                    let data = spec
                        .get("data")
                        .and_then(|d| d.as_str())
                        .ok_or_else(|| {
                            Error::BadRequest(format!("attachment {name} has neither data nor stub"))
                        })?;
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .map_err(|e| {
                            Error::BadRequest(format!("attachment {name}: bad base64: {e}"))
                        })?;
                    att_digest.update(name.as_bytes());
                    att_digest.update(Md5::digest(&data));
                    atts.push(AttInput::Inline(NewAtt {
                        name: name.clone(),
                        content_type: spec
                            .get("content_type")
                            .and_then(|c| c.as_str())
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                        data,
                        revpos: None,
                    }));
                }
            }
        }

        // Deterministic new revid, like couch_db:new_revid: a digest over the
        // parent rev, deletion flag, body and attachment identities.
        let (new_pos, mut path) = match &parent {
            Some((pos, path, _, _)) => (*pos + 1, path.clone()),
            None => (1, Vec::new()),
        };
        let mut h = Md5::new();
        h.update(id.as_bytes());
        h.update([deleted as u8]);
        if let Some((pos, path, _, _)) = &parent {
            h.update(pos.to_le_bytes());
            h.update(&path[0]);
        }
        h.update(serde_json::to_string(&body).unwrap_or_default());
        h.update(att_digest.finalize());
        let new_revid = h.finalize().to_vec();
        path.insert(0, new_revid.clone());

        let n = self.update_docs(vec![DocUpdate {
            id: id.into_bytes(),
            rev_path: (new_pos, path),
            deleted,
            body,
            atts,
        }])?;
        debug_assert_eq!(n, 1);
        Ok(SaveOutcome::Ok {
            rev: doc::rev_str(new_pos, &new_revid),
        })
    }

    /// Replace the `_security` object (takes effect at the next commit).
    /// couch_db:purge_docs — physically remove the given leaf revisions.
    /// No tombstones, nothing replicates. A doc whose last leaf is purged
    /// vanishes from the by-id and by-seq trees; a doc with surviving leaves
    /// is reindexed under a fresh update_seq (its winner may have changed).
    /// Returns what was actually removed, per doc.
    pub fn purge_docs(
        &mut self,
        req: &[(Vec<u8>, Vec<(u64, Vec<u8>)>)],
    ) -> Result<Vec<(Vec<u8>, Vec<(u64, Vec<u8>)>)>> {
        let mut purged = Vec::new();
        let mut id_inserts = Vec::new();
        let mut id_removes = Vec::new();
        let mut seq_inserts = Vec::new();
        let mut seq_removes = Vec::new();
        for (id, revs) in req {
            let key = Term::Bin(id.clone());
            let found = btree::lookup(&self.file, &self.id_root, std::slice::from_ref(&key))?
                .pop()
                .flatten();
            let Some(v) = found else {
                purged.push((id.clone(), Vec::new()));
                continue;
            };
            let fdi = Db::fdi_from_id_kv(&key, &v)?;
            let mut tree = fdi.rev_tree;
            let removed = tree.remove_leaves(revs);
            if removed.is_empty() {
                purged.push((id.clone(), removed));
                continue;
            }
            seq_removes.push(Term::Int(fdi.update_seq as i64));
            if tree.leaves().is_empty() {
                id_removes.push(key);
            } else {
                self.update_seq += 1;
                if let Some((pos, revid)) = tree.winner().map(|w| (w.pos, w.path[0].to_vec())) {
                    tree.set_leaf_seq(pos, &revid, self.update_seq);
                }
                let fdi_val = fdi_value(&tree)?;
                seq_inserts.push((Term::Int(fdi_val.update_seq as i64), seq_value(id, &fdi_val)));
                id_inserts.push((Term::Bin(id.clone()), fdi_val.id_term));
            }
            purged.push((id.clone(), removed));
        }
        self.id_root = btree::add_remove(
            &mut self.file,
            &self.id_root,
            Reducer::IdTree,
            id_inserts,
            id_removes,
        )?;
        self.seq_root = btree::add_remove(
            &mut self.file,
            &self.seq_root,
            Reducer::SeqTree,
            seq_inserts,
            seq_removes,
        )?;
        Ok(purged)
    }

    pub fn set_security(&mut self, v: &Value) -> Result<()> {
        // Term has a manual Drop, so the tuple's payload can't be moved out
        // by pattern; unwrap the 1-tuple via a mutable borrow instead.
        let mut inner = ejson::from_json(v);
        if let Term::Tuple(t) = &mut inner {
            if t.len() == 1 {
                let first = t.remove(0);
                inner = first;
            }
        }
        let (ptr, _) = self.file.append_term(&inner)?;
        self.header.security_ptr = Term::Int(ptr as i64);
        Ok(())
    }

    /// Write (or delete) a `_local/...` doc. Returns the new local rev.
    pub fn update_local(&mut self, id: &[u8], body: Option<&Value>) -> Result<String> {
        match body {
            None => {
                self.local_root = btree::add_remove(
                    &mut self.file,
                    &self.local_root,
                    Reducer::None,
                    vec![],
                    vec![Term::Bin(id.to_vec())],
                )?;
                Ok("0-0".into())
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
                Ok(format!("0-{}", rev + 1))
            }
        }
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
    fn interactive_save_semantics() {
        let path = tmppath("isv.couch");
        let mut w = DbWriter::create(&path).unwrap();

        // First save: no rev → 1-
        let SaveOutcome::Ok { rev: r1 } =
            w.save_doc(&json!({"_id": "a", "v": 1}), None).unwrap()
        else {
            panic!("first save rejected")
        };
        assert!(r1.starts_with("1-"));

        // Save without rev on an existing live doc → conflict.
        assert!(matches!(
            w.save_doc(&json!({"_id": "a", "v": 2}), None).unwrap(),
            SaveOutcome::Error { error, .. } if error == "conflict"
        ));
        // Save with a bogus rev → conflict.
        assert!(matches!(
            w.save_doc(&json!({"_id": "a", "_rev": "1-00000000000000000000000000000000", "v": 2}), None)
                .unwrap(),
            SaveOutcome::Error { error, .. } if error == "conflict"
        ));
        // Proper update chain.
        let SaveOutcome::Ok { rev: r2 } = w
            .save_doc(&json!({"_id": "a", "_rev": r1, "v": 2}), None)
            .unwrap()
        else {
            panic!()
        };
        assert!(r2.starts_with("2-"));
        // Stale rev now conflicts.
        assert!(matches!(
            w.save_doc(&json!({"_id": "a", "_rev": r1, "v": 9}), None).unwrap(),
            SaveOutcome::Error { error, .. } if error == "conflict"
        ));
        // Delete, then recreate without rev (extends the tombstone).
        let SaveOutcome::Ok { rev: r3 } = w
            .save_doc(&json!({"_id": "a", "_rev": r2, "_deleted": true}), None)
            .unwrap()
        else {
            panic!()
        };
        assert!(r3.starts_with("3-"));
        let SaveOutcome::Ok { rev: r4 } =
            w.save_doc(&json!({"_id": "a", "v": 3}), None).unwrap()
        else {
            panic!()
        };
        assert!(r4.starts_with("4-"));
        w.commit().unwrap();

        let db = Db::open(&path).unwrap();
        let doc = db.open_doc(b"a", None, &Default::default()).unwrap().unwrap();
        assert_eq!(doc["v"], json!(3));
        assert_eq!(doc["_rev"], json!(r4));
        // Exactly one branch — interactive edits never fork the tree.
        let fdi = db.open_doc_info(b"a").unwrap().unwrap();
        assert_eq!(fdi.rev_tree.leaves().len(), 1);
    }

    #[test]
    fn interactive_stub_inheritance_and_validation() {
        let path = tmppath("istub.couch");
        let mut w = DbWriter::create(&path).unwrap();
        use base64::Engine;
        let data = b"protobuf-blob-bytes".to_vec();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&data);

        let SaveOutcome::Ok { rev: r1 } = w
            .save_doc(
                &json!({"_id": "d", "meta": 1,
                    "_attachments": {"blob.data": {"content_type": "application/octet-stream", "data": b64}}}),
                None,
            )
            .unwrap()
        else {
            panic!()
        };
        // Update the body, carrying the attachment as a stub.
        let SaveOutcome::Ok { rev: r2 } = w
            .save_doc(
                &json!({"_id": "d", "_rev": r1, "meta": 2,
                    "_attachments": {"blob.data": {"stub": true}}}),
                None,
            )
            .unwrap()
        else {
            panic!()
        };
        // Unknown stub → missing_stub.
        assert!(matches!(
            w.save_doc(
                &json!({"_id": "d", "_rev": r2, "meta": 3,
                    "_attachments": {"nope.bin": {"stub": true}}}),
                None
            )
            .unwrap(),
            SaveOutcome::Error { error, .. } if error == "missing_stub"
        ));
        // Validator sees old doc and can reject.
        let validate: &dyn Fn(&Value, Option<&Value>) -> std::result::Result<(), String> =
            &|new, old| {
                assert_eq!(old.unwrap()["meta"], json!(2));
                if new["meta"] == json!(13) {
                    Err("unlucky".into())
                } else {
                    Ok(())
                }
            };
        assert!(matches!(
            w.save_doc(&json!({"_id": "d", "_rev": r2, "meta": 13}), Some(validate))
                .unwrap(),
            SaveOutcome::Error { error, reason } if error == "forbidden" && reason == "unlucky"
        ));
        w.commit().unwrap();

        let db = Db::open(&path).unwrap();
        let doc = db
            .open_doc(
                b"d",
                None,
                &crate::db::DocOpts {
                    attachments: true,
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(doc["meta"], json!(2));
        let att = &doc["_attachments"]["blob.data"];
        // revpos stays at the attachment's original revision.
        assert_eq!(att["revpos"], json!(1));
        assert_eq!(
            att["data"],
            json!(base64::engine::general_purpose::STANDARD.encode(&data))
        );
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
