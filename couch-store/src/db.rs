//! Read-side database engine: open a .couch file and serve docs, changes,
//! local docs, attachments and info — the couch_bt_engine read API.

use crate::btree::{self, Reducer};
use crate::doc::{self, AttInfo, Summary};
use crate::ejson;
use crate::error::{corrupt, Error, Result};
use crate::etf::Term;
use crate::file::CouchFile;
use crate::header::{DbHeader, TreeState};
use crate::revtree::{LeafPath, RevTree, RevVal};
use serde_json::{json, Map, Value};
use std::ops::ControlFlow;

pub struct Db {
    pub file: CouchFile,
    pub header: DbHeader,
    pub id_root: Option<TreeState>,
    pub seq_root: Option<TreeState>,
    pub local_root: Option<TreeState>,
    pub purge_root: Option<TreeState>,
    pub purge_seq_root: Option<TreeState>,
}

#[derive(Clone, Debug)]
pub struct FullDocInfo {
    pub id: Vec<u8>,
    pub update_seq: u64,
    pub deleted: bool,
    pub sizes: (i64, i64),
    pub rev_tree: RevTree,
}

impl Db {
    pub fn open(path: &str) -> Result<Db> {
        let file = CouchFile::open_read(path)?;
        let header = DbHeader::from_term(&file.read_header()?)?;
        Ok(Db {
            id_root: TreeState::from_term(&header.id_tree_state)?,
            seq_root: TreeState::from_term(&header.seq_tree_state)?,
            local_root: TreeState::from_term(&header.local_tree_state)?,
            purge_root: TreeState::from_term(&header.purge_tree_state)?,
            purge_seq_root: TreeState::from_term(&header.purge_seq_tree_state)?,
            file,
            header,
        })
    }

    pub fn doc_counts(&self) -> Result<(u64, u64, i64, i64)> {
        let red = btree::full_reduce(&self.id_root, Reducer::IdTree)?;
        let t = red.as_tuple()?;
        let live = t[0].as_u64()?;
        let del = t[1].as_u64()?;
        let (active, external) = match t.get(2) {
            Some(sz) => btree::split_sizes(sz).unwrap_or((0, 0)),
            None => (0, 0),
        };
        Ok((live, del, active, external))
    }

    pub fn purge_seq(&self) -> Result<u64> {
        // Highest key in the purge_seq_tree; 0 when empty.
        let mut seq = 0u64;
        btree::fold(&self.file, &self.purge_seq_root, None, &mut |k, _v| {
            seq = k.as_u64()?;
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(seq)
    }

    pub fn info(&self) -> Result<Value> {
        let (live, del, active, external) = self.doc_counts()?;
        Ok(json!({
            "doc_count": live,
            "doc_del_count": del,
            "update_seq": self.header.update_seq,
            "purge_seq": self.purge_seq()?,
            "compacted_seq": self.header.compacted_seq.as_u64().unwrap_or(0),
            "disk_format_version": self.header.disk_version,
            "revs_limit": self.header.revs_limit,
            "uuid": self.header.uuid_str(),
            "sizes": {
                "active": active,
                "external": external,
                "file": self.file.eof,
            },
        }))
    }

    pub fn security(&self) -> Result<Value> {
        match &self.header.security_ptr {
            Term::Int(ptr) => {
                let t = self.file.read_term(*ptr as u64)?;
                // Stored as a bare proplist; wrap as an EJSON object.
                ejson::to_json(&Term::Tuple(vec![t]))
            }
            _ => Ok(json!({})),
        }
    }

    pub fn fdi_from_id_kv(key: &Term, val: &Term) -> Result<FullDocInfo> {
        let t = val.as_tuple()?;
        if t.len() < 3 {
            return Err(corrupt("short id_tree value"));
        }
        // {Seq, Deleted, Sizes, DiskTree} or legacy {Seq, Deleted, DiskTree}
        let (sizes, tree_idx) = if t.len() == 4 {
            (btree::split_sizes(&t[2]).unwrap_or((0, 0)), 3)
        } else {
            ((0, 0), 2)
        };
        Ok(FullDocInfo {
            id: key.as_bin()?.to_vec(),
            update_seq: t[0].as_u64()?,
            deleted: t[1].as_i64()? != 0,
            sizes,
            rev_tree: RevTree::from_term(&t[tree_idx])?,
        })
    }

    pub fn fdi_from_seq_kv(key: &Term, val: &Term) -> Result<FullDocInfo> {
        let t = val.as_tuple()?;
        if t.len() < 3 {
            return Err(corrupt("short seq_tree value"));
        }
        let (sizes, tree_idx) = if t.len() == 4 {
            (btree::split_sizes(&t[2]).unwrap_or((0, 0)), 3)
        } else {
            ((0, 0), 2)
        };
        Ok(FullDocInfo {
            id: t[0].as_bin()?.to_vec(),
            update_seq: key.as_u64()?,
            deleted: t[1].as_i64()? != 0,
            sizes,
            rev_tree: RevTree::from_term(&t[tree_idx])?,
        })
    }

    pub fn fold_docs<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(FullDocInfo) -> Result<ControlFlow<()>>,
    {
        btree::fold(&self.file, &self.id_root, None, &mut |k, v| {
            f(Self::fdi_from_id_kv(k, v)?)
        })
    }

    pub fn fold_changes<F>(&self, since: u64, mut f: F) -> Result<()>
    where
        F: FnMut(FullDocInfo) -> Result<ControlFlow<()>>,
    {
        let start = Term::Int(since as i64 + 1);
        btree::fold(&self.file, &self.seq_root, Some(&start), &mut |k, v| {
            f(Self::fdi_from_seq_kv(k, v)?)
        })
    }

    pub fn open_doc_info(&self, id: &[u8]) -> Result<Option<FullDocInfo>> {
        let key = Term::Bin(id.to_vec());
        let res = btree::lookup(&self.file, &self.id_root, std::slice::from_ref(&key))?;
        match &res[0] {
            None => Ok(None),
            Some(v) => Ok(Some(Self::fdi_from_id_kv(&key, v)?)),
        }
    }

    /// Assemble the JSON document for one leaf.
    pub fn doc_json(&self, fdi: &FullDocInfo, leaf: &LeafPath<'_>, opts: &DocOpts) -> Result<Value> {
        let RevVal::Leaf(lv) = leaf.leaf else {
            return Err(Error::BadRequest(format!(
                "rev {} of {} is missing (stemmed or purged)",
                doc::rev_str(leaf.pos, leaf.path[0]),
                String::from_utf8_lossy(&fdi.id)
            )));
        };
        let mut m = Map::new();
        m.insert(
            "_id".into(),
            Value::String(
                String::from_utf8(fdi.id.clone()).map_err(|_| corrupt("non-UTF-8 doc id"))?,
            ),
        );
        m.insert(
            "_rev".into(),
            Value::String(doc::rev_str(leaf.pos, leaf.path[0])),
        );
        if lv.deleted {
            m.insert("_deleted".into(), Value::Bool(true));
        }
        let summary: Option<Summary> = match lv.ptr {
            Some(p) => Some(doc::read_summary(&self.file, p)?),
            None => None,
        };
        if let Some(s) = &summary {
            let body = ejson::to_json(&s.body)?;
            match body {
                Value::Object(o) => m.extend(o),
                Value::Array(a) if a.is_empty() => {} // make_doc's body=[] for nil ptr
                other => return Err(corrupt(format!("doc body is not an object: {other}"))),
            }
            if !s.atts.is_empty() {
                let mut atts = Map::new();
                for att in &s.atts {
                    atts.insert(
                        att.name.clone(),
                        doc::att_json(&self.file, att, opts.attachments)?,
                    );
                }
                m.insert("_attachments".into(), Value::Object(atts));
            }
        }
        if opts.revs {
            let ids: Vec<Value> = leaf
                .path
                .iter()
                .map(|r| Value::String(doc::revid_str(r)))
                .collect();
            m.insert(
                "_revisions".into(),
                json!({"start": leaf.pos, "ids": ids}),
            );
        }
        if opts.conflicts {
            let leaves = fdi.rev_tree.leaves();
            let mut conflicts = Vec::new();
            let mut deleted_conflicts = Vec::new();
            for l in &leaves {
                if l.pos == leaf.pos && l.path[0] == leaf.path[0] {
                    continue;
                }
                match l.leaf {
                    RevVal::Leaf(x) if x.deleted => {
                        deleted_conflicts.push(Value::String(doc::rev_str(l.pos, l.path[0])))
                    }
                    RevVal::Leaf(_) => {
                        conflicts.push(Value::String(doc::rev_str(l.pos, l.path[0])))
                    }
                    RevVal::Missing => {}
                }
            }
            if !conflicts.is_empty() {
                m.insert("_conflicts".into(), Value::Array(conflicts));
            }
            if !deleted_conflicts.is_empty() {
                m.insert("_deleted_conflicts".into(), Value::Array(deleted_conflicts));
            }
        }
        Ok(Value::Object(m))
    }

    /// Open a doc: winner or a specific rev.
    pub fn open_doc(&self, id: &[u8], rev: Option<&str>, opts: &DocOpts) -> Result<Option<Value>> {
        let Some(fdi) = self.open_doc_info(id)? else {
            return Ok(None);
        };
        let leaves = fdi.rev_tree.leaves();
        let chosen: Option<LeafPath<'_>> = match rev {
            None => fdi.rev_tree.winner(),
            Some(r) => {
                let (pos, revid) = doc::parse_rev(r)?;
                leaves
                    .into_iter()
                    .find(|l| l.pos == pos && l.path[0] == revid.as_slice())
            }
        };
        match chosen {
            None => Ok(None),
            Some(leaf) => Ok(Some(self.doc_json(&fdi, &leaf, opts)?)),
        }
    }

    pub fn fold_local_docs<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Value) -> Result<ControlFlow<()>>,
    {
        btree::fold(&self.file, &self.local_root, None, &mut |k, v| {
            let pair = v.tuple_n(2)?;
            let rev = match &pair[0] {
                Term::Int(i) => i.to_string(),
                Term::Bin(b) => String::from_utf8_lossy(b).into_owned(),
                other => return Err(corrupt(format!("bad local doc rev: {other:?}"))),
            };
            let mut m = Map::new();
            m.insert(
                "_id".into(),
                Value::String(String::from_utf8_lossy(k.as_bin()?).into_owned()),
            );
            m.insert("_rev".into(), Value::String(format!("0-{rev}")));
            match ejson::to_json(&pair[1])? {
                Value::Object(o) => m.extend(o),
                other => return Err(corrupt(format!("local doc body not an object: {other}"))),
            }
            f(Value::Object(m))
        })
    }

    pub fn find_att<'a>(
        &self,
        summary: &'a Summary,
        name: &str,
    ) -> Option<&'a AttInfo> {
        summary.atts.iter().find(|a| a.name == name)
    }
}

#[derive(Default, Clone)]
pub struct DocOpts {
    pub revs: bool,
    pub conflicts: bool,
    pub attachments: bool, // inline base64 data instead of stubs
}
