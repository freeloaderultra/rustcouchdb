//! The index file: a couch-store block file holding two btrees —
//! keys (composite key + docid → nothing) and ids (docid → its index keys,
//! for cleanup on update) — plus a header that checkpoints the source
//! database seq the index has seen, mirroring couch_index_updater.

use crate::keys;
use couch_mango::Selector;
use couch_store::btree::{self, Reducer};
use couch_store::db::Db;
use couch_store::error::{corrupt, Error, Result};
use couch_store::etf::Term;
use couch_store::file::CouchFile;
use couch_store::header::TreeState;
use md5::{Digest, Md5};
use serde_json::{json, Map, Value};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

const VERSION: i64 = 1;
const BATCH: usize = 4000;

#[derive(Clone, Debug)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    pub partial_filter_selector: Option<Value>,
}

impl IndexDef {
    pub fn def_json(&self) -> Value {
        let mut m = Map::new();
        m.insert(
            "fields".into(),
            Value::Array(self.fields.iter().map(|f| json!(f)).collect()),
        );
        if let Some(pfs) = &self.partial_filter_selector {
            m.insert("partial_filter_selector".into(), pfs.clone());
        }
        Value::Object(m)
    }

    pub fn auto_name(&self) -> String {
        let d = Md5::digest(self.def_json().to_string().as_bytes());
        let hex: String = d.iter().take(8).map(|b| format!("{b:02x}")).collect();
        format!("idx-{hex}")
    }
}

pub struct Index {
    pub path: PathBuf,
    pub def: IndexDef,
    pub source_uuid: String,
    pub update_seq: u64,
    file: CouchFile,
    key_root: Option<TreeState>,
    id_root: Option<TreeState>,
}

pub fn index_dir(db_path: &str) -> PathBuf {
    PathBuf::from(format!("{db_path}.indexes"))
}

impl Index {
    pub fn create(dir: &Path, def: IndexDef, source_uuid: &str) -> Result<Index> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{}.fidx", def.name));
        if path.exists() {
            return Err(Error::BadRequest(format!(
                "index {} already exists",
                def.name
            )));
        }
        let mut file = CouchFile::create(&path)?;
        let mut idx = Index {
            path,
            def,
            source_uuid: source_uuid.to_string(),
            update_seq: 0,
            file,
            key_root: None,
            id_root: None,
        };
        let header = idx.header_term();
        idx.file.write_header(&header)?;
        idx.file.sync()?;
        Ok(idx)
    }

    pub fn open(path: &Path) -> Result<Index> {
        let file = CouchFile::open_rw(path)?;
        let t = file.read_header()?;
        let tup = t.as_tuple()?;
        if tup.len() != 7 || !tup[0].is_atom("rustcouchdb_index") {
            return Err(corrupt(format!("{}: not a rustcouchdb index file", path.display())));
        }
        if tup[1].as_i64()? != VERSION {
            return Err(Error::Unsupported(format!(
                "index version {} (expected {VERSION})",
                tup[1].as_i64()?
            )));
        }
        let def_json: Value = serde_json::from_slice(tup[2].as_bin()?)
            .map_err(|e| corrupt(format!("bad index def json: {e}")))?;
        let fields = def_json["fields"]
            .as_array()
            .ok_or_else(|| corrupt("index def missing fields"))?
            .iter()
            .map(|f| f.as_str().map(String::from))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| corrupt("bad index def fields"))?;
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        Ok(Index {
            path: path.to_path_buf(),
            def: IndexDef {
                name,
                fields,
                partial_filter_selector: def_json
                    .get("partial_filter_selector")
                    .filter(|v| !v.is_null() && !v.as_object().is_some_and(|o| o.is_empty()))
                    .cloned(),
            },
            source_uuid: String::from_utf8_lossy(tup[3].as_bin()?).into_owned(),
            update_seq: tup[4].as_u64()?,
            key_root: TreeState::from_term(&tup[5])?,
            id_root: TreeState::from_term(&tup[6])?,
            file,
        })
    }

    fn header_term(&self) -> Term {
        Term::Tuple(vec![
            Term::atom("rustcouchdb_index"),
            Term::Int(VERSION),
            Term::Bin(self.def.def_json().to_string().into_bytes()),
            Term::Bin(self.source_uuid.as_bytes().to_vec()),
            Term::Int(self.update_seq as i64),
            TreeState::to_term(&self.key_root),
            TreeState::to_term(&self.id_root),
        ])
    }

    pub fn row_count(&self) -> u64 {
        self.key_root
            .as_ref()
            .and_then(|s| s.red.as_u64().ok())
            .unwrap_or(0)
    }

    /// Bring the index up to date with the source database. Mirrors
    /// couch_index_updater: fold changes since our seq, compute each doc's
    /// key, replace its old entries. Returns docs processed.
    pub fn update(&mut self, db: &Db) -> Result<u64> {
        if db.header.uuid_str() != self.source_uuid {
            return Err(Error::BadRequest(format!(
                "index {} was built from a different database (uuid mismatch)",
                self.def.name
            )));
        }
        let pfs = match &self.def.partial_filter_selector {
            Some(v) => Some(
                Selector::compile(v)
                    .map_err(|e| Error::BadRequest(format!("bad partial_filter_selector: {e}")))?,
            ),
            None => None,
        };
        let mut processed = 0u64;
        let mut key_inserts: Vec<(Term, Term)> = Vec::new();
        let mut key_removes: Vec<Term> = Vec::new();
        let mut id_inserts: Vec<(Term, Term)> = Vec::new();
        let mut id_removes: Vec<Term> = Vec::new();
        let mut last_seq = self.update_seq;

        // Collect batches first (fold borrows the file immutably), then
        // apply. Memory stays bounded by flushing every BATCH docs.
        let mut pending: Vec<(Vec<u8>, u64, Option<Term>)> = Vec::new();
        db.fold_changes(self.update_seq, |fdi| {
            let new_key = if fdi.deleted {
                None
            } else {
                let doc = match fdi.rev_tree.winner() {
                    Some(w) => db.doc_json(&fdi, &w, &Default::default())?,
                    None => return Ok(ControlFlow::Continue(())),
                };
                let matches_pfs = pfs.as_ref().map(|s| s.matches(&doc)).unwrap_or(true);
                if !matches_pfs {
                    None
                } else {
                    let mut cols = Vec::with_capacity(self.def.fields.len());
                    let mut complete = true;
                    for f in &self.def.fields {
                        match couch_mango::get_field(&doc, f) {
                            Some(v) => cols.push(v.clone()),
                            None => {
                                complete = false;
                                break;
                            }
                        }
                    }
                    if complete {
                        Some(keys::btree_key(&cols, &fdi.id))
                    } else {
                        None
                    }
                }
            };
            pending.push((fdi.id.clone(), fdi.update_seq, new_key));
            Ok(ControlFlow::Continue(()))
        })?;

        for chunk in pending.chunks(BATCH) {
            // look up existing entries for these ids
            let id_keys: Vec<Term> = chunk
                .iter()
                .map(|(id, _, _)| Term::Bin(id.clone()))
                .collect();
            let old = btree::lookup(&self.file, &self.id_root, &id_keys)?;
            for ((id, seq, new_key), old_val) in chunk.iter().zip(old) {
                processed += 1;
                last_seq = last_seq.max(*seq);
                let old_keys: Vec<Term> = match &old_val {
                    Some(t) => t.as_list()?.to_vec(),
                    None => Vec::new(),
                };
                let new_keys: Vec<Term> = new_key.iter().cloned().collect();
                if old_keys == new_keys {
                    continue;
                }
                for ok in &old_keys {
                    key_removes.push(ok.clone());
                }
                for nk in &new_keys {
                    key_inserts.push((nk.clone(), Term::List(vec![])));
                }
                if new_keys.is_empty() {
                    if !old_keys.is_empty() {
                        id_removes.push(Term::Bin(id.clone()));
                    }
                } else {
                    id_inserts.push((Term::Bin(id.clone()), Term::List(new_keys)));
                }
            }
            self.key_root = btree::add_remove(
                &mut self.file,
                &self.key_root,
                Reducer::Count,
                std::mem::take(&mut key_inserts),
                std::mem::take(&mut key_removes),
            )?;
            self.id_root = btree::add_remove(
                &mut self.file,
                &self.id_root,
                Reducer::None,
                std::mem::take(&mut id_inserts),
                std::mem::take(&mut id_removes),
            )?;
        }

        if last_seq != self.update_seq || processed > 0 {
            self.update_seq = last_seq.max(db.header.update_seq);
            let header = self.header_term();
            self.file.sync()?;
            self.file.write_header(&header)?;
            self.file.sync()?;
        }
        Ok(processed)
    }

    /// Scan index rows in range, calling `f(docid)`.
    pub fn scan<F>(
        &self,
        start: &[keys::Bound],
        end: &[keys::Bound],
        end_inclusive: bool,
        descending: bool,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<ControlFlow<()>>,
    {
        let mut end_cols: Vec<keys::Bound> = end.to_vec();
        if end_inclusive {
            end_cols.push(keys::Bound::Max);
        }
        let start_term = keys::bound_key(start, false);
        let end_list = Term::List(end_cols.iter().map(keys::encode_bound).collect());
        let start_list = Term::List(start.iter().map(keys::encode_bound).collect());

        if !descending {
            btree::fold(&self.file, &self.key_root, Some(&start_term), &mut |k, _v| {
                let Some((cols, id)) = keys::decode_btree_key(k) else {
                    return Err(corrupt("bad index key"));
                };
                let cols_list = Term::List(cols.to_vec());
                if couch_store::etf::cmp(&cols_list, &end_list) != std::cmp::Ordering::Less {
                    return Ok(ControlFlow::Break(()));
                }
                f(id)
            })
        } else {
            let upper = keys::bound_key(&end_cols, true);
            btree::fold_rev(&self.file, &self.key_root, Some(&upper), &mut |k, _v| {
                let Some((cols, id)) = keys::decode_btree_key(k) else {
                    return Err(corrupt("bad index key"));
                };
                let cols_list = Term::List(cols.to_vec());
                if couch_store::etf::cmp(&cols_list, &start_list) == std::cmp::Ordering::Less {
                    return Ok(ControlFlow::Break(()));
                }
                f(id)
            })
        }
    }

    pub fn info(&self) -> Value {
        json!({
            "name": self.def.name,
            "fields": self.def.fields,
            "partial_filter_selector": self.def.partial_filter_selector,
            "rows": self.row_count(),
            "update_seq": self.update_seq,
            "source_uuid": self.source_uuid,
            "file": self.path.to_string_lossy(),
            "size": self.file.eof,
        })
    }
}

/// All indexes in a directory.
pub fn list(dir: &Path) -> Result<Vec<Index>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(out),
    };
    for e in entries {
        let p = e?.path();
        if p.extension().map(|x| x == "fidx").unwrap_or(false) {
            out.push(Index::open(&p)?);
        }
    }
    out.sort_by(|a, b| a.def.name.cmp(&b.def.name));
    Ok(out)
}
