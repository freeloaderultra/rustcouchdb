//! Port of couch_btree: copy-on-write B+tree with reductions in interior
//! nodes. Nodes are appended terms `{kp_node, [{Key, {Ptr, Red, Size}}]}` /
//! `{kv_node, [{Key, Value}]}`; the root lives in the db header.

use crate::error::{corrupt, Result};
use crate::etf::{self, Term};
use crate::file::CouchFile;
use crate::header::TreeState;
use std::cmp::Ordering;
use std::ops::ControlFlow;

const CHUNK_SIZE: usize = 1279; // couchdb btree_chunk_size default

/// The per-tree reduce functions from couch_bt_engine.
#[derive(Clone, Copy, PartialEq)]
pub enum Reducer {
    /// {NotDeleted, Deleted, {ActiveSize, ExternalSize}} over split FDI values
    IdTree,
    /// count over split FDI values
    SeqTree,
    /// count (purge trees)
    Count,
    /// no reduce (local docs tree)
    None,
}

impl Reducer {
    /// reduce over kv-node values (split form).
    fn reduce(&self, kvs: &[(Term, Term)]) -> Result<Term> {
        match self {
            Reducer::None => Ok(Term::List(vec![])),
            Reducer::Count | Reducer::SeqTree => Ok(Term::Int(kvs.len() as i64)),
            Reducer::IdTree => {
                let (mut live, mut del, mut active, mut external) = (0i64, 0i64, 0i64, 0i64);
                for (_k, v) in kvs {
                    // v = {Seq, Deleted, {Active, External}, DiskTree}
                    let t = v.as_tuple()?;
                    if t.len() < 4 {
                        return Err(corrupt("short id_tree value"));
                    }
                    if t[1].as_i64()? != 0 {
                        del += 1;
                    } else {
                        live += 1;
                    }
                    let (a, e) = split_sizes(&t[2])?;
                    active += a;
                    external += e;
                }
                Ok(id_red(live, del, active, external))
            }
        }
    }

    /// rereduce over child reductions.
    fn rereduce(&self, reds: &[Term]) -> Result<Term> {
        match self {
            Reducer::None => Ok(Term::List(vec![])),
            Reducer::Count | Reducer::SeqTree => {
                let mut n = 0i64;
                for r in reds {
                    n += r.as_i64()?;
                }
                Ok(Term::Int(n))
            }
            Reducer::IdTree => {
                let (mut live, mut del, mut active, mut external) = (0i64, 0i64, 0i64, 0i64);
                for r in reds {
                    let t = r.as_tuple()?;
                    if t.len() < 2 {
                        return Err(corrupt("short id_tree reduction"));
                    }
                    live += t[0].as_i64()?;
                    del += t[1].as_i64()?;
                    if let Some(sz) = t.get(2) {
                        if let Ok((a, e)) = split_sizes(sz) {
                            active += a;
                            external += e;
                        }
                    }
                }
                Ok(id_red(live, del, active, external))
            }
        }
    }
}

fn id_red(live: i64, del: i64, active: i64, external: i64) -> Term {
    Term::Tuple(vec![
        Term::Int(live),
        Term::Int(del),
        Term::Tuple(vec![Term::Int(active), Term::Int(external)]),
    ])
}

/// Sizes appear as {Active, External}, a bare integer (ancient), or a
/// #size_info{} record tuple.
pub fn split_sizes(t: &Term) -> Result<(i64, i64)> {
    match t {
        Term::Int(a) => Ok((*a, 0)),
        Term::Tuple(v) if v.len() == 2 => Ok((v[0].as_i64()?, v[1].as_i64()?)),
        Term::Tuple(v) if v.len() == 3 && v[0].is_atom("size_info") => {
            Ok((v[1].as_i64()?, v[2].as_i64()?))
        }
        _ => Err(corrupt(format!("bad size info: {t:?}"))),
    }
}

fn get_node(file: &CouchFile, ptr: u64) -> Result<(bool, Vec<(Term, Term)>)> {
    let t = file.read_term(ptr)?;
    let tup = t.tuple_n(2)?;
    let is_kp = if tup[0].is_atom("kp_node") {
        true
    } else if tup[0].is_atom("kv_node") {
        false
    } else {
        return Err(corrupt(format!("bad btree node type: {:?}", tup[0])));
    };
    let mut kvs = Vec::new();
    for e in tup[1].as_list()? {
        let kv = e.tuple_n(2)?;
        kvs.push((kv[0].clone(), kv[1].clone()));
    }
    Ok((is_kp, kvs))
}

fn ptr_of(pointer_info: &Term) -> Result<u64> {
    pointer_info.as_tuple()?.first().ok_or_else(|| corrupt("empty pointer info"))?.as_u64()
}

/// In-order fold, forward direction, optional inclusive start key.
/// The callback returns ControlFlow::Break(()) to stop early.
pub fn fold<F>(
    file: &CouchFile,
    root: &Option<TreeState>,
    start_key: Option<&Term>,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&Term, &Term) -> Result<ControlFlow<()>>,
{
    let Some(root) = root else { return Ok(()) };
    let _ = fold_node(file, root.ptr, start_key, f)?;
    Ok(())
}

fn fold_node<F>(
    file: &CouchFile,
    ptr: u64,
    start_key: Option<&Term>,
    f: &mut F,
) -> Result<ControlFlow<()>>
where
    F: FnMut(&Term, &Term) -> Result<ControlFlow<()>>,
{
    let (is_kp, kvs) = get_node(file, ptr)?;
    if is_kp {
        for (key, pi) in &kvs {
            // Drop subtrees whose greatest key sorts before the start key.
            if let Some(sk) = start_key {
                if etf::cmp(key, sk) == Ordering::Less {
                    continue;
                }
            }
            if fold_node(file, ptr_of(pi)?, start_key, f)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
    } else {
        for (key, val) in &kvs {
            if let Some(sk) = start_key {
                if etf::cmp(key, sk) == Ordering::Less {
                    continue;
                }
            }
            if f(key, val)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Point lookups. Returns results in input order.
pub fn lookup(
    file: &CouchFile,
    root: &Option<TreeState>,
    keys: &[Term],
) -> Result<Vec<Option<Term>>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(match root {
            None => None,
            Some(r) => lookup_one(file, r.ptr, key)?,
        });
    }
    Ok(out)
}

fn lookup_one(file: &CouchFile, ptr: u64, key: &Term) -> Result<Option<Term>> {
    let (is_kp, kvs) = get_node(file, ptr)?;
    if is_kp {
        for (k, pi) in &kvs {
            if etf::cmp(k, key) != Ordering::Less {
                return lookup_one(file, ptr_of(pi)?, key);
            }
        }
        Ok(None)
    } else {
        for (k, v) in &kvs {
            match etf::cmp(k, key) {
                Ordering::Equal => return Ok(Some(v.clone())),
                Ordering::Greater => return Ok(None),
                Ordering::Less => continue,
            }
        }
        Ok(None)
    }
}

/// Full reduction of the tree (root reduction, or empty-tree default).
pub fn full_reduce(root: &Option<TreeState>, reducer: Reducer) -> Result<Term> {
    match root {
        Some(s) => Ok(s.red.clone()),
        None => reducer.rereduce(&[]),
    }
}

// ------------------------------------------------------------------ writing

enum Action {
    Insert(Term, Term),
    Remove(Term),
}

impl Action {
    fn key(&self) -> &Term {
        match self {
            Action::Insert(k, _) => k,
            Action::Remove(k) => k,
        }
    }
    fn order(&self) -> u8 {
        match self {
            Action::Remove(_) => 1,
            Action::Insert(_, _) => 2,
        }
    }
}

/// couch_btree:add_remove/3 — returns the new tree state.
pub fn add_remove(
    file: &mut CouchFile,
    root: &Option<TreeState>,
    reducer: Reducer,
    inserts: Vec<(Term, Term)>,
    removes: Vec<Term>,
) -> Result<Option<TreeState>> {
    let mut actions: Vec<Action> = inserts
        .into_iter()
        .map(|(k, v)| Action::Insert(k, v))
        .chain(removes.into_iter().map(Action::Remove))
        .collect();
    actions.sort_by(|a, b| match etf::cmp(a.key(), b.key()) {
        Ordering::Equal => a.order().cmp(&b.order()),
        o => o,
    });
    let root_info = root.as_ref().map(|s| {
        Term::Tuple(vec![
            Term::Int(s.ptr as i64),
            s.red.clone(),
            match s.size {
                Some(sz) => Term::Int(sz as i64),
                None => Term::nil(),
            },
        ])
    });
    let mut kps = modify_node(file, root_info.as_ref(), &actions, reducer)?;
    // complete_root: collapse multiple KPs into a single root.
    while kps.len() > 1 {
        kps = write_node(file, true, kps, reducer)?;
    }
    Ok(match kps.pop() {
        None => None,
        Some((_k, pi)) => {
            let t = pi.as_tuple()?;
            Some(TreeState {
                ptr: t[0].as_u64()?,
                red: t[1].clone(),
                size: t.get(2).and_then(|s| s.as_u64().ok()),
            })
        }
    })
}

/// Returns the replacement KP list `[(LastKey, {Ptr, Red, Size})]`.
fn modify_node(
    file: &mut CouchFile,
    node: Option<&Term>,
    actions: &[Action],
    reducer: Reducer,
) -> Result<Vec<(Term, Term)>> {
    let (is_kp, kvs) = match node {
        None => (false, Vec::new()),
        Some(pi) => get_node(file, ptr_of(pi)?)?,
    };
    let new_list = if is_kp {
        modify_kp(file, &kvs, actions, reducer)?
    } else {
        modify_kv(&kvs, actions)
    };
    if new_list.is_empty() {
        return Ok(Vec::new());
    }
    write_node(file, is_kp, new_list, reducer)
}

fn modify_kv(kvs: &[(Term, Term)], actions: &[Action]) -> Vec<(Term, Term)> {
    let mut out: Vec<(Term, Term)> = Vec::with_capacity(kvs.len() + actions.len());
    let mut i = 0usize;
    for act in actions {
        while i < kvs.len() && etf::cmp(&kvs[i].0, act.key()) == Ordering::Less {
            out.push(kvs[i].clone());
            i += 1;
        }
        let exists = i < kvs.len() && etf::cmp(&kvs[i].0, act.key()) == Ordering::Equal;
        match act {
            Action::Insert(k, v) => {
                out.push((k.clone(), v.clone()));
                if exists {
                    i += 1; // replace
                }
            }
            Action::Remove(_) => {
                if exists {
                    i += 1; // drop
                }
            }
        }
    }
    out.extend_from_slice(&kvs[i.min(kvs.len())..]);
    out
}

fn modify_kp(
    file: &mut CouchFile,
    kps: &[(Term, Term)],
    actions: &[Action],
    reducer: Reducer,
) -> Result<Vec<(Term, Term)>> {
    let mut out: Vec<(Term, Term)> = Vec::with_capacity(kps.len());
    let mut acts = actions;
    for (idx, (node_key, pi)) in kps.iter().enumerate() {
        if acts.is_empty() {
            out.push((node_key.clone(), pi.clone()));
            continue;
        }
        let last = idx + 1 == kps.len();
        // All remaining actions go to the last child; otherwise actions with
        // key <= node_key go to this child.
        let split = if last {
            acts.len()
        } else {
            acts.partition_point(|a| etf::cmp(a.key(), node_key) != Ordering::Greater)
        };
        if split == 0 {
            out.push((node_key.clone(), pi.clone()));
            continue;
        }
        let (mine, rest) = acts.split_at(split);
        acts = rest;
        let child_kps = modify_node(file, Some(pi), mine, reducer)?;
        out.extend(child_kps);
    }
    Ok(out)
}

/// couch_btree:write_node/3 — chunkify and append, returning the KP list.
fn write_node(
    file: &mut CouchFile,
    is_kp: bool,
    list: Vec<(Term, Term)>,
    reducer: Reducer,
) -> Result<Vec<(Term, Term)>> {
    let chunks = chunkify(list);
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let node_term = Term::Tuple(vec![
            Term::atom(if is_kp { "kp_node" } else { "kv_node" }),
            Term::List(
                chunk
                    .iter()
                    .map(|(k, v)| Term::Tuple(vec![k.clone(), v.clone()]))
                    .collect(),
            ),
        ]);
        let (ptr, written) = file.append_term(&node_term)?;
        let red = if is_kp {
            let child_reds: Vec<Term> = chunk
                .iter()
                .map(|(_k, pi)| Ok(pi.as_tuple()?[1].clone()))
                .collect::<Result<_>>()?;
            reducer.rereduce(&child_reds)?
        } else {
            reducer.reduce(&chunk)?
        };
        let size = if is_kp {
            let mut sz = written;
            for (_k, pi) in &chunk {
                let t = pi.as_tuple()?;
                sz += match t.get(2) {
                    Some(Term::Int(s)) => *s as u64,
                    _ => 0,
                };
            }
            sz
        } else {
            written
        };
        let last_key = chunk.last().expect("non-empty chunk").0.clone();
        out.push((
            last_key,
            Term::Tuple(vec![Term::Int(ptr as i64), red, Term::Int(size as i64)]),
        ));
    }
    Ok(out)
}

/// couch_btree:chunkify/1 — split a node list into ~CHUNK_SIZE pieces.
fn chunkify(list: Vec<(Term, Term)>) -> Vec<Vec<(Term, Term)>> {
    let sizes: Vec<usize> = list
        .iter()
        .map(|(k, v)| external_size_kv(k, v))
        .collect();
    let total: usize = sizes.iter().sum();
    if total <= CHUNK_SIZE {
        return vec![list];
    }
    let num_chunks = total / CHUNK_SIZE + 1;
    let threshold = total / num_chunks;
    let mut chunks: Vec<Vec<(Term, Term)>> = Vec::with_capacity(num_chunks + 1);
    let mut cur: Vec<(Term, Term)> = Vec::new();
    let mut cur_size = 0usize;
    for (kv, sz) in list.into_iter().zip(sizes) {
        if cur_size + sz > threshold && !cur.is_empty() {
            chunks.push(std::mem::take(&mut cur));
            cur_size = 0;
        }
        cur_size += sz;
        cur.push(kv);
    }
    if cur.len() == 1 && !chunks.is_empty() {
        // Erlang appends a trailing single item to the previous chunk.
        chunks.last_mut().unwrap().extend(cur);
    } else if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

fn external_size_kv(k: &Term, v: &Term) -> usize {
    etf::external_size(&Term::Tuple(vec![k.clone(), v.clone()]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile(name: &str) -> CouchFile {
        let dir = std::env::temp_dir().join(format!("couch-store-bt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        CouchFile::create(path).unwrap()
    }

    fn key(i: usize) -> Term {
        Term::Bin(format!("key-{i:06}").into_bytes())
    }

    #[test]
    fn build_lookup_fold() {
        let mut f = tmpfile("t1.couch");
        let kvs: Vec<(Term, Term)> = (0..2000).map(|i| (key(i), Term::Int(i as i64))).collect();
        let state = add_remove(&mut f, &None, Reducer::Count, kvs.clone(), vec![]).unwrap();
        let root = state.clone();
        assert!(full_reduce(&root, Reducer::Count).unwrap() == Term::Int(2000));

        // lookup
        let res = lookup(&f, &root, &[key(0), key(1234), key(9999)]).unwrap();
        assert!(res[0] == Some(Term::Int(0)));
        assert!(res[1] == Some(Term::Int(1234)));
        assert!(res[2].is_none());

        // fold with start key
        let mut seen = Vec::new();
        fold(&f, &root, Some(&key(1995)), &mut |k, _v| {
            seen.push(k.clone());
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
        assert_eq!(seen.len(), 5);
        assert!(seen[0] == key(1995));

        // update + remove
        let state2 = add_remove(
            &mut f,
            &root,
            Reducer::Count,
            vec![(key(10), Term::Int(-10)), (key(2000), Term::Int(2000))],
            vec![key(0), key(1999)],
        )
        .unwrap();
        // 2000 - {key0, key1999} + key2000 (key10 is a replace) = 1999
        assert!(full_reduce(&state2, Reducer::Count).unwrap() == Term::Int(1999));
        let res = lookup(&f, &state2, &[key(0), key(10), key(2000)]).unwrap();
        assert!(res[0].is_none());
        assert!(res[1] == Some(Term::Int(-10)));
        assert!(res[2] == Some(Term::Int(2000)));

        // full order check
        let mut prev: Option<Term> = None;
        let mut n = 0;
        fold(&f, &state2, None, &mut |k, _| {
            if let Some(p) = &prev {
                assert!(etf::cmp(p, k) == Ordering::Less);
            }
            prev = Some(k.clone());
            n += 1;
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
        assert_eq!(n, 1999);
    }

    #[test]
    fn empty_tree_after_removes() {
        let mut f = tmpfile("t2.couch");
        let state = add_remove(
            &mut f,
            &None,
            Reducer::Count,
            vec![(key(1), Term::Int(1))],
            vec![],
        )
        .unwrap();
        let state2 = add_remove(&mut f, &state, Reducer::Count, vec![], vec![key(1)]).unwrap();
        assert!(state2.is_none());
    }
}
