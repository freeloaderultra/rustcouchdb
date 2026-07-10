//! Port of couch_key_tree, specialised to revision trees.
//!
//! On disk a rev tree is `[{StartDepth, Node}]` where
//! `Node = {RevIdBin, Value, [ChildNode]}` and `Value` is
//! `{Deleted, Ptr, Seq, {Active, External}, Atts}` for stored revisions
//! (3- and 4-tuple legacy forms exist) or `[]` (?REV_MISSING).

use crate::btree::split_sizes;
use crate::error::{corrupt, Result};
use crate::etf::Term;

#[derive(Clone, Debug)]
pub struct LeafVal {
    pub deleted: bool,
    pub ptr: Option<u64>,
    pub seq: u64,
    pub sizes: (i64, i64),
    pub atts: Term,
}

#[derive(Clone, Debug)]
pub enum RevVal {
    Missing,
    Leaf(LeafVal),
}

#[derive(Clone, Debug)]
pub struct RevNode {
    pub key: Vec<u8>,
    pub val: RevVal,
    pub children: Vec<RevNode>,
}

#[derive(Clone, Debug, Default)]
pub struct RevTree(pub Vec<(u64, RevNode)>);

/// A leaf together with its full revision path:
/// `pos` is the leaf's revision position, `path` is `[leaf_revid, parent, ...]`.
pub struct LeafPath<'a> {
    pub pos: u64,
    pub path: Vec<&'a [u8]>,
    pub leaf: &'a RevVal,
}

impl RevTree {
    pub fn from_term(t: &Term) -> Result<RevTree> {
        let mut roots = Vec::new();
        for e in t.as_list()? {
            let pair = e.tuple_n(2)?;
            roots.push((pair[0].as_u64()?, node_from_term(&pair[1])?));
        }
        Ok(RevTree(roots))
    }

    pub fn to_term(&self) -> Term {
        Term::List(
            self.0
                .iter()
                .map(|(start, node)| {
                    Term::Tuple(vec![Term::Int(*start as i64), node_to_term(node)])
                })
                .collect(),
        )
    }

    /// couch_key_tree:get_all_leafs/1 (with full paths).
    pub fn leaves(&self) -> Vec<LeafPath<'_>> {
        let mut out = Vec::new();
        for (start, root) in &self.0 {
            collect_leaves(*start, root, &mut Vec::new(), &mut out);
        }
        out
    }

    /// The winning revision: sort descending by {not deleted, {pos, revid}}.
    pub fn winner(&self) -> Option<LeafPath<'_>> {
        let mut leaves = self.leaves();
        if leaves.is_empty() {
            return None;
        }
        leaves.sort_by(|a, b| {
            let da = matches!(a.leaf, RevVal::Leaf(l) if l.deleted);
            let db = matches!(b.leaf, RevVal::Leaf(l) if l.deleted);
            (!db, b.pos, b.path[0]).cmp(&(!da, a.pos, a.path[0]))
        });
        Some(leaves.remove(0))
    }

    /// Merge a linear revision path (new_edits:false semantics). Returns true
    /// if the tree changed (new nodes or a Missing value upgraded).
    /// Port of couch_key_tree:merge/2.
    pub fn merge_path(&mut self, start: u64, path_nodes: RevNode) -> bool {
        let before = self.to_term();
        let mut trees = std::mem::take(&mut self.0);
        let ins = (start, path_nodes);
        let mut merged = false;
        let mut acc: Vec<(u64, RevNode)> = Vec::new();
        let mut ins_opt = Some(ins);
        while let Some(tree) = trees.pop() {
            if merged {
                acc.push(tree);
                continue;
            }
            let ins_ref = ins_opt.take().unwrap();
            match merge_tree(tree, ins_ref) {
                Ok(m) => {
                    acc.push(m);
                    merged = true;
                }
                Err(orig_and_ins) => {
                    let (orig, ins_back) = *orig_and_ins;
                    acc.push(orig);
                    ins_opt = Some(ins_back);
                }
            }
        }
        if !merged {
            acc.push(ins_opt.take().unwrap());
        }
        acc.sort_by(|a, b| (a.0, &a.1.key).cmp(&(b.0, &b.1.key)));
        self.0 = acc;
        !(self.to_term() == before)
    }

    /// couch_key_tree:stem/2 — keep at most `limit` revs on each path.
    pub fn stem(&mut self, limit: u64) {
        let mut paths: Vec<(u64, Vec<(Vec<u8>, RevVal)>)> = Vec::new();
        for lp in self.full_leaf_paths() {
            paths.push(lp);
        }
        let mut stemmed: Vec<(u64, Vec<(Vec<u8>, RevVal)>)> = paths
            .into_iter()
            .map(|(pos, path)| {
                let keep: Vec<_> = path.into_iter().take(limit as usize).collect();
                (pos + 1 - keep.len() as u64, keep)
            })
            .collect();
        stemmed.sort_by(|a, b| (a.0, &a.1.last().map(|x| x.0.clone())).cmp(&(b.0, &b.1.last().map(|x| x.0.clone()))));
        let mut new_tree = RevTree(Vec::new());
        for (start, path) in stemmed {
            // path is [leaf, parent, ...] — build a linear chain oldest-first.
            let mut node: Option<RevNode> = None;
            for (key, val) in path {
                node = Some(RevNode {
                    key,
                    val,
                    children: node.into_iter().collect(),
                });
            }
            if let Some(n) = node {
                new_tree.merge_path(start, n);
            }
        }
        self.0 = new_tree.0;
    }

    /// Every revision in the tree as (pos, revid) — couch_key_tree:get_all_leafs
    /// plus interior nodes; what _revs_diff checks membership against.
    pub fn all_revs(&self) -> Vec<(u64, &[u8])> {
        fn walk<'a>(pos: u64, node: &'a RevNode, out: &mut Vec<(u64, &'a [u8])>) {
            out.push((pos, &node.key));
            for c in &node.children {
                walk(pos + 1, c, out);
            }
        }
        let mut out = Vec::new();
        for (start, root) in &self.0 {
            walk(*start, root, &mut out);
        }
        out
    }

    /// Find one revision anywhere in the tree (leaf or interior) with its
    /// ancestor path. Interior nodes keep their values until compaction, so
    /// old revisions stay readable — same as couch_db:open_doc with rev.
    pub fn rev_path(&self, pos: u64, revid: &[u8]) -> Option<LeafPath<'_>> {
        fn walk<'a>(
            cur: u64,
            node: &'a RevNode,
            stack: &mut Vec<&'a [u8]>,
            pos: u64,
            revid: &[u8],
        ) -> Option<LeafPath<'a>> {
            stack.push(&node.key);
            if cur == pos && node.key == revid {
                let mut path: Vec<&'a [u8]> = stack.clone();
                path.reverse();
                stack.pop();
                return Some(LeafPath {
                    pos,
                    path,
                    leaf: &node.val,
                });
            }
            if cur < pos {
                for c in &node.children {
                    if let Some(found) = walk(cur + 1, c, stack, pos, revid) {
                        stack.pop();
                        return Some(found);
                    }
                }
            }
            stack.pop();
            None
        }
        for (start, root) in &self.0 {
            if *start > pos {
                continue;
            }
            let mut stack = Vec::new();
            if let Some(found) = walk(*start, root, &mut stack, pos, revid) {
                return Some(found);
            }
        }
        None
    }

    /// Leaves whose path passes through (pos, revid) — the `latest=true`
    /// resolution: a requested rev maps to its leaf descendant(s).
    pub fn descendant_leaves(&self, pos: u64, revid: &[u8]) -> Vec<LeafPath<'_>> {
        self.leaves()
            .into_iter()
            .filter(|l| {
                l.pos >= pos
                    && (l.pos - pos) < l.path.len() as u64
                    && l.path[(l.pos - pos) as usize] == revid
            })
            .collect()
    }

    /// Full paths including values: [(LeafPos, [(RevId, Val) leaf-first])].
    fn full_leaf_paths(&self) -> Vec<(u64, Vec<(Vec<u8>, RevVal)>)> {
        let mut out = Vec::new();
        for (start, root) in &self.0 {
            let mut stack: Vec<(Vec<u8>, RevVal)> = Vec::new();
            full_paths(*start, root, &mut stack, &mut out);
        }
        out
    }
}

fn full_paths(
    pos: u64,
    node: &RevNode,
    stack: &mut Vec<(Vec<u8>, RevVal)>,
    out: &mut Vec<(u64, Vec<(Vec<u8>, RevVal)>)>,
) {
    stack.push((node.key.clone(), node.val.clone()));
    if node.children.is_empty() {
        let mut path = stack.clone();
        path.reverse(); // leaf first
        out.push((pos, path));
    } else {
        for c in &node.children {
            full_paths(pos + 1, c, stack, out);
        }
    }
    stack.pop();
}

fn collect_leaves<'a>(
    pos: u64,
    node: &'a RevNode,
    stack: &mut Vec<&'a [u8]>,
    out: &mut Vec<LeafPath<'a>>,
) {
    stack.push(&node.key);
    if node.children.is_empty() {
        let mut path: Vec<&'a [u8]> = stack.clone();
        path.reverse();
        out.push(LeafPath {
            pos,
            path,
            leaf: &node.val,
        });
    } else {
        for c in &node.children {
            collect_leaves(pos + 1, c, stack, out);
        }
    }
    stack.pop();
}

fn node_from_term(t: &Term) -> Result<RevNode> {
    let tup = t.tuple_n(3)?;
    let key = tup[0].as_bin()?.to_vec();
    let val = val_from_term(&tup[1])?;
    let mut children = Vec::new();
    for c in tup[2].as_list()? {
        children.push(node_from_term(c)?);
    }
    Ok(RevNode { key, val, children })
}

fn val_from_term(t: &Term) -> Result<RevVal> {
    match t {
        Term::List(l) if l.is_empty() => Ok(RevVal::Missing),
        Term::Tuple(v) if (3..=5).contains(&v.len()) => {
            let deleted = v[0].as_i64()? != 0;
            let ptr = match &v[1] {
                Term::Int(p) => Some(*p as u64),
                _ => None, // nil
            };
            let seq = v[2].as_u64()?;
            let sizes = match v.get(3) {
                Some(sz) => split_sizes(sz).unwrap_or((0, 0)),
                None => (0, 0),
            };
            let atts = v.get(4).cloned().unwrap_or(Term::List(vec![]));
            Ok(RevVal::Leaf(LeafVal {
                deleted,
                ptr,
                seq,
                sizes,
                atts,
            }))
        }
        _ => Err(corrupt(format!("bad rev tree value: {t:?}"))),
    }
}

fn node_to_term(n: &RevNode) -> Term {
    let val = match &n.val {
        RevVal::Missing => Term::List(vec![]),
        RevVal::Leaf(l) => Term::Tuple(vec![
            Term::Int(if l.deleted { 1 } else { 0 }),
            match l.ptr {
                Some(p) => Term::Int(p as i64),
                None => Term::nil(),
            },
            Term::Int(l.seq as i64),
            Term::Tuple(vec![Term::Int(l.sizes.0), Term::Int(l.sizes.1)]),
            l.atts.clone(),
        ]),
    };
    Term::Tuple(vec![
        Term::Bin(n.key.clone()),
        val,
        Term::List(n.children.iter().map(node_to_term).collect()),
    ])
}

/// couch_key_tree:merge_tree — try to merge `ins` into `tree`.
/// Ok(merged) on success, Err((tree, ins)) if they don't connect.
#[allow(clippy::type_complexity)]
fn merge_tree(
    tree: (u64, RevNode),
    ins: (u64, RevNode),
) -> std::result::Result<(u64, RevNode), Box<((u64, RevNode), (u64, RevNode))>> {
    let (depth, node) = tree;
    let (idepth, inode) = ins;
    let pos = depth as i64 - idepth as i64;
    let mut nodes = vec![node];
    match merge_at(&mut nodes, pos, &inode) {
        true => {
            let merged = nodes.remove(0);
            Ok((depth.min(idepth), merged))
        }
        false => {
            let node = nodes.remove(0);
            Err(Box::new(((depth, node), (idepth, inode))))
        }
    }
}

/// couch_key_tree:merge_at — mutates `nodes` in place on success.
fn merge_at(nodes: &mut Vec<RevNode>, pos: i64, inode: &RevNode) -> bool {
    if nodes.is_empty() {
        return false;
    }
    if pos > 0 {
        // Seek deeper into the insert path: it must be a linear chain.
        if inode.children.len() == 1 {
            // We need to attach at existing tree; the insert path's head is
            // above our tree root — walk down the insert chain.
            let child = &inode.children[0];
            if merge_at(nodes, pos - 1, child) {
                // Wrap: result keeps the inserted parent? No — Erlang keeps
                // {IK, IV, Merged} i.e. the insert node becomes the new root
                // ABOVE the existing nodes. But `nodes` here belongs to the
                // existing tree; the Erlang code returns the insert node
                // wrapping the merged children. Handle at caller: we emulate
                // by replacing nodes with the wrapper.
                let merged_children = std::mem::take(nodes);
                nodes.push(RevNode {
                    key: inode.key.clone(),
                    val: inode.val.clone(),
                    children: merged_children,
                });
                return true;
            }
            false
        } else {
            false
        }
    } else if pos < 0 {
        // Seek deeper into the existing tree.
        let n = nodes.len();
        for i in 0..n {
            let (subtree_merged, _) = {
                let node = &mut nodes[i];
                let ok = merge_at(&mut node.children, pos + 1, inode);
                (ok, ())
            };
            if subtree_merged {
                return true;
            }
        }
        false
    } else {
        // pos == 0: merging may only start at an exact key match; a miss
        // fails the whole merge so the path becomes a new root in the
        // forest (new siblings are only ever created below a matched node,
        // in merge_extend).
        for i in 0..nodes.len() {
            match nodes[i].key.cmp(&inode.key) {
                std::cmp::Ordering::Equal => {
                    let node = &mut nodes[i];
                    node.val = value_pref(&node.val, &inode.val);
                    merge_extend(&mut node.children, &inode.children);
                    return true;
                }
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Less => continue,
            }
        }
        false
    }
}

/// couch_key_tree:merge_extend — merge the linear insert chain into siblings.
fn merge_extend(nodes: &mut Vec<RevNode>, ins: &[RevNode]) {
    if ins.is_empty() {
        return;
    }
    debug_assert!(ins.len() == 1, "insert path must be linear");
    let inode = &ins[0];
    for i in 0..nodes.len() {
        match nodes[i].key.cmp(&inode.key) {
            std::cmp::Ordering::Equal => {
                let node = &mut nodes[i];
                node.val = value_pref(&node.val, &inode.val);
                merge_extend(&mut node.children, &inode.children);
                return;
            }
            std::cmp::Ordering::Greater => {
                nodes.insert(i, inode.clone());
                return;
            }
            std::cmp::Ordering::Less => continue,
        }
    }
    nodes.push(inode.clone());
}

/// couch_key_tree:value_pref — prefer stored leaves over ?REV_MISSING;
/// otherwise keep the existing value.
fn value_pref(existing: &RevVal, incoming: &RevVal) -> RevVal {
    match (existing, incoming) {
        (RevVal::Missing, other) => other.clone(),
        (keep, _) => keep.clone(),
    }
}

/// Build a linear insert path from `(pos, [revid_leaf, parent, ...])` with
/// the given leaf value; intermediate nodes are Missing.
/// Returns (start_depth, chain_root).
pub fn path_to_tree(pos: u64, revids_leaf_first: &[Vec<u8>], leaf: LeafVal) -> (u64, RevNode) {
    let start = pos + 1 - revids_leaf_first.len() as u64;
    let mut node: Option<RevNode> = None;
    for (i, key) in revids_leaf_first.iter().enumerate() {
        let val = if i == 0 {
            RevVal::Leaf(leaf.clone())
        } else {
            RevVal::Missing
        };
        node = Some(RevNode {
            key: key.clone(),
            val,
            children: node.into_iter().collect(),
        });
    }
    (start, node.expect("non-empty rev path"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(seq: u64) -> LeafVal {
        LeafVal {
            deleted: false,
            ptr: Some(100 + seq),
            seq,
            sizes: (10, 10),
            atts: Term::List(vec![]),
        }
    }

    fn rev(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn merge_linear_and_branch() {
        let mut tree = RevTree::default();
        // 1-a
        let (s, n) = path_to_tree(1, &[rev("a")], leaf(1));
        tree.merge_path(s, n);
        // 2-b on 1-a
        let (s, n) = path_to_tree(2, &[rev("b"), rev("a")], leaf(2));
        tree.merge_path(s, n);
        // conflict 2-c on 1-a
        let (s, n) = path_to_tree(2, &[rev("c"), rev("a")], leaf(3));
        tree.merge_path(s, n);

        let leaves = tree.leaves();
        assert_eq!(leaves.len(), 2);
        let mut revs: Vec<(u64, Vec<u8>)> =
            leaves.iter().map(|l| (l.pos, l.path[0].to_vec())).collect();
        revs.sort();
        assert_eq!(revs, vec![(2, rev("b")), (2, rev("c"))]);

        // winner: higher revid wins among same pos
        let w = tree.winner().unwrap();
        assert_eq!(w.path[0], rev("c").as_slice());

        // full path of winner includes parent
        assert_eq!(w.path.len(), 2);
        assert_eq!(w.path[1], rev("a").as_slice());

        // roundtrip through term encoding
        let t = tree.to_term();
        let tree2 = RevTree::from_term(&t).unwrap();
        assert_eq!(tree2.leaves().len(), 2);
    }

    #[test]
    fn merge_stemmed_path() {
        let mut tree = RevTree::default();
        let (s, n) = path_to_tree(2, &[rev("b"), rev("a")], leaf(1));
        tree.merge_path(s, n);
        // Insert 3-c whose path only knows [c, b] (stemmed history).
        let (s, n) = path_to_tree(3, &[rev("c"), rev("b")], leaf(2));
        tree.merge_path(s, n);
        let leaves = tree.leaves();
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].pos, 3);
        assert_eq!(leaves[0].path.len(), 3); // c, b, a
    }

    #[test]
    fn stem_limits_depth() {
        let mut tree = RevTree::default();
        let path: Vec<Vec<u8>> = (0..10).rev().map(|i| rev(&format!("r{i}"))).collect();
        let (s, n) = path_to_tree(10, &path, leaf(1));
        tree.merge_path(s, n);
        tree.stem(3);
        let leaves = tree.leaves();
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].pos, 10);
        assert_eq!(leaves[0].path.len(), 3);
        assert_eq!(tree.0[0].0, 8); // start depth after stemming
    }

    #[test]
    fn deleted_loses_to_live() {
        let mut tree = RevTree::default();
        let (s, n) = path_to_tree(1, &[rev("a")], leaf(1));
        tree.merge_path(s, n);
        let mut del = leaf(2);
        del.deleted = true;
        // deleted 2-z (higher rev) vs live 1-a conflict? make both leaves:
        let (s, n) = path_to_tree(2, &[rev("z"), rev("a")], del);
        tree.merge_path(s, n);
        let (s, n) = path_to_tree(2, &[rev("b"), rev("a")], leaf(3));
        tree.merge_path(s, n);
        let w = tree.winner().unwrap();
        assert_eq!(w.path[0], rev("b").as_slice()); // live beats deleted
    }
}
