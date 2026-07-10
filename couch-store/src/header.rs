//! Port of couch_bt_engine_header: the #db_header{} record.

use crate::error::{corrupt, Error, Result};
use crate::etf::Term;

pub const LATEST_DISK_VERSION: i64 = 8;

/// Field order matches the Erlang record. `Term` fields keep whatever the
/// file stored (`nil` atom, `undefined` atom, pointer int, or tuple).
#[derive(Clone, Debug)]
pub struct DbHeader {
    pub disk_version: i64,
    pub update_seq: u64,
    pub time_seq_ptr: Term,
    pub id_tree_state: Term,
    pub seq_tree_state: Term,
    pub local_tree_state: Term,
    pub purge_tree_state: Term,
    pub purge_seq_tree_state: Term,
    pub security_ptr: Term,
    pub revs_limit: u64,
    pub uuid: Term,
    pub epochs: Term,
    pub compacted_seq: Term,
    pub purge_infos_limit: u64,
    pub props_ptr: Term,
}

impl DbHeader {
    pub fn new(uuid_hex: String) -> DbHeader {
        DbHeader {
            disk_version: LATEST_DISK_VERSION,
            update_seq: 0,
            time_seq_ptr: Term::atom("undefined"),
            id_tree_state: Term::nil(),
            seq_tree_state: Term::nil(),
            local_tree_state: Term::nil(),
            purge_tree_state: Term::nil(),
            purge_seq_tree_state: Term::nil(),
            security_ptr: Term::nil(),
            revs_limit: 1000,
            uuid: Term::Bin(uuid_hex.into_bytes()),
            epochs: Term::List(vec![Term::Tuple(vec![
                Term::atom("couch-store@rust"),
                Term::Int(0),
            ])]),
            compacted_seq: Term::Int(0),
            purge_infos_limit: 1000,
            props_ptr: Term::atom("undefined"),
        }
    }

    pub fn from_term(t: &Term) -> Result<DbHeader> {
        let tup = t.as_tuple()?;
        if tup.is_empty() || !tup[0].is_atom("db_header") {
            return Err(corrupt(format!("not a db_header: {t:?}")));
        }
        if tup.len() > 16 {
            return Err(Error::Unsupported(format!(
                "db_header has {} fields; written by a newer CouchDB",
                tup.len() - 1
            )));
        }
        // Missing trailing fields take record defaults (upgrade_tuple).
        let get = |i: usize, default: Term| tup.get(i).cloned().unwrap_or(default);
        let disk_version = get(1, Term::Int(0)).as_i64()?;
        if !(6..=LATEST_DISK_VERSION).contains(&disk_version) {
            return Err(Error::Unsupported(format!(
                "disk_version {disk_version} not supported (need 6..{LATEST_DISK_VERSION})"
            )));
        }
        let mut h = DbHeader {
            disk_version,
            update_seq: get(2, Term::Int(0)).as_u64()?,
            time_seq_ptr: get(3, Term::atom("undefined")),
            id_tree_state: get(4, Term::nil()),
            seq_tree_state: get(5, Term::nil()),
            local_tree_state: get(6, Term::nil()),
            purge_tree_state: get(7, Term::nil()),
            purge_seq_tree_state: get(8, Term::nil()),
            security_ptr: get(9, Term::nil()),
            revs_limit: get(10, Term::Int(1000)).as_u64().unwrap_or(1000),
            uuid: get(11, Term::atom("undefined")),
            epochs: get(12, Term::atom("undefined")),
            compacted_seq: get(13, Term::atom("undefined")),
            purge_infos_limit: get(14, Term::Int(1000)).as_u64().unwrap_or(1000),
            props_ptr: get(15, Term::atom("undefined")),
        };
        // Pre-v8 files stored an integer purge_seq in purge_tree_state slot;
        // treat the purge trees as absent (they use an incompatible format).
        if matches!(h.purge_tree_state, Term::Int(_)) {
            h.purge_tree_state = Term::nil();
            h.purge_seq_tree_state = Term::nil();
        }
        Ok(h)
    }

    pub fn to_term(&self) -> Term {
        Term::Tuple(vec![
            Term::atom("db_header"),
            Term::Int(self.disk_version),
            Term::Int(self.update_seq as i64),
            self.time_seq_ptr.clone(),
            self.id_tree_state.clone(),
            self.seq_tree_state.clone(),
            self.local_tree_state.clone(),
            self.purge_tree_state.clone(),
            self.purge_seq_tree_state.clone(),
            self.security_ptr.clone(),
            Term::Int(self.revs_limit as i64),
            self.uuid.clone(),
            self.epochs.clone(),
            self.compacted_seq.clone(),
            Term::Int(self.purge_infos_limit as i64),
            self.props_ptr.clone(),
        ])
    }

    pub fn uuid_str(&self) -> String {
        match &self.uuid {
            Term::Bin(b) => String::from_utf8_lossy(b).into_owned(),
            other => format!("{other:?}"),
        }
    }
}

/// A btree root: `nil` or `{Pointer, Reduction, Size}` (or the pre-1.2
/// `{Pointer, Reduction}`).
#[derive(Clone, Debug)]
pub struct TreeState {
    pub ptr: u64,
    pub red: Term,
    pub size: Option<u64>,
}

impl TreeState {
    pub fn from_term(t: &Term) -> Result<Option<TreeState>> {
        if t.is_atom("nil") {
            return Ok(None);
        }
        let tup = t.as_tuple()?;
        if tup.len() != 2 && tup.len() != 3 {
            return Err(corrupt(format!("bad btree state: {t:?}")));
        }
        Ok(Some(TreeState {
            ptr: tup[0].as_u64()?,
            red: tup[1].clone(),
            size: match tup.get(2) {
                Some(Term::Int(s)) => Some(*s as u64),
                _ => None,
            },
        }))
    }

    pub fn to_term(state: &Option<TreeState>) -> Term {
        match state {
            None => Term::nil(),
            Some(s) => Term::Tuple(vec![
                Term::Int(s.ptr as i64),
                s.red.clone(),
                match s.size {
                    Some(sz) => Term::Int(sz as i64),
                    None => Term::nil(),
                },
            ]),
        }
    }
}
