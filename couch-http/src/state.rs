//! Server state: the data directory and one `Database` per .couch file.
//!
//! Concurrency model: each database has a single `DbWriter` behind a mutex
//! (all writes serialize, every batch commits) and an immutable read
//! snapshot (`Arc<Db>`) swapped after each commit. Readers never block
//! writers; a snapshot stays valid even while the file grows (append-only)
//! or is compacted away under it (the fd keeps the old inode alive).

use crate::error::{ApiError, ApiResult};
use axum::http::StatusCode;
use couch_store::db::Db;
use couch_store::writer::DbWriter;
use md5::{Digest, Md5};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

pub struct Database {
    pub name: String,
    pub path: String,
    /// None only while a compaction has the file swapped out.
    writer: Mutex<Option<DbWriter>>,
    snap: RwLock<Arc<Db>>,
    pub seq_rx: watch::Receiver<u64>,
    seq_tx: watch::Sender<u64>,
    pub compacting: AtomicBool,
    /// Serializes index builds/updates for this db (index files are RW).
    pub index_lock: tokio::sync::Mutex<()>,
}

impl Database {
    fn from_parts(name: &str, path: String, writer: DbWriter, snap: Db) -> Database {
        let (seq_tx, seq_rx) = watch::channel(snap.header.update_seq);
        Database {
            name: name.to_string(),
            path,
            writer: Mutex::new(Some(writer)),
            snap: RwLock::new(Arc::new(snap)),
            seq_rx,
            seq_tx,
            compacting: AtomicBool::new(false),
            index_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn open(name: &str, path: String) -> ApiResult<Database> {
        let writer = DbWriter::open(&path)?;
        let snap = Db::open(&path)?;
        Ok(Database::from_parts(name, path, writer, snap))
    }

    pub fn create(name: &str, path: String) -> ApiResult<Database> {
        let writer = DbWriter::create(&path)?;
        let snap = Db::open(&path)?;
        Ok(Database::from_parts(name, path, writer, snap))
    }

    /// The current committed read snapshot.
    pub fn snapshot(&self) -> Arc<Db> {
        self.snap.read().unwrap().clone()
    }

    /// Run a write batch under the writer lock, commit it, refresh the read
    /// snapshot and wake changes feeds. Call from a blocking context.
    pub fn with_writer<T>(
        &self,
        f: impl FnOnce(&mut DbWriter) -> couch_store::error::Result<T>,
    ) -> ApiResult<T> {
        let mut guard = self.writer.lock().unwrap();
        let w = guard.as_mut().ok_or_else(|| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "compaction_running",
                "database is being compacted, retry",
            )
        })?;
        let out = f(w)?;
        w.commit()?;
        let seq = w.update_seq();
        drop(guard);
        crate::metrics::bump(&crate::metrics::DATABASE_WRITES);
        self.refresh(seq)?;
        Ok(out)
    }

    fn refresh(&self, seq: u64) -> ApiResult<()> {
        let db = Db::open(&self.path)?;
        // refresh() runs after the writer lock is dropped, so two writers can
        // race here: only ever install a NEWER snapshot, or an acknowledged
        // write would disappear from reads until the next write lands.
        {
            let mut g = self.snap.write().unwrap();
            if db.header.update_seq >= g.header.update_seq {
                *g = Arc::new(db);
            }
        }
        self.seq_tx.send_if_modified(|cur| {
            if seq > *cur {
                *cur = seq;
                true
            } else {
                false
            }
        });
        Ok(())
    }

    /// Compact the file in place. Holds the writer slot empty for the
    /// duration; readers keep serving from the pre-compaction snapshot.
    pub fn compact(&self) -> ApiResult<()> {
        let mut guard = self.writer.lock().unwrap();
        if guard.is_none() {
            return Ok(()); // already compacting
        }
        self.compacting.store(true, Ordering::SeqCst);
        *guard = None; // close the writer fd before the swap
        let result = couch_store::compact::compact(&self.path);
        let reopen = DbWriter::open(&self.path);
        self.compacting.store(false, Ordering::SeqCst);
        match (result, reopen) {
            (Ok(_), Ok(w)) => {
                let seq = w.update_seq();
                *guard = Some(w);
                drop(guard);
                self.refresh(seq)
            }
            (Err(e), Ok(w)) => {
                *guard = Some(w);
                Err(e.into())
            }
            (_, Err(e)) => Err(e.into()),
        }
    }
}

/// On-disk layout of the data directory.
///
/// `Flat` is rustcouchdb's native layout: `<dir>/<name>.couch`.
///
/// `CouchDb` serves a CouchDB 3.x node directory *in place*:
/// `<dir>/shards/00000000-ffffffff/<name>.<timestamp>.couch`. An existing
/// q=1 CouchDB volume can be mounted with zero migration — same files, same
/// byte format — and switching the image back to Erlang CouchDB rolls back
/// losslessly, including writes made in the meantime. (Databases *created*
/// while running rustcouchdb are not registered in CouchDB's _dbs.couch
/// shard map, so only those wouldn't reappear after a rollback.)
/// `_dbs.couch`/`_nodes.couch` in the directory root are cluster
/// bookkeeping and are ignored; `.shards/` (Erlang view indexes) too.
#[derive(Debug)]
enum Layout {
    Flat,
    CouchDb { range_dir: PathBuf },
}

/// The single full shard range a q=1 database lives in.
const FULL_RANGE: &str = "00000000-ffffffff";

pub struct ServerState {
    pub dir: PathBuf,
    pub admin: Option<(String, String)>,
    pub secret: [u8; 16],
    pub server_uuid: String,
    pub base_url: RwLock<String>,
    pub dbs: RwLock<HashMap<String, Arc<Database>>>,
    pub soft_delete_validator: bool,
    pub repl: crate::repl::ReplManager,
    layout: Layout,
    uuid_counter: AtomicU64,
    /// Proto registry cache: (update_seq of the `_schemas` snapshot it was
    /// built from, the registry — None when `_schemas` holds nothing usable).
    proto_cache: RwLock<Option<(u64, Option<Arc<couch_proto::Registry>>)>>,
}

pub type App = Arc<ServerState>;

impl ServerState {
    pub fn new(dir: PathBuf, admin: Option<(String, String)>, soft_delete_validator: bool) -> ServerState {
        crate::metrics::init_start();
        let secret: [u8; 16] = rand::random();
        let layout = if dir.join("shards").is_dir() {
            Layout::CouchDb {
                range_dir: dir.join("shards").join(FULL_RANGE),
            }
        } else {
            Layout::Flat
        };
        ServerState {
            server_uuid: gen_uuid(),
            dir,
            admin,
            secret,
            base_url: RwLock::new(String::new()),
            dbs: RwLock::new(HashMap::new()),
            soft_delete_validator,
            repl: crate::repl::ReplManager::default(),
            layout,
            uuid_counter: AtomicU64::new(0),
            proto_cache: RwLock::new(None),
        }
    }

    /// The proto schema registry built from the `_schemas` database, rebuilt
    /// whenever that database's update_seq moves. Ok(None) without a usable
    /// `_schemas`; a structurally broken `_schemas` is an error for the
    /// caller to surface (failures are never cached — a fixed `_schemas`
    /// heals on the next call). Does file IO on rebuild — call from a
    /// blocking context.
    pub fn proto_registry(&self) -> ApiResult<Option<Arc<couch_proto::Registry>>> {
        let Some(sdb) = self.dbs.read().unwrap().get(crate::proto::SCHEMAS_DB).cloned() else {
            return Ok(None);
        };
        let snap = sdb.snapshot();
        let seq = snap.header.update_seq;
        if let Some((cached_seq, reg)) = &*self.proto_cache.read().unwrap() {
            if *cached_seq == seq {
                return Ok(reg.clone());
            }
        }
        let reg = crate::proto::build_registry(&snap)?;
        *self.proto_cache.write().unwrap() = Some((seq, reg.clone()));
        Ok(reg)
    }

    /// Where a NEW database with this name would be created. Databases that
    /// already exist carry their own path (`Database.path`); in the CouchDb
    /// layout that keeps an adopted shard file's original timestamp suffix
    /// while new databases get a CouchDB-style creation-time suffix.
    pub fn db_path(&self, name: &str) -> String {
        match &self.layout {
            Layout::Flat => self.dir.join(format!("{name}.couch")),
            Layout::CouchDb { range_dir } => {
                range_dir.join(format!("{name}.{}.couch", now_secs()))
            }
        }
        .to_string_lossy()
        .into_owned()
    }

    /// Scan the data dir and open every database (server startup).
    pub fn open_all(&self) -> ApiResult<()> {
        std::fs::create_dir_all(&self.dir).map_err(couch_store::error::Error::Io)?;
        let mut dbs = self.dbs.write().unwrap();
        let open_into = |name: &str, path: PathBuf, dbs: &mut HashMap<String, Arc<Database>>| {
            match Database::open(name, path.to_string_lossy().into_owned()) {
                Ok(db) => {
                    dbs.insert(name.to_string(), Arc::new(db));
                }
                Err(e) => {
                    tracing::error!("cannot open {}: {} {}", path.display(), e.error, e.reason);
                }
            }
        };
        match &self.layout {
            Layout::Flat => {
                for entry in std::fs::read_dir(&self.dir).map_err(couch_store::error::Error::Io)? {
                    let entry = entry.map_err(couch_store::error::Error::Io)?;
                    let fname = entry.file_name().to_string_lossy().into_owned();
                    if let Some(name) = fname.strip_suffix(".couch") {
                        if valid_db_name(name) {
                            open_into(name, entry.path(), &mut dbs);
                        }
                    }
                }
            }
            Layout::CouchDb { range_dir } => {
                // q=1 only: a single full-range shard is the whole database.
                // Any other range means the data is split across shards we
                // cannot merge — refuse to start rather than serve slices.
                let shards_dir = self.dir.join("shards");
                for entry in std::fs::read_dir(&shards_dir).map_err(couch_store::error::Error::Io)? {
                    let entry = entry.map_err(couch_store::error::Error::Io)?;
                    let range = entry.file_name().to_string_lossy().into_owned();
                    if entry.path().is_dir() && range != FULL_RANGE {
                        return Err(ApiError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "unsupported_shard_layout",
                            format!(
                                "shard range {range} found: only q=1 CouchDB data \
                                 directories (a single {FULL_RANGE} range) can be \
                                 served in place"
                            ),
                        ));
                    }
                }
                tracing::info!(
                    "serving CouchDB q=1 data directory in place: {}",
                    range_dir.display()
                );
                // <name>.<creation-ts>.couch; a recreated db can leave
                // several files for one name — CouchDB opens the one in its
                // shard map, which for live data is the newest.
                let mut newest: HashMap<String, (u64, PathBuf)> = HashMap::new();
                if range_dir.is_dir() {
                    for entry in std::fs::read_dir(range_dir).map_err(couch_store::error::Error::Io)? {
                        let entry = entry.map_err(couch_store::error::Error::Io)?;
                        let fname = entry.file_name().to_string_lossy().into_owned();
                        let Some(stem) = fname.strip_suffix(".couch") else {
                            continue;
                        };
                        let Some((name, ts)) = stem.rsplit_once('.') else {
                            continue;
                        };
                        let Ok(ts) = ts.parse::<u64>() else { continue };
                        if !valid_db_name(name) {
                            continue;
                        }
                        let e = newest.entry(name.to_string()).or_insert((ts, entry.path()));
                        if ts > e.0 {
                            *e = (ts, entry.path());
                        }
                    }
                }
                for (name, (_, path)) in newest {
                    open_into(&name, path, &mut dbs);
                }
            }
        }
        Ok(())
    }

    pub fn db(&self, name: &str) -> ApiResult<Arc<Database>> {
        self.dbs
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(ApiError::db_not_found)
    }

    pub fn create_db(&self, name: &str) -> ApiResult<Arc<Database>> {
        if !valid_db_name(name) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "illegal_database_name",
                format!(
                    "Name: '{name}'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."
                ),
            ));
        }
        let mut dbs = self.dbs.write().unwrap();
        if dbs.contains_key(name) {
            return Err(ApiError::new(
                StatusCode::PRECONDITION_FAILED,
                "file_exists",
                "The database could not be created, the file already exists.",
            ));
        }
        let path = self.db_path(name);
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).map_err(couch_store::error::Error::Io)?;
        }
        let db = Arc::new(Database::create(name, path)?);
        dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub fn delete_db(&self, name: &str) -> ApiResult<()> {
        let db = {
            let mut dbs = self.dbs.write().unwrap();
            dbs.remove(name).ok_or_else(ApiError::db_not_found)?
        };
        std::fs::remove_file(&db.path).map_err(couch_store::error::Error::Io)?;
        let _ = std::fs::remove_dir_all(couch_index::index::index_dir(&db.path));
        // In an adopted CouchDB layout a recreated database can have left
        // older <name>.<ts>.couch files behind; remove them too, or a
        // restart would resurrect the newest leftover as the database.
        if let Layout::CouchDb { range_dir } = &self.layout {
            let entries = match std::fs::read_dir(range_dir) {
                Ok(e) => e,
                Err(_) => return Ok(()),
            };
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().into_owned();
                if let Some(ts) = fname
                    .strip_prefix(&format!("{name}."))
                    .and_then(|r| r.strip_suffix(".couch"))
                {
                    if ts.chars().all(|c| c.is_ascii_digit()) {
                        let _ = std::fs::remove_file(entry.path());
                        let _ = std::fs::remove_dir_all(couch_index::index::index_dir(
                            &entry.path().to_string_lossy(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn all_db_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.dbs.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn next_uuid(&self) -> String {
        let n = self.uuid_counter.fetch_add(1, Ordering::Relaxed);
        let mut h = Md5::new();
        h.update(self.server_uuid.as_bytes());
        h.update(n.to_le_bytes());
        h.update(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_le_bytes(),
        );
        hex(&h.finalize())
    }

    /// The validator applied to interactive writes on this db (if any).
    ///
    /// Enforcement is per-database, like CouchDB: the native rule only kicks
    /// in once the client has installed its `_design/nxguide` validator design
    /// doc there (the JS body itself stays inert). Databases without the ddoc
    /// accept writes unvalidated, exactly as stock CouchDB would.
    pub fn validator_for(&self, db_name: &str) -> Option<couch_store::writer::Validator<'static>> {
        if !self.soft_delete_validator || db_name.starts_with('_') {
            return None;
        }
        let Ok(dbh) = self.db(db_name) else { return None };
        let snap = dbh.snapshot();
        let installed = snap
            .open_doc(b"_design/nxguide", None, &Default::default())
            .ok()
            .flatten()
            .map(|d| {
                d.get("_deleted") != Some(&Value::Bool(true))
                    && d.get("validate_doc_update").map(|v| !v.is_null()).unwrap_or(false)
            })
            .unwrap_or(false);
        if installed {
            Some(&crate::validate::nxguide_soft_delete)
        } else {
            None
        }
    }
}

/// CouchDB db-name rule, minus `/` (no sharding here) but allowing the
/// leading-underscore system names CouchDB accepts (_replicator, _users,
/// _global_changes) plus rustcouchdb's _schemas (proto descriptor sets).
pub fn valid_db_name(name: &str) -> bool {
    if name == "_replicator"
        || name == "_users"
        || name == "_global_changes"
        || name == crate::proto::SCHEMAS_DB
    {
        return true;
    }
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "_$()+-".contains(c))
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn gen_uuid() -> String {
    let r: [u8; 16] = rand::random();
    hex(&r)
}

/// Run blocking storage work without stalling the async worker.
pub fn blocking<T>(f: impl FnOnce() -> T) -> T {
    tokio::task::block_in_place(f)
}

/// Current time in seconds since the epoch.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// ISO8601 UTC timestamp (what _scheduler reports).
pub fn iso8601(secs: u64) -> String {
    // Days-to-date conversion, civil-from-days (Howard Hinnant's algorithm).
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Parse a "since" / seq value: plain int, "N", "N-opaque", "now", 0.
pub fn parse_seq(v: &str, now: u64) -> u64 {
    if v == "now" {
        return now;
    }
    let head = v.split('-').next().unwrap_or("0");
    head.parse().unwrap_or(0)
}

pub fn seq_json(seq: u64) -> Value {
    Value::String(seq.to_string())
}
