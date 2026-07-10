mod find;
mod index;
mod keys;
mod planner;

use clap::{Parser, Subcommand};
use couch_store::db::Db;
use couch_store::error::{Error, Result};
use find::FindQuery;
use serde_json::json;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "couch-index",
    about = "Native Mango JSON indexes for CouchDB databases — no couchjs, no Erlang. Indexes live next to the .couch file and update incrementally from its changes feed."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create an index (and build it)
    Create {
        /// Source .couch database file
        db: String,
        /// Comma-separated field list, e.g. db.DocType,db.CreatedAtMs
        #[arg(long, value_delimiter = ',', required = true)]
        fields: Vec<String>,
        /// Index name (default: derived from the definition)
        #[arg(long)]
        name: Option<String>,
        /// Only index docs matching this Mango selector (JSON)
        #[arg(long)]
        partial_filter_selector: Option<String>,
        /// Index directory (default: <db>.indexes)
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// List indexes with row counts and seqs
    List {
        db: String,
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Bring all indexes up to date with the database
    Update {
        db: String,
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Delete an index by name
    Delete {
        db: String,
        name: String,
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Run a Mango _find query: '{"selector": {...}, "limit": N, "sort": [...], "fields": [...]}'
    Find {
        db: String,
        /// Query JSON (or @path to read from a file, or - for stdin)
        query: String,
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Print the query plan instead of executing
        #[arg(long)]
        explain: bool,
        /// Skip the pre-query index update (query possibly-stale index)
        #[arg(long)]
        stale: bool,
        /// Print execution stats to stderr
        #[arg(long)]
        stats: bool,
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

fn dir_for(db: &str, dir: &Option<PathBuf>) -> PathBuf {
    dir.clone().unwrap_or_else(|| index::index_dir(db))
}

fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Create {
            db,
            fields,
            name,
            partial_filter_selector,
            dir,
        } => {
            let database = Db::open(&db)?;
            let pfs = match partial_filter_selector {
                Some(s) => Some(
                    serde_json::from_str(&s)
                        .map_err(|e| Error::BadRequest(format!("bad selector JSON: {e}")))?,
                ),
                None => None,
            };
            let mut def = index::IndexDef {
                name: String::new(),
                fields,
                partial_filter_selector: pfs,
            };
            def.name = name.unwrap_or_else(|| def.auto_name());
            let mut idx =
                index::Index::create(&dir_for(&db, &dir), def, &database.header.uuid_str())?;
            let n = idx.update(&database)?;
            println_json(
                &json!({"ok": true, "name": idx.def.name, "docs_indexed": n, "rows": idx.row_count()}),
            );
        }
        Cmd::List { db, dir } => {
            for idx in index::list(&dir_for(&db, &dir))? {
                println_json(&idx.info());
            }
        }
        Cmd::Update { db, dir } => {
            let database = Db::open(&db)?;
            for mut idx in index::list(&dir_for(&db, &dir))? {
                let n = idx.update(&database)?;
                println_json(
                    &json!({"name": idx.def.name, "docs_processed": n, "rows": idx.row_count(), "update_seq": idx.update_seq}),
                );
            }
        }
        Cmd::Delete { db, name, dir } => {
            let path = dir_for(&db, &dir).join(format!("{name}.fidx"));
            if !path.exists() {
                return Err(Error::BadRequest(format!("no such index: {name}")));
            }
            std::fs::remove_file(&path)?;
            println_json(&json!({"ok": true, "deleted": name}));
        }
        Cmd::Find {
            db,
            query,
            dir,
            explain,
            stale,
            stats,
        } => {
            let qstr = if query == "-" {
                std::io::read_to_string(std::io::stdin())?
            } else if let Some(path) = query.strip_prefix('@') {
                std::fs::read_to_string(path)?
            } else {
                query
            };
            let qjson: serde_json::Value = serde_json::from_str(&qstr)
                .map_err(|e| Error::BadRequest(format!("bad query JSON: {e}")))?;
            let fq = FindQuery::parse(&qjson)?;
            let selector = couch_mango::Selector::compile(&fq.selector)
                .map_err(|e| Error::BadRequest(format!("invalid selector: {e}")))?;
            let database = Db::open(&db)?;
            let mut indexes = index::list(&dir_for(&db, &dir))?;
            let mut chosen = find::choose(&mut indexes, &fq)?;
            if explain {
                println_json(&find::explain(&db, &chosen, &fq));
                return Ok(());
            }
            if !stale {
                if let Some(idx) = chosen.index.as_deref_mut() {
                    idx.update(&database)?;
                }
            }
            let run_stats = find::execute(&database, &chosen, &fq, &selector, &mut |doc| {
                println_json(&doc);
                Ok(())
            })?;
            if stats {
                eprintln!(
                    "index={} rows_scanned={} docs_examined={} results={}",
                    chosen
                        .index
                        .as_ref()
                        .map(|i| i.def.name.clone())
                        .unwrap_or_else(|| "<full-scan>".into()),
                    run_stats.scanned,
                    run_stats.docs_examined,
                    run_stats.results
                );
            }
        }
    }
    Ok(())
}
