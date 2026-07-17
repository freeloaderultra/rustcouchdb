//! Mango index definitions stored in `language: "query"` design documents —
//! CouchDB's source of truth for JSON indexes. The .fidx files next to the
//! database are materializations of these definitions: a definition kept in
//! a design doc survives file-level copies of the .couch and replicates with
//! the data, while a bare .fidx is lost the moment the data directory is
//! rebuilt. (Exactly that bit us: a migrated database carried 21 Mango
//! design docs but no .fidx files, and every _find fell back to a full scan.)

use crate::index::{IndexDef, IndexKind};
use couch_store::db::Db;
use couch_store::error::Result;
use serde_json::{json, Value};
use std::ops::ControlFlow;

pub struct DdocIndex {
    pub ddoc_id: String,
    pub def: IndexDef,
}

/// Parse every Mango index out of one design doc body. Non-query ddocs and
/// views without an `options.def` (the ordered CouchDB definition) yield
/// nothing. `map.fields` is not used as a fallback: it is a JSON object and
/// field order — which the index key depends on — is not preserved there.
pub fn defs_in_ddoc(doc: &Value) -> Vec<DdocIndex> {
    let mut out = Vec::new();
    if doc.get("language").and_then(|l| l.as_str()) != Some("query") {
        return out;
    }
    let Some(ddoc_id) = doc.get("_id").and_then(|v| v.as_str()) else {
        return out;
    };
    if let Some(views) = doc.get("views").and_then(|v| v.as_object()) {
        for (name, view) in views {
            let Some(def) = view.get("options").and_then(|o| o.get("def")) else {
                continue;
            };
            let Some(fields) = def.get("fields").and_then(|f| f.as_array()) else {
                continue;
            };
            let mut cols = Vec::with_capacity(fields.len());
            let mut ok = true;
            for f in fields {
                match f {
                    Value::String(s) => cols.push(s.clone()),
                    Value::Object(o) if o.len() == 1 => {
                        cols.push(o.keys().next().unwrap().clone())
                    }
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok || cols.is_empty() {
                continue;
            }
            let pfs = def
                .get("partial_filter_selector")
                .or_else(|| view.get("map").and_then(|m| m.get("partial_filter_selector")))
                .filter(|s| !s.is_null() && !matches!(s, Value::Object(o) if o.is_empty()))
                .cloned();
            out.push(DdocIndex {
                ddoc_id: ddoc_id.to_string(),
                def: IndexDef {
                    name: name.clone(),
                    fields: cols,
                    partial_filter_selector: pfs,
                    kind: IndexKind::Json,
                },
            });
        }
    }
    // Spatial definitions live in their own section (a rustcouchdb
    // extension; CouchDB ignores unknown design-doc members, so these
    // ddocs still replicate to stock peers without breaking anything).
    if let Some(spatial) = doc.get("spatial").and_then(|v| v.as_object()) {
        for (name, entry) in spatial {
            let Some(def) = entry.get("options").and_then(|o| o.get("def")) else {
                continue;
            };
            let Some(fields) = def.get("fields").and_then(|f| f.as_array()) else {
                continue;
            };
            let cols: Option<Vec<String>> = fields
                .iter()
                .map(|f| f.as_str().map(String::from))
                .collect();
            let Some(cols) = cols else { continue };
            if cols.len() != 4 {
                continue;
            }
            let pfs = def
                .get("partial_filter_selector")
                .filter(|s| !s.is_null() && !matches!(s, Value::Object(o) if o.is_empty()))
                .cloned();
            out.push(DdocIndex {
                ddoc_id: ddoc_id.to_string(),
                def: IndexDef {
                    name: name.clone(),
                    fields: cols,
                    partial_filter_selector: pfs,
                    kind: IndexKind::Spatial,
                },
            });
        }
    }
    out
}

/// All Mango index definitions in the database's design docs.
pub fn scan(db: &Db) -> Result<Vec<DdocIndex>> {
    let mut out = Vec::new();
    db.fold_docs_from(b"_design/", |fdi| {
        if !fdi.id.starts_with(b"_design/") {
            return Ok(ControlFlow::Break(()));
        }
        if fdi.deleted {
            return Ok(ControlFlow::Continue(()));
        }
        let Some(w) = fdi.rev_tree.winner() else {
            return Ok(ControlFlow::Continue(()));
        };
        let doc = db.doc_json(&fdi, &w, &Default::default())?;
        out.extend(defs_in_ddoc(&doc));
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(out)
}

/// CouchDB-shaped design doc body for one index (the shape couch_mango
/// writes), so the ddoc replicates cleanly to real CouchDB peers. Spatial
/// definitions go in their own `spatial` section instead of `views`.
pub fn ddoc_body(ddoc_id: &str, def: &IndexDef) -> Value {
    if def.kind == IndexKind::Spatial {
        let mut inner = json!({"fields": def.fields, "type": "spatial"});
        if let Some(pfs) = &def.partial_filter_selector {
            inner["partial_filter_selector"] = pfs.clone();
        }
        let mut spatial = serde_json::Map::new();
        spatial.insert(def.name.clone(), json!({"options": {"def": inner}}));
        return json!({
            "_id": ddoc_id,
            "language": "query",
            "spatial": Value::Object(spatial),
        });
    }
    let mut map_fields = serde_json::Map::new();
    for f in &def.fields {
        map_fields.insert(f.clone(), json!("asc"));
    }
    let mut map = json!({"fields": Value::Object(map_fields)});
    let mut inner = json!({"fields": def.fields});
    if let Some(pfs) = &def.partial_filter_selector {
        map["partial_filter_selector"] = pfs.clone();
        inner["partial_filter_selector"] = pfs.clone();
    } else {
        map["partial_filter_selector"] = json!({});
    }
    let mut views = serde_json::Map::new();
    views.insert(
        def.name.clone(),
        json!({"map": map, "reduce": "_count", "options": {"def": inner}}),
    );
    json!({
        "_id": ddoc_id,
        "language": "query",
        "views": Value::Object(views),
    })
}
