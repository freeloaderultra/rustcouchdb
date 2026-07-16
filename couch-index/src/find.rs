//! _find execution: pick the best usable index (mango_cursor's ranking),
//! range-scan it, post-filter every candidate with the full selector, and
//! apply skip/limit/fields — or fall back to a full database scan when no
//! index fits.

use crate::index::Index;
use crate::planner;
use couch_mango::Selector;
use couch_store::db::Db;
use couch_store::error::{Error, Result};
use serde_json::{json, Map, Value};
use std::ops::ControlFlow;

pub struct FindQuery {
    pub selector: Value,
    pub limit: usize,
    pub skip: usize,
    pub fields: Option<Vec<String>>,
    pub sort_fields: Vec<String>,
    pub descending: bool,
    pub use_index: Option<String>,
}

impl FindQuery {
    pub fn parse(q: &Value) -> Result<FindQuery> {
        let obj = q
            .as_object()
            .ok_or_else(|| Error::BadRequest("query must be an object".into()))?;
        let selector = planner::selector_of(obj);
        let (sort_fields, descending) =
            planner::parse_sort(obj.get("sort").unwrap_or(&Value::Null))
                .map_err(Error::BadRequest)?;
        let fields = match obj.get("fields") {
            None | Some(Value::Null) => None,
            Some(Value::Array(a)) => Some(
                a.iter()
                    .map(|f| {
                        f.as_str()
                            .map(String::from)
                            .ok_or_else(|| Error::BadRequest("fields must be strings".into()))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            Some(other) => {
                return Err(Error::BadRequest(format!("bad fields: {other}")));
            }
        };
        Ok(FindQuery {
            selector,
            // CouchDB's _find defaults
            limit: obj.get("limit").and_then(|v| v.as_u64()).unwrap_or(25) as usize,
            skip: obj.get("skip").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
            fields,
            sort_fields,
            descending,
            use_index: obj
                .get("use_index")
                .and_then(|v| v.as_str())
                .map(String::from),
        })
    }
}

pub struct Chosen<'a> {
    pub defined: Option<&'a mut crate::index::Defined>,
    pub plan: Option<planner::IndexPlan>,
    pub rejected: Vec<(String, String)>,
}

/// mango_cursor:choose — rank usable index definitions by equality prefix,
/// then constrained prefix, then fewer columns; ties prefer an index that is
/// already materialized over one that would have to be built first.
pub fn choose<'a>(
    defined: &'a mut [crate::index::Defined],
    query: &FindQuery,
) -> Result<Chosen<'a>> {
    let mut rejected = Vec::new();
    let mut best: Option<(usize, (usize, usize, std::cmp::Reverse<usize>, bool))> = None;
    let mut best_plan: Option<planner::IndexPlan> = None;
    for (i, d) in defined.iter().enumerate() {
        if let Some(want) = &query.use_index {
            let ddoc_matches = d.ddoc_id.as_deref().is_some_and(|id| {
                id == want || id.strip_prefix("_design/") == Some(want.as_str())
            });
            if &d.def.name != want && !ddoc_matches {
                continue;
            }
        }
        match planner::plan_index(&d.def.fields, &query.selector, &query.sort_fields) {
            Err(e) => rejected.push((d.def.name.clone(), e)),
            Ok(None) => rejected.push((d.def.name.clone(), "not usable for this selector/sort".into())),
            Ok(Some(plan)) => {
                // A partial index may only serve selectors that imply its
                // filter; conservatively require the query selector to
                // contain it verbatim — otherwise post-filtering could hide
                // matching docs that the index never stored.
                if let Some(pfs) = &d.def.partial_filter_selector {
                    if !selector_implies(&query.selector, pfs) {
                        rejected.push((
                            d.def.name.clone(),
                            "partial index filter not implied by selector".into(),
                        ));
                        continue;
                    }
                }
                let rank = (
                    plan.eq_prefix,
                    plan.constrained_prefix,
                    std::cmp::Reverse(d.def.fields.len()),
                    d.index.is_some(),
                );
                if best.as_ref().map(|(_, r)| rank > *r).unwrap_or(true) {
                    best = Some((i, rank));
                    best_plan = Some(plan);
                }
            }
        }
    }
    match best {
        Some((i, _)) => Ok(Chosen {
            defined: Some(&mut defined[i]),
            plan: best_plan,
            rejected,
        }),
        None => {
            if query.use_index.is_some() {
                return Err(Error::BadRequest(format!(
                    "use_index {:?} is not usable for this query",
                    query.use_index
                )));
            }
            if !query.sort_fields.is_empty() {
                return Err(Error::BadRequest(
                    "no index exists for this sort, try indexing by the sort fields".into(),
                ));
            }
            Ok(Chosen {
                defined: None,
                plan: None,
                rejected,
            })
        }
    }
}

/// Crude implication test used for partial indexes: every clause of `pfs`
/// appears verbatim in the query selector (directly or in an $and).
fn selector_implies(sel: &Value, pfs: &Value) -> bool {
    let (Value::Object(pm), Value::Object(_)) = (pfs, sel) else {
        return false;
    };
    pm.iter().all(|(k, v)| clause_present(sel, k, v))
}

fn clause_present(sel: &Value, key: &str, val: &Value) -> bool {
    match sel {
        Value::Object(m) => {
            if m.get(key) == Some(val) {
                return true;
            }
            if let Some(Value::Array(args)) = m.get("$and") {
                return args.iter().any(|a| clause_present(a, key, val));
            }
            false
        }
        _ => false,
    }
}

pub struct FindStats {
    pub scanned: u64,
    pub docs_examined: u64,
    pub results: u64,
}

/// Execute the query, streaming result docs to `emit`. Takes the (already
/// updated) index by reference rather than the Chosen borrow so callers can
/// release any index-file lock before the scan — reads are pure preads on
/// an append-only file and never conflict with concurrent index writers.
pub fn execute<F>(
    db: &Db,
    index: Option<(&Index, &planner::IndexPlan)>,
    query: &FindQuery,
    selector: &Selector,
    emit: &mut F,
) -> Result<FindStats>
where
    F: FnMut(Value) -> Result<()>,
{
    let mut stats = FindStats {
        scanned: 0,
        docs_examined: 0,
        results: 0,
    };
    let mut skipped = 0usize;
    let mut process = |doc: Value, stats: &mut FindStats| -> Result<ControlFlow<()>> {
        stats.docs_examined += 1;
        if !selector.matches(&doc) {
            return Ok(ControlFlow::Continue(()));
        }
        if skipped < query.skip {
            skipped += 1;
            return Ok(ControlFlow::Continue(()));
        }
        if stats.results as usize >= query.limit {
            return Ok(ControlFlow::Break(()));
        }
        stats.results += 1;
        emit(project(doc, &query.fields))?;
        if stats.results as usize >= query.limit {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    };

    match index {
        Some((idx, plan)) => {
            let (start, end, end_inclusive) = planner::scan_bounds(&plan.ranges);
            idx.scan(&start, &end, end_inclusive, query.descending, &mut |docid| {
                stats.scanned += 1;
                let Some(doc) = db.open_doc(docid, None, &Default::default())? else {
                    return Ok(ControlFlow::Continue(()));
                };
                process(doc, &mut stats)
            })?;
        }
        None => {
            // full scan fallback (like _all_docs-backed _find)
            db.fold_docs(|fdi| {
                // mango never yields design docs from a full scan
                if fdi.deleted || fdi.id.starts_with(b"_design/") {
                    return Ok(ControlFlow::Continue(()));
                }
                stats.scanned += 1;
                let Some(w) = fdi.rev_tree.winner() else {
                    return Ok(ControlFlow::Continue(()));
                };
                let doc = db.doc_json(&fdi, &w, &Default::default())?;
                process(doc, &mut stats)
            })?;
        }
    }
    Ok(stats)
}

/// Field projection with dotted paths (mango_fields).
fn project(doc: Value, fields: &Option<Vec<String>>) -> Value {
    let Some(fields) = fields else { return doc };
    let mut out = Map::new();
    for f in fields {
        if let Some(v) = couch_mango::get_field(&doc, f) {
            insert_path(&mut out, f, v.clone());
        }
    }
    Value::Object(out)
}

fn insert_path(out: &mut Map<String, Value>, field: &str, v: Value) {
    let mut segs: Vec<&str> = field.split('.').collect();
    let last = segs.pop().unwrap_or(field);
    let mut cur = out;
    for s in segs {
        let entry = cur
            .entry(s.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        cur = entry.as_object_mut().expect("just ensured object");
    }
    cur.insert(last.to_string(), v);
}

pub fn explain(db_path: &str, chosen: &Chosen<'_>, query: &FindQuery) -> Value {
    let index_info = chosen.defined.as_ref().map(|d| {
        let mut info = match &d.index {
            Some(i) => i.info(),
            None => json!({
                "name": d.def.name,
                "fields": d.def.fields,
                "partial_filter_selector": d.def.partial_filter_selector,
                "state": "unbuilt (materializes on first _find)",
            }),
        };
        if let Some(id) = &d.ddoc_id {
            info["ddoc"] = json!(id);
        }
        info
    });
    let bounds = chosen.plan.as_ref().map(|p| {
        let (start, end, incl) = planner::scan_bounds(&p.ranges);
        json!({
            "start_key": start.iter().map(bound_json).collect::<Vec<_>>(),
            "end_key": end.iter().map(bound_json).collect::<Vec<_>>(),
            "end_inclusive": incl,
        })
    });
    json!({
        "dbname": db_path,
        "index": index_info.unwrap_or(json!({"type": "full_scan"})),
        "range": bounds,
        "selector": query.selector,
        "limit": query.limit,
        "skip": query.skip,
        "fields": query.fields,
        "sort": {"fields": query.sort_fields, "descending": query.descending},
        "rejected_indexes": chosen
            .rejected
            .iter()
            .map(|(n, r)| json!({"name": n, "reason": r}))
            .collect::<Vec<_>>(),
    })
}

fn bound_json(b: &crate::keys::Bound) -> Value {
    match b {
        crate::keys::Bound::Min => json!("<min>"),
        crate::keys::Bound::Val(v) => v.clone(),
        crate::keys::Bound::Max => json!("<max>"),
    }
}
