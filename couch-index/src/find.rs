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

/// How a chosen index will be scanned.
pub enum Plan {
    View(planner::IndexPlan),
    Spatial(planner::SpatialPlan),
}

pub struct Chosen<'a> {
    pub defined: Option<&'a mut crate::index::Defined>,
    pub plan: Option<Plan>,
    pub rejected: Vec<(String, String)>,
}

/// mango_cursor:choose — rank usable index definitions by equality prefix,
/// then constrained prefix, then fewer columns; ties prefer an index that is
/// already materialized over one that would have to be built first. A usable
/// spatial index outranks JSON indexes: its usability already implies the
/// selector constrains a bbox, and no btree prefix can serve that region.
pub fn choose<'a>(
    defined: &'a mut [crate::index::Defined],
    query: &FindQuery,
) -> Result<Chosen<'a>> {
    type Rank = (bool, usize, usize, std::cmp::Reverse<usize>, bool);
    let mut rejected = Vec::new();
    let mut best: Option<(usize, Rank)> = None;
    let mut best_plan: Option<Plan> = None;
    for (i, d) in defined.iter().enumerate() {
        if let Some(want) = &query.use_index {
            let ddoc_matches = d.ddoc_id.as_deref().is_some_and(|id| {
                id == want || id.strip_prefix("_design/") == Some(want.as_str())
            });
            if &d.def.name != want && !ddoc_matches {
                continue;
            }
        }
        let planned = match d.def.kind {
            crate::index::IndexKind::Json => {
                planner::plan_index(&d.def.fields, &query.selector, &query.sort_fields).map(
                    |opt| {
                        opt.map(|plan| {
                            let rank = (
                                false,
                                plan.eq_prefix,
                                plan.constrained_prefix,
                                std::cmp::Reverse(d.def.fields.len()),
                                d.index.is_some(),
                            );
                            (Plan::View(plan), rank)
                        })
                    },
                )
            }
            crate::index::IndexKind::Spatial => {
                planner::plan_spatial(&d.def.fields, &query.selector, &query.sort_fields).map(
                    |opt| {
                        opt.map(|plan| {
                            let rank =
                                (true, 4, 4, std::cmp::Reverse(4), d.index.is_some());
                            (Plan::Spatial(plan), rank)
                        })
                    },
                )
            }
        };
        match planned {
            Err(e) => rejected.push((d.def.name.clone(), e)),
            Ok(None) => rejected.push((d.def.name.clone(), "not usable for this selector/sort".into())),
            Ok(Some((plan, rank))) => {
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

/// Optional proto-blob augmentation hook: given the database and a stored
/// doc, returns the augmented view (decoded blob fields + head overlay) when
/// the doc is a decodable blob document, None otherwise. Selector matching
/// and field projection run against the augmented view; docs emitted without
/// a `fields` projection stay the stored JSON.
///
/// The third argument is the set of paths the caller will actually look at:
/// `Some(paths)` lets the implementation extract just those fields from the
/// blob's wire bytes (index updates); `None` demands the full decoded view
/// (arbitrary selectors in _find).
///
/// `Ok(None)` means "not a decodable blob document" (no proto attachment,
/// or its type has no registered schema) — the doc is matched as stored.
/// Errors are real failures (corrupt data, unreadable attachment) and fail
/// the operation; they are never downgraded to an opaque doc.
pub type Augmenter<'a> = &'a dyn Fn(&Db, &Value, Option<&[String]>) -> Result<Option<Value>>;

/// Evaluate the selector against a stored document. The proto-native
/// implementation (couch-http) navigates the proto message on the wire, reading
/// only the fields the selector touches; when absent, matching falls back to the
/// selector's own `matches` on the raw doc.
pub type DocMatch<'a> = &'a dyn Fn(&Value, &Selector) -> Result<bool>;

/// Execute the query, streaming result docs to `emit`. Takes the (already
/// updated) index by reference rather than the Chosen borrow so callers can
/// release any index-file lock before the scan — reads are pure preads on
/// an append-only file and never conflict with concurrent index writers.
pub fn execute<F>(
    db: &Db,
    index: Option<(&Index, &Plan)>,
    query: &FindQuery,
    selector: &Selector,
    augment: Option<Augmenter<'_>>,
    match_doc: Option<DocMatch<'_>>,
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
        // Match by navigating the proto message directly (proto3 defaults
        // honored, no whole-document JSON materialized). Without a matcher wired
        // (a non-proto index run), match the raw doc.
        let matched = match match_doc {
            Some(m) => m(&doc, selector)?,
            None => selector.matches(&doc),
        };
        if !matched {
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
        // A projection may name blob-interior fields (the caller opted in) —
        // render the (projected) view for output; a bare doc result stays the
        // stored JSON, so the common no-projection path never materializes.
        let out = match &query.fields {
            Some(fields) => match augment {
                Some(f) => f(db, &doc, Some(fields))?.unwrap_or(doc),
                None => doc,
            },
            None => doc,
        };
        emit(project(out, &query.fields))?;
        if stats.results as usize >= query.limit {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    };

    match index {
        Some((idx, Plan::View(plan))) => {
            let (start, end, end_inclusive) = planner::scan_bounds(&plan.ranges);
            idx.scan(&start, &end, end_inclusive, query.descending, &mut |docid| {
                stats.scanned += 1;
                let Some(doc) = db.open_doc(docid, None, &Default::default())? else {
                    return Ok(ControlFlow::Continue(()));
                };
                process(doc, &mut stats)
            })?;
        }
        Some((idx, Plan::Spatial(plan))) => {
            // The covering's ranges are disjoint by construction, so no
            // dedup is needed; every candidate goes through the same full
            // selector post-filter, which makes false positives harmless.
            let mut done = false;
            for cover in crate::spatial::covering(&plan.query) {
                let cell = crate::keys::Bound::Val(json!(cover.key));
                let (end, end_inclusive) = if cover.subtree {
                    (
                        crate::keys::Bound::Val(json!(crate::spatial::subtree_end(&cover.key))),
                        false,
                    )
                } else {
                    (cell.clone(), true)
                };
                idx.scan(
                    std::slice::from_ref(&cell),
                    std::slice::from_ref(&end),
                    end_inclusive,
                    false,
                    &mut |docid| {
                        stats.scanned += 1;
                        let Some(doc) = db.open_doc(docid, None, &Default::default())? else {
                            return Ok(ControlFlow::Continue(()));
                        };
                        match process(doc, &mut stats)? {
                            ControlFlow::Break(()) => {
                                done = true;
                                Ok(ControlFlow::Break(()))
                            }
                            c => Ok(c),
                        }
                    },
                )?;
                if done {
                    break;
                }
            }
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
                "type": match d.def.kind {
                    crate::index::IndexKind::Json => "json",
                    crate::index::IndexKind::Spatial => "spatial",
                },
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
    let bounds = chosen.plan.as_ref().map(|p| match p {
        Plan::View(p) => {
            let (start, end, incl) = planner::scan_bounds(&p.ranges);
            json!({
                "start_key": start.iter().map(bound_json).collect::<Vec<_>>(),
                "end_key": end.iter().map(bound_json).collect::<Vec<_>>(),
                "end_inclusive": incl,
            })
        }
        Plan::Spatial(p) => {
            let inf = |x: f64| if x.is_finite() { json!(x) } else { json!(null) };
            json!({
                "query_bbox": {
                    "west": inf(p.query.w), "south": inf(p.query.s),
                    "east": inf(p.query.e), "north": inf(p.query.n),
                },
                "btree_ranges": crate::spatial::covering(&p.query).len(),
            })
        }
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
