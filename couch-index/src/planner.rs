//! Query planner — ports mango_idx_view (is_usable, range extraction) and
//! mango_selector:has_required_fields. Works directly on the selector JSON,
//! normalizing on the fly (implicit AND objects, bare-value equality).

use crate::keys::Bound;
use couch_mango::collate;
use serde_json::{Map, Value};
use std::cmp::Ordering;

/// A per-column range: (low cmp, low, high cmp, high) with cmp ∈ gt/gte/eq
/// on the low side and lt/lte/eq on the high side. `None` = empty range.
#[derive(Clone, Debug, PartialEq)]
pub struct Range {
    pub low_inclusive: bool,
    pub low: Bound,
    pub high_inclusive: bool,
    pub high: Bound,
    pub is_eq: bool,
}

impl Range {
    fn full() -> Range {
        Range {
            low_inclusive: false,
            low: Bound::Min,
            high_inclusive: false,
            high: Bound::Max,
            is_eq: false,
        }
    }

    fn eq(v: &Value) -> Range {
        Range {
            low_inclusive: true,
            low: Bound::Val(v.clone()),
            high_inclusive: true,
            high: Bound::Val(v.clone()),
            is_eq: true,
        }
    }

    pub fn is_full(&self) -> bool {
        self.low == Bound::Min && self.high == Bound::Max
    }
}

fn cmp_bound(a: &Bound, b: &Bound) -> Ordering {
    match (a, b) {
        (Bound::Min, Bound::Min) | (Bound::Max, Bound::Max) => Ordering::Equal,
        (Bound::Min, _) | (_, Bound::Max) => Ordering::Less,
        (_, Bound::Min) | (Bound::Max, _) => Ordering::Greater,
        (Bound::Val(x), Bound::Val(y)) => collate(x, y),
    }
}

/// Iterate a selector as a list of (key, value) clauses, treating an object
/// with several keys as an implicit $and.
fn clauses(sel: &Value) -> Vec<(&String, &Value)> {
    match sel {
        Value::Object(m) => m.iter().collect(),
        _ => Vec::new(),
    }
}

/// mango_selector:has_required_fields — every required field must be
/// constrained (seen through $and; $or requires coverage in all branches;
/// {$exists:false} does not count).
pub fn has_required_fields(sel: &Value, fields: &[String]) -> bool {
    let mut remainder: Vec<&String> = fields.iter().collect();
    remove_covered(sel, &mut remainder);
    remainder.is_empty()
}

fn remove_covered(sel: &Value, remainder: &mut Vec<&String>) {
    if remainder.is_empty() {
        return;
    }
    for (k, v) in clauses(sel) {
        match k.as_str() {
            "$and" => {
                if let Value::Array(args) = v {
                    for a in args {
                        remove_covered(a, remainder);
                    }
                }
            }
            "$or" => {
                if let Value::Array(args) = v {
                    if args.is_empty() {
                        continue;
                    }
                    // covered fields = intersection over branches
                    let mut covered_by_all: Option<Vec<&String>> = None;
                    for a in args {
                        let mut rem = remainder.clone();
                        remove_covered(a, &mut rem);
                        let covered: Vec<&String> = remainder
                            .iter()
                            .filter(|f| !rem.contains(*f))
                            .copied()
                            .collect();
                        covered_by_all = Some(match covered_by_all {
                            None => covered,
                            Some(prev) => {
                                prev.into_iter().filter(|f| covered.contains(f)).collect()
                            }
                        });
                    }
                    if let Some(covered) = covered_by_all {
                        remainder.retain(|f| !covered.contains(f));
                    }
                }
            }
            op if op.starts_with('$') => {}
            field => {
                // {$exists: false} explicitly does not require the field
                let exists_false = matches!(
                    v,
                    Value::Object(m) if m.len() == 1
                        && m.get("$exists") == Some(&Value::Bool(false))
                );
                if !exists_false {
                    remainder.retain(|f| f.as_str() != field);
                }
            }
        }
    }
}

/// mango_idx_view:range/6 — narrow the [low, high] range for one column by
/// walking the selector through $and (and implicit ands).
pub fn column_range(sel: &Value, column: &str) -> Option<Range> {
    let mut r = Range::full();
    if narrow(sel, column, &mut r) {
        Some(r)
    } else {
        None // provably empty
    }
}

fn narrow(sel: &Value, column: &str, r: &mut Range) -> bool {
    for (k, v) in clauses(sel) {
        match k.as_str() {
            "$and" => {
                if let Value::Array(args) = v {
                    for a in args {
                        if !narrow(a, column, r) {
                            return false;
                        }
                    }
                }
            }
            op if op.starts_with('$') => {}
            field if field == column => {
                if !narrow_cond(v, r) {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

fn narrow_cond(cond: &Value, r: &mut Range) -> bool {
    match cond {
        Value::Object(m) if m.keys().any(|k| k.starts_with('$')) => {
            for (op, arg) in m {
                if !narrow_op(op, arg, r) {
                    return false;
                }
            }
            true
        }
        // bare value (or operator-free object) is equality
        v => narrow_op("$eq", v, r),
    }
}

fn narrow_op(op: &str, arg: &Value, r: &mut Range) -> bool {
    let a = Bound::Val(arg.clone());
    match op {
        "$eq" => {
            // must lie within the current range
            let lo = cmp_bound(&a, &r.low);
            let hi = cmp_bound(&a, &r.high);
            if lo == Ordering::Less
                || (lo == Ordering::Equal && !r.low_inclusive && !r.is_eq)
                || hi == Ordering::Greater
                || (hi == Ordering::Equal && !r.high_inclusive && !r.is_eq)
            {
                return false;
            }
            *r = Range::eq(arg);
            true
        }
        "$lt" | "$lte" => {
            match cmp_bound(&a, &r.high) {
                Ordering::Less => {
                    r.high = a;
                    r.high_inclusive = op == "$lte";
                    r.is_eq = false;
                }
                Ordering::Equal => {
                    if op == "$lt" {
                        r.high_inclusive = false;
                        r.is_eq = false;
                    }
                }
                Ordering::Greater => {}
            }
            // still non-empty?
            cmp_bound(&r.low, &r.high) != Ordering::Greater
        }
        "$gt" | "$gte" => {
            match cmp_bound(&a, &r.low) {
                Ordering::Greater => {
                    r.low = a;
                    r.low_inclusive = op == "$gte";
                    r.is_eq = false;
                }
                Ordering::Equal => {
                    if op == "$gt" {
                        r.low_inclusive = false;
                        r.is_eq = false;
                    }
                }
                Ordering::Less => {}
            }
            cmp_bound(&r.low, &r.high) != Ordering::Greater
        }
        "$beginsWith" => {
            if let Value::String(s) = arg {
                let hi = format!("{s}\u{ffff}");
                narrow_op("$gte", arg, r) && narrow_op("$lte", &Value::String(hi), r)
            } else {
                true
            }
        }
        // Anything else is applied as a post-filter, not a range.
        _ => true,
    }
}

/// mango_selector:is_constant_field — the column is pinned by an $eq.
pub fn is_constant_field(sel: &Value, column: &str) -> bool {
    column_range(sel, column).is_some_and(|r| r.is_eq)
}

/// mango_idx_view:can_use_sort.
pub fn can_use_sort(columns: &[String], sort_fields: &[String], sel: &Value) -> bool {
    if sort_fields.is_empty() {
        return true;
    }
    let mut cols = columns;
    loop {
        match cols.first() {
            None => return false,
            Some(c) if c == &sort_fields[0] => {
                // sort fields must be a prefix of the remaining columns
                return sort_fields.len() <= cols.len()
                    && cols[..sort_fields.len()] == *sort_fields;
            }
            Some(c) => {
                if !is_constant_field(sel, c) {
                    return false;
                }
                cols = &cols[1..];
            }
        }
    }
}

/// mango_idx_view:is_usable + start/end key derivation for one index.
pub struct IndexPlan {
    pub ranges: Vec<Range>,
    /// leading columns constrained by equality (ranking)
    pub eq_prefix: usize,
    /// leading columns with any constraint (ranking)
    pub constrained_prefix: usize,
}

pub fn plan_index(
    columns: &[String],
    sel: &Value,
    sort_fields: &[String],
) -> Result<Option<IndexPlan>, String> {
    // every column (minus sort fields and _id/_rev) must be required
    let required: Vec<String> = columns
        .iter()
        .filter(|c| !sort_fields.contains(c))
        .filter(|c| c.as_str() != "_id" && c.as_str() != "_rev")
        .cloned()
        .collect();
    if !has_required_fields(sel, &required) {
        return Ok(None);
    }
    if !can_use_sort(columns, sort_fields, sel) {
        return Ok(None);
    }
    let mut ranges = Vec::with_capacity(columns.len());
    for col in columns {
        match column_range(sel, col) {
            Some(r) => ranges.push(r),
            None => return Err(format!("selector range for {col} is empty")),
        }
    }
    let eq_prefix = ranges.iter().take_while(|r| r.is_eq).count();
    let constrained_prefix = ranges.iter().take_while(|r| !r.is_full()).count();
    Ok(Some(IndexPlan {
        ranges,
        eq_prefix,
        constrained_prefix,
    }))
}

/// mango_idx_view:start_key / end_key — composite scan bounds.
/// start: take keys while columns are constrained from below; stop after the
/// first non-eq column. end: same from above, with Max padding.
pub fn scan_bounds(ranges: &[Range]) -> (Vec<Bound>, Vec<Bound>, bool) {
    let mut start = Vec::new();
    for r in ranges {
        if r.low == Bound::Min {
            break;
        }
        start.push(r.low.clone());
        if !r.is_eq {
            break;
        }
    }
    let mut end = Vec::new();
    let mut end_inclusive = true;
    for r in ranges {
        if r.high == Bound::Max {
            break;
        }
        end.push(r.high.clone());
        if !r.is_eq {
            end_inclusive = r.high_inclusive;
            break;
        }
    }
    // The caller appends a Max sentinel when the bound is inclusive; an
    // exclusive bound stays bare so any longer key with that prefix
    // compares greater and stops the scan.
    (start, end, end_inclusive)
}

/// Normalize a sort spec (["field" | {"field": "asc"|"desc"}]) into
/// (fields, descending). Mixed directions are rejected like CouchDB does.
pub fn parse_sort(sort: &Value) -> Result<(Vec<String>, bool), String> {
    let mut fields = Vec::new();
    let mut dirs: Vec<bool> = Vec::new();
    if let Value::Array(items) = sort {
        for item in items {
            match item {
                Value::String(f) => {
                    fields.push(f.clone());
                    dirs.push(false);
                }
                Value::Object(m) if m.len() == 1 => {
                    let (f, d) = m.iter().next().unwrap();
                    fields.push(f.clone());
                    dirs.push(d == "desc");
                }
                other => return Err(format!("invalid sort entry: {other}")),
            }
        }
    } else if !sort.is_null() {
        return Err("sort must be an array".into());
    }
    let desc = dirs.iter().any(|d| *d);
    if desc && !dirs.iter().all(|d| *d) {
        return Err("unsupported mixed sort directions".into());
    }
    Ok((fields, desc))
}

/// Build the selector's implicit view of an object query, wrapping a Map.
pub fn selector_of(query: &Map<String, Value>) -> Value {
    query.get("selector").cloned().unwrap_or(Value::Object(Map::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cols(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn required_fields() {
        let sel = json!({"db.DocType": "task", "db.CreatedAtMs": {"$gt": 5}});
        assert!(has_required_fields(&sel, &cols(&["db.DocType"])));
        assert!(has_required_fields(&sel, &cols(&["db.DocType", "db.CreatedAtMs"])));
        assert!(!has_required_fields(&sel, &cols(&["missing"])));
        // $or: all branches must cover
        let sel = json!({"$or": [{"a": 1, "b": 2}, {"a": 3}]});
        assert!(has_required_fields(&sel, &cols(&["a"])));
        assert!(!has_required_fields(&sel, &cols(&["b"])));
        // $exists:false doesn't count
        let sel = json!({"a": {"$exists": false}});
        assert!(!has_required_fields(&sel, &cols(&["a"])));
    }

    #[test]
    fn ranges() {
        let sel = json!({"a": "x", "b": {"$gte": 1, "$lt": 10}});
        let ra = column_range(&sel, "a").unwrap();
        assert!(ra.is_eq);
        let rb = column_range(&sel, "b").unwrap();
        assert_eq!(rb.low, Bound::Val(json!(1)));
        assert!(rb.low_inclusive);
        assert_eq!(rb.high, Bound::Val(json!(10)));
        assert!(!rb.high_inclusive);
        // contradiction → empty
        assert!(column_range(&json!({"a": {"$gt": 5, "$lt": 3}}), "a").is_none());
        // $and narrowing
        let sel = json!({"$and": [{"a": {"$gte": 2}}, {"a": {"$lte": 4}}]});
        let r = column_range(&sel, "a").unwrap();
        assert_eq!(r.low, Bound::Val(json!(2)));
        assert_eq!(r.high, Bound::Val(json!(4)));
    }

    #[test]
    fn usability_and_bounds() {
        let columns = cols(&["db.DocType", "db.CreatedAtMs"]);
        let sel = json!({"db.DocType": "task", "db.CreatedAtMs": {"$gt": 100}});
        let plan = plan_index(&columns, &sel, &[]).unwrap().unwrap();
        assert_eq!(plan.eq_prefix, 1);
        assert_eq!(plan.constrained_prefix, 2);
        let (start, end, incl) = scan_bounds(&plan.ranges);
        assert_eq!(start, vec![Bound::Val(json!("task")), Bound::Val(json!(100))]);
        assert_eq!(end, vec![Bound::Val(json!("task"))]);
        assert!(incl);

        // selector missing a column → unusable
        let sel = json!({"db.DocType": "task"});
        assert!(plan_index(&cols(&["db.DocType", "other"]), &sel, &[]).unwrap().is_none());

        // sort on second column usable when first is constant
        let sel = json!({"db.DocType": "task", "db.CreatedAtMs": {"$gt": 0}});
        assert!(plan_index(&columns, &sel, &cols(&["db.CreatedAtMs"])).unwrap().is_some());
        // ...but not when first is a range
        let sel2 = json!({"db.DocType": {"$gt": "a"}, "db.CreatedAtMs": {"$gt": 0}});
        assert!(plan_index(&columns, &sel2, &cols(&["db.CreatedAtMs"])).unwrap().is_none());
    }

    #[test]
    fn sort_parsing() {
        assert_eq!(parse_sort(&json!(["a", "b"])).unwrap(), (cols(&["a", "b"]), false));
        assert_eq!(
            parse_sort(&json!([{"a": "desc"}])).unwrap(),
            (cols(&["a"]), true)
        );
        assert!(parse_sort(&json!([{"a": "asc"}, {"b": "desc"}])).is_err());
    }
}
