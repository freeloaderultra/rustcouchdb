//! Native validate_doc_update rules — no couchjs. Each validator is a plain
//! Rust function with the (newDoc, oldDoc) contract; rejections surface as
//! 403 {"error":"forbidden"} exactly like the JS ones.

use serde_json::Value;

/// Port of nxguide's `_design/nxguide` validate_doc_update (soft-delete
/// metadata preservation). Mirrors the JS truthiness checks: a field counts
/// as present only when it's truthy.
pub fn nxguide_soft_delete(new: &Value, old: Option<&Value>) -> Result<(), String> {
    let old_db = |field: &str| -> bool { truthy(old.and_then(|o| o.get("db")).and_then(|d| d.get(field))) };

    let new_db = new.get("db");
    if !matches!(new_db, Some(Value::Object(_))) {
        if old_db("CreatedByUid") {
            return Err("soft delete requires db object (with ownership metadata)".into());
        }
        // System docs without ownership can be written without db metadata.
        return Ok(());
    }
    let new_db = new_db.unwrap();

    if old_db("OrganizationId") && !truthy(new_db.get("OrganizationId")) {
        return Err(
            "soft delete requires db.OrganizationId when the old document had one".into(),
        );
    }
    if old_db("CreatedByUid") && !truthy(new_db.get("CreatedByUid")) {
        return Err("soft delete requires db.CreatedByUid when the old document had one".into());
    }
    if old_db("DocType") && !truthy(new_db.get("DocType")) {
        return Err("soft delete must preserve db.DocType".into());
    }
    Ok(())
}

/// JavaScript truthiness for JSON values.
fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_)) | Some(Value::Object(_)) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn soft_delete_rules() {
        // New doc without db, no old doc → allowed.
        assert!(nxguide_soft_delete(&json!({"_id": "x", "_deleted": true}), None).is_ok());
        // Old doc had ownership → delete without db forbidden.
        let old = json!({"db": {"CreatedByUid": "u1", "DocType": "task", "OrganizationId": "o1"}});
        assert!(nxguide_soft_delete(&json!({"_deleted": true}), Some(&old)).is_err());
        // Delete carrying all metadata → allowed.
        assert!(nxguide_soft_delete(
            &json!({"_deleted": true, "db": {"CreatedByUid": "u1", "DocType": "task", "OrganizationId": "o1"}}),
            Some(&old)
        )
        .is_ok());
        // Dropping DocType → forbidden.
        assert!(nxguide_soft_delete(
            &json!({"_deleted": true, "db": {"CreatedByUid": "u1", "OrganizationId": "o1"}}),
            Some(&old)
        )
        .is_err());
        // Empty string counts as missing (JS truthiness).
        assert!(nxguide_soft_delete(
            &json!({"db": {"CreatedByUid": "", "DocType": "task", "OrganizationId": "o1"}}),
            Some(&old)
        )
        .is_err());
        // Old doc without ownership → new doc without db allowed.
        let old2 = json!({"db": {"DocType": "sys"}});
        assert!(nxguide_soft_delete(&json!({"v": 1}), Some(&old2)).is_ok());
    }
}
