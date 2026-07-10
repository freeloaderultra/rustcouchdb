//! End-to-end: build a database with couch-store, index it, and check that
//! indexed queries return exactly what a brute-force full scan returns.

use serde_json::{json, Value};
use std::ops::ControlFlow;
use std::process::Command;

fn bin(name: &str) -> String {
    // target dir shared by the workspace
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps/
    p.pop(); // debug/
    p.push(name);
    p.to_string_lossy().into_owned()
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{cmd:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn indexed_queries_match_full_scan() {
    let dir = std::env::temp_dir().join(format!("couch-index-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("e2e.couch");
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_dir_all(dir.join("e2e.couch.indexes"));
    let db = db_path.to_string_lossy().into_owned();

    // dataset: nxguide-shaped docs plus edge cases
    let mut w = couch_store::writer::DbWriter::create(&db).unwrap();
    let mut docs = Vec::new();
    for i in 0..3000usize {
        let doc_type = ["task", "field", "vehicle"][i % 3];
        docs.push(
            couch_store::writer::DocUpdate::from_json(json!({
                "_id": format!("doc{i:05}"),
                "db": {
                    "DocType": doc_type,
                    "CreatedAtMs": 1_700_000_000_000i64 + (i as i64 % 97) * 1000,
                    "CreatedByUid": format!("user-{}", i % 7),
                    "OrganizationId": if i % 5 == 0 { Value::Null } else { json!(format!("org-{}", i % 4)) },
                },
                "email": format!("u{}@example.com", i % 11),
                "n": i,
            }))
            .unwrap(),
        );
    }
    // docs missing indexed fields entirely
    docs.push(couch_store::writer::DocUpdate::from_json(json!({"_id": "no-db-field", "x": 1})).unwrap());
    w.update_docs(docs).unwrap();
    w.commit().unwrap();

    let ci = bin("couch-index");
    run(Command::new(&ci).args([
        "create", &db, "--fields", "db.DocType,db.CreatedAtMs", "--name", "idx_doctype_created",
    ]));
    run(Command::new(&ci).args(["create", &db, "--fields", "db.CreatedByUid", "--name", "idx_owner"]));
    run(Command::new(&ci).args(["create", &db, "--fields", "email,db.DocType", "--name", "idx_email"]));

    let queries = vec![
        json!({"selector": {"db.DocType": "task"}, "limit": 100000}),
        json!({"selector": {"db.DocType": "task", "db.CreatedAtMs": {"$gt": 1_700_000_050_000i64}}, "limit": 100000}),
        json!({"selector": {"db.DocType": "field", "db.CreatedAtMs": {"$gte": 1_700_000_010_000i64, "$lte": 1_700_000_020_000i64}}, "limit": 100000}),
        json!({"selector": {"db.CreatedByUid": "user-3", "n": {"$mod": [2, 0]}}, "limit": 100000}),
        json!({"selector": {"email": "u5@example.com", "db.DocType": "vehicle"}, "limit": 100000}),
        json!({"selector": {"db.DocType": "task", "db.CreatedAtMs": {"$gt": 0}},
               "sort": [{"db.DocType": "desc"}, {"db.CreatedAtMs": "desc"}], "limit": 40}),
        json!({"selector": {"db.DocType": "task"}, "limit": 7, "skip": 3}),
        json!({"selector": {"db.DocType": "task"}, "fields": ["_id", "db.CreatedAtMs"], "limit": 5}),
    ];

    // brute-force oracle
    let database = couch_store::db::Db::open(&db).unwrap();
    for q in &queries {
        let sel = couch_mango::Selector::compile(&q["selector"]).unwrap();
        let mut expected: Vec<Value> = Vec::new();
        database
            .fold_docs(|fdi| {
                if fdi.deleted {
                    return Ok(ControlFlow::Continue(()));
                }
                let w = fdi.rev_tree.winner().unwrap();
                let doc = database.doc_json(&fdi, &w, &Default::default())?;
                if sel.matches(&doc) {
                    expected.push(doc);
                }
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();

        let out = run(Command::new(&ci).args(["find", &db, &q.to_string()]));
        let got: Vec<Value> = out.lines().map(|l| serde_json::from_str(l).unwrap()).collect();

        let sort_desc = q.get("sort").is_some();
        let limit = q["limit"].as_u64().unwrap_or(25) as usize;
        let skip = q["skip"].as_u64().unwrap_or(0) as usize;
        let fields = q.get("fields");

        if fields.is_some() {
            assert_eq!(got.len(), expected.len().saturating_sub(skip).min(limit), "count for {q}");
            for d in &got {
                assert!(d.get("db").is_some() && d.get("_id").is_some(), "projection for {q}");
                assert!(d.get("email").is_none(), "unexpected field for {q}");
            }
            continue;
        }
        if sort_desc {
            // expected: sort desc by (DocType, CreatedAtMs, _id) — index order reversed
            let mut exp = expected.clone();
            exp.sort_by(|a, b| {
                let ka = (
                    a["db"]["DocType"].as_str().unwrap().to_string(),
                    a["db"]["CreatedAtMs"].as_i64().unwrap(),
                    a["_id"].as_str().unwrap().to_string(),
                );
                let kb = (
                    b["db"]["DocType"].as_str().unwrap().to_string(),
                    b["db"]["CreatedAtMs"].as_i64().unwrap(),
                    b["_id"].as_str().unwrap().to_string(),
                );
                kb.cmp(&ka)
            });
            let exp: Vec<Value> = exp.into_iter().skip(skip).take(limit).collect();
            assert_eq!(got, exp, "desc order results for {q}");
            continue;
        }
        // unordered comparison by _id after skip/limit accounting
        let mut got_ids: Vec<String> = got.iter().map(|d| d["_id"].as_str().unwrap().into()).collect();
        let mut exp_ids: Vec<String> = expected.iter().map(|d| d["_id"].as_str().unwrap().into()).collect();
        if skip > 0 || expected.len() > limit {
            assert_eq!(got_ids.len(), exp_ids.len().saturating_sub(skip).min(limit), "count for {q}");
            // all returned ids must be valid matches
            for id in &got_ids {
                assert!(exp_ids.contains(id), "unexpected id {id} for {q}");
            }
        } else {
            got_ids.sort();
            exp_ids.sort();
            assert_eq!(got_ids, exp_ids, "result set for {q}");
        }
        // full doc equality for the unlimited case
        if skip == 0 && expected.len() <= limit {
            let mut g = got.clone();
            let mut e = expected.clone();
            g.sort_by_key(|d| d["_id"].as_str().unwrap().to_string());
            e.sort_by_key(|d| d["_id"].as_str().unwrap().to_string());
            assert_eq!(g, e, "full docs for {q}");
        }
    }

    // full-scan fallback: selector on an unindexed field
    let q = json!({"selector": {"n": {"$lt": 5}}, "limit": 100});
    let out = run(Command::new(&ci).args(["find", &db, &q.to_string(), "--stats"]));
    let got: Vec<Value> = out.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(got.len(), 5);

    // incremental update: change docs, index follows
    let mut w = couch_store::writer::DbWriter::open(&db).unwrap();
    w.update_docs(vec![couch_store::writer::DocUpdate::from_json(
        json!({"_id": "newtask", "db": {"DocType": "task", "CreatedAtMs": 1, "CreatedByUid": "user-0", "OrganizationId": "org-0"}, "email": "new@example.com", "n": -1}),
    )
    .unwrap()])
    .unwrap();
    w.commit().unwrap();
    let out = run(Command::new(&ci).args([
        "find",
        &db,
        r#"{"selector": {"db.DocType": "task", "db.CreatedAtMs": {"$lt": 100}}, "limit": 10}"#,
    ]));
    assert!(out.contains("newtask"), "incremental update missed new doc");
}
