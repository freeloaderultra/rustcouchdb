//! End-to-end tests: boot the server in-process and exercise the API the
//! way kivik, couch-repl and the CouchDB replicator do.

use couch_http::state::{App, ServerState};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

struct Srv {
    base: String,
    #[allow(dead_code)]
    app: App,
    _dir: tempfile::TempDir,
}

async fn start(admin: Option<&str>, soft_delete: bool) -> Srv {
    let dir = tempfile::tempdir().unwrap();
    let admin = admin.map(|s| {
        let (u, p) = s.split_once(':').unwrap();
        (u.to_string(), p.to_string())
    });
    let app: App = Arc::new(ServerState::new(dir.path().to_path_buf(), admin, soft_delete));
    app.open_all().unwrap();
    app.create_db("_replicator").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    *app.base_url.write().unwrap() = addr.to_string();
    tokio::spawn(couch_http::repl::run(app.clone()));
    tokio::spawn(couch_http::serve(listener, app.clone(), std::future::pending()));
    Srv {
        base: format!("http://{addr}"),
        app,
        _dir: dir,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn jget(c: &reqwest::Client, url: &str) -> (u16, Value) {
    let r = c.get(url).send().await.unwrap();
    let status = r.status().as_u16();
    let v = r.json().await.unwrap_or(Value::Null);
    (status, v)
}

async fn jput(c: &reqwest::Client, url: &str, body: &Value) -> (u16, Value) {
    let r = c.put(url).json(body).send().await.unwrap();
    let status = r.status().as_u16();
    let v = r.json().await.unwrap_or(Value::Null);
    (status, v)
}

async fn jpost(c: &reqwest::Client, url: &str, body: &Value) -> (u16, Value) {
    let r = c.post(url).json(body).send().await.unwrap();
    let status = r.status().as_u16();
    let v = r.json().await.unwrap_or(Value::Null);
    (status, v)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docs_crud_attachments_local() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;

    // Welcome + db lifecycle
    let (s, v) = jget(&c, b).await;
    assert_eq!((s, v["couchdb"].as_str().unwrap()), (200, "Welcome"));
    assert_eq!(jput(&c, &format!("{b}/testdb"), &json!({})).await.0, 201);
    assert_eq!(jput(&c, &format!("{b}/testdb"), &json!({})).await.0, 412);
    let (s, v) = jget(&c, &format!("{b}/testdb")).await;
    assert_eq!((s, v["doc_count"].as_u64().unwrap()), (200, 0));

    // PUT / GET / update / conflict / delete
    let (s, v) = jput(&c, &format!("{b}/testdb/doc1"), &json!({"a": 1, "täxt": "ünïcode"})).await;
    assert_eq!(s, 201);
    let rev1 = v["rev"].as_str().unwrap().to_string();
    assert!(rev1.starts_with("1-"));
    let (s, v) = jget(&c, &format!("{b}/testdb/doc1")).await;
    assert_eq!((s, v["a"].as_i64().unwrap()), (200, 1));
    assert_eq!(v["täxt"], json!("ünïcode"));
    // update without rev → conflict
    let (s, _) = jput(&c, &format!("{b}/testdb/doc1"), &json!({"a": 2})).await;
    assert_eq!(s, 409);
    let (s, v) = jput(&c, &format!("{b}/testdb/doc1"), &json!({"a": 2, "_rev": rev1})).await;
    assert_eq!(s, 201);
    let rev2 = v["rev"].as_str().unwrap().to_string();
    // GET old rev still readable, with revs
    let (s, v) = jget(&c, &format!("{b}/testdb/doc1?rev={rev1}&revs=true")).await;
    assert_eq!((s, v["a"].as_i64().unwrap()), (200, 1));
    assert_eq!(v["_revisions"]["start"], json!(1));
    // POST without id
    let (s, v) = jpost(&c, &format!("{b}/testdb"), &json!({"posted": true})).await;
    assert_eq!(s, 201);
    assert!(v["id"].as_str().unwrap().len() >= 16);
    // DELETE
    let r = c
        .delete(format!("{b}/testdb/doc1?rev={rev2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let (s, v) = jget(&c, &format!("{b}/testdb/doc1")).await;
    assert_eq!((s, v["reason"].as_str().unwrap()), (404, "deleted"));

    // Attachments: put, stub survives doc update, get, delete
    let (s, v) = jput(&c, &format!("{b}/testdb/att1"), &json!({"kind": "blob-holder"})).await;
    assert_eq!(s, 201);
    let rev1 = v["rev"].as_str().unwrap().to_string();
    let blob = vec![7u8; 20000];
    let r = c
        .put(format!("{b}/testdb/att1/blob.data?rev={rev1}"))
        .header("content-type", "application/x-protobuf")
        .body(blob.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
    let rev2: Value = r.json().await.unwrap();
    let rev2 = rev2["rev"].as_str().unwrap().to_string();
    // update the doc body carrying the attachment as stub
    let (s, doc) = jget(&c, &format!("{b}/testdb/att1")).await;
    assert_eq!(s, 200);
    assert_eq!(doc["_attachments"]["blob.data"]["stub"], json!(true));
    let mut doc2 = doc.clone();
    doc2["kind"] = json!("updated");
    let (s, v) = jput(&c, &format!("{b}/testdb/att1"), &doc2).await;
    assert_eq!(s, 201);
    let rev3 = v["rev"].as_str().unwrap().to_string();
    assert_ne!(rev2, rev3);
    // read the attachment back
    let r = c.get(format!("{b}/testdb/att1/blob.data")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(
        r.headers()["content-type"].to_str().unwrap(),
        "application/x-protobuf"
    );
    assert_eq!(r.bytes().await.unwrap().to_vec(), blob);
    // attachment revpos stayed 2
    let (_, doc) = jget(&c, &format!("{b}/testdb/att1?attachments=true")).await;
    assert_eq!(doc["_attachments"]["blob.data"]["revpos"], json!(2));
    // delete the attachment
    let r = c
        .delete(format!("{b}/testdb/att1/blob.data?rev={rev3}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200); // CouchDB answers 200 for deletions
    let (_, doc) = jget(&c, &format!("{b}/testdb/att1")).await;
    assert!(doc.get("_attachments").is_none());

    // _local docs
    let (s, v) = jput(&c, &format!("{b}/testdb/_local/ckpt"), &json!({"seq": "12"})).await;
    assert_eq!((s, v["rev"].as_str().unwrap()), (201, "0-1"));
    let (s, v) = jget(&c, &format!("{b}/testdb/_local/ckpt")).await;
    assert_eq!((s, v["seq"].as_str().unwrap()), (200, "12"));
    let r = c.delete(format!("{b}/testdb/_local/ckpt")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(jget(&c, &format!("{b}/testdb/_local/ckpt")).await.0, 404);

    // _security
    let sec = json!({"admins": {"names": ["bob"], "roles": []}, "members": {"names": [], "roles": []}});
    assert_eq!(jput(&c, &format!("{b}/testdb/_security"), &sec).await.0, 200);
    let (_, v) = jget(&c, &format!("{b}/testdb/_security")).await;
    assert_eq!(v["admins"]["names"], json!(["bob"]));

    // multipart/related PUT (what couch-repl's attachment lane sends)
    let doc = json!({
        "_id": "mp1", "v": 1,
        "_attachments": {"f.bin": {"follows": true, "content_type": "application/octet-stream", "length": 5}}
    });
    let body = format!(
        "--BOUND\r\ncontent-type: application/json\r\n\r\n{doc}\r\n--BOUND\r\n\r\nHELLO\r\n--BOUND--"
    );
    let r = c
        .put(format!("{b}/testdb/mp1?new_edits=true"))
        .header("content-type", "multipart/related; boundary=\"BOUND\"")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
    let r = c.get(format!("{b}/testdb/mp1/f.bin")).send().await.unwrap();
    assert_eq!(r.bytes().await.unwrap().to_vec(), b"HELLO");

    // db deletion
    let r = c.delete(format!("{b}/testdb")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(jget(&c, &format!("{b}/testdb")).await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_changes_alldocs_revsdiff() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    jput(&c, &format!("{b}/bt"), &json!({})).await;

    // _bulk_docs new_edits=true
    let docs: Vec<Value> = (0..20)
        .map(|i| json!({"_id": format!("d{i:03}"), "n": i, "type": if i % 2 == 0 {"even"} else {"odd"}}))
        .collect();
    let (s, v) = jpost(&c, &format!("{b}/bt/_bulk_docs"), &json!({"docs": docs})).await;
    assert_eq!(s, 201);
    assert_eq!(v.as_array().unwrap().len(), 20);
    assert!(v[0]["ok"].as_bool().unwrap());
    let rev_d000 = v[0]["rev"].as_str().unwrap().to_string();

    // one conflict in a batch
    let (s, v) = jpost(
        &c,
        &format!("{b}/bt/_bulk_docs"),
        &json!({"docs": [
            {"_id": "d000", "n": 100},
            {"_id": "new1", "n": 21},
        ]}),
    )
    .await;
    assert_eq!(s, 201);
    assert_eq!(v[0]["error"], json!("conflict"));
    assert!(v[1]["ok"].as_bool().unwrap());

    // _bulk_docs new_edits=false replicated write with history
    let (s, v) = jpost(
        &c,
        &format!("{b}/bt/_bulk_docs"),
        &json!({"new_edits": false, "docs": [
            {"_id": "repl1", "v": 1, "_revisions": {"start": 2, "ids": ["bbb", "aaa"]}},
            {"_id": "repl1", "v": 2, "_revisions": {"start": 2, "ids": ["ccc", "aaa"]}},
        ]}),
    )
    .await;
    assert_eq!(s, 201);
    assert_eq!(v.as_array().unwrap().len(), 0);
    let (_, doc) = jget(&c, &format!("{b}/bt/repl1?conflicts=true")).await;
    assert_eq!(doc["_rev"], json!("2-ccc"));
    assert_eq!(doc["_conflicts"], json!(["2-bbb"]));

    // _revs_diff
    let (s, v) = jpost(
        &c,
        &format!("{b}/bt/_revs_diff"),
        &json!({
            "repl1": ["2-bbb", "2-zzz", "3-yyy"],
            "d000": [rev_d000],
        }),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(v["repl1"]["missing"], json!(["2-zzz", "3-yyy"]));
    assert!(v.get("d000").is_none());
    assert!(v["repl1"]["possible_ancestors"].as_array().is_some());

    // _bulk_get with revisions
    let (s, v) = jpost(
        &c,
        &format!("{b}/bt/_bulk_get?revs=true&latest=true"),
        &json!({"docs": [{"id": "repl1", "rev": "2-bbb"}, {"id": "nope", "rev": "1-x"}]}),
    )
    .await;
    assert_eq!(s, 200);
    let r0 = &v["results"][0]["docs"][0]["ok"];
    assert_eq!(r0["_rev"], json!("2-bbb"));
    assert_eq!(r0["_revisions"]["ids"], json!(["bbb", "aaa"]));
    assert!(v["results"][1]["docs"][0]["error"]["error"].as_str().is_some());

    // _all_docs range + include_docs + keys
    let (s, v) = jget(
        &c,
        &format!("{b}/bt/_all_docs?startkey=\"d005\"&endkey=\"d008\"&include_docs=true"),
    )
    .await;
    assert_eq!(s, 200);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["doc"]["n"], json!(5));
    let (_, v) = jpost(
        &c,
        &format!("{b}/bt/_all_docs"),
        &json!({"keys": ["d001", "zzz"]}),
    )
    .await;
    assert_eq!(v["rows"][0]["id"], json!("d001"));
    assert_eq!(v["rows"][1]["error"], json!("not_found"));
    // descending
    let (_, v) = jget(&c, &format!("{b}/bt/_all_docs?descending=true&limit=2&startkey=\"d003\"")).await;
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows[0]["id"], json!("d003"));
    assert_eq!(rows[1]["id"], json!("d002"));

    // _changes normal + style + selector filter
    let (s, v) = jget(&c, &format!("{b}/bt/_changes?style=all_docs")).await;
    assert_eq!(s, 200);
    let n_all = v["results"].as_array().unwrap().len();
    assert_eq!(n_all, 22); // 20 + new1 + repl1 (d000 conflict was rejected)
    assert_eq!(v["pending"], json!(0));
    let repl1_row = v["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == json!("repl1"))
        .unwrap();
    assert_eq!(repl1_row["changes"].as_array().unwrap().len(), 2);
    // main_only
    let (_, v) = jget(&c, &format!("{b}/bt/_changes")).await; // default is main_only
    let repl1_row = v["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == json!("repl1"))
        .unwrap();
    assert_eq!(repl1_row["changes"], json!([{"rev": "2-ccc"}]));
    // selector filter
    let (_, v) = jpost(
        &c,
        &format!("{b}/bt/_changes?filter=_selector"),
        &json!({"selector": {"type": "odd"}}),
    )
    .await;
    assert_eq!(v["results"].as_array().unwrap().len(), 10);
    // since resumes
    let (_, v) = jget(&c, &format!("{b}/bt/_changes?since=20&limit=1000&style=all_docs")).await;
    assert!(v["results"].as_array().unwrap().len() < n_all);

    // longpoll returns when a write happens
    let c2 = c.clone();
    let b2 = b.clone();
    let waiter = tokio::spawn(async move {
        jget(&c2, &format!("{b2}/bt/_changes?feed=longpoll&since=now&timeout=15000")).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    jput(&c, &format!("{b}/bt/late1"), &json!({"late": true})).await;
    let (s, v) = waiter.await.unwrap();
    assert_eq!(s, 200);
    assert_eq!(v["results"][0]["id"], json!("late1"));

    // continuous drains and heartbeats
    let resp = c
        .get(format!(
            "{b}/bt/_changes?feed=continuous&since=0&limit=3&heartbeat=500"
        ))
        .send()
        .await
        .unwrap();
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 4); // 3 rows + last_seq line
    assert!(lines[3].contains("last_seq"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mango_endpoints() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    jput(&c, &format!("{b}/mg"), &json!({})).await;
    let docs: Vec<Value> = (0..500)
        .map(|i| {
            json!({
                "_id": format!("m{i:04}"),
                "db": {"DocType": if i % 5 == 0 {"task"} else {"note"}, "CreatedAtMs": i * 1000},
                "n": i,
            })
        })
        .collect();
    jpost(&c, &format!("{b}/mg/_bulk_docs"), &json!({"docs": docs})).await;

    // create index (kivik shape)
    let (s, v) = jpost(
        &c,
        &format!("{b}/mg/_index"),
        &json!({"index": {"fields": ["db.DocType", "db.CreatedAtMs"]}, "name": "type-created", "type": "json"}),
    )
    .await;
    assert_eq!((s, v["result"].as_str().unwrap()), (200, "created"));
    // idempotent
    let (_, v) = jpost(
        &c,
        &format!("{b}/mg/_index"),
        &json!({"index": {"fields": ["db.DocType", "db.CreatedAtMs"]}, "name": "type-created"}),
    )
    .await;
    assert_eq!(v["result"], json!("exists"));
    // list
    let (_, v) = jget(&c, &format!("{b}/mg/_index")).await;
    assert_eq!(v["total_rows"], json!(2)); // _all_docs + ours
    assert_eq!(v["indexes"][1]["name"], json!("type-created"));

    // find via index
    let (s, v) = jpost(
        &c,
        &format!("{b}/mg/_find"),
        &json!({
            "selector": {"db.DocType": "task", "db.CreatedAtMs": {"$gte": 100000}},
            "sort": [{"db.DocType": "desc"}, {"db.CreatedAtMs": "desc"}],
            "limit": 10,
            "fields": ["_id", "db.CreatedAtMs"],
        }),
    )
    .await;
    assert_eq!(s, 200);
    let rows = v["docs"].as_array().unwrap();
    assert_eq!(rows.len(), 10);
    assert!(v.get("warning").is_none());
    // desc order, first is the largest task CreatedAtMs (495000)
    assert_eq!(rows[0]["db"]["CreatedAtMs"], json!(495000));
    assert!(rows[0].get("n").is_none()); // projection

    // full-scan fallback warns
    let (_, v) = jpost(&c, &format!("{b}/mg/_find"), &json!({"selector": {"n": 42}})).await;
    assert!(v["warning"].as_str().is_some());
    assert_eq!(v["docs"][0]["_id"], json!("m0042"));

    // explain
    let (s, v) = jpost(
        &c,
        &format!("{b}/mg/_explain"),
        &json!({"selector": {"db.DocType": "note", "db.CreatedAtMs": {"$gt": 0}}}),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(v["index"]["name"], json!("type-created"));

    // incremental: new doc appears in indexed query
    jput(
        &c,
        &format!("{b}/mg/fresh"),
        &json!({"db": {"DocType": "task", "CreatedAtMs": 999999999i64}}),
    )
    .await;
    let (_, v) = jpost(
        &c,
        &format!("{b}/mg/_find"),
        &json!({"selector": {"db.DocType": "task", "db.CreatedAtMs": {"$gt": 500000000}}}),
    )
    .await;
    assert_eq!(v["docs"].as_array().unwrap().len(), 1);
    assert_eq!(v["docs"][0]["_id"], json!("fresh"));

    // delete index
    let r = c
        .delete(format!("{b}/mg/_index/_design%2Ftype-created/json/type-created"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let (_, v) = jget(&c, &format!("{b}/mg/_index")).await;
    assert_eq!(v["total_rows"], json!(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicator_scheduler_flow() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    jput(&c, &format!("{b}/src"), &json!({})).await;
    let docs: Vec<Value> = (0..300)
        .map(|i| json!({"_id": format!("r{i:04}"), "v": i, "group": if i % 3 == 0 {"a"} else {"b"}}))
        .collect();
    jpost(&c, &format!("{b}/src/_bulk_docs"), &json!({"docs": docs})).await;

    // one-shot local->local with create_target
    let (s, _) = jput(
        &c,
        &format!("{b}/_replicator/job1"),
        &json!({"source": "src", "target": "tgt", "create_target": true}),
    )
    .await;
    assert_eq!(s, 201);

    // poll the scheduler until completed
    let mut state = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (s, v) = jget(&c, &format!("{b}/_scheduler/docs/_replicator/job1")).await;
        if s == 200 {
            state = v["state"].as_str().unwrap_or("").to_string();
            if state == "completed" || state == "failed" {
                break;
            }
        }
    }
    assert_eq!(state, "completed");
    let (_, v) = jget(&c, &format!("{b}/tgt")).await;
    assert_eq!(v["doc_count"], json!(300));
    // the doc carries state + id
    let (_, v) = jget(&c, &format!("{b}/_replicator/job1")).await;
    assert_eq!(v["_replication_state"], json!("completed"));
    assert!(v["_replication_id"].as_str().is_some());
    // scheduler doc info has stats nxguide reads
    let (_, v) = jget(&c, &format!("{b}/_scheduler/docs/_replicator/job1")).await;
    assert_eq!(v["info"]["docs_written"], json!(300));

    // selector-filtered replication (nxguide's ownership pattern)
    let (s, _) = jput(
        &c,
        &format!("{b}/_replicator/job2"),
        &json!({
            "source": "src", "target": "tgt2", "create_target": true,
            "selector": {"group": "a"}, "winning_revs_only": true,
        }),
    )
    .await;
    assert_eq!(s, 201);
    let mut state = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (s, v) = jget(&c, &format!("{b}/_scheduler/docs/_replicator/job2")).await;
        if s == 200 {
            state = v["state"].as_str().unwrap_or("").to_string();
            if state == "completed" || state == "failed" {
                break;
            }
        }
    }
    assert_eq!(state, "completed");
    let (_, v) = jget(&c, &format!("{b}/tgt2")).await;
    assert_eq!(v["doc_count"], json!(100));

    // deleting the doc removes the job from the scheduler
    let (_, v) = jget(&c, &format!("{b}/_replicator/job2")).await;
    let rev = v["_rev"].as_str().unwrap();
    c.delete(format!("{b}/_replicator/job2?rev={rev}"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (s, _) = jget(&c, &format!("{b}/_scheduler/docs/_replicator/job2")).await;
    assert_eq!(s, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auth_and_soft_delete_validator() {
    let srv = start(Some("admin:secret"), true).await;
    let c = client();
    let b = &srv.base;

    // No credentials → 401 (except / and /_session)
    assert_eq!(jget(&c, b).await.0, 200);
    let r = c.get(format!("{b}/_all_dbs")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 401);
    // Basic auth works
    let r = c
        .get(format!("{b}/_all_dbs"))
        .basic_auth("admin", Some("secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    // Cookie session (kivik's flow)
    let cj = Arc::new(reqwest::cookie::Jar::default());
    let cc = reqwest::Client::builder()
        .cookie_provider(cj.clone())
        .build()
        .unwrap();
    let r = cc
        .post(format!("{b}/_session"))
        .json(&json!({"name": "admin", "password": "secret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert!(r.headers().get("set-cookie").is_some());
    let r = cc.get(format!("{b}/_session")).send().await.unwrap();
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["userCtx"]["name"], json!("admin"));
    assert_eq!(v["userCtx"]["roles"], json!(["_admin"]));
    // and the cookie authorizes real endpoints
    let r = cc.put(format!("{b}/vdb")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 201);
    // bad password → 401
    let r = cc
        .post(format!("{b}/_session"))
        .json(&json!({"name": "admin", "password": "wrong"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);

    // Soft-delete validator (native port of nxguide's JS rule). Enforcement is
    // per-db: it only applies once the _design/nxguide validator ddoc exists,
    // exactly like installing the JS validator on stock CouchDB.
    let put = |url: String, body: Value| {
        let cc = cc.clone();
        async move {
            let r = cc.put(url).json(&body).send().await.unwrap();
            let s = r.status().as_u16();
            let v: Value = r.json().await.unwrap_or(Value::Null);
            (s, v)
        }
    };
    let (s, v) = put(
        format!("{b}/vdb/task1"),
        json!({"db": {"DocType": "task", "CreatedByUid": "u1", "OrganizationId": "o1"}}),
    )
    .await;
    assert_eq!(s, 201);
    let rev = v["rev"].as_str().unwrap().to_string();
    // Without the validator ddoc installed a bare tombstone is fine: check via
    // a throwaway doc (stock CouchDB without the ddoc allows it too).
    let (s, v2) = put(format!("{b}/vdb/unvalidated"), json!({"db": {"CreatedByUid": "u9"}})).await;
    assert_eq!(s, 201);
    let rev2 = v2["rev"].as_str().unwrap();
    let r = cc
        .delete(format!("{b}/vdb/unvalidated?rev={rev2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    // Install the validator ddoc (JS body stored inert; id triggers the native rule).
    let (s, _) = put(
        format!("{b}/vdb/_design/nxguide"),
        json!({"language": "javascript", "validate_doc_update": "function (newDoc, oldDoc) { /* enforced natively */ }"}),
    )
    .await;
    assert_eq!(s, 201);
    // Delete without metadata → 403 (DELETE builds a bare tombstone)
    let r = cc
        .delete(format!("{b}/vdb/task1?rev={rev}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);
    // Soft delete carrying metadata → allowed
    let (s, _) = put(
        format!("{b}/vdb/task1?rev={rev}"),
        json!({"_deleted": true, "db": {"DocType": "task", "CreatedByUid": "u1", "OrganizationId": "o1"}}),
    )
    .await;
    assert_eq!(s, 200); // deletions answer 200
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn utils_admin_ui() {
    let srv = start(Some("admin:secret"), true).await;
    let c = client();
    let b = &srv.base;

    // The UI shell loads without credentials so the login page can render.
    for path in ["/_utils", "/_utils/", "/_utils/index.html"] {
        let r = c.get(format!("{b}{path}")).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 200, "{path}");
        let ct = r.headers().get("content-type").unwrap().to_str().unwrap().to_string();
        assert!(ct.starts_with("text/html"), "{path}: {ct}");
        let body = r.text().await.unwrap();
        assert!(body.contains("rustcouchdb"), "{path}");
        assert!(body.contains("/_session"), "{path}: login flow missing");
    }
    // Data endpoints stay guarded.
    let r = c.get(format!("{b}/_all_dbs")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_docs() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    assert_eq!(jput(&c, &format!("{b}/pdb"), &json!({})).await.0, 201);

    // Two docs + a Mango index built over them BEFORE the purge.
    let (_, v) = jput(&c, &format!("{b}/pdb/keep"), &json!({"t": "x"})).await;
    let _keep_rev = v["rev"].as_str().unwrap().to_string();
    let (_, v) = jput(&c, &format!("{b}/pdb/gone"), &json!({"t": "x"})).await;
    let gone_rev = v["rev"].as_str().unwrap().to_string();
    let (s, _) = jpost(
        &c,
        &format!("{b}/pdb/_index"),
        &json!({"index": {"fields": ["t"]}, "name": "by-t", "type": "json"}),
    )
    .await;
    assert_eq!(s, 200);
    let (_, r) = jpost(&c, &format!("{b}/pdb/_find"), &json!({"selector": {"t": "x"}})).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 2);

    // Purge the winner rev: doc fully gone, no tombstone.
    let (s, v) = jpost(&c, &format!("{b}/pdb/_purge"), &json!({"gone": [gone_rev]})).await;
    assert_eq!(s, 201);
    assert_eq!(v["purged"]["gone"].as_array().unwrap().len(), 1);
    let (s, v) = jget(&c, &format!("{b}/pdb/gone")).await;
    assert_eq!((s, v["reason"].as_str().unwrap()), (404, "missing")); // not "deleted"
    // "keep" plus the _design/by-t doc backing the Mango index (CouchDB
    // stores index definitions as design docs; so do we).
    let (_, v) = jget(&c, &format!("{b}/pdb/_all_docs")).await;
    assert_eq!(v["total_rows"], json!(2));
    let (_, v) = jget(&c, &format!("{b}/pdb/_changes")).await;
    let ids: Vec<&str> = v["results"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(!ids.contains(&"gone"), "purged doc must vanish from _changes: {ids:?}");
    // The index must not serve entries for the purged doc.
    let (_, r) = jpost(&c, &format!("{b}/pdb/_find"), &json!({"selector": {"t": "x"}})).await;
    let found: Vec<&str> = r["docs"].as_array().unwrap().iter().map(|d| d["_id"].as_str().unwrap()).collect();
    assert_eq!(found, vec!["keep"]);
    // doc_count reflects the purge ("keep" + the index's design doc).
    let (_, v) = jget(&c, &format!("{b}/pdb")).await;
    assert_eq!(v["doc_count"], json!(2));

    // Conflict branches: purge only the winning branch, loser takes over.
    let mk = |rev: &str, val: u32| json!({"_id": "cft", "_rev": rev, "v": val});
    let bulk = json!({"new_edits": false, "docs": [
        {"_id": "cft", "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "v": 1},
        {"_id": "cft", "_rev": "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "v": 2},
    ]});
    let _ = mk; // silence
    let (s, _) = jpost(&c, &format!("{b}/pdb/_bulk_docs"), &bulk).await;
    assert_eq!(s, 201);
    let (_, v) = jget(&c, &format!("{b}/pdb/cft")).await;
    assert_eq!(v["_rev"], json!("1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")); // winner
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_purge"),
        &json!({"cft": ["1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}),
    )
    .await;
    assert_eq!(s, 201);
    assert_eq!(v["purged"]["cft"].as_array().unwrap().len(), 1);
    let (s, v) = jget(&c, &format!("{b}/pdb/cft")).await;
    assert_eq!(s, 200);
    assert_eq!(v["_rev"], json!("1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")); // loser survives

    // Purging an unknown rev is a no-op with an empty purged list.
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_purge"),
        &json!({"cft": ["9-cccccccccccccccccccccccccccccccc"]}),
    )
    .await;
    assert_eq!(s, 201);
    assert_eq!(v["purged"]["cft"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mango_indexes_live_in_design_docs() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    assert_eq!(jput(&c, &format!("{b}/idb"), &json!({})).await.0, 201);
    for i in 0..8 {
        let t = if i % 2 == 0 { "even" } else { "odd" };
        jput(&c, &format!("{b}/idb/d{i}"), &json!({"t": t, "n": i})).await;
    }

    // POST _index stores the definition as a language:"query" design doc.
    let (s, v) = jpost(
        &c,
        &format!("{b}/idb/_index"),
        &json!({"index": {"fields": ["t"]}, "name": "by-t", "type": "json"}),
    )
    .await;
    assert_eq!((s, v["result"].as_str().unwrap()), (200, "created"));
    assert_eq!(v["id"], json!("_design/by-t"));
    let (s, v) = jget(&c, &format!("{b}/idb/_design/by-t")).await;
    assert_eq!((s, v["language"].as_str().unwrap()), (200, "query"));
    assert_eq!(
        v["views"]["by-t"]["options"]["def"]["fields"],
        json!(["t"])
    );
    // Same definition again → exists, no duplicate.
    let (s, v) = jpost(
        &c,
        &format!("{b}/idb/_index"),
        &json!({"index": {"fields": ["t"]}, "name": "by-t", "type": "json"}),
    )
    .await;
    assert_eq!((s, v["result"].as_str().unwrap()), (200, "exists"));

    // First _find materializes the .fidx and uses the index.
    let q = json!({"selector": {"t": "even"}, "execution_stats": true});
    let (_, r) = jpost(&c, &format!("{b}/idb/_find"), &q).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 4);
    assert!(r.get("warning").is_none(), "index should be used: {r}");
    assert_eq!(r["execution_stats"]["total_keys_examined"], json!(4));
    assert!(r["execution_stats"]["execution_time_ms"].as_f64().unwrap() > 0.0);

    // The production failure: the .couch file is migrated but the external
    // .indexes directory is lost. The definition must survive in the ddoc
    // and the next _find must rebuild the materialization, not full-scan
    // forever.
    let idxdir = srv._dir.path().join("idb.couch.indexes");
    assert!(idxdir.join("by-t.fidx").exists());
    std::fs::remove_dir_all(&idxdir).unwrap();
    let (_, v) = jget(&c, &format!("{b}/idb/_index")).await;
    let names: Vec<&str> = v["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"by-t"), "ddoc-defined index lost: {names:?}");
    let (_, r) = jpost(&c, &format!("{b}/idb/_find"), &q).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 4);
    assert!(r.get("warning").is_none(), "index should be rebuilt: {r}");
    assert_eq!(r["execution_stats"]["total_keys_examined"], json!(4));
    assert!(idxdir.join("by-t.fidx").exists(), "materialization rebuilt");

    // A CouchDB-written Mango ddoc (hashed id, replicated in) is honored too.
    let ddoc = json!({
        "language": "query",
        "views": {"idx-n": {
            "map": {"fields": {"n": "asc"}, "partial_filter_selector": {}},
            "reduce": "_count",
            "options": {"def": {"fields": ["n"]}},
        }},
    });
    let (s, _) = jput(&c, &format!("{b}/idb/_design/abc123hash"), &ddoc).await;
    assert_eq!(s, 201);
    let (_, r) = jpost(
        &c,
        &format!("{b}/idb/_find"),
        &json!({"selector": {"n": {"$gte": 6}}, "execution_stats": true}),
    )
    .await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 2);
    assert!(r.get("warning").is_none(), "replicated ddoc index unused: {r}");

    // DELETE _index tombstones the ddoc and removes the materialization.
    let r = c
        .delete(format!("{b}/idb/_index/_design/by-t/json/by-t"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(jget(&c, &format!("{b}/idb/_design/by-t")).await.0, 404);
    assert!(!idxdir.join("by-t.fidx").exists());
    let (_, r) = jpost(&c, &format!("{b}/idb/_find"), &q).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 4);
    assert!(r.get("warning").is_some(), "by-t should be gone: {r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_index_serves_bbox_intersection() {
    let s = start(None, false).await;
    let c = client();
    let b = &s.base;
    assert_eq!(jput(&c, &format!("{b}/geo"), &json!({})).await.0, 201);

    // nxguide-shaped field docs: boundingBox.topLeft = (maxLat, minLon),
    // boundingBox.bottomRight = (minLat, maxLon), on a 10x6 grid near 48N 9E.
    let mut fields: Vec<(String, f64, f64, f64, f64)> = Vec::new(); // (id, w, s, e, n)
    for i in 0..60 {
        let w = 9.0 + (i % 10) as f64 * 0.05;
        let so = 48.0 + (i / 10) as f64 * 0.05;
        let (e, n) = (w + 0.02, so + 0.02);
        let id = format!("field-{i:02}");
        let doc = json!({
            "db": {"DocType": "field"},
            "name": id,
            "boundingBox": {
                "topLeft": {"latitude": n, "longitude": w},
                "bottomRight": {"latitude": so, "longitude": e},
            },
        });
        assert_eq!(jput(&c, &format!("{b}/geo/{id}"), &doc).await.0, 201);
        fields.push((id, w, so, e, n));
    }
    // noise: another doctype, a field with no bbox, one with a junk bbox
    jput(&c, &format!("{b}/geo/vehicle-1"), &json!({"db": {"DocType": "vehicle"}})).await;
    jput(&c, &format!("{b}/geo/field-nobox"), &json!({"db": {"DocType": "field"}})).await;
    jput(
        &c,
        &format!("{b}/geo/field-junk"),
        &json!({"db": {"DocType": "field"}, "boundingBox": {
            "topLeft": {"latitude": 48.0, "longitude": "not-a-number"},
            "bottomRight": {"latitude": 47.9, "longitude": 9.1}}}),
    )
    .await;

    // the four stored-edge paths, in west/south/east/north order
    let (st, v) = jpost(
        &c,
        &format!("{b}/geo/_index"),
        &json!({"type": "spatial", "name": "fields-bbox", "index": {"fields": [
            "boundingBox.topLeft.longitude",
            "boundingBox.bottomRight.latitude",
            "boundingBox.bottomRight.longitude",
            "boundingBox.topLeft.latitude",
        ]}}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["result"], "created");
    // same definition again, kivik-style (type inside index) → exists
    let (_, v2) = jpost(
        &c,
        &format!("{b}/geo/_index"),
        &json!({"index": {"type": "spatial", "fields": [
            "boundingBox.topLeft.longitude",
            "boundingBox.bottomRight.latitude",
            "boundingBox.bottomRight.longitude",
            "boundingBox.topLeft.latitude",
        ]}}),
    )
    .await;
    assert_eq!(v2["result"], "exists", "{v2}");
    let (_, l) = jget(&c, &format!("{b}/geo/_index")).await;
    let listed = l["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["name"] == "fields-bbox")
        .unwrap_or_else(|| panic!("spatial index not listed: {l}"));
    assert_eq!(listed["type"], "spatial");

    // the exact selector nxguide's GetFields builds for a viewport
    let (qw, qs, qe, qn) = (9.12, 48.08, 9.31, 48.22);
    let viewport = json!({
        "db.DocType": "field",
        "boundingBox.topLeft.latitude": {"$gte": qs},
        "boundingBox.bottomRight.latitude": {"$lte": qn},
        "boundingBox.topLeft.longitude": {"$lte": qe},
        "boundingBox.bottomRight.longitude": {"$gte": qw},
    });
    let mut expected: Vec<&str> = fields
        .iter()
        .filter(|(_, w, so, e, n)| *w <= qe && *e >= qw && *so <= qn && *n >= qs)
        .map(|(id, ..)| id.as_str())
        .collect();
    expected.sort();
    assert!(expected.len() > 5, "test grid should intersect the viewport");

    // _explain picks the spatial index
    let (_, ex) = jpost(
        &c,
        &format!("{b}/geo/_explain"),
        &json!({"selector": viewport}),
    )
    .await;
    assert_eq!(ex["index"]["name"], "fields-bbox", "{ex}");
    assert_eq!(ex["index"]["type"], "spatial", "{ex}");

    let q = json!({"selector": viewport, "limit": 1000, "execution_stats": true});
    let (_, r) = jpost(&c, &format!("{b}/geo/_find"), &q).await;
    let mut got: Vec<&str> = r["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_str().unwrap())
        .collect();
    got.sort();
    assert_eq!(got, expected, "spatial _find diverges from brute force");
    assert!(r.get("warning").is_none(), "used an index: {r}");
    // the index pruned: far fewer docs touched than the 62 field docs
    let examined = r["execution_stats"]["total_docs_examined"].as_u64().unwrap();
    assert!(examined < 45, "no spatial pruning happened: {examined} docs examined");

    // a selector missing one bbox clause can't use the spatial index but
    // must stay correct (json-index/full-scan fallback)
    let partial = json!({
        "db.DocType": "field",
        "boundingBox.topLeft.latitude": {"$gte": qs},
        "boundingBox.bottomRight.latitude": {"$lte": qn},
        "boundingBox.topLeft.longitude": {"$lte": qe},
    });
    let mut expected_partial: Vec<&str> = fields
        .iter()
        .filter(|(_, w, so, _, n)| *w <= qe && *so <= qn && *n >= qs)
        .map(|(id, ..)| id.as_str())
        .collect();
    expected_partial.sort();
    let (_, r) = jpost(
        &c,
        &format!("{b}/geo/_find"),
        &json!({"selector": partial, "limit": 1000}),
    )
    .await;
    let mut got: Vec<&str> = r["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_str().unwrap())
        .collect();
    got.sort();
    assert_eq!(got, expected_partial);

    // delete the index; the viewport query still answers via full scan
    let del = c
        .delete(format!("{b}/geo/_index/_design/fields-bbox/json/fields-bbox"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status().as_u16(), 200);
    let (_, r) = jpost(&c, &format!("{b}/geo/_find"), &q).await;
    let mut got: Vec<&str> = r["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_str().unwrap())
        .collect();
    got.sort();
    assert_eq!(got, expected);
    assert!(r.get("warning").is_some(), "index should be gone: {r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn couchdb_layout_served_in_place() {
    // Author a database with the flat-layout server, then present the same
    // file the way a CouchDB 3.x q=1 volume looks on disk and mount THAT.
    let author = start(None, false).await;
    let c = client();
    let b = &author.base;
    assert_eq!(jput(&c, &format!("{b}/src"), &json!({})).await.0, 201);
    for i in 0..5 {
        jput(&c, &format!("{b}/src/d{i}"), &json!({"n": i})).await;
    }
    let (s, _) = jpost(
        &c,
        &format!("{b}/src/_index"),
        &json!({"index": {"fields": ["n"]}, "name": "by-n"}),
    )
    .await;
    assert_eq!(s, 200);

    let dir = tempfile::tempdir().unwrap();
    let range = dir.path().join("shards").join("00000000-ffffffff");
    std::fs::create_dir_all(&range).unwrap();
    std::fs::copy(
        author._dir.path().join("src.couch"),
        range.join("mydb.1631712809.couch"),
    )
    .unwrap();
    // An older leftover from a recreated db: the newer timestamp must win.
    std::fs::copy(
        author._dir.path().join("src.couch"),
        range.join("mydb.1500000000.couch"),
    )
    .unwrap();
    // Cluster bookkeeping in the root is ignored.
    std::fs::write(dir.path().join("_dbs.couch"), b"not a real couch file").unwrap();

    let app: App = Arc::new(ServerState::new(dir.path().to_path_buf(), None, false));
    app.open_all().unwrap();
    app.create_db("_replicator").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    *app.base_url.write().unwrap() = addr.to_string();
    tokio::spawn(couch_http::serve(listener, app.clone(), std::future::pending()));
    let b = format!("http://{addr}");

    // The shard file serves as `mydb`, docs and Mango index included.
    let (s, v) = jget(&c, &format!("{b}/_all_dbs")).await;
    assert_eq!(s, 200);
    assert!(v.as_array().unwrap().contains(&json!("mydb")), "{v}");
    assert!(!v.as_array().unwrap().iter().any(|n| n == "_dbs"), "{v}");
    let (s, v) = jget(&c, &format!("{b}/mydb/d3")).await;
    assert_eq!((s, v["n"].as_i64().unwrap()), (200, 3));
    let (_, r) = jpost(
        &c,
        &format!("{b}/mydb/_find"),
        &json!({"selector": {"n": 2}}),
    )
    .await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 1);
    assert!(r.get("warning").is_none(), "index must come along: {r}");

    // Writes land in the same shard file (rollback to CouchDB keeps them).
    assert_eq!(jput(&c, &format!("{b}/mydb/new"), &json!({"n": 99})).await.0, 201);

    // New databases are created inside the range dir, CouchDB-style name.
    assert_eq!(jput(&c, &format!("{b}/fresh"), &json!({})).await.0, 201);
    let files: Vec<String> = std::fs::read_dir(&range)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        files.iter().any(|f| f.starts_with("fresh.") && f.ends_with(".couch")),
        "{files:?}"
    );

    // Deleting removes every timestamp variant, not just the newest.
    let r = c.delete(format!("{b}/mydb")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let files: Vec<String> = std::fs::read_dir(&range)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !files.iter().any(|f| f.starts_with("mydb.")),
        "leftover shard files resurrect the db on restart: {files:?}"
    );

    // q>1 layouts are refused loudly instead of serving partial data.
    let dir2 = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir2.path().join("shards").join("00000000-7fffffff")).unwrap();
    std::fs::create_dir_all(dir2.path().join("shards").join("80000000-ffffffff")).unwrap();
    let app2: App = Arc::new(ServerState::new(dir2.path().to_path_buf(), None, false));
    let err = app2.open_all().unwrap_err();
    assert_eq!(err.error, "unsupported_shard_layout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prometheus_metrics() {
    let srv = start(Some("admin:secret"), false).await;
    let c = client();
    let b = &srv.base;
    let with_auth = |url: String| c.get(url).basic_auth("admin", Some("secret"));

    // Admin-gated like upstream (chttpd requires _admin/_metrics for _prometheus).
    let r = c.get(format!("{b}/_node/_local/_prometheus")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 401);

    // Generate traffic: create a db, write, read, bulk write.
    let mkurl = format!("{b}/mdb");
    assert_eq!(c.put(&mkurl).basic_auth("admin", Some("secret")).send().await.unwrap().status().as_u16(), 201);
    let r = c
        .put(format!("{b}/mdb/doc1"))
        .basic_auth("admin", Some("secret"))
        .json(&json!({"v": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
    assert_eq!(with_auth(format!("{b}/mdb/doc1")).send().await.unwrap().status().as_u16(), 200);
    let r = c
        .post(format!("{b}/mdb/_bulk_docs"))
        .basic_auth("admin", Some("secret"))
        .json(&json!({"docs": [{"_id": "doc2"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    let r = with_auth(format!("{b}/_node/_local/_prometheus")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let ct = r.headers()["content-type"].to_str().unwrap().to_string();
    assert!(ct.starts_with("text/plain"), "content-type: {ct}");
    let body = r.text().await.unwrap();

    // Every metric the nx Grafana dashboards query must be present.
    for name in [
        "couchdb_httpd_requests_total",
        "couchdb_httpd_bulk_requests_total",
        "couchdb_httpd_aborted_requests_total",
        "couchdb_httpd_request_methods{method=\"PUT\"}",
        "couchdb_request_time_seconds{quantile=\"0.5\"}",
        "couchdb_request_time_seconds_sum",
        "couchdb_request_time_seconds_count",
        "couchdb_database_reads_total",
        "couchdb_database_writes_total",
        "couchdb_database_purges_total",
        "couchdb_erlang_memory_bytes{memory_type=\"total\"}",
        "couchdb_couch_replicator_jobs_running",
        "couchdb_couch_replicator_jobs_pending",
        "couchdb_couch_replicator_jobs_crashed",
        "couchdb_couch_replicator_jobs_total",
        "couchdb_couch_replicator_requests_total",
        "couchdb_couch_replicator_responses_failure_total",
        "couchdb_couch_replicator_checkpoints_total",
        "couchdb_couch_replicator_checkpoints_failure_total",
        "couchdb_couch_replicator_changes_read_failures_total",
    ] {
        assert!(body.contains(name), "missing metric {name} in:\n{body}");
    }

    // Counters are process-global (tests share the binary), so only assert
    // they moved: the traffic above guarantees at least these floors.
    let val = |metric: &str| -> f64 {
        body.lines()
            .find(|l| l.starts_with(metric) && l[metric.len()..].starts_with(' '))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(-1.0)
    };
    assert!(val("couchdb_httpd_requests_total") >= 5.0);
    assert!(val("couchdb_httpd_bulk_requests_total") >= 1.0);
    assert!(val("couchdb_database_writes_total") >= 2.0);
    assert!(val("couchdb_database_reads_total") >= 1.0);
    assert!(val("couchdb_request_time_seconds_count") >= 5.0);
    assert!(val("couchdb_request_time_seconds_sum") > 0.0);

    // TYPE lines so Prometheus parses counters/gauges/summaries correctly.
    for typ in [
        "# TYPE couchdb_httpd_requests_total counter",
        "# TYPE couchdb_request_time_seconds summary",
        "# TYPE couchdb_couch_replicator_jobs_running gauge",
        "# TYPE couchdb_erlang_memory_bytes gauge",
    ] {
        assert!(body.contains(typ), "missing: {typ}");
    }

    // Any node name resolves to this node, like upstream's _local.
    let r = with_auth(format!("{b}/_node/nonode@nohost/_prometheus")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
}

/// Gzip transport negotiation: request bodies inflate, responses compress
/// only for clients that ask, unknown encodings 415, continuous stays
/// identity, tiny responses stay identity. The dev-dep reqwest has no gzip
/// feature, so nothing here is transparently (de)compressed by the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gzip_transport_negotiation() {
    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;
    use std::io::{Read, Write};

    let srv = start(None, false).await;
    // Feature unification pulls reqwest's gzip feature into this test build
    // (couch-repl enables it), and reqwest then transparently decompresses
    // and strips Content-Encoding. This test inspects the raw wire, so turn
    // that off explicitly.
    let c = reqwest::Client::builder().no_gzip().build().unwrap();
    let b = &srv.base;

    // The welcome advertises the feature flag couch-repl probes for.
    let (_, welcome) = jget(&c, b).await;
    assert!(welcome["features"].as_array().unwrap().iter().any(|f| f == "gzip"));

    jput(&c, &format!("{b}/gz"), &json!({})).await;

    // Gzipped _bulk_docs request body (what a probed couch-repl sends).
    let docs: Vec<Value> = (0..50)
        .map(|i| json!({"_id": format!("d{i:03}"), "payload": "x".repeat(200), "n": i}))
        .collect();
    let plain = serde_json::to_vec(&json!({"docs": docs})).unwrap();
    let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(&plain).unwrap();
    let compressed = enc.finish().unwrap();
    assert!(compressed.len() < plain.len());
    let r = c
        .post(format!("{b}/gz/_bulk_docs"))
        .header("content-type", "application/json")
        .header("content-encoding", "gzip")
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
    let (_, info) = jget(&c, &format!("{b}/gz")).await;
    assert_eq!(info["doc_count"], json!(50));

    // Response compression only when asked for.
    let r = c.get(format!("{b}/gz/_all_docs?include_docs=true")).send().await.unwrap();
    assert!(r.headers().get("content-encoding").is_none());
    let plain_body = r.bytes().await.unwrap();
    let r = c
        .get(format!("{b}/gz/_all_docs?include_docs=true"))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("content-encoding").unwrap(), "gzip");
    let gz_body = r.bytes().await.unwrap();
    assert!(gz_body.len() < plain_body.len() / 2, "{} !< {}/2", gz_body.len(), plain_body.len());
    let mut inflated = Vec::new();
    GzDecoder::new(&gz_body[..]).read_to_end(&mut inflated).unwrap();
    assert_eq!(&inflated[..], &plain_body[..]);
    let v: Value = serde_json::from_slice(&inflated).unwrap();
    assert_eq!(v["rows"].as_array().unwrap().len(), 50);

    // Tiny responses skip compression even when the client accepts gzip.
    let r = c
        .get(format!("{b}/_up"))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert!(r.headers().get("content-encoding").is_none());

    // feed=continuous is never compressed: a gzip encoder would buffer the
    // heartbeat newlines the replicator's liveness check depends on.
    let r = c
        .get(format!("{b}/gz/_changes?feed=continuous&timeout=200"))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert!(r.headers().get("content-encoding").is_none());
    let _ = r.bytes().await;
    // feed=normal (paged) does compress.
    let r = c
        .get(format!("{b}/gz/_changes"))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("content-encoding").unwrap(), "gzip");

    // Protobuf attachments are treated as compressible: identity without
    // Accept-Encoding, gzip (inflating to identical bytes) with it.
    let pb: Vec<u8> = (0..2000u32)
        .flat_map(|i| [0x09, (i % 7) as u8, 0, 0, 0, 0, 0, 0, 0x40])
        .collect();
    let r = c
        .put(format!("{b}/gz/blobdoc/blob.data"))
        .header("content-type", "application/protobuf")
        .body(pb.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
    let r = c.get(format!("{b}/gz/blobdoc/blob.data")).send().await.unwrap();
    assert!(r.headers().get("content-encoding").is_none());
    assert_eq!(r.bytes().await.unwrap().as_ref(), &pb[..]);
    let r = c
        .get(format!("{b}/gz/blobdoc/blob.data"))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("content-encoding").unwrap(), "gzip");
    assert_eq!(r.headers().get("content-type").unwrap(), "application/protobuf");
    let gz_att = r.bytes().await.unwrap();
    assert!(gz_att.len() < pb.len());
    let mut inflated = Vec::new();
    GzDecoder::new(&gz_att[..]).read_to_end(&mut inflated).unwrap();
    assert_eq!(&inflated[..], &pb[..]);

    // Unsupported request encodings are rejected like stock chttpd.
    let r = c
        .post(format!("{b}/gz/_bulk_docs"))
        .header("content-type", "application/json")
        .header("content-encoding", "deflate")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 415);
}

/// Replication between two servers with the gzip probe active on both ends:
/// bulk docs and a compressible attachment arrive intact through the
/// compressed transport (the attachment lane's streamed multipart PUT rides
/// Content-Encoding: gzip because text/csv is a compressible type).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_through_gzip_transport() {
    let src = start(None, false).await;
    let tgt = start(None, false).await;
    let c = client();

    jput(&c, &format!("{}/data", src.base), &json!({})).await;
    let docs: Vec<Value> = (0..120)
        .map(|i| json!({"_id": format!("doc{i:04}"), "track": vec![i; 32], "note": "gps run"}))
        .collect();
    jpost(&c, &format!("{}/data/_bulk_docs", src.base), &json!({"docs": docs})).await;

    // A doc with a large compressible attachment (> inline threshold 64 KiB).
    let csv = "lat,lon,speed\n52.1,9.7,3.4\n".repeat(4000);
    let (s, v) = jput(
        &c,
        &format!("{}/data/with-att", src.base),
        &json!({"kind": "csv-carrier"}),
    )
    .await;
    assert_eq!(s, 201);
    let rev = v["rev"].as_str().unwrap();
    let r = c
        .put(format!("{}/data/with-att/run.csv?rev={rev}", src.base))
        .header("content-type", "text/csv")
        .body(csv.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    // Replicate src -> tgt over real HTTP (distinct servers, both new).
    let (s, _) = jput(
        &c,
        &format!("{}/_replicator/gzjob", src.base),
        &json!({
            "source": format!("{}/data", src.base),
            "target": format!("{}/data", tgt.base),
            "create_target": true,
        }),
    )
    .await;
    assert_eq!(s, 201);
    let mut state = String::new();
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (s, v) = jget(&c, &format!("{}/_scheduler/docs/_replicator/gzjob", src.base)).await;
        if s == 200 {
            state = v["state"].as_str().unwrap_or("").to_string();
            if state == "completed" || state == "failed" {
                break;
            }
        }
    }
    assert_eq!(state, "completed");

    let (_, info) = jget(&c, &format!("{}/data", tgt.base)).await;
    assert_eq!(info["doc_count"], json!(121));
    let (_, doc) = jget(&c, &format!("{}/data/doc0077", tgt.base)).await;
    assert_eq!(doc["track"].as_array().unwrap().len(), 32);
    let r = c
        .get(format!("{}/data/with-att/run.csv", tgt.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.text().await.unwrap(), csv);
}

/// Large attachments stream disk-to-disk: an 8 MiB body PUT as a stream,
/// stored as bounded chunks, served back with content-length, correct
/// through compaction and through replication to a second server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_attachment_roundtrip() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;
    jput(&c, &format!("{b}/big"), &json!({})).await;

    // Patterned 8 MiB payload, uploaded as a 64 KiB-chunk stream.
    let data: Vec<u8> = (0..8 * 1024 * 1024u32).map(|i| (i % 249) as u8).collect();
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = data
        .chunks(64 * 1024)
        .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
        .collect();
    let (s, v) = jput(&c, &format!("{b}/big/carrier"), &json!({"kind": "video"})).await;
    assert_eq!(s, 201);
    let rev = v["rev"].as_str().unwrap().to_string();
    let r = c
        .put(format!("{b}/big/carrier/blob.bin?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(reqwest::Body::wrap_stream(futures_stream_iter(chunks)))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    // GET: identity attachment streams back with an exact content-length.
    let r = c.get(format!("{b}/big/carrier/blob.bin")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(
        r.headers().get("content-length").and_then(|v| v.to_str().ok()),
        Some(data.len().to_string().as_str())
    );
    let got = r.bytes().await.unwrap();
    assert_eq!(&got[..], &data[..], "roundtrip bytes differ");

    // The stored form is bounded chunks, not one blob.
    let (_, doc) = jget(&c, &format!("{b}/big/carrier")).await;
    assert_eq!(doc["_attachments"]["blob.bin"]["length"], json!(data.len()));

    // Compaction re-chunks and must preserve every byte.
    let r = c
        .post(format!("{b}/big/_compact"))
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202);
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, info) = jget(&c, &format!("{b}/big")).await;
        if info["compact_running"] == json!(false) {
            break;
        }
    }
    let got = c
        .get(format!("{b}/big/carrier/blob.bin"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&got[..], &data[..], "post-compaction bytes differ");

    // Replicate to a second server: the attachment lane streams it across
    // (multipart PUT through the streaming parser on the target).
    let tgt = start(None, false).await;
    let (s, _) = jput(
        &c,
        &format!("{b}/_replicator/bigjob"),
        &json!({
            "source": format!("{b}/big"),
            "target": format!("{}/big", tgt.base),
            "create_target": true,
        }),
    )
    .await;
    assert_eq!(s, 201);
    let mut state = String::new();
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (s, v) = jget(&c, &format!("{b}/_scheduler/docs/_replicator/bigjob")).await;
        if s == 200 {
            state = v["state"].as_str().unwrap_or("").to_string();
            if state == "completed" || state == "failed" {
                break;
            }
        }
    }
    assert_eq!(state, "completed");
    let got = c
        .get(format!("{}/big/carrier/blob.bin", tgt.base))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&got[..], &data[..], "replicated bytes differ");
}

fn futures_stream_iter(
    chunks: Vec<Result<bytes::Bytes, std::io::Error>>,
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    futures::stream::iter(chunks)
}

// ---- proto-aware Mango ----------------------------------------------------

/// Minimal protobuf wire encoding for the test payloads (field numbers and
/// types must line up with `proto_descriptor_set` below).
fn pb_f64(field: u32, v: f64) -> Vec<u8> {
    let mut b = vec![((field << 3) | 1) as u8];
    b.extend_from_slice(&v.to_le_bytes());
    b
}
fn pb_bytes(field: u32, data: &[u8]) -> Vec<u8> {
    assert!(data.len() < 128);
    let mut b = vec![((field << 3) | 2) as u8, data.len() as u8];
    b.extend_from_slice(data);
    b
}
fn pb_varint(field: u32, mut v: u64) -> Vec<u8> {
    let mut b = vec![(field << 3) as u8];
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            b.push(byte);
            break;
        }
        b.push(byte | 0x80);
    }
    b
}

/// message test.v1.Point { double latitude = 1; double longitude = 2; }
/// message test.v1.FieldBoundary { string name = 1; double area = 2;
///   repeated Point geo_points = 3; int64 big_count = 4; Point top_left = 5; }
fn proto_descriptor_set() -> Vec<u8> {
    use prost::Message as _;
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    let field = |name: &str, number: i32, typ: Type, type_name: Option<&str>, repeated: bool| {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            r#type: Some(typ as i32),
            type_name: type_name.map(String::from),
            label: Some(if repeated { Label::Repeated } else { Label::Optional } as i32),
            ..Default::default()
        }
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test/v1/test.proto".into()),
            package: Some("test.v1".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Point".into()),
                    field: vec![
                        field("latitude", 1, Type::Double, None, false),
                        field("longitude", 2, Type::Double, None, false),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("FieldBoundary".into()),
                    field: vec![
                        field("name", 1, Type::String, None, false),
                        field("area", 2, Type::Double, None, false),
                        field("geo_points", 3, Type::Message, Some(".test.v1.Point"), true),
                        field("big_count", 4, Type::Int64, None, false),
                        field("top_left", 5, Type::Message, Some(".test.v1.Point"), false),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn boundary_blob(name: &str, area: f64, lat: f64, lon: f64, big: u64) -> Vec<u8> {
    let point = [pb_f64(1, lat), pb_f64(2, lon)].concat();
    let mut m = pb_bytes(1, name.as_bytes());
    m.extend(pb_f64(2, area));
    m.extend(pb_bytes(3, &point));
    m.extend(pb_varint(4, big));
    m.extend(pb_bytes(5, &point));
    m
}

async fn put_blob_doc(c: &reqwest::Client, base: &str, db: &str, id: &str, doctype: &str, blob: &[u8]) {
    let head = json!({"db": {"DocType": doctype, "IsBinaryBlob": true, "OwnerId": "u1"}});
    let (s, v) = jput(c, &format!("{base}/{db}/{id}"), &head).await;
    assert_eq!(s, 201, "{v}");
    let rev = v["rev"].as_str().unwrap();
    let r = c
        .put(format!("{base}/{db}/{id}/blob.data?rev={rev}"))
        .header("content-type", "application/protobuf")
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);
}

/// The nxguide blob-document pattern: JSON head + `blob.data` protobuf
/// attachment. With a FileDescriptorSet registered in `_schemas`, Mango
/// selectors, projections and indexes reach fields inside the blob; without
/// one (or for unregistered doctypes) blobs stay opaque and nothing changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proto_schema_mango() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;

    let (_, welcome) = jget(&c, b).await;
    assert!(welcome["features"].as_array().unwrap().iter().any(|f| f == "proto-schemas"));

    jput(&c, &format!("{b}/pdb"), &json!({})).await;
    put_blob_doc(&c, b, "pdb", "f1", "field_boundary", &boundary_blob("small", 50.0, 48.10, 11.5, 7)).await;
    put_blob_doc(&c, b, "pdb", "f2", "field_boundary", &boundary_blob("mid", 150.0, 48.20, 11.6, 77)).await;
    put_blob_doc(&c, b, "pdb", "f3", "field_boundary", &boundary_blob("big", 250.0, 48.30, 11.7, 777)).await;
    put_blob_doc(&c, b, "pdb", "f4", "field_boundary", &boundary_blob("huge", 999.0, 48.40, 11.8, 7777)).await;
    put_blob_doc(&c, b, "pdb", "m1", "mystery", &boundary_blob("who", 500.0, 0.0, 0.0, 1)).await;
    let (s, _) = jput(&c, &format!("{b}/pdb/plain1"), &json!({"area": 700.0, "kind": "json"})).await;
    assert_eq!(s, 201);

    let area_query = json!({"selector": {"area": {"$gte": 100}}, "limit": 50});
    let ids = |v: &Value| -> Vec<String> {
        let mut ids: Vec<String> = v["docs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };

    // Without schemas the blobs are opaque: only the plain JSON doc matches.
    let (s, v) = jpost(&c, &format!("{b}/pdb/_find"), &area_query).await;
    assert_eq!(s, 200);
    assert_eq!(ids(&v), vec!["plain1"]);

    // Register the descriptor set (an ordinary db + doc + attachment).
    assert_eq!(jput(&c, &format!("{b}/_schemas"), &json!({})).await.0, 201);
    let (s, v) = jput(
        &c,
        &format!("{b}/_schemas/nxguide"),
        &json!({"doctypes": {"legacy_thing": "test.v1.FieldBoundary"}}),
    )
    .await;
    assert_eq!(s, 201);
    let rev = v["rev"].as_str().unwrap();
    let r = c
        .put(format!("{b}/_schemas/nxguide/descriptor.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(proto_descriptor_set())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    // Same query now reaches inside the blobs (registry cache invalidated by
    // the _schemas write). Returned docs stay the stored heads.
    let (s, v) = jpost(&c, &format!("{b}/pdb/_find"), &area_query).await;
    assert_eq!(s, 200);
    assert_eq!(ids(&v), vec!["f2", "f3", "f4", "plain1"]);
    let f2 = v["docs"].as_array().unwrap().iter().find(|d| d["_id"] == "f2").unwrap();
    assert!(f2.get("area").is_none(), "bare docs must stay stored heads: {f2}");
    assert_eq!(f2["db"]["DocType"], json!("field_boundary"));

    // Projections may name blob-interior fields.
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({
            "selector": {"area": {"$gte": 100}},
            "fields": ["_id", "area", "topLeft.latitude"],
            "limit": 50,
        }),
    )
    .await;
    assert_eq!(s, 200);
    let f3 = v["docs"].as_array().unwrap().iter().find(|d| d["_id"] == "f3").unwrap();
    assert_eq!(f3["area"], json!(250.0));
    assert_eq!(f3["topLeft"]["latitude"], json!(48.30));

    // int64 fields stringify exactly like protojson.
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({"selector": {"bigCount": "77"}, "limit": 50}),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(ids(&v), vec!["f2"]);

    // Unregistered doctypes stay opaque but remain findable by their head.
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({"selector": {"db.DocType": "mystery", "area": {"$exists": true}}, "limit": 50}),
    )
    .await;
    assert_eq!(s, 200);
    assert!(ids(&v).is_empty());
    let (_, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({"selector": {"db.DocType": "mystery"}, "limit": 50}),
    )
    .await;
    assert_eq!(ids(&v), vec!["m1"]);

    // A Mango index on a blob-interior field: built through the decoder,
    // serves sorted range queries.
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_index"),
        &json!({"index": {"fields": ["area"]}, "name": "idx_area"}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_explain"),
        &json!({"selector": {"area": {"$gte": 100}}, "sort": [{"area": "asc"}]}),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(v["index"]["name"], json!("idx_area"), "{v}");
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({"selector": {"area": {"$gte": 100}}, "sort": [{"area": "asc"}], "limit": 50}),
    )
    .await;
    assert_eq!(s, 200);
    let order: Vec<String> = v["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_str().unwrap().to_string())
        .collect();
    // blob areas 150, 250, 999 collate with the plain doc's 700
    assert_eq!(order, vec!["f2", "f3", "plain1", "f4"]);

    // Incremental index update keys a NEW blob doc through the decoder, and
    // the explicit doctypes mapping resolves non-convention names.
    put_blob_doc(&c, b, "pdb", "lg1", "legacy_thing", &boundary_blob("legacy", 100000.0, 1.0, 2.0, 5)).await;
    let (s, v) = jpost(
        &c,
        &format!("{b}/pdb/_find"),
        &json!({"selector": {"area": {"$gte": 100000}}, "limit": 50}),
    )
    .await;
    assert_eq!(s, 200);
    assert_eq!(ids(&v), vec!["lg1"]);
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

fn pb_bytes_long(field: u32, data: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    let mut v = ((field as u64) << 3) | 2;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 { b.push(byte); break; }
        b.push(byte | 0x80);
    }
    let mut l = data.len() as u64;
    loop {
        let byte = (l & 0x7f) as u8;
        l >>= 7;
        if l == 0 { b.push(byte); break; }
        b.push(byte | 0x80);
    }
    b.extend_from_slice(data);
    b
}

/// Index maintenance extracts only the indexed paths from blob wire bytes:
/// a blob dominated by a huge field the index doesn't touch must still key
/// correctly (the big field is skipped, not decoded), and the same doc must
/// answer full-view selector queries too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proto_selective_extraction() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;

    jput(&c, &format!("{b}/xdb"), &json!({})).await;
    assert_eq!(jput(&c, &format!("{b}/_schemas"), &json!({})).await.0, 201);
    let (_, v) = jput(&c, &format!("{b}/_schemas/x"), &json!({})).await;
    let rev = v["rev"].as_str().unwrap();
    let r = c
        .put(format!("{b}/_schemas/x/descriptor.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(proto_descriptor_set())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    // Index first, docs after: every doc is keyed by the incremental
    // updater, whose augmenter runs in extraction mode (fields, no pfs).
    let (s, _) = jpost(
        &c,
        &format!("{b}/xdb/_index"),
        &json!({"index": {"fields": ["area"]}, "name": "idx_area"}),
    )
    .await;
    assert_eq!(s, 200);

    // 6 MiB of unknown-field payload dwarfing the wanted 9-byte area field
    // (multi-chunk in couch-store, so skipping also skips disk chunks).
    let mut blob = boundary_blob("giant", 424242.0, 3.0, 4.0, 9);
    blob.extend_from_slice(&pb_bytes_long(900, &vec![0x5Au8; 6 * 1024 * 1024]));
    put_blob_doc(&c, b, "xdb", "big1", "field_boundary", &blob).await;
    put_blob_doc(&c, b, "xdb", "small1", "field_boundary", &boundary_blob("s", 7.0, 1.0, 2.0, 1)).await;

    let (s, v) = jpost(
        &c,
        &format!("{b}/xdb/_find"),
        &json!({"selector": {"area": {"$gte": 400000}}, "limit": 10}),
    )
    .await;
    assert_eq!(s, 200);
    let ids: Vec<&str> = v["docs"].as_array().unwrap().iter().map(|d| d["_id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["big1"]);

    // Full-view path (selector on a non-indexed blob field) still works on
    // the same doc — extraction and full decode agree.
    let (s, v) = jpost(
        &c,
        &format!("{b}/xdb/_find"),
        &json!({"selector": {"name": "giant"}, "fields": ["_id", "area", "topLeft.latitude"], "limit": 10}),
    )
    .await;
    assert_eq!(s, 200);
    let d = &v["docs"].as_array().unwrap()[0];
    assert_eq!(d["_id"], json!("big1"));
    assert_eq!(d["area"], json!(424242.0));
    assert_eq!(d["topLeft"]["latitude"], json!(3.0));
}

/// No fallbacks: problems surface where they happen instead of degrading.
/// Bad descriptor uploads are rejected at the door; a corrupt blob of a
/// REGISTERED doctype fails the queries that touch it (and heals when the
/// bad doc is removed); unregistered doctypes stay opaque by contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proto_strictness() {
    let srv = start(None, false).await;
    let c = client();
    let b = &srv.base;

    jput(&c, &format!("{b}/sdb"), &json!({})).await;
    assert_eq!(jput(&c, &format!("{b}/_schemas"), &json!({})).await.0, 201);

    // Interactive _schemas writes are validated at the door.
    let (s, v) = jput(&c, &format!("{b}/_schemas/bad1"), &json!({"doctypes": "nope"})).await;
    assert_eq!(s, 400, "{v}");
    let (s, v) = jput(
        &c,
        &format!("{b}/_schemas/bad2"),
        &json!({"doctypes": {"x": 42}}),
    )
    .await;
    assert_eq!(s, 400, "{v}");
    let (_, v) = jput(&c, &format!("{b}/_schemas/x"), &json!({})).await;
    let rev = v["rev"].as_str().unwrap();
    let r = c
        .put(format!("{b}/_schemas/x/garbage.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(vec![0xffu8, 0x13, 0x37])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 400);

    // Register the real descriptor set.
    let (_, v) = jget(&c, &format!("{b}/_schemas/x")).await;
    let rev = v["_rev"].as_str().unwrap();
    let r = c
        .put(format!("{b}/_schemas/x/descriptor.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(proto_descriptor_set())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 201);

    put_blob_doc(&c, b, "sdb", "ok1", "field_boundary", &boundary_blob("fine", 10.0, 1.0, 2.0, 1)).await;
    let q = json!({"selector": {"area": {"$gte": 1}}, "limit": 10});
    let (s, v) = jpost(&c, &format!("{b}/sdb/_find"), &q).await;
    assert_eq!(s, 200);
    assert_eq!(v["docs"].as_array().unwrap().len(), 1);

    // A corrupt blob of a REGISTERED doctype fails queries that touch it —
    // loudly, instead of being silently treated as opaque.
    // (0x1a = field 3 length-delimited, length 0xff, then nothing.)
    put_blob_doc(&c, b, "sdb", "corrupt1", "field_boundary", &[0x1a, 0xff, 0x01]).await;
    let (s, v) = jpost(&c, &format!("{b}/sdb/_find"), &q).await;
    assert_eq!(s, 500, "corrupt registered blob must fail the query, got: {v}");

    // Removing the bad doc heals the database.
    let (_, v) = jget(&c, &format!("{b}/sdb/corrupt1")).await;
    let rev = v["_rev"].as_str().unwrap();
    let r = c
        .delete(format!("{b}/sdb/corrupt1?rev={rev}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let (s, v) = jpost(&c, &format!("{b}/sdb/_find"), &q).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["docs"].as_array().unwrap().len(), 1);

    // Unregistered doctypes are opaque by contract, not by failure: the
    // same corrupt bytes under an unknown doctype don't error anything.
    put_blob_doc(&c, b, "sdb", "unk1", "not_registered", &[0x1a, 0xff, 0x01]).await;
    let (s, v) = jpost(&c, &format!("{b}/sdb/_find"), &q).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["docs"].as_array().unwrap().len(), 1);
}

/// Proto-native world: application documents ARE protobuf bytes; no JSON is
/// stored. PUT/GET/_find/_delete/replication all work over proto bodies,
/// and JSON application writes are rejected (no fallbacks, no coexistence).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proto_native_documents() {
    let src = start(None, false).await;
    let c = client();
    let b = &src.base;

    let (_, welcome) = jget(&c, b).await;
    assert!(welcome["features"].as_array().unwrap().iter().any(|f| f == "proto-docs"));

    // Register schemas, create a proto-native db.
    assert_eq!(jput(&c, &format!("{b}/_schemas"), &json!({})).await.0, 201);
    let (_, v) = jput(&c, &format!("{b}/_schemas/s"), &json!({})).await;
    let rev = v["rev"].as_str().unwrap();
    let r = c.put(format!("{b}/_schemas/s/d.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(proto_descriptor_set()).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 201);
    assert_eq!(c.put(format!("{b}/pn?proto=true")).send().await.unwrap().status().as_u16(), 201);

    // JSON application write into a proto db is rejected — no fallback.
    let (s, _) = jput(&c, &format!("{b}/pn/j1"), &json!({"area": 1.0})).await;
    assert_eq!(s, 400);

    // PUT a proto document (FieldBoundary bytes).
    let put_pb = |id: &str, blob: Vec<u8>| {
        let url = format!("{b}/pn/{id}");
        let c = c.clone();
        async move {
            c.put(url).header("content-type", "application/protobuf")
                .header("x-proto-type", "test.v1.FieldBoundary")
                .body(blob).send().await.unwrap()
        }
    };
    let r = put_pb("f1", boundary_blob("north", 42.0, 48.0, 11.0, 100)).await;
    assert_eq!(r.status().as_u16(), 201);
    let rev1 = r.json::<Value>().await.unwrap()["rev"].as_str().unwrap().to_string();

    // Bytes that don't decode as the declared type are rejected at the door.
    let r = put_pb("bad", vec![0x1a, 0xff, 0x01]).await;
    assert_eq!(r.status().as_u16(), 400);

    // GET as protobuf → exact bytes back; GET as JSON → rendered view.
    let r = c.get(format!("{b}/pn/f1")).header("accept", "application/protobuf").send().await.unwrap();
    assert_eq!(r.headers().get("x-proto-type").unwrap(), "test.v1.FieldBoundary");
    let got = r.bytes().await.unwrap();
    assert_eq!(got.as_ref(), boundary_blob("north", 42.0, 48.0, 11.0, 100).as_slice());
    let (s, v) = jget(&c, &format!("{b}/pn/f1")).await;
    assert_eq!(s, 200);
    assert_eq!(v["name"], json!("north"));
    assert_eq!(v["area"], json!(42.0));
    assert_eq!(v["topLeft"]["latitude"], json!(48.0));
    assert!(v.get("$pb_body").is_none(), "view must not leak the envelope: {v}");

    // _find over proto bodies (bare + projected + index).
    for (id, a) in [("f2", 150.0), ("f3", 250.0)] {
        assert_eq!(put_pb(id, boundary_blob(id, a, 1.0, 2.0, 9)).await.status().as_u16(), 201);
    }
    let (s, v) = jpost(&c, &format!("{b}/pn/_find"),
        &json!({"selector": {"area": {"$gte": 100}}, "limit": 10})).await;
    assert_eq!(s, 200);
    let mut ids: Vec<&str> = v["docs"].as_array().unwrap().iter().map(|d| d["_id"].as_str().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec!["f2", "f3"]);
    // bare result is the rendered view, not the envelope
    let f2 = v["docs"].as_array().unwrap().iter().find(|d| d["_id"] == "f2").unwrap();
    assert_eq!(f2["area"], json!(150.0));
    assert!(f2.get("$pb_type").is_none());

    // _all_docs include_docs renders too.
    let (_, v) = jget(&c, &format!("{b}/pn/_all_docs?include_docs=true")).await;
    let doc = v["rows"].as_array().unwrap().iter().find(|r| r["id"] == "f1").unwrap();
    assert_eq!(doc["doc"]["name"], json!("north"));

    // Proto-body negotiation: a proto-aware client asks for stored $pb
    // envelopes (raw bytes, base64) instead of rendered views — no server
    // re-render, no client protojson. The bytes decode to the same message.
    let (s, v) = jpost(&c, &format!("{b}/pn/_find"),
        &json!({"selector": {"area": {"$gte": 100}}, "proto_bodies": true, "limit": 10})).await;
    assert_eq!(s, 200);
    let f2 = v["docs"].as_array().unwrap().iter().find(|d| d["_id"] == "f2").unwrap();
    assert_eq!(f2["$pb_type"], json!("test.v1.FieldBoundary"), "want envelope, got {f2}");
    assert!(f2.get("area").is_none(), "envelope must not be pre-rendered");
    let raw = base64_decode(f2["$pb_body"].as_str().unwrap());
    assert_eq!(raw, boundary_blob("f2", 150.0, 1.0, 2.0, 9));
    // _all_docs and _changes honor the query-param form.
    let (_, v) = jget(&c, &format!("{b}/pn/_all_docs?include_docs=true&proto_bodies=true")).await;
    let doc = v["rows"].as_array().unwrap().iter().find(|r| r["id"] == "f2").unwrap();
    assert_eq!(doc["doc"]["$pb_type"], json!("test.v1.FieldBoundary"));

    // Delete produces a proto tombstone; the doc is then missing.
    let r = c.delete(format!("{b}/pn/f1?rev={rev1}")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(jget(&c, &format!("{b}/pn/f1")).await.0, 404);

    // Replicate the proto-native db to a second proto-native server: bodies
    // flow through the embedded replicator as $pb envelopes and land intact.
    let tgt = start(None, false).await;
    let tb = &tgt.base;
    assert_eq!(jput(&c, &format!("{tb}/_schemas"), &json!({})).await.0, 201);
    let (_, v) = jput(&c, &format!("{tb}/_schemas/s"), &json!({})).await;
    let rev = v["rev"].as_str().unwrap();
    let r = c.put(format!("{tb}/_schemas/s/d.pb?rev={rev}"))
        .header("content-type", "application/octet-stream")
        .body(proto_descriptor_set()).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 201);
    assert_eq!(c.put(format!("{tb}/pn?proto=true")).send().await.unwrap().status().as_u16(), 201);

    let (s, rv) = jpost(&c, &format!("{b}/_replicate"),
        &json!({"source": format!("{b}/pn"), "target": format!("{tb}/pn")})).await;
    assert_eq!(s, 200, "{rv}");
    assert_eq!(rv["ok"], json!(true));

    // Target has the live docs as proto, queryable, with identical bytes.
    let (s, v) = jpost(&c, &format!("{tb}/pn/_find"),
        &json!({"selector": {"area": {"$gte": 100}}, "limit": 10})).await;
    assert_eq!(s, 200);
    assert_eq!(v["docs"].as_array().unwrap().len(), 2);
    let r = c.get(format!("{tb}/pn/f2")).header("accept", "application/protobuf").send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.bytes().await.unwrap().as_ref(), boundary_blob("f2", 150.0, 1.0, 2.0, 9).as_slice());
}
