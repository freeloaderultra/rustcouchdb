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
    let (_, v) = jget(&c, &format!("{b}/pdb/_all_docs")).await;
    assert_eq!(v["total_rows"], json!(1));
    let (_, v) = jget(&c, &format!("{b}/pdb/_changes")).await;
    let ids: Vec<&str> = v["results"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(!ids.contains(&"gone"), "purged doc must vanish from _changes: {ids:?}");
    // The index must not serve entries for the purged doc.
    let (_, r) = jpost(&c, &format!("{b}/pdb/_find"), &json!({"selector": {"t": "x"}})).await;
    let found: Vec<&str> = r["docs"].as_array().unwrap().iter().map(|d| d["_id"].as_str().unwrap()).collect();
    assert_eq!(found, vec!["keep"]);
    // doc_count reflects the purge.
    let (_, v) = jget(&c, &format!("{b}/pdb")).await;
    assert_eq!(v["doc_count"], json!(1));

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
