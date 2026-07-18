//! At-rest gzip vs identity attachment storage, on real blob bytes.
//!
//! Usage: atrest_bench <blobs_dir> <descriptor.pb> <work_dir> [replicas]
//!
//! Feeds the same real protobuf blobs (files named `<doctype>__<n>.pb`)
//! through couch-store twice — once stored as-is, once stored gzipped —
//! and measures what an at-rest-gzip feature would actually change:
//! write cost, disk footprint, full-attachment reads, and the selective
//! index-extraction path (which on gzip loses disk-skipping and must
//! inflate everything first).

use couch_store::db::Db;
use couch_store::writer::{DbWriter, SaveOutcome, SpooledAtt};
use md5::{Digest, Md5};
use serde_json::json;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Instant;

fn spool_of(bytes: &[u8]) -> SpooledAtt {
    let mut f = tempfile::tempfile().unwrap();
    f.write_all(bytes).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    SpooledAtt {
        name: "blob.data".into(),
        content_type: "application/protobuf".into(),
        revpos: None,
        len: bytes.len() as u64,
        md5: Md5::digest(bytes).to_vec(),
        file: f,
    }
}

fn write_db(path: &str, docs: &[(String, String, Vec<u8>)], gzip: bool) -> (f64, u64) {
    let _ = std::fs::remove_file(path);
    let t = Instant::now();
    let mut w = DbWriter::create(path).unwrap();
    for (i, (id, doctype, bytes)) in docs.iter().enumerate() {
        let stored: Vec<u8> = if gzip {
            let mut enc =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(bytes).unwrap();
            enc.finish().unwrap()
        } else {
            bytes.clone()
        };
        let doc = json!({
            "_id": id,
            "db": {"DocType": doctype, "IsBinaryBlob": true, "OwnerId": "bench-user"},
            "_attachments": {"blob.data": {"content_type": "application/protobuf"}},
        });
        let outcome = w.save_doc_with_spools(&doc, None, vec![spool_of(&stored)]).unwrap();
        assert!(matches!(outcome, SaveOutcome::Ok { .. }), "doc {id}");
        if i % 200 == 199 {
            w.commit().unwrap();
        }
    }
    w.commit().unwrap();
    let secs = t.elapsed().as_secs_f64();
    (secs, std::fs::metadata(path).unwrap().len())
}

fn read_all(path: &str, ids: &[String], gzip: bool) -> (f64, u64) {
    let db = Db::open(path).unwrap();
    let t = Instant::now();
    let mut total = 0u64;
    for id in ids {
        let att = db.att_info(id.as_bytes(), "blob.data").unwrap().unwrap();
        let stored = couch_store::doc::read_att_data(&db.file, &att).unwrap();
        let data = if gzip {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(&stored[..]).read_to_end(&mut out).unwrap();
            out
        } else {
            stored
        };
        total += data.len() as u64;
    }
    (t.elapsed().as_secs_f64(), total)
}

/// The index-build path: pull `db.OwnerId` out of every blob.
/// identity: trie extraction over the chunk reader (disk skips intact).
/// gzip: read + inflate the whole attachment, then trie-extract in memory.
/// full: the pre-extraction behavior (inflate if needed + decode all).
fn index_path(
    path: &str,
    ids: &[String],
    reg: &couch_proto::Registry,
    doctypes: &std::collections::HashMap<String, String>,
    mode: &str,
) -> (f64, u64) {
    let db = Db::open(path).unwrap();
    let paths = vec!["db.OwnerId".to_string()];
    let t = Instant::now();
    let mut hits = 0u64;
    for id in ids {
        let doctype = &doctypes[id];
        let desc = reg.resolve(doctype).unwrap().clone();
        let att = db.att_info(id.as_bytes(), "blob.data").unwrap().unwrap();
        let v = match mode {
            "identity-extract" => {
                let trie = couch_proto::PathTrie::compile(&desc, &paths).unwrap();
                let mut r = Adapter(couch_store::doc::AttReader::new(&db.file, &att));
                trie.extract(&mut r, att.att_len).unwrap()
            }
            "gzip-extract" => {
                let stored = couch_store::doc::read_att_data(&db.file, &att).unwrap();
                let mut data = Vec::new();
                flate2::read::GzDecoder::new(&stored[..]).read_to_end(&mut data).unwrap();
                let trie = couch_proto::PathTrie::compile(&desc, &paths).unwrap();
                trie.extract(&mut couch_proto::SliceReader(&data), data.len() as u64)
                    .unwrap()
            }
            "gzip-fulldecode" => {
                let stored = couch_store::doc::read_att_data(&db.file, &att).unwrap();
                let mut data = Vec::new();
                flate2::read::GzDecoder::new(&stored[..]).read_to_end(&mut data).unwrap();
                reg.decode_doc(doctype, &data).unwrap()
            }
            _ => unreachable!(),
        };
        if v.get("db").and_then(|d| d.get("OwnerId")).is_some() {
            hits += 1;
        }
    }
    (t.elapsed().as_secs_f64(), hits)
}

struct Adapter<'a>(couch_store::doc::AttReader<'a>);
impl couch_proto::SkipRead for Adapter<'_> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), String> {
        self.0.read_exact(buf).map_err(|e| e.to_string())
    }
    fn skip(&mut self, n: u64) -> Result<(), String> {
        self.0.skip(n).map_err(|e| e.to_string())
    }
}

fn drop_caches() -> bool {
    std::process::Command::new("sudo")
        .args(["-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (blob_dir, desc_path, work) = (&args[1], &args[2], &args[3]);
    let replicas: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(20);

    let desc_bytes = std::fs::read(desc_path).unwrap();
    let (reg, _adv) =
        couch_proto::Registry::build(&[desc_bytes], &std::collections::HashMap::new()).unwrap();

    let mut docs: Vec<(String, String, Vec<u8>)> = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(blob_dir).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    let mut excluded = 0usize;
    let mut kept = 0usize;
    for e in &entries {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some((doctype, _)) = name.split_once("__") else { continue };
        let bytes = std::fs::read(e.path()).unwrap();
        // The corpus is real production data and contains blobs that are
        // not valid protobuf for their declared type (legacy formats,
        // pre-schema-change field reuse). They fail queries loudly by
        // design; here they'd just abort the run, so exclude them and say
        // so — the benchmark measures storage cost, not data quality.
        if reg.decode_doc(doctype, &bytes).is_err() {
            excluded += 1;
            continue;
        }
        kept += 1;
        for r in 0..replicas {
            docs.push((format!("{name}--{r}"), doctype.to_string(), bytes.clone()));
        }
    }
    if excluded > 0 {
        println!("EXCLUDED {excluded} real blobs that do not decode as their declared type");
    }
    let raw_bytes: u64 = docs.iter().map(|(_, _, b)| b.len() as u64).sum();
    println!(
        "corpus: {kept} unique blobs x {replicas} = {} docs, {:.1} MB raw",
        docs.len(),
        raw_bytes as f64 / 1e6
    );

    // compression characteristics alone
    let t = Instant::now();
    let gz_bytes: u64 = docs
        .iter()
        .map(|(_, _, b)| {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(b).unwrap();
            enc.finish().unwrap().len() as u64
        })
        .sum();
    let gz_cpu = t.elapsed().as_secs_f64();
    println!(
        "gzip -1 alone: {:.1} MB -> {:.1} MB ({:.2}x), {:.2}s CPU ({:.0} MB/s)",
        raw_bytes as f64 / 1e6,
        gz_bytes as f64 / 1e6,
        raw_bytes as f64 / gz_bytes as f64,
        gz_cpu,
        raw_bytes as f64 / 1e6 / gz_cpu
    );

    let ids: Vec<String> = docs.iter().map(|(id, _, _)| id.clone()).collect();
    let doctypes: std::collections::HashMap<String, String> =
        docs.iter().map(|(id, dt, _)| (id.clone(), dt.clone())).collect();

    let ident = format!("{work}/identity.couch");
    let gzip = format!("{work}/gzip.couch");
    let (wi, si) = write_db(&ident, &docs, false);
    let (wg, sg) = write_db(&gzip, &docs, true);
    println!("\nwrite (PUT path incl. spool+md5+commit every 200):");
    println!("  identity: {wi:.2}s, file {:.1} MB", si as f64 / 1e6);
    println!("  gzip:     {wg:.2}s, file {:.1} MB   disk saved: {:.0}%",
        sg as f64 / 1e6, 100.0 * (1.0 - sg as f64 / si as f64));

    let cold = drop_caches();
    println!("\nfull attachment reads ({}):", if cold { "cold cache" } else { "warm cache" });
    let (ri, ti) = read_all(&ident, &ids, false);
    if cold { drop_caches(); }
    let (rg, tg) = read_all(&gzip, &ids, true);
    assert_eq!(ti, tg);
    println!("  identity: {ri:.2}s   gzip+inflate: {rg:.2}s   ({:+.0}% read cost)",
        100.0 * (rg / ri - 1.0));

    println!("\nindex-build path (extract db.OwnerId from every blob):");
    if cold { drop_caches(); }
    let (ei, hi) = index_path(&ident, &ids, &reg, &doctypes, "identity-extract");
    if cold { drop_caches(); }
    let (eg, hg) = index_path(&gzip, &ids, &reg, &doctypes, "gzip-extract");
    if cold { drop_caches(); }
    let (ef, _) = index_path(&gzip, &ids, &reg, &doctypes, "gzip-fulldecode");
    assert_eq!(hi, hg);
    println!("  identity + chunk-skip extract: {ei:.2}s ({hi}/{} with db.OwnerId)", ids.len());
    println!("  gzip: inflate + extract:       {eg:.2}s ({:.1}x identity)", eg / ei);
    println!("  gzip: inflate + full decode:   {ef:.2}s ({:.1}x identity)", ef / ei);
}
