//! Check which real blob files decode under prost / extract cleanly.
//! Usage: decode_check <blobs_dir> <descriptor.pb>

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let desc = std::fs::read(&args[2]).unwrap();
    let (reg, _) =
        couch_proto::Registry::build(&[desc], &std::collections::HashMap::new()).unwrap();
    let mut entries: Vec<_> = std::fs::read_dir(&args[1]).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    let (mut ok, mut bad) = (0, 0);
    for e in entries {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some((doctype, _)) = name.split_once("__") else { continue };
        let bytes = std::fs::read(e.path()).unwrap();
        let full = reg.decode_doc(doctype, &bytes);
        let desc = reg.resolve(doctype).unwrap().clone();
        let trie =
            couch_proto::PathTrie::compile(&desc, &["db.OwnerId".to_string()]).unwrap();
        let ext = trie.extract(&mut couch_proto::SliceReader(&bytes), bytes.len() as u64);
        match (&full, &ext) {
            (Ok(_), Ok(_)) => ok += 1,
            _ => {
                bad += 1;
                println!(
                    "{name}: full={} extract={}",
                    full.as_ref().err().map(|e| e.as_str()).unwrap_or("ok"),
                    ext.as_ref().err().map(|e| e.as_str()).unwrap_or("ok"),
                );
            }
        }
    }
    println!("{ok} ok, {bad} bad");
}
