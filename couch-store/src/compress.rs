//! Port of couch_compress: terms on disk are either snappy-compressed ETF
//! (prefix byte 1), plain ETF (prefix 131, possibly zlib-compressed inside),
//! or zstd (unsupported here; CouchDB only writes it on OTP 28+ when
//! explicitly configured).

use crate::error::{corrupt, Error, Result};
use crate::etf::{self, Term};

const SNAPPY_PREFIX: u8 = 1;
const TERM_PREFIX: u8 = 131;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

pub fn decompress(bin: &[u8]) -> Result<Term> {
    match bin.first() {
        Some(&SNAPPY_PREFIX) => {
            let raw = snap::raw::Decoder::new()
                .decompress_vec(&bin[1..])
                .map_err(|e| corrupt(format!("snappy decompress failed: {e}")))?;
            etf::decode(&raw)
        }
        Some(&TERM_PREFIX) => etf::decode(bin),
        _ if bin.starts_with(&ZSTD_MAGIC) => Err(Error::Unsupported(
            "zstd-compressed term (file written with file_compression = zstd)".into(),
        )),
        _ => Err(corrupt("unknown compression prefix")),
    }
}

/// Compress a term the way CouchDB's default config does (snappy).
pub fn compress(t: &Term) -> Vec<u8> {
    let raw = etf::encode(t);
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&raw)
        .expect("snappy compress");
    let mut out = Vec::with_capacity(compressed.len() + 1);
    out.push(SNAPPY_PREFIX);
    out.extend_from_slice(&compressed);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snappy_roundtrip() {
        let t = Term::Tuple(vec![
            Term::List(vec![Term::Tuple(vec![
                Term::Bin(b"key".to_vec()),
                Term::Bin(b"value".to_vec()),
            ])]),
            Term::List(vec![]),
        ]);
        let bin = compress(&t);
        assert_eq!(bin[0], SNAPPY_PREFIX);
        let back = decompress(&bin).unwrap();
        assert!(t == back);
    }

    #[test]
    fn plain_etf_accepted() {
        let t = Term::Int(42);
        let back = decompress(&etf::encode(&t)).unwrap();
        assert!(t == back);
    }
}
