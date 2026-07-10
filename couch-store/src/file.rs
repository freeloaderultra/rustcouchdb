//! Port of couch_file: the append-only block file layout.
//!
//! The file is a sequence of 4096-byte blocks. The first byte of every block
//! is a marker: 1 if a header starts at this block boundary, 0 otherwise
//! (data simply continues around it). Data chunks are:
//!
//! ```text
//! <<0:1, Len:31, Data:Len>>                      plain chunk
//! <<1:1, Len:31, Checksum:16/binary, Data:Len>>  checksummed chunk
//! ```
//!
//! with a marker byte injected at every block boundary the chunk crosses.
//! Checksums are 16 bytes: XXH3-128 (canonical big-endian, the 3.4+ default)
//! or MD5 (legacy); readers accept both. Headers live only at block
//! boundaries: `<<1, Size:32, Checksum:16, TermBin:(Size-16)>>` (also subject
//! to boundary markers) and are found by scanning backwards from EOF.

use crate::error::{corrupt, Result};
use crate::etf::{self, Term};
use md5::{Digest, Md5};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

pub const BLOCK: u64 = 4096;
const PREFIX: u64 = 5; // header marker byte + u32 size

pub struct CouchFile {
    file: File,
    pub path: PathBuf,
    pub eof: u64,
    writable: bool,
}

impl CouchFile {
    pub fn open_read(path: impl AsRef<Path>) -> Result<CouchFile> {
        let file = File::open(path.as_ref())?;
        let eof = file.metadata()?.len();
        Ok(CouchFile {
            file,
            path: path.as_ref().to_path_buf(),
            eof,
            writable: false,
        })
    }

    /// Create a brand-new file; fails if it already exists.
    pub fn create(path: impl AsRef<Path>) -> Result<CouchFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.as_ref())?;
        Ok(CouchFile {
            file,
            path: path.as_ref().to_path_buf(),
            eof: 0,
            writable: true,
        })
    }

    /// Open an existing file for appending.
    pub fn open_rw(path: impl AsRef<Path>) -> Result<CouchFile> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let eof = file.metadata()?.len();
        Ok(CouchFile {
            file,
            path: path.as_ref().to_path_buf(),
            eof,
            writable: true,
        })
    }

    fn read_at(&self, pos: u64, len: usize) -> Result<Vec<u8>> {
        if pos + len as u64 > self.eof {
            return Err(corrupt(format!(
                "read beyond eof: pos={pos} len={len} eof={}",
                self.eof
            )));
        }
        let mut buf = vec![0u8; len];
        self.file.read_exact_at(&mut buf, pos)?;
        Ok(buf)
    }

    /// Read `len` payload bytes starting at `pos`, transparently skipping the
    /// block marker bytes. Port of read + remove_block_prefixes.
    fn read_stripped(&self, pos: u64, len: usize) -> Result<Vec<u8>> {
        let block_offset = pos % BLOCK;
        let total = total_read_len(block_offset, len);
        let raw = self.read_at(pos, total)?;
        Ok(strip_block_markers(block_offset, &raw, len))
    }

    /// Number of raw file bytes a `len`-byte payload occupies starting at
    /// `block_offset` within a block. Port of calculate_total_read_len.
    fn chunk_payload_pos(&self, pos: u64) -> u64 {
        pos + total_read_len(pos % BLOCK, 4) as u64
    }

    /// Read a data chunk written by append_binary / append_raw_chunk.
    /// Verifies the checksum if the chunk has one.
    pub fn read_chunk(&self, pos: u64) -> Result<Vec<u8>> {
        let hdr = self.read_stripped(pos, 4)?;
        let word = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let has_checksum = word & 0x8000_0000 != 0;
        let len = (word & 0x7fff_ffff) as usize;
        let data_pos = self.chunk_payload_pos(pos);
        if has_checksum {
            let full = self.read_stripped(data_pos, len + 16)?;
            let (ck, data) = full.split_at(16);
            verify_checksum(data, ck)
                .map_err(|_| corrupt(format!("chunk checksum mismatch at pos {pos}")))?;
            Ok(data.to_vec())
        } else {
            self.read_stripped(data_pos, len)
        }
    }

    /// Read a term chunk (decompressing couch_compress framing).
    pub fn read_term(&self, pos: u64) -> Result<Term> {
        let bin = self.read_chunk(pos)?;
        crate::compress::decompress(&bin)
    }

    /// Find the newest valid header, scanning backwards from EOF.
    pub fn read_header(&self) -> Result<Term> {
        let mut block = self.eof / BLOCK;
        loop {
            if let Ok(Some(term)) = self.try_load_header(block) {
                return Ok(term);
            }
            if block == 0 {
                return Err(corrupt("no valid header found"));
            }
            block -= 1;
        }
    }

    fn try_load_header(&self, block: u64) -> Result<Option<Term>> {
        let pos = block * BLOCK;
        if pos + PREFIX as u64 > self.eof {
            return Ok(None);
        }
        let prefix = self.read_at(pos, PREFIX as usize)?;
        if prefix[0] != 1 {
            return Ok(None);
        }
        let size = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
        if size < 16 {
            return Ok(None);
        }
        let total = total_read_len(PREFIX, size);
        if pos + PREFIX + total as u64 > self.eof {
            return Ok(None);
        }
        let raw = self.read_at(pos + PREFIX, total)?;
        let payload = strip_block_markers(PREFIX, &raw, size);
        let (ck, header_bin) = payload.split_at(16);
        if verify_checksum(header_bin, ck).is_err() {
            return Ok(None);
        }
        match etf::decode(header_bin) {
            Ok(t) => Ok(Some(t)),
            Err(_) => Ok(None),
        }
    }

    // ------------------------------------------------------------- writing

    fn append_raw(&mut self, assembled: &[u8]) -> Result<(u64, u64)> {
        assert!(self.writable, "file opened read-only");
        let pos = self.eof;
        let blocks = add_block_markers(pos % BLOCK, assembled);
        self.file.write_all_at(&blocks, pos)?;
        self.eof += blocks.len() as u64;
        Ok((pos, blocks.len() as u64))
    }

    /// Append a plain chunk (no checksum) — what append_binary does.
    /// Returns (pos, bytes written including framing).
    pub fn append_chunk(&mut self, data: &[u8]) -> Result<(u64, u64)> {
        let mut assembled = Vec::with_capacity(data.len() + 4);
        assembled.extend_from_slice(&(data.len() as u32).to_be_bytes());
        assembled.extend_from_slice(data);
        self.append_raw(&assembled)
    }

    /// Append a checksummed chunk — what assemble_file_chunk_and_checksum
    /// produces (doc summaries). MD5 checksums: every CouchDB release
    /// verifies those, with or without xxhash support.
    pub fn append_chunk_checksummed(&mut self, data: &[u8]) -> Result<(u64, u64)> {
        let ck = Md5::digest(data);
        let mut assembled = Vec::with_capacity(data.len() + 20);
        assembled.extend_from_slice(&((data.len() as u32) | 0x8000_0000).to_be_bytes());
        assembled.extend_from_slice(&ck);
        assembled.extend_from_slice(data);
        self.append_raw(&assembled)
    }

    /// Append a term, snappy-compressed (couch_file:append_term).
    pub fn append_term(&mut self, t: &Term) -> Result<(u64, u64)> {
        self.append_chunk(&crate::compress::compress(t))
    }

    /// Write a header at the next block boundary (couch_file:write_header).
    pub fn write_header(&mut self, t: &Term) -> Result<()> {
        assert!(self.writable, "file opened read-only");
        let bin = etf::encode(t);
        let ck = Md5::digest(&bin);
        let mut payload = Vec::with_capacity(bin.len() + 16);
        payload.extend_from_slice(&ck);
        payload.extend_from_slice(&bin);

        let mut out = Vec::new();
        let block_offset = self.eof % BLOCK;
        if block_offset != 0 {
            out.resize((BLOCK - block_offset) as usize, 0);
        }
        out.push(1);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&add_block_markers(PREFIX, &payload));
        self.file.write_all_at(&out, self.eof)?;
        self.eof += out.len() as u64;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(not(unix))]
compile_error!("couch-store requires a unix platform (uses pread/pwrite)");

/// couch_file:calculate_total_read_len/2
fn total_read_len(block_offset: u64, len: usize) -> usize {
    if block_offset == 0 {
        return total_read_len(1, len) + 1;
    }
    let block_left = (BLOCK - block_offset) as usize;
    if block_left >= len {
        len
    } else {
        let rest = len - block_left;
        let per_block = (BLOCK - 1) as usize;
        len + rest / per_block + if rest % per_block == 0 { 0 } else { 1 }
    }
}

/// couch_file:remove_block_prefixes/2 — `want` payload bytes expected.
fn strip_block_markers(block_offset: u64, raw: &[u8], want: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(want);
    let mut off = block_offset % BLOCK;
    let mut i = 0usize;
    while i < raw.len() && out.len() < want {
        if off == 0 {
            i += 1; // skip marker byte
            off = 1;
            continue;
        }
        let avail = (BLOCK - off) as usize;
        let take = avail.min(raw.len() - i).min(want - out.len());
        out.extend_from_slice(&raw[i..i + take]);
        i += take;
        off = (off + take as u64) % BLOCK;
    }
    out
}

/// couch_file:make_blocks/2 — inject a 0 marker byte at each block boundary.
fn add_block_markers(block_offset: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + data.len() / BLOCK as usize + 2);
    let mut off = block_offset % BLOCK;
    let mut i = 0usize;
    while i < data.len() {
        if off == 0 {
            out.push(0);
            off = 1;
            continue;
        }
        let take = ((BLOCK - off) as usize).min(data.len() - i);
        out.extend_from_slice(&data[i..i + take]);
        i += take;
        off = (off + take as u64) % BLOCK;
    }
    out
}

fn verify_checksum(data: &[u8], ck: &[u8]) -> std::result::Result<(), ()> {
    if ck.is_empty() {
        return Ok(());
    }
    // XXH3-128 canonical form (big-endian), the default since 3.4.
    let xxh = xxhash_rust::xxh3::xxh3_128(data).to_be_bytes();
    if ck == xxh {
        return Ok(());
    }
    let md5 = Md5::digest(data);
    if ck == md5.as_slice() {
        return Ok(());
    }
    // Defensive: some builds may emit the raw little-endian hash struct.
    let xxh_le = xxhash_rust::xxh3::xxh3_128(data).to_le_bytes();
    if ck == xxh_le {
        return Ok(());
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_read_len_ports() {
        // Values mirror the Erlang function's behaviour.
        assert_eq!(total_read_len(1, 10), 10);
        assert_eq!(total_read_len(0, 10), 11);
        assert_eq!(total_read_len(4095, 1), 1);
        assert_eq!(total_read_len(4095, 2), 3);
        assert_eq!(total_read_len(1, 4095), 4095);
        assert_eq!(total_read_len(1, 4096), 4097);
    }

    #[test]
    fn markers_roundtrip() {
        for offset in [0u64, 1, 5, 4000, 4095] {
            for len in [1usize, 10, 4091, 4096, 5000, 10000] {
                let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8 + 1).collect();
                let blocks = add_block_markers(offset, &data);
                assert_eq!(blocks.len(), total_read_len(offset, len), "off={offset} len={len}");
                let back = strip_block_markers(offset, &blocks, len);
                assert_eq!(back, data, "off={offset} len={len}");
            }
        }
    }

    #[test]
    fn file_roundtrip() {
        let dir = std::env::temp_dir().join(format!("couch-store-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.couch");
        let _ = std::fs::remove_file(&path);
        {
            let mut f = CouchFile::create(&path).unwrap();
            let hdr = Term::Tuple(vec![Term::atom("db_header"), Term::Int(8)]);
            f.write_header(&hdr).unwrap();
            let big = vec![7u8; 10_000];
            let (p1, _) = f.append_chunk(&big).unwrap();
            let (p2, _) = f.append_chunk_checksummed(b"hello world").unwrap();
            let (p3, _) = f
                .append_term(&Term::List(vec![Term::Int(1), Term::Int(2)]))
                .unwrap();
            let hdr2 = Term::Tuple(vec![Term::atom("db_header"), Term::Int(9)]);
            f.write_header(&hdr2).unwrap();

            let f2 = CouchFile::open_read(&path).unwrap();
            assert_eq!(f2.read_chunk(p1).unwrap(), big);
            assert_eq!(f2.read_chunk(p2).unwrap(), b"hello world");
            let t = f2.read_term(p3).unwrap();
            assert!(t == Term::List(vec![Term::Int(1), Term::Int(2)]));
            let h = f2.read_header().unwrap();
            assert!(h == hdr2);
        }
        let _ = std::fs::remove_file(&path);
    }
}
