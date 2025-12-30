use std::fs;
use std::io::Cursor;
use std::path::Path;
use anyhow::Context;
use csv::{ReaderBuilder, StringRecord};
use encoding_rs::{GBK, UTF_8};
use blake3::Hasher;

pub struct ReadResult {
    pub new_records: usize,
    pub last_line: Option<String>,
    pub last_line_hash: Option<String>,
    pub last_line_index: Option<i64>,
    pub bytes_read: i64,
    pub lines_read: i64,
}

fn detect_and_decode(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        let s = String::from_utf8_lossy(&bytes[3..]).to_string();
        return s;
    }
    let (cow, _, had_errors) = UTF_8.decode(bytes);
    if !had_errors {
        return cow.into_owned();
    }
    let (cow, _, _) = GBK.decode(bytes);
    cow.into_owned()
}

pub struct Columns {
    pub sn_idx: usize,
    pub dt_idx: usize,
    pub rs_idx: usize,
}

fn header_index(h: &StringRecord) -> anyhow::Result<Columns> {
    let mut sn_idx = None;
    let mut dt_idx = None;
    let mut rs_idx = None;
    for (i, col) in h.iter().enumerate() {
        let k = col.trim().to_ascii_lowercase();
        if k == "sn" { sn_idx = Some(i); }
        if k == "datetime" { dt_idx = Some(i); }
        if k == "result" { rs_idx = Some(i); }
    }
    let c = Columns {
        sn_idx: sn_idx.context("missing SN column")?,
        dt_idx: dt_idx.context("missing Datetime column")?,
        rs_idx: rs_idx.context("missing Result column")?,
    };
    Ok(c)
}

pub struct CsvRecord {
    pub sn: String,
    pub datetime: String,
    pub result: String,
    pub raw: String,
    pub row_index: i64,
}

fn record_to_raw(r: &StringRecord) -> String {
    let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);
    wtr.write_record(r.iter()).unwrap();
    let vec = wtr.into_inner().unwrap();
    String::from_utf8(vec).unwrap().trim_end_matches('\n').to_string()
}

pub fn read_incremental(path: &Path, start_index: i64, anchor_raw: Option<&str>, anchor_hash: Option<&str>) 
    -> anyhow::Result<(Vec<CsvRecord>, ReadResult)> 
{
    let bytes = fs::read(path)?;
    let decoded = detect_and_decode(&bytes);
    let cur = Cursor::new(decoded.as_bytes());
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(cur);
    let headers = rdr.headers()?.clone();
    let cols = header_index(&headers)?;
    let mut records = Vec::new();
    let mut idx: i64 = 0;
    let mut anchor_reached = anchor_raw.is_none() && anchor_hash.is_none();
    let mut last_raw = None;
    let mut last_hash = None;
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    let file_bytes = bytes.len() as i64;
    for rec in rdr.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(_) => break,
        };
        idx += 1;
        if idx < start_index { continue; }
        let raw = record_to_raw(&rec);
        let raw_hash = blake3::hash(raw.as_bytes()).to_hex().to_string();
        if !anchor_reached {
            if let Some(a) = anchor_raw {
                if a == raw { anchor_reached = true; continue; }
            }
            if let Some(h) = anchor_hash.as_ref() {
                if *h == raw_hash { anchor_reached = true; continue; }
            }
            continue;
        }
        let sn = rec.get(cols.sn_idx).unwrap_or("").trim().to_string();
        let dt = rec.get(cols.dt_idx).unwrap_or("").trim().to_string();
        let rs = rec.get(cols.rs_idx).unwrap_or("").trim().to_string();
        records.push(CsvRecord { sn, datetime: dt, result: rs, raw, row_index: idx });
        last_raw = Some(records.last().unwrap().raw.clone());
        last_hash = Some(raw_hash);
    }
    let read_res = ReadResult {
        new_records: records.len(),
        last_line: last_raw,
        last_line_hash: last_hash,
        last_line_index: if idx > 0 { Some(idx) } else { None },
        bytes_read: file_bytes,
        lines_read: idx,
    };
    Ok((records, read_res))
}
