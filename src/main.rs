mod config;
mod meta;
mod reader;
mod scanner;
mod smb;
mod state;

use std::path::PathBuf;
use time::OffsetDateTime;
use tracing::{error, info, warn};

fn main() -> anyhow::Result<()> {
    let cfg = config::Config::from_env();

    tracing_subscriber::fmt()
        .with_env_filter(cfg.log_level.as_str())
        .init();

    if let Some(parent) = std::path::Path::new(&cfg.db_path).parent() {
        if parent != std::path::Path::new("") {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    smb::ensure_connection(&cfg.share_root)?;

    let db = state::Db::open(&cfg.db_path)?;

    loop {
        let paths = match scanner::scan_csv_paths(&cfg.share_root, &cfg.target_glob) {
            Ok(v) => v,
            Err(e) => { warn!("scan error: {}", e); Vec::<PathBuf>::new() }
        };

        for p in paths {
            let path_str = p.to_string_lossy().to_string();
            let m = match meta::read_meta(&p) {
                Ok(m) => m,
                Err(e) => {
                    warn!("meta error {}: {}", path_str, e);
                    continue;
                }
            };

            let st = db.load_file_state(&path_str)?;

            let changed = match st {
                None => true,
                Some(ref s) => s.mtime != m.mtime || s.size != m.size || s.hash != m.hash,
            };

            if !changed {
                continue;
            }

            let (start_index, anchor_raw_owned, anchor_hash_owned) = match st {
                None => (1i64, None, None),
                Some(s) => {
                    let idx = s.last_line_index.unwrap_or(1);
                    let start = if idx > 10 { idx - 10 } else { 1 };
                    (start, s.last_line, s.last_line_hash)
                }
            };
            let anchor_raw = anchor_raw_owned.as_deref();
            let anchor_hash = anchor_hash_owned.as_deref();
            let (recs, rr) =
                match reader::read_incremental(&p, start_index, anchor_raw, anchor_hash) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("read error {}: {}", path_str, e);
                        continue;
                    }
                };
            let mut inserted = 0usize;
            for r in recs {
                let line_hash = blake3::hash(r.raw.as_bytes()).to_hex().to_string();
                let rec = state::Record {
                    sn: r.sn,
                    datetime: r.datetime,
                    result: r.result,
                    source_path: path_str.clone(),
                    row_index: r.row_index,
                    line_hash,
                };
                if let Err(e) = db.insert_record(&rec) {
                    warn!("insert record error {}: {}", path_str, e);
                } else {
                    inserted += 1;
                }
            }
            let now = OffsetDateTime::now_utc().unix_timestamp();
            let fs = state::FileState {
                path: path_str.clone(),
                mtime: m.mtime,
                size: m.size,
                hash: m.hash.clone(),
                lines_read: rr.lines_read,
                bytes_read: rr.bytes_read,
                last_line: rr.last_line.clone(),
                last_line_hash: rr.last_line_hash.clone(),
                last_line_index: rr.last_line_index,
                updated_at: now,
            };
            if let Err(e) = db.upsert_file_state(&fs) {
                error!("upsert state error {}: {}", path_str, e);
            } else { info!("{} parsed {} inserted {}", path_str, rr.new_records, inserted); }
        }
        std::thread::sleep(std::time::Duration::from_secs(cfg.poll_interval_secs));
    }
}
