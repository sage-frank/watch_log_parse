use std::fs::File;
use std::io::{Read};
use std::path::Path;
use blake3::Hasher;

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub mtime: i64,
    pub size: i64,
    pub hash: String,
}

pub fn read_meta(path: &Path) -> anyhow::Result<FileMeta> {
    let md = std::fs::metadata(path)?;
    let mtime = md.modified()?.elapsed().ok().map(|e| {
        let now = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        let modified = now - e;
        modified.as_secs() as i64
    }).unwrap_or_else(|| {
        let secs = md.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH)
            .duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
        secs as i64
    });
    let size = md.len() as i64;
    let mut f = File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    let hash = hasher.finalize().to_hex().to_string();
    Ok(FileMeta { mtime, size, hash })
}
