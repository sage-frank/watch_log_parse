use walkdir::WalkDir;
use std::path::{PathBuf};

pub fn scan_csv_paths(root: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in WalkDir::new(root).follow_links(true).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();
        if p.is_file() {
            if let Some(ext) = p.extension() {
                if ext.to_string_lossy().to_ascii_lowercase() == "csv" {
                    out.push(p.to_path_buf());
                }
            }
        }
    }
    Ok(out)
}
