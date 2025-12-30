use walkdir::WalkDir;
use std::path::{PathBuf};
use globset::{Glob, GlobSetBuilder};

pub fn scan_csv_paths(root: &str, pattern: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut builder = GlobSetBuilder::new();
    builder.add(Glob::new(pattern)?);
    let set = builder.build()?;
    let mut out = Vec::new();
    for entry in WalkDir::new(root).follow_links(true).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();
        if p.is_file() && set.is_match(p) {
            out.push(p.to_path_buf());
        }
    }
    Ok(out)
}
