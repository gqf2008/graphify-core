//! Per-file extraction cache — skip unchanged files on re-run.
//!
//! Ported from `graphify/cache.py`.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::schema::{Edge, Extraction, Node};

/// Cache entry stored on disk — only the serialisable subset of an extraction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CacheEntry {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    #[serde(default)]
    hyperedges: Vec<serde_json::Value>,
}

impl From<Extraction> for CacheEntry {
    fn from(e: Extraction) -> Self {
        Self {
            nodes: e.nodes,
            edges: e.edges,
            hyperedges: e.hyperedges,
        }
    }
}

impl From<CacheEntry> for Extraction {
    fn from(e: CacheEntry) -> Self {
        Self {
            nodes: e.nodes,
            edges: e.edges,
            hyperedges: e.hyperedges,
            ..Default::default()
        }
    }
}

/// Strip YAML frontmatter from Markdown content, returning only the body.
fn body_content(content: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(content);
    if let Some(after_prefix) = text.strip_prefix("---")
        && let Some(end) = after_prefix.find("\n---")
    {
        let after = &after_prefix[end + 4..];
        return after.strip_prefix('\n').unwrap_or(after).as_bytes().to_vec();
    }
    content.to_vec()
}

/// SHA256 of file contents + path relative to root.
///
/// Using a relative path (not absolute) makes cache entries portable across
/// machines and checkout directories. Falls back to the resolved absolute path
/// if the file is outside root.
///
/// For Markdown files (`.md`), only the body below the YAML frontmatter is
/// hashed, so metadata-only changes do not invalidate the cache.
pub fn file_hash(path: &Path, root: &Path) -> Result<String> {
    if !path.is_file() {
        anyhow::bail!("file_hash requires a file, got: {}", path.display());
    }
    let raw = fs::read(path)
        .with_context(|| format!("cannot read file for hashing: {}", path.display()))?;
    let content = if path.extension().is_some_and(|e| e.eq_ignore_ascii_case("md")) {
        body_content(&raw)
    } else {
        raw
    };

    let mut hasher = Sha256::new();
    hasher.update(&content);
    hasher.update(b"\x00");

    let rel = path
        .canonicalize()
        .ok()
        .and_then(|canon| {
            let root_canon = root.canonicalize().ok()?;
            canon.strip_prefix(&root_canon).ok().map(|p| p.to_path_buf())
        })
        .unwrap_or_else(|| path.canonicalize().unwrap_or_else(|_| path.to_path_buf()));
    hasher.update(rel.to_string_lossy().as_bytes());

    Ok(hex::encode(hasher.finalize()))
}

/// Returns `graphify-out/cache/` — creates it if needed.
pub fn cache_dir(root: &Path) -> PathBuf {
    let d = root.canonicalize().unwrap_or_else(|_| root.to_path_buf()).join("graphify-out").join("cache");
    let _ = fs::create_dir_all(&d);
    d
}

/// Return cached extraction for this file if hash matches, else `None`.
///
/// Cache key: SHA256 of file contents.
/// Cache value: stored as `graphify-out/cache/{hash}.json`.
pub fn load_cached(path: &Path, root: &Path) -> Option<Extraction> {
    let h = file_hash(path, root).ok()?;
    let entry = cache_dir(root).join(format!("{h}.json"));
    let text = fs::read_to_string(&entry).ok()?;
    let entry: CacheEntry = serde_json::from_str(&text).ok()?;
    Some(entry.into())
}

/// Save extraction result for this file.
///
/// Stores as `graphify-out/cache/{hash}.json` where hash = SHA256 of current
/// file contents. No-ops if `path` is not a regular file.
pub fn save_cached(path: &Path, result: &Extraction, root: &Path) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let h = file_hash(path, root)?;
    let entry = cache_dir(root).join(format!("{h}.json"));
    let tmp = entry.with_extension("tmp");
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create cache dir: {}", parent.display()))?;
    }

    let entry_data = CacheEntry {
        nodes: result.nodes.clone(),
        edges: result.edges.clone(),
        hyperedges: result.hyperedges.clone(),
    };
    let json = serde_json::to_string(&entry_data)
        .with_context(|| "cannot serialise cache entry")?;

    fs::write(&tmp, json)
        .with_context(|| format!("cannot write cache tmp file: {}", tmp.display()))?;
    fs::rename(&tmp, &entry)
        .with_context(|| format!("cannot rename cache file: {} -> {}", tmp.display(), entry.display()))?;
    Ok(())
}

/// Delete all `graphify-out/cache/*.json` files.
pub fn clear_cache(root: &Path) -> Result<()> {
    let d = cache_dir(root);
    for entry in fs::read_dir(&d).with_context(|| format!("cannot read cache dir: {}", d.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "json") {
            let _ = fs::remove_file(&path);
        }
    }
    Ok(())
}

/// Check semantic extraction cache for a list of absolute file paths.
///
/// Returns `(cached_extractions, uncached_paths)`.
/// Uncached files need LLM extraction; cached files are merged directly.
pub fn check_semantic_cache(paths: &[PathBuf], root: &Path) -> (Vec<Extraction>, Vec<PathBuf>) {
    let mut cached = Vec::new();
    let mut uncached = Vec::new();

    for path in paths {
        match load_cached(path, root) {
            Some(extraction) => cached.push(extraction),
            None => uncached.push(path.clone()),
        }
    }

    (cached, uncached)
}

/// Save semantic extraction results to cache, keyed by `source_file`.
///
/// Groups nodes and edges by `source_file`, then saves one cache entry per
/// file. Returns the number of files cached.
pub fn save_semantic_cache(extraction: &Extraction, root: &Path) -> Result<usize> {
    let mut by_file: HashMap<PathBuf, Extraction> = HashMap::new();

    for node in &extraction.nodes {
        let src = PathBuf::from(&node.source_file);
        by_file
            .entry(src.clone())
            .or_default()
            .nodes
            .push(node.clone());
    }
    for edge in &extraction.edges {
        let src = PathBuf::from(&edge.source_file);
        by_file
            .entry(src.clone())
            .or_default()
            .edges
            .push(edge.clone());
    }
    for hyperedge in &extraction.hyperedges {
        if let Some(src) = hyperedge
            .get("source_file")
            .and_then(|v| v.as_str())
        {
            by_file
                .entry(PathBuf::from(src))
                .or_default()
                .hyperedges
                .push(hyperedge.clone());
        }
    }

    let mut saved = 0;
    for (fpath, per_file) in by_file {
        let abs = if fpath.is_absolute() {
            fpath
        } else {
            root.join(fpath)
        };
        if abs.is_file() {
            save_cached(&abs, &per_file, root)?;
            saved += 1;
        }
    }
    Ok(saved)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir()
            .join(format!("graphify-cache-test-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_body_content_strips_frontmatter() {
        let md = b"---\ntitle: Foo\n---\n# Hello\n";
        let body = body_content(md);
        assert_eq!(String::from_utf8_lossy(&body), "# Hello\n");
    }

    #[test]
    fn test_body_content_no_frontmatter() {
        let md = b"# Hello\n";
        let body = body_content(md);
        assert_eq!(String::from_utf8_lossy(&body), "# Hello\n");
    }

    #[test]
    fn test_file_hash_is_stable() {
        let root = tmp_dir();
        let f = root.join("foo.rs");
        fs::write(&f, b"fn main() {}").unwrap();

        let h1 = file_hash(&f, &root).unwrap();
        let h2 = file_hash(&f, &root).unwrap();
        assert_eq!(h1, h2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_file_hash_changes_with_content() {
        let root = tmp_dir();
        let f = root.join("foo.rs");
        fs::write(&f, b"fn main() {}").unwrap();
        let h1 = file_hash(&f, &root).unwrap();

        fs::write(&f, b"fn main() { 1 }").unwrap();
        let h2 = file_hash(&f, &root).unwrap();
        assert_ne!(h1, h2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_file_hash_md_ignores_frontmatter() {
        let root = tmp_dir();
        let f = root.join("doc.md");
        fs::write(&f, b"---\ntags: [a]\n---\n# Body\n").unwrap();
        let h1 = file_hash(&f, &root).unwrap();

        fs::write(&f, b"---\ntags: [b]\n---\n# Body\n").unwrap();
        let h2 = file_hash(&f, &root).unwrap();
        assert_eq!(h1, h2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_save_and_load_cached() {
        let root = tmp_dir();
        let f = root.join("src.rs");
        fs::write(&f, b"fn main() {}").unwrap();

        let extraction = Extraction {
            nodes: vec![Node {
                id: "main".into(),
                label: "main()".into(),
                source_file: f.to_string_lossy().to_string(),
                ..Default::default()
            }],
            edges: vec![],
            ..Default::default()
        };

        save_cached(&f, &extraction, &root).unwrap();
        let loaded = load_cached(&f, &root).unwrap();
        assert_eq!(loaded.nodes.len(), 1);
        assert_eq!(loaded.nodes[0].id, "main");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_check_semantic_cache_splits_cached_and_uncached() {
        let root = tmp_dir();
        let f1 = root.join("a.rs");
        let f2 = root.join("b.rs");
        fs::write(&f1, b"fn a() {}").unwrap();
        fs::write(&f2, b"fn b() {}").unwrap();

        let ex = Extraction {
            nodes: vec![Node {
                id: "a".into(),
                label: "a()".into(),
                source_file: f1.to_string_lossy().to_string(),
                ..Default::default()
            }],
            edges: vec![],
            ..Default::default()
        };
        save_cached(&f1, &ex, &root).unwrap();

        let (cached, uncached) = check_semantic_cache(&[f1.clone(), f2.clone()], &root);
        assert_eq!(cached.len(), 1);
        assert_eq!(uncached.len(), 1);
        assert_eq!(uncached[0], f2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_clear_cache_removes_json_files() {
        let root = tmp_dir();
        let f = root.join("x.rs");
        fs::write(&f, b"fn x() {}").unwrap();

        let ex = Extraction {
            nodes: vec![Node {
                id: "x".into(),
                label: "x()".into(),
                source_file: f.to_string_lossy().to_string(),
                ..Default::default()
            }],
            edges: vec![],
            ..Default::default()
        };
        save_cached(&f, &ex, &root).unwrap();
        let cache_d = cache_dir(&root);
        let before = fs::read_dir(&cache_d).unwrap().filter_map(|e| e.ok()).count();
        assert!(before >= 1);

        clear_cache(&root).unwrap();
        let after = fs::read_dir(&cache_d).unwrap().filter_map(|e| e.ok()).count();
        assert_eq!(after, 0);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_save_semantic_cache_groups_by_source_file() {
        let root = tmp_dir();
        let f1 = root.join("a.rs");
        let f2 = root.join("b.rs");
        fs::write(&f1, b"fn a() {}").unwrap();
        fs::write(&f2, b"fn b() {}").unwrap();

        let extraction = Extraction {
            nodes: vec![
                Node {
                    id: "a".into(),
                    label: "a()".into(),
                    source_file: f1.to_string_lossy().to_string(),
                    ..Default::default()
                },
                Node {
                    id: "b".into(),
                    label: "b()".into(),
                    source_file: f2.to_string_lossy().to_string(),
                    ..Default::default()
                },
            ],
            edges: vec![Edge {
                source: "a".into(),
                target: "b".into(),
                relation: "uses".into(),
                source_file: f1.to_string_lossy().to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let saved = save_semantic_cache(&extraction, &root).unwrap();
        assert_eq!(saved, 2);

        let loaded1 = load_cached(&f1, &root).unwrap();
        assert_eq!(loaded1.nodes.len(), 1);
        assert_eq!(loaded1.edges.len(), 1);

        let loaded2 = load_cached(&f2, &root).unwrap();
        assert_eq!(loaded2.nodes.len(), 1);
        assert_eq!(loaded2.edges.len(), 0);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_load_cached_returns_none_for_missing() {
        let root = tmp_dir();
        let f = root.join("missing.rs");
        fs::write(&f, b"fn main() {}").unwrap();
        assert!(load_cached(&f, &root).is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_file_hash_differs_by_path() {
        let root = tmp_dir();
        let f1 = root.join("a.rs");
        let f2 = root.join("b.rs");
        fs::write(&f1, b"same content").unwrap();
        fs::write(&f2, b"same content").unwrap();

        let h1 = file_hash(&f1, &root).unwrap();
        let h2 = file_hash(&f2, &root).unwrap();
        assert_ne!(h1, h2);
        let _ = fs::remove_dir_all(&root);
    }
}
