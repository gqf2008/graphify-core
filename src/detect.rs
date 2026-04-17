/// File discovery, type classification, and corpus health checks.
///
/// Ported from `graphify/detect.py`.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

// ── FileType ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileType {
    Code,
    Document,
    Paper,
    Image,
    Video,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileType::Code => "code",
            FileType::Document => "document",
            FileType::Paper => "paper",
            FileType::Image => "image",
            FileType::Video => "video",
        }
    }
}

// ── Extension sets ───────────────────────────────────────────────────────────

static CODE_EXTENSIONS: &[&str] = &[
    ".py", ".ts", ".js", ".jsx", ".tsx", ".go", ".rs", ".java", ".cpp", ".cc", ".cxx", ".c", ".h",
    ".hpp", ".rb", ".swift", ".kt", ".kts", ".cs", ".scala", ".php", ".lua", ".toc", ".zig",
    ".ps1", ".ex", ".exs", ".m", ".mm", ".jl", ".vue", ".svelte", ".dart", ".v", ".sv",
];

static DOC_EXTENSIONS: &[&str] = &[".md", ".txt", ".rst"];
static PAPER_EXTENSIONS: &[&str] = &[".pdf"];
static IMAGE_EXTENSIONS: &[&str] = &[".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"];
static OFFICE_EXTENSIONS: &[&str] = &[".docx", ".xlsx"];
static VIDEO_EXTENSIONS: &[&str] = &[
    ".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v", ".mp3", ".wav", ".m4a", ".ogg",
];

static SKIP_DIRS: &[&str] = &[
    "venv",
    ".venv",
    "env",
    ".env",
    "node_modules",
    "__pycache__",
    ".git",
    "dist",
    "build",
    "target",
    "out",
    "site-packages",
    "lib64",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".eggs",
];

static SKIP_FILES: &[&str] = &[
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "Cargo.lock",
    "poetry.lock",
    "Gemfile.lock",
    "composer.lock",
    "go.sum",
    "go.work.sum",
];

// ── Sensitive file patterns ──────────────────────────────────────────────────

static SENSITIVE_NAME_PARTS: &[&str] = &[
    ".env",
    ".envrc",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".cert",
    ".crt",
    ".der",
    ".p8",
    "credential",
    "secret",
    "passwd",
    "password",
    "token",
    "private_key",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    ".netrc",
    ".pgpass",
    ".htpasswd",
    "aws_credentials",
    "gcloud_credentials",
    "service.account",
];

fn is_sensitive(path: &Path) -> bool {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default();
    let name_lower = name.to_lowercase();
    let full = path.to_string_lossy().to_lowercase();

    SENSITIVE_NAME_PARTS
        .iter()
        .any(|pat| name_lower.contains(pat) || full.contains(pat))
}

fn looks_like_paper(path: &Path) -> bool {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(_) => return false,
    };
    use regex::Regex;
    use std::sync::LazyLock;

    let first_3000: String = text.chars().take(3000).collect();

    static PAPER_SIGNALS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        vec![
            Regex::new(r"(?i)\barxiv\b").unwrap(),
            Regex::new(r"(?i)\bdoi\s*:").unwrap(),
            Regex::new(r"(?i)\babstract\b").unwrap(),
            Regex::new(r"(?i)\bproceedings\b").unwrap(),
            Regex::new(r"(?i)\bjournal\b").unwrap(),
            Regex::new(r"(?i)\bpreprint\b").unwrap(),
            Regex::new(r"\\cite\{").unwrap(),
            Regex::new(r"\[\d+\]").unwrap(),
            Regex::new(r"\[\n\d+\n\]").unwrap(),
            Regex::new(r"(?i)eq\.\s*\d+|equation\s+\d+").unwrap(),
            Regex::new(r"\d{4}\.\d{4,5}").unwrap(),
            Regex::new(r"(?i)\bwe propose\b").unwrap(),
            Regex::new(r"(?i)\bliterature\b").unwrap(),
        ]
    });

    let hits = PAPER_SIGNALS
        .iter()
        .filter(|pattern| pattern.is_match(&first_3000))
        .count();

    hits >= 3
}

// ── Asset dir markers ────────────────────────────────────────────────────────

static ASSET_DIR_MARKERS: &[&str] = &[
    ".imageset",
    ".xcassets",
    ".appiconset",
    ".colorset",
    ".launchimage",
];

// ── File classification ──────────────────────────────────────────────────────

/// Classify a file by its extension and content heuristics.
/// Returns `None` for files that should be ignored (e.g. PDFs in asset catalogs).
pub fn classify_file(path: &Path) -> Option<FileType> {
    let name_lower = path
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();

    // Compound extensions
    if name_lower.ends_with(".blade.php") {
        return Some(FileType::Code);
    }

    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy().to_lowercase()))
        .unwrap_or_default();

    if CODE_EXTENSIONS.contains(&ext.as_str()) {
        return Some(FileType::Code);
    }

    if PAPER_EXTENSIONS.contains(&ext.as_str()) {
        // PDFs inside Xcode asset catalogs are vector icons, not papers
        if path.components().any(|c| {
            let part = c.as_os_str().to_string_lossy().to_lowercase();
            ASSET_DIR_MARKERS.iter().any(|m| part.ends_with(m))
        }) {
            return None;
        }
        return Some(FileType::Paper);
    }

    if IMAGE_EXTENSIONS.contains(&ext.as_str()) {
        return Some(FileType::Image);
    }

    if DOC_EXTENSIONS.contains(&ext.as_str()) {
        if looks_like_paper(path) {
            return Some(FileType::Paper);
        }
        return Some(FileType::Document);
    }

    if OFFICE_EXTENSIONS.contains(&ext.as_str()) {
        return Some(FileType::Document);
    }

    if VIDEO_EXTENSIONS.contains(&ext.as_str()) {
        return Some(FileType::Video);
    }

    None
}

// ── Word count ───────────────────────────────────────────────────────────────

/// Count whitespace-separated words in a text file.
pub fn count_words(path: &Path) -> usize {
    match fs::read_to_string(path) {
        Ok(text) => text.split_whitespace().count(),
        Err(_) => 0,
    }
}

// ── Noise directory detection ────────────────────────────────────────────────

fn is_noise_dir(part: &str) -> bool {
    if SKIP_DIRS.contains(&part) {
        return true;
    }
    if part.ends_with("_venv") || part.ends_with("_env") {
        return true;
    }
    if part.ends_with(".egg-info") {
        return true;
    }
    false
}

// ── .graphifyignore ──────────────────────────────────────────────────────────

/// A pattern loaded from a `.graphifyignore` file, paired with the directory
/// where that file was found.
#[derive(Debug, Clone)]
struct IgnorePattern {
    anchor: PathBuf,
    pattern: String,
}

/// Read `.graphifyignore` from root and ancestor directories up to a `.git` boundary.
fn load_graphifyignore(root: &Path) -> Vec<IgnorePattern> {
    let mut patterns = Vec::new();
    let mut current = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    loop {
        let ignore_file = current.join(".graphifyignore");
        if ignore_file.exists() {
            if let Ok(content) = fs::read_to_string(&ignore_file) {
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    patterns.push(IgnorePattern {
                        anchor: current.clone(),
                        pattern: line.to_string(),
                    });
                }
            }
        }

        if current.join(".git").exists() {
            break;
        }

        let parent = current.parent().map(|p| p.to_path_buf());
        match parent {
            Some(p) if p != current => current = p,
            _ => break,
        }
    }

    patterns
}

/// Check if a path matches any `.graphifyignore` pattern.
fn is_ignored(path: &Path, root: &Path, patterns: &[IgnorePattern]) -> bool {
    if patterns.is_empty() {
        return false;
    }

    let filename = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    for ip in patterns {
        let pat = ip.pattern.trim_matches('/');
        if pat.is_empty() {
            continue;
        }

        // Try relative to scan root
        if let Ok(rel) = path.strip_prefix(root) {
            let rel_str = rel.to_string_lossy();
            if matches_path(&rel_str, &filename, pat) {
                return true;
            }
        }

        // Try relative to anchor dir
        if ip.anchor != root {
            if let Ok(rel) = path.strip_prefix(&ip.anchor) {
                let rel_str = rel.to_string_lossy();
                if matches_path(&rel_str, &filename, pat) {
                    return true;
                }
            }
        }
    }

    false
}

/// Match a pattern against a relative path string and filename using glob-style matching.
fn matches_path(rel: &str, filename: &str, pattern: &str) -> bool {
    // Use globset for fnmatch-style matching
    use globset::GlobBuilder;

    let glob = match GlobBuilder::new(pattern).empty_alternates(true).build() {
        Ok(g) => g,
        Err(_) => return false,
    };
    let matcher = glob.compile_matcher();

    // Match against full relative path
    if matcher.is_match(rel) {
        return true;
    }

    // Match against filename only
    if matcher.is_match(filename) {
        return true;
    }

    // Match against each path component and prefix
    let parts: Vec<&str> = rel.split('/').collect();
    for i in 0..parts.len() {
        let prefix = parts[..=i].join("/");
        if matcher.is_match(&prefix) || matcher.is_match(parts[i]) {
            return true;
        }
    }

    false
}

// ── Content hash for caching ─────────────────────────────────────────────────

/// Compute SHA-256 hex digest of file content.
pub fn content_hash(path: &Path) -> Option<String> {
    let data = fs::read(path).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    Some(hex::encode(hasher.finalize()))
}

fn display_path_for_output(path: &Path, canonical_root: &Path, requested_root: &Path) -> String {
    let rel = path.strip_prefix(canonical_root).unwrap_or(path);
    let display = if requested_root == Path::new(".") {
        rel.to_path_buf()
    } else {
        requested_root.join(rel)
    };
    display.to_string_lossy().to_string()
}

fn normalize_manifest_key(path: &str) -> String {
    Path::new(path)
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(path))
        .to_string_lossy()
        .to_string()
}

// ── Detect result ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct DetectResult {
    pub files: std::collections::HashMap<String, Vec<String>>,
    pub total_files: usize,
    pub total_words: usize,
    pub needs_graph: bool,
    pub warning: Option<String>,
    pub skipped_sensitive: Vec<String>,
    pub graphifyignore_patterns: usize,
}

const CORPUS_WARN_THRESHOLD: usize = 50_000;
const CORPUS_UPPER_THRESHOLD: usize = 500_000;
const FILE_COUNT_UPPER: usize = 200;

/// Discover files in a directory tree, classify them, and return a structured result.
///
/// Ported from `graphify/detect.py::detect()`.
pub fn detect(root: &Path, follow_symlinks: bool) -> Result<DetectResult> {
    let requested_root = root.to_path_buf();
    let canonical_root = root
        .canonicalize()
        .with_context(|| format!("Cannot canonicalize root path: {}", root.display()))?;

    let memory_dir = canonical_root.join("graphify-out").join("memory");
    let converted_dir = canonical_root.join("graphify-out").join("converted");
    let ignore_patterns = load_graphifyignore(&canonical_root);

    // Build scan paths
    let mut scan_paths = vec![canonical_root.clone()];
    if memory_dir.exists() {
        scan_paths.push(memory_dir.clone());
    }

    let mut all_files: Vec<PathBuf> = Vec::new();
    let mut skipped_sensitive: Vec<String> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    for scan_root in &scan_paths {
        let in_memory_tree = memory_dir.exists() && scan_root.starts_with(&memory_dir);

        let root_for_filter = canonical_root.clone();
        let ignore_patterns_for_filter = ignore_patterns.clone();
        let mut builder = ignore::WalkBuilder::new(scan_root);
        builder
            .follow_links(follow_symlinks)
            .hidden(false)
            .standard_filters(false)
            .max_depth(None);
        if !in_memory_tree {
            builder.filter_entry(move |entry| {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy();
                if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
                    && (name.starts_with('.') || is_noise_dir(&name))
                {
                    return false;
                }
                !is_ignored(path, &root_for_filter, &ignore_patterns_for_filter)
            });
        }
        let walker = builder.build();

        for entry in walker {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let path = entry.path().to_path_buf();

            // Deduplicate
            if !seen.insert(path.clone()) {
                continue;
            }

            // Handle directories
            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                if follow_symlinks && path.is_symlink() {
                    // Cycle detection for symlinks
                    if let Ok(real) = path.canonicalize() {
                        if let Some(parent_real) = path.parent().and_then(|p| p.canonicalize().ok())
                        {
                            if parent_real == real || parent_real.starts_with(&real) {
                                continue;
                            }
                        }
                    }
                }

                let dp = &path;
                if !in_memory_tree {
                    if let Some(name) = dp.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with('.') || is_noise_dir(name) {
                            continue;
                        }
                        if is_ignored(dp, &canonical_root, &ignore_patterns) {
                            continue;
                        }
                    }
                }
                continue;
            }

            // Handle files
            let fname = path
                .file_name()
                .map(|n| n.to_string_lossy())
                .unwrap_or_default();

            // Skip known lock/summary files
            if SKIP_FILES.iter().any(|s| fname == *s) {
                continue;
            }

            // Skip hidden files
            if fname.starts_with('.') {
                if is_sensitive(&path) {
                    skipped_sensitive.push(display_path_for_output(
                        &path,
                        &canonical_root,
                        &requested_root,
                    ));
                }
                continue;
            }

            if !in_memory_tree {
                if is_ignored(&path, &canonical_root, &ignore_patterns) {
                    continue;
                }
            }

            // Skip files inside our own converted/ dir
            if path.starts_with(&converted_dir) {
                continue;
            }

            // Sensitive file check
            if is_sensitive(&path) {
                skipped_sensitive.push(display_path_for_output(
                    &path,
                    &canonical_root,
                    &requested_root,
                ));
                continue;
            }

            all_files.push(path);
        }
    }

    // Classify files
    let mut files: std::collections::HashMap<String, Vec<String>> = [
        ("code".to_string(), Vec::new()),
        ("document".to_string(), Vec::new()),
        ("paper".to_string(), Vec::new()),
        ("image".to_string(), Vec::new()),
        ("video".to_string(), Vec::new()),
    ]
    .into_iter()
    .collect();

    let mut total_words: usize = 0;
    for p in &all_files {
        let ftype = match classify_file(p) {
            Some(ft) => ft,
            None => continue,
        };

        let key = ftype.as_str().to_string();

        // Office files — convert to markdown sidecar
        if ftype == FileType::Document {
            if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                if OFFICE_EXTENSIONS.contains(&format!(".{}", ext.to_lowercase()).as_str()) {
                    skipped_sensitive.push(format!(
                        "{} [office conversion — pip install graphifyy[office]]",
                        display_path_for_output(p, &canonical_root, &requested_root)
                    ));
                    continue;
                }
            }
        }

        files.entry(key).or_default().push(display_path_for_output(
            p,
            &canonical_root,
            &requested_root,
        ));

        if ftype != FileType::Video {
            total_words += count_words(p);
        }
    }

    let total_files: usize = files.values().map(|v| v.len()).sum();
    let needs_graph = total_words >= CORPUS_WARN_THRESHOLD;

    let warning = if !needs_graph {
        Some(format!(
            "Corpus is ~{} words - fits in a single context window. You may not need a graph.",
            total_words
        ))
    } else if total_words >= CORPUS_UPPER_THRESHOLD || total_files >= FILE_COUNT_UPPER {
        Some(format!(
            "Large corpus: {} files · ~{} words. \
             Semantic extraction will be expensive (many Claude tokens). \
             Consider running on a subfolder, or use --no-semantic to run AST-only.",
            total_files, total_words
        ))
    } else {
        None
    };

    Ok(DetectResult {
        files,
        total_files,
        total_words,
        needs_graph,
        warning,
        skipped_sensitive,
        graphifyignore_patterns: ignore_patterns.len(),
    })
}

// ── Manifest ─────────────────────────────────────────────────────────────────

/// File modification time manifest entry.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest(pub std::collections::HashMap<String, f64>);

const MANIFEST_PATH: &str = "graphify-out/manifest.json";
const MTIME_EPSILON_SECS: f64 = 1e-6;

/// Load the file modification time manifest from a previous run.
pub fn load_manifest(
    manifest_path: Option<&str>,
) -> Result<std::collections::HashMap<String, f64>> {
    let path = manifest_path.unwrap_or(MANIFEST_PATH);
    let content =
        fs::read_to_string(path).with_context(|| format!("Failed to read manifest: {}", path))?;
    let map: std::collections::HashMap<String, f64> =
        serde_json::from_str(&content).with_context(|| "Failed to parse manifest JSON")?;
    Ok(map)
}

/// Save current file mtimes for the next incremental run.
pub fn save_manifest(
    files: &std::collections::HashMap<String, Vec<String>>,
    manifest_path: Option<&str>,
) -> Result<()> {
    let path = manifest_path.unwrap_or(MANIFEST_PATH);
    let mut manifest: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

    for file_list in files.values() {
        for f in file_list {
            if let Ok(meta) = fs::metadata(f) {
                if let Ok(modified) = meta.modified() {
                    if let Ok(dur) = modified.duration_since(std::time::UNIX_EPOCH) {
                        manifest.insert(f.clone(), dur.as_secs_f64());
                    }
                }
            }
        }
    }

    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(&manifest)?;
    fs::write(path, json)?;
    Ok(())
}

/// Result of incremental detection — only new or modified files.
#[derive(Debug, Serialize, Deserialize)]
pub struct DetectIncrementalResult {
    pub files: std::collections::HashMap<String, Vec<String>>,
    pub total_files: usize,
    pub total_words: usize,
    pub needs_graph: bool,
    pub warning: Option<String>,
    pub skipped_sensitive: Vec<String>,
    pub graphifyignore_patterns: usize,
    pub incremental: bool,
    pub new_files: std::collections::HashMap<String, Vec<String>>,
    pub unchanged_files: std::collections::HashMap<String, Vec<String>>,
    pub new_total: usize,
    pub deleted_files: Vec<String>,
}

/// Incremental detection — returns only new or modified files since the last run.
pub fn detect_incremental(
    root: &Path,
    follow_symlinks: bool,
    manifest_path: Option<&str>,
) -> Result<DetectIncrementalResult> {
    let full = detect(root, follow_symlinks)?;

    let manifest = match load_manifest(manifest_path) {
        Ok(m) => m,
        Err(_) => {
            // No previous run — treat everything as new
            return Ok(DetectIncrementalResult {
                files: full.files.clone(),
                total_files: full.total_files,
                total_words: full.total_words,
                needs_graph: full.needs_graph,
                warning: full.warning,
                skipped_sensitive: full.skipped_sensitive,
                graphifyignore_patterns: full.graphifyignore_patterns,
                incremental: true,
                new_files: full.files.clone(),
                unchanged_files: [
                    ("code".to_string(), Vec::new()),
                    ("document".to_string(), Vec::new()),
                    ("paper".to_string(), Vec::new()),
                    ("image".to_string(), Vec::new()),
                    ("video".to_string(), Vec::new()),
                ]
                .into_iter()
                .collect(),
                new_total: full.total_files,
                deleted_files: Vec::new(),
            });
        }
    };

    let empty_map: std::collections::HashMap<String, Vec<String>> = [
        ("code".to_string(), Vec::new()),
        ("document".to_string(), Vec::new()),
        ("paper".to_string(), Vec::new()),
        ("image".to_string(), Vec::new()),
        ("video".to_string(), Vec::new()),
    ]
    .into_iter()
    .collect();

    let mut new_files = empty_map.clone();
    let mut unchanged_files = empty_map.clone();
    let manifest_lookup: std::collections::HashMap<String, f64> = manifest
        .iter()
        .map(|(path, mtime)| (normalize_manifest_key(path), *mtime))
        .collect();

    for (ftype, file_list) in &full.files {
        for f in file_list {
            let normalized_key = normalize_manifest_key(f);
            let stored_mtime = manifest_lookup
                .get(&normalized_key)
                .copied()
                .or_else(|| manifest.get(f).copied());
            let current_mtime = fs::metadata(f)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            if stored_mtime.is_none() || current_mtime - stored_mtime.unwrap() > MTIME_EPSILON_SECS
            {
                new_files.entry(ftype.clone()).or_default().push(f.clone());
            } else {
                unchanged_files
                    .entry(ftype.clone())
                    .or_default()
                    .push(f.clone());
            }
        }
    }

    // Find deleted files
    let current_files: HashSet<String> = full
        .files
        .values()
        .flatten()
        .map(|path| normalize_manifest_key(path))
        .collect();
    let deleted_files: Vec<String> = manifest
        .keys()
        .filter(|f| !current_files.contains(&normalize_manifest_key(f)))
        .cloned()
        .collect();

    let new_total: usize = new_files.values().map(|v| v.len()).sum();

    Ok(DetectIncrementalResult {
        files: full.files.clone(),
        total_files: full.total_files,
        total_words: full.total_words,
        needs_graph: full.needs_graph,
        warning: full.warning,
        skipped_sensitive: full.skipped_sensitive,
        graphifyignore_patterns: full.graphifyignore_patterns,
        incremental: true,
        new_files,
        unchanged_files,
        new_total,
        deleted_files,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn test_classify_code() {
        assert_eq!(classify_file(Path::new("foo.py")), Some(FileType::Code));
        assert_eq!(classify_file(Path::new("foo.rs")), Some(FileType::Code));
        assert_eq!(classify_file(Path::new("foo.tsx")), Some(FileType::Code));
        assert_eq!(
            classify_file(Path::new("foo.blade.php")),
            Some(FileType::Code)
        );
    }

    #[test]
    fn test_classify_document() {
        assert_eq!(
            classify_file(Path::new("readme.md")),
            Some(FileType::Document)
        );
        assert_eq!(
            classify_file(Path::new("notes.txt")),
            Some(FileType::Document)
        );
    }

    #[test]
    fn test_classify_markdown_paper_by_signals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.md");
        fs::write(
            &path,
            "# Intro\n\nThis method is discussed in [1] and [23].\nSee Equation 3 for details.\nIdentifier 1706.03762.\n",
        )
        .unwrap();
        assert_eq!(classify_file(&path), Some(FileType::Paper));
    }

    #[test]
    fn test_markdown_review_that_mentions_signal_patterns_stays_document() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("review.md");
        fs::write(
            &path,
            "This report says the heuristic checks `\\barxiv\\b`, `\\babstract\\b`, and `1706.03762`, but it is not itself a paper.\n",
        )
        .unwrap();
        assert_eq!(classify_file(&path), Some(FileType::Document));
    }

    #[test]
    fn test_classify_image() {
        assert_eq!(classify_file(Path::new("photo.jpg")), Some(FileType::Image));
        assert_eq!(classify_file(Path::new("icon.png")), Some(FileType::Image));
    }

    #[test]
    fn test_classify_video() {
        assert_eq!(classify_file(Path::new("clip.mp4")), Some(FileType::Video));
    }

    #[test]
    fn test_classify_unknown() {
        assert_eq!(classify_file(Path::new("foo.xyz")), None);
    }

    #[test]
    fn test_is_sensitive() {
        assert!(is_sensitive(Path::new(".env")));
        assert!(is_sensitive(Path::new("config/secret.key")));
        assert!(is_sensitive(Path::new("aws_credentials.json")));
        assert!(!is_sensitive(Path::new("src/main.rs")));
    }

    #[test]
    fn test_is_noise_dir() {
        assert!(is_noise_dir("node_modules"));
        assert!(is_noise_dir(".git"));
        assert!(is_noise_dir("venv"));
        assert!(is_noise_dir("my_project_venv"));
        assert!(!is_noise_dir("src"));
        assert!(!is_noise_dir("lib"));
    }

    #[test]
    fn test_count_words() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        fs::write(&path, "hello world foo bar baz").unwrap();
        assert_eq!(count_words(&path), 5);
    }

    #[test]
    fn test_detect_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 0);
        assert!(!result.needs_graph);
    }

    #[test]
    fn test_detect_with_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("main.py"), "print('hello')").unwrap();
        fs::write(dir.path().join("readme.md"), "# Hello\nThis is a readme.").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 2);

        let code = result.files.get("code").unwrap();
        assert_eq!(code.len(), 1);
        assert!(code[0].ends_with("main.py"));

        let docs = result.files.get("document").unwrap();
        assert_eq!(docs.len(), 1);
        assert!(docs[0].ends_with("readme.md"));
    }

    #[test]
    fn test_detect_preserves_absolute_input_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.py");
        fs::write(&file, "print('hello')").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(
            result.files.get("code").unwrap(),
            &vec![file.to_string_lossy().to_string()]
        );
    }

    #[test]
    fn test_detect_skips_hidden_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("main.py"), "print('hi')").unwrap();
        fs::write(dir.path().join(".env"), "SECRET=abc").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 1);
        assert!(result.skipped_sensitive.iter().any(|s| s.contains(".env")));
    }

    #[test]
    fn test_detect_skips_noise_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let node_modules = dir.path().join("node_modules");
        fs::create_dir(&node_modules).unwrap();
        fs::write(node_modules.join("pkg.js"), "module.exports = 1").unwrap();
        fs::write(dir.path().join("app.py"), "x = 1").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 1);
        assert!(result.files.get("code").unwrap()[0].ends_with("app.py"));
    }

    #[test]
    fn test_detect_skips_hidden_dir_contents() {
        let dir = tempfile::tempdir().unwrap();
        let hidden = dir.path().join(".cache");
        fs::create_dir(&hidden).unwrap();
        fs::write(hidden.join("ignored.py"), "x = 1").unwrap();
        fs::write(dir.path().join("main.py"), "x = 1").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 1);
        assert!(result.files.get("code").unwrap()[0].ends_with("main.py"));
    }

    #[test]
    fn test_graphifyignore() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(".graphifyignore"), "vendor/\n*.log").unwrap();
        fs::write(dir.path().join("main.py"), "x = 1").unwrap();
        fs::write(dir.path().join("debug.log"), "stuff").unwrap();

        let vendor = dir.path().join("vendor");
        fs::create_dir(&vendor).unwrap();
        fs::write(vendor.join("lib.py"), "y = 2").unwrap();

        let result = detect(dir.path(), false).unwrap();
        assert_eq!(result.total_files, 1);
        assert!(result.files.get("code").unwrap()[0].ends_with("main.py"));
    }

    #[test]
    fn test_content_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        fs::write(&path, "hello world").unwrap();
        let hash = content_hash(&path).unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 hex = 64 chars

        // Same content = same hash
        let hash2 = content_hash(&path).unwrap();
        assert_eq!(hash, hash2);

        // Different content = different hash
        fs::write(&path, "different content").unwrap();
        let hash3 = content_hash(&path).unwrap();
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let test_file = dir.path().join("test.py");
        fs::write(&test_file, "x = 1").unwrap();

        let mut files = std::collections::HashMap::new();
        files.insert(
            "code".to_string(),
            vec![test_file.to_string_lossy().to_string()],
        );

        save_manifest(&files, Some(manifest_path.to_str().unwrap())).unwrap();
        let loaded = load_manifest(Some(manifest_path.to_str().unwrap())).unwrap();

        assert!(loaded.contains_key(&test_file.to_string_lossy().to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn test_detect_incremental_matches_manifest_across_path_representations() {
        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real");
        fs::create_dir(&real_root).unwrap();
        let file = real_root.join("main.py");
        fs::write(&file, "x = 1").unwrap();

        let link_root = dir.path().join("link");
        symlink(&real_root, &link_root).unwrap();

        let manifest_path = dir.path().join("manifest.json");
        let mut manifest = std::collections::HashMap::new();
        manifest.insert(
            file.to_string_lossy().to_string(),
            fs::metadata(&file)
                .unwrap()
                .modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        );
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let result =
            detect_incremental(&link_root, false, Some(manifest_path.to_str().unwrap())).unwrap();
        assert_eq!(result.new_total, 0);
        assert!(result.deleted_files.is_empty());
        assert_eq!(
            result.unchanged_files.get("code").unwrap(),
            &vec![link_root.join("main.py").to_string_lossy().to_string()]
        );
    }
}
