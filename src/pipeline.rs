use anyhow::{Context, Result};
use rayon::join;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::{build, detect, extract};

#[derive(Debug, Clone, Serialize)]
pub struct RebuildCodeResult {
    pub ok: bool,
    pub message: String,
    pub nodes: usize,
    pub edges: usize,
    pub communities: usize,
    pub preserved_semantic_nodes: usize,
    pub preserved_semantic_edges: usize,
    pub out_dir: String,
    pub graph_path: String,
    pub html_path: String,
    pub report_path: String,
    pub wiki_path: Option<String>,
    pub wiki_articles: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterOnlyResult {
    pub ok: bool,
    pub nodes: usize,
    pub edges: usize,
    pub communities: usize,
    pub out_dir: String,
    pub graph_path: String,
    pub html_path: String,
    pub report_path: String,
    pub wiki_path: Option<String>,
    pub wiki_articles: usize,
}

#[derive(Debug, Default)]
struct PreservedSemanticGraph {
    nodes: Vec<Value>,
    edges: Vec<Value>,
    hyperedges: Vec<Value>,
}

#[derive(Debug)]
struct GraphOutputSummary {
    communities: usize,
    out_dir: PathBuf,
    graph_path: PathBuf,
    html_path: PathBuf,
    report_path: PathBuf,
    wiki_path: Option<PathBuf>,
    wiki_articles: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchedFileState {
    file_type: detect::FileType,
    size: u64,
    modified_ns: u128,
}

pub fn rebuild_code(
    root: &Path,
    follow_symlinks: bool,
    today: Option<&str>,
    write_wiki: bool,
) -> Result<RebuildCodeResult> {
    let detection = detect::detect(root, follow_symlinks)?;
    let code_files = detection.files.get("code").cloned().unwrap_or_default();
    let out_dir = root.join("graphify-out");
    let graph_path = out_dir.join("graph.json");
    let report_path = out_dir.join("GRAPH_REPORT.md");

    if code_files.is_empty() {
        return Ok(RebuildCodeResult {
            ok: false,
            message: "No code files found - nothing to rebuild.".to_string(),
            nodes: 0,
            edges: 0,
            communities: 0,
            preserved_semantic_nodes: 0,
            preserved_semantic_edges: 0,
            out_dir: out_dir.to_string_lossy().to_string(),
            graph_path: graph_path.to_string_lossy().to_string(),
            html_path: out_dir.join("graph-3d.html").to_string_lossy().to_string(),
            report_path: report_path.to_string_lossy().to_string(),
            wiki_path: None,
            wiki_articles: 0,
        });
    }

    let extraction = extract::extract_paths(&code_files)?;
    let preserved = load_preserved_semantic_graph(root);
    let graph = build_graph_with_preserved_semantics(extraction, &preserved);
    let detection_value = serde_json::to_value(&detection)?;
    let output = write_graph_outputs(root, &graph, detection_value, today, write_wiki)?;

    Ok(RebuildCodeResult {
        ok: true,
        message: "Rebuilt code graph.".to_string(),
        nodes: graph.nodes.len(),
        edges: graph.edges.len(),
        communities: output.communities,
        preserved_semantic_nodes: preserved.nodes.len(),
        preserved_semantic_edges: preserved.edges.len(),
        out_dir: output.out_dir.to_string_lossy().to_string(),
        graph_path: output.graph_path.to_string_lossy().to_string(),
        html_path: output.html_path.to_string_lossy().to_string(),
        report_path: output.report_path.to_string_lossy().to_string(),
        wiki_path: output
            .wiki_path
            .as_ref()
            .map(|path| path.to_string_lossy().to_string()),
        wiki_articles: output.wiki_articles,
    })
}

pub fn cluster_only(
    root: &Path,
    today: Option<&str>,
    write_wiki: bool,
) -> Result<ClusterOnlyResult> {
    let graph_path = root.join("graphify-out").join("graph.json");
    let graph_json = fs::read_to_string(&graph_path).with_context(|| {
        format!(
            "no graph found at {} - run graphify first",
            graph_path.display()
        )
    })?;
    let graph_value: Value = serde_json::from_str(&graph_json)
        .with_context(|| format!("invalid graph JSON: {}", graph_path.display()))?;
    let graph = build::merge_extractions(std::slice::from_ref(&graph_value));
    let output = write_graph_outputs(root, &graph, json!({}), today, write_wiki)?;

    Ok(ClusterOnlyResult {
        ok: true,
        nodes: graph.nodes.len(),
        edges: graph.edges.len(),
        communities: output.communities,
        out_dir: output.out_dir.to_string_lossy().to_string(),
        graph_path: output.graph_path.to_string_lossy().to_string(),
        html_path: output.html_path.to_string_lossy().to_string(),
        report_path: output.report_path.to_string_lossy().to_string(),
        wiki_path: output
            .wiki_path
            .as_ref()
            .map(|path| path.to_string_lossy().to_string()),
        wiki_articles: output.wiki_articles,
    })
}

pub fn notify_only(root: &Path) -> Result<PathBuf> {
    let flag = root.join("graphify-out").join("needs_update");
    if let Some(parent) = flag.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("cannot create output directory: {}", parent.display()))?;
    }
    fs::write(&flag, "1").with_context(|| format!("cannot write {}", flag.display()))?;
    Ok(flag)
}

pub fn watch(
    root: &Path,
    debounce: Duration,
    follow_symlinks: bool,
    today: Option<&str>,
) -> Result<()> {
    let root = root
        .canonicalize()
        .with_context(|| format!("watch root not found: {}", root.display()))?;
    let poll_interval = Duration::from_millis(500);
    let mut previous = snapshot_watched_files(&root, follow_symlinks)?;
    let mut last_trigger = Instant::now();
    let mut pending = false;
    let mut changed: HashMap<PathBuf, detect::FileType> = HashMap::new();

    println!(
        "[graphify watch] Watching {} - press Ctrl+C to stop",
        root.display()
    );
    println!(
        "[graphify watch] Code changes rebuild graph automatically. Doc/image changes require /graphify --update."
    );
    println!("[graphify watch] Debounce: {}s", debounce.as_secs_f64());

    loop {
        thread::sleep(poll_interval);

        let current = snapshot_watched_files(&root, follow_symlinks)?;
        let diffs = diff_watched_files(&previous, &current);
        if !diffs.is_empty() {
            pending = true;
            last_trigger = Instant::now();
            changed.extend(diffs);
        }
        previous = current;

        if pending && last_trigger.elapsed() >= debounce {
            println!("\n[graphify watch] {} file(s) changed", changed.len());
            if has_non_code_changes(&changed) {
                let flag = notify_only(&root)?;
                println!(
                    "[graphify watch] New or changed files detected in {}",
                    root.display()
                );
                println!(
                    "[graphify watch] Non-code files changed - semantic re-extraction requires LLM."
                );
                println!(
                    "[graphify watch] Run `/graphify --update` in Claude Code to update the graph."
                );
                println!("[graphify watch] Flag written to {}", flag.display());
            } else {
                let result = rebuild_code(&root, follow_symlinks, today, false)?;
                if !result.ok {
                    println!("[graphify watch] {}", result.message);
                } else {
                    println!(
                        "[graphify watch] Rebuilt: {} nodes, {} edges, {} communities",
                        result.nodes, result.edges, result.communities
                    );
                    println!(
                        "[graphify watch] graph.json, graph-3d.html and GRAPH_REPORT.md updated in {}",
                        result.out_dir
                    );
                }
            }
            changed.clear();
            pending = false;
        }
    }
}

fn build_graph_with_preserved_semantics(
    extraction: crate::schema::Extraction,
    preserved: &PreservedSemanticGraph,
) -> build::Graph {
    let mut payload = json!({
        "nodes": extraction.nodes,
        "edges": extraction.edges,
        "hyperedges": preserved.hyperedges,
        "input_tokens": 0,
        "output_tokens": 0,
    });

    if let Some(nodes) = payload.get_mut("nodes").and_then(Value::as_array_mut) {
        nodes.extend(preserved.nodes.clone());
    }
    if let Some(edges) = payload.get_mut("edges").and_then(Value::as_array_mut) {
        edges.extend(preserved.edges.clone());
    }

    build::merge_extractions(std::slice::from_ref(&payload))
}

fn snapshot_watched_files(
    root: &Path,
    follow_symlinks: bool,
) -> Result<HashMap<PathBuf, WatchedFileState>> {
    let mut builder = ignore::WalkBuilder::new(root);
    builder.hidden(false);
    builder.follow_links(follow_symlinks);
    builder.git_ignore(true);
    builder.git_global(true);
    builder.git_exclude(true);

    let mut files = HashMap::new();

    for entry in builder.build() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let rel = match path.strip_prefix(root) {
            Ok(rel) => rel.to_path_buf(),
            Err(_) => continue,
        };
        if should_ignore_watch_path(&rel) {
            continue;
        }
        let file_type = match watched_file_type(path) {
            Some(file_type) => file_type,
            None => continue,
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        files.insert(
            rel,
            WatchedFileState {
                file_type,
                size: metadata.len(),
                modified_ns,
            },
        );
    }

    Ok(files)
}

fn diff_watched_files(
    previous: &HashMap<PathBuf, WatchedFileState>,
    current: &HashMap<PathBuf, WatchedFileState>,
) -> HashMap<PathBuf, detect::FileType> {
    let mut changed = HashMap::new();

    for (path, state) in current {
        match previous.get(path) {
            Some(old)
                if old.size == state.size
                    && old.modified_ns == state.modified_ns
                    && old.file_type == state.file_type => {}
            _ => {
                changed.insert(path.clone(), state.file_type);
            }
        }
    }

    for (path, state) in previous {
        if !current.contains_key(path) {
            changed.insert(path.clone(), state.file_type);
        }
    }

    changed
}

fn should_ignore_watch_path(rel: &Path) -> bool {
    rel.components().any(|component| {
        let part = component.as_os_str().to_string_lossy();
        part == "graphify-out" || part.starts_with('.')
    })
}

fn watched_file_type(path: &Path) -> Option<detect::FileType> {
    let ext = path
        .extension()
        .map(|ext| format!(".{}", ext.to_string_lossy().to_lowercase()))
        .unwrap_or_default();
    if matches!(ext.as_str(), ".docx" | ".xlsx") {
        return None;
    }

    let file_type = detect::classify_file(path)?;
    match file_type {
        detect::FileType::Code
        | detect::FileType::Document
        | detect::FileType::Paper
        | detect::FileType::Image => Some(file_type),
        detect::FileType::Video => None,
    }
}

fn has_non_code_changes(changed: &HashMap<PathBuf, detect::FileType>) -> bool {
    changed
        .values()
        .any(|file_type| *file_type != detect::FileType::Code)
}

fn load_preserved_semantic_graph(root: &Path) -> PreservedSemanticGraph {
    let graph_path = root.join("graphify-out").join("graph.json");
    let graph_text = match fs::read_to_string(graph_path) {
        Ok(text) => text,
        Err(_) => return PreservedSemanticGraph::default(),
    };
    let graph_value: Value = match serde_json::from_str(&graph_text) {
        Ok(value) => value,
        Err(_) => return PreservedSemanticGraph::default(),
    };

    let node_values = graph_value
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let code_ids: HashSet<String> = node_values
        .iter()
        .filter(|node| node.get("file_type").and_then(Value::as_str) == Some("code"))
        .filter_map(|node| {
            node.get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect();
    let nodes = node_values
        .into_iter()
        .filter(|node| node.get("file_type").and_then(Value::as_str) != Some("code"))
        .collect();

    let edge_values = graph_value
        .get("links")
        .or_else(|| graph_value.get("edges"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let edges = edge_values
        .into_iter()
        .filter(|edge| {
            let confidence = edge.get("confidence").and_then(Value::as_str).unwrap_or("");
            if matches!(confidence, "INFERRED" | "AMBIGUOUS") {
                return true;
            }
            let source = edge
                .get("source")
                .or_else(|| edge.get("from"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let target = edge
                .get("target")
                .or_else(|| edge.get("to"))
                .and_then(Value::as_str)
                .unwrap_or("");
            !code_ids.contains(source) && !code_ids.contains(target)
        })
        .collect();
    let hyperedges = graph_value
        .get("hyperedges")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    PreservedSemanticGraph {
        nodes,
        edges,
        hyperedges,
    }
}

fn write_graph_outputs(
    root: &Path,
    graph: &build::Graph,
    detection_result: Value,
    today: Option<&str>,
    write_wiki: bool,
) -> Result<GraphOutputSummary> {
    let communities = build::cluster(graph);
    let cohesion_scores = build::score_all(graph, &communities);
    let community_labels = default_community_labels(&communities);
    let god_nodes = build::god_nodes(graph, 10);
    let surprising_connections = build::surprising_connections(graph, &communities, 5);
    let suggested_questions = build::suggest_questions(graph, &communities, &community_labels, 7);
    let token_cost = json!({"input": 0, "output": 0});

    let out_dir = root.join("graphify-out");
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("cannot create output directory: {}", out_dir.display()))?;
    let graph_path = out_dir.join("graph.json");
    let html_path = out_dir.join("graph-3d.html");
    let report_path = out_dir.join("GRAPH_REPORT.md");
    let legacy_html_path = out_dir.join("graph.html");
    let wiki_path = if write_wiki {
        Some(out_dir.join("wiki"))
    } else {
        existing_wiki_dir(&out_dir)
    };

    let ((report, graph_json), (graph_html, wiki_articles)) = join(
        || {
            join(
                || {
                    build::generate_report(
                        graph,
                        &communities,
                        &cohesion_scores,
                        &community_labels,
                        &god_nodes,
                        &surprising_connections,
                        &detection_result,
                        &token_cost,
                        &root.to_string_lossy(),
                        &suggested_questions,
                        today,
                    )
                },
                || {
                    serde_json::to_string_pretty(&build::export_json_data(graph, &communities))
                        .context("cannot serialize graph.json")
                },
            )
        },
        || {
            join(
                || build::export_html_3d(graph, &communities, &community_labels, "graph-3d.html"),
                || {
                    if let Some(wiki_dir) = wiki_path.as_ref() {
                        build::export_wiki(
                            graph,
                            &communities,
                            wiki_dir,
                            &community_labels,
                            &cohesion_scores,
                            &god_nodes,
                        )
                        .with_context(|| format!("cannot refresh wiki at {}", wiki_dir.display()))
                    } else {
                        Ok(0)
                    }
                },
            )
        },
    );
    let graph_json = graph_json?;
    let wiki_articles = wiki_articles?;

    fs::write(&graph_path, graph_json)
        .with_context(|| format!("cannot write {}", graph_path.display()))?;
    fs::write(&html_path, graph_html)
        .with_context(|| format!("cannot write {}", html_path.display()))?;
    fs::write(&report_path, report)
        .with_context(|| format!("cannot write {}", report_path.display()))?;
    if legacy_html_path.exists() {
        fs::remove_file(&legacy_html_path)
            .with_context(|| format!("cannot remove {}", legacy_html_path.display()))?;
    }

    let needs_update = out_dir.join("needs_update");
    if needs_update.exists() {
        let _ = fs::remove_file(needs_update);
    }

    Ok(GraphOutputSummary {
        communities: communities.len(),
        out_dir,
        graph_path,
        html_path,
        report_path,
        wiki_path,
        wiki_articles,
    })
}

fn default_community_labels(communities: &HashMap<usize, Vec<String>>) -> HashMap<usize, String> {
    communities
        .keys()
        .copied()
        .map(|cid| (cid, format!("Community {}", cid)))
        .collect()
}

fn existing_wiki_dir(out_dir: &Path) -> Option<PathBuf> {
    let wiki_dir = out_dir.join("wiki");
    (wiki_dir.join("index.md").exists()).then_some(wiki_dir)
}

#[cfg(test)]
mod tests {
    use super::{
        cluster_only, diff_watched_files, has_non_code_changes, notify_only, rebuild_code,
        snapshot_watched_files,
    };
    use crate::detect;
    use anyhow::Result;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn rebuild_code_returns_false_when_no_code_files_exist() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("notes.md"), "# Notes\n")?;

        let result = rebuild_code(dir.path(), false, Some("2026-04-16"), false)?;

        assert!(!result.ok);
        assert_eq!(result.message, "No code files found - nothing to rebuild.");
        Ok(())
    }

    #[test]
    fn rebuild_code_preserves_non_code_nodes_and_edges() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("service.py"),
            "class Service:\n    def run(self):\n        return 1\n",
        )?;

        let out_dir = dir.path().join("graphify-out");
        fs::create_dir_all(&out_dir)?;
        fs::write(
            out_dir.join("graph.json"),
            serde_json::to_string_pretty(&json!({
                "nodes": [
                    {"id": "doc1", "label": "Guide", "file_type": "document", "source_file": "guide.md"},
                    {"id": "doc2", "label": "Spec", "file_type": "document", "source_file": "spec.md"}
                ],
                "links": [
                    {"source": "doc1", "target": "doc2", "relation": "references", "confidence": "AMBIGUOUS", "source_file": "guide.md"}
                ],
                "hyperedges": [{"id": "docs", "nodes": ["doc1", "doc2"]}]
            }))?,
        )?;
        fs::write(out_dir.join("needs_update"), "1")?;

        let result = rebuild_code(dir.path(), false, Some("2026-04-16"), false)?;
        let graph: Value = serde_json::from_str(&fs::read_to_string(out_dir.join("graph.json"))?)?;

        assert!(result.ok);
        assert_eq!(result.preserved_semantic_nodes, 2);
        assert_eq!(result.preserved_semantic_edges, 1);
        assert!(graph["nodes"].as_array().is_some_and(|nodes| {
            nodes
                .iter()
                .any(|node| node.get("id").and_then(Value::as_str) == Some("doc1"))
        }));
        assert!(graph["links"].as_array().is_some_and(|edges| {
            edges
                .iter()
                .any(|edge| edge.get("relation").and_then(Value::as_str) == Some("references"))
        }));
        assert!(out_dir.join("graph-3d.html").exists());
        assert!(!out_dir.join("graph.html").exists());
        assert!(!out_dir.join("needs_update").exists());
        Ok(())
    }

    #[test]
    fn cluster_only_rewrites_graph_json_report_and_html() -> Result<()> {
        let dir = tempdir()?;
        let out_dir = dir.path().join("graphify-out");
        fs::create_dir_all(&out_dir)?;
        fs::write(
            out_dir.join("graph.json"),
            serde_json::to_string_pretty(&json!({
                "nodes": [
                    {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                    {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
                ],
                "links": [
                    {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
                ]
            }))?,
        )?;

        let result = cluster_only(dir.path(), Some("2026-04-16"), false)?;
        let graph: Value = serde_json::from_str(&fs::read_to_string(out_dir.join("graph.json"))?)?;
        let report = fs::read_to_string(out_dir.join("GRAPH_REPORT.md"))?;
        let html = fs::read_to_string(out_dir.join("graph-3d.html"))?;

        assert!(result.ok);
        assert!(result.communities >= 1);
        assert!(
            graph["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.iter().all(|node| node.get("community").is_some()))
        );
        assert!(report.contains("## Communities"));
        assert!(html.contains("3d-force-graph"));
        Ok(())
    }

    #[test]
    fn rebuild_code_refreshes_existing_wiki() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("service.py"),
            "class Service:\n    def run(self):\n        return 1\n",
        )?;

        let wiki_dir = dir.path().join("graphify-out").join("wiki");
        fs::create_dir_all(&wiki_dir)?;
        fs::write(wiki_dir.join("index.md"), "# stale wiki\n")?;

        let result = rebuild_code(dir.path(), false, Some("2026-04-16"), false)?;
        let index = fs::read_to_string(wiki_dir.join("index.md"))?;

        assert!(result.ok);
        assert!(index.contains("# Knowledge Graph Index"));
        assert!(wiki_dir.read_dir()?.filter_map(Result::ok).any(|entry| {
            entry.path().extension().and_then(|ext| ext.to_str()) == Some("md")
                && entry.file_name() != "index.md"
        }));
        Ok(())
    }

    #[test]
    fn cluster_only_refreshes_existing_wiki() -> Result<()> {
        let dir = tempdir()?;
        let out_dir = dir.path().join("graphify-out");
        fs::create_dir_all(out_dir.join("wiki"))?;
        fs::write(out_dir.join("wiki").join("index.md"), "# stale wiki\n")?;
        fs::write(
            out_dir.join("graph.json"),
            serde_json::to_string_pretty(&json!({
                "nodes": [
                    {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                    {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
                ],
                "links": [
                    {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
                ]
            }))?,
        )?;

        let result = cluster_only(dir.path(), Some("2026-04-16"), false)?;
        let index = fs::read_to_string(out_dir.join("wiki").join("index.md"))?;

        assert!(result.ok);
        assert!(index.contains("# Knowledge Graph Index"));
        assert!(
            out_dir
                .join("wiki")
                .read_dir()?
                .filter_map(Result::ok)
                .any(
                    |entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("md")
                        && entry.file_name() != "index.md"
                )
        );
        Ok(())
    }

    #[test]
    fn rebuild_code_creates_wiki_when_requested() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("service.py"),
            "class Service:\n    def run(self):\n        return 1\n",
        )?;

        let result = rebuild_code(dir.path(), false, Some("2026-04-16"), true)?;
        let wiki_dir = dir.path().join("graphify-out").join("wiki");

        assert!(result.ok);
        assert_eq!(
            result.wiki_path.as_deref(),
            Some(wiki_dir.to_string_lossy().as_ref())
        );
        assert!(result.wiki_articles >= 1);
        assert!(wiki_dir.join("index.md").exists());
        assert!(wiki_dir.read_dir()?.filter_map(Result::ok).any(|entry| {
            entry.path().extension().and_then(|ext| ext.to_str()) == Some("md")
                && entry.file_name() != "index.md"
        }));
        Ok(())
    }

    #[test]
    fn cluster_only_creates_wiki_when_requested() -> Result<()> {
        let dir = tempdir()?;
        let out_dir = dir.path().join("graphify-out");
        fs::create_dir_all(&out_dir)?;
        fs::write(
            out_dir.join("graph.json"),
            serde_json::to_string_pretty(&json!({
                "nodes": [
                    {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                    {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
                ],
                "links": [
                    {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
                ]
            }))?,
        )?;

        let result = cluster_only(dir.path(), Some("2026-04-16"), true)?;
        let wiki_dir = out_dir.join("wiki");

        assert!(result.ok);
        assert_eq!(
            result.wiki_path.as_deref(),
            Some(wiki_dir.to_string_lossy().as_ref())
        );
        assert!(result.wiki_articles >= 1);
        assert!(wiki_dir.join("index.md").exists());
        assert!(wiki_dir.read_dir()?.filter_map(Result::ok).any(|entry| {
            entry.path().extension().and_then(|ext| ext.to_str()) == Some("md")
                && entry.file_name() != "index.md"
        }));
        Ok(())
    }

    #[test]
    fn notify_only_writes_needs_update_flag() -> Result<()> {
        let dir = tempdir()?;
        let flag = notify_only(dir.path())?;

        assert_eq!(flag, dir.path().join("graphify-out").join("needs_update"));
        assert_eq!(fs::read_to_string(flag)?, "1");
        Ok(())
    }

    #[test]
    fn snapshot_watched_files_ignores_hidden_and_graphify_out() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("main.py"), "print('ok')\n")?;
        fs::create_dir_all(dir.path().join(".git"))?;
        fs::write(dir.path().join(".git").join("config"), "[core]\n")?;
        fs::create_dir_all(dir.path().join("graphify-out"))?;
        fs::write(dir.path().join("graphify-out").join("graph.json"), "{}")?;
        fs::write(dir.path().join("notes.docx"), "fake")?;

        let snapshot = snapshot_watched_files(dir.path(), false)?;

        assert!(snapshot.contains_key(&PathBuf::from("main.py")));
        assert!(!snapshot.contains_key(&PathBuf::from(".git/config")));
        assert!(!snapshot.contains_key(&PathBuf::from("graphify-out/graph.json")));
        assert!(!snapshot.contains_key(&PathBuf::from("notes.docx")));
        Ok(())
    }

    #[test]
    fn diff_watched_files_detects_modified_and_removed_types() -> Result<()> {
        let dir = tempdir()?;
        let code_path = dir.path().join("main.py");
        let doc_path = dir.path().join("guide.md");
        fs::write(&code_path, "print('v1')\n")?;
        fs::write(&doc_path, "# Guide\n")?;

        let before = snapshot_watched_files(dir.path(), false)?;
        fs::write(&code_path, "print('v2 changed')\n")?;
        fs::remove_file(&doc_path)?;
        let after = snapshot_watched_files(dir.path(), false)?;

        let changed = diff_watched_files(&before, &after);

        assert_eq!(
            changed.get(&PathBuf::from("main.py")),
            Some(&detect::FileType::Code)
        );
        assert_eq!(
            changed.get(&PathBuf::from("guide.md")),
            Some(&detect::FileType::Document)
        );
        assert!(has_non_code_changes(&changed));
        assert!(!has_non_code_changes(&HashMap::from([(
            PathBuf::from("main.py"),
            detect::FileType::Code,
        )])));
        Ok(())
    }
}
