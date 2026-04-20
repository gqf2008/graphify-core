use crate::build::strip_diacritics;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
struct QueryNode {
    id: String,
    label: String,
    source_file: String,
    source_location: String,
    file_type: String,
    norm_label: String,
    community: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct QueryEdge {
    relation: String,
    confidence: String,
}

#[derive(Debug, Clone, Default)]
struct QueryGraph {
    nodes: HashMap<String, QueryNode>,
    adjacency: HashMap<String, Vec<String>>,
    edges: HashMap<(String, String), QueryEdge>,
}

pub fn run_query_cli(
    graph_path: &Path,
    question: &str,
    use_dfs: bool,
    depth: usize,
    budget: usize,
) -> i32 {
    match query_text(graph_path, question, use_dfs, depth, budget) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_path_cli(
    graph_path: &Path,
    source_label: &str,
    target_label: &str,
    max_hops: Option<usize>,
) -> i32 {
    match path_text(graph_path, source_label, target_label, max_hops) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_explain_cli(graph_path: &Path, label: &str) -> i32 {
    match explain_text(graph_path, label) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_neighbors_cli(graph_path: &Path, label: &str, relation_filter: Option<&str>) -> i32 {
    match neighbors_text(graph_path, label, relation_filter) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_community_cli(graph_path: &Path, community_id: usize) -> i32 {
    match community_text(graph_path, community_id) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_god_nodes_cli(graph_path: &Path, top_n: usize) -> i32 {
    match god_nodes_text(graph_path, top_n) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn run_stats_cli(graph_path: &Path) -> i32 {
    match stats_text(graph_path) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

pub fn query_text(
    graph_path: &Path,
    question: &str,
    use_dfs: bool,
    depth: usize,
    budget: usize,
) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    let terms: Vec<String> = question
        .split_whitespace()
        .filter(|term| term.len() > 2)
        .map(|term| term.to_lowercase())
        .collect();
    let scored = score_nodes(&graph, &terms);
    if scored.is_empty() {
        return Ok("No matching nodes found.".to_string());
    }

    let start: Vec<String> = scored
        .into_iter()
        .take(5)
        .map(|(_, node_id)| node_id)
        .collect();
    let (nodes, edges) = if use_dfs {
        dfs(&graph, &start, depth)
    } else {
        bfs(&graph, &start, depth)
    };
    Ok(subgraph_to_text(&graph, &nodes, &edges, budget))
}

pub fn path_text(
    graph_path: &Path,
    source_label: &str,
    target_label: &str,
    max_hops: Option<usize>,
) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    let src_terms: Vec<String> = source_label
        .split_whitespace()
        .map(|term| term.to_lowercase())
        .collect();
    let tgt_terms: Vec<String> = target_label
        .split_whitespace()
        .map(|term| term.to_lowercase())
        .collect();
    let src_scored = score_nodes(&graph, &src_terms);
    let tgt_scored = score_nodes(&graph, &tgt_terms);

    if src_scored.is_empty() {
        return Err(format!("No node matching '{source_label}' found."));
    }
    if tgt_scored.is_empty() {
        return Err(format!("No node matching '{target_label}' found."));
    }

    let src_id = &src_scored[0].1;
    let tgt_id = &tgt_scored[0].1;
    let Some(path_nodes) = shortest_path(&graph, src_id, tgt_id) else {
        return Ok(format!(
            "No path found between '{source_label}' and '{target_label}'."
        ));
    };

    let hops = path_nodes.len().saturating_sub(1);
    if let Some(limit) = max_hops
        && hops > limit {
            return Ok(format!(
                "Path exceeds max_hops={} ({} hops found).",
                limit, hops
            ));
        }

    let mut segments: Vec<String> = Vec::new();
    for (index, window) in path_nodes.windows(2).enumerate() {
        let source = &window[0];
        let target = &window[1];
        let edge = graph.edge(source, target).cloned().unwrap_or_default();
        let confidence = if edge.confidence.is_empty() {
            String::new()
        } else {
            format!(" [{}]", edge.confidence)
        };
        if index == 0 {
            let source_label = graph
                .node(source)
                .map(|node| node.label.clone())
                .unwrap_or_else(|| source.clone());
            segments.push(source_label);
        }
        let target_label = graph
            .node(target)
            .map(|node| node.label.clone())
            .unwrap_or_else(|| target.clone());
        segments.push(format!(
            "--{}{}--> {}",
            edge.relation, confidence, target_label
        ));
    }

    Ok(format!(
        "Shortest path ({hops} hops):\n  {}",
        segments.join(" ")
    ))
}

pub fn explain_text(graph_path: &Path, label: &str) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    let matches = find_node(&graph, label);
    if matches.is_empty() {
        return Ok(format!("No node matching '{label}' found."));
    }

    let node_id = &matches[0];
    let Some(node) = graph.node(node_id) else {
        return Ok(format!("No node matching '{label}' found."));
    };

    let mut lines = vec![
        format!("Node: {}", node.label),
        format!("  ID:        {}", node.id),
        format!(
            "  Source:    {}",
            format!("{} {}", node.source_file, node.source_location)
                .trim()
                .to_string()
        ),
        format!("  Type:      {}", node.file_type),
        format!(
            "  Community: {}",
            node.community.as_deref().unwrap_or_default()
        ),
        format!("  Degree:    {}", graph.degree(node_id)),
    ];

    let neighbors = graph.neighbors(node_id);
    if !neighbors.is_empty() {
        lines.push(String::new());
        lines.push(format!("Connections ({}):", neighbors.len()));
        let mut ranked = neighbors;
        ranked.sort_by(|left, right| {
            graph
                .degree(right)
                .cmp(&graph.degree(left))
                .then_with(|| left.cmp(right))
        });
        for neighbor_id in ranked.iter().take(20) {
            let edge = graph
                .edge(node_id, neighbor_id)
                .cloned()
                .unwrap_or_default();
            let label = graph
                .node(neighbor_id)
                .map(|neighbor| neighbor.label.clone())
                .unwrap_or_else(|| neighbor_id.clone());
            lines.push(format!(
                "  --> {label} [{}] [{}]",
                edge.relation, edge.confidence
            ));
        }
        if ranked.len() > 20 {
            lines.push(format!("  ... and {} more", ranked.len() - 20));
        }
    }

    Ok(lines.join("\n"))
}

pub fn neighbors_text(
    graph_path: &Path,
    label: &str,
    relation_filter: Option<&str>,
) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    Ok(format_neighbors(&graph, label, relation_filter))
}

pub fn community_text(graph_path: &Path, community_id: usize) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    Ok(format_community(&graph, community_id))
}

pub fn god_nodes_text(graph_path: &Path, top_n: usize) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    Ok(format_god_nodes(&graph, top_n))
}

pub fn stats_text(graph_path: &Path) -> Result<String, String> {
    let graph = load_graph(graph_path)?;
    Ok(format_stats(&graph))
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkQuestionResult {
    pub question: String,
    pub query_tokens: usize,
    pub reduction: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResult {
    pub corpus_tokens: usize,
    pub corpus_words: usize,
    pub nodes: usize,
    pub edges: usize,
    pub avg_query_tokens: usize,
    pub reduction_ratio: f64,
    pub per_question: Vec<BenchmarkQuestionResult>,
}

const SAMPLE_QUESTIONS: [&str; 5] = [
    "how does authentication work",
    "what is the main entry point",
    "how are errors handled",
    "what connects the data layer to the api",
    "what are the core abstractions",
];

const CHARS_PER_TOKEN: usize = 4;

pub fn run_benchmark_json(
    graph_path: &Path,
    corpus_words: Option<usize>,
    questions: &[String],
) -> Result<Value, String> {
    let graph = load_graph(graph_path)?;
    Ok(benchmark_value(&graph, corpus_words, questions))
}

fn load_graph(graph_path: &Path) -> Result<QueryGraph, String> {
    let resolved = display_path(graph_path);
    if resolved.extension().and_then(|ext| ext.to_str()) != Some("json") {
        return Err("error: graph file must be a .json file".to_string());
    }
    if !resolved.exists() {
        return Err(format!(
            "error: graph file not found: {}",
            resolved.display()
        ));
    }

    let raw = fs::read_to_string(&resolved)
        .map_err(|err| format!("error: could not load graph: {err}"))?;
    let payload: Value =
        serde_json::from_str(&raw).map_err(|err| format!("error: could not load graph: {err}"))?;
    QueryGraph::from_value(&payload)
}

fn display_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

impl QueryGraph {
    fn from_value(payload: &Value) -> Result<Self, String> {
        let Some(object) = payload.as_object() else {
            return Err(
                "error: could not load graph: top-level JSON must be an object".to_string(),
            );
        };

        let Some(nodes_value) = object.get("nodes") else {
            return Err("error: could not load graph: missing nodes array".to_string());
        };
        let Some(nodes_array) = nodes_value.as_array() else {
            return Err("error: could not load graph: nodes must be an array".to_string());
        };

        let mut graph = QueryGraph::default();
        for node_value in nodes_array {
            let Some(node_object) = node_value.as_object() else {
                continue;
            };
            let Some(id) = get_string(node_object.get("id")) else {
                continue;
            };
            let label = get_string(node_object.get("label")).unwrap_or_else(|| id.clone());
            let source_file = get_string(node_object.get("source_file")).unwrap_or_default();
            let source_location =
                get_string(node_object.get("source_location")).unwrap_or_default();
            let file_type = get_string(node_object.get("file_type")).unwrap_or_default();
            let norm_label = get_string(node_object.get("norm_label"))
                .unwrap_or_else(|| strip_diacritics(&label).to_lowercase());
            let community = value_to_string(node_object.get("community"));

            graph.nodes.insert(
                id.clone(),
                QueryNode {
                    id: id.clone(),
                    label,
                    source_file,
                    source_location,
                    file_type,
                    norm_label,
                    community,
                },
            );
            graph.adjacency.entry(id).or_default();
        }

        let edges_array = object
            .get("links")
            .or_else(|| object.get("edges"))
            .and_then(Value::as_array);

        if let Some(edges_array) = edges_array {
            for edge_value in edges_array {
                let Some(edge_object) = edge_value.as_object() else {
                    continue;
                };
                let Some(source) = get_string(
                    edge_object
                        .get("source")
                        .or_else(|| edge_object.get("from")),
                ) else {
                    continue;
                };
                let Some(target) =
                    get_string(edge_object.get("target").or_else(|| edge_object.get("to")))
                else {
                    continue;
                };
                if !graph.nodes.contains_key(&source) || !graph.nodes.contains_key(&target) {
                    continue;
                }

                graph.link_nodes(&source, &target);
                graph.edges.insert(
                    edge_key(&source, &target),
                    QueryEdge {
                        relation: get_string(edge_object.get("relation")).unwrap_or_default(),
                        confidence: get_string(edge_object.get("confidence")).unwrap_or_default(),
                    },
                );
            }
        }

        Ok(graph)
    }

    fn link_nodes(&mut self, source: &str, target: &str) {
        add_neighbor(&mut self.adjacency, source, target);
        if source != target {
            add_neighbor(&mut self.adjacency, target, source);
        }
    }

    fn node(&self, node_id: &str) -> Option<&QueryNode> {
        self.nodes.get(node_id)
    }

    fn edge(&self, left: &str, right: &str) -> Option<&QueryEdge> {
        self.edges.get(&edge_key(left, right))
    }

    fn degree(&self, node_id: &str) -> usize {
        self.adjacency.get(node_id).map_or(0, Vec::len)
    }

    fn neighbors(&self, node_id: &str) -> Vec<String> {
        self.adjacency.get(node_id).cloned().unwrap_or_default()
    }
}

fn add_neighbor(adjacency: &mut HashMap<String, Vec<String>>, source: &str, target: &str) {
    let neighbors = adjacency.entry(source.to_string()).or_default();
    if !neighbors.iter().any(|neighbor| neighbor == target) {
        neighbors.push(target.to_string());
    }
}

fn edge_key(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

fn get_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_to_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Null => None,
        _ => Some(value.to_string()),
    })
}

fn score_nodes(graph: &QueryGraph, terms: &[String]) -> Vec<(i32, String)> {
    let normalized_terms: Vec<String> = terms
        .iter()
        .map(|term| strip_diacritics(term).to_lowercase())
        .collect();
    let mut scored: Vec<(i32, String)> = Vec::new();

    for (node_id, node) in &graph.nodes {
        let label_hits = normalized_terms
            .iter()
            .filter(|term| node.norm_label.contains(term.as_str()))
            .count() as i32;
        let source_hits = normalized_terms
            .iter()
            .filter(|term| node.source_file.to_lowercase().contains(term.as_str()))
            .count() as i32;
        let score = label_hits * 2 + source_hits;
        if score > 0 {
            scored.push((score, node_id.clone()));
        }
    }

    scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
    scored
}

fn estimate_tokens(text: &str) -> usize {
    std::cmp::max(1usize, text.len() / CHARS_PER_TOKEN)
}

fn query_subgraph_tokens(graph: &QueryGraph, question: &str, depth: usize) -> usize {
    let terms: Vec<String> = question
        .split_whitespace()
        .filter(|term| term.len() > 2)
        .map(|term| term.to_lowercase())
        .collect();
    let scored = score_nodes(graph, &terms);
    let start_nodes: Vec<String> = scored
        .into_iter()
        .take(3)
        .map(|(_, node_id)| node_id)
        .collect();
    if start_nodes.is_empty() {
        return 0;
    }

    let (nodes, edges) = bfs(graph, &start_nodes, depth);
    estimate_tokens(&subgraph_to_text(graph, &nodes, &edges, usize::MAX / 8))
}

fn benchmark_value(graph: &QueryGraph, corpus_words: Option<usize>, questions: &[String]) -> Value {
    let effective_corpus_words = corpus_words.unwrap_or_else(|| graph.nodes.len() * 50);
    let corpus_tokens = effective_corpus_words * 100 / 75;
    let prompts: Vec<String> = if questions.is_empty() {
        SAMPLE_QUESTIONS
            .iter()
            .map(|question| (*question).to_string())
            .collect()
    } else {
        questions.to_vec()
    };

    let mut per_question: Vec<BenchmarkQuestionResult> = Vec::new();
    for question in prompts {
        let query_tokens = query_subgraph_tokens(graph, &question, 3);
        if query_tokens > 0 {
            per_question.push(BenchmarkQuestionResult {
                reduction: ((corpus_tokens as f64 / query_tokens as f64) * 10.0).round() / 10.0,
                question,
                query_tokens,
            });
        }
    }

    if per_question.is_empty() {
        return serde_json::json!({
            "error": "No matching nodes found for sample questions. Build the graph first."
        });
    }

    let avg_query_tokens = per_question
        .iter()
        .map(|entry| entry.query_tokens)
        .sum::<usize>()
        / per_question.len();
    let reduction_ratio = if avg_query_tokens > 0 {
        ((corpus_tokens as f64 / avg_query_tokens as f64) * 10.0).round() / 10.0
    } else {
        0.0
    };

    serde_json::to_value(BenchmarkResult {
        corpus_tokens,
        corpus_words: effective_corpus_words,
        nodes: graph.nodes.len(),
        edges: graph.edges.len(),
        avg_query_tokens,
        reduction_ratio,
        per_question,
    })
    .unwrap_or_else(|_| serde_json::json!({ "error": "Failed to encode benchmark result." }))
}

fn bfs(
    graph: &QueryGraph,
    start_nodes: &[String],
    depth: usize,
) -> (HashSet<String>, Vec<(String, String)>) {
    let mut visited: HashSet<String> = start_nodes.iter().cloned().collect();
    let mut frontier: Vec<String> = start_nodes.to_vec();
    let mut edges_seen: Vec<(String, String)> = Vec::new();

    for _ in 0..depth {
        let mut next_frontier: Vec<String> = Vec::new();
        let mut queued: HashSet<String> = HashSet::new();
        for node_id in &frontier {
            for neighbor in graph.neighbors(node_id) {
                if !visited.contains(&neighbor) && queued.insert(neighbor.clone()) {
                    next_frontier.push(neighbor.clone());
                    edges_seen.push((node_id.clone(), neighbor));
                }
            }
        }
        for node_id in &next_frontier {
            visited.insert(node_id.clone());
        }
        frontier = next_frontier;
    }

    (visited, edges_seen)
}

fn dfs(
    graph: &QueryGraph,
    start_nodes: &[String],
    depth: usize,
) -> (HashSet<String>, Vec<(String, String)>) {
    let mut visited: HashSet<String> = HashSet::new();
    let mut edges_seen: Vec<(String, String)> = Vec::new();
    let mut stack: Vec<(String, usize)> = start_nodes
        .iter()
        .rev()
        .cloned()
        .map(|node_id| (node_id, 0))
        .collect();

    while let Some((node_id, current_depth)) = stack.pop() {
        if visited.contains(&node_id) || current_depth > depth {
            continue;
        }
        visited.insert(node_id.clone());
        for neighbor in graph.neighbors(&node_id) {
            if !visited.contains(&neighbor) {
                stack.push((neighbor.clone(), current_depth + 1));
                edges_seen.push((node_id.clone(), neighbor));
            }
        }
    }

    (visited, edges_seen)
}

fn subgraph_to_text(
    graph: &QueryGraph,
    nodes: &HashSet<String>,
    edges: &[(String, String)],
    token_budget: usize,
) -> String {
    let char_budget = token_budget.saturating_mul(3);
    let mut lines: Vec<String> = Vec::new();
    let mut sorted_nodes: Vec<String> = nodes.iter().cloned().collect();
    sorted_nodes.sort_by(|left, right| {
        graph
            .degree(right)
            .cmp(&graph.degree(left))
            .then_with(|| left.cmp(right))
    });

    for node_id in sorted_nodes {
        let Some(node) = graph.node(&node_id) else {
            continue;
        };
        lines.push(format!(
            "NODE {} [src={} loc={} community={}]",
            sanitize_label(&node.label),
            node.source_file,
            node.source_location,
            node.community.as_deref().unwrap_or_default()
        ));
    }

    for (source, target) in edges {
        if !nodes.contains(source) || !nodes.contains(target) {
            continue;
        }
        let edge = graph.edge(source, target).cloned().unwrap_or_default();
        let source_label = graph
            .node(source)
            .map(|node| sanitize_label(&node.label))
            .unwrap_or_else(|| source.clone());
        let target_label = graph
            .node(target)
            .map(|node| sanitize_label(&node.label))
            .unwrap_or_else(|| target.clone());
        lines.push(format!(
            "EDGE {} --{} [{}]--> {}",
            source_label, edge.relation, edge.confidence, target_label
        ));
    }

    let mut output = lines.join("\n");
    if output.len() > char_budget {
        output = truncate_to_boundary(&output, char_budget);
        output.push_str(&format!(
            "\n... (truncated to ~{token_budget} token budget)"
        ));
    }
    output
}

fn find_node(graph: &QueryGraph, label: &str) -> Vec<String> {
    let term = strip_diacritics(label).to_lowercase();
    graph
        .nodes
        .iter()
        .filter_map(|(node_id, node)| {
            if node.norm_label.contains(&term) || term == node_id.to_lowercase() {
                Some(node_id.clone())
            } else {
                None
            }
        })
        .collect()
}

fn format_neighbors(graph: &QueryGraph, label: &str, relation_filter: Option<&str>) -> String {
    let matches = find_node(graph, label);
    if matches.is_empty() {
        return format!("No node matching '{}' found.", label.to_lowercase());
    }

    let node_id = &matches[0];
    let node_label = graph
        .node(node_id)
        .map(|node| node.label.clone())
        .unwrap_or_else(|| node_id.clone());
    let filter = relation_filter.unwrap_or_default().to_lowercase();
    let mut lines = vec![format!("Neighbors of {}:", node_label)];
    let mut neighbors = graph.neighbors(node_id);
    neighbors.sort();
    for neighbor_id in neighbors {
        let edge = graph
            .edge(node_id, &neighbor_id)
            .cloned()
            .unwrap_or_default();
        if !filter.is_empty() && !edge.relation.to_lowercase().contains(&filter) {
            continue;
        }
        let neighbor_label = graph
            .node(&neighbor_id)
            .map(|node| node.label.clone())
            .unwrap_or(neighbor_id);
        lines.push(format!(
            "  --> {} [{}] [{}]",
            neighbor_label, edge.relation, edge.confidence
        ));
    }
    lines.join("\n")
}

fn format_community(graph: &QueryGraph, community_id: usize) -> String {
    let cid = community_id.to_string();
    let mut members: Vec<&QueryNode> = graph
        .nodes
        .values()
        .filter(|node| node.community.as_deref() == Some(cid.as_str()))
        .collect();
    if members.is_empty() {
        return format!("Community {} not found.", community_id);
    }
    members.sort_by(|left, right| left.label.cmp(&right.label).then(left.id.cmp(&right.id)));
    let mut lines = vec![format!(
        "Community {} ({} nodes):",
        community_id,
        members.len()
    )];
    for node in members {
        lines.push(format!("  {} [{}]", node.label, node.source_file));
    }
    lines.join("\n")
}

fn format_god_nodes(graph: &QueryGraph, top_n: usize) -> String {
    let mut ranked: Vec<(usize, &QueryNode)> = graph
        .nodes
        .values()
        .filter_map(|node| {
            let degree = graph.degree(node.id.as_str());
            if is_file_node(node, degree) || is_concept_node(node) {
                None
            } else {
                Some((degree, node))
            }
        })
        .collect();
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.id.cmp(&right.1.id)));

    let mut lines = vec!["God nodes (most connected):".to_string()];
    for (index, (degree, node)) in ranked.into_iter().take(top_n).enumerate() {
        lines.push(format!(
            "  {}. {} - {} edges",
            index + 1,
            node.label,
            degree
        ));
    }
    lines.join("\n")
}

fn format_stats(graph: &QueryGraph) -> String {
    let communities: HashSet<String> = graph
        .nodes
        .values()
        .filter_map(|node| node.community.clone())
        .collect();
    let mut extracted = 0usize;
    let mut inferred = 0usize;
    let mut ambiguous = 0usize;
    for edge in graph.edges.values() {
        match edge.confidence.as_str() {
            "INFERRED" => inferred += 1,
            "AMBIGUOUS" => ambiguous += 1,
            _ => extracted += 1,
        }
    }
    let total = graph.edges.len().max(1) as f64;
    format!(
        "Nodes: {}\nEdges: {}\nCommunities: {}\nEXTRACTED: {}%\nINFERRED: {}%\nAMBIGUOUS: {}%\n",
        graph.nodes.len(),
        graph.edges.len(),
        communities.len(),
        ((extracted as f64 / total) * 100.0).round() as usize,
        ((inferred as f64 / total) * 100.0).round() as usize,
        ((ambiguous as f64 / total) * 100.0).round() as usize,
    )
}

fn is_file_node(node: &QueryNode, degree: usize) -> bool {
    if node.source_file.is_empty() {
        return false;
    }
    if let Some(filename) = node.source_file.rsplit('/').next()
        && node.label == filename {
            return true;
        }
    if node.label.starts_with('.') && node.label.ends_with("()") {
        return true;
    }
    node.label.ends_with("()") && degree <= 1
}

fn is_concept_node(node: &QueryNode) -> bool {
    if node.source_file.is_empty() {
        return true;
    }
    node.source_file
        .rsplit('/')
        .next()
        .is_some_and(|filename| !filename.contains('.'))
}

fn shortest_path(graph: &QueryGraph, start: &str, goal: &str) -> Option<Vec<String>> {
    if start == goal {
        return Some(vec![start.to_string()]);
    }

    let mut queue: VecDeque<String> = VecDeque::from([start.to_string()]);
    let mut previous: HashMap<String, Option<String>> = HashMap::new();
    previous.insert(start.to_string(), None);

    while let Some(node_id) = queue.pop_front() {
        for neighbor in graph.neighbors(&node_id) {
            if previous.contains_key(&neighbor) {
                continue;
            }
            previous.insert(neighbor.clone(), Some(node_id.clone()));
            if neighbor == goal {
                return Some(reconstruct_path(previous, goal));
            }
            queue.push_back(neighbor);
        }
    }

    None
}

fn reconstruct_path(previous: HashMap<String, Option<String>>, goal: &str) -> Vec<String> {
    let mut current = Some(goal.to_string());
    let mut path: Vec<String> = Vec::new();

    while let Some(node_id) = current {
        current = previous.get(&node_id).cloned().unwrap_or(None);
        path.push(node_id);
    }

    path.reverse();
    path
}

fn sanitize_label(text: &str) -> String {
    text.chars()
        .filter(|ch| !ch.is_control())
        .take(256)
        .collect()
}

fn truncate_to_boundary(text: &str, max_len: usize) -> String {
    if max_len >= text.len() {
        return text.to_string();
    }

    let boundary = text
        .char_indices()
        .take_while(|(index, _)| *index < max_len)
        .last()
        .map_or(0, |(index, ch)| index + ch.len_utf8());
    text[..boundary].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_graph() -> Value {
        serde_json::json!({
            "nodes": [
                {
                    "id": "n1",
                    "label": "Crème Parser",
                    "source_file": "parser.py",
                    "source_location": "L10",
                    "file_type": "code",
                    "community": 0,
                    "norm_label": "creme parser"
                },
                {
                    "id": "n2",
                    "label": "Renderer",
                    "source_file": "renderer.py",
                    "source_location": "L20",
                    "file_type": "code",
                    "community": 1
                },
                {
                    "id": "n3",
                    "label": "Report",
                    "source_file": "report.md",
                    "source_location": "L1",
                    "file_type": "document",
                    "community": 1
                }
            ],
            "links": [
                {
                    "source": "n1",
                    "target": "n2",
                    "relation": "uses",
                    "confidence": "INFERRED"
                },
                {
                    "source": "n2",
                    "target": "n3",
                    "relation": "references",
                    "confidence": "EXTRACTED"
                }
            ]
        })
    }

    #[test]
    fn score_nodes_uses_folded_norm_label_and_source_file() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let scored = score_nodes(&graph, &["creme".to_string(), "renderer".to_string()]);
        let ranked: Vec<String> = scored.into_iter().map(|(_, node_id)| node_id).collect();
        assert_eq!(ranked[0], "n2");
        assert!(ranked.contains(&"n1".to_string()));
    }

    #[test]
    fn shortest_path_finds_two_hop_chain() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let path = shortest_path(&graph, "n1", "n3").expect("path");
        assert_eq!(path, vec!["n1", "n2", "n3"]);
    }

    #[test]
    fn subgraph_to_text_includes_nodes_edges_and_truncation() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let nodes = HashSet::from(["n1".to_string(), "n2".to_string(), "n3".to_string()]);
        let edges = vec![
            ("n1".to_string(), "n2".to_string()),
            ("n2".to_string(), "n3".to_string()),
        ];
        let text = subgraph_to_text(&graph, &nodes, &edges, 200);
        assert!(text.contains("NODE Crème Parser"));
        assert!(text.contains("EDGE"));
        let truncated = subgraph_to_text(&graph, &nodes, &edges, 1);
        assert!(truncated.contains("truncated"));
    }

    #[test]
    fn load_graph_rejects_non_json_suffix() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("graph.txt");
        fs::write(&path, "{}").expect("write graph");
        let err = load_graph(&path).expect_err("expected suffix error");
        assert!(err.contains(".json"));
    }

    #[test]
    fn run_explain_cli_prints_success_for_known_node() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("graph.json");
        fs::write(&path, serde_json::to_string(&sample_graph()).expect("json"))
            .expect("write graph");
        assert_eq!(run_explain_cli(&path, "parser"), 0);
    }

    #[test]
    fn run_path_cli_enforces_max_hops() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("graph.json");
        fs::write(&path, serde_json::to_string(&sample_graph()).expect("json"))
            .expect("write graph");
        assert_eq!(run_path_cli(&path, "parser", "report", Some(1)), 0);
    }

    #[test]
    fn format_neighbors_and_stats_include_expected_fields() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let neighbors = format_neighbors(&graph, "parser", Some("use"));
        let stats = format_stats(&graph);
        assert!(neighbors.contains("Neighbors of Crème Parser:"));
        assert!(neighbors.contains("Renderer [uses] [INFERRED]"));
        assert!(stats.contains("Nodes: 3"));
        assert!(stats.contains("Edges: 2"));
    }

    #[test]
    fn format_community_and_god_nodes_are_stable() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let community = format_community(&graph, 1);
        let god_nodes = format_god_nodes(&graph, 2);
        assert!(community.contains("Community 1 (2 nodes):"));
        assert!(community.contains("Renderer [renderer.py]"));
        assert!(god_nodes.contains("God nodes (most connected):"));
        assert!(god_nodes.contains("Renderer - 2 edges"));
    }

    #[test]
    fn benchmark_value_returns_reduction_summary() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let result = benchmark_value(
            &graph,
            Some(5_000),
            &[String::from("parser"), String::from("renderer")],
        );
        assert_eq!(result["nodes"], 3);
        assert_eq!(result["edges"], 2);
        assert!(result["reduction_ratio"].as_f64().unwrap_or(0.0) > 1.0);
        assert!(
            result["per_question"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
    }

    #[test]
    fn benchmark_value_returns_error_when_no_questions_match() {
        let graph = QueryGraph::from_value(&sample_graph()).expect("graph");
        let result = benchmark_value(&graph, Some(1_000), &[String::from("xyzzy plugh zorkmid")]);
        assert!(result["error"].as_str().is_some());
    }
}
