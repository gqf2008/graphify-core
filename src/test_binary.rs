use std::collections::HashMap;
use std::fs;

use serde::Deserialize;
use graphify_core::layout;
use graphify_core::binary_schema;

#[derive(Debug, Deserialize)]
struct GraphJson {
    nodes: Vec<NodeJson>,
    links: Vec<EdgeJson>,
}

#[derive(Debug, Deserialize)]
struct NodeJson {
    id: String,
    label: String,
    #[serde(default)]
    source_file: String,
    community: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct EdgeJson {
    source: String,
    target: String,
    #[serde(default)]
    relation: String,
    #[serde(default)]
    confidence: String,
    #[serde(default = "default_weight")]
    weight: f64,
}

fn default_weight() -> f64 { 1.0 }

fn main() {
    let t_load = std::time::Instant::now();
    let data = fs::read_to_string("/Users/sqb/Documents/GitHub/openclaw/graphify-out/graph.json").unwrap();
    let gj: GraphJson = serde_json::from_str(&data).unwrap();
    println!("Loaded in {:.1}s: {} nodes, {} edges", t_load.elapsed().as_secs_f64(), gj.nodes.len(), gj.links.len());

    // Build internal Graph
    let mut graph = graphify_core::build::Graph::default();
    for n in &gj.nodes {
        graph.nodes.push(graphify_core::schema::Node {
            id: n.id.clone(),
            label: n.label.clone(),
            source_file: n.source_file.clone(),
            ..Default::default()
        });
    }
    for e in &gj.links {
        graph.edges.push(graphify_core::schema::Edge {
            source: e.source.clone(),
            target: e.target.clone(),
            relation: e.relation.clone(),
            confidence: e.confidence.clone(),
            weight: e.weight,
            ..Default::default()
        });
    }

    // Build communities from JSON
    let mut communities: HashMap<usize, Vec<String>> = HashMap::new();
    for n in &gj.nodes {
        if let Some(comm) = n.community {
            communities.entry(comm).or_default().push(n.id.clone());
        }
    }
    println!("Communities from JSON: {}", communities.len());

    let community_labels: HashMap<usize, String> = communities
        .keys().copied()
        .map(|cid| (cid, format!("Community {}", cid)))
        .collect();

    // Compute layout
    let t0 = std::time::Instant::now();
    let layout_data = layout::compute_layout(&graph, &communities);
    println!("Layout computed in {:.1}s", t0.elapsed().as_secs_f64());

    // Encode
    let t1 = std::time::Instant::now();
    let bin_data = binary_schema::encode(&graph, &communities, &community_labels, &layout_data);
    println!("Encoded in {:.2}s, size: {:.1} MB", t1.elapsed().as_secs_f64(), bin_data.len() as f64 / 1e6);

    // Decode
    let t2 = std::time::Instant::now();
    let decoded = binary_schema::decode(&bin_data).unwrap();
    println!("Decoded in {:.2}s: {} nodes, {} edges, {} communities",
        t2.elapsed().as_secs_f64(), decoded.nodes.len(), decoded.edges.len(), decoded.communities.len());

    fs::write("/Users/sqb/Documents/GitHub/openclaw/graphify-out/graph.bin", &bin_data).unwrap();
    println!("Written graph.bin ({:.1} MB)", bin_data.len() as f64 / 1e6);
}
