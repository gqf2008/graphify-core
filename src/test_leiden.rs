use std::collections::{HashMap, HashSet};
use rustworkx_core::petgraph::graph::{UnGraph, NodeIndex};
use rustworkx_core::petgraph::visit::EdgeRef;
use rustworkx_core::community::leiden_communities;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Node { id: String }
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Edge { source: String, target: String, #[serde(default)] value: f64 }
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Graph { nodes: Vec<Node>, links: Vec<Edge> }

fn compute_modularity(pg_graph: &UnGraph<(), f64>, communities: &rustworkx_core::dictmap::DictMap<NodeIndex, u32>) -> (f64, usize) {
    let num_communities: HashSet<u32> = communities.values().copied().collect();
    let m = pg_graph.edge_count() as f64;
    let mut q = 0.0;

    for comm in &num_communities {
        let members: Vec<NodeIndex> = communities.iter()
            .filter(|(_, c)| **c == *comm)
            .map(|(ni, _)| *ni)
            .collect();
        let member_set: HashSet<NodeIndex> = members.iter().copied().collect();
        for &node_id in &members {
            let k_i = pg_graph.edges(node_id).count() as f64;
            for neighbor in pg_graph.neighbors(node_id) {
                if member_set.contains(&neighbor) {
                    let edge_weight = pg_graph.find_edge(node_id, neighbor)
                        .and_then(|e| pg_graph.edge_weight(e))
                        .copied()
                        .unwrap_or(1.0);
                    let k_j = pg_graph.edges(neighbor).count() as f64;
                    q += edge_weight - (k_i * k_j) / (2.0 * m);
                }
            }
        }
    }
    q /= 2.0 * m;
    (q, num_communities.len())
}

fn main() {
    let data = std::fs::read_to_string("/Users/sqb/Documents/GitHub/openclaw/graphify-out/graph.json").unwrap();
    let graph: Graph = serde_json::from_str(&data).unwrap();

    let mut has_edge: HashMap<String, bool> = HashMap::new();
    for node in graph.nodes.iter() { has_edge.insert(node.id.clone(), false); }
    for edge in &graph.links {
        if edge.source != edge.target {
            has_edge.insert(edge.source.clone(), true);
            has_edge.insert(edge.target.clone(), true);
        }
    }

    let mut pg_graph = UnGraph::<(), f64>::new_undirected();
    let mut node_indices: HashMap<String, NodeIndex> = HashMap::new();
    for node in graph.nodes.iter() {
        if has_edge[&node.id] {
            let idx = pg_graph.add_node(());
            node_indices.insert(node.id.clone(), idx);
        }
    }
    for edge in &graph.links {
        if let (Some(&src), Some(&tgt)) = (node_indices.get(&edge.source), node_indices.get(&edge.target))
            && src != tgt {
                let weight = if edge.value == 0.0 { 1.0 } else { edge.value };
                pg_graph.add_edge(src, tgt, weight);
            }
    }
    println!("Graph: {} nodes, {} edges", pg_graph.node_count(), pg_graph.edge_count());

    let communities = leiden_communities(&pg_graph, Some(1), Some(1.0), Some(0.001), Some(42));
    let (q, nc) = compute_modularity(&pg_graph, &communities);

    // Intra-community edges
    let mut intra = 0usize;
    let mut total = 0usize;
    for edge in pg_graph.edge_references() {
        let src_comm = communities[&edge.source()];
        let tgt_comm = communities[&edge.target()];
        total += 1;
        if src_comm == tgt_comm { intra += 1; }
    }

    println!("\n=== AFTER FIX ===");
    println!("communities={}, Q={:.4}", nc, q);
    println!("Intra-community edges: {} / {} = {:.1}%", intra, total, 100.0 * intra as f64 / total as f64);
}
