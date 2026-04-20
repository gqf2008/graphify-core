use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::build::Graph;

const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinGraph {
    pub version: u32,
    pub strings: Vec<String>,
    pub nodes: Vec<BinNode>,
    pub edges: Vec<BinEdge>,
    pub communities: Vec<BinCommunity>,
    pub positions: Vec<(f32, f32)>,
    pub community_positions: Vec<(f32, f32)>,
    pub community_radii: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinNode {
    pub id: u32,
    pub label: u32,
    pub source_file: u32,
    pub community: u32,
    pub degree: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinEdge {
    pub source: u32,
    pub target: u32,
    pub relation: u32,
    pub weight: f32,
    pub confidence: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinCommunity {
    pub label: u32,
    pub node_start: u32,
    pub node_end: u32,
    pub cross_edges: u32,
}

struct StringTable {
    strings: Vec<String>,
    index: HashMap<String, u32>,
}

impl StringTable {
    fn new() -> Self {
        Self {
            strings: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&idx) = self.index.get(s) {
            return idx;
        }
        let idx = self.strings.len() as u32;
        self.strings.push(s.to_string());
        self.index.insert(s.to_string(), idx);
        idx
    }
}

pub struct LayoutData {
    pub positions: Vec<(f32, f32)>,
    pub community_positions: Vec<(f32, f32)>,
    pub community_radii: Vec<f32>,
}

pub fn encode(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    layout: &LayoutData,
) -> Vec<u8> {
    let mut st = StringTable::new();
    let node_count = graph.nodes.len();

    // Build node-id → index map
    let mut node_index: HashMap<&str, u32> = HashMap::with_capacity(node_count);
    for (i, node) in graph.nodes.iter().enumerate() {
        node_index.insert(&node.id, i as u32);
    }

    // Compute per-node degree
    let mut degree = vec![0u16; node_count];
    for edge in &graph.edges {
        if let (Some(&si), Some(&ti)) = (node_index.get(edge.source.as_str()), node_index.get(edge.target.as_str())) {
            degree[si as usize] = degree[si as usize].saturating_add(1);
            degree[ti as usize] = degree[ti as usize].saturating_add(1);
        }
    }

    // Build node-id → community-id map
    let mut node_community: HashMap<&str, u32> = HashMap::with_capacity(node_count);
    for (&comm_id, members) in communities {
        for member in members {
            node_community.insert(member.as_str(), comm_id as u32);
        }
    }

    // Encode nodes — sorted by community for locality
    let mut sorted_indices: Vec<usize> = (0..node_count).collect();
    sorted_indices.sort_by_key(|&i| node_community.get(graph.nodes[i].id.as_str()).copied().unwrap_or(u32::MAX));

    let mut old_to_new: Vec<u32> = vec![0; node_count];
    let mut bin_nodes: Vec<BinNode> = Vec::with_capacity(node_count);
    let mut bin_positions: Vec<(f32, f32)> = Vec::with_capacity(node_count);
    for (new_i, &old_i) in sorted_indices.iter().enumerate() {
        old_to_new[old_i] = new_i as u32;
        let node = &graph.nodes[old_i];
        bin_nodes.push(BinNode {
            id: st.intern(&node.id),
            label: st.intern(&node.label),
            source_file: st.intern(&node.source_file),
            community: node_community.get(node.id.as_str()).copied().unwrap_or(u32::MAX),
            degree: degree[old_i],
        });
        bin_positions.push(layout.positions.get(old_i).copied().unwrap_or((0.0, 0.0)));
    }

    // Encode edges with remapped node indices
    let mut bin_edges: Vec<BinEdge> = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        let Some(&si) = node_index.get(edge.source.as_str()) else { continue };
        let Some(&ti) = node_index.get(edge.target.as_str()) else { continue };
        bin_edges.push(BinEdge {
            source: old_to_new[si as usize],
            target: old_to_new[ti as usize],
            relation: st.intern(&edge.relation),
            weight: edge.weight as f32,
            confidence: match edge.confidence.as_str() {
                "EXTRACTED" => 0,
                "INFERRED" => 1,
                _ => 2,
            },
        });
    }

    // Encode communities (sorted by id)
    let max_comm = communities.keys().max().copied().unwrap_or(0);
    let mut bin_communities: Vec<BinCommunity> = Vec::with_capacity(max_comm + 1);
    for comm_id in 0..=max_comm {
        let _members = communities.get(&comm_id);
        let node_start = bin_nodes
            .iter()
            .position(|n| n.community == comm_id as u32)
            .unwrap_or(bin_nodes.len()) as u32;
        let node_end = bin_nodes
            .iter()
            .rposition(|n| n.community == comm_id as u32)
            .map(|p| (p + 1) as u32)
            .unwrap_or(node_start);

        let cross = count_cross_edges(&bin_edges, &bin_nodes, node_start, node_end);
        bin_communities.push(BinCommunity {
            label: st.intern(&community_labels.get(&comm_id).cloned().unwrap_or_default()),
            node_start,
            node_end,
            cross_edges: cross,
        });
    }

    let data = BinGraph {
        version: VERSION,
        strings: st.strings,
        nodes: bin_nodes,
        edges: bin_edges,
        communities: bin_communities,
        positions: bin_positions,
        community_positions: layout.community_positions.clone(),
        community_radii: layout.community_radii.clone(),
    };

    bincode::serde::encode_to_vec(&data, bincode::config::standard()).unwrap()
}

pub fn decode(data: &[u8]) -> Result<BinGraph, bincode::error::DecodeError> {
    let (graph, _): (BinGraph, usize) =
        bincode::serde::decode_from_slice(data, bincode::config::standard())?;
    Ok(graph)
}

fn count_cross_edges(edges: &[BinEdge], nodes: &[BinNode], start: u32, _end: u32) -> u32 {
    let mut count = 0u32;
    for edge in edges {
        let src_comm = nodes.get(edge.source as usize).map(|n| n.community);
        let tgt_comm = nodes.get(edge.target as usize).map(|n| n.community);
        if src_comm != tgt_comm {
            if src_comm == Some(start) || tgt_comm == Some(start) {
                // Approximation: count edges touching this community
            }
            count += 1;
        }
    }
    count
}
