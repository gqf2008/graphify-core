use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    path::Path,
};

use crate::schema::{Edge, Node};

// ── Graph building ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub hyperedges: Vec<serde_json::Value>,
    #[serde(default)]
    pub neighbor_order: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
}

/// Merge multiple extraction dicts into one, deduplicating nodes by ID.
pub fn merge_extractions(extractions: &[serde_json::Value]) -> Graph {
    let mut nodes: Vec<Node> = Vec::new();
    let mut node_positions: HashMap<String, usize> = HashMap::new();
    let mut all_edges: Vec<Edge> = Vec::new();
    let mut hyperedges: Vec<serde_json::Value> = Vec::new();
    let mut input_tokens: u32 = 0;
    let mut output_tokens: u32 = 0;

    for ext in extractions {
        if let Some(nodes_arr) = ext.get("nodes").and_then(|v| v.as_array()) {
            for n in nodes_arr {
                if let Ok(node) = serde_json::from_value::<Node>(n.clone()) {
                    let position = node_positions.entry(node.id.clone()).or_insert_with(|| {
                        let index = nodes.len();
                        nodes.push(Node::default());
                        index
                    });
                    nodes[*position] = node;
                }
            }
        }
        if let Some(edges_arr) = edge_values(ext) {
            for e in edges_arr {
                if let Some(edge) = normalize_edge(e) {
                    all_edges.push(edge);
                }
            }
        }
        if let Some(hyper) = ext.get("hyperedges").and_then(|v| v.as_array()) {
            for h in hyper {
                hyperedges.push(h.clone());
            }
        }
        input_tokens += ext
            .get("input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        output_tokens += ext
            .get("output_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
    }

    let node_ids: HashSet<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    let edges: Vec<Edge> = all_edges
        .into_iter()
        .filter(|e| node_ids.contains(e.source.as_str()) && node_ids.contains(e.target.as_str()))
        .collect();

    Graph {
        nodes,
        edges,
        hyperedges,
        neighbor_order: BTreeMap::new(),
        input_tokens,
        output_tokens,
    }
}

fn edge_values(extraction: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    extraction
        .get("edges")
        .or_else(|| extraction.get("links"))
        .and_then(|v| v.as_array())
}

fn normalize_edge(edge: &serde_json::Value) -> Option<Edge> {
    let mut value = edge.clone();
    let object = value.as_object_mut()?;

    if !object.contains_key("source") {
        if let Some(source) = object.get("from").cloned() {
            object.insert("source".to_string(), source);
        }
    }
    if !object.contains_key("target") {
        if let Some(target) = object.get("to").cloned() {
            object.insert("target".to_string(), target);
        }
    }

    serde_json::from_value::<Edge>(value).ok()
}

// ── Community detection (Louvain, Python-parity) ─────────────────────────────

const LOUVAIN_RESOLUTION: f64 = 1.0;
const LOUVAIN_THRESHOLD: f64 = 1e-4;
const LOUVAIN_MAX_LEVEL: usize = 10;
const LOUVAIN_SEED: u64 = 42;

#[derive(Debug, Clone)]
struct WeightedGraphLevel {
    members: Vec<HashSet<usize>>,
    adjacency: Vec<BTreeMap<usize, f64>>,
    neighbor_order: Vec<Vec<usize>>,
    edge_order: Vec<(usize, usize)>,
    pair_weights: BTreeMap<(usize, usize), f64>,
    degrees: Vec<f64>,
    total_weight: f64,
}

pub fn cluster(graph: &Graph) -> HashMap<usize, Vec<String>> {
    if graph.nodes.is_empty() {
        return HashMap::new();
    }

    let n = graph.nodes.len();
    let (node_index, adj, _) = graph_adjacency(graph);
    let weighted = weighted_graph_level(graph);

    if weighted.total_weight == 0.0 {
        let mut node_ids: Vec<String> = graph.nodes.iter().map(|node| node.id.clone()).collect();
        node_ids.sort();
        return node_ids
            .into_iter()
            .enumerate()
            .map(|(i, node_id)| (i, vec![node_id]))
            .collect();
    }

    // Match the Python implementation: cluster each non-isolate connected
    // component separately, then add isolates back as singleton communities.
    let mut communities: Vec<Vec<String>> = Vec::new();
    let connected_nodes: Vec<usize> = adj
        .iter()
        .enumerate()
        .filter_map(|(idx, neighbors)| (!neighbors.is_empty()).then_some(idx))
        .collect();

    if !connected_nodes.is_empty() {
        let connected_graph = induced_weighted_subgraph(&weighted, &connected_nodes);
        for group in louvain_groups(&connected_graph) {
            let nodes = group
                .into_iter()
                .map(|global_i| graph.nodes[global_i].id.clone())
                .collect();
            communities.push(nodes);
        }
    }

    for (global_i, neighbors) in adj.iter().enumerate() {
        if neighbors.is_empty() {
            communities.push(vec![graph.nodes[global_i].id.clone()]);
        }
    }

    // Split oversized communities (> 25% of graph, min 10 nodes)
    let max_size = std::cmp::max(10, (n as f64 * 0.25) as usize);
    let mut final_communities: Vec<Vec<String>> = Vec::new();

    for nodes in communities {
        if nodes.len() > max_size {
            let splits = split_community(&weighted, &nodes, &node_index);
            if splits.len() <= 1 {
                final_communities.push(nodes);
            } else {
                final_communities.extend(splits);
            }
        } else {
            final_communities.push(nodes);
        }
    }

    final_communities.iter_mut().for_each(|v| v.sort());
    final_communities.sort_by_key(|nodes| Reverse(nodes.len()));
    final_communities.into_iter().enumerate().collect()
}

fn weighted_graph_level(graph: &Graph) -> WeightedGraphLevel {
    let node_index: HashMap<&str, usize> = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.id.as_str(), idx))
        .collect();
    let mut pair_weights: BTreeMap<(usize, usize), f64> = BTreeMap::new();
    let mut neighbor_order = vec![Vec::new(); graph.nodes.len()];
    let mut seen_pairs = HashSet::new();
    let mut seen_neighbors = vec![HashSet::new(); graph.nodes.len()];

    if !graph.neighbor_order.is_empty() {
        for (node_id, neighbors) in &graph.neighbor_order {
            let Some(&src) = node_index.get(node_id.as_str()) else {
                continue;
            };
            for neighbor_id in neighbors {
                let Some(&tgt) = node_index.get(neighbor_id.as_str()) else {
                    continue;
                };
                if src == tgt || !seen_neighbors[src].insert(tgt) {
                    continue;
                }
                neighbor_order[src].push(tgt);
            }
        }
    }

    for edge in &graph.edges {
        let Some(&src) = node_index.get(edge.source.as_str()) else {
            continue;
        };
        let Some(&tgt) = node_index.get(edge.target.as_str()) else {
            continue;
        };
        let pair = if src <= tgt { (src, tgt) } else { (tgt, src) };
        if seen_pairs.insert(pair) {
            if src != tgt && graph.neighbor_order.is_empty() {
                neighbor_order[src].push(tgt);
                neighbor_order[tgt].push(src);
            }
        }
        pair_weights.insert(pair, normalize_weight(edge.weight));
    }

    weighted_level_from_parts(
        (0..graph.nodes.len())
            .map(|idx| HashSet::from([idx]))
            .collect(),
        pair_weights,
        neighbor_order,
    )
}

fn weighted_level_from_parts(
    members: Vec<HashSet<usize>>,
    pair_weights: BTreeMap<(usize, usize), f64>,
    neighbor_order: Vec<Vec<usize>>,
) -> WeightedGraphLevel {
    let mut adjacency: Vec<BTreeMap<usize, f64>> = vec![BTreeMap::new(); members.len()];
    let mut degrees = vec![0.0; members.len()];
    let mut total_weight = 0.0;

    for (&(u, v), &weight) in &pair_weights {
        total_weight += weight;
        if u == v {
            adjacency[u].insert(v, weight);
            degrees[u] += 2.0 * weight;
        } else {
            adjacency[u].insert(v, weight);
            adjacency[v].insert(u, weight);
            degrees[u] += weight;
            degrees[v] += weight;
        }
    }
    let edge_order = edge_iteration_order(members.len(), &neighbor_order, &pair_weights);

    WeightedGraphLevel {
        members,
        adjacency,
        neighbor_order,
        edge_order,
        pair_weights,
        degrees,
        total_weight,
    }
}

fn edge_iteration_order(
    node_count: usize,
    neighbor_order: &[Vec<usize>],
    pair_weights: &BTreeMap<(usize, usize), f64>,
) -> Vec<(usize, usize)> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    for u in 0..node_count {
        if pair_weights.contains_key(&(u, u)) && seen.insert((u, u)) {
            order.push((u, u));
        }
        for &v in &neighbor_order[u] {
            let pair = if u <= v { (u, v) } else { (v, u) };
            if seen.insert(pair) {
                order.push(pair);
            }
        }
    }
    order
}

fn induced_weighted_subgraph(level: &WeightedGraphLevel, nodes: &[usize]) -> WeightedGraphLevel {
    let global_local: HashMap<usize, usize> = nodes
        .iter()
        .enumerate()
        .map(|(local, &global)| (global, local))
        .collect();
    let members = nodes
        .iter()
        .map(|&global| level.members[global].clone())
        .collect::<Vec<_>>();
    let mut pair_weights = BTreeMap::new();
    let mut neighbor_order = vec![Vec::new(); nodes.len()];
    let mut seen_pairs = HashSet::new();

    for &(global_u, global_v) in &level.edge_order {
        let Some(&weight) = level.pair_weights.get(&(global_u, global_v)) else {
            continue;
        };
        let Some(&local_u) = global_local.get(&global_u) else {
            continue;
        };
        let Some(&local_v) = global_local.get(&global_v) else {
            continue;
        };
        let pair = if local_u <= local_v {
            (local_u, local_v)
        } else {
            (local_v, local_u)
        };
        pair_weights.insert(pair, normalize_weight(weight));
        if seen_pairs.insert(pair) {
            if local_u != local_v {
                neighbor_order[local_u].push(local_v);
                neighbor_order[local_v].push(local_u);
            }
        }
    }

    weighted_level_from_parts(members, pair_weights, neighbor_order)
}

fn louvain_groups(level: &WeightedGraphLevel) -> Vec<Vec<usize>> {
    if level.members.is_empty() {
        return Vec::new();
    }
    if level.members.len() == 1 || level.total_weight == 0.0 {
        return level
            .members
            .iter()
            .map(|members| members.iter().copied().collect())
            .collect();
    }

    let mut rng = PyRandom::new(LOUVAIN_SEED);
    let mut graph = level.clone();
    let mut modularity_score = modularity(
        &graph,
        &(0..graph.members.len())
            .map(|i| HashSet::from([i]))
            .collect::<Vec<_>>(),
    );
    let (mut partition, mut inner_partition, _) =
        louvain_one_level(&graph, graph.members.clone(), &mut rng);

    for level_idx in 0..LOUVAIN_MAX_LEVEL {
        let new_modularity = modularity(&graph, &inner_partition);

        if new_modularity - modularity_score <= LOUVAIN_THRESHOLD
            || level_idx + 1 >= LOUVAIN_MAX_LEVEL
        {
            return partition
                .into_iter()
                .filter(|members| !members.is_empty())
                .map(|members| {
                    let mut values: Vec<_> = members.into_iter().collect();
                    values.sort_unstable();
                    values
                })
                .collect();
        }

        modularity_score = new_modularity;
        graph = coarse_grain_graph(&graph, &inner_partition);
        let (next_partition, next_inner_partition, improvement) =
            louvain_one_level(&graph, partition.clone(), &mut rng);
        if !improvement {
            return partition
                .into_iter()
                .filter(|members| !members.is_empty())
                .map(|members| {
                    let mut values: Vec<_> = members.into_iter().collect();
                    values.sort_unstable();
                    values
                })
                .collect();
        }
        partition = next_partition;
        inner_partition = next_inner_partition;
    }

    partition
        .into_iter()
        .filter(|members| !members.is_empty())
        .map(|members| {
            let mut values: Vec<_> = members.into_iter().collect();
            values.sort_unstable();
            values
        })
        .collect()
}

fn louvain_one_level(
    graph: &WeightedGraphLevel,
    mut partition: Vec<HashSet<usize>>,
    rng: &mut PyRandom,
) -> (Vec<HashSet<usize>>, Vec<HashSet<usize>>, bool) {
    let node_count = graph.members.len();
    let mut node_to_community: Vec<usize> = (0..node_count).collect();
    let mut inner_partition: Vec<HashSet<usize>> =
        (0..node_count).map(|i| HashSet::from([i])).collect();
    let mut community_degree = graph.degrees.clone();
    let mut order: Vec<usize> = (0..node_count).collect();
    rng.shuffle(&mut order);

    let mut moves = 1usize;
    while moves > 0 {
        moves = 0;
        for &node in &order {
            let current_community = node_to_community[node];
            let weights_to_community =
                neighbor_community_weights(node, graph, &node_to_community);
            let degree = graph.degrees[node];
            community_degree[current_community] -= degree;
            let current_weight = weights_to_community
                .iter()
                .find_map(|(community, weight)| (*community == current_community).then_some(*weight))
                .unwrap_or(0.0);
            let remove_cost = -current_weight / graph.total_weight
                + LOUVAIN_RESOLUTION
                    * (community_degree[current_community] * degree)
                    / (2.0 * graph.total_weight * graph.total_weight);

            let mut best_gain = 0.0f64;
            let mut best_community = current_community;
            for (candidate_community, weight) in &weights_to_community {
                let gain = remove_cost
                    + weight / graph.total_weight
                    - LOUVAIN_RESOLUTION
                        * (community_degree[*candidate_community] * degree)
                        / (2.0 * graph.total_weight * graph.total_weight);
                if gain > best_gain {
                    best_gain = gain;
                    best_community = *candidate_community;
                }
            }

            community_degree[best_community] += degree;
            if best_community != current_community {
                for member in &graph.members[node] {
                    partition[current_community].remove(member);
                }
                inner_partition[current_community].remove(&node);
                partition[best_community].extend(graph.members[node].iter().copied());
                inner_partition[best_community].insert(node);
                node_to_community[node] = best_community;
                moves += 1;
            }
        }
    }

    partition.retain(|community| !community.is_empty());
    inner_partition.retain(|community| !community.is_empty());
    (partition, inner_partition, node_to_community.iter().enumerate().any(|(idx, cid)| *cid != idx))
}

fn neighbor_community_weights(
    node: usize,
    graph: &WeightedGraphLevel,
    node_to_community: &[usize],
) -> Vec<(usize, f64)> {
    let mut weights: Vec<(usize, f64)> = Vec::new();
    for &neighbor in &graph.neighbor_order[node] {
        let Some(&weight) = graph.adjacency[node].get(&neighbor) else {
            continue;
        };
        let community = node_to_community[neighbor];
        if let Some((_, total)) = weights.iter_mut().find(|(cid, _)| *cid == community) {
            *total += weight;
        } else {
            weights.push((community, weight));
        }
    }
    weights
}

fn modularity(graph: &WeightedGraphLevel, communities: &[HashSet<usize>]) -> f64 {
    if graph.total_weight == 0.0 {
        return 0.0;
    }

    let mut score = 0.0;
    for community in communities {
        if community.is_empty() {
            continue;
        }
        let total_degree: f64 = community.iter().map(|&node| graph.degrees[node]).sum();
        let mut intra_weight = 0.0;
        for (&(u, v), &weight) in &graph.pair_weights {
            if community.contains(&u) && community.contains(&v) {
                intra_weight += weight;
            }
        }
        score += intra_weight / graph.total_weight
            - LOUVAIN_RESOLUTION
                * (total_degree / (2.0 * graph.total_weight)).powi(2);
    }
    score
}

fn coarse_grain_graph(
    graph: &WeightedGraphLevel,
    inner_partition: &[HashSet<usize>],
) -> WeightedGraphLevel {
    let mut node_to_community = vec![0usize; graph.members.len()];
    let mut members = Vec::with_capacity(inner_partition.len());

    for (community_idx, community) in inner_partition.iter().enumerate() {
        let mut aggregate = HashSet::new();
        for &node in community {
            node_to_community[node] = community_idx;
            aggregate.extend(graph.members[node].iter().copied());
        }
        members.push(aggregate);
    }

    let mut pair_weights = BTreeMap::new();
    let mut neighbor_order = vec![Vec::new(); inner_partition.len()];
    let mut seen_pairs = HashSet::new();

    for &(u, v) in &graph.edge_order {
        let Some(&weight) = graph.pair_weights.get(&(u, v)) else {
            continue;
        };
        let community_u = node_to_community[u];
        let community_v = node_to_community[v];
        let pair = if community_u <= community_v {
            (community_u, community_v)
        } else {
            (community_v, community_u)
        };
        let entry = pair_weights.entry(pair).or_insert(0.0);
        *entry = normalize_weight(*entry + weight);
        if seen_pairs.insert(pair) {
            if community_u != community_v {
                neighbor_order[community_u].push(community_v);
                neighbor_order[community_v].push(community_u);
            }
        }
    }

    weighted_level_from_parts(members, pair_weights, neighbor_order)
}

#[derive(Debug, Clone)]
struct PyRandom {
    mt: [u32; 624],
    index: usize,
}

impl PyRandom {
    fn new(seed: u64) -> Self {
        let mut rng = Self {
            mt: [0; 624],
            index: 625,
        };
        rng.init_by_array(&[seed as u32]);
        rng
    }

    fn init_genrand(&mut self, seed: u32) {
        self.mt[0] = seed;
        for i in 1..624 {
            self.mt[i] = 1812433253u32
                .wrapping_mul(self.mt[i - 1] ^ (self.mt[i - 1] >> 30))
                .wrapping_add(i as u32);
        }
        self.index = 624;
    }

    fn init_by_array(&mut self, seed_words: &[u32]) {
        self.init_genrand(19650218);
        let mut i = 1usize;
        let mut j = 0usize;
        let mut k = 624usize.max(seed_words.len());

        while k > 0 {
            let mixed = (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1664525);
            self.mt[i] = (self.mt[i] ^ mixed)
                .wrapping_add(seed_words[j])
                .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
            if j >= seed_words.len() {
                j = 0;
            }
            k -= 1;
        }

        k = 623;
        while k > 0 {
            let mixed = (self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1566083941);
            self.mt[i] = (self.mt[i] ^ mixed).wrapping_sub(i as u32);
            i += 1;
            if i >= 624 {
                self.mt[0] = self.mt[623];
                i = 1;
            }
            k -= 1;
        }

        self.mt[0] = 0x8000_0000;
        self.index = 624;
    }

    fn genrand_uint32(&mut self) -> u32 {
        const N: usize = 624;
        const M: usize = 397;
        const MATRIX_A: u32 = 0x9908_b0df;
        const UPPER_MASK: u32 = 0x8000_0000;
        const LOWER_MASK: u32 = 0x7fff_ffff;

        if self.index >= N {
            for kk in 0..(N - M) {
                let y = (self.mt[kk] & UPPER_MASK) | (self.mt[kk + 1] & LOWER_MASK);
                self.mt[kk] = self.mt[kk + M] ^ (y >> 1) ^ if y & 1 == 0 { 0 } else { MATRIX_A };
            }
            for kk in (N - M)..(N - 1) {
                let y = (self.mt[kk] & UPPER_MASK) | (self.mt[kk + 1] & LOWER_MASK);
                self.mt[kk] =
                    self.mt[kk + M - N] ^ (y >> 1) ^ if y & 1 == 0 { 0 } else { MATRIX_A };
            }
            let y = (self.mt[N - 1] & UPPER_MASK) | (self.mt[0] & LOWER_MASK);
            self.mt[N - 1] = self.mt[M - 1] ^ (y >> 1) ^ if y & 1 == 0 { 0 } else { MATRIX_A };
            self.index = 0;
        }

        let mut y = self.mt[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y
    }

    fn getrandbits(&mut self, bits: u32) -> u32 {
        if bits == 0 {
            return 0;
        }
        let value = self.genrand_uint32();
        value >> (32 - bits)
    }

    fn randbelow(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        let bit_count = usize::BITS - upper.leading_zeros();
        loop {
            let value = self.getrandbits(bit_count) as usize;
            if value < upper {
                return value;
            }
        }
    }

    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            let j = self.randbelow(i + 1);
            values.swap(i, j);
        }
    }
}

fn split_community(
    weighted: &WeightedGraphLevel,
    nodes: &[String],
    node_to_idx: &HashMap<&str, usize>,
) -> Vec<Vec<String>> {
    let local_global: Vec<usize> = nodes
        .iter()
        .filter_map(|id| node_to_idx.get(id.as_str()).copied())
        .collect();
    let subgraph = induced_weighted_subgraph(weighted, &local_global);
    let groups = louvain_groups(&subgraph);
    if groups.len() <= 1 {
        return vec![nodes.to_vec()];
    }
    groups
        .into_iter()
        .map(|members| {
            let mut community: Vec<String> = members
                .into_iter()
                .map(|global_i| weighted.members[global_i].iter().next().copied().unwrap_or(global_i))
                .map(|global_i| nodes[local_global.iter().position(|&idx| idx == global_i).unwrap()].clone())
                .collect();
            community.sort();
            community
        })
        .collect()
}

// ── Analysis ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GodNode {
    pub id: String,
    pub label: String,
    pub edges: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurprisingConnection {
    pub source: String,
    pub target: String,
    pub source_files: Vec<String>,
    pub confidence: String,
    pub relation: String,
    #[serde(default)]
    pub why: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedQuestion {
    #[serde(rename = "type")]
    pub question_type: String,
    pub question: Option<String>,
    pub why: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub god_nodes: Vec<GodNode>,
    pub surprising_connections: Vec<SurprisingConnection>,
    pub suggested_questions: Vec<SuggestedQuestion>,
}

fn is_file_node(node: &Node, degree: usize) -> bool {
    if node.source_file.is_empty() {
        return false;
    }
    if let Some(filename) = node.source_file.rsplit('/').next() {
        if node.label == filename {
            return true;
        }
    }
    if node.label.starts_with('.') && node.label.ends_with("()") {
        return true;
    }
    if node.label.ends_with("()") && degree <= 1 {
        return true;
    }
    false
}

fn is_concept_node(node: &Node) -> bool {
    if node.source_file.is_empty() {
        return true;
    }
    if let Some(filename) = node.source_file.rsplit('/').next() {
        if !filename.contains('.') {
            return true;
        }
    }
    false
}

fn compute_degrees(graph: &Graph) -> HashMap<String, usize> {
    let mut deg: HashMap<String, usize> = HashMap::new();
    for edge in &graph.edges {
        *deg.entry(edge.source.clone()).or_default() += 1;
        *deg.entry(edge.target.clone()).or_default() += 1;
    }
    deg
}

fn graph_adjacency(
    graph: &Graph,
) -> (
    HashMap<&str, usize>,
    Vec<Vec<usize>>,
    HashMap<(usize, usize), &Edge>,
) {
    let node_index: HashMap<&str, usize> = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.id.as_str(), idx))
        .collect();
    let mut adj_sets: Vec<HashSet<usize>> = vec![HashSet::new(); graph.nodes.len()];
    let mut edge_map: HashMap<(usize, usize), &Edge> = HashMap::new();

    for edge in &graph.edges {
        let Some(&src) = node_index.get(edge.source.as_str()) else {
            continue;
        };
        let Some(&tgt) = node_index.get(edge.target.as_str()) else {
            continue;
        };
        if src == tgt {
            continue;
        }
        let pair = if src < tgt { (src, tgt) } else { (tgt, src) };
        adj_sets[src].insert(tgt);
        adj_sets[tgt].insert(src);
        edge_map.entry(pair).or_insert(edge);
    }

    let adjacency = adj_sets
        .into_iter()
        .map(|neighbors| {
            let mut values: Vec<usize> = neighbors.into_iter().collect();
            values.sort_unstable();
            values
        })
        .collect();

    (node_index, adjacency, edge_map)
}

fn brandes_centrality(graph: &Graph) -> (Vec<f64>, HashMap<(usize, usize), f64>) {
    let node_count = graph.nodes.len();
    let (_, adjacency, _) = graph_adjacency(graph);
    let mut node_scores = vec![0.0; node_count];
    let mut edge_scores: HashMap<(usize, usize), f64> = HashMap::new();

    for source in 0..node_count {
        let mut stack: Vec<usize> = Vec::new();
        let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); node_count];
        let mut sigma = vec![0.0; node_count];
        let mut distance = vec![-1isize; node_count];
        let mut queue = VecDeque::new();

        sigma[source] = 1.0;
        distance[source] = 0;
        queue.push_back(source);

        while let Some(vertex) = queue.pop_front() {
            stack.push(vertex);
            for &neighbor in &adjacency[vertex] {
                if distance[neighbor] < 0 {
                    queue.push_back(neighbor);
                    distance[neighbor] = distance[vertex] + 1;
                }
                if distance[neighbor] == distance[vertex] + 1 {
                    sigma[neighbor] += sigma[vertex];
                    predecessors[neighbor].push(vertex);
                }
            }
        }

        let mut delta = vec![0.0; node_count];
        while let Some(vertex) = stack.pop() {
            for &predecessor in &predecessors[vertex] {
                if sigma[vertex] == 0.0 {
                    continue;
                }
                let contribution = (sigma[predecessor] / sigma[vertex]) * (1.0 + delta[vertex]);
                delta[predecessor] += contribution;
                let pair = if predecessor < vertex {
                    (predecessor, vertex)
                } else {
                    (vertex, predecessor)
                };
                *edge_scores.entry(pair).or_insert(0.0) += contribution;
            }
            if vertex != source {
                node_scores[vertex] += delta[vertex];
            }
        }
    }

    for score in &mut node_scores {
        *score /= 2.0;
    }
    for score in edge_scores.values_mut() {
        *score /= 2.0;
    }

    if node_count > 2 {
        let scale = 2.0 / ((node_count as f64 - 1.0) * (node_count as f64 - 2.0));
        for score in &mut node_scores {
            *score *= scale;
        }
    }
    if node_count > 1 {
        let scale = 2.0 / (node_count as f64 * (node_count as f64 - 1.0));
        for score in edge_scores.values_mut() {
            *score *= scale;
        }
    }

    (node_scores, edge_scores)
}

pub fn god_nodes(graph: &Graph, top_n: usize) -> Vec<GodNode> {
    let degree = compute_degrees(graph);
    let mut result: Vec<GodNode> = Vec::new();

    let mut sorted: Vec<&Node> = graph.nodes.iter().collect();
    sorted.sort_by_key(|node| Reverse(degree.get(node.id.as_str()).copied().unwrap_or(0)));

    for node in sorted {
        let deg = degree.get(node.id.as_str()).copied().unwrap_or(0);
        if is_file_node(node, deg) || is_concept_node(node) {
            continue;
        }
        result.push(GodNode {
            id: node.id.clone(),
            label: node.label.clone(),
            edges: deg,
        });
        if result.len() >= top_n {
            break;
        }
    }

    result
}

pub fn surprising_connections(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    top_n: usize,
) -> Vec<SurprisingConnection> {
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();

    let node_map: HashMap<&str, &Node> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let source_files: HashSet<&str> = graph
        .nodes
        .iter()
        .filter_map(|n| {
            if n.source_file.is_empty() {
                None
            } else {
                Some(n.source_file.as_str())
            }
        })
        .collect();
    let is_multi_source = source_files.len() > 1;

    let degree = compute_degrees(graph);
    let mut candidates: Vec<(usize, SurprisingConnection)> = Vec::new();

    if is_multi_source {
        for edge in &graph.edges {
            if matches!(
                edge.relation.as_str(),
                "imports" | "imports_from" | "contains" | "method"
            ) {
                continue;
            }
            let src = match node_map.get(edge.source.as_str()) {
                Some(&n) => n,
                None => continue,
            };
            let tgt = match node_map.get(edge.target.as_str()) {
                Some(&n) => n,
                None => continue,
            };
            if is_file_node(src, *degree.get(&src.id).unwrap_or(&0))
                || is_file_node(tgt, *degree.get(&tgt.id).unwrap_or(&0))
            {
                continue;
            }
            if is_concept_node(src) || is_concept_node(tgt) {
                continue;
            }
            if src.source_file.is_empty()
                || tgt.source_file.is_empty()
                || src.source_file == tgt.source_file
            {
                continue;
            }

            let (score, reasons) = surprise_score(src, tgt, edge, &node_community, &degree);
            let (source, target, source_files) = display_edge_endpoints(edge, src, tgt, &node_map);
            candidates.push((
                score,
                SurprisingConnection {
                    source,
                    target,
                    source_files,
                    confidence: edge.confidence.clone(),
                    relation: edge.relation.clone(),
                    why: if reasons.is_empty() {
                        "cross-file semantic connection".to_string()
                    } else {
                        reasons.join("; ")
                    },
                    note: String::new(),
                },
            ));
        }
    }

    candidates.sort_by(|a, b| b.0.cmp(&a.0));
    let cross_file: Vec<SurprisingConnection> =
        candidates.into_iter().take(top_n).map(|(_, c)| c).collect();
    if !cross_file.is_empty() || is_multi_source {
        return cross_file;
    }

    cross_community_surprises(graph, communities, &degree, top_n)
}

fn surprise_score(
    src: &Node,
    tgt: &Node,
    edge: &Edge,
    node_community: &HashMap<&str, usize>,
    degree: &HashMap<String, usize>,
) -> (usize, Vec<String>) {
    let mut score: usize = 0;
    let mut reasons: Vec<String> = Vec::new();

    let conf_bonus = match edge.confidence.as_str() {
        "AMBIGUOUS" => 3,
        "INFERRED" => 2,
        _ => 1,
    };
    score += conf_bonus;
    if matches!(edge.confidence.as_str(), "AMBIGUOUS" | "INFERRED") {
        reasons.push(format!(
            "{} connection - not explicitly stated in source",
            edge.confidence.to_lowercase()
        ));
    }

    let cat_u = file_category(&src.source_file);
    let cat_v = file_category(&tgt.source_file);
    if cat_u != cat_v {
        score += 2;
        reasons.push(format!("crosses file types ({cat_u} ↔ {cat_v})"));
    }

    let dir_u = top_level_dir(&src.source_file);
    let dir_v = top_level_dir(&tgt.source_file);
    if dir_u != dir_v {
        score += 2;
        reasons.push("connects across different repos/directories".to_string());
    }

    if let (Some(&cu), Some(&cv)) = (
        node_community.get(src.id.as_str()),
        node_community.get(tgt.id.as_str()),
    ) {
        if cu != cv {
            score += 1;
            reasons.push("bridges separate communities".to_string());
        }
    }

    if edge.relation == "semantically_similar_to" {
        score = (score as f64 * 1.5).round() as usize;
        reasons.push("semantically similar concepts with no structural link".to_string());
    }

    let deg_u = *degree.get(&src.id).unwrap_or(&0);
    let deg_v = *degree.get(&tgt.id).unwrap_or(&0);
    if std::cmp::min(deg_u, deg_v) <= 2 && std::cmp::max(deg_u, deg_v) >= 5 {
        score += 1;
        let peripheral = if deg_u <= 2 { &src.label } else { &tgt.label };
        let hub = if deg_u <= 2 { &tgt.label } else { &src.label };
        reasons.push(format!(
            "peripheral node `{peripheral}` unexpectedly reaches hub `{hub}`"
        ));
    }

    (score, reasons)
}

fn edge_endpoint_ids(edge: &Edge) -> (&str, &str) {
    (
        edge.original_source.as_deref().unwrap_or(&edge.source),
        edge.original_target.as_deref().unwrap_or(&edge.target),
    )
}

fn display_edge_endpoints(
    edge: &Edge,
    src: &Node,
    tgt: &Node,
    node_map: &HashMap<&str, &Node>,
) -> (String, String, Vec<String>) {
    let (src_id, tgt_id) = edge_endpoint_ids(edge);
    let display_src = node_map.get(src_id).copied().unwrap_or(src);
    let display_tgt = node_map.get(tgt_id).copied().unwrap_or(tgt);
    (
        display_src.label.clone(),
        display_tgt.label.clone(),
        vec![
            display_src.source_file.clone(),
            display_tgt.source_file.clone(),
        ],
    )
}

fn cross_community_surprises(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    degree: &HashMap<String, usize>,
    top_n: usize,
) -> Vec<SurprisingConnection> {
    if communities.is_empty() {
        let (_, edge_scores) = brandes_centrality(graph);
        let node_map: HashMap<&str, &Node> =
            graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
        let (_, _, edge_map) = graph_adjacency(graph);
        let mut ranked: Vec<_> = edge_scores.into_iter().collect();
        ranked.sort_by(
            |((left_a, left_b), left_score), ((right_a, right_b), right_score)| {
                right_score
                    .total_cmp(left_score)
                    .then_with(|| left_a.cmp(right_a))
                    .then_with(|| left_b.cmp(right_b))
            },
        );

        return ranked
            .into_iter()
            .filter_map(|(pair, score)| {
                let edge = edge_map.get(&pair)?;
                let src = node_map.get(edge.source.as_str()).copied()?;
                let tgt = node_map.get(edge.target.as_str()).copied()?;
                let (source, target, source_files) =
                    display_edge_endpoints(edge, src, tgt, &node_map);
                Some(SurprisingConnection {
                    source,
                    target,
                    source_files,
                    confidence: edge.confidence.clone(),
                    relation: edge.relation.clone(),
                    why: String::new(),
                    note: format!("Bridges graph structure (betweenness={score:.3})"),
                })
            })
            .take(top_n)
            .collect();
    }

    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();
    let node_map: HashMap<&str, &Node> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let mut candidates: Vec<(usize, (usize, usize), SurprisingConnection)> = Vec::new();

    for edge in &graph.edges {
        let Some(&src) = node_map.get(edge.source.as_str()) else {
            continue;
        };
        let Some(&tgt) = node_map.get(edge.target.as_str()) else {
            continue;
        };
        if is_file_node(src, *degree.get(&src.id).unwrap_or(&0))
            || is_file_node(tgt, *degree.get(&tgt.id).unwrap_or(&0))
        {
            continue;
        }
        if matches!(
            edge.relation.as_str(),
            "imports" | "imports_from" | "contains" | "method"
        ) {
            continue;
        }
        let Some(&cid_u) = node_community.get(src.id.as_str()) else {
            continue;
        };
        let Some(&cid_v) = node_community.get(tgt.id.as_str()) else {
            continue;
        };
        if cid_u == cid_v {
            continue;
        }

        let (source, target, source_files) = display_edge_endpoints(edge, src, tgt, &node_map);
        let confidence_rank = match edge.confidence.as_str() {
            "AMBIGUOUS" => 0,
            "INFERRED" => 1,
            _ => 2,
        };
        candidates.push((
            confidence_rank,
            (cid_u.min(cid_v), cid_u.max(cid_v)),
            SurprisingConnection {
                source,
                target,
                source_files,
                confidence: edge.confidence.clone(),
                relation: edge.relation.clone(),
                why: format!("Bridges community {cid_u} → community {cid_v}"),
                note: String::new(),
            },
        ));
    }

    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    let mut seen_pairs: HashSet<(usize, usize)> = HashSet::new();
    let mut deduped = Vec::new();
    for (_confidence_rank, pair, connection) in candidates {
        if seen_pairs.insert(pair) {
            deduped.push(connection);
        }
        if deduped.len() >= top_n {
            break;
        }
    }
    deduped
}

fn file_category(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "py" | "js" | "ts" | "tsx" | "go" | "rs" | "java" | "c" | "h" | "cpp" | "cc" | "cxx"
        | "hpp" | "rb" | "cs" | "kt" | "kts" | "scala" | "php" | "swift" | "lua" | "toc"
        | "zig" | "ps1" | "ex" | "exs" | "m" | "mm" | "jl" | "vue" | "svelte" | "dart" | "v"
        | "sv" | "svh" => "code",
        "pdf" => "paper",
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" => "image",
        _ => "doc",
    }
}

fn top_level_dir(path: &str) -> &str {
    path.split('/').next().unwrap_or(path)
}

fn format_number(value: usize) -> String {
    let digits = value.to_string();
    let mut parts = Vec::new();
    let mut end = digits.len();
    while end > 3 {
        parts.push(digits[end - 3..end].to_string());
        end -= 3;
    }
    parts.push(digits[..end].to_string());
    parts.reverse();
    parts.join(",")
}

fn normalize_weight(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn trim_float(value: f64) -> String {
    let mut text = format!("{value:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn python_float(value: f64) -> String {
    let text = trim_float(value);
    if text.contains(['.', 'e', 'E']) {
        text
    } else {
        format!("{text}.0")
    }
}

fn decompose_hangul_syllable(out: &mut String, ch: char) -> bool {
    const S_BASE: u32 = 0xAC00;
    const L_BASE: u32 = 0x1100;
    const V_BASE: u32 = 0x1161;
    const T_BASE: u32 = 0x11A7;
    const L_COUNT: u32 = 19;
    const V_COUNT: u32 = 21;
    const T_COUNT: u32 = 28;
    const N_COUNT: u32 = V_COUNT * T_COUNT;
    const S_COUNT: u32 = L_COUNT * N_COUNT;

    let code = ch as u32;
    if !(S_BASE..S_BASE + S_COUNT).contains(&code) {
        return false;
    }

    let s_index = code - S_BASE;
    let l_index = s_index / N_COUNT;
    let v_index = (s_index % N_COUNT) / T_COUNT;
    let t_index = s_index % T_COUNT;

    if let Some(lead) = char::from_u32(L_BASE + l_index) {
        out.push(lead);
    }
    if let Some(vowel) = char::from_u32(V_BASE + v_index) {
        out.push(vowel);
    }
    if t_index != 0 {
        if let Some(tail) = char::from_u32(T_BASE + t_index) {
            out.push(tail);
        }
    }
    true
}

fn push_folded_char(out: &mut String, ch: char) {
    if decompose_hangul_syllable(out, ch) {
        return;
    }

    match ch {
        '\u{3000}' => out.push(' '),
        '\u{FF01}'..='\u{FF5E}' => {
            if let Some(ascii) = char::from_u32(ch as u32 - 0xFEE0) {
                out.push(ascii);
            } else {
                out.push(ch);
            }
        }
        '…' => out.push_str("..."),
        'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' | 'Ǎ' | 'Ǟ' | 'Ǻ' => {
            out.push('A')
        }
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' | 'ǎ' | 'ǟ' | 'ǻ' => {
            out.push('a')
        }
        'Æ' | 'Ǽ' => out.push_str("AE"),
        'æ' | 'ǽ' => out.push_str("ae"),
        'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' => out.push('C'),
        'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => out.push('c'),
        'Ð' | 'Ď' | 'Đ' => out.push('D'),
        'ð' | 'ď' | 'đ' => out.push('d'),
        'È' | 'É' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => out.push('E'),
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => out.push('e'),
        'Ĝ' | 'Ğ' | 'Ġ' | 'Ģ' => out.push('G'),
        'ĝ' | 'ğ' | 'ġ' | 'ģ' => out.push('g'),
        'Ĥ' | 'Ħ' => out.push('H'),
        'ĥ' | 'ħ' => out.push('h'),
        'Ì' | 'Í' | 'Î' | 'Ï' | 'Ĩ' | 'Ī' | 'Ĭ' | 'Į' | 'İ' | 'Ǐ' => out.push('I'),
        'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' | 'ı' | 'ǐ' => out.push('i'),
        'Ĵ' => out.push('J'),
        'ĵ' => out.push('j'),
        'Ķ' => out.push('K'),
        'ķ' | 'ĸ' => out.push('k'),
        'Ĺ' | 'Ļ' | 'Ľ' | 'Ŀ' | 'Ł' => out.push('L'),
        'ĺ' | 'ļ' | 'ľ' | 'ŀ' | 'ł' => out.push('l'),
        'Ñ' | 'Ń' | 'Ņ' | 'Ň' | 'Ŋ' => out.push('N'),
        'ñ' | 'ń' | 'ņ' | 'ň' | 'ŉ' | 'ŋ' => out.push('n'),
        'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' | 'Ō' | 'Ŏ' | 'Ő' | 'Ǒ' | 'Ǿ' => out.push('O'),
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' | 'ǒ' | 'ǿ' => out.push('o'),
        'Œ' => out.push_str("OE"),
        'œ' => out.push_str("oe"),
        'Ŕ' | 'Ŗ' | 'Ř' => out.push('R'),
        'ŕ' | 'ŗ' | 'ř' => out.push('r'),
        'Ś' | 'Ŝ' | 'Ş' | 'Š' => out.push('S'),
        'ś' | 'ŝ' | 'ş' | 'š' => out.push('s'),
        'ß' => out.push_str("ss"),
        'Ţ' | 'Ť' | 'Ŧ' => out.push('T'),
        'ţ' | 'ť' | 'ŧ' => out.push('t'),
        'Ù' | 'Ú' | 'Û' | 'Ü' | 'Ũ' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' | 'Ǔ' => out.push('U'),
        'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' | 'ǔ' => out.push('u'),
        'Ŵ' => out.push('W'),
        'ŵ' => out.push('w'),
        'Ý' | 'Ŷ' | 'Ÿ' => out.push('Y'),
        'ý' | 'ÿ' | 'ŷ' => out.push('y'),
        'Ź' | 'Ż' | 'Ž' => out.push('Z'),
        'ź' | 'ż' | 'ž' => out.push('z'),
        'が' | 'ぎ' | 'ぐ' | 'げ' | 'ご' => match ch {
            'が' => out.push('か'),
            'ぎ' => out.push('き'),
            'ぐ' => out.push('く'),
            'げ' => out.push('け'),
            _ => out.push('こ'),
        },
        'ざ' | 'じ' | 'ず' | 'ぜ' | 'ぞ' => match ch {
            'ざ' => out.push('さ'),
            'じ' => out.push('し'),
            'ず' => out.push('す'),
            'ぜ' => out.push('せ'),
            _ => out.push('そ'),
        },
        'だ' | 'ぢ' | 'づ' | 'で' | 'ど' => match ch {
            'だ' => out.push('た'),
            'ぢ' => out.push('ち'),
            'づ' => out.push('つ'),
            'で' => out.push('て'),
            _ => out.push('と'),
        },
        'ば' | 'び' | 'ぶ' | 'べ' | 'ぼ' => match ch {
            'ば' => out.push('は'),
            'び' => out.push('ひ'),
            'ぶ' => out.push('ふ'),
            'べ' => out.push('へ'),
            _ => out.push('ほ'),
        },
        'ぱ' | 'ぴ' | 'ぷ' | 'ぺ' | 'ぽ' => match ch {
            'ぱ' => out.push('は'),
            'ぴ' => out.push('ひ'),
            'ぷ' => out.push('ふ'),
            'ぺ' => out.push('へ'),
            _ => out.push('ほ'),
        },
        'ゔ' => out.push('う'),
        'ガ' | 'ギ' | 'グ' | 'ゲ' | 'ゴ' => match ch {
            'ガ' => out.push('カ'),
            'ギ' => out.push('キ'),
            'グ' => out.push('ク'),
            'ゲ' => out.push('ケ'),
            _ => out.push('コ'),
        },
        'ザ' | 'ジ' | 'ズ' | 'ゼ' | 'ゾ' => match ch {
            'ザ' => out.push('サ'),
            'ジ' => out.push('シ'),
            'ズ' => out.push('ス'),
            'ゼ' => out.push('セ'),
            _ => out.push('ソ'),
        },
        'ダ' | 'ヂ' | 'ヅ' | 'デ' | 'ド' => match ch {
            'ダ' => out.push('タ'),
            'ヂ' => out.push('チ'),
            'ヅ' => out.push('ツ'),
            'デ' => out.push('テ'),
            _ => out.push('ト'),
        },
        'バ' | 'ビ' | 'ブ' | 'ベ' | 'ボ' => match ch {
            'バ' => out.push('ハ'),
            'ビ' => out.push('ヒ'),
            'ブ' => out.push('フ'),
            'ベ' => out.push('ヘ'),
            _ => out.push('ホ'),
        },
        'パ' | 'ピ' | 'プ' | 'ペ' | 'ポ' => match ch {
            'パ' => out.push('ハ'),
            'ピ' => out.push('ヒ'),
            'プ' => out.push('フ'),
            'ペ' => out.push('ヘ'),
            _ => out.push('ホ'),
        },
        'ヴ' => out.push('ウ'),
        _ => out.push(ch),
    }
}

pub(crate) fn strip_diacritics(text: &str) -> String {
    let mut folded = String::with_capacity(text.len());
    for ch in text.chars() {
        push_folded_char(&mut folded, ch);
    }
    folded
}

const COMMUNITY_COLORS: [&str; 10] = [
    "#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F", "#EDC948", "#B07AA1", "#FF9DA7",
    "#9C755F", "#BAB0AC",
];

fn safe_note_name(label: &str) -> String {
    let mut cleaned = String::new();
    for ch in label
        .replace("\r\n", " ")
        .replace('\r', " ")
        .replace('\n', " ")
        .chars()
    {
        if !matches!(
            ch,
            '\\' | '/' | '*' | '?' | ':' | '"' | '<' | '>' | '|' | '#' | '^' | '[' | ']'
        ) {
            cleaned.push(ch);
        }
    }
    let trimmed = cleaned.trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(stripped) = lower
        .strip_suffix(".md")
        .or_else(|| lower.strip_suffix(".mdx"))
        .or_else(|| lower.strip_suffix(".markdown"))
    {
        let keep_len = stripped.len();
        let original = &trimmed[..keep_len];
        if original.is_empty() {
            "unnamed".to_string()
        } else {
            original.to_string()
        }
    } else if trimmed.is_empty() {
        "unnamed".to_string()
    } else {
        trimmed.to_string()
    }
}

fn build_node_filenames(graph: &Graph) -> HashMap<String, String> {
    let mut node_filename: HashMap<String, String> = HashMap::new();
    let mut seen_names: HashMap<String, usize> = HashMap::new();
    for node in &graph.nodes {
        let base = safe_note_name(&node.label);
        let name = if let Some(count) = seen_names.get_mut(&base) {
            *count += 1;
            format!("{}_{}", base, count)
        } else {
            seen_names.insert(base.clone(), 0);
            base
        };
        node_filename.insert(node.id.clone(), name);
    }
    node_filename
}

fn merged_node_filenames(
    graph: &Graph,
    provided: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut node_filename = provided.clone();
    for (node_id, filename) in build_node_filenames(graph) {
        node_filename.entry(node_id).or_insert(filename);
    }
    node_filename
}

fn svg_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn safe_wiki_filename(name: &str) -> String {
    name.replace('/', "-").replace(' ', "_").replace(':', "-")
}

fn safe_community_name(label: &str) -> String {
    safe_note_name(label)
}

fn cypher_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\'', "\\'")
}

fn cypher_key(key: &str) -> String {
    let sanitized: String = key
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "_prop".to_string()
    } else if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        format!("_{sanitized}")
    } else {
        sanitized
    }
}

fn cypher_string(text: &str) -> String {
    format!("'{}'", cypher_escape(text))
}

fn cypher_literal(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => Some(cypher_string(text)),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(flag) => Some(if *flag { "true" } else { "false" }.to_string()),
        _ => None,
    }
}

fn push_cypher_string(props: &mut BTreeMap<String, String>, key: &str, value: &str) {
    if !value.is_empty() {
        props.insert(cypher_key(key), cypher_string(value));
    }
}

fn push_cypher_opt_string(props: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(text) = value.filter(|text| !text.is_empty()) {
        props.insert(cypher_key(key), cypher_string(text));
    }
}

fn push_cypher_float(props: &mut BTreeMap<String, String>, key: &str, value: f64) {
    if value.is_finite() {
        props.insert(cypher_key(key), value.to_string());
    }
}

fn push_cypher_extra_props(
    props: &mut BTreeMap<String, String>,
    extra: &BTreeMap<String, serde_json::Value>,
) {
    for (key, value) in extra {
        let Some(literal) = cypher_literal(value) else {
            continue;
        };
        props.entry(cypher_key(key)).or_insert(literal);
    }
}

fn cypher_props_map(props: &BTreeMap<String, String>) -> String {
    let body = props
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{body}}}")
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn js_safe_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "null".to_string())
        .replace("</", "<\\/")
}

pub fn suggest_questions(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    top_n: usize,
) -> Vec<SuggestedQuestion> {
    let mut questions: Vec<SuggestedQuestion> = Vec::new();
    let node_map: HashMap<&str, &Node> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let degree = compute_degrees(graph);
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();

    // 1. AMBIGUOUS edges
    for edge in &graph.edges {
        if edge.confidence == "AMBIGUOUS" {
            let src_label = node_map
                .get(edge.source.as_str())
                .map(|n| n.label.as_str())
                .unwrap_or(&edge.source);
            let tgt_label = node_map
                .get(edge.target.as_str())
                .map(|n| n.label.as_str())
                .unwrap_or(&edge.target);
            questions.push(SuggestedQuestion {
                question_type: "ambiguous_edge".to_string(),
                question: Some(format!(
                    "What is the exact relationship between `{src_label}` and `{tgt_label}`?"
                )),
                why: format!(
                    "Edge tagged AMBIGUOUS (relation: {}) - confidence is low.",
                    edge.relation
                ),
            });
        }
    }

    // 2. Bridge nodes (high betweenness) -> cross-cutting concern questions
    if !communities.is_empty() && !graph.edges.is_empty() {
        let (node_scores, _) = brandes_centrality(graph);
        let mut bridges: Vec<(usize, f64)> = node_scores
            .into_iter()
            .enumerate()
            .filter(|(idx, score)| {
                let node = &graph.nodes[*idx];
                *score > 0.0
                    && !is_file_node(node, *degree.get(node.id.as_str()).unwrap_or(&0))
                    && !is_concept_node(node)
            })
            .collect();
        bridges.sort_by(|(_, left_score), (_, right_score)| right_score.total_cmp(left_score));

        for (idx, score) in bridges.into_iter().take(3) {
            let node = &graph.nodes[idx];
            let Some(&cid) = node_community.get(node.id.as_str()) else {
                continue;
            };

            let mut neighbor_communities: Vec<usize> = graph
                .edges
                .iter()
                .filter_map(|edge| {
                    let other = if edge.source == node.id {
                        edge.target.as_str()
                    } else if edge.target == node.id {
                        edge.source.as_str()
                    } else {
                        return None;
                    };
                    let other_cid = node_community.get(other).copied()?;
                    (other_cid != cid).then_some(other_cid)
                })
                .collect();
            neighbor_communities.sort_unstable();
            neighbor_communities.dedup();
            if neighbor_communities.is_empty() {
                continue;
            }

            let community_label = community_labels
                .get(&cid)
                .cloned()
                .unwrap_or_else(|| format!("Community {cid}"));
            let mut other_labels: Vec<String> = neighbor_communities
                .into_iter()
                .map(|other_cid| {
                    community_labels
                        .get(&other_cid)
                        .cloned()
                        .unwrap_or_else(|| format!("Community {other_cid}"))
                })
                .collect();
            other_labels.sort();
            let others = other_labels
                .into_iter()
                .map(|label| format!("`{label}`"))
                .collect::<Vec<_>>()
                .join(", ");

            questions.push(SuggestedQuestion {
                question_type: "bridge_node".to_string(),
                question: Some(format!(
                    "Why does `{}` connect `{}` to {}?",
                    node.label, community_label, others
                )),
                why: format!(
                    "High betweenness centrality ({score:.3}) - this node is a cross-community bridge."
                ),
            });
        }
    }

    // 3. God nodes with INFERRED edges
    let mut sorted: Vec<&Node> = graph
        .nodes
        .iter()
        .filter(|node| !is_file_node(node, *degree.get(node.id.as_str()).unwrap_or(&0)))
        .collect();
    sorted.sort_by(|left, right| {
        degree
            .get(right.id.as_str())
            .copied()
            .unwrap_or(0)
            .cmp(&degree.get(left.id.as_str()).copied().unwrap_or(0))
    });

    for node in sorted.into_iter().take(5) {
        let id = node.id.as_str();
        let inferred: Vec<&Edge> = graph
            .neighbor_order
            .get(id)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .filter_map(|neighbor| {
                        graph.edges.iter().find(|edge| {
                            edge.confidence == "INFERRED"
                                && ((edge.source == id && edge.target == *neighbor)
                                    || (edge.target == id && edge.source == *neighbor))
                        })
                    })
                    .collect()
            })
            .unwrap_or_else(|| {
                graph
                    .edges
                    .iter()
                    .filter(|e| (e.source == id || e.target == id) && e.confidence == "INFERRED")
                    .collect()
            });
        if inferred.len() >= 2 {
            let others: Vec<String> = inferred
                .iter()
                .take(2)
                .filter_map(|e| {
                    let (src_id, tgt_id) = edge_endpoint_ids(e);
                    let other = if src_id == id { tgt_id } else { src_id };
                    node_map.get(other).map(|n| n.label.clone())
                })
                .collect();
            if others.len() == 2 {
                questions.push(SuggestedQuestion {
                    question_type: "verify_inferred".to_string(),
                    question: Some(format!("Are the {} inferred relationships involving `{}` (e.g. with `{}` and `{}`) actually correct?", inferred.len(), node.label, others[0], others[1])),
                    why: format!("`{}` has {} INFERRED edges - model-reasoned connections that need verification.", node.label, inferred.len()),
                });
            }
        }
    }

    // 4. Isolated or weakly-connected nodes
    let isolated: Vec<&Node> = graph
        .nodes
        .iter()
        .filter(|n| {
            let deg = *degree.get(&n.id).unwrap_or(&0);
            deg <= 1 && !is_file_node(n, deg) && !is_concept_node(n)
        })
        .collect();
    if !isolated.is_empty() {
        let labels: Vec<String> = isolated
            .iter()
            .take(3)
            .map(|n| format!("`{}`", n.label))
            .collect();
        questions.push(SuggestedQuestion {
            question_type: "isolated_nodes".to_string(),
            question: Some(format!(
                "What connects {} to the rest of the system?",
                labels.join(", ")
            )),
            why: format!(
                "{} weakly-connected nodes found - possible documentation gaps or missing edges.",
                isolated.len()
            ),
        });
    }

    // 5. Low-cohesion communities
    let mut sorted_communities: Vec<(usize, &Vec<String>)> =
        communities.iter().map(|(&cid, nodes)| (cid, nodes)).collect();
    sorted_communities.sort_by_key(|(cid, _)| *cid);
    for (cid, nodes) in sorted_communities {
        let score = cohesion_score(graph, nodes);
        if score < 0.15 && nodes.len() >= 5 {
            let label = community_labels
                .get(&cid)
                .map(|s| s.as_str())
                .unwrap_or("Unknown");
            questions.push(SuggestedQuestion {
                question_type: "low_cohesion".to_string(),
                question: Some(format!(
                    "Should `{label}` be split into smaller, more focused modules?"
                )),
                why: format!(
                    "Cohesion score {} - nodes in this community are weakly interconnected.",
                    python_float(score)
                ),
            });
        }
    }

    if questions.is_empty() {
        questions.push(SuggestedQuestion {
            question_type: "no_signal".to_string(),
            question: None,
            why: "Not enough signal to generate questions. This usually means the corpus has no AMBIGUOUS edges, no bridge nodes, no INFERRED relationships, and all communities are tightly cohesive. Add more files or run with --mode deep to extract richer edges.".to_string(),
        });
    }

    questions.truncate(top_n.max(1));
    questions
}

pub fn analyze(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    top_n: usize,
) -> AnalysisResult {
    AnalysisResult {
        god_nodes: god_nodes(graph, top_n.max(1)),
        surprising_connections: surprising_connections(graph, communities, top_n.max(1)),
        suggested_questions: suggest_questions(graph, communities, community_labels, top_n.max(1)),
    }
}

pub fn generate_report(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    cohesion_scores: &HashMap<usize, f64>,
    community_labels: &HashMap<usize, String>,
    god_node_list: &[GodNode],
    surprise_list: &[SurprisingConnection],
    detection_result: &serde_json::Value,
    token_cost: &serde_json::Value,
    root: &str,
    suggested_questions: &[SuggestedQuestion],
    today: Option<&str>,
) -> String {
    let today = today.unwrap_or("unknown-date");
    let degree = compute_degrees(graph);
    let confidences: Vec<&str> = graph.edges.iter().map(|e| e.confidence.as_str()).collect();
    let total = usize::max(confidences.len(), 1) as f64;
    let ext_pct = ((confidences.iter().filter(|&&c| c == "EXTRACTED").count() as f64 / total)
        * 100.0)
        .round() as usize;
    let inf_pct = ((confidences.iter().filter(|&&c| c == "INFERRED").count() as f64 / total)
        * 100.0)
        .round() as usize;
    let amb_pct = ((confidences.iter().filter(|&&c| c == "AMBIGUOUS").count() as f64 / total)
        * 100.0)
        .round() as usize;

    let inferred_edges: Vec<&Edge> = graph
        .edges
        .iter()
        .filter(|edge| edge.confidence == "INFERRED")
        .collect();
    let inferred_avg = if inferred_edges.is_empty() {
        None
    } else {
        let total_score: f64 = inferred_edges
            .iter()
            .map(|edge| edge.confidence_score.unwrap_or(0.5) as f64)
            .sum();
        Some(((total_score / inferred_edges.len() as f64) * 100.0).round() / 100.0)
    };

    let mut lines = vec![
        format!("# Graph Report - {root}  ({today})"),
        String::new(),
        "## Corpus Check".to_string(),
    ];
    if let Some(warning) = detection_result.get("warning").and_then(|v| v.as_str()) {
        if !warning.is_empty() {
            lines.push(format!("- {warning}"));
        }
    } else {
        let total_files = detection_result
            .get("total_files")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let total_words = detection_result
            .get("total_words")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        lines.push(format!(
            "- {} files · ~{} words",
            format_number(total_files as usize),
            format_number(total_words as usize)
        ));
        lines
            .push("- Verdict: corpus is large enough that graph structure adds value.".to_string());
    }

    let mut extraction_line =
        format!("- Extraction: {ext_pct}% EXTRACTED · {inf_pct}% INFERRED · {amb_pct}% AMBIGUOUS");
    if let Some(avg) = inferred_avg {
        extraction_line.push_str(&format!(
            " · INFERRED: {} edges (avg confidence: {})",
            inferred_edges.len(),
            trim_float(avg)
        ));
    }

    let input_tokens = token_cost
        .get("input")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = token_cost
        .get("output")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    lines.extend([
        String::new(),
        "## Summary".to_string(),
        format!(
            "- {} nodes · {} edges · {} communities detected",
            graph.nodes.len(),
            graph.edges.len(),
            communities.len()
        ),
        extraction_line,
        format!(
            "- Token cost: {} input · {} output",
            format_number(input_tokens as usize),
            format_number(output_tokens as usize)
        ),
    ]);

    if !communities.is_empty() {
        lines.push(String::new());
        lines.push("## Community Hubs (Navigation)".to_string());
        let mut sorted_cids: Vec<usize> = communities.keys().copied().collect();
        sorted_cids.sort_unstable();
        for cid in sorted_cids {
            let label = community_labels
                .get(&cid)
                .cloned()
                .unwrap_or_else(|| format!("Community {cid}"));
            lines.push(format!(
                "- [[_COMMUNITY_{}|{}]]",
                safe_community_name(&label),
                label
            ));
        }
    }

    lines.push(String::new());
    lines.push("## God Nodes (most connected - your core abstractions)".to_string());
    for (idx, node) in god_node_list.iter().enumerate() {
        lines.push(format!(
            "{}. `{}` - {} edges",
            idx + 1,
            node.label,
            node.edges
        ));
    }

    lines.push(String::new());
    lines.push("## Surprising Connections (you probably didn't know these)".to_string());
    if surprise_list.is_empty() {
        lines.push(
            "- None detected - all connections are within the same source files.".to_string(),
        );
    } else {
        for surprise in surprise_list {
            let note = surprise.note.as_str();
            let conf_tag = if surprise.confidence == "INFERRED" {
                let matching_edge = graph.edges.iter().find(|edge| {
                    edge.confidence == "INFERRED" && edge.relation == surprise.relation
                });
                if let Some(score) = matching_edge.and_then(|edge| edge.confidence_score) {
                    format!("INFERRED {:.2}", score)
                } else {
                    surprise.confidence.clone()
                }
            } else {
                surprise.confidence.clone()
            };
            let sem_tag = if surprise.relation == "semantically_similar_to" {
                " [semantically similar]"
            } else {
                ""
            };
            let from_file = surprise.source_files.first().cloned().unwrap_or_default();
            let to_file = surprise.source_files.get(1).cloned().unwrap_or_default();
            lines.push(format!(
                "- `{}` --{}--> `{}`  [{}]{}",
                surprise.source, surprise.relation, surprise.target, conf_tag, sem_tag
            ));
            let suffix = if note.is_empty() {
                String::new()
            } else {
                format!("  _{}_", note)
            };
            lines.push(format!("  {} → {}{}", from_file, to_file, suffix));
        }
    }

    if !graph.hyperedges.is_empty() {
        lines.push(String::new());
        lines.push("## Hyperedges (group relationships)".to_string());
        for hyperedge in &graph.hyperedges {
            let label = hyperedge
                .get("label")
                .and_then(|v| v.as_str())
                .or_else(|| hyperedge.get("id").and_then(|v| v.as_str()))
                .unwrap_or("");
            let node_labels = hyperedge
                .get("nodes")
                .and_then(|v| v.as_array())
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let confidence = hyperedge
                .get("confidence")
                .and_then(|v| v.as_str())
                .unwrap_or("INFERRED");
            let conf_tag =
                if let Some(score) = hyperedge.get("confidence_score").and_then(|v| v.as_f64()) {
                    format!("{confidence} {:.2}", score)
                } else {
                    confidence.to_string()
                };
            lines.push(format!("- **{}** — {} [{}]", label, node_labels, conf_tag));
        }
    }

    lines.push(String::new());
    lines.push("## Communities".to_string());
    let node_map: HashMap<&str, &Node> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let mut sorted_cids: Vec<usize> = communities.keys().copied().collect();
    sorted_cids.sort_unstable();
    for cid in sorted_cids {
        let label = community_labels
            .get(&cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {cid}"));
        let nodes = communities.get(&cid).cloned().unwrap_or_default();
        let score = cohesion_scores.get(&cid).copied().unwrap_or(0.0);
        let real_nodes: Vec<String> = nodes
            .iter()
            .filter_map(|node_id| {
                let node = node_map.get(node_id.as_str()).copied()?;
                let node_degree = *degree.get(node_id).unwrap_or(&0);
                if is_file_node(node, node_degree) {
                    None
                } else {
                    Some(node.label.clone())
                }
            })
            .collect();
        let display = real_nodes
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if real_nodes.len() > 8 {
            format!(" (+{} more)", real_nodes.len() - 8)
        } else {
            String::new()
        };
        lines.push(String::new());
        lines.push(format!("### Community {} - \"{}\"", cid, label));
        lines.push(format!("Cohesion: {}", python_float(score)));
        lines.push(format!(
            "Nodes ({}): {}{}",
            real_nodes.len(),
            display,
            suffix
        ));
    }

    let ambiguous_edges: Vec<&Edge> = graph
        .edges
        .iter()
        .filter(|edge| edge.confidence == "AMBIGUOUS")
        .collect();
    if !ambiguous_edges.is_empty() {
        lines.push(String::new());
        lines.push("## Ambiguous Edges - Review These".to_string());
        for edge in ambiguous_edges {
            let (src_id, tgt_id) = edge_endpoint_ids(edge);
            let src_label = node_map
                .get(src_id)
                .map(|node| node.label.clone())
                .unwrap_or_else(|| src_id.to_string());
            let tgt_label = node_map
                .get(tgt_id)
                .map(|node| node.label.clone())
                .unwrap_or_else(|| tgt_id.to_string());
            lines.push(format!("- `{}` → `{}`  [AMBIGUOUS]", src_label, tgt_label));
            lines.push(format!(
                "  {} · relation: {}",
                edge.source_file, edge.relation
            ));
        }
    }

    let isolated: Vec<&Node> = graph
        .nodes
        .iter()
        .filter(|node| {
            let node_degree = *degree.get(node.id.as_str()).unwrap_or(&0);
            node_degree <= 1 && !is_file_node(node, node_degree) && !is_concept_node(node)
        })
        .collect();
    let mut thin_communities: Vec<(usize, Vec<String>)> = communities
        .iter()
        .filter(|(_, nodes)| nodes.len() < 3)
        .map(|(&cid, nodes)| (cid, nodes.clone()))
        .collect();
    thin_communities.sort_by_key(|(cid, _)| *cid);
    if !isolated.is_empty() || !thin_communities.is_empty() || amb_pct > 20 {
        lines.push(String::new());
        lines.push("## Knowledge Gaps".to_string());
        if !isolated.is_empty() {
            let labels: Vec<String> = isolated
                .iter()
                .take(5)
                .map(|node| format!("`{}`", node.label))
                .collect();
            let suffix = if isolated.len() > 5 {
                format!(" (+{} more)", isolated.len() - 5)
            } else {
                String::new()
            };
            lines.push(format!(
                "- **{} isolated node(s):** {}{}",
                isolated.len(),
                labels.join(", "),
                suffix
            ));
            lines.push(
                "  These have ≤1 connection - possible missing edges or undocumented components."
                    .to_string(),
            );
        }
        for (cid, nodes) in thin_communities {
            let label = community_labels
                .get(&cid)
                .cloned()
                .unwrap_or_else(|| format!("Community {cid}"));
            let node_labels: Vec<String> = nodes
                .iter()
                .filter_map(|node_id| {
                    node_map
                        .get(node_id.as_str())
                        .map(|node| format!("`{}`", node.label))
                })
                .collect();
            lines.push(format!(
                "- **Thin community `{}`** ({} nodes): {}",
                label,
                nodes.len(),
                node_labels.join(", ")
            ));
            lines.push(
                "  Too small to be a meaningful cluster - may be noise or needs more connections extracted."
                    .to_string(),
            );
        }
        if amb_pct > 20 {
            lines.push(format!(
                "- **High ambiguity: {}% of edges are AMBIGUOUS.** Review the Ambiguous Edges section above.",
                amb_pct
            ));
        }
    }

    if !suggested_questions.is_empty() {
        lines.push(String::new());
        lines.push("## Suggested Questions".to_string());
        let no_signal =
            suggested_questions.len() == 1 && suggested_questions[0].question_type == "no_signal";
        if no_signal {
            lines.push(format!("_{}_", suggested_questions[0].why));
        } else {
            lines.push("_Questions this graph is uniquely positioned to answer:_".to_string());
            lines.push(String::new());
            for question in suggested_questions {
                if let Some(text) = &question.question {
                    lines.push(format!("- **{}**", text));
                    lines.push(format!("  _{}_", question.why));
                }
            }
        }
    }

    lines.join("\n")
}

pub fn export_json_data(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
) -> serde_json::Value {
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();
    let nodes: Vec<serde_json::Value> = graph
        .nodes
        .iter()
        .map(|node| {
            let mut value = serde_json::to_value(node).unwrap_or_else(|_| serde_json::json!({}));
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "community".to_string(),
                    node_community
                        .get(node.id.as_str())
                        .copied()
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                );
                object.insert(
                    "norm_label".to_string(),
                    serde_json::Value::String(strip_diacritics(&node.label).to_lowercase()),
                );
            }
            value
        })
        .collect();
    let links: Vec<serde_json::Value> = graph
        .edges
        .iter()
        .map(|edge| {
            let mut value = serde_json::to_value(edge).unwrap_or_else(|_| serde_json::json!({}));
            if let Some(object) = value.as_object_mut() {
                if let Some(weight) = object.get("weight").and_then(|value| value.as_f64()) {
                    let rounded = (weight * 1_000_000.0).round() / 1_000_000.0;
                    object.insert("weight".to_string(), serde_json::Value::from(rounded));
                }
                if !edge.confidence_score_present {
                    let score = match edge.confidence.as_str() {
                        "INFERRED" => 0.5,
                        "AMBIGUOUS" => 0.2,
                        _ => 1.0,
                    };
                    object.insert(
                        "confidence_score".to_string(),
                        serde_json::Value::from(score),
                    );
                }
            }
            value
        })
        .collect();

    serde_json::json!({
        "directed": false,
        "multigraph": false,
        "graph": {},
        "nodes": nodes,
        "links": links,
        "hyperedges": graph.hyperedges,
    })
}

pub fn export_html(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    title: &str,
) -> String {
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();
    let degree = compute_degrees(graph);
    let max_degree = degree.values().copied().max().unwrap_or(1).max(1) as f64;

    let vis_nodes: Vec<serde_json::Value> = graph
        .nodes
        .iter()
        .map(|node| {
            let cid = node_community.get(node.id.as_str()).copied().unwrap_or(0);
            let deg = degree.get(node.id.as_str()).copied().unwrap_or(1);
            let color = COMMUNITY_COLORS[cid % COMMUNITY_COLORS.len()];
            serde_json::json!({
                "id": node.id,
                "label": node.label,
                "color": {
                    "background": color,
                    "border": color,
                    "highlight": { "background": "#ffffff", "border": color }
                },
                "size": ((10.0 + 30.0 * (deg as f64 / max_degree)) * 10.0).round() / 10.0,
                "font": {"size": if (deg as f64) >= max_degree * 0.15 { 12 } else { 0 }, "color": "#ffffff"},
                "title": html_escape(&node.label),
                "community": cid,
                "community_name": community_labels
                    .get(&cid)
                    .cloned()
                    .unwrap_or_else(|| format!("Community {}", cid)),
                "source_file": node.source_file,
                "file_type": node.file_type,
                "degree": deg,
            })
        })
        .collect();

    let vis_edges: Vec<serde_json::Value> = graph
        .edges
        .iter()
        .map(|edge| {
            let confidence = edge.confidence.as_str();
            serde_json::json!({
                "from": edge.source,
                "to": edge.target,
                "label": edge.relation,
                "title": html_escape(&format!("{} [{}]", edge.relation, edge.confidence)),
                "dashes": confidence != "EXTRACTED",
                "width": if confidence == "EXTRACTED" { 2 } else { 1 },
                "color": {"opacity": if confidence == "EXTRACTED" { 0.7 } else { 0.35 }},
            })
        })
        .collect();

    let mut community_ids: Vec<usize> = communities.keys().copied().collect();
    community_ids.sort_unstable();
    let legend: Vec<serde_json::Value> = community_ids
        .into_iter()
        .map(|cid| {
            serde_json::json!({
                "cid": cid,
                "color": COMMUNITY_COLORS[cid % COMMUNITY_COLORS.len()],
                "label": community_labels.get(&cid).cloned().unwrap_or_else(|| format!("Community {}", cid)),
                "count": communities.get(&cid).map(|nodes| nodes.len()).unwrap_or(0),
            })
        })
        .collect();

    let title = html_escape(title);
    let stats = format!(
        "{} nodes &middot; {} edges &middot; {} communities",
        graph.nodes.len(),
        graph.edges.len(),
        communities.len()
    );
    let nodes_json = js_safe_json(&vis_nodes);
    let edges_json = js_safe_json(&vis_edges);
    let legend_json = js_safe_json(&legend);
    let hyperedges_json = js_safe_json(&graph.hyperedges);

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>graphify - {title}</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f0f1a; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; display: flex; height: 100vh; overflow: hidden; }}
  #graph {{ flex: 1; }}
  #sidebar {{ width: 280px; background: #1a1a2e; border-left: 1px solid #2a2a4e; display: flex; flex-direction: column; overflow: hidden; }}
  #search-wrap {{ padding: 12px; border-bottom: 1px solid #2a2a4e; }}
  #search {{ width: 100%; background: #0f0f1a; border: 1px solid #3a3a5e; color: #e0e0e0; padding: 7px 10px; border-radius: 6px; font-size: 13px; outline: none; }}
  #search:focus {{ border-color: #4E79A7; }}
  #search-results {{ max-height: 140px; overflow-y: auto; padding: 4px 12px; border-bottom: 1px solid #2a2a4e; display: none; }}
  .search-item {{ padding: 4px 6px; cursor: pointer; border-radius: 4px; font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .search-item:hover {{ background: #2a2a4e; }}
  #info-panel {{ padding: 14px; border-bottom: 1px solid #2a2a4e; min-height: 140px; }}
  #info-panel h3 {{ font-size: 13px; color: #aaa; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.05em; }}
  #info-content {{ font-size: 13px; color: #ccc; line-height: 1.6; }}
  #info-content .field {{ margin-bottom: 5px; }}
  #info-content .field b {{ color: #e0e0e0; }}
  #info-content .empty {{ color: #555; font-style: italic; }}
  .neighbor-link {{ display: block; padding: 2px 6px; margin: 2px 0; border-radius: 3px; cursor: pointer; font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; border-left: 3px solid #333; }}
  .neighbor-link:hover {{ background: #2a2a4e; }}
  #neighbors-list {{ max-height: 160px; overflow-y: auto; margin-top: 4px; }}
  .legend-item {{ display: flex; align-items: center; gap: 8px; padding: 4px 0; cursor: pointer; border-radius: 4px; font-size: 12px; }}
  .legend-item:hover {{ background: #2a2a4e; padding-left: 4px; }}
  .legend-item.dimmed {{ opacity: 0.35; }}
  .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}
  .legend-label {{ flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .legend-count {{ color: #666; font-size: 11px; }}
  #legend-wrap {{ flex: 1; overflow-y: auto; padding: 12px; }}
  #legend-wrap h3 {{ font-size: 13px; color: #aaa; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.05em; }}
  #stats {{ padding: 10px 14px; border-top: 1px solid #2a2a4e; font-size: 11px; color: #555; }}
</style>
</head>
<body>
<div id="graph"></div>
<div id="sidebar">
  <div id="search-wrap">
    <input id="search" type="text" placeholder="Search nodes..." autocomplete="off">
    <div id="search-results"></div>
  </div>
  <div id="info-panel">
    <h3>Node Info</h3>
    <div id="info-content"><span class="empty">Click a node to inspect it</span></div>
  </div>
  <div id="legend-wrap">
    <h3>Communities</h3>
    <div id="legend"></div>
  </div>
  <div id="stats">{stats}</div>
</div>
<script>
const RAW_NODES = {nodes_json};
const RAW_EDGES = {edges_json};
const LEGEND = {legend_json};
const hyperedges = {hyperedges_json};
function esc(s) {{
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}}
const nodesDS = new vis.DataSet(RAW_NODES.map(n => ({{
  id: n.id, label: n.label, color: n.color, size: n.size, font: n.font, title: n.title,
  _community: n.community, _community_name: n.community_name, _source_file: n.source_file, _file_type: n.file_type, _degree: n.degree,
}})));
const edgesDS = new vis.DataSet(RAW_EDGES.map((e, i) => ({{
  id: i, from: e.from, to: e.to, label: '', title: e.title, dashes: e.dashes, width: e.width,
  color: e.color, arrows: {{ to: {{ enabled: true, scaleFactor: 0.5 }} }},
}})));
const container = document.getElementById('graph');
const network = new vis.Network(container, {{ nodes: nodesDS, edges: edgesDS }}, {{
  physics: {{
    enabled: true,
    solver: 'forceAtlas2Based',
    forceAtlas2Based: {{
      gravitationalConstant: -60,
      centralGravity: 0.005,
      springLength: 120,
      springConstant: 0.08,
      damping: 0.4,
      avoidOverlap: 0.8,
    }},
    stabilization: {{ iterations: 200, fit: true }},
  }},
  interaction: {{
    hover: true,
    tooltipDelay: 100,
    hideEdgesOnDrag: true,
    navigationButtons: false,
    keyboard: false,
  }},
  nodes: {{ shape: 'dot', borderWidth: 1.5 }},
  edges: {{ smooth: {{ type: 'continuous', roundness: 0.2 }}, selectionWidth: 3 }},
}});
network.once('stabilizationIterationsDone', () => network.setOptions({{ physics: {{ enabled: false }} }}));
function showInfo(nodeId) {{
  const n = nodesDS.get(nodeId);
  if (!n) return;
  const neighborIds = network.getConnectedNodes(nodeId);
  const neighborItems = neighborIds.map(nid => {{
    const nb = nodesDS.get(nid);
    const color = nb ? nb.color.background : '#555';
    return `<span class="neighbor-link" style="border-left-color:${{esc(color)}}" onclick="focusNode(${{JSON.stringify(nid)}})">${{esc(nb ? nb.label : nid)}}</span>`;
  }}).join('');
  document.getElementById('info-content').innerHTML = `
    <div class="field"><b>${{esc(n.label)}}</b></div>
    <div class="field">Type: ${{esc(n._file_type || 'unknown')}}</div>
    <div class="field">Community: ${{esc(n._community_name)}}</div>
    <div class="field">Source: ${{esc(n._source_file || '-')}}</div>
    <div class="field">Degree: ${{n._degree}}</div>
    ${{neighborIds.length ? `<div class="field" style="margin-top:8px;color:#aaa;font-size:11px">Neighbors (${{neighborIds.length}})</div><div id="neighbors-list">${{neighborItems}}</div>` : ''}}
  `;
}}
function focusNode(nodeId) {{
  network.focus(nodeId, {{ scale: 1.4, animation: true }});
  network.selectNodes([nodeId]);
  showInfo(nodeId);
}}
let hoveredNodeId = null;
network.on('hoverNode', params => {{
  hoveredNodeId = params.node;
  container.style.cursor = 'pointer';
}});
network.on('blurNode', () => {{
  hoveredNodeId = null;
  container.style.cursor = 'default';
}});
container.addEventListener('click', () => {{
  if (hoveredNodeId !== null) {{
    showInfo(hoveredNodeId);
    network.selectNodes([hoveredNodeId]);
  }}
}});
network.on('click', params => {{
  if (params.nodes.length > 0) {{
    showInfo(params.nodes[0]);
  }} else if (hoveredNodeId === null) {{
    document.getElementById('info-content').innerHTML = '<span class="empty">Click a node to inspect it</span>';
  }}
}});
const searchInput = document.getElementById('search');
const searchResults = document.getElementById('search-results');
searchInput.addEventListener('input', () => {{
  const q = searchInput.value.toLowerCase().trim();
  searchResults.innerHTML = '';
  if (!q) {{ searchResults.style.display = 'none'; return; }}
  const matches = RAW_NODES.filter(n => n.label.toLowerCase().includes(q)).slice(0, 20);
  if (!matches.length) {{ searchResults.style.display = 'none'; return; }}
  searchResults.style.display = 'block';
  matches.forEach(n => {{
    const el = document.createElement('div');
    el.className = 'search-item';
    el.textContent = n.label;
    el.style.borderLeft = `3px solid ${{n.color.background}}`;
    el.style.paddingLeft = '8px';
    el.onclick = () => {{
      network.focus(n.id, {{ scale: 1.5, animation: true }});
      network.selectNodes([n.id]);
      showInfo(n.id);
      searchResults.style.display = 'none';
      searchInput.value = '';
    }};
    searchResults.appendChild(el);
  }});
}});
document.addEventListener('click', e => {{
  if (!searchResults.contains(e.target) && e.target !== searchInput)
    searchResults.style.display = 'none';
}});
const hiddenCommunities = new Set();
const legendEl = document.getElementById('legend');
LEGEND.forEach(c => {{
  const item = document.createElement('div');
  item.className = 'legend-item';
  item.innerHTML = `<div class="legend-dot" style="background:${{c.color}}"></div><span class="legend-label">${{esc(c.label)}}</span><span class="legend-count">${{c.count}}</span>`;
  item.onclick = () => {{
    if (hiddenCommunities.has(c.cid)) {{
      hiddenCommunities.delete(c.cid);
      item.classList.remove('dimmed');
    }} else {{
      hiddenCommunities.add(c.cid);
      item.classList.add('dimmed');
    }}
    nodesDS.update(RAW_NODES.filter(n => n.community === c.cid).map(n => ({{ id: n.id, hidden: hiddenCommunities.has(c.cid) }})));
  }};
  legendEl.appendChild(item);
}});
network.on('afterDrawing', function(ctx) {{
  hyperedges.forEach(h => {{
    const positions = h.nodes
      .map(nid => network.getPositions([nid])[nid])
      .filter(p => p !== undefined);
    if (positions.length < 2) return;
    ctx.save();
    ctx.globalAlpha = 0.12;
    ctx.fillStyle = '#6366f1';
    ctx.strokeStyle = '#6366f1';
    ctx.lineWidth = 2;
    ctx.beginPath();
    const cx = positions.reduce((s, p) => s + p.x, 0) / positions.length;
    const cy = positions.reduce((s, p) => s + p.y, 0) / positions.length;
    const expanded = positions.map(p => ({{
      x: cx + (p.x - cx) * 1.15,
      y: cy + (p.y - cy) * 1.15
    }}));
    ctx.moveTo(expanded[0].x, expanded[0].y);
    expanded.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
    ctx.closePath();
    ctx.fill();
    ctx.globalAlpha = 0.4;
    ctx.stroke();
    ctx.globalAlpha = 0.8;
    ctx.fillStyle = '#4f46e5';
    ctx.font = 'bold 11px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(h.label, cx, cy - 5);
    ctx.restore();
  }});
}});
</script>
</body>
</html>"#
    )
}

pub fn export_html_to_path(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    output_path: &Path,
) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let html = export_html(
        graph,
        communities,
        community_labels,
        &output_path.to_string_lossy(),
    );
    fs::write(output_path, html)
}

pub fn export_obsidian(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    output_dir: &Path,
    community_labels: &HashMap<usize, String>,
    cohesion: &HashMap<usize, f64>,
) -> std::io::Result<usize> {
    fs::create_dir_all(output_dir)?;
    let degree = compute_degrees(graph);

    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();

    let node_filename = build_node_filenames(graph);

    let mut inter_community_edges: HashMap<usize, HashMap<usize, usize>> = HashMap::new();
    for edge in &graph.edges {
        let Some(&source_cid) = node_community.get(edge.source.as_str()) else {
            continue;
        };
        let Some(&target_cid) = node_community.get(edge.target.as_str()) else {
            continue;
        };
        if source_cid == target_cid {
            continue;
        }
        *inter_community_edges
            .entry(source_cid)
            .or_default()
            .entry(target_cid)
            .or_default() += 1;
        *inter_community_edges
            .entry(target_cid)
            .or_default()
            .entry(source_cid)
            .or_default() += 1;
    }

    for node in &graph.nodes {
        let cid = node_community.get(node.id.as_str()).copied();
        let community_name = cid
            .map(|community_id| {
                community_labels
                    .get(&community_id)
                    .cloned()
                    .unwrap_or_else(|| format!("Community {}", community_id))
            })
            .unwrap_or_else(|| "Community None".to_string());
        let ftype_tag = match node.file_type.as_str() {
            "code" => "graphify/code".to_string(),
            "document" => "graphify/document".to_string(),
            "paper" => "graphify/paper".to_string(),
            "image" => "graphify/image".to_string(),
            other if !other.is_empty() => format!("graphify/{other}"),
            _ => "graphify/document".to_string(),
        };
        let dominant_confidence = graph
            .edges
            .iter()
            .filter(|edge| edge.source == node.id || edge.target == node.id)
            .map(|edge| edge.confidence.clone())
            .fold(HashMap::<String, usize>::new(), |mut counts, confidence| {
                *counts.entry(confidence).or_default() += 1;
                counts
            })
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(confidence, _)| confidence)
            .unwrap_or_else(|| "EXTRACTED".to_string());
        let tags = vec![
            ftype_tag,
            format!("graphify/{}", dominant_confidence),
            format!("community/{}", community_name.replace(' ', "_")),
        ];

        let mut lines = vec![
            "---".to_string(),
            format!("source_file: \"{}\"", node.source_file.replace('"', "\\\"")),
            format!("type: \"{}\"", node.file_type.replace('"', "\\\"")),
            format!("community: \"{}\"", community_name.replace('"', "\\\"")),
        ];
        if let Some(location) = &node.source_location {
            lines.push(format!("location: \"{}\"", location.replace('"', "\\\"")));
        }
        lines.push("tags:".to_string());
        for tag in &tags {
            lines.push(format!("  - {}", tag));
        }
        lines.extend([
            "---".to_string(),
            String::new(),
            format!("# {}", node.label),
            String::new(),
        ]);

        let mut neighbors: Vec<_> = graph
            .edges
            .iter()
            .filter_map(|edge| {
                let other = if edge.source == node.id {
                    edge.target.as_str()
                } else if edge.target == node.id {
                    edge.source.as_str()
                } else {
                    return None;
                };
                Some((
                    other.to_string(),
                    edge.relation.clone(),
                    edge.confidence.clone(),
                ))
            })
            .collect();
        neighbors.sort_by(|left, right| left.0.cmp(&right.0));
        if !neighbors.is_empty() {
            lines.push("## Connections".to_string());
            for (other_id, relation, confidence) in neighbors {
                let label = node_filename
                    .get(&other_id)
                    .cloned()
                    .unwrap_or_else(|| safe_note_name(&other_id));
                lines.push(format!("- [[{}]] - `{}` [{}]", label, relation, confidence));
            }
            lines.push(String::new());
        }
        lines.push(
            tags.iter()
                .map(|tag| format!("#{}", tag))
                .collect::<Vec<_>>()
                .join(" "),
        );

        let filename = format!(
            "{}.md",
            node_filename
                .get(&node.id)
                .cloned()
                .unwrap_or_else(|| safe_note_name(&node.label))
        );
        fs::write(output_dir.join(filename), lines.join("\n"))?;
    }

    let mut community_notes_written = 0usize;
    let mut community_ids: Vec<usize> = communities.keys().copied().collect();
    community_ids.sort_unstable();
    for cid in community_ids {
        let members = communities.get(&cid).cloned().unwrap_or_default();
        let label = community_labels
            .get(&cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        let mut lines = vec!["---".to_string(), "type: community".to_string()];
        if let Some(score) = cohesion.get(&cid) {
            lines.push(format!("cohesion: {:.2}", score));
        }
        lines.push(format!("members: {}", members.len()));
        lines.extend([
            "---".to_string(),
            String::new(),
            format!("# {}", label),
            String::new(),
        ]);

        if let Some(score) = cohesion.get(&cid) {
            let description = if *score >= 0.7 {
                "tightly connected"
            } else if *score >= 0.4 {
                "moderately connected"
            } else {
                "loosely connected"
            };
            lines.push(format!("**Cohesion:** {:.2} - {}", score, description));
        }
        lines.push(format!("**Members:** {} nodes", members.len()));
        lines.push(String::new());
        lines.push("## Members".to_string());
        let mut sorted_members = members.clone();
        sorted_members.sort();
        for member in sorted_members {
            let node = graph.nodes.iter().find(|node| node.id == member);
            if let Some(node) = node {
                let filename = node_filename
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_else(|| safe_note_name(&node.label));
                let mut entry = format!("- [[{}]]", filename);
                if !node.file_type.is_empty() {
                    entry.push_str(&format!(" - {}", node.file_type));
                }
                if !node.source_file.is_empty() {
                    entry.push_str(&format!(" - {}", node.source_file));
                }
                lines.push(entry);
            }
        }
        lines.push(String::new());
        lines.push("## Live Query (requires Dataview plugin)".to_string());
        lines.push(String::new());
        lines.push("```dataview".to_string());
        lines.push(format!(
            "TABLE source_file, type FROM #community/{}",
            label.replace(' ', "_")
        ));
        lines.push("SORT file.name ASC".to_string());
        lines.push("```".to_string());
        lines.push(String::new());

        if let Some(cross) = inter_community_edges.get(&cid) {
            let mut cross_items: Vec<_> = cross.iter().collect();
            cross_items.sort_by(|left, right| right.1.cmp(left.1));
            if !cross_items.is_empty() {
                lines.push("## Connections to other communities".to_string());
                for (&other_cid, &count) in cross_items {
                    let other_name = community_labels
                        .get(&other_cid)
                        .cloned()
                        .unwrap_or_else(|| format!("Community {}", other_cid));
                    lines.push(format!(
                        "- {} edge{} to [[_COMMUNITY_{}]]",
                        count,
                        if count == 1 { "" } else { "s" },
                        safe_note_name(&other_name)
                    ));
                }
                lines.push(String::new());
            }
        }

        let mut bridge_nodes: Vec<(String, usize, usize)> = members
            .iter()
            .filter_map(|member| {
                let reach: HashSet<usize> = graph
                    .edges
                    .iter()
                    .filter_map(|edge| {
                        let other = if edge.source == *member {
                            edge.target.as_str()
                        } else if edge.target == *member {
                            edge.source.as_str()
                        } else {
                            return None;
                        };
                        let other_cid = node_community.get(other).copied()?;
                        (other_cid != cid).then_some(other_cid)
                    })
                    .collect();
                (!reach.is_empty()).then(|| {
                    (
                        member.clone(),
                        *degree.get(member).unwrap_or(&0),
                        reach.len(),
                    )
                })
            })
            .collect();
        bridge_nodes.sort_by(|left, right| right.2.cmp(&left.2).then(right.1.cmp(&left.1)));
        if !bridge_nodes.is_empty() {
            lines.push("## Top bridge nodes".to_string());
            for (member, member_degree, reach) in bridge_nodes.into_iter().take(5) {
                let filename = node_filename
                    .get(&member)
                    .cloned()
                    .unwrap_or_else(|| safe_note_name(&member));
                lines.push(format!(
                    "- [[{}]] - degree {}, connects to {} {}",
                    filename,
                    member_degree,
                    reach,
                    if reach == 1 {
                        "community"
                    } else {
                        "communities"
                    }
                ));
            }
        }

        fs::write(
            output_dir.join(format!("_COMMUNITY_{}.md", safe_note_name(&label))),
            lines.join("\n"),
        )?;
        community_notes_written += 1;
    }

    let obsidian_dir = output_dir.join(".obsidian");
    fs::create_dir_all(&obsidian_dir)?;
    let mut label_entries: Vec<_> = community_labels.iter().collect();
    label_entries.sort_by_key(|(cid, _)| *cid);
    let color_groups: Vec<serde_json::Value> = label_entries
        .into_iter()
        .map(|(&cid, label)| {
            serde_json::json!({
                "query": format!("tag:#community/{}", label.replace(' ', "_")),
                "color": {
                    "a": 1,
                    "rgb": i64::from_str_radix(COMMUNITY_COLORS[cid % COMMUNITY_COLORS.len()].trim_start_matches('#'), 16).unwrap_or(0)
                }
            })
        })
        .collect();
    fs::write(
        obsidian_dir.join("graph.json"),
        serde_json::to_string_pretty(&serde_json::json!({ "colorGroups": color_groups }))
            .unwrap_or_else(|_| "{\"colorGroups\":[]}".to_string()),
    )?;

    Ok(graph.nodes.len() + community_notes_written)
}

pub fn export_canvas_data(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    provided_node_filenames: &HashMap<String, String>,
) -> serde_json::Value {
    const CANVAS_COLORS: [&str; 6] = ["1", "2", "3", "4", "5", "6"];

    let node_filename = merged_node_filenames(graph, provided_node_filenames);
    let num_communities = communities.len();
    let cols = if num_communities == 0 {
        1usize
    } else {
        (num_communities as f64).sqrt().ceil() as usize
    };
    let rows = if num_communities == 0 {
        1usize
    } else {
        num_communities.div_ceil(cols)
    };

    let gap = 80usize;
    let mut sorted_cids: Vec<usize> = communities.keys().copied().collect();
    sorted_cids.sort_unstable();

    let mut group_sizes: HashMap<usize, (usize, usize)> = HashMap::new();
    for cid in &sorted_cids {
        let members = communities.get(cid).cloned().unwrap_or_default();
        let member_count = members.len();
        let width = std::cmp::max(
            600usize,
            if member_count > 0 {
                220usize * (member_count as f64).sqrt().ceil() as usize
            } else {
                600usize
            },
        );
        let height = std::cmp::max(
            400usize,
            if member_count > 0 {
                100usize * member_count.div_ceil(3) + 120usize
            } else {
                400usize
            },
        );
        group_sizes.insert(*cid, (width, height));
    }

    let mut col_widths = vec![0usize; cols];
    for (col_idx, width_slot) in col_widths.iter_mut().enumerate() {
        let mut max_width = 0usize;
        for row_idx in 0..rows {
            let linear = row_idx * cols + col_idx;
            if let Some(cid) = sorted_cids.get(linear) {
                max_width = max_width.max(group_sizes.get(cid).map(|(w, _)| *w).unwrap_or(0));
            }
        }
        *width_slot = max_width;
    }

    let mut row_heights = vec![0usize; rows];
    for (row_idx, height_slot) in row_heights.iter_mut().enumerate() {
        let mut max_height = 0usize;
        for col_idx in 0..cols {
            let linear = row_idx * cols + col_idx;
            if let Some(cid) = sorted_cids.get(linear) {
                max_height = max_height.max(group_sizes.get(cid).map(|(_, h)| *h).unwrap_or(0));
            }
        }
        *height_slot = max_height;
    }

    let mut group_layout: HashMap<usize, (usize, usize, usize, usize)> = HashMap::new();
    for (index, cid) in sorted_cids.iter().enumerate() {
        let col_idx = index % cols;
        let row_idx = index / cols;
        let gx = col_widths.iter().take(col_idx).sum::<usize>() + col_idx * gap;
        let gy = row_heights.iter().take(row_idx).sum::<usize>() + row_idx * gap;
        let (gw, gh) = group_sizes
            .get(cid)
            .copied()
            .unwrap_or((600usize, 400usize));
        group_layout.insert(*cid, (gx, gy, gw, gh));
    }

    let all_canvas_nodes: HashSet<&str> = communities
        .values()
        .flat_map(|members| members.iter().map(String::as_str))
        .collect();

    let mut canvas_nodes: Vec<serde_json::Value> = Vec::new();
    let mut canvas_edges: Vec<serde_json::Value> = Vec::new();

    for (index, cid) in sorted_cids.iter().enumerate() {
        let members = communities.get(cid).cloned().unwrap_or_default();
        let community_name = community_labels
            .get(cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        let (gx, gy, gw, gh) = group_layout
            .get(cid)
            .copied()
            .unwrap_or((0usize, 0usize, 600usize, 400usize));
        let canvas_color = CANVAS_COLORS[index % CANVAS_COLORS.len()];

        canvas_nodes.push(serde_json::json!({
            "id": format!("g{}", cid),
            "type": "group",
            "label": community_name,
            "x": gx,
            "y": gy,
            "width": gw,
            "height": gh,
            "color": canvas_color,
        }));

        let mut sorted_members = members;
        sorted_members.sort_by_key(|node_id| {
            graph
                .nodes
                .iter()
                .find(|node| node.id == *node_id)
                .map(|node| node.label.clone())
                .unwrap_or_else(|| node_id.clone())
        });
        for (member_index, node_id) in sorted_members.iter().enumerate() {
            let col = member_index % 3;
            let row = member_index / 3;
            let node_x = gx + 20 + col * 200;
            let node_y = gy + 80 + row * 80;
            let filename = node_filename.get(node_id).cloned().unwrap_or_else(|| {
                graph
                    .nodes
                    .iter()
                    .find(|node| node.id == *node_id)
                    .map(|node| safe_note_name(&node.label))
                    .unwrap_or_else(|| safe_note_name(node_id))
            });
            canvas_nodes.push(serde_json::json!({
                "id": format!("n_{}", node_id),
                "type": "file",
                "file": format!("graphify/obsidian/{}.md", filename),
                "x": node_x,
                "y": node_y,
                "width": 180,
                "height": 60,
            }));
        }
    }

    let mut weighted_edges: Vec<(f64, &Edge)> = graph
        .edges
        .iter()
        .filter(|edge| {
            all_canvas_nodes.contains(edge.source.as_str())
                && all_canvas_nodes.contains(edge.target.as_str())
        })
        .map(|edge| (edge.weight, edge))
        .collect();
    weighted_edges.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for (_weight, edge) in weighted_edges.into_iter().take(200) {
        let label = if edge.relation.is_empty() {
            format!("[{}]", edge.confidence)
        } else {
            format!("{} [{}]", edge.relation, edge.confidence)
        };
        canvas_edges.push(serde_json::json!({
            "id": format!("e_{}_{}", edge.source, edge.target),
            "fromNode": format!("n_{}", edge.source),
            "toNode": format!("n_{}", edge.target),
            "label": label,
        }));
    }

    serde_json::json!({
        "nodes": canvas_nodes,
        "edges": canvas_edges,
    })
}

pub fn export_canvas_to_path(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    node_filenames: &HashMap<String, String>,
    output_path: &Path,
) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = export_canvas_data(graph, communities, community_labels, node_filenames);
    fs::write(
        output_path,
        serde_json::to_string_pretty(&data).unwrap_or_else(|_| "{}".to_string()),
    )
}

pub fn export_svg(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    figsize: (f64, f64),
) -> String {
    let mut node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |node_id| (node_id.as_str(), cid)))
        .collect();
    if node_community.is_empty() {
        for node in &graph.nodes {
            node_community.insert(node.id.as_str(), 0usize);
        }
    }

    let mut grouped_nodes: HashMap<usize, Vec<&Node>> = HashMap::new();
    for node in &graph.nodes {
        let cid = node_community
            .get(node.id.as_str())
            .copied()
            .unwrap_or(0usize);
        grouped_nodes.entry(cid).or_default().push(node);
    }

    let mut sorted_cids: Vec<usize> = grouped_nodes.keys().copied().collect();
    sorted_cids.sort_unstable();
    let community_count = std::cmp::max(1usize, sorted_cids.len());
    let cols = (community_count as f64).sqrt().ceil() as usize;
    let rows = community_count.div_ceil(cols);
    let group_gap = 120.0f64;
    let margin = 60.0f64;

    let mut group_sizes: HashMap<usize, (f64, f64)> = HashMap::new();
    for cid in &sorted_cids {
        let members = grouped_nodes.get(cid).cloned().unwrap_or_default();
        let count = std::cmp::max(1usize, members.len());
        let group_cols = (count as f64).sqrt().ceil() as usize;
        let group_rows = count.div_ceil(group_cols);
        let width = (group_cols as f64 * 160.0 + 120.0).max(320.0);
        let height = (group_rows as f64 * 120.0 + 140.0).max(220.0);
        group_sizes.insert(*cid, (width, height));
    }

    let mut col_widths = vec![0.0f64; cols];
    for (col_idx, width_slot) in col_widths.iter_mut().enumerate() {
        let mut max_width = 0.0f64;
        for row_idx in 0..rows {
            let linear = row_idx * cols + col_idx;
            if let Some(cid) = sorted_cids.get(linear) {
                max_width = max_width.max(group_sizes.get(cid).map(|(w, _)| *w).unwrap_or(0.0));
            }
        }
        *width_slot = max_width;
    }

    let mut row_heights = vec![0.0f64; rows];
    for (row_idx, height_slot) in row_heights.iter_mut().enumerate() {
        let mut max_height = 0.0f64;
        for col_idx in 0..cols {
            let linear = row_idx * cols + col_idx;
            if let Some(cid) = sorted_cids.get(linear) {
                max_height = max_height.max(group_sizes.get(cid).map(|(_, h)| *h).unwrap_or(0.0));
            }
        }
        *height_slot = max_height;
    }

    let mut positions: HashMap<&str, (f64, f64)> = HashMap::new();
    let degree = compute_degrees(graph);
    let max_deg = degree.values().copied().max().unwrap_or(1).max(1) as f64;
    let mut max_x = margin;
    let mut max_y = margin;
    let mut group_boxes: Vec<(usize, String, f64, f64, f64, f64)> = Vec::new();

    for (index, cid) in sorted_cids.iter().enumerate() {
        let col_idx = index % cols;
        let row_idx = index / cols;
        let group_x =
            margin + col_widths.iter().take(col_idx).sum::<f64>() + col_idx as f64 * group_gap;
        let group_y =
            margin + row_heights.iter().take(row_idx).sum::<f64>() + row_idx as f64 * group_gap;
        let (group_w, group_h) = group_sizes.get(cid).copied().unwrap_or((320.0, 220.0));
        let label = community_labels
            .get(cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        group_boxes.push((*cid, label, group_x, group_y, group_w, group_h));
        max_x = max_x.max(group_x + group_w);
        max_y = max_y.max(group_y + group_h);

        let mut members = grouped_nodes.get(cid).cloned().unwrap_or_default();
        members.sort_by(|left, right| left.label.cmp(&right.label).then(left.id.cmp(&right.id)));
        let count = std::cmp::max(1usize, members.len());
        let group_cols = (count as f64).sqrt().ceil() as usize;
        for (member_index, node) in members.into_iter().enumerate() {
            let col = member_index % group_cols;
            let row = member_index / group_cols;
            let x = group_x + 80.0 + col as f64 * 160.0;
            let y = group_y + 110.0 + row as f64 * 120.0;
            positions.insert(node.id.as_str(), (x, y));
        }
    }

    let width_px = (figsize.0.max(1.0) * 72.0).round();
    let height_px = (figsize.1.max(1.0) * 72.0).round();
    let view_width = (max_x + margin).max(400.0);
    let view_height = (max_y + margin).max(260.0);

    let mut svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{:.0}" height="{:.0}" viewBox="0 0 {:.1} {:.1}" role="img" aria-label="graphify graph export">"##,
        width_px, height_px, view_width, view_height
    );
    svg.push_str(r##"<rect width="100%" height="100%" fill="#1a1a2e"/>"##);

    for (cid, label, x, y, width, height) in &group_boxes {
        let color = COMMUNITY_COLORS[*cid % COMMUNITY_COLORS.len()];
        svg.push_str(&format!(
            r##"<rect x="{:.1}" y="{:.1}" width="{:.1}" height="{:.1}" rx="20" fill="{}" fill-opacity="0.08" stroke="{}" stroke-opacity="0.35" stroke-width="2"/>"##,
            x, y, width, height, color, color
        ));
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" fill="{}" font-family="sans-serif" font-size="20" font-weight="600">{}</text>"##,
            x + 24.0,
            y + 36.0,
            color,
            svg_escape(label)
        ));
    }

    for edge in &graph.edges {
        let Some((x1, y1)) = positions.get(edge.source.as_str()).copied() else {
            continue;
        };
        let Some((x2, y2)) = positions.get(edge.target.as_str()).copied() else {
            continue;
        };
        let dashed = edge.confidence != "EXTRACTED";
        let opacity = if edge.confidence == "EXTRACTED" {
            "0.6"
        } else {
            "0.3"
        };
        let dash_array = if dashed {
            r##" stroke-dasharray="8 6""##
        } else {
            ""
        };
        svg.push_str(&format!(
            r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#aaaaaa" stroke-width="2" stroke-opacity="{}"{} />"##,
            x1, y1, x2, y2, opacity, dash_array
        ));
    }

    for node in &graph.nodes {
        let Some((x, y)) = positions.get(node.id.as_str()).copied() else {
            continue;
        };
        let cid = node_community
            .get(node.id.as_str())
            .copied()
            .unwrap_or(0usize);
        let color = COMMUNITY_COLORS[cid % COMMUNITY_COLORS.len()];
        let node_degree = degree.get(&node.id).copied().unwrap_or(1) as f64;
        let radius = 18.0 + 24.0 * (node_degree / max_deg);
        svg.push_str(&format!(
            r##"<circle cx="{:.1}" cy="{:.1}" r="{:.1}" fill="{}" fill-opacity="0.9" stroke="{}" stroke-width="2"/>"##,
            x, y, radius, color, color
        ));
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" fill="#ffffff" font-family="sans-serif" font-size="14" text-anchor="middle">{}</text>"##,
            x,
            y + 5.0,
            svg_escape(&node.label)
        ));
    }

    if !community_labels.is_empty() {
        let legend_x = view_width - 260.0;
        let legend_y = 30.0;
        let legend_height = 28.0 * community_labels.len() as f64 + 28.0;
        svg.push_str(&format!(
            r##"<rect x="{:.1}" y="{:.1}" width="220" height="{:.1}" rx="14" fill="#2a2a4e" fill-opacity="0.75"/>"##,
            legend_x, legend_y, legend_height
        ));
        let mut label_entries: Vec<_> = community_labels.iter().collect();
        label_entries.sort_by_key(|(cid, _)| *cid);
        for (index, (cid, label)) in label_entries.into_iter().enumerate() {
            let entry_y = legend_y + 24.0 + index as f64 * 28.0;
            let color = COMMUNITY_COLORS[*cid % COMMUNITY_COLORS.len()];
            let count = communities.get(cid).map_or(0usize, Vec::len);
            svg.push_str(&format!(
                r##"<circle cx="{:.1}" cy="{:.1}" r="7" fill="{}"/>"##,
                legend_x + 18.0,
                entry_y - 4.0,
                color
            ));
            svg.push_str(&format!(
                r##"<text x="{:.1}" y="{:.1}" fill="#ffffff" font-family="sans-serif" font-size="13">{}</text>"##,
                legend_x + 32.0,
                entry_y,
                svg_escape(&format!("{} ({})", label, count))
            ));
        }
    }

    svg.push_str("</svg>");
    svg
}

pub fn export_svg_to_path(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    community_labels: &HashMap<usize, String>,
    figsize: (f64, f64),
    output_path: &Path,
) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        output_path,
        export_svg(graph, communities, community_labels, figsize),
    )
}

fn community_wiki_article(
    graph: &Graph,
    community_id: usize,
    nodes: &[String],
    label: &str,
    labels: &HashMap<usize, String>,
    cohesion: Option<f64>,
    node_community: &HashMap<&str, usize>,
) -> String {
    let degree = compute_degrees(graph);
    let mut top_nodes = nodes.to_vec();
    top_nodes.sort_by(|left, right| {
        degree
            .get(right)
            .copied()
            .unwrap_or(0)
            .cmp(&degree.get(left).copied().unwrap_or(0))
            .then(left.cmp(right))
    });
    top_nodes.truncate(25);

    let mut cross_counts: HashMap<String, usize> = HashMap::new();
    let node_set: HashSet<&str> = nodes.iter().map(String::as_str).collect();
    let mut conf_counts: HashMap<String, usize> = HashMap::new();
    let mut sources: HashSet<String> = HashSet::new();
    for node_id in nodes {
        if let Some(node) = graph.nodes.iter().find(|node| node.id == *node_id) {
            if !node.source_file.is_empty() {
                sources.insert(node.source_file.clone());
            }
        }
        for edge in &graph.edges {
            let other = if edge.source == *node_id {
                Some(edge.target.as_str())
            } else if edge.target == *node_id {
                Some(edge.source.as_str())
            } else {
                None
            };
            let Some(other_id) = other else {
                continue;
            };
            *conf_counts.entry(edge.confidence.clone()).or_default() += 1;
            if node_set.contains(other_id) {
                continue;
            }
            if let Some(other_cid) = node_community.get(other_id).copied() {
                if other_cid != community_id {
                    let other_label = labels
                        .get(&other_cid)
                        .cloned()
                        .unwrap_or_else(|| format!("Community {}", other_cid));
                    *cross_counts.entry(other_label).or_default() += 1;
                }
            }
        }
    }
    let total_edges = conf_counts.values().sum::<usize>().max(1);
    let mut cross: Vec<(String, usize)> = cross_counts.into_iter().collect();
    cross.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));

    let mut sorted_sources: Vec<String> = sources.into_iter().collect();
    sorted_sources.sort();

    let mut lines = vec![format!("# {}", label), String::new()];
    let mut meta_parts = vec![format!("{} nodes", nodes.len())];
    if let Some(score) = cohesion {
        meta_parts.push(format!("cohesion {:.2}", score));
    }
    lines.push(format!("> {}", meta_parts.join(" · ")));
    lines.push(String::new());

    lines.push("## Key Concepts".to_string());
    lines.push(String::new());
    for node_id in &top_nodes {
        if let Some(node) = graph.nodes.iter().find(|node| node.id == *node_id) {
            let source = if node.source_file.is_empty() {
                String::new()
            } else {
                format!(" — `{}`", node.source_file)
            };
            lines.push(format!(
                "- **{}** ({} connections){}",
                node.label,
                degree.get(node_id).copied().unwrap_or(0),
                source
            ));
        }
    }
    let remaining = nodes.len().saturating_sub(top_nodes.len());
    if remaining > 0 {
        lines.push(format!(
            "- *... and {} more nodes in this community*",
            remaining
        ));
    }
    lines.push(String::new());

    lines.push("## Relationships".to_string());
    lines.push(String::new());
    if cross.is_empty() {
        lines.push("- No strong cross-community connections detected".to_string());
    } else {
        for (other_label, count) in cross.into_iter().take(12) {
            lines.push(format!(
                "- [[{}]] ({count} shared connections)",
                other_label
            ));
        }
    }
    lines.push(String::new());

    if !sorted_sources.is_empty() {
        lines.push("## Source Files".to_string());
        lines.push(String::new());
        for source in sorted_sources.into_iter().take(20) {
            lines.push(format!("- `{}`", source));
        }
        lines.push(String::new());
    }

    lines.push("## Audit Trail".to_string());
    lines.push(String::new());
    for confidence in ["EXTRACTED", "INFERRED", "AMBIGUOUS"] {
        let count = conf_counts.get(confidence).copied().unwrap_or(0);
        let pct = ((count as f64 / total_edges as f64) * 100.0).round() as usize;
        lines.push(format!("- {}: {} ({pct}%)", confidence, count));
    }
    lines.push(String::new());
    lines.push("---".to_string());
    lines.push(String::new());
    lines.push("*Part of the graphify knowledge wiki. See [[index]] to navigate.*".to_string());
    lines.join("\n")
}

fn god_node_wiki_article(
    graph: &Graph,
    node_id: &str,
    labels: &HashMap<usize, String>,
    node_community: &HashMap<&str, usize>,
) -> Option<String> {
    let degree = compute_degrees(graph);
    let node = graph.nodes.iter().find(|node| node.id == node_id)?;
    let mut lines = vec![format!("# {}", node.label), String::new()];
    lines.push(format!(
        "> God node · {} connections · `{}`",
        degree.get(node_id).copied().unwrap_or(0),
        node.source_file
    ));
    lines.push(String::new());

    if let Some(cid) = node_community.get(node_id).copied() {
        let community_name = labels
            .get(&cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        lines.push(format!("**Community:** [[{}]]", community_name));
        lines.push(String::new());
    }

    let mut by_relation: HashMap<String, Vec<String>> = HashMap::new();
    let mut neighbors: Vec<String> = graph
        .edges
        .iter()
        .filter_map(|edge| {
            if edge.source == node_id {
                Some(edge.target.clone())
            } else if edge.target == node_id {
                Some(edge.source.clone())
            } else {
                None
            }
        })
        .collect();
    neighbors.sort_by(|left, right| {
        degree
            .get(right)
            .copied()
            .unwrap_or(0)
            .cmp(&degree.get(left).copied().unwrap_or(0))
            .then(left.cmp(right))
    });

    for neighbor_id in neighbors {
        let Some(neighbor) = graph.nodes.iter().find(|node| node.id == neighbor_id) else {
            continue;
        };
        let edge = graph
            .edges
            .iter()
            .find(|edge| {
                (edge.source == node_id && edge.target == neighbor.id)
                    || (edge.target == node_id && edge.source == neighbor.id)
            })
            .cloned()
            .unwrap_or_default();
        let conf = if edge.confidence.is_empty() {
            String::new()
        } else {
            format!(" `{}`", edge.confidence)
        };
        by_relation
            .entry(if edge.relation.is_empty() {
                "related".to_string()
            } else {
                edge.relation
            })
            .or_default()
            .push(format!("[[{}]]{}", neighbor.label, conf));
    }

    lines.push("## Connections by Relation".to_string());
    lines.push(String::new());
    let mut relations: Vec<_> = by_relation.into_iter().collect();
    relations.sort_by(|left, right| left.0.cmp(&right.0));
    for (relation, targets) in relations {
        lines.push(format!("### {}", relation));
        for target in targets.into_iter().take(20) {
            lines.push(format!("- {}", target));
        }
        lines.push(String::new());
    }

    lines.push("---".to_string());
    lines.push(String::new());
    lines.push("*Part of the graphify knowledge wiki. See [[index]] to navigate.*".to_string());
    Some(lines.join("\n"))
}

fn wiki_index_markdown(
    communities: &HashMap<usize, Vec<String>>,
    labels: &HashMap<usize, String>,
    god_nodes: &[GodNode],
    total_nodes: usize,
    total_edges: usize,
) -> String {
    let mut lines = vec![
        "# Knowledge Graph Index".to_string(),
        String::new(),
        "> Auto-generated by graphify. Start here — read community articles for context, then drill into god nodes for detail.".to_string(),
        String::new(),
        format!(
            "**{} nodes · {} edges · {} communities**",
            total_nodes,
            total_edges,
            communities.len()
        ),
        String::new(),
        "---".to_string(),
        String::new(),
        "## Communities".to_string(),
        "(sorted by size, largest first)".to_string(),
        String::new(),
    ];

    let mut sorted_communities: Vec<_> = communities.iter().collect();
    sorted_communities
        .sort_by(|left, right| right.1.len().cmp(&left.1.len()).then(left.0.cmp(right.0)));
    for (&cid, nodes) in sorted_communities {
        let label = labels
            .get(&cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        lines.push(format!("- [[{}]] — {} nodes", label, nodes.len()));
    }
    lines.push(String::new());

    if !god_nodes.is_empty() {
        lines.push("## God Nodes".to_string());
        lines.push("(most connected concepts — the load-bearing abstractions)".to_string());
        lines.push(String::new());
        for node in god_nodes {
            lines.push(format!("- [[{}]] — {} connections", node.label, node.edges));
        }
        lines.push(String::new());
    }

    lines.push("---".to_string());
    lines.push(String::new());
    lines.push("*Generated by [graphify](https://github.com/safishamsi/graphify)*".to_string());
    lines.join("\n")
}

pub fn export_wiki(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    output_dir: &Path,
    community_labels: &HashMap<usize, String>,
    cohesion: &HashMap<usize, f64>,
    god_nodes: &[GodNode],
) -> std::io::Result<usize> {
    fs::create_dir_all(output_dir)?;

    let labels: HashMap<usize, String> = if community_labels.is_empty() {
        communities
            .keys()
            .map(|cid| (*cid, format!("Community {}", cid)))
            .collect()
    } else {
        community_labels.clone()
    };
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |node_id| (node_id.as_str(), cid)))
        .collect();

    let mut count = 0usize;
    let mut sorted_communities: Vec<_> = communities.iter().collect();
    sorted_communities.sort_by_key(|(cid, _)| *cid);
    for (&cid, nodes) in sorted_communities {
        let label = labels
            .get(&cid)
            .cloned()
            .unwrap_or_else(|| format!("Community {}", cid));
        let article = community_wiki_article(
            graph,
            cid,
            nodes,
            &label,
            &labels,
            cohesion.get(&cid).copied(),
            &node_community,
        );
        fs::write(
            output_dir.join(format!("{}.md", safe_wiki_filename(&label))),
            article,
        )?;
        count += 1;
    }

    for god_node in god_nodes {
        if let Some(article) = god_node_wiki_article(graph, &god_node.id, &labels, &node_community)
        {
            fs::write(
                output_dir.join(format!("{}.md", safe_wiki_filename(&god_node.label))),
                article,
            )?;
            count += 1;
        }
    }

    fs::write(
        output_dir.join("index.md"),
        wiki_index_markdown(
            communities,
            &labels,
            god_nodes,
            graph.nodes.len(),
            graph.edges.len(),
        ),
    )?;

    Ok(count)
}

pub fn export_cypher(graph: &Graph) -> String {
    let mut lines = vec![
        "// Neo4j Cypher import - generated by /graphify".to_string(),
        String::new(),
    ];
    for node in &graph.nodes {
        let raw_ftype: String = node
            .file_type
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect();
        let mut ftype = if raw_ftype.is_empty() {
            "unknown".to_string()
        } else {
            raw_ftype
        };
        if !ftype
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
        {
            ftype = "Entity".to_string();
        }
        let mut props = BTreeMap::new();
        push_cypher_string(&mut props, "id", &node.id);
        push_cypher_string(&mut props, "label", &node.label);
        push_cypher_string(&mut props, "file_type", &node.file_type);
        push_cypher_string(&mut props, "source_file", &node.source_file);
        push_cypher_opt_string(
            &mut props,
            "source_location",
            node.source_location.as_deref(),
        );
        push_cypher_opt_string(&mut props, "node_type", node.node_type.as_deref());
        push_cypher_opt_string(&mut props, "docstring", node.docstring.as_deref());
        push_cypher_opt_string(&mut props, "signature", node.signature.as_deref());
        push_cypher_extra_props(&mut props, &node.extra);
        lines.push(format!(
            "MERGE (n:{ftype} {{id: {node_id}}}) SET n += {props};",
            node_id = cypher_string(&node.id),
            props = cypher_props_map(&props),
        ));
    }
    lines.push(String::new());
    for edge in &graph.edges {
        let relation: String = edge
            .relation
            .to_uppercase()
            .replace([' ', '-'], "_")
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    ch
                } else {
                    '_'
                }
            })
            .collect();
        let rel = if relation.is_empty() {
            "RELATED_TO".to_string()
        } else {
            relation
        };
        let mut props = BTreeMap::new();
        push_cypher_string(&mut props, "relation", &edge.relation);
        push_cypher_string(&mut props, "confidence", &edge.confidence);
        push_cypher_string(&mut props, "source_file", &edge.source_file);
        push_cypher_opt_string(
            &mut props,
            "source_location",
            edge.source_location.as_deref(),
        );
        if let Some(score) = edge.confidence_score {
            push_cypher_float(&mut props, "confidence_score", score);
        }
        push_cypher_float(&mut props, "weight", edge.weight);
        push_cypher_extra_props(&mut props, &edge.extra);
        lines.push(format!(
            "MATCH (a {{id: {source}}}), (b {{id: {target}}}) MERGE (a)-[r:{rel}]->(b) SET r += {props};",
            source = cypher_string(&edge.source),
            target = cypher_string(&edge.target),
            props = cypher_props_map(&props),
        ));
    }
    lines.join("\n")
}

pub fn export_graphml(graph: &Graph, communities: &HashMap<usize, Vec<String>>) -> String {
    let node_community: HashMap<&str, usize> = communities
        .iter()
        .flat_map(|(&cid, nodes)| nodes.iter().map(move |n| (n.as_str(), cid)))
        .collect();
    let mut lines = vec![
        r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string(),
        r#"<graphml xmlns="http://graphml.graphdrawing.org/xmlns">"#.to_string(),
        r#"  <key id="label" for="node" attr.name="label" attr.type="string"/>"#.to_string(),
        r#"  <key id="source_file" for="node" attr.name="source_file" attr.type="string"/>"#
            .to_string(),
        r#"  <key id="file_type" for="node" attr.name="file_type" attr.type="string"/>"#
            .to_string(),
        r#"  <key id="community" for="node" attr.name="community" attr.type="int"/>"#.to_string(),
        r#"  <key id="relation" for="edge" attr.name="relation" attr.type="string"/>"#.to_string(),
        r#"  <key id="confidence" for="edge" attr.name="confidence" attr.type="string"/>"#
            .to_string(),
        r#"  <graph id="G" edgedefault="undirected">"#.to_string(),
    ];

    for node in &graph.nodes {
        lines.push(format!(r#"    <node id="{}">"#, xml_escape(&node.id)));
        lines.push(format!(
            r#"      <data key="label">{}</data>"#,
            xml_escape(&node.label)
        ));
        lines.push(format!(
            r#"      <data key="source_file">{}</data>"#,
            xml_escape(&node.source_file)
        ));
        lines.push(format!(
            r#"      <data key="file_type">{}</data>"#,
            xml_escape(&node.file_type)
        ));
        let community = node_community
            .get(node.id.as_str())
            .copied()
            .map(|cid| cid.to_string())
            .unwrap_or_else(|| "-1".to_string());
        lines.push(format!(
            r#"      <data key="community">{}</data>"#,
            xml_escape(&community)
        ));
        lines.push("    </node>".to_string());
    }

    for (idx, edge) in graph.edges.iter().enumerate() {
        lines.push(format!(
            r#"    <edge id="e{}" source="{}" target="{}">"#,
            idx,
            xml_escape(&edge.source),
            xml_escape(&edge.target)
        ));
        lines.push(format!(
            r#"      <data key="relation">{}</data>"#,
            xml_escape(&edge.relation)
        ));
        lines.push(format!(
            r#"      <data key="confidence">{}</data>"#,
            xml_escape(&edge.confidence)
        ));
        lines.push("    </edge>".to_string());
    }

    lines.push("  </graph>".to_string());
    lines.push("</graphml>".to_string());
    lines.join("\n")
}

fn cohesion_score(graph: &Graph, community_nodes: &[String]) -> f64 {
    let n = community_nodes.len();
    if n <= 1 {
        return 1.0;
    }
    let node_set: HashSet<&str> = community_nodes.iter().map(|s| s.as_str()).collect();
    let actual = graph
        .edges
        .iter()
        .filter(|e| node_set.contains(e.source.as_str()) && node_set.contains(e.target.as_str()))
        .count();
    let possible = n * (n - 1) / 2;
    if possible == 0 {
        return 0.0;
    }
    (actual as f64 / possible as f64 * 100.0).round() / 100.0
}

pub fn score_all(graph: &Graph, communities: &HashMap<usize, Vec<String>>) -> HashMap<usize, f64> {
    communities
        .iter()
        .map(|(&cid, nodes)| (cid, cohesion_score(graph, nodes)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        PyRandom, cluster, export_canvas_data, export_html, export_json_data, export_svg,
        merge_extractions, suggest_questions, surprising_connections,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn merge_preserves_node_order_with_last_write_wins() {
        let result = merge_extractions(&[
            json!({
                "nodes": [
                    {"id": "n1", "label": "Old", "file_type": "code", "source_file": "a.py"},
                    {"id": "n2", "label": "Second", "file_type": "code", "source_file": "a.py"}
                ],
                "edges": []
            }),
            json!({
                "nodes": [
                    {"id": "n1", "label": "New", "file_type": "document", "source_file": "b.md"}
                ],
                "edges": []
            }),
        ]);

        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.nodes[0].id, "n1");
        assert_eq!(result.nodes[0].label, "New");
        assert_eq!(result.nodes[0].file_type, "document");
        assert_eq!(result.nodes[1].id, "n2");
    }

    #[test]
    fn merge_accepts_links_and_filters_dangling_edges() {
        let result = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "b.py"}
            ],
            "links": [
                {"from": "n1", "to": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "a.py"},
                {"from": "n1", "to": "missing", "relation": "uses", "confidence": "INFERRED", "source_file": "a.py"}
            ]
        })]);

        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].source, "n1");
        assert_eq!(result.edges[0].target, "n2");
        assert_eq!(result.edges[0].weight, 1.0);
    }

    #[test]
    fn merge_accepts_edges_without_source_file() {
        let result = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "b.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED"}
            ]
        })]);

        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].source, "n1");
        assert_eq!(result.edges[0].target, "n2");
        assert_eq!(result.edges[0].source_file, "");
    }

    #[test]
    fn merge_accepts_nodes_without_source_file() {
        let result = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "A"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "b.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "b.py"}
            ]
        })]);

        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.nodes[0].id, "n1");
        assert_eq!(result.nodes[0].label, "A");
        assert_eq!(result.nodes[0].source_file, "");
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].source, "n1");
        assert_eq!(result.edges[0].target, "n2");
    }

    #[test]
    fn merge_keeps_hyperedges_and_token_totals() {
        let result = merge_extractions(&[
            json!({
                "nodes": [{"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"}],
                "edges": [],
                "hyperedges": [{"id": "h1"}],
                "input_tokens": 3,
                "output_tokens": 5
            }),
            json!({
                "nodes": [{"id": "n2", "label": "B", "file_type": "code", "source_file": "b.py"}],
                "edges": [],
                "hyperedges": [{"id": "h2"}],
                "input_tokens": 7,
                "output_tokens": 11
            }),
        ]);

        assert_eq!(result.hyperedges.len(), 2);
        assert_eq!(result.input_tokens, 10);
        assert_eq!(result.output_tokens, 16);
    }

    #[test]
    fn surprising_connections_use_edge_betweenness_without_communities() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "a", "label": "Alpha", "file_type": "code", "source_file": "single.py"},
                {"id": "b", "label": "Beta", "file_type": "code", "source_file": "single.py"},
                {"id": "c", "label": "Gamma", "file_type": "code", "source_file": "single.py"}
            ],
            "edges": [
                {"source": "a", "target": "b", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"},
                {"source": "b", "target": "c", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"}
            ]
        })]);

        let surprises = surprising_connections(&graph, &HashMap::new(), 2);

        assert_eq!(surprises.len(), 2);
        assert!(surprises[0].note.contains("betweenness="));
    }

    #[test]
    fn suggest_questions_includes_bridge_node_questions() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "a1", "label": "AlphaOne", "file_type": "code", "source_file": "single.py"},
                {"id": "a2", "label": "AlphaTwo", "file_type": "code", "source_file": "single.py"},
                {"id": "bridge", "label": "Bridge", "file_type": "code", "source_file": "single.py"},
                {"id": "b1", "label": "BetaOne", "file_type": "code", "source_file": "single.py"},
                {"id": "b2", "label": "BetaTwo", "file_type": "code", "source_file": "single.py"}
            ],
            "edges": [
                {"source": "a1", "target": "a2", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"},
                {"source": "a2", "target": "bridge", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"},
                {"source": "bridge", "target": "b1", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"},
                {"source": "b1", "target": "b2", "relation": "calls", "confidence": "EXTRACTED", "source_file": "single.py"}
            ]
        })]);
        let communities = HashMap::from([
            (
                0usize,
                vec!["a1".to_string(), "a2".to_string(), "bridge".to_string()],
            ),
            (1usize, vec!["b1".to_string(), "b2".to_string()]),
        ]);
        let community_labels =
            HashMap::from([(0usize, "Alpha".to_string()), (1usize, "Beta".to_string())]);

        let questions = suggest_questions(&graph, &communities, &community_labels, 7);

        assert!(questions.iter().any(|question| {
            question.question_type == "bridge_node"
                && question
                    .question
                    .as_ref()
                    .is_some_and(|text| text.contains("Bridge"))
        }));
    }

    #[test]
    fn export_json_strips_basic_diacritics_in_norm_label() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "Crème Brûlée Straße", "file_type": "document", "source_file": "notes.md"}
            ],
            "edges": []
        })]);

        let exported = export_json_data(&graph, &HashMap::new());
        let norm_label = exported["nodes"][0]["norm_label"].as_str().unwrap_or("");

        assert_eq!(norm_label, "creme brulee strasse");
    }

    #[test]
    fn cluster_sorts_singleton_communities_when_graph_has_no_edges() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n_b", "label": "B", "file_type": "code", "source_file": "b.py"},
                {"id": "n_a", "label": "A", "file_type": "code", "source_file": "a.py"}
            ],
            "edges": []
        })]);

        let communities = cluster(&graph);
        assert_eq!(communities.get(&0), Some(&vec!["n_a".to_string()]));
        assert_eq!(communities.get(&1), Some(&vec!["n_b".to_string()]));
    }

    #[test]
    fn cluster_keeps_isolates_alongside_connected_components() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "a.py"},
                {"id": "n3", "label": "C", "file_type": "code", "source_file": "b.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "calls", "confidence": "EXTRACTED", "source_file": "a.py"}
            ]
        })]);

        let communities = cluster(&graph);
        let all_nodes: std::collections::HashSet<String> = communities
            .values()
            .flat_map(|nodes| nodes.iter().cloned())
            .collect();
        assert_eq!(
            all_nodes,
            ["n1".to_string(), "n2".to_string(), "n3".to_string()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn cluster_handles_multiple_disconnected_non_isolate_components() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "a.py"},
                {"id": "n3", "label": "C", "file_type": "code", "source_file": "b.py"},
                {"id": "n4", "label": "D", "file_type": "code", "source_file": "b.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "calls", "confidence": "EXTRACTED", "source_file": "a.py"},
                {"source": "n3", "target": "n4", "relation": "calls", "confidence": "EXTRACTED", "source_file": "b.py"}
            ]
        })]);

        let communities = cluster(&graph);
        let mut groups: Vec<Vec<String>> = communities.into_values().collect();
        groups.iter_mut().for_each(|group| group.sort());
        groups.sort();

        assert_eq!(
            groups,
            vec![
                vec!["n1".to_string(), "n2".to_string()],
                vec!["n3".to_string(), "n4".to_string()]
            ]
        );
    }

    #[test]
    fn py_random_matches_python_random_42_getrandbits32() {
        let mut rng = PyRandom::new(42);
        let actual: Vec<u32> = (0..10).map(|_| rng.getrandbits(32)).collect();
        assert_eq!(
            actual,
            vec![
                2746317213, 478163327, 107420369, 3184935163, 1181241943, 1051802512,
                958682846, 599310825, 3163119785, 440213415,
            ]
        );
    }

    #[test]
    fn py_random_matches_python_random_42_shuffle() {
        let mut rng = PyRandom::new(42);
        let mut values: Vec<usize> = (0..20).collect();
        rng.shuffle(&mut values);
        assert_eq!(
            values,
            vec![19, 5, 14, 4, 9, 13, 15, 18, 6, 12, 17, 10, 1, 11, 2, 16, 7, 8, 0, 3]
        );
    }

    #[test]
    fn export_canvas_data_includes_group_and_file_nodes() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
            ]
        })]);
        let communities = HashMap::from([(0usize, vec!["n1".to_string(), "n2".to_string()])]);
        let labels = HashMap::from([(0usize, "Core".to_string())]);

        let exported = export_canvas_data(&graph, &communities, &labels, &HashMap::new());

        assert_eq!(exported["nodes"][0]["type"], "group");
        assert_eq!(exported["nodes"][1]["type"], "file");
        assert_eq!(exported["edges"][0]["label"], "uses [INFERRED]");
        assert_eq!(exported["nodes"][1]["file"], "graphify/obsidian/Parser.md");
    }

    #[test]
    fn export_svg_contains_svg_markup_and_legend_label() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
            ]
        })]);
        let communities = HashMap::from([(0usize, vec!["n1".to_string(), "n2".to_string()])]);
        let labels = HashMap::from([(0usize, "Core".to_string())]);

        let svg = export_svg(&graph, &communities, &labels, (12.0, 8.0));

        assert!(svg.contains("<svg"));
        assert!(svg.contains("Parser"));
        assert!(svg.contains("Core (2)"));
        assert!(svg.contains("stroke-dasharray"));
    }

    #[test]
    fn export_html_contains_hyperedge_overlay_and_neighbor_ui() {
        let graph = merge_extractions(&[json!({
            "nodes": [
                {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py"},
                {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED", "source_file": "parser.py"}
            ],
            "hyperedges": [
                {"id": "flow", "label": "Auth Flow", "nodes": ["n1", "n2"]}
            ]
        })]);
        let communities = HashMap::from([(0usize, vec!["n1".to_string(), "n2".to_string()])]);
        let labels = HashMap::from([(0usize, "Core".to_string())]);

        let html = export_html(&graph, &communities, &labels, "graph.html");

        assert!(html.contains("neighbor-link"));
        assert!(html.contains("dimmed"));
        assert!(html.contains("afterDrawing"));
        assert!(html.contains("Auth Flow"));
        assert!(html.contains("hoverNode"));
    }
}
