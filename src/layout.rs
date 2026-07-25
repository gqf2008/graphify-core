use std::collections::HashMap;

use rand::Rng;
use rayon::prelude::*;

use crate::binary_schema::LayoutData;
use crate::build::Graph;

/// Canvas size for community-level layout (abstract units).
const CANVAS: f32 = 5000.0;

pub fn compute_layout(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
) -> LayoutData {
    let node_count = graph.nodes.len();

    // Build node-id → index map
    let mut node_idx: HashMap<&str, usize> = HashMap::with_capacity(node_count);
    for (i, node) in graph.nodes.iter().enumerate() {
        node_idx.insert(&node.id, i);
    }

    // Build node-id → community-id map
    let mut node_comm: HashMap<&str, usize> = HashMap::with_capacity(node_count);
    for (&comm_id, members) in communities {
        for member in members {
            node_comm.insert(member.as_str(), comm_id);
        }
    }

    // Phase 1: Community-level layout — only lay out communities that have members
    let active_comm_ids: Vec<usize> = communities.keys().copied().collect();
    let comm_id_to_active: HashMap<usize, usize> = active_comm_ids
        .iter()
        .enumerate()
        .map(|(i, &cid)| (cid, i))
        .collect();

    let community_positions = community_fr_layout(
        graph,
        communities,
        &node_comm,
        &comm_id_to_active,
        CANVAS,
        500,
    );

    // Build full community position vec (include empty slots for missing IDs)
    let max_comm = *active_comm_ids.iter().max().unwrap_or(&0);
    let mut full_comm_pos = vec![(CANVAS / 2.0, CANVAS / 2.0); max_comm + 1];
    for (&cid, &active_idx) in &comm_id_to_active {
        full_comm_pos[cid] = community_positions[active_idx];
    }

    // Compute community radii — smaller multiplier to avoid visual overlap
    let community_radii: Vec<f32> = (0..=max_comm)
        .map(|cid| {
            communities
                .get(&cid)
                .map(|m| (m.len() as f32).sqrt() * 1.0)
                .unwrap_or(1.0)
        })
        .collect();

    // Phase 2: Per-community internal layout (parallel)
    let internal_results: Vec<(usize, Vec<(f32, f32)>)> = active_comm_ids
        .par_iter()
        .filter_map(|&cid| {
            let members = communities.get(&cid)?;
            let center = full_comm_pos[cid];
            let radius = community_radii[cid].max(10.0);
            let positions = community_internal_layout(graph, members, center, radius);
            Some((cid, positions))
        })
        .collect();

    // Assemble node positions
    let mut positions = vec![(0.0f32, 0.0f32); node_count];
    for (cid, pos_list) in internal_results {
        let members = communities.get(&cid).unwrap();
        for (i, member_id) in members.iter().enumerate() {
            if let Some(&idx) = node_idx.get(member_id.as_str()) {
                positions[idx] = pos_list[i];
            }
        }
    }

    LayoutData {
        positions,
        community_positions: full_comm_pos,
        community_radii,
    }
}

fn community_fr_layout(
    graph: &Graph,
    communities: &HashMap<usize, Vec<String>>,
    node_comm: &HashMap<&str, usize>,
    comm_id_to_active: &HashMap<usize, usize>,
    canvas: f32,
    iterations: usize,
) -> Vec<(f32, f32)> {
    let n = comm_id_to_active.len();
    if n == 0 {
        return Vec::new();
    }
    let mut pos: Vec<(f32, f32)> = Vec::with_capacity(n);

    // Initial placement: uniform grid to avoid random clumping
    let cols = (n as f32).sqrt().ceil() as usize;
    let spacing = canvas / (cols as f32 + 1.0);
    for i in 0..n {
        let row = i / cols;
        let col = i % cols;
        pos.push((
            (col as f32 + 1.0) * spacing + rand::rng().random_range(-spacing * 0.2..spacing * 0.2),
            (row as f32 + 1.0) * spacing + rand::rng().random_range(-spacing * 0.2..spacing * 0.2),
        ));
    }

    // Compute cross-community edge weights
    let mut cross_weight: HashMap<(usize, usize), f32> = HashMap::new();
    for edge in &graph.edges {
        let src_comm = node_comm.get(edge.source.as_str());
        let tgt_comm = node_comm.get(edge.target.as_str());
        if let (Some(&sc), Some(&tc)) = (src_comm, tgt_comm)
            && sc != tc {
                let key = (sc.min(tc), sc.max(tc));
                *cross_weight.entry(key).or_insert(0.0) += edge.weight as f32;
            }
    }

    // Compute repulsion radii for hard-shell constraint
    let radii: Vec<f32> = communities
        .values()
        .map(|m| (m.len() as f32).sqrt() * 1.0)
        .collect();

    let k = (canvas * canvas / n as f32).sqrt() * 2.0; // larger ideal distance
    let mut temperature = canvas / 3.0;
    let cooling = 0.995; // exponential cooling

    for iter in 0..iterations {
        let mut disp: Vec<(f32, f32)> = vec![(0.0, 0.0); n];

        // Repulsive forces between all pairs + hard-shell overlap repulsion
        for i in 0..n {
            for j in (i + 1)..n {
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist_sq = dx * dx + dy * dy;
                let dist = dist_sq.sqrt().max(0.01);

                // Standard FR repulsion
                let fr_force = k * k / dist;
                let mut fx = dx / dist * fr_force;
                let mut fy = dy / dist * fr_force;

                // Hard-shell: extra repulsion if communities overlap
                let min_dist = radii.get(i).copied().unwrap_or(5.0)
                    + radii.get(j).copied().unwrap_or(5.0);
                if dist < min_dist {
                    let overlap = min_dist - dist;
                    let shell_force = overlap * overlap * 0.5;
                    fx += dx / dist * shell_force;
                    fy += dy / dist * shell_force;
                }

                disp[i].0 += fx;
                disp[i].1 += fy;
                disp[j].0 -= fx;
                disp[j].1 -= fy;
            }
        }

        // Attractive forces along cross-community edges
        for (&(ci, cj), &w) in &cross_weight {
            let Some(&ai) = comm_id_to_active.get(&ci) else { continue };
            let Some(&aj) = comm_id_to_active.get(&cj) else { continue };
            let dx = pos[aj].0 - pos[ai].0;
            let dy = pos[aj].1 - pos[ai].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let force = dist * dist / k * w.min(10.0); // cap weight to prevent super-strong pulls
            let fx = dx / dist * force;
            let fy = dy / dist * force;
            disp[ai].0 += fx;
            disp[ai].1 += fy;
            disp[aj].0 -= fx;
            disp[aj].1 -= fy;
        }

        // Center gravity: weak pull toward canvas center to prevent drift
        let center = (canvas / 2.0, canvas / 2.0);
        for i in 0..n {
            let dx = center.0 - pos[i].0;
            let dy = center.1 - pos[i].1;
            disp[i].0 += dx * 0.001;
            disp[i].1 += dy * 0.001;
        }

        // Apply displacement with temperature limiting
        for i in 0..n {
            let dx = disp[i].0;
            let dy = disp[i].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let limited = dist.min(temperature);
            pos[i].0 += dx / dist * limited;
            pos[i].1 += dy / dist * limited;
            // Soft bounds: strong repulsion near edges instead of hard clamp
            let margin = 100.0;
            if pos[i].0 < margin {
                pos[i].0 += (margin - pos[i].0) * 0.1;
            }
            if pos[i].0 > canvas - margin {
                pos[i].0 -= (pos[i].0 - (canvas - margin)) * 0.1;
            }
            if pos[i].1 < margin {
                pos[i].1 += (margin - pos[i].1) * 0.1;
            }
            if pos[i].1 > canvas - margin {
                pos[i].1 -= (pos[i].1 - (canvas - margin)) * 0.1;
            }
        }

        temperature *= cooling;
        // Early exit if temperature is very low
        if temperature < 0.1 && iter > iterations / 2 {
            break;
        }
    }

    pos
}

fn community_internal_layout(
    graph: &Graph,
    members: &[String],
    center: (f32, f32),
    radius: f32,
) -> Vec<(f32, f32)> {
    let n = members.len();
    if n <= 1 {
        return vec![center];
    }

    let member_set: HashMap<&str, usize> = members.iter().enumerate().map(|(i, id)| (id.as_str(), i)).collect();

    // Collect internal edges
    let mut internal_edges: Vec<(usize, usize, f32)> = Vec::new();
    for edge in &graph.edges {
        if let (Some(&si), Some(&ti)) = (member_set.get(edge.source.as_str()), member_set.get(edge.target.as_str())) {
            internal_edges.push((si, ti, edge.weight as f32));
        }
    }

    let mut pos: Vec<(f32, f32)> = Vec::with_capacity(n);
    let mut rng = rand::rng();

    // Initial placement: circular ring + some jitter
    for i in 0..n {
        let angle = 2.0 * std::f32::consts::PI * i as f32 / n as f32;
        let r = radius * 0.5;
        pos.push((
            center.0 + r * angle.cos() + rng.random_range(-2.0..2.0),
            center.1 + r * angle.sin() + rng.random_range(-2.0..2.0),
        ));
    }

    let iterations = if n > 2000 { 30 } else if n > 500 { 60 } else { 120 };
    let k = (radius * radius * 4.0 / n as f32).sqrt().max(3.0);
    let mut temperature = radius;
    let cooling = 0.98;

    for _ in 0..iterations {
        let mut disp: Vec<(f32, f32)> = vec![(0.0, 0.0); n];

        // Repulsive forces (sampling for large n)
        if n > 500 {
            let samples = (n * 10).min(50000);
            for _ in 0..samples {
                let i = rng.random_range(0..n);
                let j = rng.random_range(0..n);
                if i == j { continue; }
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist = (dx * dx + dy * dy).sqrt().max(0.01);
                let force = k * k / dist * (n as f32 / samples as f32);
                let fx = dx / dist * force;
                let fy = dy / dist * force;
                disp[i].0 += fx;
                disp[i].1 += fy;
            }
        } else {
            for i in 0..n {
                for j in (i + 1)..n {
                    let dx = pos[i].0 - pos[j].0;
                    let dy = pos[i].1 - pos[j].1;
                    let dist = (dx * dx + dy * dy).sqrt().max(0.01);
                    let force = k * k / dist;
                    let fx = dx / dist * force;
                    let fy = dy / dist * force;
                    disp[i].0 += fx;
                    disp[i].1 += fy;
                    disp[j].0 -= fx;
                    disp[j].1 -= fy;
                }
            }
        }

        // Attractive forces along internal edges
        for &(si, ti, w) in &internal_edges {
            let dx = pos[ti].0 - pos[si].0;
            let dy = pos[ti].1 - pos[si].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let force = dist * dist / k * w;
            let fx = dx / dist * force;
            let fy = dy / dist * force;
            disp[si].0 += fx;
            disp[si].1 += fy;
            disp[ti].0 -= fx;
            disp[ti].1 -= fy;
        }

        // Weak center gravity
        for i in 0..n {
            let dx = center.0 - pos[i].0;
            let dy = center.1 - pos[i].1;
            disp[i].0 += dx * 0.01;
            disp[i].1 += dy * 0.01;
        }

        // Apply displacement
        for i in 0..n {
            let dx = disp[i].0;
            let dy = disp[i].1;
            let dist = (dx * dx + dy * dy).sqrt().max(0.01);
            let limited = dist.min(temperature);
            pos[i].0 += dx / dist * limited;
            pos[i].1 += dy / dist * limited;
        }

        temperature *= cooling;
    }

    pos
}
