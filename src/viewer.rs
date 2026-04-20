use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::sync::Arc;

use eframe::egui;

use graphify_core::binary_schema::BinGraph;

fn main() -> eframe::Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = args.get(1).expect("usage: graphify-viewer <path/to/graph.bin>");

    let data = fs::read(path).expect("cannot read graph.bin");
    let graph = graphify_core::binary_schema::decode(&data).expect("cannot decode graph.bin");

    // Build adjacency list for neighbor lookup
    let mut neighbors: Vec<Vec<(usize, u32)>> = vec![Vec::new(); graph.nodes.len()];
    for edge in &graph.edges {
        let si = edge.source as usize;
        let ti = edge.target as usize;
        if si < graph.nodes.len() && ti < graph.nodes.len() {
            let _rel_str = graph.strings.get(edge.relation as usize).map(|s| s.as_str()).unwrap_or("");
            neighbors[si].push((ti, edge.relation));
            neighbors[ti].push((si, edge.relation));
        }
    }

    let graph = Arc::new(graph);
    let neighbors = Arc::new(neighbors);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1600.0, 950.0])
            .with_title("graphify viewer"),
        ..Default::default()
    };

    eframe::run_native(
        "graphify viewer",
        options,
        Box::new(move |_cc| Ok(Box::new(GraphViewer::new(graph, neighbors)))),
    )
}

struct GraphViewer {
    graph: Arc<BinGraph>,
    neighbors: Arc<Vec<Vec<(usize, u32)>>>,
    // View state
    zoom: f32,
    pan: (f32, f32),
    // Interaction
    hovered_community: Option<usize>,
    hovered_node: Option<usize>,
    selected_node: Option<usize>,
    drilled_community: Option<usize>,
    hidden_communities: HashSet<usize>,
    // Search
    search_query: String,
    search_results: Vec<usize>,
    // Dragging
    dragging: bool,
    last_pos: Option<egui::Pos2>,
    // Caches
    _node_id_to_index: HashMap<u32, usize>,
}

impl GraphViewer {
    fn new(graph: Arc<BinGraph>, neighbors: Arc<Vec<Vec<(usize, u32)>>>) -> Self {
        let node_id_to_index: HashMap<u32, usize> = graph
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id, i))
            .collect();

        // Compute center of all non-empty community positions
        let mut cx = 0.0f32;
        let mut cy = 0.0f32;
        let mut count = 0usize;
        for (idx, comm) in graph.communities.iter().enumerate() {
            if comm.node_end <= comm.node_start {
                continue;
            }
            if let Some(&(x, y)) = graph.community_positions.get(idx) {
                cx += x;
                cy += y;
                count += 1;
            }
        }
        let center = if count > 0 {
            (cx / count as f32, cy / count as f32)
        } else {
            (2500.0, 2500.0)
        };

        Self {
            graph,
            neighbors,
            zoom: 1.0,
            pan: center,
            hovered_community: None,
            hovered_node: None,
            selected_node: None,
            drilled_community: None,
            hidden_communities: HashSet::new(),
            search_query: String::new(),
            search_results: Vec::new(),
            dragging: false,
            last_pos: None,
            _node_id_to_index: node_id_to_index,
        }
    }

    fn world_to_screen(&self, wx: f32, wy: f32, canvas: (f32, f32)) -> (f32, f32) {
        (
            (wx - self.pan.0) * self.zoom + canvas.0 * 0.5,
            (wy - self.pan.1) * self.zoom + canvas.1 * 0.5,
        )
    }

    fn screen_to_world(&self, sx: f32, sy: f32, canvas: (f32, f32)) -> (f32, f32) {
        (
            (sx - canvas.0 * 0.5) / self.zoom + self.pan.0,
            (sy - canvas.1 * 0.5) / self.zoom + self.pan.1,
        )
    }

    fn community_color(&self, idx: usize) -> egui::Color32 {
        let hue = (idx as f32 * 0.618033988749895) % 1.0;
        let r = (hue * 6.0).floor();
        let f = hue * 6.0 - r;
        let q = 1.0 - f;
        let (cr, cg, cb) = match r as u32 % 6 {
            0 => (1.0, f, 0.0),
            1 => (q, 1.0, 0.0),
            2 => (0.0, 1.0, f),
            3 => (0.0, q, 1.0),
            4 => (f, 0.0, 1.0),
            _ => (1.0, 0.0, q),
        };
        egui::Color32::from_rgb(
            (cr * 180.0 + 50.0) as u8,
            (cg * 180.0 + 50.0) as u8,
            (cb * 180.0 + 50.0) as u8,
        )
    }

    fn node_label(&self, ni: usize) -> &str {
        let node = &self.graph.nodes[ni];
        self.graph.strings.get(node.label as usize).map(|s| s.as_str()).unwrap_or("?")
    }

    fn node_source(&self, ni: usize) -> &str {
        let node = &self.graph.nodes[ni];
        self.graph.strings.get(node.source_file as usize).map(|s| s.as_str()).unwrap_or("")
    }

    fn comm_label(&self, ci: usize) -> &str {
        if let Some(comm) = self.graph.communities.get(ci) {
            self.graph.strings.get(comm.label as usize).map(|s| s.as_str()).unwrap_or("?")
        } else {
            "?"
        }
    }

    // ── Bezier edge drawing ─────────────────────────────────────────────────

    fn draw_bezier_edge(
        painter: &egui::Painter,
        p0: egui::Pos2,
        p1: egui::Pos2,
        color: egui::Color32,
        width: f32,
    ) {
        // Quadratic bezier: offset midpoint perpendicular by 15% of edge length
        let dx = p1.x - p0.x;
        let dy = p1.y - p0.y;
        let len = (dx * dx + dy * dy).sqrt().max(1.0);
        let offset = len * 0.15;
        // Perpendicular direction (rotate 90°)
        let nx = -dy / len * offset;
        let ny = dx / len * offset;
        let ctrl = egui::pos2((p0.x + p1.x) / 2.0 + nx, (p0.y + p1.y) / 2.0 + ny);

        // Tessellate into 8 segments
        let segments = 8;
        let mut prev = p0;
        for i in 1..=segments {
            let t = i as f32 / segments as f32;
            let it = 1.0 - t;
            let px = it * it * p0.x + 2.0 * it * t * ctrl.x + t * t * p1.x;
            let py = it * it * p0.y + 2.0 * it * t * ctrl.y + t * t * p1.y;
            let cur = egui::pos2(px, py);
            painter.line_segment([prev, cur], egui::Stroke::new(width, color));
            prev = cur;
        }
    }

    // ── Community overview view ─────────────────────────────────────────────

    fn draw_community_view(&mut self, ui: &mut egui::Ui) {
        let (rect, response) = ui.allocate_exact_size(
            ui.available_size(),
            egui::Sense::click_and_drag(),
        );
        let painter = ui.painter_at(rect);
        let canvas = (rect.width(), rect.height());

        let (wx0, wy0) = self.screen_to_world(rect.left(), rect.top(), canvas);
        let (wx1, wy1) = self.screen_to_world(rect.right(), rect.bottom(), canvas);
        let margin = 100.0 / self.zoom;

        // ── Draw cross-community bezier edges ──
        let edge_alpha = if self.zoom < 0.2 { 20 } else { 45 };
        for edge in &self.graph.edges {
            let src_node = &self.graph.nodes[edge.source as usize];
            let tgt_node = &self.graph.nodes[edge.target as usize];
            if src_node.community == tgt_node.community {
                continue;
            }
            let sc = src_node.community as usize;
            let tc = tgt_node.community as usize;
            if self.hidden_communities.contains(&sc) || self.hidden_communities.contains(&tc) {
                continue;
            }
            let sp = self.graph.community_positions.get(sc).copied().unwrap_or((0.0, 0.0));
            let tp = self.graph.community_positions.get(tc).copied().unwrap_or((0.0, 0.0));

            if sp.0 < wx0 - margin && tp.0 < wx0 - margin { continue; }
            if sp.0 > wx1 + margin && tp.0 > wx1 + margin { continue; }
            if sp.1 < wy0 - margin && tp.1 < wy0 - margin { continue; }
            if sp.1 > wy1 + margin && tp.1 > wy1 + margin { continue; }

            let ss = self.world_to_screen(sp.0, sp.1, canvas);
            let st = self.world_to_screen(tp.0, tp.1, canvas);
            let src_color = self.community_color(sc);
            let edge_color = egui::Color32::from_rgba_premultiplied(
                src_color.r(),
                src_color.g(),
                src_color.b(),
                edge_alpha,
            );
            Self::draw_bezier_edge(
                &painter,
                egui::pos2(ss.0, ss.1),
                egui::pos2(st.0, st.1),
                edge_color,
                1.0,
            );
        }

        // ── Draw community circles ──
        self.hovered_community = None;
        let mouse_pos = ui.input(|i| i.pointer.hover_pos());
        for (idx, comm) in self.graph.communities.iter().enumerate() {
            if comm.node_end <= comm.node_start { continue; }
            if self.hidden_communities.contains(&idx) { continue; }

            let pos = self.graph.community_positions.get(idx).copied().unwrap_or((0.0, 0.0));
            let radius = self.graph.community_radii.get(idx).copied().unwrap_or(5.0);

            if pos.0 + radius < wx0 - margin || pos.0 - radius > wx1 + margin { continue; }
            if pos.1 + radius < wy0 - margin || pos.1 - radius > wy1 + margin { continue; }

            let screen = self.world_to_screen(pos.0, pos.1, canvas);
            let screen_r = radius * self.zoom;

            let member_count = comm.node_end.saturating_sub(comm.node_start);
            let color = self.community_color(idx);
            let is_hovered = mouse_pos.map_or(false, |mp| {
                let dx = mp.x - screen.0;
                let dy = mp.y - screen.1;
                dx * dx + dy * dy < screen_r * screen_r
            });

            if is_hovered {
                self.hovered_community = Some(idx);
            }

            // Shadow for depth
            painter.circle_filled(
                egui::pos2(screen.0 + 2.0, screen.1 + 2.0),
                screen_r.max(3.0) + 1.0,
                egui::Color32::from_black_alpha(40),
            );

            // Fill
            let alpha = if is_hovered { 230 } else { 170 };
            let fill = egui::Color32::from_rgba_premultiplied(color.r(), color.g(), color.b(), alpha);
            painter.circle_filled(egui::pos2(screen.0, screen.1), screen_r.max(3.0), fill);

            // Border
            let border = if is_hovered {
                egui::Color32::WHITE
            } else {
                egui::Color32::from_rgba_premultiplied(255, 255, 255, 80)
            };
            painter.circle(
                egui::pos2(screen.0, screen.1),
                screen_r.max(3.0),
                egui::Color32::TRANSPARENT,
                egui::Stroke::new(if is_hovered { 2.0 } else { 1.0 }, border),
            );

            // Labels
            if screen_r > 18.0 {
                let label = self.comm_label(idx);
                let font_size = (screen_r * 0.25).clamp(9.0, 22.0);
                painter.text(
                    egui::pos2(screen.0, screen.1 - screen_r * 0.15),
                    egui::Align2::CENTER_CENTER,
                    label,
                    egui::FontId::proportional(font_size),
                    egui::Color32::WHITE,
                );
                painter.text(
                    egui::pos2(screen.0, screen.1 + screen_r * 0.25),
                    egui::Align2::CENTER_CENTER,
                    format!("{} nodes", member_count),
                    egui::FontId::proportional(font_size * 0.7),
                    egui::Color32::from_rgba_premultiplied(255, 255, 255, 160),
                );
            }
        }

        // Click to drill in
        if response.double_clicked() {
            if let Some(comm_idx) = self.hovered_community {
                self.drilled_community = Some(comm_idx);
                let pos = self.graph.community_positions.get(comm_idx).copied().unwrap_or((0.0, 0.0));
                self.pan = pos;
                self.zoom = 3.0;
                self.selected_node = None;
            }
        }

        // Handle input
        self.handle_input(ui, &response, canvas);
    }

    // ── Node detail view ────────────────────────────────────────────────────

    fn draw_node_view(&mut self, ui: &mut egui::Ui, comm_idx: usize) {
        let (rect, response) = ui.allocate_exact_size(
            ui.available_size(),
            egui::Sense::click_and_drag(),
        );
        let painter = ui.painter_at(rect);
        let canvas = (rect.width(), rect.height());

        let comm = &self.graph.communities[comm_idx];
        let node_start = comm.node_start as usize;
        let node_end = comm.node_end as usize;
        let member_count = node_end.saturating_sub(node_start);

        let (wx0, wy0) = self.screen_to_world(rect.left(), rect.top(), canvas);
        let (wx1, wy1) = self.screen_to_world(rect.right(), rect.bottom(), canvas);
        let margin = 30.0 / self.zoom;

        // ── Draw internal bezier edges ──
        let show_edges = member_count < 5000 && self.zoom > 0.3;
        if show_edges {
            for edge in &self.graph.edges {
                let si = edge.source as usize;
                let ti = edge.target as usize;
                if si < node_start || si >= node_end { continue; }
                if ti < node_start || ti >= node_end { continue; }
                let sp = self.graph.positions.get(si).copied().unwrap_or((0.0, 0.0));
                let tp = self.graph.positions.get(ti).copied().unwrap_or((0.0, 0.0));
                if sp.0 < wx0 - margin && tp.0 < wx0 - margin { continue; }
                if sp.0 > wx1 + margin && tp.0 > wx1 + margin { continue; }
                if sp.1 < wy0 - margin && tp.1 < wy0 - margin { continue; }
                if sp.1 > wy1 + margin && tp.1 > wy1 + margin { continue; }

                let ss = self.world_to_screen(sp.0, sp.1, canvas);
                let st = self.world_to_screen(tp.0, tp.1, canvas);

                let is_selected_edge = self.selected_node == Some(si) || self.selected_node == Some(ti);
                let alpha = if is_selected_edge { 80 } else { 25 };
                let color = self.community_color(comm_idx);
                let edge_color = egui::Color32::from_rgba_premultiplied(
                    color.r(), color.g(), color.b(), alpha,
                );
                Self::draw_bezier_edge(&painter, egui::pos2(ss.0, ss.1), egui::pos2(st.0, st.1), edge_color, 0.8);
            }
        }

        // ── Draw nodes ──
        self.hovered_node = None;
        let mouse_pos = ui.input(|i| i.pointer.hover_pos());
        let base_r = (3.0 / self.zoom.max(0.1)).min(6.0);

        for ni in node_start..node_end {
            let pos = self.graph.positions.get(ni).copied().unwrap_or((0.0, 0.0));
            if pos.0 < wx0 - margin || pos.0 > wx1 + margin { continue; }
            if pos.1 < wy0 - margin || pos.1 > wy1 + margin { continue; }

            let screen = self.world_to_screen(pos.0, pos.1, canvas);
            let node = &self.graph.nodes[ni];
            let degree = node.degree;
            let r = (base_r + degree as f32 * 0.04).min(base_r * 2.5);

            let is_hovered = mouse_pos.map_or(false, |mp| {
                let dx = mp.x - screen.0;
                let dy = mp.y - screen.1;
                dx * dx + dy * dy < (r * self.zoom + 5.0).powi(2)
            });

            if is_hovered {
                self.hovered_node = Some(ni);
            }

            let is_selected = self.selected_node == Some(ni);
            let color = self.community_color(node.community as usize);

            let fill = if is_selected {
                egui::Color32::WHITE
            } else if is_hovered {
                egui::Color32::from_rgba_premultiplied(
                    (color.r() as u16 * 3 / 2).min(255) as u8,
                    (color.g() as u16 * 3 / 2).min(255) as u8,
                    (color.b() as u16 * 3 / 2).min(255) as u8,
                    240,
                )
            } else {
                egui::Color32::from_rgba_premultiplied(color.r(), color.g(), color.b(), 200)
            };

            painter.circle_filled(egui::pos2(screen.0, screen.1), r.max(2.0), fill);

            if is_selected {
                painter.circle(
                    egui::pos2(screen.0, screen.1),
                    r.max(2.0) + 2.0,
                    egui::Color32::TRANSPARENT,
                    egui::Stroke::new(2.0, egui::Color32::WHITE),
                );
            }

            // Label on hover or high-degree nodes
            if (is_hovered || (degree > 10 && self.zoom > 2.0)) && r * self.zoom > 3.0 {
                let label = self.node_label(ni);
                painter.text(
                    egui::pos2(screen.0, screen.1 - r - 4.0),
                    egui::Align2::CENTER_BOTTOM,
                    label,
                    egui::FontId::proportional(11.0),
                    egui::Color32::from_rgba_premultiplied(255, 255, 255, 200),
                );
            }
        }

        // Click to select node
        if response.clicked() && !self.dragging {
            if let Some(ni) = self.hovered_node {
                self.selected_node = Some(ni);
            } else {
                self.selected_node = None;
            }
        }

        self.handle_input(ui, &response, canvas);
    }

    fn handle_input(&mut self, ui: &mut egui::Ui, _response: &egui::Response, canvas: (f32, f32)) {
        let scroll = ui.input(|i| i.smooth_scroll_delta.y);
        if scroll != 0.0 {
            if let Some(mp) = ui.input(|i| i.pointer.hover_pos()) {
                let (wx, wy) = self.screen_to_world(mp.x, mp.y, canvas);
                self.zoom *= 1.0 + scroll * 0.001;
                self.zoom = self.zoom.clamp(0.05, 80.0);
                self.pan.0 = wx - (mp.x - canvas.0 * 0.5) / self.zoom;
                self.pan.1 = wy - (mp.y - canvas.1 * 0.5) / self.zoom;
            }
        }

        if ui.input(|i| i.pointer.primary_down()) {
            let current = ui.input(|i| i.pointer.hover_pos());
            if let (Some(cur), Some(last)) = (current, self.last_pos) {
                let dx = (cur.x - last.x) / self.zoom;
                let dy = (cur.y - last.y) / self.zoom;
                self.pan.0 -= dx;
                self.pan.1 -= dy;
                self.dragging = true;
            }
            self.last_pos = current;
        } else {
            self.dragging = false;
            self.last_pos = ui.input(|i| i.pointer.hover_pos());
        }

        if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
            if self.drilled_community.is_some() {
                self.drilled_community = None;
                self.selected_node = None;
                self.zoom = 1.0;
            }
        }
    }

    // ── Search ──────────────────────────────────────────────────────────────

    fn search(&self, query: &str) -> Vec<usize> {
        if query.is_empty() {
            return Vec::new();
        }
        let q = query.to_lowercase();
        let mut results = Vec::new();
        for (i, node) in self.graph.nodes.iter().enumerate() {
            let label = self.graph.strings.get(node.label as usize).map(|s| s.to_lowercase()).unwrap_or_default();
            if label.contains(&q) {
                results.push(i);
                if results.len() >= 12 {
                    break;
                }
            }
        }
        results
    }

    fn focus_on_node(&mut self, ni: usize) {
        let node = &self.graph.nodes[ni];
        let comm_idx = node.community as usize;
        self.drilled_community = Some(comm_idx);
        self.selected_node = Some(ni);
        let pos = self.graph.positions.get(ni).copied().unwrap_or((0.0, 0.0));
        self.pan = pos;
        self.zoom = 5.0;
    }

    // ── Legend sidebar ──────────────────────────────────────────────────────

    fn draw_legend_panel(&mut self, ui: &mut egui::Ui) {
        egui::Panel::left("legend_panel")
            .min_size(180.0)
            .default_size(200.0)
            .show_inside(ui, |ui| {
                ui.vertical_centered(|ui| {
                    ui.heading("graphify");
                });
                ui.add_space(4.0);
                ui.label(format!(
                    "{} nodes · {} edges",
                    self.graph.nodes.len(),
                    self.graph.edges.len(),
                ));
                let active_comms = self.graph.communities.iter().filter(|c| c.node_end > c.node_start).count();
                ui.label(format!("{} communities", active_comms));
                ui.add_space(4.0);
                ui.separator();

                ui.horizontal(|ui| {
                    if ui.button("Overview").clicked() {
                        self.drilled_community = None;
                        self.selected_node = None;
                        self.zoom = 1.0;
                    }
                    if self.drilled_community.is_some() && ui.button("← Back").clicked() {
                        self.drilled_community = None;
                        self.selected_node = None;
                        self.zoom = 1.0;
                    }
                });
                if let Some(ci) = self.drilled_community {
                    ui.label(format!("Drilled: {}", self.comm_label(ci)));
                }
                ui.separator();

                ui.label(egui::RichText::new("Communities").strong());
                egui::ScrollArea::vertical()
                    .max_height(ui.available_height())
                    .show(ui, |ui| {
                        for (idx, comm) in self.graph.communities.iter().enumerate() {
                            if comm.node_end <= comm.node_start { continue; }
                            let color = self.community_color(idx);
                            let label = self.comm_label(idx).to_string();
                            let count = comm.node_end - comm.node_start;
                            let hidden = self.hidden_communities.contains(&idx);
                            let is_drilled = self.drilled_community == Some(idx);

                            let tint = if hidden { egui::Color32::GRAY } else { color };
                            ui.painter().circle_filled(
                                ui.next_widget_position() + egui::vec2(7.0, 7.0),
                                6.0,
                                tint,
                            );
                            ui.horizontal(|ui| {
                                ui.add_space(16.0);
                                let text = if hidden {
                                    egui::RichText::new(format!("{} ({})", label, count)).strikethrough()
                                } else if is_drilled {
                                    egui::RichText::new(format!("{} ({})", label, count)).strong()
                                } else {
                                    egui::RichText::new(format!("{} ({})", label, count))
                                };
                                ui.label(text);
                            });

                            // Use a transparent button overlay for click detection
                            let _ = ui.next_widget_position();
                            let (_rect, btn_resp) = ui.allocate_exact_size(
                                egui::vec2(ui.available_width(), 16.0),
                                egui::Sense::click(),
                            );
                            if btn_resp.clicked() {
                                if ui.input(|i| i.modifiers.shift) {
                                    // Shift+click toggles visibility
                                    if self.hidden_communities.contains(&idx) {
                                        self.hidden_communities.remove(&idx);
                                    } else {
                                        self.hidden_communities.insert(idx);
                                    }
                                } else {
                                    self.drilled_community = Some(idx);
                                    let pos = self.graph.community_positions.get(idx).copied().unwrap_or((0.0, 0.0));
                                    self.pan = pos;
                                    self.zoom = 3.0;
                                    self.selected_node = None;
                                }
                            }
                        }
                    });
            });
    }

    // ── Detail sidebar ──────────────────────────────────────────────────────

    fn draw_detail_panel(&mut self, ui: &mut egui::Ui) {
        egui::Panel::right("detail_panel")
            .min_size(240.0)
            .default_size(280.0)
            .show_inside(ui, |ui| {
                // ── Search ──
                ui.heading("Search");
                let response = ui.add(
                    egui::TextEdit::singleline(&mut self.search_query)
                        .hint_text("node label...")
                        .desired_width(f32::INFINITY),
                );
                if response.changed() {
                    self.search_results = self.search(&self.search_query);
                }
                // Collect focus targets to avoid borrow issues
                let mut focus_target: Option<usize> = None;
                for &ni in &self.search_results {
                    let label = self.node_label(ni).to_string();
                    if ui.button(&label).clicked() {
                        focus_target = Some(ni);
                    }
                }
                if let Some(ni) = focus_target {
                    self.focus_on_node(ni);
                }
                ui.separator();

                // ── Zoom indicator ──
                ui.horizontal(|ui| {
                    ui.label("Zoom:");
                    ui.label(format!("{:.1}x", self.zoom));
                });
                ui.add_space(4.0);

                // ── Node details ──
                let detail_ni = self.selected_node.or(self.hovered_node);
                if let Some(ni) = detail_ni {
                    let node = &self.graph.nodes[ni];
                    let label = self.node_label(ni).to_string();
                    let source = self.node_source(ni).to_string();
                    let degree = node.degree;
                    let comm_name = self.comm_label(node.community as usize).to_string();
                    let comm_color = self.community_color(node.community as usize);

                    ui.heading(egui::RichText::new(&label).size(14.0));
                    ui.add_space(2.0);
                    ui.label(egui::RichText::new(&source).color(egui::Color32::GRAY).size(11.0));
                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        ui.painter().circle_filled(ui.next_widget_position() + egui::vec2(6.0, 6.0), 5.0, comm_color);
                        ui.add_space(14.0);
                        ui.label(&comm_name);
                    });
                    ui.label(format!("Degree: {}", degree));
                    ui.separator();

                    // Collect neighbor info before the closure
                    let nbr_count = self.neighbors[ni].len();
                    let nbr_info: Vec<(usize, String)> = self.neighbors[ni]
                        .iter()
                        .take(50)
                        .map(|&(nbr_ni, _rel)| (nbr_ni, self.node_label(nbr_ni).to_string()))
                        .collect();

                    ui.label(egui::RichText::new(format!("Neighbors ({})", nbr_count)).strong());
                    let mut nbr_focus: Option<usize> = None;
                    egui::ScrollArea::vertical()
                        .max_height(ui.available_height() - 30.0)
                        .show(ui, |ui| {
                            for (nbr_ni, nbr_label) in &nbr_info {
                                if ui.button(nbr_label).clicked() {
                                    nbr_focus = Some(*nbr_ni);
                                }
                            }
                            if nbr_count > 50 {
                                ui.label(format!("... and {} more", nbr_count - 50));
                            }
                        });
                    if let Some(ni) = nbr_focus {
                        self.focus_on_node(ni);
                    }
                } else if let Some(ci) = self.hovered_community {
                    let comm = &self.graph.communities[ci];
                    let label = self.comm_label(ci).to_string();
                    let count = comm.node_end - comm.node_start;
                    let color = self.community_color(ci);

                    ui.heading(egui::RichText::new(&label).size(14.0));
                    ui.horizontal(|ui| {
                        ui.painter().circle_filled(ui.next_widget_position() + egui::vec2(6.0, 6.0), 5.0, color);
                        ui.add_space(14.0);
                        ui.label(format!("{} nodes", count));
                    });
                    ui.add_space(8.0);
                    ui.label("Double-click to explore");
                } else {
                    ui.label(egui::RichText::new("Hover or click a node\nto see details").color(egui::Color32::GRAY));
                }

                ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                    ui.label(
                        egui::RichText::new("Scroll=zoom · Drag=pan · Dbl-click=drill · Esc=back · Shift+click legend=toggle")
                            .color(egui::Color32::GRAY)
                            .size(10.0),
                    );
                });
            });
    }
}

impl eframe::App for GraphViewer {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.draw_legend_panel(ui);
        self.draw_detail_panel(ui);

        egui::CentralPanel::default().show_inside(ui, |ui| {
            if let Some(comm_idx) = self.drilled_community {
                self.draw_node_view(ui, comm_idx);
            } else {
                self.draw_community_view(ui);
            }
        });

        ui.ctx().request_repaint();
    }
}
