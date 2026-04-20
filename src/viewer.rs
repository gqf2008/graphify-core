use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::Arc;

use eframe::egui;

use graphify_core::binary_schema::{BinGraph, BinNode};

fn main() -> eframe::Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = args.get(1).expect("usage: graphify-viewer <path/to/graph.bin>");

    let data = fs::read(path).expect("cannot read graph.bin");
    let graph = graphify_core::binary_schema::decode(&data).expect("cannot decode graph.bin");
    let graph = Arc::new(graph);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1400.0, 900.0])
            .with_title("graphify viewer"),
        ..Default::default()
    };

    eframe::run_native(
        "graphify viewer",
        options,
        Box::new(move |_cc| Ok(Box::new(GraphViewer::new(graph)))),
    )
}

struct GraphViewer {
    graph: Arc<BinGraph>,
    // View state
    zoom: f32,
    pan: (f32, f32),
    // Interaction
    hovered_community: Option<usize>,
    hovered_node: Option<usize>,
    drilled_community: Option<usize>,
    // Search
    search_query: String,
    search_result: Option<usize>,
    // Dragging
    dragging: bool,
    last_pos: Option<egui::Pos2>,
    // Tooltip
    tooltip_text: String,
    // Node lookup cache (built once)
    node_id_to_index: HashMap<u32, usize>,
}

impl GraphViewer {
    fn new(graph: Arc<BinGraph>) -> Self {
        let node_id_to_index: HashMap<u32, usize> = graph
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id, i))
            .collect();

        Self {
            graph,
            zoom: 1.0,
            pan: (500.0, 500.0),
            hovered_community: None,
            hovered_node: None,
            drilled_community: None,
            search_query: String::new(),
            search_result: None,
            dragging: false,
            last_pos: None,
            tooltip_text: String::new(),
            node_id_to_index,
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
            (cr * 180.0 + 40.0) as u8,
            (cg * 180.0 + 40.0) as u8,
            (cb * 180.0 + 40.0) as u8,
        )
    }

    fn draw_community_view(&mut self, ui: &mut egui::Ui) {
        let (rect, response) = ui.allocate_exact_size(ui.available_size(), egui::Sense::click_and_drag());
        let painter = ui.painter_at(rect);
        let canvas = (rect.width(), rect.height());

        // Visible world bounds for culling
        let (wx0, wy0) = self.screen_to_world(rect.left(), rect.top(), canvas);
        let (wx1, wy1) = self.screen_to_world(rect.right(), rect.bottom(), canvas);
        let margin = 50.0 / self.zoom;

        // Draw cross-community edges
        let edge_alpha = if self.zoom < 0.3 { 20 } else { 50 };
        for edge in &self.graph.edges {
            let src_node = &self.graph.nodes[edge.source as usize];
            let tgt_node = &self.graph.nodes[edge.target as usize];
            if src_node.community == tgt_node.community {
                continue;
            }
            let src_comm = src_node.community as usize;
            let tgt_comm = tgt_node.community as usize;
            let sp = self.graph.community_positions.get(src_comm).copied().unwrap_or((0.0, 0.0));
            let tp = self.graph.community_positions.get(tgt_comm).copied().unwrap_or((0.0, 0.0));

            // Cull
            if sp.0 < wx0 - margin && tp.0 < wx0 - margin { continue; }
            if sp.0 > wx1 + margin && tp.0 > wx1 + margin { continue; }
            if sp.1 < wy0 - margin && tp.1 < wy0 - margin { continue; }
            if sp.1 > wy1 + margin && tp.1 > wy1 + margin { continue; }

            let ss = self.world_to_screen(sp.0, sp.1, canvas);
            let st = self.world_to_screen(tp.0, tp.1, canvas);
            painter.line_segment(
                [egui::pos2(ss.0, ss.1), egui::pos2(st.0, st.1)],
                egui::Stroke::new(1.0, egui::Color32::from_white_alpha(edge_alpha)),
            );
        }

        // Draw community circles
        self.hovered_community = None;
        let mouse_pos = ui.input(|i| i.pointer.hover_pos());
        for (idx, comm) in self.graph.communities.iter().enumerate() {
            let pos = self.graph.community_positions.get(idx).copied().unwrap_or((0.0, 0.0));
            let radius = self.graph.community_radii.get(idx).copied().unwrap_or(5.0);

            // Cull
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

            let fill = if is_hovered {
                self.hovered_community = Some(idx);
                egui::Color32::from_rgba_premultiplied(color.r(), color.g(), color.b(), 220)
            } else {
                egui::Color32::from_rgba_premultiplied(color.r(), color.g(), color.b(), 160)
            };

            painter.circle_filled(egui::pos2(screen.0, screen.1), screen_r.max(3.0), fill);
            painter.circle(
                egui::pos2(screen.0, screen.1),
                screen_r.max(3.0),
                fill,
                egui::Stroke::new(1.0, egui::Color32::WHITE),
            );

            // Label when zoomed in enough
            if screen_r > 20.0 {
                let label = self.graph.strings.get(comm.label as usize).map(|s| s.as_str()).unwrap_or("?");
                painter.text(
                    egui::pos2(screen.0, screen.1),
                    egui::Align2::CENTER_CENTER,
                    label,
                    egui::FontId::proportional((screen_r * 0.3).clamp(8.0, 20.0)),
                    egui::Color32::WHITE,
                );
                // Member count below
                painter.text(
                    egui::pos2(screen.0, screen.1 + screen_r * 0.4),
                    egui::Align2::CENTER_CENTER,
                    format!("{}", member_count),
                    egui::FontId::proportional((screen_r * 0.2).clamp(7.0, 14.0)),
                    egui::Color32::GRAY,
                );
            }
        }

        // Handle click to drill in
        if response.clicked() {
            if let Some(comm_idx) = self.hovered_community {
                self.drilled_community = Some(comm_idx);
                let comm = &self.graph.communities[comm_idx];
                // Center view on this community
                let pos = self.graph.community_positions.get(comm_idx).copied().unwrap_or((0.0, 0.0));
                self.pan = pos;
                self.zoom = 2.0;
            }
        }

        // Handle drag and zoom
        self.handle_input(ui, &response, canvas);
    }

    fn draw_node_view(&mut self, ui: &mut egui::Ui, comm_idx: usize) {
        let (rect, response) = ui.allocate_exact_size(ui.available_size(), egui::Sense::click_and_drag());
        let painter = ui.painter_at(rect);
        let canvas = (rect.width(), rect.height());

        let comm = &self.graph.communities[comm_idx];
        let node_start = comm.node_start as usize;
        let node_end = comm.node_end as usize;
        let member_count = node_end.saturating_sub(node_start);

        // Visible world bounds
        let (wx0, wy0) = self.screen_to_world(rect.left(), rect.top(), canvas);
        let (wx1, wy1) = self.screen_to_world(rect.right(), rect.bottom(), canvas);
        let margin = 20.0 / self.zoom;

        // Draw internal edges (skip if too many or too zoomed out)
        let show_edges = member_count < 5000 && self.zoom > 0.5;
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
                painter.line_segment(
                    [egui::pos2(ss.0, ss.1), egui::pos2(st.0, st.1)],
                    egui::Stroke::new(0.5, egui::Color32::from_white_alpha(25)),
                );
            }
        }

        // Draw nodes
        self.hovered_node = None;
        let mouse_pos = ui.input(|i| i.pointer.hover_pos());
        let node_r = (3.0 / self.zoom.max(0.1)).min(6.0);

        for ni in node_start..node_end {
            let pos = self.graph.positions.get(ni).copied().unwrap_or((0.0, 0.0));
            if pos.0 < wx0 - margin || pos.0 > wx1 + margin { continue; }
            if pos.1 < wy0 - margin || pos.1 > wy1 + margin { continue; }

            let screen = self.world_to_screen(pos.0, pos.1, canvas);
            let node = &self.graph.nodes[ni];
            let degree = node.degree;
            let r = (node_r + degree as f32 * 0.05).min(node_r * 2.0);

            let color = self.community_color(node.community as usize);
            let is_hovered = mouse_pos.map_or(false, |mp| {
                let dx = mp.x - screen.0;
                let dy = mp.y - screen.1;
                dx * dx + dy * dy < (r * self.zoom + 4.0).powi(2)
            });

            if is_hovered {
                self.hovered_node = Some(ni);
                let label = self.graph.strings.get(node.label as usize).map(|s| s.as_str()).unwrap_or("?");
                let source = self.graph.strings.get(node.source_file as usize).map(|s| s.as_str()).unwrap_or("");
                self.tooltip_text = format!("{}\n{}\ndegree={}", label, source, degree);
            }

            let fill = if is_hovered {
                egui::Color32::WHITE
            } else {
                egui::Color32::from_rgba_premultiplied(color.r(), color.g(), color.b(), 200)
            };

            painter.circle_filled(egui::pos2(screen.0, screen.1), r.max(2.0), fill);
        }

        // Handle input
        self.handle_input(ui, &response, canvas);
    }

    fn handle_input(&mut self, ui: &mut egui::Ui, _response: &egui::Response, canvas: (f32, f32)) {
        // Zoom with scroll
        let scroll = ui.input(|i| i.smooth_scroll_delta.y);
        if scroll != 0.0 {
            let mouse = ui.input(|i| i.pointer.hover_pos());
            if let Some(mp) = mouse {
                let (wx, wy) = self.screen_to_world(mp.x, mp.y, canvas);
                self.zoom *= 1.0 + scroll * 0.001;
                self.zoom = self.zoom.clamp(0.05, 50.0);
                // Re-center on mouse
                self.pan.0 = wx - (mp.x - canvas.0 * 0.5) / self.zoom;
                self.pan.1 = wy - (mp.y - canvas.1 * 0.5) / self.zoom;
            }
        }

        // Pan with drag
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

        // Escape to go back
        if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
            if self.drilled_community.is_some() {
                self.drilled_community = None;
                self.zoom = 1.0;
            }
        }
    }
}

impl eframe::App for GraphViewer {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Top bar
        ui.horizontal(|ui| {
            if self.drilled_community.is_some() {
                if ui.button("← Back").clicked() {
                    self.drilled_community = None;
                    self.zoom = 1.0;
                }
            }

            let view_label = match self.drilled_community {
                Some(ci) => {
                    let comm = &self.graph.communities[ci];
                    let name = self.graph.strings.get(comm.label as usize).map(|s| s.as_str()).unwrap_or("?");
                    format!("{} ({} nodes)", name, comm.node_end - comm.node_start)
                }
                None => format!("{} communities", self.graph.communities.len()),
            };
            ui.label(&view_label);
            ui.separator();
            ui.label(format!("zoom: {:.1}x", self.zoom));
            ui.separator();
            ui.label("Search:");
            let response = ui.add(
                egui::TextEdit::singleline(&mut self.search_query)
                    .desired_width(200.0)
                    .hint_text("node label..."),
            );
            if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                self.search_result = self.find_node(&self.search_query);
                if let Some(ni) = self.search_result {
                    let node = &self.graph.nodes[ni];
                    let pos = self.graph.positions.get(ni).copied().unwrap_or((0.0, 0.0));
                    self.pan = pos;
                    self.zoom = 5.0;
                    self.drilled_community = Some(node.community as usize);
                }
            }
        });

        ui.separator();

        // Main canvas
        if let Some(comm_idx) = self.drilled_community {
            self.draw_node_view(ui, comm_idx);
        } else {
            self.draw_community_view(ui);
        }

        // Tooltip
        if !self.tooltip_text.is_empty() {
            egui::Area::new(egui::Id::new("tooltip"))
                .fixed_pos(ui.input(|i| i.pointer.hover_pos().unwrap_or(egui::pos2(0.0, 0.0))) + egui::vec2(15.0, 15.0))
                .show(ui.ctx(), |ui| {
                    ui.set_max_width(300.0);
                    ui.add(egui::Label::new(&self.tooltip_text).wrap());
                });
        }
        if self.hovered_node.is_none() && self.hovered_community.is_none() {
            self.tooltip_text.clear();
        }

        ui.ctx().request_repaint();
    }

    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            self.ui(ui, frame);
        });
    }
}

impl GraphViewer {
    fn find_node(&self, query: &str) -> Option<usize> {
        let q = query.to_lowercase();
        for (i, node) in self.graph.nodes.iter().enumerate() {
            let label = self.graph.strings.get(node.label as usize).map(|s| s.to_lowercase()).unwrap_or_default();
            if label.contains(&q) {
                return Some(i);
            }
        }
        None
    }
}
