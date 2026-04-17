use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use graphify_core::{build, detect, extract, pipeline, query};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;

fn parse_graph_value(value: &serde_json::Value) -> Result<build::Graph> {
    match value.as_array() {
        Some(extractions) => Ok(build::merge_extractions(extractions)),
        None => match serde_json::from_value(value.clone()) {
            Ok(graph) => Ok(graph),
            Err(_) => Ok(build::merge_extractions(std::slice::from_ref(value))),
        },
    }
}

#[derive(Parser)]
#[command(name = "graphify-core")]
#[command(about = "Core graph building engine for graphify", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Discover and classify files in a directory
    Detect {
        /// Root directory to scan
        root: PathBuf,

        /// Follow symlinks
        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,
    },

    /// Incremental detection — only new or modified files
    DetectIncremental {
        /// Root directory to scan
        root: PathBuf,

        /// Path to the manifest file from a previous run
        #[arg(long)]
        manifest: Option<String>,

        /// Follow symlinks
        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,
    },

    /// Extract structural nodes and edges from code files
    Extract {
        /// Files to extract
        #[arg(required = true)]
        paths: Vec<String>,
    },

    /// Merge extraction JSON into a normalized graph payload
    Build {},

    /// Run community detection on a graph payload
    Cluster {},

    /// Run graph analysis on a graph payload and communities
    Analyze {},

    /// Render GRAPH_REPORT.md content from a graph payload
    Report {},

    /// Export graph.json-compatible node-link data
    ExportJson {},

    /// Export Neo4j Cypher text
    ExportCypher {},

    /// Export GraphML text
    ExportGraphml {},

    /// Export HTML visualization to a file
    ExportHtml {
        /// Output HTML path
        #[arg(long)]
        output: PathBuf,
    },

    /// Export an Obsidian vault to a directory
    ExportObsidian {
        /// Output directory
        #[arg(long = "output-dir")]
        output_dir: PathBuf,
    },

    /// Export an Obsidian canvas file
    ExportCanvas {
        /// Output canvas path
        #[arg(long)]
        output: PathBuf,
    },

    /// Export an SVG visualization to a file
    ExportSvg {
        /// Output SVG path
        #[arg(long)]
        output: PathBuf,
    },

    /// Export a markdown wiki to a directory
    ExportWiki {
        /// Output directory
        #[arg(long = "output-dir")]
        output_dir: PathBuf,
    },

    /// Rebuild the code graph and write graphify-out outputs
    RebuildCode {
        /// Root directory to scan
        root: PathBuf,

        /// Follow symlinks
        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,

        /// Report date override (YYYY-MM-DD)
        #[arg(long)]
        today: Option<String>,
    },

    /// Re-cluster an existing graph.json and regenerate report outputs
    ClusterOnly {
        /// Root directory containing graphify-out/graph.json
        root: PathBuf,

        /// Report date override (YYYY-MM-DD)
        #[arg(long)]
        today: Option<String>,
    },

    /// Watch a directory and rebuild or flag graph updates on change
    Watch {
        /// Root directory to watch
        root: PathBuf,

        /// Debounce window in seconds
        #[arg(long, default_value_t = 3.0)]
        debounce: f64,

        /// Follow symlinks
        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,

        /// Report date override (YYYY-MM-DD)
        #[arg(long)]
        today: Option<String>,
    },

    /// Query graph.json for relevant local context
    Query {
        /// Question or keyword search
        question: String,

        /// Use DFS instead of BFS
        #[arg(long, default_value_t = false)]
        dfs: bool,

        /// Traversal depth
        #[arg(long, default_value_t = 2)]
        depth: usize,

        /// Approximate output token budget
        #[arg(long, default_value_t = 2000)]
        budget: usize,

        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Find the shortest path between two nodes
    #[command(name = "path")]
    ShortestPath {
        /// Source node label
        source: String,

        /// Target node label
        target: String,

        /// Maximum number of hops to allow
        #[arg(long)]
        max_hops: Option<usize>,

        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Explain a node and its neighborhood
    Explain {
        /// Node label or ID
        label: String,

        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Get all direct neighbors of a node
    Neighbors {
        /// Node label or ID
        label: String,

        /// Optional relation filter
        #[arg(long = "relation-filter")]
        relation_filter: Option<String>,

        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Get all nodes in a community
    Community {
        /// Community ID
        community_id: usize,

        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Rank the most connected nodes
    #[command(name = "god-nodes")]
    GodNodes {
        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,

        /// Number of nodes to include
        #[arg(long, default_value_t = 10)]
        top_n: usize,
    },

    /// Print graph summary statistics
    Stats {
        /// Path to graph.json
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Benchmark token reduction versus naive full-corpus reads
    Benchmark {
        /// Path to graph.json
        #[arg(default_value = "graphify-out/graph.json")]
        graph: PathBuf,

        /// Total corpus words
        #[arg(long = "corpus-words")]
        corpus_words: Option<usize>,

        /// Benchmark question. Repeat for multiple questions.
        #[arg(long = "question")]
        questions: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
struct AnalyzeInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default = "default_top_n")]
    top_n: usize,
}

fn default_top_n() -> usize {
    5
}

#[derive(Debug, Deserialize)]
struct ReportInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    cohesion_scores: HashMap<usize, f64>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    god_node_list: Vec<build::GodNode>,
    #[serde(default)]
    surprise_list: Vec<build::SurprisingConnection>,
    #[serde(default)]
    detection_result: serde_json::Value,
    #[serde(default)]
    token_cost: serde_json::Value,
    root: String,
    #[serde(default)]
    suggested_questions: Vec<build::SuggestedQuestion>,
    #[serde(default)]
    today: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExportJsonInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExportHtmlInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
}

#[derive(Debug, Deserialize)]
struct ExportObsidianInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    cohesion: HashMap<usize, f64>,
}

#[derive(Debug, Deserialize)]
struct ExportCanvasInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    node_filenames: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ExportSvgInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default = "default_figsize")]
    figsize: [f64; 2],
}

#[derive(Debug, Deserialize)]
struct ExportWikiInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    cohesion: HashMap<usize, f64>,
    #[serde(default)]
    god_nodes: Vec<build::GodNode>,
}

#[derive(Debug, Deserialize)]
struct GraphInput {
    graph: serde_json::Value,
}

fn default_figsize() -> [f64; 2] {
    [20.0, 14.0]
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut exit_code = 0;

    match cli.command {
        Commands::Detect {
            root,
            follow_symlinks,
        } => {
            let result = detect::detect(&root, follow_symlinks)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::DetectIncremental {
            root,
            manifest,
            follow_symlinks,
        } => {
            let result = detect::detect_incremental(&root, follow_symlinks, manifest.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Extract { paths } => {
            let result = extract::extract_paths(&paths)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Build {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("build expects extraction JSON on stdin");
            }
            let payload: serde_json::Value = serde_json::from_str(&stdin)?;
            let result = parse_graph_value(&payload)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Cluster {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("cluster expects graph JSON on stdin");
            }
            let payload: serde_json::Value = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&payload)?;
            let communities = build::cluster(&graph);
            println!("{}", serde_json::to_string_pretty(&communities)?);
        }
        Commands::Analyze {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("analyze expects graph JSON on stdin");
            }
            let input: AnalyzeInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let result = build::analyze(
                &graph,
                &input.communities,
                &input.community_labels,
                input.top_n,
            );
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Report {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("report expects graph JSON on stdin");
            }
            let input: ReportInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let report = build::generate_report(
                &graph,
                &input.communities,
                &input.cohesion_scores,
                &input.community_labels,
                &input.god_node_list,
                &input.surprise_list,
                &input.detection_result,
                &input.token_cost,
                &input.root,
                &input.suggested_questions,
                input.today.as_deref(),
            );
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "report": report }))?
            );
        }
        Commands::ExportJson {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-json expects graph JSON on stdin");
            }
            let input: ExportJsonInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let result = build::export_json_data(&graph, &input.communities);
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::ExportCypher {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-cypher expects graph JSON on stdin");
            }
            let input: GraphInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let content = build::export_cypher(&graph);
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "content": content }))?
            );
        }
        Commands::ExportGraphml {} => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-graphml expects graph JSON on stdin");
            }
            let input: ExportJsonInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let content = build::export_graphml(&graph, &input.communities);
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "content": content }))?
            );
        }
        Commands::ExportHtml { output } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-html expects graph JSON on stdin");
            }
            let input: ExportHtmlInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            build::export_html_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "output": output }))?
            );
        }
        Commands::ExportObsidian { output_dir } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-obsidian expects graph JSON on stdin");
            }
            let input: ExportObsidianInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let notes_written = build::export_obsidian(
                &graph,
                &input.communities,
                &output_dir,
                &input.community_labels,
                &input.cohesion,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({ "output_dir": output_dir, "notes_written": notes_written })
                )?
            );
        }
        Commands::ExportCanvas { output } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-canvas expects graph JSON on stdin");
            }
            let input: ExportCanvasInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            build::export_canvas_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                &input.node_filenames,
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "output": output }))?
            );
        }
        Commands::ExportSvg { output } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-svg expects graph JSON on stdin");
            }
            let input: ExportSvgInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            build::export_svg_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                (input.figsize[0], input.figsize[1]),
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "output": output }))?
            );
        }
        Commands::ExportWiki { output_dir } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-wiki expects graph JSON on stdin");
            }
            let input: ExportWikiInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            let articles_written = build::export_wiki(
                &graph,
                &input.communities,
                &output_dir,
                &input.community_labels,
                &input.cohesion,
                &input.god_nodes,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output_dir": output_dir,
                    "articles_written": articles_written
                }))?
            );
        }
        Commands::RebuildCode {
            root,
            follow_symlinks,
            today,
        } => {
            let result = pipeline::rebuild_code(&root, follow_symlinks, today.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::ClusterOnly { root, today } => {
            let result = pipeline::cluster_only(&root, today.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Watch {
            root,
            debounce,
            follow_symlinks,
            today,
        } => {
            pipeline::watch(
                &root,
                std::time::Duration::from_secs_f64(debounce.max(0.0)),
                follow_symlinks,
                today.as_deref(),
            )?;
        }
        Commands::Query {
            question,
            dfs,
            depth,
            budget,
            graph,
        } => {
            exit_code = query::run_query_cli(&graph, &question, dfs, depth, budget);
        }
        Commands::ShortestPath {
            source,
            target,
            max_hops,
            graph,
        } => {
            exit_code = query::run_path_cli(&graph, &source, &target, max_hops);
        }
        Commands::Explain { label, graph } => {
            exit_code = query::run_explain_cli(&graph, &label);
        }
        Commands::Neighbors {
            label,
            relation_filter,
            graph,
        } => {
            exit_code = query::run_neighbors_cli(&graph, &label, relation_filter.as_deref());
        }
        Commands::Community {
            community_id,
            graph,
        } => {
            exit_code = query::run_community_cli(&graph, community_id);
        }
        Commands::GodNodes { graph, top_n } => {
            exit_code = query::run_god_nodes_cli(&graph, top_n);
        }
        Commands::Stats { graph } => {
            exit_code = query::run_stats_cli(&graph);
        }
        Commands::Benchmark {
            graph,
            corpus_words,
            questions,
        } => {
            let result = query::run_benchmark_json(&graph, corpus_words, &questions)
                .map_err(anyhow::Error::msg)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }

    Ok(())
}
