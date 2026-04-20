use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use graphify_core::{detect, extract, ingest, memory, pipeline, query, serve, setup, timeutil};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn parse_graph_value(value: &Value) -> Result<graphify_core::build::Graph> {
    Ok(graphify_core::build::coerce_graph(value)?)
}

#[derive(Parser)]
#[command(name = "graphify")]
#[command(about = "Rust CLI for graphify graph workflows", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Install the graphify skill for a coding assistant platform
    Install {
        /// Platform to install for; defaults to windows on Windows, claude elsewhere
        #[arg(long)]
        platform: Option<String>,

        #[arg(long = "home-dir", hide = true)]
        home_dir: Option<PathBuf>,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,

        #[arg(long = "version-stamp", hide = true)]
        version_stamp: Option<String>,
    },

    /// Configure CLAUDE.md and Claude Code hooks in the current project
    Claude {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure GEMINI.md and Gemini CLI hooks in the current project
    Gemini {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "home-dir", hide = true)]
        home_dir: Option<PathBuf>,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,

        #[arg(long = "version-stamp", hide = true)]
        version_stamp: Option<String>,
    },

    /// Configure Cursor project rules
    Cursor {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Install or remove the Copilot home skill
    Copilot {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "home-dir", hide = true)]
        home_dir: Option<PathBuf>,

        #[arg(long = "version-stamp", hide = true)]
        version_stamp: Option<String>,
    },

    /// Configure VS Code Copilot Chat skill and project instructions
    Vscode {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "home-dir", hide = true)]
        home_dir: Option<PathBuf>,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,

        #[arg(long = "version-stamp", hide = true)]
        version_stamp: Option<String>,
    },

    /// Configure Kiro project steering and skill references
    Kiro {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure Google Antigravity project rules and skill references
    Antigravity {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "home-dir", hide = true)]
        home_dir: Option<PathBuf>,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,

        #[arg(long = "version-stamp", hide = true)]
        version_stamp: Option<String>,
    },

    /// Install or remove git hooks for graph rebuilds
    Hook {
        #[command(subcommand)]
        action: HookAction,

        #[arg(long, hide = true, default_value = ".")]
        path: PathBuf,
    },

    /// Configure project AGENTS.md for Aider
    Aider {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for Codex
    Codex {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for OpenCode
    Opencode {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for OpenClaw
    Claw {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for Factory Droid
    Droid {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for Trae
    Trae {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for Trae CN
    #[command(name = "trae-cn")]
    TraeCn {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
    },

    /// Configure project AGENTS.md for Hermes
    Hermes {
        #[command(subcommand)]
        action: InstallAction,

        #[arg(long = "project-dir", hide = true, default_value = ".")]
        project_dir: PathBuf,
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
    Path {
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

    #[command(hide = true, name = "neighbors")]
    Neighbors {
        label: String,

        #[arg(long = "relation-filter")]
        relation_filter: Option<String>,

        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    #[command(hide = true, name = "community")]
    Community {
        community_id: usize,

        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    #[command(hide = true, name = "god-nodes")]
    GodNodes {
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,

        #[arg(long, default_value_t = 10)]
        top_n: usize,
    },

    #[command(hide = true, name = "stats")]
    Stats {
        #[arg(long, default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Start an MCP stdio server backed by the local graph
    Serve {
        /// Path to graph.json
        #[arg(default_value = "graphify-out/graph.json")]
        graph: PathBuf,
    },

    /// Watch a folder and rebuild the graph on code changes
    Watch {
        /// Root directory to watch
        #[arg(default_value = ".")]
        path: PathBuf,

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

    /// Re-extract code files and update the graph
    Update {
        /// Root directory to rebuild
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Follow symlinks
        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,

        /// Report date override (YYYY-MM-DD)
        #[arg(long)]
        today: Option<String>,

        /// Also write graphify-out/wiki markdown articles
        #[arg(long, default_value_t = false)]
        wiki: bool,
    },

    /// Rerun clustering on an existing graph.json and regenerate report
    #[command(name = "cluster-only")]
    ClusterOnly {
        /// Root directory containing graphify-out/graph.json
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Report date override (YYYY-MM-DD)
        #[arg(long)]
        today: Option<String>,

        /// Also write graphify-out/wiki markdown articles
        #[arg(long, default_value_t = false)]
        wiki: bool,
    },

    /// Measure token reduction versus naive full-corpus reads
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

    /// Save a Q&A result for graph feedback loop
    #[command(name = "save-result")]
    SaveResult {
        /// The question asked
        #[arg(long)]
        question: String,

        /// The answer to save
        #[arg(long)]
        answer: String,

        /// Query type: query|path_query|explain
        #[arg(long = "type", default_value = "query")]
        query_type: String,

        /// Source node labels cited in the answer
        #[arg(long = "nodes", num_args = 1..)]
        nodes: Vec<String>,

        /// Memory directory
        #[arg(long = "memory-dir", default_value = "graphify-out/memory")]
        memory_dir: PathBuf,

        /// Emit machine-readable JSON
        #[arg(long = "json", hide = true, default_value_t = false)]
        json_output: bool,
    },

    /// Fetch a URL into a graphify-ready raw file
    Add {
        /// URL to fetch
        url: String,

        /// Target directory
        #[arg(long = "dir", default_value = "raw")]
        target_dir: PathBuf,

        /// Author metadata
        #[arg(long)]
        author: Option<String>,

        /// Contributor metadata
        #[arg(long)]
        contributor: Option<String>,

        /// Emit machine-readable JSON
        #[arg(long = "json", hide = true, default_value_t = false)]
        json_output: bool,
    },

    #[command(hide = true, name = "detect")]
    Detect {
        root: PathBuf,

        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,
    },

    #[command(hide = true, name = "detect-incremental")]
    DetectIncremental {
        root: PathBuf,

        #[arg(long)]
        manifest: Option<String>,

        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,
    },

    #[command(hide = true, name = "extract")]
    Extract {
        #[arg(required = true)]
        paths: Vec<String>,
    },

    #[command(hide = true, name = "build")]
    Build {},

    #[command(hide = true, name = "cluster")]
    Cluster {},

    #[command(hide = true, name = "analyze")]
    Analyze {},

    #[command(hide = true, name = "report")]
    Report {},

    #[command(hide = true, name = "export-json")]
    ExportJson {},

    #[command(hide = true, name = "export-cypher")]
    ExportCypher {},

    #[command(hide = true, name = "export-graphml")]
    ExportGraphml {},

    #[command(hide = true, name = "export-html")]
    ExportHtml {
        #[arg(long)]
        output: PathBuf,
    },

    #[command(hide = true, name = "export-html-3d")]
    ExportHtml3d {
        #[arg(long)]
        output: PathBuf,
    },

    #[command(hide = true, name = "export-obsidian")]
    ExportObsidian {
        #[arg(long = "output-dir")]
        output_dir: PathBuf,
    },

    #[command(hide = true, name = "export-canvas")]
    ExportCanvas {
        #[arg(long)]
        output: PathBuf,
    },

    #[command(hide = true, name = "export-svg")]
    ExportSvg {
        #[arg(long)]
        output: PathBuf,
    },

    #[command(hide = true, name = "export-wiki")]
    ExportWiki {
        #[arg(long = "output-dir")]
        output_dir: PathBuf,
    },

    #[command(hide = true, name = "rebuild-code")]
    RebuildCode {
        root: PathBuf,

        #[arg(long, default_value_t = false)]
        follow_symlinks: bool,

        #[arg(long)]
        today: Option<String>,

        #[arg(long, default_value_t = false)]
        wiki: bool,
    },

    #[command(hide = true, name = "benchmark-json")]
    BenchmarkJson {
        #[arg(default_value = "graphify-out/graph.json")]
        graph: PathBuf,

        #[arg(long = "corpus-words")]
        corpus_words: Option<usize>,

        #[arg(long = "question")]
        questions: Vec<String>,
    },

    #[command(hide = true, name = "setup-install-platform")]
    SetupInstallPlatform {
        #[arg(long)]
        platform: String,
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "version-stamp")]
        version_stamp: String,
    },

    #[command(hide = true, name = "setup-claude-install")]
    SetupClaudeInstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-claude-uninstall")]
    SetupClaudeUninstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-gemini-install")]
    SetupGeminiInstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        #[arg(long = "version-stamp")]
        version_stamp: String,
    },

    #[command(hide = true, name = "setup-gemini-uninstall")]
    SetupGeminiUninstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-vscode-install")]
    SetupVscodeInstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        #[arg(long = "version-stamp")]
        version_stamp: String,
    },

    #[command(hide = true, name = "setup-vscode-uninstall")]
    SetupVscodeUninstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-cursor-install")]
    SetupCursorInstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-cursor-uninstall")]
    SetupCursorUninstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-agents-install")]
    SetupAgentsInstall {
        #[arg(long)]
        platform: String,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-agents-uninstall")]
    SetupAgentsUninstall {
        #[arg(long, default_value = "")]
        platform: String,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-hook-install")]
    SetupHookInstall {
        #[arg(long, default_value = ".")]
        path: PathBuf,
    },

    #[command(hide = true, name = "setup-hook-uninstall")]
    SetupHookUninstall {
        #[arg(long, default_value = ".")]
        path: PathBuf,
    },

    #[command(hide = true, name = "setup-hook-status")]
    SetupHookStatus {
        #[arg(long, default_value = ".")]
        path: PathBuf,
    },

    #[command(hide = true, name = "setup-kiro-install")]
    SetupKiroInstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-kiro-uninstall")]
    SetupKiroUninstall {
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    #[command(hide = true, name = "setup-antigravity-install")]
    SetupAntigravityInstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        #[arg(long = "version-stamp")]
        version_stamp: String,
    },

    #[command(hide = true, name = "setup-antigravity-uninstall")]
    SetupAntigravityUninstall {
        #[arg(long = "home-dir")]
        home_dir: PathBuf,
        #[arg(long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },
}

#[derive(Debug, serde::Deserialize)]
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

#[derive(Debug, serde::Deserialize)]
struct ReportInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    cohesion_scores: HashMap<usize, f64>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    god_node_list: Vec<graphify_core::build::GodNode>,
    #[serde(default)]
    surprise_list: Vec<graphify_core::build::SurprisingConnection>,
    #[serde(default)]
    detection_result: serde_json::Value,
    #[serde(default)]
    token_cost: serde_json::Value,
    root: String,
    #[serde(default)]
    suggested_questions: Vec<graphify_core::build::SuggestedQuestion>,
    #[serde(default)]
    today: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExportJsonInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
}

#[derive(Debug, serde::Deserialize)]
struct ExportHtmlInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExportObsidianInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    cohesion: HashMap<usize, f64>,
}

#[derive(Debug, serde::Deserialize)]
struct ExportCanvasInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    node_filenames: HashMap<String, String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExportSvgInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default = "default_figsize")]
    figsize: [f64; 2],
}

#[derive(Debug, serde::Deserialize)]
struct ExportWikiInput {
    graph: serde_json::Value,
    #[serde(default)]
    communities: HashMap<usize, Vec<String>>,
    #[serde(default)]
    community_labels: HashMap<usize, String>,
    #[serde(default)]
    cohesion: HashMap<usize, f64>,
    #[serde(default)]
    god_nodes: Vec<graphify_core::build::GodNode>,
}

#[derive(Debug, serde::Deserialize)]
struct GraphInput {
    graph: serde_json::Value,
}

fn default_figsize() -> [f64; 2] {
    [20.0, 14.0]
}

#[derive(Subcommand, Clone, Copy)]
enum InstallAction {
    Install,
    Uninstall,
}

#[derive(Subcommand, Clone, Copy)]
enum HookAction {
    Install,
    Uninstall,
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut exit_code = 0;

    match cli.command {
        Commands::Install {
            platform,
            home_dir,
            project_dir,
            version_stamp,
        } => {
            let platform = platform.unwrap_or_else(default_install_platform);
            let version_stamp = version_stamp.unwrap_or_else(default_version_stamp);
            match platform.as_str() {
                "gemini" => print_lines(setup::gemini_install(
                    &resolve_home_dir(home_dir)?,
                    &project_dir,
                    &version_stamp,
                )?),
                "cursor" => print_lines(setup::cursor_install(&project_dir)?),
                "opencode" => print_lines(setup::opencode_install(
                    &resolve_home_dir(home_dir)?,
                    &project_dir,
                    &version_stamp,
                )?),
                "vscode" => print_lines(setup::vscode_install(
                    &resolve_home_dir(home_dir)?,
                    &project_dir,
                    &version_stamp,
                )?),
                _ => print_lines(setup::install_platform(
                    &resolve_home_dir(home_dir)?,
                    &platform,
                    &version_stamp,
                )?),
            }
        }
        Commands::Claude {
            action,
            project_dir,
        } => match action {
            InstallAction::Install => print_lines(setup::claude_install(&project_dir)?),
            InstallAction::Uninstall => print_lines(setup::claude_uninstall(&project_dir)?),
        },
        Commands::Gemini {
            action,
            home_dir,
            project_dir,
            version_stamp,
        } => match action {
            InstallAction::Install => print_lines(setup::gemini_install(
                &resolve_home_dir(home_dir)?,
                &project_dir,
                &version_stamp.unwrap_or_else(default_version_stamp),
            )?),
            InstallAction::Uninstall => print_lines(setup::gemini_uninstall(
                &resolve_home_dir(home_dir)?,
                &project_dir,
            )?),
        },
        Commands::Cursor {
            action,
            project_dir,
        } => match action {
            InstallAction::Install => print_lines(setup::cursor_install(&project_dir)?),
            InstallAction::Uninstall => print_lines(setup::cursor_uninstall(&project_dir)?),
        },
        Commands::Copilot {
            action,
            home_dir,
            version_stamp,
        } => match action {
            InstallAction::Install => print_lines(setup::install_platform(
                &resolve_home_dir(home_dir)?,
                "copilot",
                &version_stamp.unwrap_or_else(default_version_stamp),
            )?),
            InstallAction::Uninstall => print_lines(setup::uninstall_platform_skill(
                &resolve_home_dir(home_dir)?,
                "copilot",
            )?),
        },
        Commands::Vscode {
            action,
            home_dir,
            project_dir,
            version_stamp,
        } => match action {
            InstallAction::Install => print_lines(setup::vscode_install(
                &resolve_home_dir(home_dir)?,
                &project_dir,
                &version_stamp.unwrap_or_else(default_version_stamp),
            )?),
            InstallAction::Uninstall => print_lines(setup::vscode_uninstall(
                &resolve_home_dir(home_dir)?,
                &project_dir,
            )?),
        },
        Commands::Kiro {
            action,
            project_dir,
        } => match action {
            InstallAction::Install => print_lines(setup::kiro_install(&project_dir)?),
            InstallAction::Uninstall => print_lines(setup::kiro_uninstall(&project_dir)?),
        },
        Commands::Antigravity {
            action,
            home_dir,
            project_dir,
            version_stamp,
        } => match action {
            InstallAction::Install => print_lines(setup::antigravity_install(
                &resolve_home_dir(home_dir)?,
                &project_dir,
                &version_stamp.unwrap_or_else(default_version_stamp),
            )?),
            InstallAction::Uninstall => print_lines(setup::antigravity_uninstall(
                &resolve_home_dir(home_dir)?,
                &project_dir,
            )?),
        },
        Commands::Hook { action, path } => match action {
            HookAction::Install => print_lines(setup::hook_install(&path)?),
            HookAction::Uninstall => print_lines(setup::hook_uninstall(&path)?),
            HookAction::Status => print_lines(setup::hook_status(&path)?),
        },
        Commands::Aider {
            action,
            project_dir,
        } => run_agents_action("aider", action, &project_dir)?,
        Commands::Codex {
            action,
            project_dir,
        } => run_agents_action("codex", action, &project_dir)?,
        Commands::Opencode {
            action,
            project_dir,
        } => run_agents_action("opencode", action, &project_dir)?,
        Commands::Claw {
            action,
            project_dir,
        } => run_agents_action("claw", action, &project_dir)?,
        Commands::Droid {
            action,
            project_dir,
        } => run_agents_action("droid", action, &project_dir)?,
        Commands::Trae {
            action,
            project_dir,
        } => run_agents_action("trae", action, &project_dir)?,
        Commands::TraeCn {
            action,
            project_dir,
        } => run_agents_action("trae-cn", action, &project_dir)?,
        Commands::Hermes {
            action,
            project_dir,
        } => run_agents_action("hermes", action, &project_dir)?,
        Commands::Query {
            question,
            dfs,
            depth,
            budget,
            graph,
        } => {
            exit_code = query::run_query_cli(&graph, &question, dfs, depth, budget);
        }
        Commands::Path {
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
        Commands::Serve { graph } => {
            serve::run_stdio_server(&graph)?;
        }
        Commands::Watch {
            path,
            debounce,
            follow_symlinks,
            today,
        } => {
            require_path_exists(&path)?;
            let today = today.unwrap_or_else(today_utc);
            pipeline::watch(
                &path,
                Duration::from_secs_f64(debounce.max(0.0)),
                follow_symlinks,
                Some(today.as_str()),
            )?;
        }
        Commands::Update {
            path,
            follow_symlinks,
            today,
            wiki,
        } => {
            require_path_exists(&path)?;
            println!(
                "Re-extracting code files in {} (no LLM needed)...",
                path.display()
            );
            let today = today.unwrap_or_else(today_utc);
            let result =
                pipeline::rebuild_code(&path, follow_symlinks, Some(today.as_str()), wiki)?;
            if result.ok {
                if result.wiki_path.is_some() {
                    println!(
                        "Code graph updated. graph.json, graph-3d.html, GRAPH_REPORT.md and graphify-out/wiki refreshed. For doc/paper/image changes run /graphify --update in your AI assistant."
                    );
                } else {
                    println!(
                        "Code graph updated. graph.json, graph-3d.html and GRAPH_REPORT.md refreshed. For doc/paper/image changes run /graphify --update in your AI assistant."
                    );
                }
            } else {
                println!("{}", result.message);
                eprintln!("Nothing to update or rebuild failed — check output above.");
                exit_code = 1;
            }
        }
        Commands::ClusterOnly { path, today, wiki } => {
            let graph_json = path.join("graphify-out").join("graph.json");
            if !graph_json.exists() {
                bail!(
                    "error: no graph found at {} — run /graphify first",
                    graph_json.display()
                );
            }
            let today = today.unwrap_or_else(today_utc);
            let result = pipeline::cluster_only(&path, Some(today.as_str()), wiki)?;
            if result.wiki_path.is_some() {
                println!(
                    "Done — {} communities. GRAPH_REPORT.md, graph.json, graph-3d.html and graphify-out/wiki updated.",
                    result.communities
                );
            } else {
                println!(
                    "Done — {} communities. GRAPH_REPORT.md, graph.json and graph-3d.html updated.",
                    result.communities
                );
            }
        }
        Commands::Benchmark {
            graph,
            corpus_words,
            questions,
        } => {
            let result = query::run_benchmark_json(&graph, corpus_words, &questions)
                .map_err(anyhow::Error::msg)?;
            print_benchmark(&result);
        }
        Commands::SaveResult {
            question,
            answer,
            query_type,
            nodes,
            memory_dir,
            json_output,
        } => {
            let out =
                memory::save_query_result(&question, &answer, &memory_dir, &query_type, &nodes)?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&json!({"path": out}))?);
            } else {
                println!("Saved to {}", out.display());
            }
        }
        Commands::Add {
            url,
            target_dir,
            author,
            contributor,
            json_output,
        } => {
            let url_kind = ingest::detect_url_type(&url).label().to_string();
            let added = match maybe_python_ingest_fallback(
                &url,
                &target_dir,
                author.as_deref(),
                contributor.as_deref(),
            ) {
                Ok(path) => ingest::AddedFile {
                    path,
                    kind: url_kind,
                },
                Err(_) => {
                    ingest::add_url(&url, &target_dir, author.as_deref(), contributor.as_deref())?
                }
            };
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "path": added.path,
                        "kind": added.kind,
                    }))?
                );
            } else {
                match added.kind.as_str() {
                    "pdf" => println!(
                        "Downloaded PDF: {}",
                        added.path.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    "image" => println!(
                        "Downloaded image: {}",
                        added.path.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    _ => println!(
                        "Saved {}: {}",
                        added.kind,
                        added.path.file_name().unwrap_or_default().to_string_lossy()
                    ),
                }
            }
        }
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
            let communities = graphify_core::build::cluster(&graph);
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
            let result = graphify_core::build::analyze(
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
            let report = graphify_core::build::generate_report(
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
                serde_json::to_string_pretty(&json!({ "report": report }))?
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
            let result = graphify_core::build::export_json_data(&graph, &input.communities);
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
            let content = graphify_core::build::export_cypher(&graph);
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "content": content }))?
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
            let content = graphify_core::build::export_graphml(&graph, &input.communities);
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "content": content }))?
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
            graphify_core::build::export_html_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "output": output }))?
            );
        }
        Commands::ExportHtml3d { output } => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            if stdin.trim().is_empty() {
                bail!("export-html-3d expects graph JSON on stdin");
            }
            let input: ExportHtmlInput = serde_json::from_str(&stdin)?;
            let graph = parse_graph_value(&input.graph)?;
            graphify_core::build::export_html_3d_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "output": output }))?
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
            let notes_written = graphify_core::build::export_obsidian(
                &graph,
                &input.communities,
                &output_dir,
                &input.community_labels,
                &input.cohesion,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &json!({ "output_dir": output_dir, "notes_written": notes_written })
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
            graphify_core::build::export_canvas_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                &input.node_filenames,
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "output": output }))?
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
            graphify_core::build::export_svg_to_path(
                &graph,
                &input.communities,
                &input.community_labels,
                (input.figsize[0], input.figsize[1]),
                &output,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "output": output }))?
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
            let articles_written = graphify_core::build::export_wiki(
                &graph,
                &input.communities,
                &output_dir,
                &input.community_labels,
                &input.cohesion,
                &input.god_nodes,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &json!({ "output_dir": output_dir, "articles_written": articles_written })
                )?
            );
        }
        Commands::RebuildCode {
            root,
            follow_symlinks,
            today,
            wiki,
        } => {
            let result = pipeline::rebuild_code(&root, follow_symlinks, today.as_deref(), wiki)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::BenchmarkJson {
            graph,
            corpus_words,
            questions,
        } => {
            let result = query::run_benchmark_json(&graph, corpus_words, &questions)
                .map_err(anyhow::Error::msg)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::SetupInstallPlatform {
            platform,
            home_dir,
            version_stamp,
        } => print_lines(setup::install_platform(
            &home_dir,
            &platform,
            &version_stamp,
        )?),
        Commands::SetupClaudeInstall { project_dir } => {
            print_lines(setup::claude_install(&project_dir)?)
        }
        Commands::SetupClaudeUninstall { project_dir } => {
            print_lines(setup::claude_uninstall(&project_dir)?)
        }
        Commands::SetupGeminiInstall {
            home_dir,
            project_dir,
            version_stamp,
        } => print_lines(setup::gemini_install(
            &home_dir,
            &project_dir,
            &version_stamp,
        )?),
        Commands::SetupGeminiUninstall {
            home_dir,
            project_dir,
        } => print_lines(setup::gemini_uninstall(&home_dir, &project_dir)?),
        Commands::SetupVscodeInstall {
            home_dir,
            project_dir,
            version_stamp,
        } => print_lines(setup::vscode_install(
            &home_dir,
            &project_dir,
            &version_stamp,
        )?),
        Commands::SetupVscodeUninstall {
            home_dir,
            project_dir,
        } => print_lines(setup::vscode_uninstall(&home_dir, &project_dir)?),
        Commands::SetupCursorInstall { project_dir } => {
            print_lines(setup::cursor_install(&project_dir)?)
        }
        Commands::SetupCursorUninstall { project_dir } => {
            print_lines(setup::cursor_uninstall(&project_dir)?)
        }
        Commands::SetupAgentsInstall {
            platform,
            project_dir,
        } => print_lines(setup::agents_install(&project_dir, &platform)?),
        Commands::SetupAgentsUninstall {
            platform,
            project_dir,
        } => print_lines(setup::agents_uninstall(&project_dir, &platform)?),
        Commands::SetupHookInstall { path } => print_lines(setup::hook_install(&path)?),
        Commands::SetupHookUninstall { path } => print_lines(setup::hook_uninstall(&path)?),
        Commands::SetupHookStatus { path } => print_lines(setup::hook_status(&path)?),
        Commands::SetupKiroInstall { project_dir } => {
            print_lines(setup::kiro_install(&project_dir)?)
        }
        Commands::SetupKiroUninstall { project_dir } => {
            print_lines(setup::kiro_uninstall(&project_dir)?)
        }
        Commands::SetupAntigravityInstall {
            home_dir,
            project_dir,
            version_stamp,
        } => print_lines(setup::antigravity_install(
            &home_dir,
            &project_dir,
            &version_stamp,
        )?),
        Commands::SetupAntigravityUninstall {
            home_dir,
            project_dir,
        } => print_lines(setup::antigravity_uninstall(&home_dir, &project_dir)?),
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

fn maybe_python_ingest_fallback(
    url: &str,
    target_dir: &std::path::Path,
    author: Option<&str>,
    contributor: Option<&str>,
) -> Result<PathBuf> {
    let needs_python = matches!(ingest::detect_url_type(url), ingest::UrlType::Youtube)
        || !ingest::curl_available();
    if !needs_python {
        bail!("python ingest fallback not needed");
    }

    let python = find_python_bin().ok_or_else(|| anyhow::anyhow!("python not found"))?;
    let target_dir_str = target_dir.to_string_lossy().into_owned();
    let mut command = std::process::Command::new(python);
    command.args(["-m", "graphify.ingest", url, &target_dir_str]);
    if let Some(author) = author {
        command.args(["--author", author]);
    }
    if let Some(contributor) = contributor {
        command.args(["--contributor", contributor]);
    }
    let output = command
        .output()
        .context("failed to execute python ingest fallback")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        bail!(
            "python ingest fallback failed: {}",
            if stderr.is_empty() { stdout } else { stderr }
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().rev() {
        if let Some(path) = line.strip_prefix("Ready for graphify: ") {
            return Ok(PathBuf::from(path.trim()));
        }
    }
    bail!("python ingest fallback did not report output path");
}

fn find_python_bin() -> Option<String> {
    if let Ok(explicit) = env::var("GRAPHIFY_PYTHON_BIN")
        && !explicit.trim().is_empty() {
            return Some(explicit);
        }
    for candidate in ["python3", "python"] {
        if let Ok(output) = std::process::Command::new(candidate)
            .arg("--version")
            .output()
            && output.status.success() {
                return Some(candidate.to_string());
            }
    }
    None
}

fn today_utc() -> String {
    timeutil::current_utc_datetime().date_string()
}

fn require_path_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        bail!("error: path not found: {}", path.display());
    }
    Ok(())
}

fn run_agents_action(platform: &str, action: InstallAction, project_dir: &Path) -> Result<()> {
    let lines = match action {
        InstallAction::Install => setup::agents_install(project_dir, platform)?,
        InstallAction::Uninstall => setup::agents_uninstall(project_dir, platform)?,
    };
    print_lines(lines);
    Ok(())
}

fn default_install_platform() -> String {
    if cfg!(windows) {
        "windows".to_string()
    } else {
        "claude".to_string()
    }
}

fn resolve_home_dir(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("error: could not determine home directory"))
}

fn default_version_stamp() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn print_benchmark(result: &Value) {
    if let Some(error) = result.get("error").and_then(Value::as_str) {
        println!("Benchmark error: {error}");
        return;
    }

    let corpus_words = result
        .get("corpus_words")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let corpus_tokens = result
        .get("corpus_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let nodes = result.get("nodes").and_then(Value::as_u64).unwrap_or(0);
    let edges = result.get("edges").and_then(Value::as_u64).unwrap_or(0);
    let avg_query_tokens = result
        .get("avg_query_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let reduction_ratio = result
        .get("reduction_ratio")
        .map(json_number_to_string)
        .unwrap_or_else(|| "0".to_string());

    println!("\ngraphify token reduction benchmark");
    println!("{}", "─".repeat(50));
    println!(
        "  Corpus:          {} words -> ~{} tokens (naive)",
        format_with_commas(corpus_words),
        format_with_commas(corpus_tokens),
    );
    println!(
        "  Graph:           {} nodes, {} edges",
        format_with_commas(nodes),
        format_with_commas(edges),
    );
    println!(
        "  Avg query cost:  ~{} tokens",
        format_with_commas(avg_query_tokens),
    );
    println!("  Reduction:       {reduction_ratio}x fewer tokens per query");
    println!("\n  Per question:");
    for question in result
        .get("per_question")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let reduction = question
            .get("reduction")
            .map(json_number_to_string)
            .unwrap_or_else(|| "0".to_string());
        let text = question
            .get("question")
            .and_then(Value::as_str)
            .unwrap_or("");
        println!("    [{reduction}x] {}", truncate(text, 55));
    }
    println!();
}

fn json_number_to_string(value: &Value) -> String {
    if let Some(number) = value.as_i64() {
        return number.to_string();
    }
    if let Some(number) = value.as_u64() {
        return number.to_string();
    }
    if let Some(number) = value.as_f64() {
        return format!("{number:.1}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
    }
    value.to_string()
}

fn truncate(text: &str, max_len: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_len).collect();
    if chars.next().is_some() {
        truncated
    } else {
        text.to_string()
    }
}

fn format_with_commas(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn print_lines(lines: Vec<String>) {
    for line in lines {
        println!("{line}");
    }
}
