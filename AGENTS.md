# AGENTS.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

# graphify-core — Project Reference

## Project Overview

`graphify-core` is the Rust implementation of graphify's core graph-building engine. It turns any folder of files (code, documents, papers, images) into a navigable knowledge graph through deterministic AST extraction, graph construction, community detection, and multi-format export.

The crate is intentionally self-contained so it can be split into a standalone repository later without depending on a parent Python package.

## Technology Stack

- **Language:** Rust (edition 2024)
- **Build tool:** Cargo
- **AST parsing:** tree-sitter (20+ language grammars)
- **Graph algorithms:** rustworkx-core
- **Parallelism:** rayon
- **CLI framework:** clap (derive macros)
- **Error handling:** anyhow
- **Serialization:** serde + serde_json
- **Document parsing:** pdf-extract, docx-lite, calamine (for .xlsx)

## Architecture

The project is organized as a library (`src/lib.rs`) with two binary entry points:

| Binary | File | Purpose |
|--------|------|---------|
| `graphify-core` | `src/main.rs` | Stage-oriented CLI for scripting and pipelines |
| `graphify` | `src/cli.rs` | User-facing CLI with assistant integrations |

### Core Modules

- **`detect`** — File discovery, type classification (code/document/paper/image/video), corpus health checks, `.graphifyignore` support, sensitive-file skipping, and office-file conversion (.docx/.xlsx → markdown sidecars).
- **`extract`** — AST-based structural extraction using tree-sitter. Supports 20+ languages plus embedded-script extraction for Vue/Svelte. Extracts classes, functions, imports, calls, inheritance, and rationale comments.
- **`build`** — Graph construction (`merge_extractions`), normalization, community detection (Leiden via rustworkx-core's ported graspologic-native implementation), cohesion scoring, god-node ranking, surprising-connection detection, report generation, and exports (JSON, Cypher, GraphML, HTML, SVG, Obsidian, Wiki).
- **`pipeline`** — End-to-end orchestration: `rebuild_code`, `cluster_only`, and `watch`. Preserves non-code semantic nodes/edges across rebuilds.
- **`query`** — Graph traversal (BFS/DFS), shortest path, node explanation, neighbor lookup, community listing, god-node ranking, and stats.
- **`serve`** — MCP stdio server exposing graph query tools to AI assistants.
- **`setup`** — Platform-specific skill installation for Claude, Cursor, VS Code, Gemini, Copilot, Kiro, Antigravity, Aider, Codex, OpenCode, OpenClaw, Factory Droid, Trae, Trae CN, and Hermes.
- **`ingest`** — URL fetching and content ingestion with type detection (tweet, arXiv, GitHub, YouTube, PDF, image, webpage).
- **`memory`** — Save Q&A results to markdown files for graph feedback loops.
- **`schema`** — Shared data types: `Node`, `Edge`, `Extraction`, `RawCall`, `FunctionReturn`.
- **`validate`** — Validation utilities.
- **`timeutil`** — UTC datetime helpers.

### Key Output Directories

Running the pipeline produces artifacts under `<root>/graphify-out/`:

- `graph.json` — normalized node-link graph data
- `GRAPH_REPORT.md` — human-readable report with communities, god nodes, and surprising connections
- `graph-3d.html` — 3D interactive HTML visualization
- `wiki/` — markdown wiki export (optional, enabled with `--wiki`)
- `memory/` — saved query results
- `converted/` — office file markdown sidecars

## Build and Test Commands

```bash
# Run all tests
cargo test --quiet

# Run the user-facing CLI
cargo run --quiet --bin graphify -- --help
cargo run --quiet --bin graphify -- update . --wiki

# Run the stage-oriented CLI
cargo run --quiet --bin graphify-core -- --help
cargo run --quiet --bin graphify-core -- rebuild-code .

# Build release binary
cargo build --release --bin graphify
```

## Testing Strategy

Tests live in `tests/` and use fixture files under `tests/fixtures/`:

| Test file | Coverage |
|-----------|----------|
| `detect_test.rs` | File classification, word counting, detection behavior |
| `extract_test.rs` | AST extraction across languages, no dangling edges, structural edge confidence |
| `build_test.rs` | Graph construction, merging, clustering, god nodes |
| `pipeline_test.rs` | End-to-end detect → extract → build → cluster → analyze |
| `parity_test.rs` | Rust output vs. documented Python baseline counts (no Python runtime needed) |

`src/pipeline.rs` also contains inline unit tests using `tempfile` for temporary directories.

**Fixture samples:** The `tests/fixtures/` directory contains minimal code samples in ~20 languages (Python, Rust, Go, Java, C, C++, TypeScript, JavaScript, Ruby, C#, Kotlin, Scala, PHP, Lua, Swift, Zig, Elixir, Julia, Objective-C, PowerShell, Verilog, Dart, Markdown).

## Code Style Guidelines

- Use `anyhow::{Result, Context, bail}` for error handling; propagate with `?`.
- Match the existing visual style: section dividers like `// ── Section ────────────────────────────────────────────────────`.
- Use `serde_json::Value` for flexible JSON interop at pipeline boundaries; prefer strongly typed structs (`Node`, `Edge`, `Extraction`) inside module logic.
- Parallelize with `rayon::prelude::*` (`.par_iter()`) and `rayon::join` for independent work.
- Tree-sitter language configs are built declaratively via the `LanguageConfig` struct and the `cfg!` macro.
- Keep CLI argument structs close to the `main()` function that dispatches them.
- When adding new export formats, follow the pattern in `build.rs`: accept `&Graph`, `&communities`, `&community_labels`, and write to a path or return a `String`.

## Security Considerations

- **`detect.rs` skips sensitive files** by name pattern (credentials, keys, tokens, `.env`, `.pem`, etc.). Do not remove or weaken these checks.
- **`ingest.rs` blocks internal hosts** (`metadata.google.internal`) and caps fetched content size (`MAX_FETCH_BYTES = 52_428_800`).
- **Office file conversion** writes sanitized markdown sidecars into `graphify-out/converted/`; originals are never exposed in graph output.
- **Hidden files are skipped** during detection; sensitive hidden files are logged in `skipped_sensitive` rather than processed.

## Common Tasks

### Adding support for a new programming language
1. Add the tree-sitter grammar to `Cargo.toml` dependencies.
2. Add a `LanguageConfig` builder in `src/extract.rs` (follow the existing `xx_cfg()` pattern).
3. Map the file extension(s) in `config_for_path()`.
4. Add a minimal sample to `tests/fixtures/sample.<ext>`.
5. Add an extract test in `tests/extract_test.rs`.
6. Add a parity baseline in `tests/parity_test.rs` if the output should be stable.
7. Run `cargo test` to verify.

### Adding a new export format
1. Implement the export function in `src/build.rs`.
2. Add a CLI subcommand in both `src/main.rs` (`graphify-core`) and `src/cli.rs` (`graphify`) if user-facing.
3. Wire the subcommand to the export function in the respective `main()` match arms.
4. Add tests in `tests/build_test.rs` or inline in `src/build.rs` if appropriate.

### Modifying the pipeline
- `rebuild_code()` in `src/pipeline.rs` is the main entry point. It preserves non-code semantic nodes/edges by loading the existing `graph.json` and filtering out code-related items before merging new extractions.
- The `watch()` function polls the filesystem every 500ms with a configurable debounce. Code-only changes trigger automatic rebuild; non-code changes write a `needs_update` flag.

## Important Notes

- **Do not depend on files outside this directory.** The crate is split-ready and embeds its own assets under `assets/skills/`.
- **Tree-sitter grammar versions matter.** The `Cargo.toml` pins specific versions for each grammar. Bumping a grammar may change AST node kinds and break extraction parity.
- **macOS manifest compatibility:** `detect.rs` strips `/private` prefixes from canonicalized paths when the original path starts with `/var`, to maintain compatibility with old Python manifests.
- **Custom ignore files:** `.graphifyignore` is supported at the project root and ancestor directories up to a `.git` boundary, using glob-style matching.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
