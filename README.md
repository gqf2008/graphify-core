# graphify-core

Rust implementation of graphify's core graph workflows.

This directory is kept self-contained so it can be split into a standalone repository later without depending on files from the parent Python package.

## Current scope

- deterministic detection and AST extraction
- graph construction, clustering, analysis, and exports
- user-facing `graphify` Rust CLI
- stage-oriented `graphify-core` Rust CLI
- assistant setup/install assets under `assets/skills/`

## Local commands

```bash
cargo test --quiet
cargo run --quiet --bin graphify -- --help
cargo run --quiet --bin graphify-core -- --help
```

## Split readiness

The crate now keeps its own embedded skill assets and version stamping, so moving `graphify-core/` into a separate repository no longer requires reading `../graphify/...` assets or the parent `pyproject.toml`.
