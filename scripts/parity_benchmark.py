#!/usr/bin/env python3
"""Parity benchmark: compare Python graphify vs Rust graphify-core on the same fixtures.

Usage:
    PYTHONPATH=../graphify python3 scripts/parity_benchmark.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
RUST_BIN = Path(__file__).resolve().parent.parent / "target" / "debug" / "graphify"
PYTHON_GRAPHIFY = Path(__file__).resolve().parent.parent.parent / "graphify"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sh(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{result.stderr}")
    return result.stdout


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Rust pipeline (graphify-core)
# ---------------------------------------------------------------------------

def run_rust(fixture: Path) -> dict:
    """Run Rust graphify on a fixture and return the exported JSON.
    
    Rust's detect() requires a directory input, so we copy the fixture file
    into a temp subdirectory and run from there.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        fixture_dir = tmpdir_path / "fixture"
        fixture_dir.mkdir()
        # Copy fixture file into the temp directory
        import shutil
        shutil.copy(fixture, fixture_dir / fixture.name)
        # Rust graphify writes to <input_dir>/graphify-out/
        cmd = [str(RUST_BIN), "update", str(fixture_dir)]
        sh(cmd, cwd=tmpdir_path)
        graph_json = fixture_dir / "graphify-out" / "graph.json"
        if not graph_json.exists():
            raise FileNotFoundError(f"Rust did not produce graph.json for {fixture}")
        return load_json(graph_json)


# ---------------------------------------------------------------------------
# Python pipeline (graphify)
# ---------------------------------------------------------------------------

def run_python(fixture: Path) -> dict:
    """Run Python graphify on a fixture and return a JSON-compatible dict.
    
    Uses a temp directory so path-based node IDs align with Rust's output.
    """
    sys.path.insert(0, str(PYTHON_GRAPHIFY))
    try:
        from graphify.extract import extract, collect_files
        from graphify.build import build_from_json
        from graphify.cluster import cluster
        from graphify.analyze import god_nodes, surprising_connections, suggest_questions
    except ImportError as e:
        raise ImportError(f"Cannot import graphify Python package: {e}")

    import shutil
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        fixture_dir = tmpdir_path / "fixture"
        fixture_dir.mkdir()
        shutil.copy(fixture, fixture_dir / fixture.name)
        # Run on the temp directory so paths match Rust's layout
        paths = collect_files(fixture_dir / fixture.name)
        extraction = extract(paths)
        G = build_from_json(extraction)
        communities = cluster(G)
        gods = god_nodes(G, top_n=10)
        surprising = surprising_connections(G, top_n=10)
        community_labels = {cid: f"Community {cid}" for cid in communities}
        questions = suggest_questions(G, communities, community_labels, top_n=5)

        return {
            "nodes": [{"id": n, **G.nodes[n]} for n in G.nodes()],
            "edges": [
                {"source": u, "target": v, **{k: d[k] for k in d if not k.startswith("_")}}
                for u, v, d in G.edges(data=True)
            ],
            "communities": communities,
            "god_nodes": gods,
            "surprising_connections": surprising,
            "suggest_questions": questions,
        }


# ---------------------------------------------------------------------------
# Comparison metrics
# ---------------------------------------------------------------------------

def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _normalize_node_id(nid: str, fixture_name: str) -> str:
    """Normalize file-node IDs by stripping path prefixes.
    
    Both Python and Rust generate file-node IDs from the full path,
    but use different roots. We normalize them to a common basename
    so structural parity is measured independently of path prefixes.
    """
    # If the ID ends with the fixture filename (with underscores),
    # replace the whole thing with a normalized form.
    base = fixture_name.replace(".", "_")
    if nid.endswith(base):
        return base
    return nid


def compare(py_result: dict, rust_result: dict, fixture_name: str = "") -> dict:
    """Compare Python and Rust outputs."""
    # Build normalized node ID maps
    py_node_map = {n["id"]: n for n in py_result.get("nodes", [])}
    rust_node_map = {n["id"]: n for n in rust_result.get("nodes", [])}

    py_norm = {_normalize_node_id(nid, fixture_name): nid for nid in py_node_map}
    rust_norm = {_normalize_node_id(nid, fixture_name): nid for nid in rust_node_map}

    py_nodes = set(py_norm.keys())
    rust_nodes = set(rust_norm.keys())

    # Normalize edges using the normalized node IDs
    py_edges = {
        tuple(sorted((_normalize_node_id(e["source"], fixture_name),
                      _normalize_node_id(e["target"], fixture_name))))
        for e in py_result.get("edges", [])
    }
    rust_edges = {
        tuple(sorted((_normalize_node_id(e["source"], fixture_name),
                      _normalize_node_id(e["target"], fixture_name))))
        for e in rust_result.get("links", [])
    }

    py_communities = py_result.get("communities", {})
    # Rust communities may have string keys from JSON serialization
    raw_rust_comms = rust_result.get("communities", {})
    rust_communities: dict[int, list[str]] = {}
    for k, v in raw_rust_comms.items():
        try:
            rust_communities[int(k)] = v
        except (ValueError, TypeError):
            rust_communities[k] = v

    # God nodes: compare top-10 lists by normalized node ID overlap
    py_gods = {_normalize_node_id(g.get("node", g.get("id", "")), fixture_name) for g in py_result.get("god_nodes", [])}
    rust_gods = {_normalize_node_id(g.get("node", g.get("id", "")), fixture_name) for g in rust_result.get("god_nodes", [])}

    # Surprising connections: compare by normalized (source, target) pairs
    py_surprising = {
        tuple(sorted((_normalize_node_id(s["source"], fixture_name),
                      _normalize_node_id(s["target"], fixture_name))))
        for s in py_result.get("surprising_connections", [])
    }
    rust_surprising = {
        tuple(sorted((_normalize_node_id(s["source"], fixture_name),
                      _normalize_node_id(s["target"], fixture_name))))
        for s in rust_result.get("surprising_connections", [])
    }

    # Suggest questions: compare by text content (filter out None)
    py_questions = {q["question"] for q in py_result.get("suggest_questions", []) if q.get("question")}
    rust_questions = {q["question"] for q in rust_result.get("suggest_questions", []) if q.get("question")}

    # Community count and size distribution
    py_comm_sizes = sorted([len(v) for v in py_communities.values()], reverse=True)
    rust_comm_sizes = sorted([len(v) for v in rust_communities.values()], reverse=True)

    # Node-to-community assignment agreement using normalized IDs
    # Normalize community member IDs too
    py_comm_norm: dict[int, set[str]] = {}
    for cid, members in py_communities.items():
        py_comm_norm[cid] = {_normalize_node_id(m, fixture_name) for m in members}
    rust_comm_norm: dict[int, set[str]] = {}
    for cid, members in rust_communities.items():
        rust_comm_norm[cid] = {_normalize_node_id(m, fixture_name) for m in members}

    common_nodes = py_nodes & rust_nodes
    py_assign = {}
    rust_assign = {}
    for cid, members in py_comm_norm.items():
        for m in members:
            py_assign[m] = cid
    for cid, members in rust_comm_norm.items():
        for m in members:
            rust_assign[m] = cid

    agreements = sum(1 for n in common_nodes if py_assign.get(n) == rust_assign.get(n))
    node_agreement = agreements / len(common_nodes) if common_nodes else 0.0

    return {
        "nodes": {
            "python": len(py_nodes),
            "rust": len(rust_nodes),
            "jaccard": jaccard(py_nodes, rust_nodes),
        },
        "edges": {
            "python": len(py_edges),
            "rust": len(rust_edges),
            "jaccard": jaccard(py_edges, rust_edges),
        },
        "communities": {
            "python_count": len(py_communities),
            "rust_count": len(rust_communities),
            "python_sizes": py_comm_sizes[:20],  # top 20
            "rust_sizes": rust_comm_sizes[:20],
        },
        "god_nodes": {
            "python": list(py_gods),
            "rust": list(rust_gods),
            "jaccard": jaccard(py_gods, rust_gods),
        },
        "surprising_connections": {
            "python": list(py_surprising),
            "rust": list(rust_surprising),
            "jaccard": jaccard(py_surprising, rust_surprising),
        },
        "suggest_questions": {
            "python": list(py_questions),
            "rust": list(rust_questions),
            "jaccard": jaccard(py_questions, rust_questions),
        },
        "node_community_agreement": node_agreement,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("graphify Parity Benchmark")
    print(f"Fixtures: {FIXTURES}")
    print("=" * 60)

    # Ensure Rust binary exists
    if not RUST_BIN.exists():
        print("Building Rust binary...")
        sh(["cargo", "build", "--bin", "graphify"], cwd=Path(__file__).resolve().parent.parent)

    fixtures = sorted(p for p in FIXTURES.iterdir() if p.is_file() and p.suffix in {".py", ".ts", ".js", ".go", ".rs", ".java", ".cpp", ".c", ".kt", ".swift", ".php", ".rb", ".cs", ".scala", ".zig", ".ex", ".exs", ".jl", ".m", ".mm", ".ps1", ".dart", ".lua"})
    if not fixtures:
        print(f"No fixture files found in {FIXTURES}")
        sys.exit(1)

    all_results = {}
    for fixture in fixtures:
        name = fixture.name
        print(f"\n--- {name} ---")
        try:
            print("  [Python] running...")
            py_result = run_python(fixture)
            print(f"  [Python] nodes={len(py_result['nodes'])}, edges={len(py_result['edges'])}, communities={len(py_result['communities'])}")

            print("  [Rust] running...")
            rust_result = run_rust(fixture)
            print(f"  [Rust] nodes={len(rust_result.get('nodes', []))}, edges={len(rust_result.get('links', []))}, communities={len(rust_result.get('communities', {}))}")

            metrics = compare(py_result, rust_result, fixture.name)
            all_results[name] = metrics

            print(f"  Node Jaccard:      {metrics['nodes']['jaccard']:.3f}")
            print(f"  Edge Jaccard:      {metrics['edges']['jaccard']:.3f}")
            print(f"  Communities:       py={metrics['communities']['python_count']} rust={metrics['communities']['rust_count']}")
            print(f"  God-node Jaccard:  {metrics['god_nodes']['jaccard']:.3f}")
            print(f"  Surprising Jaccard:{metrics['surprising_connections']['jaccard']:.3f}")
            print(f"  Question Jaccard:  {metrics['suggest_questions']['jaccard']:.3f}")
            print(f"  Node→Comm Agree:   {metrics['node_community_agreement']:.3f}")
        except Exception as e:
            print(f"  FAILED: {e}")
            import traceback
            traceback.print_exc()
            all_results[name] = {"error": str(e)}

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, metrics in all_results.items():
        if "error" in metrics:
            print(f"{name:20s} ERROR: {metrics['error']}")
            continue
        print(
            f"{name:20s}  "
            f"nodes={metrics['nodes']['jaccard']:.2f}  "
            f"edges={metrics['edges']['jaccard']:.2f}  "
            f"comms=py:{metrics['communities']['python_count']:3d}/rs:{metrics['communities']['rust_count']:3d}  "
            f"gods={metrics['god_nodes']['jaccard']:.2f}  "
            f"surp={metrics['surprising_connections']['jaccard']:.2f}  "
            f"agree={metrics['node_community_agreement']:.2f}"
        )

    # Write full report
    report_path = Path("parity_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nFull report written to {report_path}")


if __name__ == "__main__":
    main()
