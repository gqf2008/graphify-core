use crate::query;
use anyhow::Result;
use serde_json::{Map, Value, json};
use std::io::{self, BufRead, Write};
use std::path::Path;

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";

pub fn run_stdio_server(graph_path: &Path) -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                continue;
            }
        };
        if let Some(response) = handle_message(&request, graph_path) {
            writeln!(writer, "{}", serde_json::to_string(&response)?)?;
            writer.flush()?;
        }
    }

    Ok(())
}

fn handle_message(request: &Value, graph_path: &Path) -> Option<Value> {
    let object = request.as_object()?;
    let method = object.get("method").and_then(Value::as_str)?;
    let id = object.get("id").cloned();

    match method {
        "initialize" => id.map(|id| initialize_response(id, object.get("params"))),
        "ping" => id.map(success_empty_response),
        "tools/list" => id.map(tools_list_response),
        "tools/call" => id.map(|id| tools_call_response(id, object.get("params"), graph_path)),
        method if method.starts_with("notifications/") => None,
        _ => id.map(|id| error_response(id, -32601, format!("Unknown method: {method}"))),
    }
}

fn initialize_response(id: Value, params: Option<&Value>) -> Value {
    let protocol_version = params
        .and_then(Value::as_object)
        .and_then(|params| params.get("protocolVersion"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": protocol_version,
            "capabilities": {
                "tools": {
                    "listChanged": false
                }
            },
            "serverInfo": {
                "name": "graphify",
                "title": "graphify",
                "version": env!("CARGO_PKG_VERSION")
            },
            "instructions": "Use graphify tools to query the local code graph."
        }
    })
}

fn success_empty_response(id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {}
    })
}

fn tools_list_response(id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "tools": tool_definitions()
        }
    })
}

fn tools_call_response(id: Value, params: Option<&Value>, graph_path: &Path) -> Value {
    let Some(params) = params.and_then(Value::as_object) else {
        return error_response(id, -32602, "Invalid params".to_string());
    };
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error_response(id, -32602, "Missing tool name".to_string());
    };
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    match dispatch_tool(name, &arguments, graph_path) {
        Ok(text) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": text
                    }
                ],
                "isError": false
            }
        }),
        Err(err) if err.starts_with("Unknown tool:") => error_response(id, -32602, err),
        Err(err) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": err
                    }
                ],
                "isError": true
            }
        }),
    }
}

fn error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn dispatch_tool(
    name: &str,
    arguments: &Map<String, Value>,
    graph_path: &Path,
) -> Result<String, String> {
    match name {
        "query_graph" => {
            let question = required_string(arguments, "question")?;
            let mode = optional_string(arguments, "mode").unwrap_or("bfs");
            let depth = optional_usize(arguments, "depth").unwrap_or(3).min(6);
            let token_budget = optional_usize(arguments, "token_budget").unwrap_or(2000);
            query::query_text(graph_path, question, mode == "dfs", depth, token_budget)
        }
        "get_node" => {
            let label = required_string(arguments, "label")?;
            query::explain_text(graph_path, label)
        }
        "get_neighbors" => {
            let label = required_string(arguments, "label")?;
            let relation_filter = optional_string(arguments, "relation_filter");
            query::neighbors_text(graph_path, label, relation_filter)
        }
        "get_community" => {
            let community_id = required_usize(arguments, "community_id")?;
            query::community_text(graph_path, community_id)
        }
        "god_nodes" => {
            let top_n = optional_usize(arguments, "top_n").unwrap_or(10);
            query::god_nodes_text(graph_path, top_n)
        }
        "graph_stats" => query::stats_text(graph_path),
        "shortest_path" => {
            let source = required_string(arguments, "source")?;
            let target = required_string(arguments, "target")?;
            let max_hops = optional_usize(arguments, "max_hops");
            query::path_text(graph_path, source, target, max_hops)
        }
        _ => Err(format!("Unknown tool: {name}")),
    }
}

fn required_string<'a>(arguments: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Missing required string argument: {key}"))
}

fn optional_string<'a>(arguments: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    arguments.get(key).and_then(Value::as_str)
}

fn required_usize(arguments: &Map<String, Value>, key: &str) -> Result<usize, String> {
    optional_usize(arguments, key)
        .ok_or_else(|| format!("Missing required integer argument: {key}"))
}

fn optional_usize(arguments: &Map<String, Value>, key: &str) -> Option<usize> {
    arguments.get(key).and_then(value_to_usize)
}

fn value_to_usize(value: &Value) -> Option<usize> {
    match value {
        Value::Number(number) => number.as_u64().map(|value| value as usize),
        Value::String(text) => text.parse::<usize>().ok(),
        _ => None,
    }
}

fn tool_definitions() -> Value {
    json!([
        {
            "name": "query_graph",
            "description": "Search the knowledge graph using BFS or DFS. Returns relevant nodes and edges as text context.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Natural language question or keyword search"},
                    "mode": {"type": "string", "enum": ["bfs", "dfs"], "default": "bfs", "description": "bfs=broad context, dfs=trace a specific path"},
                    "depth": {"type": "integer", "default": 3, "description": "Traversal depth (1-6)"},
                    "token_budget": {"type": "integer", "default": 2000, "description": "Max output tokens"}
                },
                "required": ["question"]
            }
        },
        {
            "name": "get_node",
            "description": "Get full details for a specific node by label or ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node label or ID to look up"}
                },
                "required": ["label"]
            }
        },
        {
            "name": "get_neighbors",
            "description": "Get all direct neighbors of a node with edge details.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "label": {"type": "string"},
                    "relation_filter": {"type": "string", "description": "Optional: filter by relation type"}
                },
                "required": ["label"]
            }
        },
        {
            "name": "get_community",
            "description": "Get all nodes in a community by community ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "community_id": {"type": "integer", "description": "Community ID (0-indexed by size)"}
                },
                "required": ["community_id"]
            }
        },
        {
            "name": "god_nodes",
            "description": "Return the most connected nodes - the core abstractions of the knowledge graph.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "top_n": {"type": "integer", "default": 10}
                }
            }
        },
        {
            "name": "graph_stats",
            "description": "Return summary statistics: node count, edge count, communities, confidence breakdown.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "shortest_path",
            "description": "Find the shortest path between two concepts in the knowledge graph.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Source concept label or keyword"},
                    "target": {"type": "string", "description": "Target concept label or keyword"},
                    "max_hops": {"type": "integer", "default": 8, "description": "Maximum hops to consider"}
                },
                "required": ["source", "target"]
            }
        }
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    fn sample_graph(path: &Path) {
        fs::write(
            path,
            serde_json::to_string(&json!({
                "nodes": [
                    {"id": "n1", "label": "Parser", "file_type": "code", "source_file": "parser.py", "source_location": "L1", "community": 0},
                    {"id": "n2", "label": "Renderer", "file_type": "code", "source_file": "renderer.py", "source_location": "L2", "community": 0},
                    {"id": "n3", "label": "Report", "file_type": "document", "source_file": "report.md", "source_location": "L3", "community": 1}
                ],
                "links": [
                    {"source": "n1", "target": "n2", "relation": "uses", "confidence": "INFERRED"},
                    {"source": "n2", "target": "n3", "relation": "references", "confidence": "EXTRACTED"}
                ]
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn initialize_echoes_protocol_version() {
        let response = handle_message(
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-03-26"}
            }),
            Path::new("graph.json"),
        )
        .unwrap();

        assert_eq!(response["result"]["protocolVersion"], "2025-03-26");
        assert!(response["result"]["capabilities"]["tools"].is_object());
    }

    #[test]
    fn tools_list_returns_graphify_tools() {
        let response = handle_message(
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            Path::new("graph.json"),
        )
        .unwrap();
        let tools = response["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "query_graph"));
        assert!(tools.iter().any(|tool| tool["name"] == "shortest_path"));
    }

    #[test]
    fn tools_call_dispatches_query_handlers() {
        let dir = tempdir().unwrap();
        let graph = dir.path().join("graph.json");
        sample_graph(&graph);

        let response = handle_message(
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "shortest_path",
                    "arguments": {"source": "Parser", "target": "Report", "max_hops": 8}
                }
            }),
            &graph,
        )
        .unwrap();

        assert_eq!(response["result"]["isError"], false);
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Shortest path (2 hops)"));
    }

    #[test]
    fn tools_call_unknown_tool_returns_protocol_error() {
        let response = handle_message(
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "unknown", "arguments": {}}
            }),
            Path::new("graph.json"),
        )
        .unwrap();

        assert_eq!(response["error"]["code"], -32602);
    }
}
