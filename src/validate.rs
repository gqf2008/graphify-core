//! Extraction JSON validation — ported from `graphify/validate.py`.
//!
//! Validates that an extraction object has the required structure,
//! valid enum values, and no dangling edges before graph assembly.

use serde_json::Value;
use std::collections::HashSet;

static VALID_FILE_TYPES: &[&str] = &["code", "document", "paper", "image", "rationale"];
static VALID_CONFIDENCES: &[&str] = &["EXTRACTED", "INFERRED", "AMBIGUOUS"];

/// Validate an extraction JSON value against the graphify schema.
///
/// Returns a list of error strings — an empty list means the extraction is valid.
///
/// Ported from `graphify/validate.py::validate_extraction()`.
pub fn validate_extraction(data: &Value) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();

    if !data.is_object() {
        errors.push("Extraction must be a JSON object".to_string());
        return errors;
    }

    // ── Nodes ────────────────────────────────────────────────────────────────
    match data.get("nodes") {
        None => errors.push("Missing required key 'nodes'".to_string()),
        Some(Value::Array(arr)) => {
            for (i, node) in arr.iter().enumerate() {
                if !node.is_object() {
                    errors.push(format!("Node {i} must be an object"));
                    continue;
                }
                let node_id = node
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                for field in ["id", "label", "file_type", "source_file"] {
                    if node.get(field).is_none() {
                        errors.push(format!(
                            "Node {i} (id={node_id:?}) missing required field '{field}'"
                        ));
                    }
                }
                if let Some(ft) = node.get("file_type").and_then(|v| v.as_str())
                    && !VALID_FILE_TYPES.contains(&ft) {
                        errors.push(format!(
                            "Node {i} (id={node_id:?}) has invalid file_type '{ft}' \
                             — must be one of {VALID_FILE_TYPES:?}"
                        ));
                    }
            }
        }
        Some(_) => errors.push("'nodes' must be a list".to_string()),
    }

    // ── Edges ────────────────────────────────────────────────────────────────
    // Accept "links" (NetworkX <= 3.1) as fallback for "edges"
    let edge_list = if data.get("edges").is_some() {
        data.get("edges")
    } else {
        data.get("links")
    };

    match edge_list {
        None => errors.push("Missing required key 'edges'".to_string()),
        Some(Value::Array(arr)) => {
            let node_ids: HashSet<&str> = data
                .get("nodes")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|n| n.get("id").and_then(|v| v.as_str()))
                        .collect()
                })
                .unwrap_or_default();

            for (i, edge) in arr.iter().enumerate() {
                if !edge.is_object() {
                    errors.push(format!("Edge {i} must be an object"));
                    continue;
                }
                for field in ["source", "target", "relation", "confidence", "source_file"] {
                    if edge.get(field).is_none() {
                        errors.push(format!("Edge {i} missing required field '{field}'"));
                    }
                }
                if let Some(conf) = edge.get("confidence").and_then(|v| v.as_str())
                    && !VALID_CONFIDENCES.contains(&conf) {
                        errors.push(format!(
                            "Edge {i} has invalid confidence '{conf}' \
                             — must be one of {VALID_CONFIDENCES:?}"
                        ));
                    }
                if let Some(src) = edge.get("source").and_then(|v| v.as_str())
                    && !node_ids.is_empty() && !node_ids.contains(src) {
                        errors.push(format!(
                            "Edge {i} source '{src}' does not match any node id"
                        ));
                    }
                if let Some(tgt) = edge.get("target").and_then(|v| v.as_str())
                    && !node_ids.is_empty() && !node_ids.contains(tgt) {
                        errors.push(format!(
                            "Edge {i} target '{tgt}' does not match any node id"
                        ));
                    }
            }
        }
        Some(_) => errors.push("'edges' must be a list".to_string()),
    }

    errors
}

/// Raise an error if the extraction is invalid.
///
/// Ported from `graphify/validate.py::assert_valid()`.
pub fn assert_valid(data: &Value) -> Result<(), String> {
    let errors = validate_extraction(data);
    if errors.is_empty() {
        Ok(())
    } else {
        let msg = format!(
            "Extraction JSON has {} error(s):\n  • {}",
            errors.len(),
            errors.join("\n  • ")
        );
        Err(msg)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_node() -> Value {
        serde_json::json!({
            "id": "n1",
            "label": "A",
            "file_type": "code",
            "source_file": "a.py"
        })
    }

    fn valid_edge() -> Value {
        serde_json::json!({
            "source": "n1",
            "target": "n2",
            "relation": "contains",
            "confidence": "EXTRACTED",
            "source_file": "a.py",
            "weight": 1.0
        })
    }

    #[test]
    fn test_valid_extraction_passes() {
        let data = serde_json::json!({
            "nodes": [valid_node(), {
                "id": "n2",
                "label": "B",
                "file_type": "document",
                "source_file": "b.md"
            }],
            "edges": [valid_edge()],
            "input_tokens": 0,
            "output_tokens": 0
        });
        assert!(validate_extraction(&data).is_empty());
        assert!(assert_valid(&data).is_ok());
    }

    #[test]
    fn test_missing_nodes_key() {
        let data = serde_json::json!({"edges": []});
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("Missing required key 'nodes'")));
    }

    #[test]
    fn test_missing_edges_key() {
        let data = serde_json::json!({"nodes": []});
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("Missing required key 'edges'")));
    }

    #[test]
    fn test_accepts_links_as_edges_fallback() {
        let data = serde_json::json!({
            "nodes": [valid_node()],
            "links": [{"source": "n1", "target": "n1", "relation": "contains",
                       "confidence": "EXTRACTED", "source_file": "a.py", "weight": 1.0}]
        });
        assert!(validate_extraction(&data).is_empty());
    }

    #[test]
    fn test_node_missing_required_field() {
        let data = serde_json::json!({
            "nodes": [{"id": "n1", "label": "A"}],
            "edges": []
        });
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("missing required field 'file_type'")));
        assert!(errs.iter().any(|e| e.contains("missing required field 'source_file'")));
    }

    #[test]
    fn test_invalid_file_type() {
        let data = serde_json::json!({
            "nodes": [{"id": "n1", "label": "A", "file_type": "unknown", "source_file": "a.py"}],
            "edges": []
        });
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("invalid file_type")));
    }

    #[test]
    fn test_invalid_confidence() {
        let data = serde_json::json!({
            "nodes": [
                {"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"},
                {"id": "n2", "label": "B", "file_type": "code", "source_file": "b.py"}
            ],
            "edges": [{"source": "n1", "target": "n2", "relation": "calls",
                       "confidence": "MAYBE", "source_file": "a.py"}]
        });
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("invalid confidence")));
    }

    #[test]
    fn test_dangling_edge_source() {
        let data = serde_json::json!({
            "nodes": [{"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"}],
            "edges": [{"source": "n_missing", "target": "n1", "relation": "calls",
                       "confidence": "EXTRACTED", "source_file": "a.py"}]
        });
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("source 'n_missing' does not match any node id")));
    }

    #[test]
    fn test_dangling_edge_target() {
        let data = serde_json::json!({
            "nodes": [{"id": "n1", "label": "A", "file_type": "code", "source_file": "a.py"}],
            "edges": [{"source": "n1", "target": "n_missing", "relation": "calls",
                       "confidence": "EXTRACTED", "source_file": "a.py"}]
        });
        let errs = validate_extraction(&data);
        assert!(errs.iter().any(|e| e.contains("target 'n_missing' does not match any node id")));
    }

    #[test]
    fn test_assert_valid_raises_on_errors() {
        let data = serde_json::json!({
            "nodes": [valid_node()],
            "edges": [{"source": "n1", "target": "n_missing", "relation": "calls",
                       "confidence": "EXTRACTED", "source_file": "a.py"}]
        });
        let result = assert_valid(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Extraction JSON has"));
    }
}
