//! Semantic extraction for documents, papers, and images via LLM.
//!
//! Reads non-code files (markdown, PDF, docx, images), sends their content to
//! an LLM (Claude or OpenAI), and extracts entities + relationships as graph
//! nodes and edges.
//!
//! Results are cached per-file in `graphify-out/cache/` via `crate::cache`.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use crate::cache;
use crate::schema::{Edge, Extraction, Node};

const SYSTEM_PROMPT: &str = r#"You are a knowledge-graph extractor.
Given a document or image, extract key entities (concepts, people, organisations, technologies, topics) and the relationships between them.

Return **only** a JSON object in this exact shape (no markdown fences, no extra text):

{
  "nodes": [
    {
      "id": "unique_snake_case_id",
      "label": "Human-readable Name",
      "node_type": "concept"
    }
  ],
  "edges": [
    {
      "source": "id_of_source",
      "target": "id_of_target",
      "relation": "describes|uses|relates_to|depends_on|contains"
    }
  ]
}

Rules:
- Use snake_case for IDs.
- Labels should be concise (1-3 words).
- Every edge must connect two node IDs that exist in the nodes list.
- If the content is empty or unrecognisable, return empty arrays."#;

// ── Claude text API ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct ClaudeMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct ClaudeRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    messages: Vec<ClaudeMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
}

#[derive(Debug, Clone, Deserialize)]
struct ClaudeContent {
    #[serde(rename = "type")]
    ty: String,
    text: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContent>,
}

// ── Claude vision API ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct ClaudeVisionRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<serde_json::Value>,
    system: String,
}

// ── OpenAI text API ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct OpenAiMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct OpenAiRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiMessage<'a>>,
    max_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessageContent,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenAiMessageContent {
    content: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
}

// ── OpenAI vision API ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct OpenAiVisionRequest {
    model: String,
    messages: Vec<serde_json::Value>,
    max_tokens: u32,
}

// ── Shared response parsing ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct LlmExtraction {
    #[serde(default)]
    nodes: Vec<LlmNode>,
    #[serde(default)]
    edges: Vec<LlmEdge>,
}

#[derive(Debug, Clone, Deserialize)]
struct LlmNode {
    id: String,
    label: String,
    #[serde(default = "default_concept")]
    node_type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LlmEdge {
    source: String,
    target: String,
    relation: String,
}

fn default_concept() -> String {
    "concept".to_string()
}

// ── File type helpers ───────────────────────────────────────────────────────

fn is_image(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase().as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "bmp" | "tiff"
    )
}

fn image_mime_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "tiff" | "tif" => "image/tiff",
        _ => "image/png",
    }
}

fn read_document_text(path: &Path) -> String {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "pdf" => crate::detect::extract_pdf_text(path),
        "docx" => crate::detect::docx_to_markdown(path),
        "xlsx" => crate::detect::xlsx_to_markdown(path),
        _ => fs::read_to_string(path).unwrap_or_default(),
    }
}

fn encode_image_base64(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("cannot read image: {}", path.display()))?;
    Ok(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes))
}

// ── LLM callers ─────────────────────────────────────────────────────────────

fn parse_llm_json(raw: &str) -> Result<LlmExtraction> {
    let json_str = raw
        .trim()
        .strip_prefix("```json")
        .and_then(|s| s.strip_suffix("```"))
        .or_else(|| raw.trim().strip_prefix("```").and_then(|s| s.strip_suffix("```")))
        .unwrap_or(raw)
        .trim();
    serde_json::from_str(json_str)
        .with_context(|| format!("LLM returned invalid JSON: {json_str:.200}"))
}

fn extraction_from_llm(llm: LlmExtraction, source_file: &str, file_type: &str) -> Extraction {
    let mut extraction = Extraction::default();
    for n in llm.nodes {
        extraction.nodes.push(Node {
            id: n.id.clone(),
            label: n.label,
            file_type: file_type.to_string(),
            source_file: source_file.to_string(),
            node_type: Some(n.node_type),
            ..Default::default()
        });
    }
    for e in llm.edges {
        extraction.edges.push(Edge {
            source: e.source,
            target: e.target,
            relation: e.relation,
            confidence: "INFERRED".to_string(),
            source_file: source_file.to_string(),
            confidence_score: Some(0.7),
            ..Default::default()
        });
    }
    extraction
}

/// Call Claude text API synchronously.
fn call_claude(prompt: &str) -> Result<String> {
    let api_key = env::var("ANTHROPIC_API_KEY")
        .context("ANTHROPIC_API_KEY not set")?;
    let client = reqwest::blocking::Client::new();
    let req = ClaudeRequest {
        model: "claude-3-5-haiku-20241022",
        max_tokens: 4096,
        system: Some(SYSTEM_PROMPT),
        messages: vec![ClaudeMessage {
            role: "user",
            content: prompt,
        }],
    };
    let resp: ClaudeResponse = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&req)
        .send()
        .context("Claude API request failed")?
        .json()
        .context("Claude API response is not valid JSON")?;

    resp.content
        .into_iter()
        .find(|c| c.ty == "text")
        .map(|c| c.text)
        .context("Claude response contains no text content")
}

/// Call Claude vision API synchronously.
fn call_claude_vision(base64_data: &str, media_type: &str) -> Result<String> {
    let api_key = env::var("ANTHROPIC_API_KEY")
        .context("ANTHROPIC_API_KEY not set")?;
    let client = reqwest::blocking::Client::new();

    let content = json!([
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64_data
            }
        },
        {
            "type": "text",
            "text": SYSTEM_PROMPT
        }
    ]);

    let req = ClaudeVisionRequest {
        model: "claude-3-5-sonnet-20241022".to_string(),
        max_tokens: 4096,
        messages: vec![json!({"role": "user", "content": content})],
        system: SYSTEM_PROMPT.to_string(),
    };

    let resp: ClaudeResponse = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&req)
        .send()
        .context("Claude vision API request failed")?
        .json()
        .context("Claude vision API response is not valid JSON")?;

    resp.content
        .into_iter()
        .find(|c| c.ty == "text")
        .map(|c| c.text)
        .context("Claude vision response contains no text content")
}

/// Call OpenAI text API synchronously.
fn call_openai(prompt: &str) -> Result<String> {
    let api_key = env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY not set")?;
    let client = reqwest::blocking::Client::new();
    let req = OpenAiRequest {
        model: "gpt-4o-mini",
        max_tokens: 4096,
        messages: vec![
            OpenAiMessage {
                role: "system",
                content: SYSTEM_PROMPT,
            },
            OpenAiMessage {
                role: "user",
                content: prompt,
            },
        ],
    };
    let resp: OpenAiResponse = client
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&req)
        .send()
        .context("OpenAI API request failed")?
        .json()
        .context("OpenAI API response is not valid JSON")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .context("OpenAI response contains no choices")
}

/// Call OpenAI vision API synchronously.
fn call_openai_vision(base64_data: &str, media_type: &str) -> Result<String> {
    let api_key = env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY not set")?;
    let client = reqwest::blocking::Client::new();

    let data_url = format!("data:{};base64,{}", media_type, base64_data);
    let req = OpenAiVisionRequest {
        model: "gpt-4o".to_string(),
        max_tokens: 4096,
        messages: vec![json!({
            "role": "user",
            "content": [
                {"type": "text", "text": SYSTEM_PROMPT},
                {"type": "image_url", "image_url": {"url": data_url}}
            ]
        })],
    };

    let resp: OpenAiResponse = client
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&req)
        .send()
        .context("OpenAI vision API request failed")?
        .json()
        .context("OpenAI vision API response is not valid JSON")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .context("OpenAI vision response contains no choices")
}

/// Call whichever LLM is configured (text mode).
fn call_llm(prompt: &str) -> Result<String> {
    if env::var("ANTHROPIC_API_KEY").is_ok() {
        call_claude(prompt)
    } else if env::var("OPENAI_API_KEY").is_ok() {
        call_openai(prompt)
    } else {
        bail!(
            "No LLM API key configured. Set ANTHROPIC_API_KEY or OPENAI_API_KEY \
             to enable semantic extraction."
        )
    }
}

/// Call whichever LLM is configured (vision mode).
fn call_llm_vision(base64_data: &str, media_type: &str) -> Result<String> {
    if env::var("ANTHROPIC_API_KEY").is_ok() {
        call_claude_vision(base64_data, media_type)
    } else if env::var("OPENAI_API_KEY").is_ok() {
        call_openai_vision(base64_data, media_type)
    } else {
        bail!(
            "No LLM API key configured. Set ANTHROPIC_API_KEY or OPENAI_API_KEY \
             to enable image semantic extraction."
        )
    }
}

// ── Per-file extraction ─────────────────────────────────────────────────────

fn extract_document_semantic(path: &Path) -> Result<Extraction> {
    let text = read_document_text(path);
    if text.trim().is_empty() {
        return Ok(Extraction::default());
    }
    let truncated = if text.len() > 120_000 {
        &text[..120_000]
    } else {
        &text
    };
    let prompt = format!(
        "Document path: {}\n\n---\n{}\n---\n\nExtract the knowledge graph.",
        path.display(),
        truncated
    );
    let raw = call_llm(&prompt)?;
    let llm = parse_llm_json(&raw)?;
    Ok(extraction_from_llm(llm, &path.to_string_lossy(), "document"))
}

fn extract_image_semantic(path: &Path) -> Result<Extraction> {
    let base64_data = encode_image_base64(path)?;
    let media_type = image_mime_type(path);
    let raw = call_llm_vision(&base64_data, media_type)?;
    let llm = parse_llm_json(&raw)?;
    Ok(extraction_from_llm(llm, &path.to_string_lossy(), "image"))
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Extract semantic content from a list of document / paper / image paths.
///
/// Checks the per-file cache first; uncached files are sent to the LLM and
/// the results are written back to cache.
pub fn extract_semantic_documents(paths: &[String], root: &Path) -> Result<Extraction> {
    if paths.is_empty() {
        return Ok(Extraction::default());
    }

    let (cached, uncached) = cache::check_semantic_cache(
        &paths.iter().map(PathBuf::from).collect::<Vec<_>>(),
        root,
    );

    let mut combined = Extraction::default();
    for c in cached {
        append_extraction(&mut combined, c);
    }

    if uncached.is_empty() {
        return Ok(combined);
    }

    let results: Vec<(String, Result<Extraction>)> = uncached
        .into_par_iter()
        .map(|path_buf| {
            let path_str = path_buf.to_string_lossy().to_string();
            let path = Path::new(&path_str);
            let result = if is_image(path) {
                extract_image_semantic(path)
            } else {
                extract_document_semantic(path)
            };
            (path_str, result)
        })
        .collect();

    for (path_str, result) in results {
        match result {
            Ok(extraction) => {
                let path = Path::new(&path_str);
                let _ = cache::save_cached(path, &extraction, root);
                append_extraction(&mut combined, extraction);
            }
            Err(e) => {
                eprintln!("[graphify semantic] Skipping {}: {e}", path_str);
            }
        }
    }

    Ok(combined)
}

fn append_extraction(dst: &mut Extraction, src: Extraction) {
    dst.nodes.extend(src.nodes);
    dst.edges.extend(src.edges);
    dst.hyperedges.extend(src.hyperedges);
    dst.input_tokens += src.input_tokens;
    dst.output_tokens += src.output_tokens;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_document_text_markdown() {
        let tmp = std::env::temp_dir().join(format!("graphify-sem-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let md = tmp.join("doc.md");
        fs::write(&md, "# Hello\n\nThis is a test.").unwrap();
        let text = read_document_text(&md);
        assert!(text.contains("Hello"));

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_image_mime_type() {
        assert_eq!(image_mime_type(Path::new("foo.png")), "image/png");
        assert_eq!(image_mime_type(Path::new("foo.jpg")), "image/jpeg");
        assert_eq!(image_mime_type(Path::new("foo.jpeg")), "image/jpeg");
        assert_eq!(image_mime_type(Path::new("foo.webp")), "image/webp");
    }

    #[test]
    fn test_is_image() {
        assert!(is_image(Path::new("foo.png")));
        assert!(is_image(Path::new("foo.jpg")));
        assert!(!is_image(Path::new("foo.pdf")));
        assert!(!is_image(Path::new("foo.md")));
    }

    #[test]
    fn test_llm_json_strip_fences() {
        let raw = "```json\n{\"nodes\":[],\"edges\":[]}\n```";
        let stripped = raw
            .trim()
            .strip_prefix("```json")
            .and_then(|s| s.strip_suffix("```"))
            .or_else(|| raw.trim().strip_prefix("```").and_then(|s| s.strip_suffix("```")))
            .unwrap_or(raw)
            .trim();
        assert_eq!(stripped, "{\"nodes\":[],\"edges\":[]}");
    }

    #[test]
    fn test_parse_llm_json_valid() {
        let raw = r#"{"nodes":[{"id":"a","label":"A","node_type":"concept"}],"edges":[{"source":"a","target":"b","relation":"uses"}]}"#;
        let result = parse_llm_json(raw).unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].id, "a");
        assert_eq!(result.nodes[0].label, "A");
        assert_eq!(result.nodes[0].node_type, "concept");
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].source, "a");
    }

    #[test]
    fn test_parse_llm_json_with_fences() {
        let raw = "```json\n{\"nodes\":[{\"id\":\"x\",\"label\":\"X\"}],\"edges\":[]}\n```";
        let result = parse_llm_json(raw).unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].id, "x");
    }

    #[test]
    fn test_parse_llm_json_empty() {
        let raw = r#"{"nodes":[],"edges":[]}"#;
        let result = parse_llm_json(raw).unwrap();
        assert!(result.nodes.is_empty());
        assert!(result.edges.is_empty());
    }

    #[test]
    fn test_parse_llm_json_defaults_node_type() {
        let raw = r#"{"nodes":[{"id":"a","label":"A"}],"edges":[]}"#;
        let result = parse_llm_json(raw).unwrap();
        assert_eq!(result.nodes[0].node_type, "concept");
    }

    #[test]
    fn test_encode_image_base64() {
        let tmp = std::env::temp_dir().join(format!("graphify-sem-img-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let img = tmp.join("test.png");
        fs::write(&img, b"\x89PNG\r\n\x1a\n").unwrap();
        let b64 = encode_image_base64(&img).unwrap();
        assert!(!b64.is_empty());
        assert_eq!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &b64).unwrap(), b"\x89PNG\r\n\x1a\n");

        let _ = fs::remove_dir_all(&tmp);
    }
}
