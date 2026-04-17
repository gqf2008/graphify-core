use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::timeutil;

pub fn save_query_result(
    question: &str,
    answer: &str,
    memory_dir: &Path,
    query_type: &str,
    source_nodes: &[String],
) -> Result<PathBuf> {
    fs::create_dir_all(memory_dir)
        .with_context(|| format!("cannot create memory directory: {}", memory_dir.display()))?;

    let now = timeutil::current_utc_datetime();
    let slug = question_slug(question);
    let filename = format!("query_{}_{}.md", now.filename_stamp(), slug);

    let mut frontmatter_lines = vec![
        "---".to_string(),
        format!("type: \"{}\"", yaml_str(query_type)),
        format!("date: \"{}\"", now.iso_string()),
        format!("question: \"{}\"", yaml_str(question)),
        "contributor: \"graphify\"".to_string(),
    ];
    if !source_nodes.is_empty() {
        let nodes = source_nodes
            .iter()
            .take(10)
            .map(|node| format!("\"{}\"", yaml_str(node)))
            .collect::<Vec<_>>()
            .join(", ");
        frontmatter_lines.push(format!("source_nodes: [{}]", nodes));
    }
    frontmatter_lines.push("---".to_string());

    let mut body_lines = vec![
        String::new(),
        format!("# Q: {question}"),
        String::new(),
        "## Answer".to_string(),
        String::new(),
        answer.to_string(),
    ];
    if !source_nodes.is_empty() {
        body_lines.push(String::new());
        body_lines.push("## Source Nodes".to_string());
        body_lines.push(String::new());
        body_lines.extend(source_nodes.iter().map(|node| format!("- {node}")));
    }

    let out_path = memory_dir.join(filename);
    fs::write(
        &out_path,
        frontmatter_lines
            .into_iter()
            .chain(body_lines)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .with_context(|| format!("cannot write {}", out_path.display()))?;
    Ok(out_path)
}

fn yaml_str(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn question_slug(question: &str) -> String {
    let slug: String = question
        .chars()
        .flat_map(|ch| {
            let mapped = if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            };
            [mapped]
        })
        .take(50)
        .collect();
    let trimmed = slug.trim_matches('_');
    if trimmed.is_empty() {
        "query".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::save_query_result;
    use anyhow::Result;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn save_query_result_writes_markdown_with_frontmatter() -> Result<()> {
        let dir = tempdir()?;
        let path = save_query_result(
            "How does auth work?",
            "It uses middleware.",
            dir.path(),
            "query",
            &["AuthMiddleware".to_string(), "SessionStore".to_string()],
        )?;
        let content = fs::read_to_string(&path)?;

        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("query_") && name.ends_with("_how_does_auth_work.md")
                })
        );
        assert!(content.contains("type: \"query\""));
        assert!(content.contains("question: \"How does auth work?\""));
        assert!(content.contains("source_nodes: [\"AuthMiddleware\", \"SessionStore\"]"));
        assert!(content.contains("# Q: How does auth work?"));
        assert!(content.contains("- AuthMiddleware"));
        Ok(())
    }

    #[test]
    fn save_query_result_falls_back_to_generic_slug() -> Result<()> {
        let dir = tempdir()?;
        let path = save_query_result("???", "A", dir.path(), "query", &[])?;
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("_query.md"))
        );
        Ok(())
    }
}
