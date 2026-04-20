use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde_json::Value;
use std::env;
use std::fs;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::timeutil::current_utc_datetime;

const BLOCKED_HOSTS: &[&str] = &["metadata.google.internal", "metadata.google.com"];
const MAX_FETCH_BYTES: usize = 52_428_800;
const MAX_TEXT_BYTES: usize = 10_485_760;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlType {
    Tweet,
    Arxiv,
    Github,
    Youtube,
    Pdf,
    Image,
    Webpage,
}

impl UrlType {
    pub fn label(self) -> &'static str {
        match self {
            UrlType::Tweet => "tweet",
            UrlType::Arxiv => "arxiv",
            UrlType::Github => "github",
            UrlType::Youtube => "youtube",
            UrlType::Pdf => "pdf",
            UrlType::Image => "image",
            UrlType::Webpage => "webpage",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AddedFile {
    pub path: PathBuf,
    pub kind: String,
}

#[derive(Debug, Clone)]
struct ParsedUrl {
    hostname: String,
    path: String,
    default_port: u16,
}

pub fn detect_url_type(url: &str) -> UrlType {
    let lower = url.to_ascii_lowercase();
    if lower.contains("twitter.com") || lower.contains("x.com") {
        return UrlType::Tweet;
    }
    if lower.contains("arxiv.org") {
        return UrlType::Arxiv;
    }
    if lower.contains("github.com") {
        return UrlType::Github;
    }
    if lower.contains("youtube.com") || lower.contains("youtu.be") {
        return UrlType::Youtube;
    }
    let path = parse_url(url)
        .map(|parsed| parsed.path.to_ascii_lowercase())
        .unwrap_or_default();
    if path.ends_with(".pdf") {
        return UrlType::Pdf;
    }
    if [".png", ".jpg", ".jpeg", ".webp", ".gif"]
        .iter()
        .any(|ext| path.ends_with(ext))
    {
        return UrlType::Image;
    }
    UrlType::Webpage
}

pub fn curl_available() -> bool {
    find_curl_bin().is_some()
}

pub fn add_url(
    url: &str,
    target_dir: &Path,
    author: Option<&str>,
    contributor: Option<&str>,
) -> Result<AddedFile> {
    target_dir
        .mkdir_parents()
        .with_context(|| format!("Failed to create target dir {}", target_dir.display()))?;
    validate_url(url)?;

    match detect_url_type(url) {
        UrlType::Pdf => {
            let out = download_binary(url, ".pdf", target_dir)?;
            Ok(AddedFile {
                path: out,
                kind: "pdf".to_string(),
            })
        }
        UrlType::Image => {
            let suffix = parse_url(url)
                .ok()
                .and_then(|parsed| {
                    Path::new(&parsed.path)
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| format!(".{ext}"))
                })
                .unwrap_or_else(|| ".jpg".to_string());
            let out = download_binary(url, &suffix, target_dir)?;
            Ok(AddedFile {
                path: out,
                kind: "image".to_string(),
            })
        }
        UrlType::Youtube => bail!("Rust add does not ingest YouTube directly; use Python fallback"),
        UrlType::Tweet => {
            let (content, filename) = fetch_tweet(url, author, contributor)?;
            let out = save_text_file(target_dir, &filename, &content)?;
            Ok(AddedFile {
                path: out,
                kind: "tweet".to_string(),
            })
        }
        UrlType::Arxiv => {
            let (content, filename) = fetch_arxiv(url, author, contributor)?;
            let out = save_text_file(target_dir, &filename, &content)?;
            Ok(AddedFile {
                path: out,
                kind: "arxiv".to_string(),
            })
        }
        UrlType::Github | UrlType::Webpage => {
            let (content, filename) = fetch_webpage(url, author, contributor)?;
            let out = save_text_file(target_dir, &filename, &content)?;
            Ok(AddedFile {
                path: out,
                kind: detect_url_type(url).label().to_string(),
            })
        }
    }
}

fn parse_url(url: &str) -> Result<ParsedUrl> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow!("invalid URL {url:?}: missing scheme"))?;
    let scheme = scheme.to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => bail!("blocked URL scheme '{scheme}' - only http and https are allowed. Got: {url:?}"),
    };
    let authority_and_path = rest;
    let authority_end = authority_and_path
        .find(['/', '?', '#'])
        .unwrap_or(authority_and_path.len());
    let authority = &authority_and_path[..authority_end];
    if authority.is_empty() {
        bail!("invalid URL {url:?}: missing host");
    }
    let path = &authority_and_path[authority_end..];
    let hostport = authority.rsplit('@').next().unwrap_or(authority);
    let hostname = if hostport.starts_with('[') {
        let end = hostport
            .find(']')
            .ok_or_else(|| anyhow!("invalid URL {url:?}: malformed IPv6 host"))?;
        hostport[1..end].to_string()
    } else {
        hostport
            .split_once(':')
            .map(|(host, _)| host)
            .unwrap_or(hostport)
            .to_string()
    };
    if hostname.is_empty() {
        bail!("invalid URL {url:?}: missing host");
    }
    Ok(ParsedUrl {
        hostname,
        path: path.to_string(),
        default_port,
    })
}

fn validate_url(url: &str) -> Result<()> {
    let parsed = parse_url(url)?;
    if BLOCKED_HOSTS
        .iter()
        .any(|blocked| blocked.eq_ignore_ascii_case(&parsed.hostname))
    {
        bail!(
            "blocked cloud metadata endpoint '{}' Got: {:?}",
            parsed.hostname,
            url
        );
    }
    for ip in resolve_host_ips(&parsed.hostname, parsed.default_port) {
        if is_blocked_ip(ip) {
            bail!(
                "blocked private/internal IP {} (resolved from '{}'). Got: {:?}",
                ip,
                parsed.hostname,
                url
            );
        }
    }
    Ok(())
}

fn resolve_host_ips(host: &str, port: u16) -> Vec<IpAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return vec![ip];
    }
    (host, port)
        .to_socket_addrs()
        .map(|iter| iter.map(|addr| addr.ip()).collect())
        .unwrap_or_default()
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
                || ip.is_documentation()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
        }
    }
}

fn find_curl_bin() -> Option<String> {
    if let Ok(explicit) = env::var("GRAPHIFY_CURL_BIN")
        && !explicit.trim().is_empty() {
            return Some(explicit);
        }
    Some("curl".to_string())
}

fn run_curl(url: &str, max_time_secs: u32) -> Result<Vec<u8>> {
    let curl = find_curl_bin().ok_or_else(|| anyhow!("curl not found"))?;
    let output = Command::new(curl)
        .args([
            "--proto",
            "=http,https",
            "--proto-redir",
            "=http,https",
            "--location",
            "--silent",
            "--show-error",
            "--fail",
            "--max-time",
            &max_time_secs.to_string(),
            "--user-agent",
            "Mozilla/5.0 graphify/1.0",
            url,
        ])
        .output()
        .with_context(|| format!("failed to execute curl for {url}"))?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "curl failed for {url:?}: {}",
            if detail.is_empty() {
                "unknown error"
            } else {
                &detail
            }
        );
    }
    Ok(output.stdout)
}

fn safe_fetch(url: &str, max_bytes: usize, timeout_secs: u32) -> Result<Vec<u8>> {
    let bytes = run_curl(url, timeout_secs)?;
    if bytes.len() > max_bytes {
        bail!(
            "response from {:?} exceeds size limit ({} MB). Aborting download.",
            url,
            max_bytes / 1_048_576
        );
    }
    Ok(bytes)
}

fn safe_fetch_text(url: &str, max_bytes: usize, timeout_secs: u32) -> Result<String> {
    let bytes = safe_fetch(url, max_bytes, timeout_secs)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn yaml_str(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace(['\n', '\r'], " ")
}

fn safe_filename(url: &str, suffix: &str) -> String {
    let mut name = match parse_url(url) {
        Ok(parsed) => format!("{}{}", parsed.hostname, parsed.path),
        Err(_) => url.to_string(),
    };
    let bad = Regex::new(r"[^\w\-]").unwrap();
    let dup = Regex::new(r"_+").unwrap();
    name = bad.replace_all(&name, "_").into_owned();
    name = name.trim_matches('_').to_string();
    name = dup.replace_all(&name, "_").into_owned();
    let name = name.chars().take(80).collect::<String>();
    format!("{name}{suffix}")
}

fn html_to_markdown(html: &str) -> String {
    let scripts = Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap();
    let styles = Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap();
    let tags = Regex::new(r"(?is)<[^>]+>").unwrap();
    let whitespace = Regex::new(r"\s+").unwrap();
    let text = scripts.replace_all(html, "");
    let text = styles.replace_all(&text, "");
    let text = tags.replace_all(&text, " ");
    whitespace
        .replace_all(&text, " ")
        .trim()
        .chars()
        .take(8_000)
        .collect()
}

fn strip_tags(text: &str) -> String {
    let tags = Regex::new(r"(?is)<[^>]+>").unwrap();
    let whitespace = Regex::new(r"\s+").unwrap();
    whitespace
        .replace_all(&tags.replace_all(text, " "), " ")
        .trim()
        .to_string()
}

fn fetch_tweet(
    url: &str,
    author: Option<&str>,
    contributor: Option<&str>,
) -> Result<(String, String)> {
    let oembed_url = url.replace("x.com", "twitter.com");
    let api_url = format!(
        "https://publish.twitter.com/oembed?url={}&omit_script=true",
        percent_encode(&oembed_url)
    );
    let (tweet_text, tweet_author) = match safe_fetch_text(&api_url, MAX_TEXT_BYTES, 15)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    {
        Some(data) => (
            strip_tags(data.get("html").and_then(Value::as_str).unwrap_or("")),
            data.get("author_name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        ),
        None => (
            format!("Tweet at {url} (could not fetch content)"),
            "unknown".to_string(),
        ),
    };

    let now = current_utc_datetime().iso_string();
    let content = format!(
        "---\nsource_url: \"{}\"\ntype: tweet\nauthor: \"{}\"\ncaptured_at: {}\ncontributor: \"{}\"\n---\n\n# Tweet by @{}\n\n{}\n\nSource: {}\n",
        yaml_str(url),
        yaml_str(&tweet_author),
        now,
        yaml_str(contributor.or(author).unwrap_or("unknown")),
        tweet_author,
        tweet_text,
        url
    );
    Ok((content, safe_filename(url, ".md")))
}

fn fetch_webpage(
    url: &str,
    author: Option<&str>,
    contributor: Option<&str>,
) -> Result<(String, String)> {
    let html = safe_fetch_text(url, MAX_TEXT_BYTES, 15)?;
    let title_re = Regex::new(r"(?is)<title[^>]*>(.*?)</title>").unwrap();
    let title = title_re
        .captures(&html)
        .and_then(|captures| captures.get(1))
        .map(|m| collapse_whitespace(m.as_str()))
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| url.to_string());
    let markdown = html_to_markdown(&html);
    let now = current_utc_datetime().iso_string();
    let content = format!(
        "---\nsource_url: \"{}\"\ntype: webpage\ntitle: \"{}\"\ncaptured_at: {}\ncontributor: \"{}\"\n---\n\n# {}\n\nSource: {}\n\n---\n\n{}\n",
        yaml_str(url),
        yaml_str(&title),
        now,
        yaml_str(contributor.or(author).unwrap_or("unknown")),
        title,
        url,
        markdown.chars().take(12_000).collect::<String>()
    );
    Ok((content, safe_filename(url, ".md")))
}

fn fetch_arxiv(
    url: &str,
    author: Option<&str>,
    contributor: Option<&str>,
) -> Result<(String, String)> {
    let arxiv_id_re = Regex::new(r"(\d{4}\.\d{4,5})").unwrap();
    let Some(arxiv_id) = arxiv_id_re
        .captures(url)
        .and_then(|captures| captures.get(1))
        .map(|m| m.as_str().to_string())
    else {
        return fetch_webpage(url, author, contributor);
    };

    let api_url = format!("https://export.arxiv.org/abs/{arxiv_id}");
    let html = safe_fetch_text(&api_url, MAX_TEXT_BYTES, 15).unwrap_or_default();
    let abstract_re =
        Regex::new(r#"(?is)class=\"abstract[^\"]*\"[^>]*>(.*?)</blockquote>"#).unwrap();
    let title_re = Regex::new(r#"(?is)class=\"title[^\"]*\"[^>]*>(.*?)</h1>"#).unwrap();
    let authors_re = Regex::new(r#"(?is)class=\"authors\"[^>]*>(.*?)</div>"#).unwrap();
    let abstract_text = abstract_re
        .captures(&html)
        .and_then(|captures| captures.get(1))
        .map(|m| strip_tags(m.as_str()))
        .unwrap_or_default();
    let title = title_re
        .captures(&html)
        .and_then(|captures| captures.get(1))
        .map(|m| strip_tags(m.as_str()))
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| arxiv_id.clone());
    let paper_authors = authors_re
        .captures(&html)
        .and_then(|captures| captures.get(1))
        .map(|m| strip_tags(m.as_str()))
        .unwrap_or_default();
    let now = current_utc_datetime().iso_string();
    let content = format!(
        "---\nsource_url: \"{}\"\narxiv_id: \"{}\"\ntype: paper\ntitle: \"{}\"\npaper_authors: \"{}\"\ncaptured_at: {}\ncontributor: \"{}\"\n---\n\n# {}\n\n**Authors:** {}\n**arXiv:** {}\n\n## Abstract\n\n{}\n\nSource: {}\n",
        yaml_str(url),
        yaml_str(&arxiv_id),
        yaml_str(&title),
        yaml_str(&paper_authors),
        now,
        yaml_str(contributor.or(author).unwrap_or("unknown")),
        title,
        paper_authors,
        arxiv_id,
        abstract_text,
        url
    );
    Ok((content, format!("arxiv_{}.md", arxiv_id.replace('.', "_"))))
}

fn download_binary(url: &str, suffix: &str, target_dir: &Path) -> Result<PathBuf> {
    let filename = safe_filename(url, suffix);
    let out_path = unique_target_path(target_dir, &filename)?;
    let bytes = safe_fetch(url, MAX_FETCH_BYTES, 30)?;
    fs::write(&out_path, bytes)
        .with_context(|| format!("Failed to write {}", out_path.display()))?;
    Ok(out_path)
}

fn save_text_file(target_dir: &Path, filename: &str, content: &str) -> Result<PathBuf> {
    let out_path = unique_target_path(target_dir, filename)?;
    fs::write(&out_path, content)
        .with_context(|| format!("Failed to write {}", out_path.display()))?;
    Ok(out_path)
}

fn unique_target_path(target_dir: &Path, filename: &str) -> Result<PathBuf> {
    let mut out_path = target_dir.join(filename);
    let stem = Path::new(filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("item");
    let ext = Path::new(filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{ext}"))
        .unwrap_or_default();
    let mut counter = 1usize;
    while out_path.exists() && counter < 1000 {
        out_path = target_dir.join(format!("{stem}_{counter}{ext}"));
        counter += 1;
    }
    Ok(out_path)
}

fn collapse_whitespace(text: &str) -> String {
    Regex::new(r"\s+")
        .unwrap()
        .replace_all(text, " ")
        .trim()
        .to_string()
}

fn percent_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

trait CreateDirAll {
    fn mkdir_parents(&self) -> std::io::Result<()>;
}

impl CreateDirAll for Path {
    fn mkdir_parents(&self) -> std::io::Result<()> {
        fs::create_dir_all(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn detects_url_types() {
        assert_eq!(detect_url_type("https://x.com/test"), UrlType::Tweet);
        assert_eq!(
            detect_url_type("https://arxiv.org/abs/1706.03762"),
            UrlType::Arxiv
        );
        assert_eq!(
            detect_url_type("https://example.invalid/file.pdf"),
            UrlType::Pdf
        );
        assert_eq!(
            detect_url_type("https://example.invalid/p.png"),
            UrlType::Image
        );
        assert_eq!(detect_url_type("https://youtu.be/abc"), UrlType::Youtube);
        assert_eq!(
            detect_url_type("https://example.invalid/page"),
            UrlType::Webpage
        );
    }

    #[test]
    fn validate_url_blocks_local_targets() {
        assert!(validate_url("file:///tmp/test").is_err());
        assert!(validate_url("http://127.0.0.1/test").is_err());
        assert!(validate_url("http://localhost/test").is_err());
    }

    #[test]
    fn html_to_markdown_strips_tags() {
        let markdown = html_to_markdown(
            "<html><head><title>Doc</title><style>.x{}</style></head><body><script>bad()</script><h1>Hello</h1><p>world</p></body></html>",
        );
        assert!(markdown.contains("Hello"));
        assert!(markdown.contains("world"));
        assert!(!markdown.contains("bad()"));
    }

    #[cfg(unix)]
    #[test]
    fn add_url_writes_markdown_with_fake_curl() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("fake-curl");
        let html_path = dir.path().join("page.html");
        fs::write(
            &html_path,
            "<html><title>Example Title</title><body><h1>Hello</h1><p>world</p></body></html>",
        )
        .unwrap();
        let mut file = fs::File::create(&script).unwrap();
        writeln!(file, "#!/bin/sh\ncat {}\n", html_path.display()).unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let old = env::var("GRAPHIFY_CURL_BIN").ok();
        unsafe {
            env::set_var("GRAPHIFY_CURL_BIN", script.as_os_str());
        }
        let out_dir = dir.path().join("raw");
        let added = add_url("https://docs.example/page", &out_dir, Some("Alice"), None).unwrap();
        if let Some(previous) = old {
            unsafe {
                env::set_var("GRAPHIFY_CURL_BIN", previous);
            }
        } else {
            unsafe {
                env::remove_var("GRAPHIFY_CURL_BIN");
            }
        }

        let content = fs::read_to_string(&added.path).unwrap();
        assert_eq!(added.kind, "webpage");
        assert!(content.contains("# Example Title"));
        assert!(content.contains("Source: https://docs.example/page"));
        assert!(content.contains("Hello world"));
    }
}
