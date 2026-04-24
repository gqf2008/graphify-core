use anyhow::{Context, Result, bail};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

static GITHUB_URL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?$").unwrap()
});

fn parse_github_url(url: &str) -> Result<(String, String)> {
    let url = url.trim_end_matches('/');
    let normalized = url
        .strip_suffix(".git")
        .unwrap_or(url);

    GITHUB_URL_RE
        .captures(normalized)
        .map(|caps| (caps[1].to_string(), caps[2].to_string()))
        .with_context(|| format!("not a recognised GitHub URL: {url}"))
}

fn ensure_git_suffix(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.ends_with(".git") {
        url.to_string()
    } else {
        format!("{url}.git")
    }
}

pub fn clone_and_build(
    url: &str,
    branch: Option<&str>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let (owner, repo) = parse_github_url(url)?;

    let dest = if let Some(out_dir) = out {
        out_dir.to_path_buf()
    } else {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("error: could not determine home directory"))?;
        home.join(".graphify").join("repos").join(&owner).join(&repo)
    };

    if dest.join(".git").exists() {
        eprintln!("Repo already cloned at {} — pulling latest...", dest.display());
        let mut cmd = Command::new("git");
        cmd.args(["-C"]).arg(&dest).arg("pull");
        if let Some(b) = branch {
            cmd.args(["origin", b]);
        }
        let result = cmd.output().with_context(|| "failed to run git pull")?;
        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            eprintln!("warning: git pull failed:\n{stderr}");
        }
    } else {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        eprintln!("Cloning {url} → {} ...", dest.display());
        let git_url = ensure_git_suffix(url);
        let mut cmd = Command::new("git");
        cmd.args(["clone", "--depth", "1"]);
        if let Some(b) = branch {
            cmd.args(["--branch", b]);
        }
        cmd.arg(&git_url).arg(&dest);
        let output = cmd.output().with_context(|| "failed to run git clone")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("git clone failed: {stderr}");
        }
    }

    eprintln!("Ready at: {}", dest.display());
    Ok(dest)
}
