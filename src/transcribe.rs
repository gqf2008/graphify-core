//! Video/audio transcription using a local Whisper CLI.
//!
//! Mirrors Python `graphify/transcribe.py`.  If the system has `whisper`
//! (OpenAI's CLI) or a compatible tool installed, audio/video files are
//! transcribed to `.txt` transcripts.

use anyhow::{Context, Result, anyhow, bail};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const DEFAULT_MODEL: &str = "base";
const TRANSCRIPTS_DIR: &str = "graphify-out/transcripts";
const FALLBACK_PROMPT: &str = "Use proper punctuation and paragraph breaks.";

fn model_name() -> String {
    env::var("GRAPHIFY_WHISPER_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.into())
}

fn find_whisper() -> Option<String> {
    if let Ok(explicit) = env::var("GRAPHIFY_WHISPER_BIN")
        && !explicit.trim().is_empty()
    {
        return Some(explicit);
    }
    // Try common whisper CLI names
    for cmd in ["whisper", "whisper-cli"] {
        if Command::new(cmd).arg("--help").output().is_ok() {
            return Some(cmd.to_string());
        }
    }
    None
}

/// Build a domain hint for Whisper from god-node labels.
pub fn build_whisper_prompt(god_nodes: &[(String, usize)]) -> String {
    if let Ok(override_prompt) = env::var("GRAPHIFY_WHISPER_PROMPT") {
        return override_prompt;
    }
    let labels: Vec<&str> = god_nodes
        .iter()
        .take(5)
        .map(|(label, _)| label.as_str())
        .filter(|l| !l.is_empty())
        .collect();
    if labels.is_empty() {
        return FALLBACK_PROMPT.into();
    }
    format!(
        "Technical discussion about {}. Use proper punctuation and paragraph breaks.",
        labels.join(", ")
    )
}

/// Transcribe an audio/video file to a `.txt` transcript.
///
/// Uses cached transcript if it already exists unless `force` is true.
/// Returns the path to the saved transcript file.
pub fn transcribe(
    audio_path: &Path,
    output_dir: Option<&Path>,
    initial_prompt: Option<&str>,
    force: bool,
) -> Result<PathBuf> {
    let whisper = find_whisper()
        .ok_or_else(|| anyhow!(
            "Whisper CLI not found. Install OpenAI whisper: pip install openai-whisper, \
             or set GRAPHIFY_WHISPER_BIN to the path of your whisper binary."
        ))?;

    let out_dir = output_dir.map(PathBuf::from).unwrap_or_else(|| PathBuf::from(TRANSCRIPTS_DIR));
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("cannot create transcript dir: {}", out_dir.display()))?;

    let transcript_path = out_dir.join(format!(
        "{}.txt",
        audio_path.file_stem().unwrap_or_default().to_string_lossy()
    ));

    if transcript_path.exists() && !force {
        return Ok(transcript_path);
    }

    let model = model_name();
    let prompt = initial_prompt.unwrap_or(FALLBACK_PROMPT);

    eprintln!(
        "  transcribing {} (model={model}) ...",
        audio_path.display()
    );

    // OpenAI whisper CLI writes output next to the input by default,
    // so we run from the output directory and use the basename.
    let output = Command::new(&whisper)
        .current_dir(&out_dir)
        .args([
            audio_path.to_str().unwrap_or(""),
            "--model",
            &model,
            "--output_format",
            "txt",
            "--output_dir",
            out_dir.to_str().unwrap_or("."),
            "--language",
            "en",
            "--initial_prompt",
            prompt,
        ])
        .output()
        .with_context(|| format!("failed to execute {whisper} for {}", audio_path.display()))?;

    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!("{whisper} failed for {}: {detail}", audio_path.display());
    }

    // Whisper writes <stem>.txt in the output directory
    if !transcript_path.exists() {
        // Try to find any .txt with a matching stem prefix
        let stem = audio_path.file_stem().unwrap_or_default().to_string_lossy().to_string();
        if let Ok(entries) = fs::read_dir(&out_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&stem) && name.ends_with(".txt") {
                    return Ok(entry.path());
                }
            }
        }
        bail!(
            "transcription succeeded but no .txt output found for {}",
            audio_path.display()
        );
    }

    let text = fs::read_to_string(&transcript_path)
        .with_context(|| format!("cannot read transcript: {}", transcript_path.display()))?;
    let lines: Vec<&str> = text.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
    eprintln!(
        "  transcript saved -> {} ({} lines)",
        transcript_path.display(),
        lines.len()
    );
    Ok(transcript_path)
}

/// Transcribe a list of audio/video files, returning paths to the `.txt` outputs.
///
/// Already-transcribed files are returned from cache instantly.
/// Failures are logged and skipped rather than aborting the batch.
pub fn transcribe_all(
    paths: &[String],
    output_dir: Option<&Path>,
    initial_prompt: Option<&str>,
) -> Vec<PathBuf> {
    if paths.is_empty() {
        return Vec::new();
    }

    let mut results = Vec::new();
    for p in paths {
        let path = Path::new(p);
        match transcribe(path, output_dir, initial_prompt, false) {
            Ok(t) => results.push(t),
            Err(e) => {
                eprintln!("[graphify transcribe] Skipping {}: {e}", path.display());
            }
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_build_whisper_prompt_fallback() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::remove_var("GRAPHIFY_WHISPER_PROMPT"); }
        let prompt = build_whisper_prompt(&[]);
        assert_eq!(prompt, FALLBACK_PROMPT);
    }

    #[test]
    fn test_build_whisper_prompt_from_god_nodes() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::remove_var("GRAPHIFY_WHISPER_PROMPT"); }
        let nodes = vec![
            ("Parser".to_string(), 10),
            ("Renderer".to_string(), 8),
        ];
        let prompt = build_whisper_prompt(&nodes);
        assert!(prompt.contains("Parser"));
        assert!(prompt.contains("Renderer"));
        assert!(prompt.contains("Technical discussion about"));
    }

    #[test]
    fn test_build_whisper_prompt_env_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            env::set_var("GRAPHIFY_WHISPER_PROMPT", "Custom domain hint.");
        }
        let prompt = build_whisper_prompt(&[("Python".to_string(), 5)]);
        assert_eq!(prompt, "Custom domain hint.");
        unsafe {
            env::remove_var("GRAPHIFY_WHISPER_PROMPT");
        }
    }

    #[test]
    fn test_build_whisper_prompt_skips_empty_labels() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::remove_var("GRAPHIFY_WHISPER_PROMPT"); }
        let nodes = vec![
            ("".to_string(), 5),
            ("".to_string(), 3),
        ];
        let prompt = build_whisper_prompt(&nodes);
        assert_eq!(prompt, FALLBACK_PROMPT);
    }

    #[test]
    fn test_model_name_reads_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            env::set_var("GRAPHIFY_WHISPER_MODEL", "large-v3");
        }
        assert_eq!(model_name(), "large-v3");
        unsafe {
            env::remove_var("GRAPHIFY_WHISPER_MODEL");
        }
        assert_eq!(model_name(), DEFAULT_MODEL);
    }

    #[test]
    fn test_transcript_path_derived_from_audio_stem() {
        let audio = Path::new("/tmp/video.mp4");
        let out = Path::new("/out");
        let t = out.join("video.txt");
        assert_eq!(
            t,
            out.join(format!(
                "{}.txt",
                audio.file_stem().unwrap().to_string_lossy()
            ))
        );
    }

    #[cfg(unix)]
    fn fake_whisper_bin(dir: &std::path::Path) -> std::path::PathBuf {
        let script = dir.join("fake-whisper");
        fs::write(&script, b"#!/bin/sh\ntouch \"$3.txt\"\n").unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&script, perms).unwrap();
        script
    }

    #[test]
    #[cfg(unix)]
    fn test_transcribe_uses_cache() {
        let dir = tempfile::tempdir().unwrap();
        let video = dir.path().join("lecture.mp4");
        fs::write(&video, b"fake").unwrap();
        let out_dir = dir.path().join("transcripts");
        fs::create_dir_all(&out_dir).unwrap();
        let cached = out_dir.join("lecture.txt");
        fs::write(&cached, "Cached transcript content.").unwrap();

        let old = env::var("GRAPHIFY_WHISPER_BIN").ok();
        let fake = fake_whisper_bin(dir.path());
        unsafe { env::set_var("GRAPHIFY_WHISPER_BIN", &fake); }
        let result = transcribe(&video, Some(&out_dir), None, false).unwrap();
        if let Some(prev) = old {
            unsafe { env::set_var("GRAPHIFY_WHISPER_BIN", prev); }
        } else {
            unsafe { env::remove_var("GRAPHIFY_WHISPER_BIN"); }
        }
        assert_eq!(result, cached);
    }

    #[test]
    fn test_transcribe_all_empty() {
        let result = transcribe_all(&[], None, None);
        assert!(result.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn test_transcribe_all_uses_cache() {
        let dir = tempfile::tempdir().unwrap();
        let video = dir.path().join("lecture.mp4");
        fs::write(&video, b"fake").unwrap();
        let out_dir = dir.path().join("transcripts");
        fs::create_dir_all(&out_dir).unwrap();
        let cached = out_dir.join("lecture.txt");
        fs::write(&cached, "Cached.").unwrap();

        let old = env::var("GRAPHIFY_WHISPER_BIN").ok();
        let fake = fake_whisper_bin(dir.path());
        unsafe { env::set_var("GRAPHIFY_WHISPER_BIN", &fake); }
        let results = transcribe_all(&[video.to_string_lossy().to_string()], Some(&out_dir), None);
        if let Some(prev) = old {
            unsafe { env::set_var("GRAPHIFY_WHISPER_BIN", prev); }
        } else {
            unsafe { env::remove_var("GRAPHIFY_WHISPER_BIN"); }
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], cached);
    }
}
