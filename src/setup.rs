use anyhow::Result;
use serde_json::{Value, json};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

macro_rules! skill_asset {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/assets/skills/",
            $name
        ))
    };
}

const SKILL_REGISTRATION: &str = "\n# graphify\n- **graphify** (`~/.claude/skills/graphify/SKILL.md`) - any input to knowledge graph. Trigger: `/graphify`\nWhen the user types `/graphify`, invoke the Skill tool with `skill: \"graphify\"` before doing anything else.\n";

const HOOK_MARKER: &str = "# graphify-hook-start";
const HOOK_MARKER_END: &str = "# graphify-hook-end";
const CHECKOUT_MARKER: &str = "# graphify-checkout-hook-start";
const CHECKOUT_MARKER_END: &str = "# graphify-checkout-hook-end";
const HOOK_SCRIPT: &str = "# graphify-hook-start\n# Auto-rebuilds the knowledge graph after each commit (code files only, no LLM needed).\n# Installed by: graphify hook install\n\n# Skip during rebase, merge, cherry-pick\nif [ -d \"$(git rev-parse --git-dir 2>/dev/null)/rebase-merge\" ] || \\\n   [ -d \"$(git rev-parse --git-dir 2>/dev/null)/rebase-apply\" ] || \\\n   [ -f \"$(git rev-parse --git-dir 2>/dev/null)/MERGE_HEAD\" ] || \\\n   [ -f \"$(git rev-parse --git-dir 2>/dev/null)/CHERRY_PICK_HEAD\" ]; then\n    exit 0\nfi\n\nCHANGED=$(git diff --name-only HEAD~1 HEAD 2>/dev/null || git diff --name-only HEAD 2>/dev/null)\nif [ -z \"$CHANGED\" ]; then\n    exit 0\nfi\n\nGRAPHIFY_BIN=$(command -v graphify 2>/dev/null)\n\nexport GRAPHIFY_CHANGED=\"$CHANGED\"\nCHANGED_COUNT=$(printf \"%s\\n\" \"$GRAPHIFY_CHANGED\" | sed '/^$/d' | wc -l | tr -d ' ')\nif [ \"$CHANGED_COUNT\" = \"0\" ]; then\n    exit 0\nfi\n\necho \"[graphify hook] $CHANGED_COUNT file(s) changed - rebuilding graph...\"\n\nif [ -n \"$GRAPHIFY_BIN\" ]; then\n    \"$GRAPHIFY_BIN\" update .\nelse\n    exit 0\nfi\n# graphify-hook-end\n";
const CHECKOUT_SCRIPT: &str = "# graphify-checkout-hook-start\n# Auto-rebuilds the knowledge graph (code only) when switching branches.\n# Installed by: graphify hook install\n\nPREV_HEAD=$1\nNEW_HEAD=$2\nBRANCH_SWITCH=$3\n\n# Skip during rebase, merge, cherry-pick\nif [ -d \"$(git rev-parse --git-dir 2>/dev/null)/rebase-merge\" ] || \\\n   [ -d \"$(git rev-parse --git-dir 2>/dev/null)/rebase-apply\" ] || \\\n   [ -f \"$(git rev-parse --git-dir 2>/dev/null)/MERGE_HEAD\" ] || \\\n   [ -f \"$(git rev-parse --git-dir 2>/dev/null)/CHERRY_PICK_HEAD\" ]; then\n    exit 0\nfi\n\n# Only run on branch switches, not file checkouts\nif [ \"$BRANCH_SWITCH\" != \"1\" ]; then\n    exit 0\nfi\n\n# Only run if graphify-out/ exists (graph has been built before)\nif [ ! -d \"graphify-out\" ]; then\n    exit 0\nfi\n\nGRAPHIFY_BIN=$(command -v graphify 2>/dev/null)\n\necho \"[graphify] Branch switched - rebuilding knowledge graph (code files)...\"\nif [ -n \"$GRAPHIFY_BIN\" ]; then\n    \"$GRAPHIFY_BIN\" update .\nelse\n    exit 0\nfi\n# graphify-checkout-hook-end\n";
const ANTIGRAVITY_RULES_PATH: &str = ".agent/rules/graphify.md";
const ANTIGRAVITY_WORKFLOW_PATH: &str = ".agent/workflows/graphify.md";
const ANTIGRAVITY_SKILL_FRONTMATTER: &str = "---\nname: graphify-manager\ndescription: Rebuild the code graph or perform manual CLI queries when MCP server is offline.\n---\n\n";
const ANTIGRAVITY_RULES: &str = "This project has a graphify knowledge graph at graphify-out/.\n\nRules:\n- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure\n- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files\n- If the graphify MCP server is active, utilize tools like `query_graph`, `get_node`, and `shortest_path` for precise architecture navigation instead of falling back to `grep`\n- After modifying code files in this session, run `graphify update .` to keep the graph current\n";
const ANTIGRAVITY_WORKFLOW: &str = "# Workflow: graphify\n**Command:** /graphify\n**Description:** Turn any folder of files into a navigable knowledge graph\n\n## Steps\nFollow the graphify skill installed at ~/.agent/skills/graphify/SKILL.md to run the full pipeline.\n\nIf no path argument is given, use `.` (current directory).\n";
const KIRO_STEERING: &str = "---\ninclusion: always\n---\n\ngraphify: A knowledge graph of this project lives in `graphify-out/`. If `graphify-out/GRAPH_REPORT.md` exists, read it before answering architecture questions, tracing dependencies, or searching files — it contains god nodes, community structure, and surprising connections the graph found. Navigate by graph structure instead of grepping raw files.\n";
const KIRO_STEERING_MARKER: &str = "graphify: A knowledge graph of this project";

pub const CLAUDE_MD_SECTION: &str = "## graphify\n\nThis project has a graphify knowledge graph at graphify-out/.\n\nRules:\n- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure\n- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files\n- After modifying code files in this session, run `graphify update .` to keep the graph current\n";

pub const AGENTS_MD_SECTION: &str = "## graphify\n\nThis project has a graphify knowledge graph at graphify-out/.\n\nRules:\n- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure\n- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files\n- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)\n";

pub const GEMINI_MD_SECTION: &str = "## graphify\n\nThis project has a graphify knowledge graph at graphify-out/.\n\nRules:\n- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure\n- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files\n- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)\n";

pub const VSCODE_INSTRUCTIONS_SECTION: &str = "## graphify\n\nBefore answering architecture or codebase questions, read `graphify-out/GRAPH_REPORT.md` if it exists.\nIf `graphify-out/wiki/index.md` exists, navigate it for deep questions.\nType `/graphify` in Copilot Chat to build or update the knowledge graph.\n";

pub const CURSOR_RULE: &str = "---\ndescription: graphify knowledge graph context\nalwaysApply: true\n---\n\nThis project has a graphify knowledge graph at graphify-out/.\n\n- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure\n- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files\n- After modifying code files in this session, run `graphify update .` to keep the graph current\n";

pub const OPENCODE_PLUGIN_JS: &str = "// graphify OpenCode plugin\n// Injects a knowledge graph reminder before bash tool calls when the graph exists.\nimport { existsSync } from \"fs\";\nimport { join } from \"path\";\n\nexport const GraphifyPlugin = async ({ directory }) => {\n  let reminded = false;\n\n  return {\n    \"tool.execute.before\": async (input, output) => {\n      if (reminded) return;\n      if (!existsSync(join(directory, \"graphify-out\", \"graph.json\"))) return;\n\n      if (input.tool === \"bash\") {\n        output.args.command =\n          'echo \"[graphify] Knowledge graph available. Read graphify-out/GRAPH_REPORT.md for god nodes and architecture context before searching files.\" && ' +\n          output.args.command;\n        reminded = true;\n      }\n    },\n  };\n};\n";

#[derive(Clone, Copy)]
pub struct PlatformConfig {
    pub skill_dst: &'static str,
    pub claude_md: bool,
    pub skill: &'static str,
}

pub fn platform_config(platform: &str) -> Option<PlatformConfig> {
    Some(match platform {
        "claude" => PlatformConfig {
            skill_dst: ".claude/skills/graphify/SKILL.md",
            claude_md: true,
            skill: skill_asset!("skill.md"),
        },
        "codex" => PlatformConfig {
            skill_dst: ".agents/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-codex.md"),
        },
        "opencode" => PlatformConfig {
            skill_dst: ".config/opencode/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-opencode.md"),
        },
        "aider" => PlatformConfig {
            skill_dst: ".aider/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-aider.md"),
        },
        "copilot" => PlatformConfig {
            skill_dst: ".copilot/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-copilot.md"),
        },
        "claw" => PlatformConfig {
            skill_dst: ".openclaw/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-claw.md"),
        },
        "droid" => PlatformConfig {
            skill_dst: ".factory/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-droid.md"),
        },
        "trae" => PlatformConfig {
            skill_dst: ".trae/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-trae.md"),
        },
        "trae-cn" => PlatformConfig {
            skill_dst: ".trae-cn/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-trae.md"),
        },
        "hermes" => PlatformConfig {
            skill_dst: ".hermes/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-claw.md"),
        },
        "kiro" => PlatformConfig {
            skill_dst: ".kiro/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill-kiro.md"),
        },
        "antigravity" => PlatformConfig {
            skill_dst: ".agent/skills/graphify/SKILL.md",
            claude_md: false,
            skill: skill_asset!("skill.md"),
        },
        "windows" => PlatformConfig {
            skill_dst: ".claude/skills/graphify/SKILL.md",
            claude_md: true,
            skill: skill_asset!("skill-windows.md"),
        },
        _ => return None,
    })
}

pub fn install_platform(
    home_dir: &Path,
    platform: &str,
    version_stamp: &str,
) -> Result<Vec<String>> {
    let cfg = platform_config(platform).ok_or_else(|| {
        anyhow::anyhow!(
            "error: unknown platform '{}'. Choose from: claude, windows, codex, opencode, aider, copilot, claw, droid, trae, trae-cn, hermes, kiro, antigravity",
            platform
        )
    })?;

    let skill_dst = home_dir.join(cfg.skill_dst);
    write_file(&skill_dst, cfg.skill)?;
    write_file(
        &skill_dst
            .parent()
            .unwrap_or(home_dir)
            .join(".graphify_version"),
        version_stamp,
    )?;

    let mut lines = vec![format!("  skill installed  ->  {}", skill_dst.display())];

    if cfg.claude_md {
        let claude_md = home_dir.join(".claude/CLAUDE.md");
        if claude_md.exists() {
            let content = fs::read_to_string(&claude_md)?;
            if content.contains("graphify") {
                lines.push("  CLAUDE.md        ->  already registered (no change)".to_string());
            } else {
                fs::write(
                    &claude_md,
                    format!("{}{}", content.trim_end(), SKILL_REGISTRATION),
                )?;
                lines.push(format!(
                    "  CLAUDE.md        ->  skill registered in {}",
                    claude_md.display()
                ));
            }
        } else {
            write_file(&claude_md, SKILL_REGISTRATION.trim_start())?;
            lines.push(format!(
                "  CLAUDE.md        ->  created at {}",
                claude_md.display()
            ));
        }
    }

    lines.push(String::new());
    lines.push("Done. Open your AI coding assistant and type:".to_string());
    lines.push(String::new());
    lines.push("  /graphify .".to_string());
    lines.push(String::new());
    Ok(lines)
}

pub fn uninstall_platform_skill(home_dir: &Path, platform: &str) -> Result<Vec<String>> {
    let cfg = platform_config(platform).ok_or_else(|| {
        anyhow::anyhow!(
            "error: unknown platform '{}'. Choose from: claude, windows, codex, opencode, aider, copilot, claw, droid, trae, trae-cn, hermes, kiro, antigravity",
            platform
        )
    })?;

    let skill_dst = home_dir.join(cfg.skill_dst);
    let existed = skill_dst.exists();
    remove_skill(&skill_dst)?;
    if existed {
        Ok(vec![format!("skill removed: {}", skill_dst.display())])
    } else {
        Ok(vec!["nothing to remove".to_string()])
    }
}

pub fn opencode_install(
    home_dir: &Path,
    project_dir: &Path,
    version_stamp: &str,
) -> Result<Vec<String>> {
    let cfg = platform_config("opencode").expect("opencode platform config must exist");
    let skill_dst = home_dir.join(cfg.skill_dst);
    write_file(&skill_dst, cfg.skill)?;
    write_file(
        &skill_dst
            .parent()
            .unwrap_or(home_dir)
            .join(".graphify_version"),
        version_stamp,
    )?;

    let mut lines = vec![format!("  skill installed  ->  {}", skill_dst.display())];
    lines.extend(install_opencode_plugin(project_dir)?);
    lines.push(String::new());
    lines.push("Done. Open your AI coding assistant and type:".to_string());
    lines.push(String::new());
    lines.push("  /graphify .".to_string());
    lines.push(String::new());
    Ok(lines)
}

pub fn claude_install(project_dir: &Path) -> Result<Vec<String>> {
    let target = project_dir.join("CLAUDE.md");
    let mut lines = Vec::new();

    if target.exists() {
        let content = fs::read_to_string(&target)?;
        if content.contains("## graphify") {
            lines.push("graphify already configured in CLAUDE.md".to_string());
            return Ok(lines);
        }
        fs::write(
            &target,
            format!("{}\n\n{}", content.trim_end(), CLAUDE_MD_SECTION),
        )?;
    } else {
        write_file(&target, CLAUDE_MD_SECTION)?;
    }
    lines.push(format!("graphify section written to {}", target.display()));
    lines.extend(install_claude_hook(project_dir)?);
    lines.push(String::new());
    lines.push("Claude Code will now check the knowledge graph before answering".to_string());
    lines.push("codebase questions and rebuild it after code changes.".to_string());
    Ok(lines)
}

pub fn claude_uninstall(project_dir: &Path) -> Result<Vec<String>> {
    let target = project_dir.join("CLAUDE.md");
    let mut lines = Vec::new();

    if !target.exists() {
        lines.push("No CLAUDE.md found in current directory - nothing to do".to_string());
        return Ok(lines);
    }

    let content = fs::read_to_string(&target)?;
    if !content.contains("## graphify") {
        lines.push("graphify section not found in CLAUDE.md - nothing to do".to_string());
        return Ok(lines);
    }

    let cleaned = remove_graphify_section(&content);
    if cleaned.is_empty() {
        fs::remove_file(&target)?;
        lines.push(format!(
            "CLAUDE.md was empty after removal - deleted {}",
            target.display()
        ));
    } else {
        fs::write(&target, format!("{cleaned}\n"))?;
        lines.push(format!(
            "graphify section removed from {}",
            target.display()
        ));
    }
    lines.extend(uninstall_claude_hook(project_dir)?);
    Ok(lines)
}

pub fn gemini_install(
    home_dir: &Path,
    project_dir: &Path,
    version_stamp: &str,
) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    let skill_dst = gemini_skill_dst(home_dir);
    write_file(&skill_dst, skill_asset!("skill.md"))?;
    write_file(
        &skill_dst
            .parent()
            .unwrap_or(home_dir)
            .join(".graphify_version"),
        version_stamp,
    )?;
    lines.push(format!("  skill installed  ->  {}", skill_dst.display()));

    let target = project_dir.join("GEMINI.md");
    lines.push(upsert_markdown_section(
        &target,
        "## graphify",
        GEMINI_MD_SECTION,
        "graphify already configured in GEMINI.md",
        "graphify section written to",
    )?);
    lines.extend(install_gemini_hook(project_dir)?);
    lines.push(String::new());
    lines.push("Gemini CLI will now check the knowledge graph before answering".to_string());
    lines.push("codebase questions and rebuild it after code changes.".to_string());
    Ok(lines)
}

pub fn gemini_uninstall(home_dir: &Path, project_dir: &Path) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    let skill_dst = gemini_skill_dst(home_dir);
    remove_skill(&skill_dst)?;

    let target = project_dir.join("GEMINI.md");
    if !target.exists() {
        lines.push("No GEMINI.md found in current directory - nothing to do".to_string());
        return Ok(lines);
    }
    let content = fs::read_to_string(&target)?;
    if !content.contains("## graphify") {
        lines.push("graphify section not found in GEMINI.md - nothing to do".to_string());
        return Ok(lines);
    }
    let cleaned = remove_graphify_section(&content);
    if cleaned.is_empty() {
        fs::remove_file(&target)?;
        lines.push(format!(
            "GEMINI.md was empty after removal - deleted {}",
            target.display()
        ));
    } else {
        fs::write(&target, format!("{cleaned}\n"))?;
        lines.push(format!(
            "graphify section removed from {}",
            target.display()
        ));
    }
    lines.extend(uninstall_gemini_hook(project_dir)?);
    Ok(lines)
}

pub fn vscode_install(
    home_dir: &Path,
    project_dir: &Path,
    version_stamp: &str,
) -> Result<Vec<String>> {
    let skill_dst = home_dir.join(".copilot/skills/graphify/SKILL.md");
    write_file(&skill_dst, skill_asset!("skill-vscode.md"))?;
    write_file(
        &skill_dst
            .parent()
            .unwrap_or(home_dir)
            .join(".graphify_version"),
        version_stamp,
    )?;

    let instructions = project_dir.join(".github/copilot-instructions.md");
    let instructions_display = display_relative(project_dir, &instructions);

    let mut lines = vec![format!("  skill installed  ->  {}", skill_dst.display())];
    if instructions.exists() {
        let content = fs::read_to_string(&instructions)?;
        if content.contains("## graphify") {
            lines.push(format!(
                "  {}  ->  already configured (no change)",
                instructions_display
            ));
        } else {
            fs::write(
                &instructions,
                format!("{}\n\n{}", content.trim_end(), VSCODE_INSTRUCTIONS_SECTION),
            )?;
            lines.push(format!(
                "  {}  ->  graphify section added",
                instructions_display
            ));
        }
    } else {
        write_file(&instructions, VSCODE_INSTRUCTIONS_SECTION)?;
        lines.push(format!("  {}  ->  created", instructions_display));
    }

    lines.push(String::new());
    lines.push(
        "VS Code Copilot Chat configured. Type /graphify in the chat panel to build the graph."
            .to_string(),
    );
    lines
        .push("Note: for GitHub Copilot CLI (terminal), use: graphify copilot install".to_string());
    Ok(lines)
}

pub fn vscode_uninstall(home_dir: &Path, project_dir: &Path) -> Result<Vec<String>> {
    let skill_dst = home_dir.join(".copilot/skills/graphify/SKILL.md");
    let mut lines = Vec::new();
    if skill_dst.exists() {
        remove_skill(&skill_dst)?;
        lines.push(format!("  skill removed    ->  {}", skill_dst.display()));
    }

    let instructions = project_dir.join(".github/copilot-instructions.md");
    if !instructions.exists() {
        return Ok(lines);
    }
    let content = fs::read_to_string(&instructions)?;
    if !content.contains("## graphify") {
        return Ok(lines);
    }

    let cleaned = remove_graphify_section(&content);
    let instructions_display = display_relative(project_dir, &instructions);
    if cleaned.is_empty() {
        fs::remove_file(&instructions)?;
        lines.push(format!(
            "  {}  ->  deleted (was empty after removal)",
            instructions_display
        ));
    } else {
        fs::write(&instructions, format!("{cleaned}\n"))?;
        lines.push(format!(
            "  graphify section removed from {}",
            instructions_display
        ));
    }

    Ok(lines)
}

pub fn cursor_install(project_dir: &Path) -> Result<Vec<String>> {
    let rule_path = project_dir.join(".cursor/rules/graphify.mdc");
    if rule_path.exists() {
        return Ok(vec![format!(
            "graphify rule already exists at {} (no change)",
            rule_path.display()
        )]);
    }
    write_file(&rule_path, CURSOR_RULE)?;
    Ok(vec![
        format!("graphify rule written to {}", rule_path.display()),
        String::new(),
        "Cursor will now always include the knowledge graph context.".to_string(),
        "Run /graphify . first to build the graph if you haven't already.".to_string(),
    ])
}

pub fn cursor_uninstall(project_dir: &Path) -> Result<Vec<String>> {
    let rule_path = project_dir.join(".cursor/rules/graphify.mdc");
    if !rule_path.exists() {
        return Ok(vec![
            "No graphify Cursor rule found - nothing to do".to_string(),
        ]);
    }
    fs::remove_file(&rule_path)?;
    Ok(vec![format!(
        "graphify Cursor rule removed from {}",
        rule_path.display()
    )])
}

pub fn kiro_install(project_dir: &Path) -> Result<Vec<String>> {
    let cfg = platform_config("kiro").expect("kiro platform config");
    let skill_dst = project_dir.join(".kiro/skills/graphify/SKILL.md");
    write_file(&skill_dst, cfg.skill)?;

    let steering_dst = project_dir.join(".kiro/steering/graphify.md");
    let mut lines = vec![format!(
        "  {}  ->  /graphify skill",
        display_relative(project_dir, &skill_dst)
    )];
    if steering_dst.exists() && fs::read_to_string(&steering_dst)?.contains(KIRO_STEERING_MARKER) {
        lines.push("  .kiro/steering/graphify.md  ->  already configured".to_string());
    } else {
        write_file(&steering_dst, KIRO_STEERING)?;
        lines.push("  .kiro/steering/graphify.md  ->  always-on steering written".to_string());
    }

    lines.push(String::new());
    lines.push("Kiro will now read the knowledge graph before every conversation.".to_string());
    lines.push("Use /graphify to build or update the graph.".to_string());
    Ok(lines)
}

pub fn kiro_uninstall(project_dir: &Path) -> Result<Vec<String>> {
    let mut removed = Vec::new();
    let skill_dst = project_dir.join(".kiro/skills/graphify/SKILL.md");
    if skill_dst.exists() {
        fs::remove_file(&skill_dst)?;
        removed.push(display_relative(project_dir, &skill_dst));
        remove_empty_parents(&skill_dst, project_dir, 3);
    }

    let steering_dst = project_dir.join(".kiro/steering/graphify.md");
    if steering_dst.exists() {
        fs::remove_file(&steering_dst)?;
        removed.push(display_relative(project_dir, &steering_dst));
        remove_empty_parents(&steering_dst, project_dir, 2);
    }

    Ok(vec![format!(
        "Removed: {}",
        if removed.is_empty() {
            "nothing to remove".to_string()
        } else {
            removed.join(", ")
        }
    )])
}

pub fn antigravity_install(
    home_dir: &Path,
    project_dir: &Path,
    version_stamp: &str,
) -> Result<Vec<String>> {
    let cfg = platform_config("antigravity").expect("antigravity platform config");
    let skill_dst = home_dir.join(cfg.skill_dst);
    write_file(&skill_dst, cfg.skill)?;
    write_file(
        &skill_dst
            .parent()
            .unwrap_or(home_dir)
            .join(".graphify_version"),
        version_stamp,
    )?;

    let mut skill_content = fs::read_to_string(&skill_dst)?;
    if !skill_content.starts_with("---\n") {
        skill_content = format!("{ANTIGRAVITY_SKILL_FRONTMATTER}{skill_content}");
        fs::write(&skill_dst, skill_content)?;
    }

    let mut lines = vec![format!("  skill installed  ->  {}", skill_dst.display())];

    let rules_path = project_dir.join(ANTIGRAVITY_RULES_PATH);
    if rules_path.exists() {
        lines.push(format!(
            "graphify rule already exists at {} (no change)",
            rules_path.display()
        ));
    } else {
        write_file(&rules_path, ANTIGRAVITY_RULES)?;
        lines.push(format!("graphify rule written to {}", rules_path.display()));
    }

    let workflow_path = project_dir.join(ANTIGRAVITY_WORKFLOW_PATH);
    if workflow_path.exists() {
        lines.push(format!(
            "graphify workflow already exists at {} (no change)",
            workflow_path.display()
        ));
    } else {
        write_file(&workflow_path, ANTIGRAVITY_WORKFLOW)?;
        lines.push(format!(
            "graphify workflow written to {}",
            workflow_path.display()
        ));
    }

    lines.push(String::new());
    lines.push("Antigravity will now check the knowledge graph before answering".to_string());
    lines.push("codebase questions. Run /graphify first to build the graph.".to_string());
    lines.push(String::new());
    lines.push(
        "To enable full MCP architecture navigation, add this to ~/.gemini/antigravity/mcp_config.json:"
            .to_string(),
    );
    lines.push("  \"graphify\": {".to_string());
    lines.push("    \"command\": \"graphify\",".to_string());
    lines.push(
        "    \"args\": [\"serve\", \"${workspace.path}/graphify-out/graph.json\"]".to_string(),
    );
    lines.push("  }".to_string());
    Ok(lines)
}

pub fn antigravity_uninstall(home_dir: &Path, project_dir: &Path) -> Result<Vec<String>> {
    let mut lines = Vec::new();

    let rules_path = project_dir.join(ANTIGRAVITY_RULES_PATH);
    if rules_path.exists() {
        fs::remove_file(&rules_path)?;
        lines.push(format!(
            "graphify rule removed from {}",
            rules_path.display()
        ));
        remove_empty_parents(&rules_path, project_dir, 2);
    } else {
        lines.push("No graphify Antigravity rule found - nothing to do".to_string());
    }

    let workflow_path = project_dir.join(ANTIGRAVITY_WORKFLOW_PATH);
    if workflow_path.exists() {
        fs::remove_file(&workflow_path)?;
        lines.push(format!(
            "graphify workflow removed from {}",
            workflow_path.display()
        ));
        remove_empty_parents(&workflow_path, project_dir, 2);
    }

    let cfg = platform_config("antigravity").expect("antigravity platform config");
    let skill_dst = home_dir.join(cfg.skill_dst);
    if skill_dst.exists() {
        fs::remove_file(&skill_dst)?;
        lines.push(format!(
            "graphify skill removed from {}",
            skill_dst.display()
        ));
    }
    let version_file = skill_dst
        .parent()
        .unwrap_or(home_dir)
        .join(".graphify_version");
    if version_file.exists() {
        fs::remove_file(&version_file)?;
    }
    remove_empty_parents(&skill_dst, home_dir, 3);
    Ok(lines)
}

pub fn hook_install(path: &Path) -> Result<Vec<String>> {
    let root = git_root(path)
        .ok_or_else(|| anyhow::anyhow!("No git repository found at or above {}", path.display()))?;
    let hooks_dir = hooks_dir(&root)?;
    let commit_msg = install_hook(&hooks_dir, "post-commit", HOOK_SCRIPT, HOOK_MARKER)?;
    let checkout_msg = install_hook(
        &hooks_dir,
        "post-checkout",
        CHECKOUT_SCRIPT,
        CHECKOUT_MARKER,
    )?;
    Ok(vec![
        format!("post-commit: {commit_msg}"),
        format!("post-checkout: {checkout_msg}"),
    ])
}

pub fn hook_uninstall(path: &Path) -> Result<Vec<String>> {
    let root = git_root(path)
        .ok_or_else(|| anyhow::anyhow!("No git repository found at or above {}", path.display()))?;
    let hooks_dir = hooks_dir(&root)?;
    let commit_msg = uninstall_hook(&hooks_dir, "post-commit", HOOK_MARKER, HOOK_MARKER_END)?;
    let checkout_msg = uninstall_hook(
        &hooks_dir,
        "post-checkout",
        CHECKOUT_MARKER,
        CHECKOUT_MARKER_END,
    )?;
    Ok(vec![
        format!("post-commit: {commit_msg}"),
        format!("post-checkout: {checkout_msg}"),
    ])
}

pub fn hook_status(path: &Path) -> Result<Vec<String>> {
    let Some(root) = git_root(path) else {
        return Ok(vec!["Not in a git repository.".to_string()]);
    };
    let hooks_dir = hooks_dir(&root)?;
    Ok(vec![
        format!(
            "post-commit: {}",
            hook_status_one(&hooks_dir, "post-commit", HOOK_MARKER)?
        ),
        format!(
            "post-checkout: {}",
            hook_status_one(&hooks_dir, "post-checkout", CHECKOUT_MARKER)?
        ),
    ])
}

fn hooks_dir(root: &Path) -> Result<PathBuf> {
    if let Ok(output) = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["config", "core.hooksPath"])
        .output()
        && output.status.success() {
            let custom = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !custom.is_empty() {
                let path = PathBuf::from(custom);
                let hooks_dir = if path.is_absolute() {
                    path
                } else {
                    root.join(path)
                };
                fs::create_dir_all(&hooks_dir)?;
                return Ok(hooks_dir);
            }
        }

    let hooks_dir = root.join(".git/hooks");
    fs::create_dir_all(&hooks_dir)?;
    Ok(hooks_dir)
}

pub fn agents_install(project_dir: &Path, platform: &str) -> Result<Vec<String>> {
    let target = project_dir.join("AGENTS.md");
    let line = upsert_markdown_section(
        &target,
        "## graphify",
        AGENTS_MD_SECTION,
        "graphify already configured in AGENTS.md",
        "graphify section written to",
    )?;
    let mut lines = vec![line];
    if platform == "codex" {
        lines.extend(install_codex_hook(project_dir)?);
    } else if platform == "opencode" {
        lines.extend(install_opencode_plugin(project_dir)?);
    }

    lines.push(String::new());
    lines.push(format!(
        "{} will now check the knowledge graph before answering",
        capitalize(platform)
    ));
    lines.push("codebase questions and rebuild it after code changes.".to_string());
    if !matches!(platform, "codex" | "opencode") {
        lines.push(String::new());
        lines.push(
            "Note: unlike Claude Code, there is no PreToolUse hook equivalent for".to_string(),
        );
        lines.push(format!(
            "{} — the AGENTS.md rules are the always-on mechanism.",
            capitalize(platform)
        ));
    }
    Ok(lines)
}

pub fn agents_uninstall(project_dir: &Path, platform: &str) -> Result<Vec<String>> {
    let target = project_dir.join("AGENTS.md");
    let mut lines = Vec::new();
    if !target.exists() {
        lines.push("No AGENTS.md found in current directory - nothing to do".to_string());
        return Ok(lines);
    }
    let content = fs::read_to_string(&target)?;
    if !content.contains("## graphify") {
        lines.push("graphify section not found in AGENTS.md - nothing to do".to_string());
        return Ok(lines);
    }
    let cleaned = remove_graphify_section(&content);
    if cleaned.is_empty() {
        fs::remove_file(&target)?;
        lines.push(format!(
            "AGENTS.md was empty after removal - deleted {}",
            target.display()
        ));
    } else {
        fs::write(&target, format!("{cleaned}\n"))?;
        lines.push(format!(
            "graphify section removed from {}",
            target.display()
        ));
    }

    if platform == "codex" {
        lines.extend(uninstall_codex_hook(project_dir)?);
    } else if platform == "opencode" {
        lines.extend(uninstall_opencode_plugin(project_dir)?);
    }
    Ok(lines)
}

fn install_claude_hook(project_dir: &Path) -> Result<Vec<String>> {
    let settings_path = project_dir.join(".claude/settings.json");
    let mut settings = read_json_or_default(&settings_path);
    let hooks = settings
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap();
    let pre_tool = hooks
        .entry("PreToolUse")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .unwrap();
    pre_tool.retain(|hook| {
        !(hook.get("matcher").and_then(Value::as_str) == Some("Glob|Grep")
            && hook.to_string().contains("graphify"))
    });
    pre_tool.push(json!({
        "matcher": "Glob|Grep",
        "hooks": [{
            "type": "command",
            "command": "[ -f graphify-out/graph.json ] && echo '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"additionalContext\":\"graphify: Knowledge graph exists. Read graphify-out/GRAPH_REPORT.md for god nodes and community structure before searching raw files.\"}}' || true"
        }]
    }));
    write_json(&settings_path, &settings)?;
    Ok(vec![
        "  .claude/settings.json  ->  PreToolUse hook registered".to_string(),
    ])
}

fn uninstall_claude_hook(project_dir: &Path) -> Result<Vec<String>> {
    let settings_path = project_dir.join(".claude/settings.json");
    if !settings_path.exists() {
        return Ok(Vec::new());
    }
    let mut settings = read_json_or_default(&settings_path);
    let Some(pre_tool) = settings
        .get_mut("hooks")
        .and_then(Value::as_object_mut)
        .and_then(|hooks| hooks.get_mut("PreToolUse"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(Vec::new());
    };
    let original = pre_tool.len();
    pre_tool.retain(|hook| {
        !(hook.get("matcher").and_then(Value::as_str) == Some("Glob|Grep")
            && hook.to_string().contains("graphify"))
    });
    if pre_tool.len() == original {
        return Ok(Vec::new());
    }
    write_json(&settings_path, &settings)?;
    Ok(vec![
        "  .claude/settings.json  ->  PreToolUse hook removed".to_string(),
    ])
}

fn install_gemini_hook(project_dir: &Path) -> Result<Vec<String>> {
    let settings_path = project_dir.join(".gemini/settings.json");
    let mut settings = read_json_or_default(&settings_path);
    let hooks = settings
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap();
    let before_tool = hooks
        .entry("BeforeTool")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .unwrap();
    before_tool.retain(|hook| !hook.to_string().contains("graphify"));
    before_tool.push(json!({
        "matcher": "read_file|list_directory",
        "hooks": [{
            "type": "command",
            "command": "[ -f graphify-out/graph.json ] && echo '{\"decision\":\"allow\",\"additionalContext\":\"graphify: Knowledge graph exists. Read graphify-out/GRAPH_REPORT.md for god nodes and community structure before searching raw files.\"}' || echo '{\"decision\":\"allow\"}'"
        }]
    }));
    write_json(&settings_path, &settings)?;
    Ok(vec![
        "  .gemini/settings.json  ->  BeforeTool hook registered".to_string(),
    ])
}

fn uninstall_gemini_hook(project_dir: &Path) -> Result<Vec<String>> {
    let settings_path = project_dir.join(".gemini/settings.json");
    if !settings_path.exists() {
        return Ok(Vec::new());
    }
    let mut settings = read_json_or_default(&settings_path);
    let Some(before_tool) = settings
        .get_mut("hooks")
        .and_then(Value::as_object_mut)
        .and_then(|hooks| hooks.get_mut("BeforeTool"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(Vec::new());
    };
    let original = before_tool.len();
    before_tool.retain(|hook| !hook.to_string().contains("graphify"));
    if before_tool.len() == original {
        return Ok(Vec::new());
    }
    write_json(&settings_path, &settings)?;
    Ok(vec![
        "  .gemini/settings.json  ->  BeforeTool hook removed".to_string(),
    ])
}

fn install_codex_hook(project_dir: &Path) -> Result<Vec<String>> {
    let hooks_path = project_dir.join(".codex/hooks.json");
    let mut existing = read_json_or_default(&hooks_path);
    let hooks = existing
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap();
    let pre_tool = hooks
        .entry("PreToolUse")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .unwrap();
    pre_tool.retain(|hook| !hook.to_string().contains("graphify"));
    pre_tool.push(json!({
        "matcher": "Bash",
        "hooks": [{
            "type": "command",
            "command": "[ -f graphify-out/graph.json ] && echo '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"additionalContext\":\"graphify: Knowledge graph exists. Read graphify-out/GRAPH_REPORT.md for god nodes and community structure before searching raw files.\"}}' || true"
        }]
    }));
    write_json(&hooks_path, &existing)?;
    Ok(vec![
        "  .codex/hooks.json  ->  PreToolUse hook registered".to_string(),
    ])
}

fn uninstall_codex_hook(project_dir: &Path) -> Result<Vec<String>> {
    let hooks_path = project_dir.join(".codex/hooks.json");
    if !hooks_path.exists() {
        return Ok(Vec::new());
    }
    let mut existing = read_json_or_default(&hooks_path);
    let Some(pre_tool) = existing
        .get_mut("hooks")
        .and_then(Value::as_object_mut)
        .and_then(|hooks| hooks.get_mut("PreToolUse"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(Vec::new());
    };
    let original = pre_tool.len();
    pre_tool.retain(|hook| !hook.to_string().contains("graphify"));
    if pre_tool.len() == original {
        return Ok(Vec::new());
    }
    write_json(&hooks_path, &existing)?;
    Ok(vec![
        "  .codex/hooks.json  ->  PreToolUse hook removed".to_string(),
    ])
}

fn install_opencode_plugin(project_dir: &Path) -> Result<Vec<String>> {
    let plugin_file = project_dir.join(".opencode/plugins/graphify.js");
    write_file(&plugin_file, OPENCODE_PLUGIN_JS)?;
    let mut lines =
        vec!["  .opencode/plugins/graphify.js  ->  tool.execute.before hook written".to_string()];

    let config_file = project_dir.join("opencode.json");
    let mut config = read_json_or_default(&config_file);
    let plugins = config
        .as_object_mut()
        .unwrap()
        .entry("plugin")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .unwrap();
    let entry = Value::String(".opencode/plugins/graphify.js".to_string());
    if !plugins.iter().any(|plugin| plugin == &entry) {
        plugins.push(entry);
        write_json(&config_file, &config)?;
        lines.push("  opencode.json  ->  plugin registered".to_string());
    } else {
        lines.push("  opencode.json  ->  plugin already registered (no change)".to_string());
    }
    Ok(lines)
}

fn uninstall_opencode_plugin(project_dir: &Path) -> Result<Vec<String>> {
    let plugin_file = project_dir.join(".opencode/plugins/graphify.js");
    let mut lines = Vec::new();
    if plugin_file.exists() {
        fs::remove_file(&plugin_file)?;
        lines.push("  .opencode/plugins/graphify.js  ->  removed".to_string());
    }

    let config_file = project_dir.join("opencode.json");
    if !config_file.exists() {
        return Ok(lines);
    }
    let mut config = read_json_or_default(&config_file);
    let Some(plugins) = config.get_mut("plugin").and_then(Value::as_array_mut) else {
        return Ok(lines);
    };
    let before = plugins.len();
    plugins.retain(|plugin| plugin.as_str() != Some(".opencode/plugins/graphify.js"));
    if plugins.len() != before {
        if plugins.is_empty() {
            config.as_object_mut().unwrap().remove("plugin");
        }
        write_json(&config_file, &config)?;
        lines.push("  opencode.json  ->  plugin deregistered".to_string());
    }
    Ok(lines)
}

fn upsert_markdown_section(
    target: &Path,
    marker: &str,
    section: &str,
    already_message: &str,
    written_prefix: &str,
) -> Result<String> {
    if target.exists() {
        let content = fs::read_to_string(target)?;
        if content.contains(marker) {
            return Ok(already_message.to_string());
        }
        fs::write(target, format!("{}\n\n{}", content.trim_end(), section))?;
    } else {
        write_file(target, section)?;
    }
    Ok(format!("{written_prefix} {}", target.display()))
}

fn remove_graphify_section(content: &str) -> String {
    let Some(start) = content.find("## graphify\n") else {
        return content.trim().to_string();
    };
    let end = content[start + "## graphify\n".len()..]
        .find("\n## ")
        .map(|offset| start + "## graphify\n".len() + offset)
        .unwrap_or(content.len());

    let mut out = String::new();
    out.push_str(content[..start].trim_end());
    let suffix = content[end..].trim_start();
    if !out.is_empty() && !suffix.is_empty() {
        out.push('\n');
        out.push('\n');
    }
    out.push_str(suffix);
    out.trim().to_string()
}

fn read_json_or_default(path: &Path) -> Value {
    match fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    }
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(value)?)?;
    Ok(())
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}

fn remove_skill(skill_dst: &Path) -> Result<()> {
    if skill_dst.exists() {
        fs::remove_file(skill_dst)?;
    }
    let version_file = skill_dst
        .parent()
        .unwrap_or(skill_dst)
        .join(".graphify_version");
    if version_file.exists() {
        fs::remove_file(version_file)?;
    }
    for dir in [
        skill_dst.parent(),
        skill_dst.parent().and_then(Path::parent),
        skill_dst
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent),
    ]
    .into_iter()
    .flatten()
    {
        let _ = fs::remove_dir(dir);
    }
    Ok(())
}

fn gemini_skill_dst(home_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        home_dir.join(".agents/skills/graphify/SKILL.md")
    } else {
        home_dir.join(".gemini/skills/graphify/SKILL.md")
    }
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn display_relative(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn remove_empty_parents(path: &Path, stop: &Path, levels: usize) {
    let mut current = path.parent();
    for _ in 0..levels {
        let Some(dir) = current else {
            break;
        };
        if dir == stop {
            break;
        }
        if fs::remove_dir(dir).is_err() {
            break;
        }
        current = dir.parent();
    }
}

fn git_root(path: &Path) -> Option<PathBuf> {
    let current = path.canonicalize().ok()?;
    for dir in current.as_path().ancestors() {
        if dir.join(".git").exists() {
            return Some(dir.to_path_buf());
        }
    }
    None
}

fn install_hook(hooks_dir: &Path, name: &str, script: &str, marker: &str) -> Result<String> {
    let hook_path = hooks_dir.join(name);
    if hook_path.exists() {
        let content = fs::read_to_string(&hook_path)?;
        if content.contains(marker) {
            return Ok(format!("already installed at {}", hook_path.display()));
        }
        fs::write(&hook_path, format!("{}\n\n{}", content.trim_end(), script))?;
        return Ok(format!(
            "appended to existing {name} hook at {}",
            hook_path.display()
        ));
    }
    fs::write(&hook_path, format!("#!/bin/sh\n{script}"))?;
    set_executable(&hook_path)?;
    Ok(format!("installed at {}", hook_path.display()))
}

fn uninstall_hook(hooks_dir: &Path, name: &str, marker: &str, marker_end: &str) -> Result<String> {
    let hook_path = hooks_dir.join(name);
    if !hook_path.exists() {
        return Ok(format!("no {name} hook found - nothing to remove."));
    }
    let content = fs::read_to_string(&hook_path)?;
    let Some(start) = content.find(marker) else {
        return Ok(format!(
            "graphify hook not found in {name} - nothing to remove."
        ));
    };
    let Some(end_rel) = content[start..].find(marker_end) else {
        return Ok(format!(
            "graphify hook not found in {name} - nothing to remove."
        ));
    };
    let end = start + end_rel + marker_end.len();
    let mut new_content = String::new();
    new_content.push_str(content[..start].trim_end());
    let suffix = content[end..].trim_start_matches('\n').trim_start();
    if !new_content.is_empty() && !suffix.is_empty() {
        new_content.push('\n');
        new_content.push('\n');
    }
    new_content.push_str(suffix);
    let trimmed = new_content.trim().to_string();
    if trimmed.is_empty() || trimmed == "#!/bin/bash" || trimmed == "#!/bin/sh" {
        fs::remove_file(&hook_path)?;
        return Ok(format!("removed {name} hook at {}", hook_path.display()));
    }
    fs::write(&hook_path, format!("{trimmed}\n"))?;
    set_executable(&hook_path)?;
    Ok(format!(
        "graphify removed from {name} at {} (other hook content preserved)",
        hook_path.display()
    ))
}

fn hook_status_one(hooks_dir: &Path, name: &str, marker: &str) -> Result<String> {
    let hook_path = hooks_dir.join(name);
    if !hook_path.exists() {
        return Ok("not installed".to_string());
    }
    let content = fs::read_to_string(&hook_path)?;
    Ok(if content.contains(marker) {
        "installed".to_string()
    } else {
        "not installed (hook exists but graphify not found)".to_string()
    })
}

fn set_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hook_round_trip() {
        let dir = tempdir().unwrap();
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(dir.path())
            .status()
            .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(dir.path())
            .args(["config", "core.hooksPath", ".git/hooks"])
            .status()
            .unwrap();

        let installed = hook_install(dir.path()).unwrap();
        assert!(installed[0].contains("post-commit:"));
        assert!(dir.path().join(".git/hooks/post-commit").exists());
        assert!(
            fs::read_to_string(dir.path().join(".git/hooks/post-checkout"))
                .unwrap()
                .contains(CHECKOUT_MARKER)
        );

        let status_lines = hook_status(dir.path()).unwrap();
        assert!(status_lines.iter().all(|line| line.contains("installed")));

        let removed = hook_uninstall(dir.path()).unwrap();
        assert!(removed[0].contains("removed") || removed[0].contains("nothing to remove"));
        assert!(!dir.path().join(".git/hooks/post-commit").exists());
    }

    #[test]
    fn hook_install_respects_core_hooks_path() {
        let dir = tempdir().unwrap();
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(dir.path())
            .status()
            .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(dir.path())
            .args(["config", "core.hooksPath", ".husky"])
            .status()
            .unwrap();

        let installed = hook_install(dir.path()).unwrap();

        assert!(installed[0].contains(".husky/post-commit"));
        assert!(dir.path().join(".husky/post-commit").exists());
        assert!(dir.path().join(".husky/post-checkout").exists());
        assert!(!dir.path().join(".git/hooks/post-commit").exists());
    }

    #[test]
    fn kiro_round_trip() {
        let dir = tempdir().unwrap();

        kiro_install(dir.path()).unwrap();
        assert!(dir.path().join(".kiro/skills/graphify/SKILL.md").exists());
        assert!(
            fs::read_to_string(dir.path().join(".kiro/steering/graphify.md"))
                .unwrap()
                .contains(KIRO_STEERING_MARKER)
        );

        let removed = kiro_uninstall(dir.path()).unwrap();
        assert!(removed[0].contains("Removed:"));
        assert!(!dir.path().join(".kiro/skills/graphify/SKILL.md").exists());
        assert!(!dir.path().join(".kiro/steering/graphify.md").exists());
    }

    #[test]
    fn antigravity_round_trip() {
        let home = tempdir().unwrap();
        let project = tempdir().unwrap();

        antigravity_install(home.path(), project.path(), "test-version").unwrap();
        let skill = home.path().join(".agent/skills/graphify/SKILL.md");
        assert!(skill.exists());
        assert!(fs::read_to_string(&skill).unwrap().starts_with("---\n"));
        assert!(project.path().join(ANTIGRAVITY_RULES_PATH).exists());
        assert!(project.path().join(ANTIGRAVITY_WORKFLOW_PATH).exists());

        let removed = antigravity_uninstall(home.path(), project.path()).unwrap();
        assert!(
            removed
                .iter()
                .any(|line| line.contains("graphify skill removed"))
        );
        assert!(!skill.exists());
        assert!(!project.path().join(ANTIGRAVITY_RULES_PATH).exists());
        assert!(!project.path().join(ANTIGRAVITY_WORKFLOW_PATH).exists());
    }

    #[test]
    fn vscode_round_trip() {
        let home = tempdir().unwrap();
        let project = tempdir().unwrap();

        let installed = vscode_install(home.path(), project.path(), "test-version").unwrap();
        assert!(
            installed
                .iter()
                .any(|line| line.contains("skill installed"))
        );

        let skill = home.path().join(".copilot/skills/graphify/SKILL.md");
        let instructions = project.path().join(".github/copilot-instructions.md");
        assert!(skill.exists());
        assert!(instructions.exists());
        let instructions_content = fs::read_to_string(&instructions).unwrap();
        assert!(instructions_content.contains("## graphify"));
        assert!(instructions_content.contains("Type `/graphify`"));

        let removed = vscode_uninstall(home.path(), project.path()).unwrap();
        assert!(!removed.is_empty());
        assert!(!skill.exists());
        assert!(!instructions.exists());
    }

    #[test]
    fn opencode_install_writes_plugin() {
        let home = tempdir().unwrap();
        let project = tempdir().unwrap();

        let installed = opencode_install(home.path(), project.path(), "test-version").unwrap();
        assert!(
            installed
                .iter()
                .any(|line| line.contains("plugin registered"))
        );
        assert!(
            home.path()
                .join(".config/opencode/skills/graphify/SKILL.md")
                .exists()
        );
        assert!(
            project
                .path()
                .join(".opencode/plugins/graphify.js")
                .exists()
        );
        let config = fs::read_to_string(project.path().join("opencode.json")).unwrap();
        assert!(config.contains(".opencode/plugins/graphify.js"));
    }
}
