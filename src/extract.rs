#![allow(clippy::too_many_arguments)]

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use rayon::prelude::*;
use regex::Regex;
use tree_sitter::{Language, Node as TsNode, Parser};

use crate::cache;
use crate::detect::{FileType, classify_file};
use crate::schema::{Edge, Extraction, FunctionReturn, Node, RawCall};

const PYTHON_RATIONALE_PREFIXES: [&str; 7] = [
    "# NOTE:",
    "# IMPORTANT:",
    "# HACK:",
    "# WHY:",
    "# RATIONALE:",
    "# TODO:",
    "# FIXME:",
];

// ── Language configuration ────────────────────────────────────────────────────

struct LanguageConfig {
    class_types: &'static [&'static str],
    function_types: &'static [&'static str],
    import_types: &'static [&'static str],
    call_types: &'static [&'static str],
    call_function_field: &'static str,
    call_accessor_field: &'static str,
    call_accessor_node_types: &'static [&'static str],
    allow_receiver_call_cross_file: bool,
    name_field: &'static str,
    name_fallback_child_types: &'static [&'static str],
    body_field: &'static str,
    body_fallback_child_types: &'static [&'static str],
    function_boundary_types: &'static [&'static str],
    inheritance_child_types: &'static [&'static str],
    language: Language,
}

fn lang_config(language: Language) -> LanguageConfig {
    LanguageConfig {
        class_types: &[],
        function_types: &[],
        import_types: &[],
        call_types: &[],
        call_function_field: "",
        call_accessor_field: "",
        call_accessor_node_types: &[],
        allow_receiver_call_cross_file: false,
        name_field: "name",
        name_fallback_child_types: &[],
        body_field: "body",
        body_fallback_child_types: &[],
        function_boundary_types: &[],
        inheritance_child_types: &[],
        language,
    }
}

macro_rules! cfg {
    ($lang:expr => { $($key:ident => $val:expr),* $(,)? }) => {{
        let mut c = lang_config($lang);
        $(c.$key = $val;)*
        c
    }};
}

// ── Language configs ──────────────────────────────────────────────────────────

fn python_cfg() -> LanguageConfig {
    cfg!(tree_sitter_python::LANGUAGE.into() => {
        class_types => &["class_definition"],
        function_types => &["function_definition", "async_function_definition"],
        import_types => &["import_statement", "import_from_statement"],
        call_types => &["call"],
        call_function_field => "function",
        call_accessor_field => "attribute",
        call_accessor_node_types => &["attribute"],
        body_field => "body",
        function_boundary_types => &["function_definition", "async_function_definition"],
        inheritance_child_types => &["superclasses"],
    })
}

fn js_cfg() -> LanguageConfig {
    cfg!(tree_sitter_javascript::LANGUAGE.into() => {
        class_types => &["class_declaration"],
        function_types => &["function_declaration", "method_definition", "arrow_function"],
        import_types => &["import_statement"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "property",
        call_accessor_node_types => &["member_expression"],
        body_field => "body",
        function_boundary_types => &["function_declaration", "arrow_function", "method_definition"],
    })
}

fn ts_cfg() -> LanguageConfig {
    cfg!(tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into() => {
        class_types => &["class_declaration", "interface_declaration", "enum_declaration", "type_alias_declaration"],
        function_types => &["function_declaration", "method_definition", "arrow_function"],
        import_types => &["import_statement", "import_require_clause"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "property",
        call_accessor_node_types => &["member_expression"],
        body_field => "body",
        function_boundary_types => &["function_declaration", "arrow_function", "method_definition"],
    })
}

fn java_cfg() -> LanguageConfig {
    cfg!(tree_sitter_java::LANGUAGE.into() => {
        class_types => &["class_declaration", "interface_declaration", "enum_declaration"],
        function_types => &["method_declaration", "constructor_declaration"],
        import_types => &["import_declaration"],
        call_types => &["method_invocation"],
        call_function_field => "name",
        call_accessor_field => "",
        call_accessor_node_types => &[],
        name_field => "name",
        body_field => "body",
        body_fallback_child_types => &["declaration_list"],
        function_boundary_types => &["method_declaration", "constructor_declaration"],
        inheritance_child_types => &["superclass", "super_interfaces"],
    })
}

fn c_cfg() -> LanguageConfig {
    cfg!(tree_sitter_c::LANGUAGE.into() => {
        function_types => &["function_definition"],
        import_types => &["preproc_include"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_expression"],
        body_field => "body",
        body_fallback_child_types => &["compound_statement"],
        function_boundary_types => &["function_definition"],
    })
}

fn cpp_cfg() -> LanguageConfig {
    cfg!(tree_sitter_cpp::LANGUAGE.into() => {
        class_types => &["class_specifier", "struct_specifier"],
        function_types => &["function_definition"],
        import_types => &["preproc_include"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_expression", "qualified_identifier"],
        name_field => "name",
        body_field => "body",
        body_fallback_child_types => &["compound_statement", "field_declaration_list"],
        function_boundary_types => &["function_definition"],
        inheritance_child_types => &["base_class_clause"],
    })
}

fn ruby_cfg() -> LanguageConfig {
    cfg!(tree_sitter_ruby::LANGUAGE.into() => {
        class_types => &["class", "module"],
        function_types => &["method", "singleton_method"],
        import_types => &[],
        call_types => &["call"],
        call_function_field => "method",
        call_accessor_field => "",
        call_accessor_node_types => &[],
        name_field => "name",
        name_fallback_child_types => &["constant", "scope_resolution", "identifier"],
        body_field => "body",
        body_fallback_child_types => &["body_statement"],
        function_boundary_types => &["method", "singleton_method"],
    })
}

fn csharp_cfg() -> LanguageConfig {
    cfg!(tree_sitter_c_sharp::LANGUAGE.into() => {
        class_types => &["class_declaration", "interface_declaration", "struct_declaration", "enum_declaration", "record_declaration"],
        function_types => &["method_declaration"],
        import_types => &["using_directive"],
        call_types => &["invocation_expression"],
        call_function_field => "function",
        call_accessor_field => "name",
        call_accessor_node_types => &["member_access_expression"],
        name_field => "name",
        body_field => "body",
        body_fallback_child_types => &["declaration_list"],
        function_boundary_types => &["method_declaration"],
        inheritance_child_types => &["base_list"],
    })
}

fn kotlin_cfg() -> LanguageConfig {
    cfg!(tree_sitter_kotlin_ng::LANGUAGE.into() => {
        class_types => &["class_declaration", "object_declaration"],
        function_types => &["function_declaration"],
        // Match the current Python baseline exactly: Kotlin imports/calls are
        // not emitted there because the configured node kinds do not match.
        import_types => &["import_header"],
        call_types => &[],
        call_function_field => "",
        call_accessor_field => "",
        call_accessor_node_types => &["navigation_expression"],
        name_field => "name",
        name_fallback_child_types => &["simple_identifier"],
        body_field => "body",
        body_fallback_child_types => &["function_body", "class_body"],
        function_boundary_types => &["function_declaration"],
    })
}

fn scala_cfg() -> LanguageConfig {
    cfg!(tree_sitter_scala::LANGUAGE.into() => {
        class_types => &["class_definition", "object_definition", "trait_definition"],
        function_types => &["function_definition"],
        import_types => &["import_declaration"],
        call_types => &["call_expression"],
        call_function_field => "",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_expression"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["template_body"],
        function_boundary_types => &["function_definition"],
        inheritance_child_types => &["extends_clause"],
    })
}

fn php_cfg() -> LanguageConfig {
    cfg!(tree_sitter_php::LANGUAGE_PHP.into() => {
        class_types => &["class_declaration", "interface_declaration", "trait_declaration"],
        function_types => &["function_definition", "method_declaration"],
        import_types => &["namespace_use_clause"],
        call_types => &["function_call_expression", "member_call_expression", "scoped_call_expression"],
        call_function_field => "function",
        call_accessor_field => "name",
        call_accessor_node_types => &["member_call_expression", "scoped_call_expression"],
        name_field => "name",
        name_fallback_child_types => &["name"],
        body_field => "body",
        body_fallback_child_types => &["declaration_list", "compound_statement"],
        function_boundary_types => &["function_definition", "method_declaration"],
        inheritance_child_types => &["class_clause", "interface_clause"],
    })
}

fn lua_cfg() -> LanguageConfig {
    cfg!(tree_sitter_lua::LANGUAGE.into() => {
        function_types => &["function_declaration", "function_definition"],
        import_types => &["variable_declaration"],
        call_types => &["function_call"],
        call_function_field => "name",
        call_accessor_field => "name",
        call_accessor_node_types => &["method_index_expression"],
        name_field => "name",
        name_fallback_child_types => &["identifier", "method_index_expression"],
        body_field => "body",
        body_fallback_child_types => &["block"],
        function_boundary_types => &["function_declaration", "function_definition"],
    })
}

fn swift_cfg() -> LanguageConfig {
    cfg!(tree_sitter_swift::LANGUAGE.into() => {
        class_types => &["class_declaration", "protocol_declaration", "struct_declaration", "enum_declaration", "actor_declaration"],
        function_types => &["function_declaration", "init_declaration", "deinit_declaration", "subscript_declaration"],
        import_types => &["import_declaration"],
        call_types => &["call_expression"],
        call_function_field => "",
        call_accessor_field => "",
        call_accessor_node_types => &["navigation_expression"],
        name_field => "name",
        name_fallback_child_types => &["simple_identifier", "type_identifier", "user_type"],
        body_field => "body",
        body_fallback_child_types => &["class_body", "protocol_body", "function_body", "enum_class_body"],
        function_boundary_types => &["function_declaration", "init_declaration", "deinit_declaration", "subscript_declaration"],
        inheritance_child_types => &["inheritance_specifier"],
    })
}

fn zig_cfg() -> LanguageConfig {
    cfg!(tree_sitter_zig::LANGUAGE.into() => {
        class_types => &["struct_declaration", "enum_declaration", "union_declaration"],
        function_types => &["function_declaration"],
        import_types => &["variable_declaration"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_access"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["block"],
        function_boundary_types => &["function_declaration"],
    })
}

fn elixir_cfg() -> LanguageConfig {
    cfg!(tree_sitter_elixir::LANGUAGE.into() => {
        class_types => &["call"],
        function_types => &["call"],
        import_types => &["call"],
        call_types => &["call"],
        call_function_field => "function",
        call_accessor_field => "",
        call_accessor_node_types => &[],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["block", "do_block"],
        function_boundary_types => &["do_block"],
    })
}

fn julia_cfg() -> LanguageConfig {
    cfg!(tree_sitter_julia::LANGUAGE.into() => {
        class_types => &["struct_definition", "abstract_definition", "primitive_definition"],
        function_types => &["function_definition", "function_declaration", "short_function_definition"],
        import_types => &["import_statement", "using_statement"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_expression"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["block"],
        function_boundary_types => &["function_definition", "function_declaration", "short_function_definition"],
        inheritance_child_types => &["sub_type"],
    })
}

fn objc_cfg() -> LanguageConfig {
    cfg!(tree_sitter_objc::LANGUAGE.into() => {
        class_types => &["class_declaration", "protocol_declaration", "category_declaration"],
        function_types => &["function_definition", "method_declaration"],
        import_types => &["preproc_include"],
        call_types => &["call_expression", "message_expression"],
        call_function_field => "function",
        call_accessor_field => "selector",
        call_accessor_node_types => &["message_expression"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["compound_statement"],
        function_boundary_types => &["function_definition", "method_declaration"],
        inheritance_child_types => &["superclass_clause", "protocol_list"],
    })
}

fn powershell_cfg() -> LanguageConfig {
    cfg!(tree_sitter_powershell::LANGUAGE.into() => {
        class_types => &["class_definition"],
        function_types => &["function_declaration"],
        import_types => &["using_statement"],
        call_types => &["command", "command_call"],
        call_function_field => "command_name",
        call_accessor_field => "member_name",
        call_accessor_node_types => &["member_access"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["script_block"],
        function_boundary_types => &["function_declaration"],
    })
}

fn verilog_cfg() -> LanguageConfig {
    cfg!(tree_sitter_verilog::LANGUAGE.into() => {
        class_types => &["module_declaration"],
        function_types => &["function_declaration", "task_declaration"],
        import_types => &["package_import_declaration", "include_statement"],
        call_types => &["function_subroutine_call"],
        call_function_field => "subroutine",
        call_accessor_field => "",
        call_accessor_node_types => &[],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["function_body_declaration", "statement_block"],
        function_boundary_types => &["function_declaration", "task_declaration"],
    })
}

fn go_cfg() -> LanguageConfig {
    cfg!(tree_sitter_go::LANGUAGE.into() => {
        class_types => &["type_declaration"],
        function_types => &["function_declaration", "method_declaration"],
        import_types => &["import_declaration"],
        call_types => &["call_expression"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["selector_expression"],
        name_field => "name",
        name_fallback_child_types => &["type_identifier"],
        body_field => "body",
        body_fallback_child_types => &["block"],
        function_boundary_types => &["function_declaration", "method_declaration"],
    })
}

fn rust_cfg() -> LanguageConfig {
    cfg!(tree_sitter_rust::LANGUAGE.into() => {
        class_types => &["struct_item", "enum_item", "trait_item", "impl_item"],
        function_types => &["function_item"],
        import_types => &["use_declaration", "extern_crate_declaration", "foreign_mod_item"],
        call_types => &["call_expression", "macro_invocation"],
        call_function_field => "function",
        call_accessor_field => "field",
        call_accessor_node_types => &["field_expression"],
        allow_receiver_call_cross_file => true,
        name_field => "name",
        name_fallback_child_types => &["identifier", "type_identifier"],
        body_field => "body",
        body_fallback_child_types => &["block"],
        function_boundary_types => &["function_item"],
    })
}

fn dart_cfg() -> LanguageConfig {
    cfg!(tree_sitter_dart::LANGUAGE.into() => {
        class_types => &["class_declaration", "enum_declaration", "extension_declaration"],
        function_types => &["method_signature", "function_signature", "local_function_declaration"],
        import_types => &["import_or_export"],
        call_types => &["constructor_invocation", "selector"],
        call_function_field => "type",
        call_accessor_field => "identifier",
        call_accessor_node_types => &["unconditional_assignable_selector"],
        name_field => "name",
        name_fallback_child_types => &["identifier"],
        body_field => "body",
        body_fallback_child_types => &["class_body", "function_body", "block"],
        function_boundary_types => &["method_signature", "function_signature", "local_function_declaration"],
        inheritance_child_types => &["superclass", "interfaces"],
    })
}

// ── Extension → config dispatch ───────────────────────────────────────────────

fn config_for_path(path: &Path) -> Option<LanguageConfig> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let name_lower = name.to_lowercase();

    if name_lower.ends_with(".blade.php") {
        return Some(php_cfg());
    }

    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let ext_lower = ext.to_lowercase();

    match ext_lower.as_str() {
        "py" => Some(python_cfg()),
        "js" | "cjs" | "mjs" | "jsx" => Some(js_cfg()),
        "ts" | "tsx" | "mts" | "cts" => Some(ts_cfg()),
        "java" => Some(java_cfg()),
        "c" | "h" => Some(c_cfg()),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" => Some(cpp_cfg()),
        "rb" => Some(ruby_cfg()),
        "cs" => Some(csharp_cfg()),
        "kt" | "kts" => Some(kotlin_cfg()),
        "scala" | "sc" => Some(scala_cfg()),
        "php" => Some(php_cfg()),
        "lua" | "toc" => Some(lua_cfg()),
        "swift" => Some(swift_cfg()),
        "zig" => Some(zig_cfg()),
        "ex" | "exs" => Some(elixir_cfg()),
        "jl" => Some(julia_cfg()),
        "m" | "mm" => Some(objc_cfg()),
        "ps1" => Some(powershell_cfg()),
        "v" | "sv" | "svh" => Some(verilog_cfg()),
        "go" => Some(go_cfg()),
        "rs" => Some(rust_cfg()),
        "dart" => Some(dart_cfg()),
        // Vue/Svelte: handled separately by embedded script extraction.
        _ => None,
    }
}

// ── Main entry point ──────────────────────────────────────────────────────────

#[derive(Debug)]
struct ExtractPathResult {
    path: PathBuf,
    extraction: Extraction,
    is_python: bool,
}

fn extract_path(path_str: &str) -> Result<Option<ExtractPathResult>> {
    let path = Path::new(path_str);
    match classify_file(path) {
        Some(FileType::Document) | Some(FileType::Paper) => Ok(Some(ExtractPathResult {
            path: path.to_path_buf(),
            extraction: extract_text_document(path)?,
            is_python: false,
        })),
        Some(FileType::Code) => {
            if is_embedded_script_path(path) {
                return Ok(Some(ExtractPathResult {
                    path: path.to_path_buf(),
                    extraction: extract_embedded_script_file(path)?,
                    is_python: false,
                }));
            }
            if let Some(result) = extract_special_case_code(path)? {
                return Ok(Some(ExtractPathResult {
                    path: path.to_path_buf(),
                    extraction: result,
                    is_python: false,
                }));
            }
            let Some(cfg) = config_for_path(path) else {
                return Ok(None);
            };
            Ok(Some(ExtractPathResult {
                path: path.to_path_buf(),
                extraction: extract_generic(path, &cfg)?,
                is_python: path.extension().and_then(|ext| ext.to_str()) == Some("py"),
            }))
        }
        _ => Ok(None),
    }
}

pub fn extract_paths(paths: &[String]) -> Result<Extraction> {
    if paths.is_empty() {
        return Ok(Extraction::default());
    }

    let mut combined = Extraction::default();
    let mut python_results: Vec<(PathBuf, Extraction)> = Vec::new();

    let mut extracted: Vec<(usize, Result<Option<ExtractPathResult>>)> = paths
        .par_iter()
        .enumerate()
        .map(|(index, path_str)| (index, extract_path(path_str)))
        .collect();
    extracted.sort_by_key(|(index, _)| *index);

    for (_, result) in extracted {
        let Some(result) = result? else {
            continue;
        };
        if result.is_python {
            python_results.push((result.path.clone(), result.extraction.clone()));
        }
        append_extraction(&mut combined, result.extraction);
    }

    if !python_results.is_empty() {
        combined
            .edges
            .extend(resolve_python_cross_file_imports(&python_results)?);
    }

    resolve_cross_file_calls(&mut combined);

    Ok(combined)
}

/// Extract paths with per-file caching.
///
/// For each path, checks `graphify-out/cache/` first. If a cached extraction
/// exists and the file hash matches, the cached result is used directly.
/// Uncached files are extracted normally and the results are saved back to
/// cache keyed by source_file.
pub fn extract_paths_cached(paths: &[String], cache_root: &Path) -> Result<Extraction> {
    if paths.is_empty() {
        return Ok(Extraction::default());
    }

    // Split into cached and uncached.
    let mut combined = Extraction::default();
    let mut uncached: Vec<String> = Vec::new();

    for path_str in paths {
        let path = Path::new(path_str);
        match cache::load_cached(path, cache_root) {
            Some(cached) => append_extraction(&mut combined, cached),
            None => uncached.push(path_str.clone()),
        }
    }

    if uncached.is_empty() {
        return Ok(combined);
    }

    let fresh = extract_paths(&uncached)?;

    // Save per-file results to cache.
    let mut by_file: HashMap<PathBuf, Extraction> = HashMap::new();
    for node in &fresh.nodes {
        let src = PathBuf::from(&node.source_file);
        by_file.entry(src).or_default().nodes.push(node.clone());
    }
    for edge in &fresh.edges {
        let src = PathBuf::from(&edge.source_file);
        by_file.entry(src).or_default().edges.push(edge.clone());
    }
    for hyperedge in &fresh.hyperedges {
        if let Some(src) = hyperedge.get("source_file").and_then(|v| v.as_str()) {
            by_file.entry(PathBuf::from(src)).or_default().hyperedges.push(hyperedge.clone());
        }
    }
    for (fpath, per_file) in by_file {
        let abs = if fpath.is_absolute() {
            fpath
        } else {
            cache_root.join(fpath)
        };
        if abs.is_file() {
            let _ = cache::save_cached(&abs, &per_file, cache_root);
        }
    }

    append_extraction(&mut combined, fresh);
    Ok(combined)
}

fn append_extraction(dst: &mut Extraction, src: Extraction) {
    dst.nodes.extend(src.nodes);
    dst.edges.extend(src.edges);
    dst.raw_calls.extend(src.raw_calls);
    dst.function_returns.extend(src.function_returns);
    dst.hyperedges.extend(src.hyperedges);
    dst.input_tokens += src.input_tokens;
    dst.output_tokens += src.output_tokens;
}

fn resolve_cross_file_calls(extraction: &mut Extraction) {
    if extraction.raw_calls.is_empty() {
        return;
    }

    let label_to_id = build_label_index(&extraction.nodes);
    let function_return_index = build_function_return_index(&extraction.function_returns);
    let method_index = build_method_index(&extraction.nodes, &extraction.edges);
    let mut existing_pairs: HashSet<(String, String)> = extraction
        .edges
        .iter()
        .map(|edge| (edge.source.clone(), edge.target.clone()))
        .collect();

    for raw_call in extraction.raw_calls.drain(..) {
        let Some(target_id) = lookup_call_target(&label_to_id, &raw_call.callee).or_else(|| {
            lookup_receiver_method_target(&method_index, &function_return_index, &raw_call)
        }) else {
            continue;
        };
        if target_id == &raw_call.caller_nid {
            continue;
        }
        let pair = (raw_call.caller_nid.clone(), target_id.clone());
        if !existing_pairs.insert(pair.clone()) {
            continue;
        }
        extraction.edges.push(Edge {
            source: pair.0,
            target: pair.1,
            relation: "calls".to_string(),
            confidence: "INFERRED".to_string(),
            source_file: raw_call.source_file,
            original_source: None,
            original_target: None,
            source_location: raw_call.source_location,
            confidence_score: Some(0.8),
            confidence_score_present: true,
            weight: 1.0,
            extra: Default::default(),
        });
    }
}

fn is_embedded_script_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("vue" | "svelte")
    )
}

#[derive(Debug)]
struct EmbeddedScriptBlock {
    code: String,
    line_offset: usize,
    use_typescript: bool,
}

fn extract_embedded_script_file(path: &Path) -> Result<Extraction> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Failed to read embedded script file: {}", path.display()))?;
    let blocks = extract_embedded_script_blocks(&raw)?;

    let source_file = path.to_string_lossy().to_string();
    let file_label = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| source_file.clone());
    let file_id = make_id(&source_file);

    let mut combined = Extraction::default();
    combined.nodes.push(Node {
        id: file_id.clone(),
        label: file_label,
        file_type: "code".to_string(),
        source_file: source_file.clone(),
        source_location: Some("L1".to_string()),
        node_type: Some("file".to_string()),
        docstring: None,
        parameters: Vec::new(),
        signature: None,
        extra: Default::default(),
    });

    let mut seen_node_ids: HashSet<String> = HashSet::from([file_id]);
    let mut seen_edges: HashSet<(String, String, String, Option<String>)> = HashSet::new();

    for block in blocks {
        let cfg = if block.use_typescript {
            ts_cfg()
        } else {
            js_cfg()
        };
        let mut extracted = extract_generic_from_source(path, block.code.as_bytes(), &cfg)?;
        shift_extraction_source_locations(&mut extracted, block.line_offset);
        extracted
            .nodes
            .retain(|node| node.node_type.as_deref() != Some("file"));
        append_extraction_unique(
            &mut combined,
            extracted,
            &mut seen_node_ids,
            &mut seen_edges,
        );
    }

    Ok(combined)
}

fn extract_embedded_script_blocks(source: &str) -> Result<Vec<EmbeddedScriptBlock>> {
    let script_re = Regex::new(r"(?is)<script\b(?P<attrs>[^>]*)>(?P<body>.*?)</script>")
        .map_err(|err| anyhow!("Failed to compile embedded script regex: {err}"))?;

    let mut blocks = Vec::new();
    for caps in script_re.captures_iter(source) {
        let Some(body) = caps.name("body") else {
            continue;
        };
        if body.as_str().trim().is_empty() {
            continue;
        }
        let attrs = caps.name("attrs").map(|m| m.as_str()).unwrap_or_default();
        let attrs_lower = attrs.to_lowercase();
        let use_typescript = attrs_lower.contains("lang=\"ts\"")
            || attrs_lower.contains("lang='ts'")
            || attrs_lower.contains("lang=ts")
            || attrs_lower.contains("lang=\"tsx\"")
            || attrs_lower.contains("lang='tsx'")
            || attrs_lower.contains("lang=tsx")
            || attrs_lower.contains("lang=\"typescript\"")
            || attrs_lower.contains("lang='typescript'")
            || attrs_lower.contains("lang=typescript");
        let line_offset = source[..body.start()]
            .bytes()
            .filter(|b| *b == b'\n')
            .count();
        blocks.push(EmbeddedScriptBlock {
            code: body.as_str().to_string(),
            line_offset,
            use_typescript,
        });
    }
    Ok(blocks)
}

fn shift_extraction_source_locations(extraction: &mut Extraction, line_offset: usize) {
    if line_offset == 0 {
        return;
    }
    for node in &mut extraction.nodes {
        shift_line_location(&mut node.source_location, line_offset);
    }
    for edge in &mut extraction.edges {
        shift_line_location(&mut edge.source_location, line_offset);
    }
}

fn shift_line_location(location: &mut Option<String>, line_offset: usize) {
    let Some(raw) = location else { return };
    let Some(line_no) = raw.strip_prefix('L').and_then(|n| n.parse::<usize>().ok()) else {
        return;
    };
    *raw = format!("L{}", line_no + line_offset);
}

fn append_extraction_unique(
    dst: &mut Extraction,
    src: Extraction,
    seen_node_ids: &mut HashSet<String>,
    seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
) {
    for node in src.nodes {
        if seen_node_ids.insert(node.id.clone()) {
            dst.nodes.push(node);
        }
    }
    for edge in src.edges {
        let key = (
            edge.source.clone(),
            edge.target.clone(),
            edge.relation.clone(),
            edge.source_location.clone(),
        );
        if seen_edges.insert(key) {
            dst.edges.push(edge);
        }
    }
    dst.raw_calls.extend(src.raw_calls);
    dst.function_returns.extend(src.function_returns);
    dst.hyperedges.extend(src.hyperedges);
    dst.input_tokens += src.input_tokens;
    dst.output_tokens += src.output_tokens;
}

fn extract_special_case_code(path: &Path) -> Result<Option<Extraction>> {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let extraction = match ext.as_str() {
        "m" | "mm" => Some(extract_objc_file(path)?),
        "swift" => Some(extract_swift_file(path)?),
        "php" => Some(extract_php_file(path)?),
        "ps1" => Some(extract_powershell_file(path)?),
        "jl" => Some(extract_julia_file(path)?),
        "ex" | "exs" => Some(extract_elixir_file(path)?),
        "zig" => Some(extract_zig_file(path)?),
        "cs" => Some(extract_csharp_file(path)?),
        _ => None,
    };
    Ok(extraction)
}

fn new_file_extraction(path: &Path) -> (Extraction, HashSet<String>, String, String) {
    let source_file = path.to_string_lossy().to_string();
    let file_label = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| source_file.clone());
    let file_id = make_id(&source_file);
    let mut extraction = Extraction::default();
    let mut seen_ids = HashSet::new();
    add_node(
        &mut extraction.nodes,
        &mut seen_ids,
        Node {
            id: file_id.clone(),
            label: file_label,
            file_type: "code".to_string(),
            source_file: source_file.clone(),
            source_location: Some("L1".to_string()),
            node_type: Some("file".to_string()),
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );
    (extraction, seen_ids, file_id, source_file)
}

// ── Generic AST extraction ────────────────────────────────────────────────────

fn extract_generic(path: &Path, cfg: &LanguageConfig) -> Result<Extraction> {
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    extract_generic_from_source(path, &source, cfg)
}

fn extract_generic_from_source(
    path: &Path,
    source: &[u8],
    cfg: &LanguageConfig,
) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser.set_language(&cfg.language).map_err(|err| {
        anyhow!(
            "Failed to set parser language for {}: {err}",
            path.display()
        )
    })?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let source_file = path.to_string_lossy().to_string();
    let file_label = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| source_file.clone());
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let file_id = make_id(&source_file);

    let mut extraction = Extraction::default();
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut pending_calls: Vec<PendingCall> = Vec::new();

    add_node(
        &mut extraction.nodes,
        &mut seen_ids,
        Node {
            id: file_id.clone(),
            label: file_label,
            file_type: "code".to_string(),
            source_file: source_file.clone(),
            source_location: Some("L1".to_string()),
            node_type: Some("file".to_string()),
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );

    walk_tree(
        root,
        None,
        source,
        &source_file,
        stem,
        &file_id,
        cfg,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut pending_calls,
        &mut extraction.function_returns,
    );

    // Resolve pending calls
    let label_to_id = build_label_index(&extraction.nodes);
    let mut seen_call_pairs: HashSet<(String, String)> = HashSet::new();
    for pending in pending_calls {
        if let Some(target_id) = lookup_call_target(&label_to_id, &pending.callee_name) {
            if target_id == &pending.caller_id {
                continue;
            }
            let pair = (pending.caller_id.clone(), target_id.clone());
            if !seen_call_pairs.insert(pair.clone()) {
                continue;
            }
            push_edge(
                &mut extraction.edges,
                EdgeSpec {
                    source: pair.0,
                    target: pair.1,
                    relation: "calls".to_string(),
                    source_file: source_file.clone(),
                    line_no: pending.line_no,
                },
            );
        } else if pending.allow_cross_file {
            extraction.raw_calls.push(RawCall {
                caller_nid: pending.caller_id,
                callee: pending.callee_name,
                source_file: source_file.clone(),
                source_location: Some(format!("L{}", pending.line_no)),
                receiver_call: pending.receiver_call,
            });
        }
    }

    if path.extension().and_then(|ext| ext.to_str()) == Some("py") {
        extract_python_rationale(
            root,
            source,
            path,
            stem,
            &file_id,
            &mut extraction.nodes,
            &mut extraction.edges,
            &mut seen_ids,
        );
    }

    Ok(extraction)
}

// ── Tree walk ─────────────────────────────────────────────────────────────────

fn walk_tree<'a>(
    node: TsNode<'a>,
    parent_class_id: Option<&str>,
    source: &[u8],
    source_file: &str,
    stem: &str,
    file_id: &str,
    cfg: &LanguageConfig,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
    pending_calls: &mut Vec<PendingCall>,
    function_returns: &mut Vec<FunctionReturn>,
) {
    let t = node.kind();

    // Import types — handle and don't recurse
    if cfg.import_types.contains(&t) {
        handle_import(node, source, file_id, source_file, cfg, edges);
        return;
    }

    // Class types
    if cfg.class_types.contains(&t) {
        handle_class(
            node,
            parent_class_id,
            source,
            source_file,
            stem,
            file_id,
            cfg,
            nodes,
            edges,
            seen_ids,
            pending_calls,
            function_returns,
        );
        return;
    }

    // Function types
    if cfg.function_types.contains(&t) {
        handle_function(
            node,
            parent_class_id,
            source,
            source_file,
            stem,
            file_id,
            cfg,
            nodes,
            edges,
            seen_ids,
            pending_calls,
            function_returns,
        );
        return;
    }

    // Recurse
    for child in named_children(node) {
        walk_tree(
            child,
            parent_class_id,
            source,
            source_file,
            stem,
            file_id,
            cfg,
            nodes,
            edges,
            seen_ids,
            pending_calls,
            function_returns,
        );
    }
}

// ── Class handling ────────────────────────────────────────────────────────────

/// Map a tree-sitter AST node kind to the correct semantic node_type.
/// Types like interface/enum/type_alias are distinct from classes and should
/// be labeled accordingly for downstream consumers.
fn class_node_type(kind: &str) -> &'static str {
    match kind {
        "interface_declaration" | "protocol_declaration" => "interface",
        "enum_declaration" => "enum",
        "type_alias_declaration" => "type_alias",
        "struct_declaration" | "struct_specifier" | "struct_item" | "struct_definition" => "struct",
        "trait_item" | "trait_declaration" | "trait_definition" => "trait",
        "impl_item" => "impl",
        "actor_declaration" => "actor",
        "union_declaration" => "union",
        "abstract_definition" => "abstract_type",
        "primitive_definition" => "primitive_type",
        "module" | "module_declaration" => "module",
        "category_declaration" => "category",
        "extension_declaration" => "extension",
        "record_declaration" => "record",
        "object_declaration" | "object_definition" => "object",
        _ => "class",
    }
}

fn handle_class<'a>(
    node: TsNode<'a>,
    _parent_class_id: Option<&str>,
    source: &[u8],
    source_file: &str,
    stem: &str,
    file_id: &str,
    cfg: &LanguageConfig,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
    pending_calls: &mut Vec<PendingCall>,
    function_returns: &mut Vec<FunctionReturn>,
) {
    let class_name = if node.kind() == "impl_item" {
        resolve_impl_type(node, source, stem)
    } else {
        resolve_name(node, source, cfg)
    }
    .or_else(|| {
        // Go type_declaration: walk into type_spec
        if node.kind() == "type_declaration" {
            for child in named_children(node) {
                if child.kind() == "type_spec" {
                    return resolve_name(child, source, cfg);
                }
            }
        }
        // Rust struct_item/enum_item: name may be in type_identifier
        if node.kind() == "struct_item" || node.kind() == "enum_item" || node.kind() == "trait_item"
        {
            for child in named_children(node) {
                if child.kind() == "type_identifier" {
                    return node_text(child, source);
                }
            }
        }
        None
    });
    let Some(class_name) = class_name else { return };

    let class_scope = if source_extension(source_file) == "go" && node.kind() == "type_declaration"
    {
        package_scope(source_file, stem)
    } else {
        stem.to_string()
    };
    let class_id = make_id(&format!("{class_scope}_{class_name}"));
    let line_no = node.start_position().row + 1;
    let already_seen = seen_ids.contains(&class_id);

    add_node(
        nodes,
        seen_ids,
        Node {
            id: class_id.clone(),
            label: class_name.clone(),
            file_type: "code".to_string(),
            source_file: source_file.to_string(),
            source_location: Some(format!("L{line_no}")),
            node_type: Some(class_node_type(node.kind()).to_string()),
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );
    if node.kind() != "impl_item" && !already_seen {
        push_edge(
            edges,
            EdgeSpec {
                source: file_id.to_string(),
                target: class_id.clone(),
                relation: "contains".to_string(),
                source_file: source_file.to_string(),
                line_no,
            },
        );
    }

    // Inheritance
    handle_inheritance(
        &class_id,
        node,
        source,
        source_file,
        stem,
        cfg,
        nodes,
        edges,
        seen_ids,
    );

    // Body recurse
    let body = find_body(node, cfg);
    if let Some(body_node) = body {
        for child in named_children(body_node) {
            walk_tree(
                child,
                Some(&class_id),
                source,
                source_file,
                stem,
                file_id,
                cfg,
                nodes,
                edges,
                seen_ids,
                pending_calls,
                function_returns,
            );
        }
    }
}

fn handle_inheritance(
    class_id: &str,
    node: TsNode<'_>,
    source: &[u8],
    source_file: &str,
    stem: &str,
    cfg: &LanguageConfig,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
) {
    let line_no = node.start_position().row + 1;
    let ext = source_extension(source_file);
    for inh_type in cfg.inheritance_child_types {
        let mut inheritance_nodes = Vec::new();
        if let Some(child) = node.child_by_field_name(inh_type) {
            inheritance_nodes.push(child);
        }
        inheritance_nodes.extend(
            named_children(node)
                .into_iter()
                .filter(|child| child.kind() == *inh_type),
        );
        for child in inheritance_nodes {
            let bases = if ext == "py" {
                named_children(child)
                    .into_iter()
                    .filter(|arg| arg.kind() == "identifier")
                    .filter_map(|arg| node_text(arg, source))
                    .collect()
            } else {
                collect_identifiers(child, source)
            };
            for base in bases {
                let mut base_nid = make_id(&format!("{stem}_{base}"));
                if !seen_ids.contains(&base_nid) {
                    base_nid = make_id(&base);
                    if !seen_ids.contains(&base_nid) {
                        add_node(
                            nodes,
                            seen_ids,
                            Node {
                                id: base_nid.clone(),
                                label: base.clone(),
                                file_type: "code".to_string(),
                                source_file: String::new(),
                                source_location: None,
                                node_type: None,
                                docstring: None,
                                parameters: Vec::new(),
                                signature: None,
                                extra: Default::default(),
                            },
                        );
                    }
                }
                push_edge(
                    edges,
                    EdgeSpec {
                        source: class_id.to_string(),
                        target: base_nid,
                        relation: "inherits".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
            }
        }
    }
}

fn package_scope(source_file: &str, stem: &str) -> String {
    Path::new(source_file)
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(stem)
        .to_string()
}

fn resolve_impl_type(node: TsNode<'_>, source: &[u8], _stem: &str) -> Option<String> {
    // For `impl Trait for Type`, prefer the implemented type (`Type`), not the trait.
    let raw = node_text(node, source);
    let mut type_names = Vec::new();
    for child in named_children(node) {
        if child.kind() == "type_identifier"
            && let Some(type_name) = node_text(child, source) {
                type_names.push(type_name);
            }
        if child.kind() == "trait_bound" {
            for sub in named_children(child) {
                if sub.kind() == "type_identifier"
                    && let Some(type_name) = node_text(sub, source) {
                        type_names.push(type_name);
                    }
            }
        }
    }
    if raw.as_deref().is_some_and(|text| text.contains(" for ")) {
        type_names.into_iter().next_back()
    } else {
        type_names.into_iter().next()
    }
}

// ── Function handling ─────────────────────────────────────────────────────────

fn handle_function<'a>(
    node: TsNode<'a>,
    parent_class_id: Option<&str>,
    source: &[u8],
    source_file: &str,
    stem: &str,
    file_id: &str,
    cfg: &LanguageConfig,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
    pending_calls: &mut Vec<PendingCall>,
    function_returns: &mut Vec<FunctionReturn>,
) {
    let ext = source_extension(source_file);
    let func_name = if matches!(ext, "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx") {
        node.child_by_field_name("declarator")
            .and_then(|decl| resolve_c_like_function_name(decl, source, ext != "c" && ext != "h"))
    } else {
        resolve_name(node, source, cfg)
            .or_else(|| resolve_arrow_or_func_expr_name(node, source))
    };
    let Some(func_name) = func_name else {
        return;
    };
    let line_no = node.start_position().row + 1;
    let python_property =
        source_extension(source_file) == "py" && is_python_property_function(node, source);

    // For Go methods: check for receiver field to determine if this is a method
    let resolved_parent: Option<String> = parent_class_id.map(|s| s.to_string()).or_else(|| {
        // Go: func (r *ReceiverType) MethodName() — extract type from receiver
        node.child_by_field_name("receiver").and_then(|receiver| {
            for param in named_children(receiver) {
                for child in named_children(param) {
                    if child.kind() == "pointer_type" {
                        for sub in named_children(child) {
                            if sub.kind() == "type_identifier"
                                && let Some(type_name) = node_text(sub, source) {
                                    return Some(make_id(&format!(
                                        "{}_{type_name}",
                                        package_scope(source_file, stem)
                                    )));
                                }
                        }
                    } else if child.kind() == "type_identifier"
                        && let Some(type_name) = node_text(child, source) {
                            return Some(make_id(&format!(
                                "{}_{type_name}",
                                package_scope(source_file, stem)
                            )));
                        }
                }
            }
            None
        })
    });

    let (func_id, label, relation_source, relation) = if python_property {
        (
            make_id(&format!("{stem}_{func_name}")),
            format!("{func_name}()"),
            file_id.to_string(),
            "contains".to_string(),
        )
    } else if let Some(class_id) = &resolved_parent {
        (
            make_id(&format!("{class_id}_{func_name}")),
            format!(".{func_name}()"),
            class_id.to_string(),
            "method".to_string(),
        )
    } else {
        (
            make_id(&format!("{stem}_{func_name}")),
            format!("{func_name}()"),
            file_id.to_string(),
            "contains".to_string(),
        )
    };

    add_node(
        nodes,
        seen_ids,
        Node {
            id: func_id.clone(),
            label,
            file_type: "code".to_string(),
            source_file: source_file.to_string(),
            source_location: Some(format!("L{line_no}")),
            node_type: Some("function".to_string()),
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );
    push_edge(
        edges,
        EdgeSpec {
            source: relation_source,
            target: func_id.clone(),
            relation,
            source_file: source_file.to_string(),
            line_no,
        },
    );

    if ext == "rs"
        && let Some(return_type_name) = rust_return_type_name(node, source) {
            function_returns.push(FunctionReturn {
                function_name: func_name.clone(),
                return_type_name,
            });
        }

    // Collect calls inside this function body
    let body = find_body(node, cfg);
    if let Some(body_node) = body {
        collect_calls(body_node, &func_id, source, cfg, pending_calls);
    }
}

fn rust_return_type_name(node: TsNode<'_>, source: &[u8]) -> Option<String> {
    let return_node = node.child_by_field_name("return_type").or_else(|| {
        named_children(node)
            .into_iter()
            .find(|child| child.kind() == "return_type")
    })?;
    let return_text = node_text(return_node, source)?;
    normalize_rust_type_name(&return_text)
}

fn normalize_rust_type_name(value: &str) -> Option<String> {
    let mut last = None;
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch);
            continue;
        }
        if !current.is_empty() {
            if !matches!(
                current.as_str(),
                "pub" | "mut" | "const" | "dyn" | "impl" | "where" | "fn"
            ) {
                last = Some(current.clone());
            }
            current.clear();
        }
    }
    if !current.is_empty()
        && !matches!(
            current.as_str(),
            "pub" | "mut" | "const" | "dyn" | "impl" | "where" | "fn"
        )
    {
        last = Some(current);
    }
    last.filter(|name| name != "Self")
}

fn is_python_property_function(node: TsNode<'_>, source: &[u8]) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    if parent.kind() != "decorated_definition" {
        return false;
    }

    named_children(parent).into_iter().any(|child| {
        child.kind() == "decorator"
            && node_text(child, source).is_some_and(|text| {
                text.split_whitespace()
                    .any(|part| part.ends_with("property"))
            })
    })
}

// ── Import handling ───────────────────────────────────────────────────────────

fn handle_import(
    node: TsNode<'_>,
    source: &[u8],
    file_id: &str,
    source_file: &str,
    cfg: &LanguageConfig,
    edges: &mut Vec<Edge>,
) {
    let line_no = node.start_position().row + 1;
    let kind = node.kind();
    let ext = source_extension(source_file);

    if !cfg.import_types.contains(&kind) {
        return;
    }

    if ext == "go" && kind == "import_declaration" {
        for child in all_children(node) {
            let specs = if child.kind() == "import_spec" {
                vec![child]
            } else if child.kind() == "import_spec_list" {
                all_children(child)
                    .into_iter()
                    .filter(|spec| spec.kind() == "import_spec")
                    .collect()
            } else {
                Vec::new()
            };
            for spec in specs {
                let Some(path_node) = spec.child_by_field_name("path") else {
                    continue;
                };
                let Some(raw) = node_text(path_node, source) else {
                    continue;
                };
                let pkg_path = raw.trim_matches('"');
                if pkg_path.is_empty() {
                    continue;
                }
                // Prefix with "go_pkg_" so stdlib names (e.g. "context")
                // don't collide with local files of the same basename. (#431)
                let target_id = make_id(&format!("go_pkg_{}", pkg_path));
                push_edge(
                    edges,
                    EdgeSpec {
                        source: file_id.to_string(),
                        target: target_id,
                        relation: "imports_from".to_string(),
                        source_file: source_file.to_string(),
                        line_no: spec.start_position().row + 1,
                    },
                );
            }
        }
        return;
    }

    if ext == "scala" && kind == "import_declaration" {
        for child in all_children(node) {
            if !matches!(child.kind(), "stable_id" | "identifier") {
                continue;
            }
            let Some(raw) = node_text(child, source) else {
                continue;
            };
            let module = raw
                .split('.')
                .next_back()
                .unwrap_or(raw.as_str())
                .trim_matches(['{', '}', ' ']);
            if !module.is_empty() && module != "_" {
                push_edge(
                    edges,
                    EdgeSpec {
                        source: file_id.to_string(),
                        target: make_id(module),
                        relation: "imports".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
            }
            return;
        }
        return;
    }

    if ext == "swift" && kind == "import_declaration" {
        for child in all_children(node) {
            if child.kind() != "identifier" {
                continue;
            }
            if let Some(raw) = node_text(child, source) {
                push_edge(
                    edges,
                    EdgeSpec {
                        source: file_id.to_string(),
                        target: make_id(&raw),
                        relation: "imports".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
            }
            return;
        }
        return;
    }

    let targets: Vec<(String, String)> = match kind {
        "import_statement" if is_js_like_extension(ext) => {
            extract_js_import_targets(node, source, source_file)
        }
        "import_statement" if ext == "py" => {
            if let Some(text) = node_text(node, source) {
                text.trim_start_matches("import")
                    .trim()
                    .split(',')
                    .filter_map(|segment| {
                        let module = segment
                            .trim()
                            .split(" as ")
                            .next()
                            .unwrap_or("")
                            .trim()
                            .trim_start_matches('.');
                        if module.is_empty() {
                            None
                        } else {
                            Some((make_id(module), "imports".to_string()))
                        }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        "import_statement" => extract_import_targets(node, source),
        "import_from_statement" => {
            if let Some(raw) = node_text(node, source) {
                if let Some((lhs, _rhs)) = raw.split_once(" import ") {
                    let module = lhs.trim_start_matches("from").trim();
                    let cleaned = module.trim_matches('"').trim_matches('\'');
                    if cleaned.is_empty() {
                        vec![]
                    } else if cleaned.starts_with('.') {
                        let dots = cleaned.chars().take_while(|&ch| ch == '.').count();
                        let module_name = cleaned.trim_start_matches('.');
                        let mut base = Path::new(source_file)
                            .parent()
                            .unwrap_or_else(|| Path::new(""));
                        for _ in 0..dots.saturating_sub(1) {
                            base = base.parent().unwrap_or(base);
                        }
                        let rel = if module_name.is_empty() {
                            "__init__.py".to_string()
                        } else {
                            format!("{}.py", module_name.replace('.', "/"))
                        };
                        vec![(
                            make_id(&base.join(rel).to_string_lossy()),
                            "imports_from".to_string(),
                        )]
                    } else {
                        vec![(make_id(cleaned), "imports_from".to_string())]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "preproc_include" => {
            let path_node = node.child_by_field_name("path").or_else(|| node.child(1));
            if let Some(path_node) = path_node {
                if let Some(raw) = node_text(path_node, source) {
                    let cleaned = raw.trim_matches('"').trim_matches('<').trim_matches('>');
                    let module = cleaned
                        .split('/')
                        .next_back()
                        .map(|s| s.split('.').next().unwrap_or(s))
                        .unwrap_or(cleaned);
                    if !module.is_empty() {
                        vec![(make_id(module), "imports".to_string())]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "import_declaration" if ext == "java" => collect_java_import_targets(node, source),
        "import_declaration" => {
            let mut targets = Vec::new();
            collect_import_paths(node, source, &mut targets);
            let relation = if ext == "go" {
                "imports_from"
            } else {
                "imports"
            };
            targets
                .into_iter()
                .map(|m| (make_id(&m), relation.to_string()))
                .collect()
        }
        "using_directive" | "import_header" | "import_require_clause" => {
            if let Some(text) = node_text(node, source) {
                let text = text
                    .trim_start_matches("using")
                    .trim_start_matches("import")
                    .trim_matches(';')
                    .trim();
                let module = text
                    .rsplit("::")
                    .next()
                    .unwrap_or(text)
                    .split('.')
                    .next_back()
                    .unwrap_or(text)
                    .trim_start_matches('{')
                    .trim_end_matches('}')
                    .trim();
                if !module.is_empty() {
                    vec![(make_id(module), "imports".to_string())]
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "import" => {
            if let Some(text) = node_text(node, source) {
                let text = text.trim_start_matches("import").trim();
                if !text.is_empty() {
                    vec![(make_id(text), "imports".to_string())]
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "use_declaration" => {
            // Match Python's Rust extractor: collapse grouped imports to the base module.
            if let Some(text) = node_text(node, source) {
                let text = text.trim_start_matches("use").trim().trim_end_matches(';');
                let clean = text.split('{').next().unwrap_or(text).trim();
                let clean = clean
                    .trim_end_matches(':')
                    .trim_end_matches('*')
                    .trim_end_matches(':')
                    .trim();
                let module = clean.rsplit("::").next().unwrap_or(clean).trim();
                if module.is_empty() {
                    vec![]
                } else {
                    vec![(make_id(module), "imports_from".to_string())]
                }
            } else {
                vec![]
            }
        }
        "using_statement" => {
            // PowerShell: using namespace System
            if let Some(text) = node_text(node, source) {
                let text = text.trim_start_matches("using").trim();
                let module = text
                    .split("namespace")
                    .nth(1)
                    .unwrap_or(text)
                    .trim()
                    .split('.')
                    .next_back()
                    .unwrap_or(text)
                    .trim();
                if !module.is_empty() {
                    vec![(make_id(module), "imports".to_string())]
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "namespace_use_clause" => {
            // PHP: use App\Models\User
            if let Some(text) = node_text(node, source) {
                let text = text.trim_start_matches("use").trim().trim_end_matches(';');
                let module = text.split('\\').next_back().unwrap_or(text).trim();
                if !module.is_empty() {
                    vec![(make_id(module), "imports".to_string())]
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "variable_declaration" => {
            // Lua: local mod = require("module")
            if let Some(text) = node_text(node, source) {
                if text.contains("require") {
                    if let Some(module) = text
                        .split("require")
                        .nth(1)
                        .map(|s| {
                            s.trim_start_matches('(')
                                .trim_start_matches('"')
                                .trim_start_matches('\'')
                                .split('"')
                                .next()
                                .unwrap_or("")
                                .split('\'')
                                .next()
                                .unwrap_or("")
                        })
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                    {
                        vec![(make_id(&module), "imports".to_string())]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        _ => {
            // Fallback for elixir etc.
            if let Some(text) = node_text(node, source) {
                if text.contains("defmodule")
                    || text.contains("def")
                    || text.contains("import")
                    || text.contains("require")
                    || text.contains("use")
                    || text.contains("alias")
                {
                    // Try to extract the module/function name
                    let words: Vec<&str> = text.split_whitespace().collect();
                    if words.len() >= 2 {
                        let target = words[1].trim_matches(['(', ')', ':', ',']);
                        if !target.is_empty() && target.chars().any(|c| c.is_alphabetic()) {
                            vec![(make_id(target), "contains".to_string())]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
    };

    for (target_id, relation) in targets {
        push_edge(
            edges,
            EdgeSpec {
                source: file_id.to_string(),
                target: target_id,
                relation,
                source_file: source_file.to_string(),
                line_no,
            },
        );
    }
}

fn collect_import_paths(node: TsNode<'_>, source: &[u8], targets: &mut Vec<String>) {
    for child in named_children(node) {
        match child.kind() {
            "string" | "interpreted_string_literal" | "raw_string_literal" => {
                if let Some(text) = node_text(child, source) {
                    let cleaned = text.trim_matches('"').trim_matches('"');
                    let module = cleaned
                        .split('/')
                        .next_back()
                        .unwrap_or(cleaned)
                        .split('.')
                        .next()
                        .unwrap_or(cleaned);
                    if !module.is_empty() {
                        targets.push(module.to_string());
                    }
                }
            }
            "import_spec" | "import_spec_list" | "parenthesized_argument_list" => {
                collect_import_paths(child, source, targets);
            }
            _ => {}
        }
    }
}

fn source_extension(source_file: &str) -> &str {
    Path::new(source_file)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
}

fn is_js_like_extension(ext: &str) -> bool {
    matches!(ext, "js" | "jsx" | "ts" | "tsx" | "vue" | "svelte")
}

fn extract_js_import_targets(
    node: TsNode<'_>,
    source: &[u8],
    source_file: &str,
) -> Vec<(String, String)> {
    for child in named_children(node) {
        if child.kind() != "string" {
            continue;
        }
        let Some(raw) = node_text(child, source) else {
            continue;
        };
        let raw = raw
            .trim_matches('\'')
            .trim_matches('"')
            .trim_matches('`')
            .trim();
        if raw.is_empty() {
            continue;
        }
        let target = if raw.starts_with('.') {
            let mut resolved = normalize_lexical_path(
                Path::new(source_file)
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .join(raw),
            );
            if resolved.extension().and_then(|ext| ext.to_str()) == Some("js") {
                resolved.set_extension("ts");
            } else if resolved.extension().and_then(|ext| ext.to_str()) == Some("jsx") {
                resolved.set_extension("tsx");
            }
            make_id(&resolved.to_string_lossy())
        } else {
            let module = raw.split('/').next_back().unwrap_or(raw);
            if module.is_empty() {
                continue;
            }
            make_id(module)
        };
        return vec![(target, "imports_from".to_string())];
    }
    Vec::new()
}

fn normalize_lexical_path(path: PathBuf) -> PathBuf {
    let mut prefix = None;
    let mut has_root = false;
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(value) => prefix = Some(value.as_os_str().to_owned()),
            std::path::Component::RootDir => has_root = true,
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if parts
                    .last()
                    .is_some_and(|part: &std::ffi::OsString| part != "..")
                {
                    parts.pop();
                } else if !has_root {
                    parts.push("..".into());
                }
            }
            std::path::Component::Normal(part) => parts.push(part.to_owned()),
        }
    }
    let mut normalized = PathBuf::new();
    if let Some(prefix) = prefix {
        normalized.push(prefix);
    }
    if has_root {
        normalized.push(std::path::MAIN_SEPARATOR.to_string());
    }
    for part in parts {
        normalized.push(part);
    }
    normalized
}

fn collect_java_import_targets(node: TsNode<'_>, source: &[u8]) -> Vec<(String, String)> {
    fn walk_scoped(node: TsNode<'_>, source: &[u8]) -> Option<String> {
        match node.kind() {
            "scoped_identifier" => {
                let mut parts = Vec::new();
                let mut current = Some(node);
                while let Some(cur) = current {
                    if cur.kind() == "scoped_identifier" {
                        if let Some(name) = cur
                            .child_by_field_name("name")
                            .and_then(|n| node_text(n, source))
                        {
                            parts.push(name);
                        }
                        current = cur.child_by_field_name("scope");
                    } else if cur.kind() == "identifier" {
                        parts.push(node_text(cur, source)?);
                        break;
                    } else {
                        break;
                    }
                }
                parts.reverse();
                Some(parts.join("."))
            }
            "identifier" => node_text(node, source),
            _ => None,
        }
    }

    for child in named_children(node) {
        if !matches!(child.kind(), "scoped_identifier" | "identifier") {
            continue;
        }
        if let Some(path) = walk_scoped(child, source) {
            let segments: Vec<&str> = path.split('.').collect();
            let module = segments
                .last()
                .copied()
                .unwrap_or(path.as_str())
                .trim_matches('*')
                .trim_matches('.');
            let fallback = if module.is_empty() && segments.len() > 1 {
                segments[segments.len() - 2]
            } else {
                module
            };
            if !fallback.is_empty() {
                return vec![(make_id(fallback), "imports".to_string())];
            }
        }
    }
    Vec::new()
}

fn extract_import_targets(node: TsNode<'_>, source: &[u8]) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    if let Some(text) = node_text(node, source) {
        let inner = text.trim_start_matches("import").trim();
        for segment in inner.split(',') {
            let seg = segment.trim().trim_start_matches('{').trim_end_matches('}');
            let base = seg.split(" as ").next().unwrap_or(seg).trim();
            let module = base
                .split('/')
                .next_back()
                .unwrap_or(base)
                .split('.')
                .next()
                .unwrap_or(base);
            if !module.is_empty() && !module.starts_with('"') {
                targets.push((make_id(module), "imports".to_string()));
            }
        }
    }
    targets
}

#[derive(Debug)]
struct PythonImportFromStatement {
    target_stem: String,
    imported_names: Vec<String>,
}

fn resolve_python_cross_file_imports(per_file: &[(PathBuf, Extraction)]) -> Result<Vec<Edge>> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .map_err(|err| {
            anyhow!("Failed to set Python parser for cross-file import resolution: {err}")
        })?;

    let mut stem_to_entities: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (_path, extraction) in per_file {
        for node in &extraction.nodes {
            if node.source_file.is_empty() {
                continue;
            }
            if node.node_type.as_deref() == Some("file") {
                continue;
            }
            // Match Python parity: index both classes and functions
            // (not method stubs ending with ")" or file nodes ending with ".py")
            if node.label.ends_with(')') || node.label.ends_with(".py") {
                continue;
            }
            let Some(stem) = Path::new(&node.source_file)
                .file_stem()
                .and_then(|name| name.to_str())
            else {
                continue;
            };
            stem_to_entities
                .entry(stem.to_string())
                .or_default()
                .insert(node.label.clone(), node.id.clone());
        }
    }

    let mut edges = Vec::new();
    let mut seen_edges: HashSet<(String, String, usize)> = HashSet::new();

    for (path, extraction) in per_file {
        let source_file = path.to_string_lossy().to_string();
        let local_entities: Vec<String> = extraction
            .nodes
            .iter()
            .filter(|node| {
                node.source_file == source_file
                    && !node.label.ends_with(')')
                    && !node.label.ends_with(".py")
            })
            .map(|node| node.id.clone())
            .collect();
        if local_entities.is_empty() {
            continue;
        }

        let source = fs::read(path).with_context(|| {
            format!(
                "Failed to read Python source for import resolution: {}",
                path.display()
            )
        })?;
        let Some(tree) = parser.parse(&source, None) else {
            continue;
        };

        collect_python_cross_file_import_edges(
            tree.root_node(),
            &source,
            &source_file,
            &local_entities,
            &stem_to_entities,
            &mut seen_edges,
            &mut edges,
        );
    }

    Ok(edges)
}

fn collect_python_cross_file_import_edges(
    node: TsNode<'_>,
    source: &[u8],
    source_file: &str,
    local_entities: &[String],
    stem_to_entities: &HashMap<String, HashMap<String, String>>,
    seen_edges: &mut HashSet<(String, String, usize)>,
    edges: &mut Vec<Edge>,
) {
    if node.kind() == "import_from_statement"
        && let Some(import_statement) = parse_python_import_from_statement(node, source) {
            let line_no = node.start_position().row + 1;
            if let Some(targets) = stem_to_entities.get(&import_statement.target_stem) {
                for imported_name in import_statement.imported_names {
                    if let Some(target_id) = targets.get(&imported_name) {
                        for source_id in local_entities {
                            let key = (source_id.clone(), target_id.clone(), line_no);
                            if !seen_edges.insert(key) {
                                continue;
                            }
                            edges.push(Edge {
                                source: source_id.clone(),
                                target: target_id.clone(),
                                relation: "uses".to_string(),
                                confidence: "INFERRED".to_string(),
                                source_file: source_file.to_string(),
                                original_source: None,
                                original_target: None,
                                source_location: Some(format!("L{}", line_no)),
                                confidence_score: None,
                                confidence_score_present: false,
                                weight: 0.8,
                                extra: Default::default(),
                            });
                        }
                    }
                }
            }
        }

    for child in named_children(node) {
        collect_python_cross_file_import_edges(
            child,
            source,
            source_file,
            local_entities,
            stem_to_entities,
            seen_edges,
            edges,
        );
    }
}

fn parse_python_import_from_statement(
    node: TsNode<'_>,
    source: &[u8],
) -> Option<PythonImportFromStatement> {
    let raw = node_text(node, source)?;
    let (lhs, rhs) = raw.split_once(" import ")?;
    let module = lhs
        .trim_start_matches("from")
        .trim()
        .trim_start_matches('.')
        .trim();
    if module.is_empty() {
        return None;
    }
    let target_stem = module.split('.').next_back()?.trim();
    if target_stem.is_empty() {
        return None;
    }

    let cleaned_rhs = rhs.replace(['(', ')', '\n', '\r'], " ");
    let imported_names: Vec<String> = cleaned_rhs
        .split(',')
        .filter_map(|part| {
            let name = part.trim().split(" as ").next()?.trim();
            if name.is_empty() || name == "*" {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect();
    if imported_names.is_empty() {
        return None;
    }

    Some(PythonImportFromStatement {
        target_stem: target_stem.to_string(),
        imported_names,
    })
}

// ── Call collection ───────────────────────────────────────────────────────────

fn collect_calls(
    node: TsNode<'_>,
    caller_id: &str,
    source: &[u8],
    cfg: &LanguageConfig,
    pending_calls: &mut Vec<PendingCall>,
) {
    let t = node.kind();

    if cfg.function_boundary_types.contains(&t) {
        return;
    }

    if cfg.call_types.contains(&t)
        && let Some(resolved) = resolve_callee(node, source, cfg) {
            pending_calls.push(PendingCall {
                caller_id: caller_id.to_string(),
                callee_name: resolved.callee_name,
                line_no: node.start_position().row + 1,
                allow_cross_file: resolved.allow_cross_file,
                receiver_call: resolved.receiver_call,
            });
        }

    for child in named_children(node) {
        collect_calls(child, caller_id, source, cfg, pending_calls);
    }
}

struct ResolvedCall {
    callee_name: String,
    allow_cross_file: bool,
    receiver_call: Option<String>,
}

fn resolve_callee(node: TsNode<'_>, source: &[u8], cfg: &LanguageConfig) -> Option<ResolvedCall> {
    if node.kind() == "class_constant_access_expression" {
        return None;
    }

    let func_node = node
        .child_by_field_name(cfg.call_function_field)
        .or_else(|| node.child_by_field_name("name"))
        .or_else(|| named_children(node).into_iter().next())?;

    // Accessor call (obj.method())
    if cfg
        .call_accessor_node_types
        .iter()
        .any(|at| *at == func_node.kind())
    {
        let receiver_call = if cfg.allow_receiver_call_cross_file {
            accessor_receiver_call_name(func_node, source, cfg)
        } else {
            None
        };
        if !cfg.call_accessor_field.is_empty()
            && let Some(attr) = func_node.child_by_field_name(cfg.call_accessor_field) {
                return node_text(attr, source).map(|text| ResolvedCall {
                    callee_name: text,
                    allow_cross_file: true,
                    receiver_call,
                });
            }
        // Last named child as fallback
        if let Some(last) = named_children(func_node).into_iter().last() {
            return node_text(last, source).map(|text| ResolvedCall {
                callee_name: text,
                allow_cross_file: true,
                receiver_call,
            });
        }
    }

    if func_node.kind() == "scoped_identifier" {
        if let Some(name) = func_node
            .child_by_field_name("name")
            .and_then(|node| node_text(node, source))
        {
            return Some(ResolvedCall {
                callee_name: name,
                allow_cross_file: true,
                receiver_call: None,
            });
        }
        if let Some(last) = named_children(func_node).into_iter().last() {
            return node_text(last, source).map(|text| ResolvedCall {
                callee_name: text,
                allow_cross_file: true,
                receiver_call: None,
            });
        }
    }

    node_text(func_node, source).map(|text| ResolvedCall {
        callee_name: text,
        allow_cross_file: true,
        receiver_call: None,
    })
}

fn accessor_receiver_call_name(
    func_node: TsNode<'_>,
    source: &[u8],
    cfg: &LanguageConfig,
) -> Option<String> {
    let receiver = func_node
        .child_by_field_name("value")
        .or_else(|| named_children(func_node).into_iter().next())?;
    if receiver.kind() != "call_expression" {
        return None;
    }
    resolve_callee(receiver, source, cfg).map(|resolved| resolved.callee_name)
}

fn resolve_c_like_function_name(node: TsNode<'_>, source: &[u8], is_cpp: bool) -> Option<String> {
    if node.kind() == "identifier" {
        return node_text(node, source);
    }
    if is_cpp && node.kind() == "qualified_identifier"
        && let Some(name_node) = node.child_by_field_name("name") {
            return node_text(name_node, source);
        }
    if let Some(decl) = node.child_by_field_name("declarator") {
        return resolve_c_like_function_name(decl, source, is_cpp);
    }
    for child in named_children(node) {
        if child.kind() == "identifier" {
            return node_text(child, source);
        }
    }
    None
}

// ── Name resolution ───────────────────────────────────────────────────────────

fn resolve_name(node: TsNode<'_>, source: &[u8], cfg: &LanguageConfig) -> Option<String> {
    if !cfg.name_field.is_empty()
        && let Some(name_node) = node.child_by_field_name(cfg.name_field) {
            return node_text(name_node, source);
        }

    for child_type in cfg.name_fallback_child_types {
        for child in named_children(node) {
            if child.kind() == *child_type {
                return node_text(child, source);
            }
        }
    }

    // Try direct identifier or field_identifier
    for child in named_children(node) {
        if child.kind() == "identifier"
            || child.kind() == "type_identifier"
            || child.kind() == "field_identifier"
        {
            return node_text(child, source);
        }
        // C/C++: name is inside function_declarator
        if child.kind() == "function_declarator" {
            for sub in named_children(child) {
                if sub.kind() == "identifier" || sub.kind() == "field_identifier" {
                    return node_text(sub, source);
                }
            }
        }
    }

    None
}

/// Resolve the name of an arrow_function or function expression that is the
/// value of a variable_declarator, e.g. `const foo = () => {}` or
/// `const bar = function() {}`. The arrow/function node itself has no name
/// field — the name comes from the parent declarator.
fn resolve_arrow_or_func_expr_name(node: TsNode<'_>, source: &[u8]) -> Option<String> {
    if node.kind() != "arrow_function" && node.kind() != "function" {
        return None;
    }
    let parent = node.parent()?;
    if parent.kind() == "variable_declarator"
        && let Some(name_node) = parent.child_by_field_name("name") {
            return node_text(name_node, source);
        }
    None
}

fn find_body<'a>(node: TsNode<'a>, cfg: &LanguageConfig) -> Option<TsNode<'a>> {
    if !cfg.body_field.is_empty()
        && let Some(b) = node.child_by_field_name(cfg.body_field) {
            return Some(b);
        }
    for child_type in cfg.body_fallback_child_types {
        for child in named_children(node) {
            if child.kind() == *child_type {
                return Some(child);
            }
        }
    }
    None
}

fn collect_identifiers<'a>(node: TsNode<'a>, source: &'a [u8]) -> Vec<String> {
    let mut ids = Vec::new();
    collect_identifiers_inner(node, source, &mut ids);
    ids
}

fn collect_identifiers_inner(node: TsNode<'_>, source: &[u8], ids: &mut Vec<String>) {
    if (node.kind() == "identifier" || node.kind() == "type_identifier")
        && let Some(text) = node_text(node, source) {
            ids.push(text);
        }
    for child in named_children(node) {
        collect_identifiers_inner(child, source, ids);
    }
}

// ── Python rationale extraction ──────────────────────────────────────────────

fn extract_python_rationale(
    root: TsNode<'_>,
    source: &[u8],
    path: &Path,
    stem: &str,
    file_id: &str,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
) {
    let source_file = path.to_string_lossy().to_string();

    if let Some((text, line_no)) = python_docstring(root, source) {
        add_rationale_node_and_edge(
            stem,
            &source_file,
            text,
            line_no,
            file_id,
            nodes,
            edges,
            seen_ids,
        );
    }

    walk_python_docstrings(
        root,
        source,
        stem,
        file_id,
        file_id,
        &source_file,
        nodes,
        edges,
        seen_ids,
    );

    if let Ok(source_text) = std::str::from_utf8(source) {
        for (line_no, line_text) in source_text.lines().enumerate() {
            let stripped = line_text.trim();
            if PYTHON_RATIONALE_PREFIXES
                .iter()
                .any(|prefix| stripped.starts_with(prefix))
            {
                add_rationale_node_and_edge(
                    stem,
                    &source_file,
                    stripped.to_string(),
                    line_no + 1,
                    file_id,
                    nodes,
                    edges,
                    seen_ids,
                );
            }
        }
    }
}

fn walk_python_docstrings(
    node: TsNode<'_>,
    source: &[u8],
    stem: &str,
    file_id: &str,
    parent_id: &str,
    source_file: &str,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
) {
    match node.kind() {
        "class_definition" => {
            let Some(name_node) = node.child_by_field_name("name") else {
                return;
            };
            let Some(body_node) = node.child_by_field_name("body") else {
                return;
            };
            let Some(class_name) = node_text(name_node, source) else {
                return;
            };
            let class_id = make_id(&format!("{stem}_{class_name}"));
            if let Some((text, line_no)) = python_docstring(body_node, source) {
                add_rationale_node_and_edge(
                    stem,
                    source_file,
                    text,
                    line_no,
                    &class_id,
                    nodes,
                    edges,
                    seen_ids,
                );
            }
            for child in named_children(body_node) {
                walk_python_docstrings(
                    child,
                    source,
                    stem,
                    file_id,
                    &class_id,
                    source_file,
                    nodes,
                    edges,
                    seen_ids,
                );
            }
            return;
        }
        "function_definition" => {
            let Some(name_node) = node.child_by_field_name("name") else {
                return;
            };
            let Some(body_node) = node.child_by_field_name("body") else {
                return;
            };
            let Some(func_name) = node_text(name_node, source) else {
                return;
            };
            let func_id = if parent_id != file_id {
                make_id(&format!("{parent_id}_{func_name}"))
            } else {
                make_id(&format!("{stem}_{func_name}"))
            };
            if let Some((text, line_no)) = python_docstring(body_node, source) {
                add_rationale_node_and_edge(
                    stem,
                    source_file,
                    text,
                    line_no,
                    &func_id,
                    nodes,
                    edges,
                    seen_ids,
                );
            }
            return;
        }
        _ => {}
    }

    for child in named_children(node) {
        walk_python_docstrings(
            child,
            source,
            stem,
            file_id,
            parent_id,
            source_file,
            nodes,
            edges,
            seen_ids,
        );
    }
}

fn python_docstring(node: TsNode<'_>, source: &[u8]) -> Option<(String, usize)> {
    let child = named_children(node).into_iter().next()?;
    if child.kind() == "expression_statement" {
        for sub in named_children(child) {
            if matches!(sub.kind(), "string" | "concatenated_string") {
                let raw = node_text(sub, source)?;
                let text = trim_python_docstring(&raw);
                if text.chars().count() > 20 {
                    return Some((text, child.start_position().row + 1));
                }
            }
        }
    }
    None
}

fn trim_python_docstring(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_string()
}

fn add_rationale_node_and_edge(
    stem: &str,
    source_file: &str,
    text: String,
    line_no: usize,
    parent_id: &str,
    nodes: &mut Vec<Node>,
    edges: &mut Vec<Edge>,
    seen_ids: &mut HashSet<String>,
) {
    let label = text
        .replace("\r\n", " ")
        .replace(['\r', '\n'], " ")
        .trim()
        .chars()
        .take(80)
        .collect::<String>()
        .trim_end()
        .to_string();
    let rationale_id = make_id(&format!("{stem}_rationale_{line_no}"));

    add_node(
        nodes,
        seen_ids,
        Node {
            id: rationale_id.clone(),
            label,
            file_type: "rationale".to_string(),
            source_file: source_file.to_string(),
            source_location: Some(format!("L{line_no}")),
            node_type: None,
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );

    push_edge(
        edges,
        EdgeSpec {
            source: rationale_id,
            target: parent_id.to_string(),
            relation: "rationale_for".to_string(),
            source_file: source_file.to_string(),
            line_no,
        },
    );
}

// ── Utility helpers ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PendingCall {
    caller_id: String,
    callee_name: String,
    line_no: usize,
    allow_cross_file: bool,
    receiver_call: Option<String>,
}

#[derive(Debug)]
struct EdgeSpec {
    source: String,
    target: String,
    relation: String,
    source_file: String,
    line_no: usize,
}

fn push_edge(edges: &mut Vec<Edge>, spec: EdgeSpec) {
    edges.push(Edge {
        source: spec.source,
        target: spec.target,
        relation: spec.relation,
        confidence: "EXTRACTED".to_string(),
        source_file: spec.source_file,
        original_source: None,
        original_target: None,
        source_location: Some(format!("L{}", spec.line_no)),
        confidence_score: Some(1.0),
        confidence_score_present: true,
        weight: 1.0,
        extra: Default::default(),
    });
}

fn add_node(nodes: &mut Vec<Node>, seen_ids: &mut HashSet<String>, node: Node) {
    if seen_ids.insert(node.id.clone()) {
        nodes.push(node);
    }
}

fn named_children(node: TsNode<'_>) -> Vec<TsNode<'_>> {
    let mut children = Vec::new();
    for idx in 0..node.named_child_count() {
        if let Some(child) = node.named_child(idx) {
            children.push(child);
        }
    }
    children
}

fn node_text(node: TsNode<'_>, source: &[u8]) -> Option<String> {
    node.utf8_text(source).ok().map(|text| text.to_string())
}

fn build_label_index(nodes: &[Node]) -> HashMap<String, String> {
    let mut label_to_id = HashMap::new();
    for node in nodes {
        let normalized = normalize_lookup_name(&node.label);
        if normalized.is_empty() {
            continue;
        }
        label_to_id.insert(normalized, node.id.clone());
    }
    label_to_id
}

fn build_function_return_index(function_returns: &[FunctionReturn]) -> HashMap<String, String> {
    let mut index = HashMap::new();
    let mut ambiguous = HashSet::new();
    for function_return in function_returns {
        let function_name = normalize_lookup_name(&function_return.function_name);
        let return_type_name = normalize_lookup_name(&function_return.return_type_name);
        if function_name.is_empty() || return_type_name.is_empty() {
            continue;
        }
        insert_unique_lookup(&mut index, &mut ambiguous, function_name, return_type_name);
    }
    index
}

fn build_method_index(nodes: &[Node], edges: &[Edge]) -> HashMap<String, String> {
    let nodes_by_id: HashMap<&str, &Node> =
        nodes.iter().map(|node| (node.id.as_str(), node)).collect();
    let mut index = HashMap::new();
    let mut ambiguous = HashSet::new();
    for edge in edges {
        if edge.relation != "method" {
            continue;
        }
        let Some(owner) = nodes_by_id.get(edge.source.as_str()) else {
            continue;
        };
        let Some(method) = nodes_by_id.get(edge.target.as_str()) else {
            continue;
        };
        let owner_name = normalize_lookup_name(&owner.label);
        let method_name = normalize_lookup_name(&method.label);
        if owner_name.is_empty() || method_name.is_empty() {
            continue;
        }
        insert_unique_lookup(
            &mut index,
            &mut ambiguous,
            method_lookup_key(&owner_name, &method_name),
            method.id.clone(),
        );
    }
    index
}

fn lookup_receiver_method_target<'a>(
    method_index: &'a HashMap<String, String>,
    function_return_index: &HashMap<String, String>,
    raw_call: &RawCall,
) -> Option<&'a String> {
    let receiver_call = raw_call.receiver_call.as_ref()?;
    let receiver_name = normalize_lookup_name(receiver_call);
    let method_name = normalize_lookup_name(&raw_call.callee);
    let return_type = function_return_index.get(&receiver_name)?;
    method_index.get(&method_lookup_key(return_type, &method_name))
}

fn method_lookup_key(owner_name: &str, method_name: &str) -> String {
    format!("{owner_name}::{method_name}")
}

fn insert_unique_lookup(
    index: &mut HashMap<String, String>,
    ambiguous: &mut HashSet<String>,
    key: String,
    value: String,
) {
    if ambiguous.contains(&key) {
        return;
    }
    match index.get(&key) {
        None => {
            index.insert(key, value);
        }
        Some(existing) if existing == &value => {}
        Some(_) => {
            index.remove(&key);
            ambiguous.insert(key);
        }
    }
}

fn normalize_lookup_name(value: &str) -> String {
    value
        .trim()
        .trim_end_matches('!')
        .trim_matches(|ch| ch == '(' || ch == ')')
        .trim_start_matches('.')
        .to_lowercase()
}

fn call_lookup_keys(callee: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let trimmed = callee.trim();
    let mut push = |candidate: &str| {
        let normalized = normalize_lookup_name(candidate);
        if !normalized.is_empty() && !keys.contains(&normalized) {
            keys.push(normalized);
        }
    };

    push(trimmed);
    for separator in ["::", "->", ".", "/"] {
        if let Some((_, tail)) = trimmed.rsplit_once(separator) {
            push(tail);
        }
    }
    keys
}

fn lookup_call_target<'a>(
    label_to_id: &'a HashMap<String, String>,
    callee: &str,
) -> Option<&'a String> {
    call_lookup_keys(callee)
        .into_iter()
        .find_map(|key| label_to_id.get(&key))
}

fn make_id(value: &str) -> String {
    let mut out = String::new();
    let mut last_was_sep = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep {
            out.push('_');
            last_was_sep = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn all_children(node: TsNode<'_>) -> Vec<TsNode<'_>> {
    let mut children = Vec::new();
    for idx in 0..node.child_count() {
        if let Some(child) = node.child(idx) {
            children.push(child);
        }
    }
    children
}

fn add_code_node(
    nodes: &mut Vec<Node>,
    seen_ids: &mut HashSet<String>,
    id: String,
    label: String,
    source_file: &str,
    line_no: usize,
) {
    add_node(
        nodes,
        seen_ids,
        Node {
            id,
            label,
            file_type: "code".to_string(),
            source_file: source_file.to_string(),
            source_location: Some(format!("L{line_no}")),
            node_type: None,
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );
}

fn push_edge_with_confidence(
    edges: &mut Vec<Edge>,
    source: String,
    target: String,
    relation: String,
    source_file: &str,
    line_no: usize,
    confidence: &str,
    weight: f64,
) {
    edges.push(Edge {
        source,
        target,
        relation,
        confidence: confidence.to_string(),
        source_file: source_file.to_string(),
        original_source: None,
        original_target: None,
        source_location: Some(format!("L{line_no}")),
        confidence_score: Some(1.0),
        confidence_score_present: true,
        weight,
        extra: Default::default(),
    });
}

fn edge_signature(edge: &Edge) -> (String, String, String, Option<String>) {
    (
        edge.source.clone(),
        edge.target.clone(),
        edge.relation.clone(),
        edge.source_location.clone(),
    )
}

fn php_basename(raw: &str) -> &str {
    raw.rsplit('\\').next().unwrap_or(raw).trim()
}

fn line_no_from_offset(text: &str, start_line: usize, offset: usize) -> usize {
    start_line
        + text.as_bytes()[..offset]
            .iter()
            .filter(|&&b| b == b'\n')
            .count()
}

fn extract_csharp_file(path: &Path) -> Result<Extraction> {
    let mut extraction = extract_generic(path, &csharp_cfg())?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_c_sharp::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();
    let source_file = path.to_string_lossy().to_string();
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let file_id = make_id(&source_file);
    let mut seen_ids: HashSet<String> = extraction
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect();
    let mut seen_edges: HashSet<(String, String, String, Option<String>)> =
        extraction.edges.iter().map(edge_signature).collect();

    fn walk(
        node: TsNode<'_>,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
    ) {
        if node.kind() == "namespace_declaration"
            && let Some(name) = node
                .child_by_field_name("name")
                .and_then(|child| node_text(child, source))
            {
                let line_no = node.start_position().row + 1;
                let namespace_id = make_id(&format!("{stem}_{name}"));
                add_code_node(
                    nodes,
                    seen_ids,
                    namespace_id.clone(),
                    name,
                    source_file,
                    line_no,
                );
                let edge = Edge {
                    source: file_id.to_string(),
                    target: namespace_id,
                    relation: "contains".to_string(),
                    confidence: "EXTRACTED".to_string(),
                    source_file: source_file.to_string(),
                    original_source: None,
                    original_target: None,
                    source_location: Some(format!("L{line_no}")),
                    confidence_score: Some(1.0),
                    confidence_score_present: true,
                    weight: 1.0,
                    extra: Default::default(),
                };
                if seen_edges.insert(edge_signature(&edge)) {
                    edges.push(edge);
                }
            }

        for child in named_children(node) {
            walk(
                child,
                stem,
                file_id,
                source,
                source_file,
                nodes,
                edges,
                seen_ids,
                seen_edges,
            );
        }
    }

    walk(
        root,
        stem,
        &file_id,
        &source,
        &source_file,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut seen_edges,
    );

    Ok(extraction)
}

fn extract_julia_file(path: &Path) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_julia::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let (mut extraction, mut seen_ids, file_id, source_file) = new_file_extraction(path);
    let mut function_bodies: Vec<(String, TsNode<'_>)> = Vec::new();

    fn func_name_from_signature(node: TsNode<'_>, source: &[u8]) -> Option<String> {
        for child in all_children(node) {
            if child.kind() != "call_expression" {
                continue;
            }
            if let Some(callee) = child.child(0)
                && callee.kind() == "identifier" {
                    return node_text(callee, source);
                }
        }
        None
    }

    fn walk_calls(
        node: TsNode<'_>,
        func_id: &str,
        stem: &str,
        source: &[u8],
        edges: &mut Vec<Edge>,
        source_file: &str,
    ) {
        match node.kind() {
            "function_definition" | "short_function_definition" => return,
            "call_expression" => {
                if let Some(callee) = node.child(0) {
                    let target = match callee.kind() {
                        "identifier" => {
                            node_text(callee, source).map(|name| make_id(&format!("{stem}_{name}")))
                        }
                        "field_expression" => callee
                            .child(callee.child_count().saturating_sub(1))
                            .and_then(|child| node_text(child, source))
                            .map(|name| make_id(&format!("{stem}_{name}"))),
                        _ => None,
                    };
                    if let Some(target_id) = target {
                        push_edge_with_confidence(
                            edges,
                            func_id.to_string(),
                            target_id,
                            "calls".to_string(),
                            source_file,
                            node.start_position().row + 1,
                            "EXTRACTED",
                            1.0,
                        );
                    }
                }
            }
            _ => {}
        }

        for child in all_children(node) {
            walk_calls(child, func_id, stem, source, edges, source_file);
        }
    }

    fn walk<'a>(
        node: TsNode<'a>,
        scope_id: &str,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        function_bodies: &mut Vec<(String, TsNode<'a>)>,
    ) {
        match node.kind() {
            "module_definition" => {
                let name = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "identifier")
                    .and_then(|child| node_text(child, source));
                if let Some(name) = name {
                    let module_id = make_id(&format!("{stem}_{name}"));
                    let line_no = node.start_position().row + 1;
                    add_code_node(
                        nodes,
                        seen_ids,
                        module_id.clone(),
                        name,
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: module_id.clone(),
                            relation: "defines".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    for child in all_children(node) {
                        walk(
                            child,
                            &module_id,
                            stem,
                            file_id,
                            source,
                            source_file,
                            nodes,
                            edges,
                            seen_ids,
                            function_bodies,
                        );
                    }
                }
                return;
            }
            "struct_definition" => {
                let type_head = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "type_head");
                if let Some(type_head) = type_head {
                    let line_no = node.start_position().row + 1;
                    if let Some(binary_expr) = all_children(type_head)
                        .into_iter()
                        .find(|child| child.kind() == "binary_expression")
                    {
                        let identifiers: Vec<TsNode<'_>> = all_children(binary_expr)
                            .into_iter()
                            .filter(|child| child.kind() == "identifier")
                            .collect();
                        if let Some(first) = identifiers
                            .first()
                            .and_then(|child| node_text(*child, source))
                        {
                            let struct_id = make_id(&format!("{stem}_{first}"));
                            add_code_node(
                                nodes,
                                seen_ids,
                                struct_id.clone(),
                                first,
                                source_file,
                                line_no,
                            );
                            push_edge(
                                edges,
                                EdgeSpec {
                                    source: scope_id.to_string(),
                                    target: struct_id.clone(),
                                    relation: "defines".to_string(),
                                    source_file: source_file.to_string(),
                                    line_no,
                                },
                            );
                            if let Some(last) = identifiers
                                .last()
                                .and_then(|child| node_text(*child, source))
                                && identifiers.len() >= 2 {
                                    push_edge(
                                        edges,
                                        EdgeSpec {
                                            source: struct_id,
                                            target: make_id(&format!("{stem}_{last}")),
                                            relation: "inherits".to_string(),
                                            source_file: source_file.to_string(),
                                            line_no,
                                        },
                                    );
                                }
                        }
                    } else if let Some(name) = all_children(type_head)
                        .into_iter()
                        .find(|child| child.kind() == "identifier")
                        .and_then(|child| node_text(child, source))
                    {
                        let struct_id = make_id(&format!("{stem}_{name}"));
                        add_code_node(
                            nodes,
                            seen_ids,
                            struct_id.clone(),
                            name,
                            source_file,
                            line_no,
                        );
                        push_edge(
                            edges,
                            EdgeSpec {
                                source: scope_id.to_string(),
                                target: struct_id,
                                relation: "defines".to_string(),
                                source_file: source_file.to_string(),
                                line_no,
                            },
                        );
                    }
                }
                return;
            }
            "abstract_definition" => {
                if let Some(type_head) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "type_head")
                    && let Some(name) = all_children(type_head)
                        .into_iter()
                        .find(|child| child.kind() == "identifier")
                        .and_then(|child| node_text(child, source))
                    {
                        let abs_id = make_id(&format!("{stem}_{name}"));
                        let line_no = node.start_position().row + 1;
                        add_code_node(nodes, seen_ids, abs_id.clone(), name, source_file, line_no);
                        push_edge(
                            edges,
                            EdgeSpec {
                                source: scope_id.to_string(),
                                target: abs_id,
                                relation: "defines".to_string(),
                                source_file: source_file.to_string(),
                                line_no,
                            },
                        );
                    }
                return;
            }
            "function_definition" => {
                if let Some(signature) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "signature")
                    && let Some(func_name) = func_name_from_signature(signature, source) {
                        let func_id = make_id(&format!("{stem}_{func_name}"));
                        let line_no = node.start_position().row + 1;
                        add_code_node(
                            nodes,
                            seen_ids,
                            func_id.clone(),
                            format!("{func_name}()"),
                            source_file,
                            line_no,
                        );
                        push_edge(
                            edges,
                            EdgeSpec {
                                source: scope_id.to_string(),
                                target: func_id.clone(),
                                relation: "defines".to_string(),
                                source_file: source_file.to_string(),
                                line_no,
                            },
                        );
                        function_bodies.push((func_id, node));
                    }
                return;
            }
            "assignment" => {
                if let Some(lhs) = node.child(0)
                    && lhs.kind() == "call_expression"
                        && let Some(callee) = lhs.child(0)
                            && callee.kind() == "identifier"
                                && let Some(func_name) = node_text(callee, source) {
                                    let func_id = make_id(&format!("{stem}_{func_name}"));
                                    let line_no = node.start_position().row + 1;
                                    add_code_node(
                                        nodes,
                                        seen_ids,
                                        func_id.clone(),
                                        format!("{func_name}()"),
                                        source_file,
                                        line_no,
                                    );
                                    push_edge(
                                        edges,
                                        EdgeSpec {
                                            source: scope_id.to_string(),
                                            target: func_id.clone(),
                                            relation: "defines".to_string(),
                                            source_file: source_file.to_string(),
                                            line_no,
                                        },
                                    );
                                    if let Some(rhs) =
                                        node.child(node.child_count().saturating_sub(1))
                                    {
                                        function_bodies.push((func_id, rhs));
                                    }
                                }
                return;
            }
            "using_statement" | "import_statement" => {
                let line_no = node.start_position().row + 1;
                for child in all_children(node) {
                    match child.kind() {
                        "identifier" => {
                            if let Some(name) = node_text(child, source) {
                                let import_id = make_id(&name);
                                add_code_node(
                                    nodes,
                                    seen_ids,
                                    import_id.clone(),
                                    name,
                                    source_file,
                                    line_no,
                                );
                                push_edge(
                                    edges,
                                    EdgeSpec {
                                        source: scope_id.to_string(),
                                        target: import_id,
                                        relation: "imports".to_string(),
                                        source_file: source_file.to_string(),
                                        line_no,
                                    },
                                );
                            }
                        }
                        "selected_import" => {
                            let identifiers: Vec<TsNode<'_>> = all_children(child)
                                .into_iter()
                                .filter(|sub| sub.kind() == "identifier")
                                .collect();
                            if let Some(name) =
                                identifiers.first().and_then(|sub| node_text(*sub, source))
                            {
                                let import_id = make_id(&name);
                                add_code_node(
                                    nodes,
                                    seen_ids,
                                    import_id.clone(),
                                    name,
                                    source_file,
                                    line_no,
                                );
                                push_edge(
                                    edges,
                                    EdgeSpec {
                                        source: scope_id.to_string(),
                                        target: import_id,
                                        relation: "imports".to_string(),
                                        source_file: source_file.to_string(),
                                        line_no,
                                    },
                                );
                            }
                        }
                        _ => {}
                    }
                }
                return;
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                scope_id,
                stem,
                file_id,
                source,
                source_file,
                nodes,
                edges,
                seen_ids,
                function_bodies,
            );
        }
    }

    walk(
        root,
        &file_id,
        stem,
        &file_id,
        &source,
        &source_file,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut function_bodies,
    );

    for (func_id, body_node) in function_bodies {
        if body_node.kind() == "function_definition" {
            for child in all_children(body_node) {
                if child.kind() != "signature" {
                    walk_calls(
                        child,
                        &func_id,
                        stem,
                        &source,
                        &mut extraction.edges,
                        &source_file,
                    );
                }
            }
        } else {
            walk_calls(
                body_node,
                &func_id,
                stem,
                &source,
                &mut extraction.edges,
                &source_file,
            );
        }
    }

    Ok(extraction)
}

fn extract_zig_file(path: &Path) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_zig::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let (mut extraction, mut seen_ids, file_id, source_file) = new_file_extraction(path);
    let mut function_bodies: Vec<(String, TsNode<'_>)> = Vec::new();

    fn extract_import(
        node: TsNode<'_>,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        edges: &mut Vec<Edge>,
    ) {
        for child in all_children(node) {
            match child.kind() {
                "builtin_function" => {
                    let mut builtin = None;
                    let mut arguments = None;
                    for sub in all_children(child) {
                        match sub.kind() {
                            "builtin_identifier" => builtin = node_text(sub, source),
                            "arguments" => arguments = Some(sub),
                            _ => {}
                        }
                    }
                    if matches!(builtin.as_deref(), Some("@import" | "@cImport"))
                        && let Some(arguments) = arguments {
                            for arg in all_children(arguments) {
                                if !matches!(arg.kind(), "string_literal" | "string") {
                                    continue;
                                }
                                if let Some(raw) = node_text(arg, source) {
                                    let module = raw
                                        .trim_matches('"')
                                        .split('/')
                                        .next_back()
                                        .unwrap_or(raw.as_str())
                                        .split('.')
                                        .next()
                                        .unwrap_or(raw.as_str());
                                    if !module.is_empty() {
                                        push_edge(
                                            edges,
                                            EdgeSpec {
                                                source: file_id.to_string(),
                                                target: make_id(module),
                                                relation: "imports_from".to_string(),
                                                source_file: source_file.to_string(),
                                                line_no: node.start_position().row + 1,
                                            },
                                        );
                                    }
                                    return;
                                }
                            }
                        }
                }
                "field_expression" => {
                    extract_import(child, file_id, source, source_file, edges);
                    return;
                }
                _ => {}
            }
        }
    }

    fn walk_calls(
        node: TsNode<'_>,
        caller_id: &str,
        source: &[u8],
        nodes: &[Node],
        edges: &mut Vec<Edge>,
        source_file: &str,
        seen_pairs: &mut HashSet<(String, String)>,
    ) {
        if node.kind() == "function_declaration" {
            return;
        }
        if node.kind() == "call_expression"
            && let Some(function_node) = node.child_by_field_name("function")
                && let Some(callee) = node_text(function_node, source) {
                    let callee = callee.split('.').next_back().unwrap_or(callee.as_str());
                    let target = nodes.iter().find_map(|node| {
                        (node.label == format!("{callee}()")
                            || node.label == format!(".{callee}()"))
                        .then(|| node.id.clone())
                    });
                    if let Some(target_id) = target {
                        let pair = (caller_id.to_string(), target_id.clone());
                        if caller_id != target_id && seen_pairs.insert(pair.clone()) {
                            push_edge_with_confidence(
                                edges,
                                pair.0,
                                pair.1,
                                "calls".to_string(),
                                source_file,
                                node.start_position().row + 1,
                                "EXTRACTED",
                                1.0,
                            );
                        }
                    }
                }

        for child in all_children(node) {
            walk_calls(
                child,
                caller_id,
                source,
                nodes,
                edges,
                source_file,
                seen_pairs,
            );
        }
    }

    fn walk<'a>(
        node: TsNode<'a>,
        parent_struct_id: Option<&str>,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        function_bodies: &mut Vec<(String, TsNode<'a>)>,
    ) {
        match node.kind() {
            "function_declaration" => {
                if let Some(name) = node
                    .child_by_field_name("name")
                    .and_then(|child| node_text(child, source))
                {
                    let line_no = node.start_position().row + 1;
                    let (func_id, label, relation, source_id) =
                        if let Some(parent) = parent_struct_id {
                            (
                                make_id(&format!("{parent}_{name}")),
                                format!(".{name}()"),
                                "method".to_string(),
                                parent.to_string(),
                            )
                        } else {
                            (
                                make_id(&format!("{stem}_{name}")),
                                format!("{name}()"),
                                "contains".to_string(),
                                file_id.to_string(),
                            )
                        };
                    add_code_node(
                        nodes,
                        seen_ids,
                        func_id.clone(),
                        label,
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: source_id,
                            target: func_id.clone(),
                            relation,
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    if let Some(body) = node.child_by_field_name("body") {
                        function_bodies.push((func_id, body));
                    }
                }
                return;
            }
            "variable_declaration" => {
                let mut name_node = None;
                let mut value_node = None;
                for child in all_children(node) {
                    match child.kind() {
                        "identifier" => name_node = Some(child),
                        "struct_declaration" | "enum_declaration" | "union_declaration"
                        | "builtin_function" | "field_expression" => value_node = Some(child),
                        _ => {}
                    }
                }

                if let Some(value_node) = value_node {
                    match value_node.kind() {
                        "struct_declaration" => {
                            if let Some(name) = name_node.and_then(|child| node_text(child, source))
                            {
                                let line_no = node.start_position().row + 1;
                                let struct_id = make_id(&format!("{stem}_{name}"));
                                add_code_node(
                                    nodes,
                                    seen_ids,
                                    struct_id.clone(),
                                    name,
                                    source_file,
                                    line_no,
                                );
                                push_edge(
                                    edges,
                                    EdgeSpec {
                                        source: file_id.to_string(),
                                        target: struct_id.clone(),
                                        relation: "contains".to_string(),
                                        source_file: source_file.to_string(),
                                        line_no,
                                    },
                                );
                                for child in all_children(value_node) {
                                    walk(
                                        child,
                                        Some(&struct_id),
                                        stem,
                                        file_id,
                                        source,
                                        source_file,
                                        nodes,
                                        edges,
                                        seen_ids,
                                        function_bodies,
                                    );
                                }
                            }
                            return;
                        }
                        "enum_declaration" | "union_declaration" => {
                            if let Some(name) = name_node.and_then(|child| node_text(child, source))
                            {
                                let line_no = node.start_position().row + 1;
                                let type_id = make_id(&format!("{stem}_{name}"));
                                add_code_node(
                                    nodes,
                                    seen_ids,
                                    type_id.clone(),
                                    name,
                                    source_file,
                                    line_no,
                                );
                                push_edge(
                                    edges,
                                    EdgeSpec {
                                        source: file_id.to_string(),
                                        target: type_id,
                                        relation: "contains".to_string(),
                                        source_file: source_file.to_string(),
                                        line_no,
                                    },
                                );
                            }
                            return;
                        }
                        "builtin_function" | "field_expression" => {
                            extract_import(node, file_id, source, source_file, edges);
                            return;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                parent_struct_id,
                stem,
                file_id,
                source,
                source_file,
                nodes,
                edges,
                seen_ids,
                function_bodies,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &file_id,
        &source,
        &source_file,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut function_bodies,
    );

    let mut seen_pairs = HashSet::new();
    let nodes_snapshot = extraction.nodes.clone();
    for (caller_id, body) in function_bodies {
        walk_calls(
            body,
            &caller_id,
            &source,
            &nodes_snapshot,
            &mut extraction.edges,
            &source_file,
            &mut seen_pairs,
        );
    }

    extraction.edges.retain(|edge| {
        seen_ids.contains(&edge.source)
            && (seen_ids.contains(&edge.target) || edge.relation == "imports_from")
    });
    Ok(extraction)
}

fn extract_powershell_file(path: &Path) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_powershell::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let (mut extraction, mut seen_ids, file_id, source_file) = new_file_extraction(path);
    let mut function_bodies: Vec<(String, TsNode<'_>)> = Vec::new();
    let skip_keywords: HashSet<&str> = HashSet::from([
        "using", "return", "if", "else", "elseif", "foreach", "for", "while", "do", "switch",
        "try", "catch", "finally", "throw", "break", "continue", "exit", "param", "begin",
        "process", "end",
    ]);

    fn find_script_block_body(node: TsNode<'_>) -> Option<TsNode<'_>> {
        for child in all_children(node) {
            if child.kind() == "script_block" {
                for sub in all_children(child) {
                    if sub.kind() == "script_block_body" {
                        return Some(sub);
                    }
                }
                return Some(child);
            }
        }
        None
    }

    fn walk_calls(
        node: TsNode<'_>,
        caller_id: &str,
        source: &[u8],
        label_to_id: &HashMap<String, String>,
        skip_keywords: &HashSet<&str>,
        edges: &mut Vec<Edge>,
        source_file: &str,
        seen_pairs: &mut HashSet<(String, String)>,
    ) {
        if matches!(node.kind(), "function_statement" | "class_statement") {
            return;
        }
        if node.kind() == "command" {
            let command_name = all_children(node)
                .into_iter()
                .find(|child| child.kind() == "command_name")
                .and_then(|child| node_text(child, source));
            if let Some(command_name) = command_name {
                let lowered = command_name.to_lowercase();
                if !skip_keywords.contains(lowered.as_str())
                    && let Some(target_id) = label_to_id.get(&lowered) {
                        let pair = (caller_id.to_string(), target_id.clone());
                        if caller_id != target_id && seen_pairs.insert(pair.clone()) {
                            push_edge_with_confidence(
                                edges,
                                pair.0,
                                pair.1,
                                "calls".to_string(),
                                source_file,
                                node.start_position().row + 1,
                                "EXTRACTED",
                                1.0,
                            );
                        }
                    }
            }
        }

        for child in all_children(node) {
            walk_calls(
                child,
                caller_id,
                source,
                label_to_id,
                skip_keywords,
                edges,
                source_file,
                seen_pairs,
            );
        }
    }

    fn walk<'a>(
        node: TsNode<'a>,
        parent_class_id: Option<&str>,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        function_bodies: &mut Vec<(String, TsNode<'a>)>,
    ) {
        match node.kind() {
            "function_statement" => {
                if let Some(name) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "function_name")
                    .and_then(|child| node_text(child, source))
                {
                    let line_no = node.start_position().row + 1;
                    let func_id = make_id(&format!("{stem}_{name}"));
                    add_code_node(
                        nodes,
                        seen_ids,
                        func_id.clone(),
                        format!("{name}()"),
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: func_id.clone(),
                            relation: "contains".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    if let Some(body) = find_script_block_body(node) {
                        function_bodies.push((func_id, body));
                    }
                }
                return;
            }
            "class_statement" => {
                if let Some(name) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "simple_name")
                    .and_then(|child| node_text(child, source))
                {
                    let line_no = node.start_position().row + 1;
                    let class_id = make_id(&format!("{stem}_{name}"));
                    add_code_node(
                        nodes,
                        seen_ids,
                        class_id.clone(),
                        name,
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: class_id.clone(),
                            relation: "contains".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    for child in all_children(node) {
                        walk(
                            child,
                            Some(&class_id),
                            stem,
                            file_id,
                            source,
                            source_file,
                            nodes,
                            edges,
                            seen_ids,
                            function_bodies,
                        );
                    }
                }
                return;
            }
            "class_method_definition" => {
                if let Some(name) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "simple_name")
                    .and_then(|child| node_text(child, source))
                {
                    let line_no = node.start_position().row + 1;
                    let (method_id, label, relation, source_id) =
                        if let Some(parent) = parent_class_id {
                            (
                                make_id(&format!("{parent}_{name}")),
                                format!(".{name}()"),
                                "method".to_string(),
                                parent.to_string(),
                            )
                        } else {
                            (
                                make_id(&format!("{stem}_{name}")),
                                format!("{name}()"),
                                "contains".to_string(),
                                file_id.to_string(),
                            )
                        };
                    add_code_node(
                        nodes,
                        seen_ids,
                        method_id.clone(),
                        label,
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: source_id,
                            target: method_id.clone(),
                            relation,
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    if let Some(body) = find_script_block_body(node) {
                        function_bodies.push((method_id, body));
                    }
                }
                return;
            }
            "command" => {
                let command_name = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "command_name")
                    .and_then(|child| node_text(child, source));
                if command_name.as_deref() == Some("using") {
                    let mut tokens = Vec::new();
                    for child in all_children(node) {
                        if child.kind() != "command_elements" {
                            continue;
                        }
                        for element in all_children(child) {
                            if element.kind() == "generic_token"
                                && let Some(token) = node_text(element, source) {
                                    tokens.push(token);
                                }
                        }
                    }
                    let module_tokens: Vec<String> = tokens
                        .into_iter()
                        .filter(|token| {
                            !matches!(
                                token.to_lowercase().as_str(),
                                "namespace" | "module" | "assembly"
                            )
                        })
                        .collect();
                    if let Some(module) = module_tokens.last() {
                        let module = module.split('.').next_back().unwrap_or(module.as_str());
                        push_edge(
                            edges,
                            EdgeSpec {
                                source: file_id.to_string(),
                                target: make_id(module),
                                relation: "imports_from".to_string(),
                                source_file: source_file.to_string(),
                                line_no: node.start_position().row + 1,
                            },
                        );
                    }
                }
                return;
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                parent_class_id,
                stem,
                file_id,
                source,
                source_file,
                nodes,
                edges,
                seen_ids,
                function_bodies,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &file_id,
        &source,
        &source_file,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut function_bodies,
    );

    let label_to_id = build_label_index(&extraction.nodes);
    let mut seen_pairs = HashSet::new();
    for (caller_id, body) in function_bodies {
        walk_calls(
            body,
            &caller_id,
            &source,
            &label_to_id,
            &skip_keywords,
            &mut extraction.edges,
            &source_file,
            &mut seen_pairs,
        );
    }

    extraction.edges.retain(|edge| {
        seen_ids.contains(&edge.source)
            && (seen_ids.contains(&edge.target) || edge.relation == "imports_from")
    });
    Ok(extraction)
}

fn extract_objc_file(path: &Path) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_objc::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let (mut extraction, mut seen_ids, file_id, source_file) = new_file_extraction(path);
    let mut method_bodies: Vec<(String, TsNode<'_>)> = Vec::new();

    fn walk_calls(
        node: TsNode<'_>,
        caller_id: &str,
        source: &[u8],
        source_file: &str,
        method_ids: &HashSet<String>,
        edges: &mut Vec<Edge>,
        seen_calls: &mut HashSet<(String, String)>,
    ) {
        if node.kind() == "message_expression" {
            for child in all_children(node) {
                if !matches!(child.kind(), "selector" | "keyword_argument_list") {
                    continue;
                }
                let mut selector_parts = Vec::new();
                if child.kind() == "selector" {
                    if let Some(selector) = node_text(child, source) {
                        selector_parts.push(selector);
                    }
                } else {
                    for sub in all_children(child) {
                        if sub.kind() != "keyword_argument" {
                            continue;
                        }
                        for selector in all_children(sub) {
                            if selector.kind() == "selector"
                                && let Some(part) = node_text(selector, source) {
                                    selector_parts.push(part);
                                }
                        }
                    }
                }
                let method_name = selector_parts.join("");
                if method_name.is_empty() {
                    continue;
                }
                let suffix = make_id(&method_name);
                for candidate in method_ids {
                    if candidate.ends_with(suffix.trim_start_matches('_')) {
                        let pair = (caller_id.to_string(), candidate.clone());
                        if caller_id != candidate && seen_calls.insert(pair.clone()) {
                            push_edge_with_confidence(
                                edges,
                                pair.0,
                                pair.1,
                                "calls".to_string(),
                                source_file,
                                node.start_position().row + 1,
                                "EXTRACTED",
                                1.0,
                            );
                        }
                    }
                }
            }
        }

        for child in all_children(node) {
            walk_calls(
                child,
                caller_id,
                source,
                source_file,
                method_ids,
                edges,
                seen_calls,
            );
        }
    }

    fn walk<'a>(
        node: TsNode<'a>,
        parent_id: Option<&str>,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        method_bodies: &mut Vec<(String, TsNode<'a>)>,
    ) {
        let line_no = node.start_position().row + 1;
        match node.kind() {
            "preproc_include" => {
                for child in all_children(node) {
                    match child.kind() {
                        "system_lib_string" => {
                            if let Some(raw) = node_text(child, source) {
                                let module = raw
                                    .trim_matches('<')
                                    .trim_matches('>')
                                    .split('/')
                                    .next_back()
                                    .unwrap_or(raw.as_str())
                                    .replace(".h", "");
                                if !module.is_empty() {
                                    push_edge(
                                        edges,
                                        EdgeSpec {
                                            source: file_id.to_string(),
                                            target: make_id(&module),
                                            relation: "imports".to_string(),
                                            source_file: source_file.to_string(),
                                            line_no,
                                        },
                                    );
                                }
                            }
                        }
                        "string_literal" => {
                            for sub in all_children(child) {
                                if sub.kind() != "string_content" {
                                    continue;
                                }
                                if let Some(raw) = node_text(sub, source) {
                                    let module = raw
                                        .split('/')
                                        .next_back()
                                        .unwrap_or(raw.as_str())
                                        .replace(".h", "");
                                    if !module.is_empty() {
                                        push_edge(
                                            edges,
                                            EdgeSpec {
                                                source: file_id.to_string(),
                                                target: make_id(&module),
                                                relation: "imports".to_string(),
                                                source_file: source_file.to_string(),
                                                line_no,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                return;
            }
            "class_interface" => {
                let identifiers: Vec<TsNode<'_>> = all_children(node)
                    .into_iter()
                    .filter(|child| child.kind() == "identifier")
                    .collect();
                if identifiers.is_empty() {
                    for child in all_children(node) {
                        walk(
                            child,
                            parent_id,
                            stem,
                            file_id,
                            source,
                            source_file,
                            nodes,
                            edges,
                            seen_ids,
                            method_bodies,
                        );
                    }
                    return;
                }
                let Some(name) = node_text(identifiers[0], source) else {
                    return;
                };
                let class_id = make_id(&format!("{stem}_{name}"));
                add_code_node(
                    nodes,
                    seen_ids,
                    class_id.clone(),
                    name,
                    source_file,
                    line_no,
                );
                push_edge(
                    edges,
                    EdgeSpec {
                        source: file_id.to_string(),
                        target: class_id.clone(),
                        relation: "contains".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
                let mut colon_seen = false;
                for child in all_children(node) {
                    match child.kind() {
                        ":" => colon_seen = true,
                        "identifier" if colon_seen => {
                            if let Some(super_name) = node_text(child, source) {
                                push_edge(
                                    edges,
                                    EdgeSpec {
                                        source: class_id.clone(),
                                        target: make_id(&super_name),
                                        relation: "inherits".to_string(),
                                        source_file: source_file.to_string(),
                                        line_no,
                                    },
                                );
                            }
                            colon_seen = false;
                        }
                        "parameterized_arguments" => {
                            for sub in all_children(child) {
                                if sub.kind() != "type_name" {
                                    continue;
                                }
                                for ty in all_children(sub) {
                                    if ty.kind() == "type_identifier"
                                        && let Some(proto_name) = node_text(ty, source) {
                                            push_edge(
                                                edges,
                                                EdgeSpec {
                                                    source: class_id.clone(),
                                                    target: make_id(&proto_name),
                                                    relation: "imports".to_string(),
                                                    source_file: source_file.to_string(),
                                                    line_no,
                                                },
                                            );
                                        }
                                }
                            }
                        }
                        "method_declaration" => {
                            walk(
                                child,
                                Some(&class_id),
                                stem,
                                file_id,
                                source,
                                source_file,
                                nodes,
                                edges,
                                seen_ids,
                                method_bodies,
                            );
                        }
                        _ => {}
                    }
                }
                return;
            }
            "class_implementation" => {
                let name = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "identifier")
                    .and_then(|child| node_text(child, source));
                let Some(name) = name else {
                    for child in all_children(node) {
                        walk(
                            child,
                            parent_id,
                            stem,
                            file_id,
                            source,
                            source_file,
                            nodes,
                            edges,
                            seen_ids,
                            method_bodies,
                        );
                    }
                    return;
                };
                let impl_id = make_id(&format!("{stem}_{name}"));
                if !seen_ids.contains(&impl_id) {
                    add_code_node(nodes, seen_ids, impl_id.clone(), name, source_file, line_no);
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: impl_id.clone(),
                            relation: "contains".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                }
                for child in all_children(node) {
                    if child.kind() == "implementation_definition" {
                        for sub in all_children(child) {
                            walk(
                                sub,
                                Some(&impl_id),
                                stem,
                                file_id,
                                source,
                                source_file,
                                nodes,
                                edges,
                                seen_ids,
                                method_bodies,
                            );
                        }
                    }
                }
                return;
            }
            "protocol_declaration" => {
                if let Some(name) = all_children(node)
                    .into_iter()
                    .find(|child| child.kind() == "identifier")
                    .and_then(|child| node_text(child, source))
                {
                    let proto_id = make_id(&format!("{stem}_{name}"));
                    add_code_node(
                        nodes,
                        seen_ids,
                        proto_id.clone(),
                        format!("<{name}>"),
                        source_file,
                        line_no,
                    );
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: proto_id.clone(),
                            relation: "contains".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                    for child in all_children(node) {
                        walk(
                            child,
                            Some(&proto_id),
                            stem,
                            file_id,
                            source,
                            source_file,
                            nodes,
                            edges,
                            seen_ids,
                            method_bodies,
                        );
                    }
                }
                return;
            }
            "method_declaration" | "method_definition" => {
                let container = parent_id.unwrap_or(file_id);
                let mut parts = Vec::new();
                for child in all_children(node) {
                    if child.kind() == "identifier"
                        && let Some(part) = node_text(child, source) {
                            parts.push(part);
                        }
                }
                let method_name = parts.join("");
                if method_name.is_empty() {
                    return;
                }
                let method_id = make_id(&format!("{container}_{method_name}"));
                add_code_node(
                    nodes,
                    seen_ids,
                    method_id.clone(),
                    format!("-{method_name}"),
                    source_file,
                    line_no,
                );
                push_edge(
                    edges,
                    EdgeSpec {
                        source: container.to_string(),
                        target: method_id.clone(),
                        relation: "method".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
                if node.kind() == "method_definition" {
                    method_bodies.push((method_id, node));
                }
                return;
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                parent_id,
                stem,
                file_id,
                source,
                source_file,
                nodes,
                edges,
                seen_ids,
                method_bodies,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &file_id,
        &source,
        &source_file,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut method_bodies,
    );

    let method_ids: HashSet<String> = extraction
        .nodes
        .iter()
        .filter(|node| node.id != file_id)
        .map(|node| node.id.clone())
        .collect();
    let mut seen_calls = HashSet::new();
    for (caller_id, body) in method_bodies {
        walk_calls(
            body,
            &caller_id,
            &source,
            &source_file,
            &method_ids,
            &mut extraction.edges,
            &mut seen_calls,
        );
    }

    Ok(extraction)
}

fn extract_elixir_file(path: &Path) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_elixir::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let (mut extraction, mut seen_ids, file_id, source_file) = new_file_extraction(path);
    let mut function_bodies: Vec<(String, TsNode<'_>)> = Vec::new();
    let import_keywords: HashSet<&str> = HashSet::from(["alias", "import", "require", "use"]);
    let skip_keywords: HashSet<&str> = HashSet::from([
        "def",
        "defp",
        "defmodule",
        "defmacro",
        "defmacrop",
        "defstruct",
        "defprotocol",
        "defimpl",
        "defguard",
        "alias",
        "import",
        "require",
        "use",
        "if",
        "unless",
        "case",
        "cond",
        "with",
        "for",
    ]);

    fn alias_text(node: TsNode<'_>, source: &[u8]) -> Option<String> {
        all_children(node)
            .into_iter()
            .find(|child| child.kind() == "alias")
            .and_then(|child| node_text(child, source))
    }

    fn walk_calls(
        node: TsNode<'_>,
        caller_id: &str,
        source: &[u8],
        label_to_id: &HashMap<String, String>,
        skip_keywords: &HashSet<&str>,
        edges: &mut Vec<Edge>,
        source_file: &str,
        seen_pairs: &mut HashSet<(String, String)>,
    ) {
        if node.kind() != "call" {
            for child in all_children(node) {
                walk_calls(
                    child,
                    caller_id,
                    source,
                    label_to_id,
                    skip_keywords,
                    edges,
                    source_file,
                    seen_pairs,
                );
            }
            return;
        }

        for child in all_children(node) {
            if child.kind() == "identifier"
                && let Some(keyword) = node_text(child, source) {
                    if skip_keywords.contains(keyword.as_str()) {
                        for nested in all_children(node) {
                            walk_calls(
                                nested,
                                caller_id,
                                source,
                                label_to_id,
                                skip_keywords,
                                edges,
                                source_file,
                                seen_pairs,
                            );
                        }
                        return;
                    }
                    break;
                }
        }

        let mut callee_name = None;
        for child in all_children(node) {
            match child.kind() {
                "dot" => {
                    if let Some(dot_text) = node_text(child, source) {
                        let parts: Vec<&str> = dot_text.trim_end_matches('.').split('.').collect();
                        if let Some(last) = parts.last() {
                            callee_name = Some((*last).to_string());
                        }
                    }
                    break;
                }
                "identifier" => {
                    callee_name = node_text(child, source);
                    break;
                }
                _ => {}
            }
        }

        if let Some(callee_name) = callee_name
            && let Some(target_id) = label_to_id.get(&callee_name.to_lowercase()) {
                let pair = (caller_id.to_string(), target_id.clone());
                if caller_id != target_id && seen_pairs.insert(pair.clone()) {
                    push_edge_with_confidence(
                        edges,
                        pair.0,
                        pair.1,
                        "calls".to_string(),
                        source_file,
                        node.start_position().row + 1,
                        "EXTRACTED",
                        1.0,
                    );
                }
            }

        for child in all_children(node) {
            walk_calls(
                child,
                caller_id,
                source,
                label_to_id,
                skip_keywords,
                edges,
                source_file,
                seen_pairs,
            );
        }
    }

    fn walk<'a>(
        node: TsNode<'a>,
        parent_module_id: Option<&str>,
        stem: &str,
        file_id: &str,
        source: &[u8],
        source_file: &str,
        import_keywords: &HashSet<&str>,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        function_bodies: &mut Vec<(String, TsNode<'a>)>,
    ) {
        if node.kind() != "call" {
            for child in all_children(node) {
                walk(
                    child,
                    parent_module_id,
                    stem,
                    file_id,
                    source,
                    source_file,
                    import_keywords,
                    nodes,
                    edges,
                    seen_ids,
                    function_bodies,
                );
            }
            return;
        }

        let mut identifier_node = None;
        let mut arguments_node = None;
        let mut do_block_node = None;
        for child in all_children(node) {
            match child.kind() {
                "identifier" => identifier_node = Some(child),
                "arguments" => arguments_node = Some(child),
                "do_block" => do_block_node = Some(child),
                _ => {}
            }
        }

        let Some(identifier_node) = identifier_node else {
            for child in all_children(node) {
                walk(
                    child,
                    parent_module_id,
                    stem,
                    file_id,
                    source,
                    source_file,
                    import_keywords,
                    nodes,
                    edges,
                    seen_ids,
                    function_bodies,
                );
            }
            return;
        };

        let Some(keyword) = node_text(identifier_node, source) else {
            return;
        };
        let line_no = node.start_position().row + 1;

        if keyword == "defmodule" {
            let module_name = arguments_node.and_then(|node| alias_text(node, source));
            if let Some(module_name) = module_name {
                let module_id = make_id(&format!("{stem}_{module_name}"));
                add_code_node(
                    nodes,
                    seen_ids,
                    module_id.clone(),
                    module_name,
                    source_file,
                    line_no,
                );
                push_edge(
                    edges,
                    EdgeSpec {
                        source: file_id.to_string(),
                        target: module_id.clone(),
                        relation: "contains".to_string(),
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
                if let Some(do_block) = do_block_node {
                    for child in all_children(do_block) {
                        walk(
                            child,
                            Some(&module_id),
                            stem,
                            file_id,
                            source,
                            source_file,
                            import_keywords,
                            nodes,
                            edges,
                            seen_ids,
                            function_bodies,
                        );
                    }
                }
            }
            return;
        }

        if matches!(keyword.as_str(), "def" | "defp") {
            let mut func_name = None;
            if let Some(arguments) = arguments_node {
                for child in all_children(arguments) {
                    match child.kind() {
                        "call" => {
                            for sub in all_children(child) {
                                if sub.kind() == "identifier" {
                                    func_name = node_text(sub, source);
                                    break;
                                }
                            }
                        }
                        "identifier" => {
                            func_name = node_text(child, source);
                        }
                        _ => {}
                    }
                    if func_name.is_some() {
                        break;
                    }
                }
            }
            if let Some(func_name) = func_name {
                let container = parent_module_id.unwrap_or(file_id);
                let func_id = make_id(&format!("{container}_{func_name}"));
                add_code_node(
                    nodes,
                    seen_ids,
                    func_id.clone(),
                    format!("{func_name}()"),
                    source_file,
                    line_no,
                );
                push_edge(
                    edges,
                    EdgeSpec {
                        source: container.to_string(),
                        target: func_id.clone(),
                        relation: if parent_module_id.is_some() {
                            "method".to_string()
                        } else {
                            "contains".to_string()
                        },
                        source_file: source_file.to_string(),
                        line_no,
                    },
                );
                if let Some(do_block) = do_block_node {
                    function_bodies.push((func_id, do_block));
                }
            }
            return;
        }

        if import_keywords.contains(keyword.as_str()) {
            if let Some(arguments) = arguments_node
                && let Some(module_name) = alias_text(arguments, source) {
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: file_id.to_string(),
                            target: make_id(&module_name),
                            relation: "imports".to_string(),
                            source_file: source_file.to_string(),
                            line_no,
                        },
                    );
                }
            return;
        }

        for child in all_children(node) {
            walk(
                child,
                parent_module_id,
                stem,
                file_id,
                source,
                source_file,
                import_keywords,
                nodes,
                edges,
                seen_ids,
                function_bodies,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &file_id,
        &source,
        &source_file,
        &import_keywords,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut function_bodies,
    );

    let label_to_id = build_label_index(&extraction.nodes);
    let mut seen_pairs = HashSet::new();
    for (caller_id, body) in function_bodies {
        walk_calls(
            body,
            &caller_id,
            &source,
            &label_to_id,
            &skip_keywords,
            &mut extraction.edges,
            &source_file,
            &mut seen_pairs,
        );
    }

    extraction.edges.retain(|edge| {
        seen_ids.contains(&edge.source)
            && (seen_ids.contains(&edge.target) || edge.relation == "imports")
    });
    Ok(extraction)
}

fn extract_swift_file(path: &Path) -> Result<Extraction> {
    let mut extraction = extract_generic(path, &swift_cfg())?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_swift::LANGUAGE.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();
    let source_file = path.to_string_lossy().to_string();
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");

    let mut seen_ids: HashSet<String> = extraction
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect();
    let mut seen_edges: HashSet<(String, String, String, Option<String>)> =
        extraction.edges.iter().map(edge_signature).collect();
    let extension_re =
        Regex::new(r"extension\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*([A-Za-z0-9_,\s]+))?")
            .map_err(|err| anyhow!("Failed to compile Swift extension regex: {err}"))?;

    fn find_swift_name(node: TsNode<'_>, source: &[u8]) -> Option<String> {
        node.child_by_field_name("name")
            .and_then(|child| node_text(child, source))
            .or_else(|| {
                all_children(node)
                    .into_iter()
                    .find(|child| {
                        matches!(
                            child.kind(),
                            "simple_identifier" | "type_identifier" | "user_type" | "identifier"
                        )
                    })
                    .and_then(|child| node_text(child, source))
            })
    }

    fn insert_edge_once(
        edges: &mut Vec<Edge>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
        edge: Edge,
    ) {
        if seen_edges.insert(edge_signature(&edge)) {
            edges.push(edge);
        }
    }

    fn walk(
        node: TsNode<'_>,
        parent_type_id: Option<&str>,
        stem: &str,
        source: &[u8],
        source_file: &str,
        extension_re: &Regex,
        nodes: &mut Vec<Node>,
        edges: &mut Vec<Edge>,
        seen_ids: &mut HashSet<String>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
    ) {
        match node.kind() {
            "class_declaration"
            | "protocol_declaration"
            | "struct_declaration"
            | "enum_declaration"
            | "actor_declaration" => {
                if let Some(type_name) = find_swift_name(node, source) {
                    let line_no = node.start_position().row + 1;
                    let type_id = make_id(&format!("{stem}_{type_name}"));
                    add_code_node(
                        nodes,
                        seen_ids,
                        type_id.clone(),
                        type_name,
                        source_file,
                        line_no,
                    );
                    for child in all_children(node) {
                        walk(
                            child,
                            Some(&type_id),
                            stem,
                            source,
                            source_file,
                            extension_re,
                            nodes,
                            edges,
                            seen_ids,
                            seen_edges,
                        );
                    }
                    return;
                }
            }
            "extension_declaration" => {
                if let Some(raw) = node_text(node, source)
                    && let Some(caps) = extension_re.captures(&raw) {
                        let type_name = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                        if !type_name.is_empty() {
                            let line_no = node.start_position().row + 1;
                            let type_id = make_id(&format!("{stem}_{type_name}"));
                            add_code_node(
                                nodes,
                                seen_ids,
                                type_id.clone(),
                                type_name.to_string(),
                                source_file,
                                line_no,
                            );
                            if let Some(conformance) = caps.get(2) {
                                for base in conformance
                                    .as_str()
                                    .split(',')
                                    .map(str::trim)
                                    .filter(|s| !s.is_empty())
                                {
                                    let edge = Edge {
                                        source: type_id.clone(),
                                        target: make_id(&format!("{stem}_{base}")),
                                        relation: "inherits".to_string(),
                                        confidence: "EXTRACTED".to_string(),
                                        source_file: source_file.to_string(),
                                        original_source: None,
                                        original_target: None,
                                        source_location: Some(format!("L{line_no}")),
                                        confidence_score: Some(1.0),
                                        confidence_score_present: true,
                                        weight: 1.0,
                                        extra: Default::default(),
                                    };
                                    insert_edge_once(edges, seen_edges, edge);
                                }
                            }
                            for child in all_children(node) {
                                walk(
                                    child,
                                    Some(&type_id),
                                    stem,
                                    source,
                                    source_file,
                                    extension_re,
                                    nodes,
                                    edges,
                                    seen_ids,
                                    seen_edges,
                                );
                            }
                            return;
                        }
                    }
            }
            "enum_entry" => {
                if let Some(parent_type_id) = parent_type_id {
                    let line_no = node.start_position().row + 1;
                    for child in all_children(node) {
                        if child.kind() == "simple_identifier"
                            && let Some(case_name) = node_text(child, source) {
                                let case_id = make_id(&format!("{parent_type_id}_{case_name}"));
                                add_code_node(
                                    nodes,
                                    seen_ids,
                                    case_id.clone(),
                                    case_name,
                                    source_file,
                                    line_no,
                                );
                                let edge = Edge {
                                    source: parent_type_id.to_string(),
                                    target: case_id,
                                    relation: "case_of".to_string(),
                                    confidence: "EXTRACTED".to_string(),
                                    source_file: source_file.to_string(),
                                    original_source: None,
                                    original_target: None,
                                    source_location: Some(format!("L{line_no}")),
                                    confidence_score: Some(1.0),
                                    confidence_score_present: true,
                                    weight: 1.0,
                                    extra: Default::default(),
                                };
                                insert_edge_once(edges, seen_edges, edge);
                            }
                    }
                    return;
                }
            }
            "deinit_declaration" | "subscript_declaration" => {
                if let Some(parent_type_id) = parent_type_id {
                    let line_no = node.start_position().row + 1;
                    let func_name = if node.kind() == "deinit_declaration" {
                        "deinit".to_string()
                    } else {
                        "subscript".to_string()
                    };
                    let func_id = make_id(&format!("{parent_type_id}_{func_name}"));
                    add_code_node(
                        nodes,
                        seen_ids,
                        func_id.clone(),
                        format!(".{func_name}()"),
                        source_file,
                        line_no,
                    );
                    let edge = Edge {
                        source: parent_type_id.to_string(),
                        target: func_id,
                        relation: "method".to_string(),
                        confidence: "EXTRACTED".to_string(),
                        source_file: source_file.to_string(),
                        original_source: None,
                        original_target: None,
                        source_location: Some(format!("L{line_no}")),
                        confidence_score: Some(1.0),
                        confidence_score_present: true,
                        weight: 1.0,
                        extra: Default::default(),
                    };
                    insert_edge_once(edges, seen_edges, edge);
                    return;
                }
            }
            "function_declaration" => {
                if let Some(parent_type_id) = parent_type_id
                    && let Some(func_name) = find_swift_name(node, source) {
                        let line_no = node.start_position().row + 1;
                        let func_id = make_id(&format!("{parent_type_id}_{func_name}"));
                        add_code_node(
                            nodes,
                            seen_ids,
                            func_id.clone(),
                            format!(".{func_name}()"),
                            source_file,
                            line_no,
                        );
                        let edge = Edge {
                            source: parent_type_id.to_string(),
                            target: func_id,
                            relation: "method".to_string(),
                            confidence: "EXTRACTED".to_string(),
                            source_file: source_file.to_string(),
                            original_source: None,
                            original_target: None,
                            source_location: Some(format!("L{line_no}")),
                            confidence_score: Some(1.0),
                            confidence_score_present: true,
                            weight: 1.0,
                            extra: Default::default(),
                        };
                        insert_edge_once(edges, seen_edges, edge);
                    }
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                parent_type_id,
                stem,
                source,
                source_file,
                extension_re,
                nodes,
                edges,
                seen_ids,
                seen_edges,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &source,
        &source_file,
        &extension_re,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut seen_edges,
    );

    let invalid_ids: HashSet<String> = extraction
        .nodes
        .iter()
        .filter(|node| node.label == ".String?()" || node.label == "String?()")
        .map(|node| node.id.clone())
        .collect();
    if !invalid_ids.is_empty() {
        extraction
            .nodes
            .retain(|node| !invalid_ids.contains(&node.id));
        extraction.edges.retain(|edge| {
            !invalid_ids.contains(&edge.source) && !invalid_ids.contains(&edge.target)
        });
    }

    Ok(extraction)
}

fn extract_php_file(path: &Path) -> Result<Extraction> {
    let mut extraction = extract_generic(path, &php_cfg())?;
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_php::LANGUAGE_PHP.into())
        .map_err(|err| {
            anyhow!(
                "Failed to set parser language for {}: {err}",
                path.display()
            )
        })?;
    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("Parser returned no syntax tree for {}", path.display()))?;
    let root = tree.root_node();
    let source_file = path.to_string_lossy().to_string();
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("module");
    let label_to_id = build_label_index(&extraction.nodes);
    let mut seen_edges: HashSet<(String, String, String, Option<String>)> =
        extraction.edges.iter().map(edge_signature).collect();
    let mut seen_relation_pairs: HashSet<(String, String, String)> = HashSet::new();

    let config_call_re = Regex::new(r#"config\s*\(\s*['"]([^'"]+)['"]"#)
        .map_err(|err| anyhow!("Failed to compile PHP config regex: {err}"))?;
    let static_prop_re = Regex::new(r#"([A-Za-z_\\][A-Za-z0-9_\\]*)::\$[A-Za-z_][A-Za-z0-9_]*"#)
        .map_err(|err| anyhow!("Failed to compile PHP static property regex: {err}"))?;
    let bind_re = Regex::new(
        r#"->(?:bind|singleton|scoped|instance)\s*\(\s*([A-Za-z_\\][A-Za-z0-9_\\]*)::class\s*,\s*([A-Za-z_\\][A-Za-z0-9_\\]*)::class"#,
    )
    .map_err(|err| anyhow!("Failed to compile PHP bind regex: {err}"))?;
    let listen_entry_re = Regex::new(r#"(?s)([A-Za-z_\\][A-Za-z0-9_\\]*)::class\s*=>\s*\[(.*?)\]"#)
        .map_err(|err| anyhow!("Failed to compile PHP listener entry regex: {err}"))?;
    let class_const_re = Regex::new(r#"([A-Za-z_\\][A-Za-z0-9_\\]*)::class"#)
        .map_err(|err| anyhow!("Failed to compile PHP class constant regex: {err}"))?;
    let const_access_re =
        Regex::new(r#"([A-Za-z_\\][A-Za-z0-9_\\]*)::([A-Za-z_][A-Za-z0-9_]*)"#)
            .map_err(|err| anyhow!("Failed to compile PHP constant access regex: {err}"))?;

    fn insert_edge_once(
        edges: &mut Vec<Edge>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
        edge: Edge,
    ) {
        if seen_edges.insert(edge_signature(&edge)) {
            edges.push(edge);
        }
    }

    fn scan_php_body(
        body: TsNode<'_>,
        caller_id: &str,
        source: &[u8],
        source_file: &str,
        label_to_id: &HashMap<String, String>,
        edges: &mut Vec<Edge>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
        seen_relation_pairs: &mut HashSet<(String, String, String)>,
        config_call_re: &Regex,
        static_prop_re: &Regex,
        bind_re: &Regex,
        const_access_re: &Regex,
    ) {
        let Ok(text) = body.utf8_text(source) else {
            return;
        };
        let start_line = body.start_position().row + 1;

        for captures in config_call_re.captures_iter(text) {
            let Some(full) = captures.get(0) else {
                continue;
            };
            let Some(key) = captures.get(1) else {
                continue;
            };
            let segment = key
                .as_str()
                .split('.')
                .next()
                .unwrap_or(key.as_str())
                .to_lowercase();
            let Some(target_id) = label_to_id
                .get(&segment)
                .or_else(|| label_to_id.get(&format!("{segment}.php")))
            else {
                continue;
            };
            if target_id == caller_id {
                continue;
            }
            let line_no = line_no_from_offset(text, start_line, full.start());
            insert_edge_once(
                edges,
                seen_edges,
                Edge {
                    source: caller_id.to_string(),
                    target: target_id.clone(),
                    relation: "uses_config".to_string(),
                    confidence: "EXTRACTED".to_string(),
                    source_file: source_file.to_string(),
                    original_source: None,
                    original_target: None,
                    source_location: Some(format!("L{line_no}")),
                    confidence_score: Some(1.0),
                    confidence_score_present: true,
                    weight: 1.0,
                    extra: Default::default(),
                },
            );
        }

        for captures in static_prop_re.captures_iter(text) {
            let Some(full) = captures.get(0) else {
                continue;
            };
            let Some(class_name) = captures.get(1) else {
                continue;
            };
            let key = php_basename(class_name.as_str()).to_lowercase();
            let Some(target_id) = label_to_id.get(&key) else {
                continue;
            };
            if target_id == caller_id {
                continue;
            }
            let pair = (
                caller_id.to_string(),
                target_id.clone(),
                "uses_static_prop".to_string(),
            );
            if !seen_relation_pairs.insert(pair) {
                continue;
            }
            let line_no = line_no_from_offset(text, start_line, full.start());
            insert_edge_once(
                edges,
                seen_edges,
                Edge {
                    source: caller_id.to_string(),
                    target: target_id.clone(),
                    relation: "uses_static_prop".to_string(),
                    confidence: "EXTRACTED".to_string(),
                    source_file: source_file.to_string(),
                    original_source: None,
                    original_target: None,
                    source_location: Some(format!("L{line_no}")),
                    confidence_score: Some(1.0),
                    confidence_score_present: true,
                    weight: 1.0,
                    extra: Default::default(),
                },
            );
        }

        for captures in bind_re.captures_iter(text) {
            let Some(full) = captures.get(0) else {
                continue;
            };
            let Some(contract_name) = captures.get(1) else {
                continue;
            };
            let Some(impl_name) = captures.get(2) else {
                continue;
            };
            let contract_key = php_basename(contract_name.as_str()).to_lowercase();
            let impl_key = php_basename(impl_name.as_str()).to_lowercase();
            let (Some(contract_id), Some(impl_id)) =
                (label_to_id.get(&contract_key), label_to_id.get(&impl_key))
            else {
                continue;
            };
            if contract_id == impl_id {
                continue;
            }
            let pair = (contract_id.clone(), impl_id.clone(), "bound_to".to_string());
            if !seen_relation_pairs.insert(pair) {
                continue;
            }
            let line_no = line_no_from_offset(text, start_line, full.start());
            insert_edge_once(
                edges,
                seen_edges,
                Edge {
                    source: contract_id.clone(),
                    target: impl_id.clone(),
                    relation: "bound_to".to_string(),
                    confidence: "EXTRACTED".to_string(),
                    source_file: source_file.to_string(),
                    original_source: None,
                    original_target: None,
                    source_location: Some(format!("L{line_no}")),
                    confidence_score: Some(1.0),
                    confidence_score_present: true,
                    weight: 1.0,
                    extra: Default::default(),
                },
            );
        }

        for captures in const_access_re.captures_iter(text) {
            let Some(full) = captures.get(0) else {
                continue;
            };
            let Some(class_name) = captures.get(1) else {
                continue;
            };
            let key = php_basename(class_name.as_str()).to_lowercase();
            let Some(target_id) = label_to_id.get(&key) else {
                continue;
            };
            if target_id == caller_id {
                continue;
            }
            let pair = (
                caller_id.to_string(),
                target_id.clone(),
                "references_constant".to_string(),
            );
            if !seen_relation_pairs.insert(pair) {
                continue;
            }
            let line_no = line_no_from_offset(text, start_line, full.start());
            insert_edge_once(
                edges,
                seen_edges,
                Edge {
                    source: caller_id.to_string(),
                    target: target_id.clone(),
                    relation: "references_constant".to_string(),
                    confidence: "EXTRACTED".to_string(),
                    source_file: source_file.to_string(),
                    original_source: None,
                    original_target: None,
                    source_location: Some(format!("L{line_no}")),
                    confidence_score: Some(1.0),
                    confidence_score_present: true,
                    weight: 1.0,
                    extra: Default::default(),
                },
            );
        }
    }

    fn scan_php_listeners(
        node: TsNode<'_>,
        source: &[u8],
        source_file: &str,
        label_to_id: &HashMap<String, String>,
        edges: &mut Vec<Edge>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
        listen_entry_re: &Regex,
        class_const_re: &Regex,
    ) {
        let Ok(text) = node.utf8_text(source) else {
            return;
        };
        if !(text.contains("$listen") || text.contains("$subscribe")) {
            return;
        }
        let start_line = node.start_position().row + 1;

        for captures in listen_entry_re.captures_iter(text) {
            let Some(event_name) = captures.get(1) else {
                continue;
            };
            let Some(listener_block) = captures.get(2) else {
                continue;
            };
            let event_key = php_basename(event_name.as_str()).to_lowercase();
            let Some(event_id) = label_to_id.get(&event_key) else {
                continue;
            };
            for listener_cap in class_const_re.captures_iter(listener_block.as_str()) {
                let Some(listener_name) = listener_cap.get(1) else {
                    continue;
                };
                let listener_key = php_basename(listener_name.as_str()).to_lowercase();
                let Some(listener_id) = label_to_id.get(&listener_key) else {
                    continue;
                };
                if listener_id == event_id {
                    continue;
                }
                let Some(listener_match) = listener_cap.get(0) else {
                    continue;
                };
                let absolute_offset = listener_block.start() + listener_match.start();
                let line_no = line_no_from_offset(text, start_line, absolute_offset);
                insert_edge_once(
                    edges,
                    seen_edges,
                    Edge {
                        source: event_id.clone(),
                        target: listener_id.clone(),
                        relation: "listened_by".to_string(),
                        confidence: "EXTRACTED".to_string(),
                        source_file: source_file.to_string(),
                        original_source: None,
                        original_target: None,
                        source_location: Some(format!("L{line_no}")),
                        confidence_score: Some(1.0),
                        confidence_score_present: true,
                        weight: 1.0,
                        extra: Default::default(),
                    },
                );
            }
        }
    }

    fn walk(
        node: TsNode<'_>,
        parent_class_id: Option<&str>,
        stem: &str,
        source: &[u8],
        source_file: &str,
        label_to_id: &HashMap<String, String>,
        edges: &mut Vec<Edge>,
        seen_edges: &mut HashSet<(String, String, String, Option<String>)>,
        seen_relation_pairs: &mut HashSet<(String, String, String)>,
        config_call_re: &Regex,
        static_prop_re: &Regex,
        bind_re: &Regex,
        const_access_re: &Regex,
        listen_entry_re: &Regex,
        class_const_re: &Regex,
    ) {
        match node.kind() {
            "class_declaration" => {
                if let Some(class_name) = resolve_name(node, source, &php_cfg()) {
                    let class_id = make_id(&format!("{stem}_{class_name}"));
                    for child in all_children(node) {
                        walk(
                            child,
                            Some(&class_id),
                            stem,
                            source,
                            source_file,
                            label_to_id,
                            edges,
                            seen_edges,
                            seen_relation_pairs,
                            config_call_re,
                            static_prop_re,
                            bind_re,
                            const_access_re,
                            listen_entry_re,
                            class_const_re,
                        );
                    }
                    return;
                }
            }
            "method_declaration" | "function_definition" => {
                if let Some(func_name) = resolve_name(node, source, &php_cfg()) {
                    let caller_id = if let Some(parent_class_id) = parent_class_id {
                        make_id(&format!("{parent_class_id}_{func_name}"))
                    } else {
                        make_id(&format!("{stem}_{func_name}"))
                    };
                    if let Some(body) = find_body(node, &php_cfg()) {
                        scan_php_body(
                            body,
                            &caller_id,
                            source,
                            source_file,
                            label_to_id,
                            edges,
                            seen_edges,
                            seen_relation_pairs,
                            config_call_re,
                            static_prop_re,
                            bind_re,
                            const_access_re,
                        );
                    }
                    return;
                }
            }
            "property_declaration" => {
                scan_php_listeners(
                    node,
                    source,
                    source_file,
                    label_to_id,
                    edges,
                    seen_edges,
                    listen_entry_re,
                    class_const_re,
                );
            }
            _ => {}
        }

        for child in all_children(node) {
            walk(
                child,
                parent_class_id,
                stem,
                source,
                source_file,
                label_to_id,
                edges,
                seen_edges,
                seen_relation_pairs,
                config_call_re,
                static_prop_re,
                bind_re,
                const_access_re,
                listen_entry_re,
                class_const_re,
            );
        }
    }

    walk(
        root,
        None,
        stem,
        &source,
        &source_file,
        &label_to_id,
        &mut extraction.edges,
        &mut seen_edges,
        &mut seen_relation_pairs,
        &config_call_re,
        &static_prop_re,
        &bind_re,
        &const_access_re,
        &listen_entry_re,
        &class_const_re,
    );

    Ok(extraction)
}

// ── Text document extraction ──────────────────────────────────────────────────

fn extract_text_document(path: &Path) -> Result<Extraction> {
    let file_type = classify_file(path)
        .ok_or_else(|| anyhow!("Unsupported file for text extraction: {}", path.display()))?;
    let file_type_str = file_type.as_str().to_string();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Failed to read text document: {}", path.display()))?;
    let text = strip_frontmatter(&raw);
    let source_file = path.to_string_lossy().to_string();
    let file_label = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| source_file.clone());
    let file_id = make_id(&source_file);
    let mut extraction = Extraction::default();

    extraction.nodes.push(Node {
        id: file_id.clone(),
        label: file_label,
        file_type: file_type_str.clone(),
        source_file: source_file.clone(),
        source_location: Some("L1".to_string()),
        node_type: Some("file".to_string()),
        docstring: summarize_text(&text, 400),
        parameters: Vec::new(),
        signature: None,
        extra: Default::default(),
    });

    for section in text_sections(&text) {
        let section_id = make_id(&format!(
            "{}:{}:{}",
            source_file, section.line_no, section.label
        ));
        extraction.nodes.push(Node {
            id: section_id.clone(),
            label: section.label,
            file_type: file_type_str.clone(),
            source_file: source_file.clone(),
            source_location: Some(format!("L{}", section.line_no)),
            node_type: Some("section".to_string()),
            docstring: summarize_text(&section.body, 1200),
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        });
        push_edge(
            &mut extraction.edges,
            EdgeSpec {
                source: file_id.clone(),
                target: section_id,
                relation: "contains".to_string(),
                source_file: source_file.clone(),
                line_no: section.line_no,
            },
        );
    }

    Ok(extraction)
}

#[derive(Debug)]
struct TextSection {
    label: String,
    body: String,
    line_no: usize,
}

fn text_sections(text: &str) -> Vec<TextSection> {
    let markdown_sections = markdown_sections(text);
    if !markdown_sections.is_empty() {
        return markdown_sections;
    }
    paragraph_sections(text)
}

fn markdown_sections(text: &str) -> Vec<TextSection> {
    let mut sections = Vec::new();
    let mut current_label: Option<String> = None;
    let mut current_body = Vec::new();
    let mut current_line = 1usize;
    let mut saw_heading = false;

    for (idx, line) in text.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        if let Some(label) = markdown_heading_label(trimmed) {
            saw_heading = true;
            push_section(
                &mut sections,
                current_label.take(),
                std::mem::take(&mut current_body),
                current_line,
            );
            current_label = Some(label);
            current_line = line_no;
        } else if current_label.is_some() || !trimmed.is_empty() {
            if current_body.is_empty() {
                current_line = line_no;
            }
            current_body.push(line.to_string());
        }
    }

    push_section(
        &mut sections,
        current_label.take(),
        std::mem::take(&mut current_body),
        current_line,
    );
    if saw_heading { sections } else { Vec::new() }
}

fn paragraph_sections(text: &str) -> Vec<TextSection> {
    let mut sections = Vec::new();
    let mut current_lines = Vec::new();
    let mut current_line_no = 1usize;

    for (idx, line) in text.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            if !current_lines.is_empty() {
                let body = current_lines.join("\n");
                sections.push(TextSection {
                    label: preview_label(&body, 80),
                    body,
                    line_no: current_line_no,
                });
                current_lines.clear();
            }
            continue;
        }
        if current_lines.is_empty() {
            current_line_no = line_no;
        }
        current_lines.push(line.to_string());
    }

    if !current_lines.is_empty() {
        let body = current_lines.join("\n");
        sections.push(TextSection {
            label: preview_label(&body, 80),
            body,
            line_no: current_line_no,
        });
    }
    sections
}

fn push_section(
    sections: &mut Vec<TextSection>,
    label: Option<String>,
    body_lines: Vec<String>,
    line_no: usize,
) {
    let body = body_lines.join("\n").trim().to_string();
    if label.is_none() && body.is_empty() {
        return;
    }
    sections.push(TextSection {
        label: label.unwrap_or_else(|| preview_label(&body, 80)),
        body,
        line_no,
    });
}

fn markdown_heading_label(line: &str) -> Option<String> {
    if !line.starts_with('#') {
        return None;
    }
    let label = line.trim_start_matches('#').trim();
    if label.is_empty() {
        None
    } else {
        Some(label.to_string())
    }
}

fn strip_frontmatter(text: &str) -> String {
    if let Some(rest) = text.strip_prefix("---\n")
        && let Some(end) = rest.find("\n---\n") {
            return rest[end + 5..].to_string();
        }
    text.to_string()
}

fn preview_label(text: &str, max_len: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "Untitled section".to_string();
    }
    if collapsed.chars().count() <= max_len {
        return collapsed;
    }
    let truncated = collapsed
        .chars()
        .take(max_len.saturating_sub(1))
        .collect::<String>()
        .trim_end()
        .to_string();
    format!("{}…", truncated)
}

fn summarize_text(text: &str, max_len: usize) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(preview_label(trimmed, max_len))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_markdown_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        fs::write(
            &path,
            "# Overview\n\nThis is the first section.\n\n## Details\n\nMore text here.\n",
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "notes.md"));
        assert!(labels.iter().any(|l| *l == "Overview"));
        assert!(labels.iter().any(|l| *l == "Details"));
        assert!(result.edges.iter().any(|e| e.relation == "contains"));
    }

    #[test]
    fn markdown_sections_use_python_compatible_line_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        fs::write(
            &path,
            "# Overview\n\nThis is the first section.\n\n## Details\n\nMore text here.\n",
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let overview = result
            .nodes
            .iter()
            .find(|node| node.label == "Overview")
            .and_then(|node| node.source_location.as_deref());
        let details = result
            .nodes
            .iter()
            .find(|node| node.label == "Details")
            .and_then(|node| node.source_location.as_deref());

        assert_eq!(overview, Some("L2"));
        assert_eq!(details, Some("L6"));
    }

    #[test]
    fn python_property_extracts_as_file_level_contains_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.py");
        fs::write(
            &path,
            "class Response:\n    @property\n    def text(self):\n        return 'ok'\n",
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let text_node = result.nodes.iter().find(|node| node.label == "text()");
        assert!(text_node.is_some());
        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "contains" && edge.target == "sample_text")
        );
        assert!(
            result
                .edges
                .iter()
                .all(|edge| !(edge.relation == "method" && edge.target == "sample_text"))
        );
    }

    #[test]
    fn preview_label_trims_before_ellipsis() {
        assert_eq!(preview_label("alpha beta gamma", 12), "alpha beta…");
    }

    #[test]
    fn rationale_labels_collapse_internal_whitespace_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.py");
        fs::write(
            &path,
            "def check():\n    \"\"\"Check semantic extraction cache for a list of absolute file paths.\n\n    Returns graphify-out/cache/ - creates it if needed.\n    \"\"\"\n    return None\n",
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let rationale = result
            .nodes
            .iter()
            .find(|node| node.file_type == "rationale")
            .map(|node| node.label.clone());
        assert_eq!(
            rationale.as_deref(),
            Some("Check semantic extraction cache for a list of absolute file paths.      Returns")
        );
    }

    #[test]
    fn skips_unsupported_files_without_failing_batch() {
        let dir = tempfile::tempdir().unwrap();
        let supported = dir.path().join("sample.py");
        let unsupported = dir.path().join("notes.xyz");
        fs::write(&supported, "def keep():\n    return 1\n").unwrap();
        fs::write(&unsupported, "opaque content\n").unwrap();

        let result = extract_paths(&[
            supported.to_string_lossy().to_string(),
            unsupported.to_string_lossy().to_string(),
        ])
        .unwrap();

        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "keep()"));
        assert!(labels.iter().all(|l| *l != "notes.xyz"));
    }

    #[test]
    fn extracts_python_structure_and_calls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.py");
        fs::write(
            &path,
            r#"
class Transformer:
    def __init__(self, d_model: int):
        self.d_model = d_model

    def forward(self, x):
        return normalize(x)

def normalize(value):
    return value
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Transformer"));
        assert!(labels.iter().any(|l| *l == "normalize()"));
        assert!(labels.iter().any(|l| *l == ".forward()"));
    }

    #[test]
    fn extracts_python_import_edges_and_cross_file_uses() {
        let dir = tempfile::tempdir().unwrap();
        let auth_path = dir.path().join("auth.py");
        let models_path = dir.path().join("models.py");
        fs::write(&models_path, "class Response:\n    pass\n").unwrap();
        fs::write(&auth_path, "from models import Response\n\nclass DigestAuth:\n    def build(self):\n        return Response()\n").unwrap();

        let paths = vec![
            auth_path.to_string_lossy().to_string(),
            models_path.to_string_lossy().to_string(),
        ];
        let result = extract_paths(&paths).unwrap();
        let models_module_id = make_id("models");

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == models_module_id)
        );
        assert!(result.edges.iter().any(|edge| {
            edge.relation == "uses" && edge.confidence == "INFERRED" && edge.weight == 0.8
        }));
    }

    #[test]
    fn infers_python_cross_file_uses_from_rationale_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let auth_path = dir.path().join("auth.py");
        let models_path = dir.path().join("models.py");
        fs::write(&models_path, "class Response:\n    pass\n").unwrap();
        fs::write(
            &auth_path,
            "\"\"\"Authentication handlers describe how requests use shared response models.\"\"\"\nfrom models import Response\n\nclass DigestAuth:\n    \"\"\"Digest auth relies on the shared response model for challenge handling.\"\"\"\n    pass\n",
        )
        .unwrap();

        let paths = vec![
            auth_path.to_string_lossy().to_string(),
            models_path.to_string_lossy().to_string(),
        ];
        let result = extract_paths(&paths).unwrap();
        let response_id = result
            .nodes
            .iter()
            .find(|node| node.label == "Response")
            .map(|node| node.id.clone())
            .unwrap();
        let rationale_ids: HashSet<String> = result
            .nodes
            .iter()
            .filter(|node| {
                node.source_file == auth_path.to_string_lossy() && node.file_type == "rationale"
            })
            .map(|node| node.id.clone())
            .collect();

        assert!(!rationale_ids.is_empty());
        assert!(result.edges.iter().any(|edge| {
            rationale_ids.contains(&edge.source)
                && edge.target == response_id
                && edge.relation == "uses"
                && edge.confidence == "INFERRED"
        }));
    }

    #[test]
    fn extracts_python_inheritance_to_external_placeholder_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.py");
        fs::write(&path, "class BasicAuth(Auth):\n    pass\n").unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let basic_auth_id = result
            .nodes
            .iter()
            .find(|node| node.label == "BasicAuth")
            .map(|node| node.id.clone())
            .unwrap();
        let auth_id = result
            .nodes
            .iter()
            .find(|node| node.label == "Auth" && node.source_file.is_empty())
            .map(|node| node.id.clone())
            .unwrap();

        assert!(result.edges.iter().any(|edge| {
            edge.source == basic_auth_id
                && edge.target == auth_id
                && edge.relation == "inherits"
                && edge.confidence == "EXTRACTED"
        }));
    }

    #[test]
    fn ignores_python_dotted_superclasses_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("security.py");
        fs::write(
            &path,
            "class Handler(urllib.request.HTTPRedirectHandler):\n    pass\n",
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        assert!(!result.edges.iter().any(|edge| edge.relation == "inherits"));
        assert!(
            !result
                .nodes
                .iter()
                .any(|node| node.label == "HTTPRedirectHandler" && node.source_file.is_empty())
        );
    }

    #[test]
    fn extracts_python_plain_import_edges_to_module_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let auth_path = dir.path().join("auth.py");
        let models_path = dir.path().join("models.py");
        fs::write(&models_path, "class Response:\n    pass\n").unwrap();
        fs::write(&auth_path, "import models\n").unwrap();

        let paths = vec![
            auth_path.to_string_lossy().to_string(),
            models_path.to_string_lossy().to_string(),
        ];
        let result = extract_paths(&paths).unwrap();
        let models_module_id = make_id("models");

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports" && edge.target == models_module_id)
        );
    }

    #[test]
    fn extracts_python_aliased_dotted_imports_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("security.py");
        fs::write(
            &path,
            "import urllib.error as urllib_error\nimport urllib.request as urllib_request\n",
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        assert!(
            result.edges.iter().any(|edge| {
                edge.relation == "imports" && edge.target == make_id("urllib.error")
            })
        );
        assert!(result.edges.iter().any(|edge| {
            edge.relation == "imports" && edge.target == make_id("urllib.request")
        }));
        assert!(
            !result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports" && edge.target == make_id("urllib"))
        );
    }

    #[test]
    fn infers_cross_file_calls_for_generic_extractors() {
        let dir = tempfile::tempdir().unwrap();
        let caller_path = dir.path().join("caller.js");
        let callee_path = dir.path().join("callee.js");
        fs::write(
            &caller_path,
            r#"
function start() {
  return helper();
}
"#,
        )
        .unwrap();
        fs::write(
            &callee_path,
            r#"
function helper() {
  return 1;
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[
            caller_path.to_string_lossy().to_string(),
            callee_path.to_string_lossy().to_string(),
        ])
        .unwrap();

        let start_id = result
            .nodes
            .iter()
            .find(|node| node.label == "start()")
            .map(|node| node.id.clone())
            .unwrap();
        let helper_id = result
            .nodes
            .iter()
            .find(|node| node.label == "helper()")
            .map(|node| node.id.clone())
            .unwrap();

        assert!(result.edges.iter().any(|edge| {
            edge.source == start_id
                && edge.target == helper_id
                && edge.relation == "calls"
                && edge.confidence == "INFERRED"
                && edge.confidence_score == Some(0.8)
        }));
        assert!(result.raw_calls.is_empty());
    }

    #[test]
    fn infers_cross_file_calls_for_rust_module_paths() {
        let dir = tempfile::tempdir().unwrap();
        let caller_path = dir.path().join("caller.rs");
        let callee_path = dir.path().join("helper.rs");
        fs::write(
            &caller_path,
            r#"
mod helper;

fn start() {
    helper::normalize();
}
"#,
        )
        .unwrap();
        fs::write(
            &callee_path,
            r#"
pub fn normalize() {}
"#,
        )
        .unwrap();

        let result = extract_paths(&[
            caller_path.to_string_lossy().to_string(),
            callee_path.to_string_lossy().to_string(),
        ])
        .unwrap();

        let start_id = result
            .nodes
            .iter()
            .find(|node| node.label == "start()")
            .map(|node| node.id.clone())
            .unwrap();
        let normalize_id = result
            .nodes
            .iter()
            .find(|node| node.label == "normalize()")
            .map(|node| node.id.clone())
            .unwrap();

        assert!(result.edges.iter().any(|edge| {
            edge.source == start_id
                && edge.target == normalize_id
                && edge.relation == "calls"
                && edge.confidence == "INFERRED"
                && edge.confidence_score == Some(0.8)
        }));
        assert!(result.raw_calls.is_empty());
    }

    #[test]
    fn infers_cross_file_calls_for_rust_methods_on_function_returns() {
        let dir = tempfile::tempdir().unwrap();
        let timeutil_path = dir.path().join("timeutil.rs");
        let ingest_path = dir.path().join("ingest.rs");
        fs::write(
            &timeutil_path,
            r#"
pub struct UtcDateTime;

impl UtcDateTime {
    pub fn iso_string(&self) -> String {
        String::new()
    }
}

pub fn current_utc_datetime() -> UtcDateTime {
    UtcDateTime
}
"#,
        )
        .unwrap();
        fs::write(
            &ingest_path,
            r#"
fn fetch() {
    let _ = current_utc_datetime().iso_string();
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[
            timeutil_path.to_string_lossy().to_string(),
            ingest_path.to_string_lossy().to_string(),
        ])
        .unwrap();

        let fetch_id = result
            .nodes
            .iter()
            .find(|node| node.label == "fetch()")
            .map(|node| node.id.clone())
            .unwrap();
        let iso_string_id = result
            .nodes
            .iter()
            .find(|node| node.label == ".iso_string()")
            .map(|node| node.id.clone())
            .unwrap();

        assert!(result.edges.iter().any(|edge| {
            edge.source == fetch_id
                && edge.target == iso_string_id
                && edge.relation == "calls"
                && edge.confidence == "INFERRED"
                && edge.confidence_score == Some(0.8)
        }));
        assert!(result.raw_calls.is_empty());
    }

    #[test]
    fn extracts_js_classes_and_functions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.js");
        fs::write(
            &path,
            r#"
class App {
    constructor() {
        this.init();
    }
    init() {
        console.log("started");
    }
}
function main() {
    const app = new App();
    app.init();
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "App"));
        assert!(labels.iter().any(|l| *l == "main()"));
        assert!(labels.iter().any(|l| *l == ".init()"));
    }

    #[test]
    fn normalizes_js_relative_import_targets_before_id_generation() {
        let dir = tempfile::tempdir().unwrap();
        let nested_dir = dir.path().join("src").join("ui");
        fs::create_dir_all(&nested_dir).unwrap();
        let app_path = nested_dir.join("app.js");
        let util_path = dir.path().join("src").join("shared").join("util.ts");
        fs::create_dir_all(util_path.parent().unwrap()).unwrap();
        fs::write(
            &app_path,
            "import helper from '../shared/../shared/util.js';\n",
        )
        .unwrap();
        fs::write(&util_path, "export function helper() {}\n").unwrap();

        let result = extract_paths(&[
            app_path.to_string_lossy().to_string(),
            util_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let expected_target = make_id(&util_path.to_string_lossy());

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == expected_target)
        );
    }

    #[test]
    fn extracts_go_functions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main.go");
        fs::write(
            &path,
            r#"
package main

import "fmt"

type Server struct {
    port int
}

func (s *Server) Start() {
    fmt.Println("starting")
}

func main() {
    s := &Server{port: 8080}
    s.Start()
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Server"));
        assert!(labels.iter().any(|l| *l == "main()"));
        assert!(labels.iter().any(|l| *l == ".Start()"));
    }

    #[test]
    fn extracts_rust_items() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lib.rs");
        fs::write(
            &path,
            r#"
pub struct Config {
    pub host: String,
}

impl Config {
    pub fn new(host: &str) -> Self {
        Config { host: host.to_string() }
    }
}

pub fn run() {
    let _ = Config::new("localhost");
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Config"));
        assert!(labels.iter().any(|l| *l == "run()"));
    }

    #[test]
    fn extracts_rust_use_declarations_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lib.rs");
        fs::write(
            &path,
            r#"
use std::{collections::HashMap, fmt::Debug};

pub fn run() {}
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let std_id = make_id("std");
        let hashmap_id = make_id("HashMap");
        let debug_id = make_id("Debug");

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == std_id)
        );
        assert!(
            !result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == hashmap_id)
        );
        assert!(
            !result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == debug_id)
        );
    }

    #[test]
    fn extracts_rust_trait_impl_methods_under_implemented_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lib.rs");
        fs::write(
            &path,
            r#"
trait CreateDirAll {
    fn mkdir_parents(&self);
}

struct Path;

impl CreateDirAll for Path {
    fn mkdir_parents(&self) {}
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let path_id = result
            .nodes
            .iter()
            .find(|node| node.label == "Path")
            .map(|node| node.id.clone())
            .unwrap();
        let mkdir_id = result
            .nodes
            .iter()
            .find(|node| node.label == ".mkdir_parents()")
            .map(|node| node.id.clone())
            .unwrap();

        assert_eq!(mkdir_id, make_id("lib_path_mkdir_parents"));
        assert!(result.edges.iter().any(|edge| {
            edge.source == path_id && edge.target == mkdir_id && edge.relation == "method"
        }));
    }

    #[test]
    fn infers_cross_file_rust_method_calls() {
        let dir = tempfile::tempdir().unwrap();
        let defs = dir.path().join("defs.rs");
        let main = dir.path().join("main.rs");
        fs::write(
            &defs,
            r#"
pub struct DetectFileType;

impl DetectFileType {
    pub fn as_str(&self) -> &str {
        "rs"
    }
}
"#,
        )
        .unwrap();
        fs::write(
            &main,
            r#"
pub fn build() {
    let kind = "rs";
    let _ = kind.as_str();
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[
            defs.to_string_lossy().to_string(),
            main.to_string_lossy().to_string(),
        ])
        .unwrap();
        let build_id = result
            .nodes
            .iter()
            .find(|node| node.label == "build()")
            .map(|node| node.id.clone())
            .unwrap();
        let as_str_id = result
            .nodes
            .iter()
            .find(|node| node.label == ".as_str()" && node.source_file == defs.to_string_lossy())
            .map(|node| node.id.clone())
            .unwrap();

        // Accessor calls now resolve cross-file (matching Python parity)
        assert!(result.edges.iter().any(|edge| {
            edge.source == build_id && edge.target == as_str_id && edge.relation == "calls"
        }));
    }

    #[test]
    fn extracts_java_classes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("App.java");
        fs::write(
            &path,
            r#"
public class App {
    public void run() {
        System.out.println("hello");
    }
    public static void main(String[] args) {
        new App().run();
    }
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "App"));
        assert!(labels.iter().any(|l| *l == ".run()"));
        assert!(labels.iter().any(|l| *l == ".main()"));
    }

    #[test]
    fn extracts_ts_interfaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("types.ts");
        fs::write(
            &path,
            r#"
export interface User {
    id: string;
    name: string;
}

export function getUser(id: string): User {
    return { id, name: "test" };
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "User"));
        assert!(labels.iter().any(|l| *l == "getUser()"));
    }

    #[test]
    fn extracts_vue_script_setup_with_typescript() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("App.vue");
        fs::write(
            &path,
            r#"<template><div /></template>
<script setup lang="ts">
class Store {}
function boot() {
  helper()
}
function helper() {}
</script>
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Store"));
        assert!(labels.iter().any(|l| *l == "boot()"));
        assert!(labels.iter().any(|l| *l == "helper()"));
        assert!(result.edges.iter().any(|edge| edge.relation == "calls"));
        assert_eq!(
            result
                .nodes
                .iter()
                .find(|node| node.label == "boot()")
                .and_then(|node| node.source_location.as_deref()),
            Some("L4")
        );
    }

    #[test]
    fn extracts_svelte_script_with_typescript() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Widget.svelte");
        fs::write(
            &path,
            r#"<script lang="ts">
class Widget {}
function mount() {
  hydrate()
}
function hydrate() {}
</script>

<h1>Hello</h1>
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Widget"));
        assert!(labels.iter().any(|l| *l == "mount()"));
        assert!(labels.iter().any(|l| *l == "hydrate()"));
        assert!(result.edges.iter().any(|edge| edge.relation == "calls"));
        assert_eq!(
            result
                .nodes
                .iter()
                .find(|node| node.label == "mount()")
                .and_then(|node| node.source_location.as_deref()),
            Some("L3")
        );
    }

    #[test]
    fn extracts_kotlin_structure_and_calls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.kt");
        fs::write(
            &path,
            r#"import kotlinx.coroutines.delay
import kotlin.math.max

data class Config(val baseUrl: String, val timeout: Int)

class HttpClient(private val config: Config) {
    fun get(path: String): String {
        return buildRequest("GET", path)
    }

    fun post(path: String, body: String): String {
        return buildRequest("POST", path)
    }

    private fun buildRequest(method: String, path: String): String {
        return "$method ${config.baseUrl}$path"
    }
}

fun createClient(baseUrl: String): HttpClient {
    val config = Config(baseUrl, 30)
    return HttpClient(config)
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "HttpClient"));
        assert!(labels.iter().any(|l| *l == "Config"));
        assert!(labels.iter().any(|l| *l == ".get()"));
        assert!(labels.iter().any(|l| *l == ".post()"));
        assert!(labels.iter().any(|l| *l == "createClient()"));
        assert!(!result.edges.iter().any(|edge| edge.relation == "imports"));
        assert!(!result.edges.iter().any(|edge| edge.relation == "calls"));
    }

    #[test]
    fn extracts_php_constant_references() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.php");
        fs::write(
            &path,
            r#"<?php
class Config {
    const VERSION = '1.0';
}

class App {
    public function boot() {
        return Config::VERSION;
    }
}
"#,
        )
        .unwrap();

        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let config_id = result
            .nodes
            .iter()
            .find(|node| node.label == "Config")
            .map(|node| node.id.clone())
            .unwrap();
        let boot_id = result
            .nodes
            .iter()
            .find(|node| node.label == ".boot()")
            .map(|node| node.id.clone())
            .unwrap();

        assert!(result.edges.iter().any(|edge| {
            edge.source == boot_id
                && edge.target == config_id
                && edge.relation == "references_constant"
                && edge.confidence == "EXTRACTED"
        }));
    }

    #[test]
    fn extracts_cpp_structs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main.cpp");
        fs::write(
            &path,
            r#"
#include <iostream>

class Greeter {
public:
    void greet() {
        std::cout << "hello" << std::endl;
    }
};

int main() {
    Greeter g;
    g.greet();
    return 0;
}
"#,
        )
        .unwrap();
        let result = extract_paths(&[path.to_string_lossy().to_string()]).unwrap();
        let labels: Vec<&str> = result.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.iter().any(|l| *l == "Greeter"));
        assert!(labels.iter().any(|l| *l == "main()"));
        assert!(!labels.iter().any(|l| *l == ".greet()"));
    }
}
