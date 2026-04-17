use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use tree_sitter::{Language, Node as TsNode, Parser};

use crate::detect::{FileType, classify_file};
use crate::schema::{Edge, Extraction, Node};

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

pub fn extract_paths(paths: &[String]) -> Result<Extraction> {
    if paths.is_empty() {
        return Ok(Extraction::default());
    }

    let mut combined = Extraction::default();
    let mut python_results: Vec<(PathBuf, Extraction)> = Vec::new();

    for path_str in paths {
        let path = Path::new(path_str);
        match classify_file(path) {
            Some(FileType::Document) | Some(FileType::Paper) => {
                append_extraction(&mut combined, extract_text_document(path)?);
            }
            Some(FileType::Code) => {
                if is_embedded_script_path(path) {
                    append_extraction(&mut combined, extract_embedded_script_file(path)?);
                    continue;
                }
                let cfg = match config_for_path(path) {
                    Some(c) => c,
                    None => {
                        continue;
                    }
                };
                let result = extract_generic(path, &cfg)?;
                if path.extension().and_then(|ext| ext.to_str()) == Some("py") {
                    python_results.push((path.to_path_buf(), result.clone()));
                }
                append_extraction(&mut combined, result);
            }
            _ => continue,
        }
    }

    if !python_results.is_empty() {
        combined
            .edges
            .extend(resolve_python_cross_file_imports(&python_results)?);
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
        let Some(body) = caps.name("body") else { continue };
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
        let line_offset = source[..body.start()].bytes().filter(|b| *b == b'\n').count();
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
    dst.hyperedges.extend(src.hyperedges);
    dst.input_tokens += src.input_tokens;
    dst.output_tokens += src.output_tokens;
}

// ── Generic AST extraction ────────────────────────────────────────────────────

fn extract_generic(path: &Path, cfg: &LanguageConfig) -> Result<Extraction> {
    let source =
        fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))?;
    extract_generic_from_source(path, &source, cfg)
}

fn extract_generic_from_source(path: &Path, source: &[u8], cfg: &LanguageConfig) -> Result<Extraction> {
    let mut parser = Parser::new();
    parser.set_language(&cfg.language).map_err(|err| {
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
        &source,
        &source_file,
        stem,
        &file_id,
        cfg,
        &mut extraction.nodes,
        &mut extraction.edges,
        &mut seen_ids,
        &mut pending_calls,
    );

    // Resolve pending calls
    let label_to_id = build_label_index(&extraction.nodes);
    let mut seen_call_pairs: HashSet<(String, String)> = HashSet::new();
    for pending in pending_calls {
        let key = pending.callee_name.to_lowercase();
        let Some(target_id) = label_to_id.get(&key) else {
            continue;
        };
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
) {
    let t = node.kind();

    // Import types — handle and don't recurse
    if cfg.import_types.iter().any(|it| *it == t) {
        handle_import(node, source, file_id, source_file, cfg, edges);
        return;
    }

    // Class types
    if cfg.class_types.iter().any(|ct| *ct == t) {
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
        );
        return;
    }

    // Function types
    if cfg.function_types.iter().any(|ft| *ft == t) {
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
        );
    }
}

// ── Class handling ────────────────────────────────────────────────────────────

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
) {
    let class_name = resolve_name(node, source, cfg).or_else(|| {
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
        // Rust impl_item: extract the type being implemented
        if node.kind() == "impl_item" {
            return resolve_impl_type(node, source, stem);
        }
        None
    });
    let Some(class_name) = class_name else { return };

    let class_id = make_id(&format!("{stem}_{class_name}"));
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
            node_type: Some("class".to_string()),
            docstring: None,
            parameters: Vec::new(),
            signature: None,
            extra: Default::default(),
        },
    );
    if !(node.kind() == "impl_item" && already_seen) {
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
    handle_inheritance(&class_id, node, source, stem, cfg, edges);

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
            );
        }
    }
}

fn handle_inheritance(
    class_id: &str,
    node: TsNode<'_>,
    source: &[u8],
    stem: &str,
    cfg: &LanguageConfig,
    edges: &mut Vec<Edge>,
) {
    let line_no = node.start_position().row + 1;
    for inh_type in cfg.inheritance_child_types {
        for child in named_children(node) {
            if child.kind() == *inh_type {
                for base in collect_identifiers(child, source) {
                    let base_nid = make_id(&format!("{stem}_{base}"));
                    push_edge(
                        edges,
                        EdgeSpec {
                            source: class_id.to_string(),
                            target: base_nid,
                            relation: "inherits".to_string(),
                            source_file: String::new(),
                            line_no,
                        },
                    );
                }
            }
        }
    }
}

fn resolve_impl_type(node: TsNode<'_>, source: &[u8], _stem: &str) -> Option<String> {
    // For Rust impl blocks, return the type being implemented
    for child in named_children(node) {
        if child.kind() == "type_identifier" {
            return node_text(child, source);
        }
        // Handle impl<Trait> for Type — we want Type
        if child.kind() == "trait_bound" {
            for sub in named_children(child) {
                if sub.kind() == "type_identifier" {
                    return node_text(sub, source);
                }
            }
        }
    }
    None
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
) {
    let ext = source_extension(source_file);
    let func_name = if matches!(ext, "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx") {
        node.child_by_field_name("declarator")
            .and_then(|decl| resolve_c_like_function_name(decl, source, ext != "c" && ext != "h"))
    } else {
        resolve_name(node, source, cfg)
    };
    let Some(func_name) = func_name else {
        return;
    };
    let line_no = node.start_position().row + 1;
    let python_property = source_extension(source_file) == "py" && is_python_property_function(node, source);

    // For Go methods: check for receiver field to determine if this is a method
    let resolved_parent: Option<String> = parent_class_id.map(|s| s.to_string()).or_else(|| {
        // Go: func (r *ReceiverType) MethodName() — extract type from receiver
        node.child_by_field_name("receiver").and_then(|receiver| {
            for param in named_children(receiver) {
                for child in named_children(param) {
                    if child.kind() == "pointer_type" {
                        for sub in named_children(child) {
                            if sub.kind() == "type_identifier" {
                                if let Some(type_name) = node_text(sub, source) {
                                    return Some(make_id(&format!("{stem}_{type_name}")));
                                }
                            }
                        }
                    } else if child.kind() == "type_identifier" {
                        if let Some(type_name) = node_text(child, source) {
                            return Some(make_id(&format!("{stem}_{type_name}")));
                        }
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

    // Collect calls inside this function body
    let body = find_body(node, cfg);
    if let Some(body_node) = body {
        collect_calls(body_node, &func_id, source, cfg, pending_calls);
    }
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
            && node_text(child, source)
                .is_some_and(|text| text.split_whitespace().any(|part| part.ends_with("property")))
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

    if !cfg.import_types.iter().any(|it| *it == kind) {
        return;
    }

    let targets: Vec<(String, String)> = match kind {
        "import_statement" if is_js_like_extension(ext) => {
            extract_js_import_targets(node, source, source_file)
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
            let relation = if ext == "go" { "imports_from" } else { "imports" };
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
            // Rust: use std::collections::HashMap
            if let Some(text) = node_text(node, source) {
                let text = text.trim_start_matches("use").trim().trim_end_matches(';');
                text.split(',')
                    .map(|seg| {
                        let seg = seg.trim();
                        seg.rsplit("::")
                            .next()
                            .unwrap_or(seg)
                            .split('{')
                            .next_back()
                            .unwrap_or(seg)
                            .split(" as ")
                            .next()
                            .unwrap_or(seg)
                            .trim()
                            .to_string()
                    })
                    .filter(|m: &String| !m.is_empty())
                    .map(|m: String| (make_id(&m), "imports_from".to_string()))
                    .collect()
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
        let raw = raw.trim_matches('\'').trim_matches('"').trim_matches('`').trim();
        if raw.is_empty() {
            continue;
        }
        let target = if raw.starts_with('.') {
            let mut resolved = Path::new(source_file).parent().unwrap_or_else(|| Path::new("")).join(raw);
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

fn collect_java_import_targets(node: TsNode<'_>, source: &[u8]) -> Vec<(String, String)> {
    fn walk_scoped(node: TsNode<'_>, source: &[u8]) -> Option<String> {
        match node.kind() {
            "scoped_identifier" => {
                let mut parts = Vec::new();
                let mut current = Some(node);
                while let Some(cur) = current {
                    if cur.kind() == "scoped_identifier" {
                        if let Some(name) = cur.child_by_field_name("name").and_then(|n| node_text(n, source)) {
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
    module: String,
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
    let mut module_to_file_id: HashMap<String, String> = HashMap::new();
    for (_path, extraction) in per_file {
        for node in &extraction.nodes {
            if node.source_file.is_empty() {
                continue;
            }
            if node.node_type.as_deref() == Some("file") {
                for module_name in python_module_candidates(Path::new(&node.source_file)) {
                    module_to_file_id.insert(module_name, node.id.clone());
                }
                continue;
            }
            if node.node_type.as_deref() != Some("class") {
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
        let Some(local_file_id) = extraction
            .nodes
            .iter()
            .find(|node| {
                node.node_type.as_deref() == Some("file") && node.source_file == source_file
            })
            .map(|node| node.id.clone())
        else {
            continue;
        };
        let local_classes: Vec<String> = extraction
            .nodes
            .iter()
            .filter(|node| {
                node.source_file == source_file && node.node_type.as_deref() == Some("class")
            })
            .map(|node| node.id.clone())
            .collect();

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
            &local_file_id,
            &source_file,
            &local_classes,
            &module_to_file_id,
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
    local_file_id: &str,
    source_file: &str,
    local_classes: &[String],
    module_to_file_id: &HashMap<String, String>,
    stem_to_entities: &HashMap<String, HashMap<String, String>>,
    seen_edges: &mut HashSet<(String, String, usize)>,
    edges: &mut Vec<Edge>,
) {
    if node.kind() == "import_statement" {
        let line_no = node.start_position().row + 1;
        for module in parse_python_import_statement_modules(node, source) {
            if let Some(target_file_id) = module_to_file_id.get(&module) {
                let key = (local_file_id.to_string(), target_file_id.clone(), line_no);
                if seen_edges.insert(key) {
                    edges.push(Edge {
                        source: local_file_id.to_string(),
                        target: target_file_id.clone(),
                        relation: "imports".to_string(),
                        confidence: "EXTRACTED".to_string(),
                        source_file: source_file.to_string(),
                        original_source: None,
                        original_target: None,
                        source_location: Some(format!("L{}", line_no)),
                        confidence_score: Some(1.0),
                        confidence_score_present: true,
                        weight: 1.0,
                        extra: Default::default(),
                    });
                }
            }
        }
    }

    if node.kind() == "import_from_statement" {
        if let Some(import_statement) = parse_python_import_from_statement(node, source) {
            let line_no = node.start_position().row + 1;
            if let Some(target_file_id) = module_to_file_id.get(&import_statement.module) {
                let key = (local_file_id.to_string(), target_file_id.clone(), line_no);
                if seen_edges.insert(key) {
                    edges.push(Edge {
                        source: local_file_id.to_string(),
                        target: target_file_id.clone(),
                        relation: "imports_from".to_string(),
                        confidence: "EXTRACTED".to_string(),
                        source_file: source_file.to_string(),
                        original_source: None,
                        original_target: None,
                        source_location: Some(format!("L{}", line_no)),
                        confidence_score: Some(1.0),
                        confidence_score_present: true,
                        weight: 1.0,
                        extra: Default::default(),
                    });
                }
            }

            if let Some(targets) = stem_to_entities.get(&import_statement.target_stem) {
                for imported_name in import_statement.imported_names {
                    if let Some(target_id) = targets.get(&imported_name) {
                        for source_id in local_classes {
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
    }

    for child in named_children(node) {
        collect_python_cross_file_import_edges(
            child,
            source,
            local_file_id,
            source_file,
            local_classes,
            module_to_file_id,
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
        module: module.to_string(),
        target_stem: target_stem.to_string(),
        imported_names,
    })
}

fn parse_python_import_statement_modules(node: TsNode<'_>, source: &[u8]) -> Vec<String> {
    let Some(raw) = node_text(node, source) else {
        return Vec::new();
    };
    raw.trim_start_matches("import")
        .trim()
        .replace(['\n', '\r'], " ")
        .split(',')
        .filter_map(|part| {
            let module = part.trim().split(" as ").next()?.trim();
            if module.is_empty() {
                None
            } else {
                Some(module.to_string())
            }
        })
        .collect()
}

fn python_module_candidates(path: &Path) -> Vec<String> {
    let mut components: Vec<String> = path
        .iter()
        .filter_map(|part| part.to_str().map(|s| s.to_string()))
        .collect();

    if components.is_empty() {
        return Vec::new();
    }

    if let Some(last) = components.last_mut() {
        if let Some(stripped) = last.strip_suffix(".py") {
            *last = stripped.to_string();
        }
    }

    let is_init = components.last().is_some_and(|name| name == "__init__");
    let end = if is_init {
        components.len().saturating_sub(1)
    } else {
        components.len()
    };
    if end == 0 {
        return Vec::new();
    }

    let mut candidates = Vec::new();
    for start in 0..end {
        let module = components[start..end].join(".");
        if !module.is_empty() {
            candidates.push(module);
        }
    }
    candidates
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

    if cfg.function_boundary_types.iter().any(|bt| *bt == t) {
        return;
    }

    if cfg.call_types.iter().any(|ct| *ct == t) {
        if let Some(callee) = resolve_callee(node, source, cfg) {
            pending_calls.push(PendingCall {
                caller_id: caller_id.to_string(),
                callee_name: callee,
                line_no: node.start_position().row + 1,
            });
        }
    }

    for child in named_children(node) {
        collect_calls(child, caller_id, source, cfg, pending_calls);
    }
}

fn resolve_callee(node: TsNode<'_>, source: &[u8], cfg: &LanguageConfig) -> Option<String> {
    let func_node = node
        .child_by_field_name(cfg.call_function_field)
        .or_else(|| named_children(node).into_iter().next())?;

    // Accessor call (obj.method())
    if cfg
        .call_accessor_node_types
        .iter()
        .any(|at| *at == func_node.kind())
    {
        if !cfg.call_accessor_field.is_empty() {
            if let Some(attr) = func_node.child_by_field_name(cfg.call_accessor_field) {
                return node_text(attr, source);
            }
        }
        // Last named child as fallback
        if let Some(last) = named_children(func_node).into_iter().last() {
            return node_text(last, source);
        }
    }

    if func_node.kind() == "scoped_identifier" {
        if let Some(name) = func_node.child_by_field_name("name").and_then(|node| node_text(node, source)) {
            return Some(name);
        }
        if let Some(last) = named_children(func_node).into_iter().last() {
            return node_text(last, source);
        }
    }

    node_text(func_node, source)
}

fn resolve_c_like_function_name(node: TsNode<'_>, source: &[u8], is_cpp: bool) -> Option<String> {
    if node.kind() == "identifier" {
        return node_text(node, source);
    }
    if is_cpp && node.kind() == "qualified_identifier" {
        if let Some(name_node) = node.child_by_field_name("name") {
            return node_text(name_node, source);
        }
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
    if !cfg.name_field.is_empty() {
        if let Some(name_node) = node.child_by_field_name(cfg.name_field) {
            return node_text(name_node, source);
        }
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

fn find_body<'a>(node: TsNode<'a>, cfg: &LanguageConfig) -> Option<TsNode<'a>> {
    if !cfg.body_field.is_empty() {
        if let Some(b) = node.child_by_field_name(cfg.body_field) {
            return Some(b);
        }
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
    if node.kind() == "identifier" || node.kind() == "type_identifier" {
        if let Some(text) = node_text(node, source) {
            ids.push(text);
        }
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
    for child in named_children(node) {
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
            break;
        }
        break;
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
        .replace('\r', " ")
        .replace('\n', " ")
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
        let normalized = node
            .label
            .trim_matches(|ch| ch == '(' || ch == ')')
            .trim_start_matches('.')
            .to_lowercase();
        if normalized.is_empty() {
            continue;
        }
        label_to_id.insert(normalized, node.id.clone());
    }
    label_to_id
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
    if let Some(rest) = text.strip_prefix("---\n") {
        if let Some(end) = rest.find("\n---\n") {
            return rest[end + 5..].to_string();
        }
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
        assert!(result
            .edges
            .iter()
            .any(|edge| edge.relation == "contains" && edge.target == "sample_text"));
        assert!(result
            .edges
            .iter()
            .all(|edge| !(edge.relation == "method" && edge.target == "sample_text")));
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
        let models_file_id = result
            .nodes
            .iter()
            .find(|node| {
                node.node_type.as_deref() == Some("file")
                    && node.source_file == models_path.to_string_lossy()
            })
            .map(|node| node.id.clone())
            .unwrap();

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports_from" && edge.target == models_file_id)
        );
        assert!(result.edges.iter().any(|edge| {
            edge.relation == "uses" && edge.confidence == "INFERRED" && edge.weight == 0.8
        }));
    }

    #[test]
    fn extracts_python_plain_import_edges_to_file_nodes() {
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
        let models_file_id = result
            .nodes
            .iter()
            .find(|node| {
                node.node_type.as_deref() == Some("file")
                    && node.source_file == models_path.to_string_lossy()
            })
            .map(|node| node.id.clone())
            .unwrap();

        assert!(
            result
                .edges
                .iter()
                .any(|edge| edge.relation == "imports" && edge.target == models_file_id)
        );
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
