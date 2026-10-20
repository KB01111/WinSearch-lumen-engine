//! Native syntax trees. Occurrences are syntactic, without cross-file resolution.
use crate::{
    model::SymbolRole,
    symbols::{ExtractedSymbol, GrammarKind},
};
use std::collections::HashSet;
use tree_sitter::{Language, Node, Parser};

fn language(kind: GrammarKind, tag: &str) -> Option<Language> {
    Some(match kind {
        GrammarKind::Rust => tree_sitter_rust::LANGUAGE.into(),
        GrammarKind::C => tree_sitter_c::LANGUAGE.into(),
        GrammarKind::Cpp => tree_sitter_cpp::LANGUAGE.into(),
        GrammarKind::CSharp => tree_sitter_c_sharp::LANGUAGE.into(),
        GrammarKind::Python => tree_sitter_python::LANGUAGE.into(),
        GrammarKind::JavaScript => tree_sitter_javascript::LANGUAGE.into(),
        GrammarKind::TypeScript if tag == "tsx" => tree_sitter_typescript::LANGUAGE_TSX.into(),
        GrammarKind::TypeScript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        GrammarKind::Go => tree_sitter_go::LANGUAGE.into(),
        GrammarKind::Java => tree_sitter_java::LANGUAGE.into(),
        GrammarKind::Kotlin => tree_sitter_kotlin_ng::LANGUAGE.into(),
        GrammarKind::Swift => tree_sitter_swift::LANGUAGE.into(),
        GrammarKind::Php => tree_sitter_php::LANGUAGE_PHP.into(),
        GrammarKind::Ruby => tree_sitter_ruby::LANGUAGE.into(),
        GrammarKind::Sql => tree_sitter_sqlite3::LANGUAGE.into(),
        GrammarKind::PowerShell => tree_sitter_powershell::LANGUAGE.into(),
        GrammarKind::Bash => tree_sitter_bash::LANGUAGE.into(),
        GrammarKind::Html => tree_sitter_html::LANGUAGE.into(),
        GrammarKind::Css => tree_sitter_css::LANGUAGE.into(),
        GrammarKind::Yaml => tree_sitter_yaml::LANGUAGE.into(),
        GrammarKind::Toml => tree_sitter_toml_ng::LANGUAGE.into(),
        GrammarKind::FallbackText => return None,
    })
}

fn text<'a>(n: Node<'_>, source: &'a str) -> &'a str {
    &source[n.byte_range()]
}

fn child_kind<'a>(n: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = n.walk();
    let found = n.named_children(&mut cursor).find(|c| c.kind() == kind);
    found
}

fn definition(n: Node<'_>, grammar: GrammarKind) -> Option<(Node<'_>, &'static str)> {
    let k = n.kind();
    let kind = match k {
        "function_item"
        | "function_definition"
        | "function_declaration"
        | "method_definition"
        | "method_declaration"
        | "method"
        | "singleton_method"
        | "function_statement" => "function",
        "class_declaration" | "class_definition" | "class_specifier" | "class"
        | "class_statement" => "class",
        "struct_item" | "struct_specifier" => "struct",
        "enum_item" | "enum_declaration" => "enum",
        "trait_item" | "interface_declaration" | "protocol_declaration" => "interface",
        "mod_item" | "module" => "module",
        "const_item" => "constant",
        "static_item" => "variable",
        "type_item" | "type_spec" | "type_alias_declaration" | "typealias_declaration" => "type",
        "create_table_statement" => "table",
        "block_mapping_pair" | "flow_pair" => "key",
        "pair" if grammar == GrammarKind::Toml => "key",
        "table" | "table_array_element" if grammar == GrammarKind::Toml => "table",
        "keyframes_statement" => "keyframes",
        _ => return None,
    };
    let name = n
        .child_by_field_name("name")
        .or_else(|| n.child_by_field_name("key"))
        .or_else(|| child_kind(n, "function_name"))
        .or_else(|| child_kind(n, "keyframes_name"))
        .or_else(|| {
            if grammar == GrammarKind::Toml {
                n.named_child(0)
            } else {
                None
            }
        })
        .or_else(|| {
            let mut d = n.child_by_field_name("declarator")?;
            while let Some(next) = d.child_by_field_name("declarator") {
                d = next;
            }
            Some(d)
        })?;
    Some((name, kind))
}

pub(crate) fn parse_symbols(
    source: &str,
    tag: &str,
    base: u32,
) -> (Vec<ExtractedSymbol>, Vec<String>) {
    if source.len() as u64 > crate::MAX_CODE_FILE_BYTES {
        return (Vec::new(), vec!["source exceeds 2 MiB".into()]);
    }
    let grammar = GrammarKind::from_language(tag);
    let Some(language) = language(grammar, tag) else {
        return (Vec::new(), Vec::new());
    };
    let mut parser = Parser::new();
    if let Err(e) = parser.set_language(&language) {
        return (Vec::new(), vec![format!("grammar ABI: {e}")]);
    }
    let Some(tree) = parser.parse(source, None) else {
        return (Vec::new(), vec!["syntax parsing interrupted".into()]);
    };
    let mut diagnostics = Vec::new();
    if tree.root_node().has_error() {
        diagnostics.push("syntax error: recovered partial tree".into());
    }
    let mut out = Vec::new();
    let mut names = HashSet::new();
    let mut cursor = tree.walk();
    loop {
        let n = cursor.node();
        let k = n.kind();
        let skip = k.contains("comment")
            || k.contains("string")
            || k.contains("heredoc")
            || matches!(k, "raw_text" | "text" | "character_literal");
        let mut emit =
            |name: Node<'_>, kind: &str, role: SymbolRole, range: Node<'_>, value: &str| {
                if value.is_empty() || value.len() > 1024 || !names.insert(name.id()) {
                    return;
                }
                let signature = text(range, source).chars().take(256).collect::<String>();
                out.push(ExtractedSymbol {
                    name: value.to_string(),
                    kind: kind.into(),
                    role,
                    line: base + range.start_position().row as u32,
                    end_line: base + range.end_position().row as u32
                        - u32::from(
                            range.end_position().column == 0
                                && range.end_byte() > range.start_byte(),
                        ),
                    signature,
                    notebook: None,
                });
            };
        if !skip {
            if let Some((name, kind)) = definition(n, grammar) {
                emit(name, kind, SymbolRole::Definition, n, text(name, source));
            }
            if grammar == GrammarKind::Html && k == "attribute" {
                if let (Some(key), Some(value)) = (n.named_child(0), n.named_child(1)) {
                    let key = text(key, source);
                    let value_text = text(value, source).trim_matches(['\'', '"']);
                    if key == "id" {
                        emit(value, "id", SymbolRole::Definition, n, value_text);
                    } else if key == "href" && value_text.starts_with('#') {
                        emit(value, "id", SymbolRole::Reference, n, &value_text[1..]);
                    }
                }
            }
            if grammar == GrammarKind::Css
                && matches!(k, "class_name" | "keyframes_name" | "id_name")
            {
                emit(n, "selector", SymbolRole::Definition, n, text(n, source));
            }
            if grammar == GrammarKind::Yaml && k == "anchor_name" {
                emit(n, "anchor", SymbolRole::Definition, n, text(n, source));
            }
            if grammar == GrammarKind::Yaml && k == "alias_name" {
                emit(n, "alias", SymbolRole::Reference, n, text(n, source));
            }
            let identifier = k == "identifier"
                || k == "type_identifier"
                || k == "field_identifier"
                || k == "property_identifier"
                || k == "simple_identifier"
                || k == "name"
                || k == "command_name"
                || k == "command_name_expr"
                || (grammar == GrammarKind::Bash && k == "word")
                || (grammar == GrammarKind::Css && matches!(k, "plain_value" | "function_name"));
            if identifier {
                emit(n, "reference", SymbolRole::Reference, n, text(n, source));
            }
        }
        if !skip && cursor.goto_first_child() {
            continue;
        }
        loop {
            if cursor.goto_next_sibling() {
                break;
            }
            if !cursor.goto_parent() {
                out.sort_by_key(|s| (s.line, !s.role.is_definition()));
                return (out, diagnostics);
            }
        }
    }
}
