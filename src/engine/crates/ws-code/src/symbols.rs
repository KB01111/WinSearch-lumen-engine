//! Language-aware symbol extraction (definitions vs. references/call sites) for
//! all 20 supported languages in `ws-code`:
//! Rust, C, C++, C#, Python, JavaScript, TypeScript, Go, Java, Kotlin, Swift,
//! PHP, Ruby, SQL, PowerShell, Bash, HTML, CSS, YAML, and TOML.

use crate::model::SymbolRole;

/// One extracted symbol occurrence (definition or reference) with 1-based line
/// numbers and its cleaned source signature line.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExtractedSymbol {
    #[serde(default)]
    pub notebook: Option<crate::model::NotebookLocation>,
    pub name: String,
    pub kind: String,
    pub role: SymbolRole,
    pub line: u32,
    pub end_line: u32,
    pub signature: String,
}

/// Supported grammar families for structural parsing and symbol extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrammarKind {
    Rust,
    C,
    Cpp,
    CSharp,
    Python,
    JavaScript,
    TypeScript,
    Go,
    Java,
    Kotlin,
    Swift,
    Php,
    Ruby,
    Sql,
    PowerShell,
    Bash,
    Html,
    Css,
    Yaml,
    Toml,
    FallbackText,
}

impl GrammarKind {
    pub fn from_language(lang: &str) -> Self {
        match lang.to_ascii_lowercase().as_str() {
            "rust" => Self::Rust,
            "c" => Self::C,
            "cpp" | "c++" => Self::Cpp,
            "csharp" | "c#" => Self::CSharp,
            "python" => Self::Python,
            "javascript" | "js" => Self::JavaScript,
            "typescript" | "ts" | "tsx" => Self::TypeScript,
            "go" => Self::Go,
            "java" => Self::Java,
            "kotlin" => Self::Kotlin,
            "swift" => Self::Swift,
            "php" => Self::Php,
            "ruby" => Self::Ruby,
            "sql" => Self::Sql,
            "powershell" => Self::PowerShell,
            "bash" | "sh" | "shell" => Self::Bash,
            "html" | "xml" => Self::Html,
            "css" => Self::Css,
            "yaml" => Self::Yaml,
            "toml" => Self::Toml,
            _ => Self::FallbackText,
        }
    }

    pub fn uses_brace_blocks(self) -> bool {
        matches!(
            self,
            Self::Rust
                | Self::C
                | Self::Cpp
                | Self::CSharp
                | Self::JavaScript
                | Self::TypeScript
                | Self::Go
                | Self::Java
                | Self::Kotlin
                | Self::Swift
                | Self::Php
                | Self::PowerShell
                | Self::Bash
                | Self::Css
        )
    }

    pub fn uses_indentation_blocks(self) -> bool {
        matches!(self, Self::Python | Self::Yaml)
    }
}

pub fn extract_symbols(source: &str, language: &str) -> Vec<ExtractedSymbol> {
    extract_symbols_with_offset(source, language, 1)
}
pub fn extract_symbols_with_offset(
    source: &str,
    language: &str,
    base_line: u32,
) -> Vec<ExtractedSymbol> {
    crate::syntax::parse_symbols(source, language, base_line).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn annotation_skip_detects_method_and_handles_multibyte() {
        let src = "@Override public void run() { // café\n    work();\n}\n";
        let syms = extract_symbols(src, "java");
        assert!(
            syms.iter()
                .any(|s| s.name == "run" && s.role == SymbolRole::Definition),
            "got {syms:?}"
        );
    }

    #[test]
    fn brace_end_ignores_braces_in_strings_and_comments() {
        let src = "fn demo() {\n    let s = \"}\"; // }\n    let x = 1;\n}\n";
        let syms = extract_symbols(src, "rust");
        let def = syms
            .iter()
            .find(|s| s.name == "demo" && s.role == SymbolRole::Definition)
            .expect("demo definition");
        assert_eq!(def.line, 1);
        assert_eq!(def.end_line, 4);
    }

    #[test]
    fn ruby_end_counts_nested_blocks() {
        let src = "def compute(x)\n  if x > 0\n    items.each do |i|\n      puts(i)\n    end\n  end\n  x + 1\nend\n";
        let syms = extract_symbols(src, "ruby");
        let def = syms
            .iter()
            .find(|s| s.name == "compute" && s.role == SymbolRole::Definition)
            .expect("compute definition");
        assert_eq!(def.line, 1);
        assert_eq!(def.end_line, 8);
    }

    #[test]
    fn multiline_docstring_does_not_emit_false_symbols() {
        let src = "def real_fn():\n    \"\"\"\n    def hidden_in_docstring():\n        fake_call()\n    \"\"\"\n    return helper_call()\n";
        let syms = extract_symbols(src, "python");
        assert!(syms
            .iter()
            .any(|s| s.name == "real_fn" && s.role == SymbolRole::Definition));
        assert!(
            !syms.iter().any(|s| s.name == "hidden_in_docstring"),
            "docstring code should not be indexed: {syms:?}"
        );
        assert!(
            !syms.iter().any(|s| s.name == "fake_call"),
            "docstring calls should not be indexed: {syms:?}"
        );
        assert!(syms
            .iter()
            .any(|s| s.name == "helper_call" && s.role == SymbolRole::Reference));
    }

    #[test]
    fn callable_some_is_indexed_as_reference() {
        let src = "const ok = items.some(pred);\n";
        let syms = extract_symbols(src, "typescript");
        assert!(
            syms.iter()
                .any(|s| s.name == "some" && s.role == SymbolRole::Reference),
            "expected 'some' call reference, got {syms:?}"
        );
    }

    #[test]
    fn rust_const_and_const_fn_and_swift_class_and_class_func_are_indexed() {
        let rust_src =
            "pub const LIMIT: usize = 4;\npub const fn calc() -> usize {\n    LIMIT\n}\n";
        let rust_syms = extract_symbols(rust_src, "rust");
        assert!(
            rust_syms.iter().any(|s| s.name == "LIMIT"
                && s.kind == "constant"
                && s.role == SymbolRole::Definition),
            "expected Rust const LIMIT, got {rust_syms:?}"
        );
        assert!(
            rust_syms.iter().any(|s| s.name == "calc"
                && s.kind == "function"
                && s.role == SymbolRole::Definition),
            "expected Rust const fn calc, got {rust_syms:?}"
        );

        let swift_src = "public class Widget {\n    public class func make() {}\n}\n";
        let swift_syms = extract_symbols(swift_src, "swift");
        assert!(
            swift_syms.iter().any(|s| s.name == "Widget"
                && s.kind == "class"
                && s.role == SymbolRole::Definition),
            "expected Swift class Widget, got {swift_syms:?}"
        );
        assert!(
            swift_syms.iter().any(|s| s.name == "make"
                && s.kind == "function"
                && s.role == SymbolRole::Definition),
            "expected Swift class func make, got {swift_syms:?}"
        );
    }
}
