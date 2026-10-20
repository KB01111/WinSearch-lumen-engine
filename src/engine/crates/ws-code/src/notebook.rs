//! `.ipynb` Jupyter Notebook parser for `ws-code`.
//!
//! Rules (P5.2):
//! - Extracts `code` and `markdown` cell `source` lines ONLY.
//! - **Never** reads or indexes cell `outputs` (`output_type`, `data`, `text`,
//!   `traceback`, base64 images, etc.) so notebook execution dumps never
//!   pollute code search or inflate `code.db`.
//! - Detects notebook language from `metadata.kernelspec.language` or
//!   `metadata.language_info.name` (defaulting to `"python"`), with per-cell
//!   magic overrides (`%%sql`, `%%bash`, `%%sh`, `%%html`, `%%javascript`,
//!   `%%js`, `%%ruby`, `%%r`).
//! - Preserves deterministic 1-based virtual line ranges across cells and runs
//!   symbol + chunk extraction on code cells.

use serde::Deserialize;

use crate::parse::{parse_source_with_offset, ParsedCodeFile};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotebookCell {
    pub cell_index: usize,
    pub cell_type: String,
    pub language: String,
    pub start_line: u32,
    pub end_line: u32,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedNotebook {
    pub default_language: String,
    pub cells: Vec<NotebookCell>,
    pub parsed: ParsedCodeFile,
}

#[derive(Debug, Deserialize)]
struct RawNotebook {
    #[serde(default)]
    metadata: RawMetadata,
    #[serde(default)]
    cells: Vec<RawCell>,
}

#[derive(Debug, Default, Deserialize)]
struct RawMetadata {
    #[serde(default)]
    kernelspec: Option<RawKernelSpec>,
    #[serde(default)]
    language_info: Option<RawLanguageInfo>,
}

#[derive(Debug, Deserialize)]
struct RawKernelSpec {
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawLanguageInfo {
    #[serde(default)]
    name: Option<String>,
}

/// Note: `outputs` is intentionally omitted from `RawCell` so serde skips cell
/// outputs entirely during deserialization.
#[derive(Debug, Deserialize)]
struct RawCell {
    #[serde(default)]
    cell_type: String,
    #[serde(default)]
    source: RawSource,
    #[serde(default)]
    metadata: Option<RawCellMetadata>,
}

#[derive(Debug, Default, Deserialize)]
struct RawCellMetadata {
    #[serde(default)]
    language: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawSource {
    Lines(Vec<String>),
    Single(String),
}

impl Default for RawSource {
    fn default() -> Self {
        Self::Lines(Vec::new())
    }
}

impl RawSource {
    fn into_text(self) -> String {
        match self {
            Self::Lines(lines) => lines.concat(),
            Self::Single(s) => s,
        }
    }
}

/// Parses a `.ipynb` JSON string into cells (without outputs), per-cell
/// languages, symbol-aware chunks, and extracted symbols with preserved line
/// numbers.
pub fn parse_notebook(raw_json: &str) -> Result<ParsedNotebook, serde_json::Error> {
    if raw_json.len() as u64 > crate::MAX_CODE_FILE_BYTES {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "notebook exceeds 2 MiB",
        )));
    }
    let raw: RawNotebook = serde_json::from_str(raw_json)?;
    let default_language = detect_notebook_language(&raw.metadata);

    let mut cells = Vec::new();
    let mut chunks = Vec::new();
    let mut symbols = Vec::new();
    let mut diagnostics = Vec::new();
    let mut current_line = 1u32;

    for (cell_index, raw_cell) in raw.cells.into_iter().enumerate() {
        let cell_type = raw_cell.cell_type.to_ascii_lowercase();
        if cell_type != "code" && cell_type != "markdown" {
            continue;
        }
        let source = raw_cell.source.into_text();
        if source.trim().is_empty() {
            continue;
        }

        let line_count = source.lines().count().max(1) as u32;
        let start_line = current_line;
        let end_line = start_line + line_count - 1;
        current_line = end_line + 1;

        let cell_lang = if cell_type == "markdown" {
            "markdown".to_string()
        } else {
            detect_cell_language(
                &source,
                raw_cell
                    .metadata
                    .as_ref()
                    .and_then(|m| m.language.as_deref()),
                &default_language,
            )
        };

        let mut cell_parsed = if cell_type == "code" && source.trim_start().starts_with("%%") {
            let magic_end = source.find('\n').map_or(source.len(), |i| i + 1);
            let mut parsed =
                parse_source_with_offset(&source[magic_end..], &cell_lang, start_line + 1);
            let mut magic =
                parse_source_with_offset(&source[..magic_end], "text", start_line).chunks;
            magic.append(&mut parsed.chunks);
            parsed.chunks = magic;
            parsed
        } else {
            parse_source_with_offset(&source, &cell_lang, start_line)
        };
        for mut ch in cell_parsed.chunks {
            ch.notebook = Some(crate::model::NotebookLocation {
                cell_ordinal: cell_index as u32,
                start_line: ch.start_line - start_line + 1,
                end_line: ch.end_line - start_line + 1,
            });
            if ch.symbol_name.is_empty() {
                ch.symbol_name = format!("cell_{}", cell_index + 1);
                ch.symbol_kind = if cell_type == "markdown" {
                    "markdown_cell".into()
                } else {
                    format!("notebook_cell:{cell_lang}")
                };
            }
            chunks.push(ch);
        }
        for sym in &mut cell_parsed.symbols {
            sym.notebook = Some(crate::model::NotebookLocation {
                cell_ordinal: cell_index as u32,
                start_line: sym.line - start_line + 1,
                end_line: sym.end_line - start_line + 1,
            });
        }
        symbols.extend(cell_parsed.symbols);
        for d in cell_parsed.diagnostics {
            if diagnostics.len() < 8 {
                diagnostics.push(format!("cell {cell_index}: {d}"));
            }
        }

        cells.push(NotebookCell {
            cell_index,
            cell_type,
            language: cell_lang,
            start_line,
            end_line,
            source,
        });
    }

    Ok(ParsedNotebook {
        default_language: default_language.clone(),
        cells,
        parsed: ParsedCodeFile {
            diagnostics,
            language: "notebook".to_string(),
            chunks,
            symbols,
        },
    })
}

fn detect_notebook_language(meta: &RawMetadata) -> String {
    if let Some(lang) = meta
        .language_info
        .as_ref()
        .and_then(|li| li.name.as_deref())
        .filter(|s| !s.trim().is_empty())
    {
        return normalize_lang(lang);
    }
    if let Some(ks) = meta.kernelspec.as_ref() {
        if let Some(lang) = ks.language.as_deref().filter(|s| !s.trim().is_empty()) {
            return normalize_lang(lang);
        }
        if let Some(name) = ks.name.as_deref().filter(|s| !s.trim().is_empty()) {
            return normalize_lang(name);
        }
    }
    "python".to_string()
}

fn detect_cell_language(source: &str, cell_meta_lang: Option<&str>, default_lang: &str) -> String {
    if let Some(l) = cell_meta_lang.filter(|s| !s.trim().is_empty()) {
        return normalize_lang(l);
    }
    if let Some(first_line) = source.lines().find(|l| !l.trim().is_empty()) {
        let t = first_line.trim();
        if let Some(magic) = t.strip_prefix("%%") {
            let magic_cmd = magic.split_whitespace().next().unwrap_or("");
            if let Some(lang) = detect_cell_magic_language(magic_cmd) {
                return lang;
            }
        }
    }
    default_lang.to_string()
}

fn detect_cell_magic_language(magic_cmd: &str) -> Option<String> {
    let lower = magic_cmd.trim().to_ascii_lowercase();
    match lower.as_str() {
        "python" | "python2" | "python3" | "py" | "pypy" | "r" | "ruby" | "perl" | "sh"
        | "bash" | "zsh" | "sql" | "html" | "svg" | "js" | "javascript" | "ts" | "typescript"
        | "rust" | "c" | "cpp" | "c++" | "cython" | "go" | "java" | "scala" | "julia" | "lua"
        | "markdown" | "md" | "latex" | "tex" => Some(normalize_lang(&lower)),
        _ => None,
    }
}

fn normalize_lang(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    match lower.as_str() {
        "py" | "python" | "python2" | "python3" | "pypy" | "ipython" => "python".into(),
        "rs" | "rust" | "evcxr" => "rust".into(),
        "js" | "javascript" | "node" => "javascript".into(),
        "ts" | "typescript" | "deno" => "typescript".into(),
        "sh" | "bash" | "zsh" | "shell" => "bash".into(),
        "ps1" | "pwsh" | "powershell" => "powershell".into(),
        "c#" | "csharp" | "dotnet-csharp" => "csharp".into(),
        "c++" | "cpp" => "cpp".into(),
        "sql" => "sql".into(),
        "html" | "svg" => "html".into(),
        "css" => "css".into(),
        "ruby" | "rb" => "ruby".into(),
        "go" | "golang" => "go".into(),
        "markdown" | "md" => "markdown".into(),
        "latex" | "tex" => "latex".into(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SymbolRole;

    #[test]
    fn source_identity_survives_non_code_cells_and_magic() {
        let source = r#"{"cells":[{"cell_type":"raw","source":"ignore"},{"cell_type":"markdown","source":"heading"},{"cell_type":"code","source":"%%python\ndef real():\n return helper()\n","outputs":[{"text":"fake"}]}]}"#;
        let nb = parse_notebook(source).unwrap();
        let sym = nb
            .parsed
            .symbols
            .iter()
            .find(|s| s.name == "real" && s.role.is_definition())
            .unwrap();
        let loc = sym.notebook.expect("cell identity must survive flattening");
        assert_eq!(loc.cell_ordinal, 2);
        assert_eq!(loc.start_line, 2);
        assert!(!nb.parsed.chunks.iter().any(|c| c.text.contains("fake")));
    }

    #[test]
    fn non_language_cell_magics_preserve_default_notebook_language() {
        let raw_ipynb = r##"{
  "metadata": {
    "language_info": { "name": "python" }
  },
  "cells": [
    {
      "cell_type": "code",
      "source": [
        "%%time\n",
        "def timed_worker(x):\n",
        "    return run_step(x)\n"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "%%capture\n",
        "def captured_worker(y):\n",
        "    return y\n"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "%%sql\n",
        "CREATE TABLE metrics (id INT);\n"
      ]
    }
  ]
}"##;

        let nb = parse_notebook(raw_ipynb).unwrap();
        assert_eq!(nb.cells.len(), 3);
        assert_eq!(nb.cells[0].language, "python");
        assert_eq!(nb.cells[1].language, "python");
        assert_eq!(nb.cells[2].language, "sql");
        assert!(nb
            .parsed
            .symbols
            .iter()
            .any(|s| { s.name == "timed_worker" && s.role == SymbolRole::Definition }));
        assert!(nb
            .parsed
            .symbols
            .iter()
            .any(|s| { s.name == "captured_worker" && s.role == SymbolRole::Definition }));
    }
}
