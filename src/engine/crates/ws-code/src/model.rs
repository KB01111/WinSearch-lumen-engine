//! Shared value types for the `ws-code` index.

use serde::{Deserialize, Serialize};

/// Schema version recorded in `code.db`'s `meta` table.
pub const CODE_SCHEMA_VERSION: &str = "2";

/// Expected production embedding width (EmbeddingGemma 2 unified space).
pub const EMBEDDING_DIM: usize = 768;

/// Outcome of upserting one code file (used for counters, rename tracking, and tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileUpsertOutcome {
    /// Newly discovered file.
    Inserted { id: i64 },
    /// Existing file updated in place at the same path (mtime/size changed).
    Updated { id: i64 },
    /// Existing file moved/renamed from an old path (matched by `(volume_serial, file_id)`).
    Renamed { id: i64 },
    /// Unchanged mtime/size/path.
    Unchanged { id: i64 },
}

impl FileUpsertOutcome {
    pub fn file_id(&self) -> i64 {
        match *self {
            Self::Inserted { id }
            | Self::Updated { id }
            | Self::Renamed { id }
            | Self::Unchanged { id } => id,
        }
    }
}

/// Discovered repository or standalone code folder under a configured code root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectedRepo {
    /// Absolute path of the repository root (or the configured root itself when no `.git` exists).
    pub root_path: String,
    /// Display name (typically the final path component).
    pub name: String,
    /// True if a `.git` directory or `.git` worktree file was found at `root_path`.
    pub has_git: bool,
}

/// Stored repository row in `code.db`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoRow {
    pub id: i64,
    pub root_path: String,
    pub name: String,
    pub has_git: bool,
    pub enabled: bool,
    pub current_generation: i64,
}

/// One discovered source file produced by the gitignore-aware code crawler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeFileRecord {
    /// Absolute filesystem path.
    pub path: String,
    /// Path relative to the repo root using forward slashes (`src/lib.rs`).
    pub rel_path: String,
    /// File name (`lib.rs`).
    pub name: String,
    /// Lowercase extension without leading dot (`rs`, `py`, `ipynb`).
    pub ext: String,
    /// Canonical language identifier (`rust`, `python`, `notebook`, `text`, ...).
    pub language: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modification time (Unix seconds).
    pub mtime: i64,
    /// Windows volume serial number for rename preservation.
    pub volume_serial: u32,
    /// Windows 64-bit file identifier for rename preservation.
    pub file_id: u64,
}

/// Stored code file row in `code.db`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeFileRow {
    pub id: i64,
    pub repo_id: i64,
    pub path: String,
    pub rel_path: String,
    pub name: String,
    pub name_lower: String,
    pub ext: String,
    pub language: String,
    pub size: u64,
    pub mtime: i64,
    pub volume_serial: u32,
    pub file_id: u64,
    pub scan_generation: i64,
}

/// Input for inserting or replacing a code chunk in `code.db`.
#[derive(Debug, Clone)]
pub struct NewCodeChunk<'a> {
    pub notebook: Option<NotebookLocation>,
    pub file_id: i64,
    pub chunk_index: i64,
    pub start_line: u32,
    pub end_line: u32,
    pub symbol_name: &'a str,
    pub symbol_kind: &'a str,
    pub text: &'a str,
    /// Optional embedding vector (`f32`); when empty (`&[]`), stored with empty BLOB and `norm = 0.0`
    /// until the background embedder populates it.
    pub vector: &'a [f32],
}

/// Stored code chunk row in `code.db`.
#[derive(Debug, Clone, PartialEq)]
pub struct CodeChunk {
    pub notebook: Option<NotebookLocation>,
    pub id: i64,
    pub file_id: i64,
    pub chunk_index: i64,
    pub start_line: u32,
    pub end_line: u32,
    pub symbol_name: String,
    pub symbol_kind: String,
    pub text: String,
    /// Normalized embedding decoded from FP16 (empty if not yet embedded).
    pub vector: Vec<f32>,
    /// Original L2 norm before normalization (`0.0` if not yet embedded).
    pub norm: f32,
}

/// Scored code chunk from vector or lexical search.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredCodeChunk {
    pub chunk: CodeChunk,
    pub score: f32,
}

/// Role of a symbol occurrence (definitions rank above references in Code search).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolRole {
    Definition,
    Reference,
}

impl SymbolRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Definition => "definition",
            Self::Reference => "reference",
        }
    }

    pub fn is_definition(self) -> bool {
        matches!(self, Self::Definition)
    }
}

/// Input for inserting a symbol occurrence into `code.db`.
#[derive(Debug, Clone)]
pub struct NewCodeSymbol<'a> {
    pub notebook: Option<NotebookLocation>,
    pub file_id: i64,
    pub name: &'a str,
    pub kind: &'a str,
    pub role: SymbolRole,
    pub line: u32,
    pub end_line: u32,
    pub signature: &'a str,
}

/// Stored symbol row in `code.db`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeSymbol {
    pub notebook: Option<NotebookLocation>,
    pub id: i64,
    pub file_id: i64,
    pub name: String,
    pub name_lower: String,
    pub kind: String,
    pub is_definition: bool,
    pub line: u32,
    pub end_line: u32,
    pub signature: String,
}

/// Map a lowercase extension or filename to a canonical language tag.
pub fn language_for_file(name: &str, ext: &str) -> &'static str {
    let lower_name = name.to_ascii_lowercase();
    match lower_name.as_str() {
        "dockerfile" => return "dockerfile",
        "makefile" | "gnumakefile" => return "makefile",
        "cmakelists.txt" => return "cmake",
        _ => {}
    }
    match ext.to_ascii_lowercase().as_str() {
        "rs" => "rust",
        "c" | "h" => "c",
        "cc" | "cpp" | "cxx" | "hpp" | "hh" | "hxx" => "cpp",
        "cs" => "csharp",
        "py" | "pyi" => "python",
        "js" | "mjs" | "cjs" | "jsx" => "javascript",
        "ts" | "mts" | "cts" | "tsx" => "typescript",
        "go" => "go",
        "java" => "java",
        "kt" | "kts" => "kotlin",
        "swift" => "swift",
        "php" => "php",
        "rb" => "ruby",
        "sql" => "sql",
        "ps1" | "psm1" | "psd1" => "powershell",
        "sh" | "bash" | "zsh" => "bash",
        "html" | "htm" => "html",
        "css" | "scss" | "less" => "css",
        "yaml" | "yml" => "yaml",
        "toml" => "toml",
        "json" | "jsonc" => "json",
        "md" | "markdown" => "markdown",
        "xml" | "csproj" | "props" | "targets" | "sln" => "xml",
        "ipynb" => "notebook",
        _ => "text",
    }
}

/// Virtual notebook coordinates, never a physical JSON line. Ordinal is zero-based;
/// local line numbers are one-based and include the cell magic line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotebookLocation {
    pub cell_ordinal: u32,
    pub start_line: u32,
    pub end_line: u32,
}

/// Revision of persisted native syntax content; zero denotes stale heuristics.
pub const CODE_PARSE_VERSION: i64 = 1;
