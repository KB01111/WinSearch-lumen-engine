//! `ws-code`: repository detection, `.gitignore`-aware source crawling,
//! syntax/symbol-aware chunking across 20 languages + `.ipynb` notebooks, and
//! the standalone `code.db` SQLite store (files, chunks, symbols, FTS5, and
//! FP16 chunk vectors).
//!
//! Isolated by design: `code.db` (`ws_platform::paths::code_db_path()`) never
//! shares tables or vectors with `index.db`.

pub mod crawl;
pub mod model;
pub mod notebook;
pub mod parse;
pub mod store;
pub mod symbols;
mod syntax;

use std::path::Path;

pub use crawl::{
    crawl_repo, detect_repos, detect_repos_with_depth, file_identity, has_git_marker,
    is_excluded_code_dir, is_excluded_code_file, is_probably_binary, record_for_code_path,
    EXCLUDED_CODE_DIRS, EXCLUDED_CODE_EXTS, EXCLUDED_CODE_FILES, EXCLUDED_CODE_SUFFIXES,
    MAX_CODE_FILE_BYTES,
};
pub use model::{
    language_for_file, CodeChunk, CodeFileRecord, CodeFileRow, CodeSymbol, DetectedRepo,
    FileUpsertOutcome, NewCodeChunk, NewCodeSymbol, RepoRow, ScoredCodeChunk, SymbolRole,
    CODE_SCHEMA_VERSION, EMBEDDING_DIM,
};
pub use notebook::{parse_notebook, NotebookCell, ParsedNotebook};
pub use parse::{
    parse_file, parse_source, parse_source_with_offset, persist_parsed_file, ParsedCodeFile,
    ParsedCodeUnit, FALLBACK_CHUNK_LINES, FALLBACK_CHUNK_OVERLAP,
};
pub use store::{CodeIndexStats, CodeStore, CodeStoreError};
pub use symbols::{extract_symbols, extract_symbols_with_offset, ExtractedSymbol, GrammarKind};

/// High-level helper: upserts `record` into `store`, reads and parses its
/// source (or `.ipynb` cells without outputs), populates `code_symbols` and
/// `code_chunks` in `code.db`, and returns `(file_id, chunk_ids, parsed)`.
pub fn index_code_file(
    store: &mut CodeStore,
    repo_id: i64,
    scan_generation: i64,
    record: &CodeFileRecord,
) -> anyhow::Result<(i64, Vec<i64>, ParsedCodeFile)> {
    let raw = parse::read_source(Path::new(&record.path))?;
    let parsed = if record.language.eq_ignore_ascii_case("notebook")
        || record.language.eq_ignore_ascii_case("jupyter")
        || record.ext.eq_ignore_ascii_case("ipynb")
    {
        parse_notebook(&raw)
            .map_err(|e| anyhow::anyhow!("invalid notebook {}: {e}", record.path))?
            .parsed
    } else {
        parse_source(
            &raw,
            parse::file_parse_language(Path::new(&record.path), &record.language),
        )
    };
    let outcomes =
        store.apply_file_batch(repo_id, scan_generation, std::slice::from_ref(record))?;
    let file_id = outcomes
        .first()
        .map(|o| o.file_id())
        .ok_or_else(|| anyhow::anyhow!("apply_file_batch returned no outcome"))?;
    let chunk_ids = persist_parsed_file(store, file_id, &parsed)?;
    Ok((file_id, chunk_ids, parsed))
}
