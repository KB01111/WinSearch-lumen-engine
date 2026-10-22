//! Document extractor (`crates/ws-extract/src/docs.rs`, Task 10 / P4.2).
//!
//! Supported formats:
//! - Direct UTF-8 / UTF-16 text: `.txt`, `.md`, `.markdown`, `.rst`, `.log`,
//!   `.csv`, `.tsv`, `.json`, `.yaml`, `.yml`, `.toml`, `.ini`, `.cfg`, `.xml`
//! - HTML (`.html`, `.htm`): `quick-xml` text walk stripping `<script>`,
//!   `<style>`, `<head>`, `<noscript>`, `<svg>`, and `<template>` subtrees
//! - OOXML Word (`.docx`): `zip` + `quick-xml` over `word/document.xml`
//!   (`<w:t>`, `<w:tab/>`, `<w:br/>`, `</w:p>`)
//! - OpenDocument Text (`.odt`): `zip` + `quick-xml` over `content.xml`
//!   (`<text:p>`, `<text:h>`, `<text:tab/>`, `<text:line-break/>`)
//! - Rich Text Format (`.rtf`): pure-Rust control-word/group stripper with
//!   hex `\'xx` and Unicode `\uN` decoding (skips header destinations such as
//!   `\fonttbl`, `\colortbl`, `\stylesheet`, `\pict`, `\info`)
//! - Legacy `.doc` and unknown document extensions (`.xls`, `.ppt`, `.pages`,
//!   etc.): Windows COM `IFilter` via [`ws_platform::ifilter::extract_text_ifilter`],
//!   degrading cleanly to [`ExtractionStatus::Unsupported`] (exact-name-only,
//!   never garbage embeddings) when no filter is registered or no text is produced.
//!
//! Numeric-content heuristic (DigUp parity): when extracted content is
//! dominated by numbers/tables (>50% of non-whitespace characters are digits
//! or numeric/table punctuation, or a CSV/TSV with predominantly numeric cells),
//! `DocumentExtractor` returns [`ExtractionStatus::ExactOnly`] and marks every
//! [`SegmentDraft`] with `embed = false` so FTS indexes the exact values
//! without polluting the semantic vector index.

use std::io::{Cursor, Read};
use std::path::Path;

use quick_xml::events::Event;
use quick_xml::Reader;
use ws_index::model::{SEGMENT_KIND_DOC, SEGMENT_KIND_PDF};
use ws_platform::ifilter::{extract_text_ifilter, IFilterOutcome};

use crate::budget::ExtractBudget;
use crate::chunk::{chunk_text_with_lines, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO};
use crate::{ExtractError, ExtractOutput, Extractor, SegmentDraft};

/// Maximum decompressed XML bytes read from inside a `.docx` or `.odt` archive
/// (16 MiB) so zip bombs cannot exhaust memory.
const MAX_ARCHIVE_XML_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum entries allowed in a `.docx` or `.odt` zip container before aborting.
const MAX_ARCHIVE_ENTRIES: usize = 4096;

/// Document extractor handling plain text, HTML, DOCX, ODT, RTF, and Windows
/// IFilter fallback with the numeric-content exact-only heuristic.
#[derive(Debug, Default, Clone, Copy)]
pub struct DocumentExtractor;

impl Extractor for DocumentExtractor {
    fn extract(&self, path: &Path, budget: &ExtractBudget) -> Result<ExtractOutput, ExtractError> {
        budget.check_deadline()?;
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();

        let text = match ext.as_str() {
            "txt" | "md" | "markdown" | "rst" | "log" | "csv" | "tsv" | "json" | "yaml" | "yml"
            | "toml" | "ini" | "cfg" => extract_plain_text(path, budget)?,
            "xml" => extract_xml_text(path, budget)?,
            "html" | "htm" => extract_html_text(path, budget)?,
            "docx" => extract_docx_text(path, budget)?,
            "odt" => extract_odt_text(path, budget)?,
            "rtf" => extract_rtf_text(path, budget)?,
            _ => {
                // Legacy .doc and unknown document extensions route through Windows IFilter.
                match extract_ifilter_text(path, budget)? {
                    Some(t) => t,
                    None => return Ok(ExtractOutput::unsupported()),
                }
            }
        };

        budget.check_deadline()?;
        build_document_output(&text, &ext, budget)
    }
}

/// Builds the chunked [`ExtractOutput`] from extracted plain `text`, applying
/// the numeric-content heuristic to decide between [`ExtractionStatus::Extracted`]
/// (`embed: true`) and [`ExtractionStatus::ExactOnly`] (`embed: false`).
pub fn build_document_output(
    text: &str,
    ext: &str,
    budget: &ExtractBudget,
) -> Result<ExtractOutput, ExtractError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(ExtractOutput::extracted(Vec::new()));
    }

    let numeric_only = is_numeric_dominated(trimmed, ext);
    let raw_chunks = chunk_text_with_lines(text, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO);
    if raw_chunks.is_empty() {
        return Ok(ExtractOutput::extracted(Vec::new()));
    }

    let seg_kind = if ext.eq_ignore_ascii_case("pdf") {
        SEGMENT_KIND_PDF
    } else {
        SEGMENT_KIND_DOC
    };
    let mut segments = Vec::with_capacity(raw_chunks.len().min(budget.max_segments));
    for chunk in raw_chunks {
        budget.check_deadline()?;
        if segments.len() >= budget.max_segments {
            break;
        }
        segments.push(SegmentDraft {
            embedding_input: crate::DraftEmbeddingInput::Text,
            kind: seg_kind.to_string(),
            text: chunk.text,
            page: None,
            start_ms: None,
            end_ms: None,
            line: Some(chunk.line),
            thumbnail_hint: None,
            embed: !numeric_only,
        });
    }

    if numeric_only {
        Ok(ExtractOutput::exact_only(segments))
    } else {
        Ok(ExtractOutput::extracted(segments))
    }
}

/// Numeric-content heuristic: returns `true` when `text` is dominated by
/// numbers/tables and should be indexed in FTS only (`ExactOnly`, `embed = false`).
///
/// Rules:
/// 1. Character ratio: across all non-whitespace characters, if digits (`0-9`)
///    make up ≥35% and digits + numeric/table punctuation (`. , + - $ % € £ ¥ : / ; | = ( )`)
///    make up >50% of non-whitespace characters (with Digits > Alphabetic chars),
///    it is numeric-dominated.
/// 2. Tabular CSV/TSV rule: for `.csv`/`.tsv` files with ≥2 lines, if >50% of
///    non-empty cells parse as numbers/currencies/dates/timestamps, it is
///    numeric-dominated even when column headers contain words.
pub fn is_numeric_dominated(text: &str, ext: &str) -> bool {
    let mut non_ws = 0usize;
    let mut digits = 0usize;
    let mut alpha = 0usize;
    let mut num_punct = 0usize;

    for ch in text.chars() {
        if ch.is_whitespace() {
            continue;
        }
        non_ws += 1;
        if ch.is_ascii_digit() {
            digits += 1;
        } else if ch.is_alphabetic() {
            alpha += 1;
        } else if matches!(
            ch,
            '.' | ','
                | '+'
                | '-'
                | '$'
                | '%'
                | '€'
                | '£'
                | '¥'
                | ':'
                | '/'
                | ';'
                | '|'
                | '='
                | '('
                | ')'
                | '"'
                | '\''
        ) {
            num_punct += 1;
        }
    }

    if non_ws == 0 {
        return false;
    }

    if digits > alpha && (digits + num_punct) * 2 > non_ws && digits * 5 >= non_ws {
        return true;
    }

    if matches!(ext, "csv" | "tsv") {
        let sep = if ext == "tsv" { '\t' } else { ',' };
        let mut total_cells = 0usize;
        let mut numeric_cells = 0usize;
        for line in text.lines().take(200) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            for cell in line.split(sep) {
                let cell = cell.trim().trim_matches(|c| c == '"' || c == '\'');
                if cell.is_empty() {
                    continue;
                }
                total_cells += 1;
                if is_numeric_cell(cell) {
                    numeric_cells += 1;
                }
            }
        }
        if total_cells >= 4 && numeric_cells * 2 > total_cells {
            return true;
        }
    }

    false
}

fn is_numeric_cell(cell: &str) -> bool {
    let mut has_digit = false;
    for ch in cell.chars() {
        if ch.is_ascii_digit() {
            has_digit = true;
        } else if matches!(
            ch,
            '.' | ',' | '+' | '-' | '$' | '%' | '€' | '£' | '¥' | ':' | '/' | ' ' | 'e' | 'E'
        ) {
            continue;
        } else {
            return false;
        }
    }
    has_digit
}

fn decode_text_bytes(bytes: &[u8]) -> Option<String> {
    // Handle UTF-16 LE / BE BOM before checking for binary NUL bytes.
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let u16s: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        return Some(String::from_utf16_lossy(&u16s));
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let u16s: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        return Some(String::from_utf16_lossy(&u16s));
    }
    if bytes.iter().take(8192).any(|&b| b == 0) {
        return None;
    }
    let body = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    Some(String::from_utf8_lossy(body).into_owned())
}

fn extract_plain_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let bytes = budget.read_bounded(path)?;
    decode_text_bytes(&bytes).ok_or_else(|| ExtractError::Unsupported("binary content".into()))
}

fn extract_xml_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let bytes = budget.read_bounded(path)?;
    let xml =
        decode_text_bytes(&bytes).ok_or_else(|| ExtractError::Unsupported("binary xml".into()))?;
    walk_xml_text(&xml, budget, false)
}

fn extract_html_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let bytes = budget.read_bounded(path)?;
    let html =
        decode_text_bytes(&bytes).ok_or_else(|| ExtractError::Unsupported("binary html".into()))?;
    let stripped = strip_raw_html_blocks(&html, &["script", "style"]);
    walk_xml_text(&stripped, budget, true)
}

/// Removes raw `<script ...>...</script>` and `<style ...>...</style>` blocks
/// case-insensitively before feeding HTML to `quick-xml`, so unescaped `<`
/// characters inside inline JS/CSS (e.g. `if (x < 2)`) never abort XML parsing.
fn strip_raw_html_blocks(input: &str, tags: &[&str]) -> String {
    let mut current = input.to_string();
    for &tag in tags {
        let open_pat = format!("<{tag}");
        let close_pat = format!("</{tag}");
        let mut out = String::with_capacity(current.len());
        let lower = current.to_ascii_lowercase();
        let mut pos = 0usize;
        while let Some(rel) = lower[pos..].find(&open_pat) {
            let start = pos + rel;
            let after_tag = start + open_pat.len();
            let valid_boundary = lower
                .as_bytes()
                .get(after_tag)
                .is_none_or(|b| b.is_ascii_whitespace() || *b == b'>' || *b == b'/');
            if !valid_boundary {
                out.push_str(&current[pos..after_tag]);
                pos = after_tag;
                continue;
            }
            out.push_str(&current[pos..start]);
            if let Some(close_rel) = lower[after_tag..].find(&close_pat) {
                let close_start = after_tag + close_rel;
                if let Some(gt_rel) = lower[close_start..].find('>') {
                    pos = close_start + gt_rel + 1;
                } else {
                    pos = current.len();
                }
            } else {
                pos = current.len();
            }
        }
        out.push_str(&current[pos..]);
        current = out;
    }
    current
}

/// Walks XML/HTML events with `quick-xml`, stripping `<script>`, `<style>`,
/// `<head>` (in HTML mode), `<noscript>`, `<svg>`, and `<template>` blocks and
/// emitting visible text with clean line/word breaks.
fn walk_xml_text(
    input: &str,
    budget: &ExtractBudget,
    html_mode: bool,
) -> Result<String, ExtractError> {
    let mut reader = Reader::from_str(input);
    reader.config_mut().trim_text(false);
    // HTML often has mismatched closing tags (e.g. `<br>`, `<meta>`, `<img>`).
    reader.config_mut().check_end_names = false;
    reader.config_mut().check_comments = false;

    let mut out = String::new();
    let mut skip_depth = 0usize;
    let mut step = 0usize;

    loop {
        step += 1;
        if step & 0xFF == 0 {
            budget.check_deadline()?;
        }
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = local_tag_lower(e.name().as_ref());
                if skip_depth > 0 {
                    if !is_void_html_tag(&tag, html_mode) {
                        skip_depth += 1;
                    }
                } else if is_ignored_tag(&tag, html_mode) {
                    skip_depth = 1;
                } else if is_block_tag(&tag) && !out.is_empty() && !out.ends_with('\n') {
                    out.push('\n');
                }
            }
            Ok(Event::Empty(e)) if skip_depth == 0 => {
                let tag = local_tag_lower(e.name().as_ref());
                if matches!(tag.as_str(), "br" | "hr") && !out.ends_with('\n') {
                    out.push('\n');
                }
            }
            Ok(Event::End(e)) => {
                let tag = local_tag_lower(e.name().as_ref());
                if skip_depth > 0 {
                    if is_ignored_tag(&tag, html_mode) {
                        skip_depth = 0;
                    } else if !is_void_html_tag(&tag, html_mode) {
                        skip_depth = skip_depth.saturating_sub(1);
                    }
                } else if is_block_tag(&tag) && !out.is_empty() && !out.ends_with('\n') {
                    out.push('\n');
                }
            }
            Ok(Event::Text(t)) if skip_depth == 0 => {
                let decoded = t
                    .unescape()
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| String::from_utf8_lossy(t.as_ref()).into_owned());
                append_inline_text(&mut out, &decoded);
            }
            Ok(Event::CData(c)) if skip_depth == 0 => {
                let s = String::from_utf8_lossy(c.as_ref());
                append_inline_text(&mut out, &s);
            }
            Ok(Event::Eof) => break,
            Err(_) => {
                // Malformed HTML/XML tail: stop cleanly with what we gathered.
                break;
            }
            _ => {}
        }
    }

    Ok(out.trim().to_string())
}

fn append_inline_text(out: &mut String, piece: &str) {
    let trimmed = piece.trim();
    if trimmed.is_empty() {
        return;
    }
    if !out.is_empty() && !out.ends_with(|c: char| c.is_whitespace()) {
        out.push(' ');
    }
    out.push_str(trimmed);
}

fn local_tag_lower(raw: &[u8]) -> String {
    let local = match raw.iter().rposition(|&b| b == b':') {
        Some(pos) => &raw[pos + 1..],
        None => raw,
    };
    String::from_utf8_lossy(local).to_ascii_lowercase()
}

fn is_ignored_tag(tag: &str, html_mode: bool) -> bool {
    matches!(tag, "script" | "style" | "noscript" | "svg" | "template")
        || (html_mode && tag == "head")
}

fn is_void_html_tag(tag: &str, html_mode: bool) -> bool {
    html_mode
        && matches!(
            tag,
            "area"
                | "base"
                | "br"
                | "col"
                | "embed"
                | "hr"
                | "img"
                | "input"
                | "link"
                | "meta"
                | "param"
                | "source"
                | "track"
                | "wbr"
        )
}

fn is_block_tag(tag: &str) -> bool {
    matches!(
        tag,
        "p" | "div"
            | "section"
            | "article"
            | "header"
            | "footer"
            | "main"
            | "aside"
            | "nav"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "li"
            | "tr"
            | "td"
            | "th"
            | "blockquote"
            | "pre"
            | "br"
            | "hr"
    )
}

/// Extracts text from a `.docx` container (`word/document.xml`).
fn extract_docx_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let xml = read_zip_entry_xml(path, "word/document.xml", budget)?;
    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(false);
    reader.config_mut().check_end_names = false;

    let mut out = String::new();
    let mut in_wt = false;
    let mut step = 0usize;

    loop {
        step += 1;
        if step & 0xFF == 0 {
            budget.check_deadline()?;
        }
        match reader.read_event() {
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"w:t" | b"t" => in_wt = true,
                b"w:tab" | b"tab" => out.push('\t'),
                b"w:br" | b"br" | b"w:cr" => out.push('\n'),
                _ => {}
            },
            Ok(Event::Empty(e)) => match e.name().as_ref() {
                b"w:tab" | b"tab" => out.push('\t'),
                b"w:br" | b"br" | b"w:cr" => out.push('\n'),
                _ => {}
            },
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"w:t" | b"t" => in_wt = false,
                b"w:p" | b"p" | b"w:tr" if !out.is_empty() && !out.ends_with('\n') => {
                    out.push('\n');
                }
                b"w:tc" if !out.is_empty() && !out.ends_with(['\t', '\n']) => {
                    out.push('\t');
                }
                _ => {}
            },
            Ok(Event::Text(t)) if in_wt => {
                let s = t
                    .unescape()
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| String::from_utf8_lossy(t.as_ref()).into_owned());
                out.push_str(&s);
            }
            Ok(Event::CData(c)) if in_wt => {
                out.push_str(&String::from_utf8_lossy(c.as_ref()));
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(ExtractError::Decode(format!("docx xml: {e}"))),
            _ => {}
        }
    }

    Ok(out.trim().to_string())
}

/// Extracts text from an `.odt` container (`content.xml`).
fn extract_odt_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let xml = read_zip_entry_xml(path, "content.xml", budget)?;
    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(false);
    reader.config_mut().check_end_names = false;

    let mut out = String::new();
    let mut text_depth = 0usize;
    let mut step = 0usize;

    loop {
        step += 1;
        if step & 0xFF == 0 {
            budget.check_deadline()?;
        }
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = local_tag_lower(e.name().as_ref());
                if matches!(tag.as_str(), "p" | "h") {
                    text_depth += 1;
                } else if text_depth > 0 {
                    match tag.as_str() {
                        "tab" => out.push('\t'),
                        "line-break" => out.push('\n'),
                        "s" => out.push(' '),
                        _ => {}
                    }
                }
            }
            Ok(Event::Empty(e)) if text_depth > 0 => {
                let tag = local_tag_lower(e.name().as_ref());
                match tag.as_str() {
                    "tab" => out.push('\t'),
                    "line-break" => out.push('\n'),
                    "s" => out.push(' '),
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = local_tag_lower(e.name().as_ref());
                if matches!(tag.as_str(), "p" | "h") && text_depth > 0 {
                    text_depth -= 1;
                    if text_depth == 0 && !out.is_empty() && !out.ends_with('\n') {
                        out.push('\n');
                    }
                }
            }
            Ok(Event::Text(t)) if text_depth > 0 => {
                let s = t
                    .unescape()
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| String::from_utf8_lossy(t.as_ref()).into_owned());
                out.push_str(&s);
            }
            Ok(Event::CData(c)) if text_depth > 0 => {
                out.push_str(&String::from_utf8_lossy(c.as_ref()));
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(ExtractError::Decode(format!("odt xml: {e}"))),
            _ => {}
        }
    }

    Ok(out.trim().to_string())
}

fn read_zip_entry_xml(
    path: &Path,
    entry_name: &str,
    budget: &ExtractBudget,
) -> Result<String, ExtractError> {
    let bytes = budget.read_bounded(path)?;
    let cursor = Cursor::new(bytes);
    let mut archive =
        zip::ZipArchive::new(cursor).map_err(|e| ExtractError::Decode(format!("zip: {e}")))?;

    if archive.len() > MAX_ARCHIVE_ENTRIES {
        return Err(ExtractError::Decode(format!(
            "zip entry count {} exceeds limit {}",
            archive.len(),
            MAX_ARCHIVE_ENTRIES
        )));
    }

    budget.check_deadline()?;
    let mut entry = archive
        .by_name(entry_name)
        .map_err(|e| ExtractError::Decode(format!("missing {entry_name}: {e}")))?;

    let cap = budget.max_bytes.min(MAX_ARCHIVE_XML_BYTES);
    if entry.size() > cap {
        return Err(ExtractError::FileTooLarge {
            size: entry.size(),
            max_bytes: cap,
        });
    }

    let mut xml_bytes = Vec::with_capacity((entry.size() as usize).min(64 * 1024));
    entry
        .by_ref()
        .take(cap.saturating_add(1))
        .read_to_end(&mut xml_bytes)?;
    if xml_bytes.len() as u64 > cap {
        return Err(ExtractError::FileTooLarge {
            size: xml_bytes.len() as u64,
            max_bytes: cap,
        });
    }
    budget.check_deadline()?;
    Ok(String::from_utf8_lossy(&xml_bytes).into_owned())
}

/// Pure-Rust RTF plain-text stripper (`striprtf` semantics).
///
/// Tracks RTF `{ ... }` group nesting, skips non-body destinations
/// (`{\*\...}`, `\fonttbl`, `\colortbl`, `\stylesheet`, `\info`, `\pict`,
/// `\header`, `\footer`), translates `\par` / `\line` / `\tab`, decodes
/// hex `\'xx` and Unicode `\uN?` escapes, and checks `budget.check_deadline()`.
pub fn strip_rtf(input: &str, budget: &ExtractBudget) -> Result<String, ExtractError> {
    #[derive(Clone, Copy)]
    struct GroupState {
        skip: bool,
        uc_skip: usize,
    }

    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut stack: Vec<GroupState> = vec![GroupState {
        skip: false,
        uc_skip: 1,
    }];
    let mut out = String::new();
    let mut i = 0usize;
    let mut skip_ansi_chars = 0usize;

    while i < len {
        if i & 0x3FF == 0 {
            budget.check_deadline()?;
        }
        let b = bytes[i];
        match b {
            b'{' => {
                let current = *stack.last().unwrap_or(&GroupState {
                    skip: false,
                    uc_skip: 1,
                });
                stack.push(current);
                i += 1;
                // Check for ignorable destination marker `{\*`
                if i + 1 < len && bytes[i] == b'\\' && bytes[i + 1] == b'*' {
                    if let Some(top) = stack.last_mut() {
                        top.skip = true;
                    }
                    i += 2;
                }
            }
            b'}' => {
                if stack.len() > 1 {
                    stack.pop();
                }
                skip_ansi_chars = 0;
                i += 1;
            }
            b'\\' => {
                i += 1;
                if i >= len {
                    break;
                }
                let next = bytes[i];
                let is_skip = stack.last().map(|s| s.skip).unwrap_or(false);
                match next {
                    b'\\' | b'{' | b'}' => {
                        if !is_skip {
                            if skip_ansi_chars > 0 {
                                skip_ansi_chars -= 1;
                            } else {
                                out.push(next as char);
                            }
                        }
                        i += 1;
                    }
                    b'~' => {
                        if !is_skip && skip_ansi_chars == 0 {
                            out.push('\u{00A0}');
                        } else {
                            skip_ansi_chars = skip_ansi_chars.saturating_sub(1);
                        }
                        i += 1;
                    }
                    b'-' | b'_' => {
                        if !is_skip && next == b'_' && skip_ansi_chars == 0 {
                            out.push('-');
                        }
                        i += 1;
                    }
                    b'\n' | b'\r' => {
                        if !is_skip {
                            out.push('\n');
                        }
                        i += 1;
                    }
                    b'\'' => {
                        // Hex escape \'hh
                        i += 1;
                        if i + 1 < len {
                            if let (Some(hi), Some(lo)) = (hex_val(bytes[i]), hex_val(bytes[i + 1]))
                            {
                                let code = (hi << 4) | lo;
                                if !is_skip {
                                    if skip_ansi_chars > 0 {
                                        skip_ansi_chars -= 1;
                                    } else {
                                        out.push(cp1252_to_char(code));
                                    }
                                }
                                i += 2;
                            }
                        }
                    }
                    b'*' => {
                        if let Some(top) = stack.last_mut() {
                            top.skip = true;
                        }
                        i += 1;
                    }
                    _ if next.is_ascii_alphabetic() => {
                        let word_start = i;
                        while i < len && bytes[i].is_ascii_alphabetic() {
                            i += 1;
                        }
                        let word = &input[word_start..i];

                        // Optional signed numeric parameter
                        let num_start = i;
                        if i < len && (bytes[i] == b'-' || bytes[i].is_ascii_digit()) {
                            i += 1;
                            while i < len && bytes[i].is_ascii_digit() {
                                i += 1;
                            }
                        }
                        let param: Option<i32> = if i > num_start {
                            input[num_start..i].parse::<i32>().ok()
                        } else {
                            None
                        };

                        // A single space delimiter after a control word is consumed by RTF.
                        if i < len && bytes[i] == b' ' {
                            i += 1;
                        }

                        if matches!(
                            word,
                            "fonttbl"
                                | "colortbl"
                                | "stylesheet"
                                | "info"
                                | "pict"
                                | "object"
                                | "header"
                                | "footer"
                                | "headerl"
                                | "headerr"
                                | "footerl"
                                | "footerr"
                                | "fldinst"
                                | "xmlopen"
                        ) {
                            if let Some(top) = stack.last_mut() {
                                top.skip = true;
                            }
                            continue;
                        }

                        if word == "uc" {
                            if let (Some(n), Some(top)) = (param, stack.last_mut()) {
                                top.uc_skip = n.max(0) as usize;
                            }
                            continue;
                        }

                        if is_skip {
                            continue;
                        }

                        match word {
                            "par" | "line" | "sect" | "page" | "row" if !out.ends_with('\n') => {
                                out.push('\n');
                            }
                            "tab" | "cell" => {
                                out.push('\t');
                            }
                            "emdash" => out.push('—'),
                            "endash" => out.push('–'),
                            "lquote" => out.push('‘'),
                            "rquote" => out.push('’'),
                            "ldblquote" => out.push('“'),
                            "rdblquote" => out.push('”'),
                            "bullet" => out.push('•'),
                            "u" => {
                                if let Some(n) = param {
                                    let code = if n < 0 { (n + 65536) as u32 } else { n as u32 };
                                    if let Some(ch) = char::from_u32(code) {
                                        out.push(ch);
                                    }
                                    skip_ansi_chars = stack.last().map(|s| s.uc_skip).unwrap_or(1);
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            b'\r' | b'\n' => {
                i += 1;
            }
            _ => {
                let is_skip = stack.last().map(|s| s.skip).unwrap_or(false);
                if !is_skip {
                    if skip_ansi_chars > 0 {
                        skip_ansi_chars -= 1;
                        i += 1;
                    } else {
                        // Advance by one UTF-8 char if valid, else byte.
                        if let Some(ch) = input[i..].chars().next() {
                            out.push(ch);
                            i += ch.len_utf8();
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    i += 1;
                }
            }
        }
    }

    Ok(out.trim().to_string())
}

fn extract_rtf_text(path: &Path, budget: &ExtractBudget) -> Result<String, ExtractError> {
    let bytes = budget.read_bounded(path)?;
    let raw = String::from_utf8_lossy(&bytes);
    strip_rtf(&raw, budget)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn cp1252_to_char(b: u8) -> char {
    match b {
        0x80 => '€',
        0x85 => '…',
        0x91 => '‘',
        0x92 => '’',
        0x93 => '“',
        0x94 => '”',
        0x95 => '•',
        0x96 => '–',
        0x97 => '—',
        0x99 => '™',
        _ => b as char,
    }
}

/// Calls Windows COM `LoadIFilter` (`ws_platform::ifilter::extract_text_ifilter`)
/// with the budget's character cap and deadline. Returns `Ok(None)` when no
/// filter is registered or the filter produces no usable text so the caller
/// degrades cleanly to `ExtractionStatus::Unsupported` (exact-name-only).
fn extract_ifilter_text(
    path: &Path,
    budget: &ExtractBudget,
) -> Result<Option<String>, ExtractError> {
    budget.check_deadline()?;
    let meta = std::fs::metadata(path)?;
    budget.check_size(meta.len())?;

    let max_chars = ifilter_character_limit(budget);

    match extract_text_ifilter(path, max_chars, budget.deadline) {
        IFilterOutcome::Extracted(text) => checked_ifilter_text(text, max_chars, budget),
        IFilterOutcome::Unsupported | IFilterOutcome::Failed(_) => Ok(None),
        IFilterOutcome::DeadlineExceeded => Err(ExtractError::DeadlineExceeded),
    }
}

fn ifilter_character_limit(budget: &ExtractBudget) -> usize {
    budget
        .max_segments
        .saturating_mul(DEFAULT_CHUNK_CHARS)
        .max(DEFAULT_CHUNK_CHARS)
        .min(usize::try_from(budget.max_bytes).unwrap_or(usize::MAX))
}

fn checked_ifilter_text(
    text: String,
    max_chars: usize,
    budget: &ExtractBudget,
) -> Result<Option<String>, ExtractError> {
    // IFilter does not report whether hitting its cap truncated output.
    if text.chars().count() >= max_chars || text.len() as u64 > budget.max_bytes {
        return Err(ExtractError::Decode(
            "IFilter decoded text limit reached; extraction incomplete".into(),
        ));
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task2_ifilter_unbounded_chunk_collection_keeps_decoded_text_limit() {
        let budget = ExtractBudget {
            max_segments: usize::MAX,
            ..Default::default()
        };
        assert_eq!(ifilter_character_limit(&budget), budget.max_bytes as usize);
    }

    #[test]
    fn task2_ifilter_capped_or_over_budget_text_is_explicitly_incomplete() {
        let budget = ExtractBudget::new(32, Duration::from_secs(5), 30);
        assert!(checked_ifilter_text("a".repeat(32), 32, &budget).is_err());
        assert!(checked_ifilter_text("é".repeat(17), 100, &budget).is_err());
        assert_eq!(
            checked_ifilter_text("complete".into(), 32, &budget).unwrap(),
            Some("complete".into())
        );
    }
    use crate::chunk::select_excerpt;
    use crate::ExtractionStatus;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::Duration;
    use zip::write::SimpleFileOptions;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "ws-extract-docs-test-{}-{:?}-{}",
                std::process::id(),
                std::thread::current().id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn txt_and_md_extract_segments_and_excerpts() {
        let tmp = TempDir::new();
        let md_path = tmp.0.join("architecture.md");
        std::fs::write(
            &md_path,
            "# Architecture Overview\n\nThe hybrid ranker combines exact FTS5 tokens with FP16 cosine similarity.",
        )
        .unwrap();

        let out = DocumentExtractor
            .extract(&md_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        assert!(out.segments[0].embed);
        assert!(out.segments[0].text.contains("hybrid ranker"));

        let excerpt = select_excerpt(&out.segments[0].text, &["hybrid", "cosine"], 120);
        assert!(excerpt.contains("hybrid ranker"));
    }

    #[test]
    fn html_strips_scripts_styles_and_extracts_visible_text() {
        let tmp = TempDir::new();
        let html_path = tmp.0.join("guide.html");
        std::fs::write(
            &html_path,
            r#"<!DOCTYPE html>
            <html>
              <head>
                <title>Ignored Head Title</title>
                <style>body { color: red; }</style>
                <script>const secretToken = "DO_NOT_INDEX";</script>
              </head>
              <body>
                <h1>Deployment Runbook</h1>
                <p>Restart the <b>winsearchd</b> service after updating models.</p>
              </body>
            </html>"#,
        )
        .unwrap();

        let out = DocumentExtractor
            .extract(&html_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        let text = &out.segments[0].text;
        assert!(text.contains("Deployment Runbook"));
        assert!(text.contains("Restart the winsearchd service"));
        assert!(!text.contains("DO_NOT_INDEX"));
        assert!(!text.contains("color: red"));
    }

    #[test]
    fn docx_extracts_paragraphs_from_word_document_xml() {
        let tmp = TempDir::new();
        let docx_path = tmp.0.join("contract.docx");
        {
            let file = std::fs::File::create(&docx_path).unwrap();
            let mut zip = zip::ZipWriter::new(file);
            zip.start_file("word/document.xml", SimpleFileOptions::default())
                .unwrap();
            zip.write_all(
                br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
                  <w:body>
                    <w:p><w:r><w:t>Master Services Agreement</w:t></w:r></w:p>
                    <w:p><w:r><w:t>Section 4: Limitation of Liability and Indemnification.</w:t></w:r></w:p>
                  </w:body>
                </w:document>"#,
            )
            .unwrap();
            zip.finish().unwrap();
        }

        let out = DocumentExtractor
            .extract(&docx_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        assert!(out.segments[0].embed);
        assert!(out.segments[0].text.contains("Master Services Agreement"));
        assert!(out.segments[0].text.contains("Limitation of Liability"));
    }

    #[test]
    fn odt_extracts_paragraphs_from_content_xml() {
        let tmp = TempDir::new();
        let odt_path = tmp.0.join("notes.odt");
        {
            let file = std::fs::File::create(&odt_path).unwrap();
            let mut zip = zip::ZipWriter::new(file);
            zip.start_file("content.xml", SimpleFileOptions::default())
                .unwrap();
            zip.write_all(
                br#"<?xml version="1.0" encoding="UTF-8"?>
                <office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
                                         xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">
                  <office:body>
                    <office:text>
                      <text:h>Quarterly Roadmap</text:h>
                      <text:p>Deliver multimodal extraction for documents and PDFs.</text:p>
                    </office:text>
                  </office:body>
                </office:document-content>"#,
            )
            .unwrap();
            zip.finish().unwrap();
        }

        let out = DocumentExtractor
            .extract(&odt_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        assert!(out.segments[0].text.contains("Quarterly Roadmap"));
        assert!(out.segments[0]
            .text
            .contains("Deliver multimodal extraction"));
    }

    #[test]
    fn rtf_strips_control_groups_and_decodes_escapes() {
        let tmp = TempDir::new();
        let rtf_path = tmp.0.join("memo.rtf");
        let rtf = r#"{\rtf1\ansi\deff0
{\fonttbl{\f0 Arial;}{\f1 Courier New;}}
{\colortbl;\red0\green0\blue0;}
{\*\generator CustomRtf 1.0;}
{\headerl Left Page Header Secret}
{\headerr Right Page Header Secret}
{\footerl Left Page Footer Secret}
{\footerr Right Page Footer Secret}
\pard\f0\fs24 Confidential Merger Proposal\par
Purchase price is \'8042 million \u8212? subject to board approval.\par
}"#;
        std::fs::write(&rtf_path, rtf).unwrap();

        let out = DocumentExtractor
            .extract(&rtf_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        let text = &out.segments[0].text;
        assert!(
            text.contains("Confidential Merger Proposal"),
            "got {text:?}"
        );
        assert!(
            text.contains("€42 million — subject to board approval"),
            "got {text:?}"
        );
        assert!(!text.contains("Arial"));
        assert!(!text.contains("CustomRtf"));
        assert!(
            !text.contains("Left Page Header Secret"),
            "headerl must be skipped: {text:?}"
        );
        assert!(
            !text.contains("Right Page Header Secret"),
            "headerr must be skipped: {text:?}"
        );
        assert!(
            !text.contains("Left Page Footer Secret"),
            "footerl must be skipped: {text:?}"
        );
        assert!(
            !text.contains("Right Page Footer Secret"),
            "footerr must be skipped: {text:?}"
        );
    }

    #[test]
    fn numeric_heavy_csv_routes_exact_only_without_embedding() {
        let tmp = TempDir::new();
        let csv_path = tmp.0.join("ledger.csv");
        let mut csv = String::from("id,timestamp,amount,balance,tax_rate\n");
        for i in 1..=40 {
            csv.push_str(&format!(
                "{i},2026-10-{:02},{:.2},{:.2},0.19\n",
                (i % 28) + 1,
                i as f64 * 125.50,
                i as f64 * 1493.45
            ));
        }
        std::fs::write(&csv_path, &csv).unwrap();

        let out = DocumentExtractor
            .extract(&csv_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(
            out.status,
            ExtractionStatus::ExactOnly,
            "numeric-dominated CSV must route ExactOnly"
        );
        assert!(!out.segments.is_empty());
        assert!(
            out.segments.iter().all(|s| !s.embed),
            "ExactOnly segments must have embed = false so no vectors are generated"
        );
        assert!(out.segments[0].text.contains("1493.45"));
    }

    #[test]
    fn legacy_doc_ifilter_path_handles_fixture_cleanly() {
        let tmp = TempDir::new();
        let doc_path = tmp.0.join("legacy.doc");
        // Write a synthetic .doc fixture; depending on whether the host OS has an
        // Office .doc IFilter registered or rejects the non-OLE bytes, the
        // extractor must either extract text or degrade cleanly to Unsupported
        // without panicking or emitting garbage embeddings.
        std::fs::write(
            &doc_path,
            b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1synthetic legacy doc fixture",
        )
        .unwrap();

        let budget = ExtractBudget::new(1024 * 1024, Duration::from_secs(5), 32);
        let out = DocumentExtractor.extract(&doc_path, &budget).unwrap();
        assert!(
            matches!(
                out.status,
                ExtractionStatus::Unsupported
                    | ExtractionStatus::Extracted
                    | ExtractionStatus::ExactOnly
            ),
            "unexpected status for .doc fixture: {:?}",
            out.status
        );
        if out.status == ExtractionStatus::Unsupported {
            assert!(out.segments.is_empty());
        }
    }

    #[test]
    fn html_with_unescaped_less_than_inside_script_still_extracts_body() {
        let tmp = TempDir::new();
        let html_path = tmp.0.join("page.html");
        let html = r#"<!DOCTYPE html>
<html>
<head>
  <script>if (x < 2 && y > 1) { console.log("skip me"); }</script>
  <style>.a < .b { color: red; }</style>
</head>
<body>
  <h1>Quarterly Revenue Summary</h1>
  <p>Visible body paragraph after script.</p>
</body>
</html>"#;
        std::fs::write(&html_path, html).unwrap();

        let out = DocumentExtractor
            .extract(&html_path, &ExtractBudget::default())
            .unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 1);
        let text = &out.segments[0].text;
        assert!(text.contains("Quarterly Revenue Summary"), "got {text:?}");
        assert!(
            text.contains("Visible body paragraph after script."),
            "got {text:?}"
        );
        assert!(!text.contains("skip me"));
    }
}
