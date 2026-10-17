//! Content extraction trait, per-file budgets, overlapping text chunker,
//! and baseline plain-text extraction for WinSearch (Phase 4, P4.1).

pub mod audio;
pub mod budget;
pub mod chunk;
pub mod code;
pub mod decoder;
pub mod docs;
pub mod image;
pub mod ocr;
pub mod pdf;
pub mod video;
pub mod worker;

use std::path::{Path, PathBuf};

use ws_index::model::{
    SEGMENT_KIND_AUDIO, SEGMENT_KIND_DOC, SEGMENT_KIND_IMAGE, SEGMENT_KIND_PDF, SEGMENT_KIND_VIDEO,
};

pub use budget::{
    ExtractBudget, DEFAULT_EXTRACT_TIMEOUT, DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_SEGMENTS,
};
pub use chunk::{
    chunk_text, chunk_text_default, chunk_text_with_lines, select_excerpt, select_excerpt_at, TextChunk,
    DEFAULT_CHUNK_CHARS, DEFAULT_EXCERPT_CHARS, DEFAULT_OVERLAP_CHARS, DEFAULT_OVERLAP_RATIO,
};
pub use docs::{build_document_output, is_numeric_dominated, strip_rtf, DocumentExtractor};
pub use image::{
    is_screenshot_path, parse_exif_from_bytes, stat_thumbnail_key, thumbnail_cache_path,
    ExifMetadata, ImageExtractor, DEFAULT_MAX_IMAGE_EDGE, DEFAULT_THUMBNAIL_MAX_EDGE,
};
pub use ocr::{
    build_ocr_segments, OcrProvider, OcrResultData, OcrWordBox, WinRtOcrProvider,
    DEFAULT_MAX_OCR_LANGUAGES,
};
pub use ws_index::model::DraftEmbeddingInput;

/// Outcome status of running an [`Extractor`] on a file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExtractionStatus {
    /// Content was extracted (into embeddable and/or FTS-indexed segments).
    Extracted,
    /// Content is exact-only (indexed in FTS, no semantic vectors emitted —
    /// e.g. numeric-dominated tables or unsupported binary formats).
    ExactOnly,
    /// File format is not supported by this extractor; keep name-only indexing.
    Unsupported,
    /// Extraction failed; caller records diagnostics and falls back to name-only.
    Failed(String),
}

/// One extracted segment draft before embedding / storage in `ws-index`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentDraft {
    pub kind: String,
    pub text: String,
    pub page: Option<u32>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub line: Option<u32>,
    pub thumbnail_hint: Option<PathBuf>,
    /// True if this segment should be embedded into `segments` (in addition to
    /// FTS content indexing when `text` is non-empty). False for `ExactOnly`
    /// segments that only feed FTS.
    pub embed: bool,
    #[serde(default)]
    pub embedding_input: DraftEmbeddingInput,
}

impl SegmentDraft {
    /// Constructs a text segment draft (`embed = true`, no page/time/thumbnail).
    pub fn text(kind: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            text: text.into(),
            page: None,
            start_ms: None,
            end_ms: None,
            line: None,
            thumbnail_hint: None,
            embed: true,
            embedding_input: DraftEmbeddingInput::Text,
        }
    }
}

/// Output produced by an [`Extractor::extract`] call.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExtractOutput {
    pub status: ExtractionStatus,
    pub segments: Vec<SegmentDraft>,
}

impl ExtractOutput {
    pub fn extracted(segments: Vec<SegmentDraft>) -> Self {
        Self {
            status: ExtractionStatus::Extracted,
            segments,
        }
    }

    pub fn exact_only(segments: Vec<SegmentDraft>) -> Self {
        Self {
            status: ExtractionStatus::ExactOnly,
            segments,
        }
    }

    pub fn unsupported() -> Self {
        Self {
            status: ExtractionStatus::Unsupported,
            segments: Vec::new(),
        }
    }
}

/// Errors returned by [`Extractor::extract`] when a budget is blown or I/O fails.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("extraction deadline exceeded")]
    DeadlineExceeded,
    #[error("file size {size} exceeds budget {max_bytes} bytes")]
    FileTooLarge { size: u64, max_bytes: u64 },
    #[error("segment count {count} exceeds budget {max_segments}")]
    SegmentLimitExceeded { count: usize, max_segments: usize },
    #[error("unsupported format: {0}")]
    Unsupported(String),
    #[error("decode error: {0}")]
    Decode(String),
}

/// Common interface for modality extractors (`doc`, `pdf`, `image`, `audio`, `video`).
pub trait Extractor: Send + Sync {
    /// Extracts [`SegmentDraft`]s from `path` while respecting `budget`.
    fn extract(&self, path: &Path, budget: &ExtractBudget) -> Result<ExtractOutput, ExtractError>;

    /// Stable chunk ordinal cursor. Parsing stays within the original byte/deadline bounds;
    /// only the requested window crosses the process boundary.
    fn extract_batch(
        &self,
        path: &Path,
        budget: &ExtractBudget,
        cursor: u64,
    ) -> Result<worker::ExtractResponse, ExtractError> {
        let full_budget = ExtractBudget {
            max_segments: usize::MAX,
            ..budget.clone()
        };
        let mut output = self.extract(path, &full_budget)?;
        budget.check_deadline()?;
        let total = output.segments.len() as u64;
        let start = usize::try_from(cursor)
            .unwrap_or(usize::MAX)
            .min(output.segments.len());
        let end = start
            .saturating_add(budget.max_segments.min(30))
            .min(output.segments.len());
        output.segments = output.segments.drain(start..end).collect();
        let successful = matches!(
            output.status,
            ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
        );
        Ok(worker::ExtractResponse { code: None,
            output,
            next_cursor: (successful && (end as u64) < total).then_some(end as u64),
            total_units: successful.then_some(total),
            diagnostics: Vec::new(),
            media: None,
            video_selection: None,
        })
    }
}

/// Maps a lowercase file extension (without dot) to the `extract_jobs.kind`
/// modality that should process the file (`doc`, `pdf`, `image`, `audio`,
/// `video`), or `None` when the file is name-only.
pub fn modality_kind_for_ext(ext: &str) -> Option<&'static str> {
    let ext = ext.trim_start_matches('.').to_ascii_lowercase();
    match ext.as_str() {
        "pdf" => Some(SEGMENT_KIND_PDF),
        "txt" | "md" | "markdown" | "rst" | "csv" | "tsv" | "log" | "json" | "yaml" | "yml"
        | "toml" | "ini" | "cfg" | "html" | "htm" | "xml" | "rtf" | "doc" | "docx" | "odt" => {
            Some(SEGMENT_KIND_DOC)
        }
        "jpg" | "jpeg" | "png" | "webp" | "heic" | "heif" | "avif" | "tiff" | "tif" | "bmp"
        | "gif" | "raw" | "cr2" | "cr3" | "nef" | "arw" | "dng" | "orf" | "rw2" => {
            Some(SEGMENT_KIND_IMAGE)
        }
        "mp3" | "wav" | "flac" | "m4a" | "aac" | "ogg" | "wma" | "opus" | "aiff" => {
            Some(SEGMENT_KIND_AUDIO)
        }
        "mp4" | "mkv" | "mov" | "avi" | "webm" | "wmv" | "m4v" | "mpg" | "mpeg" => {
            Some(SEGMENT_KIND_VIDEO)
        }
        _ => None,
    }
}

/// Baseline UTF-8 plain-text extractor for `.txt`, `.md`, `.log`, `.csv`, etc.
/// Reads within `budget`, chunks with [`chunk_text_with_lines`] (~1200 chars,
/// ~15% overlap), and checks [`ExtractBudget::check_deadline`] and
/// `max_segments` on every chunk. Richer document formats (`docx`, `odt`,
/// `rtf`, `html`, `IFilter`, numeric heuristics) expand this in P4.2 (Task 10).
#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTextExtractor;

impl Extractor for PlainTextExtractor {
    fn extract(&self, path: &Path, budget: &ExtractBudget) -> Result<ExtractOutput, ExtractError> {
        budget.check_deadline()?;
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        if !matches!(
            ext.as_str(),
            "txt" | "md" | "markdown" | "rst" | "csv" | "tsv" | "log" | "json" | "yaml" | "yml"
                | "toml" | "ini" | "cfg"
        ) {
            return Ok(ExtractOutput::unsupported());
        }

        let bytes = budget.read_bounded(path)?;
        // Reject obvious binary files (NUL bytes in the first 8 KiB).
        if bytes.iter().take(8192).any(|&b| b == 0) {
            return Ok(ExtractOutput::unsupported());
        }
        let text = String::from_utf8_lossy(&bytes);
        budget.check_deadline()?;

        let raw_chunks =
            chunk_text_with_lines(&text, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO);
        if raw_chunks.is_empty() {
            return Ok(ExtractOutput::extracted(Vec::new()));
        }

        let mut segments = Vec::with_capacity(raw_chunks.len().min(budget.max_segments));
        for chunk in raw_chunks {
            budget.check_deadline()?;
            if segments.len() >= budget.max_segments {
                break;
            }
            segments.push(SegmentDraft {
                kind: SEGMENT_KIND_DOC.to_string(),
                text: chunk.text,
                page: None,
                start_ms: None,
                end_ms: None,
                line: Some(chunk.line),
                thumbnail_hint: None,
                embed: true,
                embedding_input: DraftEmbeddingInput::Text,
            });
        }
        Ok(ExtractOutput::extracted(segments))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn modality_routing_covers_core_extensions() {
        assert_eq!(modality_kind_for_ext("txt"), Some(SEGMENT_KIND_DOC));
        assert_eq!(modality_kind_for_ext("DOCX"), Some(SEGMENT_KIND_DOC));
        assert_eq!(modality_kind_for_ext("pdf"), Some(SEGMENT_KIND_PDF));
        assert_eq!(modality_kind_for_ext("png"), Some(SEGMENT_KIND_IMAGE));
        assert_eq!(modality_kind_for_ext("mp3"), Some(SEGMENT_KIND_AUDIO));
        assert_eq!(modality_kind_for_ext("mp4"), Some(SEGMENT_KIND_VIDEO));
        assert_eq!(modality_kind_for_ext("exe"), None);
    }

    #[test]
    fn plain_text_extractor_respects_size_and_segment_budgets() {
        let dir = std::env::temp_dir().join(format!(
            "ws-extract-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("sample.txt");
        let content = "Paragraph line with searchable words for chunking.\n".repeat(120);
        std::fs::write(&file, &content).unwrap();

        // 1. Normal extraction with segment cap = 2 truncates cleanly at 2 segments.
        let budget = ExtractBudget::new(1024 * 1024, Duration::from_secs(5), 2);
        let out = PlainTextExtractor.extract(&file, &budget).unwrap();
        assert_eq!(out.status, ExtractionStatus::Extracted);
        assert_eq!(out.segments.len(), 2);
        assert_eq!(out.segments[0].kind, SEGMENT_KIND_DOC);
        assert_eq!(out.segments[0].line, Some(1));
        assert!(out.segments[0].embed);

        // 2. Tiny byte budget aborts with FileTooLarge.
        let tiny = ExtractBudget::new(16, Duration::from_secs(5), 10);
        let err = PlainTextExtractor.extract(&file, &tiny).unwrap_err();
        assert!(matches!(err, ExtractError::FileTooLarge { .. }));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
