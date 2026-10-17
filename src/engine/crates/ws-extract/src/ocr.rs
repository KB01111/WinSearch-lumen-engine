//! OCR extraction and word bounding-box serialization (`crates/ws-extract/src/ocr.rs`, Task 11 / P4.3).
//!
//! Wraps [`ws_platform::ocr`] behind an injectable [`OcrProvider`] trait so
//! production uses WinRT `Windows.Media.Ocr` (auto-detecting the best installed
//! language up to [`DEFAULT_MAX_OCR_LANGUAGES`]) while unit/integration tests
//! can inject deterministic OCR results with word bounding boxes or exercise
//! the real OS OCR engine on rendered bitmaps.

use serde::{Deserialize, Serialize};

use crate::budget::ExtractBudget;
use crate::chunk::{chunk_text_with_lines, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO};
use crate::{ExtractError, SegmentDraft};
use ws_index::model::SEGMENT_KIND_OCR;

/// Default cap on installed WinRT OCR languages probed per image.
pub const DEFAULT_MAX_OCR_LANGUAGES: usize = 4;

/// One recognized word with its pixel bounding box in the image/page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OcrWordBox {
    pub text: String,
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

impl From<ws_platform::ocr::OcrWordBox> for OcrWordBox {
    fn from(w: ws_platform::ocr::OcrWordBox) -> Self {
        Self {
            text: w.text,
            x: w.x,
            y: w.y,
            w: w.w,
            h: w.h,
        }
    }
}

/// Complete OCR result for an image or rendered PDF page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OcrResultData {
    pub text: String,
    pub words: Vec<OcrWordBox>,
    pub language_tag: Option<String>,
}

impl From<ws_platform::ocr::OcrExtraction> for OcrResultData {
    fn from(ext: ws_platform::ocr::OcrExtraction) -> Self {
        Self {
            text: ext.text,
            words: ext.words.into_iter().map(OcrWordBox::from).collect(),
            language_tag: ext.language_tag,
        }
    }
}

/// Injectable OCR provider over 32-bpp BGRA8 image buffers.
pub trait OcrProvider: Send + Sync {
    fn recognize_bgra8(
        &self,
        bgra8: &[u8],
        width: u32,
        height: u32,
        budget: &ExtractBudget,
    ) -> Result<OcrResultData, ExtractError>;
}

/// Production WinRT `Windows.Media.Ocr` provider.
#[derive(Debug, Clone, Copy)]
pub struct WinRtOcrProvider {
    pub max_languages: usize,
}

impl Default for WinRtOcrProvider {
    fn default() -> Self {
        Self {
            max_languages: DEFAULT_MAX_OCR_LANGUAGES,
        }
    }
}

impl OcrProvider for WinRtOcrProvider {
    fn recognize_bgra8(
        &self,
        bgra8: &[u8],
        width: u32,
        height: u32,
        budget: &ExtractBudget,
    ) -> Result<OcrResultData, ExtractError> {
        budget.check_deadline()?;
        match ws_platform::ocr::recognize_bgra8_winrt(bgra8, width, height, self.max_languages) {
            Ok(ext) => {
                budget.check_deadline()?;
                Ok(OcrResultData::from(ext))
            }
            Err(_) => {
                // Gracefully degrade when WinRT OCR is unavailable or fails on an unusual frame.
                Ok(OcrResultData::default())
            }
        }
    }
}

/// Converts an [`OcrResultData`] into `ocr` [`SegmentDraft`]s (for both FTS
/// exact search and vector embedding), attaching `thumbnail_hint` and optional
/// `page`. Also returns the serialized word bounding-box JSON when words are
/// present so callers can inspect or store bbox highlights.
pub fn build_ocr_segments(
    ocr: &OcrResultData,
    page: Option<u32>,
    thumbnail_hint: Option<std::path::PathBuf>,
    budget: &ExtractBudget,
) -> Result<(Vec<SegmentDraft>, Option<String>), ExtractError> {
    let trimmed = ocr.text.trim();
    if trimmed.is_empty() {
        return Ok((Vec::new(), None));
    }

    let bbox_json = if ocr.words.is_empty() {
        None
    } else {
        serde_json::to_string(&ocr.words).ok()
    };

    let raw_chunks = chunk_text_with_lines(trimmed, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO);
    let mut segments = Vec::with_capacity(raw_chunks.len().min(budget.max_segments));
    for chunk in raw_chunks {
        budget.check_deadline()?;
        if segments.len() >= budget.max_segments {
            break;
        }
        segments.push(SegmentDraft {
            embedding_input: crate::DraftEmbeddingInput::Text,
            kind: SEGMENT_KIND_OCR.to_string(),
            text: chunk.text,
            page,
            start_ms: None,
            end_ms: None,
            line: Some(chunk.line),
            thumbnail_hint: thumbnail_hint.clone(),
            embed: true,
        });
    }

    Ok((segments, bbox_json))
}
