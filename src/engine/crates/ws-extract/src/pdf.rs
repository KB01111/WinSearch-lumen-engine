//! Real pages with an opaque `(zero_based_page << 32) | draft_within_page` cursor.
//! A page advances only after all text, OCR and rendered visual drafts are emitted.
use crate::worker::{ExtractRequest, ExtractResponse, MAX_DRAFTS};
use crate::{
    chunk_text_with_lines, ExtractBudget, ExtractError, ExtractOutput, Extractor, SegmentDraft,
    DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO,
};
use std::path::Path;

pub const MAX_PAGES: u32 = 5000;
pub const MAX_PAGE_DRAFTS: u32 = 8192;
pub const VISUAL_KIND: &str = "pdf_visual";
pub fn decode_cursor(cursor: u64) -> Result<(u32, u32), String> {
    let page = (cursor >> 32) as u32;
    let chunk = cursor as u32;
    if page > MAX_PAGES || chunk >= MAX_PAGE_DRAFTS || (page == MAX_PAGES && chunk != 0) {
        return Err("invalid PDF page/chunk cursor bounds".into());
    }
    Ok((page, chunk))
}
pub fn encode_cursor(page: u32, chunk: u32) -> u64 {
    (u64::from(page) << 32) | u64::from(chunk)
}

#[derive(Default)]
pub struct PdfExtractor;
impl Extractor for PdfExtractor {
    fn extract(&self, _: &Path, _: &ExtractBudget) -> Result<ExtractOutput, ExtractError> {
        Err(ExtractError::Unsupported(
            "PDF requires bounded child page batches".into(),
        ))
    }
}

pub fn extract_pdf_batch(
    request: &ExtractRequest,
    budget: &ExtractBudget,
) -> Result<ExtractResponse, ExtractError> {
    budget.check_deadline()?;
    let (mut page, mut chunk) = decode_cursor(request.cursor).map_err(ExtractError::Decode)?;
    let draft_limit = budget.max_segments.min(MAX_DRAFTS);
    if draft_limit == 0 {
        return Err(ExtractError::Decode("PDF draft budget is zero".into()));
    }
    budget.check_size(std::fs::metadata(&request.path)?.len())?;
    let engine = ws_platform::pdfium::Pdfium::load().map_err(ExtractError::Decode)?;
    let mut document = engine
        .open(&request.path, budget.max_bytes)
        .map_err(ExtractError::Decode)?;
    let total = document.page_count.min(MAX_PAGES);
    if page > total || (page == total && chunk != 0) {
        return Err(ExtractError::Decode("PDF cursor beyond page count".into()));
    }
    let mut segments = Vec::new();
    let first_page = page;
    while page < total && page - first_page < request.max_units.min(30) {
        budget.check_deadline()?;
        let raster = document.page(page).map_err(ExtractError::Decode)?;
        budget.check_deadline()?;
        let artifact = request.scratch_dir.join(format!("page-{}.png", page + 1));
        ws_platform::wic::write_bgra_thumbnail_png(
            &raster.bgra,
            raster.width,
            raster.height,
            2048,
            &artifact,
        )
        .map_err(ExtractError::Decode)?;
        let mut drafts = Vec::new();
        for text in chunk_text_with_lines(&raster.text, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO)
        {
            let embed = !crate::is_numeric_dominated(&text.text, "pdf");
            drafts.push(SegmentDraft {
                kind: "pdf".into(),
                text: text.text,
                page: Some(page + 1),
                start_ms: None,
                end_ms: None,
                line: Some(text.line),
                thumbnail_hint: Some(artifact.clone()),
                embed,
            });
        }
        let has_pixels = raster
            .bgra
            .chunks_exact(4)
            .any(|p| p[0] < 245 || p[1] < 245 || p[2] < 245);
        if raster.visual && has_pixels {
            let ocr = ws_platform::ocr::recognize_bgra8_winrt(
                &raster.bgra,
                raster.width,
                raster.height,
                crate::DEFAULT_MAX_OCR_LANGUAGES,
            )
            .map_err(ExtractError::Decode)?;
            if ocr.language_tag.is_none() {
                return Err(ExtractError::Decode(
                    "PDF OCR recognizer unavailable".into(),
                ));
            }
            if ocr.text.chars().take(500_001).count() > 500_000 {
                return Err(ExtractError::Decode(
                    "PDF page OCR text exceeds bounded 500000 characters".into(),
                ));
            }
            for text in chunk_text_with_lines(&ocr.text, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO)
            {
                let embed = !crate::is_numeric_dominated(&text.text, "pdf");
                drafts.push(SegmentDraft {
                    kind: "ocr".into(),
                    text: text.text,
                    page: Some(page + 1),
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    thumbnail_hint: Some(artifact.clone()),
                    embed,
                });
            }
            drafts.push(SegmentDraft {
                kind: VISUAL_KIND.into(),
                text: String::new(),
                page: Some(page + 1),
                start_ms: None,
                end_ms: None,
                line: None,
                thumbnail_hint: Some(artifact.clone()),
                embed: true,
            });
        }
        if drafts.len() >= MAX_PAGE_DRAFTS as usize {
            return Err(ExtractError::Decode(
                "PDF page draft budget exceeded".into(),
            ));
        }
        if chunk as usize > drafts.len() || (chunk != 0 && chunk as usize == drafts.len()) {
            return Err(ExtractError::Decode(
                "PDF cursor outside actual page drafts".into(),
            ));
        }
        if drafts.is_empty() {
            drafts.push(SegmentDraft {
                kind: "pdf".into(),
                text: String::new(),
                page: Some(page + 1),
                start_ms: None,
                end_ms: None,
                line: None,
                thumbnail_hint: Some(artifact),
                embed: false,
            });
        }
        let page_draft_count = drafts.len();
        let take = (draft_limit - segments.len()).min(page_draft_count - chunk as usize);
        segments.extend(drafts.drain(chunk as usize..chunk as usize + take));
        chunk += take as u32;
        let complete = chunk as usize == page_draft_count;
        if complete {
            page += 1;
            chunk = 0;
        } else {
            break;
        }
        if segments.len() == draft_limit {
            break;
        }
    }
    budget.check_deadline()?;
    let diagnostics = if document.page_count > MAX_PAGES {
        vec![format!("PDF page limit: source_pages={} indexed_pages={MAX_PAGES}; remaining pages excluded by product limit",document.page_count)]
    } else {
        vec![]
    };
    Ok(ExtractResponse {
        output: ExtractOutput::extracted(segments),
        next_cursor: (page < total).then_some(encode_cursor(page, chunk)),
        total_units: Some(u64::from(total)),
        diagnostics,
    })
}
