//! Internal, bounded extraction-worker protocol (no frontend arbitrary-file API).
use crate::{ExtractOutput, ExtractionStatus};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::PathBuf;

pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_DRAFTS: usize = 256;
pub const MAX_SEGMENT_BYTES: usize = 64 * 1024;
pub const SUPPORTED_KINDS: &[&str] = &["doc", "pdf", "image", "audio", "video"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractRequest {
    pub path: PathBuf,
    pub kind: String,
    pub scratch_dir: PathBuf,
    pub cursor: u64,
    pub max_units: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractResponse {
    pub output: ExtractOutput,
    pub next_cursor: Option<u64>,
    pub total_units: Option<u64>,
    /// Bounded source-limit diagnostics; exact content may still be usable.
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default)]
    pub media: Option<MediaBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaBatch {
    pub duration_ms: u64,
    pub visual_frames: u32,
    pub has_audio: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerHello {
    pub supported_kinds: Vec<String>,
}

pub fn encode_request(request: &ExtractRequest) -> Result<Vec<u8>, String> {
    if request.max_units == 0 || request.max_units as usize > MAX_DRAFTS || request.kind.len() > 32
    {
        return Err("invalid request bounds".into());
    }
    let payload = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    if payload.len() > MAX_REQUEST_BYTES {
        return Err("request exceeds 256 KiB".into());
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame
        .write_all(&(payload.len() as u32).to_le_bytes())
        .map_err(|e| e.to_string())?;
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn read_request(reader: &mut impl Read) -> Result<ExtractRequest, String> {
    let mut length = [0; 4];
    reader.read_exact(&mut length).map_err(|e| e.to_string())?;
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_REQUEST_BYTES {
        return Err("request exceeds 256 KiB".into());
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).map_err(|e| e.to_string())?;
    let request = serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
    encode_request(&request)?;
    Ok(request)
}

pub fn validate_response(
    request: &ExtractRequest,
    response: &ExtractResponse,
) -> Result<(), String> {
    if response.diagnostics.len() > 8 || response.diagnostics.iter().any(|s| s.len() > 1024) {
        return Err("worker diagnostics exceed bounds".into());
    }
    let segments = &response.output.segments;
    if segments.len() > MAX_DRAFTS
        || (!matches!(request.kind.as_str(), "pdf" | "video") && segments.len() > request.max_units as usize)
    {
        return Err("too many drafts".into());
    }
    if matches!(
        response.output.status,
        ExtractionStatus::Unsupported | ExtractionStatus::Failed(_)
    ) && !segments.is_empty()
    {
        return Err("failed/unsupported response contains drafts".into());
    }
    if let ExtractionStatus::Failed(message) = &response.output.status {
        if message.len() > MAX_SEGMENT_BYTES {
            return Err("failure message too long".into());
        }
    }
    if request.kind == "pdf" {
        validate_pdf_progress(request, response)?;
    } else if matches!(request.kind.as_str(), "audio" | "video") {
        validate_media_progress(request, response)?;
    } else if response.next_cursor.is_some_and(|next| {
        next <= request.cursor || response.total_units.is_some_and(|total| next > total)
    }) || response
        .total_units
        .is_some_and(|total| total < request.cursor || total < segments.len() as u64)
    {
        return Err("invalid cursor progress".into());
    }
    // Document units are chunk ordinals. Future page/media protocols must
    // define their own unit-to-draft relationship rather than reuse this rule.
    if request.kind == "doc"
        && matches!(
            response.output.status,
            ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
        )
    {
        let end = request
            .cursor
            .checked_add(segments.len() as u64)
            .ok_or("cursor overflow")?;
        if response
            .next_cursor
            .is_some_and(|next| segments.is_empty() || next != end)
            || (response.next_cursor.is_none()
                && response.total_units.is_some_and(|total| total != end))
        {
            return Err("document cursor does not cover returned chunks".into());
        }
    }
    let (mut page, mut time, mut line) = (0, 0, 0);
    for draft in segments {
        let valid_kind = draft.kind == request.kind
            || (matches!(request.kind.as_str(), "image" | "pdf") && draft.kind == "ocr")
            || (request.kind == "pdf" && draft.kind == crate::pdf::VISUAL_KIND)
            || (request.kind == "video" && draft.kind == "audio");
        if !valid_kind || draft.kind.len() > 32 || draft.text.len() > MAX_SEGMENT_BYTES {
            return Err("invalid draft kind or text length".into());
        }
        if let Some(p) = draft.page {
            if p == 0 || p < page {
                return Err("unordered pages".into());
            }
            if p != page && request.kind == "pdf" {
                line = 0;
            }
            page = p;
        }
        if let Some(l) = draft.line {
            if l == 0 || l < line {
                return Err("unordered lines".into());
            }
            line = l;
        }
        match (draft.start_ms, draft.end_ms) {
            (Some(start), Some(end)) if start <= end && start >= time && end <= i64::MAX as u64 => {
                time = start
            }
            (None, None) => (),
            _ => return Err("invalid time range".into()),
        }
        if draft
            .thumbnail_hint
            .as_ref()
            .is_some_and(|p| p.as_os_str().len() > 32_768)
        {
            return Err("thumbnail path too long".into());
        }
        match &draft.embedding_input {
            crate::DraftEmbeddingInput::Text if draft.kind != "audio" => (),
            crate::DraftEmbeddingInput::Audio { path } if draft.kind == "audio"
                && draft.embed && draft.text.is_empty() && path.is_absolute()
                && path.as_os_str().len() <= 32768 && draft.thumbnail_hint.is_none() => (),
            _ => return Err("invalid typed embedding input".into()),
        }
    }
    Ok(())
}

fn validate_media_progress(request: &ExtractRequest, response: &ExtractResponse) -> Result<(), String> {
    if !matches!(response.output.status, ExtractionStatus::Extracted | ExtractionStatus::ExactOnly) {
        if response.next_cursor.is_some() || response.media.is_some() { return Err("failed media has progress".into()); }
        return Ok(());
    }
    let media = response.media.as_ref().ok_or("media progress missing")?;
    if media.duration_ms == 0 || media.duration_ms > crate::decoder::MAX_DURATION_MS
        || media.visual_frames != 0 || !media.has_audio {
        return Err("invalid audio progress metadata".into());
    }
    let total = crate::audio::window_count(media.duration_ms);
    if response.total_units != Some(total) || request.cursor > total { return Err("invalid audio total/cursor".into()); }
    let end = total.min(request.cursor + u64::from(request.max_units.min(30)));
    if response.next_cursor != (end < total).then_some(end) { return Err("invalid audio continuation".into()); }
    let mut previous = None;
    for draft in &response.output.segments {
        let start = draft.start_ms.ok_or("audio draft missing time")?;
        let finish = draft.end_ms.ok_or("audio draft missing end")?;
        let window = start / crate::audio::HOP_MS;
        if draft.kind != "audio" || draft.page.is_some() || draft.line.is_some()
            || start % crate::audio::HOP_MS != 0 || window < request.cursor || window >= end
            || previous.is_some_and(|p| window <= p) || finish <= start
            || finish > media.duration_ms.min(start + crate::audio::WINDOW_MS) {
            return Err("audio draft outside processed windows".into());
        }
        previous = Some(window);
    }
    Ok(())
}

fn validate_pdf_progress(
    request: &ExtractRequest,
    response: &ExtractResponse,
) -> Result<(), String> {
    let (page, chunk) = crate::pdf::decode_cursor(request.cursor)?;
    if !matches!(
        response.output.status,
        ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
    ) {
        if response.next_cursor.is_some() {
            return Err("failed PDF cursor".into());
        }
        return Ok(());
    }
    let total = response.total_units.ok_or("PDF page count missing")?;
    if total > crate::pdf::MAX_PAGES as u64
        || page as u64 > total
        || (page as u64 == total && chunk != 0)
    {
        return Err("invalid PDF total pages".into());
    }
    let end_page = (page + request.max_units.min(30)).min(total as u32);
    let mut page_draft_counts = [0u32; 30];
    for draft in &response.output.segments {
        let p = draft.page.ok_or("PDF draft missing page")?;
        if p <= page || p > end_page || draft.start_ms.is_some() || draft.end_ms.is_some() {
            return Err("PDF draft outside page batch".into());
        }
        let count = &mut page_draft_counts[(p - page - 1) as usize];
        *count += 1;
        let start = if p == page + 1 { chunk } else { 0 };
        if start + *count > crate::pdf::MAX_PAGE_DRAFTS {
            return Err("PDF page draft ordinals exceed bounds".into());
        }
        if draft.kind == crate::pdf::VISUAL_KIND
            && (!draft.text.is_empty() || draft.thumbnail_hint.is_none() || draft.line.is_some())
        {
            return Err("invalid PDF visual draft".into());
        }
    }
    if let Some(next) = response.next_cursor {
        let (p, c) = crate::pdf::decode_cursor(next)?;
        if next <= request.cursor || p < page || p > end_page || p as u64 >= total {
            return Err("invalid PDF cursor progress".into());
        }
        let n = response
            .output
            .segments
            .iter()
            .filter(|s| s.page == Some(p + 1))
            .count() as u32;
        if (p == page && (n == 0 || c != chunk + n))
            || (p > page && c != n)
            || response
                .output
                .segments
                .iter()
                .any(|s| s.page.unwrap() > p + u32::from(c > 0))
        {
            return Err("PDF cursor does not cover page drafts".into());
        }
        for completed in page..p {
            if !response
                .output
                .segments
                .iter()
                .any(|s| s.page == Some(completed + 1))
            {
                return Err("PDF completed page has no representation".into());
            }
        }
    } else {
        if end_page as u64 != total {
            return Err("PDF falsely completed before total pages".into());
        }
        for completed in page..total as u32 {
            if !response
                .output
                .segments
                .iter()
                .any(|s| s.page == Some(completed + 1))
            {
                return Err("PDF completed page has no representation".into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExtractOutput, SegmentDraft};
    use std::path::PathBuf;

    fn request() -> ExtractRequest {
        ExtractRequest {
            path: PathBuf::from("C:/fixture/notes.txt"),
            kind: "doc".into(),
            scratch_dir: PathBuf::from("C:/cache/private"),
            cursor: 0,
            max_units: 256,
        }
    }

    #[test]
    fn media_audio_progress_allows_silence_and_rejects_forged_times_or_inputs() {
        let mut req = request();
        req.kind = "audio".into();
        req.max_units = 30;
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(30), total_units: Some(48), diagnostics: vec![],
            media: Some(MediaBatch { duration_ms: 1_200_000, visual_frames: 0, has_audio: true }),
        };
        validate_response(&req, &response).unwrap();
        response.next_cursor = Some(31);
        assert!(validate_response(&req, &response).is_err());
        response.next_cursor = Some(30);
        let mut draft = SegmentDraft::text("audio", "");
        draft.start_ms = Some(25000);
        draft.end_ms = Some(55000);
        draft.embedding_input = crate::DraftEmbeddingInput::Audio { path: PathBuf::from("C:/private/window.wav") };
        response.output.segments.push(draft);
        validate_response(&req, &response).unwrap();
        response.output.segments[0].start_ms = Some(25001);
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].start_ms = Some(25000);
        response.output.segments[0].end_ms = Some(55001);
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].end_ms = Some(55000);
        response.output.segments[0].embedding_input = crate::DraftEmbeddingInput::Text;
        assert!(validate_response(&req, &response).is_err());
        response.output.segments.clear();
        req.cursor = 30;
        response.next_cursor = None;
        validate_response(&req, &response).unwrap();
        response.total_units = Some(47);
        assert!(validate_response(&req, &response).is_err());
    }

    #[test]
    fn protocol_roundtrip_preserves_text_and_line_metadata() {
        let mut segment = SegmentDraft::text("doc", "actual text\nsecond line");
        segment.line = Some(1);
        let response = ExtractResponse {
            output: ExtractOutput::extracted(vec![segment]),
            next_cursor: None,
            total_units: Some(1),
            diagnostics: Vec::new(),
            media: None,
        };
        let mut wire = Vec::new();
        ws_protocol::write_message(&mut wire, &response).unwrap();
        let decoded: ExtractResponse = ws_protocol::read_message(&mut wire.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(decoded, response);
        validate_response(&request(), &decoded).unwrap();
    }

    #[test]
    fn pdf_transport_multiple_drafts_and_packed_page_chunk_progress() {
        let mut req = request();
        req.kind = "pdf".into();
        req.max_units = 1;
        let mut text = SegmentDraft::text("pdf", "first");
        text.page = Some(1);
        text.line = Some(5);
        let mut ocr = text.clone();
        ocr.kind = "ocr".into();
        ocr.line = Some(6);
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![text.clone(), ocr]),
            next_cursor: Some(1 << 32),
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
        };
        validate_response(&req, &response).unwrap();
        response.output.segments = vec![text; 256];
        response.next_cursor = Some(256);
        validate_response(&req, &response).unwrap();
        req.cursor = 256;
        response.output.segments.truncate(2);
        response.next_cursor = Some(1 << 32);
        validate_response(&req, &response).unwrap();
        response.output.segments[0].kind = "image".into();
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].kind = "pdf".into();
        response.next_cursor = Some((1 << 32) | 9000);
        assert!(validate_response(&req, &response).is_err());
        response.next_cursor = Some(1 << 32);
        response.output.segments[0].page = Some(2);
        assert!(validate_response(&req, &response).is_err());
    }

    #[test]
    fn pdf_transport_rejects_missing_completed_page_and_limits_diagnostics() {
        let mut req = request();
        req.kind = "pdf".into();
        let mut a = SegmentDraft::text("pdf", "first");
        a.page = Some(1);
        a.line = Some(20);
        let mut b = a.clone();
        b.page = Some(2);
        b.line = Some(1);
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![a.clone(), b]),
            next_cursor: None,
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
        };
        validate_response(&req, &response).unwrap();
        response.output.segments.pop();
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].page = None;
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].page = Some(1);
        response.next_cursor = Some(1 << 32);
        response.diagnostics = vec!["x".repeat(1025)];
        assert!(validate_response(&req, &response).is_err());
    }

    #[test]
    fn pdf_transport_rejects_completed_or_advancing_page_draft_overflow() {
        let mut req = request();
        req.kind = "pdf".into();
        req.cursor = 8191;
        let mut draft = SegmentDraft::text("pdf", "last valid draft");
        draft.page = Some(1);
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![draft.clone()]),
            next_cursor: None,
            total_units: Some(1),
            diagnostics: vec![],
            media: None,
        };
        validate_response(&req, &response).unwrap();
        response.output.segments.push(draft);
        assert!(
            validate_response(&req, &response).is_err(),
            "terminal page emitted ordinal 8192"
        );
        response.total_units = Some(2);
        response.next_cursor = Some(1 << 32);
        assert!(
            validate_response(&req, &response).is_err(),
            "page advance emitted ordinal 8192"
        );
        response.output.segments.pop();
        validate_response(&req, &response).unwrap();
    }

    #[test]
    fn text_batches_reject_empty_progress_and_false_completion() {
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![SegmentDraft::text("doc", "one")]),
            next_cursor: None,
            total_units: Some(75),
            diagnostics: Vec::new(),
            media: None,
        };
        assert!(validate_response(&request(), &response).is_err());
        response.next_cursor = Some(30);
        assert!(validate_response(&request(), &response).is_err());
        response.output.segments.clear();
        response.next_cursor = Some(1);
        assert!(validate_response(&request(), &response).is_err());
    }

    #[test]
    fn request_and_response_bounds_reject_hostile_values() {
        assert!(
            read_request(&mut ((MAX_REQUEST_BYTES as u32 + 1).to_le_bytes().as_slice())).is_err()
        );
        let mut req = request();
        req.path = PathBuf::from("界".repeat(100_000));
        assert!(encode_request(&req).is_err());
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![SegmentDraft::text("doc", "x"); 257]),
            next_cursor: None,
            total_units: None,
            diagnostics: Vec::new(),
            media: None,
        };
        assert!(validate_response(&request(), &response).is_err());
        response.output.segments.truncate(1);
        response.output.segments[0].text = "x".repeat(MAX_SEGMENT_BYTES + 1);
        assert!(validate_response(&request(), &response).is_err());
        response.output.segments[0].text = "valid".into();
        response.next_cursor = Some(0);
        assert!(validate_response(&request(), &response).is_err());
        response.next_cursor = Some(1);
        response.total_units = Some(0);
        assert!(validate_response(&request(), &response).is_err());
    }

    #[test]
    fn response_rejects_reversed_metadata_and_foreign_kinds() {
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![
                SegmentDraft::text("doc", "one"),
                SegmentDraft::text("doc", "two"),
            ]),
            next_cursor: None,
            total_units: None,
            diagnostics: Vec::new(),
            media: None,
        };
        response.output.segments[0].page = Some(2);
        response.output.segments[1].page = Some(1);
        assert!(validate_response(&request(), &response).is_err());
        response.output.segments.truncate(1);
        response.output.segments[0].start_ms = Some(9);
        response.output.segments[0].end_ms = Some(2);
        assert!(validate_response(&request(), &response).is_err());
        response.output.segments[0].start_ms = None;
        response.output.segments[0].end_ms = None;
        response.output.segments[0].kind = "audio".into();
        assert!(validate_response(&request(), &response).is_err());
    }
}
