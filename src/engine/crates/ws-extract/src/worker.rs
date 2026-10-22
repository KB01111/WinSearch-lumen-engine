//! Internal, bounded extraction-worker protocol (no frontend arbitrary-file API).
use crate::{ExtractOutput, ExtractionStatus};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::PathBuf;

pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_DRAFTS: usize = 256;
pub const MAX_SEGMENT_BYTES: usize = 64 * 1024;
pub const SUPPORTED_KINDS: &[&str] = &["doc", "pdf", "image", "audio", "video", "code"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractRequest {
    pub path: PathBuf,
    pub kind: String,
    pub scratch_dir: PathBuf,
    pub cursor: u64,
    pub max_units: u32,
    #[serde(default)]
    pub media_plan: Option<MediaBatch>,
    #[serde(default)]
    pub video_selection: Option<ws_index::model::VideoSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sha256: Option<String>,
    pub output: ExtractOutput,
    pub next_cursor: Option<u64>,
    pub total_units: Option<u64>,
    /// Bounded source-limit diagnostics; exact content may still be usable.
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default)]
    pub media: Option<MediaBatch>,
    #[serde(default)]
    pub video_selection: Option<ws_index::model::VideoSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<crate::code::CodeBatch>,
}

pub use ws_index::model::{MediaPlan as MediaBatch, VideoSelection};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerHello {
    pub supported_kinds: Vec<String>,
}

pub fn encode_request(request: &ExtractRequest) -> Result<Vec<u8>, String> {
    if request.max_units == 0 || request.max_units as usize > MAX_DRAFTS || request.kind.len() > 32
    {
        return Err("invalid request bounds".into());
    }
    if let Some(plan) = &request.media_plan {
        plan.validate()?;
        if !matches!(request.kind.as_str(), "audio" | "video")
            || (request.kind == "audio" && !plan.frames_ms.is_empty())
        {
            return Err("media plan belongs to another modality".into());
        }
    }
    if let Some(selection) = &request.video_selection {
        selection.validate()?;
        if request.kind != "video"
            || request.media_plan.is_some()
            || request.cursor != crate::video::selection_cursor(selection.next_ms)
        {
            return Err("invalid video selection request".into());
        }
    } else if request.kind == "video" && request.cursor != 0 && request.media_plan.is_none() {
        return Err("continued video requires a source selection".into());
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
    if response.source_sha256.as_deref().is_some_and(|hash| {
        request.kind == "code"
            || !ws_protocol::valid_content_sha256(hash)
            || !matches!(
                response.output.status,
                ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
            )
    }) {
        return Err("invalid ordinary complete-source byte provenance".into());
    }
    if request.kind == "code" {
        return crate::code::validate_batch(request, response);
    }
    if response.code.is_some() {
        return Err("code payload belongs to another modality".into());
    }
    if request.kind != "video" && response.video_selection.is_some() {
        return Err("selection belongs to video".into());
    }
    let segments = &response.output.segments;
    if segments.len() > MAX_DRAFTS
        || (!matches!(request.kind.as_str(), "pdf" | "video")
            && segments.len() > request.max_units as usize)
    {
        return Err("too many drafts".into());
    }
    if matches!(
        response.output.status,
        ExtractionStatus::Unsupported
            | ExtractionStatus::Failed(_)
            | ExtractionStatus::SourceChanged
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
            || (request.kind == "video" && matches!(draft.kind.as_str(), "audio" | "ocr"));
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
            crate::DraftEmbeddingInput::Text
                if draft.kind != "audio" && !(request.kind == "video" && draft.kind == "video") => {
            }
            crate::DraftEmbeddingInput::Image { path, token_budget }
                if request.kind == "video"
                    && draft.kind == "video"
                    && draft.embed
                    && draft.text.is_empty()
                    && matches!(token_budget, 70 | 140)
                    && path.is_absolute()
                    && path.as_os_str().len() <= 32768
                    && draft.thumbnail_hint.as_ref() == Some(path) => {}
            crate::DraftEmbeddingInput::Audio { path }
                if draft.kind == "audio"
                    && draft.embed
                    && draft.text.is_empty()
                    && path.is_absolute()
                    && path.as_os_str().len() <= 32768
                    && draft.thumbnail_hint.is_none() => {}
            _ => return Err("invalid typed embedding input".into()),
        }
    }
    Ok(())
}

fn validate_media_progress(
    request: &ExtractRequest,
    response: &ExtractResponse,
) -> Result<(), String> {
    if !matches!(
        response.output.status,
        ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
    ) {
        if response.next_cursor.is_some()
            || response.media.is_some()
            || response.video_selection.is_some()
        {
            return Err("failed media has progress".into());
        }
        return Ok(());
    }
    if request.kind == "video"
        && (request.video_selection.is_some() || response.video_selection.is_some())
    {
        return validate_video_selection(request, response);
    }
    let media = response.media.as_ref().ok_or("media progress missing")?;
    media.validate()?;
    if request.kind == "video"
        && request.media_plan.is_none()
        && !media.frames_ms.is_empty()
        && media.duration_ms > crate::video::SELECTION_SPAN_MS
    {
        return Err("long video skipped selection checkpoint".into());
    }
    if request
        .media_plan
        .as_ref()
        .is_some_and(|plan| plan != media)
    {
        return Err("worker changed the revision-scoped media selection".into());
    }
    if request.kind == "video" {
        return validate_video_progress(request, response, media);
    }
    if media.duration_ms == 0
        || media.duration_ms > crate::decoder::MAX_DURATION_MS
        || !media.frames_ms.is_empty()
        || !media.has_audio
    {
        return Err("invalid audio progress metadata".into());
    }
    let total = crate::audio::window_count(media.duration_ms);
    if response.total_units != Some(total) || request.cursor > total {
        return Err("invalid audio total/cursor".into());
    }
    let end = total.min(request.cursor + u64::from(request.max_units.min(30)));
    if response.next_cursor != (end < total).then_some(end) {
        return Err("invalid audio continuation".into());
    }
    validate_audio_drafts(
        &response.output.segments,
        request.cursor,
        end,
        media.duration_ms,
    )
}

fn validate_video_selection(
    request: &ExtractRequest,
    response: &ExtractResponse,
) -> Result<(), String> {
    use crate::video;
    if request.media_plan.is_some() || !response.output.segments.is_empty() {
        return Err("selection phase contains drafts or an existing plan".into());
    }
    let state = response
        .video_selection
        .as_ref()
        .ok_or("selection response missing state")?;
    state.validate_successor(request.video_selection.as_ref())?;
    if request.video_selection.is_none()
        && state.scan_span_ms > video::SELECTION_SPAN_MS * u64::from(request.max_units.clamp(1, 30))
    {
        return Err("video selection exceeded requested work budget".into());
    }
    if request.video_selection.is_none() && request.cursor != 0 {
        return Err("invalid initial video selection".into());
    }
    if state.next_ms < state.duration_ms {
        if response.media.is_some()
            || response.total_units.is_some()
            || response.next_cursor != Some(video::selection_cursor(state.next_ms))
        {
            return Err("unfinished selection froze a plan".into());
        }
        return Ok(());
    }
    let plan = response
        .media
        .as_ref()
        .ok_or("finished selection missing plan")?;
    plan.validate()?;
    if plan.duration_ms != state.duration_ms
        || plan.has_audio != state.has_audio
        || plan.frames_ms != video::select_frames(state.candidates_ms.clone())
        || response.total_units != Some(video::total_units(plan))
        || response.next_cursor != Some(0)
    {
        return Err("finished video plan does not match selection".into());
    }
    Ok(())
}

fn validate_audio_drafts(
    segments: &[crate::SegmentDraft],
    first: u64,
    end: u64,
    duration: u64,
) -> Result<(), String> {
    let mut previous = None;
    for draft in segments {
        let start = draft.start_ms.ok_or("audio draft missing time")?;
        let finish = draft.end_ms.ok_or("audio draft missing end")?;
        let window = start / crate::audio::HOP_MS;
        if draft.kind != "audio"
            || draft.page.is_some()
            || draft.line.is_some()
            || start % crate::audio::HOP_MS != 0
            || window < first
            || window >= end
            || previous.is_some_and(|p| window <= p)
            || finish <= start
            || finish > duration.min(start + crate::audio::WINDOW_MS)
        {
            return Err("audio draft outside processed windows".into());
        }
        previous = Some(window);
    }
    Ok(())
}

fn validate_video_progress(
    request: &ExtractRequest,
    response: &ExtractResponse,
    plan: &MediaBatch,
) -> Result<(), String> {
    use crate::video::{self, Cursor};
    if response.total_units != Some(video::total_units(plan))
        || (request.cursor != 0 && request.media_plan.is_none())
    {
        return Err("invalid video units or missing continuation plan".into());
    }
    let cursor = video::decode_cursor(request.cursor)?;
    let audio_window = match cursor {
        Cursor::Audio { window } => Some(window),
        Cursor::Frame { index: 0, draft: 0 } if plan.frames_ms.is_empty() => Some(0),
        _ => None,
    };
    if let Some(first) = audio_window {
        let total = crate::audio::window_count(plan.duration_ms);
        if !plan.has_audio || first > total {
            return Err("invalid video soundtrack cursor".into());
        }
        let end = total.min(first + u64::from(request.max_units.min(30)));
        if response.next_cursor != (end < total).then_some(video::audio_cursor(end)) {
            return Err("invalid video soundtrack continuation".into());
        }
        return validate_audio_drafts(&response.output.segments, first, end, plan.duration_ms);
    }
    let Cursor::Frame {
        index: first,
        draft: offset,
    } = cursor
    else {
        return Err("invalid visual phase".into());
    };
    let first = first as usize;
    if first >= plan.frames_ms.len() {
        return Err("video frame cursor beyond selection".into());
    }
    let end_bound = (first + request.max_units.min(30) as usize).min(plan.frames_ms.len());
    let mut counts = [0u32; 30];
    for draft in &response.output.segments {
        let time = draft
            .start_ms
            .ok_or("video draft missing source timestamp")?;
        let index = plan
            .frames_ms
            .binary_search(&time)
            .map_err(|_| "video time absent from selection")?;
        if index < first
            || index >= end_bound
            || draft.end_ms != Some(time)
            || draft.page.is_some()
            || draft.line.is_some()
            || draft.thumbnail_hint.is_none()
            || !matches!(draft.kind.as_str(), "video" | "ocr")
        {
            return Err("video draft outside visual batch".into());
        }
        let count = &mut counts[index - first];
        let ordinal = *count + if index == first { offset } else { 0 };
        if ordinal >= video::MAX_FRAME_DRAFTS
            || (draft.kind == "video") != (ordinal == 0)
            || (draft.kind == "ocr" && draft.text.trim().is_empty())
        {
            return Err("invalid frame visual/OCR draft ordinal".into());
        }
        *count += 1;
    }
    let (completed, partial) = match response.next_cursor {
        Some(next) => match video::decode_cursor(next)? {
            Cursor::Audio { window: 0 } if plan.has_audio && end_bound == plan.frames_ms.len() => {
                (end_bound, None)
            }
            Cursor::Frame { index, draft } => {
                let index = index as usize;
                if next <= request.cursor
                    || index < first
                    || index > end_bound
                    || index >= plan.frames_ms.len()
                {
                    return Err("invalid visual cursor progress".into());
                }
                let n = counts.get(index - first).copied().unwrap_or(0);
                if (index == first && (n == 0 || draft != offset + n))
                    || (index > first && draft != n)
                {
                    return Err("visual cursor does not cover frame drafts".into());
                }
                (index, Some((index, draft)))
            }
            _ => return Err("video skipped a phase/window".into()),
        },
        None if !plan.has_audio && end_bound == plan.frames_ms.len() => (end_bound, None),
        None => return Err("video falsely completed before soundtrack/frames".into()),
    };
    for index in first..completed {
        if counts[index - first] == 0 {
            return Err("completed video frame has no representation".into());
        }
    }
    if let Some((index, draft)) = partial {
        let last_allowed = index + usize::from(draft > 0);
        if response
            .output
            .segments
            .iter()
            .any(|s| plan.frames_ms.binary_search(&s.start_ms.unwrap()).unwrap() >= last_allowed)
        {
            return Err("video emitted drafts beyond continuation".into());
        }
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
            media_plan: None,
            video_selection: None,
        }
    }

    #[test]
    fn video_transport_validates_opaque_phases_partial_frames_and_silence() {
        let mut req = request();
        req.kind = "video".into();
        req.max_units = 1;
        let plan = MediaBatch {
            duration_ms: 11000,
            frames_ms: vec![0, 5000, 10000],
            has_audio: true,
        };
        let mut image = SegmentDraft::text("video", "");
        image.start_ms = Some(0);
        image.end_ms = Some(0);
        image.thumbnail_hint = Some("C:/scratch/frame.png".into());
        image.embedding_input = crate::DraftEmbeddingInput::Image {
            path: "C:/scratch/frame.png".into(),
            token_budget: 140,
        };
        let mut ocr = SegmentDraft::text("ocr", "real screen text");
        ocr.start_ms = Some(0);
        ocr.end_ms = Some(0);
        ocr.thumbnail_hint = image.thumbnail_hint.clone();
        let mut response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![image.clone(), ocr.clone()]),
            next_cursor: Some(crate::video::frame_cursor(1, 0)),
            total_units: Some(4),
            diagnostics: vec![],
            media: Some(plan.clone()),
            video_selection: None,
        };
        validate_response(&req, &response).unwrap();
        response.next_cursor = Some(crate::video::audio_cursor(0));
        assert!(
            validate_response(&req, &response).is_err(),
            "visual phase cannot skip unprocessed frames"
        );
        response.next_cursor = None;
        assert!(
            validate_response(&req, &response).is_err(),
            "soundtrack cannot be falsely completed"
        );
        response.next_cursor = Some(crate::video::frame_cursor(1, 0));
        response.output.segments.remove(0);
        assert!(
            validate_response(&req, &response).is_err(),
            "first frame draft must be visual"
        );
        req.cursor = crate::video::frame_cursor(0, 1);
        req.media_plan = Some(plan.clone());
        validate_response(&req, &response).unwrap();
        response.next_cursor = Some(crate::video::frame_cursor(0, 3));
        assert!(
            validate_response(&req, &response).is_err(),
            "partial cursor cannot invent omitted drafts"
        );
        req.cursor = crate::video::audio_cursor(0);
        response.output.segments.clear();
        response.next_cursor = None;
        validate_response(&req, &response).unwrap();
        response.next_cursor = Some(crate::video::audio_cursor(1));
        assert!(
            validate_response(&req, &response).is_err(),
            "terminal audio window must complete"
        );
        response.next_cursor = None;
        response.media.as_mut().unwrap().frames_ms[1] = 6000;
        assert!(
            validate_response(&req, &response).is_err(),
            "continuation cannot mutate the stored selection"
        );
    }

    #[test]
    fn video_transport_rejects_outside_times_budgets_and_selection_bombs() {
        let mut req = request();
        req.kind = "video".into();
        req.max_units = 1;
        let plan = MediaBatch {
            duration_ms: 6000,
            frames_ms: vec![0, 5000],
            has_audio: false,
        };
        let mut image = SegmentDraft::text("video", "");
        image.start_ms = Some(0);
        image.end_ms = Some(0);
        image.thumbnail_hint = Some("C:/scratch/frame.png".into());
        image.embedding_input = crate::DraftEmbeddingInput::Image {
            path: "C:/scratch/frame.png".into(),
            token_budget: 70,
        };
        let mut response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![image]),
            next_cursor: Some(crate::video::frame_cursor(1, 0)),
            total_units: Some(2),
            diagnostics: vec![],
            media: Some(plan.clone()),
            video_selection: None,
        };
        validate_response(&req, &response).unwrap();
        response.output.segments[0].start_ms = Some(4999);
        response.output.segments[0].end_ms = Some(4999);
        assert!(validate_response(&req, &response).is_err());
        response.output.segments[0].start_ms = Some(0);
        response.output.segments[0].end_ms = Some(0);
        response.output.segments[0].embedding_input = crate::DraftEmbeddingInput::Image {
            path: "C:/scratch/frame.png".into(),
            token_budget: 280,
        };
        assert!(validate_response(&req, &response).is_err());
        req.media_plan = Some(MediaBatch {
            duration_ms: 2_000_000,
            frames_ms: (0..1201).map(|n| n * 1000).collect(),
            has_audio: false,
        });
        assert!(encode_request(&req).is_err());
        req.media_plan = None;
        req.cursor = crate::video::audio_cursor(0);
        assert!(encode_request(&req).is_err());
    }

    #[test]
    fn final_selection_can_retain_video_tail_when_soundtrack_is_longer() {
        let mut req = request();
        req.kind = "video".into();
        req.max_units = 1;
        let previous = VideoSelection {
            duration_ms: 65_000,
            scan_span_ms: crate::video::SELECTION_SPAN_MS,
            has_audio: true,
            next_ms: 60_000,
            last_sample_ms: Some(7_000),
            last_gray: vec![3; ws_index::model::VIDEO_GRAY_PIXELS],
            candidates_ms: vec![0, 5_000],
            next_periodic_ms: 10_000,
            sample_count: 8,
        };
        req.cursor = crate::video::selection_cursor(previous.next_ms);
        req.video_selection = Some(previous.clone());
        let completed = VideoSelection {
            next_ms: 65_000,
            candidates_ms: vec![0, 5_000, 7_000],
            ..previous
        };
        let response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(0),
            total_units: Some(6),
            diagnostics: vec![],
            media: Some(MediaBatch {
                duration_ms: 65_000,
                frames_ms: vec![0, 5_000, 7_000],
                has_audio: true,
            }),
            video_selection: Some(completed),
        };
        validate_response(&req, &response)
            .expect("last sampled frame must survive a longer soundtrack");
    }

    #[test]
    fn video_selection_continuation_rejects_replay_skip_and_mutated_state() {
        let mut req = request();
        req.kind = "video".into();
        req.max_units = 1;
        let first = VideoSelection {
            duration_ms: 65_000,
            scan_span_ms: crate::video::SELECTION_SPAN_MS,
            has_audio: true,
            next_ms: 15_000,
            last_sample_ms: Some(14_000),
            last_gray: vec![7; ws_index::model::VIDEO_GRAY_PIXELS],
            candidates_ms: vec![0, 5_000, 10_000],
            next_periodic_ms: 15_000,
            sample_count: 15,
        };
        req.cursor = crate::video::selection_cursor(first.next_ms);
        req.video_selection = Some(first.clone());
        let mut next = first.clone();
        next.next_ms = 30_000;
        next.last_sample_ms = Some(29_000);
        next.last_gray = vec![9; ws_index::model::VIDEO_GRAY_PIXELS];
        next.candidates_ms.extend([15_000, 20_000, 25_000]);
        next.next_periodic_ms = 30_000;
        next.sample_count = 30;
        let mut response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(crate::video::selection_cursor(30_000)),
            total_units: None,
            diagnostics: vec![],
            media: None,
            video_selection: Some(next.clone()),
        };
        validate_response(&req, &response).unwrap();
        let mut invalid_previous = req.clone();
        invalid_previous.video_selection.as_mut().unwrap().next_ms = u64::MAX;
        assert!(
            validate_response(&invalid_previous, &response).is_err(),
            "invalid previous state cannot overflow slice arithmetic"
        );
        let mut changed_quantum = response.clone();
        changed_quantum
            .video_selection
            .as_mut()
            .unwrap()
            .scan_span_ms = 30_000;
        assert!(
            validate_response(&req, &changed_quantum).is_err(),
            "continued scan quantum is immutable"
        );
        response.next_cursor = Some(req.cursor);
        assert!(validate_response(&req, &response).is_err(), "replay");
        response.next_cursor = Some(crate::video::selection_cursor(45_000));
        assert!(validate_response(&req, &response).is_err(), "skipped slice");
        response.next_cursor = Some(crate::video::selection_cursor(30_000));
        response.video_selection.as_mut().unwrap().candidates_ms[0] = 1;
        assert!(
            validate_response(&req, &response).is_err(),
            "mutated persisted candidates"
        );
        response.video_selection = Some(next.clone());
        response.video_selection.as_mut().unwrap().last_gray.pop();
        assert!(
            validate_response(&req, &response).is_err(),
            "invalid gray bytes"
        );
        response.video_selection = Some(next);
        response
            .output
            .segments
            .push(SegmentDraft::text("video", "premature"));
        assert!(
            validate_response(&req, &response).is_err(),
            "selection has no drafts"
        );
        response.output.segments.clear();
        response.media = Some(MediaBatch {
            duration_ms: 65_000,
            frames_ms: vec![0],
            has_audio: true,
        });
        assert!(
            validate_response(&req, &response).is_err(),
            "premature plan"
        );
        let mut initial = request();
        initial.kind = "video".into();
        initial.max_units = 1;
        let skipped = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(crate::video::frame_cursor(1, 0)),
            total_units: Some(4),
            diagnostics: vec![],
            media: Some(MediaBatch {
                duration_ms: 65_000,
                frames_ms: vec![0],
                has_audio: true,
            }),
            video_selection: None,
        };
        assert_eq!(
            validate_response(&initial, &skipped).unwrap_err(),
            "long video skipped selection checkpoint"
        );
    }

    #[test]
    fn media_audio_progress_allows_silence_and_rejects_forged_times_or_inputs() {
        let mut req = request();
        req.kind = "audio".into();
        req.max_units = 30;
        let mut response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(30),
            total_units: Some(48),
            diagnostics: vec![],
            media: Some(MediaBatch {
                duration_ms: 1_200_000,
                frames_ms: vec![],
                has_audio: true,
            }),
            video_selection: None,
        };
        validate_response(&req, &response).unwrap();
        response.next_cursor = Some(31);
        assert!(validate_response(&req, &response).is_err());
        response.next_cursor = Some(30);
        let mut draft = SegmentDraft::text("audio", "");
        draft.start_ms = Some(25000);
        draft.end_ms = Some(55000);
        draft.embedding_input = crate::DraftEmbeddingInput::Audio {
            path: PathBuf::from("C:/private/window.wav"),
        };
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![segment]),
            next_cursor: None,
            total_units: Some(1),
            diagnostics: Vec::new(),
            media: None,
            video_selection: None,
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
    fn code_response_requires_typed_forward_units() {
        let mut req = request();
        req.kind = "code".into();
        let response = ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(1),
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
            video_selection: None,
        };
        assert!(
            validate_response(&req, &response).is_err(),
            "code cursor advanced without any typed source units"
        );
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![text.clone(), ocr]),
            next_cursor: Some(1 << 32),
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
            video_selection: None,
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![a.clone(), b]),
            next_cursor: None,
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
            video_selection: None,
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![draft.clone()]),
            next_cursor: None,
            total_units: Some(1),
            diagnostics: vec![],
            media: None,
            video_selection: None,
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![SegmentDraft::text("doc", "one")]),
            next_cursor: None,
            total_units: Some(75),
            diagnostics: Vec::new(),
            media: None,
            video_selection: None,
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![SegmentDraft::text("doc", "x"); 257]),
            next_cursor: None,
            total_units: None,
            diagnostics: Vec::new(),
            media: None,
            video_selection: None,
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
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(vec![
                SegmentDraft::text("doc", "one"),
                SegmentDraft::text("doc", "two"),
            ]),
            next_cursor: None,
            total_units: None,
            diagnostics: Vec::new(),
            media: None,
            video_selection: None,
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
