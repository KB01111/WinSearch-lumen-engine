//! Revision-scoped frame selection, bounded frame/OCR slices, then full soundtrack.
use crate::{
    audio, decoder,
    worker::{ExtractRequest, ExtractResponse, MAX_DRAFTS},
    DraftEmbeddingInput, ExtractBudget, ExtractError, ExtractOutput, SegmentDraft,
};
use std::io::Write;
use ws_index::model::{MediaPlan, VideoSelection, MAX_MEDIA_FRAMES};

pub const AUDIO_PHASE: u64 = 1 << 62;
pub const SELECTION_PHASE: u64 = 1 << 61;
pub const MAX_FRAME_DRAFTS: u32 = 128;
pub const SELECTION_SPAN_MS: u64 = ws_index::model::VIDEO_SELECTION_SPAN_MS;

fn selection_span(info: &decoder::MediaInfo, units: u32) -> u64 {
    // Stream rates are estimates; the owned 120s watchdog remains authoritative.
    let work_ms = info
        .video_pixels_per_second
        .filter(|pixels| *pixels > 0)
        .map(|pixels| 500_000_000_000u64 / pixels)
        .unwrap_or(SELECTION_SPAN_MS);
    (work_ms / SELECTION_SPAN_MS).clamp(1, u64::from(units.clamp(1, 30))) * SELECTION_SPAN_MS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cursor {
    Selection { next_ms: u64 },
    Frame { index: u32, draft: u32 },
    Audio { window: u64 },
}
pub fn frame_cursor(index: u32, draft: u32) -> u64 {
    (u64::from(index) << 16) | u64::from(draft)
}
pub fn audio_cursor(window: u64) -> u64 {
    AUDIO_PHASE | window
}
pub fn selection_cursor(next_ms: u64) -> u64 {
    SELECTION_PHASE | next_ms
}
pub fn decode_cursor(cursor: u64) -> Result<Cursor, String> {
    if cursor & (1 << 63) != 0 {
        return Err("invalid video cursor phase".into());
    }
    if cursor & AUDIO_PHASE != 0 {
        if cursor & SELECTION_PHASE != 0 {
            return Err("invalid video cursor phase".into());
        }
        let window = cursor & !AUDIO_PHASE;
        if window > audio::window_count(decoder::MAX_DURATION_MS) {
            return Err("invalid video audio cursor".into());
        }
        Ok(Cursor::Audio { window })
    } else if cursor & SELECTION_PHASE != 0 {
        let next_ms = cursor & !SELECTION_PHASE;
        if next_ms == 0 || next_ms >= decoder::MAX_DURATION_MS {
            return Err("invalid video selection cursor".into());
        }
        Ok(Cursor::Selection { next_ms })
    } else {
        let index = cursor >> 16;
        let draft = cursor & 65535;
        if index > MAX_MEDIA_FRAMES as u64
            || draft >= u64::from(MAX_FRAME_DRAFTS)
            || (index == MAX_MEDIA_FRAMES as u64 && draft != 0)
        {
            return Err("invalid video frame/draft cursor".into());
        }
        Ok(Cursor::Frame {
            index: index as u32,
            draft: draft as u32,
        })
    }
}

pub fn extract_video_batch(
    request: &ExtractRequest,
    budget: &ExtractBudget,
) -> Result<ExtractResponse, ExtractError> {
    let info = decoder::probe(&request.path, budget)?;
    if request.media_plan.is_none()
        && info.has_video
        && (request.video_selection.is_some() || info.duration_ms > SELECTION_SPAN_MS)
    {
        return select_batch(request, budget, &info);
    }
    if request.video_selection.is_some() {
        return Err(decoder::decode("selection and plan phase conflict"));
    }
    let plan = if let Some(plan) = &request.media_plan {
        plan.validate().map_err(decoder::decode)?;
        if plan.duration_ms != info.duration_ms
            || plan.has_audio != info.has_audio
            || plan.frames_ms.is_empty() == info.has_video
        {
            return Err(decoder::decode("video selection no longer matches source"));
        }
        plan.clone()
    } else {
        if request.cursor != 0 {
            return Err(decoder::decode(
                "continued video is missing its source selection",
            ));
        }
        build_plan(request, budget, &info)?
    };
    let cursor = decode_cursor(request.cursor).map_err(decoder::decode)?;
    if matches!(cursor, Cursor::Selection { .. }) {
        return Err(decoder::decode("selection cursor without checkpoint"));
    }
    if plan.frames_ms.is_empty() || matches!(cursor, Cursor::Audio { .. }) {
        if !plan.has_audio {
            return Err(decoder::decode("video has no soundtrack phase"));
        }
        let window = match cursor {
            Cursor::Audio { window } => window,
            Cursor::Frame { index: 0, draft: 0 } if plan.frames_ms.is_empty() => 0,
            _ => return Err(decoder::decode("invalid audio-only video cursor")),
        };
        let mut audio_request = request.clone();
        audio_request.cursor = window;
        let mut response = audio::extract_windows(&audio_request, budget, plan.duration_ms)?;
        response.next_cursor = response.next_cursor.map(audio_cursor);
        response.total_units = Some(total_units(&plan));
        response.media = Some(plan);
        return Ok(response);
    }
    let Cursor::Frame { index, draft } = cursor else {
        unreachable!()
    };
    let (mut index, mut draft) = (index as usize, draft as usize);
    if index >= plan.frames_ms.len() {
        return Err(decoder::decode("video frame cursor beyond selection"));
    }
    let first = index;
    let limit = budget.max_segments.min(MAX_DRAFTS);
    if limit == 0 || request.max_units == 0 {
        return Err(decoder::decode("video batch budget is zero"));
    }
    let mut segments = Vec::new();
    let mut diagnostics = Vec::new();
    while index < plan.frames_ms.len() && index - first < request.max_units.min(30) as usize {
        budget.check_deadline()?;
        let mut drafts = frame_drafts(request, budget, &plan, index, &mut diagnostics)?;
        if draft >= drafts.len() {
            return Err(decoder::decode("video draft cursor outside actual frame"));
        }
        let take = (limit - segments.len()).min(drafts.len() - draft);
        let complete = draft + take == drafts.len();
        segments.extend(drafts.drain(draft..draft + take));
        if complete {
            index += 1;
            draft = 0;
        } else {
            draft += take;
        }
        if segments.len() == limit {
            break;
        }
    }
    let next_cursor = if index < plan.frames_ms.len() {
        Some(frame_cursor(index as u32, draft as u32))
    } else if plan.has_audio {
        Some(audio_cursor(0))
    } else {
        None
    };
    Ok(ExtractResponse {
        source_sha256: None,
        code: None,
        output: ExtractOutput::extracted(segments),
        next_cursor,
        total_units: Some(total_units(&plan)),
        diagnostics,
        media: Some(plan),
        video_selection: None,
    })
}

fn select_batch(
    request: &ExtractRequest,
    budget: &ExtractBudget,
    info: &decoder::MediaInfo,
) -> Result<ExtractResponse, ExtractError> {
    if request.media_plan.is_some() || !info.has_video {
        return Err(decoder::decode("invalid video selection phase"));
    }
    let mut state = if let Some(saved) = &request.video_selection {
        saved.validate().map_err(decoder::decode)?;
        if saved.duration_ms != info.duration_ms
            || saved.has_audio != info.has_audio
            || request.cursor != selection_cursor(saved.next_ms)
        {
            return Err(decoder::decode(
                "video selection no longer matches source/cursor",
            ));
        }
        saved.clone()
    } else {
        if request.cursor != 0 {
            return Err(decoder::decode("continued selection has no checkpoint"));
        }
        VideoSelection {
            duration_ms: info.duration_ms,
            scan_span_ms: selection_span(info, request.max_units),
            has_audio: info.has_audio,
            next_ms: 0,
            last_sample_ms: None,
            last_gray: Vec::new(),
            candidates_ms: Vec::new(),
            next_periodic_ms: 0,
            sample_count: 0,
        }
    };
    let start = state.next_ms;
    let end = (start + state.scan_span_ms).min(info.duration_ms);
    let interval = info
        .duration_ms
        .div_ceil(decoder::MAX_SHOT_SAMPLES as u64)
        .max(1000);
    let periodic = info
        .duration_ms
        .div_ceil((MAX_MEDIA_FRAMES - 1) as u64)
        .max(5000);
    let samples = decoder::sample_video_slice(
        &request.path,
        start,
        end,
        interval,
        state.last_sample_ms,
        budget,
    )?;
    if state.sample_count as usize + samples.len() > decoder::MAX_SHOT_SAMPLES {
        return Err(decoder::decode("shot sample cap exceeded"));
    }
    for sample in samples {
        if state.last_sample_ms.is_none() {
            push_candidate(&mut state.candidates_ms, sample.time_ms);
        }
        while state.next_periodic_ms < info.duration_ms && sample.time_ms >= state.next_periodic_ms
        {
            push_candidate(&mut state.candidates_ms, sample.time_ms);
            state.next_periodic_ms += periodic;
        }
        if !state.last_gray.is_empty() {
            let change: u64 = state
                .last_gray
                .iter()
                .zip(&sample.gray)
                .map(|(a, b)| u64::from(a.abs_diff(*b)))
                .sum();
            if change >= 38 * decoder::SHOT_PIXELS as u64 {
                push_candidate(&mut state.candidates_ms, sample.time_ms);
            }
        }
        state.last_sample_ms = Some(sample.time_ms);
        state.last_gray = sample.gray;
        state.sample_count += 1;
    }
    state.next_ms = end;
    if end < info.duration_ms {
        state.validate().map_err(decoder::decode)?;
        return Ok(ExtractResponse {
            source_sha256: None,
            code: None,
            output: ExtractOutput::extracted(Vec::new()),
            next_cursor: Some(selection_cursor(end)),
            total_units: None,
            diagnostics: Vec::new(),
            media: None,
            video_selection: Some(state),
        });
    }
    if let Some(last) = state.last_sample_ms {
        push_candidate(&mut state.candidates_ms, last);
    }
    let mut backoff = 1000u64;
    loop {
        let time = info.duration_ms.saturating_sub(backoff);
        if let Some(frame) = decoder::video_frame(&request.path, time, budget)? {
            if frame.time_ms >= info.duration_ms {
                return Err(decoder::decode("late frame outside duration"));
            }
            push_candidate(&mut state.candidates_ms, frame.time_ms);
            break;
        }
        if time == 0 {
            return Err(decoder::decode("video contains no decodable frame"));
        }
        backoff = (backoff * 2).min(info.duration_ms);
    }
    state.validate().map_err(decoder::decode)?;
    let plan = MediaPlan {
        duration_ms: info.duration_ms,
        frames_ms: select_frames(state.candidates_ms.clone()),
        has_audio: info.has_audio,
    };
    plan.validate().map_err(decoder::decode)?;
    Ok(ExtractResponse {
        source_sha256: None,
        code: None,
        output: ExtractOutput::extracted(Vec::new()),
        next_cursor: Some(0),
        total_units: Some(total_units(&plan)),
        diagnostics: Vec::new(),
        media: Some(plan),
        video_selection: Some(state),
    })
}

fn push_candidate(candidates: &mut Vec<u64>, time: u64) {
    if candidates.last().is_none_or(|last| *last < time) {
        candidates.push(time);
    }
}

pub fn total_units(plan: &MediaPlan) -> u64 {
    plan.frames_ms.len() as u64
        + if plan.has_audio {
            audio::window_count(plan.duration_ms)
        } else {
            0
        }
}

fn build_plan(
    request: &ExtractRequest,
    budget: &ExtractBudget,
    info: &decoder::MediaInfo,
) -> Result<MediaPlan, ExtractError> {
    let mut frames_ms = Vec::new();
    if info.has_video {
        let interval = info
            .duration_ms
            .div_ceil(decoder::MAX_SHOT_SAMPLES as u64)
            .max(1000);
        let samples = decoder::sample_video(&request.path, interval, budget)?;
        if samples.iter().any(|s| s.time_ms >= info.duration_ms) {
            return Err(decoder::decode("sample outside source duration"));
        }
        let times: Vec<_> = samples.iter().map(|s| s.time_ms).collect();
        let periodic = info
            .duration_ms
            .div_ceil((MAX_MEDIA_FRAMES - 1) as u64)
            .max(5000);
        let mut next = 0;
        let mut position = 0;
        while next < info.duration_ms {
            while position < times.len() && times[position] < next {
                position += 1;
            }
            if position == times.len() {
                break;
            }
            frames_ms.push(times[position]);
            next += periodic;
        }
        for pair in samples.windows(2) {
            // Mean absolute grayscale change >=15% is a candidate cut; selection
            // remains bounded even for motion, flashes or adversarial rapid cuts.
            let change: u64 = pair[0]
                .gray
                .iter()
                .zip(&pair[1].gray)
                .map(|(a, b)| u64::from(a.abs_diff(*b)))
                .sum();
            if change >= 38 * decoder::SHOT_PIXELS as u64 {
                frames_ms.push(pair[1].time_ms);
            }
        }
        frames_ms.push(times[0]);
        frames_ms.push(*times.last().unwrap());
        // EOF may follow a held sparse frame. Seek backwards exponentially until
        // an actual frame exists, retaining its exact source PTS.
        let mut backoff = 1000u64;
        loop {
            let time = info.duration_ms.saturating_sub(backoff);
            if let Some(frame) = decoder::video_frame(&request.path, time, budget)? {
                if frame.time_ms >= info.duration_ms {
                    return Err(decoder::decode("late frame outside duration"));
                }
                frames_ms.push(frame.time_ms);
                break;
            }
            if time == 0 {
                return Err(decoder::decode("video contains no decodable frame"));
            }
            backoff = (backoff * 2).min(info.duration_ms);
        }
        frames_ms = select_frames(frames_ms);
    }
    let plan = MediaPlan {
        duration_ms: info.duration_ms,
        frames_ms,
        has_audio: info.has_audio,
    };
    plan.validate().map_err(decoder::decode)?;
    Ok(plan)
}

pub(crate) fn select_frames(mut candidates: Vec<u64>) -> Vec<u64> {
    candidates.sort_unstable();
    candidates.dedup();
    let Some(&last) = candidates.last() else {
        return vec![];
    };
    let mut selected = Vec::new();
    for time in candidates {
        if selected.last().is_none_or(|p| time > p + 500) {
            selected.push(time);
        }
    }
    if selected.last() != Some(&last) && selected.len() > 1 {
        selected.pop();
        if selected.last().is_some_and(|p| last > p + 500) {
            selected.push(last);
        }
    }
    if selected.len() <= MAX_MEDIA_FRAMES {
        return selected;
    }
    // Evenly sample the ordered union, preserving earliest/latest coverage.
    (0..MAX_MEDIA_FRAMES)
        .map(|i| selected[i * (selected.len() - 1) / (MAX_MEDIA_FRAMES - 1)])
        .collect()
}

fn frame_drafts(
    request: &ExtractRequest,
    budget: &ExtractBudget,
    plan: &MediaPlan,
    index: usize,
    diagnostics: &mut Vec<String>,
) -> Result<Vec<SegmentDraft>, ExtractError> {
    let time = plan.frames_ms[index];
    let frame = decoder::video_frame(&request.path, time, budget)?
        .ok_or_else(|| decoder::decode("selected video frame cannot be decoded"))?;
    if frame.time_ms != time {
        return Err(decoder::decode(
            "decoded source PTS differs from selected frame",
        ));
    }
    let artifact = request.scratch_dir.join(format!("frame-{index}.png"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&artifact)?;
    file.write_all(&frame.png)?;
    drop(file);
    let raster = ws_platform::wic::decode_image_wic(&artifact, 1280).map_err(decoder::decode)?;
    if raster.width > 1280 || raster.height > 1280 {
        return Err(decoder::decode("video frame exceeds raster bound"));
    }
    budget.check_deadline()?;
    let ocr = match ws_platform::ocr::recognize_bgra8_winrt(
        &raster.bgra8,
        raster.scaled_width,
        raster.scaled_height,
        crate::DEFAULT_MAX_OCR_LANGUAGES,
    ) {
        Ok(value) if value.language_tag.is_some() => value.text,
        value => {
            let message = if value.is_ok() {
                "No installed OCR recognizer; video visual search remains available"
            } else {
                "Video frame OCR failed; visual search remains available"
            };
            if diagnostics.len() < 8 && !diagnostics.iter().any(|d| d == message) {
                diagnostics.push(message.into());
            }
            String::new()
        }
    };
    budget.check_deadline()?;
    if ocr.chars().take(500001).count() > 500000 {
        return Err(decoder::decode("video OCR exceeds bounded text"));
    }
    let mut image = SegmentDraft::text("video", "");
    image.start_ms = Some(time);
    image.end_ms = Some(time);
    image.thumbnail_hint = Some(artifact.clone());
    image.embedding_input = DraftEmbeddingInput::Image {
        path: artifact.clone(),
        token_budget: if ocr.chars().filter(|c| c.is_alphanumeric()).take(8).count() >= 8 {
            140
        } else {
            70
        },
    };
    let mut drafts = vec![image];
    for text in crate::chunk_text_with_lines(
        &ocr,
        crate::DEFAULT_CHUNK_CHARS,
        crate::DEFAULT_OVERLAP_RATIO,
    ) {
        if drafts.len() >= MAX_FRAME_DRAFTS as usize {
            return Err(decoder::decode("video frame OCR draft limit exceeded"));
        }
        let mut draft = SegmentDraft::text("ocr", text.text);
        draft.start_ms = Some(time);
        draft.end_ms = Some(time);
        draft.thumbnail_hint = Some(artifact.clone());
        draft.embed = !crate::is_numeric_dominated(&draft.text, "video");
        drafts.push(draft);
    }
    Ok(drafts)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_work_budget_coalesces_sparse_sources_but_bounds_hd() {
        let info = |width, height, rate: &str| {
            decoder::parse_probe(serde_json::to_vec(&serde_json::json!({
            "streams":[{"codec_type":"video","codec_name":"mpeg4","width":width,"height":height,
                "avg_frame_rate":rate,"r_frame_rate":rate}],"format":{"duration":"7200"}
        })).unwrap().as_slice()).unwrap()
        };
        let sparse = info(160, 100, "1/1");
        assert_eq!(selection_span(&sparse, 30), 450_000);
        assert_eq!(selection_span(&sparse, 1), 15_000);
        assert_eq!(selection_span(&info(1920, 1080, "15/1"), 30), 15_000);
        for unknown in ["0/0", "N/A", "-1/1", "999999999999999999999999999/1"] {
            assert_eq!(selection_span(&info(160, 100, unknown), 30), 15_000);
        }
    }
    #[test]
    fn selection_deduplicates_and_evenly_caps_preserving_endpoints() {
        let selected = select_frames((0..20000).map(|i| i * 1000).collect());
        assert_eq!(selected.len(), 1200);
        assert_eq!(selected.first(), Some(&0));
        assert_eq!(selected.last(), Some(&19_999_000));
        assert!(selected.windows(2).all(|p| p[1] > p[0] + 500));
        assert_eq!(select_frames(vec![0, 100, 500, 1000, 1200]), vec![0, 1200]);
    }
    #[test]
    fn cursor_phase_and_bounds_do_not_confuse_source_units() {
        assert_eq!(
            decode_cursor(audio_cursor(30)).unwrap(),
            Cursor::Audio { window: 30 }
        );
        assert_eq!(
            decode_cursor(frame_cursor(120, 127)).unwrap(),
            Cursor::Frame {
                index: 120,
                draft: 127
            }
        );
        for value in [
            1 << 63,
            frame_cursor(1201, 0),
            frame_cursor(1, 128),
            audio_cursor(u64::MAX >> 2),
        ] {
            assert!(decode_cursor(value).is_err(), "{value}");
        }
    }
}
