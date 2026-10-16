//! Internal, bounded extraction-worker protocol (no frontend arbitrary-file API).
use crate::{ExtractOutput, ExtractionStatus};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::PathBuf;

pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const MAX_DRAFTS: usize = 256;
pub const MAX_SEGMENT_BYTES: usize = 64 * 1024;
pub const SUPPORTED_KINDS: &[&str] = &["doc", "pdf", "image"];

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
    let segments = &response.output.segments;
    if segments.len() > MAX_DRAFTS || segments.len() > request.max_units as usize {
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
    if response.next_cursor.is_some_and(|next| {
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
        let valid_kind =
            draft.kind == request.kind || (request.kind == "image" && draft.kind == "ocr");
        if !valid_kind || draft.kind.len() > 32 || draft.text.len() > MAX_SEGMENT_BYTES {
            return Err("invalid draft kind or text length".into());
        }
        if let Some(p) = draft.page {
            if p == 0 || p < page {
                return Err("unordered pages".into());
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
    fn protocol_roundtrip_preserves_text_and_line_metadata() {
        let mut segment = SegmentDraft::text("doc", "actual text\nsecond line");
        segment.line = Some(1);
        let response = ExtractResponse {
            output: ExtractOutput::extracted(vec![segment]),
            next_cursor: None,
            total_units: Some(1),
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
    fn text_batches_reject_empty_progress_and_false_completion() {
        let mut response = ExtractResponse {
            output: ExtractOutput::extracted(vec![SegmentDraft::text("doc", "one")]),
            next_cursor: None,
            total_units: Some(75),
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
