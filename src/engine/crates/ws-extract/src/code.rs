//! Native code parse batches. Source stays inside the existing owned child;
//! cursors cover chunks followed by symbols, without truncating either tail.
use crate::{
    worker::{ExtractRequest, ExtractResponse},
    ExtractBudget, ExtractError, ExtractOutput, ExtractionStatus,
};
use serde::{Deserialize, Serialize};
use ws_code::{model::NotebookLocation, ExtractedSymbol, ParsedCodeUnit};

pub const MAX_CODE_BATCH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeBatch {
    pub language: String,
    pub source_sha256: String,
    pub units: Vec<CodeUnit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CodeUnit {
    Chunk {
        ordinal: u64,
        chunk: ParsedCodeUnit,
    },
    Symbol {
        ordinal: u64,
        symbol: ExtractedSymbol,
    },
}

impl CodeUnit {
    pub fn ordinal(&self) -> u64 {
        match self {
            Self::Chunk { ordinal, .. } | Self::Symbol { ordinal, .. } => *ordinal,
        }
    }
}

pub fn extract_code_batch(
    request: &ExtractRequest,
    budget: &ExtractBudget,
) -> Result<ExtractResponse, ExtractError> {
    budget.check_deadline()?;
    let (parsed,source_sha256) = ws_code::parse_file_with_sha256(&request.path)?;
    budget.check_deadline()?;
    let total = (parsed.chunks.len() + parsed.symbols.len()) as u64;
    if request.cursor > total || request.max_units == 0 {
        return Err(ExtractError::Decode("invalid code cursor/budget".into()));
    }
    let mut batch=CodeBatch {language:parsed.language,source_sha256,units:Vec::new()};
    let mut bytes = serde_json::to_vec(&batch).map_err(|e| ExtractError::Decode(e.to_string()))?.len();
    for ordinal in request.cursor..total {
        budget.check_deadline()?;
        if batch.units.len() >= request.max_units as usize {
            break;
        }
        let unit = if ordinal < parsed.chunks.len() as u64 {
            CodeUnit::Chunk {
                ordinal,
                chunk: parsed.chunks[ordinal as usize].clone(),
            }
        } else {
            CodeUnit::Symbol {
                ordinal,
                symbol: parsed.symbols[ordinal as usize - parsed.chunks.len()].clone(),
            }
        };
        let encoded = serde_json::to_vec(&unit).map_err(|e| ExtractError::Decode(e.to_string()))?;
        if bytes + encoded.len() + 1 > MAX_CODE_BATCH_BYTES {
            break;
        }
        bytes += encoded.len() + 1;
        batch.units.push(unit);
    }
    if batch.units.is_empty() && request.cursor < total {
        return Err(ExtractError::Decode(
            "code unit exceeds response budget".into(),
        ));
    }
    let end = request.cursor + batch.units.len() as u64;
    Ok(ExtractResponse {
        source_sha256: None,
        output: ExtractOutput::extracted(Vec::new()),
        next_cursor: (end < total).then_some(end),
        total_units: Some(total),
        diagnostics: parsed
            .diagnostics
            .into_iter()
            .take(8)
            .map(|d| d.chars().take(256).collect())
            .collect(),
        media: None,
        video_selection: None,
        code: Some(batch),
    })
}

fn valid_lines(start: u32, end: u32, notebook: Option<NotebookLocation>) -> bool {
    start > 0
        && end >= start
        && u64::from(end) <= ws_code::MAX_CODE_FILE_BYTES + 1
        && notebook.is_none_or(|cell| {
            cell.start_line > 0
                && cell.end_line >= cell.start_line
                && cell.start_line <= start
                && cell.end_line <= end
                && cell.end_line - cell.start_line == end - start
                && u64::from(cell.cell_ordinal) <= ws_code::MAX_CODE_FILE_BYTES
        })
}

pub(crate) fn validate_batch(
    request: &ExtractRequest,
    response: &ExtractResponse,
) -> Result<(), String> {
    if response.media.is_some() || !response.output.segments.is_empty() {
        return Err("code response contains ordinary content/media".into());
    }
    if !matches!(
        response.output.status,
        ExtractionStatus::Extracted | ExtractionStatus::ExactOnly
    ) {
        if response.code.is_some()
            || response.next_cursor.is_some()
            || response.total_units.is_some()
        {
            return Err("failed code response contains progress".into());
        }
        if let ExtractionStatus::Failed(message) = &response.output.status {
            if message.len() > crate::worker::MAX_SEGMENT_BYTES {
                return Err("failure message too long".into());
            }
        }
        return Ok(());
    }
    let code = response.code.as_ref().ok_or("typed code batch missing")?;
    if code.source_sha256.len()!=64 || !code.source_sha256.bytes().all(|b|b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err("invalid code source SHA256".into());
    }
    if code.language.is_empty()
        || code.language.len() > 64
        || code.units.len() > request.max_units as usize
        || code.units.len() > crate::worker::MAX_DRAFTS
        || serde_json::to_vec(code).map_err(|e| e.to_string())?.len() > MAX_CODE_BATCH_BYTES
    {
        return Err("code batch exceeds bounds".into());
    }
    let total = response.total_units.ok_or("code total missing")?;
    let end = request
        .cursor
        .checked_add(code.units.len() as u64)
        .ok_or("code cursor overflow")?;
    if total > ws_code::MAX_CODE_FILE_BYTES
        || request.cursor > total
        || end > total
        || (end < total && code.units.is_empty())
        || response.next_cursor != (end < total).then_some(end)
    {
        return Err("invalid code progress".into());
    }
    for (offset, unit) in code.units.iter().enumerate() {
        if unit.ordinal() != request.cursor + offset as u64 {
            return Err("noncontiguous code units".into());
        }
        let valid = match unit {
            CodeUnit::Chunk { chunk, .. } => {
                chunk.text.len() <= ws_code::parse::MAX_CHUNK_BYTES
                    && chunk.symbol_name.len() <= 1024
                    && chunk.symbol_kind.len() <= 64
                    && valid_lines(chunk.start_line, chunk.end_line, chunk.notebook)
            }
            CodeUnit::Symbol { symbol, .. } => {
                !symbol.name.is_empty()
                    && symbol.name.len() <= 1024
                    && symbol.kind.len() <= 64
                    && symbol.signature.len() <= 1024
                    && valid_lines(symbol.line, symbol.end_line, symbol.notebook)
            }
        };
        if !valid {
            return Err("invalid code unit metadata".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_provenance_batch_carries_exact_source_hash_in_every_continuation() {
        let path=std::env::temp_dir().join(format!("winsearch-code-provenance-batch-{}.rs",std::process::id()));
        std::fs::write(&path,b"\xef\xbb\xbfpub fn a() {}\r\npub fn b() {}\r\n").unwrap();
        let mut request=ExtractRequest {path:path.clone(),kind:"code".into(),scratch_dir:std::env::temp_dir(),
            cursor:0,max_units:1,media_plan:None,video_selection:None};
        let mut batches=0;
        loop {
            let response=extract_code_batch(&request,&ExtractBudget::default()).unwrap();
            crate::worker::validate_response(&request,&response).unwrap();
            assert_eq!(response.code.unwrap().source_sha256,"59f56ce02354434d0d9ace1fe5f8ca17f222ea026adedda751014489ec367db9");
            batches+=1;
            if let Some(next)=response.next_cursor {request.cursor=next;} else {break;}
        }
        std::fs::remove_file(path).unwrap();
        assert!(batches>1,"fixture must exercise real continued batches");
    }

    #[test]
    fn parent_rejects_skipped_ordinals_and_forged_locations_or_payloads() {
        let request = ExtractRequest {
            path: "C:/source.rs".into(),
            kind: "code".into(),
            scratch_dir: "C:/scratch".into(),
            cursor: 0,
            max_units: 1,
            media_plan: None,
            video_selection: None,
        };
        let valid = ExtractResponse {
            source_sha256: None,
            output: ExtractOutput::extracted(vec![]),
            next_cursor: Some(1),
            total_units: Some(2),
            diagnostics: vec![],
            media: None,
            video_selection: None,
            code: Some(CodeBatch {
                language: "rust".into(),
                source_sha256: "0".repeat(64),
                units: vec![CodeUnit::Chunk {
                    ordinal: 0,
                    chunk: ParsedCodeUnit {
                        notebook: None,
                        start_line: 1,
                        end_line: 1,
                        symbol_name: "f".into(),
                        symbol_kind: "function".into(),
                        text: "fn f() {}".into(),
                    },
                }],
            }),
        };
        crate::worker::validate_response(&request, &valid).unwrap();
        for case in 0..13 {
            let mut invalid = valid.clone();
            match case {
                0 => invalid.next_cursor = Some(2),
                1 => {
                    let CodeUnit::Chunk { ordinal, .. } =
                        &mut invalid.code.as_mut().unwrap().units[0]
                    else {
                        unreachable!()
                    };
                    *ordinal = 1;
                }
                2 => {
                    let CodeUnit::Chunk { chunk, .. } =
                        &mut invalid.code.as_mut().unwrap().units[0]
                    else {
                        unreachable!()
                    };
                    chunk.text = "x".repeat(ws_code::parse::MAX_CHUNK_BYTES + 1);
                }
                3 => {
                    let CodeUnit::Chunk { chunk, .. } =
                        &mut invalid.code.as_mut().unwrap().units[0]
                    else {
                        unreachable!()
                    };
                    chunk.notebook = Some(NotebookLocation {
                        cell_ordinal: 0,
                        start_line: 1,
                        end_line: 3,
                    });
                }
                4 => invalid
                    .output
                    .segments
                    .push(crate::SegmentDraft::text("doc", "wrong channel")),
                5 => invalid.code = None,
                6 | 7 => {
                    let CodeUnit::Chunk { chunk, .. } =
                        &mut invalid.code.as_mut().unwrap().units[0]
                    else {
                        unreachable!()
                    };
                    let local = if case == 6 { 2 } else { u32::MAX };
                    chunk.notebook = Some(NotebookLocation {
                        cell_ordinal: 0,
                        start_line: local,
                        end_line: local,
                    });
                }
                8 => invalid.code.as_mut().unwrap().source_sha256.clear(),
                9 => invalid.code.as_mut().unwrap().source_sha256="g".repeat(64),
                10 => invalid.code.as_mut().unwrap().source_sha256="a".repeat(65),
                11 => invalid.code.as_mut().unwrap().source_sha256="A".repeat(64),
                12 => invalid.source_sha256=Some("a".repeat(64)),
                _ => unreachable!(),
            }
            assert!(
                crate::worker::validate_response(&request, &invalid).is_err(),
                "forged case{case} accepted"
            );
        }
    }
}
