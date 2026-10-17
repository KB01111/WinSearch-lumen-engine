//! Thirty-second PCM windows, 25-second hop, source-relative locations.
use crate::{
    decoder,
    worker::{ExtractRequest, ExtractResponse},
    DraftEmbeddingInput, ExtractBudget, ExtractError, ExtractOutput, SegmentDraft,
};
use std::{io::Write, path::Path};

pub const WINDOW_MS: u64 = 30_000;
pub const HOP_MS: u64 = 25_000;
pub fn window_count(duration_ms: u64) -> u64 {
    duration_ms.saturating_sub(WINDOW_MS).div_ceil(HOP_MS) + 1
}

pub fn extract_audio_batch(
    request: &ExtractRequest,
    budget: &ExtractBudget,
) -> Result<ExtractResponse, ExtractError> {
    let info = decoder::probe(&request.path, budget)?;
    if !info.has_audio {
        return Err(decoder::decode("media has no audio stream"));
    }
    extract_windows(request, budget, info.duration_ms)
}

pub(crate) fn extract_windows(
    request: &ExtractRequest,
    budget: &ExtractBudget,
    duration: u64,
) -> Result<ExtractResponse, ExtractError> {
    let total = window_count(duration);
    if request.cursor > total || request.max_units == 0 {
        return Err(decoder::decode("invalid audio cursor/budget"));
    }
    let end = total.min(request.cursor + u64::from(request.max_units.min(30)));
    let mut segments = Vec::new();
    for window in request.cursor..end {
        budget.check_deadline()?;
        let start_ms = window * HOP_MS;
        let end_ms = (start_ms + WINDOW_MS).min(duration);
        let pcm = decoder::audio_window(&request.path, start_ms, end_ms, budget)?;
        let samples = pcm.len() / 2;
        let rms = (pcm
            .chunks_exact(2)
            .map(|p| {
                let sample = f64::from(i16::from_le_bytes([p[0], p[1]])) / 32768.0;
                sample * sample
            })
            .sum::<f64>()
            / samples as f64)
            .sqrt();
        if rms < 0.003 {
            continue;
        }
        let artifact = request.scratch_dir.join(format!("audio-{window}.wav"));
        write_wav(&artifact, &pcm)?;
        let mut draft = SegmentDraft::text("audio", "");
        draft.start_ms = Some(start_ms);
        draft.end_ms = Some(end_ms.min(start_ms + (samples as u64 * 1000).div_ceil(16000)));
        draft.embedding_input = DraftEmbeddingInput::Audio { path: artifact };
        segments.push(draft);
    }
    Ok(ExtractResponse {
        output: ExtractOutput::extracted(segments),
        next_cursor: (end < total).then_some(end),
        total_units: Some(total),
        diagnostics: vec![],
        media: Some(crate::worker::MediaBatch {
            duration_ms: duration,
            frames_ms: vec![],
            has_audio: true,
        }),
    })
}

fn write_wav(path: &Path, pcm: &[u8]) -> Result<(), ExtractError> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let size = pcm.len() as u32;
    file.write_all(b"RIFF")?;
    file.write_all(&(size + 36).to_le_bytes())?;
    file.write_all(b"WAVEfmt ")?;
    file.write_all(&16u32.to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?;
    file.write_all(&16000u32.to_le_bytes())?;
    file.write_all(&32000u32.to_le_bytes())?;
    file.write_all(&2u16.to_le_bytes())?;
    file.write_all(&16u16.to_le_bytes())?;
    file.write_all(b"data")?;
    file.write_all(&size.to_le_bytes())?;
    file.write_all(pcm)?;
    Ok(())
}

/// Derived artifacts have a canonical 44-byte header, never arbitrary WAV chunks.
pub fn validate_window_wav(path: &Path) -> Result<u64, ExtractError> {
    use std::io::Read;
    let size = std::fs::metadata(path)?.len();
    if !(46..=decoder::MAX_PCM_BYTES as u64 + 44).contains(&size) || size % 2 != 0 {
        return Err(decoder::decode("derived WAV byte bound invalid"));
    }
    let mut header = [0u8; 44];
    std::fs::File::open(path)?.read_exact(&mut header)?;
    if &header[..4] != b"RIFF"
        || &header[8..16] != b"WAVEfmt "
        || header[16..36]
            != [
                16, 0, 0, 0, 1, 0, 1, 0, 128, 62, 0, 0, 0, 125, 0, 0, 2, 0, 16, 0,
            ]
        || &header[36..40] != b"data"
        || u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64 + 8 != size
        || u32::from_le_bytes(header[40..44].try_into().unwrap()) as u64 + 44 != size
    {
        return Err(decoder::decode("invalid derived PCM16 mono 16k WAV"));
    }
    Ok((size - 44) / 2)
}
