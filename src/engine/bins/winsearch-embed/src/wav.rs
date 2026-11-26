//! Bounded PCM16 mono16k WAV windows; compressed media is decoded by extraction.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn read_pcm16(path: &Path, window: Option<(u64, u64)>) -> io::Result<Vec<f32>> {
    if window.is_some_and(|(s, e)| e <= s || e - s > 30_000 || e.checked_mul(16).is_none()) {
        return Err(invalid(
            "window must be a forward range of at most30seconds",
        ));
    }
    if !path.is_absolute() || !ws_platform::content::local_content_allowed(path) {
        return Err(invalid("not an allowed resident local file"));
    }
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() {
        return Err(invalid("not a regular file"));
    }
    let mut header = [0u8; 12];
    file.read_exact(&mut header)?;
    let end = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64 + 8;
    if &header[..4] != b"RIFF" || &header[8..] != b"WAVE" || end != before.len() {
        return Err(invalid("invalid RIFF/WAVE size"));
    }
    let mut position = 12u64;
    let mut format = false;
    let mut data = None;
    for _ in 0..128 {
        if position.checked_add(8).is_none_or(|v| v > end) || position > 1_048_576 {
            break;
        }
        file.seek(SeekFrom::Start(position))?;
        let mut chunk = [0u8; 8];
        file.read_exact(&mut chunk)?;
        let size = u32::from_le_bytes(chunk[4..].try_into().unwrap()) as u64;
        let start = position + 8;
        let next = start
            .checked_add(size)
            .and_then(|n| n.checked_add(size & 1))
            .ok_or_else(|| invalid("chunk overflow"))?;
        if next > end {
            return Err(invalid("chunk exceeds file"));
        }
        match &chunk[..4] {
            b"fmt " => {
                if format || !(16..=64).contains(&size) {
                    return Err(invalid("invalid format chunk"));
                }
                let mut fmt = [0u8; 16];
                file.read_exact(&mut fmt)?;
                let u16_at = |n| u16::from_le_bytes([fmt[n], fmt[n + 1]]);
                if u16_at(0) != 1
                    || u16_at(2) != 1
                    || u16_at(12) != 2
                    || u16_at(14) != 16
                    || u32::from_le_bytes(fmt[4..8].try_into().unwrap()) != 16000
                    || u32::from_le_bytes(fmt[8..12].try_into().unwrap()) != 32000
                {
                    return Err(invalid("requires uncompressed PCM16 mono16000Hz"));
                }
                format = true;
            }
            b"data" => {
                if !format || size == 0 || !size.is_multiple_of(2) {
                    return Err(invalid("invalid audio data chunk"));
                }
                data = Some((start, size / 2));
                break;
            }
            _ => {}
        }
        position = next;
    }
    let (offset, frames) = data.ok_or_else(|| invalid("missing bounded WAV header/data"))?;
    let (start, finish) = match window {
        Some((s, e)) => (
            s.checked_mul(16)
                .ok_or_else(|| invalid("window overflow"))?,
            e.checked_mul(16)
                .ok_or_else(|| invalid("window overflow"))?,
        ),
        None => (0, frames),
    };
    if finish <= start || finish > frames || finish - start > 480_000 {
        return Err(invalid("window exceeds audio or30seconds"));
    }
    let mut bytes = vec![0u8; ((finish - start) * 2) as usize];
    file.seek(SeekFrom::Start(offset + start * 2))?;
    file.read_exact(&mut bytes)?;
    let after = file.metadata()?;
    let current = std::fs::symlink_metadata(path)?;
    use std::os::windows::fs::MetadataExt;
    if before.len() != after.len()
        || before.last_write_time() != after.last_write_time()
        || current.len() != after.len()
        || current.last_write_time() != after.last_write_time()
        || !ws_platform::content::local_content_allowed(path)
    {
        return Err(invalid("source changed during audio read"));
    }
    Ok(bytes
        .chunks_exact(2)
        .map(|s| i16::from_le_bytes([s[0], s[1]]) as f32 / 32768.0)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_seek_skips_metadata_and_rejects_long_whole_file() {
        let path = std::env::temp_dir().join(format!(
            "winsearch-wav-録音 space-{}-{}.wav",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut bytes = b"RIFF\0\0\0\0WAVEfmt ".to_vec();
        bytes.extend(16u32.to_le_bytes());
        bytes.extend(1u16.to_le_bytes());
        bytes.extend(1u16.to_le_bytes());
        bytes.extend(16000u32.to_le_bytes());
        bytes.extend(32000u32.to_le_bytes());
        bytes.extend(2u16.to_le_bytes());
        bytes.extend(16u16.to_le_bytes());
        bytes.extend(b"JUNK");
        bytes.extend(3u32.to_le_bytes());
        bytes.extend(b"abc\0");
        bytes.extend(b"data");
        bytes.extend((65u32 * 32000).to_le_bytes());
        for _ in 0..65 * 16000 {
            bytes.extend(16384i16.to_le_bytes());
        }
        let size = bytes.len() as u32 - 8;
        bytes[4..8].copy_from_slice(&size.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_pcm16(&path, None).is_err());
        let pcm = read_pcm16(&path, Some((25_000, 55_000))).unwrap();
        assert_eq!(pcm.len(), 480_000);
        assert!(pcm.iter().all(|v| *v == 0.5));
        assert_eq!(
            read_pcm16(&path, Some((50_000, 65_000))).unwrap().len(),
            240_000
        );
        for window in [
            (0, 30_001),
            (66_000, 67_000),
            (u64::MAX - 1, u64::MAX),
            (10, 10),
            (20, 10),
        ] {
            assert!(read_pcm16(&path, Some(window)).is_err());
        }
        bytes[22] = 2;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_pcm16(&path, Some((0, 1000))).is_err());
        std::fs::remove_file(&path).unwrap();
    }
}
