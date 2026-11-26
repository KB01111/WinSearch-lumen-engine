//! Frame codec: 4-byte little-endian length prefix followed by a JSON payload.
//!
//! JSON over a compact length prefix is the debuggable default; the protocol
//! doc records that a binary payload format is only adopted if profiling shows
//! a measurable win.

use std::io::{Read, Write};

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Hard cap on a single frame. Search batches are small; a peer sending more
/// is malformed or hostile.
pub const MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame of {0} bytes exceeds the {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge(u32),
    #[error("malformed payload: {0}")]
    Malformed(#[from] serde_json::Error),
}

/// Writes one length-prefixed JSON frame.
pub fn write_message<W: Write, T: Serialize>(w: &mut W, msg: &T) -> Result<(), FrameError> {
    let payload = serde_json::to_vec(msg)?;
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::FrameTooLarge(u32::MAX))?;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::FrameTooLarge(len));
    }
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&payload)?;
    w.flush()?;
    Ok(())
}

/// Reads one length-prefixed JSON frame. Returns `Ok(None)` on a clean
/// end-of-stream (peer disconnected).
pub fn read_message<R: Read, T: DeserializeOwned>(r: &mut R) -> Result<Option<T>, FrameError> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(r, &mut len_buf)? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::FrameTooLarge(len));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;
    Ok(Some(serde_json::from_slice(&payload)?))
}

/// Reads exactly `buf.len()` bytes, reporting `false` if the stream ended
/// before any byte arrived and erroring on a truncated frame.
fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> Result<bool, FrameError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(FrameError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "stream ended mid-frame",
                )));
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ClientMessage, SearchMode, SearchQuery};

    #[test]
    fn frame_round_trip() {
        let msg = ClientMessage::Search {
            id: 9,
            query: SearchQuery {
                text: "invoice 2024-118".into(),
                mode: SearchMode::Files,
                ..Default::default()
            },
        };
        let mut buf = Vec::new();
        write_message(&mut buf, &msg).unwrap();
        let back: ClientMessage = read_message(&mut buf.as_slice()).unwrap().unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn empty_stream_reads_none() {
        let mut empty: &[u8] = &[];
        let got: Option<ClientMessage> = read_message(&mut empty).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_BYTES + 1).to_le_bytes());
        let err = read_message::<_, ClientMessage>(&mut buf.as_slice()).unwrap_err();
        assert!(matches!(err, FrameError::FrameTooLarge(_)));
    }

    #[test]
    fn truncated_frame_is_error() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(b"{}"); // 2 of 10 bytes
        let err = read_message::<_, ClientMessage>(&mut buf.as_slice()).unwrap_err();
        assert!(matches!(err, FrameError::Io(_)));
    }
}
