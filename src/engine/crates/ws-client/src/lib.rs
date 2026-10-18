//! Headless native IPC; this crate never starts an engine or accesses user files.
//!
//! The host owns the verified engine, instance, data directory and root grants.
//! Dropping an operation closes its owned transport; no client tasks are detached.
//!
//! ```no_run
//! use ws_client::{Client, protocol::{SearchMode, SearchQuery}};
//! # async fn example(pipe: String) -> Result<(), ws_client::Error> {
//! let client = Client::new(pipe)?;
//! let mut search = client.search(SearchQuery {
//!     text: "invoice".into(), mode: SearchMode::Files, limit: 20,
//!     ..Default::default()
//! }).await?;
//! while let Some(batch) = search.next().await? {
//!     // Recheck the host's current query/root grants before forwarding results.
//!     println!("{} results", batch.batch.len());
//! }
//! # Ok(())
//! # }
//! ```

use tokio::io::{AsyncRead, AsyncReadExt};
use ws_protocol::{FrameError, ServerMessage, MAX_FRAME_BYTES};

mod client;
mod transport;
pub use client::{Client, EngineEvent, SearchBatch, SearchStream, StatusSubscription};
pub use ws_protocol as protocol;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("engine I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine frame failed: {0}")]
    Frame(#[from] FrameError),
    #[error("engine connection closed")]
    Closed,
    #[error("engine protocol violation: {0}")]
    Protocol(&'static str),
    #[error("engine request timed out")]
    Timeout,
    #[error("engine refused request: {code:?}: {message}")]
    Remote {
        code: ws_protocol::ErrorCode,
        message: String,
    },
    #[error("unsupported engine capability: {0}")]
    Unsupported(&'static str),
    #[error("invalid client request: {0}")]
    InvalidInput(&'static str),
    #[error("mutation outcome is unknown; restart the owned engine and recreate this client before further mutations")]
    MutationOutcomeUnknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineInfo {
    pub engine_version: String,
    pub features: Vec<String>,
}

impl EngineInfo {
    pub fn supports(&self, feature: &str) -> bool {
        self.features.iter().any(|value| value == feature)
    }
}

async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> Result<ServerMessage, Error> {
    let mut header = [0; 4];
    if stream.read(&mut header[..1]).await? == 0 {
        return Err(Error::Closed);
    }
    // Idle subscriptions may be quiet indefinitely; a started frame may not stall.
    tokio::time::timeout(std::time::Duration::from_secs(35), async {
        stream.read_exact(&mut header[1..]).await?;
        let length = u32::from_le_bytes(header);
        if length > MAX_FRAME_BYTES {
            return Err(FrameError::FrameTooLarge(length).into());
        }
        let mut bytes = vec![0; length as usize];
        stream.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(|error| FrameError::Malformed(error).into())
    })
    .await
    .map_err(|_| Error::Timeout)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn async_reader_accepts_the_existing_shared_wire_codec() {
        let expected = ServerMessage::Pong { id: 41 };
        let mut bytes = Vec::new();
        ws_protocol::write_message(&mut bytes, &expected).unwrap();
        let (mut sender, mut receiver) = tokio::io::duplex(1024);
        sender.write_all(&bytes).await.unwrap();
        assert_eq!(read_frame(&mut receiver).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn async_reader_rejects_oversized_headers_before_reading_a_body() {
        let (mut sender, mut receiver) = tokio::io::duplex(16);
        sender
            .write_all(&(MAX_FRAME_BYTES + 1).to_le_bytes())
            .await
            .unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(1), read_frame(&mut receiver))
                .await
                .unwrap();
        assert!(matches!(
            result,
            Err(Error::Frame(FrameError::FrameTooLarge(_)))
        ));
    }

    #[tokio::test]
    async fn async_reader_distinguishes_closed_and_truncated_connections() {
        let (sender, mut receiver) = tokio::io::duplex(16);
        drop(sender);
        assert!(matches!(
            read_frame(&mut receiver).await,
            Err(Error::Closed)
        ));
        let (mut sender, mut receiver) = tokio::io::duplex(16);
        sender.write_all(&[2, 0]).await.unwrap();
        drop(sender);
        assert!(matches!(read_frame(&mut receiver).await,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof));
    }
}
