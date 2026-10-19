use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout_at, Instant};
use ws_protocol::{ClientMessage, ServerMessage, PROTOCOL_VERSION};

use crate::{read_frame_counted, EngineInfo, Error};

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub(crate) struct Session {
    io: Option<Box<dyn Io>>,
    pub(crate) info: EngineInfo,
    pub(crate) received_bytes: usize,
}

// Cancelling read_exact/write_all can leave partial frames. Close that stream.
struct PendingIo<'a> {
    slot: &'a mut Option<Box<dyn Io>>,
    complete: bool,
}

impl Drop for PendingIo<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.slot.take();
        }
    }
}

impl Session {
    fn new(io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) -> Self {
        Self {
            io: Some(Box::new(io)),
            info: EngineInfo::default(),
            received_bytes: 0,
        }
    }

    pub(crate) async fn handshake(
        io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        deadline: Instant,
    ) -> Result<Self, Error> {
        let mut session = Self::new(io);
        session
            .send(
                &ClientMessage::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    client: "ws-client".into(),
                    client_version: env!("CARGO_PKG_VERSION").into(),
                },
                deadline,
            )
            .await?;
        loop {
            match session.recv(Some(deadline)).await? {
                ServerMessage::HelloAck {
                    protocol_version,
                    engine_version,
                    features,
                } if protocol_version == PROTOCOL_VERSION
                    && !engine_version.is_empty()
                    && engine_version.len() <= 128
                    && features.len() <= 64
                    && features
                        .iter()
                        .all(|value| !value.is_empty() && value.len() <= 128) =>
                {
                    session.info = EngineInfo {
                        engine_version,
                        features,
                    };
                    return Ok(session);
                }
                // The daemon registers connections before Hello and can broadcast first.
                ServerMessage::Status { id: None, .. }
                | ServerMessage::ModelStatus { id: None, .. }
                | ServerMessage::Config { id: None, .. }
                | ServerMessage::IndexingConfig { id: None, .. } => {}
                ServerMessage::Error {
                    id: None,
                    code,
                    message,
                } => return Err(Error::Remote { code, message }),
                _ => return Err(Error::Protocol("invalid handshake acknowledgement")),
            }
        }
    }

    pub(crate) async fn recv(&mut self, deadline: Option<Instant>) -> Result<ServerMessage, Error> {
        let mut pending = PendingIo {
            slot: &mut self.io,
            complete: false,
        };
        if deadline.is_some_and(|value| Instant::now() >= value) {
            return Err(Error::Timeout);
        }
        let io = pending.slot.as_mut().ok_or(Error::Closed)?;
        let result = match deadline {
            Some(deadline) => timeout_at(deadline, read_frame_counted(io))
                .await
                .map_err(|_| Error::Timeout)?,
            None => read_frame_counted(io).await,
        };
        pending.complete = result.is_ok();
        let (message, bytes) = result?;
        self.received_bytes = self.received_bytes.saturating_add(bytes);
        Ok(message)
    }

    pub(crate) async fn send(
        &mut self,
        message: &ClientMessage,
        deadline: Instant,
    ) -> Result<(), Error> {
        let mut bytes = Vec::new();
        ws_protocol::write_message(&mut bytes, message)?;
        let mut pending = PendingIo {
            slot: &mut self.io,
            complete: false,
        };
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        let io = pending.slot.as_mut().ok_or(Error::Closed)?;
        timeout_at(deadline, io.write_all(&bytes))
            .await
            .map_err(|_| Error::Timeout)??;
        pending.complete = true;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use ws_protocol::{ServerMessage, PROTOCOL_VERSION};

    pub(crate) async fn read_client(peer: &mut (impl AsyncRead + Unpin)) -> ClientMessage {
        let mut header = [0; 4];
        peer.read_exact(&mut header).await.unwrap();
        let length = u32::from_le_bytes(header);
        assert!(length <= ws_protocol::MAX_FRAME_BYTES);
        let mut bytes = vec![0; length as usize];
        peer.read_exact(&mut bytes).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    pub(crate) async fn send_server(peer: &mut (impl AsyncWrite + Unpin), message: &ServerMessage) {
        let mut bytes = Vec::new();
        ws_protocol::write_message(&mut bytes, message).unwrap();
        peer.write_all(&bytes).await.unwrap();
    }

    pub(crate) fn ack() -> ServerMessage {
        ServerMessage::HelloAck {
            protocol_version: PROTOCOL_VERSION,
            engine_version: "fixture".into(),
            features: vec!["search.exact".into(), "entry.resolve".into()],
        }
    }

    pub(crate) fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn handshake_sends_current_version_then_accepts_ack() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            assert!(matches!(read_client(&mut peer).await,
                ClientMessage::Hello { protocol_version: PROTOCOL_VERSION, client, .. }
                if client == "ws-client"));
            send_server(&mut peer, &ack()).await;
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::Ping { id: 9 }
            ));
            send_server(&mut peer, &ServerMessage::Pong { id: 9 }).await;
        });
        let mut session = Session::handshake(client, deadline()).await.unwrap();
        session
            .send(&ClientMessage::Ping { id: 9 }, deadline())
            .await
            .unwrap();
        assert_eq!(
            session.recv(Some(deadline())).await.unwrap(),
            ServerMessage::Pong { id: 9 }
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_rejects_foreign_version_and_wrong_envelope() {
        for response in [
            ServerMessage::HelloAck {
                protocol_version: 999,
                engine_version: "future".into(),
                features: vec![],
            },
            ServerMessage::Pong { id: 1 },
            ServerMessage::HelloAck {
                protocol_version: 1,
                engine_version: "huge".repeat(100),
                features: vec![],
            },
            ServerMessage::HelloAck {
                protocol_version: 1,
                engine_version: "x".into(),
                features: vec!["x".repeat(129)],
            },
        ] {
            let (client, mut peer) = tokio::io::duplex(4096);
            let task = tokio::spawn(async move {
                let _ = read_client(&mut peer).await;
                send_server(&mut peer, &response).await;
                assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            });
            assert!(matches!(
                Session::handshake(client, deadline()).await,
                Err(Error::Protocol(_))
            ));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn handshake_preserves_connection_scoped_remote_error() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let _ = read_client(&mut peer).await;
            send_server(
                &mut peer,
                &ServerMessage::Error {
                    id: None,
                    code: ws_protocol::ErrorCode::ProtocolVersionMismatch,
                    message: "upgrade".into(),
                },
            )
            .await;
        });
        assert!(matches!(
            Session::handshake(client, deadline()).await,
            Err(Error::Remote {
                code: ws_protocol::ErrorCode::ProtocolVersionMismatch,
                ..
            })
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_tolerates_status_broadcast_before_version_ack() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let _ = read_client(&mut peer).await;
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: None,
                    status: ws_protocol::EngineStatus::default(),
                },
            )
            .await;
            send_server(&mut peer, &ack()).await;
        });
        assert!(Session::handshake(client, deadline()).await.is_ok());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dropped_partial_receive_closes_io_instead_of_reusing_partial_frame() {
        let (client, mut peer) = tokio::io::duplex(16);
        let mut session = Session::new(client);
        peer.write_all(&[20, 0]).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), session.recv(Some(deadline())))
                .await
                .is_err()
        );
        assert!(session.io.is_none());
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert!(matches!(
            session.recv(Some(deadline())).await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn receive_timeout_and_malformed_frame_close_owned_connection() {
        let (client, mut peer) = tokio::io::duplex(16);
        let mut session = Session::new(client);
        assert!(matches!(
            session
                .recv(Some(Instant::now() + Duration::from_millis(10)))
                .await,
            Err(Error::Timeout)
        ));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);

        let (client, mut peer) = tokio::io::duplex(16);
        let mut session = Session::new(client);
        peer.write_all(&[1, 0, 0, 0, b'!']).await.unwrap();
        assert!(matches!(
            session.recv(Some(deadline())).await,
            Err(Error::Frame(_))
        ));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn dropped_partial_send_closes_owned_connection() {
        let (client, mut peer) = tokio::io::duplex(1);
        let mut session = Session::new(client);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            session.send(&ClientMessage::Ping { id: 7 }, deadline())
        )
        .await
        .is_err());
        let mut partial = Vec::new();
        peer.read_to_end(&mut partial).await.unwrap();
        assert!(!partial.is_empty());
        assert!(session.io.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn subscription_allows_quiet_idle_but_bounds_started_frames() {
        let (client, mut peer) = tokio::io::duplex(128);
        let mut session = Session::new(client);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(40)).await;
            send_server(&mut peer, &ServerMessage::Pong { id: 8 }).await;
        });
        assert_eq!(
            session.recv(None).await.unwrap(),
            ServerMessage::Pong { id: 8 }
        );
        task.await.unwrap();

        let (client, mut peer) = tokio::io::duplex(16);
        let mut session = Session::new(client);
        peer.write_all(&[20, 0]).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(36), session.recv(None)).await;
        assert!(matches!(result, Ok(Err(Error::Timeout))));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }
}
