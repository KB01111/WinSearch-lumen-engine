//! Per-connection protocol loop: handshake, then request dispatch until the
//! client disconnects.

use std::io::Read;
use std::sync::{Arc, Mutex};

use tracing::{info, warn};
use ws_platform::pipe::PipeStream;
use ws_protocol::{read_message, ClientMessage, ErrorCode, ServerMessage, PROTOCOL_VERSION};

use crate::engine::SharedEngine;

pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

fn features() -> Vec<String> {
    vec!["search.exact".to_string(), "search.semantic".to_string(), "code".to_string()]
}

pub fn handle_connection(stream: PipeStream, engine: SharedEngine) {
    let stream = Arc::new(Mutex::new(stream));
    let client = engine.register_client(stream.clone());

    let mut reader = ReaderGuard { stream: stream.clone() };
    let first = match read_message::<_, ClientMessage>(&mut reader) {
        Ok(Some(msg)) => msg,
        Ok(None) => {
            info!("client disconnected before handshake");
            engine.unregister_client(&client);
            return;
        }
        Err(e) => {
            warn!(error = %e, "handshake read failed");
            engine.unregister_client(&client);
            return;
        }
    };

    let ClientMessage::Hello {
        protocol_version,
        client: client_name,
        client_version,
    } = first
    else {
        let _ = client.send(&ServerMessage::Error {
            id: None,
            code: ErrorCode::InvalidRequest,
            message: "first message must be hello".into(),
        });
        engine.unregister_client(&client);
        return;
    };

    if protocol_version != PROTOCOL_VERSION {
        let _ = client.send(&ServerMessage::Error {
            id: None,
            code: ErrorCode::ProtocolVersionMismatch,
            message: format!(
                "engine speaks protocol v{PROTOCOL_VERSION}, client speaks v{protocol_version}"
            ),
        });
        engine.unregister_client(&client);
        return;
    }

    info!(client = %client_name, version = %client_version, "client connected");
    let _ = client.send(&ServerMessage::HelloAck {
        protocol_version: PROTOCOL_VERSION,
        engine_version: ENGINE_VERSION.to_string(),
        features: features(),
    });
    let _ = client.send(&ServerMessage::Status {
        id: None,
        status: engine.status(),
    });

    let mut active_search: Option<ws_protocol::RequestId> = None;

    loop {
        let msg = match read_message::<_, ClientMessage>(&mut reader) {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                info!(client = %client_name, "client disconnected");
                break;
            }
            Err(e) => {
                warn!(client = %client_name, error = %e, "connection read failed");
                break;
            }
        };

        match msg {
            ClientMessage::GetIndexingConfig { id } => engine.get_indexing_config(&client, id),
            ClientMessage::SetIndexingConfig { id, config } => engine.set_indexing_config(&client, id, config),
            ClientMessage::Hello { .. } => {
                let _ = client.send(&ServerMessage::Error {
                    id: None,
                    code: ErrorCode::InvalidRequest,
                    message: "hello already completed".into(),
                });
            }
            ClientMessage::Ping { id } => {
                let _ = client.send(&ServerMessage::Pong { id });
            }
            ClientMessage::Search { id, query } => {
                if let Some(prev) = active_search.replace(id) {
                    if prev != id {
                        engine.cancel_for_client(client.id(), prev);
                    }
                }
                engine.search(&client, id, query);
            }
            ClientMessage::Cancel { id } => {
                engine.cancel_for_client(client.id(), id);
            }
            ClientMessage::GetStatus { id } => {
                let _ = client.send(&ServerMessage::Status {
                    id: Some(id),
                    status: engine.status(),
                });
            }
            ClientMessage::ModelDownloadStart { id } => {
                engine.model_download_start(&client, id);
            }
            ClientMessage::ModelDownloadCancel { id } => {
                engine.model_download_cancel(&client, id);
            }
            ClientMessage::ModelImport { id, path } => {
                engine.model_import(&client, id, path);
            }
            ClientMessage::ModelDelete { id } => {
                engine.model_delete(&client, id);
            }
            ClientMessage::GetModelStatus { id } => {
                engine.model_status(&client, id);
            }
            ClientMessage::GetConfig { id } => {
                engine.get_config(&client, id);
            }
            ClientMessage::SetConfig { id, config } => {
                engine.set_config(&client, id, config);
            }
        }
    }

    if let Some(prev) = active_search {
        engine.cancel_for_client(client.id(), prev);
    }
    engine.unregister_client(&client);
}

/// Adapter giving the framing codec read access to the shared pipe stream
/// without holding the stream mutex (or blocking `ReadFile` on the synchronous
/// pipe handle) while waiting for the peer to send the next frame.
struct ReaderGuard {
    stream: Arc<Mutex<PipeStream>>,
}

impl Read for ReaderGuard {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            {
                let mut guard = self
                    .stream
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match guard.peek_available()? {
                    None => return Ok(0),
                    Some(n) if n > 0 => return guard.read(buf),
                    Some(_) => {}
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use ws_protocol::{write_message, SearchQuery, SearchResult};

    use crate::engine::EngineService;
    use crate::indexer::Indexer;

    static SERVER_PIPE_SEQ: AtomicU64 = AtomicU64::new(0);

    fn unique_pipe() -> String {
        let n = SERVER_PIPE_SEQ.fetch_add(1, Ordering::SeqCst);
        format!(r"\\.\pipe\winsearch-server-test-{}-{n}", std::process::id())
    }

    fn connect_retry(pipe: &str) -> PipeStream {
        let start = std::time::Instant::now();
        loop {
            match PipeStream::connect(pipe) {
                Ok(s) => return s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(e) => panic!("pipe {pipe} never answered: {e}"),
            }
        }
    }

    fn read_results(stream: &mut PipeStream) -> (u64, Vec<SearchResult>, bool) {
        loop {
            let msg: Option<ServerMessage> =
                read_message(stream).expect("read server frame");
            match msg {
                Some(ServerMessage::Results { id, batch, done }) => return (id, batch, done),
                Some(ServerMessage::Status { .. }) => continue,
                Some(other) => panic!("unexpected server frame: {other:?}"),
                None => panic!("pipe closed before Results frame"),
            }
        }
    }

    #[test]
    fn indexing_config_roundtrips_over_pipe_and_survives_reconnect() {
        let indexer = Indexer::open(ws_index::IndexStore::open_in_memory().unwrap(), &[]).unwrap();
        let engine = EngineService::new_with_model_dir(indexer, std::env::temp_dir().join("winsearch-policy-unused-models"));
        let pipe = unique_pipe(); let server_pipe=pipe.clone(); let server_engine=engine.clone();
        std::thread::spawn(move || { let _ = ws_platform::pipe::serve(&server_pipe,move |stream| handle_connection(stream,server_engine.clone())); });
        fn hello(pipe: &str) -> PipeStream {
            let mut c=connect_retry(pipe);
            write_message(&mut c,&ClientMessage::Hello { protocol_version:1,client:"policy-test".into(),client_version:"1".into() }).unwrap();
            assert!(matches!(read_message::<_,ServerMessage>(&mut c).unwrap(),Some(ServerMessage::HelloAck {..})));
            c
        }
        fn reply(c: &mut PipeStream, id: u64) -> ServerMessage {
            loop { let m=read_message::<_,ServerMessage>(c).unwrap().unwrap();
                if matches!(&m,ServerMessage::IndexingConfig {id:Some(i),..}|ServerMessage::Error {id:Some(i),..}|ServerMessage::Config {id:Some(i),..} if *i==id) { return m; }
            }
        }
        let mut c=hello(&pipe);
        write_message(&mut c,&ClientMessage::GetIndexingConfig {id:1}).unwrap();
        assert!(matches!(reply(&mut c,1),ServerMessage::IndexingConfig {config,..} if config.roots.is_empty()));
        let mut config=ws_protocol::IndexingConfig::default();
        config.roots=vec![r"Z:\offline-policy-fixture".into()]; config.code_roots=vec![r"Z:\code-fixture".into()];
        config.excluded_paths=vec![r"Z:\offline-policy-fixture\skip".into()]; config.excluded_extensions=vec![".PDF".into()];
        config.mode=ws_protocol::IndexingMode::Scheduled; config.hours_start=1320; config.hours_end=360; config.paused=true;
        write_message(&mut c,&ClientMessage::SetIndexingConfig {id:2,config:config.clone()}).unwrap();
        config.excluded_extensions=vec!["pdf".into()];
        assert_eq!(reply(&mut c,2),ServerMessage::IndexingConfig {id:Some(2),config:config.clone()});
        let mut invalid=config.clone(); invalid.roots=vec!["relative".into()];
        write_message(&mut c,&ClientMessage::SetIndexingConfig {id:3,config:invalid}).unwrap();
        assert!(matches!(reply(&mut c,3),ServerMessage::Error {code:ErrorCode::InvalidRequest,..}));
        write_message(&mut c,&ClientMessage::SetConfig {id:4,config:ws_protocol::EngineConfig {semantic_enabled:false,preferred_backend:"cpu".into()}}).unwrap();
        assert!(matches!(reply(&mut c,4),ServerMessage::Config {..}));
        drop(c);
        let mut c=hello(&pipe);
        write_message(&mut c,&ClientMessage::GetIndexingConfig {id:5}).unwrap();
        assert_eq!(reply(&mut c,5),ServerMessage::IndexingConfig {id:Some(5),config});
    }

    #[test]
    fn in_band_cancel_on_pipe_aborts_slow_semantic_stage() {
        let root = std::env::temp_dir().join(format!(
            "winsearch-server-cancel-{}-{}",
            std::process::id(),
            SERVER_PIPE_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let files = root.join("files");
        std::fs::create_dir_all(&files).unwrap();
        std::fs::write(files.join("meeting-notes.txt"), b"x").unwrap();
        std::fs::write(files.join("banquet-hall.txt"), b"x").unwrap();

        let store = ws_index::IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[files.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
            .embed_once_with(|texts| {
                Ok(texts
                    .iter()
                    .map(|t| {
                        if t.contains("banquet") {
                            vec![1.0, 0.0, 0.0]
                        } else {
                            vec![0.0, 1.0, 0.0]
                        }
                    })
                    .collect())
            })
            .unwrap();

        let engine = EngineService::new_with_model_dir(indexer, root.join("models"));
        engine.set_model_ready_override(Some(true));

        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        engine.set_query_embed_hook(Arc::new(move |_| {
            let _ = entered_tx.send(());
            // Hold the semantic stage open long enough for the in-band `Cancel`
            // frame on the same pipe to be read and processed by `handle_connection`.
            std::thread::sleep(Duration::from_millis(250));
            Ok(vec![1.0, 0.0, 0.0])
        }));

        let pipe = unique_pipe();
        let server_pipe = pipe.clone();
        let server_engine = Arc::clone(&engine);
        std::thread::Builder::new()
            .name("winsearch-server-test".into())
            .spawn(move || {
                let _ = ws_platform::pipe::serve(&server_pipe, move |stream| {
                    handle_connection(stream, Arc::clone(&server_engine));
                });
            })
            .unwrap();

        let mut client = connect_retry(&pipe);
        write_message(
            &mut client,
            &ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                client: "test".into(),
                client_version: "0.1.0".into(),
            },
        )
        .unwrap();
        let ack: ServerMessage = read_message(&mut client).unwrap().unwrap();
        assert!(matches!(ack, ServerMessage::HelloAck { .. }));

        // Send `Search` followed immediately by `Cancel` on the same pipe.
        write_message(
            &mut client,
            &ClientMessage::Search {
                id: 55,
                query: SearchQuery {
                    text: "meeting".into(),
                    limit: 20,
                    include_semantic: true,
                    ..Default::default()
                },
            },
        )
        .unwrap();
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("semantic stage must begin");
        write_message(&mut client, &ClientMessage::Cancel { id: 55 }).unwrap();

        let (id1, exact_batch, done1) = read_results(&mut client);
        assert_eq!(id1, 55);
        assert!(!done1, "exact batch leaves stream open for semantic stage");
        assert!(exact_batch.iter().any(|r| r.title == "meeting-notes.txt"));

        let (id2, terminal_batch, done2) = read_results(&mut client);
        assert_eq!(id2, 55);
        assert!(done2, "cancelled search must emit terminal done:true frame");
        assert!(
            terminal_batch.is_empty(),
            "in-band Cancel must suppress slow semantic hits, got {terminal_batch:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
