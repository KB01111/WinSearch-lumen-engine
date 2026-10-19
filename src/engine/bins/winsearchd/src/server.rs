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
    vec!["search.exact".to_string(), "search.semantic".to_string(), "search.filters".to_string(), "search.outcomes".to_string(), "search.related".to_string(), "code".to_string(), "entry.resolve".to_string(), "entry.resolve.code".to_string(), "search.entry_references".to_string(), "entry.preview.text".to_string(), "entry.preview.code".to_string()]
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
            ClientMessage::ResolveEntry { id, reference } => engine.resolve_entry(&client, id, reference),
            ClientMessage::PreviewEntry { id, request } => engine.preview_entry(&client, id, request),
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
            ClientMessage::Related { id, query } => {
                if let Some(prev)=active_search.replace(id) {
                    if prev!=id {engine.cancel_for_client(client.id(),prev);}
                }
                engine.related(&client,id,query);
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
                Some(ServerMessage::Results { id, batch, done, .. }) => return (id, batch, done),
                Some(ServerMessage::Status { .. }) => continue,
                Some(other) => panic!("unexpected server frame: {other:?}"),
                None => panic!("pipe closed before Results frame"),
            }
        }
    }

    #[test]
    fn preview_pipe_bounds_work_accepts_in_band_cancel_and_revalidates_roots_and_sources() {
        use ws_protocol::{EntryReference,PreviewRequest,PreviewState,ErrorCode};
        let root=std::env::temp_dir().join(format!("winsearch-preview-{}-{}",std::process::id(),SERVER_PIPE_SEQ.fetch_add(1,Ordering::SeqCst)));
        std::fs::create_dir(&root).unwrap();let source=root.join("preview.txt");
        std::fs::write(&source,"Header\nactual cached preview text").unwrap();
        let indexer=Indexer::open(ws_index::IndexStore::open_in_memory().unwrap(),&[root.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();while indexer.scan_available(64)>0 {}
        indexer.extract_once_with(&crate::extract::ExtractorRegistry::default(),|_|Err("no model".into())).unwrap();
        let entry=indexer.store_lock().find_by_path(&source.to_string_lossy()).unwrap().unwrap().id;
        let engine=EngineService::new_with_model_dir(indexer.clone(),root.join("unused-models"));
        let pipe=unique_pipe();let server_pipe=pipe.clone();
        std::thread::spawn(move || {let _=ws_platform::pipe::serve(&server_pipe,move |stream|handle_connection(stream,engine.clone()));});
        let mut client=connect_retry(&pipe);
        write_message(&mut client,&ClientMessage::Hello {protocol_version:1,client:"preview-test".into(),client_version:"1".into()}).unwrap();
        let Some(ServerMessage::HelloAck {features,..})=read_message::<_,ServerMessage>(&mut client).unwrap() else {panic!("preview handshake");};
        assert!(features.iter().any(|feature|feature=="entry.preview.text"));
        assert!(features.iter().any(|feature|feature=="entry.preview.code"));
        assert!(features.iter().any(|feature|feature=="entry.resolve.code"));
        assert!(features.iter().any(|feature|feature=="search.entry_references"));
        assert!(matches!(read_message::<_,ServerMessage>(&mut client).unwrap(),Some(ServerMessage::Status {..})));
        fn reply(client:&mut PipeStream)->ServerMessage {
            loop {let message=read_message::<_,ServerMessage>(client).unwrap().unwrap();if !matches!(message,ServerMessage::Status {..}) {return message;}}
        }
        fn request(client:&mut PipeStream,id:u64,entry:i64,proof:Option<String>) {
            write_message(client,&ClientMessage::PreviewEntry {id,request:PreviewRequest {reference:EntryReference {result_id:format!("e{entry}"),source_revision:proof},location:None}}).unwrap();
        }
        let blocked=indexer.store_lock();
        request(&mut client,10,entry,None);request(&mut client,11,entry,None);
        write_message(&mut client,&ClientMessage::Ping {id:12}).unwrap();
        assert!(matches!(reply(&mut client),ServerMessage::Pong {id:12}),"preview work cannot block control traffic");
        request(&mut client,13,entry,None);
        assert!(matches!(reply(&mut client),ServerMessage::Error {id:Some(13),code:ErrorCode::NotReady,..}),"a third request cannot create unbounded blocked work");
        for id in [10,11] {write_message(&mut client,&ClientMessage::Cancel {id}).unwrap();}
        write_message(&mut client,&ClientMessage::Ping {id:14}).unwrap();
        assert!(matches!(reply(&mut client),ServerMessage::Pong {id:14}));
        drop(blocked);
        let mut cancelled=std::collections::HashSet::new();
        for _ in 0..2 {
            let ServerMessage::Error {id:Some(id),code:ErrorCode::Cancelled,..}=reply(&mut client) else {panic!("cancelled preview delivered content");};
            assert!([10,11].contains(&id));cancelled.insert(id);
        }
        assert_eq!(cancelled.len(),2);
        let mut id=20;let deadline=std::time::Instant::now()+Duration::from_secs(5);
        let preview=loop {
            request(&mut client,id,entry,None);
            match reply(&mut client) {
                ServerMessage::EntryPreview {id:actual,preview}=>{assert_eq!(actual,id);break preview;},
                ServerMessage::Error {code:ErrorCode::NotReady,..} if std::time::Instant::now()<deadline=>{std::thread::sleep(Duration::from_millis(2));id+=1;},
                response=>panic!("freed preview capacity did not recover: {response:?}"),
            }
        };
        assert_eq!(preview.state,PreviewState::Ready);
        assert_eq!(preview.parts[0].text,"Header\nactual cached preview text");
        let config=indexer.get_indexing_config().unwrap();let mut revoked=config.clone();revoked.roots.clear();
        indexer.set_indexing_config(&revoked).unwrap();request(&mut client,40,entry,None);
        assert!(matches!(reply(&mut client),ServerMessage::Error {id:Some(40),code:ErrorCode::NotReady,..}));
        indexer.set_indexing_config(&config).unwrap();request(&mut client,41,entry,Some(preview.entry.source_revision));
        assert!(matches!(reply(&mut client),ServerMessage::Error {id:Some(41),code:ErrorCode::NotReady,..}));
        std::fs::write(&source,"replacement bytes").unwrap();
        // The restored native watcher can advance metadata while a fresh proof is being resolved.
        let deadline=std::time::Instant::now()+Duration::from_secs(5);let mut id=42;
        let preview=loop {
            request(&mut client,id,entry,None);
            match reply(&mut client) {
                ServerMessage::EntryPreview {preview,..}=>break preview,
                ServerMessage::Error {code:ErrorCode::NotReady,..} if std::time::Instant::now()<deadline=>{std::thread::sleep(Duration::from_millis(2));id+=1;},
                response=>panic!("replacement metadata unavailable: {response:?}"),
            }
        };
        assert_eq!(preview.state,PreviewState::NotIndexed);assert!(preview.parts.is_empty());
        indexer.set_indexing_config(&revoked).unwrap();drop(client);
        std::fs::remove_file(&source).unwrap();std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn resolves_native_entry_identity_and_rejects_stale_source_or_grants() {
        let root = std::env::temp_dir().join(format!("winsearch-resolve-{}-{}",
            std::process::id(), SERVER_PIPE_SEQ.fetch_add(1, Ordering::SeqCst)));
        std::fs::create_dir(&root).unwrap();
        let source = root.join("report.txt");
        std::fs::write(&source, "original text").unwrap();
        let indexer = Indexer::open(ws_index::IndexStore::open_in_memory().unwrap(), &[root.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let entry_id = indexer.store_lock().find_by_path(&source.to_string_lossy()).unwrap().unwrap().id;
        let engine = EngineService::new_with_model_dir(indexer.clone(), root.join("unused-models"));
        let pipe = unique_pipe(); let server_pipe = pipe.clone();
        std::thread::spawn(move || { let _ = ws_platform::pipe::serve(&server_pipe,
            move |stream| handle_connection(stream, engine.clone())); });
        let mut client = connect_retry(&pipe);
        write_message(&mut client, &ClientMessage::Hello { protocol_version:1, client:"resolve-test".into(), client_version:"1".into() }).unwrap();
        assert!(matches!(read_message::<_,ServerMessage>(&mut client).unwrap(),Some(ServerMessage::HelloAck {..})));
        fn resolve(client: &mut PipeStream, id: u64, result_id: &str, proof: Option<String>) -> ServerMessage {
            write_message(client, &ClientMessage::ResolveEntry { id,
                reference: ws_protocol::EntryReference { result_id: result_id.into(), source_revision: proof } }).unwrap();
            loop {
                match read_message::<_,ServerMessage>(client).unwrap().unwrap() {
                    ServerMessage::Status { .. } => continue,
                    response => return response,
                }
            }
        }
        let result_id = format!("e{entry_id}");
        let ServerMessage::ResolvedEntry { id:1, entry } = resolve(&mut client, 1, &result_id, None)
            else { panic!("a native indexed identity must resolve to current admitted metadata"); };
        assert_eq!(entry.path, source.to_string_lossy());
        assert_eq!(entry.root_path, root.to_string_lossy());
        assert_eq!(entry.size_bytes, 13);
        assert_eq!(entry.index_revision, None, "metadata-only crawl has no content revision proof");
        std::fs::OpenOptions::new().write(true).open(&source).unwrap().set_times(
            std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH - Duration::from_millis(500))).unwrap();
        let ServerMessage::ResolvedEntry {entry,..} = resolve(&mut client,10,&result_id,None)
            else {panic!("resolve pre-epoch file metadata");};
        assert_eq!(entry.modified_unix_seconds,Some(-1),"Unix seconds floor fractional pre-epoch times");
        indexer.extract_once_with(&crate::extract::ExtractorRegistry::default(),
            |texts| Ok(vec![vec![1.0, 0.0]; texts.len()])).unwrap();
        let ServerMessage::ResolvedEntry {entry,..} = resolve(&mut client,11,&result_id,None)
            else {panic!("resolve after exact content extraction");};
        assert!(entry.index_revision.is_some_and(|revision| revision > 0));
        indexer.invalidate_vector_cache();
        assert!(matches!(resolve(&mut client,2,&result_id,Some(entry.source_revision.clone())),
            ServerMessage::ResolvedEntry {id:2,..}), "unrelated vector cache changes must not expire media references");
        indexer.set_indexing_config(&indexer.get_indexing_config().unwrap()).unwrap();
        assert!(matches!(resolve(&mut client,20,&result_id,Some(entry.source_revision.clone())),
            ServerMessage::ResolvedEntry {id:20,..}), "saving an unchanged policy must not expire a current reference");
        std::fs::write(&source, "replacement with a different size").unwrap();
        assert!(matches!(resolve(&mut client,3,&result_id,Some(entry.source_revision)),ServerMessage::Error { id:Some(3),.. }));
        let ServerMessage::ResolvedEntry {entry: fresh,..} = resolve(&mut client,4,&result_id,None) else {panic!("refresh after source replacement");};
        assert_eq!(fresh.size_bytes, 33);
        assert_eq!(fresh.index_revision,None,"old extracted content cannot prove a replacement source");
        indexer.store_lock().reset_content_vectors().unwrap();
        assert!(matches!(resolve(&mut client,5,&result_id,Some(fresh.source_revision)),ServerMessage::Error {id:Some(5),..}));
        let ServerMessage::ResolvedEntry {entry: fresh,..} = resolve(&mut client,6,&result_id,None) else {panic!("refresh after model change");};
        let config = indexer.get_indexing_config().unwrap();
        let mut revoked = config.clone(); revoked.roots.clear();
        indexer.set_indexing_config(&revoked).unwrap();
        assert!(matches!(resolve(&mut client,7,&result_id,None),ServerMessage::Error {id:Some(7),..}));
        indexer.set_indexing_config(&config).unwrap();
        assert!(matches!(resolve(&mut client,8,&result_id,Some(fresh.source_revision)),ServerMessage::Error {id:Some(8),..}),
            "restoring a grant cannot revive a pre-revocation reference");
        for bad in ["C:/outside/secret.txt","e-1","e+1","e01","e0","e9223372036854775808"] {
            assert!(matches!(resolve(&mut client,9,bad,None),ServerMessage::Error {id:Some(9),..}),"{bad}");
        }
        indexer.set_indexing_config(&revoked).unwrap();
        drop(client);
        std::fs::remove_file(&source).unwrap();
        std::fs::remove_dir(&root).unwrap();
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
