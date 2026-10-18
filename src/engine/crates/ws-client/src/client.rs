use crate::{transport::Session, EngineInfo, Error};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{timeout_at, Instant};
use ws_protocol::{
    ClientMessage, EngineConfig, EngineStatus, EntryReference, IndexingConfig, ModelStatus,
    ResolvedEntry, SearchQuery, SearchResult, ServerMessage,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(35);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SEARCH_RESULTS: usize = 4096;
const MAX_SEARCH_FRAMES: usize = 64;

struct Inner {
    pipe: String,
    ids: AtomicU64,
    reads: Arc<Semaphore>,
    controls: Arc<Semaphore>,
    subscriptions: Arc<Semaphore>,
    mutation_uncertain: AtomicBool,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

struct Operation {
    session: Session,
    id: u64,
    deadline: Instant,
    mutation: Option<MutationGuard>,
    _permit: OwnedSemaphorePermit,
}

struct MutationGuard {
    client: Arc<Inner>,
    acknowledged: bool,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.client
                .mutation_uncertain
                .store(true, Ordering::Release);
        }
    }
}

impl Operation {
    fn acknowledge(&mut self) {
        if let Some(mutation) = &mut self.mutation {
            mutation.acknowledged = true;
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchBatch {
    pub batch: Vec<SearchResult>,
    pub done: bool,
}

/// Dropping the stream (or a pending next call) closes its owned connection.
pub struct SearchStream {
    operation: Option<Operation>,
    results: usize,
    frames: usize,
}

impl SearchStream {
    pub async fn next(&mut self) -> Result<Option<SearchBatch>, Error> {
        let Some(operation) = self.operation.as_mut() else {
            return Ok(None);
        };
        let result = async {
            loop {
                let message = operation.session.recv(Some(operation.deadline)).await?;
                if is_push(&message) {
                    continue;
                }
                validate_id(&message, operation.id)?;
                let ServerMessage::Results { batch, done, .. } = message else {
                    return Err(Error::Protocol("expected search results"));
                };
                self.frames += 1;
                self.results = self.results.saturating_add(batch.len());
                if self.frames > MAX_SEARCH_FRAMES || self.results > MAX_SEARCH_RESULTS {
                    return Err(Error::Protocol("search stream exceeded its result budget"));
                }
                return Ok(SearchBatch { batch, done });
            }
        }
        .await;
        if result.as_ref().map_or(true, |batch| batch.done) {
            self.operation.take();
        }
        result.map(Some)
    }
    /// Sends best-effort in-band cancellation, then disconnects; no acknowledgement is required.
    pub async fn cancel(mut self) -> Result<(), Error> {
        if let Some(mut operation) = self.operation.take() {
            operation
                .session
                .send(
                    &ClientMessage::Cancel { id: operation.id },
                    operation
                        .deadline
                        .min(Instant::now() + Duration::from_secs(1)),
                )
                .await
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EngineEvent {
    Status(EngineStatus),
    ModelStatus(ModelStatus),
    Config(EngineConfig),
    IndexingConfig(IndexingConfig),
}

pub struct StatusSubscription {
    operation: Option<Operation>,
}

impl StatusSubscription {
    pub async fn next(&mut self) -> Result<Option<EngineEvent>, Error> {
        let Some(operation) = self.operation.as_mut() else {
            return Ok(None);
        };
        let result = match operation.session.recv(None).await {
            Ok(ServerMessage::Status { id: None, status }) => Ok(EngineEvent::Status(status)),
            Ok(ServerMessage::ModelStatus { id: None, status }) => {
                Ok(EngineEvent::ModelStatus(status))
            }
            Ok(ServerMessage::Config { id: None, config }) => Ok(EngineEvent::Config(config)),
            Ok(ServerMessage::IndexingConfig { id: None, config }) => {
                Ok(EngineEvent::IndexingConfig(config))
            }
            Ok(ServerMessage::Error {
                id: None,
                code,
                message,
            }) => Err(Error::Remote { code, message }),
            Ok(_) => Err(Error::Protocol("expected an unsolicited engine event")),
            Err(error) => Err(error),
        };
        if result.is_err() {
            self.operation.take();
        }
        result.map(Some)
    }
}

impl Client {
    /// The native host supplies its per-user/instance engine pipe.
    pub fn new(pipe: impl Into<String>) -> Result<Self, Error> {
        let pipe = pipe.into();
        let suffix = pipe.strip_prefix(r"\\.\pipe\winsearch-S-1-");
        if pipe.len() > 256
            || !suffix.is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix.bytes().all(|value| {
                        value.is_ascii_alphanumeric() || value == b'-' || value == b'_'
                    })
            })
        {
            return Err(Error::InvalidInput(
                "expected a local per-user WinSearch engine pipe",
            ));
        }
        Ok(Self(Arc::new(Inner {
            pipe,
            ids: AtomicU64::new(1),
            reads: Arc::new(Semaphore::new(4)),
            controls: Arc::new(Semaphore::new(1)),
            subscriptions: Arc::new(Semaphore::new(1)),
            mutation_uncertain: AtomicBool::new(false),
        })))
    }

    async fn open(&self, control: bool, feature: Option<&'static str>) -> Result<Operation, Error> {
        let lane = if control {
            &self.0.controls
        } else {
            &self.0.reads
        };
        self.open_lane(lane, feature, control).await
    }

    async fn open_lane(
        &self,
        lane: &Arc<Semaphore>,
        feature: Option<&'static str>,
        control: bool,
    ) -> Result<Operation, Error> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let permit = timeout_at(deadline, lane.clone().acquire_owned())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)?;
        if control && self.0.mutation_uncertain.load(Ordering::Acquire) {
            return Err(Error::MutationOutcomeUnknown);
        }
        let id = self
            .0
            .ids
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::Protocol("request identifiers exhausted"))?;
        let session = self.connect(deadline).await?;
        if let Some(feature) = feature {
            if !session.info.supports(feature) {
                return Err(Error::Unsupported(feature));
            }
        }
        Ok(Operation {
            session,
            id,
            deadline,
            mutation: None,
            _permit: permit,
        })
    }

    async fn connect(&self, request_deadline: Instant) -> Result<Session, Error> {
        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ClientOptions;
            let deadline = request_deadline.min(Instant::now() + CONNECT_TIMEOUT);
            loop {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                // Keep Tokio's SECURITY_IDENTIFICATION default; never allow impersonation.
                match ClientOptions::new().open(&self.0.pipe) {
                    Ok(io) => return Session::handshake(io, deadline).await,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound
                            || error.raw_os_error() == Some(231) =>
                    {
                        tokio::time::sleep_until(
                            deadline.min(Instant::now() + Duration::from_millis(50)),
                        )
                        .await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        #[cfg(not(windows))]
        {
            let _ = request_deadline;
            Err(Error::Unsupported("Windows named pipes"))
        }
    }

    async fn unary<T>(
        &self,
        control: bool,
        feature: Option<&'static str>,
        request: impl FnOnce(u64) -> ClientMessage,
        response: impl FnOnce(ServerMessage) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut operation = self.open(control, feature).await?;
        if control {
            operation.mutation = Some(MutationGuard {
                client: self.0.clone(),
                acknowledged: false,
            });
        }
        operation
            .session
            .send(&request(operation.id), operation.deadline)
            .await?;
        loop {
            let message = operation.session.recv(Some(operation.deadline)).await?;
            if is_push(&message) {
                continue;
            }
            if matches!(&message, ServerMessage::Error { id: Some(id), .. } if *id == operation.id)
            {
                operation.acknowledge();
            }
            validate_id(&message, operation.id)?;
            let response = response(message);
            if response.is_ok() {
                operation.acknowledge();
            }
            return response;
        }
    }

    pub async fn info(&self) -> Result<EngineInfo, Error> {
        Ok(self.open(false, None).await?.session.info.clone())
    }

    pub async fn status(&self) -> Result<EngineStatus, Error> {
        self.unary(
            false,
            None,
            |id| ClientMessage::GetStatus { id },
            |message| match message {
                ServerMessage::Status { status, .. } => Ok(status),
                _ => Err(Error::Protocol("expected engine status")),
            },
        )
        .await
    }

    pub async fn config(&self) -> Result<EngineConfig, Error> {
        self.unary(
            false,
            None,
            |id| ClientMessage::GetConfig { id },
            config_reply,
        )
        .await
    }
    pub async fn set_config(&self, config: EngineConfig) -> Result<EngineConfig, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::SetConfig { id, config },
            config_reply,
        )
        .await
    }
    pub async fn indexing_config(&self) -> Result<IndexingConfig, Error> {
        self.unary(
            false,
            None,
            |id| ClientMessage::GetIndexingConfig { id },
            indexing_reply,
        )
        .await
    }
    pub async fn set_indexing_config(
        &self,
        config: IndexingConfig,
    ) -> Result<IndexingConfig, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::SetIndexingConfig { id, config },
            indexing_reply,
        )
        .await
    }
    pub async fn model_status(&self) -> Result<ModelStatus, Error> {
        self.unary(
            false,
            None,
            |id| ClientMessage::GetModelStatus { id },
            model_reply,
        )
        .await
    }
    pub async fn start_model_download(&self) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::ModelDownloadStart { id },
            model_reply,
        )
        .await
    }
    pub async fn cancel_model_download(&self) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::ModelDownloadCancel { id },
            model_reply,
        )
        .await
    }
    pub async fn import_model(&self, path: String) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::ModelImport { id, path },
            model_reply,
        )
        .await
    }
    pub async fn delete_model(&self) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            None,
            |id| ClientMessage::ModelDelete { id },
            model_reply,
        )
        .await
    }
    pub async fn resolve_entry(&self, reference: EntryReference) -> Result<ResolvedEntry, Error> {
        let expected = reference.clone();
        self.unary(
            false,
            Some("entry.resolve"),
            |id| ClientMessage::ResolveEntry { id, reference },
            |message| match message {
                ServerMessage::ResolvedEntry { entry, .. } if entry_matches(&entry, &expected) => {
                    Ok(entry)
                }
                _ => Err(Error::Protocol(
                    "entry identity, source proof or root changed",
                )),
            },
        )
        .await
    }

    pub async fn search(&self, query: SearchQuery) -> Result<SearchStream, Error> {
        if query.limit as usize > MAX_SEARCH_RESULTS || query.text.len() > 64 * 1024 {
            return Err(Error::InvalidInput("search exceeds its request budget"));
        }
        let mut operation = self.open(false, Some("search.exact")).await?;
        for feature in [
            (query.include_semantic, "search.semantic"),
            (query.mode == ws_protocol::SearchMode::Code, "code"),
        ] {
            if feature.0 && !operation.session.info.supports(feature.1) {
                return Err(Error::Unsupported(feature.1));
            }
        }
        operation
            .session
            .send(
                &ClientMessage::Search {
                    id: operation.id,
                    query,
                },
                operation.deadline,
            )
            .await?;
        Ok(SearchStream {
            operation: Some(operation),
            results: 0,
            frames: 0,
        })
    }

    pub async fn subscribe(&self) -> Result<StatusSubscription, Error> {
        Ok(StatusSubscription {
            operation: Some(self.open_lane(&self.0.subscriptions, None, false).await?),
        })
    }
}

fn config_reply(message: ServerMessage) -> Result<EngineConfig, Error> {
    match message {
        ServerMessage::Config { config, .. } => Ok(config),
        _ => Err(Error::Protocol("expected engine configuration")),
    }
}
fn indexing_reply(message: ServerMessage) -> Result<IndexingConfig, Error> {
    match message {
        ServerMessage::IndexingConfig { config, .. } => Ok(config),
        _ => Err(Error::Protocol("expected indexing configuration")),
    }
}
fn model_reply(message: ServerMessage) -> Result<ModelStatus, Error> {
    match message {
        ServerMessage::ModelStatus { status, .. } => Ok(status),
        _ => Err(Error::Protocol("expected model status")),
    }
}

fn entry_matches(entry: &ResolvedEntry, expected: &EntryReference) -> bool {
    if entry.result_id != expected.result_id
        || entry.source_revision.trim().is_empty()
        || entry.name.trim().is_empty()
        || entry.index_revision.is_some_and(|value| value <= 0)
        || expected
            .source_revision
            .as_ref()
            .is_some_and(|value| value != &entry.source_revision)
    {
        return false;
    }
    // Lexical response check only. The native host must still revalidate its grants
    // and current native file identity before opening content or performing actions.
    let (Some(root), Some(path)) = (local_parts(&entry.root_path), local_parts(&entry.path)) else {
        return false;
    };
    path.len() >= root.len()
        && root
            .iter()
            .zip(path.iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

fn local_parts(value: &str) -> Option<Vec<String>> {
    let value = value.replace('/', r"\");
    let value = value.strip_prefix(r"\\?\").unwrap_or(&value);
    let bytes = value.as_bytes();
    if bytes.len() < 3
        || !bytes[0].is_ascii_alphabetic()
        || bytes[1] != b':'
        || bytes[2] != b'\\'
        || value[2..].contains([':', '\0'])
    {
        return None;
    }
    let mut parts = vec![value[..2].to_string()];
    for part in value[3..].split('\\') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.len() == 1 {
                    return None;
                }
                parts.pop();
            }
            _ => parts.push(part.to_string()),
        }
    }
    Some(parts)
}

fn is_push(message: &ServerMessage) -> bool {
    matches!(
        message,
        ServerMessage::Status { id: None, .. }
            | ServerMessage::ModelStatus { id: None, .. }
            | ServerMessage::Config { id: None, .. }
            | ServerMessage::IndexingConfig { id: None, .. }
    )
}

fn validate_id(message: &ServerMessage, expected: u64) -> Result<(), Error> {
    let id = match message {
        ServerMessage::Error {
            id: None,
            code,
            message,
        } => {
            return Err(Error::Remote {
                code: *code,
                message: message.clone(),
            });
        }
        ServerMessage::Error {
            id: Some(id),
            code,
            message,
        } if *id == expected => {
            return Err(Error::Remote {
                code: *code,
                message: message.clone(),
            });
        }
        ServerMessage::Status { id, .. }
        | ServerMessage::ModelStatus { id, .. }
        | ServerMessage::Config { id, .. }
        | ServerMessage::IndexingConfig { id, .. }
        | ServerMessage::Error { id, .. } => *id,
        ServerMessage::Pong { id }
        | ServerMessage::ResolvedEntry { id, .. }
        | ServerMessage::Results { id, .. } => Some(*id),
        ServerMessage::HelloAck { .. } => None,
    };
    if id != Some(expected) {
        return Err(Error::Protocol("response has a foreign request identifier"));
    }
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::transport::tests::{ack, read_client, send_server};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use ws_protocol::{ClientMessage, ServerMessage};

    static SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn fixture() -> (Client, NamedPipeServer) {
        let pipe = format!(
            r"\\.\pipe\winsearch-S-1-5-21-client-test-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&pipe)
            .unwrap();
        (Client::new(pipe).unwrap(), server)
    }

    async fn hello(peer: &mut NamedPipeServer) {
        tokio::time::timeout(Duration::from_secs(1), peer.connect())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            read_client(peer).await,
            ClientMessage::Hello {
                protocol_version: 1,
                ..
            }
        ));
        send_server(peer, &ack()).await;
    }

    #[test]
    fn refuses_remote_worker_and_foreign_pipe_namespaces() {
        for pipe in [
            r"\\server\pipe\winsearch-S-1-5-21",
            r"\\.\pipe\winsearch-embed-S-1-5-21",
            r"\\.\pipe\unrelated",
            r"\\.\pipe\winsearch-S-1-5-21\escape",
            r"\\.\pipe\winsearch-S-1-5-21:stream",
        ] {
            assert!(
                matches!(Client::new(pipe), Err(Error::InvalidInput(_))),
                "{pipe}"
            );
        }
    }

    #[tokio::test]
    async fn status_uses_secured_pipe_framing_and_ignores_only_pushes() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: None,
                    status: EngineStatus::default(),
                },
            )
            .await;
            let ClientMessage::GetStatus { id } = read_client(&mut peer).await else {
                panic!("expected status request");
            };
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: Some(id),
                    status: EngineStatus {
                        files_indexed: 123,
                        ..Default::default()
                    },
                },
            )
            .await;
        });
        assert_eq!(client.status().await.unwrap().files_indexed, 123);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn unary_rejects_wrong_id_and_type_and_preserves_matching_errors() {
        for (response, remote) in [
            (
                ServerMessage::Status {
                    id: Some(999),
                    status: EngineStatus::default(),
                },
                false,
            ),
            (ServerMessage::Pong { id: 1 }, false),
            (
                ServerMessage::Error {
                    id: Some(1),
                    code: ws_protocol::ErrorCode::NotReady,
                    message: "busy".into(),
                },
                true,
            ),
        ] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                assert!(matches!(
                    read_client(&mut peer).await,
                    ClientMessage::GetStatus { id: 1 }
                ));
                send_server(&mut peer, &response).await;
            });
            let response = client.status().await;
            if remote {
                assert!(matches!(
                    response,
                    Err(Error::Remote {
                        code: ws_protocol::ErrorCode::NotReady,
                        ..
                    })
                ));
            } else {
                assert!(matches!(response, Err(Error::Protocol(_))));
            }
            task.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_pipe_has_bounded_connect_deadline() {
        let (client, peer) = fixture();
        drop(peer);
        let before = tokio::time::Instant::now();
        assert!(matches!(client.status().await, Err(Error::Timeout)));
        assert!(before.elapsed() <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn typed_configuration_and_model_operations_preserve_wire_values() {
        for operation in 0..9 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                let request = read_client(&mut peer).await;
                let config = EngineConfig {
                    semantic_enabled: false,
                    preferred_backend: "cpu".into(),
                };
                let indexing = IndexingConfig {
                    roots: vec![r"C:\approved".into()],
                    paused: true,
                    ..Default::default()
                };
                let model = ModelStatus {
                    state: ws_protocol::ModelState::Paused,
                    bytes: 7,
                    total_bytes: 10,
                    error: None,
                };
                let response = match (operation, request) {
                    (0, ClientMessage::GetConfig { id }) => ServerMessage::Config {
                        id: Some(id),
                        config,
                    },
                    (1, ClientMessage::SetConfig { id, config: value }) => {
                        assert_eq!(value, config);
                        ServerMessage::Config {
                            id: Some(id),
                            config,
                        }
                    }
                    (2, ClientMessage::GetIndexingConfig { id }) => ServerMessage::IndexingConfig {
                        id: Some(id),
                        config: indexing,
                    },
                    (3, ClientMessage::SetIndexingConfig { id, config: value }) => {
                        assert_eq!(value, indexing);
                        ServerMessage::IndexingConfig {
                            id: Some(id),
                            config: indexing,
                        }
                    }
                    (4, ClientMessage::GetModelStatus { id })
                    | (5, ClientMessage::ModelDownloadStart { id })
                    | (6, ClientMessage::ModelDownloadCancel { id })
                    | (8, ClientMessage::ModelDelete { id }) => ServerMessage::ModelStatus {
                        id: Some(id),
                        status: model,
                    },
                    (7, ClientMessage::ModelImport { id, path }) => {
                        assert_eq!(path, r"C:\model-import");
                        ServerMessage::ModelStatus {
                            id: Some(id),
                            status: model,
                        }
                    }
                    (_, request) => panic!("wrong operation {operation}: {request:?}"),
                };
                send_server(&mut peer, &response).await;
            });
            match operation {
                0 => assert_eq!(client.config().await.unwrap().preferred_backend, "cpu"),
                1 => assert!(
                    !client
                        .set_config(EngineConfig {
                            semantic_enabled: false,
                            preferred_backend: "cpu".into()
                        })
                        .await
                        .unwrap()
                        .semantic_enabled
                ),
                2 => assert_eq!(
                    client.indexing_config().await.unwrap().roots,
                    [r"C:\approved"]
                ),
                3 => assert!(
                    client
                        .set_indexing_config(IndexingConfig {
                            roots: vec![r"C:\approved".into()],
                            paused: true,
                            ..Default::default()
                        })
                        .await
                        .unwrap()
                        .paused
                ),
                _ => {
                    let model = match operation {
                        4 => client.model_status().await,
                        5 => client.start_model_download().await,
                        6 => client.cancel_model_download().await,
                        7 => client.import_model(r"C:\model-import".into()).await,
                        8 => client.delete_model().await,
                        _ => unreachable!(),
                    }
                    .unwrap();
                    assert_eq!(
                        (model.state, model.bytes, model.total_bytes),
                        (ws_protocol::ModelState::Paused, 7, 10)
                    );
                }
            }
            task.await.unwrap();
        }
    }

    fn resolved() -> ResolvedEntry {
        ResolvedEntry {
            result_id: "e42".into(),
            root_path: r"C:\approved".into(),
            path: r"c:\approved\report.txt".into(),
            name: "report.txt".into(),
            kind: ws_protocol::ResultKind::Document,
            size_bytes: 7,
            modified_unix_seconds: Some(1),
            index_revision: Some(2),
            source_revision: "proof".into(),
        }
    }

    #[tokio::test]
    async fn entry_resolution_checks_identity_proof_and_lexical_root_before_delivery() {
        for case in 0..10 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                let ClientMessage::ResolveEntry { id, reference } = read_client(&mut peer).await
                else {
                    panic!("expected indexed reference");
                };
                assert_eq!(
                    reference,
                    EntryReference {
                        result_id: "e42".into(),
                        source_revision: Some("proof".into())
                    }
                );
                let mut entry = resolved();
                match case {
                    1 => entry.result_id = "e43".into(),
                    2 => entry.source_revision = "stale".into(),
                    3 => entry.source_revision.clear(),
                    4 => entry.path = r"C:\approved\..\outside\report.txt".into(),
                    5 => entry.path = r"\\server\approved\report.txt".into(),
                    6 => entry.path = r"C:\approved\report.txt:secret".into(),
                    7 => entry.index_revision = Some(0),
                    8 => entry.path = r"C:approved\report.txt".into(),
                    9 => entry.path = r"C:\approved-sibling\report.txt".into(),
                    _ => {}
                }
                send_server(&mut peer, &ServerMessage::ResolvedEntry { id, entry }).await;
            });
            let value = client
                .resolve_entry(EntryReference {
                    result_id: "e42".into(),
                    source_revision: Some("proof".into()),
                })
                .await;
            if case == 0 {
                assert_eq!(value.unwrap().size_bytes, 7);
            } else {
                assert!(
                    matches!(value, Err(Error::Protocol(_))),
                    "case {case}: {value:?}"
                );
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn legacy_capabilities_refuse_entry_resolution_before_sending_an_unknown_frame() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), peer.connect())
                .await
                .unwrap()
                .unwrap();
            let _ = read_client(&mut peer).await;
            send_server(
                &mut peer,
                &ServerMessage::HelloAck {
                    protocol_version: 1,
                    engine_version: "legacy".into(),
                    features: vec!["search.exact".into()],
                },
            )
            .await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "unsupported request must never be written"
            );
        });
        assert!(matches!(
            client
                .resolve_entry(EntryReference {
                    result_id: "e42".into(),
                    source_revision: None
                })
                .await,
            Err(Error::Unsupported("entry.resolve"))
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_delivers_ordered_batches_then_releases_connection_at_terminal() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let ClientMessage::Search { id, query } = read_client(&mut peer).await else {
                panic!("expected search");
            };
            assert_eq!(query.text, "invoice");
            send_server(
                &mut peer,
                &ServerMessage::Results {
                    id,
                    batch: vec![SearchResult {
                        id: "e1".into(),
                        title: "invoice.txt".into(),
                        ..Default::default()
                    }],
                    done: false,
                },
            )
            .await;
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: None,
                    status: EngineStatus::default(),
                },
            )
            .await;
            send_server(
                &mut peer,
                &ServerMessage::Results {
                    id,
                    batch: vec![SearchResult {
                        id: "e2".into(),
                        title: "invoice.pdf".into(),
                        ..Default::default()
                    }],
                    done: true,
                },
            )
            .await;
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        });
        let mut stream = client
            .search(SearchQuery {
                text: "invoice".into(),
                limit: 20,
                ..Default::default()
            })
            .await
            .unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.batch[0].id, "e1");
        assert!(!first.done);
        let last = stream.next().await.unwrap().unwrap();
        assert_eq!(last.batch[0].id, "e2");
        assert!(last.done);
        assert_eq!(stream.next().await.unwrap(), None);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_search_frames_fail_once_and_close_the_stream() {
        for case in 0..3 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                    panic!("expected search");
                };
                let response = match case {
                    0 => ServerMessage::Results {
                        id: id + 1,
                        batch: vec![],
                        done: true,
                    },
                    1 => ServerMessage::Pong { id },
                    _ => ServerMessage::Results {
                        id,
                        batch: vec![SearchResult::default(); 4097],
                        done: true,
                    },
                };
                send_server(&mut peer, &response).await;
            });
            let mut stream = client.search(SearchQuery::default()).await.unwrap();
            assert!(matches!(stream.next().await, Err(Error::Protocol(_))));
            assert_eq!(stream.next().await.unwrap(), None);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn explicit_search_cancel_and_drop_both_close_owned_pipe() {
        use tokio::io::AsyncReadExt;
        for explicit in [true, false] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                    panic!("expected search");
                };
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        batch: vec![],
                        done: false,
                    },
                )
                .await;
                if explicit {
                    assert_eq!(read_client(&mut peer).await, ClientMessage::Cancel { id });
                }
                assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            });
            let mut stream = client.search(SearchQuery::default()).await.unwrap();
            assert!(!stream.next().await.unwrap().unwrap().done);
            if explicit {
                stream.cancel().await.unwrap();
            } else {
                drop(stream);
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn search_bounds_input_before_connect_and_checks_optional_capabilities() {
        let (client, peer) = fixture();
        assert!(matches!(
            client
                .search(SearchQuery {
                    limit: 4097,
                    ..Default::default()
                })
                .await,
            Err(Error::InvalidInput(_))
        ));
        drop(peer);
        for code in [false, true] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                hello(&mut peer).await;
                assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            });
            let result = client
                .search(SearchQuery {
                    include_semantic: !code,
                    mode: if code {
                        ws_protocol::SearchMode::Code
                    } else {
                        ws_protocol::SearchMode::Files
                    },
                    ..Default::default()
                })
                .await;
            assert!(matches!(result, Err(Error::Unsupported(_))));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn subscriptions_deliver_only_unsolicited_typed_events() {
        for foreign in [false, true] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                send_server(
                    &mut peer,
                    &ServerMessage::Status {
                        id: None,
                        status: EngineStatus {
                            files_indexed: 55,
                            ..Default::default()
                        },
                    },
                )
                .await;
                send_server(
                    &mut peer,
                    &ServerMessage::Config {
                        id: if foreign { Some(99) } else { None },
                        config: EngineConfig::default(),
                    },
                )
                .await;
            });
            let mut subscription = client.subscribe().await.unwrap();
            assert!(matches!(
                subscription.next().await.unwrap(),
                Some(EngineEvent::Status(EngineStatus {
                    files_indexed: 55,
                    ..
                }))
            ));
            if foreign {
                assert!(matches!(subscription.next().await, Err(Error::Protocol(_))));
                assert_eq!(subscription.next().await.unwrap(), None);
            } else {
                assert!(matches!(
                    subscription.next().await.unwrap(),
                    Some(EngineEvent::Config(_))
                ));
            }
            drop(subscription);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn dropped_next_future_disconnects_partial_search_without_a_background_task() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let _ = read_client(&mut peer).await;
            peer.write_all(&[20, 0]).await.unwrap();
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        });
        let mut stream = client.search(SearchQuery::default()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );
        assert!(matches!(stream.next().await, Err(Error::Closed)));
        assert_eq!(stream.next().await.unwrap(), None);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_caps_total_stream_frames_and_releases_admission_after_failure() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                panic!("search");
            };
            for _ in 0..65 {
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        batch: vec![],
                        done: false,
                    },
                )
                .await;
            }
        });
        let mut stream = client.search(SearchQuery::default()).await.unwrap();
        for _ in 0..64 {
            assert!(!stream.next().await.unwrap().unwrap().done);
        }
        assert!(matches!(stream.next().await, Err(Error::Protocol(_))));
        assert_eq!(stream.next().await.unwrap(), None);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn saturated_reads_do_not_block_ordered_configuration_mutations() {
        let (client, first) = fixture();
        let mut peers = vec![first];
        for _ in 1..7 {
            peers.push(ServerOptions::new().create(&client.0.pipe).unwrap());
        }
        let read_gate = Arc::new(Semaphore::new(0));
        let control_gate = Arc::new(Semaphore::new(0));
        let (notice, mut notices) = tokio::sync::mpsc::channel(8);
        let mut servers = Vec::new();
        for mut peer in peers {
            let (read_gate, control_gate, notice) =
                (read_gate.clone(), control_gate.clone(), notice.clone());
            servers.push(tokio::spawn(async move {
                hello(&mut peer).await;
                match read_client(&mut peer).await {
                    ClientMessage::GetStatus { id } => {
                        notice.send("read").await.unwrap();
                        read_gate.acquire().await.unwrap().forget();
                        send_server(
                            &mut peer,
                            &ServerMessage::Status {
                                id: Some(id),
                                status: EngineStatus::default(),
                            },
                        )
                        .await;
                    }
                    ClientMessage::SetConfig { id, config } => {
                        notice.send("control").await.unwrap();
                        control_gate.acquire().await.unwrap().forget();
                        send_server(
                            &mut peer,
                            &ServerMessage::Config {
                                id: Some(id),
                                config,
                            },
                        )
                        .await;
                    }
                    request => panic!("unexpected lane request {request:?}"),
                }
            }));
        }
        let mut readers = Vec::new();
        for _ in 0..4 {
            let client = client.clone();
            readers.push(tokio::spawn(async move { client.status().await.unwrap() }));
        }
        for _ in 0..4 {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), notices.recv())
                    .await
                    .unwrap(),
                Some("read")
            );
        }
        let clone = client.clone();
        readers.push(tokio::spawn(async move { clone.status().await.unwrap() }));
        let mut controls = Vec::new();
        for _ in 0..2 {
            let client = client.clone();
            controls.push(tokio::spawn(async move {
                client.set_config(EngineConfig::default()).await.unwrap()
            }));
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), notices.recv())
                .await
                .unwrap(),
            Some("control")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), notices.recv())
                .await
                .is_err(),
            "fifth read and second mutation must remain unadmitted"
        );
        control_gate.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), notices.recv())
                .await
                .unwrap(),
            Some("control")
        );
        control_gate.add_permits(1);
        for control in controls {
            control.await.unwrap();
        }
        read_gate.add_permits(5);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), notices.recv())
                .await
                .unwrap(),
            Some("read")
        );
        for reader in readers {
            reader.await.unwrap();
        }
        for server in servers {
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn lost_mutation_response_is_not_retried_automatically() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::SetConfig { .. }
            ));
            // Applied, then disconnected before acknowledgement: replay could overwrite newer policy.
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.set_config(EngineConfig::default()),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Closed)));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn lost_mutation_reply_fences_later_writes_but_allows_status_recovery() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::SetConfig { .. }
            ));
        });
        assert!(matches!(
            client.set_config(EngineConfig::default()).await,
            Err(Error::Closed)
        ));
        task.await.unwrap();
        let mut peer = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&client.0.pipe)
            .unwrap();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let ClientMessage::GetStatus { id } = read_client(&mut peer).await else {
                panic!("recovery status");
            };
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: Some(id),
                    status: EngineStatus::default(),
                },
            )
            .await;
        });
        client.status().await.unwrap();
        task.await.unwrap();
        assert!(
            matches!(
                client
                    .clone()
                    .set_indexing_config(IndexingConfig::default())
                    .await,
                Err(Error::MutationOutcomeUnknown)
            ),
            "unknown older policy must not be overtaken by another write"
        );
    }

    #[tokio::test]
    async fn dropping_an_in_flight_mutation_fences_later_writes() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let (seen, received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::SetIndexingConfig { .. }
            ));
            seen.send(()).unwrap();
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        });
        {
            let mutation = client.set_indexing_config(IndexingConfig::default());
            tokio::pin!(mutation);
            tokio::select! {
                result = &mut mutation => panic!("mutation must remain in flight: {result:?}"),
                _ = received => {}
            }
        }
        task.await.unwrap();
        assert!(matches!(
            client.set_config(EngineConfig::default()).await,
            Err(Error::MutationOutcomeUnknown)
        ));
    }

    #[tokio::test]
    async fn acknowledged_rejection_does_not_fence_a_later_valid_mutation() {
        let (client, mut first) = fixture();
        let mut second = ServerOptions::new().create(&client.0.pipe).unwrap();
        let task = tokio::spawn(async move {
            hello(&mut first).await;
            let ClientMessage::SetConfig { id, .. } = read_client(&mut first).await else {
                panic!("set config");
            };
            send_server(
                &mut first,
                &ServerMessage::Error {
                    id: Some(id),
                    code: ws_protocol::ErrorCode::InvalidRequest,
                    message: "invalid backend".into(),
                },
            )
            .await;
            hello(&mut second).await;
            let ClientMessage::SetConfig { id, config } = read_client(&mut second).await else {
                panic!("set config");
            };
            send_server(
                &mut second,
                &ServerMessage::Config {
                    id: Some(id),
                    config,
                },
            )
            .await;
        });
        assert!(matches!(
            client
                .set_config(EngineConfig {
                    preferred_backend: "invalid".into(),
                    ..Default::default()
                })
                .await,
            Err(Error::Remote {
                code: ws_protocol::ErrorCode::InvalidRequest,
                ..
            })
        ));
        assert_eq!(
            client.set_config(EngineConfig::default()).await.unwrap(),
            EngineConfig::default()
        );
        task.await.unwrap();
    }
}
