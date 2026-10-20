//! Named-pipe IPC client for `winsearch-cli` to communicate with a running
//! `winsearchd` daemon.

use ws_platform::identity;
use ws_platform::pipe::PipeStream;
use ws_protocol::{
    read_message, write_message, ClientMessage, EngineConfig, EngineStatus, ModelStatus,
    SearchDisposition, SearchOutcome, SearchQuery, SearchResult, ServerMessage, PROTOCOL_VERSION,
};

pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("daemon not running on pipe {pipe}: {source}")]
    NotConnected {
        pipe: String,
        #[source]
        source: std::io::Error,
    },
    #[error("ipc framing error: {0}")]
    Framing(#[from] ws_protocol::FrameError),
    #[error("engine returned error ({code:?}): {message}")]
    ServerError {
        code: ws_protocol::ErrorCode,
        message: String,
    },
    #[error("unexpected EOF from daemon")]
    UnexpectedEof,
    #[error("invalid or missing terminal search outcome")]
    InvalidSearchOutcome,
    #[error("search interrupted: {0:?}")]
    SearchInterrupted(SearchDisposition),
    #[error("invalid search extension filter")]
    InvalidSearchExtensions,
    #[error("daemon does not support {0}")]
    UnsupportedFeature(&'static str),
    #[error("daemon returned a result outside the extension filter")]
    UnselectedSearchExtension,
    #[error("invalid search file kind filter")]
    InvalidSearchFileKinds,
    #[error("daemon returned a result outside the file kind filter")]
    UnselectedSearchFileKind,
}

pub struct DaemonClient {
    pipe_name: String,
    stream: PipeStream,
    pub engine_version: String,
    pub features: Vec<String>,
    pub initial_status: Option<EngineStatus>,
    pub last_search_outcome: Option<SearchOutcome>,
    next_id: u64,
}

impl DaemonClient {
    /// Connects to the default per-user `winsearchd` pipe (or `WS_PIPE` override
    /// for tests) and completes the protocol handshake.
    pub fn connect() -> Result<Self, IpcError> {
        let pipe = std::env::var("WS_PIPE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                identity::pipe_name().unwrap_or_else(|_| r"\\.\pipe\winsearch-local".to_string())
            });
        Self::connect_pipe(&pipe)
    }

    pub fn connect_pipe(pipe: &str) -> Result<Self, IpcError> {
        let mut stream = PipeStream::connect(pipe).map_err(|source| IpcError::NotConnected {
            pipe: pipe.to_string(),
            source,
        })?;

        write_message(
            &mut stream,
            &ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                client: "winsearch-cli".to_string(),
                client_version: CLI_VERSION.to_string(),
            },
        )?;

        let first: Option<ServerMessage> = read_message(&mut stream)?;
        let (engine_version, features) = match first {
            Some(ServerMessage::HelloAck {
                engine_version,
                features,
                ..
            }) => (engine_version, features),
            Some(ServerMessage::Error { code, message, .. }) => {
                return Err(IpcError::ServerError { code, message });
            }
            Some(_) => ("unknown".to_string(), Vec::new()),
            None => return Err(IpcError::UnexpectedEof),
        };

        Ok(Self {
            pipe_name: pipe.to_string(),
            stream,
            engine_version,
            features,
            initial_status: None,
            last_search_outcome: None,
            next_id: 1,
        })
    }

    pub fn pipe_name(&self) -> &str {
        &self.pipe_name
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Requests a live `EngineStatus` snapshot from the daemon.
    pub fn get_status(&mut self) -> Result<EngineStatus, IpcError> {
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::GetStatus { id })?;
        loop {
            match read_message::<_, ServerMessage>(&mut self.stream)? {
                Some(ServerMessage::Status {
                    id: Some(resp_id),
                    status,
                }) if resp_id == id => return Ok(status),
                Some(ServerMessage::Status { id: None, status }) => {
                    self.initial_status = Some(status);
                }
                Some(ServerMessage::Error {
                    id: Some(resp_id),
                    code,
                    message,
                }) if resp_id == id => return Err(IpcError::ServerError { code, message }),
                Some(_) => continue,
                None => return Err(IpcError::UnexpectedEof),
            }
        }
    }

    /// Requests the current `ModelStatus` from the daemon.
    pub fn get_model_status(&mut self) -> Result<ModelStatus, IpcError> {
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::GetModelStatus { id })?;
        loop {
            match read_message::<_, ServerMessage>(&mut self.stream)? {
                Some(ServerMessage::ModelStatus {
                    id: Some(resp_id),
                    status,
                }) if resp_id == id => return Ok(status),
                Some(ServerMessage::Error {
                    id: Some(resp_id),
                    code,
                    message,
                }) if resp_id == id => return Err(IpcError::ServerError { code, message }),
                Some(_) => continue,
                None => return Err(IpcError::UnexpectedEof),
            }
        }
    }

    /// Requests the current `EngineConfig` from the daemon.
    pub fn get_config(&mut self) -> Result<EngineConfig, IpcError> {
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::GetConfig { id })?;
        loop {
            match read_message::<_, ServerMessage>(&mut self.stream)? {
                Some(ServerMessage::Config {
                    id: Some(resp_id),
                    config,
                }) if resp_id == id => return Ok(config),
                Some(ServerMessage::Error {
                    id: Some(resp_id),
                    code,
                    message,
                }) if resp_id == id => return Err(IpcError::ServerError { code, message }),
                Some(_) => continue,
                None => return Err(IpcError::UnexpectedEof),
            }
        }
    }

    /// Executes a streaming search over IPC and collects all batches until
    /// `done: true`.
    pub fn search(&mut self, query: SearchQuery) -> Result<Vec<SearchResult>, IpcError> {
        self.last_search_outcome=None;
        if !ws_protocol::valid_search_extensions(&query.extensions) {
            return Err(IpcError::InvalidSearchExtensions);
        }
        if !ws_protocol::valid_search_file_kinds(&query.file_kinds) {
            return Err(IpcError::InvalidSearchFileKinds);
        }
        if !query.file_kinds.is_empty() && !self.features.iter().any(|feature| feature == "search.file_kinds") {
            return Err(IpcError::UnsupportedFeature("search.file_kinds"));
        }
        if !query.extensions.is_empty() && !self.features.iter().any(|feature| feature == "search.filters") {
            return Err(IpcError::UnsupportedFeature("search.filters"));
        }
        let extensions = query.extensions.clone();
        let file_kinds = query.file_kinds.clone();
        let requested_semantic=query.include_semantic;
        let require_outcome=self.features.iter().any(|feature|feature=="search.outcomes");
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::Search { id, query })?;
        let mut results = Vec::new();
        loop {
            match read_message::<_, ServerMessage>(&mut self.stream)? {
                Some(ServerMessage::Results {
                    id: resp_id,
                    batch,
                    done,
                    outcome,
                }) if resp_id == id => {
                    if batch.iter().any(|hit| !ws_protocol::search_file_kind_result_allowed(&file_kinds,hit)) {
                        return Err(IpcError::UnselectedSearchFileKind);
                    }
                    if !extensions.is_empty() && batch.iter().any(|hit|
                        hit.kind == ws_protocol::ResultKind::Folder || !hit.path.as_deref().is_some_and(|path|
                            ws_protocol::search_extension_allowed(&extensions,
                                std::path::Path::new(path).extension().and_then(|extension| extension.to_str()).unwrap_or("")))) {
                        return Err(IpcError::UnselectedSearchExtension);
                    }
                    if (done && require_outcome && outcome.is_none()) || outcome.as_ref()
                        .is_some_and(|state|!state.valid_for(done,batch.is_empty(),requested_semantic)) {
                        return Err(IpcError::InvalidSearchOutcome);
                    }
                    results.extend(batch);
                    if done {
                        self.last_search_outcome=outcome;
                        if let Some(outcome)=outcome.filter(|state|state.disposition!=SearchDisposition::Complete) {
                            return Err(IpcError::SearchInterrupted(outcome.disposition));
                        }
                        return Ok(results);
                    }
                }
                Some(ServerMessage::Error {
                    id: Some(resp_id),
                    code,
                    message,
                }) if resp_id == id => return Err(IpcError::ServerError { code, message }),
                Some(_) => continue,
                None => return Err(IpcError::UnexpectedEof),
            }
        }
    }

    pub fn start_model_download(
        &mut self,
        progress: impl FnMut(&ModelStatus),
    ) -> Result<ModelStatus, IpcError> {
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::ModelDownloadStart { id })?;
        self.await_model_terminal(id, progress)
    }

    pub fn import_model(
        &mut self,
        path: String,
        progress: impl FnMut(&ModelStatus),
    ) -> Result<ModelStatus, IpcError> {
        let id = self.alloc_id();
        write_message(&mut self.stream, &ClientMessage::ModelImport { id, path })?;
        self.await_model_terminal(id, progress)
    }

    fn await_model_terminal(
        &mut self,
        id: u64,
        mut progress: impl FnMut(&ModelStatus),
    ) -> Result<ModelStatus, IpcError> {
        loop {
            match read_message::<_, ServerMessage>(&mut self.stream)? {
                Some(ServerMessage::ModelStatus {
                    id: resp_id,
                    status,
                }) if resp_id.is_none() || resp_id == Some(id) => {
                    progress(&status);
                    if status.state != ws_modelmgr::ModelState::Downloading {
                        return Ok(status);
                    }
                }
                Some(ServerMessage::Error {
                    id: resp_id,
                    code,
                    message,
                }) if resp_id.is_none() || resp_id == Some(id) => {
                    return Err(IpcError::ServerError { code, message });
                }
                Some(_) => continue,
                None => return Err(IpcError::UnexpectedEof),
            }
        }
    }
}
