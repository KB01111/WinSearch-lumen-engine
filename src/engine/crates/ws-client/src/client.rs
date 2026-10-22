use crate::{transport::Session, EngineInfo, Error};
use std::num::NonZeroU32;
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
const LEGACY_MAX_SEARCH_RESULTS: usize = 4096;
const MAX_SEARCH_FRAMES: usize = 64;

struct Inner {
    pipe: String,
    expected_process_id: Option<NonZeroU32>,
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
    // This guard must drop before the control permit releases queued writers.
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
    pub outcome: Option<ws_protocol::SearchOutcome>,
}

/// Dropping the stream (or a pending next call) closes its owned connection.
pub struct SearchStream {
    operation: Option<Operation>,
    results: usize,
    frames: usize,
    byte_start: usize,
    mode: ws_protocol::SearchMode,
    extensions: Vec<String>,
    file_kinds: Vec<ws_protocol::FileKind>,
    require_outcome: bool,
    requested_semantic: bool,
    max_results: usize,
    related_source: Option<ws_protocol::IndexedEntryId>,
    related_categories: Vec<ws_protocol::ResultKind>,
}

impl SearchStream {
    pub async fn next(&mut self) -> Result<Option<SearchBatch>, Error> {
        let Some(operation) = self.operation.as_mut() else {
            return Ok(None);
        };
        let result = async {
            loop {
                let message = operation.session.recv(Some(operation.deadline)).await?;
                if operation.session.received_bytes.saturating_sub(self.byte_start) > ws_protocol::MAX_SEARCH_RESPONSE_BYTES {
                    return Err(Error::Protocol("search stream exceeded its byte budget"));
                }
                if is_push(&message) {
                    continue;
                }
                validate_id(&message, operation.id)?;
                let ServerMessage::Results { batch, done, outcome, .. } = message else {
                    return Err(Error::Protocol("expected search results"));
                };
                self.frames += 1;
                self.results = self.results.saturating_add(batch.len());
                if self.frames > MAX_SEARCH_FRAMES || self.results > self.max_results {
                    return Err(Error::Protocol("search stream exceeded its result budget"));
                }
                if (done && self.require_outcome && outcome.is_none()) || outcome.as_ref()
                    .is_some_and(|value|!value.valid_for(done,batch.is_empty(),self.requested_semantic)) {
                    return Err(Error::Protocol("search returned an invalid or missing terminal outcome"));
                }
                if let Some(source)=self.related_source {
                    if outcome.is_some_and(|outcome|outcome.disposition==ws_protocol::SearchDisposition::Complete
                        && outcome.semantic.is_some_and(|semantic|semantic.phase==ws_protocol::SemanticPhase::Disabled))
                        || batch.iter().any(|hit|!self.related_categories.is_empty() && !self.related_categories.contains(&hit.kind)
                            || hit.entry_reference.as_ref().is_none_or(|reference|reference.indexed_id()==Some(source))) {
                        return Err(Error::Protocol("Related returned its source, an unbound result or ordinary fallback state"));
                    }
                }
                if batch.iter().any(|hit|hit.entry_reference.as_ref().is_some_and(|reference|
                    !reference.is_bound() || !reference_matches_kind(reference,hit.kind)
                    || (matches!(reference.indexed_id(),Some(ws_protocol::IndexedEntryId::Code(_)))
                        != (self.mode==ws_protocol::SearchMode::Code)))) {
                    return Err(Error::Protocol("search returned an invalid bound entry reference"));
                }
                if !self.extensions.is_empty() && batch.iter().any(|hit|
                    hit.kind==ws_protocol::ResultKind::Folder || !hit.path.as_deref().is_some_and(|path|ws_protocol::search_extension_allowed(&self.extensions,
                        std::path::Path::new(path).extension().and_then(|extension|extension.to_str()).unwrap_or("")))) {
                    return Err(Error::Protocol("search returned a result outside the extension filter"));
                }
                if batch.iter().any(|hit| !ws_protocol::search_file_kind_result_allowed(&self.file_kinds,hit)) {
                    return Err(Error::Protocol("search returned a result outside the file kind filter"));
                }
                return Ok(SearchBatch { batch, done, outcome });
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
    /// Validates only the local pipe namespace. Owned hosts use `for_process`.
    pub fn new(pipe: impl Into<String>) -> Result<Self, Error> {
        Self::with_process(pipe, None)
    }

    /// Connect only to the host-owned process. Keep its process handle alive for
    /// this client's lifetime so an exited process's PID cannot be reused.
    pub fn for_process(pipe: impl Into<String>, process_id: NonZeroU32) -> Result<Self, Error> {
        Self::with_process(pipe, Some(process_id))
    }

    fn with_process(
        pipe: impl Into<String>,
        expected_process_id: Option<NonZeroU32>,
    ) -> Result<Self, Error> {
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
            expected_process_id,
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
                    Ok(io) => {
                        if let Some(expected) = self.0.expected_process_id {
                            verify_server_process(&io, expected)?;
                        }
                        return Session::handshake(io, deadline).await;
                    }
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
            if let ServerMessage::Error {
                id: Some(id), code, ..
            } = &message
            {
                if *id == operation.id {
                    // Ingestion can commit before the final admission/delivery
                    // fence fails. Keep its mutation guard armed until it poisons
                    // the client, before another control waiter obtains the lane.
                    if control
                        && feature == Some("entry.enrichment")
                        && matches!(
                            code,
                            ws_protocol::ErrorCode::Cancelled
                                | ws_protocol::ErrorCode::NotReady
                                | ws_protocol::ErrorCode::Internal
                        )
                    {
                        return Err(Error::MutationOutcomeUnknown);
                    }
                    operation.acknowledge();
                }
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

    pub async fn reset_index(
        &self,
        request: ws_protocol::ResetRequest,
    ) -> Result<ws_protocol::IndexReset, Error> {
        if !request.is_valid() {
            return Err(Error::InvalidInput("invalid reset namespace"));
        }
        let expected = request.clone();
        let result = self
            .unary(
                true,
                Some("index.reset"),
                |id| ClientMessage::ResetIndex { id, request },
                |message| match message {
                    ServerMessage::IndexReset { reset, .. }
                        if reset.is_valid()
                            && reset.index_id != expected.expected_index_id
                            && reset.code_index_id != expected.expected_code_index_id =>
                    {
                        Ok(reset)
                    }
                    _ => Err(Error::Protocol("invalid reset acknowledgment")),
                },
            )
            .await;
        if matches!(
            result,
            Err(Error::Remote {
                code: ws_protocol::ErrorCode::Internal,
                ..
            })
        ) {
            self.0.mutation_uncertain.store(true, Ordering::Release);
        }
        result
    }

    pub async fn ingest_enrichment(
        &self,
        request: ws_protocol::EnrichmentRequest,
    ) -> Result<ws_protocol::EnrichmentIngested, Error> {
        if !request.is_valid() {
            return Err(Error::InvalidInput("invalid enrichment request"));
        }
        let expected = request.clone();
        self.unary(
            true,
            Some("entry.enrichment"),
            |id| ClientMessage::IngestEnrichment { id, request },
            |message| match message {
                ServerMessage::EnrichmentIngested { result, .. }
                    if result.index_id == expected.index_id
                        && result.model_generation == expected.model_generation
                        && result.grant_generation == expected.grant_generation
                        && result.content_revision > 0
                        && result.reference.is_bound()
                        && result.reference.result_id == expected.reference.result_id =>
                {
                    Ok(result)
                }
                _ => Err(Error::Protocol("invalid enrichment acknowledgment")),
            },
        )
        .await
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
        let feature = (!config.root_policies.is_empty()).then_some("index.root_policies");
        self.unary(
            true,
            feature,
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
        self.start_model_download_with_npu(false).await
    }
    /// Explicit optional bundle installation; legacy engines must advertise support.
    pub async fn start_model_download_with_npu(
        &self,
        include_npu: bool,
    ) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            include_npu.then_some("model.npu_bundle"),
            |id| ClientMessage::ModelDownloadStart { id, include_npu },
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
        self.import_model_with_npu(path, false).await
    }
    pub async fn import_model_with_npu(
        &self,
        path: String,
        include_npu: bool,
    ) -> Result<ModelStatus, Error> {
        self.unary(
            true,
            include_npu.then_some("model.npu_bundle"),
            |id| ClientMessage::ModelImport {
                id,
                path,
                include_npu,
            },
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
        if !reference.is_valid() {
            return Err(Error::InvalidInput("invalid indexed entry reference"));
        }
        let feature = match reference.indexed_id() {
            Some(ws_protocol::IndexedEntryId::Code(_)) => "entry.resolve.code",
            _ => "entry.resolve",
        };
        let expected = reference.clone();
        self.unary(
            false,
            Some(feature),
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

    pub async fn preview_text(
        &self,
        request: ws_protocol::PreviewRequest,
    ) -> Result<ws_protocol::EntryPreview, Error> {
        if !request.reference.is_valid() {
            return Err(Error::InvalidInput("preview exceeds its request budget"));
        }
        let feature = match request.reference.indexed_id() {
            Some(ws_protocol::IndexedEntryId::File(_))
                if ws_protocol::valid_preview_location(request.location) =>
            {
                "entry.preview.text"
            }
            Some(ws_protocol::IndexedEntryId::Code(_))
                if ws_protocol::valid_code_preview_location(request.location) =>
            {
                "entry.preview.code"
            }
            _ => {
                return Err(Error::InvalidInput(
                    "invalid preview location for this entry",
                ))
            }
        };
        let expected = request.clone();
        self.unary(
            false,
            Some(feature),
            |id| ClientMessage::PreviewEntry { id, request },
            |message| match message {
                ServerMessage::EntryPreview { preview, .. }
                    if preview_matches(&preview, &expected) =>
                {
                    Ok(preview)
                }
                _ => Err(Error::Protocol(
                    "preview identity, location, state or budget changed",
                )),
            },
        )
        .await
    }

    pub async fn search(&self, query: SearchQuery) -> Result<SearchStream, Error> {
        if !ws_protocol::valid_search_query(&query) {
            return Err(Error::InvalidInput("search exceeds its request budget"));
        }
        let mut operation = self.open(false, Some("search.exact")).await?;
        let limit_aware = operation.session.info.supports("search.limits");
        if query.limit as usize > LEGACY_MAX_SEARCH_RESULTS && !limit_aware {
            return Err(Error::Unsupported("search.limits"));
        }
        let max_results = if limit_aware {
            query.limit.max(1) as usize
        } else {
            LEGACY_MAX_SEARCH_RESULTS
        };
        let byte_start = operation.session.received_bytes;
        for feature in [
            (query.include_semantic, "search.semantic"),
            (query.mode == ws_protocol::SearchMode::Code, "code"),
            (!query.extensions.is_empty(), "search.filters"),
            (!query.file_kinds.is_empty(), "search.file_kinds"),
        ] {
            if feature.0 && !operation.session.info.supports(feature.1) {
                return Err(Error::Unsupported(feature.1));
            }
        }
        let mode = query.mode;
        let extensions = query.extensions.clone();
        let file_kinds = query.file_kinds.clone();
        let requested_semantic = query.include_semantic;
        let require_outcome = operation.session.info.supports("search.outcomes");
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
            byte_start,
            mode,
            extensions,
            file_kinds,
            require_outcome,
            requested_semantic,
            max_results,
            related_source: None,
            related_categories: Vec::new(),
        })
    }

    pub async fn related(&self, query: ws_protocol::RelatedQuery) -> Result<SearchStream, Error> {
        if !query.is_valid() {
            return Err(Error::InvalidInput(
                "invalid Related source or request budget",
            ));
        }
        let mut operation = self.open(false, Some("search.related")).await?;
        let source = query.source.indexed_id().expect("validated indexed source");
        let code = matches!(source, ws_protocol::IndexedEntryId::Code(_));
        for (required, feature) in [
            (true, "search.semantic"),
            (true, "search.outcomes"),
            (code, "code"),
            (!query.extensions.is_empty(), "search.filters"),
            (!query.file_kinds.is_empty(), "search.file_kinds"),
        ] {
            if required && !operation.session.info.supports(feature) {
                return Err(Error::Unsupported(feature));
            }
        }
        let extensions = query.extensions.clone();
        let categories = query.categories.clone();
        let limit = query.limit as usize;
        let file_kinds = query.file_kinds.clone();
        let byte_start = operation.session.received_bytes;
        operation
            .session
            .send(
                &ClientMessage::Related {
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
            byte_start,
            mode: if code {
                ws_protocol::SearchMode::Code
            } else {
                ws_protocol::SearchMode::Files
            },
            extensions,
            file_kinds,
            require_outcome: true,
            requested_semantic: true,
            max_results: limit,
            related_source: Some(source),
            related_categories: categories,
        })
    }

    pub async fn subscribe(&self) -> Result<StatusSubscription, Error> {
        Ok(StatusSubscription {
            operation: Some(self.open_lane(&self.0.subscriptions, None, false).await?),
        })
    }
}

#[cfg(windows)]
fn verify_server_process(
    pipe: &tokio::net::windows::named_pipe::NamedPipeClient,
    expected: NonZeroU32,
) -> Result<(), Error> {
    use std::os::windows::io::{AsRawHandle, RawHandle};

    #[link(name = "kernel32")]
    extern "system" {
        fn GetNamedPipeServerProcessId(pipe: RawHandle, process_id: *mut u32) -> i32;
    }

    let mut actual = 0;
    // Borrow the same live connection that will carry Hello; a probe is racy.
    if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle(), &mut actual) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if actual != expected.get() {
        return Err(Error::ServerIdentityMismatch);
    }
    Ok(())
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

fn reference_matches_kind(reference: &EntryReference, kind: ws_protocol::ResultKind) -> bool {
    use ws_protocol::{IndexedEntryId, ResultKind};
    match reference.indexed_id() {
        Some(IndexedEntryId::Code(_)) => kind == ResultKind::Code,
        Some(IndexedEntryId::File(_)) => matches!(
            kind,
            ResultKind::File
                | ResultKind::Folder
                | ResultKind::Image
                | ResultKind::Screenshot
                | ResultKind::Document
                | ResultKind::Audio
                | ResultKind::Video
        ),
        None => false,
    }
}

fn entry_matches(entry: &ResolvedEntry, expected: &EntryReference) -> bool {
    if entry.result_id != expected.result_id
        || !expected.is_valid()
        || !reference_matches_kind(expected, entry.kind)
        || entry.source_revision.trim().is_empty()
        || entry.source_revision.len() > 64 * 1024
        || entry.content_sha256.as_ref().is_some_and(|hash| {
            entry.index_revision.is_none() || !ws_protocol::valid_content_sha256(hash)
        })
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

fn preview_matches(
    preview: &ws_protocol::EntryPreview,
    expected: &ws_protocol::PreviewRequest,
) -> bool {
    use ws_protocol::{
        MatchLocation, PreviewState, MAX_PREVIEW_EXTRACTIONS, MAX_PREVIEW_TEXT_BYTES,
        MAX_PREVIEW_TEXT_PARTS,
    };
    let code = matches!(
        expected.reference.indexed_id(),
        Some(ws_protocol::IndexedEntryId::Code(_))
    );
    let known_kind = |kind: &str| {
        if code {
            kind == "code"
        } else {
            matches!(
                kind,
                "doc" | "pdf" | "ocr" | "image" | "audio" | "video" | "transcript"
            )
        }
    };
    if !entry_matches(&preview.entry, &expected.reference)
        || preview.parts.len() > MAX_PREVIEW_TEXT_PARTS
        || preview.extraction.len() > MAX_PREVIEW_EXTRACTIONS
    {
        return false;
    }
    let ready = preview.state == PreviewState::Ready;
    if ready == preview.parts.is_empty()
        || (ready && preview.entry.index_revision.is_none())
        || (preview.state == PreviewState::NotIndexed && preview.entry.index_revision.is_some())
        || (preview.state == PreviewState::LocationUnavailable && expected.location.is_none())
    {
        return false;
    }
    let mut bytes = 0usize;
    for part in &preview.parts {
        bytes = match bytes.checked_add(part.text.len()) {
            Some(value) if value <= MAX_PREVIEW_TEXT_BYTES => value,
            _ => return false,
        };
        let valid = if code {
            part.location.is_some() && ws_protocol::valid_code_preview_location(part.location)
        } else {
            ws_protocol::valid_preview_location(part.location)
        };
        if part.text.is_empty() || !known_kind(&part.kind) || !valid {
            return false;
        }
        if let Some(MatchLocation::Notebook { line, end_line, .. }) = part.location {
            if u64::from(end_line - line) >= part.text.lines().count().max(1) as u64 {
                return false;
            }
        }
        let matches = match (expected.location, part.location) {
            (None, _) => true,
            (Some(MatchLocation::Page { page: expected }), Some(MatchLocation::Page { page })) => {
                expected == page
            }
            (Some(MatchLocation::Line { line: expected }), Some(MatchLocation::Line { line })) => {
                expected >= line
                    && u64::from(expected - line) < part.text.lines().count().max(1) as u64
            }
            (
                Some(MatchLocation::Notebook {
                    cell_ordinal: expected_cell,
                    line: expected,
                    end_line: expected_end,
                }),
                Some(MatchLocation::Notebook {
                    cell_ordinal,
                    line,
                    end_line,
                }),
            ) => expected_cell == cell_ordinal && expected >= line && expected_end <= end_line,
            (
                Some(MatchLocation::Timestamp {
                    seconds: expected,
                    end_seconds: expected_end,
                }),
                Some(MatchLocation::Timestamp {
                    seconds,
                    end_seconds,
                }),
            ) => expected == seconds && expected_end == end_seconds,
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    preview.extraction.iter().all(|item|known_kind(&item.kind)
        && !matches!((item.indexed_units,item.source_units),(Some(indexed),Some(source)) if indexed>source))
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
        | ServerMessage::EnrichmentIngested { id, .. }
        | ServerMessage::IndexReset { id, .. }
        | ServerMessage::ResolvedEntry { id, .. }
        | ServerMessage::EntryPreview { id, .. }
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

    #[tokio::test]
    async fn reset_capability_and_acknowledgment_are_validated_without_retry() {
        for variant in 0..4 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut reply = ack();
                if variant != 0 {
                    if let ServerMessage::HelloAck { features, .. } = &mut reply {
                        features.push("index.reset".into());
                    }
                }
                send_server(&mut peer, &reply).await;
                if variant == 0 {
                    return;
                }
                let ClientMessage::ResetIndex { id, .. } = read_client(&mut peer).await else {
                    panic!("expected one reset");
                };
                if variant == 3 {
                    std::future::pending::<()>().await;
                }
                send_server(
                    &mut peer,
                    &ServerMessage::IndexReset {
                        id,
                        reset: ws_protocol::IndexReset {
                            index_id: if variant == 2 {
                                "a".repeat(32)
                            } else {
                                "c".repeat(32)
                            },
                            code_index_id: "d".repeat(32),
                        },
                    },
                )
                .await;
            });
            let request = ws_protocol::ResetRequest {
                expected_index_id: "a".repeat(32),
                expected_code_index_id: "b".repeat(32),
            };
            if variant == 3 {
                assert!(tokio::time::timeout(
                    Duration::from_millis(100),
                    client.reset_index(request)
                )
                .await
                .is_err());
                assert!(matches!(
                    client.set_config(EngineConfig::default()).await,
                    Err(Error::MutationOutcomeUnknown)
                ));
                task.abort();
            } else {
                let result = client.reset_index(request).await;
                match variant {
                    0 => assert!(matches!(result, Err(Error::Unsupported("index.reset")))),
                    1 => assert_eq!(result.unwrap().index_id, "c".repeat(32)),
                    _ => assert!(matches!(result, Err(Error::Protocol(_)))),
                }
                task.await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn enrichment_requires_capability_and_rejects_changed_admission_acknowledgments() {
        for variant in 0..8 {
            let (client, mut peer) = fixture();
            let (dispatched, dispatch_seen) = tokio::sync::oneshot::channel();
            let (queued, queue_seen) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut reply = ack();
                if variant != 0 {
                    if let ServerMessage::HelloAck { features, .. } = &mut reply {
                        features.push("entry.enrichment".into());
                    }
                }
                send_server(&mut peer, &reply).await;
                if variant == 0 {
                    return;
                }
                let ClientMessage::IngestEnrichment { id, request } = read_client(&mut peer).await
                else {
                    panic!("expected enrichment");
                };
                if variant == 3 {
                    std::future::pending::<()>().await;
                }
                if variant >= 4 {
                    if variant <= 6 {
                        dispatched.send(()).unwrap();
                        queue_seen.await.unwrap();
                    }
                    send_server(
                        &mut peer,
                        &ServerMessage::Error {
                            id: Some(id),
                            code: match variant {
                                4 => ws_protocol::ErrorCode::Cancelled,
                                5 => ws_protocol::ErrorCode::NotReady,
                                6 => ws_protocol::ErrorCode::Internal,
                                _ => ws_protocol::ErrorCode::InvalidRequest,
                            },
                            message: "synthetic peer error after request dispatch".into(),
                        },
                    )
                    .await;
                    return;
                }
                send_server(
                    &mut peer,
                    &ServerMessage::EnrichmentIngested {
                        id,
                        result: ws_protocol::EnrichmentIngested {
                            index_id: request.index_id,
                            reference: ws_protocol::EntryReference {
                                result_id: request.reference.result_id,
                                source_revision: Some("fresh-proof".into()),
                            },
                            content_revision: 1,
                            model_generation: request.model_generation,
                            grant_generation: if variant == 2 {
                                request.grant_generation + 1
                            } else {
                                request.grant_generation
                            },
                            duplicate: false,
                        },
                    },
                )
                .await;
            });
            let request = ws_protocol::EnrichmentRequest {
                index_id: "a".repeat(32),
                reference: ws_protocol::EntryReference {
                    result_id: "e1".into(),
                    source_revision: Some("current-proof".into()),
                },
                expected_sha256: "b".repeat(64),
                model_generation: 0,
                grant_generation: 1,
                kind: ws_protocol::EnrichmentKind::Ocr,
                parts: vec![ws_protocol::EnrichmentPart {
                    text: "consented fixture text".into(),
                    location: None,
                }],
            };
            if variant == 3 {
                assert!(tokio::time::timeout(
                    Duration::from_millis(100),
                    client.ingest_enrichment(request)
                )
                .await
                .is_err());
                assert!(matches!(
                    client.set_config(EngineConfig::default()).await,
                    Err(Error::MutationOutcomeUnknown)
                ));
                task.abort();
            } else {
                let result = if (4..=6).contains(&variant) {
                    let waiting = async {
                        dispatch_seen.await.unwrap();
                        let mut next = Box::pin(client.set_config(EngineConfig::default()));
                        std::future::poll_fn(|context| {
                            assert!(
                                std::future::Future::poll(next.as_mut(), context).is_pending(),
                                "control mutation bypassed the active ingestion"
                            );
                            std::task::Poll::Ready(())
                        })
                        .await;
                        queued.send(()).unwrap();
                        next.await
                    };
                    let (result, waiting) =
                        tokio::time::timeout(Duration::from_millis(300), async {
                            tokio::join!(client.ingest_enrichment(request), waiting)
                        })
                        .await
                        .expect("an already queued mutation was not fenced immediately");
                    assert!(matches!(waiting, Err(Error::MutationOutcomeUnknown)));
                    result
                } else {
                    client.ingest_enrichment(request).await
                };
                match variant {
                    0 => assert!(matches!(
                        result,
                        Err(Error::Unsupported("entry.enrichment"))
                    )),
                    1 => assert_eq!(result.unwrap().content_revision, 1),
                    4..=6 => {
                        assert!(
                            matches!(result, Err(Error::MutationOutcomeUnknown)),
                            "post-commit-capable error was treated as definitive: {result:?}"
                        );
                        assert!(matches!(
                            tokio::time::timeout(
                                Duration::from_millis(100),
                                client.set_config(EngineConfig::default())
                            )
                            .await,
                            Ok(Err(Error::MutationOutcomeUnknown))
                        ));
                    }
                    7 => assert!(matches!(
                        result,
                        Err(Error::Remote {
                            code: ws_protocol::ErrorCode::InvalidRequest,
                            ..
                        })
                    )),
                    _ => assert!(matches!(result, Err(Error::Protocol(_)))),
                }
                task.await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn process_identity_rejects_every_lane_before_sending_hello() {
        use tokio::io::AsyncReadExt;
        for lane in 0..4 {
            let (unbound, mut peer) = fixture();
            let different_process = if std::process::id() == 1 { 2 } else { 1 };
            let client = Client::for_process(
                unbound.0.pipe.clone(),
                NonZeroU32::new(different_process).unwrap(),
            )
            .unwrap()
            .clone();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let read = peer.read(&mut [0u8; 1]).await;
                assert!(
                    matches!(read, Ok(0))
                        || matches!(read, Err(ref e)
                    if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset)),
                    "untrusted peer received protocol bytes: {read:?}"
                );
            });
            let result = match lane {
                0 => client.info().await.map(|_| ()),
                1 => client.status().await.map(|_| ()),
                2 => client.set_config(EngineConfig::default()).await.map(|_| ()),
                _ => client.subscribe().await.map(|_| ()),
            };
            assert!(
                matches!(result, Err(Error::ServerIdentityMismatch)),
                "lane {lane}: {result:?}"
            );
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn process_identity_accepts_the_connected_server_and_rechecks_cloned_clients() {
        let (unbound, mut peer) = fixture();
        let pipe = unbound.0.pipe.clone();
        let client =
            Client::for_process(pipe.clone(), NonZeroU32::new(std::process::id()).unwrap())
                .unwrap();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let ClientMessage::GetStatus { id } = read_client(&mut peer).await else {
                panic!("expected status");
            };
            send_server(
                &mut peer,
                &ServerMessage::Status {
                    id: Some(id),
                    status: EngineStatus {
                        files_indexed: 17,
                        ..Default::default()
                    },
                },
            )
            .await;
        });
        assert_eq!(client.clone().status().await.unwrap().files_indexed, 17);
        task.await.unwrap();

        // A later connection must run the check again, before it can dispatch.
        let mut peer = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&pipe)
            .unwrap();
        let other_process = if std::process::id() == 1 { 2 } else { 1 };
        let wrong = Client::for_process(pipe, NonZeroU32::new(other_process).unwrap()).unwrap();
        let task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            peer.connect().await.unwrap();
            let read = peer.read(&mut [0u8; 1]).await;
            assert!(
                matches!(read, Ok(0))
                    || matches!(read, Err(ref e)
                if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset))
            );
        });
        assert!(matches!(
            wrong.clone().status().await,
            Err(Error::ServerIdentityMismatch)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_limits_bound_aggregate_bytes_before_the_frame_count_limit() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::Hello { .. }
            ));
            let mut response = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut response {
                features.push("search.limits".into());
            }
            send_server(&mut peer, &response).await;
            let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                panic!("missing search")
            };
            for n in 0..8 {
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        batch: vec![SearchResult {
                            id: format!("large-{n}"),
                            title: "x".repeat(9 * 1024 * 1024),
                            ..Default::default()
                        }],
                        done: n == 7,
                        outcome: None,
                    },
                )
                .await;
            }
        });
        let mut stream = client
            .search(SearchQuery {
                text: "fixture".into(),
                limit: 20,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut rejected = false;
        loop {
            match stream.next().await {
                Ok(Some(batch)) if !batch.done => (),
                Err(Error::Protocol(_)) => {
                    rejected = true;
                    break;
                }
                _ => break,
            }
        }
        assert!(
            rejected,
            "a few valid frames must not bypass the aggregate byte budget"
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_limits_count_ignored_fields_and_interleaved_push_bytes() {
        use tokio::io::AsyncWriteExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            let ClientMessage::Search { .. } = read_client(&mut peer).await else {
                panic!("missing search")
            };
            let mut push = serde_json::to_value(ServerMessage::Status {
                id: None,
                status: Default::default(),
            })
            .unwrap();
            push["ignored_padding"] = serde_json::Value::String("x".repeat(9 * 1024 * 1024));
            let bytes = serde_json::to_vec(&push).unwrap();
            for _ in 0..8 {
                peer.write_all(&(bytes.len() as u32).to_le_bytes())
                    .await
                    .unwrap();
                peer.write_all(&bytes).await.unwrap();
            }
        });
        let mut stream = client.search(SearchQuery::default()).await.unwrap();
        assert!(matches!(
            stream.next().await,
            Err(Error::Protocol("search stream exceeded its byte budget"))
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_limits_large_request_requires_capability_before_dispatch() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy peer must not receive an unsupported result budget"
            );
        });
        let result = client
            .search(SearchQuery {
                limit: 5000,
                ..Default::default()
            })
            .await;
        assert!(matches!(result, Err(Error::Unsupported("search.limits"))));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_limits_accept_ten_thousand_results_from_a_capable_peer() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::Hello { .. }
            ));
            let mut response = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut response {
                features.push("search.limits".into());
            }
            send_server(&mut peer, &response).await;
            let ClientMessage::Search { id, query } = read_client(&mut peer).await else {
                panic!("missing search")
            };
            assert_eq!(query.limit, 10_000);
            send_server(
                &mut peer,
                &ServerMessage::Results {
                    id,
                    batch: (0..10_000)
                        .map(|n| SearchResult {
                            id: format!("fixture-{n}"),
                            title: format!("fixture-{n}.txt"),
                            ..Default::default()
                        })
                        .collect(),
                    done: true,
                    outcome: None,
                },
            )
            .await;
        });
        let mut stream = client
            .search(SearchQuery {
                text: "fixture".into(),
                limit: 10_000,
                ..Default::default()
            })
            .await
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.batch.len(), 10_000);
        assert!(batch.done);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn search_limits_reject_a_capable_peer_exceeding_the_requested_total() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            assert!(matches!(
                read_client(&mut peer).await,
                ClientMessage::Hello { .. }
            ));
            let mut response = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut response {
                features.push("search.limits".into());
            }
            send_server(&mut peer, &response).await;
            let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                panic!("missing search")
            };
            send_server(
                &mut peer,
                &ServerMessage::Results {
                    id,
                    batch: vec![SearchResult::default(); 2],
                    done: true,
                    outcome: None,
                },
            )
            .await;
        });
        let mut stream = client
            .search(SearchQuery {
                text: "fixture".into(),
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(matches!(stream.next().await, Err(Error::Protocol(_))));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn root_policies_require_capability_before_any_configuration_mutation() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy peer must not receive unenforceable folder restrictions"
            );
        });
        let config = serde_json::from_value(serde_json::json!({"roots":["C:/source"],
            "root_policies":[{"root":"C:/source","include_hidden":false,"max_file_size_bytes":1024,"exclusions":[]}]})).unwrap();
        let result = client.set_indexing_config(config).await;
        assert!(
            matches!(result, Err(Error::Unsupported("index.root_policies"))),
            "{result:?}"
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn optional_npu_installation_requires_capability_before_mutation() {
        use tokio::io::AsyncReadExt;
        for import in [false, true] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                hello(&mut peer).await;
                assert_eq!(
                    peer.read(&mut [0]).await.unwrap(),
                    0,
                    "legacy engine must not silently ignore NPU opt-in"
                );
            });
            let result = if import {
                client.import_model_with_npu("C:/models".into(), true).await
            } else {
                client.start_model_download_with_npu(true).await
            };
            assert!(
                matches!(result, Err(Error::Unsupported("model.npu_bundle"))),
                "{result:?}"
            );
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn optional_npu_installation_preserves_explicit_flag_and_component_status() {
        for import in [false, true] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                assert!(matches!(
                    read_client(&mut peer).await,
                    ClientMessage::Hello { .. }
                ));
                let mut response = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut response {
                    features.push("model.npu_bundle".into());
                }
                send_server(&mut peer, &response).await;
                let request = read_client(&mut peer).await;
                let id = match request {
                    ClientMessage::ModelDownloadStart {
                        id,
                        include_npu: true,
                    } if !import => id,
                    ClientMessage::ModelImport {
                        id,
                        include_npu: true,
                        path,
                    } if import && path == "C:/models" => id,
                    other => panic!("unexpected NPU request: {other:?}"),
                };
                let status = serde_json::from_value(
                    serde_json::json!({"state":"ready","bytes":4,"total_bytes":4,
                    "npu_bundle":{"state":"downloading","bytes":10,"total_bytes":1085278625u64}}),
                )
                .unwrap();
                send_server(
                    &mut peer,
                    &ServerMessage::ModelStatus {
                        id: Some(id),
                        status,
                    },
                )
                .await;
            });
            let status = if import {
                client.import_model_with_npu("C:/models".into(), true).await
            } else {
                client.start_model_download_with_npu(true).await
            }
            .unwrap();
            assert_eq!(status.state, ws_protocol::ModelState::Ready);
            assert_eq!(status.total_bytes, 4);
            assert_eq!(status.npu_bundle.unwrap().total_bytes, 1_085_278_625);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn extension_filter_requires_its_capability_before_sending_a_query() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy peer must never receive a filtered query"
            );
        });
        let outcome = client
            .search(SearchQuery {
                extensions: vec!["pdf".into()],
                ..Default::default()
            })
            .await;
        assert!(matches!(outcome, Err(Error::Unsupported("search.filters"))));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn file_kind_selection_requires_its_capability_before_dispatch() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy peer cannot receive a kind-filtered query"
            );
        });
        let outcome = client
            .search(SearchQuery {
                file_kinds: vec![ws_protocol::FileKind::Pdf],
                ..Default::default()
            })
            .await;
        assert!(matches!(
            outcome,
            Err(Error::Unsupported("search.file_kinds"))
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn file_kind_selection_validates_every_reply_against_source_classification() {
        for (path, kind, selected, allowed) in [
            (
                Some("C:/Docs/book.XLSX"),
                ws_protocol::ResultKind::Document,
                ws_protocol::FileKind::Spreadsheet,
                true,
            ),
            (
                Some("C:/Docs/book.pdf"),
                ws_protocol::ResultKind::Document,
                ws_protocol::FileKind::Spreadsheet,
                false,
            ),
            (
                Some("C:/Docs/folder.pdf"),
                ws_protocol::ResultKind::Folder,
                ws_protocol::FileKind::Pdf,
                false,
            ),
            (
                Some("C:/Docs/folder.pdf"),
                ws_protocol::ResultKind::Folder,
                ws_protocol::FileKind::Folder,
                true,
            ),
            (
                Some("C:/Docs/.json"),
                ws_protocol::ResultKind::File,
                ws_protocol::FileKind::Unknown,
                true,
            ),
            (
                None,
                ws_protocol::ResultKind::File,
                ws_protocol::FileKind::Unknown,
                false,
            ),
            (
                Some("C:/Docs/calc.pdf"),
                ws_protocol::ResultKind::Calculator,
                ws_protocol::FileKind::Pdf,
                false,
            ),
        ] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut response = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut response {
                    features.push("search.file_kinds".into());
                }
                send_server(&mut peer, &response).await;
                let ClientMessage::Search { id, query } = read_client(&mut peer).await else {
                    panic!("expected search");
                };
                assert_eq!(query.file_kinds, vec![selected]);
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        batch: vec![SearchResult {
                            path: path.map(str::to_owned),
                            kind,
                            ..Default::default()
                        }],
                        done: true,
                        outcome: None,
                    },
                )
                .await;
            });
            let mut stream = client
                .search(SearchQuery {
                    file_kinds: vec![selected],
                    ..Default::default()
                })
                .await
                .unwrap();
            let result = stream.next().await;
            assert_eq!(result.is_ok(), allowed, "{path:?}/{selected:?}");
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn file_kind_selection_rejects_excessive_lists_before_connecting() {
        let (client, _peer) = fixture();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            client.search(SearchQuery {
                file_kinds: vec![ws_protocol::FileKind::Pdf; 14],
                ..Default::default()
            }),
        )
        .await;
        assert!(matches!(result, Ok(Err(Error::InvalidInput(_)))));
    }

    #[tokio::test]
    async fn extension_filter_rejects_a_peer_returning_unselected_results() {
        for (path, kind, allowed) in [
            (
                Some("C:/Docs/image.png".to_string()),
                ws_protocol::ResultKind::Image,
                false,
            ),
            (None, ws_protocol::ResultKind::File, false),
            (
                Some("C:/Docs/source.PDF".to_string()),
                ws_protocol::ResultKind::Document,
                true,
            ),
            (
                Some("C:/Docs/folder.pdf".to_string()),
                ws_protocol::ResultKind::Folder,
                false,
            ),
        ] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut response = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut response {
                    features.push("search.filters".into());
                }
                send_server(&mut peer, &response).await;
                let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                    panic!("expected filtered search");
                };
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        outcome: None,
                        id,
                        batch: vec![SearchResult {
                            id: "e1".into(),
                            path,
                            kind,
                            ..Default::default()
                        }],
                        done: true,
                    },
                )
                .await;
            });
            let mut stream = client
                .search(SearchQuery {
                    extensions: vec!["pdf".into()],
                    ..Default::default()
                })
                .await
                .unwrap();
            let result = stream.next().await;
            if allowed {
                assert!(matches!(result,Ok(Some(batch)) if batch.done && batch.batch.len()==1));
            } else {
                assert!(matches!(result, Err(Error::Protocol(_))));
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn extension_filter_rejects_invalid_input_before_connecting() {
        let (client, _peer) = fixture();
        for extensions in [
            vec!["../pdf".into()],
            vec!["pdf".into(); 65],
            vec!["a".repeat(33)],
        ] {
            assert!(matches!(
                client
                    .search(SearchQuery {
                        extensions,
                        ..Default::default()
                    })
                    .await,
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[tokio::test]
    async fn search_outcomes_are_required_when_advertised_and_validated_before_delivery() {
        use ws_protocol::{
            SearchDisposition as D, SearchOutcome as O, SemanticIssue as I, SemanticOutcome as S,
            SemanticPhase as P,
        };
        for case in 0..14 {
            let requested = matches!(case, 3 | 7 | 9 | 10 | 11);
            let outcome = match case {
                0 => None,
                1 | 8 => Some(O::complete(S::DISABLED)),
                2 => Some(O::interrupted(D::Complete)),
                3 => Some(O::complete(S {
                    phase: P::Ready,
                    reason: Some(I::EmbeddingFailed),
                })),
                4 | 9 => Some(O::complete(S::READY)),
                5 | 11 => Some(O::interrupted(D::Superseded)),
                6 => Some(O {
                    disposition: D::Canceled,
                    semantic: Some(S::DISABLED),
                }),
                7 => Some(O::complete(S {
                    phase: P::Degraded,
                    reason: None,
                })),
                10 => Some(O::complete(S::degraded(I::ModelUnavailable))),
                12 => Some(O::interrupted(D::Canceled)),
                _ => Some(O::interrupted(D::Failed)),
            };
            let expected = outcome;
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut response = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut response {
                    features.extend(["search.outcomes".into(), "search.semantic".into()]);
                }
                send_server(&mut peer, &response).await;
                let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                    panic!("expected search");
                };
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        done: case != 1,
                        outcome,
                        batch: if case == 5 {
                            vec![SearchResult::default()]
                        } else {
                            vec![]
                        },
                    },
                )
                .await;
            });
            let mut stream = client
                .search(SearchQuery {
                    include_semantic: requested,
                    ..Default::default()
                })
                .await
                .unwrap();
            let result = stream.next().await;
            if case < 8 {
                assert!(matches!(result, Err(Error::Protocol(_))), "case{case}");
            } else {
                assert_eq!(result.unwrap().unwrap().outcome, expected, "case{case}");
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn related_rejects_unbound_source_before_opening_a_connection() {
        let (client, _peer) = fixture();
        let result = client
            .related(ws_protocol::RelatedQuery {
                source: ws_protocol::EntryReference {
                    result_id: "e1".into(),
                    source_revision: None,
                },
                limit: 20,
                categories: vec![],
                extensions: vec![],
                file_kinds: vec![],
            })
            .await;
        assert!(matches!(result, Err(Error::InvalidInput(_))));
    }

    #[tokio::test]
    async fn related_requires_each_capability_before_writing_an_unknown_or_downgraded_request() {
        use tokio::io::AsyncReadExt;
        for missing in [
            "search.related",
            "search.semantic",
            "search.outcomes",
            "code",
            "search.filters",
            "search.file_kinds",
        ] {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut hello = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut hello {
                    features.extend(
                        [
                            "search.related",
                            "search.semantic",
                            "search.outcomes",
                            "code",
                            "search.filters",
                            "search.file_kinds",
                        ]
                        .into_iter()
                        .filter(|feature| *feature != missing)
                        .map(str::to_owned),
                    );
                }
                send_server(&mut peer, &hello).await;
                assert_eq!(
                    peer.read(&mut [0]).await.unwrap(),
                    0,
                    "unsupported Related must never be written"
                );
            });
            let result = client
                .related(ws_protocol::RelatedQuery {
                    source: EntryReference {
                        result_id: "c1".into(),
                        source_revision: Some("proof".into()),
                    },
                    limit: 1,
                    categories: vec![],
                    extensions: vec!["rs".into()],
                    file_kinds: vec![ws_protocol::FileKind::Source],
                })
                .await;
            assert!(matches!(result,Err(Error::Unsupported(feature)) if feature==missing));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn related_stream_uses_distinct_operation_and_rejects_seed_or_ordinary_fallback_replies()
    {
        use ws_protocol::{EntryReference, RelatedQuery, SearchOutcome, SemanticOutcome};
        for case in 0..9 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut response = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut response {
                    features.extend([
                        "search.related".into(),
                        "search.outcomes".into(),
                        "search.semantic".into(),
                        "search.filters".into(),
                    ]);
                }
                send_server(&mut peer, &response).await;
                let ClientMessage::Related { id, query } = read_client(&mut peer).await else {
                    panic!("expected distinct Related operation");
                };
                assert_eq!(query.source.result_id, "e1");
                assert!(query.source.is_bound());
                let outcome = if case == 2 {
                    SemanticOutcome::DISABLED
                } else {
                    SemanticOutcome::READY
                };
                let target = if case == 1 {
                    "e1"
                } else if case == 4 {
                    "c2"
                } else {
                    "e2"
                };
                let hit = SearchResult {
                    id: "s2".into(),
                    kind: ws_protocol::ResultKind::Document,
                    path: Some("C:/Docs/report.txt".into()),
                    entry_reference: (case != 3).then_some(EntryReference {
                        result_id: target.into(),
                        source_revision: (case != 5).then_some("target-proof".into()),
                    }),
                    ..Default::default()
                };
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        id,
                        done: true,
                        outcome: Some(SearchOutcome::complete(outcome)),
                        batch: if case == 8 {
                            vec![hit.clone(), hit]
                        } else {
                            vec![hit]
                        },
                    },
                )
                .await;
            });
            let mut stream = client
                .related(RelatedQuery {
                    source: EntryReference {
                        result_id: "e1".into(),
                        source_revision: Some("source-proof".into()),
                    },
                    limit: 1,
                    categories: if case == 6 {
                        vec![ws_protocol::ResultKind::Audio]
                    } else {
                        vec![]
                    },
                    extensions: if case == 7 {
                        vec!["pdf".into()]
                    } else {
                        vec![]
                    },
                    file_kinds: vec![],
                })
                .await
                .expect("new peer must support Related");
            let result = stream.next().await;
            if case == 0 {
                assert_eq!(result.unwrap().unwrap().batch.len(), 1);
            } else {
                assert!(
                    matches!(result, Err(Error::Protocol(_))),
                    "case{case} must be rejected before delivery"
                );
            }
            task.await.unwrap();
        }
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
                    npu_bundle: None,
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
                    | (
                        5,
                        ClientMessage::ModelDownloadStart {
                            id,
                            include_npu: false,
                        },
                    )
                    | (6, ClientMessage::ModelDownloadCancel { id })
                    | (8, ClientMessage::ModelDelete { id }) => ServerMessage::ModelStatus {
                        id: Some(id),
                        status: model,
                    },
                    (
                        7,
                        ClientMessage::ModelImport {
                            id,
                            path,
                            include_npu: false,
                        },
                    ) => {
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
            content_sha256: None,
        }
    }

    #[tokio::test]
    async fn preview_legacy_capability_closes_before_writing_an_unknown_request() {
        use tokio::io::AsyncReadExt;
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            let _ = read_client(&mut peer).await;
            let mut hello = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut hello {
                features.clear();
            }
            send_server(&mut peer, &hello).await;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy engine must receive no preview frame"
            );
        });
        assert!(matches!(
            client
                .preview_text(ws_protocol::PreviewRequest {
                    reference: EntryReference {
                        result_id: "e42".into(),
                        source_revision: None
                    },
                    location: None,
                })
                .await,
            Err(Error::Unsupported("entry.preview.text"))
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn preview_invalid_locations_are_rejected_before_connecting() {
        use ws_protocol::{MatchLocation, PreviewRequest};
        let (client, peer) = fixture();
        drop(peer);
        for location in [
            MatchLocation::Page { page: 0 },
            MatchLocation::Page { page: 5001 },
            MatchLocation::Line { line: 0 },
            MatchLocation::Timestamp {
                seconds: f64::NAN,
                end_seconds: 1.0,
            },
            MatchLocation::Timestamp {
                seconds: 2.0,
                end_seconds: 1.0,
            },
        ] {
            assert!(matches!(
                client
                    .preview_text(PreviewRequest {
                        reference: EntryReference {
                            result_id: "e42".into(),
                            source_revision: None
                        },
                        location: Some(location)
                    })
                    .await,
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[tokio::test]
    async fn preview_checks_identity_location_utf8_budget_and_partial_state_before_delivery() {
        use ws_protocol::{
            EntryPreview, MatchLocation, PreviewExtraction, PreviewRequest, PreviewState,
            PreviewTextPart,
        };
        for case in 0..14 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut hello = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut hello {
                    features.push("entry.preview.text".into());
                }
                send_server(&mut peer, &hello).await;
                let ClientMessage::PreviewEntry { id, request } = read_client(&mut peer).await
                else {
                    panic!("preview request missing");
                };
                assert_eq!(request.reference.result_id, "e42");
                assert_eq!(request.location, Some(MatchLocation::Page { page: 8 }));
                let mut preview = EntryPreview {
                    entry: resolved(),
                    state: PreviewState::Ready,
                    parts: vec![PreviewTextPart {
                        kind: "pdf".into(),
                        text: "Actual eighth page".into(),
                        location: Some(MatchLocation::Page { page: 8 }),
                    }],
                    truncated: false,
                    extraction: vec![PreviewExtraction {
                        kind: "pdf".into(),
                        complete_within_limit: true,
                        source_units: None,
                        indexed_units: None,
                        capped: None,
                    }],
                };
                match case {
                    1 => preview.entry.result_id = "e43".into(),
                    2 => preview.entry.source_revision = "stale".into(),
                    3 => preview.entry.path = r"C:\outside\report.txt".into(),
                    4 => preview.parts = vec![preview.parts[0].clone(); 65],
                    5 => preview.parts[0].text = "🌿".repeat(17000),
                    6 => preview.parts[0].location = Some(MatchLocation::Page { page: 9 }),
                    7 => preview.parts.clear(),
                    8 => preview.state = PreviewState::NoCachedText,
                    9 => preview.extraction = vec![preview.extraction[0].clone(); 9],
                    10 => preview.extraction[0].kind = "unrecognized".into(),
                    11 => preview.entry.index_revision = None,
                    12 => preview.parts[0].text.clear(),
                    13 => preview.parts[0].kind = "C:/must-not-open".into(),
                    _ => (),
                }
                send_server(&mut peer, &ServerMessage::EntryPreview { id, preview }).await;
            });
            let result = client
                .preview_text(PreviewRequest {
                    reference: EntryReference {
                        result_id: "e42".into(),
                        source_revision: Some("proof".into()),
                    },
                    location: Some(MatchLocation::Page { page: 8 }),
                })
                .await;
            if case == 0 {
                assert_eq!(result.unwrap().parts[0].text, "Actual eighth page");
            } else {
                assert!(
                    matches!(result, Err(Error::Protocol(_))),
                    "case {case}: {result:?}"
                );
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn entry_resolution_checks_identity_proof_and_lexical_root_before_delivery() {
        for case in 0..15 {
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
                    10 => entry.content_sha256 = Some("a".repeat(63)),
                    11 => {
                        entry.content_sha256 = Some("a".repeat(64));
                        entry.index_revision = None;
                    }
                    12 => entry.content_sha256 = Some("A".repeat(64)),
                    13 => entry.kind = ws_protocol::ResultKind::Code,
                    14 => entry.source_revision = "🌿".repeat(17000),
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
    async fn preview_text_trailing_terminator_does_not_cover_the_following_line() {
        use ws_protocol::{
            EntryPreview, MatchLocation, PreviewRequest, PreviewState, PreviewTextPart,
        };
        let request = PreviewRequest {
            reference: EntryReference {
                result_id: "e42".into(),
                source_revision: Some("proof".into()),
            },
            location: Some(MatchLocation::Line { line: 3 }),
        };
        let preview = EntryPreview {
            entry: resolved(),
            state: PreviewState::Ready,
            parts: vec![PreviewTextPart {
                kind: "doc".into(),
                text: "first\nsecond\n".into(),
                location: Some(MatchLocation::Line { line: 1 }),
            }],
            truncated: false,
            extraction: Vec::new(),
        };
        assert!(
            !preview_matches(&preview, &request),
            "a reply cannot claim unreturned line3 after its trailing newline"
        );
    }

    #[tokio::test]
    async fn code_preview_client_requires_its_own_capability_before_request() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            let _ = read_client(&mut peer).await;
            let mut info = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut info {
                features.push("entry.preview.text".into());
            }
            send_server(&mut peer, &info).await;
            use tokio::io::AsyncReadExt;
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "legacy peer cannot receive a code preview frame"
            );
        });
        let result = client
            .preview_text(ws_protocol::PreviewRequest {
                reference: EntryReference {
                    result_id: "c42".into(),
                    source_revision: Some("proof".into()),
                },
                location: Some(ws_protocol::MatchLocation::Line { line: 1 }),
            })
            .await;
        if !matches!(result, Err(Error::Unsupported("entry.preview.code"))) {
            task.abort();
            panic!("{result:?}");
        }
        task.await.unwrap();
    }

    #[tokio::test]
    async fn code_preview_client_rejects_invalid_code_and_ordinary_notebook_inputs() {
        use ws_protocol::{MatchLocation, PreviewRequest};
        let (client, _peer) = fixture();
        for (id, location) in [
            ("c42", MatchLocation::Page { page: 1 }),
            (
                "c42",
                MatchLocation::Timestamp {
                    seconds: 0.0,
                    end_seconds: 1.0,
                },
            ),
            (
                "c42",
                MatchLocation::Notebook {
                    cell_ordinal: 0,
                    line: 0,
                    end_line: 1,
                },
            ),
            (
                "c42",
                MatchLocation::Notebook {
                    cell_ordinal: 0,
                    line: 3,
                    end_line: 2,
                },
            ),
            (
                "e42",
                MatchLocation::Notebook {
                    cell_ordinal: 0,
                    line: 1,
                    end_line: 1,
                },
            ),
        ] {
            assert!(matches!(
                client
                    .preview_text(PreviewRequest {
                        reference: EntryReference {
                            result_id: id.into(),
                            source_revision: None
                        },
                        location: Some(location)
                    })
                    .await,
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[tokio::test]
    async fn code_preview_client_validates_actual_cell_ranges_kinds_hashes_and_budgets() {
        use ws_protocol::{
            EntryPreview, MatchLocation, PreviewExtraction, PreviewRequest, PreviewState,
            PreviewTextPart, ResultKind,
        };
        for case in 0..12 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut info = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut info {
                    features.push("entry.preview.code".into());
                }
                send_server(&mut peer, &info).await;
                let ClientMessage::PreviewEntry { id, .. } = read_client(&mut peer).await else {
                    panic!("code preview missing");
                };
                let mut entry = resolved();
                entry.result_id = "c42".into();
                entry.kind = ResultKind::Code;
                entry.content_sha256 = Some("a".repeat(64));
                let mut preview = EntryPreview {
                    entry,
                    state: PreviewState::Ready,
                    parts: vec![PreviewTextPart {
                        kind: "code".into(),
                        text: "first\nsecond".into(),
                        location: Some(if case == 0 {
                            MatchLocation::Line { line: 1 }
                        } else {
                            MatchLocation::Notebook {
                                cell_ordinal: 2,
                                line: 1,
                                end_line: 2,
                            }
                        }),
                    }],
                    truncated: false,
                    extraction: vec![PreviewExtraction {
                        kind: "code".into(),
                        complete_within_limit: true,
                        source_units: None,
                        indexed_units: None,
                        capped: None,
                    }],
                };
                match case {
                    2 => preview.entry.result_id = "e42".into(),
                    3 => {
                        preview.parts[0].location = Some(MatchLocation::Notebook {
                            cell_ordinal: 3,
                            line: 1,
                            end_line: 2,
                        })
                    }
                    4 => {
                        preview.parts[0].location = Some(MatchLocation::Notebook {
                            cell_ordinal: 2,
                            line: 1,
                            end_line: 3,
                        })
                    }
                    5 => preview.parts[0].location = Some(MatchLocation::Line { line: 1 }),
                    6 => preview.parts[0].kind = "doc".into(),
                    7 => preview.extraction[0].kind = "doc".into(),
                    8 => preview.parts[0].text = "🌿".repeat(17000),
                    9 => preview.entry.index_revision = None,
                    10 => preview.entry.content_sha256 = Some("A".repeat(64)),
                    11 => preview.parts[0].text = "first\n".into(),
                    _ => {}
                }
                send_server(&mut peer, &ServerMessage::EntryPreview { id, preview }).await;
            });
            let result = client
                .preview_text(PreviewRequest {
                    reference: EntryReference {
                        result_id: "c42".into(),
                        source_revision: Some("proof".into()),
                    },
                    location: Some(if case == 0 {
                        MatchLocation::Line { line: 2 }
                    } else {
                        MatchLocation::Notebook {
                            cell_ordinal: 2,
                            line: 2,
                            end_line: 2,
                        }
                    }),
                })
                .await;
            let good = if case <= 1 {
                result.is_ok()
            } else {
                matches!(result, Err(Error::Protocol(_)))
            };
            if !good {
                task.abort();
                panic!("case {case}: {result:?}");
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn bound_references_reject_noncanonical_inputs_before_connecting() {
        let (client, _peer) = fixture();
        for reference in [
            "e0",
            "e01",
            "e+1",
            "c-1",
            "s42",
            "code:42:1",
            "e9223372036854775808",
        ]
        .into_iter()
        .map(|result_id| EntryReference {
            result_id: result_id.into(),
            source_revision: None,
        })
        .chain([
            EntryReference {
                result_id: "c42".into(),
                source_revision: Some(" ".into()),
            },
            EntryReference {
                result_id: "e42".into(),
                source_revision: Some("🌿".repeat(17000)),
            },
        ]) {
            let result =
                tokio::time::timeout(Duration::from_millis(300), client.resolve_entry(reference))
                    .await;
            assert!(
                matches!(result, Ok(Err(Error::InvalidInput(_)))),
                "{result:?}"
            );
        }
    }

    #[tokio::test]
    async fn bound_references_require_code_resolution_capability_before_request() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            hello(&mut peer).await;
            use tokio::io::AsyncReadExt;
            let mut header = [0; 4];
            if peer.read(&mut header).await.unwrap() == 0 {
                return false;
            }
            let length = u32::from_le_bytes(header);
            let mut bytes = vec![0; length as usize];
            peer.read_exact(&mut bytes).await.unwrap();
            let ClientMessage::ResolveEntry { id, .. } = serde_json::from_slice(&bytes).unwrap()
            else {
                panic!("resolve");
            };
            let mut entry = resolved();
            entry.result_id = "c42".into();
            entry.kind = ws_protocol::ResultKind::Code;
            send_server(&mut peer, &ServerMessage::ResolvedEntry { id, entry }).await;
            true
        });
        let result = client
            .resolve_entry(EntryReference {
                result_id: "c42".into(),
                source_revision: None,
            })
            .await;
        let sent = task.await.unwrap();
        assert!(
            matches!(result, Err(Error::Unsupported("entry.resolve.code"))),
            "{result:?}"
        );
        assert!(!sent, "legacy peer received an unsupported code request");
    }

    #[tokio::test]
    async fn bound_references_resolve_code_with_actual_hash_and_current_proof() {
        let (client, mut peer) = fixture();
        let task = tokio::spawn(async move {
            peer.connect().await.unwrap();
            let _ = read_client(&mut peer).await;
            let mut info = ack();
            if let ServerMessage::HelloAck { features, .. } = &mut info {
                features.push("entry.resolve.code".into());
            }
            send_server(&mut peer, &info).await;
            let ClientMessage::ResolveEntry { id, reference } = read_client(&mut peer).await else {
                panic!("resolve");
            };
            assert_eq!(reference.result_id, "c42");
            let mut entry = resolved();
            entry.result_id = "c42".into();
            entry.kind = ws_protocol::ResultKind::Code;
            entry.content_sha256 = Some("a".repeat(64));
            send_server(&mut peer, &ServerMessage::ResolvedEntry { id, entry }).await;
        });
        let entry = client
            .resolve_entry(EntryReference {
                result_id: "c42".into(),
                source_revision: Some("proof".into()),
            })
            .await
            .unwrap();
        assert_eq!(entry.content_sha256, Some("a".repeat(64)));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn bound_references_validate_stream_domains_and_keep_legacy_results() {
        use ws_protocol::{ResultKind, SearchMode};
        for case in 0..11 {
            let (client, mut peer) = fixture();
            let task = tokio::spawn(async move {
                peer.connect().await.unwrap();
                let _ = read_client(&mut peer).await;
                let mut info = ack();
                if let ServerMessage::HelloAck { features, .. } = &mut info {
                    features.extend(["code".into(), "search.entry_references".into()]);
                }
                send_server(&mut peer, &info).await;
                let ClientMessage::Search { id, .. } = read_client(&mut peer).await else {
                    panic!("search");
                };
                let mut hit = SearchResult {
                    id: "query-specific-hit".into(),
                    kind: ResultKind::File,
                    entry_reference: Some(EntryReference {
                        result_id: "e42".into(),
                        source_revision: Some("proof".into()),
                    }),
                    ..Default::default()
                };
                match case {
                    0 => hit.entry_reference = None,
                    2 => hit.entry_reference.as_mut().unwrap().result_id = "e042".into(),
                    3 => hit.entry_reference.as_mut().unwrap().source_revision = None,
                    4 => hit.entry_reference.as_mut().unwrap().source_revision = Some(" ".into()),
                    5 => {
                        hit.entry_reference.as_mut().unwrap().source_revision =
                            Some("🌿".repeat(17000))
                    }
                    6 => hit.entry_reference.as_mut().unwrap().result_id = "c42".into(),
                    7 => hit.kind = ResultKind::Application,
                    8 | 9 => {
                        hit.kind = ResultKind::Code;
                        hit.entry_reference.as_mut().unwrap().result_id = "c42".into();
                    }
                    10 => hit.kind = ResultKind::Code,
                    _ => {}
                }
                send_server(
                    &mut peer,
                    &ServerMessage::Results {
                        outcome: None,
                        id,
                        batch: vec![hit],
                        done: true,
                    },
                )
                .await;
            });
            let mut stream = client
                .search(SearchQuery {
                    mode: if case >= 9 {
                        SearchMode::Code
                    } else {
                        SearchMode::Auto
                    },
                    ..Default::default()
                })
                .await
                .unwrap();
            let result = stream.next().await;
            if matches!(case, 0 | 1 | 9) {
                assert_eq!(result.unwrap().unwrap().batch[0].id, "query-specific-hit");
            } else {
                assert!(
                    matches!(result, Err(Error::Protocol(_))),
                    "case {case}: {result:?}"
                );
                assert!(stream.next().await.unwrap().is_none());
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
                    outcome: None,
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
                    outcome: None,
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
                        outcome: None,
                        id: id + 1,
                        batch: vec![],
                        done: true,
                    },
                    1 => ServerMessage::Pong { id },
                    _ => ServerMessage::Results {
                        outcome: None,
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
                        outcome: None,
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
                    limit: 10_001,
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
                        outcome: None,
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
