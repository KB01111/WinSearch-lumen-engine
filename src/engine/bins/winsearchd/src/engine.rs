//! Engine service: shared state behind the IPC server, and the search
//! pipeline that turns a `SearchQuery` into streamed result batches.
//!
//! Search sources, in result order: calculator (if the query is arithmetic),
//! Windows settings pages, then index hits (applications and files/folders)
//! ranked by `ws-index`. The semantic stage (P3.6) appends embedding-similarity
//! hits in later batches when `include_semantic` is set, the policy allows it,
//! and the model is ready — exact results always stream first, unchanged.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{info, warn};
use ws_index::model::{
    kind_for_path_and_ext, EngineConfig as StoredEngineConfig, HitKind, PreferredBackend,
    SearchRequest, SEGMENT_KIND_NAME,
};
use ws_index::{evaluate, search_settings, EntryRow};
use ws_platform::paths;
use ws_platform::pipe::PipeStream;
use ws_protocol::{
    write_message, EngineConfig, EngineRunState, EngineStatus, ErrorCode, FrameError,
    MatchLocation, RequestId, ResultKind, SearchDisposition, SearchMode, SearchOutcome,
    SearchQuery, SearchResult, SemanticIssue, SemanticOutcome, ServerMessage,
};

use crate::embed_client::EmbedSupervisor;
use crate::indexer::Indexer;
use crate::model::ModelService;

#[cfg(test)]
#[path = "ipc_writer_tests.rs"]
mod ipc_writer_tests;

pub use ws_index::retrieval::SEMANTIC_TOP_K;
#[cfg(test)]
use ws_index::retrieval::{
    exact_token_boost, hybrid_base_score, recency_boost, MIN_SEMANTIC_COSINE,
};
use ws_index::retrieval::{hybrid_score, semantic_admitted};
/// Query-embedding LRU capacity: one entry per distinct query text, so
/// repeated/paused-then-resumed queries never re-embed.
pub const QUERY_CACHE_CAP: usize = 64;
/// Bounded query-embed timeout for the interactive path. A single query text
/// embeds in seconds; a hung worker fails the semantic stage (exact results
/// already streamed) instead of stalling the connection.
pub const QUERY_EMBED_TIMEOUT: Duration = Duration::from_secs(30);

/// Query-text → embedding override for tests (`Ok(vec)` fakes the worker).
/// Production leaves this unset and embeds through the supervisor.
pub type QueryEmbedHook = Arc<dyn Fn(&str) -> Result<Vec<f32>, String> + Send + Sync>;

/// A connected client that can receive pushed messages.
#[derive(Clone)]
pub struct ClientHandle {
    id: u64,
    stream: Arc<Mutex<PipeStream>>,
}

impl ClientHandle {
    pub fn new(id: u64, stream: Arc<Mutex<PipeStream>>) -> Self {
        Self { id, stream }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn send(&self, msg: &ServerMessage) -> Result<(), FrameError> {
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        write_message(&mut *guard, msg)
    }
}

pub struct EngineService {
    reference_incarnation: Option<windows::core::GUID>,
    code: Mutex<Option<Arc<crate::code::CodeService>>>,
    power: Mutex<Option<crate::policy::PowerNotifications>>,
    status: Mutex<EngineStatus>,
    clients: Mutex<Vec<ClientHandle>>,
    /// Cancellation flags for in-flight search requests, keyed by `(client_id, request_id)`.
    cancels: Mutex<HashMap<(u64, RequestId), Arc<AtomicBool>>>,
    preview_jobs: Arc<std::sync::atomic::AtomicUsize>,
    related_jobs: std::sync::atomic::AtomicUsize,
    search_jobs: std::sync::atomic::AtomicUsize,
    next_client_id: std::sync::atomic::AtomicU64,
    pub(crate) indexer: Arc<Indexer>,
    extractors: Arc<crate::extract::ExtractorRegistry>,
    models: Arc<ModelService>,
    /// Embed worker supervisor (lazy: spawns on first embed request).
    /// `winsearchd` links no native inference code; all embeddings flow
    /// through this supervisor over the internal pipe. Shared (`Arc`) with
    /// the indexer's embed scheduler thread (P3.5).
    embed: Arc<EmbedSupervisor>,
    /// Query-embedding LRU (text → embedding, capacity [`QUERY_CACHE_CAP`]).
    /// Hit on the query path before touching the worker.
    query_cache: Mutex<QueryEmbeddingCache>,
    /// Test seam: when set, the semantic stage embeds through this instead
    /// of the supervisor (deterministic fake vectors, no worker process).
    query_embed_hook: Mutex<Option<QueryEmbedHook>>,
    /// Test seam: when set, the semantic stage reads model readiness from
    /// here instead of the supervisor cache (a fresh test supervisor has no
    /// worker to handshake with). Production never sets it.
    model_ready_override: Mutex<Option<bool>>,
}

pub type SharedEngine = Arc<EngineService>;

#[derive(Clone, Copy)]
struct SearchRevision {
    source: u64,
    code: Option<u64>,
    model: Option<i64>,
}

#[path = "search_budget.rs"]
mod search_budget;
use search_budget::SearchUsage;

struct SentSearchBatch {
    ids: HashSet<i64>,
    done: bool,
    usage: SearchUsage,
}

struct SearchDelivery<'a> {
    query: &'a SearchQuery,
    revision: SearchRevision,
    cancel: &'a Arc<AtomicBool>,
    sources: &'a HashMap<String, ResultSource>,
    related_source: Option<&'a ws_protocol::EntryReference>,
}

struct PreviewWork {
    engine: SharedEngine,
    client_id: u64,
    id: RequestId,
    cancel: Arc<AtomicBool>,
}
impl Drop for PreviewWork {
    fn drop(&mut self) {
        self.engine
            .end_request_for_client(self.client_id, self.id, &self.cancel);
        self.engine.preview_jobs.fetch_sub(1, Ordering::AcqRel);
    }
}

struct RelatedWork {
    engine: SharedEngine,
    client_id: u64,
    id: RequestId,
    cancel: Arc<AtomicBool>,
}
impl Drop for RelatedWork {
    fn drop(&mut self) {
        self.engine
            .end_request_for_client(self.client_id, self.id, &self.cancel);
        self.engine.related_jobs.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ResultSource {
    entry_id: i64,
    segment_id: Option<i64>,
    content_only: bool,
}

struct SearchWork {
    engine: SharedEngine,
    client_id: u64,
    id: RequestId,
    cancel: Arc<AtomicBool>,
}

impl Drop for SearchWork {
    fn drop(&mut self) {
        self.engine
            .end_request_for_client(self.client_id, self.id, &self.cancel);
        self.engine.search_jobs.fetch_sub(1, Ordering::AcqRel);
    }
}

impl EngineService {
    pub fn ingest_enrichment(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        request: ws_protocol::EnrichmentRequest,
    ) {
        if !request.is_valid() {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::InvalidRequest,
                message: "invalid enrichment request".into(),
            });
            return;
        }
        if self
            .preview_jobs
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < 2).then_some(count + 1)
            })
            .is_err()
        {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "source resources are busy".into(),
            });
            return;
        }
        let cancel = self.begin_request_for_client(client.id(), id);
        let work = PreviewWork {
            engine: self.clone(),
            client_id: client.id(),
            id,
            cancel,
        };
        let peer = client.clone();
        if std::thread::Builder::new()
            .name("winsearch-enrichment".into())
            .spawn(move || {
                let Some(_ticket) = work.engine.indexer.work_gate.enter() else {
                    let _ = peer.send(&ServerMessage::Error {
                        id: Some(id),
                        code: ErrorCode::NotReady,
                        message: "index reset in progress".into(),
                    });
                    return;
                };
                let epoch = work.engine.indexer.policy_epoch();
                let result = crate::enrichment::ingest(&work.engine, &request, &work.cancel);
                let mut stream = peer.stream.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(reply) = work.engine.enrichment_response_for_delivery(
                    peer.id(),
                    id,
                    &work.cancel,
                    epoch,
                    result,
                ) {
                    let _ = write_message(&mut *stream, &reply);
                }
            })
            .is_err()
        {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "enrichment worker could not start".into(),
            });
        }
    }
    pub(crate) fn enrichment_response_for_delivery(
        &self,
        client_id: u64,
        id: RequestId,
        cancel: &Arc<AtomicBool>,
        epoch: u64,
        result: Result<ws_protocol::EnrichmentIngested, ErrorCode>,
    ) -> Option<ServerMessage> {
        let current = || {
            self.cancels
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(client_id, id))
                .is_some_and(|token| Arc::ptr_eq(token, cancel))
        };
        if !current() {
            return None;
        }
        let result = result.and_then(|result| {
            self.resolve_entry_snapshot(&result.reference)?;
            if self.indexer.store_lock().model_generation().ok() != Some(result.model_generation) {
                return Err(ErrorCode::NotReady);
            }
            Ok(result)
        });
        if !current() {
            return None;
        }
        // Resolve can wait on filesystem/store work. Fence again immediately
        // before delivery, including cancellation arriving during that wait.
        let result = if cancel.load(Ordering::Acquire) {
            Err(ErrorCode::Cancelled)
        } else if self.indexer.policy_epoch() != epoch {
            Err(ErrorCode::NotReady)
        } else {
            result
        };
        Some(match result {
            Ok(result) => ServerMessage::EnrichmentIngested { id, result },
            Err(code) => ServerMessage::Error {
                id: Some(id),
                code,
                message: "enrichment source is unavailable or changed".into(),
            },
        })
    }
    fn resume_after_reset_gate(&self) {
        self.indexer.work_gate.reopen();
        // Watcher notifications were deliberately excluded while draining.
        // Reconcile even an aborted reset, so no event can disappear silently.
        if let Err(error) = self.indexer.enqueue_initial_scan() {
            warn!(%error, "resuming reset scan failed");
        }
        if let Some(code) = self.code_service() {
            if let Err(error) = code.reconcile_after_gate() {
                warn!(%error, "resuming code scan failed");
            }
        }
        self.indexer.notify_work();
    }
    pub fn reset_index(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        request: ws_protocol::ResetRequest,
    ) {
        if !request.is_valid() {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::InvalidRequest,
                message: "invalid reset namespaces".into(),
            });
            return;
        }
        if !self.indexer.work_gate.close() {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "index reset unavailable".into(),
            });
            return;
        }
        for cancel in self
            .cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            cancel.store(true, Ordering::Release);
        }
        let engine = self.clone();
        let client = client.clone();
        let failure_client = client.clone();
        if std::thread::Builder::new()
            .name("winsearch-reset".into())
            .spawn(move || {
                let reply = engine.reset_index_inner(&request);
                let message = match reply {
                    Ok(reset) => ServerMessage::IndexReset { id, reset },
                    Err(code) => ServerMessage::Error {
                        id: Some(id),
                        code,
                        message: "index reset failed; restart the owned engine if unavailable"
                            .into(),
                    },
                };
                let _ = client.send(&message);
            })
            .is_err()
        {
            self.resume_after_reset_gate();
            let _ = failure_client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "reset worker could not start".into(),
            });
        }
    }

    fn reset_index_inner(
        &self,
        request: &ws_protocol::ResetRequest,
    ) -> Result<ws_protocol::IndexReset, ErrorCode> {
        if !self.indexer.work_gate.drain(Duration::from_secs(10)) {
            warn!(
                active_work = self.indexer.work_gate.active_count(),
                "index reset drain timed out before durable intent"
            );
            self.resume_after_reset_gate();
            return Err(ErrorCode::NotReady);
        }
        let Some(code) = self.code_service() else {
            self.resume_after_reset_gate();
            return Err(ErrorCode::NotReady);
        };
        let mut ordinary = self.indexer.store_lock();
        let mut code_store = code.store.lock().unwrap_or_else(|p| p.into_inner());
        if ordinary.database_id() != request.expected_index_id
            || code_store.database_id() != request.expected_code_index_id
        {
            drop(code_store);
            drop(ordinary);
            self.resume_after_reset_gate();
            return Err(ErrorCode::InvalidRequest);
        }
        let reset = crate::reset::execute(&mut ordinary, &mut code_store).map_err(|error| {
            warn!(%error, "durable reset failed; admission remains closed until restart recovery");
            self.status.lock().unwrap_or_else(|p| p.into_inner()).state = EngineRunState::Error;
            ErrorCode::Internal
        })?;
        drop(code_store);
        drop(ordinary);
        code.reset_runtime();
        self.indexer.reset_completed();
        *self.query_cache.lock().unwrap_or_else(|p| p.into_inner()) =
            QueryEmbeddingCache::new(QUERY_CACHE_CAP);
        self.status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .index_id = Some(reset.index_id.clone());
        self.resume_after_reset_gate();
        // A rescan failure does not make the completed reset ambiguous. Durable
        // roots survive and are reconciled on the next configuration/startup.
        Ok(reset)
    }
    #[cfg(test)]
    pub fn new(indexer: Arc<Indexer>) -> SharedEngine {
        Self::new_with_model_dir(indexer, paths::model_dir())
    }

    /// Production wiring used by `main.rs`: explicit embed supervisor.
    pub fn new_with_embed(indexer: Arc<Indexer>, embed: EmbedSupervisor) -> SharedEngine {
        Self::new_inner(indexer, paths::model_dir(), embed)
    }

    /// Test hook: same engine with an isolated model directory.
    #[cfg(test)]
    pub fn new_with_model_dir(
        indexer: Arc<Indexer>,
        model_dir: std::path::PathBuf,
    ) -> SharedEngine {
        Self::new_inner(indexer, model_dir, EmbedSupervisor::new())
    }

    fn new_inner(
        indexer: Arc<Indexer>,
        model_dir: std::path::PathBuf,
        embed: EmbedSupervisor,
    ) -> SharedEngine {
        let models = ModelService::new(model_dir, Arc::clone(&indexer));
        Self::new_with_models(indexer, models, embed)
    }

    pub fn new_with_models(
        indexer: Arc<Indexer>,
        models: Arc<ModelService>,
        embed: EmbedSupervisor,
    ) -> SharedEngine {
        if let Ok(cfg) = indexer.get_engine_config() {
            embed.set_preferred_backend(cfg.preferred_backend.as_str());
        }
        let index_id = indexer.store_lock().database_id().to_owned();
        Arc::new(Self {
            reference_incarnation: unsafe { windows::Win32::System::Com::CoCreateGuid() }.ok(),
            code: Mutex::new(None),
            power: Mutex::new(None),
            status: Mutex::new(EngineStatus {
                index_id: Some(index_id),
                state: EngineRunState::Idle,
                current_activity: String::new(),
                ..Default::default()
            }),
            clients: Mutex::new(Vec::new()),
            cancels: Mutex::new(HashMap::new()),
            preview_jobs: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            related_jobs: std::sync::atomic::AtomicUsize::new(0),
            search_jobs: std::sync::atomic::AtomicUsize::new(0),
            next_client_id: std::sync::atomic::AtomicU64::new(1),
            indexer,
            extractors: Arc::new(crate::extract::ExtractorRegistry::production()),
            models,
            embed: Arc::new(embed),
            query_cache: Mutex::new(QueryEmbeddingCache::new(QUERY_CACHE_CAP)),
            query_embed_hook: Mutex::new(None),
            model_ready_override: Mutex::new(None),
        })
    }

    /// Test seam: embed query texts through `hook` instead of the worker.
    #[cfg(test)]
    pub fn set_query_embed_hook(&self, hook: QueryEmbedHook) {
        *self
            .query_embed_hook
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(hook);
    }

    /// Test seam: force the semantic stage's model-readiness check.
    #[cfg(test)]
    pub fn set_model_ready_override(&self, ready: Option<bool>) {
        *self
            .model_ready_override
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = ready;
    }

    pub fn model_service(&self) -> &Arc<ModelService> {
        &self.models
    }

    pub fn embed_supervisor(&self) -> Arc<EmbedSupervisor> {
        Arc::clone(&self.embed)
    }

    pub(crate) fn extraction_registry(&self) -> Arc<crate::extract::ExtractorRegistry> {
        Arc::clone(&self.extractors)
    }

    pub fn start_code_scheduler(&self) -> anyhow::Result<()> {
        *self.power.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(crate::policy::PowerNotifications::register(&self.indexer)?);
        let code = crate::code::CodeService::new(
            ws_code::CodeStore::open(&paths::code_db_path())?,
            Arc::clone(&self.indexer),
            Arc::clone(&self.embed),
            Arc::clone(&self.models),
            crate::extract_client::ExtractClient::production(),
        );
        self.indexer.set_code_service(Arc::downgrade(&code));
        code.start()?;
        *self.code.lock().unwrap_or_else(|p| p.into_inner()) = Some(code);
        Ok(())
    }

    fn code_service(&self) -> Option<Arc<crate::code::CodeService>> {
        self.code.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn status(&self) -> EngineStatus {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        // `model_loaded` is live from the embed supervisor's cache (real
        // weights resident, or stub vectors flowing in test setups). Reading
        // the cache never spawns a worker.
        status.model_loaded = self.embed.model_loaded();
        status.model_generation = self.indexer.store_lock().model_generation().ok();
        status.embedding = Some(self.embed.diagnostics());
        status.code_index_id = self.code_service().map(|code| {
            code.store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .database_id()
                .to_owned()
        });
        // Indexing counters are live; the stored snapshot only carries the
        // broadcast state.
        if let Ok(s) = self.indexer.status() {
            status.files_discovered = s.files_discovered;
            status.files_indexed = s.files_indexed;
            status.queue_depth = s.queue_depth;
            status.index_size_bytes = s.stats.index_size_bytes;
            status.segments_indexed = s.segments_indexed;
            if status.state == EngineRunState::Idle {
                if s.indexing {
                    status.state = EngineRunState::Indexing;
                    status.current_activity = "Indexing".into();
                } else if s.embedding {
                    status.state = EngineRunState::Indexing;
                    status.current_activity = "Embedding names…".into();
                }
            }
        }
        if let Some(reason) = self.indexer.indexing_pause_reason() {
            status.state = EngineRunState::Paused;
            status.current_activity = reason;
        }
        status
    }

    pub fn set_status(&self, update: impl FnOnce(&mut EngineStatus)) {
        let mut guard = self.status.lock().unwrap_or_else(|p| p.into_inner());
        update(&mut guard);
        drop(guard);
        let snapshot = self.status();
        self.broadcast(&ServerMessage::Status {
            id: None,
            status: snapshot,
        });
    }

    pub fn register_client(&self, stream: Arc<Mutex<PipeStream>>) -> ClientHandle {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let handle = ClientHandle::new(id, stream);
        self.clients
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(handle.clone());
        handle
    }

    pub fn unregister_client(&self, client: &ClientHandle) {
        self.clients
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|c| c.id != client.id);
        self.cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(owner, _), flag| {
                if *owner == client.id {
                    flag.store(true, Ordering::Release);
                    false
                } else {
                    true
                }
            });
    }

    fn broadcast(&self, msg: &ServerMessage) {
        self.push_to_all(msg);
    }

    /// Sends one message to every connected client (used by the model
    /// service's progress pushes as well as status broadcasts).
    pub fn push_to_all(&self, msg: &ServerMessage) {
        let clients = self
            .clients
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        for client in &clients {
            if let Err(e) = client.send(msg) {
                info!(client = client.id, error = %e, "dropping unreachable client");
                self.unregister_client(client);
            }
        }
    }

    pub fn begin_request_for_client(&self, client_id: u64, id: RequestId) -> Arc<AtomicBool> {
        let flag = Arc::new(AtomicBool::new(false));
        if let Some(previous) = self
            .cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((client_id, id), flag.clone())
        {
            previous.store(true, Ordering::Release);
        }
        flag
    }

    pub fn end_request_for_client(&self, client_id: u64, id: RequestId, token: &Arc<AtomicBool>) {
        let mut requests = self.cancels.lock().unwrap_or_else(|p| p.into_inner());
        if requests
            .get(&(client_id, id))
            .is_some_and(|current| Arc::ptr_eq(current, token))
        {
            requests.remove(&(client_id, id));
        }
    }

    #[cfg(test)]
    pub fn cancel(&self, id: RequestId) {
        let guard = self.cancels.lock().unwrap_or_else(|p| p.into_inner());
        for ((_, req_id), flag) in guard.iter() {
            if *req_id == id {
                flag.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn cancel_for_client(&self, client_id: u64, id: RequestId) {
        if let Some(flag) = self
            .cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(client_id, id))
        {
            flag.store(true, Ordering::Relaxed);
        }
    }

    // ---- model management (P3.3) ----

    pub fn resolve_entry(
        &self,
        client: &ClientHandle,
        id: RequestId,
        reference: ws_protocol::EntryReference,
    ) {
        let Some(_work) = self.indexer.work_gate.enter() else {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "index reset in progress".into(),
            });
            return;
        };
        let mut stream = client.stream.lock().unwrap_or_else(|p| p.into_inner());
        let response = match self.resolve_entry_snapshot(&reference) {
            Ok(entry) => ServerMessage::ResolvedEntry { id, entry },
            Err(code) => ServerMessage::Error {
                id: Some(id),
                code,
                message: "entry is unavailable or changed".into(),
            },
        };
        let _ = write_message(&mut *stream, &response);
    }

    pub fn preview_entry(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        request: ws_protocol::PreviewRequest,
    ) {
        if self
            .preview_jobs
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |jobs| {
                (jobs < 2).then(|| jobs + 1)
            })
            .is_err()
        {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::NotReady,
                message: "preview resources are busy".into(),
            });
            return;
        }
        let cancel = self.begin_request_for_client(client.id(), id);
        let work = PreviewWork {
            engine: Arc::clone(self),
            client_id: client.id(),
            id,
            cancel,
        };
        let client = client.clone();
        let failure_client = client.clone();
        if std::thread::Builder::new()
            .name("winsearch-preview".into())
            .spawn(move || {
                let Some(_ticket) = work.engine.indexer.work_gate.enter() else {
                    let _ = client.send(&ServerMessage::Error {
                        id: Some(id),
                        code: ErrorCode::NotReady,
                        message: "index reset in progress".into(),
                    });
                    return;
                };
                let response = match work.engine.preview_snapshot(&request, &work.cancel) {
                    Ok(preview) => ServerMessage::EntryPreview { id, preview },
                    Err(code) => ServerMessage::Error {
                        id: Some(id),
                        code,
                        message: "preview is unavailable or changed".into(),
                    },
                };
                // Waiting for another writer must not preserve an obsolete content decision.
                let mut stream = client.stream.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(response) = work.engine.preview_response_for_delivery(
                    client.id(),
                    id,
                    &work.cancel,
                    response,
                ) {
                    let _ = write_message(&mut *stream, &response);
                }
            })
            .is_err()
        {
            let _ = failure_client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::Internal,
                message: "preview could not start".into(),
            });
        }
    }

    fn preview_response_for_delivery(
        &self,
        client_id: u64,
        id: RequestId,
        cancel: &Arc<AtomicBool>,
        response: ServerMessage,
    ) -> Option<ServerMessage> {
        let current = || {
            self.cancels
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(client_id, id))
                .is_some_and(|token| Arc::ptr_eq(token, cancel))
        };
        if !current() {
            return None;
        }
        let response = if let ServerMessage::EntryPreview { preview, .. } = &response {
            let reference = ws_protocol::EntryReference {
                result_id: preview.entry.result_id.clone(),
                source_revision: Some(preview.entry.source_revision.clone()),
            };
            if !self
                .resolve_entry_snapshot(&reference)
                .is_ok_and(|entry| entry == preview.entry)
            {
                ServerMessage::Error {
                    id: Some(id),
                    code: ErrorCode::NotReady,
                    message: "preview is unavailable or changed".into(),
                }
            } else {
                response
            }
        } else {
            response
        };
        if !current() {
            return None;
        }
        Some(if cancel.load(Ordering::Acquire) {
            ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::Cancelled,
                message: "preview was cancelled".into(),
            }
        } else {
            response
        })
    }

    fn preview_snapshot(
        &self,
        request: &ws_protocol::PreviewRequest,
        cancel: &Arc<AtomicBool>,
    ) -> Result<ws_protocol::EntryPreview, ErrorCode> {
        use ws_protocol::{EntryPreview, EntryReference, PreviewState};
        if cancel.load(Ordering::Acquire) {
            return Err(ErrorCode::Cancelled);
        }
        let indexed = request
            .reference
            .indexed_id()
            .ok_or(ErrorCode::InvalidRequest)?;
        let valid = match indexed {
            ws_protocol::IndexedEntryId::File(_) => {
                ws_protocol::valid_preview_location(request.location)
            }
            ws_protocol::IndexedEntryId::Code(_) => {
                ws_protocol::valid_code_preview_location(request.location)
            }
        };
        if !valid {
            return Err(ErrorCode::InvalidRequest);
        }
        let entry = self.resolve_entry_snapshot(&request.reference)?;
        if cancel.load(Ordering::Acquire) {
            return Err(ErrorCode::Cancelled);
        }
        let (parts, truncated, extraction) = if entry.index_revision.is_some() {
            match indexed {
                ws_protocol::IndexedEntryId::File(id) => {
                    let store = self.indexer.store_lock();
                    let (parts, truncated) = store
                        .preview_text(id, request.location)
                        .map_err(|_| ErrorCode::Internal)?;
                    let extraction = store
                        .preview_extraction(id)
                        .map_err(|_| ErrorCode::Internal)?;
                    (parts, truncated, extraction)
                }
                ws_protocol::IndexedEntryId::Code(id) => {
                    let (parts, truncated) = self
                        .code_service()
                        .ok_or(ErrorCode::NotReady)?
                        .preview_text(id, &entry, request.location)
                        .map_err(|_| ErrorCode::Internal)?;
                    (
                        parts,
                        truncated,
                        vec![ws_protocol::PreviewExtraction {
                            kind: "code".into(),
                            complete_within_limit: true,
                            source_units: None,
                            indexed_units: None,
                            capped: None,
                        }],
                    )
                }
            }
        } else {
            (Vec::new(), false, Vec::new())
        };
        if cancel.load(Ordering::Acquire) {
            return Err(ErrorCode::Cancelled);
        }
        let current = self.resolve_entry_snapshot(&EntryReference {
            result_id: entry.result_id.clone(),
            source_revision: Some(entry.source_revision.clone()),
        })?;
        if current != entry {
            return Err(ErrorCode::NotReady);
        }
        if cancel.load(Ordering::Acquire) {
            return Err(ErrorCode::Cancelled);
        }
        let state = if !parts.is_empty() {
            PreviewState::Ready
        } else if entry.index_revision.is_none() {
            PreviewState::NotIndexed
        } else if request.location.is_some() {
            PreviewState::LocationUnavailable
        } else {
            PreviewState::NoCachedText
        };
        Ok(EntryPreview {
            entry,
            state,
            parts,
            truncated,
            extraction,
        })
    }

    pub(crate) fn resolve_entry_snapshot(
        &self,
        reference: &ws_protocol::EntryReference,
    ) -> Result<ws_protocol::ResolvedEntry, ErrorCode> {
        let _work = self.indexer.work_gate.enter().ok_or(ErrorCode::NotReady)?;
        let invalid = ErrorCode::InvalidRequest;
        if !reference.is_valid() {
            return Err(invalid);
        }
        let incarnation = self.reference_incarnation.ok_or(ErrorCode::NotReady)?;
        let entry_id = match reference.indexed_id().ok_or(invalid)? {
            ws_protocol::IndexedEntryId::File(id) => id,
            ws_protocol::IndexedEntryId::Code(id) => {
                return self
                    .code_service()
                    .ok_or(ErrorCode::NotReady)?
                    .resolve_metadata(id, reference, incarnation)
            }
        };
        let policy_epoch = self.indexer.policy_epoch();
        let (entry, root_path, revision, generation, indexed, identity, content_sha256) = {
            let store = self.indexer.store_lock();
            if !store
                .entry_visible(entry_id)
                .map_err(|_| ErrorCode::Internal)?
            {
                return Err(ErrorCode::NotReady);
            }
            let entry = store
                .get_entry(entry_id)
                .map_err(|_| ErrorCode::Internal)?
                .ok_or(ErrorCode::NotReady)?;
            if entry.is_app || entry.missing {
                return Err(ErrorCode::NotReady);
            }
            let root = store
                .roots()
                .map_err(|_| ErrorCode::Internal)?
                .into_iter()
                .find(|r| r.id == entry.root_id && r.enabled)
                .ok_or(ErrorCode::NotReady)?;
            let revision = store
                .content_revision(entry_id)
                .map_err(|_| ErrorCode::Internal)?;
            let generation = store.model_generation().map_err(|_| ErrorCode::Internal)?;
            let indexed = store
                .entry_content_allowed(entry_id)
                .map_err(|_| ErrorCode::Internal)?;
            let identity = store
                .entry_identity(entry_id)
                .map_err(|_| ErrorCode::Internal)?
                .ok_or(ErrorCode::NotReady)?;
            let content_sha256 = store
                .content_sha256(entry_id)
                .map_err(|_| ErrorCode::Internal)?;
            (
                entry,
                root.path,
                revision,
                generation,
                indexed,
                identity,
                content_sha256,
            )
        };
        let path = std::path::Path::new(&entry.path);
        let path_allowed = || {
            if indexed {
                self.indexer.content_path_allowed(path)
            } else {
                self.indexer.name_path_allowed(path) && crate::policy::metadata_safe(path, true)
            }
        };
        if !path_allowed() || ws_platform::identity::file_identity(path) != Some(identity) {
            return Err(ErrorCode::NotReady);
        }
        let snapshot = crate::extract::source_snapshot(path);
        if snapshot.is_empty() {
            return Err(ErrorCode::NotReady);
        }
        let metadata = std::fs::metadata(path).map_err(|_| ErrorCode::NotReady)?;
        let proof =
            format!("{incarnation:?}:{policy_epoch}:{generation}:{revision}:{indexed}:{snapshot}");
        if proof.len() > 64 * 1024 {
            return Err(ErrorCode::NotReady);
        }
        if reference
            .source_revision
            .as_ref()
            .is_some_and(|expected| expected != &proof)
        {
            return Err(ErrorCode::NotReady);
        }
        let modified = metadata
            .modified()
            .ok()
            .and_then(ws_platform::identity::unix_seconds);
        {
            let store = self.indexer.store_lock();
            if self.indexer.policy_epoch() != policy_epoch
                || store.model_generation().map_err(|_| ErrorCode::Internal)? != generation
                || store
                    .content_revision(entry_id)
                    .map_err(|_| ErrorCode::Internal)?
                    != revision
                || store
                    .content_sha256(entry_id)
                    .map_err(|_| ErrorCode::Internal)?
                    != content_sha256
                || store
                    .entry_identity(entry_id)
                    .map_err(|_| ErrorCode::Internal)?
                    != Some(identity)
                || !store
                    .entry_visible(entry_id)
                    .map_err(|_| ErrorCode::Internal)?
                || !path_allowed()
                || ws_platform::identity::file_identity(path) != Some(identity)
                || crate::extract::source_snapshot(path) != snapshot
            {
                return Err(ErrorCode::NotReady);
            }
        }
        Ok(ws_protocol::ResolvedEntry {
            result_id: reference.result_id.clone(),
            root_path,
            path: entry.path.clone(),
            name: entry.name.clone(),
            kind: to_result_kind(entry_hit_kind(&entry)),
            size_bytes: metadata.len(),
            modified_unix_seconds: modified,
            index_revision: (indexed && revision > 0).then_some(revision),
            source_revision: proof,
            content_sha256: (indexed && revision > 0)
                .then_some(content_sha256)
                .flatten(),
        })
    }

    fn answer_model_status(&self, client: &ClientHandle, id: RequestId) {
        let _ = client.send(&ServerMessage::ModelStatus {
            id: Some(id),
            status: self.models.status(),
        });
    }

    pub fn model_download_start(&self, client: &ClientHandle, id: RequestId, include_npu: bool) {
        match self.models.try_start_download_with_npu(include_npu) {
            Ok(_) => {
                self.embed.reset_model();
                self.answer_model_status(client, id);
            }
            Err(message) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ErrorCode::InvalidRequest,
                    message,
                });
            }
        }
    }

    pub fn model_download_cancel(&self, client: &ClientHandle, id: RequestId) {
        self.models.cancel_download();
        self.answer_model_status(client, id);
    }

    pub fn model_import(
        &self,
        client: &ClientHandle,
        id: RequestId,
        path: String,
        include_npu: bool,
    ) {
        match self
            .models
            .start_import_with_npu(std::path::PathBuf::from(path), include_npu)
        {
            Ok(_) => {
                self.embed.reset_model();
                self.answer_model_status(client, id);
            }
            Err(e) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ws_protocol::ErrorCode::InvalidRequest,
                    message: e,
                });
            }
        }
    }

    pub fn model_delete(&self, client: &ClientHandle, id: RequestId) {
        self.embed.reset_model();
        match self.models.delete() {
            Ok(status) => {
                let _ = client.send(&ServerMessage::ModelStatus {
                    id: Some(id),
                    status,
                });
            }
            Err(e) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ws_protocol::ErrorCode::InvalidRequest,
                    message: e,
                });
            }
        }
    }

    pub fn model_status(&self, client: &ClientHandle, id: RequestId) {
        self.answer_model_status(client, id);
    }

    pub fn model_lifecycle_update(&self, message: &ServerMessage) {
        if let ServerMessage::ModelStatus { status, .. } = message {
            if status.state != ws_modelmgr::ModelState::Downloading
                && !status.npu_bundle.as_ref().is_some_and(|component| {
                    component.state == ws_modelmgr::ModelState::Downloading
                })
            {
                self.embed.reset_model();
            }
        }
        self.push_to_all(message);
    }

    // ---- engine config (P3.6) ----

    fn wire_config(stored: &StoredEngineConfig) -> EngineConfig {
        EngineConfig {
            semantic_enabled: stored.semantic_enabled,
            preferred_backend: stored.preferred_backend.as_str().to_string(),
        }
    }

    pub fn get_indexing_config(&self, client: &ClientHandle, id: RequestId) {
        match self.indexer.get_indexing_config() {
            Ok(config) => {
                let _ = client.send(&ServerMessage::IndexingConfig {
                    id: Some(id),
                    config,
                });
            }
            Err(e) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ErrorCode::Internal,
                    message: e.to_string(),
                });
            }
        }
    }
    pub fn set_indexing_config(
        &self,
        client: &ClientHandle,
        id: RequestId,
        config: ws_protocol::IndexingConfig,
    ) {
        match self.indexer.set_indexing_config(&config) {
            Ok(()) => {
                self.get_indexing_config(client, id);
                if let Ok(config) = self.indexer.get_indexing_config() {
                    self.broadcast(&ServerMessage::IndexingConfig { id: None, config });
                }
            }
            Err(e) => {
                let code = if matches!(e, ws_index::StoreError::InvalidConfig(_)) {
                    ErrorCode::InvalidRequest
                } else {
                    ErrorCode::Internal
                };
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code,
                    message: e.to_string(),
                });
            }
        }
    }

    pub fn get_config(&self, client: &ClientHandle, id: RequestId) {
        match self.indexer.get_engine_config() {
            Ok(cfg) => {
                let _ = client.send(&ServerMessage::Config {
                    id: Some(id),
                    config: Self::wire_config(&cfg),
                });
            }
            Err(e) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ErrorCode::Internal,
                    message: e.to_string(),
                });
            }
        }
    }

    pub fn set_config(&self, client: &ClientHandle, id: RequestId, config: EngineConfig) {
        let Some(backend) = PreferredBackend::parse(&config.preferred_backend) else {
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::InvalidRequest,
                message: format!(
                    "preferred_backend must be auto|npu|cuda|vulkan|cpu, got {:?}",
                    config.preferred_backend
                ),
            });
            return;
        };
        let stored = StoredEngineConfig {
            semantic_enabled: config.semantic_enabled,
            preferred_backend: backend,
        };
        match self.indexer.set_engine_config(&stored) {
            Ok(()) => {
                self.embed
                    .set_preferred_backend(stored.preferred_backend.as_str());
                let _ = client.send(&ServerMessage::Config {
                    id: Some(id),
                    config,
                });
            }
            Err(e) => {
                let _ = client.send(&ServerMessage::Error {
                    id: Some(id),
                    code: ErrorCode::Internal,
                    message: e.to_string(),
                });
            }
        }
    }

    // ---- semantic query stage (P3.6) ----

    /// Whether the embedding model counts as ready for the semantic stage:
    /// either the supervisor's cached handshake flags report a loaded/stub
    /// worker, or the on-disk model files are ready (`ModelState::Ready`, so a
    /// cold-start query lazy-spawns the worker even when the background embed
    /// queue is empty), unless a test override is set.
    fn model_ready(&self) -> bool {
        if let Some(forced) = *self
            .model_ready_override
            .lock()
            .unwrap_or_else(|p| p.into_inner())
        {
            return forced;
        }
        self.embed.model_loaded() || self.models.status().state == ws_modelmgr::ModelState::Ready
    }

    /// The full semantic gate: requested by the client, allowed by policy,
    /// servable by the model, with code opt-in and category filters applied.
    #[cfg(test)]
    fn semantic_allowed(&self, query: &SearchQuery) -> bool {
        self.semantic_unavailable(query).is_none()
    }

    fn semantic_unavailable(&self, query: &SearchQuery) -> Option<SemanticOutcome> {
        if !query.include_semantic
            || query.text.trim().is_empty()
            || (query.mode == SearchMode::Code
                && !query.categories.is_empty()
                && !query.categories.contains(&ResultKind::Code))
            || (query.mode != SearchMode::Code
                && !query.categories.is_empty()
                && hit_kinds(&query.categories).is_empty())
        {
            return Some(SemanticOutcome::DISABLED);
        }
        let config = match self.indexer.get_engine_config() {
            Ok(config) => config,
            Err(_) => return Some(SemanticOutcome::degraded(SemanticIssue::RetrievalFailed)),
        };
        if !config.semantic_enabled
            || (query.mode == SearchMode::Code
                && !self
                    .indexer
                    .get_indexing_config()
                    .is_ok_and(|config| config.code_enabled))
        {
            return Some(SemanticOutcome::DISABLED);
        }
        (!self.model_ready()).then_some(SemanticOutcome::degraded(SemanticIssue::ModelUnavailable))
    }

    /// Embeds one query text, consulting the LRU first. Failures (worker
    /// down, timeout, empty reply) are returned as strings and make the
    /// semantic stage yield no hits — exact results already streamed.
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, String> {
        let input = ws_modelmgr::recipe::search_query(text);
        self.embed_query_input(input)
    }

    fn embed_query_input(&self, input: String) -> Result<Vec<f32>, String> {
        self.embed_query_inputs(&[input])?
            .pop()
            .ok_or_else(|| "missing query vector".into())
    }

    fn embed_query_inputs(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, String> {
        let generation = self
            .indexer
            .store_lock()
            .model_generation()
            .map_err(|e| e.to_string())?;
        let keys = inputs
            .iter()
            .map(|input| format!("{generation}:{input}"))
            .collect::<Vec<_>>();
        let mut vectors = {
            let mut cache = self.query_cache.lock().unwrap_or_else(|p| p.into_inner());
            keys.iter().map(|key| cache.get(key)).collect::<Vec<_>>()
        };
        let missing = vectors
            .iter()
            .enumerate()
            .filter_map(|(index, vector)| vector.is_none().then_some(index))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let pending = missing
                .iter()
                .map(|index| inputs[*index].clone())
                .collect::<Vec<_>>();
            let hook = self
                .query_embed_hook
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let fresh = if let Some(hook) = hook {
                pending
                    .iter()
                    .map(|input| hook(input))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                self.embed
                    .embed_texts_bounded(&pending, QUERY_EMBED_TIMEOUT)
                    .map_err(|e| e.to_string())?
            };
            if fresh.len() != missing.len() {
                return Err("query embedding count mismatch".into());
            }
            for (index, vector) in missing.into_iter().zip(fresh) {
                vectors[index] = Some(vector);
            }
        }
        if self
            .indexer
            .store_lock()
            .model_generation()
            .map_err(|e| e.to_string())?
            != generation
        {
            return Err("model generation changed during query embedding".into());
        }
        let vectors = vectors
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or("missing query vector")?;
        let dimension = vectors.first().map(Vec::len).unwrap_or(0);
        if dimension == 0
            || vectors.iter().any(|vector| {
                vector.len() != dimension || ws_index::vectors::normalize_vector(vector).1 == 0.0
            })
        {
            return Err("invalid query embedding".into());
        }
        let mut cache = self.query_cache.lock().unwrap_or_else(|p| p.into_inner());
        for (key, vector) in keys.into_iter().zip(&vectors) {
            cache.put(key, vector.clone());
        }
        Ok(vectors)
    }

    /// Computes the appended semantic hits for one query (no IPC). Empty when
    /// the gate is closed, the embed fails, or nothing passes the cosine
    /// threshold after duplicate suppression.
    #[cfg(test)]
    fn plan_semantic(
        &self,
        query: &SearchQuery,
        exact_entry_ids: &HashSet<i64>,
    ) -> Vec<SearchResult> {
        self.plan_semantic_with_sources(query, exact_entry_ids, &mut HashMap::new())
    }

    #[cfg(test)]
    fn plan_semantic_with_sources(
        &self,
        query: &SearchQuery,
        exact: &HashSet<i64>,
        sources: &mut HashMap<String, ResultSource>,
    ) -> Vec<SearchResult> {
        self.plan_semantic_outcome(query, exact, sources).0
    }

    fn plan_semantic_checked(
        &self,
        query: &SearchQuery,
        exact_entry_ids: &HashSet<i64>,
        sources: &mut HashMap<String, ResultSource>,
    ) -> Result<Vec<SearchResult>, SemanticIssue> {
        self.plan_semantic_excluding_checked(query, exact_entry_ids, sources, None)
    }

    fn plan_semantic_excluding_checked(
        &self,
        query: &SearchQuery,
        exact_entry_ids: &HashSet<i64>,
        sources: &mut HashMap<String, ResultSource>,
        exclude: Option<ws_protocol::IndexedEntryId>,
    ) -> Result<Vec<SearchResult>, SemanticIssue> {
        use SemanticIssue::{EmbeddingFailed, IndexChanged, NoVectors, RetrievalFailed};
        if query.mode == SearchMode::Code {
            let code = self.code_service().ok_or(NoVectors)?;
            let epoch = code.query_epoch();
            let vectors = self
                .embed_query_inputs(&[
                    ws_modelmgr::recipe::code_query(&query.text),
                    ws_modelmgr::recipe::search_query(&query.text),
                ])
                .map_err(|_| EmbeddingFailed)?;
            if code.query_epoch() != epoch {
                return Err(IndexChanged);
            }
            let exact = if exclude.is_some() {
                Vec::new()
            } else {
                code.exact_filtered_checked(
                    &query.text,
                    query.limit.max(1) as usize,
                    &query.extensions,
                    &query.file_kinds,
                )
                .map_err(|_| RetrievalFailed)?
            };
            let candidates = (query.limit as usize).max(SEMANTIC_TOP_K).saturating_mul(8);
            let excluded = match exclude {
                Some(ws_protocol::IndexedEntryId::Code(id)) => Some(id),
                _ => None,
            };
            let (candidates, eligible) = code.semantic_excluding_checked(
                &vectors[0],
                &vectors[1],
                candidates,
                &query.extensions,
                &query.file_kinds,
                excluded,
            )?;
            if !eligible {
                return Err(NoVectors);
            }
            let mut hits = candidates
                .into_iter()
                .filter(|h| {
                    !exact.iter().any(|e| {
                        e.file.id == h.file.id
                            && e.start_line >= h.start_line
                            && e.start_line <= h.end_line
                    })
                })
                .map(|h| {
                    let score = if exclude.is_some() {
                        f64::from(h.score)
                    } else {
                        hybrid_score(
                            h.score,
                            &query.text,
                            &h.file.name_lower,
                            h.file.mtime,
                            crate::code::now() / 1000,
                        )
                    };
                    let mut result = code_result(h);
                    result.score = score;
                    if exclude.is_some() {
                        result.match_explanation =
                            format!("Related code ({:.0}% similarity)", score * 100.0);
                    }
                    result
                })
                .collect::<Vec<_>>();
            hits.sort_by(|a, b| b.score.total_cmp(&a.score));
            hits.truncate(query.limit.max(1) as usize);
            if code.query_epoch() != epoch {
                return Err(IndexChanged);
            }
            return Ok(hits);
        }
        let generation = self
            .indexer
            .store_lock()
            .model_generation()
            .map_err(|_| RetrievalFailed)?;
        let source_epoch = self.indexer.search_epoch();
        let kinds = hit_kinds(&query.categories);
        if !query.categories.is_empty() && kinds.is_empty() {
            return Ok(Vec::new());
        }
        let text = query.text.trim();
        let query_vec = match self.embed_query(text) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "semantic stage: query embed failed; exact results stand");
                return Err(EmbeddingFailed);
            }
        };
        if self.indexer.store_lock().model_generation().ok() != Some(generation)
            || self.indexer.search_epoch() != source_epoch
        {
            return Err(IndexChanged);
        }
        let candidate_limit = (query.limit.max(1) as usize)
            .max(SEMANTIC_TOP_K)
            .saturating_mul(8);
        let filter = if exclude.is_none()
            && query.mode == SearchMode::Auto
            && kinds.is_empty()
            && query.extensions.is_empty()
            && query.file_kinds.is_empty()
        {
            None
        } else {
            let mut matching_ids = match self.indexer.matching_entry_ids_filtered(
                query.mode == SearchMode::Apps,
                query.mode == SearchMode::Files,
                &kinds,
                &query.extensions,
                &query.file_kinds,
            ) {
                Ok(ids) => ids,
                Err(e) => {
                    warn!(error = %e, "semantic stage: entry filter query failed; exact results stand");
                    return Err(RetrievalFailed);
                }
            };
            if let Some(ws_protocol::IndexedEntryId::File(source)) = exclude {
                matching_ids.retain(|id| *id != source);
            }
            if matching_ids.is_empty() {
                return Err(NoVectors);
            }
            Some(ws_index::VectorFilter {
                kinds: if exclude.is_some() {
                    ws_index::model::CONTENT_SEGMENT_KINDS
                        .iter()
                        .map(|kind| (*kind).into())
                        .collect()
                } else {
                    Vec::new()
                },
                entry_ids: matching_ids,
            })
        };
        let scored = match self
            .indexer
            .search_vectors(&query_vec, candidate_limit, filter.as_ref())
        {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "semantic stage: vector search failed; exact results stand");
                return Err(RetrievalFailed);
            }
        };
        if self.indexer.search_epoch() != source_epoch {
            return Err(IndexChanged);
        }
        if scored.is_empty() {
            return Err(NoVectors);
        }
        // Best cosine per entry across all segment kinds (`name`, `doc`, `image`, `ocr`, `pdf`, ...).
        let mut best: HashMap<i64, Vec<_>> = HashMap::new();
        for hit in &scored {
            if !semantic_admitted(&hit.segment.kind, hit.cosine) {
                continue;
            }
            best.entry(hit.segment.entry_id).or_default().push(hit);
        }
        let now = now_unix();
        let mut out = Vec::new();
        for (entry_id, mut matches) in best {
            if exact_entry_ids.contains(&entry_id) {
                continue; // duplicate suppression vs the exact batch
            }
            let entry = match self.indexer.get_entry_by_id(entry_id) {
                Ok(Some(e)) => e,
                Ok(None) => continue, // deleted between vector search and join
                Err(e) => {
                    warn!(error = %e, entry = entry_id, "semantic stage: entry lookup failed");
                    return Err(RetrievalFailed);
                }
            };
            if entry.missing {
                continue;
            }
            let kind = entry_hit_kind(&entry);
            if query.mode == SearchMode::Apps && kind != HitKind::Application {
                continue;
            }
            if query.mode == SearchMode::Files && kind == HitKind::Application {
                continue;
            }
            if !kinds.is_empty() && !kinds.contains(&kind) {
                continue;
            }
            matches.sort_by(|a, b| {
                b.cosine
                    .total_cmp(&a.cosine)
                    .then_with(|| a.segment.id.cmp(&b.segment.id))
            });
            let cosine = matches[0].cosine;
            let seg_kind = &matches[0].segment.kind;
            sources.insert(
                format!("s{entry_id}"),
                ResultSource {
                    entry_id,
                    segment_id: Some(matches[0].segment.id),
                    content_only: seg_kind != "name",
                },
            );
            let terms: Vec<_> = text
                .split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .collect();
            let mut moments: Vec<ws_protocol::MatchingMoment> = Vec::new();
            for hit in &matches {
                let Some(location) = segment_location(&hit.segment) else {
                    continue;
                };
                let thumbnail = match self.indexer.segment_thumbnail(&hit.segment) {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(entry_id,error=%error,"semantic preview metadata unavailable");
                        None
                    }
                };
                let excerpt = ws_extract::select_excerpt(&hit.segment.text, &terms, 240);
                if let Some(existing) = moments.iter_mut().find(|m| m.location == Some(location)) {
                    if existing.excerpt.is_empty() {
                        existing.excerpt = excerpt;
                    }
                    if existing.thumbnail_path.is_none() {
                        existing.thumbnail_path = thumbnail;
                    }
                } else if moments.len() < 32 {
                    moments.push(ws_protocol::MatchingMoment {
                        excerpt,
                        location: Some(location),
                        thumbnail_path: thumbnail,
                    });
                }
            }
            let location = moments.first().and_then(|m| m.location);
            let excerpt = moments
                .first()
                .map(|m| m.excerpt.clone())
                .filter(|s| !s.trim().is_empty());
            let thumbnail_path = if let Some(moment) = moments.first() {
                moment.thumbnail_path.clone()
            } else {
                entry.thumbnail_path
            };
            out.push(SearchResult {
                match_source: Some(if exclude.is_some() {
                    ws_protocol::MatchSource::Related
                } else {
                    ws_protocol::MatchSource::Semantic
                }),
                id: format!("s{entry_id}"),
                kind: to_result_kind(kind),
                title: entry.name.clone(),
                subtitle: entry.path.clone(),
                match_explanation: if exclude.is_some() {
                    format!("Related content ({:.0}% similarity)", cosine * 100.0)
                } else {
                    semantic_explanation_for_kind(seg_kind, cosine)
                },
                score: if exclude.is_some() {
                    f64::from(cosine)
                } else {
                    hybrid_score(cosine, text, &entry.name_lower, entry.mtime, now)
                },
                path: Some(entry.path),
                location,
                thumbnail_path,
                excerpt,
                matching_moments: moments,
                entry_reference: None,
            });
        }
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.title.cmp(&b.title))
        });
        out.truncate(query.limit.max(1) as usize);
        if self.indexer.store_lock().model_generation().ok() != Some(generation)
            || self.indexer.search_epoch() != source_epoch
        {
            return Err(IndexChanged);
        }
        Ok(out)
    }

    fn plan_semantic_outcome(
        &self,
        query: &SearchQuery,
        exact: &HashSet<i64>,
        sources: &mut HashMap<String, ResultSource>,
    ) -> (Vec<SearchResult>, SemanticOutcome) {
        if let Some(outcome) = self.semantic_unavailable(query) {
            return (Vec::new(), outcome);
        }
        match self.plan_semantic_checked(query, exact, sources) {
            Ok(hits) => (hits, SemanticOutcome::READY),
            Err(issue) => (Vec::new(), SemanticOutcome::degraded(issue)),
        }
    }

    /// Executes one search, streaming result batches to the client. Exact
    /// results stream first under their unchanged contract; when the semantic
    /// gate is open a second stage appends embedding-similarity hits in later
    /// frames on a background thread so the connection read loop remains free
    /// to service in-band `Cancel` and superseding `Search` frames. A request
    /// cancelled between the stages closes with an empty terminal batch so the
    /// client can retire it.
    pub fn search(self: &Arc<Self>, client: &ClientHandle, id: RequestId, query: SearchQuery) {
        let cancel = self.begin_request_for_client(client.id(), id);
        if !ws_protocol::valid_search_query(&query) {
            let _ = self.send_search_failure(client, id, &cancel, ErrorCode::InvalidRequest);
            self.end_request_for_client(client.id(), id, &cancel);
            return;
        }
        if self
            .search_jobs
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 8).then_some(n + 1)
            })
            .is_err()
        {
            let _ = self.send_search_failure(client, id, &cancel, ErrorCode::NotReady);
            self.end_request_for_client(client.id(), id, &cancel);
            return;
        }
        let work = Arc::new(SearchWork {
            engine: self.clone(),
            client_id: client.id(),
            id,
            cancel: cancel.clone(),
        });
        let worker = work.clone();
        let thread_client = client.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("winsearch-search".into())
            .spawn(move || {
                if let Err(error) =
                    worker
                        .engine
                        .run_search(&thread_client, id, query, worker.cancel.clone())
                {
                    warn!(id, %error, "search transport failed");
                }
            })
        {
            warn!(id, %error, "search worker could not start");
            let _ = self.send_search_failure(client, id, &cancel, ErrorCode::NotReady);
        }
    }

    fn related_query(
        &self,
        request: &ws_protocol::RelatedQuery,
        cancel: &Arc<AtomicBool>,
    ) -> Result<SearchQuery, ErrorCode> {
        if !request.is_valid() {
            return Err(ErrorCode::InvalidRequest);
        }
        let preview = self.preview_snapshot(
            &ws_protocol::PreviewRequest {
                reference: request.source.clone(),
                location: None,
            },
            cancel,
        )?;
        let mut text = String::new();
        for part in preview.parts {
            if !text.is_empty() && text.len() < ws_protocol::MAX_PREVIEW_TEXT_BYTES {
                text.push('\n');
            }
            let mut bytes = part
                .text
                .len()
                .min(ws_protocol::MAX_PREVIEW_TEXT_BYTES - text.len());
            while !part.text.is_char_boundary(bytes) {
                bytes -= 1;
            }
            text.push_str(&part.text[..bytes]);
            if text.len() == ws_protocol::MAX_PREVIEW_TEXT_BYTES {
                break;
            }
        }
        if text.trim().is_empty() {
            return Err(ErrorCode::NotReady);
        }
        let query = SearchQuery {
            text,
            mode: if matches!(
                request.source.indexed_id(),
                Some(ws_protocol::IndexedEntryId::Code(_))
            ) {
                SearchMode::Code
            } else {
                SearchMode::Files
            },
            limit: request.limit,
            include_semantic: true,
            categories: request.categories.clone(),
            extensions: request.extensions.clone(),
            file_kinds: request.file_kinds.clone(),
        };
        if self.semantic_unavailable(&query).is_some() {
            return Err(ErrorCode::NotReady);
        }
        Ok(query)
    }

    pub fn related(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        request: ws_protocol::RelatedQuery,
    ) {
        let cancel = self.begin_request_for_client(client.id(), id);
        let reject = if !request.is_valid() {
            Some(ErrorCode::InvalidRequest)
        } else if self
            .related_jobs
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |jobs| {
                (jobs < 2).then(|| jobs + 1)
            })
            .is_err()
        {
            Some(ErrorCode::NotReady)
        } else {
            None
        };
        if let Some(code) = reject {
            let _ = self.send_search_failure(client, id, &cancel, code);
            self.end_request_for_client(client.id(), id, &cancel);
            return;
        }
        let work = Arc::new(RelatedWork {
            engine: Arc::clone(self),
            client_id: client.id(),
            id,
            cancel,
        });
        let worker = Arc::clone(&work);
        let thread_client = client.clone();
        let source = self.indexer.search_epoch();
        if std::thread::Builder::new()
            .name("winsearch-related".into())
            .spawn(move || {
                let revision = SearchRevision {
                    source,
                    code: worker
                        .engine
                        .code_service()
                        .filter(|_| {
                            matches!(
                                request.source.indexed_id(),
                                Some(ws_protocol::IndexedEntryId::Code(_))
                            )
                        })
                        .map(|code| code.query_epoch()),
                    model: worker.engine.indexer.store_lock().model_generation().ok(),
                };
                if let Err(error) = worker.engine.run_related(
                    &thread_client,
                    id,
                    &request,
                    &worker.cancel,
                    revision,
                ) {
                    warn!(id,error=%error,"Related transport failed");
                }
            })
            .is_err()
        {
            let _ = self.send_search_failure(client, id, &work.cancel, ErrorCode::Internal);
        }
    }

    fn run_related(
        &self,
        client: &ClientHandle,
        id: RequestId,
        request: &ws_protocol::RelatedQuery,
        cancel: &Arc<AtomicBool>,
        revision: SearchRevision,
    ) -> Result<(), FrameError> {
        let Some(_work) = self.indexer.work_gate.enter() else {
            return self.send_search_failure(client, id, cancel, ErrorCode::NotReady);
        };
        let mut query = SearchQuery {
            mode: if matches!(
                request.source.indexed_id(),
                Some(ws_protocol::IndexedEntryId::Code(_))
            ) {
                SearchMode::Code
            } else {
                SearchMode::Files
            },
            ..Default::default()
        };
        let mut sources = HashMap::new();
        let planned = (|| {
            if revision.model.is_none() {
                return Err(ErrorCode::Internal);
            }
            query = self.related_query(request, cancel)?;
            if cancel.load(Ordering::Acquire) || !self.query_current(&query, revision) {
                return Ok((
                    Vec::new(),
                    SearchOutcome::interrupted(SearchDisposition::Superseded),
                ));
            }
            match self.plan_semantic_excluding_checked(
                &query,
                &HashSet::new(),
                &mut sources,
                request.source.indexed_id(),
            ) {
                Ok(hits) => Ok((hits, SearchOutcome::complete(SemanticOutcome::READY))),
                Err(SemanticIssue::NoVectors) => Ok((
                    Vec::new(),
                    SearchOutcome::complete(SemanticOutcome::degraded(SemanticIssue::NoVectors)),
                )),
                Err(SemanticIssue::IndexChanged) => Ok((
                    Vec::new(),
                    SearchOutcome::interrupted(SearchDisposition::Superseded),
                )),
                Err(_) => Err(ErrorCode::Internal),
            }
        })();
        let delivery = SearchDelivery {
            query: &query,
            revision,
            cancel,
            sources: &sources,
            related_source: Some(&request.source),
        };
        match planned {
            Ok((mut hits, outcome)) => {
                for hit in &mut hits {
                    hit.match_source = Some(ws_protocol::MatchSource::Related);
                }
                self.send_results(client, id, hits, Some(outcome), &delivery)
                    .map(|_| ())
            }
            Err(code) => {
                self.send_search_failure_checked(client, id, cancel, code, Some(&delivery))
            }
        }
    }

    fn send_search_failure(
        &self,
        client: &ClientHandle,
        id: RequestId,
        cancel: &Arc<AtomicBool>,
        code: ErrorCode,
    ) -> Result<(), FrameError> {
        self.send_search_failure_checked(client, id, cancel, code, None)
    }

    fn send_search_failure_checked(
        &self,
        client: &ClientHandle,
        id: RequestId,
        cancel: &Arc<AtomicBool>,
        code: ErrorCode,
        delivery: Option<&SearchDelivery<'_>>,
    ) -> Result<(), FrameError> {
        let mut stream = client.stream.lock().unwrap_or_else(|p| p.into_inner());
        let current = || {
            self.cancels
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(client.id(), id))
                .is_some_and(|token| Arc::ptr_eq(token, cancel))
        };
        if !current() {
            return Ok(());
        }
        let stale = delivery.is_some_and(|delivery| !self.delivery_current(delivery));
        if !current() {
            return Ok(());
        }
        let canceled = cancel.load(Ordering::Acquire);
        if stale {
            return write_message(
                &mut *stream,
                &ServerMessage::Results {
                    id,
                    batch: Vec::new(),
                    done: true,
                    outcome: Some(SearchOutcome::interrupted(if canceled {
                        SearchDisposition::Canceled
                    } else {
                        SearchDisposition::Superseded
                    })),
                },
            );
        }
        if !canceled {
            write_message(
                &mut *stream,
                &ServerMessage::Error {
                    id: Some(id),
                    code,
                    message: "search request failed".into(),
                },
            )?;
        }
        if !current() {
            return Ok(());
        }
        let stale = delivery.is_some_and(|delivery| !self.delivery_current(delivery));
        if !current() {
            return Ok(());
        }
        let disposition = if cancel.load(Ordering::Acquire) {
            SearchDisposition::Canceled
        } else if stale {
            SearchDisposition::Superseded
        } else {
            SearchDisposition::Failed
        };
        write_message(
            &mut *stream,
            &ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
                outcome: Some(SearchOutcome::interrupted(disposition)),
            },
        )
    }

    fn run_search(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        query: SearchQuery,
        cancel: Arc<AtomicBool>,
    ) -> Result<(), FrameError> {
        let Some(_work) = self.indexer.work_gate.enter() else {
            return self.send_search_failure(client, id, &cancel, ErrorCode::NotReady);
        };
        if cancel.load(Ordering::Acquire) {
            return self.send_search_failure(client, id, &cancel, ErrorCode::Cancelled);
        }
        let code_epoch = self
            .code_service()
            .filter(|_| query.mode == SearchMode::Code)
            .map(|c| c.query_epoch());
        let revision = SearchRevision {
            source: self.indexer.search_epoch(),
            code: code_epoch,
            model: self.indexer.store_lock().model_generation().ok(),
        };
        let mut sources = HashMap::new();
        let mut results = match self.try_plan_results_with_sources(&query, &mut sources) {
            Ok(results) => results,
            Err(code) => {
                let outcome = self.send_search_failure(client, id, &cancel, code);
                self.end_request_for_client(client.id(), id, &cancel);
                return outcome;
            }
        };
        let delivery = SearchDelivery {
            query: &query,
            revision,
            cancel: &cancel,
            sources: &sources,
            related_source: None,
        };
        results.truncate(query.limit.max(1) as usize);
        let unavailable = if results.len() >= query.limit.max(1) as usize {
            Some(SemanticOutcome::DISABLED)
        } else {
            self.semantic_unavailable(&query)
        };
        let terminal = unavailable.map(SearchOutcome::complete);
        let first = self.send_results(client, id, results, terminal, &delivery)?;
        if first.done {
            self.end_request_for_client(client.id(), id, &cancel);
            return Ok(());
        }

        // Exact and semantic stages share one bounded worker; control reads stay independent.
        self.run_semantic_stage(client, id, &query, &first, &cancel, revision)
    }

    fn query_current(&self, query: &SearchQuery, revision: SearchRevision) -> bool {
        let source = if query.mode == SearchMode::Code {
            self.code_service().map(|code| code.query_epoch()) == revision.code
        } else {
            self.indexer.search_epoch() == revision.source
        };
        source
            && revision.model.is_none_or(|model| {
                self.indexer.store_lock().model_generation().ok() == Some(model)
            })
    }

    fn delivery_current(&self, delivery: &SearchDelivery<'_>) -> bool {
        self.query_current(delivery.query, delivery.revision)
            && delivery
                .related_source
                .is_none_or(|source| self.resolve_entry_snapshot(source).is_ok())
    }

    fn send_results(
        &self,
        client: &ClientHandle,
        id: RequestId,
        batch: Vec<SearchResult>,
        outcome: Option<SearchOutcome>,
        delivery: &SearchDelivery<'_>,
    ) -> Result<SentSearchBatch, FrameError> {
        self.send_results_after(client, id, batch, outcome, delivery, SearchUsage::default())
    }

    fn send_results_after(
        &self,
        client: &ClientHandle,
        id: RequestId,
        batch: Vec<SearchResult>,
        outcome: Option<SearchOutcome>,
        delivery: &SearchDelivery<'_>,
        mut usage: SearchUsage,
    ) -> Result<SentSearchBatch, FrameError> {
        let mut pending = std::collections::VecDeque::from([batch]);
        let mut ids = HashSet::new();
        while let Some(mut batch) = pending.pop_front() {
            if batch.len() > ws_protocol::MAX_SEARCH_BATCH_ITEMS {
                let tail = batch.split_off(ws_protocol::MAX_SEARCH_BATCH_ITEMS);
                pending.push_front(tail);
            }
            let mut terminal = if pending.is_empty() { outcome } else { None };
            let mut stream = client.stream.lock().unwrap_or_else(|p| p.into_inner());
            let current = || {
                self.cancels
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&(client.id(), id))
                    .is_some_and(|token| Arc::ptr_eq(token, delivery.cancel))
            };
            if !current() {
                return Ok(SentSearchBatch {
                    ids,
                    done: true,
                    usage,
                });
            }
            let count = batch.len();
            let mut interrupted =
                delivery.cancel.load(Ordering::Acquire) || !self.delivery_current(delivery);
            batch = if interrupted {
                Vec::new()
            } else {
                self.validate_delivery(batch, delivery.sources)
            };
            if !current() {
                return Ok(SentSearchBatch {
                    ids,
                    done: true,
                    usage,
                });
            }
            interrupted |= batch.len() != count
                || delivery.cancel.load(Ordering::Acquire)
                || !self.delivery_current(delivery);
            if interrupted {
                batch.clear();
                pending.clear();
                terminal = Some(SearchOutcome::interrupted(
                    if delivery.cancel.load(Ordering::Acquire) {
                        SearchDisposition::Canceled
                    } else {
                        SearchDisposition::Superseded
                    },
                ));
            }
            let sent = exact_entry_ids(&batch);
            let count = batch.len();
            let done = terminal.is_some();
            let message = ServerMessage::Results {
                id,
                batch,
                done,
                outcome: terminal,
            };
            let bytes = search_budget::serialized_size(&message)?;
            if bytes > ws_protocol::MAX_SEARCH_BATCH_BYTES && count > 1 && !interrupted {
                let ServerMessage::Results { mut batch, .. } = message else {
                    unreachable!()
                };
                let tail = batch.split_off(count / 2);
                pending.push_front(tail);
                pending.push_front(batch);
                continue;
            }
            if bytes > ws_protocol::MAX_SEARCH_BATCH_BYTES
                || !usage.allows(count, bytes, delivery.query.limit)
            {
                drop(stream);
                self.send_search_failure_checked(
                    client,
                    id,
                    delivery.cancel,
                    ErrorCode::Internal,
                    Some(delivery),
                )?;
                return Ok(SentSearchBatch {
                    ids,
                    done: true,
                    usage,
                });
            }
            if !current() {
                return Ok(SentSearchBatch {
                    ids,
                    done: true,
                    usage,
                });
            }
            write_message(&mut *stream, &message)?;
            usage.results += count;
            usage.bytes += bytes;
            usage.frames += 1;
            ids.extend(sent);
            if done {
                return Ok(SentSearchBatch {
                    ids,
                    done: true,
                    usage,
                });
            }
        }
        Ok(SentSearchBatch {
            ids,
            done: false,
            usage,
        })
    }

    fn run_semantic_stage(
        &self,
        client: &ClientHandle,
        id: RequestId,
        query: &SearchQuery,
        first: &SentSearchBatch,
        cancel: &Arc<AtomicBool>,
        revision: SearchRevision,
    ) -> Result<(), FrameError> {
        let mut sources = HashMap::new();
        let (mut hits, mut semantic) =
            if cancel.load(Ordering::Acquire) || !self.query_current(query, revision) {
                (
                    Vec::new(),
                    SemanticOutcome::degraded(SemanticIssue::IndexChanged),
                )
            } else {
                self.plan_semantic_outcome(query, &first.ids, &mut sources)
            };
        if let Some(unavailable) = self.semantic_unavailable(query) {
            hits.clear();
            semantic = unavailable;
        }
        hits.truncate((query.limit.max(1) as usize).saturating_sub(first.usage.results));
        for hit in &mut hits {
            hit.match_source = Some(ws_protocol::MatchSource::Semantic);
        }
        self.send_results_after(
            client,
            id,
            hits,
            Some(SearchOutcome::complete(semantic)),
            &SearchDelivery {
                query,
                revision,
                cancel,
                sources: &sources,
                related_source: None,
            },
            first.usage,
        )
        .map(|_| ())
    }

    /// Computes the ranked result list for one query (no IPC). Split from
    /// `run_search` so the pipeline is directly testable.
    #[cfg(test)]
    fn plan_results(&self, query: &SearchQuery) -> Vec<SearchResult> {
        self.plan_results_with_sources(query, &mut HashMap::new())
    }

    fn validate_delivery(
        &self,
        mut results: Vec<SearchResult>,
        sources: &HashMap<String, ResultSource>,
    ) -> Vec<SearchResult> {
        let store = self.indexer.store_lock();
        results.retain_mut(|result| {
            let Some(source) = sources.get(&result.id) else {
                return true;
            };
            if !store.entry_visible(source.entry_id).unwrap_or(false) {
                return false;
            }
            let Ok(Some(entry)) = store.get_entry(source.entry_id) else {
                return false;
            };
            if result.path.as_deref() != Some(&entry.path) {
                return false;
            }
            if let Some(segment_id) = source.segment_id {
                let Ok(Some(segment)) = store.get_segment(segment_id) else {
                    return false;
                };
                if !store.segment_source_allowed(&segment).unwrap_or(false) {
                    return false;
                }
            }
            if !store
                .entry_content_allowed(source.entry_id)
                .unwrap_or(false)
            {
                if source.content_only {
                    return false;
                }
                // A name/path can remain valid after its content proof expires.
                result.excerpt = None;
                result.location = None;
                result.thumbnail_path = None;
                result.matching_moments.clear();
            }
            true
        });
        drop(store);
        let mut metadata = HashMap::new();
        results.retain_mut(|result| {
            let source = sources.get(&result.id);
            let candidate = source
                .map(|source| ws_protocol::EntryReference {
                    result_id: format!("e{}", source.entry_id),
                    source_revision: None,
                })
                .or_else(|| result.entry_reference.take());
            result.entry_reference = None;
            if result.kind == ResultKind::Code
                && !result.path.as_deref().is_some_and(|path| {
                    self.code_service()
                        .is_some_and(|code| code.result_allowed(path, code.query_epoch()))
                })
            {
                return false;
            }
            let Some(candidate) = candidate else {
                return true;
            };
            let entry = metadata
                .entry(candidate.result_id.clone())
                .or_insert_with(|| self.resolve_entry_snapshot(&candidate).ok());
            if let Some(entry) = entry {
                if result.path.as_deref() != Some(entry.path.as_str()) {
                    return false;
                }
                if entry.index_revision.is_none() {
                    if source.is_some_and(|source| source.content_only)
                        || result.kind == ResultKind::Code
                    {
                        return false;
                    }
                    result.excerpt = None;
                    result.location = None;
                    result.thumbnail_path = None;
                    result.matching_moments.clear();
                }
                result.entry_reference = Some(ws_protocol::EntryReference {
                    result_id: entry.result_id.clone(),
                    source_revision: Some(entry.source_revision.clone()),
                });
            } else if self.reference_incarnation.is_some() {
                if source.is_some_and(|source| source.content_only)
                    || result.kind == ResultKind::Code
                {
                    return false;
                }
                result.excerpt = None;
                result.location = None;
                result.thumbnail_path = None;
                result.matching_moments.clear();
            }
            true
        });
        results
    }

    #[cfg(test)]
    fn plan_results_with_sources(
        &self,
        query: &SearchQuery,
        sources: &mut HashMap<String, ResultSource>,
    ) -> Vec<SearchResult> {
        self.try_plan_results_with_sources(query, sources)
            .unwrap_or_default()
    }
    fn try_plan_results_with_sources(
        &self,
        query: &SearchQuery,
        sources: &mut HashMap<String, ResultSource>,
    ) -> Result<Vec<SearchResult>, ErrorCode> {
        let text = query.text.trim();
        if text.is_empty() {
            return Ok(Vec::new());
        }
        if query.mode == SearchMode::Code {
            if !query.categories.is_empty() && !query.categories.contains(&ResultKind::Code) {
                return Ok(Vec::new());
            }
            return self
                .code_service()
                .map(|code| {
                    code.exact_filtered_checked(
                        text,
                        query.limit.max(1) as usize,
                        &query.extensions,
                        &query.file_kinds,
                    )
                    .map(|hits| hits.into_iter().map(code_result).collect())
                    .map_err(|_| ErrorCode::Internal)
                })
                .unwrap_or_else(|| Ok(Vec::new()));
        }

        let mut results = Vec::new();
        let wants_index = query.mode != SearchMode::Code;
        let wants_calc = query.mode == SearchMode::Auto
            && query.extensions.is_empty()
            && query.file_kinds.is_empty()
            && (query.categories.is_empty() || query.categories.contains(&ResultKind::Calculator));
        let wants_settings = query.mode == SearchMode::Auto
            && query.extensions.is_empty()
            && query.file_kinds.is_empty()
            && (query.categories.is_empty()
                || query.categories.contains(&ResultKind::Settings)
                || query.categories.contains(&ResultKind::Action));

        if wants_calc {
            match evaluate(text) {
                Ok(Some(calc)) => results.push(SearchResult {
                    id: "calc".into(),
                    kind: ResultKind::Calculator,
                    title: format!("{} = {}", calc.expression, calc.value_text),
                    subtitle: calc.value_text,
                    match_explanation: "Calculator".into(),
                    score: 1.0,
                    location: Some(MatchLocation::Line { line: 0 }),
                    ..Default::default()
                }),
                Ok(None) => {}
                Err(e) => {
                    // The query is arithmetic but invalid (e.g. 1/0): say so
                    // instead of pretending there are no matches.
                    results.push(SearchResult {
                        id: "calc".into(),
                        kind: ResultKind::Calculator,
                        title: format!("{text} — {e}"),
                        subtitle: String::new(),
                        match_explanation: "Calculator".into(),
                        score: 1.0,
                        ..Default::default()
                    });
                }
            }
        }

        if wants_settings {
            for (i, hit) in search_settings(text, 3).into_iter().enumerate() {
                results.push(SearchResult {
                    id: format!("settings-{i}"),
                    kind: ResultKind::Settings,
                    title: format!("{} — Settings", hit.name),
                    subtitle: hit.uri,
                    match_explanation: "Windows settings".into(),
                    score: 0.8 * hit.score,
                    ..Default::default()
                });
            }
        }

        let kinds = hit_kinds(&query.categories);
        if wants_index && (query.categories.is_empty() || !kinds.is_empty()) {
            let request = SearchRequest {
                text: text.to_string(),
                extensions: query.extensions.clone(),
                file_kinds: query.file_kinds.clone(),
                apps_only: query.mode == SearchMode::Apps,
                files_only: query.mode == SearchMode::Files,
                kinds,
                limit: (query.limit.max(1) as usize)
                    .saturating_mul(3)
                    .max(20)
                    .min(ws_protocol::MAX_SEARCH_RESULTS as usize),
            };
            match self.indexer.search(&request) {
                Ok(hits) => {
                    for hit in hits {
                        sources.insert(
                            format!("e{}", hit.entry_id),
                            ResultSource {
                                entry_id: hit.entry_id,
                                segment_id: None,
                                content_only: hit.content_only,
                            },
                        );
                        let terms: Vec<&str> = text
                            .split(|c: char| !c.is_alphanumeric())
                            .filter(|s| !s.is_empty())
                            .collect();
                        let moments: Vec<ws_protocol::MatchingMoment> = hit
                            .matches
                            .iter()
                            .map(|m| ws_protocol::MatchingMoment {
                                excerpt: match m.match_byte {
                                    Some(offset) => {
                                        ws_extract::select_excerpt_at(&m.excerpt, offset, 240)
                                    }
                                    None => ws_extract::select_excerpt(&m.excerpt, &terms, 240),
                                },
                                location: m
                                    .page
                                    .map(|page| MatchLocation::Page { page })
                                    .or_else(|| {
                                        m.start_ms.map(|ms| MatchLocation::Timestamp {
                                            seconds: ms as f64 / 1000.0,
                                            end_seconds: m.end_ms.unwrap_or(ms) as f64 / 1000.0,
                                        })
                                    })
                                    .or_else(|| m.line.map(|line| MatchLocation::Line { line })),
                                thumbnail_path: m.thumbnail_path.clone(),
                            })
                            .collect();
                        results.push(SearchResult {
                            match_source: Some(if hit.content_only {
                                ws_protocol::MatchSource::Content
                            } else {
                                ws_protocol::MatchSource::Filename
                            }),
                            entry_reference: None,
                            id: format!("e{}", hit.entry_id),
                            kind: to_result_kind(hit.kind),
                            title: hit.name,
                            subtitle: hit.path.clone(),
                            match_explanation: hit.explanation,
                            score: hit.score,
                            path: Some(hit.path),
                            location: moments.first().and_then(|m| m.location),
                            thumbnail_path: hit.thumbnail_path,
                            excerpt: moments.first().map(|m| m.excerpt.clone()),
                            matching_moments: moments,
                        });
                    }
                }
                Err(e) => {
                    warn!(error = %e, "exact index search failed");
                    return Err(ErrorCode::Internal);
                }
            }
        }
        Ok(results)
    }
}

/// Maps protocol category filters to index hit kinds.
fn hit_kinds(categories: &[ResultKind]) -> Vec<HitKind> {
    categories
        .iter()
        .filter_map(|k| match k {
            ResultKind::Application => Some(HitKind::Application),
            ResultKind::File => Some(HitKind::File),
            ResultKind::Folder => Some(HitKind::Folder),
            ResultKind::Image => Some(HitKind::Image),
            ResultKind::Screenshot => Some(HitKind::Screenshot),
            ResultKind::Document => Some(HitKind::Document),
            ResultKind::Audio => Some(HitKind::Audio),
            ResultKind::Video => Some(HitKind::Video),
            _ => None,
        })
        .collect()
}

fn to_result_kind(kind: HitKind) -> ResultKind {
    match kind {
        HitKind::Application => ResultKind::Application,
        HitKind::File => ResultKind::File,
        HitKind::Folder => ResultKind::Folder,
        HitKind::Image => ResultKind::Image,
        HitKind::Screenshot => ResultKind::Screenshot,
        HitKind::Document => ResultKind::Document,
        HitKind::Audio => ResultKind::Audio,
        HitKind::Video => ResultKind::Video,
    }
}

/// `HitKind` for a stored entry row (mirrors the retrieval ranking's typing:
/// applications first, then directories, then path+extension kinds including
/// `Screenshot`).
fn entry_hit_kind(row: &EntryRow) -> HitKind {
    if row.is_app {
        HitKind::Application
    } else if row.is_dir {
        HitKind::Folder
    } else {
        kind_for_path_and_ext(&row.path, &row.ext).hit_kind()
    }
}

/// Entry ids already streamed in the exact batch (`SearchResult.id` is
/// `"e{entry_id}"` for index hits; calculator/settings ids never parse and
/// are ignored). Drives duplicate suppression in the semantic stage.
fn code_result(hit: ws_code::model::CodeSearchHit) -> SearchResult {
    let location = hit
        .notebook
        .map(|n| MatchLocation::Notebook {
            cell_ordinal: n.cell_ordinal,
            line: n.start_line,
            end_line: n.end_line,
        })
        .unwrap_or(MatchLocation::Line {
            line: hit.start_line,
        });
    let explanation = if let Some(n) = hit.notebook {
        format!(
            "{} · cell {}, line {}",
            hit.symbol_kind,
            n.cell_ordinal + 1,
            n.start_line
        )
    } else {
        format!("{} · line {}", hit.symbol_kind, hit.start_line)
    };
    SearchResult {
        match_source: Some(ws_protocol::MatchSource::Content),
        entry_reference: Some(ws_protocol::EntryReference {
            result_id: format!("c{}", hit.file.id),
            source_revision: None,
        }),
        id: format!(
            "code:{}:{}:{}:{}",
            hit.file.id, hit.start_line, hit.is_definition, hit.symbol_name
        ),
        kind: ResultKind::Code,
        title: if hit.symbol_name.is_empty() {
            hit.file.name.clone()
        } else {
            hit.symbol_name
        },
        subtitle: hit.file.path.clone(),
        match_explanation: explanation,
        score: hit.score as f64,
        path: Some(hit.file.path),
        location: Some(location),
        excerpt: Some(hit.excerpt),
        ..Default::default()
    }
}

fn exact_entry_ids(results: &[SearchResult]) -> HashSet<i64> {
    results
        .iter()
        .filter_map(|r| r.id.strip_prefix('e')?.parse::<i64>().ok())
        .collect()
}

/// Human-readable reason on every semantic hit.
fn semantic_explanation(cosine: f32) -> String {
    format!("semantic name match · similarity {cosine:.2}")
}

fn semantic_explanation_for_kind(seg_kind: &str, cosine: f32) -> String {
    match seg_kind {
        SEGMENT_KIND_NAME => semantic_explanation(cosine),
        "image" => format!("semantic image match · similarity {cosine:.2}"),
        "ocr" => format!("semantic OCR match · similarity {cosine:.2}"),
        "video" => format!("semantic video frame match · similarity {cosine:.2}"),
        "audio" => format!("semantic sound match · similarity {cosine:.2}"),
        "doc" | "pdf" => format!("semantic content match · similarity {cosine:.2}"),
        other => format!("semantic {other} match · similarity {cosine:.2}"),
    }
}

fn segment_location(segment: &ws_index::model::Segment) -> Option<MatchLocation> {
    if let Some(page) = segment.page {
        let page = u32::try_from(page).ok()?;
        return (page > 0).then_some(MatchLocation::Page { page });
    }
    if let Some(start) = segment.start_ms {
        let end = segment.end_ms.unwrap_or(start);
        if start < 0 || end < start || end > ws_index::model::MAX_MEDIA_DURATION_MS as i64 {
            return None;
        }
        return Some(MatchLocation::Timestamp {
            seconds: start as f64 / 1000.0,
            end_seconds: end as f64 / 1000.0,
        });
    }
    let line = u32::try_from(segment.line?).ok()?;
    (line > 0).then_some(MatchLocation::Line { line })
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Bounded LRU of query text → query embedding (capacity [`QUERY_CACHE_CAP`]).
/// Plain `HashMap` + recency queue: `std::thread`-only codebase, no new
/// dependencies for 64 entries.
struct QueryEmbeddingCache {
    cap: usize,
    map: HashMap<String, Vec<f32>>,
    /// Recency order, front = least recently used.
    order: VecDeque<String>,
}

impl QueryEmbeddingCache {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Returns the cached embedding and refreshes its recency.
    fn get(&mut self, key: &str) -> Option<Vec<f32>> {
        let vec = self.map.get(key)?.clone();
        self.touch(key);
        Some(vec)
    }

    /// Inserts (or replaces) an entry, evicting the least recently used one
    /// when at capacity.
    fn put(&mut self, key: String, vec: Vec<f32>) {
        if self.cap == 0 {
            return;
        }
        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), vec);
            self.touch(&key);
            return;
        }
        while self.map.len() >= self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, vec);
    }

    /// Moves `key` to the most-recently-used end.
    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
            self.order.push_back(key.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use ws_index::IndexStore;

    struct TempTree(PathBuf);

    impl TempTree {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-engine-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture_store() -> IndexStore {
        let mut store = IndexStore::open_in_memory().unwrap();
        let mut config = store.get_or_migrate_indexing_config().unwrap();
        // Query fixtures exercise stored data, independently of host power mode.
        config.respect_battery_saver = false;
        store.set_indexing_config(&config).unwrap();
        store
    }

    #[test]
    fn search_limit_returns_large_exact_sets_in_bounded_progressive_frames() {
        let tree = TempTree::new();
        let names = (0..1100)
            .map(|n| format!("budget-needle-{n:04}.txt"))
            .collect::<Vec<_>>();
        let names = names.iter().map(String::as_str).collect::<Vec<_>>();
        let engine = service_with(&tree, &names);
        let request = SearchQuery {
            text: "budget-needle".into(),
            mode: SearchMode::Files,
            limit: 1000,
            ..Default::default()
        };
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 127, request);
        let mut peer = connect_retry(&pipe);
        let mut total = 0;
        let mut frames = 0;
        loop {
            let Some(ServerMessage::Results { batch, done, .. }) =
                ws_protocol::read_message(&mut peer).unwrap()
            else {
                panic!("missing search results")
            };
            assert!(
                batch.len() <= 512,
                "large exact replies must not be one unbounded UI update"
            );
            total += batch.len();
            frames += 1;
            if done {
                break;
            }
        }
        assert_eq!(
            total, 1000,
            "an internal120-candidate cap must not truncate the requested result set"
        );
        assert!(frames >= 2);
    }

    #[test]
    fn search_limit_caps_exact_and_semantic_results_together() {
        let (engine, _tree) = semantic_service();
        let mut request = semantic_query("meeting");
        request.limit = 1;
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 128, request);
        let mut peer = connect_retry(&pipe);
        let mut total = 0;
        loop {
            let Some(ServerMessage::Results { batch, done, .. }) =
                ws_protocol::read_message(&mut peer).unwrap()
            else {
                panic!("missing terminal search frame")
            };
            total += batch.len();
            if done {
                break;
            }
        }
        assert_eq!(
            total, 1,
            "optional semantic results must share the requested total budget"
        );
    }

    #[test]
    fn search_limit_full_exact_budget_does_not_start_optional_inference() {
        let (engine, _tree) = semantic_service();
        let invoked = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&invoked);
        engine.set_query_embed_hook(Arc::new(move |_| {
            observed.store(true, Ordering::Release);
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let mut request = semantic_query("meeting");
        request.limit = 1;
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 130, request);
        let mut peer = connect_retry(&pipe);
        loop {
            let Some(ServerMessage::Results { done, .. }) =
                ws_protocol::read_message(&mut peer).unwrap()
            else {
                panic!("missing search results")
            };
            if done {
                break;
            }
        }
        assert!(
            !invoked.load(Ordering::Acquire),
            "a full exact result set must not start unused inference"
        );
    }

    #[test]
    fn search_limit_stream_splits_bytes_and_fails_before_exceeding_aggregate_budget() {
        for (count, title_mib, expected_total, expected_failure) in
            [(3, 3, 3, false), (10, 7, 9, true), (1, 9, 0, true)]
        {
            let tree = TempTree::new();
            let engine = service_with(&tree, &[]);
            let pipe = unique_pipe();
            let serving = pipe.clone();
            let this = Arc::clone(&engine);
            let (finished, completion) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = ws_platform::pipe::serve(&serving, move |stream| {
                    let client = this.register_client(Arc::new(Mutex::new(stream)));
                    let cancel = this.begin_request_for_client(client.id(), 131);
                    let request = SearchQuery {
                        limit: 20,
                        ..Default::default()
                    };
                    let revision = SearchRevision {
                        source: this.indexer.search_epoch(),
                        code: None,
                        model: this.indexer.store_lock().model_generation().ok(),
                    };
                    let sources = HashMap::new();
                    let delivery = SearchDelivery {
                        query: &request,
                        revision,
                        cancel: &cancel,
                        sources: &sources,
                        related_source: None,
                    };
                    let batch = (0..count)
                        .map(|n| SearchResult {
                            id: format!("budget-{n}"),
                            title: "x".repeat(title_mib * 1024 * 1024),
                            ..Default::default()
                        })
                        .collect();
                    let result = this.send_results(
                        &client,
                        131,
                        batch,
                        Some(SearchOutcome::complete(SemanticOutcome::DISABLED)),
                        &delivery,
                    );
                    this.end_request_for_client(client.id(), 131, &cancel);
                    this.unregister_client(&client);
                    let _ = finished.send(result.is_ok());
                });
            });
            let mut peer = connect_retry(&pipe);
            let mut total = 0;
            let mut bytes = 0;
            let mut failed = false;
            loop {
                let message: ServerMessage = ws_protocol::read_message(&mut peer).unwrap().unwrap();
                let size = search_budget::serialized_size(&message).unwrap();
                bytes += size;
                match message {
                    ServerMessage::Results {
                        batch,
                        done,
                        outcome,
                        ..
                    } => {
                        assert!(size <= ws_protocol::MAX_SEARCH_BATCH_BYTES);
                        total += batch.len();
                        if done {
                            assert_eq!(
                                outcome.unwrap().disposition,
                                if expected_failure {
                                    SearchDisposition::Failed
                                } else {
                                    SearchDisposition::Complete
                                }
                            );
                            break;
                        }
                    }
                    ServerMessage::Error {
                        code: ErrorCode::Internal,
                        ..
                    } => failed = true,
                    _ => panic!("unexpected budget response"),
                }
            }
            assert_eq!(total, expected_total);
            assert_eq!(failed, expected_failure);
            assert!(bytes <= ws_protocol::MAX_SEARCH_RESPONSE_BYTES);
            assert!(completion.recv_timeout(Duration::from_secs(10)).unwrap());
        }
    }

    #[test]
    fn search_limit_rejects_oversized_raw_requests_before_searching() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["budget-needle.txt"]);
        let pipe = unique_pipe();
        serve_search(
            engine,
            &pipe,
            129,
            SearchQuery {
                text: "budget-needle".into(),
                limit: 10_001,
                ..Default::default()
            },
        );
        let mut peer = connect_retry(&pipe);
        assert!(matches!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap(),
            Some(ServerMessage::Error {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
    }

    fn service_with(tree: &TempTree, files: &[&str]) -> SharedEngine {
        for f in files {
            fs::write(tree.0.join(f), b"x").unwrap();
        }
        let store = fixture_store();
        let indexer = Indexer::open(store, std::slice::from_ref(&tree.0)).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        EngineService::new(indexer)
    }

    fn query(text: &str) -> SearchQuery {
        SearchQuery {
            text: text.into(),
            limit: 20,
            ..Default::default()
        }
    }

    fn related_fixture() -> (SharedEngine, TempTree, ws_protocol::RelatedQuery) {
        related_fixture_with_chunks(SEMANTIC_TOP_K * 8 + 9)
    }

    fn related_fixture_with_chunks(
        source_chunks: usize,
    ) -> (SharedEngine, TempTree, ws_protocol::RelatedQuery) {
        use ws_index::model::{ContentChunk, ContentMatch, DraftEmbeddingInput, NewSegment};
        let tree = TempTree::new();
        let engine = service_with(&tree, &["seed.txt", "target.txt"]);
        let mut seed = 0;
        {
            let mut store = engine.indexer.store_lock();
            let jobs = store.claim_extract_jobs_batch_for_kind("doc", 2).unwrap();
            assert_eq!(jobs.len(), 2);
            for job in jobs {
                let entry = store.get_entry(job.entry_id).unwrap().unwrap();
                let source = entry.name == "seed.txt";
                if source {
                    seed = entry.id;
                }
                let progress = store
                    .begin_content_batch(
                        entry.id,
                        "doc",
                        &crate::extract::source_snapshot(std::path::Path::new(&entry.path)),
                    )
                    .unwrap();
                let chunks = (0..if source { source_chunks } else { 1 })
                    .map(|index| ContentChunk {
                        id: 0,
                        entry_id: entry.id,
                        job_kind: "doc".into(),
                        kind: "doc".into(),
                        chunk_index: index as i64,
                        text: if source {
                            "Bound cached seed text".into()
                        } else {
                            "Related target content".into()
                        },
                        location: ContentMatch {
                            line: Some(index as u32 + 1),
                            ..Default::default()
                        },
                        embed: true,
                        visual: false,
                        attempts: 0,
                        embedding_input: DraftEmbeddingInput::Text,
                    })
                    .collect::<Vec<_>>();
                store
                    .commit_content_batch(&job, &progress, &chunks, None)
                    .unwrap();
                let vector = if source {
                    vec![1.0, 0.0]
                } else {
                    vec![0.8, 0.6]
                };
                for chunk in chunks {
                    store
                        .upsert_segment(NewSegment {
                            entry_id: entry.id,
                            kind: "doc",
                            chunk_index: chunk.chunk_index,
                            text: &chunk.text,
                            page: None,
                            start_ms: None,
                            end_ms: None,
                            line: chunk.location.line.map(|line| line as i64),
                            vector: &vector,
                        })
                        .unwrap();
                }
            }
        }
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0])));
        let entry = engine
            .resolve_entry_snapshot(&ws_protocol::EntryReference {
                result_id: format!("e{seed}"),
                source_revision: None,
            })
            .unwrap();
        let request = ws_protocol::RelatedQuery {
            source: ws_protocol::EntryReference {
                result_id: entry.result_id,
                source_revision: Some(entry.source_revision),
            },
            limit: 1,
            categories: Vec::new(),
            extensions: Vec::new(),
            file_kinds: Vec::new(),
        };
        (engine, tree, request)
    }

    #[test]
    fn related_uses_current_cached_text_without_filename_or_unbound_seed() {
        let (engine, tree, mut related) = related_fixture();
        let cancel = Arc::new(AtomicBool::new(false));
        let request = engine.related_query(&related, &cancel).unwrap();
        assert!(request.text.starts_with("Bound cached seed text"));
        assert!(!request.text.contains("seed.txt"));
        assert!(request.text.len() <= ws_protocol::MAX_PREVIEW_TEXT_BYTES);
        assert!(request.include_semantic);
        assert_eq!(request.mode, SearchMode::Files);
        assert_eq!(request.limit, 1);
        let proof = related.source.source_revision.take();
        assert_eq!(
            engine.related_query(&related, &cancel).unwrap_err(),
            ErrorCode::InvalidRequest
        );
        related.source.source_revision = proof;
        engine.set_model_ready_override(Some(false));
        assert_eq!(
            engine.related_query(&related, &cancel).unwrap_err(),
            ErrorCode::NotReady
        );
        engine.set_model_ready_override(Some(true));
        cancel.store(true, Ordering::Release);
        assert_eq!(
            engine.related_query(&related, &cancel).unwrap_err(),
            ErrorCode::Cancelled
        );
        cancel.store(false, Ordering::Release);
        fs::write(tree.0.join("seed.txt"), b"replacement source").unwrap();
        assert_eq!(
            engine.related_query(&related, &cancel).unwrap_err(),
            ErrorCode::NotReady
        );
    }

    #[test]
    fn related_excludes_every_seed_chunk_before_vector_topk_and_name_only_candidates() {
        let (engine, _tree, related) = related_fixture();
        let request = semantic_query("Bound cached seed text");
        let hits = engine
            .plan_semantic_excluding_checked(
                &SearchQuery {
                    limit: 1,
                    mode: SearchMode::Files,
                    ..request
                },
                &HashSet::new(),
                &mut HashMap::new(),
                related.source.indexed_id(),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].title, "target.txt",
            "seed chunks cannot consume candidate limits"
        );
        let (names, _tree) = semantic_service();
        let outcome = names.plan_semantic_excluding_checked(
            &semantic_query("indirect banquet"),
            &HashSet::new(),
            &mut HashMap::new(),
            Some(ws_protocol::IndexedEntryId::File(1)),
        );
        assert!(
            matches!(outcome, Err(SemanticIssue::NoVectors)),
            "Related must never retrieve filename-only vectors: {outcome:?}"
        );
    }

    #[test]
    fn related_empty_delivery_rechecks_seed_after_waiting_for_writer() {
        for change in ["source", "policy", "model", "cancel", "reuse"] {
            let (engine, tree, related) = related_fixture();
            let query = semantic_query("cached seed");
            let pipe = unique_pipe();
            let serving = pipe.clone();
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = ws_platform::pipe::serve(&serving, move |stream| {
                    let _ = send.send(Arc::new(Mutex::new(stream)));
                });
            });
            let mut peer = connect_retry(&pipe);
            let client =
                engine.register_client(receive.recv_timeout(Duration::from_secs(30)).unwrap());
            let cancel = engine.begin_request_for_client(client.id(), 91);
            let revision = SearchRevision {
                source: engine.indexer.search_epoch(),
                code: None,
                model: engine.indexer.store_lock().model_generation().ok(),
            };
            let sources = HashMap::new();
            std::thread::scope(|scope| {
                let writer = client.stream.lock().unwrap();
                let (started, waiting) = std::sync::mpsc::channel();
                let delivery = SearchDelivery {
                    query: &query,
                    revision,
                    cancel: &cancel,
                    sources: &sources,
                    related_source: Some(&related.source),
                };
                let this = &engine;
                let client = &client;
                let sender = scope.spawn(move || {
                    started.send(()).unwrap();
                    this.send_results(
                        client,
                        91,
                        Vec::new(),
                        Some(SearchOutcome::complete(SemanticOutcome::READY)),
                        &delivery,
                    )
                });
                waiting.recv_timeout(Duration::from_secs(5)).unwrap();
                let current = if change == "reuse" {
                    Some(engine.begin_request_for_client(client.id(), 91))
                } else {
                    if change == "source" {
                        fs::write(tree.0.join("seed.txt"), b"replacement source bytes").unwrap();
                    } else if change == "model" {
                        engine.indexer.store_lock().reset_content_vectors().unwrap();
                    } else if change == "cancel" {
                        engine.cancel_for_client(client.id(), 91);
                    } else {
                        let mut policy = engine.indexer.get_indexing_config().unwrap();
                        policy.roots.clear();
                        engine.indexer.set_indexing_config(&policy).unwrap();
                    }
                    None
                };
                drop(writer);
                assert!(sender.join().unwrap().unwrap().done);
                if let Some(current) = current {
                    engine.end_request_for_client(client.id(), 91, &current);
                }
            });
            if change == "reuse" {
                assert_eq!(peer.peek_available().unwrap(), Some(0));
            } else {
                let message = ws_protocol::read_message::<_, ServerMessage>(&mut peer)
                    .unwrap()
                    .unwrap();
                let ServerMessage::Results {
                    batch,
                    done,
                    outcome,
                    ..
                } = message
                else {
                    panic!("unexpected {message:?}")
                };
                assert!(done && batch.is_empty());
                assert_eq!(
                    outcome,
                    Some(SearchOutcome::interrupted(if change == "cancel" {
                        SearchDisposition::Canceled
                    } else {
                        SearchDisposition::Superseded
                    })),
                    "{change}"
                );
                engine.end_request_for_client(client.id(), 91, &cancel);
            }
            engine.unregister_client(&client);
        }
    }

    #[test]
    fn related_failure_rechecks_request_identity_after_blocked_seed_lookup() {
        let (engine, _tree, related) = related_fixture_with_chunks(1);
        let query = semantic_query("cached seed");
        let pipe = unique_pipe();
        let serving = pipe.clone();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                let _ = send.send(Arc::new(Mutex::new(stream)));
            });
        });
        let peer = connect_retry(&pipe);
        let client = engine.register_client(receive.recv_timeout(Duration::from_secs(30)).unwrap());
        let cancel = engine.begin_request_for_client(client.id(), 92);
        let revision = SearchRevision {
            source: engine.indexer.search_epoch(),
            code: None,
            model: engine.indexer.store_lock().model_generation().ok(),
        };
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let resume = Arc::new(Mutex::new(resume));
        let blocked = AtomicBool::new(false);
        engine
            .indexer
            .store_lock()
            .set_query_source_guard(move |_, _, _| {
                if !blocked.swap(true, Ordering::AcqRel) {
                    entered.send(()).unwrap();
                    resume
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                true
            })
            .unwrap();
        let sources = HashMap::new();
        std::thread::scope(|scope| {
            let this = &engine;
            let client = &client;
            let delivery = SearchDelivery {
                query: &query,
                revision,
                cancel: &cancel,
                sources: &sources,
                related_source: Some(&related.source),
            };
            let sender = scope.spawn(move || {
                this.send_search_failure_checked(
                    client,
                    92,
                    delivery.cancel,
                    ErrorCode::Internal,
                    Some(&delivery),
                )
            });
            waiting.recv_timeout(Duration::from_secs(5)).unwrap();
            let current = engine.begin_request_for_client(client.id(), 92);
            release.send(()).unwrap();
            sender.join().unwrap().unwrap();
            assert_eq!(
                peer.peek_available().unwrap(),
                Some(0),
                "old work cannot reject the reused request after seed validation"
            );
            assert!(engine
                .cancels
                .lock()
                .unwrap()
                .get(&(client.id(), 92))
                .is_some_and(|token| Arc::ptr_eq(token, &current)));
            engine.end_request_for_client(client.id(), 92, &current);
        });
        engine.unregister_client(&client);
    }

    #[test]
    fn related_public_pipe_bounds_work_keeps_controls_live_and_reports_actual_lookup_state() {
        use ws_protocol::{ClientMessage, SemanticPhase};
        let (engine, tree, related) = related_fixture_with_chunks(1);
        let pipe = unique_pipe();
        let serving = pipe.clone();
        let this = Arc::clone(&engine);
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                crate::server::handle_connection(stream, Arc::clone(&this))
            });
        });
        let mut peer = connect_retry(&pipe);
        fn reply(peer: &mut PipeStream) -> ServerMessage {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                while peer.peek_available().unwrap() == Some(0) {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "Related blocked control/result delivery"
                    );
                    std::thread::sleep(Duration::from_millis(2));
                }
                let response = ws_protocol::read_message::<_, ServerMessage>(peer)
                    .unwrap()
                    .unwrap();
                if !matches!(response, ServerMessage::Status { .. }) {
                    return response;
                }
            }
        }
        write_message(
            &mut peer,
            &ClientMessage::Hello {
                protocol_version: 1,
                client: "related-test".into(),
                client_version: "1".into(),
            },
        )
        .unwrap();
        let ServerMessage::HelloAck { features, .. } = reply(&mut peer) else {
            panic!("missing handshake")
        };
        assert!(features.iter().any(|feature| feature == "search.related"));
        assert!(
            matches!(
                ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap(),
                Some(ServerMessage::Status { .. })
            ),
            "finish the initial status snapshot before holding the store"
        );
        eprintln!("Related public stage: blocked-store controls");
        let blocked = engine.indexer.store_lock();
        for id in [10, 11] {
            write_message(
                &mut peer,
                &ClientMessage::Related {
                    id,
                    query: related.clone(),
                },
            )
            .unwrap();
        }
        write_message(&mut peer, &ClientMessage::Ping { id: 12 }).unwrap();
        assert!(
            matches!(reply(&mut peer), ServerMessage::Pong { id: 12 }),
            "Related reads must run outside the connection loop"
        );
        write_message(
            &mut peer,
            &ClientMessage::Related {
                id: 13,
                query: related.clone(),
            },
        )
        .unwrap();
        assert!(matches!(
            reply(&mut peer),
            ServerMessage::Error {
                id: Some(13),
                code: ErrorCode::NotReady,
                ..
            }
        ));
        assert!(matches!(
            reply(&mut peer),
            ServerMessage::Results {
                id: 13,
                done: true,
                outcome: Some(SearchOutcome {
                    disposition: SearchDisposition::Failed,
                    ..
                }),
                ..
            }
        ));
        write_message(&mut peer, &ClientMessage::Ping { id: 14 }).unwrap();
        assert!(matches!(reply(&mut peer), ServerMessage::Pong { id: 14 }));
        drop(blocked);
        eprintln!("Related public stage: cancelled workers");
        let mut canceled = HashSet::new();
        for _ in 0..2 {
            let ServerMessage::Results {
                id,
                batch,
                done,
                outcome,
            } = reply(&mut peer)
            else {
                panic!("cancelled work delivered a nonterminal reply")
            };
            assert!(batch.is_empty() && done && [10, 11].contains(&id));
            assert_eq!(
                outcome,
                Some(SearchOutcome::interrupted(SearchDisposition::Canceled))
            );
            canceled.insert(id);
        }
        assert_eq!(canceled.len(), 2);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while engine.related_jobs.load(Ordering::Acquire) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "Related slots did not retire"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        for (id, scenario) in [
            (20, "ready"),
            (21, "no_model"),
            (22, "embed_failure"),
            (23, "below_threshold"),
            (24, "no_vectors"),
            (25, "source_change_on_error"),
        ] {
            eprintln!("Related public stage: {scenario}");
            *engine.query_cache.lock().unwrap() = QueryEmbeddingCache::new(QUERY_CACHE_CAP);
            engine.set_model_ready_override(Some(scenario != "no_model"));
            let seed_path = tree.0.join("seed.txt");
            engine.set_query_embed_hook(Arc::new(move |_| {
                if scenario == "source_change_on_error" {
                    fs::write(&seed_path, b"replacement seed bytes").unwrap();
                    return Err("PRIVATE changed-source diagnostic".into());
                }
                if scenario == "embed_failure" {
                    Err("PRIVATE embedding diagnostic".into())
                } else {
                    Ok(if scenario == "below_threshold" {
                        vec![0.0, -1.0]
                    } else {
                        vec![1.0, 0.0]
                    })
                }
            }));
            let mut request = related.clone();
            if scenario == "no_vectors" {
                request.extensions = vec!["pdf".into()];
            }
            write_message(&mut peer, &ClientMessage::Related { id, query: request }).unwrap();
            if scenario == "no_model" || scenario == "embed_failure" {
                let ServerMessage::Error {
                    id: Some(actual),
                    code,
                    message,
                } = reply(&mut peer)
                else {
                    panic!("unavailable Related must fail explicitly")
                };
                assert_eq!(actual, id);
                assert_eq!(
                    code,
                    if scenario == "no_model" {
                        ErrorCode::NotReady
                    } else {
                        ErrorCode::Internal
                    }
                );
                assert_eq!(message, "search request failed");
            }
            let message = reply(&mut peer);
            let ServerMessage::Results {
                id: actual,
                batch,
                done,
                outcome,
            } = message
            else {
                panic!("missing terminal Related: {message:?}")
            };
            assert_eq!(actual, id);
            assert!(done);
            if scenario == "ready" {
                assert_eq!(batch.len(), 1);
                assert_eq!(batch[0].title, "target.txt");
                assert!(batch[0]
                    .entry_reference
                    .as_ref()
                    .is_some_and(|reference| reference.is_bound()
                        && reference.result_id != related.source.result_id));
            } else {
                assert!(
                    batch.is_empty(),
                    "Related must never append ordinary exact results"
                );
            }
            if scenario == "source_change_on_error" {
                assert_eq!(
                    outcome,
                    Some(SearchOutcome::interrupted(SearchDisposition::Superseded))
                );
            } else if scenario == "no_model" || scenario == "embed_failure" {
                assert_eq!(
                    outcome,
                    Some(SearchOutcome::interrupted(SearchDisposition::Failed))
                );
            } else {
                let semantic = outcome.unwrap().semantic.unwrap();
                assert_eq!(
                    semantic.phase,
                    if scenario == "no_vectors" {
                        SemanticPhase::Degraded
                    } else {
                        SemanticPhase::Ready
                    }
                );
                assert_eq!(
                    semantic.reason,
                    if scenario == "no_vectors" {
                        Some(SemanticIssue::NoVectors)
                    } else {
                        None
                    }
                );
            }
        }
    }

    #[test]
    fn related_code_uses_current_parsed_cache_and_keeps_exact_identifier_targets() {
        let (engine, code, root) = code_search_service();
        crate::code::tests::stop_fixture_watchers(&code);
        let file = code.exact("needle", 20)[0].file.id;
        let entry = engine
            .resolve_entry_snapshot(&ws_protocol::EntryReference {
                result_id: format!("c{file}"),
                source_revision: None,
            })
            .unwrap();
        let related = ws_protocol::RelatedQuery {
            source: ws_protocol::EntryReference {
                result_id: entry.result_id,
                source_revision: Some(entry.source_revision),
            },
            limit: 1,
            categories: vec![ResultKind::Code],
            extensions: vec!["rs".into()],
            file_kinds: Vec::new(),
        };
        let mut query = engine
            .related_query(&related, &Arc::new(AtomicBool::new(false)))
            .unwrap();
        assert!(query.text.contains("fn needle"));
        assert_eq!(query.mode, SearchMode::Code);
        query.text = "resolve_record".into();
        let hits = engine
            .plan_semantic_excluding_checked(
                &query,
                &HashSet::new(),
                &mut HashMap::new(),
                related.source.indexed_id(),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "resolve_record");
        assert_eq!(hits[0].kind, ResultKind::Code);
        assert!(hits[0]
            .path
            .as_ref()
            .is_some_and(|path| path.ends_with("candidate.rs")));
        assert!(hits[0]
            .entry_reference
            .as_ref()
            .is_some_and(|reference| reference.result_id != related.source.result_id));
        drop(code);
        drop(engine);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn extension_filter_excludes_synthetic_results() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let mut request = query("1+2");
        request.extensions = vec!["pdf".into()];
        assert!(
            engine.plan_results(&request).is_empty(),
            "file extensions exclude synthetic calculator/settings hits"
        );
    }

    #[test]
    fn file_kind_selection_distinguishes_lumen_kinds_before_exact_limits() {
        let tree = TempTree::new();
        fs::create_dir(tree.0.join("needle-folder.pdf")).unwrap();
        let cases = [
            ("pdf", "pdf"),
            ("document", "txt"),
            ("spreadsheet", "xlsx"),
            ("presentation", "pptx"),
            ("source", "rs"),
            ("image", "png"),
            ("video", "mp4"),
            ("audio", "wav"),
            ("archive", "zip"),
            ("executable", "exe"),
            ("model", "glb"),
            ("unknown", "unrecognized"),
        ];
        let files = cases
            .iter()
            .map(|(_, ext)| format!("needle.{ext}"))
            .collect::<Vec<_>>();
        let engine = service_with(&tree, &files.iter().map(String::as_str).collect::<Vec<_>>());
        for (kind, extension) in cases {
            let request: SearchQuery = serde_json::from_value(
                serde_json::json!({"text":"needle","mode":"files","limit":1,"file_kinds":[kind]}),
            )
            .unwrap();
            let hits = engine.plan_results(&request);
            assert_eq!(hits.len(), 1, "{kind}");
            assert_eq!(hits[0].title, format!("needle.{extension}"), "{kind}");
        }
        let request: SearchQuery = serde_json::from_value(
            serde_json::json!({"text":"needle","mode":"files","limit":1,"file_kinds":["folder"]}),
        )
        .unwrap();
        let hits = engine.plan_results(&request);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].kind, ResultKind::Folder);
        assert_eq!(hits[0].title, "needle-folder.pdf");
    }

    #[test]
    fn file_kind_selection_precedes_crowded_exact_candidates_and_intersects_extensions() {
        let tree = TempTree::new();
        let mut files = (0..510)
            .map(|index| format!("needle-{index:04}.txt"))
            .collect::<Vec<_>>();
        files.push("needle-z.xlsx".into());
        let engine = service_with(&tree, &files.iter().map(String::as_str).collect::<Vec<_>>());
        let mut request:SearchQuery=serde_json::from_value(serde_json::json!({"text":"needle","mode":"files","limit":1,"file_kinds":["spreadsheet"]})).unwrap();
        let hits = engine.plan_results(&request);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "needle-z.xlsx");
        request.extensions = vec!["pdf".into()];
        assert!(engine.plan_results(&request).is_empty());
    }

    #[test]
    fn file_kind_selection_excludes_synthetic_and_unselected_code_results() {
        let (engine, code, _root) = code_search_service();
        crate::code::tests::stop_fixture_watchers(&code);
        let request: SearchQuery = serde_json::from_value(
            serde_json::json!({"text":"needle","mode":"code","file_kinds":["pdf"]}),
        )
        .unwrap();
        assert!(engine.plan_results(&request).is_empty());
        let mut request: SearchQuery = serde_json::from_value(
            serde_json::json!({"text":"needle","mode":"code","file_kinds":["source"]}),
        )
        .unwrap();
        assert!(!engine.plan_results(&request).is_empty());
        request.text = "indirect code description".into();
        request.include_semantic = true;
        assert!(!engine.plan_semantic(&request, &HashSet::new()).is_empty());
        request.file_kinds = vec![ws_protocol::FileKind::Pdf];
        assert!(engine.plan_semantic(&request, &HashSet::new()).is_empty());
        let request: SearchQuery =
            serde_json::from_value(serde_json::json!({"text":"1+2","file_kinds":["unknown"]}))
                .unwrap();
        assert!(engine.plan_results(&request).is_empty());
    }

    #[test]
    fn file_kind_selection_precedes_semantic_topk_in_the_actual_indexer() {
        let tree = TempTree::new();
        let mut names = (0..SEMANTIC_TOP_K * 8 + 9)
            .map(|index| format!("noise-{index}.txt"))
            .collect::<Vec<_>>();
        names.push("wanted.pdf".into());
        let engine = service_with(&tree, &names.iter().map(String::as_str).collect::<Vec<_>>());
        while engine.indexer.embed_queue_depth().unwrap() > 0 {
            engine
                .indexer
                .embed_once_with(|texts| {
                    Ok(texts
                        .iter()
                        .map(|text| {
                            if text.contains("wanted") {
                                vec![0.8, 0.6]
                            } else {
                                vec![1.0, 0.0]
                            }
                        })
                        .collect())
                })
                .unwrap();
        }
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0])));
        let request:SearchQuery=serde_json::from_value(serde_json::json!({"text":"unmatched concept","include_semantic":true,"limit":1,"file_kinds":["pdf"]})).unwrap();
        let hits = engine.plan_semantic(&request, &HashSet::new());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "wanted.pdf");
    }

    #[test]
    fn related_file_kind_selection_cannot_leak_targets_of_another_kind() {
        let (engine, _tree, related) = related_fixture_with_chunks(1);
        let mut json = serde_json::to_value(&related).unwrap();
        json["file_kinds"] = serde_json::json!(["pdf"]);
        let related: ws_protocol::RelatedQuery = serde_json::from_value(json).unwrap();
        let request = engine
            .related_query(&related, &Arc::new(AtomicBool::new(false)))
            .unwrap();
        assert!(matches!(
            engine.plan_semantic_excluding_checked(
                &request,
                &HashSet::new(),
                &mut HashMap::new(),
                related.source.indexed_id()
            ),
            Err(SemanticIssue::NoVectors)
        ));
    }

    #[test]
    fn extension_filter_excludes_unselected_code_files() {
        let (engine, code, _root) = code_search_service();
        crate::code::tests::stop_fixture_watchers(&code);
        let mut request = query("needle");
        request.mode = SearchMode::Code;
        request.extensions = vec!["py".into()];
        assert!(
            engine.plan_results(&request).is_empty(),
            "code filters must be applied before result limits"
        );
        request.extensions = vec![".RS".into()];
        assert!(!engine.plan_results(&request).is_empty());
    }

    #[test]
    fn extension_filter_precedes_semantic_topk_in_the_actual_indexer() {
        let tree = TempTree::new();
        let mut names = (0..SEMANTIC_TOP_K * 8 + 9)
            .map(|index| format!("noise-{index}.txt"))
            .collect::<Vec<_>>();
        names.push("wanted.pdf".into());
        let files = names.iter().map(String::as_str).collect::<Vec<_>>();
        let engine = service_with(&tree, &files);
        while engine.indexer.embed_queue_depth().unwrap() > 0 {
            let outcome = engine
                .indexer
                .embed_once_with(|texts| {
                    Ok(texts
                        .iter()
                        .map(|text| {
                            if text.contains("wanted") {
                                vec![0.8, 0.6]
                            } else {
                                vec![1.0, 0.0]
                            }
                        })
                        .collect())
                })
                .unwrap();
            assert!(
                outcome.embedded > 0,
                "static semantic fixture must make progress: {outcome:?}"
            );
        }
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0])));
        let mut request = semantic_query("unmatched concept");
        request.limit = 1;
        request.extensions = vec![".PDF".into()];
        let hits = engine.plan_semantic(&request, &HashSet::new());
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].title, "wanted.pdf",
            "excluded higher-score entries cannot consume semantic top-k"
        );
    }

    #[test]
    fn preview_request_reuse_cancels_old_work_without_losing_the_new_token() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let old = engine.begin_request_for_client(7, 9);
        let current = engine.begin_request_for_client(7, 9);
        assert!(
            old.load(Ordering::Acquire),
            "reusing an active request ID must retire its previous work"
        );
        engine.end_request_for_client(7, 9, &old);
        engine.cancel_for_client(7, 9);
        assert!(
            current.load(Ordering::Acquire),
            "an old completion cannot remove a newer cancellation token"
        );
    }

    #[test]
    fn entry_reference_proof_cannot_revive_in_another_engine_with_the_same_store_and_models() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["current.txt"]);
        let file = tree.0.join("current.txt");
        let id = engine
            .indexer
            .store_lock()
            .find_by_path(&file.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let initial = ws_protocol::EntryReference {
            result_id: format!("e{id}"),
            source_revision: None,
        };
        let before = engine.resolve_entry_snapshot(&initial).unwrap();
        let bound = ws_protocol::EntryReference {
            result_id: initial.result_id.clone(),
            source_revision: Some(before.source_revision.clone()),
        };
        let restarted = EngineService::new_with_models(
            engine.indexer.clone(),
            engine.models.clone(),
            EmbedSupervisor::new(),
        );
        let after = restarted.resolve_entry_snapshot(&initial).unwrap();
        assert_ne!(
            before.source_revision, after.source_revision,
            "engine incarnation must be part of the metadata proof"
        );
        assert_eq!(
            restarted.resolve_entry_snapshot(&bound).unwrap_err(),
            ErrorCode::NotReady
        );
    }

    #[test]
    fn entry_reference_unavailable_incarnation_keeps_exact_name_search_available() {
        let tree = TempTree::new();
        let mut engine = service_with(&tree, &["fallback.txt"]);
        Arc::get_mut(&mut engine).unwrap().reference_incarnation = None;
        let mut sources = HashMap::new();
        let results = engine.plan_results_with_sources(&query("fallback"), &mut sources);
        assert_eq!(results.len(), 1);
        let request = ws_protocol::EntryReference {
            result_id: results[0].id.clone(),
            source_revision: None,
        };
        assert_eq!(
            engine.resolve_entry_snapshot(&request).unwrap_err(),
            ErrorCode::NotReady
        );
        let delivered = engine.validate_delivery(results, &sources);
        assert_eq!(delivered.len(), 1);
        assert!(delivered[0].entry_reference.is_none());
    }

    #[test]
    fn entry_reference_keeps_metadata_only_project_files_bound_without_reviving_content() {
        let tree = TempTree::new();
        fs::write(
            tree.0.join("metadata.txt"),
            b"Generated searchable document content for metadata identity.",
        )
        .unwrap();
        let engine = service_with(&tree, &[]);
        engine
            .indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                Err("no model".into())
            })
            .unwrap();
        let path = tree.0.join("metadata.txt");
        let id = engine
            .indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let initial = ws_protocol::EntryReference {
            result_id: format!("e{id}"),
            source_revision: None,
        };
        let before = engine.resolve_entry_snapshot(&initial).unwrap();
        assert!(
            before.index_revision.is_some(),
            "fixture must have admitted indexed content"
        );
        let previous = ws_protocol::EntryReference {
            result_id: initial.result_id.clone(),
            source_revision: Some(before.source_revision),
        };
        fs::write(tree.0.join("Cargo.toml"), b"new project marker").unwrap();
        assert_eq!(
            engine.resolve_entry_snapshot(&previous).unwrap_err(),
            ErrorCode::NotReady,
            "a content-bearing proof must expire when ordinary content admission is revoked"
        );
        let metadata = engine
            .resolve_entry_snapshot(&initial)
            .expect("an admitted filename still needs metadata-only identity");
        assert!(metadata.index_revision.is_none() && metadata.content_sha256.is_none());
        let bound = ws_protocol::EntryReference {
            result_id: initial.result_id,
            source_revision: Some(metadata.source_revision.clone()),
        };
        assert_eq!(engine.resolve_entry_snapshot(&bound).unwrap(), metadata);
    }

    #[test]
    fn entry_reference_rejects_a_replaced_file_before_native_inventory_reconciles() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["replace.txt"]);
        let path = tree.0.join("replace.txt");
        let old = engine
            .indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        let identity = ws_platform::identity::file_identity(&path);
        std::fs::rename(&path, tree.0.join("held-original.txt")).unwrap();
        std::fs::write(&path, b"different generated file").unwrap();
        assert_ne!(ws_platform::identity::file_identity(&path), identity);
        let reference = ws_protocol::EntryReference {
            result_id: format!("e{}", old.id),
            source_revision: None,
        };
        assert_eq!(
            engine.resolve_entry_snapshot(&reference).unwrap_err(),
            ErrorCode::NotReady,
            "an old indexed ID must not be rebound to a different native file at the same path"
        );
    }

    #[test]
    fn entry_reference_delivery_binds_exact_and_semantic_files_without_rewriting_hit_ids() {
        let (engine, _tree) = semantic_service();
        let mut sources = HashMap::new();
        let results = engine.plan_results_with_sources(&query("meeting"), &mut sources);
        let delivered = engine.validate_delivery(results, &sources);
        assert!(!delivered.is_empty());
        let exact = delivered.iter().find(|r| r.id.starts_with('e')).unwrap();
        let reference = exact
            .entry_reference
            .as_ref()
            .expect("exact file hits need a bound native reference");
        assert!(reference.is_bound());
        assert_eq!(reference.result_id, exact.id);
        assert_eq!(
            engine.resolve_entry_snapshot(reference).unwrap().path,
            exact.path.clone().unwrap()
        );
        let mut sources = HashMap::new();
        let semantic = engine.plan_semantic_with_sources(
            &semantic_query("meeting"),
            &HashSet::new(),
            &mut sources,
        );
        let delivered = engine.validate_delivery(semantic, &sources);
        let hit = delivered
            .iter()
            .find(|r| r.id.starts_with('s'))
            .expect("fixture must return a semantic file hit");
        let reference = hit
            .entry_reference
            .as_ref()
            .expect("semantic hit IDs must map to canonical indexed IDs");
        assert!(reference.is_bound());
        assert_eq!(reference.result_id, format!("e{}", &hit.id[1..]));
        assert_eq!(
            engine.resolve_entry_snapshot(reference).unwrap().path,
            hit.path.clone().unwrap()
        );
    }

    #[test]
    fn entry_reference_delivery_withholds_prepared_content_after_source_change_or_request_reuse() {
        for change in ["source", "policy", "model", "cancel", "reuse"] {
            let reused = change == "reuse";
            let tree = TempTree::new();
            let path = tree.0.join("report.txt");
            fs::write(&path, b"cached_delivery_secret").unwrap();
            let engine = service_with(&tree, &[]);
            engine
                .indexer
                .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                    Err("no model".into())
                })
                .unwrap();
            let request = query("cached_delivery_secret");
            let mut sources = HashMap::new();
            let planned = engine.plan_results_with_sources(&request, &mut sources);
            let prepared = engine.validate_delivery(planned, &sources);
            assert!(prepared.iter().any(|hit| hit
                .excerpt
                .as_deref()
                .is_some_and(|text| text.contains("cached_delivery_secret"))));
            let pipe = unique_pipe();
            let serving = pipe.clone();
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = ws_platform::pipe::serve(&serving, move |stream| {
                    let _ = send.send(Arc::new(Mutex::new(stream)));
                });
            });
            let mut peer = connect_retry(&pipe);
            let stream = receive.recv_timeout(Duration::from_secs(30)).unwrap();
            let client = engine.register_client(stream);
            let cancel = engine.begin_request_for_client(client.id(), 71);
            let revision = SearchRevision {
                source: engine.indexer.search_epoch(),
                code: None,
                model: engine.indexer.store_lock().model_generation().ok(),
            };
            std::thread::scope(|scope| {
                let writer = client.stream.lock().unwrap();
                let (started, waiting) = std::sync::mpsc::channel();
                let (finished, completion) = std::sync::mpsc::channel();
                let delivery = SearchDelivery {
                    query: &request,
                    revision,
                    cancel: &cancel,
                    sources: &sources,
                    related_source: None,
                };
                let sending_engine = &engine;
                let sending_client = &client;
                let sender = scope.spawn(move || {
                    started.send(()).unwrap();
                    let result = sending_engine.send_results(
                        sending_client,
                        71,
                        prepared,
                        Some(SearchOutcome::complete(SemanticOutcome::DISABLED)),
                        &delivery,
                    );
                    let _ = finished.send(());
                    result
                });
                waiting.recv_timeout(Duration::from_secs(5)).unwrap();
                let blocked = completion.recv_timeout(Duration::from_millis(150));
                let current = if reused {
                    Some(engine.begin_request_for_client(client.id(), 71))
                } else {
                    if change == "source" {
                        fs::write(&path, b"replacement bytes without the original marker").unwrap();
                    } else if change == "model" {
                        engine.indexer.store_lock().reset_content_vectors().unwrap();
                    } else if change == "cancel" {
                        engine.cancel_for_client(client.id(), 71);
                    } else {
                        let mut policy = engine.indexer.get_indexing_config().unwrap();
                        policy.roots.clear();
                        engine.indexer.set_indexing_config(&policy).unwrap();
                    }
                    None
                };
                drop(writer);
                let sent = sender.join().unwrap().unwrap();
                assert!(
                    matches!(blocked, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
                    "delivery must wait for the held writer"
                );
                assert!(
                    sent.ids.is_empty() && sent.done,
                    "only actually delivered exact IDs may suppress semantic hits"
                );
                if let Some(current) = current {
                    engine.end_request_for_client(client.id(), 71, &current);
                }
            });
            if reused {
                assert_eq!(
                    peer.peek_available().unwrap(),
                    Some(0),
                    "old work cannot terminate the newer request"
                );
            } else {
                match ws_protocol::read_message::<_, ServerMessage>(&mut peer)
                    .unwrap()
                    .unwrap()
                {
                    ServerMessage::Results {
                        batch,
                        done,
                        outcome,
                        ..
                    } => {
                        assert!(batch.is_empty() && done);
                        assert_eq!(
                            outcome,
                            Some(SearchOutcome::interrupted(if change == "cancel" {
                                SearchDisposition::Canceled
                            } else {
                                SearchDisposition::Superseded
                            }))
                        );
                    }
                    other => panic!("unexpected frame: {other:?}"),
                }
                engine.end_request_for_client(client.id(), 71, &cancel);
            }
            engine.unregister_client(&client);
        }
    }

    #[test]
    fn code_entry_reference_delivery_binds_real_exact_and_semantic_hits_and_rejects_replacement() {
        let (engine, code, root) = code_search_service();
        // Assert native identity before watcher reconciliation; live epoch races have separate coverage.
        crate::code::tests::stop_fixture_watchers(&code);
        let mut request = query("needle");
        request.mode = SearchMode::Code;
        let mut sources = HashMap::new();
        let planned = engine.plan_results_with_sources(&request, &mut sources);
        assert!(!planned.is_empty());
        let ids = planned.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>();
        let exact = engine.validate_delivery(planned, &sources);
        assert_eq!(
            exact.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>(),
            ids
        );
        for hit in &exact {
            let reference = hit
                .entry_reference
                .as_ref()
                .expect("code hit must carry its code-file reference");
            assert!(reference.is_bound() && reference.result_id.starts_with('c'));
            assert!(engine
                .resolve_entry_snapshot(reference)
                .unwrap()
                .content_sha256
                .is_some());
        }
        let mut semantic_request = semantic_query("indirect code description");
        semantic_request.mode = SearchMode::Code;
        let semantic =
            engine.plan_semantic_with_sources(&semantic_request, &HashSet::new(), &mut sources);
        assert!(
            !semantic.is_empty(),
            "fixture must exercise actual code vector retrieval"
        );
        let semantic = engine.validate_delivery(semantic, &sources);
        assert!(!semantic.is_empty());
        assert!(semantic
            .iter()
            .all(|hit| hit.entry_reference.as_ref().is_some_and(|r| r.is_bound())));
        let source = root.join("lib.rs");
        assert!(
            semantic
                .iter()
                .any(|hit| hit.path.as_deref() == Some(source.to_string_lossy().as_ref())),
            "semantic fixture must include the source being replaced"
        );
        let unaffected = semantic
            .iter()
            .filter(|hit| hit.path.as_deref() != Some(source.to_string_lossy().as_ref()))
            .map(|hit| hit.id.clone())
            .collect::<Vec<_>>();
        fs::rename(&source, root.join("held-original.rs")).unwrap();
        fs::write(&source, b"fn needle() {}\n").unwrap();
        assert!(
            engine.validate_delivery(exact, &sources).is_empty(),
            "stale code identity cannot be rebound to a replacement at the same path"
        );
        assert_eq!(
            engine
                .validate_delivery(semantic, &sources)
                .into_iter()
                .map(|hit| hit.id)
                .collect::<Vec<_>>(),
            unaffected,
            "replacement removes only the affected source's semantic hits"
        );
        drop(engine);
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_preview_uses_current_native_code_and_notebook_cache_then_rejects_stale_proofs() {
        use ws_protocol::{EntryReference, PreviewRequest, PreviewState};
        let (code, indexer, root) = crate::code::tests::unindexed_fixture();
        fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
        let notebook = root.join("cells.ipynb");
        fs::write(&notebook,r#"{"metadata":{"language_info":{"name":"python"}},"cells":[{"cell_type":"raw","source":["skip"]},{"cell_type":"code","source":["def cached_needle():\n","    return 7\n"],"outputs":[{"text":["PRIVATE_OUTPUT"]}]}]}"#).unwrap();
        code.configure().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let ready = {
                let store = code.store.lock().unwrap();
                let files = store.all_files().unwrap();
                files.len() == 2
                    && files.iter().all(|file| {
                        store
                            .source_provenance(file.id)
                            .unwrap()
                            .is_some_and(|state| state.sha256.is_some())
                    })
            };
            if ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "new notebook must complete its admitted native parse"
            );
            if !code.turn().unwrap() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        indexer.set_code_service(Arc::downgrade(&code));
        let engine =
            EngineService::new_with_model_dir(indexer, root.parent().unwrap().join("models"));
        *engine.code.lock().unwrap() = Some(Arc::clone(&code));
        for (path, location, marker) in [
            (
                root.join("lib.rs"),
                MatchLocation::Line { line: 1 },
                "needle",
            ),
            (
                notebook,
                MatchLocation::Notebook {
                    cell_ordinal: 1,
                    line: 2,
                    end_line: 2,
                },
                "return 7",
            ),
        ] {
            let id = code
                .store
                .lock()
                .unwrap()
                .find_file_by_path(&path.to_string_lossy())
                .unwrap()
                .unwrap()
                .id;
            let initial = EntryReference {
                result_id: format!("c{id}"),
                source_revision: None,
            };
            let entry = engine.resolve_entry_snapshot(&initial).unwrap();
            let request = PreviewRequest {
                reference: EntryReference {
                    result_id: initial.result_id.clone(),
                    source_revision: Some(entry.source_revision.clone()),
                },
                location: Some(location),
            };
            let cancel = engine.begin_request_for_client(777, 1);
            let preview = engine
                .preview_snapshot(&request, &cancel)
                .expect("cached code uses the same bounded preview route");
            assert_eq!(preview.state, PreviewState::Ready);
            assert_eq!(preview.entry, entry);
            assert!(preview.parts.iter().any(|part| part.text.contains(marker)));
            assert!(preview
                .parts
                .iter()
                .all(|part| part.kind == "code" && !part.text.contains("PRIVATE_OUTPUT")));
            assert!(preview.extraction.iter().all(|item| item.kind == "code"
                && item.complete_within_limit
                && item.source_units.is_none()
                && item.indexed_units.is_none()
                && item.capped.is_none()));
            let selected = engine.indexer.get_indexing_config().unwrap();
            let mut denied = selected.clone();
            denied.code_enabled = false;
            engine.indexer.set_indexing_config(&denied).unwrap();
            assert_eq!(
                engine.preview_snapshot(&request, &cancel).unwrap_err(),
                ErrorCode::NotReady
            );
            engine.indexer.set_indexing_config(&selected).unwrap();
            assert_eq!(
                engine.preview_snapshot(&request, &cancel).unwrap_err(),
                ErrorCode::NotReady
            );
            let current = engine.resolve_entry_snapshot(&initial).unwrap();
            let request = PreviewRequest {
                reference: EntryReference {
                    source_revision: Some(current.source_revision),
                    ..initial
                },
                location: Some(location),
            };
            cancel.store(true, Ordering::Release);
            assert_eq!(
                engine.preview_snapshot(&request, &cancel).unwrap_err(),
                ErrorCode::Cancelled
            );
            cancel.store(false, Ordering::Release);
            fs::write(&path, b"replacement source without the cached marker").unwrap();
            assert_eq!(
                engine.preview_snapshot(&request, &cancel).unwrap_err(),
                ErrorCode::NotReady
            );
            engine.end_request_for_client(777, 1, &cancel);
        }
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_entry_reference_resolves_current_native_hash_and_rejects_old_policy_and_source() {
        let (engine, code, root) = code_search_service();
        let path = root.join("lib.rs");
        let id = code
            .store
            .lock()
            .unwrap()
            .find_file_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let initial = ws_protocol::EntryReference {
            result_id: format!("c{id}"),
            source_revision: None,
        };
        let entry = engine
            .resolve_entry_snapshot(&initial)
            .expect("code uses its actual code-file identity");
        assert_eq!(entry.kind, ResultKind::Code);
        assert_eq!(entry.path, path.to_string_lossy());
        assert_eq!(entry.root_path, root.to_string_lossy());
        assert!(entry.index_revision.is_some_and(|n| n > 0));
        assert_eq!(
            entry.content_sha256.as_deref(),
            Some("36bc22784d3cd4b5e6ae27ab2702f579957349879a55fed91582bb0861a43f9f")
        );
        let bound = ws_protocol::EntryReference {
            result_id: initial.result_id.clone(),
            source_revision: Some(entry.source_revision),
        };
        let selected = engine.indexer.get_indexing_config().unwrap();
        let mut denied = selected.clone();
        denied.code_enabled = false;
        engine.indexer.set_indexing_config(&denied).unwrap();
        assert_eq!(
            engine.resolve_entry_snapshot(&bound).unwrap_err(),
            ErrorCode::NotReady
        );
        engine.indexer.set_indexing_config(&selected).unwrap();
        assert_eq!(
            engine.resolve_entry_snapshot(&bound).unwrap_err(),
            ErrorCode::NotReady
        );
        let current = engine.resolve_entry_snapshot(&initial).unwrap();
        let bound = ws_protocol::EntryReference {
            result_id: initial.result_id.clone(),
            source_revision: Some(current.source_revision),
        };
        std::fs::write(&path, b"fn replacement_with_different_bytes() {}\n").unwrap();
        assert_eq!(
            engine.resolve_entry_snapshot(&bound).unwrap_err(),
            ErrorCode::NotReady
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let current = loop {
            match engine.resolve_entry_snapshot(&initial) {
                Ok(entry) => break entry,
                Err(ErrorCode::NotReady) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                response => panic!("fresh code metadata did not settle: {response:?}"),
            }
        };
        assert_eq!(current.index_revision, None);
        assert_eq!(
            current.content_sha256, None,
            "old byte hashes cannot describe changed source"
        );
        drop(engine);
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn preview_delivery_rechecks_cancellation_identity_and_source_after_waiting_to_write() {
        use ws_protocol::{EntryReference, PreviewRequest};
        let tree = TempTree::new();
        let source = tree.0.join("delivery.txt");
        fs::write(&source, "cached text to withhold").unwrap();
        let engine = service_with(&tree, &[]);
        engine
            .indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                Err("no model".into())
            })
            .unwrap();
        let entry = engine
            .indexer
            .store_lock()
            .find_by_path(&source.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let request = PreviewRequest {
            reference: EntryReference {
                result_id: format!("e{entry}"),
                source_revision: None,
            },
            location: None,
        };
        let cancel = engine.begin_request_for_client(7, 1);
        let preview = engine.preview_snapshot(&request, &cancel).unwrap();
        let response = || ServerMessage::EntryPreview {
            id: 1,
            preview: preview.clone(),
        };
        assert!(matches!(
            engine.preview_response_for_delivery(7, 1, &cancel, response()),
            Some(ServerMessage::EntryPreview { .. })
        ));
        engine.cancel_for_client(7, 1);
        assert!(matches!(
            engine.preview_response_for_delivery(7, 1, &cancel, response()),
            Some(ServerMessage::Error {
                code: ErrorCode::Cancelled,
                ..
            })
        ));
        cancel.store(false, Ordering::Release);
        fs::write(&source, "new source bytes with different length").unwrap();
        assert!(matches!(
            engine.preview_response_for_delivery(7, 1, &cancel, response()),
            Some(ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            })
        ));
        let mut config = engine.indexer.get_indexing_config().unwrap();
        config.roots.clear();
        engine.indexer.set_indexing_config(&config).unwrap();
        assert!(matches!(
            engine.preview_response_for_delivery(7, 1, &cancel, response()),
            Some(ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            })
        ));
        let _replacement = engine.begin_request_for_client(7, 1);
        assert!(engine
            .preview_response_for_delivery(7, 1, &cancel, response())
            .is_none());
    }

    #[test]
    fn preview_returns_current_bounded_cached_text_while_paused_and_rejects_changed_sources() {
        use ws_protocol::{EntryReference, PreviewRequest, PreviewState};
        let tree = TempTree::new();
        let source = tree.0.join("preview.txt");
        fs::write(&source, "Header\nActual source text\nLast line").unwrap();
        let engine = service_with(&tree, &[]);
        engine
            .indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                Err("no model".into())
            })
            .unwrap();
        let id = engine
            .indexer
            .store_lock()
            .find_by_path(&source.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let mut config = engine.indexer.get_indexing_config().unwrap();
        config.paused = true;
        engine.indexer.set_indexing_config(&config).unwrap();
        let request = PreviewRequest {
            reference: EntryReference {
                result_id: format!("e{id}"),
                source_revision: None,
            },
            location: None,
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let preview = engine
            .preview_snapshot(&request, &cancel)
            .expect("already indexed preview must work while paused without a model");
        assert_eq!(preview.state, PreviewState::Ready);
        assert_eq!(
            preview
                .parts
                .iter()
                .map(|p| p.text.as_str())
                .collect::<String>(),
            "Header\nActual source text\nLast line"
        );
        assert_eq!(preview.extraction.len(), 1);
        assert!(preview.extraction[0].complete_within_limit);
        assert_eq!(
            preview.extraction[0].source_units, None,
            "completion is not full source coverage"
        );
        let mut bound = request.clone();
        bound.reference.source_revision = Some(preview.entry.source_revision);
        cancel.store(true, Ordering::Release);
        assert_eq!(
            engine.preview_snapshot(&bound, &cancel).unwrap_err(),
            ErrorCode::Cancelled
        );
        cancel.store(false, Ordering::Release);
        fs::write(&source, "replacement file bytes").unwrap();
        assert_eq!(
            engine.preview_snapshot(&bound, &cancel).unwrap_err(),
            ErrorCode::NotReady
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let unavailable = loop {
            match engine.preview_snapshot(&request, &cancel) {
                Ok(preview) => break preview,
                Err(ErrorCode::NotReady) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                response => {
                    panic!("fresh metadata did not settle after source change: {response:?}")
                }
            }
        };
        assert_eq!(unavailable.state, PreviewState::NotIndexed);
        assert!(
            unavailable.parts.is_empty(),
            "old cache cannot be relabelled as the replacement source"
        );
        config.roots.clear();
        engine.indexer.set_indexing_config(&config).unwrap();
        assert_eq!(
            engine.preview_snapshot(&request, &cancel).unwrap_err(),
            ErrorCode::NotReady
        );
    }

    #[test]
    fn calculator_results_are_synthesized() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let results = engine.plan_results(&query("2 + 2 * 3"));
        assert_eq!(results[0].kind, ResultKind::Calculator);
        assert_eq!(results[0].subtitle, "8");
    }

    #[test]
    fn calculator_errors_are_reported_honestly() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let results = engine.plan_results(&query("1 / 0"));
        assert_eq!(results[0].kind, ResultKind::Calculator);
        assert!(results[0].title.contains("division by zero"));
    }

    #[test]
    fn settings_results_are_synthesized() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let results = engine.plan_results(&query("windows update"));
        assert!(results.iter().any(|r| r.kind == ResultKind::Settings));
    }

    #[test]
    fn file_hits_come_from_the_index() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["meeting-notes.txt"]);
        let results = engine.plan_results(&query("meeting"));
        assert!(
            results.iter().any(|r| r.title == "meeting-notes.txt"
                && matches!(r.kind, ResultKind::File | ResultKind::Document)),
            "got {results:?}"
        );
    }

    #[test]
    fn task2_no_model_result_carries_actual_excerpt_and_line() {
        let tree = TempTree::new();
        fs::write(
            tree.0.join("invoice.txt"),
            "Header\nWINSEARCH_INVOICE_42 is outstanding",
        )
        .unwrap();
        let engine = service_with(&tree, &[]);
        engine
            .indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                Err("model unavailable".into())
            })
            .unwrap();
        let results = engine.plan_results(&query("WINSEARCH_INVOICE_42"));
        let result = results.iter().find(|r| r.title == "invoice.txt").unwrap();
        assert!(result
            .excerpt
            .as_ref()
            .unwrap()
            .contains("WINSEARCH_INVOICE_42"));
        assert_eq!(result.location, Some(MatchLocation::Line { line: 2 }));
        assert_eq!(result.matching_moments.len(), 1);
        assert_eq!(result.matching_moments[0].location, result.location);
    }

    #[test]
    fn task2_normalized_fts_excerpts_anchor_primary_and_all_moments() {
        for matched in ["café", "cafe\u{301}"] {
            let tree = TempTree::new();
            // The first source marker is literal data, and multibyte leading text
            // makes an original byte offset differ from a character offset.
            let padding = format!("\u{1}界 {}", "decafélate intro ".repeat(60));
            let body = format!(
                "{padding}\n{matched} invoice detail\n{padding}\n{matched} second detail\n"
            );
            fs::write(tree.0.join("normalized.txt"), &body).unwrap();
            let engine = service_with(&tree, &[]);
            engine
                .indexer
                .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                    Err("model unavailable".into())
                })
                .unwrap();
            let results = engine.plan_results(&query("cafe"));
            let result = results
                .iter()
                .find(|r| r.title == "normalized.txt")
                .unwrap();
            let excerpt = result.excerpt.as_ref().unwrap();
            assert!(
                excerpt.contains(&format!("{matched} invoice detail")),
                "primary excerpt omitted normalized match: {excerpt:?}"
            );
            assert_eq!(result.location, Some(MatchLocation::Line { line: 2 }));
            assert!(result.matching_moments.len() >= 2);
            assert!(result
                .matching_moments
                .iter()
                .any(|m| m.location == Some(MatchLocation::Line { line: 4 })));
            for moment in &result.matching_moments {
                assert!(
                    moment.excerpt.contains(&format!("{matched} ")),
                    "moment omitted normalized match: {:?}",
                    moment.excerpt
                );
                assert!(moment.excerpt.chars().count() <= 240);
                assert!(matches!(
                    moment.location,
                    Some(MatchLocation::Line { line: 2 | 4 })
                ));
            }
            assert_eq!(
                Some(&result.matching_moments[0].excerpt),
                result.excerpt.as_ref()
            );
            assert_eq!(result.matching_moments[0].location, result.location);
        }
    }

    #[test]
    fn task2_adjacent_literal_marker_runs_do_not_shift_public_match_anchor() {
        for (prefix, line) in [
            ("\u{1}".repeat(600), 1),
            (format!("Heading\n{}", "\u{1}".repeat(600)), 2),
            ("\u{1}\u{2}".repeat(300), 1),
            (format!("\u{1}{}", "\u{2}".repeat(600)), 1),
        ] {
            let tree = TempTree::new();
            fs::write(tree.0.join("adjacent.txt"), format!("{prefix}café invoice")).unwrap();
            let engine = service_with(&tree, &[]);
            engine
                .indexer
                .extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                    Err("model unavailable".into())
                })
                .unwrap();
            let results = engine.plan_results(&query("cafe"));
            let result = results.iter().find(|r| r.title == "adjacent.txt").unwrap();
            assert!(
                result.excerpt.as_ref().unwrap().contains("café invoice"),
                "adjacent literal marker run displaced the excerpt"
            );
            assert_eq!(result.location, Some(MatchLocation::Line { line }));
            assert_eq!(result.matching_moments.len(), 1);
            assert_eq!(result.matching_moments[0].location, result.location);
            assert_eq!(
                Some(&result.matching_moments[0].excerpt),
                result.excerpt.as_ref()
            );
            assert!(result.matching_moments[0].excerpt.chars().count() <= 240);
        }
    }

    #[test]
    fn code_mode_returns_no_file_results() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["main.rs"]);
        let mut q = query("main");
        q.mode = SearchMode::Code;
        let results = engine.plan_results(&q);
        assert!(results.iter().all(|r| r.kind != ResultKind::File));
    }

    #[test]
    fn category_filters_restrict_kinds() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["report.pdf"]);
        let mut q = query("report");
        q.categories = vec![ResultKind::Image];
        let results = engine.plan_results(&q);
        assert!(results.iter().all(|r| r.kind != ResultKind::Document));
    }

    #[test]
    fn explicit_apps_and_files_modes_exclude_calculator_and_settings() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["windows update.txt"]);

        let mut apps_calc = query("2 + 2");
        apps_calc.mode = SearchMode::Apps;
        assert!(engine
            .plan_results(&apps_calc)
            .iter()
            .all(|r| r.kind != ResultKind::Calculator));

        let mut files_calc = query("2 + 2");
        files_calc.mode = SearchMode::Files;
        assert!(engine
            .plan_results(&files_calc)
            .iter()
            .all(|r| r.kind != ResultKind::Calculator));

        let mut apps_settings = query("windows update");
        apps_settings.mode = SearchMode::Apps;
        assert!(engine
            .plan_results(&apps_settings)
            .iter()
            .all(|r| r.kind != ResultKind::Settings));
    }

    #[test]
    fn non_index_category_filter_excludes_file_and_app_hits() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &["windows update.txt"]);
        let mut q = query("windows update");
        q.categories = vec![ResultKind::Settings];
        let results = engine.plan_results(&q);
        assert!(
            results.iter().all(|r| r.kind == ResultKind::Settings),
            "expected only Settings results, got {results:?}"
        );
    }

    #[test]
    fn client_scoped_cancellation_does_not_cancel_other_client_same_request_id() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let flag_a = engine.begin_request_for_client(1, 1);
        let flag_b = engine.begin_request_for_client(2, 1);

        engine.cancel_for_client(2, 1);
        assert!(
            !flag_a.load(Ordering::Relaxed),
            "client 1's request id 1 must not be cancelled by client 2"
        );
        assert!(
            flag_b.load(Ordering::Relaxed),
            "client 2's request id 1 must be cancelled"
        );
        engine.end_request_for_client(1, 1, &flag_a);
        engine.end_request_for_client(2, 1, &flag_b);
    }

    #[test]
    fn status_reports_segments_and_embedding_activity() {
        // P3.5: `segments_indexed` counts name-segments; pending name jobs
        // surface as "Embedding names…" (crawl drained, embed backlog live).
        let tree = TempTree::new();
        for f in ["alpha-embed.txt", "beta-embed.txt"] {
            fs::write(tree.0.join(f), b"x").unwrap();
        }
        let store = fixture_store();
        let indexer = Indexer::open(store, std::slice::from_ref(&tree.0)).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let engine = EngineService::new(indexer);

        let s = engine.status();
        assert_eq!(s.segments_indexed, 0);
        assert_eq!(
            s.index_id.as_deref(),
            Some(engine.indexer.store_lock().database_id())
        );
        assert!(
            s.code_index_id.is_none(),
            "an unavailable code store has no invented identity"
        );
        assert!(!s.model_loaded, "fresh supervisor spawns nothing");
        assert_eq!(s.current_activity, "Embedding names…");
        assert_eq!(s.state, EngineRunState::Indexing);

        // Drain via deterministic fakes: segments land, activity idles.
        let idx = engine.indexer.clone();
        loop {
            let out = idx
                .embed_once_with(|texts| {
                    Ok(ws_embed_protocol::stub_embed_texts(
                        texts,
                        ws_embed_protocol::STUB_DIM,
                    ))
                })
                .unwrap();
            if out.claimed == 0 {
                break;
            }
        }
        let s = engine.status();
        assert!(s.segments_indexed >= 2, "got {}", s.segments_indexed);
        assert_eq!(s.current_activity, String::new());
        assert_eq!(s.state, EngineRunState::Idle);
    }

    // ---- semantic query stage (P3.6) ----

    #[test]
    fn hybrid_band_maps_cosine_below_prefix_above_fts() {
        assert_eq!(MIN_SEMANTIC_COSINE, 0.65);
        assert_eq!(SEMANTIC_TOP_K, 200);
        assert_eq!(QUERY_CACHE_CAP, 64);
        // Band edges: cosine 0 → 0.45, cosine 1 → 0.90.
        assert!((hybrid_base_score(0.0) - 0.45).abs() < 1e-12);
        assert!((hybrid_base_score(1.0) - 0.90).abs() < 1e-12);
        // Monotonic across the band.
        let mut prev = hybrid_base_score(0.0);
        for i in 1..=10 {
            let s = hybrid_base_score(i as f32 / 10.0);
            assert!(s > prev, "band must rise with cosine");
            prev = s;
        }
        // Below exact-prefix (0.92), and the threshold lands above FTS (0.5).
        assert!(hybrid_base_score(1.0) < 0.92);
        assert!(hybrid_base_score(MIN_SEMANTIC_COSINE) > 0.5);
    }

    #[test]
    fn application_query_uses_search_task_prefix() {
        let (engine, _tree) = semantic_service();
        engine.set_query_embed_hook(Arc::new(|text| {
            assert_eq!(text, "task: search result | query: find borrowed books");
            Ok(vec![1.0, 0.0, 0.0])
        }));
        engine.embed_query("  find borrowed books  ").unwrap();
    }

    #[test]
    fn application_rejects_low_confidence_semantic_hits() {
        let (engine, _tree) = semantic_service();
        engine.set_query_embed_hook(Arc::new(|_| {
            Ok(vec![0.64, 0.0, (1.0_f32 - 0.64 * 0.64).sqrt()])
        }));
        assert!(
            engine
                .plan_semantic(&semantic_query("distinct concept"), &HashSet::new())
                .is_empty(),
            "0.64 cosine is below the calibrated application admission floor"
        );
    }

    #[test]
    fn exact_token_boost_rewards_real_words_only() {
        assert_eq!(exact_token_boost("beach sunset", "beach-sunset.jpg"), 0.10);
        assert_eq!(exact_token_boost("MEETING", "meeting-notes.txt"), 0.10);
        assert_eq!(exact_token_boost("beach", "mountain-cabin.jpg"), 0.0);
        assert_eq!(exact_token_boost("", "meeting-notes.txt"), 0.0);
    }

    #[test]
    fn recency_boost_is_small_and_recent_only() {
        let now = 1_800_000_000i64;
        assert_eq!(recency_boost(now, now), 0.02);
        assert_eq!(recency_boost(now - 29 * 86_400, now), 0.02);
        assert_eq!(recency_boost(now - 60 * 86_400, now), 0.0);
        assert_eq!(recency_boost(0, now), 0.0);
    }

    #[test]
    fn hybrid_score_combines_band_and_boosts() {
        let now = 1_800_000_000i64;
        // Band top + token + recency clamps at 1.0.
        assert_eq!(
            hybrid_score(1.0, "banquet hall", "banquet-hall.txt", now, now),
            1.0
        );
        // No boosts: pure band value.
        assert!((hybrid_score(0.5, "zzz", "unrelated.txt", 0, now) - 0.675).abs() < 1e-9);
    }

    #[test]
    fn semantic_explanation_names_similarity() {
        assert_eq!(
            semantic_explanation(0.723),
            "semantic name match · similarity 0.72"
        );
        assert_eq!(
            semantic_explanation(1.0),
            "semantic name match · similarity 1.00"
        );
    }

    #[test]
    fn query_cache_evicts_lru_at_capacity() {
        let mut cache = QueryEmbeddingCache::new(2);
        cache.put("a".into(), vec![1.0]);
        cache.put("b".into(), vec![2.0]);
        // Refreshing `a` makes `b` the eviction victim.
        assert_eq!(cache.get("a"), Some(vec![1.0]));
        cache.put("c".into(), vec![3.0]);
        assert_eq!(cache.get("a"), Some(vec![1.0]));
        assert_eq!(cache.get("b"), None);
        assert_eq!(cache.get("c"), Some(vec![3.0]));
        // Replacing a live key keeps the slot count, not a duplicate.
        cache.put("a".into(), vec![9.0]);
        assert_eq!(cache.get("a"), Some(vec![9.0]));
        assert_eq!(cache.get("c"), Some(vec![3.0]));
    }

    #[test]
    fn query_cache_holds_configured_capacity() {
        let mut cache = QueryEmbeddingCache::new(QUERY_CACHE_CAP);
        for i in 0..QUERY_CACHE_CAP {
            cache.put(format!("q{i}"), vec![i as f32]);
        }
        assert_eq!(cache.get("q0"), Some(vec![0.0]));
        cache.put("overflow".into(), vec![-1.0]);
        assert_eq!(cache.get("q1"), None, "oldest must evict at 64");
        assert_eq!(cache.get("q0"), Some(vec![0.0]));
        assert_eq!(cache.get("overflow"), Some(vec![-1.0]));
    }

    #[test]
    fn exact_entry_ids_parses_index_hits_only() {
        let results = vec![
            SearchResult {
                id: "e42".into(),
                ..Default::default()
            },
            SearchResult {
                id: "calc".into(),
                ..Default::default()
            },
            SearchResult {
                id: "settings-0".into(),
                ..Default::default()
            },
            SearchResult {
                id: "s42".into(),
                ..Default::default()
            },
        ];
        let ids = exact_entry_ids(&results);
        assert_eq!(ids, HashSet::from([42]));
    }

    /// Engine with crafted name vectors (no worker): the `banquet` entry
    /// aligns with the fake query vector `[1,0,0]`, every other entry is
    /// orthogonal to it. Model readiness and query embeds are faked too.
    fn semantic_service() -> (SharedEngine, TempTree) {
        let tree = TempTree::new();
        for f in ["meeting-notes.txt", "banquet-hall.txt", "unrelated-zzz.txt"] {
            fs::write(tree.0.join(f), b"x").unwrap();
        }
        let store = fixture_store();
        let indexer = Indexer::open(store, std::slice::from_ref(&tree.0)).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let engine = EngineService::new(indexer);
        engine
            .indexer
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
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0, 0.0])));
        (engine, tree)
    }

    fn semantic_query(text: &str) -> SearchQuery {
        SearchQuery {
            text: text.into(),
            limit: 20,
            include_semantic: true,
            ..Default::default()
        }
    }

    #[test]
    fn source_guard_delivery_rechecks_content_after_ranking_without_watcher() {
        let tree = TempTree::new();
        let path = tree.0.join("sampletoken-notes.txt");
        fs::write(&path, "sampletoken documentonlytoken passage").unwrap();
        let indexer = Indexer::open(fixture_store(), std::slice::from_ref(&tree.0)).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |texts| {
                Ok(vec![vec![1.0, 0.0]; texts.len()])
            })
            .unwrap();
        let engine = EngineService::new(indexer);
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0])));
        let mut exact_sources = HashMap::new();
        let exact =
            engine.plan_results_with_sources(&query("documentonlytoken"), &mut exact_sources);
        assert_eq!(exact.len(), 1);
        let mut name_sources = HashMap::new();
        let names = engine.plan_results_with_sources(&query("sampletoken"), &mut name_sources);
        assert_eq!(names.len(), 1);
        assert!(!names[0].matching_moments.is_empty());
        let mut semantic_sources = HashMap::new();
        let semantic = engine.plan_semantic_with_sources(
            &semantic_query("indirect concept"),
            &HashSet::new(),
            &mut semantic_sources,
        );
        assert_eq!(semantic.len(), 1);
        let epoch = engine.indexer.search_epoch();
        fs::write(&path, "replacement source").unwrap();
        assert_eq!(
            engine.indexer.search_epoch(),
            epoch,
            "test must not rely on watcher notification"
        );
        assert!(
            engine.validate_delivery(exact, &exact_sources).is_empty(),
            "old exact excerpt reached delivery"
        );
        assert!(
            engine
                .validate_delivery(semantic, &semantic_sources)
                .is_empty(),
            "old semantic excerpt reached delivery"
        );
        let names = engine.validate_delivery(names, &name_sources);
        assert_eq!(names.len(), 1, "valid filename must remain available");
        assert!(names[0].excerpt.is_none());
        assert!(names[0].location.is_none());
        assert!(names[0].thumbnail_path.is_none());
        assert!(names[0].matching_moments.is_empty());
    }

    #[test]
    fn media_semantic_results_preserve_ranked_moments_thumbnails_and_file_filters() {
        use ws_index::model::{ContentChunk, ContentMatch, DraftEmbeddingInput, NewSegment};
        let tree = TempTree::new();
        let engine = service_with(&tree, &["clip.mp4", "song.wav", "archive.pdf"]);
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0])));
        let (clip, song, pdf) = {
            let mut store = engine.indexer.store_lock();
            let clip = store
                .find_by_path(&tree.0.join("clip.mp4").to_string_lossy())
                .unwrap()
                .unwrap()
                .id;
            let song = store
                .find_by_path(&tree.0.join("song.wav").to_string_lossy())
                .unwrap()
                .unwrap()
                .id;
            let pdf = store
                .find_by_path(&tree.0.join("archive.pdf").to_string_lossy())
                .unwrap()
                .unwrap()
                .id;
            let job = store
                .claim_extract_jobs_batch_for_kind("video", 1)
                .unwrap()
                .pop()
                .unwrap();
            let progress = store
                .begin_content_batch(
                    clip,
                    "video",
                    &crate::extract::source_snapshot(&tree.0.join("clip.mp4")),
                )
                .unwrap();
            let thumbnail = tree
                .0
                .join("cached-frame.png")
                .to_string_lossy()
                .into_owned();
            let chunks = [
                ContentChunk {
                    id: 0,
                    entry_id: clip,
                    job_kind: "video".into(),
                    kind: "video".into(),
                    chunk_index: 65536,
                    text: "".into(),
                    location: ContentMatch {
                        start_ms: Some(33000),
                        end_ms: Some(33000),
                        thumbnail_path: Some(thumbnail.clone()),
                        ..Default::default()
                    },
                    embed: true,
                    visual: true,
                    attempts: 0,
                    embedding_input: DraftEmbeddingInput::Image {
                        path: thumbnail.clone().into(),
                        token_budget: 140,
                    },
                },
                ContentChunk {
                    id: 0,
                    entry_id: clip,
                    job_kind: "video".into(),
                    kind: "ocr".into(),
                    chunk_index: 65537,
                    text: "DEVICE CONNECTION LOST".into(),
                    location: ContentMatch {
                        start_ms: Some(33000),
                        end_ms: Some(33000),
                        thumbnail_path: Some(thumbnail.clone()),
                        ..Default::default()
                    },
                    embed: true,
                    visual: false,
                    attempts: 0,
                    embedding_input: DraftEmbeddingInput::Text,
                },
                ContentChunk {
                    id: 0,
                    entry_id: clip,
                    job_kind: "video".into(),
                    kind: "audio".into(),
                    chunk_index: 1 << 62,
                    text: "".into(),
                    location: ContentMatch {
                        start_ms: Some(50000),
                        end_ms: Some(65000),
                        ..Default::default()
                    },
                    embed: true,
                    visual: false,
                    attempts: 0,
                    embedding_input: DraftEmbeddingInput::Audio {
                        path: tree.0.join("window.wav"),
                    },
                },
            ];
            store
                .commit_content_batch(&job, &progress, &chunks, None)
                .unwrap();
            for (chunk, vector) in
                chunks
                    .iter()
                    .zip([vec![0.8, 0.6], vec![0.9, 0.4358899], vec![1.0, 0.0]])
            {
                store
                    .upsert_segment(NewSegment {
                        entry_id: clip,
                        kind: &chunk.kind,
                        chunk_index: chunk.chunk_index,
                        text: &chunk.text,
                        page: None,
                        start_ms: chunk.location.start_ms.map(|v| v as i64),
                        end_ms: chunk.location.end_ms.map(|v| v as i64),
                        line: None,
                        vector: &vector,
                    })
                    .unwrap();
            }
            for (entry, kind, file, text, location) in [
                (
                    song,
                    "audio",
                    "song.wav",
                    "",
                    ContentMatch {
                        start_ms: Some(7000),
                        end_ms: Some(8000),
                        ..Default::default()
                    },
                ),
                (
                    pdf,
                    "pdf",
                    "archive.pdf",
                    "Actual legacy page excerpt",
                    ContentMatch {
                        page: Some(35),
                        ..Default::default()
                    },
                ),
            ] {
                let job = store
                    .claim_extract_jobs_batch_for_kind(kind, 1)
                    .unwrap()
                    .pop()
                    .unwrap();
                let progress = store
                    .begin_content_batch(
                        entry,
                        kind,
                        &crate::extract::source_snapshot(&tree.0.join(file)),
                    )
                    .unwrap();
                let chunk = ContentChunk {
                    id: 0,
                    entry_id: entry,
                    job_kind: kind.into(),
                    kind: kind.into(),
                    chunk_index: 0,
                    text: text.into(),
                    location,
                    embed: true,
                    visual: false,
                    attempts: 0,
                    embedding_input: DraftEmbeddingInput::Text,
                };
                assert!(store
                    .commit_content_batch(&job, &progress, &[chunk], None)
                    .unwrap());
            }
            store
                .upsert_segment(NewSegment {
                    entry_id: song,
                    kind: "audio",
                    chunk_index: 0,
                    text: "",
                    page: None,
                    start_ms: Some(7000),
                    end_ms: Some(8000),
                    line: None,
                    vector: &[1.0, 0.0],
                })
                .unwrap();
            store
                .upsert_segment(NewSegment {
                    entry_id: pdf,
                    kind: "pdf",
                    chunk_index: 0,
                    text: "Actual legacy page excerpt",
                    page: Some(35),
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &[1.0, 0.0],
                })
                .unwrap();
            (clip, song, pdf)
        };
        let mut q = semantic_query("indirect description");
        q.categories = vec![ResultKind::Video];
        let results = engine.plan_semantic(&q, &HashSet::new());
        assert_eq!(results.len(), 1);
        let hit = &results[0];
        assert_eq!(hit.id, format!("s{clip}"));
        assert_eq!(
            hit.location,
            Some(MatchLocation::Timestamp {
                seconds: 50.0,
                end_seconds: 65.0
            })
        );
        assert!(
            hit.match_explanation.contains("sound"),
            "{}",
            hit.match_explanation
        );
        assert_eq!(
            hit.matching_moments.len(),
            2,
            "OCR/frame at the same time should share one moment"
        );
        assert!(
            hit.matching_moments[0].excerpt.is_empty(),
            "do not invent a transcript"
        );
        assert_eq!(
            hit.matching_moments[1].location,
            Some(MatchLocation::Timestamp {
                seconds: 33.0,
                end_seconds: 33.0
            })
        );
        assert!(hit.matching_moments[1].excerpt.contains("DEVICE"));
        assert!(hit.matching_moments[1]
            .thumbnail_path
            .as_ref()
            .unwrap()
            .ends_with("cached-frame.png"));
        assert!(
            hit.thumbnail_path.is_none(),
            "sound match must not substitute an unrelated first-frame thumbnail"
        );
        q.categories = vec![ResultKind::Audio];
        let audio = engine.plan_semantic(&q, &HashSet::new());
        assert_eq!(audio.len(), 1);
        assert_eq!(audio[0].id, format!("s{song}"));
        assert_eq!(
            audio[0].location,
            Some(MatchLocation::Timestamp {
                seconds: 7.0,
                end_seconds: 8.0
            })
        );
        q.categories = vec![ResultKind::Document];
        let pages = engine.plan_semantic(&q, &HashSet::new());
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, format!("s{pdf}"));
        assert_eq!(pages[0].location, Some(MatchLocation::Page { page: 35 }));
        assert!(pages[0].excerpt.as_ref().unwrap().contains("Actual legacy"));
        let mut frame = engine
            .indexer
            .store_lock()
            .list_segments_for_entry(clip)
            .unwrap()
            .into_iter()
            .find(|s| s.kind == "video")
            .unwrap();
        assert!(engine.indexer.segment_thumbnail(&frame).unwrap().is_some());
        frame.start_ms = Some(33001);
        assert!(
            engine.indexer.segment_thumbnail(&frame).unwrap().is_none(),
            "never pair a stale vector with another chunk's pixels"
        );
        frame.start_ms = Some(-1);
        assert_eq!(segment_location(&frame), None);
        let mut config = engine.indexer.get_indexing_config().unwrap();
        config.roots.clear();
        engine.indexer.set_indexing_config(&config).unwrap();
        assert!(engine.plan_semantic(&q, &HashSet::new()).is_empty());
        assert!(engine.indexer.segment_thumbnail(&frame).unwrap().is_none());
    }

    #[test]
    fn semantic_gate_requires_request_policy_and_model() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);

        // Not requested → closed (no hook set, so any embed attempt would
        // fail loudly instead of silently passing).
        assert!(!engine.semantic_allowed(&query("meeting")));
        // Requested but no model (fresh supervisor, no override) → closed.
        let mut q = query("meeting");
        q.include_semantic = true;
        assert!(!engine.semantic_allowed(&q));
        // Empty text → closed even when forced ready.
        engine.set_model_ready_override(Some(true));
        let q = semantic_query("   ");
        assert!(!engine.semantic_allowed(&q));
        // `code:` never runs the semantic stage.
        let mut q = semantic_query("meeting");
        q.mode = SearchMode::Code;
        assert!(!engine.semantic_allowed(&q));
        // Requested + ready + policy default → open.
        assert!(engine.semantic_allowed(&semantic_query("meeting")));
        // Policy kill switch → closed.
        engine
            .indexer
            .set_engine_config(&ws_index::EngineConfig {
                semantic_enabled: false,
                preferred_backend: ws_index::PreferredBackend::Auto,
            })
            .unwrap();
        assert!(!engine.semantic_allowed(&semantic_query("meeting")));
    }

    #[test]
    fn semantic_outcomes_distinguish_disabled_missing_model_and_actual_ready_lookup() {
        let (engine, _tree) = semantic_service();
        let state = |request: &SearchQuery| {
            engine.plan_semantic_outcome(request, &HashSet::new(), &mut HashMap::new())
        };
        assert_eq!(state(&query("meeting")).1, SemanticOutcome::DISABLED);
        engine.set_model_ready_override(Some(false));
        assert_eq!(
            state(&semantic_query("meeting")).1,
            SemanticOutcome::degraded(SemanticIssue::ModelUnavailable)
        );
        engine.set_model_ready_override(Some(true));
        let (hits, outcome) = state(&semantic_query("indirect banquet"));
        assert!(!hits.is_empty());
        assert_eq!(outcome, SemanticOutcome::READY);
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![0.0, 0.0, 1.0])));
        let (hits, outcome) = state(&semantic_query("orthogonal concept"));
        assert!(hits.is_empty());
        assert_eq!(
            outcome,
            SemanticOutcome::READY,
            "valid below-threshold lookup is not unavailable"
        );
        engine
            .indexer
            .set_engine_config(&ws_index::EngineConfig {
                semantic_enabled: false,
                preferred_backend: ws_index::PreferredBackend::Auto,
            })
            .unwrap();
        assert_eq!(
            state(&semantic_query("policy disabled")).1,
            SemanticOutcome::DISABLED
        );
    }

    #[test]
    fn semantic_outcomes_expose_bounded_embedding_failure_and_preserve_exact_hits() {
        let (engine, _tree) = semantic_service();
        engine.set_query_embed_hook(Arc::new(|_| {
            Err("PRIVATE worker detail must never be a wire reason".into())
        }));
        assert!(!engine.plan_results(&query("meeting")).is_empty());
        let (hits, outcome) = engine.plan_semantic_outcome(
            &semantic_query("meeting"),
            &HashSet::new(),
            &mut HashMap::new(),
        );
        assert!(hits.is_empty());
        assert_eq!(
            outcome,
            SemanticOutcome::degraded(SemanticIssue::EmbeddingFailed)
        );
        assert!(!serde_json::to_string(&outcome).unwrap().contains("PRIVATE"));
    }

    #[test]
    fn semantic_outcomes_separate_no_vectors_and_real_sql_retrieval_failure() {
        let (engine, _tree) = semantic_service();
        engine
            .indexer
            .store_lock()
            .set_query_source_guard(|_, _, _| panic!("PRIVATE SQLite callback detail"))
            .unwrap();
        let (hits, outcome) = engine.plan_semantic_outcome(
            &semantic_query("lookup fails"),
            &HashSet::new(),
            &mut HashMap::new(),
        );
        assert!(hits.is_empty());
        assert_eq!(
            outcome,
            SemanticOutcome::degraded(SemanticIssue::RetrievalFailed)
        );
        engine
            .indexer
            .store_lock()
            .purge_segments_by_kind("name")
            .unwrap();
        engine.indexer.invalidate_vector_cache();
        let (hits, outcome) = engine.plan_semantic_outcome(
            &semantic_query("empty vector store"),
            &HashSet::new(),
            &mut HashMap::new(),
        );
        assert!(hits.is_empty());
        assert_eq!(outcome, SemanticOutcome::degraded(SemanticIssue::NoVectors));
    }

    #[test]
    fn streaming_terminal_outcomes_preserve_exact_when_semantics_fails() {
        let (engine, _tree) = semantic_service();
        engine.set_query_embed_hook(Arc::new(|_| Err("PRIVATE worker detail".into())));
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 83, semantic_query("meeting"));
        let mut stream = connect_retry(&pipe);
        let first: ServerMessage = ws_protocol::read_message(&mut stream).unwrap().unwrap();
        assert!(
            matches!(first,ServerMessage::Results {ref batch,done:false,outcome:None,..} if !batch.is_empty())
        );
        let terminal: ServerMessage = ws_protocol::read_message(&mut stream).unwrap().unwrap();
        match terminal {
            ServerMessage::Results {
                batch,
                done,
                outcome,
                ..
            } => {
                assert!(done && batch.is_empty());
                assert_eq!(
                    outcome,
                    Some(SearchOutcome::complete(SemanticOutcome::degraded(
                        SemanticIssue::EmbeddingFailed
                    )))
                );
            }
            other => panic!("unexpected terminal: {other:?}"),
        }
    }

    #[test]
    fn semantic_outcomes_require_valid_vectors_and_current_code_fingerprint() {
        let (engine, _tree) = semantic_service();
        for (index, vector) in [
            vec![],
            vec![0.0, 0.0, 0.0],
            vec![f32::NAN, 0.0, 0.0],
            vec![f32::INFINITY, 0.0, 0.0],
        ]
        .into_iter()
        .enumerate()
        {
            engine.set_query_embed_hook(Arc::new(move |_| Ok(vector.clone())));
            let (_, outcome) = engine.plan_semantic_outcome(
                &semantic_query(&format!("bad vector {index}")),
                &HashSet::new(),
                &mut HashMap::new(),
            );
            assert_eq!(
                outcome,
                SemanticOutcome::degraded(SemanticIssue::EmbeddingFailed)
            );
        }
        let (engine, code, root) = code_search_service();
        crate::code::tests::stop_fixture_watchers(&code);
        let mut request = semantic_query("conceptual code description");
        request.mode = SearchMode::Code;
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![0.0, 1.0, 0.0])));
        let (hits, outcome) =
            engine.plan_semantic_outcome(&request, &HashSet::new(), &mut HashMap::new());
        assert!(hits.is_empty());
        assert_eq!(outcome, SemanticOutcome::READY);
        engine.indexer.store_lock().reset_content_vectors().unwrap();
        let (_, outcome) =
            engine.plan_semantic_outcome(&request, &HashSet::new(), &mut HashMap::new());
        assert_eq!(outcome, SemanticOutcome::degraded(SemanticIssue::NoVectors));
        engine.code.lock().unwrap().take();
        drop(code);
        drop(engine);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn streaming_terminal_outcomes_report_exact_sql_failure_instead_of_empty_success() {
        let (engine, _tree) = semantic_service();
        engine
            .indexer
            .store_lock()
            .set_query_source_guard(|_, _, _| panic!("PRIVATE exact SQL detail"))
            .unwrap();
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 84, query("meeting"));
        let mut stream = connect_retry(&pipe);
        match ws_protocol::read_message::<_, ServerMessage>(&mut stream)
            .unwrap()
            .unwrap()
        {
            ServerMessage::Error {
                code: ErrorCode::Internal,
                message,
                ..
            } => assert_eq!(message, "search request failed"),
            other => panic!("exact failures must be errors: {other:?}"),
        }
        match ws_protocol::read_message::<_, ServerMessage>(&mut stream)
            .unwrap()
            .unwrap()
        {
            ServerMessage::Results {
                batch,
                done,
                outcome,
                ..
            } => {
                assert!(done && batch.is_empty());
                assert_eq!(
                    outcome,
                    Some(SearchOutcome::interrupted(SearchDisposition::Failed))
                );
            }
            other => panic!("unexpected failure completion: {other:?}"),
        }
    }

    #[test]
    fn cold_start_model_ready_when_model_service_is_ready() {
        let tree = TempTree::new();
        fs::write(tree.0.join("banquet-hall.txt"), b"x").unwrap();
        let store = fixture_store();
        let indexer = Indexer::open(store, std::slice::from_ref(&tree.0)).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
            .embed_once_with(|texts| Ok(texts.iter().map(|_| vec![1.0, 0.0, 0.0]).collect()))
            .unwrap();

        // Populate a tiny model dir whose manifest file is present on disk so
        // `ModelService::status().state == ModelState::Ready`, while the fresh
        // `EmbedSupervisor` has never spawned a worker (`model_loaded() == false`)
        // and `model_ready_override` is `None`.
        let model_dir = tree.0.join("models-ready");
        fs::create_dir_all(&model_dir).unwrap();
        let body = b"stub-gguf-weights";
        fs::write(model_dir.join("tiny.gguf"), body).unwrap();
        let expected = ws_modelmgr::ExpectedFile {
            name: "tiny.gguf",
            size: body.len() as u64,
            sha256: None,
        };
        let manager = ws_modelmgr::ModelManager::with_files(
            model_dir,
            ws_modelmgr::DownloadOptions::default(),
            vec![expected],
        );
        let models = ModelService::with_manager(manager, Arc::clone(&indexer));
        assert_eq!(models.status().state, ws_modelmgr::ModelState::Ready);

        let engine = EngineService::new_with_models(indexer, models, EmbedSupervisor::new());
        assert!(
            !engine.embed_supervisor().model_loaded(),
            "cold-start supervisor has not spawned the worker yet"
        );
        assert!(
            engine.model_ready(),
            "ready ModelService on disk must open the semantic gate on cold start"
        );
        assert!(engine.semantic_allowed(&semantic_query("meeting")));

        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0, 0.0])));
        let hits = engine.plan_semantic(&semantic_query("meeting"), &HashSet::new());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "banquet-hall.txt");
    }

    #[test]
    fn plan_semantic_appends_deduped_thresholded_hits() {
        let (engine, _tree) = semantic_service();
        let q = semantic_query("meeting");

        let exact = engine.plan_results(&q);
        assert!(
            exact.iter().any(|r| r.title == "meeting-notes.txt"),
            "exact batch must hold the prefix hit, got {exact:?}"
        );
        let exact_ids = exact_entry_ids(&exact);

        let sem = engine.plan_semantic(&q, &exact_ids);
        assert_eq!(sem.len(), 1, "only the banquet entry survives, got {sem:?}");
        let hit = &sem[0];
        assert_eq!(hit.title, "banquet-hall.txt");
        assert_eq!(
            hit.match_explanation,
            "semantic name match · similarity 1.00"
        );
        // Band top (0.90) + no token boost + recency (+0.02, just-written file).
        assert!(
            (hit.score - 0.92).abs() < 1e-9,
            "score {}, want 0.92",
            hit.score
        );
        assert!(hit.id.starts_with('s'));

        // Duplicate suppression: the banquet entry already streamed → nothing.
        let banquet_exact = engine.plan_results(&semantic_query("banquet"));
        let banquet_ids = exact_entry_ids(&banquet_exact);
        assert_eq!(banquet_ids.len(), 1);
        assert!(
            engine.plan_semantic(&q, &banquet_ids).is_empty(),
            "already-streamed entries must be suppressed"
        );

        // Below-threshold vectors (orthogonal query) yield nothing.
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![0.0, 0.0, 1.0])));
        assert!(engine
            .plan_semantic(&semantic_query("zzz-no-exact"), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn embed_query_caches_per_text() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        let (engine, _tree) = semantic_service();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        engine.set_query_embed_hook(Arc::new(move |_| {
            calls2.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let q = semantic_query("meeting");
        let ids = HashSet::new();
        assert_eq!(engine.plan_semantic(&q, &ids).len(), 1);
        assert_eq!(engine.plan_semantic(&q, &ids).len(), 1);
        assert_eq!(
            calls.load(AtomicOrdering::SeqCst),
            1,
            "second query must hit the LRU"
        );
        // A different text re-embeds.
        assert_eq!(
            engine.plan_semantic(&semantic_query("other"), &ids).len(),
            1
        );
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[test]
    fn application_query_rejects_semantic_disable_then_reenable() {
        let (engine, _tree) = semantic_service();
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |_| {
            let policy = indexer.get_engine_config().unwrap();
            let mut disabled = policy.clone();
            disabled.semantic_enabled = false;
            indexer.set_engine_config(&disabled).unwrap();
            indexer.set_engine_config(&policy).unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        assert!(engine
            .plan_semantic(&semantic_query("changing semantic policy"), &HashSet::new())
            .is_empty());
    }

    #[test]
    fn application_query_rejects_root_revoke_then_regrant_during_embedding() {
        let (engine, _tree) = semantic_service();
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |_| {
            let policy = indexer.get_indexing_config().unwrap();
            let mut revoked = policy.clone();
            revoked.roots.clear();
            indexer.set_indexing_config(&revoked).unwrap();
            indexer.set_indexing_config(&policy).unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        assert!(
            engine
                .plan_semantic(&semantic_query("changing authorization"), &HashSet::new())
                .is_empty(),
            "restored permission cannot revive the previous query revision"
        );
    }

    #[test]
    fn application_query_rejects_source_change_during_embedding() {
        let (engine, tree) = semantic_service();
        let indexer = Arc::clone(&engine.indexer);
        let path = tree.0.join("banquet-hall.txt");
        let stamp = fs::metadata(&path).unwrap().modified().unwrap();
        let root = indexer
            .store_lock()
            .roots()
            .unwrap()
            .into_iter()
            .find(|r| r.path == tree.0.to_string_lossy())
            .unwrap()
            .id;
        engine.set_query_embed_hook(Arc::new(move |_| {
            fs::write(&path, b"y").unwrap();
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(stamp))
                .unwrap();
            indexer.handle_event(root, crate::watcher::WatchEvent::Modified(path.clone()));
            Ok(vec![1.0, 0.0, 0.0])
        }));
        assert!(engine.plan_semantic(&semantic_query("changing source"),&HashSet::new()).is_empty(),
            "a source edit invalidates the request even when unchanged name vectors remain searchable");
    }

    #[test]
    fn application_query_cache_is_fenced_by_model_generation() {
        use std::sync::atomic::AtomicUsize;
        let (engine, _tree) = semantic_service();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        engine.set_query_embed_hook(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(vec![1.0, 0.0, 0.0])
        }));
        engine.embed_query("cached topic").unwrap();
        engine.indexer.store_lock().reset_content_vectors().unwrap();
        engine.embed_query("cached topic").unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "old-space query vector was reused after model migration"
        );
    }

    #[test]
    fn application_query_changed_generation_is_never_cached() {
        let (engine, _tree) = semantic_service();
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |_| {
            indexer.store_lock().reset_content_vectors().unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        assert!(
            engine.embed_query("changing topic").is_err(),
            "stale-space response was accepted"
        );
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![0.0, 1.0, 0.0])));
        assert_eq!(
            engine.embed_query("changing topic").unwrap(),
            [0.0, 1.0, 0.0]
        );
    }

    // ---- streaming integration (fake vectors, real pipe frames) ----

    #[test]
    fn application_stream_rejects_revoke_then_regrant_between_stages() {
        let (engine, _tree) = semantic_service();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
        let proceed_rx = Mutex::new(proceed_rx);
        engine.set_query_embed_hook(Arc::new(move |_| {
            entered_tx.send(()).unwrap();
            proceed_rx.lock().unwrap().recv().unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let pipe = unique_pipe();
        serve_search(Arc::clone(&engine), &pipe, 19, semantic_query("meeting"));
        let mut stream = connect_retry(&pipe);
        let (first, done) = read_one(&mut stream);
        assert!(!first.is_empty());
        assert!(!done);
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let policy = engine.indexer.get_indexing_config().unwrap();
        let mut revoked = policy.clone();
        revoked.roots.clear();
        engine.indexer.set_indexing_config(&revoked).unwrap();
        engine.indexer.set_indexing_config(&policy).unwrap();
        proceed_tx.send(()).unwrap();
        let (terminal, done) = read_one(&mut stream);
        assert!(done);
        assert!(
            terminal.is_empty(),
            "permission restoration cannot append to an old ordinary search"
        );
    }

    fn code_search_service() -> (SharedEngine, Arc<crate::code::CodeService>, PathBuf) {
        let (code, indexer, root) = crate::code::tests::indexed_fixture();
        indexer.set_code_service(Arc::downgrade(&code));
        let engine =
            EngineService::new_with_model_dir(indexer, root.parent().unwrap().join("models"));
        *engine.code.lock().unwrap() = Some(Arc::clone(&code));
        engine.set_model_ready_override(Some(true));
        engine.set_query_embed_hook(Arc::new(|_| Ok(vec![1.0, 0.0, 0.0])));
        (engine, code, root)
    }

    #[test]
    fn code_semantic_rejects_uncorroborated_task_affinity() {
        let (engine, code, root) = code_search_service();
        engine.set_query_embed_hook(Arc::new(|input| {
            Ok(if input.starts_with("task: code retrieval | query: ") {
                vec![1.0, 0.0, 0.0]
            } else {
                vec![0.0, 1.0, 0.0]
            })
        }));
        let mut query = semantic_query("unrelated topic");
        query.mode = SearchMode::Code;
        assert!(engine.plan_results(&query).is_empty());
        assert!(
            engine.plan_semantic(&query, &HashSet::new()).is_empty(),
            "code-task affinity alone cannot admit an unrelated source"
        );
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_semantic_query_views_cache_separately_and_keep_the_model_fence() {
        let (engine, code, root) = code_search_service();
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&inputs);
        engine.set_query_embed_hook(Arc::new(move |input| {
            observed.lock().unwrap().push(input.to_owned());
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let mut query = semantic_query("indirect code description");
        query.mode = SearchMode::Code;
        assert!(!engine.plan_semantic(&query, &HashSet::new()).is_empty());
        assert!(!engine.plan_semantic(&query, &HashSet::new()).is_empty());
        assert_eq!(
            *inputs.lock().unwrap(),
            [
                "task: code retrieval | query: indirect code description",
                "task: search result | query: indirect code description",
            ]
        );
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |input| {
            if input.starts_with("task: search result | query: ") {
                indexer.store_lock().reset_content_vectors().unwrap();
            }
            Ok(vec![1.0, 0.0, 0.0])
        }));
        query.text = "changed code topic".into();
        assert!(
            engine.plan_semantic(&query, &HashSet::new()).is_empty(),
            "a model change during corroboration invalidates both task views"
        );
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_zero_limit_uses_same_effective_limit_for_duplicate_suppression() {
        let (engine, code, root) = code_search_service();
        {
            let mut store = code.store.lock().unwrap();
            for file in store.all_files().unwrap() {
                for chunk in store.list_chunks_for_file(file.id).unwrap() {
                    store
                        .set_chunk_vector(
                            chunk.id,
                            if file.name == "lib.rs" {
                                &[1.0, 0.0, 0.0]
                            } else {
                                &[0.0, 1.0, 0.0]
                            },
                        )
                        .unwrap();
                }
            }
        }
        let mut q = semantic_query("needle");
        q.mode = SearchMode::Code;
        q.limit = 0;
        assert_eq!(engine.plan_results(&q).len(), 1);
        assert!(
            engine.plan_semantic(&q, &HashSet::new()).is_empty(),
            "the effective minimum limit cannot append the same code chunk again"
        );
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_search_honors_categories_before_exact_and_semantic_planning() {
        let (engine, code, root) = code_search_service();
        let mut q = semantic_query("needle");
        q.mode = SearchMode::Code;
        assert!(!engine.plan_results(&q).is_empty());
        assert!(!engine.plan_semantic(&q, &HashSet::new()).is_empty());
        q.categories = vec![ResultKind::Video];
        assert!(
            engine.plan_results(&q).is_empty(),
            "video-only filters exclude code exact hits"
        );
        assert!(
            !engine.semantic_allowed(&q),
            "excluded code must not start inference"
        );
        assert!(engine.plan_semantic(&q, &HashSet::new()).is_empty());
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_semantic_rejects_semantic_disable_then_reenable() {
        let (engine, code, root) = code_search_service();
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |_| {
            let policy = indexer.get_engine_config().unwrap();
            let mut disabled = policy.clone();
            disabled.semantic_enabled = false;
            indexer.set_engine_config(&disabled).unwrap();
            indexer.set_engine_config(&policy).unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let mut q = semantic_query("changing semantic policy");
        q.mode = SearchMode::Code;
        assert!(engine.plan_semantic(&q, &HashSet::new()).is_empty());
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_semantic_rejects_revoke_then_regrant_during_embedding() {
        let (engine, code, root) = code_search_service();
        let indexer = Arc::clone(&engine.indexer);
        engine.set_query_embed_hook(Arc::new(move |_| {
            let policy = indexer.get_indexing_config().unwrap();
            let mut revoked = policy.clone();
            revoked.code_roots.clear();
            indexer.set_indexing_config(&revoked).unwrap();
            indexer.set_indexing_config(&policy).unwrap();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let mut q = semantic_query("indirect description");
        q.mode = SearchMode::Code;
        assert!(
            engine.plan_semantic(&q, &HashSet::new()).is_empty(),
            "regrant cannot revive an old request"
        );
        drop(engine);
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn code_stream_rejects_revoke_then_regrant_between_stages() {
        let (engine, code, root) = code_search_service();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
        let proceed_rx = Mutex::new(proceed_rx);
        let first = std::sync::atomic::AtomicBool::new(true);
        engine.set_query_embed_hook(Arc::new(move |_| {
            if first.swap(false, Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                proceed_rx.lock().unwrap().recv().unwrap();
            }
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let mut q = semantic_query("needle");
        q.mode = SearchMode::Code;
        let pipe = unique_pipe();
        serve_search(Arc::clone(&engine), &pipe, 17, q);
        let mut stream = connect_retry(&pipe);
        let (first, done) = read_one(&mut stream);
        assert!(!first.is_empty());
        assert!(!done);
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let policy = engine.indexer.get_indexing_config().unwrap();
        let mut revoked = policy.clone();
        revoked.code_roots.clear();
        engine.indexer.set_indexing_config(&revoked).unwrap();
        engine.indexer.set_indexing_config(&policy).unwrap();
        proceed_tx.send(()).unwrap();
        let (terminal, done) = read_one(&mut stream);
        assert!(done);
        assert!(
            terminal.is_empty(),
            "the streamed append belongs to the revoked request"
        );
        engine.code.lock().unwrap().take();
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    static PIPE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn unique_pipe() -> String {
        let n = PIPE_SEQ.fetch_add(1, Ordering::SeqCst);
        format!(r"\\.\pipe\winsearch-task6-{}-{n}", std::process::id())
    }

    #[test]
    fn ordinary_search_dispatch_does_not_block_control_on_a_busy_store() {
        let indexer = Indexer::open(ws_index::IndexStore::open_in_memory().unwrap(), &[]).unwrap();
        let engine = EngineService::new_with_model_dir(
            indexer,
            std::env::temp_dir().join("winsearch-dispatch-unused"),
        );
        let pipe = unique_pipe();
        let serving = pipe.clone();
        let registering = engine.clone();
        let (registered, clients) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                let client = registering.register_client(Arc::new(Mutex::new(stream)));
                let _ = registered.send(client);
            });
        });
        let mut peer = connect_retry(&pipe);
        let client = clients.recv_timeout(Duration::from_secs(2)).unwrap();
        let guard = engine.indexer.store_lock();
        let searching = engine.clone();
        let searching_client = client.clone();
        let (returned, complete) = std::sync::mpsc::channel();
        let caller = std::thread::spawn(move || {
            searching.search(
                &searching_client,
                911,
                SearchQuery {
                    text: "invoice".into(),
                    include_semantic: false,
                    ..Default::default()
                },
            );
            let _ = returned.send(());
        });
        let dispatched = complete.recv_timeout(Duration::from_millis(500));
        drop(guard);
        caller.join().unwrap();
        while !read_one(&mut peer).1 {}
        engine.unregister_client(&client);
        assert!(
            dispatched.is_ok(),
            "exact planning blocked the protocol/control thread on the store lock"
        );
    }

    #[test]
    fn reset_pipe_drains_work_preserves_policy_and_rejects_replayed_namespaces() {
        let (code, indexer, root) = crate::code::tests::indexed_fixture();
        let normal = root.parent().unwrap().join("ordinary");
        fs::create_dir_all(&normal).unwrap();
        let source = normal.join("invoice.txt");
        fs::write(&source, "current ordinary content").unwrap();
        let mut policy = indexer.get_indexing_config().unwrap();
        policy.roots = vec![normal.to_string_lossy().into()];
        indexer.set_indexing_config(&policy).unwrap();
        while indexer.scan_available(64) > 0 {}
        let old_entry = indexer
            .store_lock()
            .find_by_path(&source.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let old_code = code.store.lock().unwrap().all_files().unwrap()[0].id;
        let models = root.parent().unwrap().join("retained-models");
        fs::create_dir_all(&models).unwrap();
        fs::write(models.join("sentinel"), b"retained model").unwrap();
        let engine = EngineService::new_with_model_dir(indexer.clone(), models.clone());
        *engine.code.lock().unwrap() = Some(code.clone());
        let request = ws_protocol::ResetRequest {
            expected_index_id: indexer.store_lock().database_id().into(),
            expected_code_index_id: code.store.lock().unwrap().database_id().into(),
        };
        let pipe = unique_pipe();
        let serving = pipe.clone();
        let handler = engine.clone();
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                crate::server::handle_connection(stream, handler.clone())
            });
        });
        let mut peer = connect_retry(&pipe);
        ws_protocol::write_message(
            &mut peer,
            &ws_protocol::ClientMessage::Hello {
                protocol_version: 1,
                client: "reset-native-test".into(),
                client_version: "1".into(),
            },
        )
        .unwrap();
        assert!(matches!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap(),
            Some(ServerMessage::HelloAck { .. })
        ));
        let _ = ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap();
        let work = indexer.work_gate.enter().unwrap();
        ws_protocol::write_message(
            &mut peer,
            &ws_protocol::ClientMessage::ResetIndex {
                id: 501,
                request: request.clone(),
            },
        )
        .unwrap();
        ws_protocol::write_message(&mut peer, &ws_protocol::ClientMessage::Ping { id: 502 })
            .unwrap();
        assert_eq!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap(),
            Some(ServerMessage::Pong { id: 502 })
        );
        drop(work);
        let Some(ServerMessage::IndexReset { id: 501, reset }) =
            ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap()
        else {
            panic!("reset acknowledgment");
        };
        assert_ne!(reset.index_id, request.expected_index_id);
        assert_ne!(reset.code_index_id, request.expected_code_index_id);
        assert_eq!(indexer.get_indexing_config().unwrap(), policy);
        assert_eq!(
            fs::read(models.join("sentinel")).unwrap(),
            b"retained model"
        );
        assert_eq!(indexer.store_lock().stats().unwrap().entries_total, 0);
        assert!(code.store.lock().unwrap().all_files().unwrap().is_empty());
        for result_id in [format!("e{old_entry}"), format!("c{old_code}")] {
            assert!(engine
                .resolve_entry_snapshot(&ws_protocol::EntryReference {
                    result_id,
                    source_revision: None
                })
                .is_err());
        }
        ws_protocol::write_message(
            &mut peer,
            &ws_protocol::ClientMessage::ResetIndex { id: 503, request },
        )
        .unwrap();
        assert!(matches!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peer).unwrap(),
            Some(ServerMessage::Error {
                id: Some(503),
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
        assert_eq!(indexer.store_lock().database_id(), reset.index_id);
        while indexer.scan_available(64) > 0 {}
        assert!(
            indexer
                .store_lock()
                .find_by_path(&source.to_string_lossy())
                .unwrap()
                .unwrap()
                .id
                > old_entry
        );
        drop(peer);
        crate::code::tests::stop_fixture_watchers(&code);
        // The accept-loop fixture intentionally outlives this test. Revoke its
        // roots before deleting the test tree so it retains no source grants.
        let mut revoked = policy;
        revoked.roots.clear();
        revoked.code_roots.clear();
        revoked.code_enabled = false;
        indexer.set_indexing_config(&revoked).unwrap();
        *engine.code.lock().unwrap() = None;
        drop(code);
        fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn ordinary_search_admission_counts_canceled_work_until_it_retires() {
        let tree = TempTree::new();
        let engine = service_with(&tree, &[]);
        let pipe = unique_pipe();
        let serving = pipe.clone();
        let registering = engine.clone();
        let (registered, clients) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                let _ = registered.send(registering.register_client(Arc::new(Mutex::new(stream))));
            });
        });
        let mut peers = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..9 {
            peers.push(connect_retry(&pipe));
            handles.push(clients.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        let guard = engine.indexer.store_lock();
        let request = SearchQuery {
            text: "invoice".into(),
            ..Default::default()
        };
        for handle in &handles[..8] {
            engine.search(handle, 1, request.clone());
        }
        assert_eq!(engine.search_jobs.load(Ordering::Acquire), 8);
        engine.search(&handles[8], 1, request.clone());
        assert!(matches!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peers[8]).unwrap(),
            Some(ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            })
        ));
        let _ = read_one(&mut peers[8]);
        for handle in &handles[..8] {
            engine.cancel_for_client(handle.id(), 1);
        }
        // Workers that already reached the store remain charged; allow workers
        // not yet started to retire instead of asserting scheduler timing.
        handles[8].send(&ServerMessage::Pong { id: 2 }).unwrap();
        assert_eq!(
            ws_protocol::read_message::<_, ServerMessage>(&mut peers[8]).unwrap(),
            Some(ServerMessage::Pong { id: 2 })
        );
        drop(guard);
        for peer in &mut peers[..8] {
            while !read_one(peer).1 {}
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while engine.search_jobs.load(Ordering::Acquire) != 0
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(engine.search_jobs.load(Ordering::Acquire), 0);
        engine.search(&handles[8], 3, request);
        while !read_one(&mut peers[8]).1 {}
        for handle in handles {
            engine.unregister_client(&handle);
        }
    }

    /// Serves one `search` per connection on a background thread (detached:
    /// the accept loop outlives the test like the embed fakes' server).
    fn serve_search(engine: SharedEngine, pipe: &str, id: RequestId, query: SearchQuery) {
        let pipe = pipe.to_string();
        std::thread::Builder::new()
            .name("winsearch-task6-serve".into())
            .spawn(move || {
                let _ = ws_platform::pipe::serve(&pipe, move |stream| {
                    let client = engine.register_client(Arc::new(Mutex::new(stream)));
                    engine.search(&client, id, query.clone());
                });
            })
            .unwrap();
    }

    fn connect_retry(pipe: &str) -> PipeStream {
        let start = std::time::Instant::now();
        loop {
            match ws_platform::pipe::PipeStream::connect(pipe) {
                Ok(s) => return s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(e) => panic!("pipe {pipe} never answered: {e}"),
            }
        }
    }

    fn read_one(stream: &mut PipeStream) -> (Vec<SearchResult>, bool) {
        let msg: Option<ServerMessage> = ws_protocol::read_message(stream).expect("read frame");
        match msg {
            Some(ServerMessage::Results {
                batch,
                done,
                outcome,
                ..
            }) => {
                assert_eq!(
                    outcome.is_some(),
                    done,
                    "new engine outcomes belong only to terminal frames"
                );
                if let Some(outcome) = outcome {
                    assert!(outcome.valid_for(done, batch.is_empty(), true));
                }
                (batch, done)
            }
            Some(ServerMessage::Error { code, message, .. }) => {
                panic!("engine error {code:?}: {message}")
            }
            Some(other) => panic!("unexpected frame: {other:?}"),
            None => panic!("pipe closed before terminal frame"),
        }
    }

    #[test]
    fn streaming_exact_first_semantic_appends() {
        let (engine, _tree) = semantic_service();
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 7, semantic_query("meeting"));

        let mut stream = connect_retry(&pipe);
        // Frame 1: exact batch first, stream held open for the semantic stage.
        let (first, done) = read_one(&mut stream);
        assert!(!done, "exact terminal frame must stay open for semantics");
        assert!(
            first.iter().any(|r| r.title == "meeting-notes.txt"),
            "exact hit streams first, got {first:?}"
        );
        assert!(
            first
                .iter()
                .all(|r| !r.match_explanation.starts_with("semantic")),
            "no semantic explanations in the exact batch"
        );
        // Frame 2: semantic append, then the terminal close.
        let (second, done) = read_one(&mut stream);
        assert!(done);
        assert_eq!(second.len(), 1, "semantic appends one hit, got {second:?}");
        assert_eq!(second[0].title, "banquet-hall.txt");
        assert_eq!(
            second[0].match_explanation,
            "semantic name match · similarity 1.00"
        );
    }

    #[test]
    fn streaming_cancel_between_stages_yields_empty_terminal() {
        let (engine, _tree) = semantic_service();
        // Rendezvous hook: blocks the semantic stage until the test cancels
        // the request — deterministic, no timing guess.
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel::<()>();
        // `Receiver` is not `Sync`: guard it so the hook stays shareable.
        let proceed_rx = Mutex::new(proceed_rx);
        engine.set_query_embed_hook(Arc::new(move |_| {
            let _ = entered_tx.send(());
            let _ = proceed_rx.lock().unwrap().recv();
            Ok(vec![1.0, 0.0, 0.0])
        }));
        let pipe = unique_pipe();
        serve_search(engine.clone(), &pipe, 9, semantic_query("meeting"));

        let mut stream = connect_retry(&pipe);
        let (first, done) = read_one(&mut stream);
        assert!(!done);
        assert!(first.iter().any(|r| r.title == "meeting-notes.txt"));

        entered_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("semantic stage must start after the exact batch");
        engine.cancel(9);
        proceed_tx.send(()).unwrap();

        let (terminal, done) = read_one(&mut stream);
        assert!(done, "cancelled request must close the stream");
        assert!(
            terminal.is_empty(),
            "cancel between stages yields an empty terminal batch, got {terminal:?}"
        );
    }

    #[test]
    fn streaming_code_mode_has_no_semantic_stage() {
        let (engine, _tree) = semantic_service();
        let mut q = semantic_query("meeting");
        q.mode = SearchMode::Code;
        let pipe = unique_pipe();
        serve_search(engine, &pipe, 11, q);

        let mut stream = connect_retry(&pipe);
        // `code:` is unaffected: the exact contract closes the stream in one
        // terminal frame, no semantic append follows.
        let (batch, done) = read_one(&mut stream);
        assert!(done, "code search must terminate on the exact frame");
        assert!(
            batch
                .iter()
                .all(|r| !r.match_explanation.starts_with("semantic")),
            "no semantic hits for code mode, got {batch:?}"
        );
    }
}
