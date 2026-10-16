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
    EngineConfig as StoredEngineConfig, HitKind, PreferredBackend, SearchRequest,
    kind_for_path_and_ext, SEGMENT_KIND_NAME,
};
use ws_index::{EntryRow, evaluate, search_settings};
use ws_platform::paths;
use ws_platform::pipe::PipeStream;
use ws_protocol::{
    write_message, EngineConfig, ErrorCode, EngineRunState, EngineStatus, FrameError, MatchLocation,
    RequestId, ResultKind, SearchMode, SearchQuery, SearchResult, ServerMessage,
};

use crate::embed_client::EmbedSupervisor;
use crate::indexer::Indexer;
use crate::model::ModelService;

/// How many cosine candidates the semantic stage retrieves before hybrid
/// ranking (retrieval depth; the appended list is capped at `query.limit`).
pub const SEMANTIC_TOP_K: usize = 200;
/// Minimum cosine similarity for a semantic hit to be shown. Tune via evals:
/// lower recalls more paraphrases, higher keeps the list exact-adjacent.
pub const MIN_SEMANTIC_COSINE: f32 = 0.35;
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
    status: Mutex<EngineStatus>,
    clients: Mutex<Vec<ClientHandle>>,
    /// Cancellation flags for in-flight search requests, keyed by `(client_id, request_id)`.
    cancels: Mutex<HashMap<(u64, RequestId), Arc<AtomicBool>>>,
    next_client_id: std::sync::atomic::AtomicU64,
    indexer: Arc<Indexer>,
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

impl EngineService {
    pub fn new(indexer: Arc<Indexer>) -> SharedEngine {
        Self::new_with_model_dir(indexer, paths::model_dir())
    }

    /// Production wiring used by `main.rs`: explicit embed supervisor.
    pub fn new_with_embed(indexer: Arc<Indexer>, embed: EmbedSupervisor) -> SharedEngine {
        Self::new_inner(indexer, paths::model_dir(), embed)
    }

    /// Test hook: same engine with an isolated model directory.
    pub fn new_with_model_dir(indexer: Arc<Indexer>, model_dir: std::path::PathBuf) -> SharedEngine {
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
        Arc::new(Self {
            status: Mutex::new(EngineStatus {
                state: EngineRunState::Idle,
                current_activity: String::new(),
                ..Default::default()
            }),
            clients: Mutex::new(Vec::new()),
            cancels: Mutex::new(HashMap::new()),
            next_client_id: std::sync::atomic::AtomicU64::new(1),
            indexer,
            models,
            embed: Arc::new(embed),
            query_cache: Mutex::new(QueryEmbeddingCache::new(QUERY_CACHE_CAP)),
            query_embed_hook: Mutex::new(None),
            model_ready_override: Mutex::new(None),
        })
    }

    /// Test seam: embed query texts through `hook` instead of the worker.
    pub fn set_query_embed_hook(&self, hook: QueryEmbedHook) {
        *self.query_embed_hook.lock().unwrap_or_else(|p| p.into_inner()) = Some(hook);
    }

    /// Test seam: force the semantic stage's model-readiness check.
    pub fn set_model_ready_override(&self, ready: Option<bool>) {
        *self.model_ready_override.lock().unwrap_or_else(|p| p.into_inner()) = ready;
    }

    pub fn model_service(&self) -> &Arc<ModelService> {
        &self.models
    }

    pub fn embed_supervisor(&self) -> Arc<EmbedSupervisor> {
        Arc::clone(&self.embed)
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
        if let Some(reason) = self.indexer.indexing_pause_reason() { status.state = EngineRunState::Paused; status.current_activity = reason; }
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
    }

    fn broadcast(&self, msg: &ServerMessage) {
        self.push_to_all(msg);
    }

    /// Sends one message to every connected client (used by the model
    /// service's progress pushes as well as status broadcasts).
    pub fn push_to_all(&self, msg: &ServerMessage) {
        let clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        for client in clients.iter() {
            if let Err(e) = client.send(msg) {
                info!(client = client.id, error = %e, "dropping unreachable client");
            }
        }
    }

    pub fn begin_request_for_client(&self, client_id: u64, id: RequestId) -> Arc<AtomicBool> {
        let flag = Arc::new(AtomicBool::new(false));
        self.cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((client_id, id), flag.clone());
        flag
    }

    pub fn end_request_for_client(&self, client_id: u64, id: RequestId) {
        self.cancels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(client_id, id));
    }

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

    fn answer_model_status(&self, client: &ClientHandle, id: RequestId) {
        let _ = client.send(&ServerMessage::ModelStatus {
            id: Some(id),
            status: self.models.status(),
        });
    }

    pub fn model_download_start(&self, client: &ClientHandle, id: RequestId) {
        self.models.start_download();
        self.answer_model_status(client, id);
    }

    pub fn model_download_cancel(&self, client: &ClientHandle, id: RequestId) {
        self.models.cancel_download();
        self.answer_model_status(client, id);
    }

    pub fn model_import(&self, client: &ClientHandle, id: RequestId, path: String) {
        match self.models.start_import(std::path::PathBuf::from(path)) {
            Ok(_) => self.answer_model_status(client, id),
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
                    "preferred_backend must be auto|cuda|vulkan|cpu, got {:?}",
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
        self.embed.model_loaded()
            || self.models.status().state == ws_modelmgr::ModelState::Ready
    }

    /// Whether the `config` policy currently allows semantic retrieval.
    /// A store read failure fails open (the stage still needs a ready model
    /// and a successful embed, so nothing unranked can leak through).
    fn semantic_enabled(&self) -> bool {
        self.indexer
            .get_engine_config()
            .map(|c| c.semantic_enabled)
            .unwrap_or(true)
    }

    /// The full semantic gate: requested by the client, allowed by policy,
    /// servable by the model — and never for `code:` (code search is
    /// unaffected by this stage).
    fn semantic_allowed(&self, query: &SearchQuery) -> bool {
        query.include_semantic
            && !query.text.trim().is_empty()
            && query.mode != SearchMode::Code
            && self.semantic_enabled()
            && self.model_ready()
    }

    /// Embeds one query text, consulting the LRU first. Failures (worker
    /// down, timeout, empty reply) are returned as strings and make the
    /// semantic stage yield no hits — exact results already streamed.
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, String> {
        let key = text.trim().to_string();
        if let Some(hit) = self
            .query_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            return Ok(hit);
        }
        let vec = if let Some(hook) = self
            .query_embed_hook
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            hook(&key)?
        } else {
            let vecs = self
                .embed
                .embed_texts_bounded(std::slice::from_ref(&key), QUERY_EMBED_TIMEOUT)
                .map_err(|e| e.to_string())?;
            vecs.into_iter().next().ok_or_else(|| {
                "embed worker returned no vectors for the query".to_string()
            })?
        };
        self.query_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .put(key, vec.clone());
        Ok(vec)
    }

    /// Computes the appended semantic hits for one query (no IPC). Empty when
    /// the gate is closed, the embed fails, or nothing passes the cosine
    /// threshold after duplicate suppression.
    fn plan_semantic(&self, query: &SearchQuery, exact_entry_ids: &HashSet<i64>) -> Vec<SearchResult> {
        if !self.semantic_allowed(query) {
            return Vec::new();
        }
        let kinds = hit_kinds(&query.categories);
        if !query.categories.is_empty() && kinds.is_empty() {
            return Vec::new();
        }
        let text = query.text.trim();
        let query_vec = match self.embed_query(text) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "semantic stage: query embed failed; exact results stand");
                return Vec::new();
            }
        };
        let candidate_limit = (query.limit.max(1) as usize)
            .max(SEMANTIC_TOP_K)
            .saturating_mul(8);
        let filter = if query.mode == SearchMode::Auto && kinds.is_empty() {
            None
        } else {
            let matching_ids = match self.indexer.matching_entry_ids(
                query.mode == SearchMode::Apps,
                query.mode == SearchMode::Files,
                &kinds,
            ) {
                Ok(ids) => ids,
                Err(e) => {
                    warn!(error = %e, "semantic stage: entry filter query failed; exact results stand");
                    return Vec::new();
                }
            };
            if matching_ids.is_empty() {
                return Vec::new();
            }
            Some(ws_index::VectorFilter {
                kinds: Vec::new(),
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
                return Vec::new();
            }
        };
        // Best cosine per entry across all segment kinds (`name`, `doc`, `image`, `ocr`, `pdf`, ...).
        let mut best: HashMap<i64, (f32, String)> = HashMap::new();
        for hit in &scored {
            if hit.cosine < MIN_SEMANTIC_COSINE {
                continue;
            }
            best.entry(hit.segment.entry_id)
                .and_modify(|(c, k)| {
                    if hit.cosine > *c {
                        *c = hit.cosine;
                        *k = hit.segment.kind.clone();
                    }
                })
                .or_insert_with(|| (hit.cosine, hit.segment.kind.clone()));
        }
        let now = now_unix();
        let mut out = Vec::new();
        for (entry_id, (cosine, seg_kind)) in best {
            if exact_entry_ids.contains(&entry_id) {
                continue; // duplicate suppression vs the exact batch
            }
            let entry = match self.indexer.get_entry_by_id(entry_id) {
                Ok(Some(e)) => e,
                Ok(None) => continue, // deleted between vector search and join
                Err(e) => {
                    warn!(error = %e, entry = entry_id, "semantic stage: entry lookup failed");
                    continue;
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
            out.push(SearchResult {
                id: format!("s{entry_id}"),
                kind: to_result_kind(kind),
                title: entry.name.clone(),
                subtitle: entry.path.clone(),
                match_explanation: semantic_explanation_for_kind(&seg_kind, cosine),
                score: hybrid_score(cosine, text, &entry.name_lower, entry.mtime, now),
                path: Some(entry.path),
                location: None,
                thumbnail_path: entry.thumbnail_path,
                ..Default::default()
            });
        }
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.title.cmp(&b.title))
        });
        out.truncate(query.limit.max(1) as usize);
        out
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
        let outcome = self.run_search(client, id, query, cancel);
        if let Err(e) = outcome {
            self.end_request_for_client(client.id(), id);
            let _ = client.send(&ServerMessage::Error {
                id: Some(id),
                code: ws_protocol::ErrorCode::Internal,
                message: e.to_string(),
            });
            let _ = client.send(&ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
            });
        }
    }

    fn run_search(
        self: &Arc<Self>,
        client: &ClientHandle,
        id: RequestId,
        query: SearchQuery,
        cancel: Arc<AtomicBool>,
    ) -> Result<(), FrameError> {
        let results = self.plan_results(&query);

        if cancel.load(Ordering::Relaxed) {
            self.end_request_for_client(client.id(), id);
            return client.send(&ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
            });
        }

        // Whether a semantic stage will follow: when it will, the exact
        // terminal frame says `done: false` so the client keeps listening.
        let semantic = self.semantic_allowed(&query);

        // Progressive response: the first batch is what the overlay shows;
        // overflow streams after so typing stays responsive.
        let limit = (query.limit.max(1) as usize).min(results.len());
        let (first, rest) = results.split_at(limit);
        client.send(&ServerMessage::Results {
            id,
            batch: first.to_vec(),
            done: rest.is_empty() && !semantic,
        })?;
        if !rest.is_empty() {
            if cancel.load(Ordering::Relaxed) {
                self.end_request_for_client(client.id(), id);
                return client.send(&ServerMessage::Results {
                    id,
                    batch: Vec::new(),
                    done: true,
                });
            }
            client.send(&ServerMessage::Results {
                id,
                batch: rest.to_vec(),
                done: !semantic,
            })?;
        }
        if !semantic {
            self.end_request_for_client(client.id(), id);
            return Ok(());
        }

        // Semantic stage runs off the connection read loop so an in-band
        // `Cancel` or superseding `Search` on the same pipe can be read and
        // applied while `embed_query` is in flight.
        let exact_ids = exact_entry_ids(&results);
        let this = Arc::clone(self);
        let thread_client = client.clone();
        if let Err(e) = std::thread::Builder::new()
            .name("winsearch-semantic".into())
            .spawn(move || {
                let outcome =
                    this.run_semantic_stage(&thread_client, id, &query, &exact_ids, &cancel);
                this.end_request_for_client(thread_client.id(), id);
                if let Err(err) = outcome {
                    let _ = thread_client.send(&ServerMessage::Error {
                        id: Some(id),
                        code: ws_protocol::ErrorCode::Internal,
                        message: err.to_string(),
                    });
                    let _ = thread_client.send(&ServerMessage::Results {
                        id,
                        batch: Vec::new(),
                        done: true,
                    });
                }
            })
        {
            warn!(error = %e, "semantic stage: thread spawn failed; closing stream");
            self.end_request_for_client(client.id(), id);
            return client.send(&ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
            });
        }
        Ok(())
    }

    fn run_semantic_stage(
        &self,
        client: &ClientHandle,
        id: RequestId,
        query: &SearchQuery,
        exact_ids: &HashSet<i64>,
        cancel: &AtomicBool,
    ) -> Result<(), FrameError> {
        if cancel.load(Ordering::Relaxed) {
            return client.send(&ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
            });
        }
        let hits = self.plan_semantic(query, exact_ids);
        if cancel.load(Ordering::Relaxed) {
            return client.send(&ServerMessage::Results {
                id,
                batch: Vec::new(),
                done: true,
            });
        }
        client.send(&ServerMessage::Results {
            id,
            batch: hits,
            done: true,
        })
    }

    /// Computes the ranked result list for one query (no IPC). Split from
    /// `run_search` so the pipeline is directly testable.
    fn plan_results(&self, query: &SearchQuery) -> Vec<SearchResult> {
        let text = query.text.trim();
        if text.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();
        let wants_index = query.mode != SearchMode::Code;
        let wants_calc = query.mode == SearchMode::Auto
            && (query.categories.is_empty() || query.categories.contains(&ResultKind::Calculator));
        let wants_settings = query.mode == SearchMode::Auto
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
                apps_only: query.mode == SearchMode::Apps,
                files_only: query.mode == SearchMode::Files,
                kinds,
                limit: (query.limit.max(1) as usize).saturating_mul(3).clamp(20, 120),
            };
            match self.indexer.search(&request) {
                Ok(hits) => {
                    for hit in hits {
                        let terms: Vec<&str> = text
                            .split(|c: char| !c.is_alphanumeric())
                            .filter(|s| !s.is_empty())
                            .collect();
                        let moments: Vec<ws_protocol::MatchingMoment> = hit
                            .matches
                            .iter()
                            .map(|m| ws_protocol::MatchingMoment {
                                excerpt: match m.match_byte {
                                    Some(offset) => ws_extract::select_excerpt_at(&m.excerpt, offset, 240),
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
                }
            }
        }
        results
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
fn exact_entry_ids(results: &[SearchResult]) -> HashSet<i64> {
    results
        .iter()
        .filter_map(|r| r.id.strip_prefix('e')?.parse::<i64>().ok())
        .collect()
}

/// Hybrid band mapping: cosine `[0, 1]` → score `[0.45, 0.90]`. The band sits
/// below exact-prefix matches (0.92) and above FTS token matches (0.5); the
/// `0.35` cosine threshold lands at `≈0.61`, so every shown semantic hit
/// already outranks FTS noise.
fn hybrid_base_score(cosine: f32) -> f64 {
    0.45 + 0.45 * cosine as f64
}

fn tokenize_lower(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// +0.10 when a query token exactly matches a name token ("beach photo" →
/// "beach-sunset.jpg"). Exact-token equality, not substring: the vector
/// already captures fuzz; the boost rewards the user typing the real word.
fn exact_token_boost(query_text: &str, name_lower: &str) -> f64 {
    let name_tokens = tokenize_lower(name_lower);
    if tokenize_lower(query_text)
        .iter()
        .any(|q| name_tokens.iter().any(|n| n == q))
    {
        0.10
    } else {
        0.0
    }
}

/// +0.02 for entries touched in the last 30 days (launcher-style recency on
/// top of the semantic band). `now_unix` is a parameter so tests pin time.
fn recency_boost(mtime: i64, now_unix: i64) -> f64 {
    if mtime > 0 && now_unix.saturating_sub(mtime) <= 30 * 86_400 {
        0.02
    } else {
        0.0
    }
}

/// Full hybrid score for one semantic hit, clamped to `[0, 1]`.
fn hybrid_score(cosine: f32, query_text: &str, name_lower: &str, mtime: i64, now: i64) -> f64 {
    (hybrid_base_score(cosine) + exact_token_boost(query_text, name_lower)
        + recency_boost(mtime, now))
    .clamp(0.0, 1.0)
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
        "doc" | "pdf" => format!("semantic content match · similarity {cosine:.2}"),
        other => format!("semantic {other} match · similarity {cosine:.2}"),
    }
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

    fn service_with(tree: &TempTree, files: &[&str]) -> SharedEngine {
        for f in files {
            fs::write(tree.0.join(f), b"x").unwrap();
        }
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
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
            let body = format!("{padding}\n{matched} invoice detail\n{padding}\n{matched} second detail\n");
            fs::write(tree.0.join("normalized.txt"), &body).unwrap();
            let engine = service_with(&tree, &[]);
            engine.indexer.extract_once_with(&crate::extract::ExtractorRegistry::default(), |_| {
                Err("model unavailable".into())
            }).unwrap();
            let results = engine.plan_results(&query("cafe"));
            let result = results.iter().find(|r| r.title == "normalized.txt").unwrap();
            let excerpt = result.excerpt.as_ref().unwrap();
            assert!(excerpt.contains(&format!("{matched} invoice detail")), "primary excerpt omitted normalized match: {excerpt:?}");
            assert_eq!(result.location, Some(MatchLocation::Line { line: 2 }));
            assert!(result.matching_moments.len() >= 2);
            assert!(result.matching_moments.iter().any(|m| m.location == Some(MatchLocation::Line { line: 4 })));
            for moment in &result.matching_moments {
                assert!(moment.excerpt.contains(&format!("{matched} ")), "moment omitted normalized match: {:?}", moment.excerpt);
                assert!(moment.excerpt.chars().count() <= 240);
                assert!(matches!(moment.location, Some(MatchLocation::Line { line: 2 | 4 })));
            }
            assert_eq!(Some(&result.matching_moments[0].excerpt), result.excerpt.as_ref());
            assert_eq!(result.matching_moments[0].location, result.location);
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
        assert!(
            engine
                .plan_results(&apps_calc)
                .iter()
                .all(|r| r.kind != ResultKind::Calculator)
        );

        let mut files_calc = query("2 + 2");
        files_calc.mode = SearchMode::Files;
        assert!(
            engine
                .plan_results(&files_calc)
                .iter()
                .all(|r| r.kind != ResultKind::Calculator)
        );

        let mut apps_settings = query("windows update");
        apps_settings.mode = SearchMode::Apps;
        assert!(
            engine
                .plan_results(&apps_settings)
                .iter()
                .all(|r| r.kind != ResultKind::Settings)
        );
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
        engine.end_request_for_client(1, 1);
        engine.end_request_for_client(2, 1);
    }

    #[test]
    fn status_reports_segments_and_embedding_activity() {
        // P3.5: `segments_indexed` counts name-segments; pending name jobs
        // surface as "Embedding names…" (crawl drained, embed backlog live).
        let tree = TempTree::new();
        for f in ["alpha-embed.txt", "beta-embed.txt"] {
            fs::write(tree.0.join(f), b"x").unwrap();
        }
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let engine = EngineService::new(indexer);

        let s = engine.status();
        assert_eq!(s.segments_indexed, 0);
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
        assert_eq!(MIN_SEMANTIC_COSINE, 0.35);
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
            SearchResult { id: "e42".into(), ..Default::default() },
            SearchResult { id: "calc".into(), ..Default::default() },
            SearchResult { id: "settings-0".into(), ..Default::default() },
            SearchResult { id: "s42".into(), ..Default::default() },
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
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
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
    fn cold_start_model_ready_when_model_service_is_ready() {
        let tree = TempTree::new();
        fs::write(tree.0.join("banquet-hall.txt"), b"x").unwrap();
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
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
        assert_eq!(hit.match_explanation, "semantic name match · similarity 1.00");
        // Band top (0.90) + no token boost + recency (+0.02, just-written file).
        assert!((hit.score - 0.92).abs() < 1e-9, "score {}, want 0.92", hit.score);
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
        assert!(
            engine
                .plan_semantic(&semantic_query("zzz-no-exact"), &HashSet::new())
                .is_empty()
        );
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
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1, "second query must hit the LRU");
        // A different text re-embeds.
        assert_eq!(engine.plan_semantic(&semantic_query("other"), &ids).len(), 1);
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }

    // ---- streaming integration (fake vectors, real pipe frames) ----

    static PIPE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn unique_pipe() -> String {
        let n = PIPE_SEQ.fetch_add(1, Ordering::SeqCst);
        format!(r"\\.\pipe\winsearch-task6-{}-{n}", std::process::id())
    }

    /// Serves one `search` per connection on a background thread (detached:
    /// the accept loop outlives the test like the embed fakes' server).
    fn serve_search(engine: SharedEngine, pipe: &str, id: RequestId, query: SearchQuery) {
        let pipe = pipe.to_string();
        std::thread::Builder::new()
            .name("winsearch-task6-serve".into())
            .spawn(move || {
                let _ = ws_platform::pipe::serve(&pipe, move |stream| {
                    let client =
                        engine.register_client(Arc::new(Mutex::new(stream)));
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
        let msg: Option<ServerMessage> =
            ws_protocol::read_message(stream).expect("read frame");
        match msg {
            Some(ServerMessage::Results { batch, done, .. }) => (batch, done),
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
            first.iter().all(|r| !r.match_explanation.starts_with("semantic")),
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
            batch.iter().all(|r| !r.match_explanation.starts_with("semantic")),
            "no semantic hits for code mode, got {batch:?}"
        );
    }
}
