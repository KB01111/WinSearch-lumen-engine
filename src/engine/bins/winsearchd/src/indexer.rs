//! The indexer orchestrator: crawl workers, watcher event handling, the
//! application catalog, disconnected-drive reconciliation, and the semantic
//! (name-embedding) pipeline.
//!
//! Concurrency model: a bounded pool of crawl workers pulls directory work
//! from the crash-safe queue inside the index database; all database access
//! is serialized through one `Mutex<IndexStore>`; watchers feed events into
//! the same store. The embed scheduler (P3.5) is one more `std::thread` that
//! claims `extract_jobs(kind='name')` in batches of [`EMBED_BATCH`], embeds
//! via the supervisor's bounded call ([`EMBED_JOB_TIMEOUT`], Task2-I1), and
//! upserts `segments(kind='name')` idempotently. Nothing here blocks the
//! IPC/serve path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use tracing::{info, warn};
use ws_extract::modality_kind_for_ext;
use ws_index::model::{
    compute_extract_priority, EngineConfig, EntryRecord, NewSegment, SearchRequest, UpsertOutcome,
    EMBED_BATCH_SIZE, EXTRACT_PRIORITY_NEW, EXTRACT_PRIORITY_UPDATED, SEGMENT_KIND_NAME,
};
use ws_index::{EntryRow, HitKind, IndexStats, IndexStore, ScoredSegment, SearchHit, StoreError, VectorFilter};

use crate::catalog;
use crate::crawl;
use crate::embed_client::{EmbedSupervisor, EMBED_JOB_TIMEOUT};
use crate::watcher::{self, WatchEvent, Watcher};

/// One pipeline batch: up to this many names per worker round-trip (P3.5).
/// Mirrors `ws_index::model::EMBED_BATCH_SIZE` (128, well under the 256-text
/// worker cap so one pipeline batch is always one worker job).
pub const EMBED_BATCH: usize = EMBED_BATCH_SIZE;

/// Poison-pill cap (fix round 1/5, I1): a name job that fails this many
/// batch attempts (timeouts, transport errors, arity mismatches, repeated
/// upsert errors) is marked `failed` instead of requeued, so a persistently
/// hung worker or poison entry cannot spin the embed queue forever. Failed
/// rows are never auto-claimed; they stay inspectable via
/// `find_extract_job`. Mid-batch changes use the attempts-preserving
/// `requeue_extract_job` path instead, so rapidly-changing files are never
/// poisoned as "failing".
pub const MAX_EMBED_ATTEMPTS: i64 = 5;

/// Crawl worker count: bounded on purpose (indexing must never starve the
/// interactive search path).
const WORKER_THREADS: usize = 4;

/// How often the drive supervisor checks for disconnected/reconnected roots.
const DRIVE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// How often the application catalog is refreshed.
const CATALOG_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30 * 60);

#[derive(Default)]
struct Counters {
    files_discovered: u64,
    files_indexed: u64,
    events_processed: u64,
}

/// Live counters surfaced through engine status.
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexerStatus {
    pub files_discovered: u64,
    pub files_indexed: u64,
    pub queue_depth: u64,
    pub indexing: bool,
    pub stats: IndexStats,
    /// Name-segments stored (`segments.kind='name'` → `EngineStatus.segments_indexed`).
    pub segments_indexed: u64,
    /// True while name embeddings are still outstanding.
    pub embedding: bool,
}

/// One claimed name job with its claim-time snapshot (I2 lost-update token).
struct ClaimedNameJob {
    job_id: i64,
    entry_id: i64,
    name: String,
    mtime: i64,
    job_priority: i64,
    attempts: i64,
}

/// Outcome of one [`Indexer::embed_once_with`] batch (test-observable).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmbedBatchOutcome {
    /// Jobs claimed for this batch.
    pub claimed: usize,
    /// Segments successfully (up)inserted.
    pub embedded: usize,
    /// Jobs released for retry (`fail_extract_job`, attempts+1).
    pub failed: usize,
    /// Jobs requeued after a mid-batch change (fresh data embeds next batch;
    /// attempts untouched).
    pub requeued: usize,
    /// Jobs poison-pilled (`mark_extract_failed`) at [`MAX_EMBED_ATTEMPTS`].
    pub poisoned: usize,
}

struct RootInfo {
    id: i64,
    path: PathBuf,
    offline: AtomicBool,
    missing_mark_pending: AtomicBool,
}

pub struct Indexer {
    store: Mutex<IndexStore>,
    vector_index: std::sync::RwLock<Option<ws_index::VectorIndex>>,
    cache_epoch: std::sync::atomic::AtomicU64,
    counters: Mutex<Counters>,
    wake: (Mutex<bool>, Condvar),
    shutdown: AtomicBool,
    roots: std::sync::RwLock<Vec<Arc<RootInfo>>>,
    policy: std::sync::RwLock<ws_protocol::IndexingConfig>,
    watchers: Mutex<Vec<Watcher>>,
    configuration_change: Mutex<()>,
}

impl Indexer {
    fn root_snapshot(&self) -> Vec<Arc<RootInfo>> {
        self.roots.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn get_indexing_config(&self) -> Result<ws_protocol::IndexingConfig, StoreError> {
        Ok(self
            .policy
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone())
    }
    pub fn set_indexing_config(
        self: &Arc<Self>,
        config: &ws_protocol::IndexingConfig,
    ) -> Result<(), StoreError> {
        let _change = self
            .configuration_change
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let config = crate::policy::validate(config)?;
        let mut store = self.store_lock();
        store.set_indexing_config(&config)?;
        *self.policy.write().unwrap_or_else(|p| p.into_inner()) = config.clone();
        let mut roots = Vec::new();
        for row in store
            .roots()?
            .into_iter()
            .filter(|r| r.enabled && r.path != CATALOG_ROOT)
        {
            let path = PathBuf::from(row.path);
            let present = path.is_dir();
            if !present { store.set_missing_for_root(row.id, true)?; }
            roots.push(Arc::new(RootInfo {
                id: row.id,
                path,
                offline: AtomicBool::new(!present),
                missing_mark_pending: AtomicBool::new(false),
            }));
        }
        *self.roots.write().unwrap_or_else(|p| p.into_inner()) = roots.clone();
        self.invalidate_vector_cache();
        drop(store);
        // Dropping a watcher joins its callback: never hold store/root locks here.
        let old = std::mem::take(&mut *self.watchers.lock().unwrap_or_else(|p| p.into_inner()));
        drop(old);
        let watchers: Vec<_> = roots.iter().filter_map(|r| self.spawn_watcher(r)).collect();
        *self.watchers.lock().unwrap_or_else(|p| p.into_inner()) = watchers;
        self.enqueue_initial_scan()?;
        self.notify_work();
        Ok(())
    }
    pub fn indexing_pause_reason(&self) -> Option<String> {
        crate::policy::native_pause_reason(&self.policy.read().unwrap_or_else(|p| p.into_inner()))
    }
    pub(crate) fn name_path_allowed(&self, path: &Path) -> bool {
        crate::policy::path_allowed(
            &self.policy.read().unwrap_or_else(|p| p.into_inner()),
            &path.to_string_lossy(),
        ) && crate::policy::metadata_safe(path, false)
    }
    pub fn content_path_allowed(&self, path: &Path) -> bool {
        self.name_path_allowed(path)
            && crate::policy::metadata_safe(path, true)
            && crate::policy::ordinary_content_allowed(
                &self.policy.read().unwrap_or_else(|p| p.into_inner()),
                path,
            )
    }
    pub(crate) fn wait_for_work(&self) {
        let guard = self.wake.0.lock().unwrap_or_else(|p| p.into_inner());
        let _ = self
            .wake
            .1
            .wait_timeout(guard, std::time::Duration::from_secs(5))
            .unwrap_or_else(|p| p.into_inner());
    }

    /// Opens the index and registers the given roots (idempotent).
    pub fn open(store: IndexStore, roots: &[PathBuf]) -> Result<Arc<Self>, StoreError> {
        let mut store = store;
        // Claims left by a crashed run become work again (crawl queue and
        // embed queue alike: a crash mid-batch re-runs the idempotent work).
        store.release_claims()?;
        let released = store.release_extract_claims()?;
        if released > 0 {
            info!(released, "released embed claims left by a previous run");
        }
        let mut config = store.get_or_migrate_indexing_config()?;
        if !roots.is_empty() {
            config.roots = roots.iter().map(|r| r.to_string_lossy().into_owned()).collect();
            config = crate::policy::validate(&config)?;
            store.set_indexing_config(&config)?;
        }
        let config = crate::policy::validate(&config)?;
        let selected: Vec<PathBuf> = config.roots.iter().map(PathBuf::from).collect();
        let mut infos = Vec::new();
        for root in &selected {
            let path = root.to_string_lossy().into_owned();
            let id = store.ensure_root(&path)?;
            let present = root.is_dir();
            if !present {
                store.set_missing_for_root(id, true)?;
            }
            infos.push(Arc::new(RootInfo {
                id,
                offline: AtomicBool::new(!present),
                missing_mark_pending: AtomicBool::new(false),
                path: root.clone(),
            }));
        }
        Ok(Arc::new(Self {
            store: Mutex::new(store),
            vector_index: std::sync::RwLock::new(None),
            cache_epoch: std::sync::atomic::AtomicU64::new(1),
            counters: Mutex::new(Counters::default()),
            wake: (Mutex::new(false), Condvar::new()),
            shutdown: AtomicBool::new(false),
            roots: std::sync::RwLock::new(infos),
            policy: std::sync::RwLock::new(config),
            watchers: Mutex::new(Vec::new()),
            configuration_change: Mutex::new(()),
        }))
    }

    /// Starts background work: crawl workers, watchers, catalog refresh, and
    /// the drive supervisor. Returns once all threads are spawned.
    pub fn start_background(self: &Arc<Self>) -> anyhow::Result<()> {
        self.enqueue_initial_scan()?;

        for _ in 0..WORKER_THREADS {
            let indexer = Arc::clone(self);
            std::thread::Builder::new()
                .name("winsearch-crawl".into())
                .spawn(move || indexer.crawl_worker())?;
        }

        {
            let mut watchers = self.watchers.lock().unwrap_or_else(|p| p.into_inner());
            for root in &self.root_snapshot() {
                match self.spawn_watcher(root) {
                    Some(w) => watchers.push(w),
                    None => warn!(root = %root.path.display(), "root not present yet; watching after reconnect"),
                }
            }
        }

        {
            let indexer = Arc::clone(self);
            std::thread::Builder::new()
                .name("winsearch-drive".into())
                .spawn(move || indexer.drive_supervisor())?;
        }
        {
            let indexer = Arc::clone(self);
            std::thread::Builder::new()
                .name("winsearch-catalog".into())
                .spawn(move || indexer.catalog_loop())?;
        }
        Ok(())
    }

    /// Enqueues a full scan of every root (first run, reconnect, or rescan).
    pub fn enqueue_initial_scan(&self) -> Result<(), StoreError> {
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        for root in &self.root_snapshot() {
            if !root.path.is_dir() {
                continue; // disconnected volume: preserved until it returns
            }
            let gen = store.begin_generation(root.id)?;
            store.enqueue_dir(root.id, &root.path.to_string_lossy())?;
            store.set_checkpoint(&format!("root:{}:generation", root.id), &gen.to_string())?;
        }
        self.notify_work();
        Ok(())
    }

    // ---- search ----

    pub fn search(&self, req: &SearchRequest) -> Result<Vec<SearchHit>, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .search(req)
    }

    /// Cosine top-k over the `segments` table for the semantic query stage
    /// (P3.6). Uses the in-memory [`ws_index::VectorIndex`] under a `RwLock`
    /// so the cosine scan does not hold the SQLite `Mutex<IndexStore>`.
    pub fn search_vectors(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<ScoredSegment>, StoreError> {
        if top_k == 0 || query.is_empty() {
            return Ok(Vec::new());
        }
        let (_, query_norm) = ws_index::vectors::normalize_vector(query);
        if query_norm == 0.0 {
            return Ok(Vec::new());
        }
        let dim = query.len();
        let need_build = {
            let guard = self.vector_index.read().unwrap_or_else(|p| p.into_inner());
            !guard.as_ref().is_some_and(|idx| idx.dim() == dim)
        };
        let hits = if need_build {
            let epoch = self.cache_epoch.load(Ordering::Acquire);
            let segs = self
                .store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .all_segments()?;
            let built = ws_index::VectorIndex::from_segments(dim, &segs);
            let hits = built.search(query, top_k, filter).unwrap_or_default();
            let mut guard = self.vector_index.write().unwrap_or_else(|p| p.into_inner());
            if self.cache_epoch.load(Ordering::Acquire) == epoch {
                *guard = Some(built);
            }
            hits
        } else {
            let guard = self.vector_index.read().unwrap_or_else(|p| p.into_inner());
            match guard.as_ref() {
                Some(idx) if idx.dim() == dim => {
                    idx.search(query, top_k, filter).unwrap_or_default()
                }
                _ => Vec::new(),
            }
        };
        if hits.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            if let Some(segment) = store.get_segment(hit.id)? {
                if !store.entry_visible(segment.entry_id)? { continue; }
                out.push(ScoredSegment {
                    segment,
                    cosine: hit.cosine,
                });
            }
        }
        Ok(out)
    }

    pub(crate) fn invalidate_vector_cache(&self) {
        self.cache_epoch.fetch_add(1, Ordering::AcqRel);
        *self.vector_index.write().unwrap_or_else(|p| p.into_inner()) = None;
    }

    pub(crate) fn update_vector_cache(&self, seg_id: i64, entry_id: i64, kind: &str, vec: &[f32]) {
        self.cache_epoch.fetch_add(1, Ordering::AcqRel);
        let mut guard = self.vector_index.write().unwrap_or_else(|p| p.into_inner());
        if let Some(ref mut idx) = *guard {
            if idx.dim() == vec.len() {
                let _ = idx.add_or_update(seg_id, entry_id, kind, vec);
            } else {
                *guard = None;
            }
        }
    }

    /// One entry by id (the semantic stage resolves names/paths/mtimes for
    /// scored segments through this).
    pub fn get_entry_by_id(&self, id: i64) -> Result<Option<EntryRow>, StoreError> {
        let store = self.store_lock();
        if !store.entry_visible(id)? { return Ok(None); }
        store.get_entry(id)
    }

    pub(crate) fn segment_thumbnail(&self,segment:&ws_index::model::Segment) -> Result<Option<String>,StoreError> {
        self.store_lock().segment_thumbnail(segment)
    }

    /// Non-missing entry ids matching the given mode/category filters.
    pub fn matching_entry_ids(
        &self,
        apps_only: bool,
        exclude_apps: bool,
        kinds: &[ws_index::HitKind],
    ) -> Result<Vec<i64>, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .matching_entry_ids(apps_only, exclude_apps, kinds)
    }

    /// Engine search policy for the semantic stage (`semantic_enabled`
    /// gates the stage; unknown/missing keys read back as defaults).
    pub fn get_engine_config(&self) -> Result<EngineConfig, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_engine_config()
    }

    /// Persists the engine search policy (IPC `set_config` path).
    pub fn set_engine_config(&self, config: &EngineConfig) -> Result<(), StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_engine_config(config)
    }

    /// Reads a `meta` record (e.g. the `embedding_model` compat record the
    /// model service maintains).
    pub fn meta_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .meta_get(key)
    }

    /// Upserts a `meta` record.
    pub fn meta_set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .meta_set(key, value)
    }

    pub fn status(&self) -> Result<IndexerStatus, StoreError> {
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let counters = self.counters.lock().unwrap_or_else(|p| p.into_inner());
        let queue_depth = store.queue_depth()?;
        let stats = store.stats()?;
        let segments_indexed = store.count_segments_by_kind(SEGMENT_KIND_NAME).unwrap_or(0);
        let embed_queue_depth = store
            .extract_queue_depth_for_kind(SEGMENT_KIND_NAME)
            .unwrap_or(0);
        Ok(IndexerStatus {
            files_discovered: counters.files_discovered,
            files_indexed: counters.files_indexed,
            queue_depth,
            indexing: queue_depth > 0,
            stats,
            segments_indexed,
            embedding: embed_queue_depth > 0,
        })
    }

    /// Internal store lock helper shared with `extract.rs`.
    pub(crate) fn store_lock(&self) -> std::sync::MutexGuard<'_, IndexStore> {
        self.store.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// True if shutdown has been requested.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Pending-or-claimed non-name content extraction jobs.
    pub fn content_extract_queue_depth(&self) -> Result<u64, StoreError> {
        self.store_lock().content_extract_queue_depth()
    }

    /// Name-segments stored (→ `EngineStatus.segments_indexed`).
    pub fn segments_indexed(&self) -> Result<u64, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .count_segments_by_kind(SEGMENT_KIND_NAME)
    }

    /// Pending-or-claimed `extract_jobs(kind='name')` (embed backlog).
    pub fn embed_queue_depth(&self) -> Result<u64, StoreError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extract_queue_depth_for_kind(SEGMENT_KIND_NAME)
    }

    /// Model-swap recovery (Task3-S4): when `check_compat` reports
    /// `Incompatible`, purge every vector produced by the old model and enqueue
    /// name and content jobs so the new model re-embeds everything. Returns
    /// `(purged_segments, enqueued_jobs)`.
    pub fn purge_model_vectors_and_requeue(&self) -> Result<(u64, u64), StoreError> {
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let mut purged = 0;
        for kind in ws_index::model::ALL_SEGMENT_KINDS {
            purged += store.purge_segments_by_kind(kind)?;
        }
        store.reset_content_vectors()?;
        let ids = store.all_entry_ids()?;
        let mut enqueued = 0u64;
        for id in ids {
            store.enqueue_extract_job_with_priority(
                id,
                SEGMENT_KIND_NAME,
                EXTRACT_PRIORITY_UPDATED,
            )?;
            enqueued += 1;
            if let Some(entry) = store.get_entry(id)? {
                if !entry.is_dir && !entry.is_app {
                    if let Some(kind) = modality_kind_for_ext(&entry.ext) {
                        if store.content_progress(id, kind)?.is_none() {
                            store.enqueue_extract_job_with_priority(
                                id,
                                kind,
                                EXTRACT_PRIORITY_UPDATED,
                            )?;
                            enqueued += 1;
                        }
                    }
                }
            }
        }
        drop(store);
        self.invalidate_vector_cache();
        if purged > 0 || enqueued > 0 {
            info!(purged, enqueued, "purged stale name vectors; requeued for re-embed");
        }
        Ok((purged, enqueued))
    }

    // ---- crawl workers ----

    fn notify_work(&self) {
        let mut guard = self.wake.0.lock().unwrap_or_else(|p| p.into_inner());
        *guard = true;
        self.wake.1.notify_all();
    }

    /// Processes up to `max_dirs` queued directories on the calling thread.
    /// Returns how many were processed. Used by tests and one-shot scans.
    #[allow(dead_code)] // exercised by tests; workers use crawl_worker in production
    pub fn scan_available(&self, max_dirs: usize) -> usize {
        let mut processed = 0;
        for _ in 0..max_dirs {
            if self.indexing_pause_reason().is_some() { break; }
            let work = { let mut store = self.store_lock();
                if self.indexing_pause_reason().is_some() { Ok(None) } else { store.claim_dir_with_generation() }
            };
            match work {
                Ok(Some((queue_id, root_id, dir_path, claimed_gen))) => {
                    if let Err(e) = self.process_dir(queue_id, root_id, &dir_path, claimed_gen) {
                        warn!(dir = %dir_path, error = %e, "directory scan failed");
                    }
                    processed += 1;
                }
                _ => break,
            }
        }
        processed
    }

    fn crawl_worker(self: Arc<Self>) {
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return;
            }
            if self.indexing_pause_reason().is_some() { self.wait_for_work(); continue; }
            let work = { let mut store = self.store_lock();
                if self.indexing_pause_reason().is_some() { Ok(None) } else { store.claim_dir_with_generation() }
            };
            match work {
                Ok(Some((queue_id, root_id, dir_path, claimed_gen))) => {
                    if let Err(e) = self.process_dir(queue_id, root_id, &dir_path, claimed_gen) {
                        warn!(dir = %dir_path, error = %e, "directory scan failed");
                    }
                }
                Ok(None) => {
                    // Queue drained: idle until work arrives or shutdown.
                    let mut guard = self.wake.0.lock().unwrap_or_else(|p| p.into_inner());
                    if !*guard && !self.shutdown.load(Ordering::SeqCst) {
                        let (g, _) = self
                            .wake
                            .1
                            .wait_timeout(guard, std::time::Duration::from_millis(500))
                            .unwrap_or_else(|p| p.into_inner());
                        guard = g;
                    }
                    *guard = false;
                }
                Err(e) => {
                    warn!(error = %e, "claim_dir failed");
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            }
        }
    }

    fn relative_depth(&self, root_id: i64, dir_path: &str) -> usize {
        self.root_snapshot()
            .iter()
            .find(|r| r.id == root_id)
            .and_then(|root| Path::new(dir_path).strip_prefix(&root.path).ok())
            .map(|rel| rel.components().count())
            .unwrap_or(0)
    }

    fn is_excluded_under_root(&self, root_id: i64, path: &Path, is_dir: bool) -> bool {
        if !self.name_path_allowed(path) { return true; }
        let roots = self.root_snapshot();
        let rel = roots
            .iter()
            .find(|r| r.id == root_id)
            .and_then(|r| path.strip_prefix(&r.path).ok());
        let Some(rel) = rel else {
            return path
                .file_name()
                .map(|n| crawl::is_excluded(&n.to_string_lossy(), is_dir))
                .unwrap_or(false);
        };
        let comps: Vec<_> = rel
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(s) => Some(s.to_string_lossy()),
                _ => None,
            })
            .collect();
        let Some((leaf, ancestors)) = comps.split_last() else {
            return false;
        };
        ancestors.iter().any(|a| crawl::is_excluded(a, true))
            || crawl::is_excluded(leaf, is_dir)
    }

    fn process_dir(
        &self,
        queue_id: i64,
        root_id: i64,
        dir_path: &str,
        claimed_generation: i64,
    ) -> Result<(), StoreError> {
        if !self.name_path_allowed(Path::new(dir_path)) { self.store_lock().complete_dir(queue_id)?; return Ok(()); }
        let depth = self.relative_depth(root_id, dir_path);
        let parent_id = self
            .store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .find_by_path(dir_path)?
            .map(|e| e.id);

        let mut scan_failed = false;
        let mut scan = match crawl::scan_directory(root_id, dir_path, parent_id, depth) {
            Ok(s) => s,
            Err(e) => {
                warn!(dir = %dir_path, error = %e, "scan error");
                scan_failed = e.kind() != std::io::ErrorKind::NotFound;
                crawl::DirScan::default()
            }
        };

        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        scan.entries.retain(|e| self.name_path_allowed(Path::new(&e.path)));
        scan.subdirs.retain(|p| self.name_path_allowed(Path::new(p)));
        let generation = self.current_generation(&store, root_id)?;
        let superseded = claimed_generation != generation || !self.name_path_allowed(Path::new(dir_path));
        if !superseded {
            if scan_failed {
                store.touch_subtree_generation(root_id, dir_path, generation)?;
            } else {
                let outcomes = store.apply_batch(root_id, generation, &scan.entries)?;
                Self::enqueue_name_jobs_for_outcomes(&mut store, &scan.entries, &outcomes)?;
                for sub in &scan.subdirs {
                    store.enqueue_dir(root_id, sub)?;
                }
                if !scan.complete {
                    store.touch_subtree_generation(root_id, dir_path, generation)?;
                }
            }
        }
        store.complete_dir(queue_id)?;

        let entries = if superseded {
            0
        } else {
            scan.entries.len() as u64
        };
        let root_done = store.pending_for_root(root_id)? == 0;
        drop(store);

        if !superseded {
            let mut counters = self.counters.lock().unwrap_or_else(|p| p.into_inner());
            counters.files_discovered += entries;
            counters.files_indexed += scan
                .entries
                .iter()
                .filter(|e| matches!(e.kind().hit_kind(), HitKind::Application) || !e.is_dir)
                .count() as u64;
        }

        if root_done && !superseded {
            // The scan of this root is complete: drop everything not seen in
            // this generation (files deleted while we were away).
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            let generation = self.current_generation(&store, root_id)?;
            let swept = store.sweep_generation(root_id, generation)?;
            drop(store);
            if swept > 0 {
                self.invalidate_vector_cache();
                info!(root_id, swept, "swept stale entries");
            }
        }
        Ok(())
    }

    fn current_generation(&self, store: &IndexStore, root_id: i64) -> Result<i64, StoreError> {
        Ok(store
            .roots()?
            .into_iter()
            .find(|r| r.id == root_id)
            .map(|r| r.current_generation)
            .unwrap_or(0))
    }

    // ---- watcher events ----

    pub(crate) fn handle_event(&self, root_id: i64, event: WatchEvent) {
        if let Err(e) = self.apply_event(root_id, &event) {
            warn!(?event, error = %e, "event handling failed");
        }
    }

    fn apply_event(&self, root_id: i64, event: &WatchEvent) -> Result<(), StoreError> {
        if !self.root_snapshot().iter().any(|r| r.id == root_id) { return Ok(()); }
        match event {
            WatchEvent::Created(path) => {
                let is_dir = path.is_dir();
                if !self.is_excluded_under_root(root_id, path, is_dir) {
                    let path_str = path.to_string_lossy().into_owned();
                    if is_dir {
                        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                        if !store.root_enabled(root_id)? || self.is_excluded_under_root(root_id, path, true) { return Ok(()); }
                        store.enqueue_dir(root_id, &path_str)?;
                        drop(store);
                        self.notify_work();
                    } else {
                        self.point_upsert(root_id, path)?;
                    }
                }
            }
            WatchEvent::Deleted(path) => {
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                if !store.root_enabled(root_id)? { return Ok(()); }
                let removed = store.remove_tree(&path.to_string_lossy())?;
                drop(store);
                if removed > 0 {
                    self.invalidate_vector_cache();
                }
            }
            WatchEvent::Modified(path) => {
                if !self.is_excluded_under_root(root_id, path, path.is_dir()) {
                    self.point_upsert(root_id, path)?;
                    let mut store = self.store_lock();
                    if store.root_enabled(root_id)?
                        && !self.is_excluded_under_root(root_id, path, path.is_dir())
                    {
                        if let Some(row) = store.find_by_path(&path.to_string_lossy())? {
                            if !row.is_dir && !row.is_app {
                                store.invalidate_content_source(row.id)?;
                                if let Some(kind) = modality_kind_for_ext(&row.ext) {
                                    store.enqueue_extract_job_with_priority(
                                        row.id,
                                        kind,
                                        compute_extract_priority(
                                            row.size,
                                            row.mtime,
                                            UpsertOutcome::Updated,
                                        ),
                                    )?;
                                }
                            }
                        }
                    }
                    drop(store);
                    self.invalidate_vector_cache();
                    self.notify_work();
                }
            }
            WatchEvent::Renamed { from, to } => {
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                if !store.root_enabled(root_id)? { return Ok(()); }
                let to_excluded = self.is_excluded_under_root(root_id, to, to.is_dir());
                let from_str = from.to_string_lossy().into_owned();
                if to_excluded {
                    let removed = store.remove_tree(&from_str)?;
                    drop(store);
                    if removed > 0 {
                        self.invalidate_vector_cache();
                    }
                } else {
                    let to_str = to.to_string_lossy().into_owned();
                    let existing = store.find_by_path(&from_str)?;
                    match existing {
                        Some(row) => {
                            let name = to
                                .file_name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_default();
                            let ext = ws_index::model::extension_of(&name);
                            let parent_id = store
                                .find_by_path(&parent_of(&to_str))?
                                .map(|e| e.id);
                            store.move_entry(row.id, parent_id, &to_str, &name, &ext)?;
                            if !row.is_dir {
                                store.invalidate_content_source(row.id)?;
                                if let Some(kind) = modality_kind_for_ext(&ext) {
                                    store.enqueue_extract_job_with_priority(
                                        row.id,
                                        kind,
                                        compute_extract_priority(
                                            row.size,
                                            row.mtime,
                                            UpsertOutcome::Updated,
                                        ),
                                    )?;
                                }
                            }
                            // The name changed: the stored name vector is stale.
                            // Re-enqueue so the scheduler embeds the new name;
                            // entry id (and its segment rows) is preserved.
                            store.enqueue_extract_job_with_priority(
                                row.id,
                                SEGMENT_KIND_NAME,
                                EXTRACT_PRIORITY_UPDATED,
                            )?;
                            let mut invalidate_cache = false;
                            if !row.is_dir && !row.ext.eq_ignore_ascii_case(&ext) {
                                let old_mod = ws_extract::modality_kind_for_ext(&row.ext);
                                let new_mod = ws_extract::modality_kind_for_ext(&ext);
                                store.delete_content_text(row.id)?;
                                if let Some(om) = old_mod {
                                    store.remove_segments_for_entry_and_kind(row.id, om)?;
                                    if om == ws_index::model::SEGMENT_KIND_IMAGE {
                                        store.remove_segments_for_entry_and_kind(
                                            row.id,
                                            ws_index::model::SEGMENT_KIND_OCR,
                                        )?;
                                    }
                                    invalidate_cache = true;
                                }
                                if let Some(nm) = new_mod {
                                    let prio = compute_extract_priority(
                                        row.size,
                                        row.mtime,
                                        UpsertOutcome::Updated,
                                    );
                                    store.enqueue_extract_job_with_priority(row.id, nm, prio)?;
                                }
                            }
                            if row.is_dir {
                                store.rename_tree_paths(&from_str, &to_str)?;
                                drop(store);
                                if invalidate_cache {
                                    self.invalidate_vector_cache();
                                }
                                let mut s = self.store.lock().unwrap_or_else(|p| p.into_inner());
                                if s.root_enabled(root_id)? && !self.is_excluded_under_root(root_id, to, true) {
                                    s.enqueue_dir(root_id, &to_str)?;
                                }
                                drop(s);
                                self.notify_work();
                            } else if invalidate_cache {
                                drop(store);
                                self.invalidate_vector_cache();
                            }
                        }
                        None => {
                            // Missed the create (e.g. move into the tree).
                            drop(store);
                            self.point_upsert(root_id, to)?;
                        }
                    }
                }
            }
            WatchEvent::Overflow => {
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                if !store.root_enabled(root_id)? { return Ok(()); }
                let root = self
                    .root_snapshot()
                    .iter()
                    .find(|r| r.id == root_id)
                    .map(|r| r.path.to_string_lossy().into_owned());
                if let Some(root_path) = root {
                    let gen = store.begin_generation(root_id)?;
                    store.requeue_dir(root_id, &root_path)?;
                    store.set_checkpoint(&format!("root:{root_id}:generation"), &gen.to_string())?;
                }
                drop(store);
                self.notify_work();
            }
        }
        {
            let mut counters = self.counters.lock().unwrap_or_else(|p| p.into_inner());
            counters.events_processed += 1;
        }
        Ok(())
    }

    fn point_upsert(&self, root_id: i64, path: &Path) -> Result<(), StoreError> {
        if self.is_excluded_under_root(root_id, path, path.is_dir()) {
            return Ok(());
        }
        let parent_id = path
            .parent()
            .and_then(|p| {
                self.store
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .find_by_path(&p.to_string_lossy())
                    .ok()
                    .flatten()
            })
            .map(|e| e.id);

        let record = match crawl::record_for_path(root_id, parent_id, path) {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(()), // excluded or vanished
            Err(e) => {
                warn!(path = %path.display(), error = %e, "stat failed");
                return Ok(());
            }
        };

        self.commit_point_record(root_id, &record)
    }

    fn commit_point_record(&self, root_id: i64, record: &EntryRecord) -> Result<(), StoreError> {
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        if !store.root_enabled(root_id)?
            || self.is_excluded_under_root(root_id, Path::new(&record.path), record.is_dir)
        {
            return Ok(());
        }
        let generation = self.current_generation(&store, root_id)?;
        let outcomes = store.apply_batch(root_id, generation, std::slice::from_ref(record))?;
        let mut changed = false;
        for outcome in &outcomes {
            if *outcome != UpsertOutcome::Unchanged {
                changed = true;
                let mut counters = self.counters.lock().unwrap_or_else(|p| p.into_inner());
                counters.files_indexed += 1;
            }
        }
        if changed {
            Self::enqueue_name_jobs_for_outcomes(
                &mut store,
                std::slice::from_ref(record),
                &outcomes,
            )?;
        }
        Ok(())
    }

    // ---- catalog ----

    fn catalog_loop(self: Arc<Self>) {
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return;
            }
            if let Err(e) = self.refresh_catalog() {
                warn!(error = %e, "catalog refresh failed");
            }
            // Sleep in small steps so shutdown is responsive.
            let mut remaining = CATALOG_REFRESH_INTERVAL;
            while remaining > std::time::Duration::ZERO && !self.shutdown.load(Ordering::SeqCst) {
                let step = remaining.min(std::time::Duration::from_secs(1));
                std::thread::sleep(step);
                remaining -= step;
            }
        }
    }

    fn refresh_catalog(&self) -> Result<(), StoreError> {
        let scan = catalog::scan_catalog_full(&catalog::app_roots());
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let root_id = store.ensure_root(CATALOG_ROOT)?;
        let generation = store.begin_generation(root_id)?;

        let records: Vec<EntryRecord> = scan
            .apps
            .iter()
            .map(|app| EntryRecord {
                root_id,
                parent_id: None,
                path: app.path.clone(),
                name: app.name.clone(),
                ext: ws_index::model::extension_of(&app.path),
                is_dir: false,
                size: 0,
                mtime: 0,
                volume_serial: 0,
                file_id: 0,
                is_app: true,
                app_target: app.target.clone(),
                keywords: app.keywords.clone(),
            })
            .collect();

        let outcomes = store.apply_batch(root_id, generation, &records)?;
        Self::enqueue_name_jobs_for_outcomes(&mut store, &records, &outcomes)?;
        let swept = if scan.complete {
            store.sweep_generation(root_id, generation)?
        } else {
            warn!("catalog scan incomplete due to I/O error; skipping stale sweep");
            0
        };
        drop(store);
        if swept > 0 {
            self.invalidate_vector_cache();
        }
        info!(apps = records.len(), swept, "application catalog refreshed");
        Ok(())
    }

    // ---- drive supervision ----

    // ---- semantic feed helper ----

    /// Enqueues one `extract_jobs(kind='name')` per changed record
    /// (`UpsertOutcome != Unchanged`), plus a modality content extraction job
    /// (`doc`, `pdf`, `image`, `audio`, `video`) for non-dir, non-app files
    /// whose extension maps to a supported modality. Content jobs use
    /// `compute_extract_priority(size, mtime, outcome)` so small and recent
    /// files drain first. Must hold the store lock.
    fn enqueue_name_jobs_for_outcomes(
        store: &mut IndexStore,
        records: &[EntryRecord],
        outcomes: &[UpsertOutcome],
    ) -> Result<(), StoreError> {
        for (rec, outcome) in records.iter().zip(outcomes.iter()) {
            let priority = match outcome {
                UpsertOutcome::Unchanged => continue,
                UpsertOutcome::Inserted => EXTRACT_PRIORITY_NEW,
                UpsertOutcome::Updated => EXTRACT_PRIORITY_UPDATED,
            };
            // The upsert just wrote the row: resolve its id by path.
            if let Some(row) = store.find_by_path(&rec.path)? {
                store.enqueue_extract_job_with_priority(
                    row.id,
                    SEGMENT_KIND_NAME,
                    priority,
                )?;
                if !rec.is_dir && !rec.is_app {
                    if let Some(modality_kind) = modality_kind_for_ext(&rec.ext) {
                        let content_priority =
                            compute_extract_priority(rec.size, rec.mtime, *outcome);
                        store.enqueue_extract_job_with_priority(
                            row.id,
                            modality_kind,
                            content_priority,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    // ---- semantic pipeline (P3.5) ----

    /// Runs one embed batch synchronously: claims up to [`EMBED_BATCH`]
    /// `name` jobs, embeds their current entry names via `embed`, and upserts
    /// one `segments(kind='name', chunk 0)` per success (replacing the prior
    /// vector for the entry, stable segment id).
    ///
    /// Correctness rules (fix round 1/5):
    /// - I2 (lost-update): names are snapshotted at claim (`name`, `mtime`,
    ///   job `priority`); after the embed each entry is re-resolved under
    ///   lock and a mismatch requeues the job (`requeue_extract_job`,
    ///   attempts untouched) instead of upserting the stale vector and
    ///   consuming the change signal. Detection covers renames always, and
    ///   content changes whenever the second write ticks `mtime` or escalates
    ///   priority (same-second same-priority rewrites inside the claim window
    ///   can still slip; the next future change re-embeds).
    /// - I1 (poison cap): batch/row failures increment attempts; at
    ///   [`MAX_EMBED_ATTEMPTS`] the job is `mark_extract_failed` (poison
    ///   pill, never auto-claimed) with a health `warn!` instead of spinning
    ///   forever. Entries deleted between claim and write complete their job
    ///   without a segment. The store lock is never held across the embed
    ///   call, so crawl workers keep moving while a batch (or its
    ///   [`EMBED_JOB_TIMEOUT`] watchdog) is in flight.
    ///
    /// `embed` is injectable so tests use deterministic fake vectors without
    /// a worker process; production passes the supervisor's bounded call.
    pub fn embed_once_with<F>(&self, embed: F) -> Result<EmbedBatchOutcome, StoreError>
    where
        F: Fn(&[String]) -> Result<Vec<Vec<f32>>, String>,
    {
        if self.indexing_pause_reason().is_some() { return Ok(EmbedBatchOutcome::default()); }
        let claimed: Vec<ClaimedNameJob> = {
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            if self.indexing_pause_reason().is_some() { return Ok(EmbedBatchOutcome::default()); }
            let jobs = store.claim_extract_jobs_batch_for_kind(SEGMENT_KIND_NAME, EMBED_BATCH)?;
            let mut out = Vec::with_capacity(jobs.len());
            for job in jobs {
                match store.get_entry(job.entry_id)? {
                    Some(row) => out.push(ClaimedNameJob {
                        job_id: job.id,
                        entry_id: job.entry_id,
                        name: row.name,
                        mtime: row.mtime,
                        job_priority: job.priority,
                        attempts: job.attempts,
                    }),
                    None => {
                        // Entry vanished after enqueue (delete cascades the
                        // job row too, but a pre-claim delete can strand the
                        // claim): drop the job, no segment.
                        store.complete_extract_job(job.id)?;
                    }
                }
            }
            out
        };
        if claimed.is_empty() {
            return Ok(EmbedBatchOutcome::default());
        }
        let texts: Vec<String> = claimed.iter().map(|c| c.name.clone()).collect();
        let vecs = match embed(&texts) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, batch = claimed.len(), "embed batch failed; releasing claims for retry");
                let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
                let mut failed = 0usize;
                let mut poisoned = 0usize;
                for c in &claimed {
                    // Best-effort: the entry may have been deleted mid-batch
                    // (cascade already removed the job → 0 rows touched).
                    if Self::fail_job_capped(&mut store, c) {
                        poisoned += 1;
                    } else {
                        failed += 1;
                    }
                }
                return Ok(EmbedBatchOutcome {
                    claimed: claimed.len(),
                    embedded: 0,
                    failed,
                    requeued: 0,
                    poisoned,
                });
            }
        };
        if vecs.len() != claimed.len() {
            warn!(got = vecs.len(), want = claimed.len(), "embed arity mismatch; releasing batch for retry");
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            let mut failed = 0usize;
            let mut poisoned = 0usize;
            for c in &claimed {
                if Self::fail_job_capped(&mut store, c) {
                    poisoned += 1;
                } else {
                    failed += 1;
                }
            }
            return Ok(EmbedBatchOutcome {
                claimed: claimed.len(),
                embedded: 0,
                failed,
                requeued: 0,
                poisoned,
            });
        }
        let mut embedded = 0usize;
        let mut failed = 0usize;
        let mut requeued = 0usize;
        let mut poisoned = 0usize;
        {
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            for (c, vec) in claimed.iter().zip(vecs.iter()) {
                // The entry may have been deleted while embedding: then the
                // job row is already gone via cascade; completing is a no-op.
                let fresh_entry = store.get_entry(c.entry_id)?;
                let Some(fresh) = fresh_entry else {
                    let _ = store.complete_extract_job(c.job_id);
                    continue;
                };
                if !fresh.is_app && !self.name_path_allowed(Path::new(&fresh.path)) {
                    store.requeue_extract_job(c.job_id)?; continue;
                }
                // I2: the entry changed under our claimed batch (the
                // concurrent write already bumped the job's priority via the
                // `ON CONFLICT ... priority` path). Requeue the fresh data
                // instead of upserting the stale vector and consuming the
                // change signal. Attempts are untouched: churn is not failure.
                let fresh_job = store.get_extract_job(c.job_id)?;
                let changed = fresh.name != c.name
                    || fresh.mtime != c.mtime
                    || fresh_job
                        .as_ref()
                        .is_some_and(|j| j.priority != c.job_priority);
                if changed {
                    if fresh_job.is_some() {
                        let _ = store.requeue_extract_job(c.job_id);
                        requeued += 1;
                    } else {
                        let _ = store.complete_extract_job(c.job_id);
                    }
                    continue;
                }
                match store.upsert_segment(NewSegment {
                    entry_id: c.entry_id,
                    kind: SEGMENT_KIND_NAME,
                    chunk_index: 0,
                    text: &fresh.name,
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: vec,
                }) {
                    Ok(seg_id) => {
                        store.complete_extract_job(c.job_id)?;
                        self.update_vector_cache(seg_id, c.entry_id, SEGMENT_KIND_NAME, vec);
                        embedded += 1;
                    }
                    Err(e) => {
                        warn!(job = c.job_id, error = %e, "segment upsert failed; releasing for retry");
                        if Self::fail_job_capped(&mut store, c) {
                            poisoned += 1;
                        } else {
                            failed += 1;
                        }
                    }
                }
            }
        }
        Ok(EmbedBatchOutcome {
            claimed: claimed.len(),
            embedded,
            failed,
            requeued,
            poisoned,
        })
    }

    /// Records one job failure under the poison-pill cap: at
    /// [`MAX_EMBED_ATTEMPTS`] the job is `mark_extract_failed` (with a health
    /// log carrying the entry) instead of requeued. Returns true when the job
    /// was poisoned. Best-effort (vanished rows are no-ops).
    fn fail_job_capped(store: &mut IndexStore, c: &ClaimedNameJob) -> bool {
        if c.attempts + 1 >= MAX_EMBED_ATTEMPTS {
            warn!(
                job = c.job_id,
                entry = c.entry_id,
                name = %c.name,
                attempts = c.attempts + 1,
                "name job failing repeatedly; poison-pilled (failed, never auto-claimed)"
            );
            // Best-effort: a mid-batch delete cascades the row (0 rows).
            let _ = store.mark_extract_failed(c.job_id);
            true
        } else {
            let _ = store.fail_extract_job(c.job_id);
            false
        }
    }

    /// Production entry: one batch through the supervisor's bounded call
    /// ([`EMBED_JOB_TIMEOUT`]; a hung worker fails the batch instead of
    /// stalling the scheduler).
    pub fn embed_once_with_supervisor(
        &self,
        sup: &Arc<EmbedSupervisor>,
    ) -> Result<EmbedBatchOutcome, StoreError> {
        self.embed_once_with(|texts| {
            sup.embed_texts_bounded(texts, EMBED_JOB_TIMEOUT)
                .map_err(|e| e.to_string())
        })
    }

    /// Starts the background embed scheduler (one `std::thread`): drains
    /// `extract_jobs(kind='name')` in [`EMBED_BATCH`] batches until shutdown.
    /// Crash-safe: claims are held per batch and released on failure or on
    /// restart (`release_extract_claims` in [`Self::open`]); segment upserts
    /// are idempotent (stable id, replace-prior-vector).
    pub fn start_embed_scheduler(self: &Arc<Self>, embed: Arc<EmbedSupervisor>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("winsearch-embed-sched".into())
            .spawn(move || {
                loop {
                    let Some(indexer) = weak.upgrade() else {
                        return;
                    };
                    if indexer.shutdown.load(Ordering::SeqCst) {
                        return;
                    }
                    match indexer.embed_once_with_supervisor(&embed) {
                        Ok(out) if out.claimed == 0 => {
                            indexer.wait_for_work();
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, "embed batch error");
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                }
            })
            .ok();
    }

    // ---- drive supervision ----

    fn drive_supervisor(self: Arc<Self>) {
        loop {
            for _ in 0..DRIVE_POLL_INTERVAL.as_secs() {
                if self.shutdown.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            self.reconcile_drives();
        }
    }

    /// One reconciliation pass over all roots: flags vanished volumes as
    /// missing (index preserved) and rescans roots that came back.
    fn reconcile_drives(self: &Arc<Self>) {
        for root in &self.root_snapshot() {
            let present = root.path.is_dir();
            let was_offline = root.offline.load(Ordering::Acquire);
            let has_missing = self.root_marked_missing(root.id).unwrap_or(false);
            if present {
                if was_offline || has_missing {
                    info!(root = %root.path.display(), "volume reconnected; reconciling");
                    if let Err(e) = self.reconnect_root(root) {
                        warn!(error = %e, "reconnect failed");
                    } else {
                        root.offline.store(false, Ordering::Release);
                        root.missing_mark_pending.store(false, Ordering::Release);
                    }
                }
            } else if !was_offline || root.missing_mark_pending.load(Ordering::Acquire) {
                info!(root = %root.path.display(), "volume disconnected; preserving index");
                root.offline.store(true, Ordering::Release);
                root.missing_mark_pending.store(true, Ordering::Release);
                let mut store =
                    self.store.lock().unwrap_or_else(|p| p.into_inner());
                match store.set_missing_for_root(root.id, true) {
                    Ok(_) => root.missing_mark_pending.store(false, Ordering::Release),
                    Err(e) => warn!(error = %e, "marking missing failed"),
                }
            }
        }
    }

    fn root_marked_missing(&self, root_id: i64) -> Result<bool, StoreError> {
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let any_missing: bool = store.has_missing_entries(root_id)?;
        Ok(any_missing)
    }

    fn reconnect_root(self: &Arc<Self>, root: &RootInfo) -> Result<(), StoreError> {
        let _change = self.configuration_change.lock().unwrap_or_else(|p| p.into_inner());
        if !self.store_lock().root_enabled(root.id)? { return Ok(()); }
        {
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            store.set_missing_for_root(root.id, false)?;
        }
        // Rescan reconciles renames/deletes that happened while disconnected.
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let gen = store.begin_generation(root.id)?;
        store.enqueue_dir(root.id, &root.path.to_string_lossy())?;
        store.set_checkpoint(&format!("root:{}:generation", root.id), &gen.to_string())?;
        drop(store);
        self.notify_work();

        // Watchers die with their volume: replace any that stopped.
        let mut watchers = self.watchers.lock().unwrap_or_else(|p| p.into_inner());
        watchers.retain(|w| w.is_alive());
        if !watchers.iter().any(|w| w.watches(&root.path)) {
            if let Some(w) = self.spawn_watcher(root) {
                watchers.push(w);
            }
        }
        Ok(())
    }

    fn spawn_watcher(self: &Arc<Self>, root: &RootInfo) -> Option<Watcher> {
        let root_id = root.id;
        let weak: Weak<Indexer> = Arc::downgrade(self);
        watcher::spawn(&root.path, move |event| {
            if let Some(indexer) = weak.upgrade() {
                indexer.handle_event(root_id, event);
            }
        })
        .map_err(|e| {
            warn!(root = %root.path.display(), error = %e, "watcher start failed");
            e
        })
        .ok()
    }
}

/// Pseudo-root for the application catalog (its entries carry real paths).
pub const CATALOG_ROOT: &str = "@apps";

fn parent_of(path: &str) -> String {
    Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

impl Drop for Indexer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.wake.1.notify_all();
        let mut watchers = self.watchers.lock().unwrap_or_else(|p| p.into_inner());
        watchers.clear(); // each Watcher::drop cancels I/O and joins its thread
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[test]
    fn watcher_point_commit_rechecks_exclusions_after_metadata_collection() {
        let tree = TempTree::new();
        let indexer =
            Indexer::open(IndexStore::open_in_memory().unwrap(), &[tree.0.clone()]).unwrap();
        let root_id = indexer.root_snapshot()[0].id;
        for extension_rule in [false, true] {
            let file = tree.0.join(if extension_rule {
                "extension.race"
            } else {
                "path.race"
            });
            fs::write(&file, b"must never be committed").unwrap();
            assert!(!indexer.is_excluded_under_root(root_id, &file, false));
            let record = crawl::record_for_path(root_id, None, &file)
                .unwrap()
                .unwrap();
            // Deterministically revoke consent after the real metadata snapshot,
            // before the same commit function used by point_upsert takes its lock.
            let mut config = indexer.get_indexing_config().unwrap();
            if extension_rule {
                config.excluded_extensions.push("race".into());
            } else {
                config
                    .excluded_paths
                    .push(file.to_string_lossy().into_owned());
            }
            indexer.set_indexing_config(&config).unwrap();
            indexer.commit_point_record(root_id, &record).unwrap();
            let store = indexer.store_lock();
            assert!(store.find_by_path(&record.path).unwrap().is_none());
            assert_eq!(
                store
                    .extract_queue_depth_for_kind(SEGMENT_KIND_NAME)
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn removed_root_readded_offline_preserves_identity_and_reconciles() {
        let tree=TempTree::new(); let drive=tree.0.join("drive"); fs::create_dir_all(&drive).unwrap();
        let file=drive.join("offline.txt"); fs::write(&file,"old").unwrap();
        let indexer=Indexer::open(IndexStore::open_in_memory().unwrap(),&[drive.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap(); while indexer.scan_available(64)>0 {}
        let id=indexer.store_lock().find_by_path(&file.to_string_lossy()).unwrap().unwrap().id;
        let selected=indexer.get_indexing_config().unwrap(); let mut empty=selected.clone(); empty.roots.clear();
        indexer.set_indexing_config(&empty).unwrap();
        fs::remove_file(&file).unwrap(); fs::remove_dir(&drive).unwrap();
        indexer.set_indexing_config(&selected).unwrap();
        let row=indexer.store_lock().get_entry(id).unwrap().unwrap(); assert!(row.missing); assert_eq!(row.id,id);
        fs::create_dir_all(&drive).unwrap(); fs::write(&file,"reconnected").unwrap();
        indexer.reconcile_drives(); while indexer.scan_available(64)>0 {}
        let row=indexer.store_lock().find_by_path(&file.to_string_lossy()).unwrap().unwrap(); assert_eq!(row.id,id); assert!(!row.missing);
    }
    #[test]
    fn dynamic_policy_revokes_claims_results_and_resumes_durable_work() {
        let tree = TempTree::new();
        let a = tree.0.join("A");
        let b = tree.0.join("B");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        fs::write(a.join("uniquealpha.txt"), "ultrauniquepolicyalpha").unwrap();
        fs::write(b.join("uniquebeta.txt"), "beta content").unwrap();
        let indexer = Indexer::open(IndexStore::open_in_memory().unwrap(), &[a.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
            .embed_once_with(|t| Ok(t.iter().map(|_| vec![1., 0.]).collect()))
            .unwrap();
        indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |t| {
                Ok(t.iter().map(|_| vec![1., 0.]).collect())
            })
            .unwrap();
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "ultrauniquepolicyalpha".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        assert!(!indexer.search_vectors(&[1.,0.],10,None).unwrap().is_empty());
        let aid = indexer.root_snapshot()[0].id;
        indexer
            .store_lock()
            .enqueue_dir(aid, &a.to_string_lossy())
            .unwrap();
        let stale = indexer
            .store_lock()
            .claim_dir_with_generation()
            .unwrap()
            .unwrap();
        let mut c = indexer.get_indexing_config().unwrap();
        c.roots = vec![b.to_string_lossy().into_owned()];
        c.paused = true;
        indexer.set_indexing_config(&c).unwrap();
        assert!(indexer
            .search(&SearchRequest {
                text: "alpha".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        assert!(indexer
            .search_vectors(&[1., 0.], 10, None)
            .unwrap()
            .is_empty());
        assert!(indexer
            .search(&SearchRequest {
                text: "ultrauniquepolicyalpha".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        indexer
            .process_dir(stale.0, stale.1, &stale.2, stale.3)
            .unwrap();
        let depth = indexer.status().unwrap().queue_depth;
        assert!(depth > 0);
        assert_eq!(indexer.scan_available(64), 0);
        assert_eq!(indexer.status().unwrap().queue_depth, depth);
        assert_eq!(
            indexer
                .embed_once_with(|_| panic!("paused"))
                .unwrap()
                .claimed,
            0
        );
        c.paused = false;
        indexer.set_indexing_config(&c).unwrap();
        while indexer.scan_available(64) > 0 {}
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "beta".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        assert!(indexer
            .store_lock()
            .find_by_path(&a.join("uniquealpha.txt").to_string_lossy())
            .unwrap()
            .is_some());
        c.roots.push(a.to_string_lossy().into_owned());
        indexer.set_indexing_config(&c).unwrap();
        while indexer.scan_available(64) > 0 {}
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "alpha".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn revoked_inflight_name_and_content_jobs_cannot_commit() {
        let tree = TempTree::new();
        fs::write(tree.0.join("consent.txt"), "uniquepolicycontent").unwrap();
        let indexer = seeded(&tree);
        let mut allowed = indexer.get_indexing_config().unwrap();
        let mut revoked = allowed.clone();
        revoked.roots.clear();
        let out = indexer
            .embed_once_with(|texts| {
                indexer.set_indexing_config(&revoked).unwrap();
                Ok(texts.iter().map(|_| vec![1., 0.]).collect())
            })
            .unwrap();
        assert_eq!(out.embedded, 0);
        allowed.paused = false;
        indexer.set_indexing_config(&allowed).unwrap();
        while indexer.scan_available(64) > 0 {}
        let out = indexer
            .extract_once_with(&crate::extract::ExtractorRegistry::default(), |texts| {
                indexer.set_indexing_config(&revoked).unwrap();
                Ok(texts.iter().map(|_| vec![1., 0.]).collect())
            })
            .unwrap();
        assert!(out.requeued);
        assert_eq!(out.segments_stored, 0);
        assert_eq!(out.fts_chunks_indexed, 0);
        assert!(indexer
            .search(&SearchRequest {
                text: "uniquepolicycontent".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    struct TempTree(PathBuf);

    impl TempTree {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-indexer-test-{}-{:?}",
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

    fn seeded(tree: &TempTree) -> Arc<Indexer> {
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
    }

    #[test]
    fn full_scan_indexes_files_and_search_finds_them() {
        let tree = TempTree::new();
        fs::write(tree.0.join("quarterly-report.pdf"), b"x").unwrap();
        let indexer = seeded(&tree);

        let hits = indexer
            .search(&SearchRequest {
                text: "quarterly".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "quarterly-report.pdf");
    }

    #[test]
    fn rename_event_preserves_identity() {
        let tree = TempTree::new();
        let file = tree.0.join("draft.txt");
        fs::write(&file, b"x").unwrap();
        let indexer = seeded(&tree);

        let before = indexer
            .search(&SearchRequest {
                text: "draft".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        let id_before = before[0].entry_id;

        // Rename on disk, then deliver the watcher event.
        let renamed = tree.0.join("final.txt");
        fs::rename(&file, &renamed).unwrap();
        let root_id = indexer.root_snapshot()[0].id;
        indexer.handle_event(
            root_id,
            WatchEvent::Renamed {
                from: file.clone(),
                to: renamed.clone(),
            },
        );

        let after = indexer
            .search(&SearchRequest {
                text: "final".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(after.len(), 1, "renamed file should be searchable");
        assert_eq!(after[0].entry_id, id_before, "identity preserved across rename");
        assert!(
            indexer
                .search(&SearchRequest {
                    text: "draft".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            "old name must not match anymore"
        );
    }

    #[test]
    fn delete_event_removes_entries() {
        let tree = TempTree::new();
        fs::write(tree.0.join("obsolete.log"), b"x").unwrap();
        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;

        let path = tree.0.join("obsolete.log");
        fs::remove_file(&path).unwrap();
        indexer.handle_event(root_id, WatchEvent::Deleted(path));

        assert!(indexer
            .search(&SearchRequest {
                text: "obsolete".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rescan_after_crash_resumes_from_queue() {
        let tree = TempTree::new();
        fs::write(tree.0.join("resume-me.txt"), b"x").unwrap();
        let store = IndexStore::open_in_memory().unwrap();
        let mut store2 = store;
        let root_id = store2.ensure_root(&tree.path_string()).unwrap();
        store2.enqueue_dir(root_id, &tree.path_string()).unwrap();
        // Simulate a crash mid-scan: the claim stays set.
        assert!(store2.claim_dir().unwrap().is_some());

        // Restart: a fresh Indexer releases claims and re-scans.
        drop(store2);
        let _ = root_id;
        let indexer = seeded(&tree);
        let hits = indexer
            .search(&SearchRequest {
                text: "resume".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    /// End-to-end disconnected-drive behavior using a `subst` drive letter as
    /// a real removable volume: unmap = disconnect, remap = reconnect.
    #[test]
    fn disconnected_volumes_are_preserved_and_reconciled() {
        let subst = |args: &[&str]| {
            let _ = std::process::Command::new("subst").args(args).output();
        };
        let tree = TempTree::new();
        fs::write(tree.0.join("usb-file.txt"), b"x").unwrap();

        let drive = "W:";
        subst(&["W:", "/d"]); // clean slate
        subst(&["W:", &tree.path_string()]);
        let root = PathBuf::from("W:\\");

        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[root.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}

        let find = |name: &str| {
            indexer
                .search(&SearchRequest {
                    text: name.into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
        };
        assert_eq!(find("usb-file").len(), 1, "indexed through the mapped drive");

        // Disconnect the volume.
        subst(&[drive, "/d"]);
        indexer.reconcile_drives();
        assert!(find("usb-file").is_empty(), "missing entries stay out of results");
        assert_eq!(
            indexer.store.lock().unwrap().stats().unwrap().entries_total,
            1,
            "entries are preserved, not deleted"
        );

        // Reconnect: results come back after reconciliation.
        subst(&["W:", &tree.path_string()]);
        indexer.reconcile_drives();
        while indexer.scan_available(64) > 0 {}
        assert_eq!(find("usb-file").len(), 1, "reconciled after reconnect");

        subst(&["W:", "/d"]);
    }

    impl TempTree {
        fn path_string(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    // ---- semantic pipeline (P3.5) ----

    /// Deterministic fake embed: stub vectors, never the real model.
    fn fake_embed(texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        Ok(ws_embed_protocol::stub_embed_texts(
            texts,
            ws_embed_protocol::STUB_DIM,
        ))
    }

    fn drain_embed_queue(indexer: &Arc<Indexer>) -> (usize, usize) {
        let mut embedded = 0usize;
        let mut batches = 0usize;
        for _ in 0..32 {
            let out = indexer.embed_once_with(fake_embed).unwrap();
            if out.claimed == 0 {
                break;
            }
            embedded += out.embedded;
            batches += 1;
            assert_eq!(out.failed, 0, "fake embed must not fail");
        }
        (embedded, batches)
    }

    fn entry_id_by_name(indexer: &Arc<Indexer>, text: &str) -> i64 {
        indexer
            .search(&SearchRequest {
                text: text.into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .into_iter()
            .next()
            .expect("entry should be searchable")
            .entry_id
    }

    #[test]
    fn pipeline_crawl_enqueues_and_embeds_names() {
        let tree = TempTree::new();
        fs::write(tree.0.join("alpha-report.txt"), b"x").unwrap();
        fs::write(tree.0.join("beta-notes.txt"), b"y").unwrap();
        let indexer = seeded(&tree);

        let depth = indexer.embed_queue_depth().unwrap();
        assert!(depth >= 2, "crawl must enqueue name jobs, got {depth}");

        let (embedded, _) = drain_embed_queue(&indexer);
        assert!(embedded >= 2, "fake embed should store segments");
        assert_eq!(indexer.embed_queue_depth().unwrap(), 0);
        let segs = indexer.segments_indexed().unwrap();
        assert!(segs >= 2, "segments_indexed counts name-segments, got {segs}");

        // Stored vectors are the deterministic stub for the file name
        // (idempotent upsert: a second drain claims nothing, segments stay).
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let row = store
                .find_by_path(&tree.0.join("alpha-report.txt").to_string_lossy())
                .unwrap()
                .unwrap();
            let segs = store.list_segments_for_entry(row.id).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].kind, "name");
            assert_eq!(segs[0].text, "alpha-report.txt");
            let want =
                ws_embed_protocol::stub_vector("alpha-report.txt", ws_embed_protocol::STUB_DIM);
            assert_eq!(segs[0].vector.len(), want.len());
            // FP16 round-trip: cosine ~1 to the stub reference.
            let cos = ws_index::vectors::cosine_similarity(&segs[0].vector, &want);
            assert!(cos > 0.99, "stored vector diverged: cosine {cos}");
        }
        let before = indexer.segments_indexed().unwrap();
        let (embedded2, _) = drain_embed_queue(&indexer);
        assert_eq!(embedded2, 0, "idempotent: no jobs left");
        assert_eq!(indexer.segments_indexed().unwrap(), before);
    }

    #[test]
    fn pipeline_new_files_first_lifo_within_priority() {
        let tree = TempTree::new();
        fs::write(tree.0.join("old.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        drain_embed_queue(&indexer);
        assert_eq!(indexer.embed_queue_depth().unwrap(), 0);

        // An update (low priority) enqueued before a new file (high priority):
        // the new file must embed first even though it arrived later.
        fs::write(tree.0.join("old.txt"), b"changed-contents-longer").unwrap();
        let root_id = indexer.root_snapshot()[0].id;
        indexer
            .handle_event(
                root_id,
                WatchEvent::Modified(tree.0.join("old.txt")),
            );
        fs::write(tree.0.join("brand-new.txt"), b"n").unwrap();
        indexer.handle_event(root_id, WatchEvent::Created(tree.0.join("brand-new.txt")));
        // Drain one batch at a time and record order via segment creation?
        // Simpler: assert priorities at the queue level (store order).
        {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let batch = store
                .claim_extract_jobs_batch_for_kind("name", 10)
                .unwrap();
            assert_eq!(batch.len(), 2);
            assert_eq!(batch[0].priority, ws_index::EXTRACT_PRIORITY_NEW);
            assert_eq!(batch[1].priority, ws_index::EXTRACT_PRIORITY_UPDATED);
            for j in batch {
                store.fail_extract_job(j.id).unwrap();
            }
        }
    }

    #[test]
    fn pipeline_rename_keeps_segment_identity_and_reembeds() {
        let tree = TempTree::new();
        fs::write(tree.0.join("draft.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        drain_embed_queue(&indexer);

        let id_before = entry_id_by_name(&indexer, "draft");
        let seg_before = {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let segs = store.list_segments_for_entry(id_before).unwrap();
            assert_eq!(segs.len(), 1);
            segs[0].id
        };

        // Rename on disk + watcher event: entry id preserved, segment row
        // stays attached (identity), and a re-embed job is queued.
        let renamed = tree.0.join("final.txt");
        fs::rename(tree.0.join("draft.txt"), &renamed).unwrap();
        let root_id = indexer.root_snapshot()[0].id;
        indexer.handle_event(
            root_id,
            WatchEvent::Renamed {
                from: tree.0.join("draft.txt"),
                to: renamed.clone(),
            },
        );
        let id_after = entry_id_by_name(&indexer, "final");
        assert_eq!(id_before, id_after, "rename must preserve entry identity");
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let segs = store.list_segments_for_entry(id_after).unwrap();
            assert_eq!(segs.len(), 1, "segment stays attached across rename");
            assert_eq!(segs[0].id, seg_before, "segment id stable");
        }
        assert!(
            indexer.embed_queue_depth().unwrap() >= 1,
            "rename must requeue the new name"
        );
        drain_embed_queue(&indexer);
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let segs = store.list_segments_for_entry(id_after).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].id, seg_before, "re-embed replaces vector in place");
            assert_eq!(segs[0].text, "final.txt");
        }
    }

    #[test]
    fn pipeline_delete_removes_segments() {
        let tree = TempTree::new();
        fs::write(tree.0.join("obsolete.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        drain_embed_queue(&indexer);
        let id = entry_id_by_name(&indexer, "obsolete");
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            assert_eq!(store.list_segments_for_entry(id).unwrap().len(), 1);
        }
        fs::remove_file(tree.0.join("obsolete.txt")).unwrap();
        indexer.handle_event(
            indexer.root_snapshot()[0].id,
            WatchEvent::Deleted(tree.0.join("obsolete.txt")),
        );
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                store.list_segments_for_entry(id).unwrap().is_empty(),
                "delete must cascade to segments"
            );
        }
        assert!(
            indexer
                .search(&SearchRequest {
                    text: "obsolete".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn pipeline_crash_mid_queue_restart_resumes_idempotently() {
        let tree = TempTree::new();
        fs::write(tree.0.join("crash-a.txt"), b"x").unwrap();
        fs::write(tree.0.join("crash-b.txt"), b"y").unwrap();
        let indexer = seeded(&tree);
        assert!(indexer.embed_queue_depth().unwrap() >= 2);

        // Simulate a crash mid-batch: claim jobs but never complete them.
        let claimed_ids: Vec<i64> = {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let batch = store
                .claim_extract_jobs_batch_for_kind("name", 10)
                .unwrap();
            assert!(!batch.is_empty());
            batch.into_iter().map(|j| j.id).collect()
        };
        // Restart path: `Indexer::open` releases claims; here the same store
        // (the unit equivalent) releases them back to pending.
        {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let released = store.release_extract_claims().unwrap();
            assert_eq!(released as usize, claimed_ids.len());
        }
        // Resume: the full batch embeds, and a repeat embed is a no-op
        // (idempotent upsert, stable segment ids).
        let (embedded, _) = drain_embed_queue(&indexer);
        assert!(embedded >= 2);
        let ids_before: Vec<i64> = {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            store
                .all_segments()
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect()
        };
        // Re-enqueue everything (model-swap style) and re-embed: same ids.
        {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            for eid in store.all_entry_ids().unwrap() {
                store.enqueue_extract_job(eid, "name").unwrap();
            }
        }
        drain_embed_queue(&indexer);
        let ids_after: Vec<i64> = {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            store
                .all_segments()
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect()
        };
        assert_eq!(ids_before, ids_after, "re-embed must be idempotent");
    }

    #[test]
    fn pipeline_failed_batch_releases_claims_for_retry() {
        // A hung worker surfaces as a batch error (the supervisor's bounded
        // call maps the watchdog to `Transport("... timed out ...")`); the
        // pipeline must release every claim (attempts+1) instead of stalling.
        let tree = TempTree::new();
        fs::write(tree.0.join("retry-me.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        assert!(indexer.embed_queue_depth().unwrap() >= 1);

        let out = indexer
            .embed_once_with(|_| Err("embed job timed out after 120s".to_string()))
            .unwrap();
        assert!(out.claimed >= 1);
        assert_eq!(out.embedded, 0);
        assert_eq!(out.failed, out.claimed);
        assert_eq!(out.poisoned, 0, "first failure must not poison");
        // Claims released: still pending, attempts bumped, embeddable after
        // the worker recovers.
        let eid = entry_id_by_name(&indexer, "retry-me");
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let job = store.find_extract_job(eid, "name").unwrap().unwrap();
            assert_eq!(job.attempts, 1);
            assert_eq!(job.state, "pending");
        }
        assert!(indexer.embed_queue_depth().unwrap() >= 1);
        let (embedded, _) = drain_embed_queue(&indexer);
        assert!(embedded >= 1);
        assert_eq!(indexer.embed_queue_depth().unwrap(), 0);
    }

    #[test]
    fn pipeline_attempt_cap_poisons_persistently_failing_job() {
        // Fix round 1/5 (I1): a job failing MAX_EMBED_ATTEMPTS batches is
        // poison-pilled (`failed`, never auto-claimed) with attempts recorded,
        // so a persistently hung worker cannot spin the queue forever.
        let tree = TempTree::new();
        fs::write(tree.0.join("poison.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        let eid = entry_id_by_name(&indexer, "poison");
        // Drain everything else first so only the poison target remains.
        // (Two files max here: the target plus the root dir entry; keep the
        // loop targeted by failing every batch — attempts accrue per job.)
        for _ in 0..(MAX_EMBED_ATTEMPTS as usize + 2) {
            let out = indexer
                .embed_once_with(|_| Err("embed job timed out after 120s".to_string()))
                .unwrap();
            if out.claimed == 0 {
                break;
            }
        }
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let job = store.find_extract_job(eid, "name").unwrap().unwrap();
            assert_eq!(job.state, "failed", "job must be poison-pilled");
            assert!(job.attempts >= MAX_EMBED_ATTEMPTS);
        }
        // Poisoned jobs are never auto-claimed: the scheduler idles on them.
        let out = indexer.embed_once_with(fake_embed).unwrap();
        assert_eq!(out.claimed, 0, "failed jobs must not be reclaimed");
    }

    #[test]
    fn pipeline_mid_batch_change_requeues_instead_of_stale_upsert() {
        // Fix round 1/5 (I2): a rename landing mid-batch must not be dropped —
        // the stale vector is never upserted, the job requeues (attempts
        // untouched), and the fresh name wins on the next batch.
        use std::sync::atomic::{AtomicBool, Ordering as AO};
        let tree = TempTree::new();
        fs::write(tree.0.join("mid-batch.txt"), b"x").unwrap();
        let indexer = seeded(&tree);
        drain_embed_queue(&indexer);
        let eid = entry_id_by_name(&indexer, "mid-batch");
        let seg_v1_text = {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            store.list_segments_for_entry(eid).unwrap()[0].text.clone()
        };
        assert_eq!(seg_v1_text, "mid-batch.txt");

        // Queue a re-embed (touch the file), then mutate DURING the embed:
        // the fake performs a real on-disk rename + watcher event (which
        // bumps the claimed row's priority) and returns vectors for the
        // now-stale names. Closure borrows `indexer` shared — safe: the
        // pipeline holds no store lock across the embed call.
        fs::write(tree.0.join("mid-batch.txt"), b"longer contents here").unwrap();
        let root_id = indexer.root_snapshot()[0].id;
        indexer.handle_event(root_id, WatchEvent::Modified(tree.0.join("mid-batch.txt")));
        assert!(indexer.embed_queue_depth().unwrap() >= 1);

        let mutated = AtomicBool::new(false);
        let out = indexer
            .embed_once_with(|texts| {
                if !mutated.swap(true, AO::SeqCst) {
                    let from = tree.0.join("mid-batch.txt");
                    let to = tree.0.join("mid-batch-renamed.txt");
                    fs::rename(&from, &to).unwrap();
                    indexer.handle_event(root_id, WatchEvent::Renamed { from, to });
                }
                Ok(ws_embed_protocol::stub_embed_texts(
                    texts,
                    ws_embed_protocol::STUB_DIM,
                ))
            })
            .unwrap();
        assert!(out.claimed >= 1);
        assert_eq!(out.embedded, 0, "stale vector must never be upserted");
        assert_eq!(out.requeued, out.claimed, "changed jobs must requeue");
        assert_eq!(out.failed, 0);
        // No stale write: the segment still carries the pre-batch text, the
        // job is pending again with attempts untouched (churn, not failure).
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let segs = store.list_segments_for_entry(eid).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].text, seg_v1_text, "stale upsert must not land");
            let job = store.find_extract_job(eid, "name").unwrap().unwrap();
            assert_eq!(job.state, "pending");
            assert_eq!(job.attempts, 0, "requeue must not bump attempts");
        }
        // Next batch embeds the fresh name and completes.
        let out2 = indexer.embed_once_with(fake_embed).unwrap();
        assert_eq!(out2.embedded, out.claimed);
        {
            let store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let segs = store.list_segments_for_entry(eid).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].text, "mid-batch-renamed.txt");
            let want = ws_embed_protocol::stub_vector(
                "mid-batch-renamed.txt",
                ws_embed_protocol::STUB_DIM,
            );
            let cos = ws_index::vectors::cosine_similarity(&segs[0].vector, &want);
            assert!(cos > 0.99, "fresh vector must win, cosine {cos}");
        }
        assert_eq!(indexer.embed_queue_depth().unwrap(), 0);
    }

    #[test]
    fn pipeline_purge_and_requeue_reembeds_everything() {
        // Task3-S4: seed vectors, then purge + requeue (the `Incompatible`
        // restart path) empties segments and makes every entry re-embeddable.
        let tree = TempTree::new();
        fs::write(tree.0.join("keep-a.txt"), b"x").unwrap();
        fs::write(tree.0.join("keep-b.txt"), b"y").unwrap();
        let indexer = seeded(&tree);
        drain_embed_queue(&indexer);
        let segs_before = indexer.segments_indexed().unwrap();
        assert!(segs_before >= 2);

        let (purged, enqueued) = indexer.purge_model_vectors_and_requeue().unwrap();
        assert_eq!(purged, segs_before);
        assert!(enqueued >= 2);
        assert_eq!(indexer.segments_indexed().unwrap(), 0);

        let (embedded, _) = drain_embed_queue(&indexer);
        assert_eq!(embedded as u64, segs_before);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), enqueued - segs_before);
        assert_eq!(indexer.segments_indexed().unwrap(), segs_before);
    }

    #[test]
    fn watcher_events_under_excluded_ancestors_are_ignored() {
        let tree = TempTree::new();
        fs::write(tree.0.join("visible.txt"), b"ok").unwrap();
        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;

        let git_dir = tree.0.join(".git");
        let nm_dir = tree.0.join("node_modules").join("pkg");
        fs::create_dir_all(&git_dir).unwrap();
        fs::create_dir_all(&nm_dir).unwrap();

        let git_cfg = git_dir.join("config");
        let nm_js = nm_dir.join("index.js");
        fs::write(&git_cfg, b"secret").unwrap();
        fs::write(&nm_js, b"module.exports = {}").unwrap();

        indexer.handle_event(root_id, WatchEvent::Created(git_cfg.clone()));
        indexer.handle_event(root_id, WatchEvent::Modified(git_cfg));
        indexer.handle_event(root_id, WatchEvent::Created(nm_js.clone()));
        indexer.handle_event(root_id, WatchEvent::Modified(nm_js));

        assert!(
            indexer
                .search(&SearchRequest {
                    text: "config".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            ".git/config must not be indexed via watcher event"
        );
        assert!(
            indexer
                .search(&SearchRequest {
                    text: "index.js".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            "node_modules/pkg/index.js must not be indexed via watcher event"
        );
    }

    #[test]
    fn touch_subtree_generation_protects_descendants_from_stale_sweep() {
        let tree = TempTree::new();
        let sub = tree.0.join("sub");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("protected.txt"), b"keep").unwrap();
        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;

        // Simulate a new generation where `sub` failed to scan with a non-NotFound
        // error: touching the subtree generation must keep `protected.txt` across sweep.
        {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            let gen = store.begin_generation(root_id).unwrap();
            let touched = store
                .touch_subtree_generation(root_id, &sub.to_string_lossy(), gen)
                .unwrap();
            assert!(touched >= 2, "expected dir + child touched, got {touched}");
            let swept = store.sweep_generation(root_id, gen).unwrap();
            assert_eq!(swept, 0);
        }

        let hits = indexer
            .search(&SearchRequest {
                text: "protected".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn startup_offline_root_marks_entries_missing_and_reconnects_on_presence() {
        let tree = TempTree::new();
        fs::write(tree.0.join("vanished.txt"), b"hello").unwrap();

        let tmp = std::env::temp_dir().join(format!(
            "winsearchd-startup-offline-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let db = tmp.join("index.db");

        {
            let store = IndexStore::open(&db).unwrap();
            let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
            indexer.enqueue_initial_scan().unwrap();
            while indexer.scan_available(64) > 0 {}
        }

        // Remove the root directory while the daemon is stopped, then reopen.
        fs::remove_dir_all(&tree.0).unwrap();
        let store = IndexStore::open(&db).unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
        indexer.reconcile_drives();

        assert!(
            indexer
                .search(&SearchRequest {
                    text: "vanished".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            "entries under an initially absent root must be marked missing"
        );

        // Recreate the root with a new file; reconcile_drives must reconnect and rescan.
        fs::create_dir_all(&tree.0).unwrap();
        fs::write(tree.0.join("restored.txt"), b"back").unwrap();
        indexer.reconcile_drives();
        while indexer.scan_available(64) > 0 {}

        let hits = indexer
            .search(&SearchRequest {
                text: "restored".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);

        drop(indexer);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn failed_disconnect_update_is_retried_without_repeating_successful_writes() {
        let tree = TempTree::new();
        let root = tree.0.join("drive");
        fs::create_dir_all(&root).unwrap();
        let file = root.join("keep.txt");
        fs::write(&file, b"keep").unwrap();
        let db = tree.0.join("index.db");
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[root.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TRIGGER fail_disconnect BEFORE UPDATE OF missing ON entries
            BEGIN SELECT RAISE(FAIL, 'simulated write failure'); END;").unwrap();
        fs::remove_dir_all(&root).unwrap();
        indexer.reconcile_drives();
        conn.execute_batch("DROP TRIGGER fail_disconnect;").unwrap();
        indexer.reconcile_drives();
        assert!(indexer.store_lock().find_by_path(&file.to_string_lossy()).unwrap().unwrap().missing);
        // Count actual writes, including the empty-root case, on later polls.
        conn.execute_batch("CREATE TABLE writes(n INTEGER); INSERT INTO writes VALUES(0);
            CREATE TRIGGER count_disconnect AFTER UPDATE OF missing ON entries
            BEGIN UPDATE writes SET n = n + 1; END;").unwrap();
        indexer.reconcile_drives();
        let writes: i64 = conn.query_row("SELECT n FROM writes", [], |r| r.get(0)).unwrap();
        assert_eq!(writes, 0);
    }

    #[test]
    fn overflow_rescans_claimed_descendants_before_sweeping() {
        let tree = TempTree::new();
        let sub = tree.0.join("sub");
        fs::create_dir_all(&sub).unwrap();
        let keep = sub.join("keep.txt");
        fs::write(&keep, b"keep").unwrap();
        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;
        let (qid, _, dir, generation) = {
            let mut store = indexer.store_lock();
            store.enqueue_dir(root_id, &sub.to_string_lossy()).unwrap();
            store.claim_dir_with_generation().unwrap().unwrap()
        };
        indexer.apply_event(root_id, &WatchEvent::Overflow).unwrap();
        assert_eq!(indexer.scan_available(1), 1); // Root sees the claimed child.
        indexer.process_dir(qid, root_id, &dir, generation).unwrap();
        while indexer.scan_available(64) > 0 {}
        assert!(indexer.store_lock().find_by_path(&keep.to_string_lossy()).unwrap().is_some());
    }

    #[test]
    fn extension_rename_within_modality_clears_content_and_requeues() {
        let tree = TempTree::new();
        let from = tree.0.join("report.docx");
        let to = tree.0.join("report.txt");
        fs::write(&from, b"fixture").unwrap();
        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;
        let id = indexer.store_lock().find_by_path(&from.to_string_lossy()).unwrap().unwrap().id;
        {
            let mut store = indexer.store_lock();
            store.upsert_segment(NewSegment {
                entry_id: id, kind: "doc", chunk_index: 0,
                page: None, start_ms: None, end_ms: None, line: None,
                text: "obsolete", vector: &[1.0, 0.0],
            }).unwrap();
            while let Some(job) = store.claim_extract_jobs_batch_for_kind("doc", 1).unwrap().first() {
                store.complete_extract_job(job.id).unwrap();
            }
        }
        indexer.search_vectors(&[1.0, 0.0], 10, None).unwrap();
        fs::rename(&from, &to).unwrap();
        indexer.apply_event(root_id, &WatchEvent::Renamed { from, to }).unwrap();
        let store = indexer.store_lock();
        assert!(store.list_segments_for_entry(id).unwrap().iter().all(|s| s.kind != "doc"));
        assert_eq!(store.extract_queue_depth_for_kind("doc").unwrap(), 1);
        drop(store);
        assert!(indexer.search_vectors(&[1.0, 0.0], 10, None).unwrap().is_empty());
    }

    #[test]
    fn superseded_inflight_scan_does_not_stamp_overflow_generation() {
        let tree = TempTree::new();
        let sub = tree.0.join("sub");
        fs::create_dir_all(&sub).unwrap();
        let doomed = sub.join("doomed.txt");
        fs::write(&doomed, b"bye").unwrap();
        let keep = tree.0.join("keep.txt");
        fs::write(&keep, b"stay").unwrap();

        let indexer = seeded(&tree);
        let root_id = indexer.root_snapshot()[0].id;
        let sub_str = sub.to_string_lossy().to_string();

        // Enqueue and claim `sub` under the pre-overflow generation.
        let (qid, _, dir, claimed_gen) = {
            let mut store = indexer.store.lock().unwrap_or_else(|p| p.into_inner());
            store.enqueue_dir(root_id, &sub_str).unwrap();
            store.claim_dir_with_generation().unwrap().unwrap()
        };

        // Now `sub` is deleted and an overflow triggers a new generation + root rescan
        // before the claimed scan of `sub` finishes (or while `sub` was still visible).
        indexer.handle_event(root_id, WatchEvent::Overflow);

        // Even if `process_dir` runs while `sub` is still on disk, its pre-overflow
        // `claimed_gen` must prevent it from stamping `doomed.txt` into the new generation.
        indexer
            .process_dir(qid, root_id, &dir, claimed_gen)
            .unwrap();

        // Remove `sub` before the new generation's root rescan runs.
        fs::remove_dir_all(&sub).unwrap();
        while indexer.scan_available(64) > 0 {}

        assert!(
            indexer
                .search(&SearchRequest {
                    text: "doomed".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            "superseded in-flight scan must not retain deleted files across overflow sweep"
        );
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "keep".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }
}
