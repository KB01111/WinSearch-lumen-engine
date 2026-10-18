//! Opt-in code index. One signalled coordinator owns durable jobs; parsing uses
//! the process-wide owned extraction child and vectors use the shared supervisor.
use crate::{
    embed_client::EmbedSupervisor,
    extract_client::ExtractClient,
    indexer::Indexer,
    model::ModelService,
    watcher::{self, WatchEvent, Watcher},
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ws_code::model::{CodeJob, CodeSearchHit};
use ws_code::{CodeFileRow, CodeStore, ParsedCodeFile};

pub const QUIET_PERIOD: Duration = Duration::from_secs(60);
const MAX_UNITS: u64 = 131_072;
const MAX_PARSE_BYTES: usize = 32 * 1024 * 1024;
const VECTOR_PAGE_BUDGET: usize = 64;
const VECTOR_BATCH: usize = 16;
const PARSER_ADMISSION_WAIT: Duration = Duration::from_millis(100);

#[derive(Default)]
struct VectorState {
    cursor: i64,
    epoch: u64,
    fingerprint: String,
    policy: Option<ws_protocol::IndexingConfig>,
    retry_due: Option<i64>,
    failures: u32,
}

struct ScanState {
    job: CodeJob,
    epoch: u64,
    policy: ws_protocol::IndexingConfig,
    repos: std::collections::VecDeque<ws_code::DetectedRepo>,
    active: Option<RepoScan>,
}
struct RepoScan {
    root: String,
    id: i64,
    generation: i64,
    crawler: ws_code::crawl::CodeCrawler,
    walk_done: bool,
}

pub struct CodeService {
    pub(crate) store: Mutex<CodeStore>,
    indexer: Arc<Indexer>,
    embed: Arc<EmbedSupervisor>,
    models: Arc<ModelService>,
    extract: ExtractClient,
    epoch: AtomicU64,
    wake: (Mutex<bool>, Condvar),
    watchers: Mutex<Vec<Watcher>>,
    config: Mutex<Option<ws_protocol::IndexingConfig>>,
    quiet: Duration,
    scan: Mutex<Option<ScanState>>,
    vectors: Mutex<VectorState>,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Precise metadata and native identity, never a content read or cloud hydration.
fn snapshot(path: &Path) -> Option<String> {
    if !ws_platform::content::local_content_allowed(path) {
        return None;
    }
    let m = std::fs::symlink_metadata(path).ok()?;
    let identity = ws_platform::identity::file_identity(path)?;
    Some(format!(
        "{}:{}:{:?}:{:?}:{identity:?}",
        ws_code::model::CODE_PARSE_VERSION,
        m.len(),
        m.modified().ok()?,
        m.created().ok()?
    ))
}

impl CodeService {
    pub fn new(
        store: CodeStore,
        indexer: Arc<Indexer>,
        embed: Arc<EmbedSupervisor>,
        models: Arc<ModelService>,
        extract: ExtractClient,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: Mutex::new(store),
            indexer,
            embed,
            models,
            extract,
            epoch: AtomicU64::new(1),
            wake: (Mutex::new(true), Condvar::new()),
            watchers: Mutex::new(Vec::new()),
            config: Mutex::new(None),
            quiet: QUIET_PERIOD,
            scan: Mutex::new(None),
            vectors: Mutex::new(VectorState::default()),
        })
    }
    pub fn signal(&self) {
        *self.wake.0.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.wake.1.notify_one();
    }
    pub fn policy_changed(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.signal();
    }
    pub fn query_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
    fn policy(&self) -> ws_protocol::IndexingConfig {
        self.indexer.get_indexing_config().unwrap_or_default()
    }
    pub fn allowed(&self, path: &Path) -> bool {
        crate::policy::code_path_allowed(&self.policy(), &path.to_string_lossy())
            && ws_platform::content::metadata_path_allowed(path, false)
    }
    fn visible(&self, file: &CodeFileRow, stored: Option<&str>) -> bool {
        self.allowed(Path::new(&file.path))
            && stored.is_some()
            && snapshot(Path::new(&file.path)).as_deref() == stored
    }
    #[cfg(test)]
    pub fn exact(&self,text:&str,limit:usize)->Vec<CodeSearchHit> {self.exact_filtered(text,limit,&[])}

    #[cfg(test)]
    pub fn exact_filtered(&self,text:&str,limit:usize,extensions:&[String])->Vec<CodeSearchHit> {
        self.exact_filtered_checked(text,limit,extensions).unwrap_or_default()
    }
    pub fn exact_filtered_checked(&self, text: &str, limit: usize,extensions:&[String]) -> Result<Vec<CodeSearchHit>,ws_code::store::CodeStoreError> {
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let failure=std::cell::RefCell::new(None);
        let hits=store
            .search_exact(text, limit, |file| {
                if !ws_protocol::search_extension_allowed(extensions,&file.ext) {return false;}
                match store.source_snapshot(file.id) {
                    Ok(source)=>self.visible(file,source.as_deref()),
                    Err(error)=>{*failure.borrow_mut()=Some(error);false}
                }
            })?;
        if let Some(error)=failure.into_inner() {return Err(error);}
        Ok(hits)
    }
    #[cfg(test)]
    pub fn semantic(&self,vector:&[f32],corroboration:&[f32],limit:usize)->Vec<CodeSearchHit> {
        self.semantic_filtered(vector,corroboration,limit,&[])
    }

    #[cfg(test)]
    pub fn semantic_filtered(&self,vector:&[f32],corroboration:&[f32],limit:usize,extensions:&[String])->Vec<CodeSearchHit> {
        self.semantic_filtered_checked(vector,corroboration,limit,extensions).unwrap_or_default().0
    }
    pub fn semantic_filtered_checked(&self, vector: &[f32], corroboration: &[f32], limit: usize,extensions:&[String])
        ->Result<(Vec<CodeSearchHit>,bool),ws_protocol::SemanticIssue> {
        use ws_protocol::SemanticIssue::RetrievalFailed;
        let fingerprint=self.fingerprint_checked().map_err(|_|RetrievalFailed)?;
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        if store.meta_get("vector_fingerprint").map_err(|_|RetrievalFailed)?.as_deref()!=Some(&fingerprint) {
            return Ok((Vec::new(),false));
        }
        let failed=std::cell::Cell::new(false);
        let report=store
            .search_semantic_report(vector, corroboration, limit, |file| {
                if !ws_protocol::search_extension_allowed(extensions,&file.ext) {return false;}
                match store.source_snapshot(file.id) {
                    Ok(source)=>self.visible(file,source.as_deref()),
                    Err(_)=>{failed.set(true);false}
                }
            }, |score|ws_index::retrieval::semantic_admitted("code",score))
            .map_err(|_|RetrievalFailed)?;
        if failed.get() {return Err(RetrievalFailed);}
        Ok(report)
    }
    pub fn result_allowed(&self, path: &str, epoch: u64) -> bool {
        let store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        self.query_epoch() == epoch
            && store
                .find_file_by_path(path)
                .ok()
                .flatten()
                .is_some_and(|f| {
                    self.visible(&f, store.source_snapshot(f.id).ok().flatten().as_deref())
                })
            && self.query_epoch() == epoch
    }

    pub fn preview_text(&self,file:i64,entry:&ws_protocol::ResolvedEntry,location:Option<ws_protocol::MatchLocation>)
        ->Result<(Vec<ws_protocol::PreviewTextPart>,bool),ws_code::store::CodeStoreError> {
        let store=self.store.lock().unwrap_or_else(|p|p.into_inner());
        store.preview_text(file,entry.index_revision.unwrap_or(0),location,|row,state| {
            row.path==entry.path && state.sha256==entry.content_sha256
                && self.visible(row,Some(&state.snapshot))
                && ws_code::parse::guard_source_path(Path::new(&row.path)).is_ok()
                && ws_platform::identity::file_identity(Path::new(&row.path))==Some((row.volume_serial,row.file_id))
        })
    }

    pub fn resolve_metadata(&self,file_id:i64,reference:&ws_protocol::EntryReference,incarnation:windows::core::GUID)
        ->Result<ws_protocol::ResolvedEntry,ws_protocol::ErrorCode> {
        use ws_protocol::ErrorCode;
        let policy_epoch=self.indexer.policy_epoch();
        let generation=self.indexer.store_lock().model_generation().map_err(|_|ErrorCode::Internal)?;
        let policy=self.policy();
        let (file,repo,state)={
            let store=self.store.lock().unwrap_or_else(|p|p.into_inner());
            let file=store.get_file(file_id).map_err(|_|ErrorCode::Internal)?.ok_or(ErrorCode::NotReady)?;
            let repo=store.get_repo(file.repo_id).map_err(|_|ErrorCode::Internal)?.ok_or(ErrorCode::NotReady)?;
            let state=store.source_provenance(file_id).map_err(|_|ErrorCode::Internal)?;
            (file,repo,state)
        };
        let path=Path::new(&file.path);
        if !repo.enabled || !crate::policy::contains(&repo.root_path,&file.path) || !self.allowed(path)
            || ws_code::parse::guard_source_path(path).is_err() {
            return Err(ErrorCode::NotReady);
        }
        let root_path=policy.code_roots.iter().filter(|root|crate::policy::contains(root,&file.path))
            .max_by_key(|root|root.len()).cloned().ok_or(ErrorCode::NotReady)?;
        if ws_platform::identity::file_identity(path)!=Some((file.volume_serial,file.file_id)) {
            return Err(ErrorCode::NotReady);
        }
        let source=snapshot(path).ok_or(ErrorCode::NotReady)?;
        let metadata=std::fs::metadata(path).map_err(|_|ErrorCode::NotReady)?;
        let revision=state.as_ref().map_or(0,|state|state.revision);
        let indexed=state.as_ref().is_some_and(|state|state.snapshot==source);
        let proof=format!("{incarnation:?}:code:{policy_epoch}:{generation}:{revision}:{source}:{}",file.path);
        if proof.len()>65536 || reference.source_revision.as_ref().is_some_and(|expected|expected!=&proof) {
            return Err(ErrorCode::NotReady);
        }
        if self.indexer.policy_epoch()!=policy_epoch
            || self.indexer.store_lock().model_generation().map_err(|_|ErrorCode::Internal)?!=generation
            || self.policy()!=policy {return Err(ErrorCode::NotReady);}
        {
            let store=self.store.lock().unwrap_or_else(|p|p.into_inner());
            if store.get_file(file_id).map_err(|_|ErrorCode::Internal)?.as_ref()!=Some(&file)
                || store.get_repo(file.repo_id).map_err(|_|ErrorCode::Internal)?.as_ref()!=Some(&repo)
                || store.source_provenance(file_id).map_err(|_|ErrorCode::Internal)?!=state
                || !self.allowed(path) || ws_code::parse::guard_source_path(path).is_err()
                || ws_platform::identity::file_identity(path)!=Some((file.volume_serial,file.file_id))
                || snapshot(path).as_deref()!=Some(source.as_str()) {
                return Err(ErrorCode::NotReady);
            }
        }
        if self.indexer.policy_epoch()!=policy_epoch
            || self.indexer.store_lock().model_generation().map_err(|_|ErrorCode::Internal)?!=generation {
            return Err(ErrorCode::NotReady);
        }
        Ok(ws_protocol::ResolvedEntry {result_id:reference.result_id.clone(),root_path,
            path:file.path.clone(),name:file.name.clone(),kind:ws_protocol::ResultKind::Code,
            size_bytes:metadata.len(),modified_unix_seconds:metadata.modified().ok().and_then(ws_platform::identity::unix_seconds),
            index_revision:indexed.then_some(revision),source_revision:proof,
            content_sha256:state.filter(|_|indexed).and_then(|state|state.sha256)})
    }
    fn fingerprint(&self) -> Option<String> {
        self.fingerprint_checked().ok()
    }
    fn fingerprint_checked(&self)->Result<String,ws_index::StoreError> {
        let generation = self.indexer.store_lock().model_generation()?;
        let compat = self
            .indexer
            .meta_get(ws_modelmgr::EMBEDDING_MODEL_META_KEY)
            ?
            .unwrap_or_default();
        Ok(format!("{generation}:{compat}:code-v1"))
    }
    pub fn changed(&self, root: &str, event: WatchEvent) -> anyhow::Result<()> {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        match event {
            WatchEvent::Created(p) | WatchEvent::Modified(p) | WatchEvent::Deleted(p) => {
                store.invalidate_sources_under(&p.to_string_lossy())?
            }
            WatchEvent::Renamed { from, to } => {
                store.invalidate_sources_under(&from.to_string_lossy())?;
                store.invalidate_sources_under(&to.to_string_lossy())?;
            }
            WatchEvent::Overflow => store.invalidate_sources_under(root)?,
        }
        let due = now() + self.quiet.as_millis() as i64;
        store.defer_jobs_under(root, due)?;
        store.enqueue_job(root, root, true, due)?;
        drop(store);
        self.signal();
        Ok(())
    }
    pub(crate) fn configure(self: &Arc<Self>) -> anyhow::Result<()> {
        let policy = self.policy();
        let mut previous = self.config.lock().unwrap_or_else(|p| p.into_inner());
        if previous.as_ref() == Some(&policy) {
            return Ok(());
        }
        let rebuild = previous.as_ref().is_none_or(|old| {
            old.code_enabled != policy.code_enabled || old.code_roots != policy.code_roots
        });
        let rescan = previous.as_ref().is_none_or(|old| {
            old.excluded_paths != policy.excluded_paths
                || old.excluded_extensions != policy.excluded_extensions
        });
        if !rebuild && !rescan {
            *previous = Some(policy);
            return Ok(());
        }
        if policy.code_enabled {
            for root in &policy.code_roots {
                self.store
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .ensure_scan_job(root, now())?;
            }
        }
        if !rebuild {
            *previous = Some(policy);
            return Ok(());
        }
        let old = std::mem::take(&mut *self.watchers.lock().unwrap_or_else(|p| p.into_inner()));
        drop(old);
        let mut watchers = Vec::new();
        if policy.code_enabled {
            for root in &policy.code_roots {
                let weak = Arc::downgrade(self);
                let watched = root.clone();
                if let Ok(watcher) = watcher::spawn(Path::new(root), move |event| {
                    if let Some(service) = weak.upgrade() {
                        if let Err(e) = service.changed(&watched, event) {
                            tracing::warn!(error=%e,"code watcher queue failed");
                        }
                    }
                }) {
                    watchers.push(watcher);
                }
            }
        }
        *self.watchers.lock().unwrap_or_else(|p| p.into_inner()) = watchers;
        *previous = Some(policy);
        Ok(())
    }
    pub fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .recover_jobs()?;
        self.configure()?;
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("winsearch-code".into())
            .spawn(move || Self::coordinate(weak))?;
        Ok(())
    }
    fn coordinate(weak: Weak<Self>) {
        while let Some(service) = weak.upgrade() {
            if service.indexer.is_shutdown() {
                break;
            }
            if let Err(e) = service.configure() {
                tracing::warn!(error=%e,"code configuration failed");
            }
            match service.turn() {
                Ok(true) => continue,
                Err(e) => tracing::warn!(error=%e,"code coordinator failed"),
                _ => (),
            }
            service.wait_for_work();
        }
    }
    fn wait_for_work(&self) {
        if self.indexer.indexing_pause_reason().is_none()
            && self.parser_work_due() && !self.extract.admission_available() {
            self.extract.wait_for_admission(PARSER_ADMISSION_WAIT);
            return;
        }
        let due = self.next_work_due();
        let due = if self.indexer.indexing_pause_reason().is_some() {
            crate::policy::schedule_wait(&self.policy()).map(|d| now() + d.as_millis() as i64)
        } else {
            due
        };
        let mut wake = self.wake.0.lock().unwrap_or_else(|p| p.into_inner());
        if *wake {
            *wake = false;
            return;
        }
        if let Some(due) = due {
            let _guard = self
                .wake
                .1
                .wait_timeout(wake, Duration::from_millis((due - now()).max(1) as u64))
                .unwrap_or_else(|p| p.into_inner());
        } else {
            let _guard = self.wake.1.wait(wake).unwrap_or_else(|p| p.into_inner());
        }
    }
    fn next_work_due(&self) -> Option<i64> {
        let jobs = self
            .store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next_job_due()
            .ok()
            .flatten();
        let vectors = self
            .vectors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retry_due;
        jobs.into_iter().chain(vectors).min()
    }
    fn parser_work_due(&self) -> bool {
        self.store.lock().unwrap_or_else(|p| p.into_inner()).next_job_due()
            .ok().flatten().is_some_and(|due| due <= now())
    }
    pub fn turn(&self) -> anyhow::Result<bool> {
        if self.indexer.indexing_pause_reason().is_some() {
            return Ok(false);
        }
        let scan = self.scan.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(mut scan) = scan {
            match self.scan_step(&mut scan) {
                Ok(true) => {
                    self.store
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .finish_job(&scan.job)?;
                }
                Ok(false) => {
                    *self.scan.lock().unwrap_or_else(|p| p.into_inner()) = Some(scan);
                }
                Err(e) => {
                    self.store
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .retry_job(
                            &scan.job,
                            now() + 60_000,
                            !e.to_string().contains("changed"),
                        )?;
                    tracing::warn!(error=%e,"code scan deferred");
                }
            }
            return Ok(true);
        }
        if self.parser_work_due() && !self.extract.admission_available() {
            return Ok(false);
        }
        let job = self
            .store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .claim_job(now())?;
        let Some(job) = job else {
            return self.embed_turn();
        };
        if !self.allowed(Path::new(&job.path)) {
            self.store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .finish_job(&job)?;
            return Ok(true);
        }
        if job.scan {
            let epoch = self.epoch.load(Ordering::Acquire);
            let policy = self.policy();
            let repos = match ws_code::detect_repos(Path::new(&job.path)) {
                Ok(repos) => repos,
                Err(e) => {
                    self.store
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .retry_job(&job, now() + 60_000, true)?;
                    tracing::warn!(error=%e,"code repository detection deferred");
                    return Ok(true);
                }
            };
            *self.scan.lock().unwrap_or_else(|p| p.into_inner()) = Some(ScanState {
                job,
                epoch,
                policy,
                repos: repos.into(),
                active: None,
            });
            return Ok(true);
        }
        self.complete_parse_job(&job, self.parse(&job))?;
        Ok(true)
    }
    fn fence(&self, job: &CodeJob, epoch: u64, policy: &ws_protocol::IndexingConfig) -> bool {
        self.epoch.load(Ordering::Acquire) == epoch
            && self.policy() == *policy
            && self.allowed(Path::new(&job.path))
            && self.indexer.indexing_pause_reason().is_none()
            && self
                .store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .job_current(job)
                .unwrap_or(false)
    }

    fn complete_parse_job(&self, job: &CodeJob, outcome: anyhow::Result<()>) -> anyhow::Result<()> {
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        match outcome {
            Ok(()) => { store.finish_job(job)?; }
            Err(error) => {
                let capacity = matches!(error.downcast_ref::<crate::extract_client::ExtractClientError>(),
                    Some(crate::extract_client::ExtractClientError::AdmissionUnavailable));
                let delay = if capacity { PARSER_ADMISSION_WAIT.as_millis() as i64 } else { 60_000 };
                store.retry_job(job, now() + delay, !capacity && !error.to_string().contains("changed"))?;
                if !capacity { tracing::warn!(path=%job.path,error=%error,"code job deferred"); }
            }
        }
        Ok(())
    }
    fn scan_step(&self, state: &mut ScanState) -> anyhow::Result<bool> {
        let valid =
            |p: &Path| self.fence(&state.job, state.epoch, &state.policy) && self.allowed(p);
        anyhow::ensure!(valid(Path::new(&state.job.path)), "code policy changed");
        if state.active.is_none() {
            let Some(repo) = state.repos.pop_front() else {
                return Ok(true);
            };
            let crawler = ws_code::crawl::CodeCrawler::open(Path::new(&repo.root_path), &valid)?;
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            let id = store.ensure_repo(&repo)?;
            let generation = store.begin_generation(id)?;
            store.clear_scan(id)?;
            state.active = Some(RepoScan {
                root: repo.root_path,
                id,
                generation,
                crawler,
                walk_done: false,
            });
        }
        let active = state.active.as_mut().unwrap();
        if !active.walk_done {
            let (records, done) = active.crawler.next_batch(512, &valid)?;
            anyhow::ensure!(valid(Path::new(&state.job.path)), "code policy changed");
            self.store
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .stage_scan(active.id, &records)?;
            active.walk_done = done;
            return Ok(false);
        }
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        let records = store.scan_batch(active.id, 256)?;
        if records.is_empty() {
            store.sweep_generation(active.id, active.generation)?;
            state.active = None;
            return Ok(false);
        }
        let outcomes = store.apply_file_batch(active.id, active.generation, &records)?;
        for (record, outcome) in records.iter().zip(outcomes) {
            let stored = store.source_snapshot(outcome.file_id())?;
            if stored.is_none() || stored.as_deref() != snapshot(Path::new(&record.path)).as_deref()
                || store.source_provenance(outcome.file_id())?.is_none_or(|state|state.sha256.is_none())
            {
                store.enqueue_file_job(&record.path, &active.root, record.mtime, now())?;
            }
        }
        store.complete_scan_batch(active.id, &records)?;
        Ok(false)
    }
    fn parse(&self, job: &CodeJob) -> anyhow::Result<()> {
        let path = PathBuf::from(&job.path);
        let epoch = self.epoch.load(Ordering::Acquire);
        let policy = self.policy();
        let before =
            snapshot(&path).ok_or_else(|| anyhow::anyhow!("source changed or unavailable"))?;
        let valid =
            || self.fence(job, epoch, &policy) && snapshot(&path).as_deref() == Some(&before);
        let mut parsed = ParsedCodeFile {
            language: String::new(),
            chunks: Vec::new(),
            symbols: Vec::new(),
            diagnostics: Vec::new(),
        };
        let mut request = self.extract.request(path.clone(), "code", 256);
        let mut expected_total = None;
        let mut source_sha256: Option<String> = None;
        let mut bytes = 0usize;
        let mut symbols_started = false;
        let mut cell_offsets = std::collections::HashMap::new();
        loop {
            anyhow::ensure!(valid(), "source or code policy changed");
            let response = self
                .extract
                .extract_while(&request, Duration::from_secs(30), valid)?;
            let total = response
                .total_units
                .ok_or_else(|| anyhow::anyhow!("missing code total"))?;
            anyhow::ensure!(
                total <= MAX_UNITS && expected_total.is_none_or(|old| old == total),
                "code unit total changed or exceeds bound"
            );
            expected_total = Some(total);
            let batch = response
                .code
                .ok_or_else(|| anyhow::anyhow!("missing typed code batch"))?;
            anyhow::ensure!(source_sha256.as_ref().is_none_or(|expected|expected==&batch.source_sha256),
                "source bytes changed between code batches");
            source_sha256=Some(batch.source_sha256);
            anyhow::ensure!(
                parsed.language.is_empty() || parsed.language == batch.language,
                "code language changed"
            );
            parsed.language = batch.language;
            for unit in batch.units {
                use ws_extract::code::CodeUnit;
                let (line, notebook) = match &unit {
                    CodeUnit::Chunk { chunk, .. } => (chunk.start_line, chunk.notebook),
                    CodeUnit::Symbol { symbol, .. } => (symbol.line, symbol.notebook),
                };
                if let Some(cell) = notebook {
                    let offset = line
                        .checked_sub(cell.start_line)
                        .ok_or_else(|| anyhow::anyhow!("notebook offset changed"))?;
                    let previous = cell_offsets.entry(cell.cell_ordinal).or_insert(offset);
                    anyhow::ensure!(
                        *previous == offset,
                        "notebook offset changed between code batches"
                    );
                }
                match unit {
                    CodeUnit::Chunk { chunk, .. } => {
                        anyhow::ensure!(!symbols_started, "code order changed");
                        bytes += chunk.text.len();
                        parsed.chunks.push(chunk);
                    }
                    CodeUnit::Symbol { symbol, .. } => {
                        symbols_started = true;
                        bytes += symbol.name.len() + symbol.signature.len() + 64;
                        parsed.symbols.push(symbol);
                    }
                }
                anyhow::ensure!(bytes <= MAX_PARSE_BYTES, "code parse exceeds memory bound");
            }
            parsed.diagnostics = response.diagnostics;
            match response.next_cursor {
                Some(next) => {
                    anyhow::ensure!(next > request.cursor, "code cursor changed");
                    request = self.extract.request(path.clone(), "code", 256);
                    request.cursor = next;
                }
                None => break,
            }
        }
        anyhow::ensure!(valid(), "source or code policy changed");
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        anyhow::ensure!(
            store.job_current(job)?
                && self.epoch.load(Ordering::Acquire) == epoch
                && self.policy() == policy
                && snapshot(&path).as_deref() == Some(&before),
            "source or code policy changed"
        );
        let file = store
            .find_file_by_path(&job.path)?
            .ok_or_else(|| anyhow::anyhow!("code file disappeared"))?;
        store.commit_parsed_source(file.id,&parsed,&before,
            source_sha256.as_deref().ok_or_else(||anyhow::anyhow!("missing code source hash"))?)?;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    fn embed_turn(&self) -> anyhow::Result<bool> {
        if !self.policy().code_enabled
            || self.models.status().state != ws_modelmgr::ModelState::Ready
        {
            self.vectors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .retry_due = None;
            return Ok(false);
        }
        let fingerprint = self
            .fingerprint()
            .ok_or_else(|| anyhow::anyhow!("model fingerprint unavailable"))?;
        let epoch = self.epoch.load(Ordering::Acquire);
        let policy = self.policy();
        let after = {
            let mut state = self.vectors.lock().unwrap_or_else(|p| p.into_inner());
            if state.epoch != epoch
                || state.fingerprint != fingerprint
                || state.policy.as_ref() != Some(&policy)
            {
                *state = VectorState {
                    epoch,
                    fingerprint: fingerprint.clone(),
                    policy: Some(policy.clone()),
                    ..Default::default()
                };
            }
            if state.retry_due.is_some_and(|due| due > now()) {
                return Ok(false);
            }
            state.retry_due = None;
            state.cursor
        };
        let page = {
            let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
            store.invalidate_vectors(&fingerprint)?;
            store.pending_vector_page(after, VECTOR_PAGE_BUDGET, VECTOR_BATCH, |file, before| {
                self.visible(file, before)
            })?
        };
        let mut work = page.work;
        if work.is_empty() {
            self.vectors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cursor = if page.more { page.cursor } else { 0 };
            return Ok(page.more);
        }
        for (file, _, text, _) in &mut work {
            *text = ws_modelmgr::recipe::code_document(&file.name, text);
        }
        let inputs = work.iter().map(|w| w.2.clone()).collect::<Vec<_>>();
        let valid = || {
            self.epoch.load(Ordering::Acquire) == epoch
                && self.policy() == policy
                && self.fingerprint().as_deref() == Some(&fingerprint)
                && self.indexer.indexing_pause_reason().is_none()
                && work.iter().all(|(file, _, _, before)| {
                    self.visible(file, before.as_deref())
                        && self
                            .store
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .source_snapshot(file.id)
                            .ok()
                            .flatten()
                            == *before
                })
        };
        let vectors = match self
            .embed
            .embed_texts_while(&inputs, Duration::from_secs(120), valid)
            .and_then(|v| {
                if v.len() == work.len() {
                    Ok(v)
                } else {
                    Err(crate::embed_client::EmbedError::Protocol(
                        "code embedding count mismatch".into(),
                    ))
                }
            }) {
            Ok(v) => v,
            Err(e) => {
                // Live-fence cancellation waits for the gate's own signal.
                // Inference failures retain a bounded wake independently of
                // durable parser poison attempts; missing models above do not.
                if valid() {
                    let mut state = self.vectors.lock().unwrap_or_else(|p| p.into_inner());
                    state.failures = state.failures.saturating_add(1);
                    let backoff = (1_000i64 << state.failures.saturating_sub(1).min(6)).min(60_000);
                    state.retry_due = Some(now() + backoff);
                }
                tracing::warn!(error=%e,"code vectors deferred");
                return Ok(false);
            }
        };
        if self.epoch.load(Ordering::Acquire) != epoch
            || self.policy() != policy
            || self.fingerprint().as_deref() != Some(&fingerprint)
            || self.indexer.indexing_pause_reason().is_some()
        {
            return Ok(false);
        }
        let mut store = self.store.lock().unwrap_or_else(|p| p.into_inner());
        for ((file, id, _, before), vector) in work.into_iter().zip(vectors) {
            if self.epoch.load(Ordering::Acquire) != epoch
                || self.policy() != policy
                || self.fingerprint().as_deref() != Some(&fingerprint)
                || self.indexer.indexing_pause_reason().is_some()
            {
                return Ok(false);
            }
            if self.visible(&file, before.as_deref()) && store.source_snapshot(file.id)? == before {
                store.set_chunk_vector(id, &vector)?;
            }
        }
        let mut state = self.vectors.lock().unwrap_or_else(|p| p.into_inner());
        state.cursor = page.cursor;
        state.failures = 0;
        Ok(true)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn occupy_parser_capacity(root: &Path) -> Vec<crate::extract_client::tests::CleanupFixture> {
        (0..2).map(|number| {
            let directory = root.join(format!("capacity-{number}"));
            std::fs::create_dir(&directory).unwrap();
            let client = ExtractClient::new(directory.join("unused.exe"), directory.join("cache"));
            crate::extract_client::tests::cleanup_trigger(&client, directory)()
        }).collect()
    }

    #[test]
    fn parser_capacity_preserves_due_code_jobs_and_waits_without_a_busy_loop() {
        let (code, _indexer, root) = indexed_fixture();
        let path = root.join("lib.rs");
        let due = now();
        code.store.lock().unwrap().enqueue_file_job(&path.to_string_lossy(),
            &root.to_string_lossy(), 0, due).unwrap();
        let occupied = occupy_parser_capacity(root.parent().unwrap());
        let worked = code.turn().unwrap();
        let (after_due, attempts) = {
            let mut store = code.store.lock().unwrap();
            let after_due = store.next_job_due().unwrap();
            let job = store.claim_job(i64::MAX).unwrap().unwrap();
            let attempts = job.attempts;
            store.retry_job(&job, after_due.unwrap(), false).unwrap();
            (after_due, attempts)
        };
        assert!(!worked && after_due == Some(due) && attempts == 0,
            "resource denial must precede claims: worked={worked}, due={after_due:?}, attempts={attempts}");
        *code.wake.0.lock().unwrap() = false;
        let started = std::time::Instant::now();
        code.wait_for_work();
        assert!(started.elapsed() >= Duration::from_millis(20), "busy admission must wait for resource availability");
        assert!(started.elapsed() < Duration::from_millis(500), "resource wait must remain policy-responsive");
        drop(occupied);
        assert!(code.turn().unwrap(), "uncharged durable work must resume when capacity returns");
        assert!(!code.exact("needle", 2).is_empty());
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn parser_capacity_race_after_claim_requeues_without_charging_poison_attempts() {
        let (code, _indexer, root) = indexed_fixture();
        let path = root.join("lib.rs");
        code.store.lock().unwrap().enqueue_file_job(&path.to_string_lossy(),
            &root.to_string_lossy(), 0, now()).unwrap();
        let job = code.store.lock().unwrap().claim_job(now()).unwrap().unwrap();
        assert!(!job.scan);
        let occupied = occupy_parser_capacity(root.parent().unwrap());
        let outcome = code.parse(&job);
        assert!(outcome.as_ref().err().is_some_and(|error|
            matches!(error.downcast_ref::<crate::extract_client::ExtractClientError>(),
                Some(crate::extract_client::ExtractClientError::AdmissionUnavailable))),
            "this must exercise a real post-claim admission race: {outcome:?}");
        code.complete_parse_job(&job, outcome).unwrap();
        {
            let mut store = code.store.lock().unwrap();
            let due = store.next_job_due().unwrap().expect("resource failure must retain a wake");
            assert!(due <= now() + 1000, "capacity must not defer code for the poison backoff");
            let retried = store.claim_job(i64::MAX).unwrap().unwrap();
            assert_eq!(retried.attempts, 0);
            assert_eq!(retried.revision, job.revision);
            store.retry_job(&retried, now(), false).unwrap();
        }
        drop(occupied);
        assert!(code.turn().unwrap());
        assert!(!code.exact("needle", 2).is_empty());
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    fn vector_fixture(delay: u64, missing_worker: bool) -> (Arc<CodeService>, PathBuf) {
        let (mut code, root) = fixture();
        let model_dir = root.parent().unwrap().join("fixture-model");
        std::fs::create_dir_all(&model_dir).unwrap();
        let manager = ws_modelmgr::ModelManager::with_files(
            model_dir.clone(),
            Default::default(),
            vec![ws_modelmgr::ExpectedFile {
                name: "fixture.bin",
                size: 1,
                sha256: None,
            }],
        );
        let pipe = format!(
            r"\\.\pipe\winsearch-code-vector-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        let service = Arc::get_mut(&mut code).unwrap();
        service.models = ModelService::with_manager(manager, Arc::clone(&service.indexer));
        service.embed = Arc::new(EmbedSupervisor::with_config(
            crate::embed_client::EmbedConfig {
                worker_bin: if missing_worker {
                    root.parent().unwrap().join("worker.exe")
                } else {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../target/debug/winsearch-embed.exe")
                },
                pipe_name: pipe.clone(),
                worker_args: vec![
                    "--pipe".into(),
                    pipe,
                    "--stub".into(),
                    "--stub-delay-ms".into(),
                    delay.to_string(),
                    "--idle-secs".into(),
                    "1".into(),
                ],
                backoff_base: Duration::from_millis(10),
                sleeper: None,
            },
        ));
        std::fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
        code.configure().unwrap();
        for _ in 0..16 {
            if !code.turn().unwrap() {
                break;
            }
        }
        std::fs::write(model_dir.join("fixture.bin"), [1]).unwrap();
        (code, root)
    }

    #[test]
    fn background_vectors_cancel_promptly_on_source_pause_or_root_change() {
        for reason in ["source", "pause", "root"] {
            let (code, root) = vector_fixture(1800, false);
            let pending = Arc::clone(&code);
            let thread = std::thread::spawn(move || pending.embed_turn().unwrap());
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !code.embed.model_loaded() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(code.embed.model_loaded());
            std::thread::sleep(Duration::from_millis(50));
            let start = std::time::Instant::now();
            match reason {
                "source" => {
                    std::fs::write(root.join("lib.rs"), "fn edited() {}\n").unwrap();
                    code.changed(
                        &root.to_string_lossy(),
                        WatchEvent::Modified(root.join("lib.rs")),
                    )
                    .unwrap();
                }
                "pause" => {
                    let mut policy = code.policy();
                    policy.paused = true;
                    code.indexer.set_indexing_config(&policy).unwrap();
                }
                _ => {
                    let mut policy = code.policy();
                    policy.code_roots.clear();
                    code.indexer.set_indexing_config(&policy).unwrap();
                }
            }
            assert!(!thread.join().unwrap());
            let elapsed = start.elapsed();
            let embedded = code
                .store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total;
            code.embed.terminate_worker();
            drop(code);
            std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
            assert_eq!(embedded, 0, "{reason} cannot publish stale vectors");
            assert!(
                elapsed < Duration::from_millis(500),
                "{reason} left inference running for {elapsed:?}"
            );
        }
    }

    #[test]
    fn background_vector_failure_retains_a_wake_and_recovers_without_signal() {
        let (code, root) = vector_fixture(0, true);
        stop_fixture_watchers(&code);
        assert!(!code.exact("needle",1).is_empty(),"recovery fixture must have current parsed text before failing inference");
        assert!(!code.embed_turn().unwrap());
        assert!(code.vectors.lock().unwrap().failures>0,"the first attempt must actually fail on the missing worker");
        let due = code.next_work_due();
        let worker = code.embed.worker_bin().clone();
        std::fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/debug/winsearch-embed.exe"),
            &worker,
        )
        .unwrap();
        let recovered = if let Some(due) = due {
            assert!(due > now() && due <= now() + 60_000);
            // Exercise the same condition-variable deadline as the coordinator;
            // there is deliberately no watcher/model/policy signal after failure.
            *code.wake.0.lock().unwrap() = false;
            let deadline=due+5_000;
            loop {
                if code.next_work_due().is_none_or(|next|next>deadline) {break false;}
                code.wait_for_work();
                if code.turn().unwrap() {break true;}
                if now()>=deadline {break false;}
            }
        } else {
            false
        };
        let embedded = code
            .store
            .lock()
            .unwrap()
            .stats()
            .unwrap()
            .embedded_chunks_total;
        code.embed.terminate_worker();
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
        assert!(
            due.is_some(),
            "deferred vectors lost their coordinator wake deadline"
        );
        assert!(recovered);
        assert_eq!(embedded, 1, "retry must use persisted exact chunks");
    }

    #[test]
    fn background_vector_turn_retains_continuation_through_hidden_pages() {
        let (code, root) = vector_fixture(0, false);
        // This test seeds durable parsed rows directly; watcher/reparse races
        // are covered separately and would deliberately reset this cursor.
        code.watchers.lock().unwrap().clear();
        let hidden = root.join("hidden.rs");
        let mut policy = code.policy();
        policy.excluded_paths.push(hidden.to_string_lossy().into());
        code.indexer.set_indexing_config(&policy).unwrap();
        {
            let mut store = code.store.lock().unwrap();
            store
                .invalidate_vectors(&code.fingerprint().unwrap())
                .unwrap();
            for file in store.all_files().unwrap() {
                for chunk in store.list_chunks_for_file(file.id).unwrap() {
                    store.set_chunk_vector(chunk.id, &[1.0, 0.0]).unwrap();
                }
            }
            let repo = store.all_files().unwrap()[0].repo_id;
            for (path, count) in [(hidden, 96), (root.join("visible.rs"), 20)] {
                std::fs::write(&path, "// generated deterministic vector fixture\n").unwrap();
                let record = ws_code::record_for_code_path(&root, &path).unwrap().unwrap();
                let file = store.apply_file_batch(repo, 1, &[record]).unwrap()[0].file_id();
                let parsed = ParsedCodeFile {
                    language: "rust".into(),
                    symbols: Vec::new(),
                    diagnostics: Vec::new(),
                    chunks: (1..=count)
                        .map(|line| ws_code::ParsedCodeUnit {
                            start_line: line,
                            end_line: line,
                            symbol_name: String::new(),
                            symbol_kind: String::new(),
                            text: format!("// fixture chunk {line}"),
                            notebook: None,
                        })
                        .collect(),
                };
                store.commit_parsed_file(file, &parsed).unwrap();
                store
                    .set_source_snapshot(file, &snapshot(&path).unwrap())
                    .unwrap();
            }
        }
        assert!(code.embed_turn().unwrap());
        assert_eq!(
            code.embed.stats().jobs,
            0,
            "a hidden-only page must yield before inference"
        );
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            1
        );
        assert!(code.embed_turn().unwrap());
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            17
        );
        assert!(code.embed_turn().unwrap());
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            21
        );
        assert!(!code.embed_turn().unwrap());
        code.embed.terminate_worker();
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }
    fn fixture() -> (Arc<CodeService>, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "winsearch-code-service-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(root.join("repo")).unwrap();
        let mut store = ws_index::IndexStore::open_in_memory().unwrap();
        let mut policy = store.get_or_migrate_indexing_config().unwrap();
        policy.code_enabled = true;
        policy.code_roots = vec![root.join("repo").to_string_lossy().into()];
        policy.respect_battery_saver = false;
        policy.index_on_battery = true;
        store.set_indexing_config(&policy).unwrap();
        let indexer = Indexer::open(store, &[]).unwrap();
        let models = ModelService::new(root.join("models"), Arc::clone(&indexer));
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/winsearch-extract.exe");
        let code = CodeService::new(
            CodeStore::open(&root.join("code.db")).unwrap(),
            indexer,
            Arc::new(EmbedSupervisor::new()),
            models,
            ExtractClient::new(worker, root.join("cache")),
        );
        (code, root.join("repo"))
    }

    pub(crate) fn indexed_fixture() -> (Arc<CodeService>, Arc<Indexer>, PathBuf) {
        let (code, root) = fixture();
        std::fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
        std::fs::write(root.join("candidate.rs"), "fn resolve_record() {}\n").unwrap();
        code.configure().unwrap();
        for _ in 0..16 {
            if !code.turn().unwrap() {
                break;
            }
        }
        {
            let mut store = code.store.lock().unwrap();
            assert_eq!(store.all_files().unwrap().len(), 2);
            store
                .invalidate_vectors(&code.fingerprint().unwrap())
                .unwrap();
            for file in store.all_files().unwrap() {
                for chunk in store.list_chunks_for_file(file.id).unwrap() {
                    store.set_chunk_vector(chunk.id, &[1.0, 0.0, 0.0]).unwrap();
                }
            }
        }
        let indexer = Arc::clone(&code.indexer);
        (code, indexer, root)
    }

    pub(crate) fn unindexed_fixture() -> (Arc<CodeService>,Arc<Indexer>,PathBuf) {
        let (code,root)=fixture();let indexer=Arc::clone(&code.indexer);(code,indexer,root)
    }

    pub(crate) fn stop_fixture_watchers(code:&CodeService) {
        code.watchers.lock().unwrap().clear();
    }

    #[test]
    fn code_provenance_native_parser_commits_actual_hash_and_advances_durable_content_revision() {
        let (code,root)=fixture();
        let path=root.join("lib.rs");
        std::fs::write(&path,b"fn needle() {}\n").unwrap();
        code.configure().unwrap();
        for _ in 0..16 {if !code.turn().unwrap() {break;}}
        let (file,initial)={
            let store=code.store.lock().unwrap();
            let file=store.find_file_by_path(&path.to_string_lossy()).unwrap().unwrap();
            let state=store.source_provenance(file.id).unwrap().expect("real parser must persist source provenance");
            assert_eq!(state.sha256.as_deref(),Some("36bc22784d3cd4b5e6ae27ab2702f579957349879a55fed91582bb0861a43f9f"));
            assert_eq!(state.snapshot,snapshot(&path).unwrap());
            assert_eq!(state.revision,1);
            (file,state)
        };
        code.store.lock().unwrap().enqueue_file_job(&file.path,&root.to_string_lossy(),0,now()).unwrap();
        assert!(code.turn().unwrap());
        {
            let store=code.store.lock().unwrap();
            let state=store.source_provenance(file.id).unwrap().unwrap();
            assert_eq!(state.sha256,initial.sha256);
            assert_eq!(state.snapshot,initial.snapshot);
            assert_eq!(state.revision,initial.revision+1);
            assert!(store.search_exact("needle",2,|_|true).unwrap().iter().any(|hit|hit.file.id==file.id));
        }
        // Legacy caches have a current metadata snapshot but no byte digest.
        code.store.lock().unwrap().set_source_snapshot(file.id,&initial.snapshot).unwrap();
        drop(code);
        let (code,reopened_root)=fixture();
        assert_eq!(reopened_root,root);
        code.configure().unwrap();
        for _ in 0..16 {if !code.turn().unwrap() {break;}}
        {
            let store=code.store.lock().unwrap();
            let recovered=store.source_provenance(file.id).unwrap().unwrap();
            assert_eq!(recovered.sha256,initial.sha256,"restart must establish unknown legacy hash from a real source parse");
            assert_eq!(recovered.revision,initial.revision+2);
        }
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn committing_native_parse_invalidates_inflight_query_revision() {
        let (code, root) = fixture();
        std::fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
        code.configure().unwrap();
        let before = code.epoch.load(Ordering::Acquire);
        for _ in 0..12 {
            if !code.turn().unwrap() {
                break;
            }
        }
        assert!(!code.exact("needle", 1).is_empty());
        assert_ne!(
            code.epoch.load(Ordering::Acquire),
            before,
            "published code text must invalidate excerpts planned against the previous revision"
        );
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }
    #[test]
    fn native_coordinator_exact_without_model_and_live_revocation() {
        let (code, root) = fixture();
        std::fs::write(root.join("lib.rs"), "fn needle() { needle(); }\n").unwrap();
        code.configure().unwrap();
        for _ in 0..8 {
            if !code.turn().unwrap() {
                break;
            }
        }
        let hits = code.exact("needle", 2);
        assert_eq!(hits.len(), 2);
        assert!(hits[0].is_definition);
        assert!(!hits[1].is_definition);
        assert!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total
                == 0
        );
        let mut policy = code.policy();
        policy.paused = true;
        code.indexer.set_indexing_config(&policy).unwrap();
        assert_eq!(
            code.exact("needle", 2).len(),
            2,
            "pause preserves exact queries"
        );
        assert!(!code.turn().unwrap());
        policy.code_roots.clear();
        code.indexer.set_indexing_config(&policy).unwrap();
        assert!(
            code.exact("needle", 2).is_empty(),
            "root revocation hides existing content immediately"
        );
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }
    #[test]
    fn watcher_same_second_edit_hides_then_coalesces() {
        let (mut code, root) = fixture();
        Arc::get_mut(&mut code).unwrap().quiet = Duration::from_millis(30);
        let path = root.join("lib.rs");
        std::fs::write(&path, "fn alpha() {}\n").unwrap();
        code.configure().unwrap();
        for _ in 0..8 {
            if !code.turn().unwrap() {
                break;
            }
        }
        assert!(!code.exact("alpha", 1).is_empty());
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::fs::write(&path, "fn omega() {}\n").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        code.changed(&root.to_string_lossy(), WatchEvent::Modified(path.clone()))
            .unwrap();
        assert!(code.exact("alpha", 1).is_empty());
        assert!(!code.turn().unwrap(), "quiet period must defer parsing");
        std::thread::sleep(Duration::from_millis(50));
        for _ in 0..8 {
            if !code.turn().unwrap() {
                break;
            }
        }
        assert!(!code.exact("omega", 1).is_empty());
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn deterministic_supervisor_vectors_defer_resume_and_reject_model_swap_inflight() {
        let (mut code, root) = fixture();
        let model_dir = root.parent().unwrap().join("fixture-model");
        std::fs::create_dir_all(&model_dir).unwrap();
        let manager = ws_modelmgr::ModelManager::with_files(
            model_dir.clone(),
            Default::default(),
            vec![ws_modelmgr::ExpectedFile {
                name: "fixture.bin",
                size: 1,
                sha256: None,
            }],
        );
        let pipe = format!(
            r"\\.\pipe\winsearch-code-stub-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        let service = Arc::get_mut(&mut code).unwrap();
        service.models = ModelService::with_manager(manager, Arc::clone(&service.indexer));
        service.embed = Arc::new(EmbedSupervisor::with_config(
            crate::embed_client::EmbedConfig {
                worker_bin: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../target/debug/winsearch-embed.exe"),
                pipe_name: pipe.clone(),
                worker_args: vec![
                    "--pipe".into(),
                    pipe,
                    "--stub".into(),
                    "--stub-delay-ms".into(),
                    "250".into(),
                    "--idle-secs".into(),
                    "1".into(),
                ],
                backoff_base: Duration::from_millis(10),
                sleeper: None,
            },
        ));
        std::fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
        code.configure().unwrap();
        for _ in 0..12 {
            if !code.turn().unwrap() {
                break;
            }
        }
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            0
        );
        std::fs::write(model_dir.join("fixture.bin"), [1]).unwrap();
        assert!(code.embed_turn().unwrap());
        assert!(code.embed.model_loaded());
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            1
        );
        code.indexer.store_lock().reset_content_vectors().unwrap();
        assert!(
            code.semantic(&[1.0; ws_embed_protocol::STUB_DIM], &[1.0; ws_embed_protocol::STUB_DIM], 1)
                .is_empty(),
            "stale recipe vectors must be hidden before invalidation"
        );
        let pending = Arc::clone(&code);
        let thread = std::thread::spawn(move || pending.embed_turn().unwrap());
        std::thread::sleep(Duration::from_millis(80));
        code.indexer.store_lock().reset_content_vectors().unwrap();
        assert!(
            !thread.join().unwrap(),
            "in-flight vectors cannot commit across the model generation"
        );
        assert_eq!(
            code.store
                .lock()
                .unwrap()
                .stats()
                .unwrap()
                .embedded_chunks_total,
            0
        );
        assert!(code.embed_turn().unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        while code.embed.is_worker_running() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            !code.embed.is_worker_running(),
            "shared supervisor idle lifecycle must release fixture worker"
        );
        drop(code);
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn owned_child_inflight_is_cancelled_by_source_pause_and_root_revocation() {
        static FIXTURE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        let binary = FIXTURE.get_or_init(|| {
            let binary = std::env::temp_dir().join(format!(
                "winsearch-code-cancel-fixture-{}.exe",
                std::process::id()
            ));
            let status = std::process::Command::new("rustc")
                .arg(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/fixtures/extract_transport.rs"),
                )
                .args(["--edition=2021", "-O", "-o"])
                .arg(&binary)
                .status()
                .unwrap();
            assert!(status.success());
            binary
        });
        for reason in ["source", "pause", "root"] {
            let (mut code, root) = fixture();
            let pid = root.parent().unwrap().join("child.pid");
            let release = root.parent().unwrap().join("release");
            let service = Arc::get_mut(&mut code).unwrap();
            service.extract =
                ExtractClient::new(binary.clone(), root.parent().unwrap().join("cache"));
            service.extract.worker_args = vec![
                "code-wait".into(),
                pid.to_string_lossy().into(),
                release.to_string_lossy().into(),
            ];
            std::fs::write(root.join("lib.rs"), "fn needle() {}\n").unwrap();
            code.configure().unwrap();
            while code.store.lock().unwrap().all_files().unwrap().is_empty()
                || code.scan.lock().unwrap().is_some()
            {
                assert!(code.turn().unwrap());
            }
            let job = code
                .store
                .lock()
                .unwrap()
                .claim_job(now())
                .unwrap()
                .unwrap();
            assert!(!job.scan);
            let pending = Arc::clone(&code);
            let thread = std::thread::spawn(move || pending.parse(&job));
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !pid.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(
                pid.exists(),
                "owned fixture child must start before revocation"
            );
            match reason {
                "source" => {
                    std::fs::write(root.join("lib.rs"), "fn changed() {}\n").unwrap();
                    code.changed(
                        &root.to_string_lossy(),
                        WatchEvent::Modified(root.join("lib.rs")),
                    )
                    .unwrap();
                }
                "pause" => {
                    let mut policy = code.policy();
                    policy.paused = true;
                    code.indexer.set_indexing_config(&policy).unwrap();
                }
                _ => {
                    let mut policy = code.policy();
                    policy.code_roots.clear();
                    code.indexer.set_indexing_config(&policy).unwrap();
                }
            }
            assert!(
                thread.join().unwrap().is_err(),
                "{reason} must prevent parse commit"
            );
            assert_eq!(code.store.lock().unwrap().stats().unwrap().chunks_total, 0);
            let child = std::fs::read_to_string(pid)
                .unwrap()
                .parse::<u32>()
                .unwrap();
            unsafe {
                use windows::Win32::{
                    Foundation::{CloseHandle, WAIT_OBJECT_0},
                    System::Threading::{OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE},
                };
                if let Ok(handle) = OpenProcess(PROCESS_SYNCHRONIZE, false, child) {
                    assert_eq!(WaitForSingleObject(handle, 1000), WAIT_OBJECT_0);
                    CloseHandle(handle).unwrap();
                }
            }
            drop(code);
            std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
        }
    }
}
