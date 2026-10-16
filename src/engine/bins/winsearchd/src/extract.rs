//! Content extraction worker pool (Phase 4, P4.1).
//!
//! Two background threads (`EXTRACT_WORKER_THREADS = 2`) drain non-name jobs
//! from `extract_jobs` in priority order (small + recent files first via
//! `compute_extract_priority`), run the registered modality [`Extractor`]
//! under a per-file [`ExtractBudget`], index extracted text into `content_fts`
//! for exact search, and embed `embed == true` segments into `segments` via
//! the embed supervisor.
//!
//! Crash-safety: jobs stay in `claimed = 1, state = 'claimed'` while in flight;
//! [`ws_index::IndexStore::release_extract_claims`] on startup returns any
//! interrupted job back to `pending`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::warn;
use ws_extract::{
    DocumentExtractor, ExtractBudget, ExtractError, ExtractOutput, ExtractionStatus, Extractor,
    ImageExtractor,
};
use ws_index::model::{SEGMENT_KIND_DOC, SEGMENT_KIND_IMAGE, SEGMENT_KIND_OCR, SEGMENT_KIND_PDF};
use ws_index::{ExtractJob, StoreError};

use crate::embed_client::{EmbedError, EmbedSupervisor, EMBED_JOB_TIMEOUT};
use crate::extract_client::{ArtifactLease, ExtractClient, ExtractClientError};
use crate::indexer::{Indexer, MAX_EMBED_ATTEMPTS};
use ws_index::model::{ContentChunk, ContentMatch};

/// Number of background content extraction worker threads (P4.1).
pub const EXTRACT_WORKER_THREADS: usize = 2;

/// Outcome of processing at most one content extraction job (`extract_once_with`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtractStepOutcome {
    /// Worker admission is temporarily unavailable; wait without charging a file failure.
    pub admission_unavailable: bool,
    /// True if a pending content job was claimed.
    pub claimed: bool,
    /// Status reported by the extractor (when a job ran).
    pub status: Option<ExtractionStatus>,
    /// Number of vector segments written to `segments`.
    pub segments_stored: usize,
    /// Number of text segments indexed into `content_fts`.
    pub fts_chunks_indexed: usize,
    /// True if the job was requeued due to a mid-extraction file change.
    pub requeued: bool,
    /// True if the job failed and was released for retry.
    pub failed: bool,
    /// True if the job reached [`MAX_EMBED_ATTEMPTS`] and was marked `failed`.
    pub poisoned: bool,
}

/// Registry mapping `extract_jobs.kind` (`doc`, `pdf`, `image`, `audio`, `video`)
/// to its [`Extractor`] implementation plus the per-file [`ExtractBudget`].
pub struct ExtractorRegistry {
    extractors: HashMap<String, Arc<dyn Extractor>>,
    budget_factory: Box<dyn Fn(&str) -> ExtractBudget + Send + Sync>,
    worker: Option<ExtractClient>,
}

impl Default for ExtractorRegistry {
    fn default() -> Self {
        let mut reg = Self {
            extractors: HashMap::new(),
            budget_factory: Box::new(|_kind| ExtractBudget::default()),
            worker: None,
        };
        reg.register(SEGMENT_KIND_DOC, Arc::new(DocumentExtractor));
        reg.register(SEGMENT_KIND_PDF, Arc::new(ws_extract::pdf::PdfExtractor));
        reg.register(SEGMENT_KIND_IMAGE, Arc::new(ImageExtractor::default()));
        reg
    }
}

/// Alias for [`ExtractorRegistry`].
#[allow(dead_code)]
pub type ExtractionPool = ExtractorRegistry;

impl ExtractorRegistry {
    fn admission_available(&self) -> bool {
        self.worker.as_ref().is_none_or(ExtractClient::admission_available)
    }

    /// Production never opens content through an in-process parser.
    pub fn production() -> Self {
        Self {
            extractors: HashMap::new(),
            budget_factory: Box::new(|_| ExtractBudget::default()),
            worker: Some(ExtractClient::production()),
        }
    }
    /// Registers (or replaces) the extractor for `kind`.
    pub fn register(&mut self, kind: impl Into<String>, extractor: Arc<dyn Extractor>) {
        self.extractors.insert(kind.into(), extractor);
    }

    /// Overrides how per-file [`ExtractBudget`]s are constructed for a given `kind`.
    pub fn set_budget_factory<F>(&mut self, factory: F)
    where
        F: Fn(&str) -> ExtractBudget + Send + Sync + 'static,
    {
        self.budget_factory = Box::new(factory);
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn Extractor>> {
        self.extractors.get(kind).cloned()
    }

    pub fn budget_for(&self, kind: &str) -> ExtractBudget {
        (self.budget_factory)(kind)
    }
}

impl Indexer {
    /// Claims and processes at most one non-name content extraction job using
    /// `registry` and the `embed` callback.
    ///
    /// Store lock discipline: the lock is acquired briefly to claim the job
    /// and snapshot the entry, released while file extraction and embedding run,
    /// and re-acquired to verify the entry did not change mid-flight before
    /// writing `content_fts` and `segments`.
    pub fn extract_once_with<F>(
        &self,
        registry: &ExtractorRegistry,
        embed: F,
    ) -> Result<ExtractStepOutcome, StoreError>
    where
        F: Fn(&[String]) -> Result<Vec<Vec<f32>>, String>,
    {
        self.extract_once_with_media(registry, embed, |_path| Ok(None))
    }

    /// Like [`Self::extract_once_with`], with an optional `embed_image`
    /// callback for computing a visual embedding (`EmbedImage` via `mtmd`) on
    /// `image` modality jobs.
    pub fn extract_once_with_media<F, FI>(
        &self,
        registry: &ExtractorRegistry,
        embed: F,
        embed_image: FI,
    ) -> Result<ExtractStepOutcome, StoreError>
    where
        F: Fn(&[String]) -> Result<Vec<Vec<f32>>, String>,
        FI: Fn(&Path) -> Result<Option<Vec<f32>>, String>,
    {
        self.extract_once_typed(
            registry,
            |texts| embed(texts).map_err(EmbedError::EmbedFailed),
            |path| embed_image(path).map_err(EmbedError::EmbedFailed),
        )
    }

    fn extract_once_typed<F, FI>(
        &self,
        registry: &ExtractorRegistry,
        embed: F,
        embed_image: FI,
    ) -> Result<ExtractStepOutcome, StoreError>
    where
        F: Fn(&[String]) -> Result<Vec<Vec<f32>>, EmbedError>,
        FI: Fn(&Path) -> Result<Option<Vec<f32>>, EmbedError>,
    {
        if self.indexing_pause_reason().is_some() {
            return Ok(ExtractStepOutcome::default());
        }
        if !registry.admission_available() {
            return Ok(ExtractStepOutcome { admission_unavailable: true, ..Default::default() });
        }
        // 1. Claim highest-priority non-name job and snapshot entry metadata.
        let claimed = {
            let mut store = self.store_lock();
            if self.indexing_pause_reason().is_some() { return Ok(ExtractStepOutcome::default()); }
            if !registry.admission_available() {
                return Ok(ExtractStepOutcome { admission_unavailable: true, ..Default::default() });
            }
            let Some(job) = store.claim_content_extract_job()? else {
                drop(store);
                return self.embed_content_pending(&embed, &embed_image, None);
            };
            match store.get_entry(job.entry_id)? {
                Some(entry)
                    if !entry.missing
                        && !entry.is_dir
                        && ws_extract::modality_kind_for_ext(&entry.ext)
                            == Some(job.kind.as_str()) =>
                {
                    Some((job, entry))
                }
                _ => {
                    store.complete_extract_job(job.id)?;
                    return Ok(ExtractStepOutcome {
                        claimed: true,
                        status: Some(ExtractionStatus::Unsupported),
                        ..Default::default()
                    });
                }
            }
        };

        let (job, entry) = claimed.expect("checked Some above");
        if !self.content_path_allowed(Path::new(&entry.path)) {
            let mut store = self.store_lock();
            clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
            store.complete_extract_job(job.id)?;
            drop(store);
            self.invalidate_vector_cache();
            return Ok(ExtractStepOutcome { claimed: true, status: Some(ExtractionStatus::Unsupported), ..Default::default() });
        }
        let budget = registry.budget_for(&job.kind);

        // 2. Look up extractor for this modality; if unregistered yet (e.g.
        // audio/video before P4.5/P4.6), complete cleanly as Unsupported so
        // the file remains searchable by exact/semantic name.
        let extractor = registry.get(&job.kind);
        if extractor.is_none() && registry.worker.is_none() {
            let mut store = self.store_lock();
            clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
            store.complete_extract_job(job.id)?;
            drop(store);
            self.invalidate_vector_cache();
            return Ok(ExtractStepOutcome {
                claimed: true,
                status: Some(ExtractionStatus::Unsupported),
                ..Default::default()
            });
        }

        // 3. Run extraction outside the store lock, enforcing the budget.
        let path = Path::new(&entry.path);
        let snapshot = source_snapshot(path);
        let (progress, pdf_source_replaced) = {
            let mut store = self.store_lock();
            let pdf_source_replaced = job.kind == SEGMENT_KIND_PDF
                && store
                    .content_progress(job.entry_id, &job.kind)?
                    .is_some_and(|previous| previous.source != snapshot);
            if pdf_source_replaced {
                store.invalidate_content_source(job.entry_id)?;
            }
            (
                store.begin_content_batch(job.entry_id, &job.kind, &snapshot)?,
                pdf_source_replaced,
            )
        };
        if pdf_source_replaced {
            self.invalidate_vector_cache();
        }
        let source = std::fs::metadata(path)
            .ok()
            .map(|m| (m.len(), m.modified().ok()));
        let identity = ws_platform::identity::file_identity(path);
        let source_unchanged = || source.is_some()
            && std::fs::metadata(path).ok().map(|m| (m.len(), m.modified().ok())) == source
            && ws_platform::identity::file_identity(path) == identity;
        let extract_res = if let Some(client) = &registry.worker {
            let mut request = client.request(
                path.to_path_buf(),
                &job.kind,
                budget.max_segments.min(30) as u32,
            );
            request.cursor = progress.cursor;
            match client.extract_while(
                &request,
                budget
                    .deadline
                    .saturating_duration_since(std::time::Instant::now()),
                || {
                    self.indexing_pause_reason().is_none()
                        && self.content_path_allowed(path)
                        && source_unchanged()
                        && self.store_lock().content_revision(job.entry_id).ok()
                            == Some(progress.revision)
                },
            ) {
                Ok(response) => Ok(response),
                Err(ExtractClientError::AdmissionUnavailable) => {
                    self.store_lock().requeue_extract_job(job.id)?;
                    return Ok(ExtractStepOutcome { claimed: true, requeued: true, admission_unavailable: true, ..Default::default() });
                }
                Err(ExtractClientError::Cancelled) => {
                    self.store_lock().requeue_extract_job(job.id)?;
                    return Ok(ExtractStepOutcome { claimed: true, requeued: true, ..Default::default() });
                }
                Err(ExtractClientError::Unsupported(_)) => {
                    Ok(ws_extract::worker::ExtractResponse {
                        output: ExtractOutput::unsupported(),
                        next_cursor: None,
                        total_units: None,
                        diagnostics: Vec::new(),
                    })
                }
                Err(ExtractClientError::Timeout) => Err(ExtractError::DeadlineExceeded),
                Err(ExtractClientError::Malformed(message)) => Err(ExtractError::Decode(message)),
                Err(ExtractClientError::Worker(message)) => Err(ExtractError::Io(std::io::Error::other(message))),
            }
        } else {
            budget
                .check_deadline()
                .and_then(|_| std::fs::metadata(path).map_err(ExtractError::from))
                .and_then(|m| budget.check_size(m.len()))
                .and_then(|_| {
                    extractor.expect("registered extractor").extract_batch(
                        path,
                        &budget,
                        progress.cursor,
                    )
                })
        };

        {
            let mut store = self.store_lock();
            if !source_unchanged()
                || store.content_revision(entry.id)? != progress.revision
                || store.model_generation()? != progress.model_generation
                || self.indexing_pause_reason().is_some()
                || !self.content_path_allowed(path)
            {
                store.requeue_extract_job(job.id)?;
                return Ok(ExtractStepOutcome {
                    claimed: true,
                    requeued: true,
                    ..Default::default()
                });
            }
        }
        let response = match extract_res {
            Ok(out) => out,
            Err(err) => {
                // Budget violations (FileTooLarge, DeadlineExceeded, SegmentLimitExceeded)
                // and Unsupported formats degrade cleanly to name-only without
                // spinning retry loops; transient I/O errors retry up to MAX_EMBED_ATTEMPTS.
                let permanent = matches!(
                    err,
                    ExtractError::FileTooLarge { .. }
                        | ExtractError::DeadlineExceeded
                        | ExtractError::SegmentLimitExceeded { .. }
                        | ExtractError::Unsupported(_)
                        | ExtractError::Decode(_)
                );
                warn!(
                    job = job.id,
                    entry = job.entry_id,
                    kind = %job.kind,
                    path = %entry.path,
                    error = %err,
                    permanent,
                    "content extraction aborted"
                );
                let mut store = self.store_lock();
                if permanent {
                    if progress.cursor == 0 {
                        clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
                    }
                    store.mark_extract_failed(job.id)?;
                    drop(store);
                    self.invalidate_vector_cache();
                    return Ok(ExtractStepOutcome {
                        claimed: true,
                        status: Some(ExtractionStatus::Failed(err.to_string())),
                        poisoned: true,
                        ..Default::default()
                    });
                }
                let poisoned = Self::fail_extract_job_capped(&mut store, &job, &entry.name)?;
                drop(store);
                if poisoned {
                    self.invalidate_vector_cache();
                }
                return Ok(ExtractStepOutcome {
                    claimed: true,
                    status: Some(ExtractionStatus::Failed(err.to_string())),
                    failed: !poisoned,
                    poisoned,
                    ..Default::default()
                });
            }
        };

        for diagnostic in &response.diagnostics {
            warn!(entry_id = job.entry_id, diagnostic, "extraction source limit");
        }
        let next_cursor = response.next_cursor;
        let output = response.output;
        let mut artifacts = registry.worker.as_ref().map(|_| ArtifactLease::for_output(&output));

        if matches!(
            output.status,
            ExtractionStatus::Unsupported | ExtractionStatus::Failed(_)
        ) {
            let mut store = self.store_lock();
            if progress.cursor == 0 {
                clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
            }
            if matches!(output.status, ExtractionStatus::Failed(_)) {
                store.mark_extract_failed(job.id)?;
            } else {
                store.complete_extract_job(job.id)?;
            }
            drop(store);
            self.invalidate_vector_cache();
            return Ok(ExtractStepOutcome {
                claimed: true,
                status: Some(output.status),
                ..Default::default()
            });
        }

        let mut store = self.store_lock();
        let valid = store.get_entry(job.entry_id)?.is_some_and(|e| {
            !e.missing
                && e.path == entry.path
                && ws_extract::modality_kind_for_ext(&e.ext) == Some(job.kind.as_str())
        }) && source_unchanged()
            && store.content_revision(job.entry_id)? == progress.revision
            && store.model_generation()? == progress.model_generation;
        if !valid || self.indexing_pause_reason().is_some() || !self.content_path_allowed(path) {
            store.requeue_extract_job(job.id)?;
            return Ok(ExtractStepOutcome {
                claimed: true,
                requeued: true,
                ..Default::default()
            });
        }
        let mut pdf_page = 0;
        let mut pdf_chunk = 0;
        let mut chunks: Vec<ContentChunk> = output
            .segments
            .iter()
            .enumerate()
            .map(|(i, d)| ContentChunk {
                id: 0,
                entry_id: job.entry_id,
                job_kind: job.kind.clone(),
                kind: d.kind.clone(),
                chunk_index: if job.kind == SEGMENT_KIND_PDF {
                    let page = d.page.expect("validated PDF page") - 1;
                    if pdf_page != page || i == 0 {
                        pdf_page = page;
                        let (start, offset) = ws_extract::pdf::decode_cursor(progress.cursor)
                            .expect("validated PDF cursor");
                        pdf_chunk = if page == start { offset } else { 0 };
                    }
                    let ordinal = ws_extract::pdf::encode_cursor(page, pdf_chunk) as i64;
                    pdf_chunk += 1;
                    ordinal
                } else {
                    (progress.cursor + i as u64) as i64
                },
                text: d.text.clone(),
                location: ContentMatch {
                    excerpt: String::new(),
                    match_byte: None,
                    page: d.page,
                    start_ms: d.start_ms,
                    end_ms: d.end_ms,
                    line: d.line,
                    thumbnail_path: d
                        .thumbnail_hint
                        .as_ref()
                        .map(|p| p.to_string_lossy().into_owned()),
                },
                embed: d.embed
                    && (!d.text.trim().is_empty() || d.kind == ws_extract::pdf::VISUAL_KIND)
                    && output.status == ExtractionStatus::Extracted,
                visual: d.kind == ws_extract::pdf::VISUAL_KIND,
                attempts: 0,
            })
            .collect();
        if progress.cursor == 0
            && job.kind == SEGMENT_KIND_IMAGE
            && output.status == ExtractionStatus::Extracted
        {
            chunks.push(ContentChunk {
                id: 0,
                entry_id: job.entry_id,
                job_kind: job.kind.clone(),
                kind: SEGMENT_KIND_IMAGE.into(),
                chunk_index: -1,
                text: entry.name.clone(),
                location: ContentMatch::default(),
                embed: true,
                visual: true,
                attempts: 0,
            });
        }
        if !store.commit_content_batch(&job, &progress, &chunks, next_cursor)? {
            store.requeue_extract_job(job.id)?;
            return Ok(ExtractStepOutcome {
                claimed: true,
                requeued: true,
                ..Default::default()
            });
        }
        drop(store);
        self.invalidate_vector_cache();
        let mut embedded = self.embed_content_pending(&embed, &embed_image, Some(entry.id))?;
        if job.kind == SEGMENT_KIND_IMAGE && !embedded.requeued && !embedded.failed {
            let visual = self.embed_content_pending(&embed, &embed_image, Some(entry.id))?;
            embedded.segments_stored += visual.segments_stored;
            embedded.requeued |= visual.requeued;
            embedded.failed |= visual.failed;
        }
        if embedded.requeued {
            embedded.segments_stored = 0;
        }
        if source_unchanged()
            && self.indexing_pause_reason().is_none()
            && self.content_path_allowed(path)
            && self.store_lock().content_revision(entry.id)? == progress.revision
        {
            if let Some(artifacts) = artifacts.as_mut() {
                artifacts.keep();
            }
        }
        Ok(ExtractStepOutcome {
            claimed: true,
            status: Some(output.status),
            fts_chunks_indexed: if embedded.requeued {
                0
            } else {
                output
                    .segments
                    .iter()
                    .filter(|d| !d.text.trim().is_empty())
                    .count()
            },
            ..embedded
        })
    }

    fn embed_content_pending<F, FI>(
        &self,
        embed: &F,
        image: &FI,
        preferred: Option<i64>,
    ) -> Result<ExtractStepOutcome, StoreError>
    where
        F: Fn(&[String]) -> Result<Vec<Vec<f32>>, EmbedError>,
        FI: Fn(&Path) -> Result<Option<Vec<f32>>, EmbedError>,
    {
        let (chunks, entry, revision, generation, progress) = {
            let mut store = self.store_lock();
            let chunks = store.claim_content_vectors(preferred)?;
            let Some(first) = chunks.first() else {
                return Ok(ExtractStepOutcome::default());
            };
            let entry = store.get_entry(first.entry_id)?.expect("content FK");
            let revision = store.content_revision(entry.id)?;
            let generation = store.model_generation()?;
            let progress = store.content_progress(entry.id, &first.job_kind)?;
            (chunks, entry, revision, generation, progress)
        };
        let path = Path::new(&entry.path);
        let snapshot = source_snapshot(path);
        if !self.content_path_allowed(path)
            || progress
                .as_ref()
                .is_none_or(|p| p.revision != revision || p.source != snapshot)
        {
            let mut store = self.store_lock();
            store.release_content_vectors(&chunks, false)?;
            if self.content_path_allowed(path) && store.content_revision(entry.id)? == revision {
                store.invalidate_content_source(entry.id)?;
                store.enqueue_extract_job(entry.id, &chunks[0].job_kind)?;
                return Ok(ExtractStepOutcome {
                    claimed: true,
                    requeued: true,
                    ..Default::default()
                });
            }
            return Ok(ExtractStepOutcome::default());
        }
        let result = (|| {
            let vectors = if chunks[0].visual {
                chunks.iter().map(|chunk| {
                    let artifact = if chunk.kind == ws_extract::pdf::VISUAL_KIND {
                        Path::new(chunk.location.thumbnail_path.as_deref().ok_or_else(|| {
                            EmbedError::Protocol("PDF visual artifact missing".into())
                        })?)
                    } else {
                        path
                    };
                    image(artifact)?.ok_or_else(|| {
                        EmbedError::Unsupported("visual embedding unavailable".into())
                    })
                }).collect::<Result<Vec<_>, _>>()?
            } else {
                embed(&chunks.iter().map(|c| c.text.clone()).collect::<Vec<_>>())?
            };
            if vectors.len() != chunks.len() {
                return Err(EmbedError::Protocol("content embed arity".into()));
            }
            Ok(vectors)
        })();
        let mut store = self.store_lock();
        if store.content_revision(entry.id)? != revision
            || store.model_generation()? != generation
            || source_snapshot(path) != snapshot
            || !self.content_path_allowed(path)
            || self.indexing_pause_reason().is_some()
        {
            if store.model_generation()? == generation {
                store.release_content_vectors(&chunks, false)?;
            }
            if store.get_entry(entry.id)?.is_some()
                && store.content_revision(entry.id)? == revision
                && (source_snapshot(path) != snapshot
                    || !self.content_path_allowed(path)
                    || self.indexing_pause_reason().is_some())
            {
                store.invalidate_content_source(entry.id)?;
                store.enqueue_extract_job(entry.id, &chunks[0].job_kind)?;
            }
            return Ok(ExtractStepOutcome {
                claimed: true,
                requeued: true,
                ..Default::default()
            });
        }
        match result {
            Ok(vectors) => {
                match store.commit_content_vectors(&chunks, &vectors, revision, generation) {
                    Ok(true) => {
                        drop(store);
                        self.invalidate_vector_cache();
                        Ok(ExtractStepOutcome {
                            claimed: true,
                            segments_stored: chunks.len(),
                            ..Default::default()
                        })
                    }
                    Ok(false) => {
                        store.release_content_vectors(&chunks, false)?;
                        Ok(ExtractStepOutcome {
                            claimed: true,
                            requeued: true,
                            ..Default::default()
                        })
                    }
                    Err(StoreError::InvalidVector(_)) => {
                        store.release_content_vectors(&chunks, true)?;
                        Ok(ExtractStepOutcome {
                            claimed: true,
                            failed: true,
                            ..Default::default()
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            Err(error) => {
                if matches!(error, EmbedError::Unsupported(_)) && chunks.iter().all(|c| c.visual) {
                    store.complete_optional_visual(&chunks)?;
                    return Ok(ExtractStepOutcome {
                        claimed: true,
                        ..Default::default()
                    });
                }
                let unavailable = matches!(
                    error,
                    EmbedError::ModelNotAvailable(_) | EmbedError::Unsupported(_)
                );
                store.release_content_vectors(&chunks, !unavailable)?;
                Ok(ExtractStepOutcome {
                    claimed: !unavailable,
                    failed: !unavailable,
                    ..Default::default()
                })
            }
        }
    }

    /// Production entry for one content extraction step using the supervisor.
    pub fn extract_once_with_supervisor(
        &self,
        registry: &ExtractorRegistry,
        sup: &Arc<EmbedSupervisor>,
    ) -> Result<ExtractStepOutcome, StoreError> {
        self.extract_once_typed(
            registry,
            |texts| sup.embed_texts_bounded(texts, EMBED_JOB_TIMEOUT),
            |path| match sup.embed_image_bounded(path, EMBED_JOB_TIMEOUT) {
                Ok(vec) => Ok(Some(vec)),
                Err(crate::embed_client::EmbedError::Unsupported(_)) => Ok(None),
                Err(e) => Err(e),
            },
        )
    }

    /// Spawns the 2-thread background content extraction worker pool
    /// (`EXTRACT_WORKER_THREADS = 2`).
    pub fn start_extract_workers(
        self: &Arc<Self>,
        embed: Arc<EmbedSupervisor>,
        registry: Arc<ExtractorRegistry>,
    ) {
        for idx in 0..EXTRACT_WORKER_THREADS {
            let weak = Arc::downgrade(self);
            let sup = Arc::clone(&embed);
            let reg = Arc::clone(&registry);
            std::thread::Builder::new()
                .name(format!("winsearch-extract-{idx}"))
                .spawn(move || {
                    loop {
                        let Some(indexer) = weak.upgrade() else {
                            return;
                        };
                        if indexer.is_shutdown() {
                            return;
                        }
                        match indexer.extract_once_with_supervisor(&reg, &sup) {
                            Ok(out) if out.admission_unavailable => {
                                std::thread::sleep(std::time::Duration::from_millis(100));
                            }
                            Ok(out) if !out.claimed => {
                                indexer.wait_for_work();
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!(worker = idx, error = %e, "extract worker error");
                                std::thread::sleep(std::time::Duration::from_secs(1));
                            }
                        }
                    }
                })
                .ok();
        }
    }

    fn fail_extract_job_capped(
        store: &mut ws_index::IndexStore,
        job: &ExtractJob,
        name: &str,
    ) -> Result<bool, StoreError> {
        if job.attempts + 1 >= MAX_EMBED_ATTEMPTS {
            warn!(
                job = job.id,
                entry = job.entry_id,
                kind = %job.kind,
                name = %name,
                attempts = job.attempts + 1,
                "content extract job failing repeatedly; poison-pilled"
            );
            store.mark_extract_failed(job.id)?;
            Ok(true)
        } else {
            store.fail_extract_job(job.id)?;
            Ok(false)
        }
    }
}

fn source_snapshot(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(m) => format!(
            "{}|{}|{:?}|{:?}",
            path.display(),
            m.len(),
            m.modified().ok(),
            ws_platform::identity::file_identity(path)
        ),
        Err(_) => String::new(),
    }
}

fn clear_modality_artifacts(
    store: &mut ws_index::IndexStore,
    entry_id: i64,
    job_kind: &str,
) -> Result<(), StoreError> {
    store.delete_content_text(entry_id)?;
    store.remove_segments_for_entry_and_kind(entry_id, job_kind)?;
    if matches!(job_kind, SEGMENT_KIND_IMAGE | SEGMENT_KIND_PDF) {
        store.remove_segments_for_entry_and_kind(entry_id, SEGMENT_KIND_OCR)?;
    }
    if job_kind == SEGMENT_KIND_PDF {
        store.remove_segments_for_entry_and_kind(entry_id, ws_extract::pdf::VISUAL_KIND)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;
    use ws_index::model::{SearchRequest, VectorFilter, SEGMENT_KIND_DOC, SEGMENT_KIND_NAME};
    use ws_index::IndexStore;
    use ws_extract::SegmentDraft;

    struct TempTree(PathBuf);

    impl TempTree {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "winsearch-extract-test-{}-{:?}-{}",
                std::process::id(),
                std::thread::current().id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

    fn fake_embed(texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        Ok(ws_embed_protocol::stub_embed_texts(
            texts,
            ws_embed_protocol::STUB_DIM,
        ))
    }

    fn seeded_indexer(tree: &TempTree) -> Arc<Indexer> {
        let store = IndexStore::open_in_memory().unwrap();
        let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        indexer
    }

    fn process_registry(cache: &TempTree) -> ExtractorRegistry {
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/winsearch-extract.exe");
        assert!(worker.is_file(), "build winsearch-extract before integration tests");
        let mut registry = ExtractorRegistry::production();
        registry.worker = Some(ExtractClient::new(worker, cache.0.clone()));
        registry
    }

    fn pdf_fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../..")
            .join("artifacts/pdf-fixtures")
            .join(name)
    }

    fn pdf_native_replacement_clears_derived_vectors(replacement: Option<&str>) {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let path = tree.0.join("snapshot.pdf");
        fs::copy(pdf_fixture("scanned.pdf"), &path).unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        indexer
            .extract_once_typed(
                &registry,
                |t| Ok(t.iter().map(|_| vec![1.0, 0.0]).collect()),
                |_| Ok(Some(vec![0.0, 1.0])),
            )
            .unwrap();
        indexer
            .extract_once_typed(
                &registry,
                |t| Ok(t.iter().map(|_| vec![1.0, 0.0]).collect()),
                |_| Ok(Some(vec![0.0, 1.0])),
            )
            .unwrap();
        let derived = VectorFilter {
            kinds: vec![SEGMENT_KIND_OCR.into(), ws_extract::pdf::VISUAL_KIND.into()],
            entry_ids: vec![entry.id],
        };
        // Populate the production semantic cache with both actual old page modalities.
        let old = indexer
            .search_vectors(&[1.0, 1.0], 10, Some(&derived))
            .unwrap();
        assert_eq!(old.len(), 2);
        assert!(old
            .iter()
            .any(|s| s.segment.kind == SEGMENT_KIND_OCR && s.segment.text.contains("INVOICE")));
        assert!(old
            .iter()
            .any(|s| s.segment.kind == ws_extract::pdf::VISUAL_KIND));
        let original = indexer
            .store_lock()
            .content_progress(entry.id, "pdf")
            .unwrap()
            .unwrap();
        assert!(original.complete);
        if let Some(name) = replacement {
            fs::copy(pdf_fixture(name), &path).unwrap();
        }
        // No watcher notification or invalidate_content_source: exercise snapshot/requeue cleanup.
        indexer
            .store_lock()
            .enqueue_extract_job(entry.id, "pdf")
            .unwrap();
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| {
                    Err(EmbedError::ModelNotAvailable(
                        "replacement exact content only".into(),
                    ))
                },
                |_| panic!("replacement model unavailable"),
            )
            .unwrap();
        assert!(step.claimed);
        assert!(
            indexer
                .search_vectors(&[1.0, 1.0], 10, Some(&derived))
                .unwrap()
                .is_empty(),
            "old PDF OCR/visual semantic/cache matches survived {replacement:?}"
        );
        assert!(
            indexer
                .store_lock()
                .list_segments_for_entry(entry.id)
                .unwrap()
                .iter()
                .all(|s| s.kind != SEGMENT_KIND_OCR && s.kind != ws_extract::pdf::VISUAL_KIND),
            "old PDF-derived rows survived {replacement:?}"
        );
        if replacement == Some("corrupt.pdf") {
            assert!(step.failed);
        } else {
            let current = indexer
                .store_lock()
                .content_progress(entry.id, "pdf")
                .unwrap()
                .unwrap();
            assert!(current.revision > original.revision);
            assert_eq!(current.source == original.source, replacement.is_none());
        }
        if replacement.is_some() {
            assert!(indexer
                .search(&SearchRequest {
                    text: "INVOICE".into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn pdf_native_snapshot_replacement_clears_derived_vectors() {
        for name in ["empty.pdf", "fyrtio-Å界.pdf"] {
            pdf_native_replacement_clears_derived_vectors(Some(name));
        }
    }

    #[test]
    fn pdf_native_completed_requeue_clears_derived_vectors() {
        pdf_native_replacement_clears_derived_vectors(None);
    }

    #[test]
    fn pdf_native_failed_replacement_clears_derived_vectors() {
        pdf_native_replacement_clears_derived_vectors(Some("corrupt.pdf"));
    }

    #[test]
    fn pdf_native_unchanged_requeue_missing_dependency_preserves_content() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let binaries = TempTree::new();
        let path = tree.0.join("unchanged.pdf");
        fs::copy(pdf_fixture("scanned.pdf"), &path).unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        for _ in 0..2 {
            indexer
                .extract_once_typed(
                    &registry,
                    |t| Ok(t.iter().map(|_| vec![1.0, 0.0]).collect()),
                    |_| Ok(Some(vec![0.0, 1.0])),
                )
                .unwrap();
        }
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        let before = indexer.store_lock().list_content_chunks(entry.id).unwrap();
        let vectors = indexer.search_vectors(&[1.0, 1.0], 10, None).unwrap();
        assert_eq!(vectors.len(), 2);
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/winsearch-extract.exe");
        let no_dll = binaries.0.join("winsearch-extract.exe");
        fs::copy(worker, &no_dll).unwrap();
        let mut missing = ExtractorRegistry::production();
        missing.worker = Some(ExtractClient::new(no_dll, cache.0.clone()));
        indexer
            .store_lock()
            .enqueue_extract_job(entry.id, "pdf")
            .unwrap();
        let step = indexer
            .extract_once_typed(
                &missing,
                |_| panic!("missing DLL cannot embed"),
                |_| panic!("missing DLL cannot render"),
            )
            .unwrap();
        assert!(step.failed);
        assert_eq!(
            indexer
                .store_lock()
                .list_content_chunks(entry.id)
                .unwrap()
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>(),
            before.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        let retained = indexer.search_vectors(&[1.0, 1.0], 10, None).unwrap();
        assert_eq!(
            retained.iter().map(|s| s.segment.id).collect::<Vec<_>>(),
            vectors.iter().map(|s| s.segment.id).collect::<Vec<_>>()
        );
        let hits = indexer
            .search(&SearchRequest {
                text: "INVOICE".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].matches[0].page, Some(1));
        assert_eq!(
            indexer
                .store_lock()
                .get_entry(entry.id)
                .unwrap()
                .unwrap()
                .thumbnail_path,
            entry.thumbnail_path
        );
    }

    #[test]
    fn pdf_native_durable_restart_exact_pages_primary_thumbnail_and_policy() {
        let tree = TempTree::new();
        let data = TempTree::new();
        let cache = TempTree::new();
        let path = tree.0.join("unicode-Å界.pdf");
        fs::copy(pdf_fixture("fyrtio-Å界.pdf"), &path).unwrap();
        let db = data.0.join("index.db");
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let registry = process_registry(&cache);
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        assert_eq!(step.fts_chunks_indexed, 30);
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        let first = indexer.store_lock().list_content_chunks(entry.id).unwrap();
        assert_eq!(first.len(), 30);
        assert_eq!(
            entry.thumbnail_path, first[0].location.thumbnail_path,
            "primary page thumbnail must be first page"
        );
        assert!(indexer
            .search(&SearchRequest {
                text: "PDF_MARKER_PAGE_35".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        drop(indexer);
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        assert_eq!(step.fts_chunks_indexed, 10);
        let chunks = indexer.store_lock().list_content_chunks(entry.id).unwrap();
        assert_eq!(chunks.len(), 40);
        assert_eq!(
            chunks[..30].iter().map(|c| c.id).collect::<Vec<_>>(),
            first.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        assert_eq!(
            chunks
                .iter()
                .map(|c| c.chunk_index)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            40
        );
        let hits = indexer
            .search(&SearchRequest {
                text: "PDF_MARKER_PAGE_35".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].matches[0].page, Some(35));
        assert!(hits[0].matches[0].excerpt.contains("PDF_MARKER_PAGE_35"));
        let matches = indexer
            .store_lock()
            .content_matches_page(entry.id, &["sharedpdfneedle".into()], 32, 32)
            .unwrap();
        assert_eq!(matches.len(), 8);
        assert_eq!(matches[0].page, Some(33));
        assert!(chunks.iter().all(|c| c
            .location
            .thumbnail_path
            .as_ref()
            .is_some_and(|p| Path::new(p).is_file())));
        let mut config = indexer.get_indexing_config().unwrap();
        config.roots.clear();
        indexer.set_indexing_config(&config).unwrap();
        assert!(indexer
            .search(&SearchRequest {
                text: "PDF_MARKER_PAGE_35".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn pdf_native_later_failure_and_missing_dependency_preserve_committed_pages() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let binaries = TempTree::new();
        let path = tree.0.join("partial.pdf");
        fs::copy(pdf_fixture("later-page-error.pdf"), &path).unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        let no_model = |_: &[String]| Err(EmbedError::ModelNotAvailable("no model".into()));
        assert_eq!(
            indexer
                .extract_once_typed(&registry, no_model, |_| Ok(None))
                .unwrap()
                .fts_chunks_indexed,
            30
        );
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        assert!(
            indexer
                .extract_once_typed(&registry, no_model, |_| Ok(None))
                .unwrap()
                .failed
        );
        assert_eq!(
            indexer
                .store_lock()
                .list_content_chunks(entry.id)
                .unwrap()
                .len(),
            30
        );
        assert!(
            !indexer
                .store_lock()
                .content_progress(entry.id, "pdf")
                .unwrap()
                .unwrap()
                .complete
        );
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/winsearch-extract.exe");
        let no_dll = binaries.0.join("winsearch-extract.exe");
        fs::copy(worker, &no_dll).unwrap();
        let mut missing = ExtractorRegistry::production();
        missing.worker = Some(ExtractClient::new(no_dll, cache.0.clone()));
        assert!(
            indexer
                .extract_once_typed(&missing, no_model, |_| Ok(None))
                .unwrap()
                .failed
        );
        let hits = indexer
            .search(&SearchRequest {
                text: "retainedpagetoken1".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].matches[0].page, Some(1));
        let mut revoked = indexer.get_indexing_config().unwrap();
        revoked.roots.clear();
        indexer.set_indexing_config(&revoked).unwrap();
        assert!(
            !indexer
                .extract_once_typed(
                    &missing,
                    |_| panic!("revoked vectors"),
                    |_| panic!("revoked visual")
                )
                .unwrap()
                .claimed
        );
        assert!(indexer
            .search(&SearchRequest {
                text: "retainedpagetoken1".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn pdf_native_ocr_exact_and_visual_embedding_consumes_rendered_png() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let path = tree.0.join("scan.pdf");
        fs::copy(pdf_fixture("scanned-two.pdf"), &path).unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        indexer
            .extract_once_typed(
                &registry,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        let hits = indexer
            .search(&SearchRequest {
                text: "INVOICE".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].matches[0].page, Some(1));
        assert_eq!(hits[0].matches[1].page, Some(2));
        assert!(hits[0].matches[0].excerpt.contains("INVOICE"));
        indexer
            .extract_once_typed(
                &registry,
                |t| Ok(t.iter().map(|_| vec![1.0, 0.0]).collect()),
                |_| panic!("text stage"),
            )
            .unwrap();
        let called = std::cell::Cell::new(0);
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| panic!("visual stage"),
                |artifact| {
                    called.set(called.get() + 1);
                    assert_eq!(artifact.extension().unwrap(), "png");
                    assert_ne!(artifact, &path);
                    let raster = ws_platform::wic::decode_image_wic(artifact, 2048).unwrap();
                    assert!(raster.bgra8.chunks_exact(4).any(|p| p[0] < 100));
                    Ok(Some(vec![0.0, 1.0]))
                },
            )
            .unwrap();
        assert_eq!(called.get(), 2);
        assert_eq!(step.segments_stored, 2);
        let visual = indexer.search_vectors(&[0.0, 1.0], 5, None).unwrap();
        assert!(visual.iter().any(|s| s.segment.page == Some(1)));
    }

    #[test]
    fn pdf_native_modified_while_parsing_cannot_commit_and_new_primary_replaces_old() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let path = tree.0.join("changing.pdf");
        fs::copy(pdf_fixture("fyrtio-Å界.pdf"), &path).unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        let changed = path.clone();
        let scratch = cache.0.join("extract");
        let blank = pdf_fixture("empty.pdf");
        let mutation = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !scratch.is_dir() || fs::read_dir(&scratch).unwrap().next().is_none() {
                assert!(std::time::Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(5));
            }
            std::thread::sleep(Duration::from_millis(50));
            fs::copy(blank, changed).unwrap();
        });
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| panic!("stale batch cannot embed"),
                |_| panic!("stale visual"),
            )
            .unwrap();
        mutation.join().unwrap();
        assert!(step.requeued);
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        assert!(indexer
            .store_lock()
            .list_content_chunks(entry.id)
            .unwrap()
            .is_empty());
        let step = indexer
            .extract_once_typed(
                &registry,
                |_| panic!("blank text"),
                |_| panic!("blank visual"),
            )
            .unwrap();
        assert_eq!(step.fts_chunks_indexed, 0);
        let blank_primary = indexer
            .store_lock()
            .get_entry(entry.id)
            .unwrap()
            .unwrap()
            .thumbnail_path
            .unwrap();
        fs::copy(pdf_fixture("scanned.pdf"), &path).unwrap();
        indexer.handle_event(
            entry.root_id,
            crate::watcher::WatchEvent::Modified(path.clone()),
        );
        indexer
            .extract_once_typed(
                &registry,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        let primary = indexer
            .store_lock()
            .get_entry(entry.id)
            .unwrap()
            .unwrap()
            .thumbnail_path
            .unwrap();
        assert_ne!(primary, blank_primary);
        let raster = ws_platform::wic::decode_image_wic(Path::new(&primary), 2048).unwrap();
        assert!(raster.bgra8.chunks_exact(4).any(|p| p[0] < 100));
    }

    #[test]
    fn pdf_native_huge_page_restart_has_stable_chunk_ordinals() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        let data = TempTree::new();
        let path = tree.0.join("large.pdf");
        fs::copy(pdf_fixture("huge-text.pdf"), &path).unwrap();
        let db = data.0.join("index.db");
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let registry = process_registry(&cache);
        let no_model = |_: &[String]| Err(EmbedError::ModelNotAvailable("no model".into()));
        assert_eq!(
            indexer
                .extract_once_typed(&registry, no_model, |_| Ok(None))
                .unwrap()
                .fts_chunks_indexed,
            256
        );
        let entry = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap();
        let first = indexer.store_lock().list_content_chunks(entry.id).unwrap();
        assert_eq!(
            indexer
                .store_lock()
                .content_progress(entry.id, "pdf")
                .unwrap()
                .unwrap()
                .cursor,
            256
        );
        drop(indexer);
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        indexer
            .extract_once_typed(&registry, no_model, |_| Ok(None))
            .unwrap();
        let chunks = indexer.store_lock().list_content_chunks(entry.id).unwrap();
        assert!(chunks.len() > 256);
        assert_eq!(
            chunks.iter().map(|c| c.chunk_index).collect::<Vec<_>>(),
            (0..chunks.len() as i64).collect::<Vec<_>>()
        );
        assert_eq!(
            chunks[..256].iter().map(|c| c.id).collect::<Vec<_>>(),
            first.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        assert!(
            indexer
                .store_lock()
                .content_progress(entry.id, "pdf")
                .unwrap()
                .unwrap()
                .complete
        );
        let hits = indexer
            .search(&SearchRequest {
                text: "finalhugepageneedle".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].matches[0].page, Some(1));
    }

    #[test]
    fn task2_no_model_keeps_exact_content() {
        let tree = TempTree::new();
        fs::write(
            tree.0.join("invoice.txt"),
            "Header\nWINSEARCH_INVOICE_42 is outstanding",
        )
        .unwrap();
        let indexer = seeded_indexer(&tree);
        indexer
            .extract_once_with(&ExtractorRegistry::default(), |_| {
                Err("model unavailable".into())
            })
            .unwrap();
        let hits = indexer
            .search(&SearchRequest {
                text: "WINSEARCH_INVOICE_42".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1, "model failure discarded exact content");
        assert!(hits[0].matches[0].excerpt.contains("WINSEARCH_INVOICE_42"));
        assert_eq!(hits[0].matches[0].line, Some(2));
        assert!(indexer
            .search_vectors(&[1.0, 0.0], 10, None)
            .unwrap()
            .is_empty());
    }

    fn same_second_write(path: &Path, text: &str) {
        fs::write(path, text).unwrap();
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(
            fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(1_750_000_000)),
        )
        .unwrap();
    }

    #[test]
    fn task2_same_size_same_second_modified_requeues() {
        let tree = TempTree::new();
        let path = tree.0.join("mutable.txt");
        same_second_write(&path, "uniqueoldtoken statement");
        let indexer = seeded_indexer(&tree);
        let reg = ExtractorRegistry::default();
        indexer.extract_once_with(&reg, fake_embed).unwrap();
        let root = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap()
            .root_id;
        same_second_write(&path, "uniquenewtoken statement");
        indexer.handle_event(root, crate::watcher::WatchEvent::Modified(path));
        let step = indexer.extract_once_with(&reg, fake_embed).unwrap();
        assert!(
            step.claimed,
            "same metadata Modified never queued replacement"
        );
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "uniquenewtoken".into(),
                    limit: 5,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        assert!(indexer
            .search(&SearchRequest {
                text: "uniqueoldtoken".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn task2_inflight_same_metadata_modified_rejects_old_batch() {
        let tree = TempTree::new();
        let path = tree.0.join("inflight.txt");
        same_second_write(&path, "uniqueoldtoken statement");
        let indexer = seeded_indexer(&tree);
        let root = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap()
            .root_id;
        let step = indexer
            .extract_once_with(&ExtractorRegistry::default(), |texts| {
                same_second_write(&path, "uniquenewtoken statement");
                indexer.handle_event(root, crate::watcher::WatchEvent::Modified(path.clone()));
                fake_embed(texts)
            })
            .unwrap();
        assert!(
            step.requeued,
            "old batch committed over actual Modified event"
        );
        assert!(indexer
            .search(&SearchRequest {
                text: "uniqueoldtoken".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn task2_long_document_first_batch_is_thirty() {
        let tree = TempTree::new();
        // A single long token gives deterministic 1200-char windows/1020 stride.
        fs::write(tree.0.join("long.txt"), "a".repeat(1200 + 74 * 1020)).unwrap();
        let indexer = seeded_indexer(&tree);
        let step = indexer
            .extract_once_with(&ExtractorRegistry::default(), fake_embed)
            .unwrap();
        assert_eq!(
            step.fts_chunks_indexed, 30,
            "long document was not bounded to 30"
        );
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 1);
    }

    #[test]
    fn task2_native_worker_seventy_five_chunks_resume_after_restart() {
        let tree = TempTree::new();
        let data = TempTree::new();
        let cache = TempTree::new();
        let path = tree.0.join("long.txt");
        fs::write(&path, "a".repeat(1200 + 74 * 1020)).unwrap();
        let db = data.0.join("index.db");
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let reg = process_registry(&cache);
        let first = indexer
            .extract_once_typed(
                &reg,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        assert_eq!(first.fts_chunks_indexed, 30);
        let id = indexer
            .store_lock()
            .find_by_path(&path.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let first_chunks = indexer.store_lock().list_content_chunks(id).unwrap();
        assert_eq!(first_chunks.len(), 30);
        assert_eq!(
            indexer
                .store_lock()
                .content_progress(id, "doc")
                .unwrap()
                .unwrap()
                .cursor,
            30
        );
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "aaaaa".into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        drop(indexer);
        let indexer = Indexer::open(IndexStore::open(&db).unwrap(), &[tree.0.clone()]).unwrap();
        // Fresh short files must jump ahead of the continuation.
        fs::write(tree.0.join("short.txt"), "newshortneedle").unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let short = indexer
            .extract_once_typed(
                &reg,
                |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                |_| Ok(None),
            )
            .unwrap();
        assert_eq!(short.fts_chunks_indexed, 1);
        for expected in [30, 15] {
            let step = indexer
                .extract_once_typed(
                    &reg,
                    |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                    |_| Ok(None),
                )
                .unwrap();
            assert_eq!(step.fts_chunks_indexed, expected);
        }
        let chunks = indexer.store_lock().list_content_chunks(id).unwrap();
        assert_eq!(chunks.len(), 75);
        assert_eq!(
            chunks.iter().map(|c| c.chunk_index).collect::<Vec<_>>(),
            (0..75).collect::<Vec<_>>()
        );
        assert_eq!(
            chunks[..30].iter().map(|c| c.id).collect::<Vec<_>>(),
            first_chunks.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        assert!(chunks.iter().all(|c| c.attempts == 0));
        assert!(
            indexer
                .store_lock()
                .content_progress(id, "doc")
                .unwrap()
                .unwrap()
                .complete
        );
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);
    }

    #[test]
    fn task2_native_worker_beyond_legacy_cap_reaches_final_sentinel() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        fs::write(
            tree.0.join("beyond.txt"),
            format!(
                "{}\nfinalsentinelafterlegacycap",
                "a".repeat(1200 + 299 * 1020)
            ),
        )
        .unwrap();
        let indexer = seeded_indexer(&tree);
        let reg = process_registry(&cache);
        let id = indexer
            .store_lock()
            .find_by_path(&tree.0.join("beyond.txt").to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        for _ in 0..20 {
            if indexer.content_extract_queue_depth().unwrap() == 0 {
                break;
            }
            let step = indexer
                .extract_once_typed(
                    &reg,
                    |_| Err(EmbedError::ModelNotAvailable("no model".into())),
                    |_| Ok(None),
                )
                .unwrap();
            assert!(step.fts_chunks_indexed <= 30);
        }
        let chunks = indexer.store_lock().list_content_chunks(id).unwrap();
        assert!(chunks.len() > 256);
        assert!(
            indexer
                .store_lock()
                .content_progress(id, "doc")
                .unwrap()
                .unwrap()
                .complete
        );
        let hits = indexer
            .search(&SearchRequest {
                text: "finalsentinelafterlegacycap".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].matches[0]
            .excerpt
            .contains("finalsentinelafterlegacycap"));
    }

    #[test]
    fn task2_native_modified_same_second_and_inflight() {
        for inflight in [false, true] {
            let tree = TempTree::new();
            let path = tree.0.join("native.txt");
            same_second_write(&path, "uniqueoldtoken statement");
            let indexer = seeded_indexer(&tree);
            let root = indexer
                .store_lock()
                .find_by_path(&path.to_string_lossy())
                .unwrap()
                .unwrap()
                .root_id;
            let reg = ExtractorRegistry::default();
            if !inflight {
                indexer.extract_once_with(&reg, fake_embed).unwrap();
            }
            let (tx, rx) = std::sync::mpsc::channel();
            let watcher = crate::watcher::spawn(&tree.0, move |event| {
                let _ = tx.send(event);
            })
            .unwrap();
            std::thread::sleep(Duration::from_millis(100));
            let modify = || {
                same_second_write(&path, "uniquenewtoken statement");
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let event = rx
                        .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                        .expect("native Modified event");
                    if matches!(&event,crate::watcher::WatchEvent::Modified(p) if p==&path) {
                        indexer.handle_event(root, event);
                        break;
                    }
                }
            };
            if inflight {
                let step = indexer
                    .extract_once_with(&reg, |texts| {
                        modify();
                        fake_embed(texts)
                    })
                    .unwrap();
                assert!(step.requeued);
            } else {
                modify();
            }
            watcher.stop();
            let step = indexer.extract_once_with(&reg, fake_embed).unwrap();
            assert!(step.claimed);
            assert_eq!(
                indexer
                    .search(&SearchRequest {
                        text: "uniquenewtoken".into(),
                        limit: 1,
                        ..Default::default()
                    })
                    .unwrap()
                    .len(),
                1
            );
            assert!(indexer
                .search(&SearchRequest {
                    text: "uniqueoldtoken".into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn real_worker_production_registry_indexes_generated_text_without_inline_provider() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        fs::write(tree.0.join("actual.txt"), "uniqueworkerfixture actual generated text").unwrap();
        let indexer = seeded_indexer(&tree);
        let registry = process_registry(&cache);
        assert!(registry.extractors.is_empty());
        let result = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert_eq!(result.status, Some(ExtractionStatus::Extracted));
        assert_eq!(result.segments_stored, 1);
        let hits = indexer.search(&SearchRequest { text: "uniqueworkerfixture".into(), limit: 10, ..Default::default() }).unwrap();
        assert_eq!(hits.len(), 1);
        assert!(fs::read_dir(cache.0.join("extract")).unwrap().next().is_none());
    }

    fn retained_cleanup_preserves_unrelated_jobs(after_claim: bool) {
        use crate::extract_client::tests::cleanup_trigger;
        use std::sync::Mutex;
        let tree = TempTree::new();
        let cache = TempTree::new();
        let words = ["unrelatedcleanupalpha", "unrelatedcleanupbeta"];
        let paths: Vec<_> = words.iter().enumerate().map(|(i, word)| {
            let path = tree.0.join(format!("fixture{i}.txt"));
            fs::write(&path, word).unwrap(); path
        }).collect();
        let indexer = seeded_indexer(&tree);
        let initial = ExtractorRegistry::default();
        while indexer.content_extract_queue_depth().unwrap() > 0 {
            indexer.extract_once_with(&initial, fake_embed).unwrap();
        }
        let before: Vec<_> = paths.iter().map(|path| {
            let mut store = indexer.store_lock();
            let entry = store.find_by_path(&path.to_string_lossy()).unwrap().unwrap();
            let segments = store.list_segments_for_entry(entry.id).unwrap();
            store.enqueue_extract_job(entry.id, SEGMENT_KIND_DOC).unwrap();
            let job = store.find_extract_job(entry.id, SEGMENT_KIND_DOC).unwrap().unwrap();
            (job, segments)
        }).collect();
        let mut registry = process_registry(&cache);
        let trigger = cleanup_trigger(registry.worker.as_ref().unwrap(), cache.0.clone());
        let held = Arc::new(Mutex::new(None));
        if after_claim {
            let slot = Arc::clone(&held);
            registry.set_budget_factory(move |_| {
                let mut fixture = slot.lock().unwrap();
                if fixture.is_none() { *fixture = Some(trigger()); }
                ExtractBudget::default()
            });
        } else {
            *held.lock().unwrap() = Some(trigger());
        }
        for step in 0..8 {
            let outcome = indexer.extract_once_with(&registry, fake_embed).unwrap();
            assert!(outcome.admission_unavailable);
            assert!(!outcome.failed && !outcome.poisoned && outcome.status.is_none());
            assert_eq!(outcome.claimed, after_claim && step == 0);
            assert_eq!(outcome.requeued, after_claim && step == 0);
        }
        for (job, segments) in &before {
            let store = indexer.store_lock();
            assert_eq!(store.get_extract_job(job.id).unwrap().as_ref(), Some(job), "admission unavailability consumed unrelated job state/attempts");
            assert_eq!(&store.list_segments_for_entry(job.entry_id).unwrap(), segments, "admission unavailability erased existing vectors");
        }
        for word in words {
            assert_eq!(indexer.search(&SearchRequest { text: word.into(), limit: 10, ..Default::default() }).unwrap().len(), 1, "admission unavailability erased existing FTS artifacts");
        }
        registry.set_budget_factory(|_| ExtractBudget::default());
        drop(held.lock().unwrap().take());
        for _ in 0..2 {
            let result = indexer.extract_once_with(&registry, fake_embed).unwrap();
            assert_eq!(result.status, Some(ExtractionStatus::Extracted));
            assert_eq!(result.segments_stored, 1);
        }
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);
    }

    #[test]
    fn native_retained_cleanup_before_claim_preserves_unrelated_queue_and_artifacts() {
        retained_cleanup_preserves_unrelated_jobs(false);
    }

    #[test]
    fn native_retained_cleanup_after_claim_preserves_unrelated_queue_and_artifacts() {
        retained_cleanup_preserves_unrelated_jobs(true);
    }

    #[test]
    fn real_worker_missing_binary_cannot_fall_back_to_registered_inline_extractor() {
        let tree = TempTree::new();
        let cache = TempTree::new();
        fs::write(tree.0.join("actual.txt"), "uniquemissingworkerfixture").unwrap();
        let indexer = seeded_indexer(&tree);
        let mut registry = ExtractorRegistry::default();
        registry.worker = Some(ExtractClient::new(cache.0.join("missing.exe"), cache.0.clone()));
        let result = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert!(matches!(result.status, Some(ExtractionStatus::Failed(_))));
        assert_eq!(result.segments_stored, 0);
        assert!(indexer.search(&SearchRequest { text: "uniquemissingworkerfixture".into(), limit: 10, ..Default::default() }).unwrap().is_empty());
    }

    #[test]
    fn real_worker_discards_promoted_artifacts_when_pause_or_source_changes_before_commit() {
        for pause in [true, false] {
            let tree = TempTree::new();
            let cache = TempTree::new();
            let path = tree.0.join("fixture.png");
            ws_platform::wic::write_bgra_thumbnail_png(&vec![140; 32 * 32 * 4], 32, 32, 32, &path).unwrap();
            let indexer = seeded_indexer(&tree);
            let registry = process_registry(&cache);
            let result = indexer.extract_once_with_media(&registry, fake_embed, |_| {
                if pause {
                    let mut config = indexer.get_indexing_config().unwrap();
                    config.paused = true;
                    indexer.set_indexing_config(&config).unwrap();
                } else {
                    fs::write(&path, "source changed while embedding").unwrap();
                }
                Ok(None)
            }).unwrap();
            assert!(result.requeued);
            assert_eq!(result.segments_stored, 0);
            assert!(fs::read_dir(cache.0.join("extract")).unwrap().next().is_none());
            assert!(fs::read_dir(cache.0.join("thumbs")).unwrap().next().is_none());
        }
    }

    #[test]
    fn crawl_enqueues_content_jobs_and_extracts_chunks_into_fts_and_vectors() {
        let tree = TempTree::new();
        let body = "Confidential indemnification clause in the master service agreement. "
            .repeat(40);
        fs::write(tree.0.join("agreement.txt"), &body).unwrap();

        let indexer = seeded_indexer(&tree);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 1);

        let registry = ExtractorRegistry::default();
        let out = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert!(out.claimed);
        assert_eq!(out.status, Some(ExtractionStatus::Extracted));
        assert!(out.segments_stored >= 2, "expected multiple overlapping chunks, got {}", out.segments_stored);
        assert_eq!(out.fts_chunks_indexed, out.segments_stored);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);

        // 1. Exact search finds the file via `content_fts` on a word not in the filename.
        let hits = indexer
            .search(&SearchRequest {
                text: "indemnification".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "agreement.txt");
        assert_eq!(hits[0].explanation, "Matches extracted content");

        // 2. Vector search finds the `doc` segments.
        let first_seg = {
            let store = indexer.store_lock();
            let segs = store.list_segments_for_entry(hits[0].entry_id).unwrap();
            let doc_segs: Vec<_> = segs.into_iter().filter(|s| s.kind == SEGMENT_KIND_DOC).collect();
            assert_eq!(doc_segs.len(), out.segments_stored);
            assert_eq!(doc_segs[0].line, Some(1));
            doc_segs[0].clone()
        };
        let qvec = ws_embed_protocol::stub_vector(&first_seg.text, ws_embed_protocol::STUB_DIM);
        let vhits = indexer
            .search_vectors(&qvec, 5, Some(&VectorFilter::for_kind(SEGMENT_KIND_DOC)))
            .unwrap();
        assert!(!vhits.is_empty());
        assert_eq!(vhits[0].segment.entry_id, hits[0].entry_id);
    }

    #[test]
    fn queue_drains_small_files_first_then_recent_first() {
        let tree = TempTree::new();
        // Create a large file (~100 KiB) and a small file (~100 bytes).
        let big_path = tree.0.join("big.txt");
        let small_path = tree.0.join("small.txt");
        fs::write(&big_path, "large content word ".repeat(6000)).unwrap();
        fs::write(&small_path, "tiny content word").unwrap();

        let indexer = seeded_indexer(&tree);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 2);

        // Peek claim order via `extract_once_with`: first step must process `small.txt` (1 chunk),
        // second step must process `big.txt` (many chunks).
        let registry = ExtractorRegistry::default();
        let first = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert_eq!(first.segments_stored, 1, "small file should be extracted first");

        let second = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert!(
            second.segments_stored > 1,
            "large file should be extracted second"
        );
        assert_eq!(second.fts_chunks_indexed, 30);
        while indexer.content_extract_queue_depth().unwrap() > 0 {
            let step = indexer.extract_once_with(&registry, fake_embed).unwrap();
            assert!(step.fts_chunks_indexed <= 30);
        }
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);
    }

    #[test]
    fn crash_resume_releases_claimed_content_jobs_on_reopen() {
        let tree = TempTree::new();
        let db_dir = TempTree::new();
        fs::write(tree.0.join("notes.txt"), "crash recovery test content").unwrap();
        let db_path = db_dir.0.join("index.db");

        {
            let store = IndexStore::open(&db_path).unwrap();
            let indexer = Indexer::open(store, &[tree.0.clone()]).unwrap();
            indexer.enqueue_initial_scan().unwrap();
            while indexer.scan_available(64) > 0 {}

            // Simulate a crash while a worker holds a claimed content job.
            let claimed = indexer
                .store_lock()
                .claim_content_extract_job()
                .unwrap()
                .expect("content job must be queued");
            assert!(claimed.claimed);
            assert_eq!(claimed.kind, SEGMENT_KIND_DOC);
            // No more unclaimed content jobs remain before crash recovery.
            assert!(indexer.store_lock().claim_content_extract_job().unwrap().is_none());
        }

        // Reopen Indexer on the same DB: `Indexer::open` releases claims, so the
        // interrupted content job is reclaimed and extracted cleanly.
        let store2 = IndexStore::open(&db_path).unwrap();
        let indexer2 = Indexer::open(store2, &[tree.0.clone()]).unwrap();
        let registry = ExtractorRegistry::default();
        let out = indexer2.extract_once_with(&registry, fake_embed).unwrap();
        assert!(out.claimed);
        assert_eq!(out.status, Some(ExtractionStatus::Extracted));
        assert_eq!(out.segments_stored, 1);
        assert_eq!(indexer2.content_extract_queue_depth().unwrap(), 0);
        // And the name job for notes.txt is still intact for the name embedder.
        assert!(indexer2.embed_queue_depth().unwrap() >= 1);
        let _ = SEGMENT_KIND_NAME;
    }

    #[test]
    fn budget_abort_marks_oversized_or_expired_job_failed_without_blocking_queue() {
        let tree = TempTree::new();
        fs::write(tree.0.join("huge.txt"), "x".repeat(4096)).unwrap();
        fs::write(tree.0.join("ok.txt"), "small valid document").unwrap();

        let indexer = seeded_indexer(&tree);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 2);

        // Configure a budget that allows <= 512 bytes: `ok.txt` (smaller, higher
        // priority) succeeds; `huge.txt` (4096 bytes) aborts with FileTooLarge
        // and is marked failed so the queue drains to 0.
        let mut registry = ExtractorRegistry::default();
        registry.set_budget_factory(|_| ExtractBudget::new(512, Duration::from_secs(5), 16));

        let first = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert_eq!(first.status, Some(ExtractionStatus::Extracted));
        assert_eq!(first.segments_stored, 1);

        let second = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert!(second.claimed);
        assert!(second.poisoned, "oversized file must be marked failed (no infinite retry)");
        assert!(matches!(second.status, Some(ExtractionStatus::Failed(_))));
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);

        // Also test an already-expired deadline budget aborts cleanly.
        fs::write(tree.0.join("late.txt"), "deadline test").unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}

        let mut expired_reg = ExtractorRegistry::default();
        expired_reg.set_budget_factory(|_| ExtractBudget {
            max_bytes: 1024 * 1024,
            deadline: std::time::Instant::now() - Duration::from_millis(5),
            max_segments: 16,
        });
        let third = indexer.extract_once_with(&expired_reg, fake_embed).unwrap();
        assert!(third.claimed);
        assert!(third.poisoned);
        assert!(matches!(third.status, Some(ExtractionStatus::Failed(_))));
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 0);
    }

    #[test]
    fn numeric_heavy_csv_indexes_fts_without_storing_vectors() {
        let tree = TempTree::new();
        let mut csv = String::from("invoice_id,date,subtotal,tax,total\n");
        for i in 1..=25 {
            csv.push_str(&format!(
                "{},2026-10-{:02},{:.2},{:.2},{:.2}\n",
                98000 + i,
                (i % 28) + 1,
                i as f64 * 100.0,
                i as f64 * 19.0,
                i as f64 * 119.0
            ));
        }
        fs::write(tree.0.join("invoices.csv"), &csv).unwrap();

        let indexer = seeded_indexer(&tree);
        let registry = ExtractorRegistry::default();
        let out = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert!(out.claimed);
        assert_eq!(out.status, Some(ExtractionStatus::ExactOnly));
        assert_eq!(out.segments_stored, 0, "numeric CSV must not store vectors");
        assert!(out.fts_chunks_indexed >= 1, "numeric CSV must still index in FTS");

        // Exact FTS search finds the invoice number inside the CSV.
        let hits = indexer
            .search(&SearchRequest {
                text: "98017".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "invoices.csv");
        assert_eq!(hits[0].explanation, "Matches extracted content");

        // And zero `doc` vectors were stored for the entry.
        let store = indexer.store_lock();
        let segs = store.list_segments_for_entry(hits[0].entry_id).unwrap();
        assert!(segs.iter().all(|s| s.kind != SEGMENT_KIND_DOC));
    }

    #[test]
    fn image_exif_ocr_screenshot_semantic_and_thumbnail_end_to_end() {
        use ws_extract::{OcrProvider, OcrResultData, OcrWordBox};
        use ws_index::model::{HitKind, SEGMENT_KIND_OCR};
        use ws_platform::wic::write_bgra_thumbnail_png;

        struct MapOcrProvider;
        impl OcrProvider for MapOcrProvider {
            fn recognize_bgra8(
                &self,
                _bgra8: &[u8],
                width: u32,
                _height: u32,
                _budget: &ExtractBudget,
            ) -> Result<OcrResultData, ExtractError> {
                // Distinguish the 800x600 error dialog screenshot from the 640x480 photo.
                if width == 800 {
                    Ok(OcrResultData {
                        text: "Fatal dialog error 0x80070005 CUDA out of memory".into(),
                        words: vec![OcrWordBox {
                            text: "0x80070005".into(),
                            x: 10,
                            y: 20,
                            w: 80,
                            h: 18,
                        }],
                        language_tag: Some("en-US".into()),
                    })
                } else {
                    Ok(OcrResultData::default())
                }
            }
        }

        let tree = TempTree::new();
        let thumb_dir = TempTree::new();

        // 1. Create a photo with EXIF (including a GPS IFD that must NOT be indexed).
        let photo_path = tree.0.join("receipt_desk.png");
        let bgra_photo = vec![180u8; 640 * 480 * 4];
        write_bgra_thumbnail_png(&bgra_photo, 640, 480, 640, &photo_path).unwrap();
        {
            // Inject PNG `eXIf` chunk with Camera = "Sony ILCE-7RM4" and GPS = "SECRET_GPS_48_8566".
            let png = fs::read(&photo_path).unwrap();
            let mut tiff = Vec::new();
            tiff.extend_from_slice(b"II*\0");
            tiff.extend_from_slice(&8u32.to_le_bytes());
            tiff.extend_from_slice(&2u16.to_le_bytes());
            let model = b"Sony ILCE-7RM4\0";
            let model_off = 38u32;
            let gps_off = model_off + model.len() as u32;
            // Tag 0x0110 Model
            tiff.extend_from_slice(&0x0110u16.to_le_bytes());
            tiff.extend_from_slice(&2u16.to_le_bytes());
            tiff.extend_from_slice(&(model.len() as u32).to_le_bytes());
            tiff.extend_from_slice(&model_off.to_le_bytes());
            // Tag 0x8825 GPSInfo IFD
            tiff.extend_from_slice(&0x8825u16.to_le_bytes());
            tiff.extend_from_slice(&4u16.to_le_bytes());
            tiff.extend_from_slice(&1u32.to_le_bytes());
            tiff.extend_from_slice(&gps_off.to_le_bytes());
            tiff.extend_from_slice(&0u32.to_le_bytes());
            while tiff.len() < model_off as usize {
                tiff.push(0);
            }
            tiff.extend_from_slice(model);
            // GPS IFD with secret coordinate string
            let gps_secret = b"SECRET_GPS_48_8566\0";
            let gps_str_off = gps_off + 2 + 12 + 4;
            tiff.extend_from_slice(&1u16.to_le_bytes());
            tiff.extend_from_slice(&0x0002u16.to_le_bytes());
            tiff.extend_from_slice(&2u16.to_le_bytes());
            tiff.extend_from_slice(&(gps_secret.len() as u32).to_le_bytes());
            tiff.extend_from_slice(&gps_str_off.to_le_bytes());
            tiff.extend_from_slice(&0u32.to_le_bytes());
            tiff.extend_from_slice(gps_secret);

            let iend = png.len() - 12;
            let mut out = Vec::new();
            out.extend_from_slice(&png[..iend]);
            out.extend_from_slice(&(tiff.len() as u32).to_be_bytes());
            out.extend_from_slice(b"eXIf");
            out.extend_from_slice(&tiff);
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&png[iend..]);
            fs::write(&photo_path, out).unwrap();
        }

        // 2. Create a screenshot inside a `Screenshots` folder.
        let shots_dir = tree.0.join("Screenshots");
        fs::create_dir_all(&shots_dir).unwrap();
        let shot_path = shots_dir.join("dialog.png");
        let bgra_shot = vec![220u8; 800 * 600 * 4];
        write_bgra_thumbnail_png(&bgra_shot, 800, 600, 800, &shot_path).unwrap();

        let indexer = seeded_indexer(&tree);
        assert_eq!(indexer.content_extract_queue_depth().unwrap(), 2);

        let mut registry = ExtractorRegistry::default();
        registry.register(
            SEGMENT_KIND_IMAGE,
            Arc::new(ImageExtractor::new(
                thumb_dir.0.clone(),
                Arc::new(MapOcrProvider),
            )),
        );

        // Visual embedding callback simulates mmproj embedding "a receipt on a desk"
        // for `receipt_desk.png`.
        let receipt_query = "a receipt on a desk";
        let visual_embed = |p: &Path| -> Result<Option<Vec<f32>>, String> {
            let seed = if p.file_name().and_then(|n| n.to_str()) == Some("receipt_desk.png") {
                receipt_query.to_string()
            } else {
                format!("image:{}", p.display())
            };
            Ok(Some(ws_embed_protocol::stub_vector(
                &seed,
                ws_embed_protocol::STUB_DIM,
            )))
        };

        while indexer.content_extract_queue_depth().unwrap() > 0 {
            let step = indexer
                .extract_once_with_media(&registry, fake_embed, visual_embed)
                .unwrap();
            assert!(step.claimed);
            assert_eq!(step.status, Some(ExtractionStatus::Extracted));
        }

        for path in [&photo_path,&shot_path] {
            let store=indexer.store_lock();
            let id=store.find_by_path(&path.to_string_lossy()).unwrap().unwrap().id;
            let actual_units=store.list_content_chunks(id).unwrap().iter().filter(|c|!c.visual).count();
            assert_eq!(store.content_progress(id,"image").unwrap().unwrap().cursor,actual_units as u64);
        }

        // A. EXIF fields are searchable via FTS, while GPS geotags are never indexed.
        let cam_hits = indexer
            .search(&SearchRequest {
                text: "ILCE-7RM4".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(cam_hits.len(), 1);
        assert_eq!(cam_hits[0].name, "receipt_desk.png");
        assert!(cam_hits[0].thumbnail_path.as_ref().is_some_and(|t| Path::new(t).is_file()));

        let gps_hits = indexer
            .search(&SearchRequest {
                text: "SECRET_GPS_48_8566".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(gps_hits.is_empty(), "GPS geotag must never be indexed");

        // B. Screenshot OCR exact error-code hit + `HitKind::Screenshot` classification.
        let ocr_hits = indexer
            .search(&SearchRequest {
                text: "0x80070005".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(ocr_hits.len(), 1);
        assert_eq!(ocr_hits[0].name, "dialog.png");
        assert_eq!(ocr_hits[0].kind, HitKind::Screenshot);

        // Verify OCR segment is also stored for semantic search.
        {
            let store = indexer.store_lock();
            let segs = store.list_segments_for_entry(ocr_hits[0].entry_id).unwrap();
            assert!(segs.iter().any(|s| s.kind == SEGMENT_KIND_OCR));
        }

        // C. Semantic visual query "a receipt on a desk" matches the mmproj image segment.
        let qvec = ws_embed_protocol::stub_vector(receipt_query, ws_embed_protocol::STUB_DIM);
        let sem_hits = indexer
            .search_vectors(&qvec, 5, Some(&VectorFilter::for_kind(SEGMENT_KIND_IMAGE)))
            .unwrap();
        assert!(!sem_hits.is_empty());
        assert_eq!(sem_hits[0].segment.entry_id, cam_hits[0].entry_id);
        assert!(sem_hits[0].cosine > 0.99);
    }

    #[test]
    fn unsupported_or_failed_reextraction_clears_stale_content_fts_and_segments() {
        let tree = TempTree::new();
        let file = tree.0.join("mutable.txt");
        fs::write(&file, "unique_stale_keyword_alpha in document").unwrap();

        let indexer = seeded_indexer(&tree);
        let registry = ExtractorRegistry::default();
        let out = indexer.extract_once_with(&registry, fake_embed).unwrap();
        assert_eq!(out.status, Some(ExtractionStatus::Extracted));
        assert!(out.segments_stored >= 1);

        let hits = indexer
            .search(&SearchRequest {
                text: "unique_stale_keyword_alpha".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        let entry_id = hits[0].entry_id;

        // Re-enqueue the content job and run against an extractor that returns Unsupported.
        struct UnsupportedDocExtractor;
        impl Extractor for UnsupportedDocExtractor {
            fn extract(
                &self,
                _path: &Path,
                _budget: &ExtractBudget,
            ) -> Result<ExtractOutput, ExtractError> {
                Ok(ExtractOutput::unsupported())
            }
        }

        {
            let mut store = indexer.store_lock();
            store.enqueue_extract_job(entry_id, SEGMENT_KIND_DOC).unwrap();
        }
        let mut unsup_reg = ExtractorRegistry::default();
        unsup_reg.register(SEGMENT_KIND_DOC, Arc::new(UnsupportedDocExtractor));
        let out2 = indexer.extract_once_with(&unsup_reg, fake_embed).unwrap();
        assert_eq!(out2.status, Some(ExtractionStatus::Unsupported));

        assert!(
            indexer
                .search(&SearchRequest {
                    text: "unique_stale_keyword_alpha".into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .is_empty(),
            "stale content_fts must be cleared on Unsupported"
        );
        {
            let store = indexer.store_lock();
            let segs = store.list_segments_for_entry(entry_id).unwrap();
            assert!(
                segs.iter().all(|s| s.kind != SEGMENT_KIND_DOC),
                "stale doc segments must be cleared on Unsupported"
            );
        }
    }

    #[test]
    fn image_reextraction_without_ocr_clears_stale_ocr_segments() {
        struct StubImageExtractor {
            with_ocr: bool,
        }
        impl Extractor for StubImageExtractor {
            fn extract(
                &self,
                _path: &Path,
                _budget: &ExtractBudget,
            ) -> Result<ExtractOutput, ExtractError> {
                let mut segments = vec![SegmentDraft::text(
                    SEGMENT_KIND_IMAGE,
                    "Camera: Canon EOS",
                )];
                if self.with_ocr {
                    segments.push(SegmentDraft::text(
                        SEGMENT_KIND_OCR,
                        "STALE_OCR_TOKEN_999",
                    ));
                }
                Ok(ExtractOutput {
                    status: ExtractionStatus::Extracted,
                    segments,
                })
            }
        }

        let tree = TempTree::new();
        let img = tree.0.join("screen.png");
        fs::write(&img, b"fake-png").unwrap();

        let indexer = seeded_indexer(&tree);
        let mut reg1 = ExtractorRegistry::default();
        reg1.register(SEGMENT_KIND_IMAGE, Arc::new(StubImageExtractor { with_ocr: true }));
        let out1 = indexer.extract_once_with(&reg1, fake_embed).unwrap();
        assert_eq!(out1.status, Some(ExtractionStatus::Extracted));

        let entry_id = {
            let store = indexer.store_lock();
            let row = store.find_by_path(&img.to_string_lossy()).unwrap().unwrap();
            let segs = store.list_segments_for_entry(row.id).unwrap();
            assert!(segs.iter().any(|s| s.kind == SEGMENT_KIND_OCR));
            row.id
        };

        // Re-extract the same image when the new extraction produces NO OCR draft.
        {
            let mut store = indexer.store_lock();
            store.enqueue_extract_job(entry_id, SEGMENT_KIND_IMAGE).unwrap();
        }
        let mut reg2 = ExtractorRegistry::default();
        reg2.register(SEGMENT_KIND_IMAGE, Arc::new(StubImageExtractor { with_ocr: false }));
        let out2 = indexer.extract_once_with(&reg2, fake_embed).unwrap();
        assert_eq!(out2.status, Some(ExtractionStatus::Extracted));

        {
            let store = indexer.store_lock();
            let segs = store.list_segments_for_entry(entry_id).unwrap();
            assert!(
                segs.iter().all(|s| s.kind != SEGMENT_KIND_OCR),
                "stale OCR segment must be removed when re-extraction produces no OCR draft"
            );
            assert!(segs.iter().any(|s| s.kind == SEGMENT_KIND_IMAGE));
        }
    }
}

