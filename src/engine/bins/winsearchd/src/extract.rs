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
    ImageExtractor, SegmentDraft,
};
use ws_index::model::{
    NewSegment, SEGMENT_KIND_DOC, SEGMENT_KIND_IMAGE, SEGMENT_KIND_OCR, SEGMENT_KIND_PDF,
};
use ws_index::{ExtractJob, StoreError};

use crate::embed_client::{EmbedSupervisor, EMBED_JOB_TIMEOUT};
use crate::indexer::{Indexer, MAX_EMBED_ATTEMPTS};

/// Number of background content extraction worker threads (P4.1).
pub const EXTRACT_WORKER_THREADS: usize = 2;

/// Outcome of processing at most one content extraction job (`extract_once_with`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtractStepOutcome {
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
}

impl Default for ExtractorRegistry {
    fn default() -> Self {
        let mut reg = Self {
            extractors: HashMap::new(),
            budget_factory: Box::new(|_kind| ExtractBudget::default()),
        };
        reg.register(SEGMENT_KIND_DOC, Arc::new(DocumentExtractor));
        reg.register(SEGMENT_KIND_PDF, Arc::new(DocumentExtractor));
        reg.register(SEGMENT_KIND_IMAGE, Arc::new(ImageExtractor::default()));
        reg
    }
}

/// Alias for [`ExtractorRegistry`].
#[allow(dead_code)]
pub type ExtractionPool = ExtractorRegistry;

impl ExtractorRegistry {
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
        if self.indexing_pause_reason().is_some() { return Ok(ExtractStepOutcome::default()); }
        // 1. Claim highest-priority non-name job and snapshot entry metadata.
        let claimed = {
            let mut store = self.store_lock();
            if self.indexing_pause_reason().is_some() { return Ok(ExtractStepOutcome::default()); }
            let Some(job) = store.claim_content_extract_job()? else {
                return Ok(ExtractStepOutcome::default());
            };
            match store.get_entry(job.entry_id)? {
                Some(entry) if !entry.missing && !entry.is_dir => Some((job, entry)),
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
        let Some(extractor) = registry.get(&job.kind) else {
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
        };

        // 3. Run extraction outside the store lock, enforcing the budget.
        let path = Path::new(&entry.path);
        let extract_res = run_extractor_with_budget(extractor.as_ref(), path, &budget);

        let output = match extract_res {
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
                    clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
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

        if matches!(
            output.status,
            ExtractionStatus::Unsupported | ExtractionStatus::Failed(_)
        ) {
            let mut store = self.store_lock();
            clear_modality_artifacts(&mut store, job.entry_id, &job.kind)?;
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

        // 4. Separate FTS texts and embeddable segments (only when status == Extracted).
        let fts_texts: Vec<String> = output
            .segments
            .iter()
            .map(|s| s.text.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect();

        let embeddable: Vec<&SegmentDraft> = if output.status == ExtractionStatus::Extracted {
            output
                .segments
                .iter()
                .filter(|s| s.embed && !s.text.trim().is_empty())
                .collect()
        } else {
            Vec::new()
        };

        // 5. Embed in batches of `EMBED_BATCH` outside the store lock.
        let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(embeddable.len());
        if !embeddable.is_empty() {
            let texts: Vec<String> = embeddable.iter().map(|s| s.text.clone()).collect();
            for chunk in texts.chunks(crate::indexer::EMBED_BATCH) {
                match embed(chunk) {
                    Ok(batch_vecs) if batch_vecs.len() == chunk.len() => {
                        vectors.extend(batch_vecs);
                    }
                    Ok(batch_vecs) => {
                        warn!(
                            got = batch_vecs.len(),
                            want = chunk.len(),
                            "content embed arity mismatch; releasing job for retry"
                        );
                        let mut store = self.store_lock();
                        let poisoned =
                            Self::fail_extract_job_capped(&mut store, &job, &entry.name)?;
                        drop(store);
                        if poisoned {
                            self.invalidate_vector_cache();
                        }
                        return Ok(ExtractStepOutcome {
                            claimed: true,
                            status: Some(output.status),
                            failed: !poisoned,
                            poisoned,
                            ..Default::default()
                        });
                    }
                    Err(e) => {
                        warn!(
                            job = job.id,
                            error = %e,
                            "content embed failed; releasing job for retry"
                        );
                        let mut store = self.store_lock();
                        let poisoned =
                            Self::fail_extract_job_capped(&mut store, &job, &entry.name)?;
                        drop(store);
                        if poisoned {
                            self.invalidate_vector_cache();
                        }
                        return Ok(ExtractStepOutcome {
                            claimed: true,
                            status: Some(output.status),
                            failed: !poisoned,
                            poisoned,
                            ..Default::default()
                        });
                    }
                }
            }
        }

        // 5b. For `image` jobs, also compute a visual embedding via `embed_image(path)`
        // when available (e.g. `mtmd` projector or stub worker).
        let visual_vec: Option<Vec<f32>> =
            if output.status == ExtractionStatus::Extracted && job.kind == SEGMENT_KIND_IMAGE {
                match if self.content_path_allowed(path) { embed_image(path) } else { Ok(None) } {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            job = job.id,
                            error = %e,
                            "visual image embed failed; releasing job for retry"
                        );
                        let mut store = self.store_lock();
                        let poisoned =
                            Self::fail_extract_job_capped(&mut store, &job, &entry.name)?;
                        drop(store);
                        if poisoned {
                            self.invalidate_vector_cache();
                        }
                        return Ok(ExtractStepOutcome {
                            claimed: true,
                            status: Some(output.status),
                            failed: !poisoned,
                            poisoned,
                            ..Default::default()
                        });
                    }
                }
            } else {
                None
            };

        // 6. Commit FTS + vector segments under the store lock if the entry is unchanged.
        let mut store = self.store_lock();
        let Some(fresh) = store.get_entry(job.entry_id)? else {
            let _ = store.complete_extract_job(job.id);
            return Ok(ExtractStepOutcome {
                claimed: true,
                status: Some(output.status),
                ..Default::default()
            });
        };
        if !self.content_path_allowed(Path::new(&fresh.path)) {
            store.requeue_extract_job(job.id)?;
            return Ok(ExtractStepOutcome { claimed: true, requeued: true, ..Default::default() });
        }
        let fresh_job = store.get_extract_job(job.id)?;
        let changed = fresh.mtime != entry.mtime
            || fresh.size != entry.size
            || fresh.path != entry.path
            || fresh_job
                .as_ref()
                .is_some_and(|j| j.priority != job.priority);
        if changed {
            if fresh_job.is_some() {
                let _ = store.requeue_extract_job(job.id);
            }
            return Ok(ExtractStepOutcome {
                claimed: true,
                status: Some(output.status),
                requeued: true,
                ..Default::default()
            });
        }

        store.replace_content_fts(job.entry_id, &job.kind, &fts_texts)?;
        store.remove_segments_for_entry_and_kind(job.entry_id, &job.kind)?;
        if job.kind == SEGMENT_KIND_IMAGE {
            store.remove_segments_for_entry_and_kind(job.entry_id, SEGMENT_KIND_OCR)?;
        }

        if let Some(thumb) = output
            .segments
            .iter()
            .find_map(|s| s.thumbnail_hint.as_ref())
        {
            let thumb_str = thumb.to_string_lossy();
            store.set_entry_thumbnail_path(job.entry_id, Some(&thumb_str))?;
        }

        // Group chunk_index per segment kind in case an extractor returns
        // multiple kinds (e.g. `image` + `ocr` or `video` + `audio`).
        let mut next_chunk_by_kind: HashMap<&str, i64> = HashMap::new();
        let mut stored = 0usize;
        if let Some(vvec) = &visual_vec {
            store.upsert_segment(NewSegment {
                entry_id: job.entry_id,
                kind: SEGMENT_KIND_IMAGE,
                chunk_index: 0,
                text: &entry.name,
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: vvec,
            })?;
            next_chunk_by_kind.insert(SEGMENT_KIND_IMAGE, 1);
            stored += 1;
        }
        for (draft, vec) in embeddable.iter().zip(vectors.iter()) {
            let kind_str = draft.kind.as_str();
            let chunk_idx = next_chunk_by_kind.entry(kind_str).or_insert(0);
            if *chunk_idx == 0 && kind_str != job.kind && kind_str != SEGMENT_KIND_OCR {
                store.remove_segments_for_entry_and_kind(job.entry_id, kind_str)?;
            }
            store.upsert_segment(NewSegment {
                entry_id: job.entry_id,
                kind: kind_str,
                chunk_index: *chunk_idx,
                text: &draft.text,
                page: draft.page.map(i64::from),
                start_ms: draft.start_ms.map(|v| v as i64),
                end_ms: draft.end_ms.map(|v| v as i64),
                line: draft.line.map(i64::from),
                vector: vec,
            })?;
            *chunk_idx += 1;
            stored += 1;
        }

        store.complete_extract_job(job.id)?;
        drop(store);
        self.invalidate_vector_cache();
        Ok(ExtractStepOutcome {
            claimed: true,
            status: Some(output.status),
            segments_stored: stored,
            fts_chunks_indexed: fts_texts.len(),
            requeued: false,
            failed: false,
            poisoned: false,
        })
    }

    /// Production entry for one content extraction step using the supervisor.
    pub fn extract_once_with_supervisor(
        &self,
        registry: &ExtractorRegistry,
        sup: &Arc<EmbedSupervisor>,
    ) -> Result<ExtractStepOutcome, StoreError> {
        self.extract_once_with_media(
            registry,
            |texts| {
                sup.embed_texts_bounded(texts, EMBED_JOB_TIMEOUT)
                    .map_err(|e| e.to_string())
            },
            |path| match sup.embed_image_bounded(path, EMBED_JOB_TIMEOUT) {
                Ok(vec) => Ok(Some(vec)),
                Err(crate::embed_client::EmbedError::Unsupported(_)) => Ok(None),
                Err(e) => Err(e.to_string()),
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
            clear_modality_artifacts(store, job.entry_id, &job.kind)?;
            store.mark_extract_failed(job.id)?;
            Ok(true)
        } else {
            store.fail_extract_job(job.id)?;
            Ok(false)
        }
    }
}

fn clear_modality_artifacts(
    store: &mut ws_index::IndexStore,
    entry_id: i64,
    job_kind: &str,
) -> Result<(), StoreError> {
    store.delete_content_text(entry_id)?;
    store.remove_segments_for_entry_and_kind(entry_id, job_kind)?;
    if job_kind == SEGMENT_KIND_IMAGE {
        store.remove_segments_for_entry_and_kind(entry_id, SEGMENT_KIND_OCR)?;
    }
    Ok(())
}

fn run_extractor_with_budget(
    extractor: &dyn Extractor,
    path: &Path,
    budget: &ExtractBudget,
) -> Result<ExtractOutput, ExtractError> {
    budget.check_deadline()?;
    let meta = std::fs::metadata(path)?;
    budget.check_size(meta.len())?;
    let mut out = extractor.extract(path, budget)?;
    budget.check_deadline()?;
    if out.segments.len() > budget.max_segments {
        out.segments.truncate(budget.max_segments);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;
    use ws_index::model::{SearchRequest, VectorFilter, SEGMENT_KIND_DOC, SEGMENT_KIND_NAME};
    use ws_index::IndexStore;

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
        assert!(second.segments_stored > 1, "large file should be extracted second");
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

