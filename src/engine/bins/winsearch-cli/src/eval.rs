//! Headless retrieval evaluation runner for `winsearch-cli eval <suite>`.
//!
//! Builds an isolated temporary fixture index from `evals/<suite>.json`,
//! populates file entries, embeds all entry names using either:
//! - the deterministic semantic concept embedder (default in CI when no GGUF is
//!   configured), or
//! - the live `winsearch-embed` worker when `WS_EMBED_TEST_MODEL` points to a
//!   valid GGUF file,
//! and runs the hybrid exact + semantic ranking pipeline across all suite
//! queries, computing Recall@5, MRR, and negative-control false-positive rate.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::Deserialize;
use ws_embed_protocol::STUB_DIM;
use ws_index::model::{extension_of, EntryRecord, NewSegment, SearchRequest, SEGMENT_KIND_NAME};
use ws_index::IndexStore;

#[cfg(test)]
use ws_index::retrieval::MIN_SEMANTIC_COSINE;
use ws_index::retrieval::{hybrid_score, semantic_admitted, SEMANTIC_TOP_K};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalSuite {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub thresholds: EvalThresholds,
    pub entries: Vec<FixtureEntry>,
    pub queries: Vec<EvalQuery>,
    #[serde(default)]
    pub media_generation: Option<MediaGeneration>,
}

/// Frozen input-generation metadata, never substituted for actual media input.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaGeneration {
    pub screen: Vec<String>,
    pub receipt: Vec<String>,
    pub speech: String,
    pub frame: Vec<String>,
    pub soundtrack: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalThresholds {
    pub min_recall_at_5: f64,
    pub min_mrr: f64,
    #[serde(default)]
    pub max_false_positive_rate: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureEntry {
    pub path: String,
    pub name: String,
    #[serde(default)]
    pub is_dir: bool,
    #[serde(default)]
    pub is_app: bool,
    #[serde(default)]
    pub keywords: String,
    #[serde(default = "default_size")]
    pub size: u64,
    #[serde(default)]
    pub segments: Vec<FixtureSegment>,
}

impl FixtureEntry {
    fn is_code(&self) -> bool {
        self.segments.iter().any(|s| s.kind == "code")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSegment {
    pub kind: String,
    #[serde(default)]
    pub text: String,
    pub asset: Option<PathBuf>,
    pub page: Option<u32>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub line: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalMode {
    #[default]
    Normal,
    Code,
}

fn default_size() -> u64 {
    1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalQuery {
    pub query: String,
    #[serde(default)]
    pub category: String,
    #[serde(default = "default_true")]
    pub include_semantic: bool,
    #[serde(default)]
    pub expected_any: Vec<String>,
    #[serde(default)]
    pub expect_empty: bool,
    #[serde(default)]
    pub mode: EvalMode,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone)]
pub struct QueryEvalOutcome {
    pub query: String,
    pub category: String,
    pub expect_empty: bool,
    pub expected_any: Vec<String>,
    pub top_hits: Vec<(String, f64, String)>,
    pub ranked_hits: Vec<RankedEvalHit>,
    pub first_relevant_rank: Option<usize>,
    pub hit_in_top_5: bool,
    pub reciprocal_rank: f64,
    pub false_positive: bool,
}

#[derive(Debug, Clone)]
pub struct SuiteReport {
    pub suite_name: String,
    pub embedder_label: String,
    pub positive_queries: usize,
    pub negative_queries: usize,
    pub recall_at_5: f64,
    pub mrr: f64,
    pub false_positive_rate: f64,
    pub thresholds: EvalThresholds,
    pub outcomes: Vec<QueryEvalOutcome>,
    pub modalities: BTreeMap<String, ModalityReport>,
}

#[derive(Debug, Clone, Default)]
pub struct ModalityReport {
    pub positive: usize,
    pub negative: usize,
    pub hits: usize,
    pub reciprocal_rank_sum: f64,
    pub false_positives: usize,
}

impl ModalityReport {
    pub fn recall(&self) -> Option<f64> {
        (self.positive > 0).then(|| self.hits as f64 / self.positive as f64)
    }
    pub fn mrr(&self) -> Option<f64> {
        (self.positive > 0).then(|| self.reciprocal_rank_sum / self.positive as f64)
    }
    pub fn fp_rate(&self) -> Option<f64> {
        (self.negative > 0).then(|| self.false_positives as f64 / self.negative as f64)
    }
}

impl SuiteReport {
    pub fn passed(&self) -> bool {
        self.recall_at_5 + 1e-9 >= self.thresholds.min_recall_at_5
            && self.mrr + 1e-9 >= self.thresholds.min_mrr
            && self.false_positive_rate <= self.thresholds.max_false_positive_rate + 1e-9
    }
}

/// Resolves `evals/<suite>.json` from the current directory or ancestor
/// workspace roots (or accepts a direct `.json` path).
pub fn resolve_suite_path(suite: &str) -> anyhow::Result<PathBuf> {
    let direct = PathBuf::from(suite);
    if direct.is_file() {
        return Ok(direct);
    }
    let file_name = if suite.ends_with(".json") {
        suite.to_string()
    } else {
        format!("{suite}.json")
    };

    let mut candidates = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        for ancestor in cwd.ancestors() {
            candidates.push(ancestor.join("evals").join(&file_name));
        }
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for ancestor in manifest_dir.ancestors() {
        candidates.push(ancestor.join("evals").join(&file_name));
    }

    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }
    anyhow::bail!(
        "eval suite {:?} not found (searched evals/{file_name} upward from cwd and manifest dir)",
        suite
    )
}

pub fn load_suite(path: &Path) -> anyhow::Result<EvalSuite> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", path.display()))?;
    let suite: EvalSuite = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("invalid suite JSON in {}: {e}", path.display()))?;
    Ok(suite)
}

/// Runs the evaluation suite against an isolated in-memory / temp fixture index.
pub fn run_eval_suite(suite: &EvalSuite) -> anyhow::Result<SuiteReport> {
    let embedder = select_embedder()?;
    run_eval_suite_with(suite, embedder.as_ref())
}

pub trait EvalEmbedder {
    fn label(&self) -> &str;
    fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>>;
    fn embed_image(&self, _path: &Path, _tokens: u32) -> anyhow::Result<Vec<f32>> {
        anyhow::bail!("media evaluation requires an image-capable real worker");
    }
    fn embed_audio(&self, _path: &Path) -> anyhow::Result<Vec<f32>> {
        anyhow::bail!("media evaluation requires an audio-capable real worker");
    }
}

/// Deterministic concept-aware semantic embedder used when no real GGUF is
/// configured. Maps domain/multilingual concept clusters into dedicated
/// orthogonal subspaces (dimensions 0..32) plus weak per-token hashed features
/// (dimensions 32..768) so unrelated queries stay below `MIN_SEMANTIC_COSINE`
/// while genuine semantic paraphrases and cross-lingual synonyms score high.
pub struct ConceptStubEmbedder;

impl EvalEmbedder for ConceptStubEmbedder {
    fn label(&self) -> &str {
        "ConceptStubEmbedder (pipeline smoke test; deterministic 768d CI fallback)"
    }

    fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|t| concept_stub_vector(t, STUB_DIM))
            .collect())
    }
}

static WORKER_PIPE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Worker-backed embedder used when `WS_EMBED_TEST_MODEL` points to a real GGUF.
pub struct WorkerModelEmbedder {
    model_path: PathBuf,
    pipe_name: String,
    verified_label: OnceLock<String>,
    verified_backend: OnceLock<String>,
    child: Mutex<Option<Child>>,
    next_id: AtomicU64,
}

impl WorkerModelEmbedder {
    pub fn new(model_path: PathBuf) -> Self {
        let pipe_name = format!(
            r"\\.\pipe\winsearch-cli-eval-{}-{}-{}",
            std::process::id(),
            WORKER_PIPE_SEQ.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        Self {
            model_path,
            pipe_name,
            verified_label: OnceLock::new(),
            verified_backend: OnceLock::new(),
            child: Mutex::new(None),
            next_id: AtomicU64::new(1),
        }
    }

    fn ensure_child(&self) -> anyhow::Result<()> {
        let mut guard = self.child.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(child) = guard.as_mut() {
            match child.try_wait() {
                Ok(None) => return Ok(()),
                _ => {
                    *guard = None;
                }
            }
        }

        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."));
        let mut worker_bin =
            exe_dir.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
        if !worker_bin.is_file() {
            if let Some(parent) = exe_dir.parent() {
                let candidate =
                    parent.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
                if candidate.is_file() {
                    worker_bin = candidate;
                }
            }
        }
        if !worker_bin.is_file() {
            anyhow::bail!(
                "WS_EMBED_TEST_MODEL is set ({}), but worker binary {} was not found",
                self.model_path.display(),
                worker_bin.display()
            );
        }

        let child = Command::new(&worker_bin)
            .arg("--pipe")
            .arg(&self.pipe_name)
            .arg("--model")
            .arg(&self.model_path)
            .arg("--idle-secs")
            .arg("60")
            .spawn()?;
        *guard = Some(child);
        Ok(())
    }
}

impl Drop for WorkerModelEmbedder {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl EvalEmbedder for WorkerModelEmbedder {
    fn label(&self) -> &str {
        self.verified_label
            .get()
            .map(String::as_str)
            .unwrap_or("WorkerModelEmbedder (awaiting verified real-worker response)")
    }

    fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.embed_request(
            ws_embed_protocol::EmbedRequest::EmbedTexts {
                id,
                texts: texts.to_vec(),
            },
            id,
            texts.len(),
            "embed_texts",
        )
    }

    fn embed_image(&self, path: &Path, tokens: u32) -> anyhow::Result<Vec<f32>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .embed_request(
                ws_embed_protocol::EmbedRequest::EmbedImage {
                    id,
                    path: path.to_string_lossy().into_owned(),
                    token_budget: Some(tokens),
                },
                id,
                1,
                "embed_image",
            )?
            .remove(0))
    }

    fn embed_audio(&self, path: &Path) -> anyhow::Result<Vec<f32>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .embed_request(
                ws_embed_protocol::EmbedRequest::EmbedAudio {
                    id,
                    path: path.to_string_lossy().into_owned(),
                    window_ms: None,
                },
                id,
                1,
                "embed_audio",
            )?
            .remove(0))
    }
}

impl WorkerModelEmbedder {
    fn embed_request(
        &self,
        request: ws_embed_protocol::EmbedRequest,
        id: u64,
        count: usize,
        feature: &str,
    ) -> anyhow::Result<Vec<Vec<f32>>> {
        self.ensure_child()?;

        let start = std::time::Instant::now();
        let mut stream = loop {
            match ws_platform::pipe::PipeStream::connect(&self.pipe_name) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < std::time::Duration::from_secs(15) => {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                Err(e) => anyhow::bail!(
                    "failed to connect to eval worker pipe {}: {e}",
                    self.pipe_name
                ),
            }
        };

        ws_protocol::write_message(
            &mut stream,
            &ws_embed_protocol::EmbedRequest::Hello {
                protocol_version: ws_embed_protocol::EMBED_PROTOCOL_VERSION,
                client: "winsearch-cli".into(),
                client_version: env!("CARGO_PKG_VERSION").into(),
            },
        )?;
        let ack: Option<ws_embed_protocol::EmbedResponse> = ws_protocol::read_message(&mut stream)?;
        match ack {
            Some(ws_embed_protocol::EmbedResponse::HelloAck {
                protocol_version,
                stub,
                features,
                ..
            }) => {
                anyhow::ensure!(!stub, "real-model evaluation refuses a stub worker");
                anyhow::ensure!(
                    protocol_version == ws_embed_protocol::EMBED_PROTOCOL_VERSION,
                    "real-model worker protocol version mismatch"
                );
                anyhow::ensure!(
                    features.iter().any(|f| f == feature),
                    "real-model worker does not support {feature}"
                );
            }
            Some(ws_embed_protocol::EmbedResponse::Error { code, message, .. }) => {
                anyhow::bail!("worker handshake error {code:?}: {message}");
            }
            other => {
                anyhow::bail!("unexpected worker handshake reply: {other:?}");
            }
        }

        ws_protocol::write_message(&mut stream, &request)?;
        let reply: Option<ws_embed_protocol::EmbedResponse> =
            ws_protocol::read_message(&mut stream)?;
        let (vectors, backend) = real_worker_vectors(reply, id, count)?;
        let observed = self.verified_backend.get_or_init(|| backend.clone());
        anyhow::ensure!(
            observed == &backend,
            "real-model worker backend changed during evaluation"
        );
        self.verified_label.get_or_init(|| {
            format!(
                "WorkerModelEmbedder (real {backend}; stub:false; dim:768; model:{})",
                self.model_path.display()
            )
        });
        Ok(vectors)
    }
}

fn real_worker_vectors(
    reply: Option<ws_embed_protocol::EmbedResponse>,
    expected_id: u64,
    expected_count: usize,
) -> anyhow::Result<(Vec<Vec<f32>>, String)> {
    match reply {
        Some(ws_embed_protocol::EmbedResponse::Embeddings {
            id,
            vecs,
            dim,
            backend,
            stub,
        }) => {
            anyhow::ensure!(
                !stub && backend != "stub",
                "real-model evaluation refuses stub vectors"
            );
            anyhow::ensure!(
                matches!(backend.as_str(), "cpu" | "cuda" | "vulkan"),
                "unknown real-model backend: {backend}"
            );
            anyhow::ensure!(id == expected_id, "real-model response ID mismatch");
            anyhow::ensure!(
                dim == ws_modelmgr::EMBEDDING_DIM as usize,
                "real-model embedding dimension mismatch"
            );
            anyhow::ensure!(
                vecs.len() == expected_count,
                "real-model embedding count mismatch"
            );
            for vector in &vecs {
                anyhow::ensure!(
                    vector.len() == dim && vector.iter().all(|v| v.is_finite()),
                    "invalid real-model vector shape or values"
                );
                let norm = vector
                    .iter()
                    .map(|v| f64::from(*v).powi(2))
                    .sum::<f64>()
                    .sqrt();
                anyhow::ensure!((norm - 1.0).abs() <= 1e-3, "non-unit real-model embedding");
            }
            Ok((vecs, backend))
        }
        Some(ws_embed_protocol::EmbedResponse::Error { code, message, .. }) => {
            anyhow::bail!("worker error {code:?}: {message}")
        }
        other => anyhow::bail!("unexpected worker reply: {other:?}"),
    }
}

fn select_embedder() -> anyhow::Result<Box<dyn EvalEmbedder>> {
    if let Some(os_path) = std::env::var_os("WS_EMBED_TEST_MODEL") {
        let p = PathBuf::from(os_path);
        anyhow::ensure!(
            p.is_file(),
            "WS_EMBED_TEST_MODEL explicitly requested a missing/non-file model: {}",
            p.display()
        );
        return Ok(Box::new(WorkerModelEmbedder::new(p.canonicalize()?)));
    }
    Ok(Box::new(ConceptStubEmbedder))
}

fn validate_fixture_segment(segment: &FixtureSegment) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            segment.kind.as_str(),
            "doc" | "pdf" | "ocr" | "image" | "audio" | "video" | "code"
        ),
        "unsupported fixture segment kind: {}",
        segment.kind
    );
    let media = matches!(segment.kind.as_str(), "image" | "audio" | "video");
    anyhow::ensure!(
        media == segment.asset.is_some(),
        "media fixture must supply an asset; text must not"
    );
    anyhow::ensure!(
        media || !segment.text.trim().is_empty(),
        "empty text fixture segment"
    );
    anyhow::ensure!(
        segment.text.len() <= ws_code::MAX_CODE_FILE_BYTES as usize,
        "fixture text exceeds 2 MiB"
    );
    anyhow::ensure!(
        segment.page != Some(0) && segment.line != Some(0),
        "invalid fixture page/line"
    );
    let timed = segment.start_ms.is_some() || segment.end_ms.is_some();
    let locations =
        u8::from(segment.page.is_some()) + u8::from(segment.line.is_some()) + u8::from(timed);
    let location_allowed = match segment.kind.as_str() {
        "doc" | "code" => segment.page.is_none() && !timed,
        "pdf" => segment.line.is_none() && !timed,
        "image" => locations == 0,
        "audio" | "video" => segment.page.is_none() && segment.line.is_none(),
        "ocr" => locations <= 1,
        _ => false,
    };
    anyhow::ensure!(
        location_allowed,
        "location fields do not apply to fixture kind {}",
        segment.kind
    );
    if let Some(start) = segment.line {
        let end = u64::from(start) + segment.text.lines().count().saturating_sub(1) as u64;
        anyhow::ensure!(
            end <= ws_code::MAX_CODE_FILE_BYTES + 1,
            "fixture exceeds bounded source lines"
        );
    }
    if segment.kind == "pdf" {
        anyhow::ensure!(segment.page.is_some(), "PDF fixture needs its actual page");
    }
    if segment.kind == "code" {
        anyhow::ensure!(segment.line.is_some(), "code fixture needs its actual line");
    }
    anyhow::ensure!(
        segment.start_ms.is_some() == segment.end_ms.is_some()
            && segment
                .start_ms
                .zip(segment.end_ms)
                .is_none_or(|(a, b)| a <= b && b <= ws_index::model::MAX_MEDIA_DURATION_MS),
        "invalid fixture time range"
    );
    if matches!(segment.kind.as_str(), "audio" | "video") {
        anyhow::ensure!(
            segment.start_ms.is_some(),
            "audio/video fixture needs actual source times"
        );
    }
    Ok(())
}

fn resolve_fixture_asset(asset: &Path) -> anyhow::Result<PathBuf> {
    let candidates = if asset.is_absolute() {
        vec![asset.to_path_buf()]
    } else {
        let cwd = std::env::current_dir()?;
        cwd.ancestors().map(|parent| parent.join(asset)).collect()
    };
    for path in candidates {
        if path.is_file() {
            anyhow::ensure!(
                ws_platform::content::local_content_allowed(&path),
                "media fixture asset is not resident/admitted"
            );
            let canonical = path.canonicalize()?;
            anyhow::ensure!(
                ws_platform::content::local_content_allowed(&canonical),
                "media fixture asset changed policy"
            );
            return Ok(canonical);
        }
    }
    anyhow::bail!("media fixture asset missing: {}", asset.display())
}

/// These are declared fixture chunks, not evidence of native extraction.
fn index_fixture_segments(
    store: &mut IndexStore,
    entry_id: i64,
    entry: &FixtureEntry,
    embedder: &dyn EvalEmbedder,
) -> anyhow::Result<()> {
    if entry.segments.is_empty() {
        return Ok(());
    }
    let job_kind = if entry.segments.iter().any(|s| s.kind == "video") {
        "video"
    } else if entry
        .segments
        .iter()
        .any(|s| s.kind == "image" || s.kind == "ocr")
    {
        "image"
    } else {
        entry.segments[0].kind.as_str()
    };
    let progress = store.begin_content_batch(entry_id, job_kind, "declared-eval-fixture")?;
    let mut chunks = Vec::new();
    let mut vectors = Vec::new();
    for (ordinal, segment) in entry.segments.iter().enumerate() {
        let asset = segment
            .asset
            .as_deref()
            .map(resolve_fixture_asset)
            .transpose()?;
        let vector = match segment.kind.as_str() {
            "image" | "video" => embedder.embed_image(
                asset.as_deref().expect("validated media asset"),
                if segment.kind == "video" { 140 } else { 280 },
            )?,
            "audio" => embedder.embed_audio(asset.as_deref().expect("validated media asset"))?,
            _ => {
                let input = ws_modelmgr::recipe::document(&entry.name, &segment.text);
                let mut reply = embedder.embed_texts(&[input])?;
                anyhow::ensure!(reply.len() == 1, "fixture text embedding count mismatch");
                reply.remove(0)
            }
        };
        let location = ws_index::model::ContentMatch {
            page: segment.page,
            start_ms: segment.start_ms,
            end_ms: segment.end_ms,
            line: segment.line,
            ..Default::default()
        };
        let input = match (segment.kind.as_str(), asset) {
            ("audio", Some(path)) => ws_index::model::DraftEmbeddingInput::Audio { path },
            ("image" | "video", Some(path)) => ws_index::model::DraftEmbeddingInput::Image {
                path,
                token_budget: if segment.kind == "video" { 140 } else { 280 },
            },
            _ => ws_index::model::DraftEmbeddingInput::Text,
        };
        chunks.push(ws_index::model::ContentChunk {
            id: 0,
            entry_id,
            job_kind: job_kind.into(),
            kind: segment.kind.clone(),
            chunk_index: ordinal as i64,
            text: segment.text.clone(),
            location,
            embed: false,
            visual: matches!(segment.kind.as_str(), "image" | "video"),
            embedding_input: input,
            attempts: 0,
        });
        vectors.push(vector);
    }
    // Fixture assembly uses the real exact-content transaction, with no daemon
    // or claim scheduler running in this isolated in-memory store.
    let job = ws_index::model::ExtractJob {
        id: 0,
        entry_id,
        kind: job_kind.into(),
        state: "claimed".into(),
        claimed: true,
        attempts: 0,
        priority: 0,
    };
    anyhow::ensure!(
        store.commit_content_batch(&job, &progress, &chunks, None)?,
        "fixture content commit lost its revision"
    );
    for (chunk, vector) in chunks.iter().zip(&vectors) {
        store.upsert_segment(NewSegment {
            entry_id,
            kind: &chunk.kind,
            chunk_index: chunk.chunk_index,
            text: &chunk.text,
            page: chunk.location.page.map(i64::from),
            start_ms: chunk.location.start_ms.map(|n| n as i64),
            end_ms: chunk.location.end_ms.map(|n| n as i64),
            line: chunk.location.line.map(i64::from),
            vector,
        })?;
    }
    Ok(())
}

fn index_code_fixtures(
    store: &mut ws_code::CodeStore,
    suite: &EvalSuite,
    embedder: &dyn EvalEmbedder,
    now: i64,
) -> anyhow::Result<()> {
    let repo = store.ensure_repo(&ws_code::DetectedRepo {
        root_path: "C:/Eval".into(),
        name: "declared-code-fixtures".into(),
        has_git: false,
    })?;
    for (index, entry) in suite
        .entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_code())
    {
        let ext = extension_of(&entry.name);
        let record = ws_code::CodeFileRecord {
            path: entry.path.clone(),
            rel_path: entry.name.clone(),
            name: entry.name.clone(),
            language: ws_code::language_for_file(&entry.name, &ext).into(),
            ext,
            size: entry.size,
            mtime: now - index as i64 * 3600,
            volume_serial: 1,
            file_id: index as u64 + 1,
        };
        let file = store.apply_file_batch(repo, 1, &[record])?[0].file_id();
        for (ordinal, segment) in entry.segments.iter().enumerate() {
            let mut vectors = embedder.embed_texts(&[ws_modelmgr::recipe::code_document(
                &entry.name,
                &segment.text,
            )])?;
            anyhow::ensure!(vectors.len() == 1, "code fixture embedding count mismatch");
            let vector = vectors.remove(0);
            let start = segment.line.unwrap_or(1);
            let end = start
                .checked_add(segment.text.lines().count().saturating_sub(1) as u32)
                .ok_or_else(|| anyhow::anyhow!("code fixture line overflow"))?;
            store.upsert_chunk(ws_code::NewCodeChunk {
                notebook: None,
                file_id: file,
                chunk_index: ordinal as i64,
                start_line: start,
                end_line: end,
                symbol_name: "",
                symbol_kind: "declared-snippet",
                text: &segment.text,
                vector: &vector,
            })?;
        }
    }
    Ok(())
}

pub fn execute_code_query(
    store: &ws_code::CodeStore,
    embedder: &dyn EvalEmbedder,
    query: &str,
    include_semantic: bool,
    limit: usize,
    now: i64,
) -> anyhow::Result<Vec<RankedEvalHit>> {
    let text = query.trim();
    if text.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let exact = store.search_exact(text, limit, |_| true)?;
    let mut semantic = Vec::new();
    if include_semantic {
        let mut vectors = embedder.embed_texts(&[ws_modelmgr::recipe::code_query(text)])?;
        anyhow::ensure!(vectors.len() == 1, "code query embedding count mismatch");
        let vector = vectors.remove(0);
        for hit in
            store.search_semantic(&vector, SEMANTIC_TOP_K.max(limit).saturating_mul(8), |_| {
                true
            })?
        {
            if !semantic_admitted("code", hit.score)
                || exact.iter().any(|e| {
                    e.file.id == hit.file.id
                        && e.start_line >= hit.start_line
                        && e.start_line <= hit.end_line
                })
            {
                continue;
            }
            let score = hybrid_score(hit.score, text, &hit.file.name_lower, hit.file.mtime, now);
            semantic.push((hit, score));
        }
        semantic.sort_by(|a, b| b.1.total_cmp(&a.1));
        semantic.truncate(limit);
    }
    let mut result: Vec<_> = exact
        .into_iter()
        .map(|hit| {
            let score = f64::from(hit.score);
            code_eval_hit(hit, score, false)
        })
        .collect();
    result.extend(
        semantic
            .into_iter()
            .map(|(hit, score)| code_eval_hit(hit, score, true)),
    );
    Ok(result)
}

fn code_eval_hit(hit: ws_code::model::CodeSearchHit, score: f64, semantic: bool) -> RankedEvalHit {
    let explanation = if semantic {
        format!("semantic code match · similarity {:.4}", hit.score)
    } else if hit.is_definition {
        "exact code definition".into()
    } else {
        "exact code reference/content".into()
    };
    RankedEvalHit {
        entry_id: hit.file.id,
        path: hit.file.path,
        name: hit.file.name,
        score,
        explanation,
        matches: vec![ws_index::model::ContentMatch {
            excerpt: hit.excerpt,
            line: Some(hit.start_line),
            ..Default::default()
        }],
        code: Some(CodeEvalLocation {
            symbol: hit.symbol_name,
            kind: hit.symbol_kind,
            is_definition: hit.is_definition,
            start_line: hit.start_line,
            end_line: hit.end_line,
            notebook: hit.notebook,
        }),
    }
}

pub fn run_eval_suite_with(
    suite: &EvalSuite,
    embedder: &dyn EvalEmbedder,
) -> anyhow::Result<SuiteReport> {
    if suite.queries.is_empty() {
        anyhow::bail!("evaluation suite {:?} contains no queries", suite.name);
    }
    if !suite.queries.iter().any(|q| !q.expect_empty) {
        anyhow::bail!(
            "evaluation suite {:?} contains no positive queries",
            suite.name
        );
    }
    for entry in &suite.entries {
        anyhow::ensure!(
            !entry.is_code() || entry.segments.iter().all(|s| s.kind == "code"),
            "code fixture entries cannot contain ordinary content"
        );
        for segment in &entry.segments {
            validate_fixture_segment(segment)?;
        }
    }
    if let Some(metadata) = &suite.media_generation {
        let panels = [&metadata.screen, &metadata.receipt, &metadata.frame];
        anyhow::ensure!(
            panels
                .iter()
                .all(|lines| !lines.is_empty() && lines.len() <= 64)
                && !metadata.speech.is_empty()
                && !metadata.soundtrack.is_empty(),
            "invalid media generation metadata"
        );
    }
    let mut store = IndexStore::open_in_memory()?;
    let mut code_store = ws_code::CodeStore::open_in_memory()?;
    let root_id = store.ensure_root("C:\\Users\\demo")?;
    let gen = store.begin_generation(root_id)?;
    let now = 1_760_000_000i64;

    let records: Vec<EntryRecord> = suite
        .entries
        .iter()
        .filter(|e| !e.is_code())
        .enumerate()
        .map(|(i, e)| EntryRecord {
            root_id,
            parent_id: None,
            path: e.path.clone(),
            name: e.name.clone(),
            ext: extension_of(&e.name),
            is_dir: e.is_dir,
            size: e.size,
            mtime: now - (i as i64 * 3600),
            volume_serial: 1,
            file_id: (i + 1) as u64,
            is_app: e.is_app,
            app_target: String::new(),
            keywords: e.keywords.clone(),
        })
        .collect();

    store.apply_batch(root_id, gen, &records)?;

    // Embed all entry names in one batch and populate `segments(kind='name')`.
    let ordinary: Vec<_> = suite.entries.iter().filter(|e| !e.is_code()).collect();
    let names: Vec<String> = ordinary
        .iter()
        .map(|e| ws_modelmgr::recipe::filename(&e.name).to_owned())
        .collect();
    let vectors = embedder.embed_texts(&names)?;
    if vectors.len() != ordinary.len() {
        anyhow::bail!(
            "embedder returned {} vectors for {} entries",
            vectors.len(),
            ordinary.len()
        );
    }

    for (entry, vec) in ordinary.iter().zip(vectors.iter()) {
        let row = store
            .find_by_path(&entry.path)?
            .ok_or_else(|| anyhow::anyhow!("missing indexed entry for {}", entry.path))?;
        store.upsert_segment(NewSegment {
            entry_id: row.id,
            kind: SEGMENT_KIND_NAME,
            chunk_index: 0,
            text: &row.name,
            page: None,
            start_ms: None,
            end_ms: None,
            line: None,
            vector: vec,
        })?;
        index_fixture_segments(&mut store, row.id, entry, embedder)?;
    }
    index_code_fixtures(&mut code_store, suite, embedder, now)?;

    let mut outcomes = Vec::with_capacity(suite.queries.len());
    let mut positive_queries = 0usize;
    let mut negative_queries = 0usize;
    let mut recall_hits = 0usize;
    let mut mrr_sum = 0.0f64;
    let mut false_positives = 0usize;

    for q in &suite.queries {
        let hits = match q.mode {
            EvalMode::Normal => {
                execute_hybrid_query(&store, embedder, &q.query, q.include_semantic, 20, now)?
            }
            EvalMode::Code => {
                execute_code_query(&code_store, embedder, &q.query, q.include_semantic, 20, now)?
            }
        };
        let top_hits: Vec<(String, f64, String)> = hits
            .iter()
            .take(5)
            .map(|h| (h.name.clone(), h.score, h.explanation.clone()))
            .collect();

        if q.expect_empty {
            negative_queries += 1;
            let fp = !hits.is_empty();
            if fp {
                false_positives += 1;
            }
            outcomes.push(QueryEvalOutcome {
                query: q.query.clone(),
                category: q.category.clone(),
                expect_empty: true,
                expected_any: q.expected_any.clone(),
                top_hits,
                ranked_hits: hits.into_iter().take(5).collect(),
                first_relevant_rank: None,
                hit_in_top_5: false,
                reciprocal_rank: 0.0,
                false_positive: fp,
            });
        } else {
            positive_queries += 1;
            let expected_set: HashSet<&str> = q.expected_any.iter().map(|s| s.as_str()).collect();
            let first_rank = hits
                .iter()
                .position(|h| expected_set.contains(h.name.as_str()))
                .map(|idx| idx + 1);
            let hit_top_5 = first_rank.is_some_and(|r| r <= 5);
            let rr = first_rank.map(|r| 1.0 / r as f64).unwrap_or(0.0);
            if hit_top_5 {
                recall_hits += 1;
            }
            mrr_sum += rr;
            outcomes.push(QueryEvalOutcome {
                query: q.query.clone(),
                category: q.category.clone(),
                expect_empty: false,
                expected_any: q.expected_any.clone(),
                top_hits,
                ranked_hits: hits.into_iter().take(5).collect(),
                first_relevant_rank: first_rank,
                hit_in_top_5: hit_top_5,
                reciprocal_rank: rr,
                false_positive: false,
            });
        }
    }

    let recall_at_5 = if positive_queries > 0 {
        recall_hits as f64 / positive_queries as f64
    } else {
        0.0
    };
    let mrr = if positive_queries > 0 {
        mrr_sum / positive_queries as f64
    } else {
        0.0
    };
    let false_positive_rate = if negative_queries > 0 {
        false_positives as f64 / negative_queries as f64
    } else {
        0.0
    };

    let mut modalities: BTreeMap<String, ModalityReport> = BTreeMap::new();
    for outcome in &outcomes {
        let modality = if outcome.category.starts_with("name-") {
            "name"
        } else if outcome.category.starts_with("ocr-") {
            "ocr"
        } else if outcome.category.is_empty() {
            "uncategorized"
        } else {
            &outcome.category
        };
        let group = modalities.entry(modality.to_owned()).or_default();
        if outcome.expect_empty {
            group.negative += 1;
            group.false_positives += usize::from(outcome.false_positive);
        } else {
            group.positive += 1;
            group.hits += usize::from(outcome.hit_in_top_5);
            group.reciprocal_rank_sum += outcome.reciprocal_rank;
        }
    }

    Ok(SuiteReport {
        suite_name: suite.name.clone(),
        embedder_label: embedder.label().to_string(),
        positive_queries,
        negative_queries,
        recall_at_5,
        mrr,
        false_positive_rate,
        thresholds: suite.thresholds.clone(),
        outcomes,
        modalities,
    })
}

#[derive(Debug, Clone)]
pub struct RankedEvalHit {
    pub entry_id: i64,
    pub path: String,
    pub name: String,
    pub score: f64,
    pub explanation: String,
    pub matches: Vec<ws_index::model::ContentMatch>,
    pub code: Option<CodeEvalLocation>,
}

#[derive(Debug, Clone)]
pub struct CodeEvalLocation {
    pub symbol: String,
    pub kind: String,
    pub is_definition: bool,
    pub start_line: u32,
    pub end_line: u32,
    pub notebook: Option<ws_code::model::NotebookLocation>,
}

/// Runs the exact retrieval stage followed by the semantic name stage + hybrid
/// ranker (mirroring `winsearchd::engine` behavior: exact results first,
/// deduplicated semantic results appended in descending hybrid-score order).
pub fn execute_hybrid_query(
    store: &IndexStore,
    embedder: &dyn EvalEmbedder,
    query_text: &str,
    include_semantic: bool,
    limit: usize,
    now_unix: i64,
) -> anyhow::Result<Vec<RankedEvalHit>> {
    execute_hybrid_request(
        store,
        embedder,
        &SearchRequest {
            text: query_text.into(),
            limit,
            ..Default::default()
        },
        include_semantic,
        now_unix,
    )
}

/// The same offline ranker with native request filters applied before caps.
pub fn execute_hybrid_request(
    store: &IndexStore,
    embedder: &dyn EvalEmbedder,
    request: &SearchRequest,
    include_semantic: bool,
    now_unix: i64,
) -> anyhow::Result<Vec<RankedEvalHit>> {
    let text = request.text.trim();
    let limit = request.limit;
    if text.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let exact_hits = store.search(&SearchRequest {
        text: text.to_string(),
        ..request.clone()
    })?;

    let mut exact_ids = HashSet::new();
    let mut results = Vec::new();
    for hit in exact_hits {
        exact_ids.insert(hit.entry_id);
        results.push(RankedEvalHit {
            entry_id: hit.entry_id,
            path: hit.path,
            name: hit.name,
            score: hit.score,
            explanation: hit.explanation,
            matches: hit.matches,
            code: None,
        });
    }

    if !include_semantic {
        return Ok(results);
    }

    let filter = if request.apps_only || request.files_only || !request.kinds.is_empty() {
        let ids =
            store.matching_entry_ids(request.apps_only, request.files_only, &request.kinds)?;
        if ids.is_empty() {
            return Ok(results);
        }
        Some(ws_index::VectorFilter {
            kinds: Vec::new(),
            entry_ids: ids,
        })
    } else {
        None
    };

    let query_vecs = embedder.embed_texts(&[ws_modelmgr::recipe::search_query(text)])?;
    let Some(query_vec) = query_vecs.into_iter().next() else {
        return Ok(results);
    };

    let scored = store.search_vectors(
        &query_vec,
        SEMANTIC_TOP_K.max(limit).saturating_mul(8),
        filter.as_ref(),
    )?;

    let mut best: HashMap<i64, Vec<_>> = HashMap::new();
    for s in &scored {
        if !semantic_admitted(&s.segment.kind, s.cosine) {
            continue;
        }
        best.entry(s.segment.entry_id).or_default().push(s);
    }

    let mut sem_hits = Vec::new();
    for (entry_id, mut matches) in best {
        if exact_ids.contains(&entry_id) {
            continue;
        }
        let Some(entry) = store.get_entry(entry_id)? else {
            continue;
        };
        if entry.missing {
            continue;
        }
        matches.sort_by(|a, b| {
            b.cosine
                .total_cmp(&a.cosine)
                .then_with(|| a.segment.id.cmp(&b.segment.id))
        });
        let cosine = matches[0].cosine;
        let kind = &matches[0].segment.kind;
        let terms: Vec<_> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();
        let mut locations = Vec::new();
        for hit in &matches {
            let segment = &hit.segment;
            if segment.page.is_none() && segment.start_ms.is_none() && segment.line.is_none() {
                continue;
            }
            let location = ws_index::model::ContentMatch {
                excerpt: ws_extract::select_excerpt(&segment.text, &terms, 240),
                page: segment.page.map(|n| n as u32),
                start_ms: segment.start_ms.map(|n| n as u64),
                end_ms: segment.end_ms.map(|n| n as u64),
                line: segment.line.map(|n| n as u32),
                ..Default::default()
            };
            if locations.iter().all(|old: &ws_index::model::ContentMatch| {
                (old.page, old.start_ms, old.end_ms, old.line)
                    != (
                        location.page,
                        location.start_ms,
                        location.end_ms,
                        location.line,
                    )
            }) && locations.len() < 32
            {
                locations.push(location);
            }
        }
        let score = hybrid_score(cosine, text, &entry.name_lower, entry.mtime, now_unix);
        sem_hits.push(RankedEvalHit {
            entry_id,
            path: entry.path,
            name: entry.name,
            score,
            explanation: format!("semantic {kind} match · similarity {cosine:.4}"),
            matches: locations,
            code: None,
        });
    }

    sem_hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.name.cmp(&b.name))
    });
    sem_hits.truncate(limit);
    results.extend(sem_hits);
    Ok(results)
}

/// Concept clusters for the deterministic semantic-concept stub embedder.
/// Each cluster maps synonyms, paraphrases, and multilingual equivalents into
/// dedicated basis dimensions `0..32`, while individual tokens hash into
/// dimensions `32..dim` with low weight so unrelated queries remain nearly
/// orthogonal (`cosine < 0.15`, well below `MIN_SEMANTIC_COSINE = 0.35`).
const CONCEPT_CLUSTERS: &[(&[&str], usize, f32)] = &[
    // 0: Finance / budget / spreadsheets (EN + JA)
    (
        &[
            "budget",
            "spreadsheet",
            "forecast",
            "finance",
            "financial",
            "expense",
            "expenses",
            "tracker",
            "projection",
            "household",
            "sheet",
            "xlsx",
            "csv",
            "予算",
            "計画",
            "年度",
            "家計",
            "支出",
        ],
        0,
        2.5,
    ),
    // 1: Tax returns (EN + DE)
    (
        &[
            "tax",
            "taxes",
            "return",
            "1040",
            "federal",
            "steuer",
            "steuererklärung",
            "einkommensteuer",
            "erklärung",
        ],
        2,
        2.5,
    ),
    // 2: Lease / rental contract (EN + DE)
    (
        &[
            "lease",
            "rental",
            "rent",
            "apartment",
            "agreement",
            "contract",
            "signed",
            "mietvertrag",
            "wohnung",
            "miete",
            "vertrag",
        ],
        4,
        2.5,
    ),
    // 3: Vacation / holiday / beach photos (EN + DE)
    (
        &[
            "vacation",
            "holiday",
            "summer",
            "beach",
            "hawaii",
            "trip",
            "urlaub",
            "sommerurlaub",
            "strand",
            "strandfoto",
            "mallorca",
        ],
        6,
        2.5,
    ),
    // 4: Career / Resume / CV
    (
        &[
            "resume",
            "cv",
            "curriculum",
            "vitae",
            "job",
            "application",
            "career",
            "engineer",
        ],
        8,
        2.5,
    ),
    // 5: Presentations / slides / board deck
    (
        &["presentation", "slides", "deck", "board", "keynote", "pptx"],
        10,
        2.5,
    ),
    // 6: Meeting notes / minutes
    (
        &["meeting", "notes", "minutes", "review", "architecture"],
        12,
        2.5,
    ),
    // 7: Medical / dental / insurance claims
    (
        &[
            "medical",
            "doctor",
            "dental",
            "insurance",
            "claim",
            "reimbursement",
            "health",
        ],
        14,
        2.5,
    ),
    // 8: Cooking / recipe / baking
    (
        &[
            "cooking",
            "recipe",
            "recipes",
            "baking",
            "bread",
            "sourdough",
            "guide",
        ],
        16,
        2.5,
    ),
];

pub fn concept_stub_vector(text: &str, dim: usize) -> Vec<f32> {
    let dim = dim.max(64);
    let mut vec = vec![0.0f32; dim];
    let lower = text.to_lowercase();
    let tokens: Vec<String> = lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect();

    let mut matched_clusters = 0usize;
    for &(keywords, slot, weight) in CONCEPT_CLUSTERS {
        let mut hits = 0usize;
        for &kw in keywords {
            let kw_hit = tokens.iter().any(|t| t == kw || t.contains(kw)) || lower.contains(kw);
            if kw_hit {
                hits += 1;
            }
        }
        if hits > 0 && slot + 1 < dim {
            matched_clusters += 1;
            let boost = weight * (1.0 + 0.25 * (hits.saturating_sub(1) as f32));
            vec[slot] += boost;
            vec[slot + 1] += boost * 0.7;
        }
    }

    // Add deterministic per-token hashed coordinates in `32..dim`.
    // When no concept cluster matched, each token spreads across a unique
    // high-dimensional random direction so non-matching negative queries stay
    // near-orthogonal (`cosine < 0.15`) to all fixture entries.
    let noise_scale = if matched_clusters > 0 { 0.18 } else { 1.0 };
    for (idx, tok) in tokens.iter().enumerate() {
        let h = fnv1a64(tok);
        let bucket = 32 + ((h as usize) % (dim - 32));
        let sign = if (h >> 32) & 1 == 0 { 1.0 } else { -1.0 };
        vec[bucket] += sign * noise_scale;
        // Also spread across a second slot tied to token + position so distinct
        // negative-control sentences don't accidentally collide on a single word.
        let h2 = fnv1a64(&format!("{tok}:{idx}:{lower}"));
        let bucket2 = 32 + ((h2 as usize) % (dim - 32));
        vec[bucket2] += sign * noise_scale * 1.4;
    }

    // Fallback if input had no alphanumeric tokens at all.
    if vec.iter().all(|&x| x == 0.0) {
        return ws_embed_protocol::stub_vector(text, dim);
    }

    let (normed, _) = ws_index::vectors::normalize_vector(&vec);
    normed
}

fn fnv1a64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_4842_2225;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_request_filters_apply_before_exact_and_semantic_limits() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:/Eval").unwrap();
        let generation = store.begin_generation(root).unwrap();
        let records: Vec<_> = [("Report", true), ("report.txt", false)]
            .into_iter()
            .enumerate()
            .map(|(index, (name, is_app))| EntryRecord {
                root_id: root,
                parent_id: None,
                path: format!("C:/Eval/{name}"),
                name: name.into(),
                ext: extension_of(name),
                is_dir: false,
                size: 1,
                mtime: 1_760_000_000,
                volume_serial: 1,
                file_id: index as u64 + 1,
                is_app,
                app_target: String::new(),
                keywords: String::new(),
            })
            .collect();
        store.apply_batch(root, generation, &records).unwrap();
        let vector = concept_stub_vector("financial budget", 768);
        for record in &records {
            let entry = store.find_by_path(&record.path).unwrap().unwrap();
            store
                .upsert_segment(NewSegment {
                    entry_id: entry.id,
                    kind: "name",
                    chunk_index: 0,
                    text: &entry.name,
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &vector,
                })
                .unwrap();
        }
        for (apps_only, files_only, expected) in
            [(false, true, "report.txt"), (true, false, "Report")]
        {
            for (text, semantic) in [("report", false), ("financial budget", true)] {
                let request = SearchRequest {
                    text: text.into(),
                    apps_only,
                    files_only,
                    limit: 1,
                    ..Default::default()
                };
                let hits = execute_hybrid_request(
                    &store,
                    &ConceptStubEmbedder,
                    &request,
                    semantic,
                    1_760_000_000,
                )
                .unwrap();
                assert_eq!(hits.len(), 1, "{text} {apps_only}/{files_only}");
                assert_eq!(hits[0].name, expected, "{text} {apps_only}/{files_only}");
            }
        }
        let no_files = SearchRequest {
            text: "financial budget".into(),
            apps_only: true,
            files_only: true,
            limit: 1,
            ..Default::default()
        };
        assert!(
            execute_hybrid_request(&store, &ConceptStubEmbedder, &no_files, true, 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn expanded_suite_searches_exact_document_content_and_isolates_code_entries() {
        let suite: EvalSuite = serde_json::from_value(serde_json::json!({
            "name":"content-and-code-isolation-fixture",
            "thresholds":{"min_recall_at_5":1.0,"min_mrr":1.0,"max_false_positive_rate":0.0},
            "entries":[
                {"path":"C:/Eval/scan.pdf","name":"scan.pdf","segments":[
                    {"kind":"pdf","page":7,"text":"Warranty claim identifier TOOL-9427."}]},
                {"path":"C:/Eval/cache.rs","name":"cache.rs","segments":[
                    {"kind":"code","line":1,"text":"pub fn evict_oldest() {}"}]}
            ],
            "queries":[
                {"query":"TOOL-9427","category":"pdf-exact","include_semantic":false,"expected_any":["scan.pdf"]},
                {"query":"cache.rs","category":"code-isolation","include_semantic":false,"expect_empty":true}
            ]
        })).expect("explicit content schema must be supported");
        let report = run_eval_suite_with(&suite, &ConceptStubEmbedder).unwrap();
        assert!(report.passed(), "{:#?}", report.outcomes);
        assert_eq!(report.outcomes[0].ranked_hits[0].matches[0].page, Some(7));
        assert_eq!(report.modalities["pdf-exact"].recall(), Some(1.0));
        assert_eq!(report.modalities["pdf-exact"].fp_rate(), None);
        assert_eq!(report.modalities["code-isolation"].recall(), None);
        assert_eq!(report.modalities["code-isolation"].fp_rate(), Some(0.0));
    }

    #[test]
    fn fixture_locations_reject_conflicting_and_wrong_kind_fields() {
        for value in [
            serde_json::json!({"kind":"code","line":1,"page":7,"text":"fn f() {}"}),
            serde_json::json!({"kind":"code","line":1,"start_ms":0,"end_ms":1,"text":"fn f() {}"}),
            serde_json::json!({"kind":"image","asset":"image.png","line":1}),
            serde_json::json!({"kind":"image","asset":"image.png","page":1}),
            serde_json::json!({"kind":"doc","text":"text","start_ms":0,"end_ms":1}),
            serde_json::json!({"kind":"doc","text":"text","page":1}),
            serde_json::json!({"kind":"pdf","text":"text","page":1,"line":1}),
            serde_json::json!({"kind":"audio","asset":"audio.wav","start_ms":0,"end_ms":1,"page":1}),
            serde_json::json!({"kind":"video","asset":"frame.png","start_ms":1,"end_ms":1,"line":1}),
            serde_json::json!({"kind":"ocr","text":"text","page":1,"start_ms":1,"end_ms":1}),
            serde_json::json!({"kind":"doc","text":"text","line":u32::MAX}),
            serde_json::json!({"kind":"ocr","text":"text","line":u32::MAX}),
        ] {
            let segment: FixtureSegment = serde_json::from_value(value.clone()).unwrap();
            assert!(
                validate_fixture_segment(&segment).is_err(),
                "incoherent fixture accepted: {value}"
            );
        }
        for value in [
            serde_json::json!({"kind":"ocr","text":"page OCR","page":7}),
            serde_json::json!({"kind":"ocr","text":"frame OCR","start_ms":12,"end_ms":12}),
            serde_json::json!({"kind":"ocr","text":"image OCR","line":2}),
            serde_json::json!({"kind":"video","asset":"frame.png","start_ms":12,"end_ms":12}),
        ] {
            let segment: FixtureSegment = serde_json::from_value(value.clone()).unwrap();
            assert!(
                validate_fixture_segment(&segment).is_ok(),
                "valid fixture rejected: {value}"
            );
        }
    }

    #[test]
    fn fixture_segments_reject_impossible_code_lines_and_missing_pdf_pages() {
        for value in [
            serde_json::json!({"kind":"code","line":u32::MAX,"text":"fn f() {}"}),
            serde_json::json!({"kind":"code","line":ws_code::MAX_CODE_FILE_BYTES,"text":"a\nb\nc"}),
            serde_json::json!({"kind":"code","text":"fn f() {}"}),
            serde_json::json!({"kind":"pdf","text":"Warranty coverage"}),
        ] {
            let segment: FixtureSegment = serde_json::from_value(value.clone()).unwrap();
            assert!(
                validate_fixture_segment(&segment).is_err(),
                "impossible source location accepted: {value}"
            );
        }
    }

    #[test]
    fn expanded_media_suite_refuses_concept_stub_instead_of_faking_visual_content() {
        let suite: EvalSuite = serde_json::from_value(serde_json::json!({
            "name":"no-fake-media-fixture",
            "thresholds":{"min_recall_at_5":1.0,"min_mrr":1.0,"max_false_positive_rate":0.0},
            "entries":[{"path":"C:/Eval/panel.png","name":"panel.png","segments":[
                {"kind":"image","asset":"Cargo.toml"}]}],
            "queries":[{"query":"wireless adapter error","expected_any":["panel.png"]}]
        }))
        .expect("explicit media schema must be supported");
        let error = run_eval_suite_with(&suite, &ConceptStubEmbedder).unwrap_err();
        assert!(error.to_string().contains("media"), "{error}");
    }

    #[test]
    fn code_fixture_uses_shared_store_and_code_recipe_without_ordinary_name_vectors() {
        struct Recorder(Mutex<Vec<String>>);
        impl EvalEmbedder for Recorder {
            fn label(&self) -> &str {
                "code-store and recipe fixture vectors"
            }
            fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
                self.0.lock().unwrap().extend_from_slice(texts);
                let mut vector = vec![0.0; 768];
                vector[0] = 1.0;
                Ok(texts.iter().map(|_| vector.clone()).collect())
            }
        }
        let suite: EvalSuite = serde_json::from_value(serde_json::json!({
            "name":"code-recipe-fixture",
            "thresholds":{"min_recall_at_5":1.0,"min_mrr":1.0,"max_false_positive_rate":0.0},
            "entries":[{"path":"C:/Eval/cache.rs","name":"cache.rs","segments":[
                {"kind":"code","line":1,"text":"pub fn evict_oldest() {}"}]}],
            "queries":[{"query":"remove the oldest memory entry","category":"code","mode":"code",
                "expected_any":["cache.rs"]}]
        }))
        .unwrap();
        let embedder = Recorder(Mutex::new(Vec::new()));
        let report =
            run_eval_suite_with(&suite, &embedder).expect("code fixture query should execute");
        assert!(report.passed(), "{:#?}", report.outcomes);
        assert_eq!(
            *embedder.0.lock().unwrap(),
            [
                "title: cache.rs | text: pub fn evict_oldest() {}",
                "task: code retrieval | query: remove the oldest memory entry"
            ]
        );
    }

    #[test]
    fn unhandled_eval_fields_are_rejected_instead_of_silently_ignored() {
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(resolve_suite_path("names").unwrap()).unwrap())
                .unwrap();
        for target in ["suite", "entry", "query"] {
            let mut changed = value.clone();
            match target {
                "suite" => changed["unsupported_fixture_kind"] = serde_json::json!(true),
                "entry" => changed["entries"][0]["unsupported_segment"] = serde_json::json!(true),
                "query" => changed["queries"][0]["unsupported_mode"] = serde_json::json!("code"),
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<EvalSuite>(changed).is_err(),
                "{target} fixture semantics were silently dropped"
            );
        }
    }

    #[test]
    fn application_query_uses_search_task_prefix() {
        struct Recorder(Mutex<Vec<String>>);
        impl EvalEmbedder for Recorder {
            fn label(&self) -> &str {
                "format-only fixture"
            }
            fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
                self.0.lock().unwrap().extend_from_slice(texts);
                Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
            }
        }
        let embedder = Recorder(Mutex::new(Vec::new()));
        execute_hybrid_query(
            &IndexStore::open_in_memory().unwrap(),
            &embedder,
            "  find borrowed books  ",
            true,
            20,
            0,
        )
        .unwrap();
        assert_eq!(
            *embedder.0.lock().unwrap(),
            ["task: search result | query: find borrowed books"]
        );
    }

    #[test]
    fn real_reply_rejects_wrong_identity_count_backend_shape_and_nonunit_vectors() {
        use ws_embed_protocol::EmbedResponse;
        let valid = EmbedResponse::Embeddings {
            id: 7,
            vecs: vec![ws_embed_protocol::stub_vector("shape-only", 768)],
            dim: 768,
            backend: "cpu".into(),
            stub: false,
        };
        assert!(real_worker_vectors(Some(valid.clone()), 7, 1).is_ok());
        for case in 0..8 {
            let mut reply = valid.clone();
            let EmbedResponse::Embeddings {
                id,
                vecs,
                dim,
                backend,
                stub,
            } = &mut reply
            else {
                unreachable!()
            };
            match case {
                0 => *id = 8,
                1 => vecs.clear(),
                2 => *stub = true,
                3 => *backend = "stub".into(),
                4 => *dim = 512,
                5 => vecs[0].pop().map(|_| ()).unwrap(),
                6 => vecs[0][0] = f32::NAN,
                7 => vecs[0].fill(0.0),
                _ => unreachable!(),
            }
            assert!(
                real_worker_vectors(Some(reply), 7, 1).is_err(),
                "invalid real reply case{case} accepted"
            );
        }
    }

    #[test]
    fn concept_stub_groups_paraphrases_and_separates_negatives() {
        let q_budget = concept_stub_vector("budget spreadsheet", STUB_DIM);
        let f_budget = concept_stub_vector("2026-annual-budget-forecast.xlsx", STUB_DIM);
        let f_expense = concept_stub_vector("household-expenses-tracker.csv", STUB_DIM);
        let f_unrelated = concept_stub_vector("ubuntu-24.04-desktop-amd64-installer.iso", STUB_DIM);
        let q_neg = concept_stub_vector("photo of a receipt from the hardware store", STUB_DIM);

        let cos_budget = ws_index::vectors::cosine_similarity(&q_budget, &f_budget);
        let cos_expense = ws_index::vectors::cosine_similarity(&q_budget, &f_expense);
        let cos_unrelated = ws_index::vectors::cosine_similarity(&q_budget, &f_unrelated);
        let cos_neg = ws_index::vectors::cosine_similarity(&q_neg, &f_budget);

        assert!(
            cos_budget > 0.70,
            "expected high similarity, got {cos_budget}"
        );
        assert!(
            cos_expense > 0.70,
            "expected high similarity, got {cos_expense}"
        );
        assert!(
            cos_unrelated < MIN_SEMANTIC_COSINE,
            "expected unrelated file below threshold, got {cos_unrelated}"
        );
        assert!(
            cos_neg < MIN_SEMANTIC_COSINE,
            "expected negative control below threshold, got {cos_neg}"
        );
    }

    #[test]
    fn names_eval_suite_meets_all_thresholds() {
        let path = resolve_suite_path("names").expect("locate evals/names.json");
        let suite = load_suite(&path).expect("parse evals/names.json");
        let report =
            run_eval_suite_with(&suite, &ConceptStubEmbedder).expect("run names eval suite");

        assert!(
            report.passed(),
            "names suite failed gates: recall@5={:.3} (min {:.3}), mrr={:.3} (min {:.3}), fp_rate={:.3} (max {:.3})\noutcomes: {:#?}",
            report.recall_at_5,
            report.thresholds.min_recall_at_5,
            report.mrr,
            report.thresholds.min_mrr,
            report.false_positive_rate,
            report.thresholds.max_false_positive_rate,
            report.outcomes
        );
    }
}
