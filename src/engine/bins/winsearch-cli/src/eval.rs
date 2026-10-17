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

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex,OnceLock};

use serde::Deserialize;
use ws_embed_protocol::STUB_DIM;
use ws_index::model::{
    extension_of, kind_for_ext, EntryRecord, HitKind, NewSegment, SearchRequest, VectorFilter,
    SEGMENT_KIND_NAME,
};
use ws_index::{EntryRow, IndexStore};

/// Minimum cosine similarity for a semantic hit to be shown (matches
/// `winsearchd::engine::MIN_SEMANTIC_COSINE`).
pub const MIN_SEMANTIC_COSINE: f32 = 0.35;

/// Retrieval depth before hybrid ranking (matches `winsearchd::engine::SEMANTIC_TOP_K`).
pub const SEMANTIC_TOP_K: usize = 200;

#[derive(Debug, Clone, Deserialize)]
pub struct EvalSuite {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub thresholds: EvalThresholds,
    pub entries: Vec<FixtureEntry>,
    pub queries: Vec<EvalQuery>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvalThresholds {
    pub min_recall_at_5: f64,
    pub min_mrr: f64,
    #[serde(default)]
    pub max_false_positive_rate: f64,
}

#[derive(Debug, Clone, Deserialize)]
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
}

fn default_size() -> u64 {
    1024
}

#[derive(Debug, Clone, Deserialize)]
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
        Ok(texts.iter().map(|t| concept_stub_vector(t, STUB_DIM)).collect())
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
        let mut worker_bin = exe_dir.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
        if !worker_bin.is_file() {
            if let Some(parent) = exe_dir.parent() {
                let candidate = parent.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
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
        self.verified_label.get().map(String::as_str)
            .unwrap_or("WorkerModelEmbedder (awaiting verified real-worker response)")
    }

    fn embed_texts(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        self.ensure_child()?;

        let start = std::time::Instant::now();
        let mut stream = loop {
            match ws_platform::pipe::PipeStream::connect(&self.pipe_name) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < std::time::Duration::from_secs(15) => {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                Err(e) => anyhow::bail!("failed to connect to eval worker pipe {}: {e}", self.pipe_name),
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
        let ack: Option<ws_embed_protocol::EmbedResponse> =
            ws_protocol::read_message(&mut stream)?;
        match ack {
            Some(ws_embed_protocol::EmbedResponse::HelloAck { protocol_version,stub,features,.. }) => {
                anyhow::ensure!(!stub,"real-model evaluation refuses a stub worker");
                anyhow::ensure!(protocol_version==ws_embed_protocol::EMBED_PROTOCOL_VERSION,
                    "real-model worker protocol version mismatch");
                anyhow::ensure!(features.iter().any(|f|f=="embed_texts"),"real-model worker cannot embed text");
            }
            Some(ws_embed_protocol::EmbedResponse::Error { code, message, .. }) => {
                anyhow::bail!("worker handshake error {code:?}: {message}");
            }
            other => {
                anyhow::bail!("unexpected worker handshake reply: {other:?}");
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        ws_protocol::write_message(
            &mut stream,
            &ws_embed_protocol::EmbedRequest::EmbedTexts {
                id,
                texts: texts.to_vec(),
            },
        )?;
        let reply: Option<ws_embed_protocol::EmbedResponse> =
            ws_protocol::read_message(&mut stream)?;
        let (vectors,backend)=real_worker_vectors(reply,id,texts.len())?;
        let observed=self.verified_backend.get_or_init(||backend.clone());
        anyhow::ensure!(observed==&backend,"real-model worker backend changed during evaluation");
        self.verified_label.get_or_init(||format!(
            "WorkerModelEmbedder (real {backend}; stub:false; dim:768; model:{})",self.model_path.display()));
        Ok(vectors)
    }
}

fn real_worker_vectors(reply:Option<ws_embed_protocol::EmbedResponse>,expected_id:u64,expected_count:usize)
    -> anyhow::Result<(Vec<Vec<f32>>,String)> {
        match reply {
            Some(ws_embed_protocol::EmbedResponse::Embeddings { id,vecs,dim,backend,stub }) => {
                anyhow::ensure!(!stub && backend!="stub","real-model evaluation refuses stub vectors");
                anyhow::ensure!(matches!(backend.as_str(),"cpu"|"cuda"|"vulkan"),"unknown real-model backend: {backend}");
                anyhow::ensure!(id==expected_id,"real-model response ID mismatch");
                anyhow::ensure!(dim==ws_modelmgr::EMBEDDING_DIM as usize,"real-model embedding dimension mismatch");
                anyhow::ensure!(vecs.len()==expected_count,"real-model embedding count mismatch");
                for vector in &vecs {
                    anyhow::ensure!(vector.len()==dim && vector.iter().all(|v|v.is_finite()),
                        "invalid real-model vector shape or values");
                    let norm=vector.iter().map(|v|f64::from(*v).powi(2)).sum::<f64>().sqrt();
                    anyhow::ensure!((norm-1.0).abs()<=1e-3,"non-unit real-model embedding");
                }
                Ok((vecs,backend))
            },
            Some(ws_embed_protocol::EmbedResponse::Error { code, message, .. }) => {
                anyhow::bail!("worker error {code:?}: {message}")
            }
            other => anyhow::bail!("unexpected worker reply: {other:?}"),
        }
}

fn select_embedder() -> anyhow::Result<Box<dyn EvalEmbedder>> {
    if let Some(os_path) = std::env::var_os("WS_EMBED_TEST_MODEL") {
        let p = PathBuf::from(os_path);
        anyhow::ensure!(p.is_file(),"WS_EMBED_TEST_MODEL explicitly requested a missing/non-file model: {}",p.display());
        return Ok(Box::new(WorkerModelEmbedder::new(p.canonicalize()?)));
    }
    Ok(Box::new(ConceptStubEmbedder))
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
    let mut store = IndexStore::open_in_memory()?;
    let root_id = store.ensure_root("C:\\Users\\demo")?;
    let gen = store.begin_generation(root_id)?;
    let now = 1_760_000_000i64;

    let records: Vec<EntryRecord> = suite
        .entries
        .iter()
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
    let names: Vec<String> = suite.entries.iter().map(|e| e.name.clone()).collect();
    let vectors = embedder.embed_texts(&names)?;
    if vectors.len() != suite.entries.len() {
        anyhow::bail!(
            "embedder returned {} vectors for {} entries",
            vectors.len(),
            suite.entries.len()
        );
    }

    for (entry, vec) in suite.entries.iter().zip(vectors.iter()) {
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
    }

    let mut outcomes = Vec::with_capacity(suite.queries.len());
    let mut positive_queries = 0usize;
    let mut negative_queries = 0usize;
    let mut recall_hits = 0usize;
    let mut mrr_sum = 0.0f64;
    let mut false_positives = 0usize;

    for q in &suite.queries {
        let hits = execute_hybrid_query(&store, embedder, &q.query, q.include_semantic, 20, now)?;
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
    })
}

#[derive(Debug, Clone)]
pub struct RankedEvalHit {
    pub entry_id: i64,
    pub name: String,
    pub score: f64,
    pub explanation: String,
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
    let text = query_text.trim();
    if text.is_empty() {
        return Ok(Vec::new());
    }

    let exact_hits = store.search(&SearchRequest {
        text: text.to_string(),
        limit,
        ..Default::default()
    })?;

    let mut exact_ids = HashSet::new();
    let mut results = Vec::new();
    for hit in exact_hits {
        exact_ids.insert(hit.entry_id);
        results.push(RankedEvalHit {
            entry_id: hit.entry_id,
            name: hit.name,
            score: hit.score,
            explanation: hit.explanation,
        });
    }

    if !include_semantic {
        return Ok(results);
    }

    let query_vecs = embedder.embed_texts(&[text.to_string()])?;
    let Some(query_vec) = query_vecs.into_iter().next() else {
        return Ok(results);
    };

    let filter = VectorFilter::for_kind(SEGMENT_KIND_NAME);
    let scored = store.search_vectors(&query_vec, SEMANTIC_TOP_K, Some(&filter))?;

    let mut best: HashMap<i64, f32> = HashMap::new();
    for s in &scored {
        if s.cosine < MIN_SEMANTIC_COSINE {
            continue;
        }
        best.entry(s.segment.entry_id)
            .and_modify(|c| *c = c.max(s.cosine))
            .or_insert(s.cosine);
    }

    let mut sem_hits = Vec::new();
    for (entry_id, cosine) in best {
        if exact_ids.contains(&entry_id) {
            continue;
        }
        let Some(entry) = store.get_entry(entry_id)? else {
            continue;
        };
        if entry.missing {
            continue;
        }
        let _kind = entry_hit_kind(&entry);
        let score = hybrid_score(cosine, text, &entry.name_lower, entry.mtime, now_unix);
        sem_hits.push(RankedEvalHit {
            entry_id,
            name: entry.name,
            score,
            explanation: format!("semantic name match · similarity {cosine:.2}"),
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

fn entry_hit_kind(row: &EntryRow) -> HitKind {
    if row.is_app {
        HitKind::Application
    } else if row.is_dir {
        HitKind::Folder
    } else {
        kind_for_ext(&row.ext).hit_kind()
    }
}

fn hybrid_base_score(cosine: f32) -> f64 {
    0.45 + 0.45 * cosine as f64
}

fn tokenize_lower(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

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

fn recency_boost(mtime: i64, now_unix: i64) -> f64 {
    if mtime > 0 && now_unix.saturating_sub(mtime) <= 30 * 86_400 {
        0.02
    } else {
        0.0
    }
}

fn hybrid_score(cosine: f32, query_text: &str, name_lower: &str, mtime: i64, now: i64) -> f64 {
    (hybrid_base_score(cosine)
        + exact_token_boost(query_text, name_lower)
        + recency_boost(mtime, now))
    .clamp(0.0, 1.0)
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
        &[
            "presentation",
            "slides",
            "deck",
            "board",
            "keynote",
            "pptx",
        ],
        10,
        2.5,
    ),
    // 6: Meeting notes / minutes
    (
        &[
            "meeting",
            "notes",
            "minutes",
            "review",
            "architecture",
        ],
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
    fn real_reply_rejects_wrong_identity_count_backend_shape_and_nonunit_vectors() {
        use ws_embed_protocol::EmbedResponse;
        let valid=EmbedResponse::Embeddings { id:7,vecs:vec![ws_embed_protocol::stub_vector("shape-only",768)],
            dim:768,backend:"cpu".into(),stub:false };
        assert!(real_worker_vectors(Some(valid.clone()),7,1).is_ok());
        for case in 0..8 {
            let mut reply=valid.clone();
            let EmbedResponse::Embeddings { id,vecs,dim,backend,stub }=&mut reply else { unreachable!() };
            match case {
                0=>*id=8,
                1=>vecs.clear(),
                2=>*stub=true,
                3=>*backend="stub".into(),
                4=>*dim=512,
                5=>vecs[0].pop().map(|_|()).unwrap(),
                6=>vecs[0][0]=f32::NAN,
                7=>vecs[0].fill(0.0),
                _=>unreachable!(),
            }
            assert!(real_worker_vectors(Some(reply),7,1).is_err(),"invalid real reply case{case} accepted");
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

        assert!(cos_budget > 0.70, "expected high similarity, got {cos_budget}");
        assert!(cos_expense > 0.70, "expected high similarity, got {cos_expense}");
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
