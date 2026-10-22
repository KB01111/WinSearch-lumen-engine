//! Shared value types for the index.

use std::path::Path;

/// What a stored entry is, for result typing and filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Folder,
    Application,
    Image,
    Screenshot,
    Document,
    Audio,
    Video,
    File,
}

/// Coarse result type of a search hit (mirrors the wire `ResultKind` minus
/// the non-indexed kinds such as `Calculator`/`Settings`, which are
/// synthesized at query time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitKind {
    Application,
    File,
    Folder,
    Image,
    Screenshot,
    Document,
    Audio,
    Video,
}

/// One filesystem (or application) entry to index. All strings are UTF-8
/// paths as produced by [`std::path::Path::to_string_lossy`] callers.
#[derive(Debug, Clone, Default)]
pub struct EntryRecord {
    pub root_id: i64,
    pub parent_id: Option<i64>,
    pub path: String,
    pub name: String,
    pub ext: String,
    pub is_dir: bool,
    pub size: u64,
    /// Unix seconds (mtime).
    pub mtime: i64,
    pub volume_serial: u32,
    pub file_id: u64,
    /// True for catalogued applications (shortcut or executable).
    pub is_app: bool,
    /// Resolved launch target for applications (exe behind a shortcut).
    pub app_target: String,
    /// Extra searchable keywords (application target names etc.).
    pub keywords: String,
}

impl EntryRecord {
    pub fn kind(&self) -> EntryKind {
        if self.is_app {
            return EntryKind::Application;
        }
        if self.is_dir {
            return EntryKind::Folder;
        }
        kind_for_path_and_ext(&self.path, &self.ext)
    }
}

/// Extension (lowercase, no dot) + path → entry kind (flags images in
/// `Screenshots` folders or with `Screenshot*` filenames as [`EntryKind::Screenshot`]).
pub fn kind_for_path_and_ext(path: &str, ext: &str) -> EntryKind {
    let base = kind_for_ext(ext);
    if base == EntryKind::Image && is_screenshot_path_str(path) {
        EntryKind::Screenshot
    } else {
        base
    }
}

/// Returns `true` when `path` is inside a known screenshot directory
/// (`Screenshots`, `Screen Shots`, `Captures`) or its filename starts with
/// `Screenshot` / `Screen Shot` / `Snip_`.
pub fn is_screenshot_path_str(path: &str) -> bool {
    let p = Path::new(path);
    for ancestor in p.ancestors().skip(1) {
        if let Some(folder) = ancestor.file_name().and_then(|n| n.to_str()) {
            let lower = folder.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "screenshots" | "screen shots" | "screencaptures" | "captures"
            ) {
                return true;
            }
        }
    }
    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
        let lower = stem.to_ascii_lowercase();
        if lower.starts_with("screenshot")
            || lower.starts_with("screen shot")
            || lower.starts_with("screen_shot")
            || lower.starts_with("snip_")
        {
            return true;
        }
    }
    false
}

/// Extension (lowercase, no dot) → entry kind.
pub fn kind_for_ext(ext: &str) -> EntryKind {
    let ext = ext.to_ascii_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "webp" | "heic" | "heif" | "avif" | "tiff" | "tif" | "bmp"
        | "gif" | "ico" | "raw" | "cr2" | "cr3" | "nef" | "arw" | "dng" | "orf" | "rw2" => {
            EntryKind::Image
        }
        "mp3" | "wav" | "flac" | "m4a" | "aac" | "ogg" | "wma" | "opus" | "aiff" => {
            EntryKind::Audio
        }
        "mp4" | "mkv" | "mov" | "avi" | "webm" | "wmv" | "m4v" | "mpg" | "mpeg" => EntryKind::Video,
        "pdf" | "doc" | "docx" | "odt" | "rtf" | "txt" | "md" | "xls" | "xlsx" | "ppt" | "pptx"
        | "csv" | "epub" | "mobi" | "one" | "pages" | "numbers" | "key" => EntryKind::Document,
        _ => EntryKind::File,
    }
}

impl EntryKind {
    pub fn hit_kind(self) -> HitKind {
        match self {
            EntryKind::Folder => HitKind::Folder,
            EntryKind::Application => HitKind::Application,
            EntryKind::Image => HitKind::Image,
            EntryKind::Screenshot => HitKind::Screenshot,
            EntryKind::Document => HitKind::Document,
            EntryKind::Audio => HitKind::Audio,
            EntryKind::Video => HitKind::Video,
            EntryKind::File => HitKind::File,
        }
    }
}

/// A retrieved hit, already ranked.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub entry_id: i64,
    pub kind: HitKind,
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub score: f64,
    /// Short human-readable match reason ("Name starts with query", ...).
    pub explanation: String,
    /// Optional cached thumbnail path on disk.
    pub thumbnail_path: Option<String>,
    pub matches: Vec<ContentMatch>,
    /// The result depends on extracted content rather than its name/path.
    pub content_only: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ContentMatch {
    pub excerpt: String,
    /// Original UTF-8 byte offset of the first FTS match; independent of query normalization.
    #[serde(default)]
    pub match_byte: Option<usize>,
    pub page: Option<u32>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub line: Option<u32>,
    pub thumbnail_path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DraftEmbeddingInput {
    #[default]
    Text,
    Image {
        path: std::path::PathBuf,
        token_budget: u32,
    },
    Audio {
        path: std::path::PathBuf,
    },
}

/// Exact content persists independently of optional vectors. Chunk ordinals are
/// stable within one source revision and shared by cursor and vector writes.
#[derive(Debug, Clone)]
pub struct ContentChunk {
    pub id: i64,
    pub entry_id: i64,
    pub job_kind: String,
    pub kind: String,
    pub chunk_index: i64,
    pub text: String,
    pub location: ContentMatch,
    pub embed: bool,
    pub visual: bool,
    pub embedding_input: DraftEmbeddingInput,
    pub attempts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentProgress {
    pub cursor: u64,
    pub complete: bool,
    pub revision: i64,
    pub source: String,
    pub source_sha256: Option<String>,
    pub model_generation: i64,
    pub media_plan: Option<MediaPlan>,
    pub video_selection: Option<VideoSelection>,
}

pub const MAX_MEDIA_DURATION_MS: u64 = 366 * 24 * 60 * 60 * 1000;
pub const MAX_MEDIA_FRAMES: usize = 1200;
pub const MAX_VIDEO_SHOT_SAMPLES: usize = 14400;
pub const VIDEO_GRAY_PIXELS: usize = 32 * 18;
pub const VIDEO_SELECTION_SPAN_MS: u64 = 15_000;
pub const MAX_VIDEO_SELECTION_SPAN_MS: u64 = VIDEO_SELECTION_SPAN_MS * 30;

/// Small, revision-scoped scan checkpoint; no derived file path crosses the worker boundary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VideoSelection {
    pub duration_ms: u64,
    #[serde(default = "default_video_selection_span")]
    pub scan_span_ms: u64,
    pub has_audio: bool,
    pub next_ms: u64,
    pub last_sample_ms: Option<u64>,
    pub last_gray: Vec<u8>,
    pub candidates_ms: Vec<u64>,
    pub next_periodic_ms: u64,
    pub sample_count: u32,
}

fn default_video_selection_span() -> u64 {
    VIDEO_SELECTION_SPAN_MS
}

impl VideoSelection {
    /// Validate one scan step at both the worker and durable commit boundaries.
    pub fn validate_successor(&self, previous: Option<&Self>) -> Result<(), &'static str> {
        self.validate()?;
        if let Some(old) = previous {
            old.validate()?;
        }
        let start = previous.map_or(0, |state| state.next_ms);
        let before = previous.map_or(0, |state| state.sample_count);
        if self.next_ms != (start + self.scan_span_ms).min(self.duration_ms)
            || self.sample_count < before
            || u64::from(self.sample_count - before) > self.scan_span_ms / 1000 + 2
        {
            return Err("video selection skipped a scan slice");
        }
        if let Some(old) = previous {
            if self.duration_ms != old.duration_ms
                || self.has_audio != old.has_audio
                || self.scan_span_ms != old.scan_span_ms
                || !self.candidates_ms.starts_with(&old.candidates_ms)
                || self.next_periodic_ms < old.next_periodic_ms
                || self.last_sample_ms < old.last_sample_ms
            {
                return Err("worker mutated video selection checkpoint");
            }
            if self.sample_count == before {
                let added = &self.candidates_ms[old.candidates_ms.len()..];
                // At EOF the known last sample and one accurately sought tail
                // may be appended even when audio outlasts the video frames.
                let valid_tail = added.is_empty()
                    || (self.next_ms == self.duration_ms
                        && added.len() <= 2
                        && old.last_sample_ms.is_some_and(|last| {
                            added[0] >= last && (added.len() == 1 || added[0] == last)
                        }));
                if self.last_sample_ms != old.last_sample_ms
                    || self.last_gray != old.last_gray
                    || self.next_periodic_ms != old.next_periodic_ms
                    || !valid_tail
                {
                    return Err("worker mutated video selection checkpoint");
                }
            } else if self.last_sample_ms <= old.last_sample_ms {
                return Err("video samples did not advance");
            }
        }
        if self.sample_count > before
            && (self.last_sample_ms.is_none_or(|time| time < start)
                || self
                    .candidates_ms
                    .iter()
                    .skip(previous.map_or(0, |old| old.candidates_ms.len()))
                    .any(|time| *time < start))
        {
            return Err("video samples precede scan slice");
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.duration_ms == 0
            || self.duration_ms > MAX_MEDIA_DURATION_MS
            || self.scan_span_ms < VIDEO_SELECTION_SPAN_MS
            || self.scan_span_ms > MAX_VIDEO_SELECTION_SPAN_MS
            || self.scan_span_ms % VIDEO_SELECTION_SPAN_MS != 0
            || self.next_ms == 0
            || self.next_ms > self.duration_ms
            || (self.next_ms != self.duration_ms && self.next_ms % self.scan_span_ms != 0)
            || self.sample_count as usize > MAX_VIDEO_SHOT_SAMPLES
            || self.candidates_ms.len() > MAX_VIDEO_SHOT_SAMPLES + 2
            || self.last_gray.len()
                != if self.last_sample_ms.is_some() {
                    VIDEO_GRAY_PIXELS
                } else {
                    0
                }
            || self.last_sample_ms.is_some_and(|t| t >= self.next_ms)
            || self.candidates_ms.iter().any(|t| *t >= self.next_ms)
            || self.candidates_ms.windows(2).any(|p| p[0] >= p[1])
            || (self.sample_count == 0) != self.last_sample_ms.is_none()
            || (self.sample_count == 0
                && (!self.candidates_ms.is_empty() || self.next_periodic_ms != 0))
            || (self.sample_count > 0 && self.candidates_ms.is_empty())
            || (self.next_ms < self.duration_ms
                && self
                    .last_sample_ms
                    .is_some_and(|t| self.candidates_ms.last().is_some_and(|c| *c > t)))
            || self.next_periodic_ms
                > self.duration_ms.saturating_add(
                    self.duration_ms
                        .div_ceil((MAX_MEDIA_FRAMES - 1) as u64)
                        .max(5000),
                )
        {
            return Err("invalid video selection checkpoint");
        }
        Ok(())
    }
}

/// Pure data, scoped to the source revision; no worker-controlled manifest path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaPlan {
    pub duration_ms: u64,
    #[serde(default)]
    pub frames_ms: Vec<u64>,
    pub has_audio: bool,
}

impl MediaPlan {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.duration_ms == 0
            || self.duration_ms > MAX_MEDIA_DURATION_MS
            || self.frames_ms.len() > MAX_MEDIA_FRAMES
            || (!self.has_audio && self.frames_ms.is_empty())
        {
            return Err("invalid media duration/stream/frame bound");
        }
        if !self.frames_ms.is_empty()
            && (self.frames_ms.iter().any(|t| *t >= self.duration_ms)
                || self.frames_ms.windows(2).any(|p| p[1] <= p[0] + 500))
        {
            return Err("media selection is outside source or not deduplicated");
        }
        Ok(())
    }
}

/// Search request against the index (calculator and settings results are
/// synthesized by the engine, not stored).
#[derive(Debug, Clone, Default)]
pub struct SearchRequest {
    pub text: String,
    pub extensions: Vec<String>,
    pub file_kinds: Vec<ws_protocol::FileKind>,
    /// Restrict to applications (`Some(true)`) or to non-apps (`Some(false)`).
    pub apps_only: bool,
    pub files_only: bool,
    /// Restrict to these hit kinds; empty means all.
    pub kinds: Vec<HitKind>,
    /// Maximum hits returned after ranking.
    pub limit: usize,
}

/// Outcome of upserting one entry (used for counters and test assertions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    Inserted,
    Updated,
    Unchanged,
}

/// Kind string for name-embedding segments (Phase 3: one per entry).
/// Stored in `segments.kind` / `extract_jobs.kind`.
pub const SEGMENT_KIND_NAME: &str = "name";
/// Kind string for document text segments (`txt`, `md`, `docx`, `odt`, `rtf`, `html`, IFilter).
pub const SEGMENT_KIND_DOC: &str = "doc";
/// Kind string for image visual / metadata segments.
pub const SEGMENT_KIND_IMAGE: &str = "image";
/// Kind string for OCR text segments extracted from images or scanned pages.
pub const SEGMENT_KIND_OCR: &str = "ocr";
/// Kind string for PDF page text segments.
pub const SEGMENT_KIND_PDF: &str = "pdf";
/// Kind string for audio window segments.
pub const SEGMENT_KIND_AUDIO: &str = "audio";
/// Kind string for video frame / audio segments.
pub const SEGMENT_KIND_VIDEO: &str = "video";

/// All non-name content segment kinds produced by Phase 4 extractors.
pub const CONTENT_SEGMENT_KINDS: &[&str] = &[
    SEGMENT_KIND_DOC,
    SEGMENT_KIND_IMAGE,
    SEGMENT_KIND_OCR,
    SEGMENT_KIND_PDF,
    SEGMENT_KIND_AUDIO,
    SEGMENT_KIND_VIDEO,
];

/// All valid segment kinds (`name` + [`CONTENT_SEGMENT_KINDS`]).
pub const ALL_SEGMENT_KINDS: &[&str] = &[
    SEGMENT_KIND_NAME,
    SEGMENT_KIND_DOC,
    SEGMENT_KIND_IMAGE,
    SEGMENT_KIND_OCR,
    SEGMENT_KIND_PDF,
    SEGMENT_KIND_AUDIO,
    SEGMENT_KIND_VIDEO,
];

/// Priority for `extract_jobs.priority`: new files first, LIFO within priority.
///
/// `claim_extract_job` orders by `priority DESC, id DESC`: higher priority
/// drains first, and within one priority the most recently enqueued job wins
/// (LIFO). Newly inserted entries (`UpsertOutcome::Inserted`) enqueue at
/// `EXTRACT_PRIORITY_NEW`; updates, re-embeds, and model-swap requeues use
/// `EXTRACT_PRIORITY_UPDATED` so fresh discoveries jump the queue.
pub const EXTRACT_PRIORITY_NEW: i64 = 10;
pub const EXTRACT_PRIORITY_UPDATED: i64 = 0;

/// Computes the priority score for a content extraction job so that small
/// files and recently modified files are extracted before large or old files
/// (`priority DESC, id DESC`).
///
/// Components:
/// - New vs Updated bonus (`+100_000` for newly discovered files).
/// - Size bucket (`0..=60_000`, smaller files score higher: ≤64 KiB → +60_000,
///   ≤1 MiB → +45_000, ≤10 MiB → +30_000, ≤100 MiB → +15_000, >100 MiB → 0;
///   plus fine-grained sub-bucket ordering so a 1 KiB file beats a 32 KiB file).
/// - Recency tie-breaker (`mtime` clamped to non-negative seconds, scaled so
///   more recent files always rank above older files in the same size tier).
pub fn compute_extract_priority(size: u64, mtime: i64, outcome: UpsertOutcome) -> i64 {
    let outcome_bonus: i64 = match outcome {
        UpsertOutcome::Inserted => 1_000_000_000_000,
        UpsertOutcome::Updated | UpsertOutcome::Unchanged => 0,
    };
    let size_tier: i64 = match size {
        0..=65_536 => 400_000_000_000 - (size as i64),
        65_537..=1_048_576 => 300_000_000_000 - (size as i64 / 16),
        1_048_577..=10_485_760 => 200_000_000_000 - (size as i64 / 256),
        10_485_761..=104_857_600 => 100_000_000_000 - (size as i64 / 4096),
        _ => -(size.min(50_000_000_000) as i64 / 65_536),
    };
    let recency: i64 = mtime.clamp(0, 9_999_999_999);
    outcome_bonus + size_tier + recency
}

/// How many name texts the winsearchd embed scheduler batches into one
/// worker round-trip (P3.5). Well under `MAX_TEXTS_PER_JOB` (256) so one
/// pipeline batch is always one worker job.
pub const EMBED_BATCH_SIZE: usize = 128;

/// States for `extract_jobs.state`.
pub const EXTRACT_STATE_PENDING: &str = "pending";
pub const EXTRACT_STATE_CLAIMED: &str = "claimed";
pub const EXTRACT_STATE_FAILED: &str = "failed";

/// One embedded segment: a chunk of an entry with its vector.
///
/// Vectors are stored in SQLite as FP16 little-endian bytes plus the L2 norm
/// of the original full-precision vector. In Rust they are exposed decoded as
/// normalized `f32` values; `norm` is `0.0` for zero vectors.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub id: i64,
    pub entry_id: i64,
    pub kind: String,
    pub chunk_index: i64,
    pub text: String,
    pub page: Option<i64>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub line: Option<i64>,
    /// Normalized embedding (unit length unless `norm == 0.0`).
    pub vector: Vec<f32>,
    /// L2 norm of the original vector before normalization.
    pub norm: f32,
}

/// Input for inserting or replacing a segment. The vector is full-precision
/// `f32`; the store normalizes it, encodes FP16, and persists the norm.
#[derive(Debug, Clone)]
pub struct NewSegment<'a> {
    pub entry_id: i64,
    pub kind: &'a str,
    pub chunk_index: i64,
    pub text: &'a str,
    pub page: Option<i64>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub line: Option<i64>,
    pub vector: &'a [f32],
}

/// One row of the `extract_jobs` queue (reused by Phase 4 extractors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractJob {
    pub id: i64,
    pub entry_id: i64,
    pub kind: String,
    pub state: String,
    pub claimed: bool,
    pub attempts: i64,
    /// Scheduling priority: higher drains first (`priority DESC, id DESC`
    /// claim order gives new-files-first, LIFO within priority).
    pub priority: i64,
}

/// Filter for vector search. Empty lists mean "no restriction".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorFilter {
    /// Restrict to these segment kinds; empty means all kinds.
    pub kinds: Vec<String>,
    /// Restrict to these entries; empty means all entries.
    pub entry_ids: Vec<i64>,
}

impl VectorFilter {
    /// Convenience for a single-kind filter (e.g. `name` segments).
    pub fn for_kind(kind: &str) -> Self {
        Self {
            kinds: vec![kind.to_string()],
            entry_ids: Vec::new(),
        }
    }

    /// Convenience for a multi-kind filter (e.g. all content segments or
    /// `ALL_SEGMENT_KINDS`).
    pub fn for_kinds(kinds: &[&str]) -> Self {
        Self {
            kinds: kinds.iter().map(|k| (*k).to_string()).collect(),
            entry_ids: Vec::new(),
        }
    }

    /// Filter matching all non-name content segment kinds (`doc`, `image`,
    /// `ocr`, `pdf`, `audio`, `video`).
    pub fn content_kinds() -> Self {
        Self::for_kinds(CONTENT_SEGMENT_KINDS)
    }

    pub fn matches_kind(&self, kind: &str) -> bool {
        self.kinds.is_empty() || self.kinds.iter().any(|k| k == kind)
    }

    pub fn matches_entry(&self, entry_id: i64) -> bool {
        self.entry_ids.is_empty() || self.entry_ids.contains(&entry_id)
    }
}

/// A segment paired with its cosine similarity to the query.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredSegment {
    pub segment: Segment,
    pub cosine: f32,
}

/// Which inference backend the embed worker should prefer. Recorded in the
/// `config` table (`preferred_backend`); the worker itself still reads
/// `WS_EMBED_BACKEND`/its CLI flag, so today this is a stored preference the
/// UI can display and a future worker-selection path can honor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PreferredBackend {
    #[default]
    Auto,
    /// AMD NPU first; GPU/CPU without one (same order as `Auto` after it).
    Npu,
    Cuda,
    Vulkan,
    Cpu,
}

impl PreferredBackend {
    /// Canonical wire/store spelling (`auto|npu|cuda|vulkan|cpu`).
    pub fn as_str(self) -> &'static str {
        match self {
            PreferredBackend::Auto => "auto",
            PreferredBackend::Npu => "npu",
            PreferredBackend::Cuda => "cuda",
            PreferredBackend::Vulkan => "vulkan",
            PreferredBackend::Cpu => "cpu",
        }
    }

    /// Parses a canonical spelling (ASCII case-insensitive); anything else is
    /// rejected so `set_config` fails closed on typos instead of storing them.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Some(PreferredBackend::Auto),
            "npu" => Some(PreferredBackend::Npu),
            "cuda" => Some(PreferredBackend::Cuda),
            "vulkan" => Some(PreferredBackend::Vulkan),
            "cpu" => Some(PreferredBackend::Cpu),
            _ => None,
        }
    }
}

/// Engine-wide search policy persisted in the SQLite `config` table
/// (P3.6, Task 6). Additive by design: unknown keys are ignored, missing keys
/// read back as defaults, and `meta.schema_version` is untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// Whether the semantic query stage may run. Defaults to true; the query
    /// path additionally requires the embedding model to be ready, so the
    /// effective default is "true once model ready".
    pub semantic_enabled: bool,
    pub preferred_backend: PreferredBackend,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            semantic_enabled: true,
            preferred_backend: PreferredBackend::Auto,
        }
    }
}

/// Splits a path into its file name (lossy UTF-8).
pub fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

/// Lowercase extension without the dot.
pub fn extension_of(name: &str) -> String {
    Path::new(name)
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_mapping_covers_media_and_documents() {
        assert_eq!(kind_for_ext("PNG"), EntryKind::Image);
        assert_eq!(kind_for_ext("mp3"), EntryKind::Audio);
        assert_eq!(kind_for_ext("mkv"), EntryKind::Video);
        assert_eq!(kind_for_ext("docx"), EntryKind::Document);
        assert_eq!(kind_for_ext("rs"), EntryKind::File);
        assert_eq!(kind_for_ext(""), EntryKind::File);
    }

    #[test]
    fn entry_kind_priority() {
        let rec = EntryRecord {
            root_id: 1,
            parent_id: None,
            path: "C:\\x\\Code.lnk".into(),
            name: "Code.lnk".into(),
            ext: "lnk".into(),
            is_dir: false,
            size: 1,
            mtime: 0,
            volume_serial: 0,
            file_id: 0,
            is_app: true,
            app_target: String::new(),
            keywords: String::new(),
        };
        assert_eq!(rec.kind(), EntryKind::Application);
    }

    #[test]
    fn backend_spellings_round_trip_and_reject_unknown() {
        for (text, want) in [
            ("auto", PreferredBackend::Auto),
            ("npu", PreferredBackend::Npu),
            ("NPU", PreferredBackend::Npu),
            ("cuda", PreferredBackend::Cuda),
            ("vulkan", PreferredBackend::Vulkan),
            ("cpu", PreferredBackend::Cpu),
            ("CUDA", PreferredBackend::Cuda),
        ] {
            let parsed = PreferredBackend::parse(text).expect("canonical spelling must parse");
            assert_eq!(parsed, want);
            assert_eq!(parsed.as_str(), want.as_str());
        }
        assert_eq!(PreferredBackend::parse("opencl"), None);
        assert_eq!(PreferredBackend::parse(""), None);
    }

    #[test]
    fn engine_config_defaults_to_semantic_on_auto_backend() {
        let cfg = EngineConfig::default();
        assert!(cfg.semantic_enabled);
        assert_eq!(cfg.preferred_backend, PreferredBackend::Auto);
    }
}
