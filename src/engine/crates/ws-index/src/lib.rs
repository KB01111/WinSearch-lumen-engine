//! SQLite/FTS5-backed search index for WinSearch.
//!
//! This crate owns everything that touches the index database: schema and
//! migrations, entry upserts with filesystem identity, exact (name/path)
//! retrieval with launcher-style ranking, the calculator evaluator, and the
//! Windows settings URI catalog. It has no Windows dependencies so the whole
//! retrieval path is testable on any machine; filesystem walking and watching
//! live in the `winsearchd` binary.

pub mod calc;
mod enrichment;
pub use enrichment::{EnrichmentBatch, EnrichmentCommit};
pub mod model;
pub mod retrieval;
pub mod search;
pub mod settings;
pub mod store;
pub mod vectors;

pub use calc::{evaluate, CalcError, CalcResult};
pub use model::{
    compute_extract_priority, is_screenshot_path_str, kind_for_path_and_ext, EngineConfig,
    EntryKind, EntryRecord, ExtractJob, HitKind, NewSegment, PreferredBackend, ScoredSegment,
    SearchHit, SearchRequest, Segment, UpsertOutcome, VectorFilter, ALL_SEGMENT_KINDS,
    CONTENT_SEGMENT_KINDS, EMBED_BATCH_SIZE, EXTRACT_PRIORITY_NEW, EXTRACT_PRIORITY_UPDATED,
    EXTRACT_STATE_CLAIMED, EXTRACT_STATE_FAILED, EXTRACT_STATE_PENDING, SEGMENT_KIND_AUDIO,
    SEGMENT_KIND_DOC, SEGMENT_KIND_IMAGE, SEGMENT_KIND_NAME, SEGMENT_KIND_OCR, SEGMENT_KIND_PDF,
    SEGMENT_KIND_VIDEO,
};
pub use settings::{search_settings, SettingsHit};
pub use store::{EntryRow, IndexStats, IndexStore, Root, StoreError};
pub use vectors::{VectorError, VectorHit, VectorIndex};
mod query_cache;
