//! `ws-modelmgr`: EmbeddingGemma model manifest, resumable download with
//! streamed SHA-256 verification, local import, and deletion.
//!
//! Layout:
//! - [`manifest`]: pinned model identity (Hugging Face repo, commit, file
//!   sizes), download URLs, and the DB compat record.
//! - [`download`]: blocking `ureq` download with `Range` resume, progress and
//!   cancellation (fits the engine's `std::thread` model; no async).
//! - [`manager`]: [`ModelManager`], the filesystem view (status/import/delete)
//!   used by `winsearchd`'s model service.
//!
//! Model files always live in `ws_platform::paths::model_dir()` and are never
//! committed to the repository.

pub mod download;
pub mod manager;
pub mod manifest;
pub mod recipe;

pub use download::{download_file, DownloadError, DownloadOptions, ProgressFn};
pub use manager::{ManagerError, ModelComponentStatus, ModelManager, ModelState, ModelStatus};
pub use manifest::{
    ExpectedFile, ModelCompat, EMBEDDING_DIM, EMBEDDING_MODEL_META_KEY, EXPECTED_FILES,
    MMPROJ_FILE, MODEL_ID, NPU_BUNDLE_DIR, NPU_BUNDLE_FILES, NPU_MODEL_ID, NPU_REPO_COMMIT,
    REPO_COMMIT, TEXT_FILE,
};
