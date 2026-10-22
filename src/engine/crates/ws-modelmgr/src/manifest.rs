//! Pinned model manifest: which files make up the embedding model, where to
//! fetch them, and how to recognise a compatible install afterwards.
//!
//! ## How the constants were obtained
//!
//! - File list and sizes come from the Hugging Face repo metadata for
//!   [`MODEL_ID`] at commit [`REPO_COMMIT`]: `HEAD` requests against
//!   `https://huggingface.co/<id>/resolve/main/<file>` return
//!   `X-Linked-Size` (309855456 for the text GGUF, 554821024 for mmproj).
//! - SHA-256 pins are independently checked against the downloaded bytes for
//!   this exact revision. They also match the linked LFS hashes in the public
//!   model metadata. Imports never trust a filename or size alone.
//!   Downloads and imports are therefore hash-verified, not just size-checked.
//! - `EMBEDDING_DIM` (768) is the unified EmbeddingGemma 2 space recorded in
//!   the plan; the compat record below binds the index to it.

use serde::{Deserialize, Serialize};

/// Hugging Face repo the GGUFs are downloaded from.
pub const MODEL_ID: &str = "ggml-org/embeddinggemma-2-GGUF";

/// Repo commit the sizes below were read at (`X-Repo-Commit` header).
pub const REPO_COMMIT: &str = "bfcd298762cc34d0357ece5ebdd31791a3a374d8";

/// Unified EmbeddingGemma 2 embedding width.
pub const EMBEDDING_DIM: u32 = 768;

/// One-pass media plus task-aware application query/document inputs.
pub const VECTOR_PIPELINE_VERSION: u32 = 2;

/// Text-embedding weights (Q8_0, ~310 MB).
pub const TEXT_FILE: &str = "embeddinggemma-2-Q8_0.gguf";

/// Multimodal projector (Q8_0, ~555 MB).
pub const MMPROJ_FILE: &str = "mmproj-embeddinggemma-2-Q8_0.gguf";

/// `meta` key holding the [`ModelCompat`] JSON record in `index.db`.
pub const EMBEDDING_MODEL_META_KEY: &str = "embedding_model";

/// One file that makes up the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedFile {
    /// File name inside the model directory.
    pub name: &'static str,
    /// Exact byte size (`X-Linked-Size` at [`REPO_COMMIT`]).
    pub size: u64,
    /// Expected lowercase hex SHA-256 (see module docs). Verified on
    /// download/import whenever present.
    pub sha256: Option<&'static str>,
}

impl ExpectedFile {
    /// `https://huggingface.co/<repo>/resolve/<commit>/<path>`: the GGUFs come
    /// from [`MODEL_ID`] at [`REPO_COMMIT`]; files under [`NPU_BUNDLE_DIR`]
    /// come from [`NPU_MODEL_ID`] at [`NPU_REPO_COMMIT`], without the prefix.
    pub fn url(&self) -> String {
        match self.npu_bundle_path() {
            Some(path) => {
                format!("https://huggingface.co/{NPU_MODEL_ID}/resolve/{NPU_REPO_COMMIT}/{path}")
            }
            None => format!(
                "https://huggingface.co/{MODEL_ID}/resolve/{REPO_COMMIT}/{}",
                self.name
            ),
        }
    }

    /// The repository-relative path of an NPU bundle file, `None` for a GGUF.
    pub fn npu_bundle_path(&self) -> Option<&'static str> {
        self.name
            .strip_prefix(NPU_BUNDLE_DIR)
            .and_then(|rest| rest.strip_prefix('/'))
    }
}

/// AMD Ryzen AI NPU2 (AIE2P: Strix Point, Strix Halo, Krackan) export of the
/// same `google/embeddinggemma-2` weights: compiled IRON kernels plus packed
/// bf16 weights, replayed by `taconite-embeddinggemma2`. It is the primary
/// text/image model when an NPU2 is present; the GGUFs above stay required
/// for audio and as the GPU/CPU fallback.
pub const NPU_MODEL_ID: &str = "brishen/iron-embeddinggemma2-npu2";

/// Bundle revision the sizes and hashes in [`NPU_BUNDLE_FILES`] were read at.
pub const NPU_REPO_COMMIT: &str = "052b6a44fe2478d86aec2cd65fbd03ea24279a9e";

/// Subdirectory of the model directory that holds the bundle.
pub const NPU_BUNDLE_DIR: &str = "iron-embeddinggemma2-npu2";

include!("npu_bundle_files.rs");

/// Both GGUFs, in download order (text weights first).
pub const EXPECTED_FILES: [ExpectedFile; 2] = [
    ExpectedFile {
        name: TEXT_FILE,
        size: 309_855_456,
        sha256: Some("2188ac1deca4b77dffefd603c2776a9d76d9d74ec01841392982ebb840b09135"),
    },
    ExpectedFile {
        name: MMPROJ_FILE,
        size: 554_821_024,
        sha256: Some("c4a8a52691ecef40618438928bdf9e68379b854e24166f292592353db0aab64f"),
    },
];

/// Combined download size (865 MB class).
pub const TOTAL_BYTES: u64 = EXPECTED_FILES[0].size + EXPECTED_FILES[1].size;

/// Compat record stored as JSON under [`EMBEDDING_MODEL_META_KEY`]. The
/// semantic pipeline (Phase 3+) reads it before trusting stored vectors: a
/// different model id or embedding width invalidates every vector and queues
/// a full re-embed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompat {
    pub model_id: String,
    pub embedding_dim: u32,
    pub repo_commit: String,
    /// Missing means the legacy incremental-media pipeline, never current.
    #[serde(default)]
    pub vector_pipeline_version: u32,
}

impl Default for ModelCompat {
    fn default() -> Self {
        Self::current()
    }
}

impl ModelCompat {
    /// The compat record for the manifest pinned in this build.
    pub fn current() -> Self {
        Self {
            model_id: MODEL_ID.to_string(),
            embedding_dim: EMBEDDING_DIM,
            repo_commit: REPO_COMMIT.to_string(),
            vector_pipeline_version: VECTOR_PIPELINE_VERSION,
        }
    }

    /// Serializes for the `meta` table.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ModelCompat is JSON-serializable")
    }

    /// Parses a stored record; unknown/invalid values mean incompatible.
    pub fn from_json(raw: &str) -> Option<Self> {
        serde_json::from_str(raw).ok()
    }

    /// True when stored vectors produced under `other` are still usable.
    pub fn is_compatible_with(&self, other: &ModelCompat) -> bool {
        self.model_id == other.model_id
            && self.embedding_dim == other.embedding_dim
            && self.repo_commit == other.repo_commit
            && self.vector_pipeline_version == other.vector_pipeline_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_recipe_rejects_raw_text_pipeline_one() {
        let current = ModelCompat::current();
        let legacy = ModelCompat {
            vector_pipeline_version: 1,
            ..current.clone()
        };
        assert!(
            !current.is_compatible_with(&legacy),
            "raw application vectors cannot mix with prompted query/document vectors"
        );
    }

    #[test]
    fn legacy_vector_pipeline_is_incompatible_even_with_matching_weights() {
        let current = ModelCompat::current();
        let mut json = serde_json::to_value(&current).unwrap();
        json.as_object_mut()
            .unwrap()
            .remove("vector_pipeline_version");
        let legacy = ModelCompat::from_json(&json.to_string()).unwrap();
        assert!(
            !current.is_compatible_with(&legacy),
            "legacy media vectors must be requeued"
        );
        assert_eq!(
            serde_json::to_value(&legacy).unwrap()["vector_pipeline_version"],
            0
        );
        assert!(
            ModelCompat::from_json("{}").is_none(),
            "partial records cannot silently become current"
        );
    }

    #[test]
    fn manifest_covers_both_ggufs() {
        assert_eq!(EXPECTED_FILES.len(), 2);
        assert_eq!(EXPECTED_FILES[0].name, TEXT_FILE);
        assert_eq!(EXPECTED_FILES[1].name, MMPROJ_FILE);
        assert_eq!(TOTAL_BYTES, 309_855_456 + 554_821_024);
        for f in &EXPECTED_FILES {
            assert!(f.url().ends_with(f.name));
            assert!(f.url().contains(MODEL_ID));
            assert!(f.url().contains(REPO_COMMIT));
            // Real SHA-256 pins: 64 lowercase hex chars, both present.
            let hash = f.sha256.expect("production GGUFs must pin SHA-256");
            assert_eq!(hash.len(), 64);
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
            assert_eq!(hash, hash.to_ascii_lowercase());
        }
        assert_eq!(
            EXPECTED_FILES[0].sha256,
            Some("2188ac1deca4b77dffefd603c2776a9d76d9d74ec01841392982ebb840b09135")
        );
        assert_eq!(
            EXPECTED_FILES[1].sha256,
            Some("c4a8a52691ecef40618438928bdf9e68379b854e24166f292592353db0aab64f")
        );
    }

    #[test]
    fn npu_bundle_is_pinned_and_served_from_its_own_repo() {
        assert_eq!(NPU_BUNDLE_FILES.len(), NPU_BUNDLE_FILE_COUNT);
        assert_eq!(
            NPU_BUNDLE_FILES.iter().map(|f| f.size).sum::<u64>(),
            NPU_BUNDLE_TOTAL_BYTES
        );
        let mut names = std::collections::HashSet::new();
        for f in &NPU_BUNDLE_FILES {
            assert!(names.insert(f.name), "duplicate {}", f.name);
            let path = f
                .npu_bundle_path()
                .expect("bundle files live under NPU_BUNDLE_DIR");
            assert!(!path.contains(".."));
            assert_eq!(
                f.url(),
                format!("https://huggingface.co/{NPU_MODEL_ID}/resolve/{NPU_REPO_COMMIT}/{path}")
            );
            let hash = f.sha256.expect("bundle files must pin SHA-256");
            assert_eq!(hash.len(), 64);
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        }
        for required in [
            "manifest.txt",
            "tensors.txt",
            "tensors.bin",
            "kernels/tgemm.xclbin",
        ] {
            assert!(
                NPU_BUNDLE_FILES
                    .iter()
                    .any(|f| f.npu_bundle_path() == Some(required)),
                "{required} missing"
            );
        }
        // GGUFs keep their own repository.
        assert!(EXPECTED_FILES.iter().all(|f| f.npu_bundle_path().is_none()));
    }

    #[test]
    fn compat_record_round_trip_and_mismatch() {
        let current = ModelCompat::current();
        let back = ModelCompat::from_json(&current.to_json()).unwrap();
        assert_eq!(current, back);
        assert!(current.is_compatible_with(&back));

        let other_dim = ModelCompat {
            embedding_dim: 384,
            ..current.clone()
        };
        assert!(!current.is_compatible_with(&other_dim));

        let other_model = ModelCompat {
            model_id: "someone-else/other-model".into(),
            ..current.clone()
        };
        assert!(!current.is_compatible_with(&other_model));

        // Different pinned weights define a different embedding space.
        let bumped_commit = ModelCompat {
            repo_commit: "deadbeef".into(),
            ..current.clone()
        };
        assert!(!current.is_compatible_with(&bumped_commit));

        assert!(ModelCompat::from_json("not json").is_none());
    }
}
