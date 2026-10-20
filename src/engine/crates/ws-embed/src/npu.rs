//! AMD NPU runtime: the IRON EmbeddingGemma 2 NPU2 bundle
//! (`brishen/iron-embeddinggemma2-npu2`) replayed through XRT by
//! `taconite-embeddinggemma2`.
//!
//! It serves the same embedding space as the llama.cpp runtime: identical
//! `google/embeddinggemma-2` weights (bf16 on the NPU, Q8_0 in the GGUF),
//! the same application inputs (the caller's `recipe` prompts, passed as raw
//! text with `<bos>`/`<eos>` like `llama_tokenize(add_special)`), the same
//! 512-token text cap and the same 70/140/280 image-token budgets. Audio has
//! no NPU path; the worker keeps it on llama.cpp.
//!
//! Loading fails with an error (never a crash) when the build has no XRT SDK,
//! the AMD driver/XRT is missing, the device is not NPU2, or the bundle is
//! absent; the caller then falls back to GPU/CPU.

use std::path::{Path, PathBuf};

use taconite_embeddinggemma2::EmbeddingGemma2;
use tracing::info;

use crate::runtime::{EmbedError, DEFAULT_N_BATCH};
use crate::vector::l2_normalize;

/// Text tokens embedded per input, matching the llama.cpp runtime's cap so a
/// long document gets the same vector on either backend.
pub const NPU_MAX_TEXT_TOKENS: usize = DEFAULT_N_BATCH as usize;

/// Whether this build carries a real XRT shim (false: built without the SDK).
pub fn npu_runtime_compiled() -> bool {
    taconite::XRT_LINKED
}

/// An opened NPU with the bundle's kernels and weights resident.
pub struct NpuRuntime {
    model: EmbeddingGemma2,
    bundle: PathBuf,
}

impl NpuRuntime {
    /// Opens the NPU and loads the bundle at `bundle_dir` (the directory that
    /// holds `manifest.txt`), then runs one warm-up text.
    pub fn load(bundle_dir: &Path) -> Result<Self, EmbedError> {
        if !npu_runtime_compiled() {
            return Err(EmbedError::LoadFailed(
                "NPU runtime not compiled in (build without the XRT SDK)".into(),
            ));
        }
        let manifest = bundle_dir.join("manifest.txt");
        if !manifest.is_file() {
            return Err(EmbedError::ModelNotFound(manifest));
        }
        let model = EmbeddingGemma2::load(bundle_dir)
            .map_err(|e| EmbedError::LoadFailed(format!("npu: {e}")))?;
        if model.tokenizer.is_none() || model.cfg.max_text_tokens < NPU_MAX_TEXT_TOKENS {
            return Err(EmbedError::LoadFailed(
                "npu: bundle has no text path (needs the 0.2.0 bundle)".into(),
            ));
        }
        let mut rt = Self {
            model,
            bundle: bundle_dir.to_path_buf(),
        };
        rt.embed_texts(&["warmup"])?;
        info!(bundle = %rt.bundle.display(), contexts = rt.model.contexts(), "NPU embedding model loaded");
        Ok(rt)
    }

    /// Embedding width (768).
    pub fn n_embd(&self) -> usize {
        self.model.cfg.out
    }

    pub fn bundle_dir(&self) -> &Path {
        &self.bundle
    }

    /// One unit vector per text, in order.
    pub fn embed_texts<S: AsRef<str>>(&mut self, texts: &[S]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        let mut out = Vec::with_capacity(texts.len());
        for (i, text) in texts.iter().enumerate() {
            let mut ids = self
                .model
                .tokenize(text.as_ref(), None)
                .map_err(|_| EmbedError::Tokenize(i))?;
            if ids.is_empty() {
                return Err(EmbedError::Tokenize(i));
            }
            ids.truncate(NPU_MAX_TEXT_TOKENS);
            let mut v = self
                .model
                .embed_ids(&ids)
                .map_err(|e| EmbedError::NpuFailed(e.to_string()))?;
            checked_unit(&mut v)?;
            out.push(v);
        }
        Ok(out)
    }

    /// Embeds an RGB8 image with at most `token_budget` (70/140/280) soft tokens.
    pub fn embed_image_with_budget(
        &mut self,
        rgb: &[u8],
        width: u32,
        height: u32,
        token_budget: u32,
    ) -> Result<Vec<f32>, EmbedError> {
        let (w, h) = (width as usize, height as usize);
        let cfg = &self.model.cfg;
        if !matches!(token_budget, 70 | 140 | 280)
            || token_budget as usize > cfg.max_soft_tokens
            || w == 0
            || h == 0
            || w.checked_mul(h).and_then(|p| p.checked_mul(3)) != Some(rgb.len())
        {
            return Err(EmbedError::MtmdFailed(
                "invalid image dimensions, RGB buffer or token budget".into(),
            ));
        }
        let patches = taconite_embeddinggemma2::preprocess::patches(
            rgb,
            w,
            h,
            cfg.patch,
            cfg.pool,
            token_budget as usize,
        )
        .ok_or_else(|| EmbedError::MtmdFailed(format!("a {w} x {h} image is too thin")))?;
        let mut v = self
            .model
            .embed_patches(&patches)
            .map_err(|e| EmbedError::NpuFailed(e.to_string()))?;
        checked_unit(&mut v)?;
        Ok(v)
    }
}

fn checked_unit(v: &mut [f32]) -> Result<(), EmbedError> {
    if v.is_empty() || v.iter().any(|x| !x.is_finite()) || v.iter().all(|x| *x == 0.0) {
        return Err(EmbedError::NpuFailed("invalid NPU vector".into()));
    }
    l2_normalize(v);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_cap_matches_llama_runtime() {
        assert_eq!(NPU_MAX_TEXT_TOKENS, 512);
    }

    #[test]
    fn missing_bundle_or_runtime_is_an_error_not_a_crash() {
        let dir = std::env::temp_dir().join(format!("ws-embed-npu-missing-{}", std::process::id()));
        let err = NpuRuntime::load(&dir).err().expect("no bundle, no runtime");
        if npu_runtime_compiled() {
            assert!(matches!(err, EmbedError::ModelNotFound(_)), "{err}");
        } else {
            assert!(matches!(err, EmbedError::LoadFailed(_)), "{err}");
        }
    }

    #[test]
    fn invalid_vectors_are_rejected() {
        assert!(checked_unit(&mut []).is_err());
        assert!(checked_unit(&mut [0.0, 0.0]).is_err());
        assert!(checked_unit(&mut [f32::NAN, 1.0]).is_err());
        let mut v = [3.0, 4.0];
        checked_unit(&mut v).unwrap();
        assert!((v[0] - 0.6).abs() < 1e-6 && (v[1] - 0.8).abs() < 1e-6);
    }
}
