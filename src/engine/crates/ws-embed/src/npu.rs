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
    /// Bounds initialization and every dispatch in one worker NPU attempt.
    pub fn set_job_budget(budget: std::time::Duration) -> Result<(), EmbedError> {
        taconite::set_execution_budget(budget)
            .map_err(|e| EmbedError::LoadFailed(format!("npu: {e}")))
    }

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
        let model = EmbeddingGemma2::load(bundle_dir).map_err(|e| match e {
            taconite_embeddinggemma2::Error::Npu(message) => EmbedError::NpuFailed(message),
            other => EmbedError::LoadFailed(format!("npu: {other}")),
        })?;
        if model.tokenizer.is_none()
            || model.cfg.max_text_tokens < NPU_MAX_TEXT_TOKENS
            || model.cfg.out != 768
        {
            return Err(EmbedError::LoadFailed(
                "npu: bundle has no text path (needs the 0.2.0 bundle)".into(),
            ));
        }
        let mut rt = Self {
            model,
            bundle: bundle_dir.to_path_buf(),
        };
        if let Err(error) = rt.embed_texts(&["warmup"]) {
            if matches!(error, EmbedError::NpuFailed(_)) {
                // A failed dispatch may still own live device buffers. The
                // worker exits on this error before any native teardown.
                std::mem::forget(rt);
            }
            return Err(error);
        }
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
            let started = std::time::Instant::now();
            let mut ids = self
                .application_tokens(text.as_ref())
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
            info!(modality = "text", total_ms = started.elapsed().as_secs_f64() * 1000.0,
                npu_dispatch_ms = self.model.timing.npu_total().as_secs_f64() * 1000.0,
                stages = %self.model.timing, "NPU host wall timing (dispatch includes driver waits)");
            out.push(v);
        }
        Ok(out)
    }

    fn application_tokens(&self, text: &str) -> Result<Vec<u32>, taconite_embeddinggemma2::Error> {
        let mut ids = self.model.tokenize(text, None)?;
        if text.starts_with("<bos>") {
            // Recipe v2's llama_tokenize(add_special=true) always adds both;
            // upstream's chat-template heuristic omits them for this prefix.
            let tokenizer = self
                .model
                .tokenizer
                .as_ref()
                .expect("validated text bundle");
            ids.insert(0, tokenizer.bos);
            ids.push(tokenizer.eos);
        }
        Ok(ids)
    }

    /// Embeds an RGB8 image at a qualified140/280token budget.70token frames
    /// retain the CPU/GPU recipe until this bundle meets cross-backend parity.
    pub fn embed_image_with_budget(
        &mut self,
        rgb: &[u8],
        width: u32,
        height: u32,
        token_budget: u32,
    ) -> Result<Vec<f32>, EmbedError> {
        qualified_image_budget(token_budget)?;
        let started = std::time::Instant::now();
        let (rgb, width, height) = crate::image::prepare(rgb, width, height, token_budget)?;
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
            &rgb,
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
        info!(modality = "image", token_budget, total_ms = started.elapsed().as_secs_f64() * 1000.0,
            npu_dispatch_ms = self.model.timing.npu_total().as_secs_f64() * 1000.0,
            stages = %self.model.timing, "NPU host wall timing (dispatch includes driver waits)");
        Ok(v)
    }
}

fn qualified_image_budget(budget: u32) -> Result<(), EmbedError> {
    if !matches!(budget, 140 | 280) {
        return Err(EmbedError::MtmdFailed(
            "NPU image budget is not qualified; use the GPU/CPU image runtime".into(),
        ));
    }
    Ok(())
}

fn checked_unit(v: &mut [f32]) -> Result<(), EmbedError> {
    if v.len() != 768 || v.iter().any(|x| !x.is_finite()) || v.iter().all(|x| *x == 0.0) {
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
        let mut v = [0.0; 768];
        v[0] = 3.0;
        v[1] = 4.0;
        checked_unit(&mut v).unwrap();
        assert!((v[0] - 0.6).abs() < 1e-6 && (v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn wrong_embedding_width_cannot_enter_the_shared_index() {
        assert!(checked_unit(&mut [1.0; 767]).is_err());
        assert!(checked_unit(&mut [1.0; 769]).is_err());
        assert!(checked_unit(&mut [1.0; 768]).is_ok());
    }

    #[test]
    fn unqualified_image_budget_is_refused_for_cpu_fallback() {
        assert!(qualified_image_budget(70).is_err());
        assert!(qualified_image_budget(140).is_ok());
        assert!(qualified_image_budget(280).is_ok());
        assert!(qualified_image_budget(0).is_err());
    }

    #[test]
    #[ignore = "requires actual NPU2 and WS_EMBED_TEST_NPU_BUNDLE; owns a disposable child"]
    fn real_npu_expired_dispatch_retires_without_native_teardown() {
        use std::time::{Duration, Instant};
        if std::env::var_os("WS_TEST_NPU_DEADLINE_CHILD").is_some() {
            assert!(npu_runtime_compiled());
            let bundle = PathBuf::from(std::env::var_os("WS_EMBED_TEST_NPU_BUNDLE").unwrap());
            NpuRuntime::set_job_budget(Duration::from_secs(45)).unwrap();
            let mut npu = NpuRuntime::load(&bundle).expect("actual NPU initialization");
            NpuRuntime::set_job_budget(Duration::from_millis(1)).unwrap();
            std::thread::sleep(Duration::from_millis(2));
            let started = Instant::now();
            let result = npu.embed_texts(&["task: search result | query: memory safety"]);
            assert!(
                matches!(result, Err(EmbedError::NpuFailed(_))),
                "{result:?}"
            );
            assert!(started.elapsed() < Duration::from_secs(2));
            eprintln!(
                "actual NPU expired dispatch returned a bounded error; retiring child with code75"
            );
            ws_platform::child_job::terminate_current_process(75);
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "npu::tests::real_npu_expired_dispatch_retires_without_native_teardown",
                "--nocapture",
            ])
            .env("WS_TEST_NPU_DEADLINE_CHILD", "1")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(75));
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("native dispatch exceeded independent parent timeout");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    #[ignore = "requires actual NPU2, WS_EMBED_TEST_NPU_BUNDLE and WS_EMBED_TEST_MODEL"]
    fn real_npu_preserves_reference_tokens_vectors_and_image_budgets() {
        use crate::runtime::test_support;
        use crate::vector::cosine;
        use crate::{BackendPreference, EmbedRuntime, LoadOptions};
        let bundle = PathBuf::from(
            std::env::var_os("WS_EMBED_TEST_NPU_BUNDLE").expect("explicit NPU bundle"),
        );
        let model =
            PathBuf::from(std::env::var_os("WS_EMBED_TEST_MODEL").expect("explicit CPU model"));
        assert!(
            npu_runtime_compiled(),
            "a stub build cannot satisfy native acceptance"
        );
        let mut cpu = EmbedRuntime::load(
            &model,
            &LoadOptions {
                backend: BackendPreference::Cpu,
                mmproj: Some(model.with_file_name("mmproj-embeddinggemma-2-Q8_0.gguf")),
                ..Default::default()
            },
        )
        .expect("CPU reference runtime");
        let mut npu = NpuRuntime::load(&bundle).expect("actual Windows NPU");
        let text_refs: Vec<(usize, String, String)> = npu
            .model
            .manifest
            .tagged("tref")
            .map(|row| {
                let prompt = row.field(1).unwrap().to_string();
                let raw = row.field(2).unwrap();
                let bytes: Vec<u8> = (0..raw.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&raw[i..i + 2], 16).unwrap())
                    .collect();
                (
                    row.field_as(0).unwrap(),
                    prompt,
                    String::from_utf8(bytes).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            text_refs.len(),
            18,
            "independent pinned tokenizer references"
        );
        for (index, prompt, text) in text_refs {
            NpuRuntime::set_job_budget(std::time::Duration::from_secs(45)).unwrap();
            let prompt = (prompt != "-").then_some(prompt.as_str());
            let full = match prompt {
                Some(name) => format!(
                    "{}{}",
                    npu.model.tokenizer.as_ref().unwrap().prompt(name).unwrap(),
                    text
                ),
                None => text.clone(),
            };
            let hf_ids: Vec<u32> = npu
                .model
                .store
                .i32(&format!("tref.{index}.ids"))
                .unwrap()
                .iter()
                .map(|&id| id as u32)
                .collect();
            assert_eq!(
                npu.model.tokenize(&full, None).unwrap(),
                hf_ids,
                "HF token IDs case{index}"
            );
            assert_eq!(
                test_support::tokens(&cpu, &full),
                npu.application_tokens(&full).unwrap(),
                "CPU/NPU application token IDs case{index}"
            );
            // The raw 1421-token upstream reference exceeds the application's
            // 512-token contract and the bounded job budget. Check its full
            // tokenizer IDs above and its truncated application vector below.
            if hf_ids.len() <= NPU_MAX_TEXT_TOKENS {
                let independent = npu
                    .model
                    .store
                    .f32(&format!("tref.{index}.embedding"))
                    .unwrap()
                    .to_vec();
                let actual = npu.model.embed_text(&text, prompt).unwrap();
                let agreement = cosine(&actual, &independent).unwrap();
                assert!(agreement >= 0.999, "independent text{index}: {agreement}");
            }
            NpuRuntime::set_job_budget(std::time::Duration::from_secs(45)).unwrap();
            let actual = npu
                .embed_texts(std::slice::from_ref(&full))
                .unwrap()
                .remove(0);
            let expected = cpu
                .embed_texts(std::slice::from_ref(&full))
                .unwrap()
                .remove(0);
            let agreement = cosine(&actual, &expected).unwrap();
            assert!(
                agreement >= 0.999,
                "application512token text{index}: {agreement}"
            );
            eprintln!(
                "NPU text{index}: tokens{}, CPU/application cosine{agreement:.9}",
                hf_ids.len()
            );
        }
        let image_refs: Vec<(String, usize, usize)> = npu
            .model
            .manifest
            .tagged("ref")
            .map(|row| {
                (
                    row.field(0).unwrap().to_string(),
                    row.get("w").unwrap(),
                    row.get("h").unwrap(),
                )
            })
            .collect();
        assert_eq!(image_refs.len(), 4);
        let mut image_failures = Vec::new();
        for (name, width, height) in image_refs {
            let rgb = npu
                .model
                .store
                .u8(&format!("ref.{name}.rgb"))
                .unwrap()
                .to_vec();
            let patches = npu.model.preprocess(&rgb, width, height).unwrap();
            assert_eq!(
                patches.pixels,
                npu.model.store.f32(&format!("ref.{name}.pixels")).unwrap(),
                "bit-exact preprocessing {name}"
            );
            let positions = npu
                .model
                .store
                .i32(&format!("ref.{name}.positions"))
                .unwrap();
            assert_eq!(positions.len(), patches.positions.len() * 2);
            for (i, &(x, y)) in patches.positions.iter().enumerate() {
                assert_eq!(
                    (x, y),
                    (positions[i * 2] as usize, positions[i * 2 + 1] as usize)
                );
            }
            NpuRuntime::set_job_budget(std::time::Duration::from_secs(45)).unwrap();
            let reference = npu
                .model
                .store
                .f32(&format!("ref.{name}.embedding"))
                .unwrap()
                .to_vec();
            let actual = npu.model.embed_patches(&patches).unwrap();
            let agreement = cosine(&actual, &reference).unwrap();
            assert!(agreement >= 0.999, "independent image{name}: {agreement}");
            assert!(
                npu.embed_image_with_budget(&rgb, width as u32, height as u32, 70)
                    .is_err(),
                "70token images cannot enter the shared space from the unqualified NPU route"
            );
            for budget in [140, 280] {
                NpuRuntime::set_job_budget(std::time::Duration::from_secs(45)).unwrap();
                let actual = npu
                    .embed_image_with_budget(&rgb, width as u32, height as u32, budget)
                    .unwrap();
                let expected = cpu
                    .embed_image_with_budget(&rgb, width as u32, height as u32, budget)
                    .unwrap();
                let agreement = cosine(&actual, &expected).unwrap();
                eprintln!("NPU image{name}/{budget}: CPU cosine{agreement:.9}");
                if agreement < 0.999 {
                    image_failures.push(format!("{name}/{budget}: {agreement:.9}"));
                }
            }
        }
        assert!(
            image_failures.is_empty(),
            "CPU/NPU image parity: {image_failures:?}"
        );
    }
}
