//! Safe embedding runtime over the llama.cpp shim.
//!
//! `EmbedRuntime::load` opens the text model (and optionally the mtmd
//! projector), warms it up, and keeps it resident until dropped or
//! [`EmbedRuntime::release_gpu`] is called. Loading on a GPU backend that
//! fails (e.g. out of VRAM) retries once on CPU.

use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

use thiserror::Error;
use tracing::{info, warn};

use crate::backends::{select_backend, BackendKind, BackendPreference};
use crate::ffi;
use crate::vector::l2_normalize;

/// Context window used for embedding. Names and short texts are far below this.
pub const DEFAULT_N_CTX: u32 = 2048;
/// Logical and physical batch size.
pub const DEFAULT_N_BATCH: u32 = 512;
/// Dummy forward passes run once after load so first real query is not cold.
pub const WARMUP_PASSES: usize = 2;

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("model file not found: {0}")]
    ModelNotFound(PathBuf),
    #[error("failed to load model: {0}")]
    LoadFailed(String),
    #[error("failed to create context: {0}")]
    ContextFailed(String),
    #[error("tokenization failed for text at index {0}")]
    Tokenize(usize),
    #[error("embedding decode failed with code {0}")]
    Decode(i32),
    #[error("multimodal projector failed to load: {0}")]
    MtmdFailed(String),
    #[error("path is not valid UTF-8 or contains NUL: {0}")]
    BadPath(String),
    #[error("embedding batch is empty")]
    EmptyBatch,
    #[error("NPU inference failed: {0}")]
    NpuFailed(String),
}

/// Options for [`EmbedRuntime::load`].
#[derive(Debug, Clone)]
pub struct LoadOptions {
    pub backend: BackendPreference,
    /// Host threads for batch processing; an explicit value overrides the bounded default.
    pub n_threads: i32,
    /// Optional mmproj GGUF (multimodal projector). `None` = text only.
    pub mmproj: Option<PathBuf>,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            backend: BackendPreference::Auto,
            n_threads: std::thread::available_parallelism()
                // More logical threads slowed measured media inference while
                // competing with the desktop. Calibration can override this.
                .map(|n| n.get().min(8) as i32)
                .unwrap_or(4),
            mmproj: None,
        }
    }
}

/// An owned, loaded embedding model.
pub struct EmbedRuntime {
    model_path: PathBuf,
    mmproj_path: Option<PathBuf>,
    // Option so each native handle is taken and freed exactly once, even when
    // a reload during release_gpu fails part-way.
    model: Option<NonNull<ffi::LlamaModel>>,
    ctx: Option<NonNull<ffi::LlamaContext>>,
    mtmd: Option<NonNull<ffi::MtmdContext>>,
    n_embd: usize,
    backend: BackendKind,
    n_threads: i32,
}

// The raw handles are only touched through &mut self (or &self for read-only
// queries), and llama.cpp contexts are not shared across threads concurrently.
// Moving the runtime to another thread is safe.
unsafe impl Send for EmbedRuntime {}

/// A device discovered in the ggml registry.
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub kind: BackendKind,
    pub name: String,
    pub index: usize,
    pub free_bytes: usize,
    pub total_bytes: usize,
    pub handle: usize,
}

/// Load all backend DLLs and enumerate usable devices.
///
/// Must run before [`EmbedRuntime::load`]. Safe to call more than once.
pub fn init_backends() {
    // SAFETY: plain C call with no pointer arguments.
    unsafe { ffi::wse_backend_load_all() };
}

/// Enumerate every device visible to the ggml registry.
pub fn enumerate_devices() -> Vec<DeviceInfo> {
    let mut out = Vec::new();
    // SAFETY: the registry returns stable device pointers for the process lifetime.
    unsafe {
        let n = ffi::wse_dev_count();
        for i in 0..n {
            let dev = ffi::wse_dev_get(i);
            if dev.is_null() {
                continue;
            }
            let reg_name = read_c_buf(|buf, cap| ffi::wse_dev_backend_name(dev, buf, cap));
            let Some(kind) = BackendKind::from_registry_name(&reg_name) else {
                continue;
            };
            let name = read_c_buf(|buf, cap| ffi::wse_dev_name(dev, buf, cap));
            let (mut free, mut total) = (0usize, 0usize);
            ffi::wse_dev_memory(dev, &mut free, &mut total);
            out.push(DeviceInfo {
                kind,
                name,
                index: i,
                free_bytes: free,
                total_bytes: total,
                handle: dev as usize,
            });
        }
    }
    out
}

/// Backend kinds that have at least one device.
pub fn available_kinds(devices: &[DeviceInfo]) -> Vec<BackendKind> {
    let mut kinds: Vec<BackendKind> = devices.iter().map(|d| d.kind).collect();
    kinds.sort_by_key(|k| *k as u8);
    kinds.dedup();
    kinds
}

fn read_c_buf(fill: impl FnOnce(*mut std::os::raw::c_char, usize) -> usize) -> String {
    let mut buf = [0u8; 256];
    let n = fill(buf.as_mut_ptr().cast(), buf.len());
    String::from_utf8_lossy(&buf[..n.min(buf.len() - 1)]).into_owned()
}

fn path_to_cstring(path: &Path) -> Result<CString, EmbedError> {
    let s = path
        .to_str()
        .ok_or_else(|| EmbedError::BadPath(path.display().to_string()))?;
    CString::new(s).map_err(|_| EmbedError::BadPath(path.display().to_string()))
}

impl EmbedRuntime {
    /// Load `text_gguf` (plus optional mmproj) using the backend chosen by
    /// `opts.backend` among the devices actually present.
    ///
    /// A GPU load failure (typically VRAM exhaustion) is retried on CPU.
    pub fn load(text_gguf: &Path, opts: &LoadOptions) -> Result<Self, EmbedError> {
        if !text_gguf.is_file() {
            return Err(EmbedError::ModelNotFound(text_gguf.to_path_buf()));
        }
        init_backends();
        let devices = enumerate_devices();
        let available = available_kinds(&devices);
        let chosen = select_backend(opts.backend, &available);
        info!(backend = %chosen, "loading embedding model");

        match Self::load_on(text_gguf, opts, &devices, chosen) {
            Ok(rt) => Ok(rt),
            Err(e) if chosen != BackendKind::Cpu => {
                warn!(error = %e, "GPU load failed, retrying on CPU");
                Self::load_on(text_gguf, opts, &devices, BackendKind::Cpu)
            }
            Err(e) => Err(e),
        }
    }

    fn load_on(
        text_gguf: &Path,
        opts: &LoadOptions,
        devices: &[DeviceInfo],
        kind: BackendKind,
    ) -> Result<Self, EmbedError> {
        let dev = devices.iter().find(|d| d.kind == kind);
        let (dev_ptr, n_gpu_layers) = match (kind, dev) {
            // CPU: force zero offload, never hand a device list.
            (BackendKind::Cpu, _) | (_, None) => (std::ptr::null_mut(), 0),
            (_, Some(d)) => (d.handle as *mut ffi::GgmlBackendDevice, -1),
        };

        let path_c = path_to_cstring(text_gguf)?;
        // SAFETY: path_c is a valid NUL-terminated string for the call duration.
        let model_raw = unsafe { ffi::wse_model_load(path_c.as_ptr(), dev_ptr, n_gpu_layers) };
        let model = NonNull::new(model_raw)
            .ok_or_else(|| EmbedError::LoadFailed(format!("{kind} load returned null")))?;

        // From here on, `rt` owns `model` and frees it in Drop, so every early
        // return below cleans up through the same path.
        let mut rt = Self {
            model_path: text_gguf.to_path_buf(),
            mmproj_path: opts.mmproj.clone(),
            model: Some(model),
            ctx: None,
            mtmd: None,
            n_embd: 0,
            backend: kind,
            n_threads: opts.n_threads,
        };

        // SAFETY: model is live and owned by rt.
        let ctx_raw = unsafe {
            ffi::wse_context_new(model.as_ptr(), DEFAULT_N_CTX,
                if opts.mmproj.is_some() { DEFAULT_N_CTX } else { DEFAULT_N_BATCH }, opts.n_threads)
        };
        rt.ctx = Some(
            NonNull::new(ctx_raw)
                .ok_or_else(|| EmbedError::ContextFailed(format!("{kind} context returned null")))?,
        );
        // SAFETY: model is live.
        rt.n_embd = unsafe { ffi::wse_model_n_embd(model.as_ptr()) }.max(0) as usize;

        if let Some(mmproj) = &opts.mmproj {
            rt.mtmd = Some(open_mtmd(mmproj, model, kind != BackendKind::Cpu, opts.n_threads)?);
        }

        rt.warmup()?;
        Ok(rt)
    }

    /// Backend this runtime is actually running on.
    pub fn backend(&self) -> BackendKind {
        self.backend
    }

    /// Embedding dimension of the loaded model (768 for EmbeddingGemma).
    pub fn n_embd(&self) -> usize {
        self.n_embd
    }

    /// Path of the loaded text model.
    pub fn model_path(&self) -> &Path {
        &self.model_path
    }

    fn model_ptr(&self) -> Result<NonNull<ffi::LlamaModel>, EmbedError> {
        self.model.ok_or_else(|| EmbedError::LoadFailed("model released".into()))
    }

    /// Whether a multimodal projector is attached.
    pub fn has_mtmd(&self) -> bool {
        self.mtmd.is_some()
    }

    /// Whether the mtmd projector supports image input (Phase 4 hook).
    pub fn mtmd_supports_vision(&self) -> bool {
        self.mtmd
            .map(|m| unsafe { ffi::wse_mtmd_supports_vision(m.as_ptr()) } == 1)
            .unwrap_or(false)
    }

    /// Whether the mtmd projector supports audio input (Phase 4 hook).
    pub fn mtmd_supports_audio(&self) -> bool {
        self.mtmd
            .map(|m| unsafe { ffi::wse_mtmd_supports_audio(m.as_ptr()) } == 1)
            .unwrap_or(false)
    }

    /// Embeds an RGB8 image (`width * height * 3` bytes) through the attached
    /// `mtmd` vision projector and text model, returning a unit-length `n_embd` vector.
    pub fn embed_image(
        &mut self,
        rgb: &[u8],
        width: u32,
        height: u32,
    ) -> Result<Vec<f32>, EmbedError> {
        self.embed_image_with_budget(rgb, width, height, 280)
    }

    pub fn embed_image_with_budget(
        &mut self,
        rgb: &[u8],
        width: u32,
        height: u32,
        token_budget: u32,
    ) -> Result<Vec<f32>, EmbedError> {
        let (rgb, width, height) = crate::image::prepare(rgb, width, height, token_budget)?;
        let mctx = self
            .mtmd
            .ok_or_else(|| EmbedError::MtmdFailed("mmproj not loaded".into()))?;
        let lctx = self
            .ctx
            .ok_or_else(|| EmbedError::ContextFailed("context released".into()))?;
        if !self.mtmd_supports_vision() {
            return Err(EmbedError::MtmdFailed(
                "loaded mmproj does not support vision".into(),
            ));
        }

        let mut out = vec![0f32; self.n_embd];
        let rc = unsafe {
            ffi::wse_mtmd_embed_image(
                mctx.as_ptr(),
                lctx.as_ptr(),
                rgb.as_ptr(),
                width,
                height,
                self.n_embd as i32,
                out.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return Err(EmbedError::Decode(rc));
        }
        normalize_media(&mut out)?;
        Ok(out)
    }

    /// Embed finite mono16k PCM, at most30seconds; long recordings are windowed by extraction.
    pub fn embed_audio(&mut self, pcm_f32_16k: &[f32]) -> Result<Vec<f32>, EmbedError> {
        if pcm_f32_16k.is_empty()
            || pcm_f32_16k.len() > 480_000
            || pcm_f32_16k.iter().any(|v| !v.is_finite())
        {
            return Err(EmbedError::MtmdFailed(
                "audio must contain 1..480000 finite mono16k samples".into(),
            ));
        }
        let mctx = self
            .mtmd
            .ok_or_else(|| EmbedError::MtmdFailed("mmproj not loaded".into()))?;
        let lctx = self
            .ctx
            .ok_or_else(|| EmbedError::ContextFailed("context released".into()))?;
        if !self.mtmd_supports_audio() {
            return Err(EmbedError::MtmdFailed(
                "loaded mmproj does not support audio".into(),
            ));
        }
        let mut out = vec![0f32; self.n_embd];
        // SAFETY: live owned handles and validated bounded slices; native output is n_embd wide.
        let rc = unsafe {
            ffi::wse_mtmd_embed_audio(
                mctx.as_ptr(),
                lctx.as_ptr(),
                pcm_f32_16k.as_ptr(),
                pcm_f32_16k.len(),
                self.n_embd as i32,
                out.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return Err(EmbedError::Decode(rc));
        }
        normalize_media(&mut out)?;
        Ok(out)
    }

    /// Embed a batch of texts. Returns one L2-normalized vector per input, in order.
    ///
    /// Inputs are processed in sub-batches bounded by both the context's sequence
    /// capacity (`max_seqs`, one llama sequence per text) and token batch capacity
    /// ([`DEFAULT_N_BATCH`]), so large or long-text batches are split across decodes.
    pub fn embed_texts<S: AsRef<str>>(&mut self, texts: &[S]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        let ctx = self.ctx.ok_or(EmbedError::ContextFailed("context released".into()))?;
        // SAFETY: plain C call with no pointer arguments.
        let max_seqs = unsafe { ffi::wse_context_max_seqs() }.max(1) as usize;
        let budget = (DEFAULT_N_BATCH as usize).max(1);

        // Tokenize each text separately (one llama sequence per input) and cap
        // any single sequence to `budget` tokens so it fits in one decode batch.
        let mut tokenized: Vec<Vec<i32>> = Vec::with_capacity(texts.len());
        for (i, text) in texts.iter().enumerate() {
            let mut t = self
                .tokenize(text.as_ref())
                .filter(|t| !t.is_empty())
                .ok_or(EmbedError::Tokenize(i))?;
            if t.len() > budget {
                t.truncate(budget);
            }
            tokenized.push(t);
        }

        let mut all: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
        for range in plan_sub_batches(&tokenized, max_seqs, budget) {
            let slice = &tokenized[range];
            let total_tokens: usize = slice.iter().map(|t| t.len()).sum();
            let mut tokens: Vec<i32> = Vec::with_capacity(total_tokens);
            let mut seq_lens: Vec<i32> = Vec::with_capacity(slice.len());
            for t in slice {
                seq_lens.push(t.len() as i32);
                tokens.extend_from_slice(t);
            }

            let n = slice.len();
            let mut out = vec![0f32; n * self.n_embd];
            // SAFETY: tokens/seq_lens hold exactly the sums the shim reads; out holds
            // n * n_embd floats. ctx is live until free_handles, which also consumes self.
            let rc = unsafe {
                ffi::wse_embed_batch(
                    ctx.as_ptr(),
                    tokens.as_ptr(),
                    seq_lens.as_ptr(),
                    n as i32,
                    self.n_embd as i32,
                    out.as_mut_ptr(),
                )
            };
            if rc != 0 {
                return Err(EmbedError::Decode(rc));
            }
            all.extend(out.chunks_exact(self.n_embd).map(|c| c.to_vec()));
        }

        for v in &mut all {
            l2_normalize(v);
        }
        Ok(all)
    }

    fn tokenize(&self, text: &str) -> Option<Vec<i32>> {
        let model = self.model_ptr().ok()?;
        let bytes = text.as_bytes();
        let len = i32::try_from(bytes.len()).ok()?;
        let mut cap = (bytes.len() as i32 + 16).max(32);
        loop {
            let mut buf = vec![0i32; cap as usize];
            // SAFETY: bytes/buf valid for the given lengths; model is live.
            let n = unsafe {
                ffi::wse_tokenize(
                    model.as_ptr(),
                    bytes.as_ptr().cast(),
                    len,
                    buf.as_mut_ptr(),
                    cap,
                )
            };
            if n >= 0 {
                buf.truncate(n as usize);
                return Some(buf);
            }
            // Negative return is -required; grow and retry once.
            let required = n.checked_neg()?;
            if required <= cap {
                return None;
            }
            cap = required;
        }
    }

    /// A few dummy passes so the first real query does not pay the cold-start cost.
    fn warmup(&mut self) -> Result<(), EmbedError> {
        for _ in 0..WARMUP_PASSES {
            self.embed_texts(&["warmup"])?;
        }
        Ok(())
    }

    /// Release GPU resources and continue serving from CPU.
    ///
    /// Frees every GPU-side handle first (VRAM returns to the driver), then
    /// reloads the model on CPU. If the CPU reload fails, the runtime is left
    /// with no handles and returns an error from subsequent calls; Drop is safe.
    ///
    /// A no-op on a CPU runtime. Called before idle exit.
    pub fn release_gpu(&mut self) -> Result<(), EmbedError> {
        if self.backend == BackendKind::Cpu {
            return Ok(());
        }
        // Free GPU handles before reloading so VRAM is returned first.
        self.free_handles();

        let opts = LoadOptions {
            backend: BackendPreference::Cpu,
            n_threads: self.n_threads,
            mmproj: self.mmproj_path.clone(),
        };
        let fresh = Self::load_on(&self.model_path, &opts, &[], BackendKind::Cpu)?;
        // Adopt the fresh handles. `fresh` is consumed without running Drop,
        // so the handles are owned exactly once, by `self`.
        let fresh = std::mem::ManuallyDrop::new(fresh);
        self.model = fresh.model;
        self.ctx = fresh.ctx;
        self.mtmd = fresh.mtmd;
        self.n_embd = fresh.n_embd;
        self.backend = BackendKind::Cpu;
        Ok(())
    }

    /// Free every owned handle, exactly once. Safe to call repeatedly.
    fn free_handles(&mut self) {
        if let Some(m) = self.mtmd.take() {
            // SAFETY: handle was owned by self and is taken here, so freed once.
            unsafe { ffi::wse_mtmd_free(m.as_ptr()) };
        }
        if let Some(c) = self.ctx.take() {
            // SAFETY: handle was owned by self and is taken here, so freed once.
            unsafe { ffi::wse_context_free(c.as_ptr()) };
        }
        if let Some(m) = self.model.take() {
            // SAFETY: handle was owned by self and is taken here, so freed once.
            unsafe { ffi::wse_model_free(m.as_ptr()) };
        }
    }
}

fn normalize_media(out: &mut [f32]) -> Result<(), EmbedError> {
    if out.is_empty() || out.iter().any(|v| !v.is_finite()) || !out.iter().any(|v| *v != 0.0) {
        return Err(EmbedError::MtmdFailed("invalid media vector".into()));
    }
    l2_normalize(out);
    if out.iter().any(|v| !v.is_finite()) || !out.iter().any(|v| *v != 0.0) {
        return Err(EmbedError::MtmdFailed("invalid normalized media vector".into()));
    }
    Ok(())
}

fn open_mtmd(
    mmproj: &Path,
    model: NonNull<ffi::LlamaModel>,
    use_gpu: bool,
    n_threads: i32,
) -> Result<NonNull<ffi::MtmdContext>, EmbedError> {
    if !mmproj.is_file() {
        return Err(EmbedError::ModelNotFound(mmproj.to_path_buf()));
    }
    let c = path_to_cstring(mmproj)?;
    // SAFETY: c is valid for the call; model is live.
    let raw = unsafe { ffi::wse_mtmd_init(c.as_ptr(), model.as_ptr(), use_gpu, n_threads) };
    NonNull::new(raw).ok_or_else(|| EmbedError::MtmdFailed(format!("mmproj init failed: {}", mmproj.display())))
}

impl Drop for EmbedRuntime {
    fn drop(&mut self) {
        self.free_handles();
    }
}

/// Packs tokenized sequences into contiguous index ranges where each range has
/// at most `max_seqs` sequences and at most `budget` total tokens.
fn plan_sub_batches(
    tokenized: &[Vec<i32>],
    max_seqs: usize,
    budget: usize,
) -> Vec<std::ops::Range<usize>> {
    let max_seqs = max_seqs.max(1);
    let budget = budget.max(1);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < tokenized.len() {
        let mut end = start;
        let mut used = 0;
        while end < tokenized.len()
            && (end - start) < max_seqs
            && used + tokenized[end].len() <= budget
        {
            used += tokenized[end].len();
            end += 1;
        }
        if end == start {
            end = start + 1;
        }
        ranges.push(start..end);
        start = end;
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cpu_threads_preserve_interactive_headroom() {
        let threads = LoadOptions::default().n_threads;
        assert!((1..=8).contains(&threads), "unbounded logical CPU use: {threads}");
    }

    #[test]
    fn sub_batches_respect_token_budget_and_max_seqs() {
        // Three 200-token sequences with a 512-token budget must split into [0..2, 2..3].
        let seqs = vec![vec![1i32; 200], vec![1i32; 200], vec![1i32; 200]];
        let batches = plan_sub_batches(&seqs, 32, 512);
        assert_eq!(batches, vec![0..2, 2..3]);

        // Five 10-token sequences with max_seqs = 2 must split by sequence count.
        let short = vec![vec![1i32; 10]; 5];
        let batches = plan_sub_batches(&short, 2, 512);
        assert_eq!(batches, vec![0..2, 2..4, 4..5]);

        // Full-budget sequence stands alone in its own sub-batch.
        let mixed = vec![vec![1i32; 100], vec![1i32; 512], vec![1i32; 100]];
        let batches = plan_sub_batches(&mixed, 32, 512);
        assert_eq!(batches, vec![0..1, 1..2, 2..3]);
    }
}
