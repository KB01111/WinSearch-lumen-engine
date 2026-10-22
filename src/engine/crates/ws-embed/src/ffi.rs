//! Raw FFI declarations for the flat C shim (`shim/ws_embed_shim.cpp`).
//!
//! Only pointers and scalars cross this boundary. The large by-value llama.cpp
//! parameter structs stay on the C++ side. All functions here are `unsafe`
//! to call; the safe wrappers live in [`crate::runtime`] and [`crate::backends`].

use std::os::raw::{c_char, c_void};

/// Opaque handle types (never dereferenced from Rust).
#[repr(C)]
pub struct LlamaModel {
    _p: [u8; 0],
}
#[repr(C)]
pub struct LlamaContext {
    _p: [u8; 0],
}
#[repr(C)]
pub struct MtmdContext {
    _p: [u8; 0],
}
#[repr(C)]
pub struct GgmlBackendDevice {
    _p: [u8; 0],
}

/// `ggml_log_callback`: `(level, text, user_data)`.
pub type GgmlLogCallback =
    Option<unsafe extern "C" fn(level: i32, text: *const c_char, user: *mut c_void)>;

/// `ggml_backend_dev_type` values returned by `wse_dev_type`.
pub const DEV_TYPE_CPU: i32 = 0;
pub const DEV_TYPE_GPU: i32 = 1;
pub const DEV_TYPE_IGPU: i32 = 2;
pub const DEV_TYPE_ACCEL: i32 = 3;
pub const DEV_TYPE_META: i32 = 4;

extern "C" {
    pub fn wse_backend_load_all();
    pub fn wse_backend_load_all_from_path(dir: *const c_char);
    pub fn wse_dev_count() -> usize;
    pub fn wse_dev_get(index: usize) -> *mut GgmlBackendDevice;
    pub fn wse_dev_backend_name(dev: *mut GgmlBackendDevice, buf: *mut c_char, cap: usize)
        -> usize;
    pub fn wse_dev_type(dev: *mut GgmlBackendDevice) -> i32;
    pub fn wse_dev_memory(
        dev: *mut GgmlBackendDevice,
        free_bytes: *mut usize,
        total_bytes: *mut usize,
    );
    pub fn wse_dev_name(dev: *mut GgmlBackendDevice, buf: *mut c_char, cap: usize) -> usize;

    pub fn wse_log_set(cb: GgmlLogCallback, user: *mut c_void);

    pub fn wse_model_load(
        path: *const c_char,
        dev: *mut GgmlBackendDevice,
        n_gpu_layers: i32,
    ) -> *mut LlamaModel;
    pub fn wse_model_free(model: *mut LlamaModel);
    pub fn wse_model_n_embd(model: *const LlamaModel) -> i32;

    pub fn wse_context_new(
        model: *mut LlamaModel,
        n_ctx: u32,
        n_batch: u32,
        n_threads: i32,
    ) -> *mut LlamaContext;
    pub fn wse_context_max_seqs() -> i32;
    pub fn wse_context_free(ctx: *mut LlamaContext);
    pub fn wse_context_pooling_type(ctx: *const LlamaContext) -> i32;

    pub fn wse_tokenize(
        model: *const LlamaModel,
        text: *const c_char,
        text_len: i32,
        out: *mut i32,
        cap: i32,
    ) -> i32;
    pub fn wse_embed_batch(
        ctx: *mut LlamaContext,
        tokens: *const i32,
        seq_lens: *const i32,
        n_seqs: i32,
        n_embd: i32,
        out: *mut f32,
    ) -> i32;

    pub fn wse_mtmd_init(
        mmproj_path: *const c_char,
        text_model: *const LlamaModel,
        use_gpu: bool,
        n_threads: i32,
    ) -> *mut MtmdContext;
    pub fn wse_mtmd_free(ctx: *mut MtmdContext);
    pub fn wse_mtmd_supports_vision(ctx: *const MtmdContext) -> i32;
    pub fn wse_mtmd_supports_audio(ctx: *const MtmdContext) -> i32;
    pub fn wse_mtmd_embed_image(
        mctx: *mut MtmdContext,
        lctx: *mut LlamaContext,
        rgb: *const u8,
        width: u32,
        height: u32,
        n_embd: i32,
        out: *mut f32,
    ) -> i32;
    pub fn wse_mtmd_embed_audio(
        mctx: *mut MtmdContext,
        lctx: *mut LlamaContext,
        pcm: *const f32,
        n_samples: usize,
        n_embd: i32,
        out: *mut f32,
    ) -> i32;
}
