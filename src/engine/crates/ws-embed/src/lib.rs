//! `ws-embed`: llama.cpp FFI for WinSearch embeddings.
//!
//! Layers:
//! - [`ffi`]: hand-written declarations for the llama.cpp / ggml / mtmd C API
//!   (pinned to the tag recorded in `third_party/llama.cpp.md`).
//! - [`backends`]: backend kind + selection policy (CUDA > Vulkan > CPU).
//! - [`vector`]: cosine helpers used for reference-vector verification.
//! - [`runtime`]: `EmbedRuntime`, the owned model/context wrapper.
//!
//! The crate never assumes a GPU. CPU is always available.

pub mod backends;
pub mod ffi;
mod image;
pub mod runtime;
pub mod vector;

pub use backends::{select_backend, BackendKind, BackendPreference};
pub use runtime::{EmbedError, EmbedRuntime, LoadOptions};
