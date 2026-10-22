//! Windows platform layer for the WinSearch engine: per-user named pipes,
//! data paths, current-user identity, and single-instance guards.

pub mod child_job;
pub mod content;
pub mod directory_guard;
pub mod identity;
pub mod ifilter;
pub mod npu;
pub mod ocr;
pub mod paths;
pub mod pdfium;
pub mod pipe;
pub mod single_instance;
pub mod source_guard;
pub mod wic;

pub use identity::current_user_sid;
pub use single_instance::SingleInstanceGuard;
