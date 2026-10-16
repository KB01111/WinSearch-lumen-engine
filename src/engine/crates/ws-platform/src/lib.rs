//! Windows platform layer for the WinSearch engine: per-user named pipes,
//! data paths, current-user identity, and single-instance guards.

pub mod identity;
pub mod content;
pub mod child_job;
pub mod ifilter;
pub mod ocr;
pub mod paths;
pub mod pdfium;
pub mod pipe;
pub mod single_instance;
pub mod wic;

pub use identity::current_user_sid;
pub use single_instance::SingleInstanceGuard;
