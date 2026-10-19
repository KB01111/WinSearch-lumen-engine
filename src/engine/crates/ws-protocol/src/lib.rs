//! WinSearch IPC protocol.
//!
//! Typed request/response messages exchanged between the WinUI frontend and
//! `winsearchd` over a per-user named pipe, plus the length-prefixed JSON frame
//! codec. The protocol is versioned: peers exchange `PROTOCOL_VERSION` during
//! the handshake and refuse mismatched majors.

pub mod entry;
pub mod framing;
pub mod messages;
pub mod outcome;
pub mod related;
pub use related::RelatedQuery;
pub mod preview;

pub use entry::{EntryReference, ResolvedEntry, IndexedEntryId, valid_content_sha256};
pub use framing::{read_message, write_message, FrameError, MAX_FRAME_BYTES};
pub use messages::*;
pub use outcome::*;
pub use preview::*;

// Model lifecycle snapshots ride the wire verbatim; the canonical shape lives
// in ws-modelmgr so Rust and the C# mirror cannot drift.
pub use ws_modelmgr::{ModelState, ModelStatus};
