use serde::{Deserialize, Serialize};

use crate::ResultKind;

/// Native consumers resolve an indexed identity, never a caller supplied path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EntryReference {
    pub result_id: String,
    #[serde(default)]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexedEntryId {
    File(i64),
    Code(i64),
}

impl EntryReference {
    pub fn indexed_id(&self) -> Option<IndexedEntryId> {
        let prefix = self.result_id.chars().next()?;
        if !matches!(prefix, 'e' | 'c') {
            return None;
        }
        let id = self.result_id.get(1..)?.parse::<i64>().ok()?;
        if id <= 0 || self.result_id != format!("{prefix}{id}") {
            return None;
        }
        Some(if prefix == 'c' {
            IndexedEntryId::Code(id)
        } else {
            IndexedEntryId::File(id)
        })
    }
    pub fn is_valid(&self) -> bool {
        self.indexed_id().is_some()
            && self
                .source_revision
                .as_ref()
                .is_none_or(|proof| !proof.trim().is_empty() && proof.len() <= 64 * 1024)
    }
    pub fn is_bound(&self) -> bool {
        self.is_valid() && self.source_revision.is_some()
    }
}

pub fn valid_content_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedEntry {
    pub result_id: String,
    pub root_path: String,
    pub path: String,
    pub name: String,
    pub kind: ResultKind,
    pub size_bytes: u64,
    pub modified_unix_seconds: Option<i64>,
    pub index_revision: Option<i64>,
    /// Opaque metadata/identity and policy proof; this is not a content hash.
    pub source_revision: String,
    /// Actual complete source-byte digest when established by current extraction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}
