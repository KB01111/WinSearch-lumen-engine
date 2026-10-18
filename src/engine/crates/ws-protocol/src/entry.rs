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
}
