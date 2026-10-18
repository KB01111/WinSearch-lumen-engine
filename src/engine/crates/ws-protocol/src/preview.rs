use serde::{Deserialize, Serialize};
use crate::{EntryReference, MatchLocation, ResolvedEntry};

pub const MAX_PREVIEW_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_PREVIEW_TEXT_PARTS: usize = 64;
pub const MAX_PREVIEW_EXTRACTIONS: usize = 8;

/// Locations supported by the bounded cached-text capability.
pub fn valid_preview_location(location:Option<MatchLocation>)->bool {
    match location {
        None=>true,
        Some(MatchLocation::Page {page})=>(1..=5000).contains(&page),
        Some(MatchLocation::Line {line})=>(1..=64*1024*1024).contains(&line),
        Some(MatchLocation::Timestamp {seconds,end_seconds})=>seconds.is_finite()
            && end_seconds.is_finite() && seconds>=0.0 && end_seconds>=seconds
            && end_seconds<=366.0*86400.0,
        _=>false,
    }
}

pub fn valid_code_preview_location(location:Option<MatchLocation>)->bool {
    match location {
        None=>true,
        Some(MatchLocation::Line {line})=>(1..=64*1024*1024).contains(&line),
        Some(MatchLocation::Notebook {line,end_line,..})=>line>0 && end_line>=line && end_line<=64*1024*1024,
        _=>false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreviewRequest {
    pub reference: EntryReference,
    #[serde(default)]
    pub location: Option<MatchLocation>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreviewState {
    Ready,
    NotIndexed,
    NoCachedText,
    LocationUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreviewTextPart {
    pub kind: String,
    pub text: String,
    pub location: Option<MatchLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreviewExtraction {
    pub kind: String,
    /// Completion under the product's extraction limit, never full source coverage.
    pub complete_within_limit: bool,
    /// Unknown unless established by actual extractor metadata.
    pub source_units: Option<u64>,
    pub indexed_units: Option<u64>,
    pub capped: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntryPreview {
    /// Current metadata/policy identity; its source_revision is not a content hash.
    pub entry: ResolvedEntry,
    pub state: PreviewState,
    pub parts: Vec<PreviewTextPart>,
    /// The reply's text/part budget was reached, independently of extraction coverage.
    pub truncated: bool,
    pub extraction: Vec<PreviewExtraction>,
}
