//! Consented provider output is attached to an existing native source, never a path.
use crate::{EntryReference, MatchLocation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentKind {
    Ocr,
    Transcript,
}
impl EnrichmentKind {
    pub fn storage_kind(self) -> &'static str {
        match self {
            Self::Ocr => "enriched_ocr",
            Self::Transcript => "enriched_transcript",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentPart {
    pub text: String,
    pub location: Option<MatchLocation>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentRequest {
    pub index_id: String,
    pub reference: EntryReference,
    pub expected_sha256: String,
    pub model_generation: i64,
    /// Host-native admission ticket. The host must recheck it before dispatch
    /// and delivery; its echo is not a daemon-issued consent grant.
    pub grant_generation: u64,
    pub kind: EnrichmentKind,
    pub parts: Vec<EnrichmentPart>,
}
impl EnrichmentRequest {
    pub fn is_valid(&self) -> bool {
        crate::valid_index_id(&self.index_id)
            && self.reference.is_bound()
            && matches!(
                self.reference.indexed_id(),
                Some(crate::IndexedEntryId::File(_))
            )
            && crate::valid_content_sha256(&self.expected_sha256)
            && self.model_generation >= 0
            && self.grant_generation > 0
            && !self.parts.is_empty()
            && self.parts.len() <= 128
            && self.parts.iter().map(|part| part.text.len()).sum::<usize>() <= 64 * 1024
            && self.parts.iter().all(|part| {
                !part.text.trim().is_empty()
                    && !part.text.contains('\0')
                    && match (self.kind, part.location) {
                        (_, None) => true,
                        (
                            EnrichmentKind::Transcript,
                            Some(MatchLocation::Timestamp {
                                seconds,
                                end_seconds,
                            }),
                        ) => {
                            seconds.is_finite()
                                && end_seconds.is_finite()
                                && seconds >= 0.0
                                && seconds < end_seconds
                                && end_seconds <= 366.0 * 24.0 * 60.0 * 60.0
                        }
                        _ => false,
                    }
            })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentIngested {
    pub index_id: String,
    pub reference: EntryReference,
    pub content_revision: i64,
    pub model_generation: i64,
    pub grant_generation: u64,
    pub duplicate: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> EnrichmentRequest {
        EnrichmentRequest {
            index_id: "a".repeat(32),
            reference: EntryReference {
                result_id: "e1".into(),
                source_revision: Some("proof".into()),
            },
            expected_sha256: "b".repeat(64),
            model_generation: 0,
            grant_generation: 1,
            kind: EnrichmentKind::Ocr,
            parts: vec![EnrichmentPart {
                text: "source OCR".into(),
                location: None,
            }],
        }
    }
    #[test]
    fn enrichment_rejects_missing_hash_cross_namespace_and_oversized_provider_text() {
        let valid = request();
        assert!(valid.is_valid());
        let mut invalid = valid.clone();
        invalid.expected_sha256.clear();
        assert!(!invalid.is_valid());
        let mut invalid = valid.clone();
        invalid.reference.result_id = "c1".into();
        assert!(!invalid.is_valid());
        let mut invalid = valid.clone();
        invalid.parts[0].text = "x".repeat(65537);
        assert!(!invalid.is_valid());
        let mut invalid = valid.clone();
        invalid.parts.clear();
        assert!(!invalid.is_valid());
        let mut invalid = valid;
        invalid.grant_generation = 0;
        assert!(!invalid.is_valid());
    }
    #[test]
    fn enrichment_has_no_path_escape_or_fabricated_location() {
        let mut value = serde_json::to_value(request()).unwrap();
        value["path"] = "C:/private/source.png".into();
        assert!(serde_json::from_value::<EnrichmentRequest>(value).is_err());
        let mut invalid = request();
        invalid.kind = EnrichmentKind::Transcript;
        invalid.parts[0].location = Some(MatchLocation::Timestamp {
            seconds: f64::NAN,
            end_seconds: 1.0,
        });
        assert!(!invalid.is_valid());
        invalid.parts[0].location = Some(MatchLocation::Timestamp {
            seconds: 2.0,
            end_seconds: 1.0,
        });
        assert!(!invalid.is_valid());
        invalid.parts[0].location = Some(MatchLocation::Page { page: 1 });
        assert!(!invalid.is_valid());
    }
}
