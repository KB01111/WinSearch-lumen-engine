use crate::{EntryReference, ResultKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelatedQuery {
    pub source: EntryReference,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub categories: Vec<ResultKind>,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_kinds: Vec<crate::FileKind>,
}

fn default_limit() -> u32 {
    20
}

impl RelatedQuery {
    pub fn is_valid(&self) -> bool {
        self.source.is_bound()
            && (1..=10_000).contains(&self.limit)
            && self.categories.len() <= 32
            && crate::valid_search_extensions(&self.extensions)
            && crate::valid_search_file_kinds(&self.file_kinds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query() -> RelatedQuery {
        RelatedQuery {
            source: EntryReference {
                result_id: "e1".into(),
                source_revision: Some("current-source-proof".into()),
            },
            limit: 20,
            categories: vec![],
            extensions: vec![".PDF".into()],
            file_kinds: Vec::new(),
        }
    }

    #[test]
    fn related_requires_bound_indexed_source_and_bounded_native_query() {
        let valid = query();
        assert!(valid.is_valid());
        for id in ["s1", "code:1:2", "e0", "e01", "C:/private.txt"] {
            let mut invalid = valid.clone();
            invalid.source.result_id = id.into();
            assert!(!invalid.is_valid());
        }
        let mut invalid = valid.clone();
        invalid.source.source_revision = None;
        assert!(!invalid.is_valid());
        for limit in [0, 10_001, u32::MAX] {
            let mut invalid = valid.clone();
            invalid.limit = limit;
            assert!(!invalid.is_valid());
        }
        let mut invalid = valid.clone();
        invalid.extensions = vec!["../txt".into()];
        assert!(!invalid.is_valid());
        let mut invalid = valid.clone();
        invalid.categories = vec![ResultKind::File; 33];
        assert!(!invalid.is_valid());
        let mut invalid = valid.clone();
        invalid.file_kinds = vec![crate::FileKind::Pdf; 14];
        assert!(!invalid.is_valid());
        let mut code = valid.clone();
        code.source.result_id = "c12".into();
        assert!(code.is_valid());
    }

    #[test]
    fn related_is_a_separate_typed_operation_without_caller_paths_or_query_text() {
        let message = crate::ClientMessage::Related {
            id: 9,
            query: query(),
        };
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(json["type"], "related");
        assert!(json["query"].get("text").is_none());
        assert_eq!(
            serde_json::from_value::<crate::ClientMessage>(json).unwrap(),
            message
        );
        for field in ["path", "text", "embedding"] {
            let mut json = serde_json::to_value(query()).unwrap();
            json[field] = serde_json::json!("private");
            assert!(serde_json::from_value::<RelatedQuery>(json).is_err());
        }
    }
}
