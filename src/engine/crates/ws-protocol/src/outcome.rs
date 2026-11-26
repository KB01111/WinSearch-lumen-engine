use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchDisposition {
    Complete,
    Superseded,
    Canceled,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticPhase {
    Disabled,
    Ready,
    Degraded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SemanticIssue {
    ModelUnavailable,
    NoVectors,
    EmbeddingFailed,
    RetrievalFailed,
    IndexChanged,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SemanticOutcome {
    pub phase: SemanticPhase,
    pub reason: Option<SemanticIssue>,
}

impl SemanticOutcome {
    pub const DISABLED: Self = Self {
        phase: SemanticPhase::Disabled,
        reason: None,
    };
    pub const READY: Self = Self {
        phase: SemanticPhase::Ready,
        reason: None,
    };
    pub fn degraded(reason: SemanticIssue) -> Self {
        Self {
            phase: SemanticPhase::Degraded,
            reason: Some(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchOutcome {
    pub disposition: SearchDisposition,
    pub semantic: Option<SemanticOutcome>,
}

impl SearchOutcome {
    pub fn complete(semantic: SemanticOutcome) -> Self {
        Self {
            disposition: SearchDisposition::Complete,
            semantic: Some(semantic),
        }
    }
    pub fn interrupted(disposition: SearchDisposition) -> Self {
        Self {
            disposition,
            semantic: None,
        }
    }
    pub fn valid_for(&self, done: bool, empty_batch: bool, requested_semantic: bool) -> bool {
        if !done {
            return false;
        }
        match self.disposition {
            SearchDisposition::Complete => self.semantic.is_some_and(|semantic| {
                (requested_semantic || semantic.phase == SemanticPhase::Disabled)
                    && match semantic.phase {
                        SemanticPhase::Disabled | SemanticPhase::Ready => semantic.reason.is_none(),
                        SemanticPhase::Degraded => semantic.reason.is_some(),
                    }
            }),
            _ => empty_batch && self.semantic.is_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_outcomes_require_consistent_disposition_and_semantic_state() {
        for semantic in [
            SemanticOutcome::DISABLED,
            SemanticOutcome::READY,
            SemanticOutcome::degraded(SemanticIssue::NoVectors),
        ] {
            assert!(SearchOutcome::complete(semantic).valid_for(true, true, true));
            assert!(!SearchOutcome::complete(semantic).valid_for(false, true, true));
        }
        assert!(!SearchOutcome::complete(SemanticOutcome::READY).valid_for(true, true, false));
        assert!(SearchOutcome::complete(SemanticOutcome::DISABLED).valid_for(true, false, false));
        for disposition in [
            SearchDisposition::Superseded,
            SearchDisposition::Canceled,
            SearchDisposition::Failed,
        ] {
            let outcome = SearchOutcome::interrupted(disposition);
            assert!(outcome.valid_for(true, true, true));
            assert!(!outcome.valid_for(true, false, true));
            assert!(!outcome.valid_for(false, true, true));
            assert!(!SearchOutcome {
                semantic: Some(SemanticOutcome::DISABLED),
                ..outcome
            }
            .valid_for(true, true, true));
        }
        assert!(
            !SearchOutcome::interrupted(SearchDisposition::Complete).valid_for(true, true, true)
        );
        for semantic in [
            SemanticOutcome {
                phase: SemanticPhase::Ready,
                reason: Some(SemanticIssue::EmbeddingFailed),
            },
            SemanticOutcome {
                phase: SemanticPhase::Disabled,
                reason: Some(SemanticIssue::NoVectors),
            },
            SemanticOutcome {
                phase: SemanticPhase::Degraded,
                reason: None,
            },
        ] {
            assert!(!SearchOutcome::complete(semantic).valid_for(true, true, true));
        }
    }

    #[test]
    fn outcome_reasons_are_closed_codes_and_legacy_results_stay_unknown() {
        let legacy: crate::ServerMessage =
            serde_json::from_str(r#"{"type":"results","id":1,"batch":[],"done":true}"#).unwrap();
        assert!(matches!(
            legacy,
            crate::ServerMessage::Results { outcome: None, .. }
        ));
        let outcome =
            SearchOutcome::complete(SemanticOutcome::degraded(SemanticIssue::ModelUnavailable));
        let encoded = serde_json::to_string(&outcome).unwrap();
        assert!(encoded.contains("model_unavailable"));
        assert!(serde_json::from_str::<SearchOutcome>(r#"{"disposition":"complete","semantic":{"phase":"degraded","reason":"private worker exception"}}"#).is_err());
        assert!(serde_json::from_str::<SearchOutcome>(r#"{"disposition":"complete","semantic":{"phase":"ready","reason":null},"path":"C:/private"}"#).is_err());
    }
}
