//! Shared application admission and ranking. Neither depends on evaluation
//! labels, query-specific exceptions, or the corpus being searched.

/// Conservative common floor selected on frozen development data with the
/// pinned model and task-aware recipe. Raw-model cosine is not a probability.
pub const MIN_SEMANTIC_COSINE: f32 = 0.65;
pub const SEMANTIC_TOP_K: usize = 200;

pub fn semantic_admitted(kind: &str, cosine: f32) -> bool {
    (crate::ALL_SEGMENT_KINDS.contains(&kind) || matches!(kind, "pdf_visual" | "code"))
        && cosine.is_finite()
        && (MIN_SEMANTIC_COSINE..=1.001).contains(&cosine)
}

pub fn hybrid_base_score(cosine: f32) -> f64 {
    0.45 + 0.45 * cosine as f64
}

fn tokenize_lower(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

pub fn exact_token_boost(query_text: &str, name_lower: &str) -> f64 {
    let name_tokens = tokenize_lower(name_lower);
    if tokenize_lower(query_text)
        .iter()
        .any(|q| name_tokens.contains(q))
    {
        0.10
    } else {
        0.0
    }
}

pub fn recency_boost(mtime: i64, now: i64) -> f64 {
    if mtime > 0 && now.saturating_sub(mtime) <= 30 * 86_400 {
        0.02
    } else {
        0.0
    }
}

pub fn hybrid_score(cosine: f32, query_text: &str, name_lower: &str, mtime: i64, now: i64) -> f64 {
    (hybrid_base_score(cosine)
        + exact_token_boost(query_text, name_lower)
        + recency_boost(mtime, now))
    .clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_fails_closed_for_invalid_scores_and_unknown_sources() {
        for kind in crate::ALL_SEGMENT_KINDS
            .iter()
            .copied()
            .chain(["pdf_visual", "code"])
        {
            assert!(semantic_admitted(kind, 0.65));
            assert!(semantic_admitted(kind, 1.0));
            for score in [0.64, -1.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 1.01] {
                assert!(!semantic_admitted(kind, score), "{kind}: {score}");
            }
        }
        assert!(!semantic_admitted("unrecognized", 1.0));
    }
}
