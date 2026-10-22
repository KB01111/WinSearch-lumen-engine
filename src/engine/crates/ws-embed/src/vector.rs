//! Vector helpers used to verify embeddings against reference vectors.

/// Cosine similarity computed with f64 accumulation.
///
/// Returns `None` when lengths differ or either vector has zero norm.
pub fn cosine(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let (mut dot, mut na, mut nb) = (0f64, 0f64, 0f64);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some(dot / (na.sqrt() * nb.sqrt()))
}

/// Tolerance used for reference-vector checks: `1 - cos(actual, expected)`.
pub const REFERENCE_COSINE_TOLERANCE: f64 = 1e-3;

/// Whether `actual` matches `expected` within [`REFERENCE_COSINE_TOLERANCE`].
pub fn matches_reference(actual: &[f32], expected: &[f32]) -> bool {
    match cosine(actual, expected) {
        Some(c) => 1.0 - c <= REFERENCE_COSINE_TOLERANCE,
        None => false,
    }
}

/// L2-normalize in place. A zero vector is left unchanged.
pub fn l2_normalize(v: &mut [f32]) {
    let norm = v
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x = (f64::from(*x) / norm) as f32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_vectors_have_cosine_one() {
        let v = [1.0f32, 2.0, 3.0];
        assert!((cosine(&v, &v).unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn orthogonal_vectors_have_cosine_zero() {
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).unwrap().abs() < 1e-12);
    }

    #[test]
    fn opposite_vectors_have_cosine_minus_one() {
        assert!((cosine(&[1.0, 2.0], &[-1.0, -2.0]).unwrap() + 1.0).abs() < 1e-12);
    }

    #[test]
    fn scale_invariant() {
        let a = [0.3f32, -1.2, 4.5];
        let b: Vec<f32> = a.iter().map(|x| x * 7.0).collect();
        assert!((cosine(&a, &b).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn rejects_mismatched_or_degenerate_input() {
        assert!(cosine(&[1.0], &[1.0, 2.0]).is_none());
        assert!(cosine(&[], &[]).is_none());
        assert!(cosine(&[0.0, 0.0], &[1.0, 1.0]).is_none());
    }

    #[test]
    fn reference_match_accepts_tiny_noise_and_rejects_drift() {
        let expected = [0.6f32, 0.8, 0.0];
        let noisy = [0.6001f32, 0.7999, 0.0001];
        assert!(matches_reference(&noisy, &expected));
        let drifted = [0.8f32, 0.6, 0.0];
        assert!(!matches_reference(&drifted, &expected));
    }

    #[test]
    fn l2_normalize_produces_unit_norm() {
        let mut v = [3.0f32, 4.0];
        l2_normalize(&mut v);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
        let mut z = [0.0f32, 0.0];
        l2_normalize(&mut z);
        assert_eq!(z, [0.0, 0.0]);
    }
}
