//! Pure-Rust vector math and the in-memory brute-force scan index.
//!
//! Storage format: vectors are L2-normalized, encoded as FP16 little-endian
//! bytes (`half` crate), with the original L2 norm kept alongside for
//! diagnostics. Search decodes FP16 on the fly and accumulates the dot product
//! in `f32`; for unit-length vectors the dot product is the cosine.
//!
//! `VectorIndex` is the contiguous scan structure: one flat `Vec<u8>` FP16
//! plane (`num_vectors * dim * 2` bytes) plus parallel id/entry/kind/norm
//! arrays. It is built at startup from the `segments` table (see
//! [`VectorIndex::from_segments`]) and updated incrementally as segments land
//! (`add_or_update` / `remove` / `remove_by_entry`). No OS calls, no threads,
//! no async — safe to drive from a plain `std::thread` worker.
//!
//! Memory note: 768-d FP16 is 1536 bytes per segment. At Phase 3 scale (one
//! name-segment per entry) 1M entries ≈ 1.5 GB in memory; keep in-memory for
//! now and measure in `docs/benchmarks.md`. Contingency (only if measured too
//! high): an mmap-backed sidecar file.

use std::collections::{HashMap, HashSet};

use crate::model::{Segment, VectorFilter};

/// Expected production embedding width (EmbeddingGemma 2 unified space).
/// `VectorIndex` itself accepts any `dim > 0` so unit tests can use tiny dims.
pub const EMBEDDING_DIM: usize = 768;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VectorError {
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    #[error("empty vector")]
    EmptyVector,
}

/// L2 norm with `f64` accumulation so finite `f32` values near `1e20` do not
/// overflow `x * x`. Returns `0.0` for empty input.
pub fn l2_norm(v: &[f32]) -> f32 {
    let mut sum = 0.0f64;
    for &x in v {
        let xf = f64::from(x);
        sum += xf * xf;
    }
    sum.sqrt() as f32
}

/// Normalizes `v` to unit length. Returns `(normalized, norm)` where `norm`
/// is the original L2 length. Zero, empty, or non-finite inputs yield an
/// all-zero vector with norm `0.0` (cosine against them is defined as `0.0`).
pub fn normalize_vector(v: &[f32]) -> (Vec<f32>, f32) {
    if v.is_empty() {
        return (Vec::new(), 0.0);
    }
    if !v.iter().all(|x| x.is_finite()) {
        return (vec![0.0; v.len()], 0.0);
    }
    let mut sum = 0.0f64;
    for &x in v {
        let xf = f64::from(x);
        sum += xf * xf;
    }
    let norm_f64 = sum.sqrt();
    if norm_f64 == 0.0 || !norm_f64.is_finite() {
        return (vec![0.0; v.len()], 0.0);
    }
    let norm = (norm_f64 as f32).min(f32::MAX);
    (
        v.iter()
            .map(|&x| (f64::from(x) / norm_f64) as f32)
            .collect(),
        norm,
    )
}

/// Encodes (already normalized) `f32` values as FP16 little-endian bytes.
pub fn encode_fp16_vector(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 2);
    for &x in v {
        out.extend_from_slice(&half::f16::from_f32(x).to_le_bytes());
    }
    out
}

/// Decodes FP16 little-endian bytes back to `f32`. Trailing odd byte (if any)
/// is ignored.
pub fn decode_fp16_vector(bytes: &[u8]) -> Vec<f32> {
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let bits = u16::from_le_bytes([chunk[0], chunk[1]]);
        out.push(half::f16::from_bits(bits).to_f32());
    }
    out
}

/// Normalizes `v` then encodes it as FP16 bytes. Returns `(bytes, norm)`.
pub fn encode_normalized_vector(v: &[f32]) -> (Vec<u8>, f32) {
    let (normed, norm) = normalize_vector(v);
    (encode_fp16_vector(&normed), norm)
}

/// Cosine similarity with `f32` accumulation. Returns `0.0` for empty,
/// mismatched-length, zero-norm, or non-finite inputs. Output is clamped to
/// `[-1, 1]`.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0f32;
    let mut na = 0f32;
    let mut nb = 0f32;
    for (&x, &y) in a.iter().zip(b.iter()) {
        if !x.is_finite() || !y.is_finite() {
            return 0.0;
        }
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = (na * nb).sqrt();
    if denom == 0.0 || !denom.is_finite() {
        return 0.0;
    }
    (dot / denom).clamp(-1.0, 1.0)
}

/// Cosine between one stored FP16-normalized vector and a raw `f32` query.
/// The query is normalized internally; a zero/non-finite query yields `0.0`.
/// Length mismatch (`stored.len() / 2 != query.len()`) yields `0.0`.
///
/// Single-shot convenience wrapper: batch callers (e.g. [`VectorIndex::search`],
/// `IndexStore::search_vectors`) normalize the query once and call
/// [`dot_fp16_row_vs_normalized`] per row instead.
pub fn cosine_fp16_vs_f32(stored_fp16: &[u8], stored_norm: f32, query: &[f32]) -> f32 {
    if stored_norm == 0.0 || !stored_norm.is_finite() {
        return 0.0;
    }
    if query.is_empty() {
        return 0.0;
    }
    let (normed_query, query_norm) = normalize_vector(query);
    if query_norm == 0.0 {
        return 0.0;
    }
    dot_fp16_row_vs_normalized(stored_fp16, &normed_query)
}

/// Dot product of one FP16-normalized row against an already-normalized query,
/// with `f32` accumulation and output clamped to `[-1, 1]`.
///
/// This is the shared scoring kernel used by both [`VectorIndex::search`] and
/// `IndexStore::search_vectors`, so the two paths always agree. Callers must
/// pass a unit-length query; byte-width mismatch
/// (`stored.len() != query.len() * 2`) yields `0.0`.
pub fn dot_fp16_row_vs_normalized(stored_fp16: &[u8], normed_query: &[f32]) -> f32 {
    if stored_fp16.len() != normed_query.len() * 2 {
        return 0.0;
    }
    let mut dot = 0f32;
    for (i, chunk) in stored_fp16.chunks_exact(2).enumerate() {
        let bits = u16::from_le_bytes([chunk[0], chunk[1]]);
        dot += half::f16::from_bits(bits).to_f32() * normed_query[i];
    }
    dot.clamp(-1.0, 1.0)
}

/// One scored in-memory hit (ids + kind; join metadata from SQLite as needed).
#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub id: i64,
    pub entry_id: i64,
    pub kind: String,
    pub cosine: f32,
}

/// Contiguous brute-force scan index over FP16 planes.
///
/// Layout: `data.len() == ids.len() * dim * 2`. `pos` maps segment id to its
/// slot. Removal is swap-remove so iteration order is not stable; search
/// output is always re-sorted by score (then id) so callers never observe it.
pub struct VectorIndex {
    dim: usize,
    ids: Vec<i64>,
    entry_ids: Vec<i64>,
    kinds: Vec<String>,
    data: Vec<u8>,
    norms: Vec<f32>,
    pos: HashMap<i64, usize>,
}

impl VectorIndex {
    /// Creates an empty index for `dim`-wide vectors. Panics for `dim == 0`.
    pub fn new(dim: usize) -> Self {
        assert!(dim > 0, "VectorIndex dim must be > 0");
        Self {
            dim,
            ids: Vec::new(),
            entry_ids: Vec::new(),
            kinds: Vec::new(),
            data: Vec::new(),
            norms: Vec::new(),
            pos: HashMap::new(),
        }
    }

    /// Builds an index from decoded [`Segment`]s (startup path). All segments
    /// must share `dim`; mismatches and empty vectors are skipped.
    pub fn from_segments(dim: usize, segments: &[Segment]) -> Self {
        let mut idx = Self::new(dim);
        for seg in segments {
            if seg.vector.len() == dim && !seg.vector.is_empty() {
                let _ = idx.add_or_update(seg.id, seg.entry_id, &seg.kind, &seg.vector);
            }
        }
        idx
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn contains(&self, id: i64) -> bool {
        self.pos.contains_key(&id)
    }

    /// Inserts a new vector or replaces the existing one for `id`
    /// (incremental upsert path). The input is normalized + FP16-encoded.
    pub fn add_or_update(
        &mut self,
        id: i64,
        entry_id: i64,
        kind: &str,
        vector: &[f32],
    ) -> Result<(), VectorError> {
        if vector.is_empty() {
            return Err(VectorError::EmptyVector);
        }
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                actual: vector.len(),
            });
        }
        let (bytes, norm) = encode_normalized_vector(vector);
        debug_assert_eq!(bytes.len(), self.dim * 2);
        if let Some(&slot) = self.pos.get(&id) {
            let off = slot * self.dim * 2;
            self.data[off..off + bytes.len()].copy_from_slice(&bytes);
            self.norms[slot] = norm;
            self.entry_ids[slot] = entry_id;
            self.kinds[slot] = kind.to_string();
            return Ok(());
        }
        let slot = self.ids.len();
        self.ids.push(id);
        self.entry_ids.push(entry_id);
        self.kinds.push(kind.to_string());
        self.data.extend_from_slice(&bytes);
        self.norms.push(norm);
        self.pos.insert(id, slot);
        Ok(())
    }

    /// Removes one segment id. Returns `false` when absent.
    pub fn remove(&mut self, id: i64) -> bool {
        let Some(&slot) = self.pos.get(&id) else {
            return false;
        };
        let last = self.ids.len() - 1;
        let stride = self.dim * 2;
        if slot != last {
            let src = last * stride;
            let dst = slot * stride;
            self.data.copy_within(src..src + stride, dst);
        }
        // `swap_remove` moves the last metadata row into `slot`, matching the
        // byte move above.
        self.ids.swap_remove(slot);
        self.entry_ids.swap_remove(slot);
        self.kinds.swap_remove(slot);
        self.norms.swap_remove(slot);
        self.data.truncate(self.ids.len() * stride);
        self.pos.remove(&id);
        if slot < self.ids.len() {
            let moved = self.ids[slot];
            self.pos.insert(moved, slot);
        }
        true
    }

    /// Removes every vector belonging to `entry_id` (file-deleted path).
    /// Returns the number removed.
    pub fn remove_by_entry(&mut self, entry_id: i64) -> usize {
        let victims: Vec<i64> = self
            .ids
            .iter()
            .filter_map(|&id| {
                let slot = self.pos[&id];
                (self.entry_ids[slot] == entry_id).then_some(id)
            })
            .collect();
        let n = victims.len();
        for id in victims {
            self.remove(id);
        }
        n
    }

    pub fn clear(&mut self) {
        self.ids.clear();
        self.entry_ids.clear();
        self.kinds.clear();
        self.data.clear();
        self.norms.clear();
        self.pos.clear();
    }

    /// Decoded (normalized `f32`) vector for `id`, if present.
    pub fn get_vector(&self, id: i64) -> Option<Vec<f32>> {
        let &slot = self.pos.get(&id)?;
        let stride = self.dim * 2;
        let off = slot * stride;
        Some(decode_fp16_vector(&self.data[off..off + stride]))
    }

    /// Segment kind (`name`, `doc`, `image`, `ocr`, `pdf`, `audio`, `video`)
    /// for `id`, if present.
    pub fn get_kind(&self, id: i64) -> Option<&str> {
        let &slot = self.pos.get(&id)?;
        Some(self.kinds[slot].as_str())
    }

    /// Counts vectors currently stored for `kind`.
    pub fn count_by_kind(&self, kind: &str) -> usize {
        self.kinds.iter().filter(|k| k.as_str() == kind).count()
    }

    /// Removes every vector matching `kind`. Returns the number removed.
    pub fn remove_by_kind(&mut self, kind: &str) -> usize {
        let victims: Vec<i64> = self
            .ids
            .iter()
            .copied()
            .filter(|id| {
                let slot = self.pos[id];
                self.kinds[slot] == kind
            })
            .collect();
        let n = victims.len();
        for id in victims {
            self.remove(id);
        }
        n
    }

    /// Brute-force cosine top-k. The query is normalized once; each stored
    /// FP16 row is scored with [`dot_fp16_row_vs_normalized`] (`f32`
    /// accumulation) — the same kernel `IndexStore::search_vectors` uses.
    ///
    /// Contract (shared with `IndexStore::search_vectors`):
    /// `top_k == 0`, an empty index, an empty query, and a zero/non-finite
    /// query all yield `Ok(vec![])` — an empty query is not an error, matching
    /// the crate's other search APIs. A non-empty query whose width differs
    /// from the index dimension is a programming error and yields
    /// `Err(DimensionMismatch)`; the store instead skips non-matching rows
    /// (it is schemaless and has no single expected width — see
    /// `IndexStore::search_vectors`).
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<VectorHit>, VectorError> {
        self.search_where(query, top_k, filter, |_| true)
    }

    /// Admit in score order before the result limit. Native source checks can
    /// stop once k valid hits are found without probing every indexed file.
    pub fn search_where(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&VectorFilter>,
        mut allowed: impl FnMut(&VectorHit) -> bool,
    ) -> Result<Vec<VectorHit>, VectorError> {
        if top_k == 0 || self.is_empty() {
            return Ok(Vec::new());
        }
        if query.is_empty() {
            // Aligned with `IndexStore::search_vectors`: empty query → no hits.
            return Ok(Vec::new());
        }
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                actual: query.len(),
            });
        }
        let (normed_query, query_norm) = normalize_vector(query);
        if query_norm == 0.0 {
            return Ok(Vec::new());
        }
        let entry_set: Option<HashSet<i64>> = filter
            .and_then(|f| (!f.entry_ids.is_empty()).then(|| f.entry_ids.iter().copied().collect()));

        let stride = self.dim * 2;
        let mut hits = Vec::with_capacity(self.ids.len().min(top_k.saturating_mul(2)));
        for slot in 0..self.ids.len() {
            if let Some(f) = filter {
                if !f.matches_kind(&self.kinds[slot]) {
                    continue;
                }
                if let Some(set) = &entry_set {
                    if !set.contains(&self.entry_ids[slot]) {
                        continue;
                    }
                }
            }
            if self.norms[slot] == 0.0 {
                continue;
            }
            let off = slot * stride;
            let row = &self.data[off..off + stride];
            let dot = dot_fp16_row_vs_normalized(row, &normed_query);
            hits.push(VectorHit {
                id: self.ids[slot],
                entry_id: self.entry_ids[slot],
                kind: self.kinds[slot].clone(),
                cosine: dot,
            });
        }
        hits.sort_by(|a, b| b.cosine.total_cmp(&a.cosine).then_with(|| a.id.cmp(&b.id)));
        Ok(hits
            .into_iter()
            .filter(|hit| allowed(hit))
            .take(top_k)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f32, b: f32, eps: f32) -> bool {
        (a - b).abs() <= eps
    }

    #[test]
    fn fp16_round_trip_preserves_normalized_vectors() {
        // Hand-picked values incl. small/large magnitudes and negatives.
        let cases: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.5, -0.5, 0.5, -0.5],
            vec![0.001, 100.0, -3.25, 0.0],
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        ];
        for v in cases {
            let (normed, norm) = normalize_vector(&v);
            let bytes = encode_fp16_vector(&normed);
            assert_eq!(bytes.len(), v.len() * 2);
            let back = decode_fp16_vector(&bytes);
            assert_eq!(back.len(), v.len());
            for (a, b) in normed.iter().zip(back.iter()) {
                // FP16 has ~3 decimal digits; 1e-3 is the right tolerance.
                assert!(
                    approx_eq(*a, *b, 1e-3),
                    "round-trip diverged: {a} vs {b} (norm {norm})"
                );
            }
            // Norm survives the encode/decode path untouched.
            let (_, norm2) = normalize_vector(&v);
            assert!(approx_eq(norm, norm2, 1e-6));
        }
    }

    #[test]
    fn zero_and_nonfinite_vectors_encode_to_zero_norm() {
        for v in [
            vec![0.0, 0.0, 0.0],
            vec![f32::NAN, 1.0],
            vec![f32::INFINITY, 1.0],
        ] {
            let (normed, norm) = normalize_vector(&v);
            assert_eq!(norm, 0.0);
            assert!(normed.iter().all(|&x| x == 0.0));
            assert_eq!(cosine_similarity(&v, &[1.0, 0.0]), 0.0);
        }
    }

    #[test]
    fn cosine_matches_f64_reference() {
        // Deterministic pseudo-random vectors; f64 is the oracle.
        fn pseudo(i: usize, dim: usize) -> Vec<f32> {
            (0..dim)
                .map(|j| ((i * 2654435761 + j * 40503) % 10000) as f32 / 5000.0 - 1.0)
                .collect()
        }
        for dim in [4, 16, 64] {
            for i in 0..25 {
                let a = pseudo(i, dim);
                let b = pseudo(i * 7 + 3, dim);
                let got = cosine_similarity(&a, &b);
                let mut dot = 0f64;
                let mut na = 0f64;
                let mut nb = 0f64;
                for (&x, &y) in a.iter().zip(b.iter()) {
                    dot += x as f64 * y as f64;
                    na += x as f64 * x as f64;
                    nb += y as f64 * y as f64;
                }
                let want = (dot / (na * nb).sqrt()) as f32;
                assert!(
                    (got - want).abs() <= 1e-5,
                    "dim {dim} pair {i}: f32 {got} vs f64 {want}"
                );
                // FP16 path agrees within quantization tolerance.
                let (bytes, norm) = encode_normalized_vector(&a);
                let fp16_cos = cosine_fp16_vs_f32(&bytes, norm, &b);
                assert!(
                    (fp16_cos - want).abs() <= 5e-3,
                    "fp16 path diverged: {fp16_cos} vs {want}"
                );
            }
        }
        // Known angles.
        assert!(approx_eq(
            cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]),
            1.0,
            1e-6
        ));
        assert!(approx_eq(
            cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]),
            0.0,
            1e-6
        ));
        assert!(approx_eq(
            cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]),
            -1.0,
            1e-6
        ));
    }

    #[test]
    fn incremental_add_update_remove_behaves() {
        let mut idx = VectorIndex::new(3);
        idx.add_or_update(1, 10, "name", &[1.0, 0.0, 0.0]).unwrap();
        idx.add_or_update(2, 20, "name", &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 2);

        // Update in place keeps the slot count and replaces the vector.
        idx.add_or_update(1, 10, "name", &[0.0, 0.0, 1.0]).unwrap();
        assert_eq!(idx.len(), 2);
        let v = idx.get_vector(1).unwrap();
        assert!(cosine_similarity(&v, &[0.0, 0.0, 1.0]) > 0.999);

        // Remove one; the survivor stays searchable.
        assert!(idx.remove(2));
        assert!(!idx.remove(2));
        assert_eq!(idx.len(), 1);
        let hits = idx.search(&[0.0, 0.0, 1.0], 10, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 1);

        // Remove by entry drops every segment of that entry.
        idx.add_or_update(3, 10, "name", &[1.0, 0.0, 0.0]).unwrap();
        idx.add_or_update(4, 10, "name", &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(idx.remove_by_entry(10), 3);
        assert!(idx.is_empty());
    }

    #[test]
    fn top_k_order_and_filters_are_correct() {
        let mut idx = VectorIndex::new(2);
        // Angles from +x axis: id 1 = 0°, 2 = 10°, 3 = 45°, 4 = 90°.
        let deg = std::f32::consts::PI / 180.0;
        idx.add_or_update(1, 1, "name", &[1.0, 0.0]).unwrap();
        idx.add_or_update(2, 2, "name", &[(10.0 * deg).cos(), (10.0 * deg).sin()])
            .unwrap();
        idx.add_or_update(3, 3, "text", &[(45.0 * deg).cos(), (45.0 * deg).sin()])
            .unwrap();
        idx.add_or_update(4, 4, "name", &[0.0, 1.0]).unwrap();

        let hits = idx.search(&[1.0, 0.0], 2, None).unwrap();
        assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![1, 2]);
        assert!(hits[0].cosine >= hits[1].cosine);

        // Kind filter.
        let text_only = VectorFilter::for_kind("text");
        let hits = idx.search(&[1.0, 0.0], 10, Some(&text_only)).unwrap();
        assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![3]);

        // Entry filter.
        let filter = VectorFilter {
            kinds: Vec::new(),
            entry_ids: vec![4, 2],
        };
        let hits = idx.search(&[1.0, 0.0], 10, Some(&filter)).unwrap();
        assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![2, 4]);
    }

    #[test]
    fn search_error_contract_matches_store() {
        // Aligned contract (see `VectorIndex::search` rustdoc): degenerate
        // queries yield Ok(empty); only a width mismatch on this fixed-dim
        // index is an error (the schemaless store skips such rows instead).
        let mut idx = VectorIndex::new(2);
        idx.add_or_update(1, 1, "name", &[1.0, 0.0]).unwrap();

        assert!(idx.search(&[], 10, None).unwrap().is_empty());
        assert!(idx.search(&[0.0, 0.0], 10, None).unwrap().is_empty());
        assert!(idx.search(&[f32::NAN, 1.0], 10, None).unwrap().is_empty());
        assert!(idx.search(&[1.0, 0.0], 0, None).unwrap().is_empty());
        assert!(VectorIndex::new(2)
            .search(&[1.0, 0.0], 10, None)
            .unwrap()
            .is_empty());
        assert_eq!(
            idx.search(&[1.0, 0.0, 0.0], 10, None).unwrap_err(),
            VectorError::DimensionMismatch {
                expected: 2,
                actual: 3
            }
        );
        // `EmptyVector` is still the error for empty inserts (fixed-dim slot
        // cannot hold a widthless vector).
        assert_eq!(
            idx.add_or_update(9, 9, "name", &[]).unwrap_err(),
            VectorError::EmptyVector
        );
        assert_eq!(
            idx.add_or_update(9, 9, "name", &[1.0]).unwrap_err(),
            VectorError::DimensionMismatch {
                expected: 2,
                actual: 1
            }
        );
    }

    #[test]
    fn multi_kind_segments_filter_and_remove_by_kind() {
        use crate::model::{
            ALL_SEGMENT_KINDS, CONTENT_SEGMENT_KINDS, SEGMENT_KIND_AUDIO, SEGMENT_KIND_DOC,
            SEGMENT_KIND_IMAGE, SEGMENT_KIND_NAME, SEGMENT_KIND_OCR, SEGMENT_KIND_PDF,
            SEGMENT_KIND_VIDEO,
        };
        let mut idx = VectorIndex::new(2);
        for (i, &kind) in ALL_SEGMENT_KINDS.iter().enumerate() {
            idx.add_or_update((i + 1) as i64, 42, kind, &[1.0, 0.0])
                .unwrap();
        }
        assert_eq!(idx.len(), 9);
        assert_eq!(idx.get_kind(1), Some(SEGMENT_KIND_NAME));
        assert_eq!(idx.get_kind(2), Some(SEGMENT_KIND_DOC));
        assert_eq!(idx.get_kind(3), Some(SEGMENT_KIND_IMAGE));
        assert_eq!(idx.get_kind(4), Some(SEGMENT_KIND_OCR));
        assert_eq!(idx.get_kind(5), Some(SEGMENT_KIND_PDF));
        assert_eq!(idx.get_kind(6), Some(SEGMENT_KIND_AUDIO));
        assert_eq!(idx.get_kind(7), Some(SEGMENT_KIND_VIDEO));
        assert_eq!(idx.get_kind(8), Some("enriched_ocr"));
        assert_eq!(idx.get_kind(9), Some("enriched_transcript"));

        // Filter to content kinds excludes `name`.
        let content_filter = VectorFilter::content_kinds();
        let hits = idx.search(&[1.0, 0.0], 10, Some(&content_filter)).unwrap();
        assert_eq!(hits.len(), CONTENT_SEGMENT_KINDS.len());
        assert!(hits.iter().all(|h| h.kind != SEGMENT_KIND_NAME));

        // Remove by kind removes only that modality.
        assert_eq!(idx.count_by_kind(SEGMENT_KIND_PDF), 1);
        assert_eq!(idx.remove_by_kind(SEGMENT_KIND_PDF), 1);
        assert_eq!(idx.count_by_kind(SEGMENT_KIND_PDF), 0);
        assert_eq!(idx.len(), 8);
    }

    #[test]
    fn large_finite_components_do_not_overflow_to_zero_norm() {
        let (normed, norm) = normalize_vector(&[1e20, 0.0]);
        assert!(norm > 0.0, "expected positive norm for 1e20, got {norm}");
        assert!((normed[0] - 1.0).abs() < 1e-5, "got {normed:?}");
        assert_eq!(normed[1], 0.0);
    }
}
