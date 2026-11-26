//! Shared overlapping text chunker (~1200 chars, ~15% overlap) and excerpt
//! selection for search result highlights.

/// Default target chunk length in Unicode scalar values (`char`s).
pub const DEFAULT_CHUNK_CHARS: usize = 1200;
/// Default overlap ratio between consecutive chunks (15% of 1200 = 180 chars).
pub const DEFAULT_OVERLAP_RATIO: f32 = 0.15;
/// Default overlap in characters (`180`).
pub const DEFAULT_OVERLAP_CHARS: usize = 180;
/// Default maximum excerpt length in characters.
pub const DEFAULT_EXCERPT_CHARS: usize = 240;

/// One chunk produced by [`chunk_text_with_lines`], carrying its 1-based
/// starting line number in the original text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunk {
    pub text: String,
    /// 1-based line number in the original input where this chunk starts.
    pub line: u32,
}

/// Splits `text` into overlapping chunks of approximately `target_chars`
/// characters (default ~1200) with `overlap_ratio` overlap (default ~0.15).
///
/// Boundaries snap to natural breaks (paragraph `\n\n`, newline `\n`, sentence
/// punctuation, or whitespace) near the end of each window so words are not
/// torn in half unless a single token exceeds the window.
pub fn chunk_text(text: &str, target_chars: usize, overlap_ratio: f32) -> Vec<String> {
    chunk_text_with_lines(text, target_chars, overlap_ratio)
        .into_iter()
        .map(|c| c.text)
        .collect()
}

/// Convenience wrapper using [`DEFAULT_CHUNK_CHARS`] (1200) and
/// [`DEFAULT_OVERLAP_RATIO`] (0.15).
pub fn chunk_text_default(text: &str) -> Vec<String> {
    chunk_text(text, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO)
}

/// Splits `text` into overlapping [`TextChunk`]s with 1-based starting line
/// numbers.
pub fn chunk_text_with_lines(
    text: &str,
    target_chars: usize,
    overlap_ratio: f32,
) -> Vec<TextChunk> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let target = target_chars.max(1);
    let ratio = overlap_ratio.clamp(0.0, 0.5);
    let overlap = ((target as f32 * ratio).round() as usize).min(target.saturating_sub(1));

    // Collect char-to-byte offsets on `text` so all slices are valid UTF-8 and
    // line numbers refer to the original document.
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let total_chars = chars.len();
    if total_chars == 0 {
        return Vec::new();
    }

    let slice_chars = |start_c: usize, end_c: usize| -> &str {
        let start_b = chars[start_c].0;
        let end_b = if end_c >= total_chars {
            text.len()
        } else {
            chars[end_c].0
        };
        &text[start_b..end_b]
    };

    // Advance `start` past leading whitespace.
    let mut start = 0usize;
    while start < total_chars && chars[start].1.is_whitespace() {
        start += 1;
    }

    let mut out = Vec::new();
    let mut line_at_pos = 1u32;
    let mut counted_up_to = 0usize;

    while start < total_chars {
        // Update 1-based line counter up to `start`.
        while counted_up_to < start {
            if chars[counted_up_to].1 == '\n' {
                line_at_pos = line_at_pos.saturating_add(1);
            }
            counted_up_to += 1;
        }

        if start + target >= total_chars {
            let chunk_str = slice_chars(start, total_chars).trim();
            if !chunk_str.is_empty() {
                out.push(TextChunk {
                    text: chunk_str.to_string(),
                    line: line_at_pos,
                });
            }
            break;
        }

        let hard_end = (start + target).min(total_chars);
        let min_end = start + (target * 2 / 3).max(1);
        let end = find_natural_break(&chars, min_end, hard_end);

        let chunk_str = slice_chars(start, end).trim();
        if !chunk_str.is_empty() {
            out.push(TextChunk {
                text: chunk_str.to_string(),
                line: line_at_pos,
            });
        }

        if end >= total_chars {
            break;
        }

        // Compute the next chunk's start with `overlap` characters before `end`,
        // snapped forward to a word boundary so the overlap doesn't start mid-word.
        let raw_next = end.saturating_sub(overlap);
        let mut next_start = if raw_next <= start {
            end
        } else {
            snap_overlap_start(&chars, raw_next, end)
        };
        while next_start < total_chars && chars[next_start].1.is_whitespace() {
            next_start += 1;
        }
        if next_start <= start {
            next_start = end;
        }
        start = next_start;
    }

    out
}

/// Finds a clean break point in `(min_end..=hard_end]` preferring paragraph
/// breaks, then line breaks, then sentence endings, then whitespace.
fn find_natural_break(chars: &[(usize, char)], min_end: usize, hard_end: usize) -> usize {
    // 1. Paragraph break (\n\n)
    for i in (min_end..hard_end).rev() {
        if chars[i].1 == '\n' && i > 0 && chars[i - 1].1 == '\n' {
            return i + 1;
        }
    }
    // 2. Single newline
    for i in (min_end..hard_end).rev() {
        if chars[i].1 == '\n' {
            return i + 1;
        }
    }
    // 3. Sentence punctuation followed by whitespace
    for i in (min_end..hard_end).rev() {
        if matches!(chars[i - 1].1, '.' | '!' | '?' | ';' | '。') && chars[i].1.is_whitespace() {
            return i;
        }
    }
    // 4. Any whitespace
    for i in (min_end..hard_end).rev() {
        if chars[i].1.is_whitespace() {
            return i;
        }
    }
    hard_end
}

/// Snaps the overlap start position to a clean word boundary before `end`.
fn snap_overlap_start(chars: &[(usize, char)], raw_start: usize, end: usize) -> usize {
    if raw_start == 0 || chars[raw_start - 1].1.is_whitespace() {
        return raw_start;
    }
    // Look forward up to halfway through the overlap for the next whitespace.
    let limit = (raw_start + (end - raw_start) / 2).min(end);
    for (i, &(_, ch)) in chars.iter().enumerate().take(limit).skip(raw_start) {
        if ch.is_whitespace() {
            return i + 1;
        }
    }
    raw_start
}

/// Selects a short excerpt (up to `max_chars` characters) from `text` centered
/// around the densest cluster of `query_terms` for search result highlights.
///
/// Whitespace runs inside the excerpt are collapsed to single spaces; if the
/// window does not start/end at the boundaries of `text`, `"…"` ellipses are
/// added.
pub fn select_excerpt<S: AsRef<str>>(text: &str, query_terms: &[S], max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let normalized = collapse_whitespace(text);
    if normalized.is_empty() {
        return String::new();
    }
    let chars: Vec<(usize, char)> = normalized.char_indices().collect();
    let total_chars = chars.len();
    if total_chars <= max_chars {
        return normalized;
    }

    let lower = normalized.to_lowercase();
    let terms: Vec<String> = query_terms
        .iter()
        .map(|s| s.as_ref().trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    // Find char positions of all term matches in `lower`.
    // Map byte offsets in `lower` to approximate char positions in `normalized`.
    let mut hits: Vec<(usize, usize)> = Vec::new(); // (char_pos, term_idx)
    if !terms.is_empty() {
        let lower_chars: Vec<(usize, char)> = lower.char_indices().collect();
        for (term_idx, term) in terms.iter().enumerate() {
            let mut search_from = 0usize;
            while let Some(rel) = lower[search_from..].find(term.as_str()) {
                let byte_pos = search_from + rel;
                let char_pos = lower_chars
                    .binary_search_by_key(&byte_pos, |&(b, _)| b)
                    .unwrap_or_else(|i| i)
                    .min(total_chars.saturating_sub(1));
                hits.push((char_pos, term_idx));
                search_from = byte_pos + term.len().max(1);
            }
        }
    }

    let window = max_chars.saturating_sub(2).max(1);
    let best_start = if hits.is_empty() {
        0usize
    } else {
        hits.sort_by_key(|&(pos, _)| pos);
        let mut best_score = (0usize, 0usize); // (distinct_terms, total_hits)
        let mut best_anchor = hits[0].0;
        for &(anchor, _) in &hits {
            let win_end = anchor + window;
            let mut distinct = 0u64;
            let mut count = 0usize;
            for &(pos, tidx) in &hits {
                if pos >= anchor && pos < win_end {
                    count += 1;
                    if tidx < 64 {
                        distinct |= 1u64 << tidx;
                    }
                }
            }
            let score = (distinct.count_ones() as usize, count);
            if score > best_score {
                best_score = score;
                best_anchor = anchor;
            }
        }
        // Leave a small leading context before the first matched term.
        let lead = (window / 5).min(32);
        best_anchor.saturating_sub(lead)
    };

    render_excerpt(&normalized, &chars, max_chars, best_start, None)
}

/// Selects an excerpt at an authoritative match offset in the original UTF-8
/// text. Whitespace collapsing retains that position without normalizing or
/// searching the query again.
pub fn select_excerpt_at(text: &str, match_byte: usize, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let normalized = collapse_whitespace(text);
    if normalized.is_empty() {
        return String::new();
    }
    let prefix = text.get(..match_byte).unwrap_or("");
    let anchor = collapse_whitespace(prefix).chars().count()
        + usize::from(prefix.chars().next_back().is_some_and(char::is_whitespace));
    let chars: Vec<(usize, char)> = normalized.char_indices().collect();
    let lead = (max_chars.saturating_sub(2).max(1) / 5).min(32);
    render_excerpt(
        &normalized,
        &chars,
        max_chars,
        anchor.saturating_sub(lead),
        Some(anchor),
    )
}

fn render_excerpt(
    normalized: &str,
    chars: &[(usize, char)],
    max_chars: usize,
    best_start: usize,
    anchor: Option<usize>,
) -> String {
    let total_chars = chars.len();
    let window = max_chars.saturating_sub(2).max(1);
    let mut start = best_start.min(total_chars.saturating_sub(window));
    // Snap `start` to a word boundary if not at index 0.
    if start > 0 && !chars[start - 1].1.is_whitespace() {
        for (i, &(_, ch)) in chars
            .iter()
            .enumerate()
            .take((start + 16).min(total_chars))
            .skip(start)
        {
            if ch.is_whitespace() {
                start = i + 1;
                break;
            }
        }
    }
    if let Some(anchor) = anchor {
        start = start.min(anchor);
    }

    let need_prefix = start > 0;
    let budget_after_prefix = max_chars.saturating_sub(usize::from(need_prefix));
    let mut end = (start + budget_after_prefix).min(total_chars);
    let need_suffix = end < total_chars;
    if need_suffix {
        end = (start + budget_after_prefix.saturating_sub(1)).min(total_chars);
        // Snap `end` back to whitespace so we don't chop the trailing word.
        if end < total_chars && !chars[end].1.is_whitespace() {
            for i in ((start + 1).max(end.saturating_sub(16))..end).rev() {
                if chars[i].1.is_whitespace() {
                    end = i;
                    break;
                }
            }
        }
    }

    let start_b = chars.get(start).map(|&(b, _)| b).unwrap_or(0);
    let end_b = if end >= total_chars {
        normalized.len()
    } else {
        chars[end].0
    };
    let slice = normalized[start_b..end_b].trim();

    let mut out = String::new();
    if need_prefix {
        out.push('…');
    }
    out.push_str(slice);
    if need_suffix {
        out.push('…');
    }
    out
}

fn collapse_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut prev_ws = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !prev_ws && !out.is_empty() {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(ch);
            prev_ws = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_whitespace_inputs_produce_no_chunks() {
        assert!(chunk_text("", 1200, 0.15).is_empty());
        assert!(chunk_text("   \n\t\n  ", 1200, 0.15).is_empty());
    }

    #[test]
    fn short_text_fits_in_single_chunk() {
        let chunks = chunk_text_with_lines("Hello world.\nSecond line.", 1200, 0.15);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Hello world.\nSecond line.");
        assert_eq!(chunks[0].line, 1);
    }

    #[test]
    fn long_text_splits_with_15_percent_overlap_and_tracks_lines() {
        // Build ~3200 chars across multiple lines.
        let mut doc = String::new();
        for i in 1..=80 {
            doc.push_str(&format!(
                "Line {i:02}: The quick brown fox jumps over the lazy dog.\n"
            ));
        }
        assert!(doc.chars().count() > 3000);

        let chunks = chunk_text_with_lines(&doc, DEFAULT_CHUNK_CHARS, DEFAULT_OVERLAP_RATIO);
        assert!(
            chunks.len() >= 3,
            "expected >=3 chunks for ~4300 chars, got {}",
            chunks.len()
        );
        for (idx, c) in chunks.iter().enumerate() {
            let len = c.text.chars().count();
            assert!(
                len <= DEFAULT_CHUNK_CHARS,
                "chunk {idx} exceeded target: {len} > {DEFAULT_CHUNK_CHARS}"
            );
            if idx == 0 {
                assert_eq!(c.line, 1);
            } else {
                assert!(
                    c.line > chunks[idx - 1].line,
                    "chunk {idx} line {} must advance past {}",
                    c.line,
                    chunks[idx - 1].line
                );
            }
        }

        // Consecutive chunks must share overlapping content (~15% of 1200 ≈ 180 chars).
        for pair in chunks.windows(2) {
            let prev = &pair[0].text;
            let next = &pair[1].text;
            // The first line of `next` should appear near the tail of `prev`.
            let first_line = next.lines().next().unwrap();
            assert!(
                prev.contains(first_line),
                "expected overlap: first line of next chunk {first_line:?} not in tail of previous chunk"
            );
        }
    }

    #[test]
    fn multibyte_utf8_never_splits_codepoints() {
        let unit = "日本語テスト — Ümlaut — 🚀火箭 ";
        let text = unit.repeat(200);
        let chunks = chunk_text(&text, 300, 0.15);
        assert!(chunks.len() > 5);
        for c in &chunks {
            assert!(c.chars().count() <= 300);
            assert!(c.contains("日本語") || c.contains("Ümlaut"));
        }
    }

    #[test]
    fn select_excerpt_centers_on_matching_query_terms() {
        let prefix = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(10);
        let target = "The termination clause requires thirty days written notice before renewal.";
        let suffix =
            " Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.".repeat(10);
        let full = format!("{prefix}{target}{suffix}");

        let excerpt = select_excerpt(&full, &["termination", "clause"], 140);
        assert!(
            excerpt.to_lowercase().contains("termination clause"),
            "excerpt missed query terms: {excerpt:?}"
        );
        assert!(
            excerpt.chars().count() <= 140,
            "excerpt exceeded max_chars: {} ({excerpt:?})",
            excerpt.chars().count()
        );
        assert!(
            excerpt.starts_with('…'),
            "expected leading ellipsis: {excerpt:?}"
        );
        assert!(
            excerpt.ends_with('…'),
            "expected trailing ellipsis: {excerpt:?}"
        );
    }

    #[test]
    fn select_excerpt_falls_back_to_leading_text_when_no_term_matches() {
        let text = "Alpha beta gamma delta. ".repeat(20);
        let excerpt = select_excerpt(&text, &["nonexistent"], 60);
        assert!(excerpt.starts_with("Alpha beta"));
        assert!(excerpt.ends_with('…'));
        assert!(excerpt.chars().count() <= 60);
    }

    #[test]
    fn anchored_excerpt_preserves_original_byte_position_through_whitespace_collapse() {
        let prefix = "界\t  \n".repeat(200);
        let text = format!(
            "{prefix}café invoice detail {}",
            "trailing context ".repeat(30)
        );
        let excerpt = select_excerpt_at(&text, prefix.len(), 80);
        assert!(excerpt.contains("café invoice detail"), "{excerpt:?}");
        assert!(excerpt.chars().count() <= 80);
        assert!(!excerpt.contains('\n'));
        assert!(!excerpt.contains('\t'));
        assert!(select_excerpt_at(&text, prefix.len(), 0).is_empty());
    }
}
