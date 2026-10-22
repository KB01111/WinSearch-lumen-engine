//! Retrieval: candidate generation (exact name, name prefix, trigram
//! substring, FTS token match) and launcher-style ranking.
//!
//! Every candidate source is index-driven — there is no table scan on the
//! query path, so latency stays flat as the index grows. Ranking is
//! deterministic and explainable: exact name beats prefix beats substring
//! beats token matches, with small boosts for applications and recency and a
//! small penalty for deep paths. Every hit carries the reason it matched so
//! the UI can show a match explanation.

use rusqlite::params;

use crate::model::{HitKind, SearchHit, SearchRequest};
use crate::store::{EntryRow, IndexStore, StoreError};

/// Baseline per-source budget; larger requests may raise it up to the wire maximum.
const CANDIDATE_CAP: usize = 500;

struct Candidate {
    row: EntryRow,
    /// True when the match came from the FTS token index (names or keywords)
    /// rather than a whole-string name match.
    via_fts: bool,
    /// True when the match came from extracted content (`content_fts`).
    via_content_fts: bool,
}

impl IndexStore {
    /// Query-pass reuse requires a native change fence and fresh delivery checks.
    /// An unavailable or broken fence must retain the ordinary live guard path.
    pub fn search_fenced(
        &self,
        req: &SearchRequest,
        fence: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Vec<SearchHit>, StoreError> {
        let _scope = self
            .query_cache
            .begin(fence)
            .map_err(|message| StoreError::InvalidConfig(message.into()))?;
        self.search(req)
    }
    pub(crate) fn set_search_request_filter(&self, req: &SearchRequest) -> Result<(), StoreError> {
        if !ws_protocol::valid_search_extensions(&req.extensions)
            || !ws_protocol::valid_search_file_kinds(&req.file_kinds)
        {
            return Err(StoreError::InvalidConfig(
                "invalid search extension filter".into(),
            ));
        }
        let (apps_only, files_only, kinds) = (req.apps_only, req.files_only, req.kinds.clone());
        let extensions = req.extensions.clone();
        let file_kinds = req.file_kinds.clone();
        // Connection-local request scope must not change admission/metadata in query_entries.
        self.conn().create_scalar_function(
            "search_request_allowed",
            4,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |ctx| {
                let path = ctx.get::<String>(0)?;
                let ext = ctx.get::<String>(1)?;
                let is_dir = ctx.get::<bool>(2)?;
                let is_app = ctx.get::<bool>(3)?;
                let kind = if is_app {
                    HitKind::Application
                } else if is_dir {
                    HitKind::Folder
                } else {
                    crate::model::kind_for_path_and_ext(&path, &ext).hit_kind()
                };
                Ok((!apps_only || is_app)
                    && (!files_only || !is_app)
                    && (kinds.is_empty() || kinds.contains(&kind))
                    && (extensions.is_empty() || !is_dir)
                    && ws_protocol::search_file_kind_allowed(&file_kinds, &ext, is_dir)
                    && ws_protocol::search_extension_allowed(&extensions, &ext))
            },
        )?;
        Ok(())
    }

    /// Runs one retrieval pass. Results are ranked, filtered by the request's
    /// mode/kinds, and truncated to `limit`.
    pub fn search(&self, req: &SearchRequest) -> Result<Vec<SearchHit>, StoreError> {
        let query = req.text.trim();
        let query_lower = query.to_lowercase();
        let tokens = tokenize(&query_lower);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        self.set_search_request_filter(req)?;

        let candidate_limit = req
            .limit
            .clamp(CANDIDATE_CAP, ws_protocol::MAX_SEARCH_RESULTS as usize);
        let mut candidates = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut push_unique = |rows: Vec<EntryRow>, via_fts: bool, via_content_fts: bool| {
            for row in rows {
                if seen.insert(row.id) {
                    candidates.push(Candidate {
                        row,
                        via_fts,
                        via_content_fts,
                    });
                }
            }
        };
        push_unique(self.exact_candidates(&query_lower)?, false, false);
        push_unique(
            self.prefix_candidates(&query_lower, candidate_limit)?,
            false,
            false,
        );
        push_unique(
            self.substring_candidates(&query_lower, candidate_limit)?,
            false,
            false,
        );
        push_unique(self.fts_candidates(&tokens, candidate_limit)?, true, false);
        push_unique(
            self.content_fts_candidates(&tokens, candidate_limit)?,
            false,
            true,
        );
        push_unique(
            self.path_candidates(&query_lower, candidate_limit)?,
            false,
            false,
        );

        let depth = |path: &str| path.matches('\\').count().max(path.matches('/').count());
        let mut hits: Vec<SearchHit> = candidates
            .into_iter()
            .filter(|c| {
                if c.row.missing {
                    return false;
                }
                if req.apps_only && !c.row.is_app {
                    return false;
                }
                if req.files_only && c.row.is_app {
                    return false;
                }
                if !req.kinds.is_empty() {
                    let kind = hit_kind(&c.row);
                    if !req.kinds.contains(&kind) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|c| {
                let (score, explanation, content_only) = rank(
                    &c.row,
                    &query_lower,
                    &tokens,
                    c.via_fts,
                    c.via_content_fts,
                    depth(&c.row.path),
                );
                (score > 0.0).then(|| SearchHit {
                    entry_id: c.row.id,
                    kind: hit_kind(&c.row),
                    name: c.row.name,
                    path: c.row.path,
                    is_dir: c.row.is_dir,
                    score,
                    explanation,
                    thumbnail_path: c.row.thumbnail_path,
                    matches: Vec::new(),
                    content_only,
                })
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.name.cmp(&b.name))
        });
        hits.truncate(req.limit.max(1));
        for hit in &mut hits {
            hit.matches = self.content_matches(hit.entry_id, &tokens)?;
        }
        Ok(hits)
    }

    /// All matching chunks for one visible file. Metadata is joined by exact FTS
    /// row identity, never by whichever segment happens to share the file.
    pub fn content_matches(
        &self,
        entry: i64,
        tokens: &[String],
    ) -> Result<Vec<crate::model::ContentMatch>, StoreError> {
        self.content_matches_page(entry, tokens, 0, 32)
    }

    /// Bounded preview pages retain access to every matching moment without
    /// materializing an entire long document on the primary result path.
    pub fn content_matches_page(
        &self,
        entry: i64,
        tokens: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<Vec<crate::model::ContentMatch>, StoreError> {
        let expression = tokens
            .iter()
            .map(|t| format!("\"{}\"*", t.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" ");
        if expression.is_empty() {
            return Ok(Vec::new());
        }
        let mut stmt=self.conn().prepare_cached("SELECT f.text,c.page,c.start_ms,c.end_ms,c.line,c.thumbnail_path,highlight(content_fts,2,char(1),char(2)),f.rowid FROM content_fts f LEFT JOIN content_chunks c ON c.id=f.rowid AND c.entry_id=CAST(f.entry_id AS INTEGER) WHERE content_fts MATCH ?1 AND CAST(f.entry_id AS INTEGER)=?2 AND EXISTS(SELECT 1 FROM query_entries e WHERE e.id=?2 AND e.missing=0) AND EXISTS(SELECT 1 FROM query_content_rows v WHERE v.id=f.rowid) ORDER BY f.rowid LIMIT ?3 OFFSET ?4")?;
        let rows = stmt.query_map(
            params![
                expression,
                entry,
                limit.min(32) as i64,
                offset.min(i64::MAX as usize) as i64
            ],
            |r| {
                let text: String = r.get(0)?;
                let mut marker = "\u{1}".to_string();
                while text.contains(&marker) {
                    marker.push_str(&"\u{2}".repeat(marker.len()));
                }
                // U+0001 occurs only at the marker's start: a literal source suffix
                // cannot complete a false marker across the inserted marker boundary.
                let highlighted: String = if marker.len() == 1 {
                    r.get(6)?
                } else {
                    self.conn().query_row(
                        "SELECT highlight(content_fts,2,?1,'') FROM content_fts WHERE content_fts MATCH ?2 AND rowid=?3",
                        params![marker, expression, r.get::<_, i64>(7)?],
                        |row| row.get(0),
                    )?
                };
                let match_byte = highlighted.find(&marker);
                let line: Option<u32> = r.get(4)?;
                let line = line.map(|n| {
                    n.saturating_add(
                        text[..match_byte.unwrap_or(0)]
                            .bytes()
                            .filter(|b| *b == b'\n')
                            .count() as u32,
                    )
                });
                Ok(crate::model::ContentMatch {
                    excerpt: text,
                    match_byte,
                    page: r.get(1)?,
                    start_ms: r.get::<_, Option<i64>>(2)?.map(|n| n as u64),
                    end_ms: r.get::<_, Option<i64>>(3)?.map(|n| n as u64),
                    line,
                    thumbnail_path: r.get(5)?,
                })
            },
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Whole-name equality: an index point lookup.
    fn exact_candidates(&self, query_lower: &str) -> Result<Vec<EntryRow>, StoreError> {
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE missing = 0 AND name_lower = ?1
             LIMIT 8",
            params![query_lower],
        )
    }

    /// Name prefix: a range seek on the name index. A `>= / <` pair (rather
    /// than `LIKE 'x%'`) keeps the query index-eligible regardless of LIKE
    /// optimization rules and wildcard characters in the query.
    fn prefix_candidates(
        &self,
        query_lower: &str,
        limit: usize,
    ) -> Result<Vec<EntryRow>, StoreError> {
        let Some(end) = prefix_upper_bound(query_lower) else {
            return Ok(Vec::new());
        };
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE missing = 0 AND name_lower >= ?1 AND name_lower < ?2
             LIMIT ?3",
            params![query_lower, end, limit as i64],
        )
    }

    /// Whole-query substring (e.g. "code" inside "VSCode"): served by the
    /// trigram FTS index. Trigrams need three characters; shorter queries rely
    /// on prefix/token sources (mid-word two-character noise is excluded on
    /// purpose).
    fn substring_candidates(
        &self,
        query_lower: &str,
        limit: usize,
    ) -> Result<Vec<EntryRow>, StoreError> {
        if query_lower.chars().count() < 3 {
            return Ok(Vec::new());
        }
        let escaped = query_lower.replace('"', "\"\"");
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE id IN (
                 SELECT rowid FROM entries_tri WHERE entries_tri MATCH ?1
                 AND EXISTS (SELECT 1 FROM search_entries v WHERE v.id = entries_tri.rowid) LIMIT ?2
             )",
            params![format!("\"{escaped}\""), limit as i64],
        )
    }

    /// All query tokens present as word prefixes (name or keywords column).
    /// No ORDER BY rank here: the rank sort is the expensive part and our own
    /// ranking decides the order anyway.
    fn fts_candidates(&self, tokens: &[String], limit: usize) -> Result<Vec<EntryRow>, StoreError> {
        let match_expr = tokens
            .iter()
            .map(|t| format!("\"{t}\"*"))
            .collect::<Vec<_>>()
            .join(" ");
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE id IN (
                 SELECT rowid FROM entries_fts WHERE entries_fts MATCH ?1
                 AND EXISTS (SELECT 1 FROM search_entries v WHERE v.id = entries_fts.rowid) LIMIT ?2
             )",
            params![match_expr, limit as i64],
        )
    }

    /// All query tokens present as word prefixes in extracted content (`content_fts`).
    fn content_fts_candidates(
        &self,
        tokens: &[String],
        limit: usize,
    ) -> Result<Vec<EntryRow>, StoreError> {
        let match_expr = tokens
            .iter()
            .map(|t| format!("\"{t}\"*"))
            .collect::<Vec<_>>()
            .join(" ");
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE id IN (
                 SELECT DISTINCT CAST(entry_id AS INTEGER) FROM content_fts
                 WHERE content_fts MATCH ?1
                 AND EXISTS (SELECT 1 FROM query_content_rows v WHERE v.id = content_fts.rowid)
                 AND EXISTS (SELECT 1 FROM search_entries e WHERE e.id = CAST(content_fts.entry_id AS INTEGER)) LIMIT ?2
             )",
            params![match_expr, limit as i64],
        )
    }

    /// Substring match against `entries.path` (for queries matching a parent
    /// directory substring or multi-segment path fragment of length >= 3).
    /// Backed by the `entries_path_tri` trigram FTS index over lowercased paths.
    fn path_candidates(
        &self,
        query_lower: &str,
        limit: usize,
    ) -> Result<Vec<EntryRow>, StoreError> {
        if query_lower.chars().count() < 3 {
            return Ok(Vec::new());
        }
        let escaped = format!("\"{}\"", query_lower.replace('"', "\"\""));
        self.rows_by(
            "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                    size, mtime, missing, thumbnail_path
             FROM search_entries
             WHERE missing = 0 AND id IN (
                 SELECT rowid FROM entries_path_tri WHERE entries_path_tri MATCH ?1
                 AND EXISTS (SELECT 1 FROM search_entries v WHERE v.id = entries_path_tri.rowid) LIMIT ?2
             )",
            params![escaped, limit as i64],
        )
    }

    fn rows_by(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<EntryRow>, StoreError> {
        let mut stmt = self.conn().prepare_cached(sql)?;
        let rows = stmt.query_map(params, row_to_entry)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

/// Deterministic score in (0, 1] plus a human-readable match reason.
fn rank(
    row: &EntryRow,
    query: &str,
    tokens: &[String],
    via_fts: bool,
    via_content_fts: bool,
    depth: usize,
) -> (f64, String, bool) {
    let name = &row.name_lower;
    let (base, explanation, content_only) = if name == query {
        (1.0, "Exact name match", false)
    } else if name.starts_with(query) {
        (0.92, "Name starts with query", false)
    } else if name.contains(query) {
        (0.78, "Name contains query", false)
    } else if tokens.len() > 1 && all_tokens_match_name(name, tokens) {
        (0.74, "All words match the name", false)
    } else if via_fts {
        (0.5, "Matches name or keywords", false)
    } else if via_content_fts {
        (0.42, "Matches extracted content", true)
    } else if row.path.to_lowercase().contains(query) {
        (0.35, "Path matches query", false)
    } else {
        return (0.0, String::new(), false);
    };

    let mut score = base;
    if row.is_app {
        score += 0.06;
    }
    // Very mild recency boost so equally named results prefer newer files.
    if !row.is_dir && !row.is_app && row.mtime > 0 {
        let age_days = (now_unix() - row.mtime).max(0) / 86_400;
        score += match age_days {
            0..=7 => 0.03,
            8..=30 => 0.02,
            31..=90 => 0.01,
            _ => 0.0,
        };
    }
    score -= 0.005 * depth.min(12) as f64;
    (score.clamp(0.0, 1.0), explanation.to_string(), content_only)
}

fn all_tokens_match_name(name: &str, tokens: &[String]) -> bool {
    let name_tokens: Vec<&str> = name
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    tokens.iter().all(|q| {
        name_tokens
            .iter()
            .any(|n| n.starts_with(q.as_str()) || n.contains(q.as_str()))
    })
}

fn hit_kind(row: &EntryRow) -> HitKind {
    if row.is_app {
        HitKind::Application
    } else if row.is_dir {
        HitKind::Folder
    } else {
        crate::model::kind_for_path_and_ext(&row.path, &row.ext).hit_kind()
    }
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<EntryRow> {
    Ok(EntryRow {
        id: row.get(0)?,
        root_id: row.get(1)?,
        parent_id: row.get(2)?,
        path: row.get(3)?,
        name: row.get(4)?,
        name_lower: row.get(5)?,
        ext: row.get(6)?,
        is_dir: row.get::<_, i64>(7)? != 0,
        is_app: row.get::<_, i64>(8)? != 0,
        size: row.get::<_, i64>(9)?.max(0) as u64,
        mtime: row.get(10)?,
        missing: row.get::<_, i64>(11)? != 0,
        thumbnail_path: row.get::<_, Option<String>>(12).unwrap_or(None),
    })
}

/// Splits query text into lowercase alphanumeric tokens.
pub(crate) fn tokenize(query: &str) -> Vec<String> {
    query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// Exclusive upper bound of the string range covering everything with
/// `prefix` as its start: the prefix with its last codepoint incremented.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    let last = chars.pop()?;
    let bumped = char::from_u32(last as u32 + 1)?;
    chars.push(bumped);
    Some(chars.into_iter().collect())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EntryRecord;
    use crate::store::IndexStore;

    fn rec(path: &str, name: &str, is_app: bool) -> EntryRecord {
        EntryRecord {
            root_id: 0,
            parent_id: None,
            path: path.into(),
            name: name.into(),
            ext: crate::model::extension_of(name),
            is_dir: false,
            size: 1,
            mtime: now_unix(),
            volume_serial: 1,
            file_id: 0,
            is_app,
            app_target: String::new(),
            keywords: if is_app {
                "visual studio code editor".into()
            } else {
                String::new()
            },
        }
    }

    fn seeded_store() -> IndexStore {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let gen = store.begin_generation(root).unwrap();
        let records = vec![
            rec("C:\\data\\notes.txt", "notes.txt", false),
            rec("C:\\data\\meeting notes.txt", "meeting notes.txt", false),
            rec("C:\\data\\VSCode.exe", "VSCode.exe", false),
            rec("C:\\data\\Code.lnk", "Code.lnk", true),
            rec("C:\\data\\codex.md", "codex.md", false),
        ];
        store.apply_batch(root, gen, &records).unwrap();
        store
    }

    #[test]
    fn exact_match_ranks_first() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "notes.txt".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits[0].name, "notes.txt");
        assert_eq!(hits[0].explanation, "Exact name match");
    }

    #[test]
    fn query_pass_reuses_guards_only_while_fenced_and_never_across_operations() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        };
        let store = seeded_store();
        let calls = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let observed = calls.clone();
        let admitted = Arc::new(AtomicBool::new(true));
        let admission = admitted.clone();
        store
            .set_query_source_guard(move |path, source, app| {
                *observed
                    .lock()
                    .unwrap()
                    .entry((path.to_owned(), source.map(str::to_owned), app))
                    .or_insert(0) += 1;
                admission.load(Ordering::SeqCst)
            })
            .unwrap();
        let request = SearchRequest {
            text: "notes".into(),
            limit: 20,
            ..Default::default()
        };
        let hits = store.search_fenced(&request, Box::new(|| true)).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(
            calls.lock().unwrap().values().all(|count| *count == 1),
            "duplicate live filesystem work in one unchanged query pass: {:?}",
            calls.lock().unwrap()
        );
        let entry = hits[0].entry_id;
        assert!(store.entry_visible(entry).unwrap());
        let path = hits[0].path.clone();
        assert_eq!(
            calls.lock().unwrap()[&(path, None, false)],
            2,
            "delivery/source operations must be fresh"
        );
        calls.lock().unwrap().clear();
        store.search_fenced(&request, Box::new(|| false)).unwrap();
        assert!(
            calls.lock().unwrap().values().any(|count| *count > 1),
            "broken fences cannot reuse prior validation"
        );
        admitted.store(false, Ordering::SeqCst);
        assert!(
            store
                .search_fenced(&request, Box::new(|| true))
                .unwrap()
                .is_empty(),
            "a new operation must recheck admission"
        );
    }

    #[test]
    fn substring_matches_inside_words() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "code".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        let names: Vec<&str> = hits.iter().map(|h| h.name.as_str()).collect();
        assert!(names.contains(&"VSCode.exe"), "got {names:?}");
    }

    #[test]
    fn apps_can_be_filtered_and_rank_higher() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "code".into(),
                limit: 10,
                apps_only: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].kind, HitKind::Application);

        let all = store
            .search(&SearchRequest {
                text: "code".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(all[0].kind, HitKind::Application); // app boost
    }

    #[test]
    fn multi_word_queries_match_all_tokens() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "meeting notes".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits[0].name, "meeting notes.txt");
    }

    #[test]
    fn kind_filter_restricts_results() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "code".into(),
                limit: 10,
                kinds: vec![HitKind::Application],
                ..Default::default()
            })
            .unwrap();
        assert!(hits.iter().all(|h| h.kind == HitKind::Application));
    }

    fn assert_filter_precedes_every_candidate_cap(mut request: SearchRequest, wanted_app: bool) {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let generation = store.begin_generation(root).unwrap();
        let mut records = (0..CANDIDATE_CAP + 9)
            .map(|index| {
                rec(
                    &format!("C:\\data\\noise\\{index}\\needle.pdf"),
                    "needle.pdf",
                    !wanted_app,
                )
            })
            .collect::<Vec<_>>();
        let wanted = "C:\\data\\selected\\needle.pdf";
        records.push(rec(wanted, "needle.pdf", wanted_app));
        store.apply_batch(root, generation, &records).unwrap();
        // Offline query fixtures admit virtual content; production source admission stays separate.
        for record in &records {
            let entry = store.find_by_path(&record.path).unwrap().unwrap();
            store
                .conn()
                .execute(
                    "INSERT INTO content_fts(entry_id,kind,text) VALUES(?1,'doc','contentneedle')",
                    [entry.id],
                )
                .unwrap();
        }
        request.text = "needle".into();
        request.limit = 1;
        let hits = store.search(&request).unwrap();
        assert_eq!(
            hits.len(),
            1,
            "excluded candidates cannot consume the scoped candidate budget"
        );
        assert_eq!(hits[0].path, wanted);
        for (source, rows) in [
            ("exact", store.exact_candidates("needle.pdf").unwrap()),
            (
                "prefix",
                store.prefix_candidates("needle", CANDIDATE_CAP).unwrap(),
            ),
            (
                "substring",
                store.substring_candidates("needle", CANDIDATE_CAP).unwrap(),
            ),
            (
                "tokens",
                store
                    .fts_candidates(&["needle".into()], CANDIDATE_CAP)
                    .unwrap(),
            ),
            (
                "content",
                store
                    .content_fts_candidates(&["contentneedle".into()], CANDIDATE_CAP)
                    .unwrap(),
            ),
            (
                "path",
                store.path_candidates("needle", CANDIDATE_CAP).unwrap(),
            ),
        ] {
            assert_eq!(rows.len(), 1, "{source} must filter before its own cap");
            assert_eq!(rows[0].path, wanted, "{source}");
        }
        let rejected = store.find_by_path(&records[0].path).unwrap().unwrap();
        assert!(
            store.entry_visible(rejected.id).unwrap(),
            "request scope cannot revoke general metadata admission"
        );
        let all = store
            .search(&SearchRequest {
                text: "needle".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(all.len(), 1);
        assert_ne!(
            all[0].path, wanted,
            "a new request must replace the previous scope"
        );
        store
            .set_query_source_guard(|path, _, _| !path.contains("selected"))
            .unwrap();
        assert!(
            store.search(&request).unwrap().is_empty(),
            "request filters cannot bypass source admission"
        );
    }

    #[test]
    fn files_mode_precedes_every_candidate_cap() {
        assert_filter_precedes_every_candidate_cap(
            SearchRequest {
                files_only: true,
                ..Default::default()
            },
            false,
        );
    }

    #[test]
    fn apps_mode_precedes_every_candidate_cap() {
        assert_filter_precedes_every_candidate_cap(
            SearchRequest {
                apps_only: true,
                ..Default::default()
            },
            true,
        );
    }

    #[test]
    fn category_filter_precedes_every_candidate_cap() {
        assert_filter_precedes_every_candidate_cap(
            SearchRequest {
                kinds: vec![HitKind::Document],
                ..Default::default()
            },
            false,
        );
    }

    #[test]
    fn extension_filter_precedes_candidate_caps_and_preserves_case_and_dot_compatibility() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let generation = store.begin_generation(root).unwrap();
        let mut records = (0..CANDIDATE_CAP + 9)
            .map(|index| {
                rec(
                    &format!("C:\\data\\needle-{index:04}.png"),
                    &format!("needle-{index:04}.png"),
                    false,
                )
            })
            .collect::<Vec<_>>();
        let wanted = "C:\\data\\needlez.pdf";
        records.push(rec(wanted, "needlez.pdf", false));
        store.apply_batch(root, generation, &records).unwrap();
        for extensions in [
            vec!["pdf".into()],
            vec![".PDF".into()],
            vec!["txt".into(), "PDF".into()],
        ] {
            let hits = store
                .search(&SearchRequest {
                    text: "needle".into(),
                    extensions,
                    limit: 1,
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].path, wanted);
        }
        assert!(store
            .search(&SearchRequest {
                text: "needle".into(),
                extensions: vec!["txt".into()],
                limit: 1,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn empty_and_punctuation_queries_return_nothing() {
        let store = seeded_store();
        for q in ["", "   ", "...", "---"] {
            let hits = store
                .search(&SearchRequest {
                    text: q.into(),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap();
            assert!(hits.is_empty(), "query {q:?} returned hits");
        }
    }

    #[test]
    fn limit_is_respected() {
        let store = seeded_store();
        let hits = store
            .search(&SearchRequest {
                text: "notes".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn wildcard_like_queries_do_not_match_everything() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let gen = store.begin_generation(root).unwrap();
        store
            .apply_batch(
                root,
                gen,
                &[rec("C:\\data\\100%_real.txt", "100%_real.txt", false)],
            )
            .unwrap();
        let hits = store
            .search(&SearchRequest {
                text: "100%".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        // A query full of LIKE wildcards must not match everything.
        let hits = store
            .search(&SearchRequest {
                text: "%_%".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn parent_directory_substring_matches_via_path_candidates() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let gen = store.begin_generation(root).unwrap();
        store
            .apply_batch(
                root,
                gen,
                &[rec(
                    "C:\\data\\AlphabetSoupProject\\readme.txt",
                    "readme.txt",
                    false,
                )],
            )
            .unwrap();
        let hits = store
            .search(&SearchRequest {
                text: "soupproject".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "readme.txt");
        assert_eq!(hits[0].explanation, "Path matches query");
    }

    #[test]
    fn unicode_parent_directory_substring_matches_case_insensitively() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        let gen = store.begin_generation(root).unwrap();
        store
            .apply_batch(
                root,
                gen,
                &[rec(
                    "C:\\data\\BioÄpfelErnte\\readme.txt",
                    "readme.txt",
                    false,
                )],
            )
            .unwrap();
        let hits = store
            .search(&SearchRequest {
                text: "äpfelernt".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "readme.txt");
        assert_eq!(hits[0].explanation, "Path matches query");
    }
}
