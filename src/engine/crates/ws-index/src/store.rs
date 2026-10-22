//! SQLite storage for the index: schema, entry upserts with filesystem
//! identity, the crash-safe crawl queue, and checkpoints.
//!
//! Durability model: WAL journal with `synchronous=NORMAL`. All writes for one
//! logical step (a directory's entries, a queue pop) commit in one
//! transaction, so an abrupt crash at any point leaves the database
//! consistent and the crawl resumable.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

use crate::model::{
    EngineConfig, EntryRecord, ExtractJob, NewSegment, PreferredBackend, ScoredSegment, Segment,
    UpsertOutcome, VectorFilter,
};
use crate::vectors::{dot_fp16_row_vs_normalized, encode_normalized_vector, normalize_vector};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("unsupported schema version: {0}")]
    UnsupportedSchemaVersion(String),
    #[error("invalid vector: {0}")]
    InvalidVector(String),
    #[error("invalid engine config: {0}")]
    InvalidConfig(String),
    #[error("invalid durable index identity")]
    InvalidIdentity,
}

/// A configured indexing root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    pub id: i64,
    pub path: String,
    pub enabled: bool,
    pub current_generation: i64,
}

/// Aggregate index counters (also mirrored into engine status).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub entries_total: u64,
    pub files_total: u64,
    pub dirs_total: u64,
    pub apps_total: u64,
    pub index_size_bytes: u64,
}

/// One stored entry as seen by search and identity lookups.
#[derive(Debug, Clone)]
pub struct EntryRow {
    pub id: i64,
    pub root_id: i64,
    pub parent_id: Option<i64>,
    pub path: String,
    pub name: String,
    pub name_lower: String,
    pub ext: String,
    pub is_dir: bool,
    pub is_app: bool,
    pub size: u64,
    pub mtime: i64,
    pub missing: bool,
    pub thumbnail_path: Option<String>,
}

pub struct IndexStore {
    conn: Connection,
    database_id: String,
}

#[derive(Clone)]
pub struct SegmentSource {
    pub path: String,
    pub is_app: bool,
    /// None is a filename vector; empty is content without a durable proof.
    pub source: Option<String>,
}

#[derive(Default)]
pub struct ContentBatch<'a> {
    pub chunks: &'a [crate::model::ContentChunk],
    pub next: Option<u64>,
    pub media: Option<&'a crate::model::MediaPlan>,
    pub selection: Option<&'a crate::model::VideoSelection>,
    pub source_sha256: Option<&'a str>,
}

pub type RawSegmentVector = (i64, i64, String, Vec<u8>, f32);
type ExistingEntry = (i64, i64, i64, String, bool, String, String, i64, i64);

impl IndexStore {
    /// Offline fixture stores allow virtual paths; every production Indexer installs
    /// its metadata-only native/source policy before exposing retrieval.
    pub fn set_query_source_guard(
        &self,
        guard: impl Fn(&str, Option<&str>, bool) -> bool + Send + Sync + 'static,
    ) -> Result<(), StoreError> {
        self.conn.create_scalar_function(
            "query_source_allowed",
            3,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |ctx| {
                Ok(guard(
                    &ctx.get::<String>(0)?,
                    ctx.get::<Option<String>>(1)?.as_deref(),
                    ctx.get::<bool>(2)?,
                ))
            },
        )?;
        Ok(())
    }

    /// Durable source proofs are captured under the store lock, before scoring
    /// under the vector-cache lock. Missing/legacy content proofs are empty.
    pub fn segment_sources(
        &self,
    ) -> Result<std::collections::HashMap<i64, SegmentSource>, StoreError> {
        let mut stmt=self.conn.prepare("SELECT s.id,e.path,e.is_app,CASE WHEN s.kind='name' THEN NULL ELSE COALESCE(p.source,'') END FROM segments s JOIN entries e ON e.id=s.entry_id LEFT JOIN content_chunks c ON c.entry_id=s.entry_id AND c.kind=s.kind AND c.chunk_index=s.chunk_index LEFT JOIN content_revisions r ON r.entry_id=s.entry_id LEFT JOIN content_progress p ON p.entry_id=c.entry_id AND p.kind=c.job_kind AND p.revision=r.revision")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get(0)?,
                SegmentSource {
                    path: r.get(1)?,
                    is_app: r.get(2)?,
                    source: r.get(3)?,
                },
            ))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn segment_source_allowed(&self, segment: &Segment) -> Result<bool, StoreError> {
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM segments s JOIN query_entries e ON e.id=s.entry_id LEFT JOIN content_chunks c ON c.entry_id=s.entry_id AND c.kind=s.kind AND c.chunk_index=s.chunk_index LEFT JOIN content_revisions r ON r.entry_id=s.entry_id LEFT JOIN content_progress p ON p.entry_id=c.entry_id AND p.kind=c.job_kind AND p.revision=r.revision WHERE s.id=?1 AND query_source_allowed(e.path,CASE WHEN s.kind='name' THEN NULL ELSE COALESCE(p.source,'') END,e.is_app))",[segment.id],|r|r.get(0))?)
    }

    pub fn entry_content_allowed(&self, entry: i64) -> Result<bool, StoreError> {
        Ok(self.conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM query_entries e JOIN content_progress p ON p.entry_id=e.id JOIN content_revisions r ON r.entry_id=e.id AND r.revision=p.revision WHERE e.id=?1 AND query_source_allowed(e.path,p.source,e.is_app))")?.query_row([entry],|r|r.get(0))?)
    }

    pub fn content_revision(&self, entry: i64) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT revision FROM content_revisions WHERE entry_id=?1",
                [entry],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn content_sha256(&self, entry: i64) -> Result<Option<String>, StoreError> {
        let mut statement=self.conn.prepare_cached("SELECT DISTINCT p.source_sha256 FROM content_progress p JOIN content_revisions r ON r.entry_id=p.entry_id AND r.revision=p.revision JOIN query_entries e ON e.id=p.entry_id WHERE p.entry_id=?1 AND p.source_sha256_revision=p.revision AND p.source_sha256 IS NOT NULL AND query_source_allowed(e.path,p.source,e.is_app) LIMIT 2")?;
        let hashes = statement
            .query_map([entry], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match hashes.as_slice() {
            [hash] if ws_protocol::valid_content_sha256(hash) => Some(hash.clone()),
            _ => None,
        })
    }

    /// Watch events invalidate even when coarse entry metadata is unchanged.
    pub fn invalidate_content_source(&mut self, entry: i64) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute("INSERT INTO content_revisions(entry_id,revision) VALUES(?1,1) ON CONFLICT(entry_id) DO UPDATE SET revision=revision+1", [entry])?;
        tx.execute("DELETE FROM content_progress WHERE entry_id=?1", [entry])?;
        tx.execute("DELETE FROM content_chunks WHERE entry_id=?1", [entry])?;
        tx.execute("DELETE FROM content_fts WHERE entry_id=?1", [entry])?;
        tx.execute(
            "DELETE FROM segments WHERE entry_id=?1 AND kind!='name'",
            [entry],
        )?;
        tx.execute(
            "UPDATE entries SET thumbnail_path=NULL WHERE id=?1",
            [entry],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn model_generation(&self) -> Result<i64, StoreError> {
        Ok(self
            .meta_get("content_model_generation")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    /// Model changes discard only vectors; valid exact content and parse cursors survive.
    pub fn reset_content_vectors(&mut self) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute("INSERT INTO meta(key,value) VALUES('content_model_generation','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1", [])?;
        tx.execute(
            "UPDATE content_chunks SET embed_state='pending',attempts=0 WHERE embed=1",
            [],
        )?;
        tx.execute("UPDATE content_progress SET model_generation=(SELECT CAST(value AS INTEGER) FROM meta WHERE key='content_model_generation')", [])?;
        tx.commit()?;
        Ok(())
    }

    pub fn content_progress(
        &self,
        entry: i64,
        kind: &str,
    ) -> Result<Option<crate::model::ContentProgress>, StoreError> {
        Ok(self.conn.query_row("SELECT cursor,complete,revision,source,model_generation,media_plan,video_selection,CASE WHEN source_sha256_revision=revision THEN source_sha256 ELSE NULL END FROM content_progress WHERE entry_id=?1 AND kind=?2", params![entry,kind], |r| Ok(crate::model::ContentProgress {
            cursor:r.get::<_,i64>(0)? as u64, complete:r.get(1)?, revision:r.get(2)?, source:r.get(3)?, model_generation:r.get(4)?,
            source_sha256:r.get(7)?,
            media_plan:r.get::<_,Option<String>>(5)?.map(|s| serde_json::from_str(&s))
                .transpose().map_err(|e| rusqlite::Error::FromSqlConversionFailure(5,rusqlite::types::Type::Text,Box::new(e)))?,
            video_selection:r.get::<_,Option<String>>(6)?.map(|s| serde_json::from_str(&s))
                .transpose().map_err(|e| rusqlite::Error::FromSqlConversionFailure(6,rusqlite::types::Type::Text,Box::new(e)))?,
        })).optional()?)
    }

    pub fn begin_content_batch(
        &mut self,
        entry: i64,
        kind: &str,
        source: &str,
    ) -> Result<crate::model::ContentProgress, StoreError> {
        let revision = self.content_revision(entry)?;
        let generation = self.model_generation()?;
        if let Some(progress) = self.content_progress(entry, kind)? {
            if !progress.complete
                && progress.revision == revision
                && progress.source == source
                && (kind != "video"
                    || progress.cursor == 0
                    || progress.media_plan.is_some()
                    || progress.video_selection.is_some())
            {
                return Ok(progress);
            }
        }
        self.conn.execute("INSERT INTO content_revisions(entry_id,revision) VALUES(?1,1) ON CONFLICT(entry_id) DO UPDATE SET revision=revision+1",[entry])?;
        let revision = self.content_revision(entry)?;
        self.conn.execute("INSERT INTO content_progress(entry_id,kind,revision,source,model_generation) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(entry_id,kind) DO UPDATE SET cursor=0,complete=0,revision=excluded.revision,source=excluded.source,model_generation=excluded.model_generation,media_plan=NULL,video_selection=NULL,source_sha256=NULL,source_sha256_revision=NULL",params![entry,kind,revision,source,generation])?;
        Ok(self
            .content_progress(entry, kind)?
            .expect("inserted progress"))
    }

    /// Exact text, locations, next cursor, and release of the claim are one transaction.
    /// A stale revision/space cannot advance or overwrite a newer source.
    pub fn commit_content_batch_with_media(
        &mut self,
        job: &ExtractJob,
        progress: &crate::model::ContentProgress,
        chunks: &[crate::model::ContentChunk],
        next: Option<u64>,
        media: Option<&crate::model::MediaPlan>,
    ) -> Result<bool, StoreError> {
        self.commit_content_batch_with_video_selection(job, progress, chunks, next, media, None)
    }

    pub fn commit_content_batch_with_video_selection(
        &mut self,
        job: &ExtractJob,
        progress: &crate::model::ContentProgress,
        chunks: &[crate::model::ContentChunk],
        next: Option<u64>,
        media: Option<&crate::model::MediaPlan>,
        selection: Option<&crate::model::VideoSelection>,
    ) -> Result<bool, StoreError> {
        self.commit_content_batch_with_source(
            job,
            progress,
            ContentBatch {
                chunks,
                next,
                media,
                selection,
                source_sha256: None,
            },
        )
    }

    pub fn commit_content_batch_with_source(
        &mut self,
        job: &ExtractJob,
        progress: &crate::model::ContentProgress,
        batch: ContentBatch<'_>,
    ) -> Result<bool, StoreError> {
        let ContentBatch {
            chunks,
            next,
            media,
            selection,
            source_sha256,
        } = batch;
        if source_sha256.is_some_and(|hash| !ws_protocol::valid_content_sha256(hash)) {
            return Err(StoreError::InvalidConfig(
                "invalid complete-source SHA256".into(),
            ));
        }
        if progress
            .source_sha256
            .as_deref()
            .is_some_and(|hash| Some(hash) != source_sha256)
        {
            return Ok(false);
        }
        // A resumed legacy parse has no byte attestation for its earlier chunks or media plan.
        let source_sha256 = source_sha256.filter(|_| {
            progress.source_sha256.is_some()
                || (progress.cursor == 0
                    && progress.media_plan.is_none()
                    && progress.video_selection.is_none())
        });
        if self.content_revision(job.entry_id)? != progress.revision
            || self.model_generation()? != progress.model_generation
        {
            return Ok(false);
        }
        if self.content_progress(job.entry_id, &job.kind)?.as_ref() != Some(progress) {
            return Ok(false);
        }
        if let Some(plan) = media {
            plan.validate()
                .map_err(|s| StoreError::InvalidConfig(s.into()))?;
            if !matches!(job.kind.as_str(), "audio" | "video")
                || progress
                    .media_plan
                    .as_ref()
                    .is_some_and(|saved| saved != plan)
            {
                return Err(StoreError::InvalidConfig(
                    "media plan changed within source revision".into(),
                ));
            }
        }
        if let Some(state) = selection {
            state
                .validate_successor(progress.video_selection.as_ref())
                .map_err(|s| StoreError::InvalidConfig(s.into()))?;
            if job.kind != "video"
                || !chunks.is_empty()
                || progress.media_plan.is_some()
                || (progress.video_selection.is_none() && progress.cursor != 0)
                || progress
                    .video_selection
                    .as_ref()
                    .is_some_and(|p| progress.cursor != ((1u64 << 61) | p.next_ms))
                || (state.next_ms < state.duration_ms
                    && (media.is_some() || next != Some((1u64 << 61) | state.next_ms)))
                || (state.next_ms == state.duration_ms && (media.is_none() || next != Some(0)))
            {
                return Err(StoreError::InvalidConfig(
                    "invalid video selection commit".into(),
                ));
            }
        } else if progress.video_selection.is_some() {
            return Err(StoreError::InvalidConfig(
                "selection phase lost its checkpoint".into(),
            ));
        }
        let media_json = media
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StoreError::InvalidConfig(e.to_string()))?;
        let selection_json = selection
            .filter(|s| s.next_ms < s.duration_ms)
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StoreError::InvalidConfig(e.to_string()))?;
        let phase_finish = selection.is_some_and(|s| s.next_ms == s.duration_ms) && next == Some(0);
        if next.is_some_and(|n| (!phase_finish && n <= progress.cursor) || n > i64::MAX as u64)
            || (chunks.is_empty()
                && next.is_some()
                && !matches!(job.kind.as_str(), "audio" | "video"))
        {
            return Err(StoreError::InvalidConfig(
                "cursor advanced without content".into(),
            ));
        }
        let tx = self.conn.transaction()?;
        if progress.cursor == 0 {
            tx.execute("DELETE FROM segments WHERE entry_id=?1 AND kind!='name' AND EXISTS(SELECT 1 FROM content_chunks c WHERE c.entry_id=?1 AND c.job_kind=?2 AND c.kind=segments.kind AND c.chunk_index=segments.chunk_index)",params![job.entry_id,job.kind])?;
            tx.execute(
                "DELETE FROM content_chunks WHERE entry_id=?1 AND job_kind=?2",
                params![job.entry_id, job.kind],
            )?;
            tx.execute(
                "DELETE FROM content_fts WHERE entry_id=?1 AND kind=?2",
                params![job.entry_id, job.kind],
            )?;
            tx.execute("DELETE FROM segments WHERE entry_id=?1 AND (kind=?2 OR (?2 IN ('image','pdf') AND kind='ocr') OR (?2='pdf' AND kind='pdf_visual'))",params![job.entry_id,job.kind])?;
            tx.execute(
                "UPDATE entries SET thumbnail_path=NULL WHERE id=?1",
                [job.entry_id],
            )?;
        }
        for chunk in chunks {
            let input = serde_json::to_string(&chunk.embedding_input)
                .map_err(|e| StoreError::InvalidConfig(e.to_string()))?;
            tx.execute("INSERT INTO content_chunks(entry_id,job_kind,kind,chunk_index,text,page,start_ms,end_ms,line,thumbnail_path,embed,visual,embedding_input) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![job.entry_id,job.kind,chunk.kind,chunk.chunk_index,chunk.text,chunk.location.page,chunk.location.start_ms.map(|n| n as i64),chunk.location.end_ms.map(|n| n as i64),chunk.location.line,chunk.location.thumbnail_path,chunk.embed,chunk.visual,input])?;
            let id = tx.last_insert_rowid();
            if !chunk.visual && !chunk.text.trim().is_empty() {
                tx.execute(
                    "INSERT INTO content_fts(rowid,entry_id,kind,text) VALUES(?1,?2,?3,?4)",
                    params![id, job.entry_id, job.kind, chunk.text],
                )?;
            }
            if let Some(thumb) = &chunk.location.thumbnail_path {
                tx.execute(
                    "UPDATE entries SET thumbnail_path=COALESCE(thumbnail_path,?2) WHERE id=?1",
                    params![job.entry_id, thumb],
                )?;
            }
        }
        tx.execute(
            "UPDATE content_progress SET cursor=?3,complete=?4,media_plan=COALESCE(?5,media_plan),video_selection=?6,source_sha256=?7,source_sha256_revision=CASE WHEN ?7 IS NULL THEN NULL ELSE ?8 END WHERE entry_id=?1 AND kind=?2",
            params![
                job.entry_id,
                job.kind,
                next.unwrap_or(progress.cursor + chunks.iter().filter(|c|!c.visual).count() as u64) as i64,
                next.is_none(),
                media_json, selection_json, source_sha256, progress.revision
            ],
        )?;
        if next.is_some() {
            tx.execute("UPDATE extract_jobs SET claimed=0,state='pending',priority=-1000000000000,attempts=0 WHERE id=?1",[job.id])?;
        } else {
            tx.execute("DELETE FROM extract_jobs WHERE id=?1", [job.id])?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn list_content_chunks(
        &self,
        entry: i64,
    ) -> Result<Vec<crate::model::ContentChunk>, StoreError> {
        self.content_chunks_by("WHERE entry_id=?1 ORDER BY chunk_index", entry)
    }

    /// Reads only a bounded byte prefix and source locations from admitted rows.
    pub fn preview_text(
        &self,
        entry: i64,
        location: Option<ws_protocol::MatchLocation>,
    ) -> Result<(Vec<ws_protocol::PreviewTextPart>, bool), StoreError> {
        use ws_protocol::{
            MatchLocation, PreviewTextPart, MAX_PREVIEW_TEXT_BYTES, MAX_PREVIEW_TEXT_PARTS,
        };
        let (mut page, mut line, mut start, mut end) = (None, None, None, None);
        match location {
            Some(MatchLocation::Page { page: value }) if (1..=5000).contains(&value) => {
                page = Some(value)
            }
            Some(MatchLocation::Line { line: value }) if value > 0 => line = Some(value),
            Some(MatchLocation::Timestamp {
                seconds,
                end_seconds,
            }) if seconds.is_finite()
                && end_seconds.is_finite()
                && seconds >= 0.0
                && end_seconds >= seconds
                && end_seconds <= 366.0 * 86400.0 =>
            {
                start = Some(seconds);
                end = Some(end_seconds);
            }
            None => (),
            _ => {
                return Err(StoreError::InvalidConfig(
                    "unsupported preview location".into(),
                ))
            }
        }
        let mut statement=self.conn.prepare("SELECT c.kind,substr(CAST(c.text AS BLOB),1,?2),length(CAST(c.text AS BLOB)),c.page,c.start_ms,c.end_ms,c.line FROM content_chunks c JOIN query_content_rows admitted ON admitted.id=c.id WHERE c.entry_id=?1 AND c.text!='' AND (?3 IS NULL OR c.page=?3) AND (?4 IS NULL OR c.line<=?4) AND (?5 IS NULL OR (c.start_ms/1000.0=?5 AND COALESCE(c.end_ms,c.start_ms)/1000.0=?6)) ORDER BY CASE WHEN ?4 IS NOT NULL THEN -c.line ELSE 0 END,c.chunk_index,c.id LIMIT ?7")?;
        let mut rows = statement.query(params![
            entry,
            (MAX_PREVIEW_TEXT_BYTES + 1) as i64,
            page,
            line,
            start,
            end,
            (MAX_PREVIEW_TEXT_PARTS + 1) as i64
        ])?;
        let (mut parts, mut used, mut seen, mut truncated) = (Vec::new(), 0, 0, false);
        while let Some(row) = rows.next()? {
            seen += 1;
            if seen > MAX_PREVIEW_TEXT_PARTS {
                truncated = true;
                break;
            }
            let bytes: Vec<u8> = row.get(1)?;
            let original_size = usize::try_from(row.get::<_, i64>(2)?)
                .map_err(|_| StoreError::InvalidConfig("invalid cached preview size".into()))?;
            let available = MAX_PREVIEW_TEXT_BYTES - used;
            let prefix = &bytes[..bytes.len().min(available)];
            let text = match std::str::from_utf8(prefix) {
                Ok(text) => text,
                Err(error) if error.error_len().is_none() => {
                    std::str::from_utf8(&prefix[..error.valid_up_to()])
                        .expect("validated UTF8 prefix")
                }
                Err(_) => {
                    return Err(StoreError::InvalidConfig(
                        "cached preview text is not UTF8".into(),
                    ))
                }
            };
            let part_line: Option<u32> = row.get(6)?;
            if line.is_some_and(|focus| {
                part_line.is_none_or(|first| {
                    focus < first || u64::from(focus - first) >= text.lines().count().max(1) as u64
                })
            }) {
                truncated |= original_size > text.len();
                continue;
            }
            let part_page: Option<u32> = row.get(3)?;
            let part_start: Option<i64> = row.get(4)?;
            let part_end: Option<i64> = row.get(5)?;
            let location = if let Some(page) = part_page {
                Some(MatchLocation::Page { page })
            } else if let Some(start) = part_start {
                let end = part_end.unwrap_or(start);
                if start < 0 || end < start || end > 366 * 86400 * 1000 {
                    return Err(StoreError::InvalidConfig(
                        "invalid cached preview time".into(),
                    ));
                }
                Some(MatchLocation::Timestamp {
                    seconds: start as f64 / 1000.0,
                    end_seconds: end as f64 / 1000.0,
                })
            } else {
                part_line.map(|line| MatchLocation::Line { line })
            };
            if !text.is_empty() {
                used += text.len();
                parts.push(PreviewTextPart {
                    kind: row.get(0)?,
                    text: text.to_owned(),
                    location,
                });
            }
            if original_size > text.len() {
                truncated = true;
                break;
            }
        }
        Ok((parts, truncated))
    }

    pub fn preview_extraction(
        &self,
        entry: i64,
    ) -> Result<Vec<ws_protocol::PreviewExtraction>, StoreError> {
        let mut statement=self.conn.prepare("SELECT substr(p.kind,1,33),p.complete FROM content_progress p JOIN content_revisions r ON r.entry_id=p.entry_id AND r.revision=p.revision JOIN query_entries e ON e.id=p.entry_id WHERE p.entry_id=?1 AND query_source_allowed(e.path,p.source,e.is_app) ORDER BY p.kind LIMIT ?2")?;
        let rows = statement.query_map(
            params![entry, (ws_protocol::MAX_PREVIEW_EXTRACTIONS + 1) as i64],
            |row| {
                Ok(ws_protocol::PreviewExtraction {
                    kind: row.get(0)?,
                    complete_within_limit: row.get(1)?,
                    source_units: None,
                    indexed_units: None,
                    capped: None,
                })
            },
        )?;
        let metadata = rows.collect::<Result<Vec<_>, _>>()?;
        if metadata.len() > ws_protocol::MAX_PREVIEW_EXTRACTIONS
            || metadata
                .iter()
                .any(|m| !crate::CONTENT_SEGMENT_KINDS.contains(&m.kind.as_str()))
        {
            return Err(StoreError::InvalidConfig(
                "invalid preview extraction metadata".into(),
            ));
        }
        Ok(metadata)
    }

    /// Cached pixels must belong to the exact chunk metadata that was scored.
    pub fn segment_thumbnail(
        &self,
        segment: &crate::model::Segment,
    ) -> Result<Option<String>, StoreError> {
        if !self.segment_source_allowed(segment)? {
            return Ok(None);
        }
        Ok(self.conn.query_row("SELECT thumbnail_path FROM content_chunks WHERE entry_id=?1 AND kind=?2 AND chunk_index=?3 AND text=?4 AND page IS ?5 AND start_ms IS ?6 AND end_ms IS ?7 AND line IS ?8 LIMIT 1",
            params![segment.entry_id,segment.kind,segment.chunk_index,segment.text,segment.page,segment.start_ms,segment.end_ms,segment.line],
            |r|r.get::<_,Option<String>>(0)).optional()?.flatten())
    }

    pub fn commit_content_batch(
        &mut self,
        job: &ExtractJob,
        progress: &crate::model::ContentProgress,
        chunks: &[crate::model::ContentChunk],
        next: Option<u64>,
    ) -> Result<bool, StoreError> {
        self.commit_content_batch_with_media(job, progress, chunks, next, None)
    }

    fn content_chunks_by(
        &self,
        suffix: &str,
        entry: i64,
    ) -> Result<Vec<crate::model::ContentChunk>, StoreError> {
        let mut stmt = self.conn.prepare(&format!("SELECT id,entry_id,job_kind,kind,chunk_index,text,page,start_ms,end_ms,line,thumbnail_path,embed,attempts,visual,embedding_input FROM content_chunks {suffix}"))?;
        let rows = stmt.query_map([entry], |r| {
            Ok(crate::model::ContentChunk {
                id: r.get(0)?,
                entry_id: r.get(1)?,
                job_kind: r.get(2)?,
                kind: r.get(3)?,
                chunk_index: r.get(4)?,
                text: r.get(5)?,
                location: crate::model::ContentMatch {
                    excerpt: String::new(),
                    match_byte: None,
                    page: r.get(6)?,
                    start_ms: r.get::<_, Option<i64>>(7)?.map(|n| n as u64),
                    end_ms: r.get::<_, Option<i64>>(8)?.map(|n| n as u64),
                    line: r.get(9)?,
                    thumbnail_path: r.get(10)?,
                },
                embed: r.get(11)?,
                attempts: r.get(12)?,
                visual: r.get(13)?,
                embedding_input: serde_json::from_str(&r.get::<_, String>(14)?).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        14,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Atomically claims one source's pending vectors; no parser invocation is needed.
    pub fn claim_content_vectors(
        &mut self,
        preferred: Option<i64>,
    ) -> Result<Vec<crate::model::ContentChunk>, StoreError> {
        let entry: Option<i64> = self.conn.query_row("SELECT c.entry_id FROM content_chunks c JOIN visible_entries e ON e.id=c.entry_id WHERE (?1 IS NULL OR c.entry_id=?1) AND c.embed=1 AND c.embed_state='pending' AND c.attempts<3 AND e.missing=0 ORDER BY c.attempts,c.id LIMIT 1",[preferred],|r|r.get(0)).optional()?;
        let Some(entry) = entry else {
            return Ok(Vec::new());
        };
        let mut chunks = self.content_chunks_by("WHERE entry_id=?1 AND embed=1 AND embed_state='pending' AND attempts<3 AND visual=(SELECT visual FROM content_chunks WHERE entry_id=?1 AND embed=1 AND embed_state='pending' AND attempts<3 ORDER BY visual,id LIMIT 1) ORDER BY id LIMIT 30",entry)?;
        if let Some(first) = chunks.first() {
            let class = std::mem::discriminant(&first.embedding_input);
            chunks.retain(|c| std::mem::discriminant(&c.embedding_input) == class);
        }
        let tx = self.conn.transaction()?;
        for chunk in &chunks {
            tx.execute(
                "UPDATE content_chunks SET embed_state='claimed' WHERE id=?1",
                [chunk.id],
            )?;
        }
        tx.commit()?;
        Ok(chunks)
    }

    /// Replace a regenerated input without changing exact rows or vector state.
    pub fn replace_content_artifact(
        &mut self,
        chunk: &crate::model::ContentChunk,
        input: &crate::model::DraftEmbeddingInput,
        revision: i64,
        generation: i64,
    ) -> Result<bool, StoreError> {
        if self.content_revision(chunk.entry_id)? != revision
            || self.model_generation()? != generation
        {
            return Ok(false);
        }
        if std::mem::discriminant(input) != std::mem::discriminant(&chunk.embedding_input)
            || matches!(input, crate::model::DraftEmbeddingInput::Text)
        {
            return Err(StoreError::InvalidConfig(
                "regenerated artifact type changed".into(),
            ));
        }
        let encode = |input| {
            serde_json::to_string(input).map_err(|e| StoreError::InvalidConfig(e.to_string()))
        };
        let tx = self.conn.transaction()?;
        if tx.execute("UPDATE content_chunks SET embedding_input=?3 WHERE id=?1 AND entry_id=?2 AND embed_state IN ('claimed','done') AND embedding_input=?4",
            params![chunk.id, chunk.entry_id, encode(input)?, encode(&chunk.embedding_input)?])? != 1 { return Ok(false); }
        if let (
            crate::model::DraftEmbeddingInput::Image { path: old, .. },
            crate::model::DraftEmbeddingInput::Image { path: new, .. },
        ) = (&chunk.embedding_input, input)
        {
            let (old, new) = (old.to_string_lossy(), new.to_string_lossy());
            // A continued frame is rendered in another worker and can have a
            // different PNG path for its OCR. Source time identifies its siblings.
            tx.execute("UPDATE content_chunks SET thumbnail_path=?3 WHERE entry_id=?1 AND job_kind=?2 AND (thumbnail_path=?4 OR (?5 AND start_ms=?6 AND end_ms=?7))",
                params![chunk.entry_id,chunk.job_kind,new,old,
                    chunk.job_kind=="video" && chunk.kind=="video",
                    chunk.location.start_ms.and_then(|t|i64::try_from(t).ok()),
                    chunk.location.end_ms.and_then(|t|i64::try_from(t).ok())])?;
            tx.execute(
                "UPDATE entries SET thumbnail_path=?2 WHERE id=?1 AND thumbnail_path=?3",
                params![chunk.entry_id, new, old],
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn release_content_vectors(
        &mut self,
        chunks: &[crate::model::ContentChunk],
        failed: bool,
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        for c in chunks {
            tx.execute("UPDATE content_chunks SET embed_state='pending',attempts=attempts+?2 WHERE id=?1 AND embed_state='claimed'",params![c.id,failed])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn complete_optional_visual(
        &mut self,
        chunks: &[crate::model::ContentChunk],
    ) -> Result<(), StoreError> {
        for c in chunks {
            self.conn.execute(
                "UPDATE content_chunks SET embed_state='done' WHERE id=?1 AND visual=1",
                [c.id],
            )?;
        }
        Ok(())
    }

    pub fn commit_content_vectors(
        &mut self,
        chunks: &[crate::model::ContentChunk],
        vectors: &[Vec<f32>],
        revision: i64,
        generation: i64,
    ) -> Result<bool, StoreError> {
        if chunks.is_empty() {
            return Ok(true);
        }
        if vectors.len() != chunks.len() {
            return Err(StoreError::InvalidVector("content vector arity".into()));
        }
        if self.content_revision(chunks[0].entry_id)? != revision
            || self.model_generation()? != generation
        {
            return Ok(false);
        }
        let encoded = vectors
            .iter()
            .map(|v| {
                if v.is_empty() || v.iter().any(|n| !n.is_finite()) || v.iter().all(|n| *n == 0.0) {
                    return Err(StoreError::InvalidVector(
                        "empty, zero or nonfinite content vector".into(),
                    ));
                }
                Ok(encode_normalized_vector(v))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let tx = self.conn.transaction()?;
        for (c, (blob, norm)) in chunks.iter().zip(encoded) {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM content_chunks WHERE id=?1 AND embed_state='claimed')",
                [c.id],
                |r| r.get(0),
            )?;
            if !exists {
                return Ok(false);
            }
            tx.execute("INSERT INTO segments(entry_id,kind,chunk_index,text,page,start_ms,end_ms,line,vector,norm) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(entry_id,kind,chunk_index) DO UPDATE SET text=excluded.text,page=excluded.page,start_ms=excluded.start_ms,end_ms=excluded.end_ms,line=excluded.line,vector=excluded.vector,norm=excluded.norm",params![c.entry_id,c.kind,c.chunk_index,c.text,c.location.page,c.location.start_ms.map(|n|n as i64),c.location.end_ms.map(|n|n as i64),c.location.line,blob,norm])?;
            tx.execute(
                "UPDATE content_chunks SET embed_state='done',attempts=0 WHERE id=?1",
                [c.id],
            )?;
        }
        tx.commit()?;
        Ok(true)
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roots (
    id                 INTEGER PRIMARY KEY,
    path               TEXT NOT NULL UNIQUE,
    enabled            INTEGER NOT NULL DEFAULT 1,
    current_generation INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS entries (
    id              INTEGER PRIMARY KEY,
    root_id         INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
    parent_id       INTEGER,
    path            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    name_lower      TEXT NOT NULL,
    ext             TEXT NOT NULL DEFAULT '',
    is_dir          INTEGER NOT NULL DEFAULT 0,
    size            INTEGER NOT NULL DEFAULT 0,
    mtime           INTEGER NOT NULL DEFAULT 0,
    volume_serial   INTEGER NOT NULL DEFAULT 0,
    file_id         INTEGER NOT NULL DEFAULT 0,
    is_app          INTEGER NOT NULL DEFAULT 0,
    app_target      TEXT NOT NULL DEFAULT '',
    missing         INTEGER NOT NULL DEFAULT 0,
    scan_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_id);
CREATE INDEX IF NOT EXISTS idx_entries_file   ON entries(volume_serial, file_id);
CREATE INDEX IF NOT EXISTS idx_entries_root   ON entries(root_id, scan_generation);
CREATE INDEX IF NOT EXISTS idx_entries_name   ON entries(name_lower);

CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
    name,
    keywords,
    tokenize = 'unicode61 remove_diacritics 2'
);

-- Trigram index for whole-string substring matching ("code" -> "VSCode").
-- Also accelerates LIKE on name_lower for callers that need it.
CREATE VIRTUAL TABLE IF NOT EXISTS entries_tri USING fts5(
    name,
    tokenize = 'trigram case_sensitive 0'
);

-- Trigram index over lowercased full paths for fast Unicode-aware path substring matching.
CREATE VIRTUAL TABLE IF NOT EXISTS entries_path_tri USING fts5(
    path,
    tokenize = 'trigram case_sensitive 0'
);

CREATE TABLE IF NOT EXISTS crawl_queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id    INTEGER NOT NULL,
    dir_path   TEXT NOT NULL,
    claimed    INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 0,
    UNIQUE(root_id, dir_path)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

/// Schema v2 (additive): vector segments + extract-job queue (reusable for
/// Phase 4). Applied with `IF NOT EXISTS` so opening a v1 file migrates it in
/// place; `meta.schema_version` is then bumped 1 → 2.
const SCHEMA_V2: &str = r#"
CREATE TABLE IF NOT EXISTS segments (
    id          INTEGER PRIMARY KEY,
    entry_id    INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    text        TEXT NOT NULL DEFAULT '',
    page        INTEGER NULL,
    start_ms    INTEGER NULL,
    end_ms      INTEGER NULL,
    line        INTEGER NULL,
    vector      BLOB NOT NULL,
    norm        REAL NOT NULL DEFAULT 0,
    UNIQUE(entry_id, kind, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_segments_entry ON segments(entry_id);

CREATE TABLE IF NOT EXISTS extract_jobs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    kind     TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',
    claimed  INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    UNIQUE(entry_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_extract_jobs_entry ON extract_jobs(entry_id);
CREATE INDEX IF NOT EXISTS idx_extract_jobs_state ON extract_jobs(state, claimed);

CREATE VIRTUAL TABLE IF NOT EXISTS content_fts USING fts5(
    entry_id UNINDEXED,
    kind UNINDEXED,
    text,
    tokenize = 'unicode61 remove_diacritics 2'
);
"#;

/// Engine config store (P3.6, additive): a plain key/value table holding the
/// `EngineConfig` fields (`semantic_enabled`, `preferred_backend`). Wired into
/// `init` with `IF NOT EXISTS`, so fresh and v1/v2 files all gain it without
/// a migration and `meta.schema_version` stays "2" (same pattern as the
/// `priority`-column exception: a policy hint, not a format break).
const SCHEMA_CONFIG: &str = r#"
CREATE VIEW IF NOT EXISTS visible_entries AS
WITH policy AS (SELECT value AS config FROM meta WHERE key='indexing_config'),
normalized AS (SELECT *, policy_fold(path) AS policy_path FROM entries)
SELECT e.id,e.root_id,e.parent_id,e.path,e.name,e.name_lower,e.ext,e.is_dir,e.is_app,e.size,e.mtime,e.missing,e.thumbnail_path
FROM normalized e
WHERE e.is_app=1 OR NOT EXISTS(SELECT 1 FROM policy) OR (
 EXISTS(SELECT 1 FROM roots WHERE roots.id=e.root_id AND enabled=1)
 AND EXISTS(SELECT 1 FROM policy,json_each(config,'$.roots') r
 WHERE (e.policy_path=policy_fold(r.value)
 OR substr(e.policy_path,1,length(policy_fold(r.value))+1)=policy_fold(r.value)||'\')
 AND NOT EXISTS(SELECT 1 FROM json_each(config,'$.excluded_paths') x
 WHERE length(policy_fold(x.value))>=length(policy_fold(r.value))
 AND (e.policy_path=policy_fold(x.value)
 OR substr(e.policy_path,1,length(policy_fold(x.value))+1)=policy_fold(x.value)||'\')))
 AND NOT EXISTS(SELECT 1 FROM policy,json_each(config,'$.excluded_extensions') x WHERE lower(e.ext)=x.value)
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

/// `config` keys backing [`EngineConfig`].
pub const CONFIG_KEY_SEMANTIC_ENABLED: &str = "semantic_enabled";
pub const CONFIG_KEY_PREFERRED_BACKEND: &str = "preferred_backend";

impl IndexStore {
    pub fn entry_visible(&self, id: i64) -> Result<bool, StoreError> {
        Ok(self
            .conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM query_entries WHERE id=?1)")?
            .query_row([id], |r| r.get(0))?)
    }
    pub fn root_enabled(&self, id: i64) -> Result<bool, StoreError> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM roots WHERE id=?1 AND enabled=1)",
            [id],
            |r| r.get(0),
        )?)
    }
    /// Loads policy once, migrating only roots already registered by legacy engines.
    pub fn get_or_migrate_indexing_config(
        &mut self,
    ) -> Result<ws_protocol::IndexingConfig, StoreError> {
        if let Some(json) = self.meta_get("indexing_config")? {
            return serde_json::from_str(&json)
                .map_err(|e| StoreError::InvalidConfig(e.to_string()));
        }
        let config = ws_protocol::IndexingConfig {
            roots: self
                .roots()?
                .into_iter()
                .filter(|r| r.enabled && r.path != "@apps")
                .map(|r| r.path)
                .collect(),
            ..Default::default()
        };
        self.set_indexing_config(&config)?;
        Ok(config)
    }

    /// Policy and root visibility share a single SQLite transaction.
    pub fn set_indexing_config(
        &mut self,
        config: &ws_protocol::IndexingConfig,
    ) -> Result<(), StoreError> {
        let json =
            serde_json::to_string(config).map_err(|e| StoreError::InvalidConfig(e.to_string()))?;
        let tx = self.conn.transaction()?;
        tx.execute("UPDATE roots SET enabled = 0 WHERE path != '@apps'", [])?;
        for path in &config.roots {
            tx.execute("INSERT INTO roots(path, enabled) VALUES(?1, 1) ON CONFLICT(path) DO UPDATE SET enabled = 1", [path])?;
        }
        tx.execute("INSERT INTO meta(key,value) VALUES('indexing_config',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [json])?;
        tx.commit()?;
        Ok(())
    }

    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, StoreError> {
        use rusqlite::functions::FunctionFlags;
        conn.create_scalar_function(
            "policy_fold",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |ctx| {
                Ok(ctx
                    .get::<String>(0)?
                    .replace('/', "\\")
                    .trim_end_matches('\\')
                    .to_lowercase())
            },
        )?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Names are stored and queried lowercased; making LIKE case-sensitive
        // lets the prefix query use the name index instead of scanning.
        conn.pragma_update(None, "case_sensitive_like", "ON")?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(SCHEMA_V2)?;
        conn.execute_batch(SCHEMA_CONFIG)?;
        // Additive migration for pre-priority v2 files: v2 DBs created before
        // the `priority` column gain it here (fresh DBs already have it from
        // SCHEMA_V2 above) BEFORE creating the priority index. `schema_version`
        // stays "2": the column is a queue-ordering hint, not a format break.
        Self::ensure_extract_jobs_priority_column(&conn)?;
        Self::ensure_crawl_queue_generation_column(&conn)?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS content_revisions (
 entry_id INTEGER PRIMARY KEY REFERENCES entries(id) ON DELETE CASCADE,
 revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS content_progress (
 entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
 kind TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0,
 revision INTEGER NOT NULL, source TEXT NOT NULL, model_generation INTEGER NOT NULL,
 PRIMARY KEY(entry_id,kind)
);
CREATE TABLE IF NOT EXISTS content_chunks (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
 job_kind TEXT NOT NULL, kind TEXT NOT NULL, chunk_index INTEGER NOT NULL,
 text TEXT NOT NULL, page INTEGER, start_ms INTEGER, end_ms INTEGER, line INTEGER,
 thumbnail_path TEXT, embed INTEGER NOT NULL DEFAULT 0, visual INTEGER NOT NULL DEFAULT 0,
 embed_state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 UNIQUE(entry_id,job_kind,chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_content_pending ON content_chunks(embed,embed_state,id);
CREATE INDEX IF NOT EXISTS idx_content_segment_location ON content_chunks(entry_id,kind,chunk_index);
CREATE TRIGGER IF NOT EXISTS content_chunk_delete AFTER DELETE ON content_chunks BEGIN
 DELETE FROM content_fts WHERE rowid=old.id;
END;
UPDATE content_chunks SET embed_state='pending' WHERE embed_state='claimed';
"#,
        )?;
        let progress_columns = conn
            .prepare("PRAGMA table_info(content_progress)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !progress_columns.iter().any(|c| c == "media_plan") {
            conn.execute(
                "ALTER TABLE content_progress ADD COLUMN media_plan TEXT",
                [],
            )?;
        }
        if !progress_columns.iter().any(|c| c == "video_selection") {
            conn.execute(
                "ALTER TABLE content_progress ADD COLUMN video_selection TEXT",
                [],
            )?;
        }
        if !progress_columns.iter().any(|c| c == "source_sha256") {
            conn.execute(
                "ALTER TABLE content_progress ADD COLUMN source_sha256 TEXT",
                [],
            )?;
        }
        if !progress_columns
            .iter()
            .any(|c| c == "source_sha256_revision")
        {
            conn.execute(
                "ALTER TABLE content_progress ADD COLUMN source_sha256_revision INTEGER",
                [],
            )?;
        }
        let chunk_columns = conn
            .prepare("PRAGMA table_info(content_chunks)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !chunk_columns.iter().any(|c| c == "embedding_input") {
            conn.execute("ALTER TABLE content_chunks ADD COLUMN embedding_input TEXT NOT NULL DEFAULT '\"Text\"'", [])?;
        }
        // Preserve legacy FTS rows, including numeric exact-only text without segments.
        if conn.query_row(
            "SELECT COUNT(*) FROM meta WHERE key='content_batches_v1'",
            [],
            |r| r.get::<_, i64>(0),
        )? == 0
        {
            conn.execute_batch(r#"
BEGIN IMMEDIATE;
INSERT INTO content_chunks(entry_id,job_kind,kind,chunk_index,text,page,start_ms,end_ms,line)
 SELECT CAST(f.entry_id AS INTEGER),f.kind,f.kind,f.rowid,f.text,
 (SELECT page FROM segments s WHERE s.entry_id=f.entry_id AND s.text=f.text LIMIT 1),
 (SELECT start_ms FROM segments s WHERE s.entry_id=f.entry_id AND s.text=f.text LIMIT 1),
 (SELECT end_ms FROM segments s WHERE s.entry_id=f.entry_id AND s.text=f.text LIMIT 1),
 (SELECT line FROM segments s WHERE s.entry_id=f.entry_id AND s.text=f.text LIMIT 1)
 FROM content_fts f JOIN entries e ON e.id=CAST(f.entry_id AS INTEGER);
DELETE FROM content_fts;
INSERT INTO content_fts(rowid,entry_id,kind,text) SELECT id,entry_id,job_kind,text FROM content_chunks;
INSERT INTO meta(key,value) VALUES('content_batches_v1','1');
COMMIT;
"#)?;
        }
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_extract_jobs_priority ON extract_jobs(state, claimed, priority, id);",
        )?;
        {
            let entry_cols: Vec<String> = conn
                .prepare("PRAGMA table_info(entries)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            if !entry_cols.iter().any(|c| c == "thumbnail_path") {
                conn.execute(
                    "ALTER TABLE entries ADD COLUMN thumbnail_path TEXT NULL",
                    [],
                )?;
            }
        }
        Self::ensure_entries_path_tri_backfilled(&conn)?;
        // Additive migration 1 → 2: fresh DBs land directly on 2, v1 files
        // keep every existing row and just gain the new tables.
        let version: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match version.as_deref() {
            None => {
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', '2')",
                    [],
                )?;
            }
            Some("1") => {
                conn.execute(
                    "UPDATE meta SET value = '2' WHERE key = 'schema_version'",
                    [],
                )?;
            }
            Some("2") => {}
            Some(other) => {
                return Err(StoreError::UnsupportedSchemaVersion(other.to_string()));
            }
        }
        conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('database_id',lower(hex(randomblob(16))))", [])?;
        let database_id: String = conn.query_row(
            "SELECT value FROM meta WHERE key='database_id'",
            [],
            |row| row.get(0),
        )?;
        if database_id.len() != 32
            || !database_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StoreError::InvalidIdentity);
        }
        let maximum: i64 =
            conn.query_row("SELECT COALESCE(MAX(id),0) FROM entries", [], |row| {
                row.get(0)
            })?;
        let watermark: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key='entry_id_high_water'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let watermark = watermark
            .as_deref()
            .unwrap_or("0")
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .ok_or(StoreError::InvalidIdentity)?;
        conn.execute("INSERT INTO meta(key,value) VALUES('entry_id_high_water',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(meta.value AS INTEGER)<CAST(excluded.value AS INTEGER)", [watermark.max(maximum).to_string()])?;
        conn.execute_batch("CREATE TRIGGER IF NOT EXISTS entries_id_high_water AFTER INSERT ON entries BEGIN UPDATE meta SET value=CAST(NEW.id AS TEXT) WHERE key='entry_id_high_water' AND CAST(value AS INTEGER)<NEW.id; END;")?;
        let store = Self { conn, database_id };
        store.set_query_source_guard(|_, _, _| true)?;
        store.conn.execute_batch("CREATE TEMP VIEW IF NOT EXISTS query_entries AS SELECT * FROM visible_entries WHERE query_source_allowed(path,NULL,is_app); CREATE TEMP VIEW IF NOT EXISTS query_content_rows AS SELECT f.rowid AS id FROM content_fts f JOIN query_entries e ON e.id=CAST(f.entry_id AS INTEGER) LEFT JOIN content_chunks c ON c.id=f.rowid AND c.entry_id=e.id LEFT JOIN content_revisions r ON r.entry_id=e.id LEFT JOIN content_progress p ON p.entry_id=c.entry_id AND p.kind=c.job_kind AND p.revision=r.revision WHERE query_source_allowed(e.path,COALESCE(p.source,''),e.is_app);")?;
        store.set_search_request_filter(&crate::model::SearchRequest::default())?;
        store.conn.execute_batch("CREATE TEMP VIEW IF NOT EXISTS search_entries AS SELECT * FROM query_entries WHERE missing=0 AND search_request_allowed(path,ext,is_dir,is_app);")?;
        Ok(store)
    }

    /// Current `meta.schema_version` (`"2"` on fresh and migrated DBs).
    pub fn schema_version(&self) -> Result<String, StoreError> {
        let v: String = self.conn.query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )?;
        Ok(v)
    }

    /// Non-secret database incarnation, stable across process restarts.
    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    pub fn begin_index_reset(&mut self, intent: &str) -> Result<(), StoreError> {
        self.conn.pragma_update(None, "synchronous", "FULL")?;
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES('index_reset_intent',?1)",
            [intent],
        )?;
        Ok(())
    }

    pub fn finish_index_reset(&mut self) -> Result<(), StoreError> {
        self.conn.pragma_update(None, "synchronous", "FULL")?;
        self.conn
            .execute("DELETE FROM meta WHERE key='index_reset_intent'", [])?;
        self.conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(())
    }

    /// Idempotent replay of a durable two-namespace reset. Preserve policy,
    /// schema/model metadata and ID watermarks; no old numeric ID is reused.
    pub fn reset_to(&mut self, database_id: &str) -> Result<(), StoreError> {
        if !ws_protocol::valid_index_id(database_id) {
            return Err(StoreError::InvalidIdentity);
        }
        if self.database_id == database_id {
            return Ok(());
        }
        self.conn.pragma_update(None, "synchronous", "FULL")?;
        let tx = self.conn.transaction()?;
        tx.execute_batch("DELETE FROM content_fts; DELETE FROM entries_fts; DELETE FROM entries_tri; DELETE FROM entries_path_tri; DELETE FROM entries; DELETE FROM crawl_queue; DELETE FROM checkpoints; UPDATE roots SET current_generation=current_generation+1;")?;
        tx.execute(
            "UPDATE meta SET value=?1 WHERE key='database_id'",
            [database_id],
        )?;
        tx.commit()?;
        self.database_id = database_id.to_owned();
        Ok(())
    }

    fn ensure_extract_jobs_priority_column(conn: &Connection) -> Result<(), StoreError> {
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(extract_jobs)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<Result<_, _>>()?;
        if !cols.iter().any(|c| c == "priority") {
            conn.execute(
                "ALTER TABLE extract_jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        Ok(())
    }

    fn ensure_crawl_queue_generation_column(conn: &Connection) -> Result<(), StoreError> {
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(crawl_queue)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<Result<_, _>>()?;
        if !cols.iter().any(|c| c == "generation") {
            conn.execute(
                "ALTER TABLE crawl_queue ADD COLUMN generation INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
            conn.execute(
                "UPDATE crawl_queue
                 SET generation = COALESCE(
                     (SELECT current_generation FROM roots WHERE roots.id = crawl_queue.root_id),
                     0
                 )",
                [],
            )?;
        }
        Ok(())
    }

    fn ensure_entries_path_tri_backfilled(conn: &Connection) -> Result<(), StoreError> {
        let backfilled: bool = conn
            .query_row(
                "SELECT 1 FROM meta WHERE key = 'entries_path_tri_backfilled'",
                [],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if backfilled {
            return Ok(());
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM entries_path_tri", [])?;
        let rows: Vec<(i64, String)> = {
            let mut stmt = tx.prepare("SELECT id, path FROM entries")?;
            let mapped = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            mapped.collect::<Result<_, _>>()?
        };
        {
            let mut insert =
                tx.prepare("INSERT INTO entries_path_tri(rowid, path) VALUES(?1, ?2)")?;
            for (id, path) in rows {
                insert.execute(params![id, path.to_lowercase()])?;
            }
        }
        tx.execute(
            "INSERT INTO meta(key, value) VALUES('entries_path_tri_backfilled', '1')",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    // ---- roots ----

    /// Registers (or fetches) a root and returns its id.
    pub fn ensure_root(&mut self, path: &str) -> Result<i64, StoreError> {
        self.conn.execute(
            "INSERT INTO roots(path) VALUES(?1) ON CONFLICT(path) DO NOTHING",
            params![path],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT id FROM roots WHERE path = ?1",
            params![path],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    pub fn roots(&self) -> Result<Vec<Root>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, path, enabled, current_generation FROM roots ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok(Root {
                id: row.get(0)?,
                path: row.get(1)?,
                enabled: row.get::<_, i64>(2)? != 0,
                current_generation: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Starts a new scan generation for `root_id` and returns it.
    pub fn begin_generation(&mut self, root_id: i64) -> Result<i64, StoreError> {
        let gen: i64 = self.conn.query_row(
            "UPDATE roots SET current_generation = current_generation + 1
             WHERE id = ?1 RETURNING current_generation",
            params![root_id],
            |row| row.get(0),
        )?;
        Ok(gen)
    }

    /// Deletes entries of `root_id` that were not touched in generation `gen`.
    /// Only call while the root's volume is present (disconnected volumes keep
    /// their entries, flagged `missing`).
    pub fn sweep_generation(&mut self, root_id: i64, gen: i64) -> Result<u64, StoreError> {
        let tx = self.conn.transaction()?;
        let stale = "SELECT id FROM entries WHERE root_id = ?1 AND scan_generation < ?2";
        tx.execute(
            &format!("DELETE FROM entries_fts WHERE rowid IN ({stale})"),
            params![root_id, gen],
        )?;
        tx.execute(
            &format!("DELETE FROM entries_tri WHERE rowid IN ({stale})"),
            params![root_id, gen],
        )?;
        tx.execute(
            &format!("DELETE FROM entries_path_tri WHERE rowid IN ({stale})"),
            params![root_id, gen],
        )?;
        tx.execute(
            &format!("DELETE FROM content_fts WHERE entry_id IN ({stale})"),
            params![root_id, gen],
        )?;
        let deleted_rows = tx.execute(
            "DELETE FROM entries WHERE root_id = ?1 AND scan_generation < ?2",
            params![root_id, gen],
        )?;
        tx.commit()?;
        Ok(deleted_rows as u64)
    }

    /// Marks all of a root's entries as missing (volume gone) or present again.
    pub fn set_missing_for_root(&mut self, root_id: i64, missing: bool) -> Result<u64, StoreError> {
        let n = self.conn.execute(
            "UPDATE entries SET missing = ?2 WHERE root_id = ?1 AND missing != ?2",
            params![root_id, missing as i64],
        )?;
        Ok(n as u64)
    }

    /// Stamps `scan_generation` onto `dir_path` and all of its indexed
    /// descendants under `root_id` so a transient directory scan failure (e.g.
    /// `PermissionDenied`) does not cause [`Self::sweep_generation`] to delete
    /// previously indexed children.
    pub fn touch_subtree_generation(
        &mut self,
        root_id: i64,
        dir_path: &str,
        scan_generation: i64,
    ) -> Result<u64, StoreError> {
        let prefix = dir_path.trim_end_matches(['\\', '/']);
        let prefix_len = prefix.chars().count() as i64;
        let n = self.conn.execute(
            "UPDATE entries SET scan_generation = ?3
             WHERE root_id = ?1
               AND (path = ?2
                    OR (length(path) > ?5
                        AND substr(path, 1, ?5) = ?4
                        AND substr(path, ?5 + 1, 1) IN ('\\', '/')))",
            params![root_id, dir_path, scan_generation, prefix, prefix_len],
        )?;
        Ok(n as u64)
    }

    // ---- entries ----

    /// Upserts a batch of entries in one transaction. `scan_generation` is
    /// stamped onto every row so [`Self::sweep_generation`] can remove stale
    /// ones after a completed scan.
    pub fn apply_batch(
        &mut self,
        root_id: i64,
        scan_generation: i64,
        records: &[EntryRecord],
    ) -> Result<Vec<UpsertOutcome>, StoreError> {
        let tx = self.conn.transaction()?;
        let mut outcomes = Vec::with_capacity(records.len());
        {
            let mut select = tx.prepare_cached(
                "SELECT id, size, mtime, name_lower, is_app, app_target, name, root_id, scan_generation
                 FROM entries WHERE path = ?1",
            )?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO entries(id, root_id, parent_id, path, name, name_lower, ext, is_dir,
                                     size, mtime, volume_serial, file_id, is_app, app_target,
                                     missing, scan_generation)
                 VALUES((SELECT CAST(value AS INTEGER)+1 FROM meta WHERE key='entry_id_high_water'),?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,?14)",
            )?;
            let mut update = tx.prepare_cached(
                "UPDATE entries SET root_id=?2, parent_id=?3, name=?4, name_lower=?5, ext=?6,
                                    is_dir=?7, size=?8, mtime=?9, volume_serial=?10, file_id=?11,
                                    is_app=?12, app_target=?13, missing=0, scan_generation=?14
                 WHERE id=?1",
            )?;
            let mut fts_insert = tx.prepare_cached(
                "INSERT INTO entries_fts(rowid, name, keywords) VALUES(?1, ?2, ?3)",
            )?;
            let mut fts_delete = tx.prepare_cached("DELETE FROM entries_fts WHERE rowid = ?1")?;
            let mut tri_insert =
                tx.prepare_cached("INSERT INTO entries_tri(rowid, name) VALUES(?1, ?2)")?;
            let mut tri_delete = tx.prepare_cached("DELETE FROM entries_tri WHERE rowid = ?1")?;
            let mut path_tri_insert =
                tx.prepare_cached("INSERT INTO entries_path_tri(rowid, path) VALUES(?1, ?2)")?;
            let mut path_tri_delete =
                tx.prepare_cached("DELETE FROM entries_path_tri WHERE rowid = ?1")?;

            for rec in records {
                let name_lower = rec.name.to_lowercase();
                let path_lower = rec.path.to_lowercase();
                let keywords = fts_keywords(rec);
                let existing: Option<ExistingEntry> = select
                    .query_row(params![rec.path], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get::<_, i64>(4)? != 0,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                            row.get(8)?,
                        ))
                    })
                    .optional()?;

                match existing {
                    Some((
                        id,
                        existing_size,
                        existing_mtime,
                        existing_name_lower,
                        existing_is_app,
                        existing_app_target,
                        existing_name,
                        existing_root_id,
                        existing_scan_gen,
                    )) => {
                        let preserve_app = existing_is_app && !rec.is_app;
                        let eff_is_app = if preserve_app { true } else { rec.is_app };
                        let eff_app_target = if preserve_app {
                            existing_app_target.as_str()
                        } else {
                            rec.app_target.as_str()
                        };
                        let eff_name = if preserve_app {
                            existing_name.as_str()
                        } else {
                            rec.name.as_str()
                        };
                        let eff_name_lower = if preserve_app {
                            existing_name_lower.as_str()
                        } else {
                            name_lower.as_str()
                        };
                        let eff_size = if rec.is_app && rec.size == 0 {
                            existing_size
                        } else {
                            rec.size as i64
                        };
                        let eff_mtime = if rec.is_app && rec.mtime == 0 {
                            existing_mtime
                        } else {
                            rec.mtime
                        };
                        let eff_root_id = if preserve_app {
                            existing_root_id
                        } else {
                            root_id
                        };
                        let eff_scan_gen = if preserve_app && existing_root_id != root_id {
                            existing_scan_gen
                        } else {
                            scan_generation
                        };

                        if existing_size == eff_size
                            && existing_mtime == eff_mtime
                            && existing_name_lower == eff_name_lower
                            && existing_is_app == eff_is_app
                            && existing_app_target == eff_app_target
                        {
                            // Unchanged: keep it alive for the sweep and clear a
                            // stale missing flag (the row was seen on disk).
                            tx.execute(
                                "UPDATE entries SET root_id = ?2, scan_generation = ?3, missing = 0 WHERE id = ?1",
                                params![id, eff_root_id, eff_scan_gen],
                            )?;
                            outcomes.push(UpsertOutcome::Unchanged);
                        } else {
                            update.execute(params![
                                id,
                                eff_root_id,
                                rec.parent_id,
                                eff_name,
                                eff_name_lower,
                                rec.ext,
                                rec.is_dir as i64,
                                eff_size,
                                eff_mtime,
                                rec.volume_serial as i64,
                                rec.file_id as i64,
                                eff_is_app as i64,
                                eff_app_target,
                                eff_scan_gen,
                            ])?;
                            if !preserve_app {
                                fts_delete.execute(params![id])?;
                                fts_insert.execute(params![id, eff_name, keywords])?;
                                tri_delete.execute(params![id])?;
                                tri_insert.execute(params![id, eff_name_lower])?;
                                path_tri_delete.execute(params![id])?;
                                path_tri_insert.execute(params![id, path_lower])?;
                            }
                            outcomes.push(UpsertOutcome::Updated);
                        }
                    }
                    None => {
                        insert.execute(params![
                            root_id,
                            rec.parent_id,
                            rec.path,
                            rec.name,
                            name_lower,
                            rec.ext,
                            rec.is_dir as i64,
                            rec.size as i64,
                            rec.mtime,
                            rec.volume_serial as i64,
                            rec.file_id as i64,
                            rec.is_app as i64,
                            rec.app_target,
                            scan_generation,
                        ])?;
                        let id = tx.last_insert_rowid();
                        fts_insert.execute(params![id, rec.name, keywords])?;
                        tri_insert.execute(params![id, name_lower])?;
                        path_tri_insert.execute(params![id, path_lower])?;
                        outcomes.push(UpsertOutcome::Inserted);
                    }
                }
            }
        }
        tx.commit()?;
        Ok(outcomes)
    }

    /// Applies a rename/move while preserving the entry's identity (and with
    /// it history, result ids and the FTS rowid).
    pub fn move_entry(
        &mut self,
        id: i64,
        parent_id: Option<i64>,
        new_path: &str,
        new_name: &str,
        new_ext: &str,
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        {
            let name_lower = new_name.to_lowercase();
            let path_lower = new_path.to_lowercase();
            tx.execute(
                "UPDATE entries SET parent_id=?2, path=?3, name=?4, name_lower=?5, ext=?6
                 WHERE id=?1",
                params![id, parent_id, new_path, new_name, name_lower, new_ext],
            )?;
            let keywords: String = tx.query_row(
                "SELECT app_target FROM entries WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )?;
            let keywords = fts_keywords(&EntryRecord {
                root_id: 0,
                parent_id,
                path: new_path.to_string(),
                name: new_name.to_string(),
                ext: new_ext.to_string(),
                keywords,
                ..Default::default()
            });
            tx.execute("DELETE FROM entries_fts WHERE rowid = ?1", params![id])?;
            tx.execute(
                "INSERT INTO entries_fts(rowid, name, keywords) VALUES(?1, ?2, ?3)",
                params![id, new_name, keywords],
            )?;
            tx.execute("DELETE FROM entries_tri WHERE rowid = ?1", params![id])?;
            tx.execute(
                "INSERT INTO entries_tri(rowid, name) VALUES(?1, ?2)",
                params![id, name_lower],
            )?;
            tx.execute("DELETE FROM entries_path_tri WHERE rowid = ?1", params![id])?;
            tx.execute(
                "INSERT INTO entries_path_tri(rowid, path) VALUES(?1, ?2)",
                params![id, path_lower],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn find_by_path(&self, path: &str) -> Result<Option<EntryRow>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                        size, mtime, missing, thumbnail_path
                 FROM entries WHERE path = ?1",
                params![path],
                map_entry_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Finds an entry by filesystem identity (volume serial + file id) —
    /// the backbone of rename/move detection.
    pub fn find_by_file_id(
        &self,
        volume_serial: u32,
        file_id: u64,
    ) -> Result<Option<EntryRow>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                        size, mtime, missing, thumbnail_path
                 FROM entries WHERE volume_serial = ?1 AND file_id = ?2 LIMIT 1",
                params![volume_serial as i64, file_id as i64],
                map_entry_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn remove_by_path(&mut self, path: &str) -> Result<bool, StoreError> {
        let tx = self.conn.transaction()?;
        let id: Option<i64> = tx
            .query_row(
                "SELECT id FROM entries WHERE path = ?1",
                params![path],
                |r| r.get(0),
            )
            .optional()?;
        let removed = match id {
            Some(id) => {
                tx.execute("DELETE FROM entries_fts WHERE rowid = ?1", params![id])?;
                tx.execute("DELETE FROM entries_tri WHERE rowid = ?1", params![id])?;
                tx.execute("DELETE FROM entries_path_tri WHERE rowid = ?1", params![id])?;
                tx.execute("DELETE FROM content_fts WHERE entry_id = ?1", params![id])?;
                tx.execute("DELETE FROM entries WHERE id = ?1", params![id])? > 0
            }
            None => false,
        };
        tx.commit()?;
        Ok(removed)
    }

    /// Removes an entry and its whole subtree (a directory disappeared).
    pub fn remove_tree(&mut self, path: &str) -> Result<u64, StoreError> {
        let tx = self.conn.transaction()?;
        let ids: Vec<i64> = {
            let mut stmt = tx.prepare(
                "WITH RECURSIVE sub(id) AS (
                     SELECT id FROM entries WHERE path = ?1
                     UNION ALL
                     SELECT e.id FROM entries e JOIN sub ON e.parent_id = sub.id
                 )
                 SELECT id FROM sub",
            )?;
            let rows = stmt.query_map(params![path], |r| r.get::<_, i64>(0))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut deleted = 0u64;
        for id in ids {
            tx.execute("DELETE FROM entries_fts WHERE rowid = ?1", params![id])?;
            tx.execute("DELETE FROM entries_tri WHERE rowid = ?1", params![id])?;
            tx.execute("DELETE FROM entries_path_tri WHERE rowid = ?1", params![id])?;
            tx.execute("DELETE FROM content_fts WHERE entry_id = ?1", params![id])?;
            deleted += tx.execute("DELETE FROM entries WHERE id = ?1", params![id])? as u64;
        }
        tx.commit()?;
        Ok(deleted)
    }

    // ---- crawl queue (crash-safe work list) ----

    pub fn enqueue_dir(&mut self, root_id: i64, dir_path: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO crawl_queue(root_id, dir_path, generation)
             VALUES(
                 ?1,
                 ?2,
                 COALESCE((SELECT current_generation FROM roots WHERE id = ?1), 0)
             )
             ON CONFLICT(root_id, dir_path) DO UPDATE SET
                 id = excluded.id, generation = excluded.generation, claimed = 0
             WHERE crawl_queue.generation != excluded.generation",
            params![root_id, dir_path],
        )?;
        Ok(())
    }

    /// Forces `(root_id, dir_path)` to be queued as an unclaimed crawl item
    /// with a fresh queue id (used on watcher overflow so an already-claimed
    /// scan cannot swallow the rescan request when it completes).
    pub fn requeue_dir(&mut self, root_id: i64, dir_path: &str) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM crawl_queue WHERE root_id = ?1 AND dir_path = ?2",
            params![root_id, dir_path],
        )?;
        tx.execute(
            "INSERT INTO crawl_queue(root_id, dir_path, generation)
             VALUES(
                 ?1,
                 ?2,
                 COALESCE((SELECT current_generation FROM roots WHERE id = ?1), 0)
             )",
            params![root_id, dir_path],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Atomically claims the oldest unclaimed directory for a worker. The row
    /// stays in the queue until [`Self::complete_dir`] runs, so a crash at any
    /// point re-runs the (idempotent) scan of that directory on restart.
    pub fn claim_dir(&mut self) -> Result<Option<(i64, i64, String)>, StoreError> {
        self.claim_dir_with_generation()
            .map(|opt| opt.map(|(id, root_id, dir_path, _gen)| (id, root_id, dir_path)))
    }

    /// Atomically claims the oldest unclaimed directory for a worker along
    /// with the `generation` in which it was enqueued.
    pub fn claim_dir_with_generation(
        &mut self,
    ) -> Result<Option<(i64, i64, String, i64)>, StoreError> {
        self.conn
            .query_row(
                "UPDATE crawl_queue SET claimed = 1
                 WHERE id = (SELECT id FROM crawl_queue WHERE claimed = 0 AND root_id IN (SELECT id FROM roots WHERE enabled=1) ORDER BY id LIMIT 1)
                 RETURNING id, root_id, dir_path, generation",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Requeues directories claimed by a previous process that never finished.
    pub fn release_claims(&mut self) -> Result<(), StoreError> {
        self.conn
            .execute("UPDATE crawl_queue SET claimed = 0 WHERE claimed = 1", [])?;
        Ok(())
    }

    /// Marks one queued directory as processed.
    pub fn complete_dir(&mut self, queue_id: i64) -> Result<(), StoreError> {
        self.conn
            .execute("DELETE FROM crawl_queue WHERE id = ?1", params![queue_id])?;
        Ok(())
    }

    pub fn queue_depth(&self) -> Result<u64, StoreError> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM crawl_queue", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    pub fn clear_queue(&mut self) -> Result<(), StoreError> {
        self.conn.execute("DELETE FROM crawl_queue", [])?;
        Ok(())
    }

    /// Rewrites descendant paths and FTS keywords after a directory rename.
    /// The renamed row itself is moved via [`Self::move_entry`]; only subtree
    /// paths and path-derived keywords change here (names are untouched).
    pub fn rename_tree_paths(
        &mut self,
        old_prefix: &str,
        new_prefix: &str,
    ) -> Result<u64, StoreError> {
        let old_len = old_prefix.chars().count() as i64;
        let tx = self.conn.transaction()?;
        let descendants: Vec<(i64, String, String, String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT id, name, ext, app_target, (?1 || substr(path, ?3 + 1)) AS new_path
                 FROM entries
                 WHERE length(path) > ?3
                   AND substr(path, 1, ?3) = ?2
                   AND substr(path, ?3 + 1, 1) IN ('\\', '/')",
            )?;
            let rows = stmt.query_map(params![new_prefix, old_prefix, old_len], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?;
            rows.collect::<Result<_, _>>()?
        };
        let n = descendants.len() as u64;
        if n > 0 {
            let mut update_path =
                tx.prepare_cached("UPDATE entries SET path = ?2 WHERE id = ?1")?;
            let mut fts_delete = tx.prepare_cached("DELETE FROM entries_fts WHERE rowid = ?1")?;
            let mut fts_insert = tx.prepare_cached(
                "INSERT INTO entries_fts(rowid, name, keywords) VALUES(?1, ?2, ?3)",
            )?;
            let mut path_tri_delete =
                tx.prepare_cached("DELETE FROM entries_path_tri WHERE rowid = ?1")?;
            let mut path_tri_insert =
                tx.prepare_cached("INSERT INTO entries_path_tri(rowid, path) VALUES(?1, ?2)")?;
            for (id, name, ext, app_target, new_path) in descendants {
                let path_lower = new_path.to_lowercase();
                update_path.execute(params![id, new_path])?;
                let keywords = fts_keywords(&EntryRecord {
                    root_id: 0,
                    parent_id: None,
                    path: new_path,
                    name: name.clone(),
                    ext,
                    keywords: app_target,
                    ..Default::default()
                });
                fts_delete.execute(params![id])?;
                fts_insert.execute(params![id, name, keywords])?;
                path_tri_delete.execute(params![id])?;
                path_tri_insert.execute(params![id, path_lower])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Alias for [`Self::rename_tree_paths`].
    pub fn rename_subtree(
        &mut self,
        old_prefix: &str,
        new_prefix: &str,
    ) -> Result<u64, StoreError> {
        self.rename_tree_paths(old_prefix, new_prefix)
    }

    /// Number of queued (unfinished) directories for a root.
    pub fn pending_for_root(&self, root_id: i64) -> Result<u64, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM crawl_queue WHERE root_id = ?1",
            params![root_id],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// True when any of the root's entries are flagged missing (disconnected
    /// volume state).
    pub fn has_missing_entries(&self, root_id: i64) -> Result<bool, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM entries WHERE root_id = ?1 AND missing = 1 LIMIT 1",
            params![root_id],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    // ---- meta (unversioned key/value records, e.g. embedding compat) ----

    /// Reads a `meta` value (e.g. the `embedding_model` compat record).
    pub fn meta_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Upserts a `meta` value. Additive by design: new keys never disturb
    /// existing rows, and `schema_version` handling is untouched.
    pub fn meta_set(&mut self, key: &str, value: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    // ---- engine config (P3.6: semantic policy + backend preference) ----

    /// Reads one raw `config` value (`None` when the key was never set).
    pub fn config_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row(
                "SELECT value FROM config WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Upserts one raw `config` value. Additive: new keys never disturb
    /// existing rows.
    pub fn config_set(&mut self, key: &str, value: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO config(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Reads the engine search policy. Missing keys read back as defaults
    /// (`semantic_enabled = true`, `preferred_backend = auto`); a stored
    /// `preferred_backend` outside `auto|npu|cuda|vulkan|cpu` fails closed with
    /// `InvalidConfig` so typos surface instead of silently sticking.
    pub fn get_engine_config(&self) -> Result<EngineConfig, StoreError> {
        let defaults = EngineConfig::default();
        let semantic_enabled = match self.config_get(CONFIG_KEY_SEMANTIC_ENABLED)? {
            None => defaults.semantic_enabled,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => true,
                "0" | "false" | "no" | "off" => false,
                other => {
                    return Err(StoreError::InvalidConfig(format!(
                        "config {CONFIG_KEY_SEMANTIC_ENABLED} has no boolean meaning: {other:?}"
                    )));
                }
            },
        };
        let preferred_backend = match self.config_get(CONFIG_KEY_PREFERRED_BACKEND)? {
            None => defaults.preferred_backend,
            Some(v) => PreferredBackend::parse(&v).ok_or_else(|| {
                StoreError::InvalidConfig(format!(
                    "config {CONFIG_KEY_PREFERRED_BACKEND} must be auto|npu|cuda|vulkan|cpu, got {v:?}"
                ))
            })?,
        };
        Ok(EngineConfig {
            semantic_enabled,
            preferred_backend,
        })
    }

    /// Persists the engine search policy in canonical spellings
    /// (`"1"`/`"0"`, `auto|npu|cuda|vulkan|cpu`).
    pub fn set_engine_config(&mut self, config: &EngineConfig) -> Result<(), StoreError> {
        self.config_set(
            CONFIG_KEY_SEMANTIC_ENABLED,
            if config.semantic_enabled { "1" } else { "0" },
        )?;
        self.config_set(
            CONFIG_KEY_PREFERRED_BACKEND,
            config.preferred_backend.as_str(),
        )?;
        Ok(())
    }

    // ---- checkpoints ----

    pub fn set_checkpoint(&mut self, key: &str, value: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO checkpoints(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn checkpoint(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row(
                "SELECT value FROM checkpoints WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    // ---- vector segments (schema v2) ----

    /// Inserts or replaces the segment for `(entry_id, kind, chunk_index)`.
    /// The id is stable across replacements so the in-memory [`crate::vectors::VectorIndex`]
    /// can update its slot instead of churning. Returns the segment id.
    pub fn upsert_segment(&mut self, seg: NewSegment<'_>) -> Result<i64, StoreError> {
        if seg.vector.is_empty() {
            return Err(StoreError::InvalidVector("empty vector".into()));
        }
        if seg.kind.is_empty() {
            return Err(StoreError::InvalidVector("empty kind".into()));
        }
        let (bytes, norm) = encode_normalized_vector(seg.vector);
        if norm == 0.0 {
            return Err(StoreError::InvalidVector(
                "zero-length or non-finite vector".into(),
            ));
        }
        let id: i64 = self.conn.query_row(
            "INSERT INTO segments(entry_id, kind, chunk_index, text, page, start_ms, end_ms,
                                  line, vector, norm)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(entry_id, kind, chunk_index) DO UPDATE SET
                 text = excluded.text, page = excluded.page,
                 start_ms = excluded.start_ms, end_ms = excluded.end_ms,
                 line = excluded.line, vector = excluded.vector, norm = excluded.norm
             RETURNING id",
            params![
                seg.entry_id,
                seg.kind,
                seg.chunk_index,
                seg.text,
                seg.page,
                seg.start_ms,
                seg.end_ms,
                seg.line,
                bytes,
                norm as f64,
            ],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    pub fn get_segment(&self, id: i64) -> Result<Option<Segment>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, entry_id, kind, chunk_index, text, page, start_ms, end_ms,
                        line, vector, norm
                 FROM segments WHERE id = ?1",
                params![id],
                map_segment_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_segments_for_entry(&self, entry_id: i64) -> Result<Vec<Segment>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, entry_id, kind, chunk_index, text, page, start_ms, end_ms,
                    line, vector, norm
             FROM segments WHERE entry_id = ?1 ORDER BY kind, chunk_index",
        )?;
        let rows = stmt.query_map(params![entry_id], map_segment_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Every segment ordered by id (startup path for rebuilding the in-memory
    /// scan index). Decodes FP16 to normalized `f32`.
    pub fn all_segments(&self) -> Result<Vec<Segment>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, entry_id, kind, chunk_index, text, page, start_ms, end_ms,
                    line, vector, norm
             FROM segments WHERE EXISTS (SELECT 1 FROM visible_entries v WHERE v.id = segments.entry_id) ORDER BY id",
        )?;
        let rows = stmt.query_map([], map_segment_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Raw `(id, entry_id, kind, fp16_bytes, norm)` rows for bulk loading a
    /// [`crate::vectors::VectorIndex`] without an intermediate `f32` decode.
    pub fn all_segment_vectors_raw(&self) -> Result<Vec<RawSegmentVector>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, entry_id, kind, vector, norm FROM segments WHERE EXISTS (SELECT 1 FROM visible_entries v WHERE v.id = segments.entry_id) ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            let bytes: Vec<u8> = row.get(3)?;
            let norm_f64: f64 = row.get(4)?;
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                bytes,
                norm_f64 as f32,
            ))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn remove_segment(&mut self, id: i64) -> Result<bool, StoreError> {
        let n = self
            .conn
            .execute("DELETE FROM segments WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }

    /// Deletes every segment of one entry (file-deleted path). Entry deletes
    /// also cascade via FK; this helper covers explicit eviction and tests.
    pub fn remove_segments_for_entry(&mut self, entry_id: i64) -> Result<u64, StoreError> {
        let n = self.conn.execute(
            "DELETE FROM segments WHERE entry_id = ?1",
            params![entry_id],
        )?;
        Ok(n as u64)
    }

    /// Deletes every segment of `(entry_id, kind)` (re-extraction path before
    /// writing the fresh chunk set).
    pub fn remove_segments_for_entry_and_kind(
        &mut self,
        entry_id: i64,
        kind: &str,
    ) -> Result<u64, StoreError> {
        let n = self.conn.execute(
            "DELETE FROM segments WHERE entry_id = ?1 AND kind = ?2",
            params![entry_id, kind],
        )?;
        Ok(n as u64)
    }

    /// Deletes all `content_fts` rows for `entry_id` (used when re-extraction
    /// returns `Unsupported` or `Failed`).
    pub fn delete_content_text(&mut self, entry_id: i64) -> Result<u64, StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM segments WHERE entry_id=?1 AND kind!='name'",
            [entry_id],
        )?;
        let n = tx.execute(
            "DELETE FROM content_fts WHERE entry_id = ?1",
            params![entry_id],
        )?;
        tx.execute("DELETE FROM content_chunks WHERE entry_id=?1", [entry_id])?;
        tx.execute("DELETE FROM content_progress WHERE entry_id=?1", [entry_id])?;
        tx.commit()?;
        Ok(n as u64)
    }

    /// Replaces the FTS-indexed content text for `(entry_id, kind)` with
    /// `texts` (non-empty strings are concatenated and indexed in
    /// `content_fts`, making extracted document text and OCR searchable via
    /// FTS as well as vectors).
    pub fn replace_content_fts<S: AsRef<str>>(
        &mut self,
        entry_id: i64,
        kind: &str,
        texts: &[S],
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM content_fts WHERE entry_id = ?1 AND kind = ?2",
            params![entry_id, kind],
        )?;
        tx.execute(
            "DELETE FROM content_chunks WHERE entry_id=?1 AND job_kind=?2",
            params![entry_id, kind],
        )?;
        let mut combined = String::new();
        for t in texts {
            let s = t.as_ref().trim();
            if !s.is_empty() {
                if !combined.is_empty() {
                    combined.push('\n');
                }
                combined.push_str(s);
            }
        }
        if !combined.is_empty() {
            tx.execute("INSERT INTO content_chunks(entry_id,job_kind,kind,chunk_index,text) VALUES(?1,?2,?2,0,?3)",params![entry_id,kind,combined])?;
            let chunk_id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO content_fts(rowid,entry_id, kind, text) VALUES(?1, ?2, ?3,?4)",
                params![chunk_id, entry_id, kind, combined],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn segment_count(&self) -> Result<u64, StoreError> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM segments", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Counts segments of one kind (e.g. `name` → `segments_indexed` status).
    pub fn count_segments_by_kind(&self, kind: &str) -> Result<u64, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM segments WHERE kind = ?1",
            params![kind],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// Brute-force cosine top-k over the `segments` table. The query is
    /// normalized once; each stored FP16 row is scored directly from its raw
    /// `vector` BLOB bytes with [`crate::vectors::dot_fp16_row_vs_normalized`]
    /// (`f32` accumulation — the same kernel as
    /// [`crate::vectors::VectorIndex::search`]).
    /// Rows whose width differs from the query are skipped. Results are
    /// `(segment, cosine)` pairs sorted by descending cosine (ties by id).
    ///
    /// Contract (shared with [`crate::vectors::VectorIndex::search`]):
    /// `top_k == 0`, an empty query, and a zero/non-finite query all yield
    /// `Ok(vec![])` — an empty query is not an error, matching the crate's
    /// other search APIs. Width mismatch is the one deliberate difference:
    /// the fixed-dim index reports `Err(DimensionMismatch)` while this
    /// schemaless table skips non-matching rows (so a query matching no
    /// row's width yields `Ok(vec![])`). Callers needing strictness should
    /// validate the width up front (`VectorIndex::dim` gives the expected one).
    pub fn search_vectors(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<ScoredSegment>, StoreError> {
        if top_k == 0 || query.is_empty() {
            return Ok(Vec::new());
        }
        let (normed_query, query_norm) = normalize_vector(query);
        if query_norm == 0.0 {
            return Ok(Vec::new());
        }
        // Push the filter into SQL so large indexes don't materialize every row.
        let mut sql = String::from(
            "SELECT id, entry_id, kind, chunk_index, text, page, start_ms, end_ms,
                    line, vector, norm
             FROM segments WHERE EXISTS (SELECT 1 FROM visible_entries v WHERE v.id = segments.entry_id)",
        );
        // Own the bound values so the prepared statement can borrow them.
        let kinds_owned: Vec<String>;
        let entries_owned: Vec<i64>;
        let mut clauses: Vec<String> = Vec::new();
        let mut heap: std::collections::BinaryHeap<WorstFirstScored> =
            std::collections::BinaryHeap::with_capacity(top_k.min(1024));
        let push_bounded = |heap: &mut std::collections::BinaryHeap<WorstFirstScored>,
                            cand: ScoredSegment| {
            let item = WorstFirstScored(cand);
            if heap.len() < top_k {
                heap.push(item);
            } else if let Some(worst) = heap.peek() {
                if item < *worst {
                    heap.pop();
                    heap.push(item);
                }
            }
        };
        if let Some(f) = filter {
            if !f.kinds.is_empty() {
                kinds_owned = f.kinds.clone();
                let ph = kinds_owned
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("?{}", i + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                clauses.push(format!("kind IN ({ph})"));
            } else {
                kinds_owned = Vec::new();
            }
            if !f.entry_ids.is_empty() {
                let base = kinds_owned.len();
                entries_owned = f.entry_ids.clone();
                let ph = entries_owned
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("?{}", base + i + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                clauses.push(format!("entry_id IN ({ph})"));
            } else {
                entries_owned = Vec::new();
            }
            if !clauses.is_empty() {
                sql.push_str(" AND ");
                sql.push_str(&clauses.join(" AND "));
            }
            let mut stmt = self.conn.prepare(&sql)?;
            let mut params_vec: Vec<&dyn rusqlite::ToSql> = Vec::new();
            for k in &kinds_owned {
                params_vec.push(k);
            }
            for e in &entries_owned {
                params_vec.push(e);
            }
            let rows = stmt.query_map(params_vec.as_slice(), map_segment_row_raw)?;
            for row in rows {
                let raw: RawSegmentRow = row?;
                if raw.bytes.len() != query.len() * 2 {
                    continue; // mixed-width DB (e.g. model swap): skip, don't fail
                }
                if raw.segment.norm == 0.0 {
                    continue; // mirrors VectorIndex::search (unreachable via upsert)
                }
                let cos = dot_fp16_row_vs_normalized(&raw.bytes, &normed_query);
                push_bounded(
                    &mut heap,
                    ScoredSegment {
                        segment: raw.segment,
                        cosine: cos,
                    },
                );
            }
            let mut scored: Vec<ScoredSegment> = heap.into_iter().map(|w| w.0).collect();
            scored.sort_by(|a, b| {
                b.cosine
                    .total_cmp(&a.cosine)
                    .then_with(|| a.segment.id.cmp(&b.segment.id))
            });
            return Ok(scored);
        }
        // No filter: full scan.
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], map_segment_row_raw)?;
        for row in rows {
            let raw: RawSegmentRow = row?;
            if raw.bytes.len() != query.len() * 2 {
                continue;
            }
            if raw.segment.norm == 0.0 {
                continue; // mirrors VectorIndex::search (unreachable via upsert)
            }
            let cos = dot_fp16_row_vs_normalized(&raw.bytes, &normed_query);
            push_bounded(
                &mut heap,
                ScoredSegment {
                    segment: raw.segment,
                    cosine: cos,
                },
            );
        }
        let mut scored: Vec<ScoredSegment> = heap.into_iter().map(|w| w.0).collect();
        scored.sort_by(|a, b| {
            b.cosine
                .total_cmp(&a.cosine)
                .then_with(|| a.segment.id.cmp(&b.segment.id))
        });
        Ok(scored)
    }

    // ---- extract jobs (schema v2, crash-safe queue for Phase 4) ----

    /// Enqueues `(entry_id, kind)` idempotently at default priority
    /// (`EXTRACT_PRIORITY_UPDATED`). See [`Self::enqueue_extract_job_with_priority`].
    pub fn enqueue_extract_job(&mut self, entry_id: i64, kind: &str) -> Result<(), StoreError> {
        self.enqueue_extract_job_with_priority(
            entry_id,
            kind,
            crate::model::EXTRACT_PRIORITY_UPDATED,
        )
    }

    /// Enqueues `(entry_id, kind)` idempotently at `priority`. A repeat enqueue
    /// for an already-queued job keeps the higher of the two priorities so a
    /// later `Inserted` upgrades a pending `Updated` instead of being dropped;
    /// if the existing row is unclaimed (e.g. previously marked `'failed'`),
    /// it is reset to `state = 'pending', attempts = 0` so a modified file is
    /// re-extracted.
    pub fn enqueue_extract_job_with_priority(
        &mut self,
        entry_id: i64,
        kind: &str,
        priority: i64,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO extract_jobs(entry_id, kind, state, claimed, attempts, priority)
             VALUES(?1, ?2, 'pending', 0, 0, ?3)
             ON CONFLICT(entry_id, kind) DO UPDATE SET
                 state = CASE WHEN extract_jobs.claimed = 0 THEN 'pending' ELSE extract_jobs.state END,
                 attempts = CASE WHEN extract_jobs.claimed = 0 THEN 0 ELSE extract_jobs.attempts END,
                 priority = CASE WHEN extract_jobs.state = 'failed'
                                 THEN excluded.priority
                                 WHEN excluded.priority > extract_jobs.priority
                                 THEN excluded.priority
                                 ELSE extract_jobs.priority END",
            params![entry_id, kind, priority],
        )?;
        Ok(())
    }

    /// Atomically claims the highest-priority pending job (new files first,
    /// LIFO within priority: `priority DESC, id DESC`). Sets `claimed = 1`,
    /// `state = 'claimed'`. The row stays queued until
    /// [`Self::complete_extract_job`], so a crash re-runs it after
    /// [`Self::release_extract_claims`].
    pub fn claim_extract_job(&mut self) -> Result<Option<ExtractJob>, StoreError> {
        self.conn
            .query_row(
                "UPDATE extract_jobs SET claimed = 1, state = 'claimed'
                 WHERE id = (SELECT id FROM extract_jobs
                             WHERE claimed = 0 AND state = 'pending' AND EXISTS (SELECT 1 FROM visible_entries v WHERE v.id=extract_jobs.entry_id)
                             ORDER BY priority DESC, id DESC LIMIT 1)
                 RETURNING id, entry_id, kind, state, claimed, attempts, priority",
                [],
                map_extract_job_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Claims up to `limit` pending jobs in priority order (one
    /// [`Self::claim_extract_job`] per job). Returns the claimed batch, which
    /// the caller embeds and then completes/fails per job.
    pub fn claim_extract_jobs_batch(
        &mut self,
        limit: usize,
    ) -> Result<Vec<ExtractJob>, StoreError> {
        let mut out = Vec::new();
        for _ in 0..limit {
            match self.claim_extract_job()? {
                Some(job) => out.push(job),
                None => break,
            }
        }
        Ok(out)
    }

    /// Claims up to `limit` pending jobs of one `kind` in priority order
    /// (`priority DESC, id DESC`: new files first, LIFO within priority).
    /// The embed scheduler (P3.5) claims `kind='name'` so Phase-4 kinds are
    /// never stolen.
    pub fn claim_extract_jobs_batch_for_kind(
        &mut self,
        kind: &str,
        limit: usize,
    ) -> Result<Vec<ExtractJob>, StoreError> {
        let mut out = Vec::new();
        for _ in 0..limit {
            let job: Option<ExtractJob> = self
                .conn
                .query_row(
                    "UPDATE extract_jobs SET claimed = 1, state = 'claimed'
                     WHERE id = (SELECT id FROM extract_jobs
                                 WHERE claimed = 0 AND state = 'pending' AND EXISTS (SELECT 1 FROM visible_entries v WHERE v.id=extract_jobs.entry_id) AND kind = ?1
                                 ORDER BY priority DESC, id DESC LIMIT 1)
                     RETURNING id, entry_id, kind, state, claimed, attempts, priority",
                    params![kind],
                    map_extract_job_row,
                )
                .optional()?;
            match job {
                Some(j) => out.push(j),
                None => break,
            }
        }
        Ok(out)
    }

    /// Releases claims held by a previous (crashed) process back to pending.
    /// Returns the number of rows released.
    pub fn release_extract_claims(&mut self) -> Result<u64, StoreError> {
        let n = self.conn.execute(
            "UPDATE extract_jobs SET claimed = 0, state = 'pending'
             WHERE claimed = 1 AND state = 'claimed'",
            [],
        )?;
        Ok(n as u64)
    }

    /// Marks one job done (dequeues it).
    pub fn complete_extract_job(&mut self, id: i64) -> Result<(), StoreError> {
        self.conn
            .execute("DELETE FROM extract_jobs WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Records a retryable failure: `attempts + 1`, back to pending.
    /// Permanent failures are the caller's call (delete or mark `failed` via
    /// [`Self::mark_extract_failed`]).
    pub fn fail_extract_job(&mut self, id: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE extract_jobs SET claimed = 0, state = 'pending',
                 attempts = attempts + 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Releases a claim WITHOUT recording a failure (mid-batch change path:
    /// the entry changed under a claimed batch, so the fresh data — already
    /// re-enqueued with its priority bump — embeds next batch; attempts is
    /// untouched so a rapidly-changing file is never poisoned as "failing").
    pub fn requeue_extract_job(&mut self, id: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE extract_jobs SET claimed = 0, state = 'pending' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Looks up one `(entry_id, kind)` job (poison-pill inspection, tests).
    pub fn find_extract_job(
        &self,
        entry_id: i64,
        kind: &str,
    ) -> Result<Option<ExtractJob>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, entry_id, kind, state, claimed, attempts, priority
                 FROM extract_jobs WHERE entry_id = ?1 AND kind = ?2",
                params![entry_id, kind],
                map_extract_job_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Records a permanent failure (kept for inspection, never auto-claimed).
    pub fn mark_extract_failed(&mut self, id: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE extract_jobs SET claimed = 0, state = 'failed',
                 attempts = attempts + 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn extract_queue_depth(&self) -> Result<u64, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM extract_jobs WHERE state = 'pending' OR claimed = 1",
            [],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// Claims the highest-priority pending content extraction job (`kind != 'name'`),
    /// ordered by `priority DESC, id DESC` (small + recent files first, LIFO on ties).
    pub fn claim_content_extract_job(&mut self) -> Result<Option<ExtractJob>, StoreError> {
        self.conn
            .query_row(
                "UPDATE extract_jobs SET claimed = 1, state = 'claimed'
                 WHERE id = (SELECT id FROM extract_jobs
                             WHERE claimed = 0 AND state = 'pending' AND EXISTS (SELECT 1 FROM visible_entries v WHERE v.id=extract_jobs.entry_id) AND kind != ?1
                             ORDER BY priority DESC, id DESC LIMIT 1)
                 RETURNING id, entry_id, kind, state, claimed, attempts, priority",
                params![crate::model::SEGMENT_KIND_NAME],
                map_extract_job_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Pending-or-claimed depth for all non-name content extraction jobs.
    pub fn content_extract_queue_depth(&self) -> Result<u64, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM extract_jobs
             WHERE kind != ?1 AND (state = 'pending' OR claimed = 1)",
            params![crate::model::SEGMENT_KIND_NAME],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }
    pub fn extract_queue_depth_for_kind(&self, kind: &str) -> Result<u64, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM extract_jobs
              WHERE kind = ?1 AND (state = 'pending' OR claimed = 1)",
            params![kind],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    pub fn get_extract_job(&self, id: i64) -> Result<Option<ExtractJob>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, entry_id, kind, state, claimed, attempts, priority
                 FROM extract_jobs WHERE id = ?1",
                params![id],
                map_extract_job_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// One entry by id (embed scheduler resolves names for claimed jobs).
    pub fn get_entry(&self, id: i64) -> Result<Option<EntryRow>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, root_id, parent_id, path, name, name_lower, ext, is_dir, is_app,
                        size, mtime, missing, thumbnail_path
                 FROM entries WHERE id = ?1",
                params![id],
                map_entry_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn entry_identity(&self, id: i64) -> Result<Option<(u32, u64)>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT volume_serial,file_id FROM entries WHERE id=?1",
                [id],
                |r| Ok((r.get::<_, i64>(0)? as u32, r.get::<_, i64>(1)? as u64)),
            )
            .optional()?)
    }

    /// Persists or clears the cached `thumbnail_path` for `entry_id`.
    pub fn set_entry_thumbnail_path(
        &mut self,
        entry_id: i64,
        thumbnail_path: Option<&str>,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE entries SET thumbnail_path = ?2 WHERE id = ?1",
            params![entry_id, thumbnail_path],
        )?;
        Ok(())
    }

    /// Every entry id, ordered (model-swap requeue path enqueues one name job
    /// per entry).
    pub fn all_entry_ids(&self) -> Result<Vec<i64>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT id FROM entries ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, i64>(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Returns non-missing entry ids matching the given `apps_only`,
    /// `exclude_apps`, and `kinds` filters. Used to pre-filter semantic vector
    /// candidates before top-k truncation so filtered queries stay bounded.
    pub fn matching_entry_ids(
        &self,
        apps_only: bool,
        exclude_apps: bool,
        kinds: &[crate::model::HitKind],
    ) -> Result<Vec<i64>, StoreError> {
        self.matching_entry_ids_filtered(apps_only, exclude_apps, kinds, &[], &[])
    }

    pub fn matching_entry_ids_filtered(
        &self,
        apps_only: bool,
        exclude_apps: bool,
        kinds: &[crate::model::HitKind],
        extensions: &[String],
        file_kinds: &[ws_protocol::FileKind],
    ) -> Result<Vec<i64>, StoreError> {
        if !ws_protocol::valid_search_extensions(extensions)
            || !ws_protocol::valid_search_file_kinds(file_kinds)
        {
            return Err(StoreError::InvalidConfig(
                "invalid search extension filter".into(),
            ));
        }
        let mut sql = String::from(
            "SELECT id, path, ext, is_dir, is_app FROM visible_entries WHERE missing = 0",
        );
        if apps_only {
            sql.push_str(" AND is_app = 1");
        }
        if exclude_apps {
            sql.push_str(" AND is_app = 0");
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)? != 0,
                row.get::<_, i64>(4)? != 0,
            ))
        })?;
        let mut ids = Vec::new();
        for r in rows {
            let (id, path, ext, is_dir, is_app) = r?;
            if (!extensions.is_empty() && is_dir)
                || !ws_protocol::search_extension_allowed(extensions, &ext)
            {
                continue;
            }
            if !ws_protocol::search_file_kind_allowed(file_kinds, &ext, is_dir) {
                continue;
            }
            if !kinds.is_empty() {
                let kind = if is_app {
                    crate::model::HitKind::Application
                } else if is_dir {
                    crate::model::HitKind::Folder
                } else {
                    crate::model::kind_for_path_and_ext(&path, &ext).hit_kind()
                };
                if !kinds.contains(&kind) {
                    continue;
                }
            }
            ids.push(id);
        }
        Ok(ids)
    }

    /// Deletes every segment of one kind (model-swap purge path). Returns the
    /// number removed.
    pub fn purge_segments_by_kind(&self, kind: &str) -> Result<u64, StoreError> {
        let n = self
            .conn
            .execute("DELETE FROM segments WHERE kind = ?1", params![kind])?;
        Ok(n as u64)
    }

    // ---- stats ----

    pub fn stats(&self) -> Result<IndexStats, StoreError> {
        let entries_total: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))?;
        let files_total: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM entries WHERE is_dir = 0", [], |r| {
                    r.get(0)
                })?;
        let dirs_total: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM entries WHERE is_dir = 1", [], |r| {
                    r.get(0)
                })?;
        let apps_total: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM entries WHERE is_app = 1", [], |r| {
                    r.get(0)
                })?;
        let page_count: i64 = self.conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let page_size: i64 = self.conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        Ok(IndexStats {
            entries_total: entries_total as u64,
            files_total: files_total as u64,
            dirs_total: dirs_total as u64,
            apps_total: apps_total as u64,
            index_size_bytes: (page_count * page_size) as u64,
        })
    }

    /// Raw connection access for the search module (same crate only).
    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }
}

fn map_entry_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EntryRow> {
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

fn map_segment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Segment> {
    map_segment_row_raw(row).map(|raw| raw.segment)
}

/// Wrapper ordering `ScoredSegment` with the *worst* element considered
/// `Greater`, so `BinaryHeap::peek()` exposes the eviction candidate in a
/// bounded top-K heap.
struct WorstFirstScored(ScoredSegment);

impl PartialEq for WorstFirstScored {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for WorstFirstScored {}

impl PartialOrd for WorstFirstScored {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorstFirstScored {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .cosine
            .total_cmp(&self.0.cosine)
            .then_with(|| self.0.segment.id.cmp(&other.0.segment.id))
    }
}

/// A segment row carrying both the decoded payload and the raw FP16 bytes.
/// Scoring reads `bytes` directly (no decode→re-encode round trip); `segment`
/// is the payload returned to callers.
struct RawSegmentRow {
    segment: Segment,
    bytes: Vec<u8>,
}

fn map_segment_row_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawSegmentRow> {
    let bytes: Vec<u8> = row.get(9)?;
    let norm_f64: f64 = row.get(10)?;
    let vector = crate::vectors::decode_fp16_vector(&bytes);
    Ok(RawSegmentRow {
        segment: Segment {
            id: row.get(0)?,
            entry_id: row.get(1)?,
            kind: row.get(2)?,
            chunk_index: row.get(3)?,
            text: row.get(4)?,
            page: row.get(5)?,
            start_ms: row.get(6)?,
            end_ms: row.get(7)?,
            line: row.get(8)?,
            vector,
            norm: norm_f64 as f32,
        },
        bytes,
    })
}

fn map_extract_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExtractJob> {
    Ok(ExtractJob {
        id: row.get(0)?,
        entry_id: row.get(1)?,
        kind: row.get(2)?,
        state: row.get(3)?,
        claimed: row.get::<_, i64>(4)? != 0,
        attempts: row.get(5)?,
        priority: row.get::<_, i64>(6).unwrap_or(0),
    })
}

/// The searchable text for an entry: its name is the primary column; the
/// keywords column carries path words and app keywords so token matches can
/// hit them at lower rank.
fn fts_keywords(rec: &EntryRecord) -> String {
    let mut kw = String::with_capacity(rec.path.len() + rec.keywords.len());
    for word in rec.path.split(['\\', '/', '.', '-', '_']) {
        let word = word.trim();
        if !word.is_empty() && !word.eq_ignore_ascii_case(&rec.name) {
            kw.push_str(word);
            kw.push(' ');
        }
    }
    kw.push_str(&rec.keywords);
    kw.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_entry_queries_recheck_live_source_policy_and_invalidated_content() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };
        let (mut store, job, progress) = batch_fixture();
        let chunk = content_chunk(&job, 0, "freshneedle generated fixture");
        assert!(store
            .commit_content_batch(&job, &progress, &[chunk], None)
            .unwrap());
        let hash = "a".repeat(64);
        store.conn.execute("UPDATE content_progress SET source_sha256=?2,source_sha256_revision=revision WHERE entry_id=?1",
            params![job.entry_id, hash]).unwrap();
        let admitted = Arc::new(AtomicBool::new(true));
        let current = Arc::clone(&admitted);
        store
            .set_query_source_guard(move |_, _, _| current.load(Ordering::Acquire))
            .unwrap();
        let tokens = vec!["freshneedle".into()];
        for allowed in [true, false, true] {
            admitted.store(allowed, Ordering::Release);
            for _ in 0..2 {
                assert_eq!(store.entry_visible(job.entry_id).unwrap(), allowed);
                assert_eq!(store.entry_content_allowed(job.entry_id).unwrap(), allowed);
                assert_eq!(
                    store.content_sha256(job.entry_id).unwrap(),
                    allowed.then_some(hash.clone())
                );
                assert_eq!(
                    store.content_matches(job.entry_id, &tokens).unwrap().len(),
                    usize::from(allowed)
                );
            }
        }
        store.invalidate_content_source(job.entry_id).unwrap();
        assert!(store.entry_visible(job.entry_id).unwrap());
        assert!(!store.entry_content_allowed(job.entry_id).unwrap());
        assert!(store.content_sha256(job.entry_id).unwrap().is_none());
        assert!(store
            .content_matches(job.entry_id, &tokens)
            .unwrap()
            .is_empty());
        store
            .set_indexing_config(&ws_protocol::IndexingConfig::default())
            .unwrap();
        assert!(
            !store.entry_visible(job.entry_id).unwrap(),
            "cached SQL must honor root revocation"
        );
    }

    #[test]
    fn extraction_claims_check_candidate_visibility_without_materializing_the_index() {
        use rusqlite::functions::FunctionFlags;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let mut counts = Vec::new();
        for mode in 0..3 {
            let mut store = IndexStore::open_in_memory().unwrap();
            let root = store.ensure_root("C:/claims").unwrap();
            let wanted = "C:/claims/allowed.txt";
            let denied = "C:/claims/denied.txt";
            let mut rows = (0..1100)
                .map(|n| {
                    rec(
                        &format!("C:/claims/metadata-{n:04}.bin"),
                        &format!("metadata-{n:04}.bin"),
                        1,
                        1,
                    )
                })
                .collect::<Vec<_>>();
            rows.push(rec(wanted, "allowed.txt", 1, 1));
            rows.push(rec(denied, "denied.txt", 1, 1));
            store.apply_batch(root, 1, &rows).unwrap();
            store
                .set_indexing_config(&ws_protocol::IndexingConfig {
                    roots: vec!["C:/claims".into()],
                    excluded_paths: vec![denied.into()],
                    ..Default::default()
                })
                .unwrap();
            let allowed = store.find_by_path(wanted).unwrap().unwrap().id;
            let rejected = store.find_by_path(denied).unwrap().unwrap().id;
            let kind = if mode == 2 { "doc" } else { "name" };
            for row in &rows[..1100] {
                let entry = store.find_by_path(&row.path).unwrap().unwrap().id;
                store
                    .enqueue_extract_job_with_priority(entry, kind, 0)
                    .unwrap();
            }
            store
                .enqueue_extract_job_with_priority(allowed, kind, 10)
                .unwrap();
            store
                .enqueue_extract_job_with_priority(rejected, kind, 100)
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            store
                .conn
                .create_scalar_function(
                    "policy_fold",
                    1,
                    FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                    move |ctx| {
                        observed.fetch_add(1, Ordering::Relaxed);
                        Ok(ctx
                            .get::<String>(0)?
                            .replace('/', "\\")
                            .trim_end_matches('\\')
                            .to_lowercase())
                    },
                )
                .unwrap();
            let job = match mode {
                0 => store.claim_extract_job().unwrap().unwrap(),
                1 => store
                    .claim_extract_jobs_batch_for_kind(kind, 1)
                    .unwrap()
                    .remove(0),
                _ => store.claim_content_extract_job().unwrap().unwrap(),
            };
            assert_eq!(
                job.entry_id, allowed,
                "excluded high-priority jobs must not consume the claim limit"
            );
            counts.push(calls.load(Ordering::Relaxed));
        }
        eprintln!("CLAIM_POLICY_FOLDS {counts:?}");
        assert!(
            counts.iter().all(|calls| *calls < 100),
            "one claim must check only queued candidates: {counts:?}"
        );
    }

    #[test]
    fn database_identity_survives_reopen_and_changes_for_a_recreated_index() {
        let path = std::env::temp_dir().join(format!(
            "winsearch-index-identity-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let first = IndexStore::open(&path).unwrap();
        let identity = first
            .meta_get("database_id")
            .unwrap()
            .expect("durable index identity");
        assert_eq!(identity.len(), 32);
        assert!(identity
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        drop(first);
        let reopened = IndexStore::open(&path).unwrap();
        assert_eq!(
            reopened.meta_get("database_id").unwrap().as_ref(),
            Some(&identity)
        );
        assert_ne!(
            IndexStore::open_in_memory()
                .unwrap()
                .meta_get("database_id")
                .unwrap()
                .as_ref(),
            Some(&identity)
        );
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        let recreated = IndexStore::open(&path).unwrap();
        assert_ne!(
            recreated.meta_get("database_id").unwrap().as_ref(),
            Some(&identity)
        );
        drop(recreated);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ordinary_ids_do_not_alias_deleted_entries_and_exhaustion_rolls_back() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:/identity").unwrap();
        let first = rec("C:/identity/first.txt", "first.txt", 1, 1);
        store.apply_batch(root, 1, &[first]).unwrap();
        let old = store
            .find_by_path("C:/identity/first.txt")
            .unwrap()
            .unwrap()
            .id;
        store.sweep_generation(root, 2).unwrap();
        let second = rec("C:/identity/second.txt", "second.txt", 1, 1);
        store.apply_batch(root, 2, &[second]).unwrap();
        assert!(
            store
                .find_by_path("C:/identity/second.txt")
                .unwrap()
                .unwrap()
                .id
                > old
        );
        store
            .meta_set("entry_id_high_water", &i64::MAX.to_string())
            .unwrap();
        let overflow = rec("C:/identity/overflow.txt", "overflow.txt", 1, 1);
        assert!(store.apply_batch(root, 3, &[overflow]).is_err());
        assert!(store
            .find_by_path("C:/identity/overflow.txt")
            .unwrap()
            .is_none());
    }

    #[test]
    fn database_identity_corruption_is_rejected_without_rotating_existing_bindings() {
        let store = IndexStore::open_in_memory().unwrap();
        store
            .conn
            .execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('database_id','invalid')",
                [],
            )
            .unwrap();
        assert!(IndexStore::init(store.conn).is_err());
    }
    fn batch_fixture() -> (IndexStore, ExtractJob, crate::model::ContentProgress) {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root(r"C:\task2").unwrap();
        store
            .apply_batch(
                root,
                0,
                &[rec(r"C:\task2\fixture.txt", "fixture.txt", 1, 1)],
            )
            .unwrap();
        let id = store
            .find_by_path(r"C:\task2\fixture.txt")
            .unwrap()
            .unwrap()
            .id;
        store.enqueue_extract_job(id, "doc").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(id, "doc", "fixture-source")
            .unwrap();
        (store, job, progress)
    }
    fn content_chunk(job: &ExtractJob, index: i64, text: &str) -> crate::model::ContentChunk {
        crate::model::ContentChunk {
            id: 0,
            entry_id: job.entry_id,
            job_kind: job.kind.clone(),
            kind: job.kind.clone(),
            chunk_index: index,
            text: text.into(),
            location: Default::default(),
            embed: true,
            visual: false,
            embedding_input: Default::default(),
            attempts: 0,
        }
    }

    #[test]
    fn preview_text_bounds_utf8_and_rows_without_loading_vectors_or_artifact_inputs() {
        let (mut store, job, progress) = batch_fixture();
        let text = format!("ab{}", "🌿".repeat(18000));
        let chunk = content_chunk(&job, 0, &text);
        assert!(store
            .commit_content_batch(&job, &progress, &[chunk], None)
            .unwrap());
        store.conn.execute("UPDATE content_chunks SET embedding_input='not valid JSON',thumbnail_path='C:/must-not-open'",[]).unwrap();
        let (parts, truncated) = store.preview_text(job.entry_id, None).unwrap();
        assert!(truncated);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].text.len(), 65534);
        assert_eq!(parts[0].text, format!("ab{}", "🌿".repeat(16383)));

        let (mut store, job, progress) = batch_fixture();
        let chunks = (0..65)
            .map(|index| content_chunk(&job, index, "row"))
            .collect::<Vec<_>>();
        assert!(store
            .commit_content_batch(&job, &progress, &chunks, None)
            .unwrap());
        let (parts, truncated) = store.preview_text(job.entry_id, None).unwrap();
        assert_eq!(parts.len(), 64);
        assert!(
            truncated,
            "the bounded extra row must expose omitted content"
        );
    }

    #[test]
    fn preview_text_applies_current_source_guard_before_row_limit() {
        let (mut store, job, progress) = batch_fixture();
        let chunks = (0..65)
            .map(|index| {
                content_chunk(
                    &job,
                    index,
                    if index == 64 {
                        "admitted tail"
                    } else {
                        "stale private text"
                    },
                )
            })
            .collect::<Vec<_>>();
        assert!(store
            .commit_content_batch(&job, &progress, &chunks, None)
            .unwrap());
        store.conn.execute("INSERT INTO content_progress(entry_id,kind,cursor,complete,revision,source,model_generation) VALUES(?1,'stale',64,1,?2,'stale-source',?3)",params![job.entry_id,progress.revision,progress.model_generation]).unwrap();
        store
            .conn
            .execute(
                "UPDATE content_chunks SET job_kind='stale' WHERE chunk_index<64",
                [],
            )
            .unwrap();
        store
            .set_query_source_guard(|_, source, _| {
                source.is_none_or(|source| source == "fixture-source")
            })
            .unwrap();
        let (parts, truncated) = store.preview_text(job.entry_id, None).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].text, "admitted tail");
        assert!(!truncated);
        store.invalidate_content_source(job.entry_id).unwrap();
        assert!(store.preview_text(job.entry_id, None).unwrap().0.is_empty());
    }

    #[test]
    fn preview_text_trailing_terminator_does_not_cover_the_following_line() {
        let (mut store, job, progress) = batch_fixture();
        let mut lines = content_chunk(&job, 0, "first\nsecond\n");
        lines.location.line = Some(20);
        assert!(store
            .commit_content_batch(&job, &progress, &[lines], None)
            .unwrap());
        assert_eq!(
            store
                .preview_text(
                    job.entry_id,
                    Some(ws_protocol::MatchLocation::Line { line: 21 })
                )
                .unwrap()
                .0
                .len(),
            1
        );
        assert!(
            store
                .preview_text(
                    job.entry_id,
                    Some(ws_protocol::MatchLocation::Line { line: 22 })
                )
                .unwrap()
                .0
                .is_empty(),
            "a trailing terminator does not return the following line"
        );
    }

    #[test]
    fn preview_text_preserves_page_line_and_time_without_fabricating_requested_positions() {
        use ws_protocol::MatchLocation;
        let (mut store, job, progress) = batch_fixture();
        let mut page = content_chunk(&job, 0, "actual page eight");
        page.location.page = Some(8);
        let mut lines = content_chunk(&job, 1, "first\nsecond\nthird");
        lines.location.line = Some(20);
        let mut timed = content_chunk(&job, 2, "screen words");
        timed.location.start_ms = Some(50250);
        timed.location.end_ms = Some(50250);
        assert!(store
            .commit_content_batch(&job, &progress, &[page, lines, timed], None)
            .unwrap());
        for (focus, text, actual) in [
            (
                MatchLocation::Page { page: 8 },
                "actual page eight",
                MatchLocation::Page { page: 8 },
            ),
            (
                MatchLocation::Line { line: 21 },
                "first\nsecond\nthird",
                MatchLocation::Line { line: 20 },
            ),
            (
                MatchLocation::Timestamp {
                    seconds: 50.25,
                    end_seconds: 50.25,
                },
                "screen words",
                MatchLocation::Timestamp {
                    seconds: 50.25,
                    end_seconds: 50.25,
                },
            ),
        ] {
            let (parts, truncated) = store.preview_text(job.entry_id, Some(focus)).unwrap();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].text, text);
            assert_eq!(parts[0].location, Some(actual));
            assert!(!truncated);
        }
        for focus in [
            MatchLocation::Page { page: 9 },
            MatchLocation::Line { line: 23 },
            MatchLocation::Timestamp {
                seconds: 50.251,
                end_seconds: 50.251,
            },
            MatchLocation::Timestamp {
                seconds: 50.2501,
                end_seconds: 50.2501,
            },
        ] {
            assert!(store
                .preview_text(job.entry_id, Some(focus))
                .unwrap()
                .0
                .is_empty());
        }
    }

    #[test]
    fn media_split_frame_restore_repairs_continued_ocr_thumbnail_only_at_same_time() {
        use crate::model::{DraftEmbeddingInput, MediaPlan};
        let (mut store, old, _) = batch_fixture();
        store.complete_extract_job(old.id).unwrap();
        store.enqueue_extract_job(old.entry_id, "video").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "video", "same-video")
            .unwrap();
        let plan = MediaPlan {
            duration_ms: 10000,
            frames_ms: vec![0, 5000],
            has_audio: false,
        };
        let mut frame = content_chunk(&job, 0, "");
        frame.kind = "video".into();
        frame.visual = true;
        frame.location.start_ms = Some(0);
        frame.location.end_ms = Some(0);
        frame.location.thumbnail_path = Some("C:/private/frame-first.png".into());
        frame.embedding_input = DraftEmbeddingInput::Image {
            path: "C:/private/frame-first.png".into(),
            token_budget: 140,
        };
        assert!(store
            .commit_content_batch_with_media(
                &job,
                &progress,
                &[frame.clone()],
                Some(1),
                Some(&plan)
            )
            .unwrap());
        let resumed = store.claim_content_extract_job().unwrap().unwrap();
        let continued = store
            .content_progress(job.entry_id, "video")
            .unwrap()
            .unwrap();
        let mut ocr = content_chunk(&job, 1, "continued screen words");
        ocr.kind = "ocr".into();
        ocr.embed = false;
        ocr.location.start_ms = Some(0);
        ocr.location.end_ms = Some(0);
        ocr.location.thumbnail_path = Some("C:/private/frame-continuation.png".into());
        let mut later = frame.clone();
        later.chunk_index = 65536;
        later.location.start_ms = Some(5000);
        later.location.end_ms = Some(5000);
        later.location.thumbnail_path = Some("C:/private/later-frame.png".into());
        later.embedding_input = DraftEmbeddingInput::Image {
            path: "C:/private/later-frame.png".into(),
            token_budget: 140,
        };
        assert!(store
            .commit_content_batch_with_media(&resumed, &continued, &[ocr, later], None, Some(&plan))
            .unwrap());
        let claimed = store.claim_content_vectors(Some(job.entry_id)).unwrap();
        assert_eq!(claimed.len(), 2);
        let input = DraftEmbeddingInput::Image {
            path: "C:/private/restored.png".into(),
            token_budget: 140,
        };
        assert!(store
            .replace_content_artifact(
                &claimed[0],
                &input,
                progress.revision,
                progress.model_generation
            )
            .unwrap());
        let chunks = store.list_content_chunks(job.entry_id).unwrap();
        assert_eq!(
            chunks[0].location.thumbnail_path.as_deref(),
            Some("C:/private/restored.png")
        );
        assert_eq!(chunks[1].text, "continued screen words");
        assert_eq!(
            chunks[1].location.thumbnail_path.as_deref(),
            Some("C:/private/restored.png")
        );
        assert_eq!(
            chunks[2].location.thumbnail_path.as_deref(),
            Some("C:/private/later-frame.png")
        );
        assert!(!store
            .replace_content_artifact(
                &claimed[0],
                &input,
                progress.revision,
                progress.model_generation
            )
            .unwrap());
    }

    #[test]
    fn media_video_replacement_and_failure_clear_every_derived_kind() {
        use crate::model::DraftEmbeddingInput;
        let (mut store, old, _) = batch_fixture();
        store.complete_extract_job(old.id).unwrap();
        store.enqueue_extract_job(old.entry_id, "video").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "video", "same-source")
            .unwrap();
        let mut frame = content_chunk(&job, 0, "");
        frame.kind = "video".into();
        frame.visual = true;
        frame.embedding_input = DraftEmbeddingInput::Image {
            path: "C:/private/frame.png".into(),
            token_budget: 70,
        };
        let mut ocr = content_chunk(&job, 1, "old screen text");
        ocr.kind = "ocr".into();
        let mut audio = content_chunk(&job, 1 << 62, "");
        audio.kind = "audio".into();
        audio.embedding_input = DraftEmbeddingInput::Audio {
            path: "C:/private/window.wav".into(),
        };
        assert!(store
            .commit_content_batch(&job, &progress, &[frame.clone(), ocr, audio], None)
            .unwrap());
        loop {
            let chunks = store.claim_content_vectors(Some(job.entry_id)).unwrap();
            if chunks.is_empty() {
                break;
            }
            let vectors = chunks.iter().map(|_| vec![1.0, 0.0]).collect::<Vec<_>>();
            assert!(store
                .commit_content_vectors(
                    &chunks,
                    &vectors,
                    progress.revision,
                    progress.model_generation
                )
                .unwrap());
        }
        assert_eq!(
            store.list_segments_for_entry(job.entry_id).unwrap().len(),
            3
        );
        let name = store
            .upsert_segment(NewSegment {
                entry_id: job.entry_id,
                kind: "name",
                chunk_index: 0,
                text: "filename",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0],
            })
            .unwrap();
        store.enqueue_extract_job(job.entry_id, "video").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let replacement = store
            .begin_content_batch(job.entry_id, "video", "same-source")
            .unwrap();
        assert!(store
            .commit_content_batch(&job, &replacement, &[frame], Some(65536))
            .unwrap());
        assert_eq!(
            store
                .list_segments_for_entry(job.entry_id)
                .unwrap()
                .iter()
                .map(|s| s.id)
                .collect::<Vec<_>>(),
            vec![name],
            "replace every prior derived kind while retaining the independent name vector"
        );
        let chunks = store.claim_content_vectors(Some(job.entry_id)).unwrap();
        assert!(store
            .commit_content_vectors(
                &chunks,
                &[vec![1.0, 0.0]],
                replacement.revision,
                replacement.model_generation
            )
            .unwrap());
        store.delete_content_text(job.entry_id).unwrap();
        assert_eq!(
            store
                .list_segments_for_entry(job.entry_id)
                .unwrap()
                .iter()
                .map(|s| s.id)
                .collect::<Vec<_>>(),
            vec![name],
            "failed content removes derived data and preserves name search"
        );
        assert!(store
            .content_progress(job.entry_id, "video")
            .unwrap()
            .is_none());
    }

    #[test]
    fn media_video_plan_and_frame_cursor_commit_together_and_reject_mutation() {
        let (mut store, old, _) = batch_fixture();
        store.complete_extract_job(old.id).unwrap();
        store.enqueue_extract_job(old.entry_id, "video").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "video", "same-video")
            .unwrap();
        let plan = crate::model::MediaPlan {
            duration_ms: 65000,
            frames_ms: vec![0, 33000, 64999],
            has_audio: true,
        };
        let chunk = content_chunk(&job, 0, "durable frame OCR");
        store.conn.execute_batch("CREATE TEMP TRIGGER abort_video BEFORE UPDATE ON content_progress WHEN NEW.cursor=65536 BEGIN SELECT RAISE(ABORT,'interrupted video'); END;").unwrap();
        assert!(store
            .commit_content_batch_with_media(
                &job,
                &progress,
                std::slice::from_ref(&chunk),
                Some(65536),
                Some(&plan)
            )
            .is_err());
        assert_eq!(
            store
                .content_progress(job.entry_id, "video")
                .unwrap()
                .unwrap(),
            progress
        );
        assert!(store.list_content_chunks(job.entry_id).unwrap().is_empty());
        store
            .conn
            .execute_batch("DROP TRIGGER abort_video")
            .unwrap();
        assert!(store
            .commit_content_batch_with_media(&job, &progress, &[chunk], Some(65536), Some(&plan))
            .unwrap());
        let saved = store
            .content_progress(job.entry_id, "video")
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.media_plan,
            Some(plan.clone()),
            "selection and visual cursor must survive the same commit"
        );
        assert!(!store
            .commit_content_batch_with_media(&job, &progress, &[], Some(65536), Some(&plan))
            .unwrap());
        let mut changed = plan.clone();
        changed.frames_ms[1] = 34000;
        assert!(store
            .commit_content_batch_with_media(&job, &saved, &[], Some(131072), Some(&changed))
            .is_err());
        assert_eq!(
            store
                .content_progress(job.entry_id, "video")
                .unwrap()
                .unwrap(),
            saved
        );
        store.reset_content_vectors().unwrap();
        assert_eq!(
            store
                .content_progress(job.entry_id, "video")
                .unwrap()
                .unwrap()
                .media_plan,
            Some(plan)
        );
        let replaced = store
            .begin_content_batch(job.entry_id, "video", "changed-video")
            .unwrap();
        assert_eq!(replaced.cursor, 0);
        assert_eq!(replaced.media_plan, None);
    }

    #[test]
    fn final_selection_with_short_video_and_long_audio_commits_tail() {
        use crate::model::{VideoSelection, VIDEO_GRAY_PIXELS};
        let (mut store, old, _) = batch_fixture();
        store.complete_extract_job(old.id).unwrap();
        store.enqueue_extract_job(old.entry_id, "video").unwrap();
        let mut state = VideoSelection {
            duration_ms: 65_000,
            scan_span_ms: crate::model::VIDEO_SELECTION_SPAN_MS,
            has_audio: true,
            next_ms: 15_000,
            last_sample_ms: Some(7_000),
            last_gray: vec![4; VIDEO_GRAY_PIXELS],
            candidates_ms: vec![0, 5_000],
            next_periodic_ms: 10_000,
            sample_count: 8,
        };
        for next_ms in [15_000, 30_000, 45_000, 60_000] {
            let job = store.claim_content_extract_job().unwrap().unwrap();
            let progress = store
                .begin_content_batch(job.entry_id, "video", "short-visual-source")
                .unwrap();
            state.next_ms = next_ms;
            assert!(store
                .commit_content_batch_with_video_selection(
                    &job,
                    &progress,
                    &[],
                    Some((1u64 << 61) | next_ms),
                    None,
                    Some(&state)
                )
                .unwrap());
        }
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "video", "short-visual-source")
            .unwrap();
        state.next_ms = 65_000;
        state.candidates_ms.push(7_000);
        let plan = crate::model::MediaPlan {
            duration_ms: 65_000,
            frames_ms: vec![0, 5_000, 7_000],
            has_audio: true,
        };
        assert!(store
            .commit_content_batch_with_video_selection(
                &job,
                &progress,
                &[],
                Some(0),
                Some(&plan),
                Some(&state)
            )
            .expect("completed scan can retain its known visual tail"));
        assert_eq!(
            store
                .content_progress(job.entry_id, "video")
                .unwrap()
                .unwrap()
                .media_plan,
            Some(plan)
        );
    }

    #[test]
    fn video_selection_checkpoint_cursor_and_claim_commit_atomically() {
        use crate::model::{VideoSelection, VIDEO_GRAY_PIXELS};
        let (mut store, old, _) = batch_fixture();
        store.complete_extract_job(old.id).unwrap();
        store.enqueue_extract_job(old.entry_id, "video").unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "video", "selection-source")
            .unwrap();
        let state = VideoSelection {
            duration_ms: 30_000,
            scan_span_ms: crate::model::VIDEO_SELECTION_SPAN_MS,
            has_audio: true,
            next_ms: 15_000,
            last_sample_ms: Some(14_000),
            last_gray: vec![4; VIDEO_GRAY_PIXELS],
            candidates_ms: vec![0, 5_000, 10_000],
            next_periodic_ms: 15_000,
            sample_count: 15,
        };
        let cursor = (1u64 << 61) | 15_000;
        store.conn.execute_batch(&format!("CREATE TEMP TRIGGER interrupt_selection BEFORE UPDATE ON content_progress WHEN NEW.cursor={cursor} BEGIN SELECT RAISE(ABORT,'interrupted selection'); END;")).unwrap();
        assert!(store
            .commit_content_batch_with_video_selection(
                &job,
                &progress,
                &[],
                Some(cursor),
                None,
                Some(&state)
            )
            .is_err());
        assert_eq!(
            store
                .content_progress(job.entry_id, "video")
                .unwrap()
                .unwrap(),
            progress
        );
        store
            .conn
            .execute_batch("DROP TRIGGER interrupt_selection")
            .unwrap();
        assert!(store
            .commit_content_batch_with_video_selection(
                &job,
                &progress,
                &[],
                Some(cursor),
                None,
                Some(&state)
            )
            .unwrap());
        let saved = store
            .content_progress(job.entry_id, "video")
            .unwrap()
            .unwrap();
        assert_eq!(saved.cursor, cursor);
        assert_eq!(saved.video_selection, Some(state.clone()));
        assert!(saved.media_plan.is_none());
        assert!(!store
            .commit_content_batch_with_video_selection(
                &job,
                &progress,
                &[],
                Some(cursor),
                None,
                Some(&state)
            )
            .unwrap());
        let resumed = store.claim_content_extract_job().unwrap().unwrap();
        let final_state = VideoSelection {
            next_ms: 30_000,
            last_sample_ms: Some(29_999),
            candidates_ms: vec![0, 5_000, 10_000, 29_999],
            next_periodic_ms: 30_000,
            sample_count: 16,
            ..state
        };
        let plan = crate::model::MediaPlan {
            duration_ms: 30_000,
            frames_ms: final_state.candidates_ms.clone(),
            has_audio: true,
        };
        assert!(store
            .commit_content_batch_with_video_selection(
                &resumed,
                &saved,
                &[],
                Some(0),
                Some(&plan),
                Some(&final_state)
            )
            .unwrap());
        let frozen = store
            .content_progress(job.entry_id, "video")
            .unwrap()
            .unwrap();
        assert_eq!(frozen.cursor, 0);
        assert_eq!(frozen.media_plan, Some(plan));
        assert!(frozen.video_selection.is_none());
    }

    #[test]
    fn media_typed_artifact_persists_independently_of_text_and_vector_reset() {
        use crate::model::DraftEmbeddingInput;
        let (mut store, job, progress) = batch_fixture();
        let mut chunk = content_chunk(&job, 0, "");
        chunk.kind = "audio".into();
        chunk.location.start_ms = Some(50000);
        chunk.location.end_ms = Some(65000);
        chunk.embedding_input = DraftEmbeddingInput::Audio {
            path: std::path::PathBuf::from("C:/private/cache/時刻 window.wav"),
        };
        assert!(store
            .commit_content_batch(&job, &progress, std::slice::from_ref(&chunk), None)
            .unwrap());
        assert_eq!(
            store.list_content_chunks(job.entry_id).unwrap()[0].embedding_input,
            chunk.embedding_input
        );
        store.reset_content_vectors().unwrap();
        let claimed = store.claim_content_vectors(Some(job.entry_id)).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].embedding_input, chunk.embedding_input);
        assert!(claimed[0].text.is_empty());
        assert_eq!(claimed[0].location.start_ms, Some(50000));
        let generation = store.model_generation().unwrap();
        let restored = DraftEmbeddingInput::Audio {
            path: std::path::PathBuf::from("C:/private/cache/restored.wav"),
        };
        assert!(!store
            .replace_content_artifact(&claimed[0], &restored, progress.revision + 1, generation)
            .unwrap());
        assert!(!store
            .replace_content_artifact(&claimed[0], &restored, progress.revision, generation + 1)
            .unwrap());
        assert!(store
            .replace_content_artifact(&claimed[0], &restored, progress.revision, generation)
            .unwrap());
        assert!(!store
            .replace_content_artifact(&claimed[0], &restored, progress.revision, generation)
            .unwrap());
        let after = store.list_content_chunks(job.entry_id).unwrap();
        assert_eq!(after[0].id, claimed[0].id);
        assert_eq!(after[0].embedding_input, restored);
        assert_eq!(after[0].location.start_ms, Some(50000));
        assert!(store
            .commit_content_vectors(&after, &[vec![0.0, 1.0]], progress.revision, generation)
            .unwrap());
        let segments = store.list_segments_for_entry(job.entry_id).unwrap();
        let completed_input = DraftEmbeddingInput::Audio {
            path: "C:/private/cache/completed.wav".into(),
        };
        assert!(!store
            .replace_content_artifact(
                &after[0],
                &completed_input,
                progress.revision + 1,
                generation
            )
            .unwrap());
        assert!(!store
            .replace_content_artifact(
                &after[0],
                &completed_input,
                progress.revision,
                generation + 1
            )
            .unwrap());
        assert!(store
            .replace_content_artifact(&after[0], &completed_input, progress.revision, generation)
            .unwrap());
        assert!(!store
            .replace_content_artifact(&after[0], &completed_input, progress.revision, generation)
            .unwrap());
        assert!(store
            .claim_content_vectors(Some(job.entry_id))
            .unwrap()
            .is_empty());
        let preserved = store.list_segments_for_entry(job.entry_id).unwrap();
        assert_eq!(
            preserved
                .iter()
                .map(|s| (s.id, &s.vector))
                .collect::<Vec<_>>(),
            segments
                .iter()
                .map(|s| (s.id, &s.vector))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn media_silent_progress_is_durable_without_content_and_rejects_replay() {
        let (mut store, old_job, _) = batch_fixture();
        store.complete_extract_job(old_job.id).unwrap();
        store
            .enqueue_extract_job(old_job.entry_id, "audio")
            .unwrap();
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .begin_content_batch(job.entry_id, "audio", "silent-source")
            .unwrap();
        assert!(store
            .commit_content_batch(&job, &progress, &[], Some(30))
            .unwrap());
        assert!(store.list_content_chunks(job.entry_id).unwrap().is_empty());
        let saved = store
            .content_progress(job.entry_id, "audio")
            .unwrap()
            .unwrap();
        assert_eq!(saved.cursor, 30);
        assert!(!saved.complete);
        assert!(!store
            .commit_content_batch(&job, &progress, &[], Some(30))
            .unwrap());
        let resumed = store.claim_content_extract_job().unwrap().unwrap();
        assert!(store
            .commit_content_batch(&resumed, &saved, &[], None)
            .unwrap());
        assert!(
            store
                .content_progress(job.entry_id, "audio")
                .unwrap()
                .unwrap()
                .complete
        );
        assert!(store
            .claim_content_vectors(Some(job.entry_id))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn ordinary_source_hash_commits_with_chunks_and_survives_only_current_source_and_model_reset() {
        let (mut store, job, progress) = batch_fixture();
        let hash = "a".repeat(64);
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap(),
            None,
            "legacy progress has no invented byte hash"
        );
        let chunk = content_chunk(&job, 0, "current bytes");
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: &[chunk],
                    next: Some(1),
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap().as_deref(),
            Some(hash.as_str())
        );
        store.reset_content_vectors().unwrap();
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap().as_deref(),
            Some(hash.as_str())
        );
        store.set_query_source_guard(|_, _, _| false).unwrap();
        assert_eq!(store.content_sha256(job.entry_id).unwrap(), None);
        store.set_query_source_guard(|_, _, _| true).unwrap();
        store.invalidate_content_source(job.entry_id).unwrap();
        assert_eq!(store.content_sha256(job.entry_id).unwrap(), None);
    }

    #[test]
    fn ordinary_source_hash_rejects_malformed_and_changed_or_missing_continuation_without_advancing(
    ) {
        for invalid in [
            String::new(),
            "a".repeat(63),
            "A".repeat(64),
            "g".repeat(64),
        ] {
            let (mut store, job, progress) = batch_fixture();
            assert!(store
                .commit_content_batch_with_source(
                    &job,
                    &progress,
                    ContentBatch {
                        chunks: &[content_chunk(&job, 0, "uncommitted")],
                        next: None,
                        media: None,
                        selection: None,
                        source_sha256: Some(&invalid)
                    }
                )
                .is_err());
            assert_eq!(
                store.content_progress(job.entry_id, "doc").unwrap(),
                Some(progress)
            );
            assert!(store.list_content_chunks(job.entry_id).unwrap().is_empty());
        }
        for next_hash in [Some("b".repeat(64)), None] {
            let (mut store, job, progress) = batch_fixture();
            let hash = "a".repeat(64);
            assert!(store
                .commit_content_batch_with_source(
                    &job,
                    &progress,
                    ContentBatch {
                        chunks: &[content_chunk(&job, 0, "first")],
                        next: Some(1),
                        media: None,
                        selection: None,
                        source_sha256: Some(&hash)
                    }
                )
                .unwrap());
            let next_job = store.claim_content_extract_job().unwrap().unwrap();
            let saved = store
                .content_progress(job.entry_id, "doc")
                .unwrap()
                .unwrap();
            assert!(!store
                .commit_content_batch_with_source(
                    &next_job,
                    &saved,
                    ContentBatch {
                        chunks: &[content_chunk(&job, 1, "different source")],
                        next: None,
                        media: None,
                        selection: None,
                        source_sha256: next_hash.as_deref()
                    }
                )
                .unwrap());
            assert_eq!(
                store.content_progress(job.entry_id, "doc").unwrap(),
                Some(saved)
            );
            assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 1);
            assert_eq!(
                store.content_sha256(job.entry_id).unwrap().as_deref(),
                Some(hash.as_str())
            );
        }
    }

    #[test]
    fn ordinary_source_hash_from_an_older_writer_cannot_bind_to_a_later_content_revision() {
        let (mut store, job, progress) = batch_fixture();
        let hash = "a".repeat(64);
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: &[content_chunk(&job, 0, "first")],
                    next: Some(1),
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        // An older binary changes only the columns it knows and leaves new columns untouched.
        store
            .conn
            .execute(
                "UPDATE content_revisions SET revision=revision+1 WHERE entry_id=?1",
                [job.entry_id],
            )
            .unwrap();
        store.conn.execute("UPDATE content_progress SET revision=revision+1,source='new source snapshot' WHERE entry_id=?1",[job.entry_id]).unwrap();
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap(),
            None,
            "a retained old digest is not provenance for the new revision"
        );
        assert_eq!(
            store
                .content_progress(job.entry_id, "doc")
                .unwrap()
                .unwrap()
                .source_sha256,
            None
        );
    }

    #[test]
    fn ordinary_source_hash_cannot_attest_earlier_chunks_of_a_resumed_legacy_parse() {
        let (mut store, job, progress) = batch_fixture();
        let hash = "a".repeat(64);
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: &[content_chunk(&job, 0, "legacy chunk")],
                    next: Some(1),
                    media: None,
                    selection: None,
                    source_sha256: None
                }
            )
            .unwrap());
        let resumed = store.claim_content_extract_job().unwrap().unwrap();
        let saved = store
            .content_progress(job.entry_id, "doc")
            .unwrap()
            .unwrap();
        assert!(store
            .commit_content_batch_with_source(
                &resumed,
                &saved,
                ContentBatch {
                    chunks: &[content_chunk(&job, 1, "newly hashed chunk")],
                    next: None,
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 2);
        assert_eq!(
            store
                .content_progress(job.entry_id, "doc")
                .unwrap()
                .unwrap()
                .source_sha256,
            None,
            "new continuation bytes cannot attest the earlier legacy chunks"
        );
        assert_eq!(store.content_sha256(job.entry_id).unwrap(), None);
        store.enqueue_extract_job(job.entry_id, "doc").unwrap();
        let fresh_job = store.claim_content_extract_job().unwrap().unwrap();
        let fresh = store
            .begin_content_batch(job.entry_id, "doc", &progress.source)
            .unwrap();
        assert!(fresh.revision > saved.revision);
        assert!(store
            .commit_content_batch_with_source(
                &fresh_job,
                &fresh,
                ContentBatch {
                    chunks: &[content_chunk(&job, 0, "fresh full parse")],
                    next: None,
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap().as_deref(),
            Some(hash.as_str())
        );
        assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 1);
    }

    #[test]
    fn ordinary_source_hash_and_cursor_roll_back_with_chunk_transaction() {
        let (mut store, job, progress) = batch_fixture();
        let hash = "a".repeat(64);
        store.conn.execute_batch("CREATE TRIGGER fail_source_hash_fixture BEFORE UPDATE ON content_progress BEGIN SELECT RAISE(ABORT,'source hash fixture failure'); END;").unwrap();
        let chunk = content_chunk(&job, 0, "must roll back");
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: std::slice::from_ref(&chunk),
                    next: Some(1),
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .is_err());
        assert_eq!(
            store.content_progress(job.entry_id, "doc").unwrap(),
            Some(progress.clone())
        );
        assert_eq!(store.content_sha256(job.entry_id).unwrap(), None);
        assert!(store.list_content_chunks(job.entry_id).unwrap().is_empty());
        store
            .conn
            .execute_batch("DROP TRIGGER fail_source_hash_fixture")
            .unwrap();
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: &[chunk],
                    next: None,
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        assert_eq!(
            store.content_sha256(job.entry_id).unwrap().as_deref(),
            Some(hash.as_str())
        );
    }

    #[test]
    fn ordinary_source_hash_restart_and_legacy_migration_preserve_existing_content_and_jobs() {
        let (mut store, job, progress) = batch_fixture();
        let hash = "a".repeat(64);
        assert!(store
            .commit_content_batch_with_source(
                &job,
                &progress,
                ContentBatch {
                    chunks: &[content_chunk(&job, 0, "preservedprovenanceneedle")],
                    next: Some(1),
                    media: None,
                    selection: None,
                    source_sha256: Some(&hash)
                }
            )
            .unwrap());
        let id = store.list_content_chunks(job.entry_id).unwrap()[0].id;
        let reopened = IndexStore::init(store.conn).unwrap();
        assert_eq!(
            reopened.content_sha256(job.entry_id).unwrap().as_deref(),
            Some(hash.as_str())
        );
        assert_eq!(
            reopened.list_content_chunks(job.entry_id).unwrap()[0].id,
            id
        );
        reopened
            .conn
            .execute_batch("ALTER TABLE content_progress DROP COLUMN source_sha256")
            .unwrap();
        let mut legacy = IndexStore::init(reopened.conn).unwrap();
        assert_eq!(
            legacy.content_sha256(job.entry_id).unwrap(),
            None,
            "old databases never invent a digest"
        );
        let saved = legacy
            .content_progress(job.entry_id, "doc")
            .unwrap()
            .unwrap();
        assert_eq!(
            (saved.cursor, saved.revision, saved.source),
            (1, progress.revision, progress.source)
        );
        assert_eq!(legacy.list_content_chunks(job.entry_id).unwrap()[0].id, id);
        assert_eq!(
            legacy
                .search(&crate::model::SearchRequest {
                    text: "preservedprovenanceneedle".into(),
                    limit: 5,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            legacy
                .claim_content_extract_job()
                .unwrap()
                .unwrap()
                .entry_id,
            job.entry_id
        );
    }

    #[test]
    fn task2_content_cursor_and_text_roll_back_together_and_reject_replay() {
        let (mut store, job, progress) = batch_fixture();
        let first = (0..30)
            .map(|i| content_chunk(&job, i, "firstbatchneedle"))
            .collect::<Vec<_>>();
        assert!(store
            .commit_content_batch(&job, &progress, &first, Some(30))
            .unwrap());
        assert!(!store
            .commit_content_batch(&job, &progress, &first, Some(30))
            .unwrap());
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .content_progress(job.entry_id, "doc")
            .unwrap()
            .unwrap();
        let second = (30..60)
            .map(|i| content_chunk(&job, i, "secondbatchneedle"))
            .collect::<Vec<_>>();
        store.conn.execute_batch("CREATE TEMP TRIGGER interrupt_batch BEFORE UPDATE ON content_progress WHEN NEW.cursor=60 BEGIN SELECT RAISE(ABORT,'interrupted commit'); END;").unwrap();
        assert!(store
            .commit_content_batch(&job, &progress, &second, Some(60))
            .is_err());
        assert_eq!(
            store
                .content_progress(job.entry_id, "doc")
                .unwrap()
                .unwrap()
                .cursor,
            30
        );
        assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 30);
        assert!(store.get_extract_job(job.id).unwrap().unwrap().claimed);
        assert!(store
            .search(&crate::model::SearchRequest {
                text: "secondbatchneedle".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        store
            .conn
            .execute_batch("DROP TRIGGER interrupt_batch")
            .unwrap();
        assert!(store
            .commit_content_batch(&job, &progress, &second, Some(60))
            .unwrap());
        assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 60);
        let job = store.claim_content_extract_job().unwrap().unwrap();
        let progress = store
            .content_progress(job.entry_id, "doc")
            .unwrap()
            .unwrap();
        store.invalidate_content_source(job.entry_id).unwrap();
        assert!(!store
            .commit_content_batch(
                &job,
                &progress,
                &[content_chunk(&job, 60, "staleneedle")],
                None
            )
            .unwrap());
        assert!(store.list_content_chunks(job.entry_id).unwrap().is_empty());
    }

    #[test]
    fn task2_exact_locations_multiple_moments_numeric_and_policy_before_cap() {
        let (mut store, job, progress) = batch_fixture();
        let mut chunks = vec![
            content_chunk(&job, 0, "firstpagetoken repeatedmoment"),
            content_chunk(&job, 1, "secondpagetoken repeatedmoment"),
            content_chunk(&job, 2, "timetoken 98017"),
        ];
        chunks[0].location.page = Some(2);
        chunks[1].location.page = Some(9);
        chunks[2].location.start_ms = Some(42_000);
        chunks[2].location.end_ms = Some(48_000);
        for c in &mut chunks {
            c.embed = false;
        }
        store
            .commit_content_batch(&job, &progress, &chunks, None)
            .unwrap();
        for (query, page) in [("firstpagetoken", Some(2)), ("secondpagetoken", Some(9))] {
            let hits = store
                .search(&crate::model::SearchRequest {
                    text: query.into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(hits[0].matches.len(), 1);
            assert_eq!(hits[0].matches[0].page, page);
            assert!(hits[0].matches[0].excerpt.contains(query));
        }
        let hits = store
            .search(&crate::model::SearchRequest {
                text: "repeatedmoment".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits[0].matches.len(), 2);
        let first = store
            .content_matches_page(job.entry_id, &["repeatedmoment".into()], 0, 1)
            .unwrap();
        let second = store
            .content_matches_page(job.entry_id, &["repeatedmoment".into()], 1, 1)
            .unwrap();
        assert_eq!(first[0].page, Some(2));
        assert_eq!(second[0].page, Some(9));
        let hit = store
            .search(&crate::model::SearchRequest {
                text: "98017".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap()
            .remove(0);
        assert_eq!(hit.matches[0].start_ms, Some(42_000));
        assert!(store.claim_content_vectors(None).unwrap().is_empty());
        let root = store.roots().unwrap()[0].id;
        for i in 0..510 {
            let name = format!("hidden{i}.txt");
            let path = format!(r"C:\task2\hidden\{name}");
            store
                .apply_batch(root, 0, &[rec(&path, &name, 1, 1)])
                .unwrap();
            let id = store.find_by_path(&path).unwrap().unwrap().id;
            store.replace_content_fts(id, "doc", &["98017"]).unwrap();
        }
        let config = ws_protocol::IndexingConfig {
            roots: vec![r"C:\task2".into()],
            excluded_paths: vec![r"C:\task2\hidden".into()],
            ..Default::default()
        };
        store.set_indexing_config(&config).unwrap();
        let hits = store
            .search(&crate::model::SearchRequest {
                text: "98017".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_id, job.entry_id);
        let config = ws_protocol::IndexingConfig {
            roots: vec![],
            ..Default::default()
        };
        store.set_indexing_config(&config).unwrap();
        assert!(store
            .search(&crate::model::SearchRequest {
                text: "98017".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        assert!(store
            .content_matches(job.entry_id, &["98017".into()])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn task2_migrates_legacy_numeric_fts_without_vector_rows() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch("INSERT INTO meta VALUES('schema_version','2'); INSERT INTO roots(id,path) VALUES(1,'C:\\legacy'); INSERT INTO entries(id,root_id,path,name,name_lower,ext) VALUES(7,1,'C:\\legacy\\invoice.csv','invoice.csv','invoice.csv','csv'); INSERT INTO content_fts(entry_id,kind,text) VALUES(7,'doc','98017 2026 10 42');").unwrap();
        let store = IndexStore::init(conn).unwrap();
        assert_eq!(store.get_entry(7).unwrap().unwrap().name, "invoice.csv");
        assert!(store.list_segments_for_entry(7).unwrap().is_empty());
        let hits = store
            .search(&crate::model::SearchRequest {
                text: "98017".into(),
                limit: 5,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].matches[0].excerpt.contains("98017"));
        assert!(!store.list_content_chunks(7).unwrap()[0].embed);
        // The migration marker makes reopen idempotent, preserving row identity.
        let id = store.list_content_chunks(7).unwrap()[0].id;
        let store = IndexStore::init(store.conn).unwrap();
        assert_eq!(store.list_content_chunks(7).unwrap()[0].id, id);
    }

    #[test]
    fn task2_deferred_vector_failures_never_remove_exact_and_space_change_rejects_inflight() {
        let (mut store, job, progress) = batch_fixture();
        store
            .commit_content_batch(
                &job,
                &progress,
                &[content_chunk(&job, 0, "durableexactneedle")],
                None,
            )
            .unwrap();
        for _ in 0..3 {
            let chunks = store.claim_content_vectors(None).unwrap();
            assert_eq!(chunks.len(), 1);
            assert!(store
                .commit_content_vectors(
                    &chunks,
                    &[vec![]],
                    progress.revision,
                    progress.model_generation
                )
                .is_err());
            assert!(store
                .commit_content_vectors(
                    &chunks,
                    &[vec![0., 0.]],
                    progress.revision,
                    progress.model_generation
                )
                .is_err());
            store.release_content_vectors(&chunks, true).unwrap();
        }
        assert!(store.claim_content_vectors(None).unwrap().is_empty());
        assert_eq!(store.list_content_chunks(job.entry_id).unwrap().len(), 1);
        assert!(store
            .list_segments_for_entry(job.entry_id)
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .search(&crate::model::SearchRequest {
                    text: "durableexactneedle".into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        store.reset_content_vectors().unwrap();
        let chunks = store.claim_content_vectors(None).unwrap();
        let generation = store.model_generation().unwrap();
        store.reset_content_vectors().unwrap();
        assert!(!store
            .commit_content_vectors(&chunks, &[vec![1., 0.]], progress.revision, generation)
            .unwrap());
        let chunks = store.claim_content_vectors(None).unwrap();
        assert!(store
            .commit_content_vectors(
                &chunks,
                &[vec![1., 0.]],
                progress.revision,
                store.model_generation().unwrap()
            )
            .unwrap());
        assert_eq!(
            store.list_segments_for_entry(job.entry_id).unwrap().len(),
            1
        );
    }
    #[test]
    fn active_policy_sparse_search_does_not_classify_unmatched_index_rows() {
        use rusqlite::functions::FunctionFlags;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root(r"C:\scale").unwrap();
        let mut config = ws_protocol::IndexingConfig {
            roots: vec![r"C:\scale".into()],
            ..Default::default()
        };
        store.set_indexing_config(&config).unwrap();
        let records: Vec<_> = (0..50_000)
            .map(|i| {
                let name = if i == 49_999 {
                    "singleneedle.txt".to_string()
                } else {
                    format!("ordinary{i}.txt")
                };
                rec(&format!(r"C:\scale\{name}"), &name, 1, 1)
            })
            .collect();
        store.apply_batch(root, 0, &records).unwrap();
        let content_entry = store
            .find_by_path(r"C:\scale\ordinary0.txt")
            .unwrap()
            .unwrap()
            .id;
        store
            .replace_content_fts(
                content_entry,
                crate::model::SEGMENT_KIND_DOC,
                &["contentsingleneedle"],
            )
            .unwrap();
        store
            .upsert_segment(NewSegment {
                entry_id: content_entry,
                kind: crate::model::SEGMENT_KIND_NAME,
                chunk_index: 0,
                text: "ordinary0",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[0., 1.],
            })
            .unwrap();
        let hidden = store
            .find_by_path(r"C:\scale\ordinary1.txt")
            .unwrap()
            .unwrap()
            .id;
        store
            .upsert_segment(NewSegment {
                entry_id: hidden,
                kind: crate::model::SEGMENT_KIND_NAME,
                chunk_index: 0,
                text: "ordinary1",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1., 0.],
            })
            .unwrap();
        config.excluded_paths.push(r"C:\scale\ordinary1.txt".into());
        store.set_indexing_config(&config).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        store
            .conn
            .create_scalar_function(
                "policy_fold",
                1,
                FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                move |ctx| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    Ok(ctx
                        .get::<String>(0)?
                        .replace('/', "\\")
                        .trim_end_matches('\\')
                        .to_lowercase())
                },
            )
            .unwrap();
        for (query, expected_hits) in [
            ("singleneedle", 1),
            ("contentsingleneedle", 1),
            ("zzz-no-match", 0),
        ] {
            calls.store(0, Ordering::Relaxed);
            let hits = store
                .search(&crate::model::SearchRequest {
                    text: query.into(),
                    limit: 20,
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(hits.len(), expected_hits);
            // Count actual policy evaluations, independent of host speed: unmatched
            // rows must not be materialized by any production candidate source.
            assert!(
                calls.load(Ordering::Relaxed) < 1000,
                "{query} policy folds: {}",
                calls.load(Ordering::Relaxed)
            );
        }
        calls.store(0, Ordering::Relaxed);
        let vectors = store.search_vectors(&[1., 0.], 1, None).unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].segment.entry_id, content_entry);
        assert!(
            calls.load(Ordering::Relaxed) < 1000,
            "vector search policy folds: {}",
            calls.load(Ordering::Relaxed)
        );
        calls.store(0, Ordering::Relaxed);
        assert_eq!(store.all_segments().unwrap().len(), 1);
        assert!(
            calls.load(Ordering::Relaxed) < 1000,
            "segment load policy folds: {}",
            calls.load(Ordering::Relaxed)
        );
        calls.store(0, Ordering::Relaxed);
        assert_eq!(store.all_segment_vectors_raw().unwrap().len(), 1);
        assert!(
            calls.load(Ordering::Relaxed) < 1000,
            "raw vector load policy folds: {}",
            calls.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn policy_visibility_uses_unicode_case_and_component_boundaries() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let id = store.ensure_root(r"C:\DÄTA").unwrap();
        store
            .apply_batch(
                id,
                0,
                &[
                    rec(r"C:\DÄTA\note.txt", "note.txt", 1, 1),
                    rec(r"C:\DÄTABASE\leak.txt", "leak.txt", 1, 1),
                ],
            )
            .unwrap();
        let c = ws_protocol::IndexingConfig {
            roots: vec![r"C:\DÄTA".into()],
            excluded_paths: vec![r"c:\däta\note.txt".into()],
            ..Default::default()
        };
        store.set_indexing_config(&c).unwrap();
        assert!(!store
            .entry_visible(store.find_by_path(r"C:\DÄTA\note.txt").unwrap().unwrap().id)
            .unwrap());
        assert!(!store
            .entry_visible(
                store
                    .find_by_path(r"C:\DÄTABASE\leak.txt")
                    .unwrap()
                    .unwrap()
                    .id
            )
            .unwrap());
    }
    #[test]
    fn indexing_policy_migrates_once_and_fresh_is_empty() {
        let mut fresh = IndexStore::open_in_memory().unwrap();
        assert!(fresh
            .get_or_migrate_indexing_config()
            .unwrap()
            .roots
            .is_empty());
        let mut legacy = IndexStore::open_in_memory().unwrap();
        let id = legacy.ensure_root(r"Z:\offline").unwrap();
        legacy.ensure_root("@apps").unwrap();
        let c = legacy.get_or_migrate_indexing_config().unwrap();
        assert_eq!(c.roots, vec![r"Z:\offline"]);
        legacy.ensure_root(r"D:\later").unwrap();
        assert_eq!(legacy.get_or_migrate_indexing_config().unwrap(), c);
        assert_eq!(legacy.roots().unwrap()[0].id, id);
    }
    #[test]
    fn indexing_policy_write_failure_keeps_policy_and_visibility() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let c = ws_protocol::IndexingConfig {
            roots: vec![r"C:\A".into()],
            ..Default::default()
        };
        store.set_indexing_config(&c).unwrap();
        let before = store.roots().unwrap();
        store.conn.execute_batch("CREATE TRIGGER fail_policy BEFORE UPDATE ON meta WHEN new.key='indexing_config' BEGIN SELECT RAISE(ABORT,'fixture write failure'); END;").unwrap();
        let mut next = c.clone();
        next.roots = vec![r"C:\B".into()];
        assert!(store.set_indexing_config(&next).is_err());
        assert_eq!(store.get_or_migrate_indexing_config().unwrap(), c);
        assert_eq!(store.roots().unwrap(), before);
    }
    #[test]
    fn policy_filter_precedes_candidate_limits_and_keeps_rows() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let a = store.ensure_root(r"C:\A").unwrap();
        let b = store.ensure_root(r"C:\B").unwrap();
        let rows: Vec<_> = (0..600)
            .map(|i| {
                rec(
                    &format!(r"C:\A\match{i}.txt"),
                    &format!("match{i}.txt"),
                    1,
                    1,
                )
            })
            .collect();
        store.apply_batch(a, 0, &rows).unwrap();
        store
            .apply_batch(b, 0, &[rec(r"C:\B\matchgood.txt", "matchgood.txt", 1, 1)])
            .unwrap();
        let mut c = ws_protocol::IndexingConfig {
            roots: vec![r"C:\B".into()],
            ..Default::default()
        };
        store.set_indexing_config(&c).unwrap();
        let hits = store
            .search(&crate::model::SearchRequest {
                text: "match".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits[0].path, r"C:\B\matchgood.txt");
        assert!(store.find_by_path(r"C:\A\match0.txt").unwrap().is_some());
        store.enqueue_dir(a, r"C:\A").unwrap();
        assert!(store.claim_dir().unwrap().is_none());
        c.roots.push(r"C:\A".into());
        store.set_indexing_config(&c).unwrap();
        assert!(store.claim_dir().unwrap().is_some());
    }

    fn rec(path: &str, name: &str, size: u64, mtime: i64) -> EntryRecord {
        EntryRecord {
            root_id: 0,
            parent_id: None,
            path: path.into(),
            name: name.into(),
            ext: crate::model::extension_of(name),
            is_dir: false,
            size,
            mtime,
            volume_serial: 1,
            file_id: size, // unique per record in tests
            is_app: false,
            app_target: String::new(),
            keywords: String::new(),
        }
    }

    fn setup() -> (IndexStore, i64) {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:\\data").unwrap();
        (store, root)
    }

    #[test]
    fn upsert_insert_update_unchanged() {
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();

        let outcomes = store
            .apply_batch(root, gen, &[rec("C:\\data\\a.txt", "a.txt", 10, 100)])
            .unwrap();
        assert_eq!(outcomes, vec![UpsertOutcome::Inserted]);

        // Same size/mtime → unchanged.
        let outcomes = store
            .apply_batch(root, gen, &[rec("C:\\data\\a.txt", "a.txt", 10, 100)])
            .unwrap();
        assert_eq!(outcomes, vec![UpsertOutcome::Unchanged]);

        // New mtime → updated.
        let outcomes = store
            .apply_batch(root, gen, &[rec("C:\\data\\a.txt", "a.txt", 10, 200)])
            .unwrap();
        assert_eq!(outcomes, vec![UpsertOutcome::Updated]);

        assert_eq!(store.stats().unwrap().entries_total, 1);
    }

    #[test]
    fn generation_sweep_removes_stale_entries() {
        let (mut store, root) = setup();
        let g1 = store.begin_generation(root).unwrap();
        store
            .apply_batch(
                root,
                g1,
                &[
                    rec("C:\\data\\a.txt", "a.txt", 1, 1),
                    rec("C:\\data\\b.txt", "b.txt", 2, 2),
                ],
            )
            .unwrap();

        let g2 = store.begin_generation(root).unwrap();
        store
            .apply_batch(root, g2, &[rec("C:\\data\\a.txt", "a.txt", 1, 1)])
            .unwrap();
        let swept = store.sweep_generation(root, g2).unwrap();
        assert_eq!(swept, 1);
        assert!(store.find_by_path("C:\\data\\a.txt").unwrap().is_some());
        assert!(store.find_by_path("C:\\data\\b.txt").unwrap().is_none());
    }

    #[test]
    fn missing_entries_are_preserved() {
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();
        store
            .apply_batch(root, gen, &[rec("D:\\usb\\x.txt", "x.txt", 1, 1)])
            .unwrap();
        store.set_missing_for_root(root, true).unwrap();
        assert_eq!(store.stats().unwrap().entries_total, 1); // preserved
        store.set_missing_for_root(root, false).unwrap();
        assert!(
            !store
                .find_by_path("D:\\usb\\x.txt")
                .unwrap()
                .unwrap()
                .missing
        );
    }

    #[test]
    fn file_id_lookup_finds_moved_entry() {
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();
        let mut r = rec("C:\\data\\old.txt", "old.txt", 7, 1);
        r.file_id = 4242;
        store.apply_batch(root, gen, &[r]).unwrap();

        let found = store.find_by_file_id(1, 4242).unwrap().unwrap();
        assert_eq!(found.name, "old.txt");

        store
            .move_entry(found.id, None, "C:\\data\\new.txt", "new.txt", "txt")
            .unwrap();
        assert!(store.find_by_path("C:\\data\\old.txt").unwrap().is_none());
        let moved = store.find_by_path("C:\\data\\new.txt").unwrap().unwrap();
        assert_eq!(moved.id, found.id); // identity preserved
    }

    #[test]
    fn remove_tree_removes_subtree() {
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();
        let mut dir = rec("C:\\data\\sub", "sub", 0, 0);
        dir.is_dir = true;
        store.apply_batch(root, gen, &[dir]).unwrap();
        let parent = store.find_by_path("C:\\data\\sub").unwrap().unwrap().id;
        let mut child = rec("C:\\data\\sub\\c.txt", "c.txt", 1, 1);
        child.parent_id = Some(parent);
        store.apply_batch(root, gen, &[child]).unwrap();

        assert_eq!(store.remove_tree("C:\\data\\sub").unwrap(), 2);
        assert_eq!(store.stats().unwrap().entries_total, 0);
    }

    #[test]
    fn crawl_queue_claim_and_resume() {
        let (mut store, root) = setup();
        store.enqueue_dir(root, "C:\\data\\a").unwrap();
        store.enqueue_dir(root, "C:\\data\\a").unwrap(); // idempotent
        store.enqueue_dir(root, "C:\\data\\b").unwrap();
        assert_eq!(store.queue_depth().unwrap(), 2);

        let first = store.claim_dir().unwrap().unwrap();
        assert_eq!(first.2, "C:\\data\\a");
        // A claimed row cannot be claimed twice (single worker owns it).
        assert_eq!(store.claim_dir().unwrap().unwrap().2, "C:\\data\\b");
        assert!(store.claim_dir().unwrap().is_none());

        // Simulated crash: release_claims makes everything claimable again.
        store.release_claims().unwrap();
        assert!(store.claim_dir().unwrap().is_some());

        store.complete_dir(first.0).unwrap();
        assert_eq!(store.queue_depth().unwrap(), 1);
    }

    #[test]
    fn checkpoints_round_trip() {
        let (mut store, _root) = setup();
        assert_eq!(store.checkpoint("phase").unwrap(), None);
        store.set_checkpoint("phase", "crawling").unwrap();
        store.set_checkpoint("phase", "done").unwrap();
        assert_eq!(store.checkpoint("phase").unwrap().as_deref(), Some("done"));
    }

    #[test]
    fn meta_round_trip_is_additive() {
        let (mut store, _root) = setup();
        assert_eq!(store.meta_get("embedding_model").unwrap(), None);
        store
            .meta_set("embedding_model", r#"{"model_id":"x","embedding_dim":768}"#)
            .unwrap();
        assert_eq!(
            store.meta_get("embedding_model").unwrap().as_deref(),
            Some(r#"{"model_id":"x","embedding_dim":768}"#)
        );
        // Overwrite + unrelated keys coexist (schema_version untouched).
        store.meta_set("embedding_model", "{}").unwrap();
        store.meta_set("other_key", "1").unwrap();
        assert_eq!(
            store.meta_get("embedding_model").unwrap().as_deref(),
            Some("{}")
        );
        assert_eq!(
            store.meta_get("schema_version").unwrap().as_deref(),
            Some("2")
        );
    }

    #[test]
    fn stats_counts_apps_and_dirs() {
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();
        let mut d = rec("C:\\data\\d", "d", 0, 0);
        d.is_dir = true;
        let mut app = rec("C:\\data\\Code.lnk", "Code.lnk", 1, 1);
        app.is_app = true;
        app.app_target = "C:\\Program Files\\Code\\Code.exe".into();
        store
            .apply_batch(root, gen, &[d, app, rec("C:\\data\\f.txt", "f.txt", 1, 1)])
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.entries_total, 3);
        assert_eq!(stats.dirs_total, 1);
        assert_eq!(stats.apps_total, 1);
        assert!(stats.index_size_bytes > 0);
    }

    // ---- schema v2 ----

    fn entry_id_for(
        store: &mut IndexStore,
        root: i64,
        path: &str,
        name: &str,
        file_id: u64,
    ) -> i64 {
        let gen = store.begin_generation(root).unwrap();
        let mut r = rec(path, name, 1, 1);
        r.file_id = file_id;
        store.apply_batch(root, gen, &[r]).unwrap();
        store.find_by_path(path).unwrap().unwrap().id
    }

    #[test]
    fn fresh_db_is_schema_version_2() {
        let (store, _) = setup();
        assert_eq!(store.schema_version().unwrap(), "2");
        assert_eq!(store.segment_count().unwrap(), 0);
    }

    #[test]
    fn segments_upsert_keeps_stable_id_and_replaces_vector() {
        let (mut store, root) = setup();
        let eid = entry_id_for(&mut store, root, "C:\\data\\a.txt", "a.txt", 101);
        let first = store
            .upsert_segment(NewSegment {
                entry_id: eid,
                kind: "name",
                chunk_index: 0,
                text: "a.txt",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0, 0.0, 0.0],
            })
            .unwrap();
        let second = store
            .upsert_segment(NewSegment {
                entry_id: eid,
                kind: "name",
                chunk_index: 0,
                text: "a.txt renamed",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[0.0, 1.0, 0.0, 0.0],
            })
            .unwrap();
        assert_eq!(first, second, "upsert must preserve the segment id");
        assert_eq!(store.segment_count().unwrap(), 1);
        let got = store.get_segment(first).unwrap().unwrap();
        assert_eq!(got.text, "a.txt renamed");
        assert_eq!(got.vector.len(), 4);
        // Stored vector is normalized: cosine to the new input is ~1.
        let cos = crate::vectors::cosine_similarity(&got.vector, &[0.0, 1.0, 0.0, 0.0]);
        assert!(cos > 0.999, "stored vector not replaced: cosine {cos}");
        assert!(got.norm > 0.0);
    }

    #[test]
    fn segments_incremental_add_remove() {
        let (mut store, root) = setup();
        let a = entry_id_for(&mut store, root, "C:\\data\\a.txt", "a.txt", 201);
        let b = entry_id_for(&mut store, root, "C:\\data\\b.txt", "b.txt", 202);
        let c = entry_id_for(&mut store, root, "C:\\data\\c.txt", "c.txt", 203);
        for (eid, v) in [
            (a, vec![1.0, 0.0, 0.0]),
            (b, vec![0.0, 1.0, 0.0]),
            (c, vec![0.0, 0.0, 1.0]),
        ] {
            store
                .upsert_segment(NewSegment {
                    entry_id: eid,
                    kind: "name",
                    chunk_index: 0,
                    text: "t",
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &v,
                })
                .unwrap();
        }
        assert_eq!(store.segment_count().unwrap(), 3);
        assert_eq!(store.count_segments_by_kind("name").unwrap(), 3);

        // Remove one entry's segments explicitly.
        assert_eq!(store.remove_segments_for_entry(b).unwrap(), 1);
        assert_eq!(store.segment_count().unwrap(), 2);
        assert!(store.list_segments_for_entry(b).unwrap().is_empty());

        // Query no longer returns the removed entry.
        let hits = store.search_vectors(&[0.0, 1.0, 0.0], 10, None).unwrap();
        assert!(hits.iter().all(|h| h.segment.entry_id != b));

        // Entry delete cascades to segments via FK.
        assert!(store.remove_by_path("C:\\data\\a.txt").unwrap());
        assert_eq!(store.segment_count().unwrap(), 1);
        assert!(store.list_segments_for_entry(a).unwrap().is_empty());
    }

    #[test]
    fn search_vectors_top_k_and_filters_are_correct() {
        let (mut store, root) = setup();
        let ids: Vec<i64> = ["a", "b", "c", "d"]
            .iter()
            .enumerate()
            .map(|(i, n)| {
                entry_id_for(
                    &mut store,
                    root,
                    &format!("C:\\data\\{n}.txt"),
                    &format!("{n}.txt"),
                    300 + i as u64,
                )
            })
            .collect();
        // Angles from +x: 0°, 10°, 45°, 90° (2-d).
        let deg = std::f32::consts::PI / 180.0;
        let vecs = [
            vec![1.0, 0.0],
            vec![(10.0 * deg).cos(), (10.0 * deg).sin()],
            vec![(45.0 * deg).cos(), (45.0 * deg).sin()],
            vec![0.0, 1.0],
        ];
        for (i, eid) in ids.iter().enumerate() {
            let kind = if i == 2 { "text" } else { "name" };
            store
                .upsert_segment(NewSegment {
                    entry_id: *eid,
                    kind,
                    chunk_index: 0,
                    text: "t",
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &vecs[i],
                })
                .unwrap();
        }
        // Top-2 by cosine from +x.
        let hits = store.search_vectors(&[1.0, 0.0], 2, None).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].segment.entry_id, ids[0]);
        assert_eq!(hits[1].segment.entry_id, ids[1]);
        assert!(hits[0].cosine >= hits[1].cosine);
        assert!(hits[0].cosine > 0.999);

        // Kind filter isolates the text segment.
        let hits = store
            .search_vectors(&[1.0, 0.0], 10, Some(&VectorFilter::for_kind("text")))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].segment.entry_id, ids[2]);

        // Entry filter restricts the candidate set.
        let hits = store
            .search_vectors(
                &[1.0, 0.0],
                10,
                Some(&VectorFilter {
                    kinds: Vec::new(),
                    entry_ids: vec![ids[3], ids[1]],
                }),
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].segment.entry_id, ids[1]);
        assert_eq!(hits[1].segment.entry_id, ids[3]);

        // Degenerate queries return empty, never an error (aligned contract
        // with `VectorIndex::search`; see its rustdoc).
        assert!(store.search_vectors(&[], 10, None).unwrap().is_empty());
        assert!(store
            .search_vectors(&[0.0, 0.0], 10, None)
            .unwrap()
            .is_empty());
        assert!(store
            .search_vectors(&[f32::NAN, 0.0], 10, None)
            .unwrap()
            .is_empty());
        assert!(store
            .search_vectors(&[1.0, 0.0], 0, None)
            .unwrap()
            .is_empty());
        // Width mismatch: schemaless table skips non-matching rows instead of
        // erroring (the fixed-dim index reports DimensionMismatch).
        assert!(store
            .search_vectors(&[1.0, 0.0, 0.0], 10, None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn search_vectors_skips_mixed_width_rows() {
        let (mut store, root) = setup();
        let a = entry_id_for(&mut store, root, "C:\\data\\a.txt", "a.txt", 601);
        let b = entry_id_for(&mut store, root, "C:\\data\\b.txt", "b.txt", 602);
        store
            .upsert_segment(NewSegment {
                entry_id: a,
                kind: "name",
                chunk_index: 0,
                text: "a",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0],
            })
            .unwrap();
        store
            .upsert_segment(NewSegment {
                entry_id: b,
                kind: "name",
                chunk_index: 0,
                text: "b",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0, 0.0],
            })
            .unwrap();
        let hits = store.search_vectors(&[1.0, 0.0], 10, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].segment.entry_id, a);
        let hits = store.search_vectors(&[1.0, 0.0, 0.0], 10, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].segment.entry_id, b);
    }

    #[test]
    fn search_vectors_agrees_with_in_memory_index() {
        use crate::vectors::VectorIndex;
        let (mut store, root) = setup();
        let mut eids = Vec::new();
        for i in 0..8 {
            let eid = entry_id_for(
                &mut store,
                root,
                &format!("C:\\data\\f{i}.txt"),
                &format!("f{i}.txt"),
                400 + i as u64,
            );
            let v = vec![(i as f32 + 1.0), (7 - i) as f32, 1.0];
            store
                .upsert_segment(NewSegment {
                    entry_id: eid,
                    kind: "name",
                    chunk_index: 0,
                    text: "t",
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &v,
                })
                .unwrap();
            eids.push(eid);
        }
        let query = vec![1.0, 0.5, 0.25];
        let db_hits = store.search_vectors(&query, 8, None).unwrap();
        let segs = store.all_segments().unwrap();
        assert_eq!(segs.len(), 8);
        let mem = VectorIndex::from_segments(3, &segs);
        let mem_hits = mem.search(&query, 8, None).unwrap();
        assert_eq!(db_hits.len(), mem_hits.len());
        for (db, mm) in db_hits.iter().zip(mem_hits.iter()) {
            assert_eq!(db.segment.id, mm.id);
            assert!((db.cosine - mm.cosine).abs() <= 1e-5);
        }
        let _ = eids;
    }

    #[test]
    fn extract_jobs_queue_claim_resume_and_fail() {
        let (mut store, root) = setup();
        let a = entry_id_for(&mut store, root, "C:\\data\\a.txt", "a.txt", 501);
        let b = entry_id_for(&mut store, root, "C:\\data\\b.txt", "b.txt", 502);
        store.enqueue_extract_job(a, "name").unwrap();
        store.enqueue_extract_job(a, "name").unwrap(); // idempotent
        store.enqueue_extract_job(b, "name").unwrap();
        assert_eq!(store.extract_queue_depth().unwrap(), 2);

        // Claim order is LIFO within priority (newest first): b before a.
        let first = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(first.entry_id, b);
        assert!(first.claimed);
        let second = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(second.entry_id, a);
        assert!(store.claim_extract_job().unwrap().is_none());

        // Simulated crash: claims go back to pending, jobs are not lost.
        assert_eq!(store.release_extract_claims().unwrap(), 2);
        let retry = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(retry.entry_id, b);

        // Retryable failure bumps attempts and re-queues.
        store.fail_extract_job(retry.id).unwrap();
        let again = store.get_extract_job(retry.id).unwrap().unwrap();
        assert_eq!(again.attempts, 1);
        assert!(!again.claimed);

        // Completion dequeues.
        store.complete_extract_job(retry.id).unwrap();
        store.complete_extract_job(second.id).unwrap();
        assert_eq!(store.extract_queue_depth().unwrap(), 0);

        // Permanent failure is kept for inspection, never auto-claimed.
        store.enqueue_extract_job(a, "text").unwrap();
        let job = store.claim_extract_job().unwrap().unwrap();
        store.mark_extract_failed(job.id).unwrap();
        let failed = store.get_extract_job(job.id).unwrap().unwrap();
        assert_eq!(failed.state, crate::model::EXTRACT_STATE_FAILED);
        assert!(store.claim_extract_job().unwrap().is_none());
    }

    #[test]
    fn extract_jobs_new_files_first_lifo_within_priority() {
        use crate::model::{EXTRACT_PRIORITY_NEW, EXTRACT_PRIORITY_UPDATED};
        let (mut store, root) = setup();
        let updated_old = entry_id_for(&mut store, root, "C:\\data\\u1.txt", "u1.txt", 511);
        let new_file = entry_id_for(&mut store, root, "C:\\data\\n1.txt", "n1.txt", 512);
        let updated_new = entry_id_for(&mut store, root, "C:\\data\\u2.txt", "u2.txt", 513);
        store
            .enqueue_extract_job_with_priority(updated_old, "name", EXTRACT_PRIORITY_UPDATED)
            .unwrap();
        store
            .enqueue_extract_job_with_priority(new_file, "name", EXTRACT_PRIORITY_NEW)
            .unwrap();
        store
            .enqueue_extract_job_with_priority(updated_new, "name", EXTRACT_PRIORITY_UPDATED)
            .unwrap();

        // New files first regardless of enqueue order ...
        let first = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(first.entry_id, new_file);
        // ... then LIFO within the updated priority (most recent first).
        let second = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(second.entry_id, updated_new);
        let third = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(third.entry_id, updated_old);

        // A repeat enqueue upgrades a pending job to the higher priority.
        store.complete_extract_job(first.id).unwrap();
        for j in [second.id, third.id] {
            store.fail_extract_job(j).unwrap();
        }
        store
            .enqueue_extract_job_with_priority(updated_old, "name", EXTRACT_PRIORITY_NEW)
            .unwrap();
        let upgraded = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(upgraded.entry_id, updated_old);
        assert_eq!(upgraded.priority, EXTRACT_PRIORITY_NEW);
    }

    #[test]
    fn extract_jobs_entry_helpers_and_purge() {
        let (mut store, root) = setup();
        let eid = entry_id_for(&mut store, root, "C:\\data\\a.txt", "a.txt", 521);
        let row = store.get_entry(eid).unwrap().unwrap();
        assert_eq!(row.name, "a.txt");
        assert!(store.get_entry(eid + 9999).unwrap().is_none());
        assert_eq!(store.all_entry_ids().unwrap(), vec![eid]);

        store
            .upsert_segment(NewSegment {
                entry_id: eid,
                kind: "name",
                chunk_index: 0,
                text: "a.txt",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0],
            })
            .unwrap();
        assert_eq!(store.count_segments_by_kind("name").unwrap(), 1);
        assert_eq!(
            store.extract_queue_depth_for_kind("name").unwrap(),
            0,
            "no jobs enqueued yet"
        );
        store.enqueue_extract_job(eid, "name").unwrap();
        assert_eq!(store.extract_queue_depth_for_kind("name").unwrap(), 1);
        assert_eq!(store.purge_segments_by_kind("name").unwrap(), 1);
        assert_eq!(store.segment_count().unwrap(), 0);
    }

    #[test]
    fn migration_from_v1_file_preserves_data_and_bumps_version() {
        // Build a genuine v1 file with raw SQL (no segments/extract_jobs).
        let dir = std::env::temp_dir().join(format!(
            "winsearch-v1mig-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("index.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(super::SCHEMA).unwrap();
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', '1')",
                [],
            )
            .unwrap();
            conn.execute("INSERT INTO roots(path) VALUES('C:\\data')", [])
                .unwrap();
            let root: i64 = conn
                .query_row("SELECT id FROM roots WHERE path = 'C:\\data'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            conn.execute(
                "INSERT INTO entries(root_id, path, name, name_lower, ext, is_dir, size,
                                     mtime, volume_serial, file_id, scan_generation)
                 VALUES(?1, 'C:\\data\\keep.txt', 'keep.txt', 'keep.txt', 'txt',
                        0, 5, 100, 1, 999, 1)",
                rusqlite::params![root],
            )
            .unwrap();
            // No segments/extract_jobs tables in a v1 file.
            let tables: Vec<String> = conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert!(!tables.contains(&"segments".to_string()));
            assert!(!tables.contains(&"extract_jobs".to_string()));
        }
        // Open with v2 code: migrates in place, data intact, version == 2.
        let mut store = IndexStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), "2");
        let kept = store.find_by_path("C:\\data\\keep.txt").unwrap().unwrap();
        assert_eq!(kept.name, "keep.txt");
        // New tables are usable immediately.
        store.enqueue_extract_job(kept.id, "name").unwrap();
        assert_eq!(store.extract_queue_depth().unwrap(), 1);
        let sid = store
            .upsert_segment(NewSegment {
                entry_id: kept.id,
                kind: "name",
                chunk_index: 0,
                text: "keep.txt",
                page: None,
                start_ms: None,
                end_ms: None,
                line: None,
                vector: &[1.0, 0.0, 0.0],
            })
            .unwrap();
        assert!(store.get_segment(sid).unwrap().is_some());
        // Reopening keeps version 2 and the new rows.
        drop(store);
        let store2 = IndexStore::open(&db_path).unwrap();
        assert_eq!(store2.schema_version().unwrap(), "2");
        assert_eq!(store2.segment_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_config_defaults_semantic_on_auto_backend() {
        let store = IndexStore::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), "2");
        let cfg = store.get_engine_config().unwrap();
        assert!(cfg.semantic_enabled);
        assert_eq!(cfg.preferred_backend, crate::model::PreferredBackend::Auto);
    }

    #[test]
    fn engine_config_round_trip_persists_both_fields() {
        let mut store = IndexStore::open_in_memory().unwrap();
        let cfg = crate::model::EngineConfig {
            semantic_enabled: false,
            preferred_backend: crate::model::PreferredBackend::Cuda,
        };
        store.set_engine_config(&cfg).unwrap();
        assert_eq!(store.get_engine_config().unwrap(), cfg);
        // Raw spellings are canonical ("0", "cuda").
        assert_eq!(
            store
                .config_get(super::CONFIG_KEY_SEMANTIC_ENABLED)
                .unwrap()
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            store
                .config_get(super::CONFIG_KEY_PREFERRED_BACKEND)
                .unwrap()
                .as_deref(),
            Some("cuda")
        );
        // Re-enabling flips just the one key.
        store
            .set_engine_config(&crate::model::EngineConfig {
                semantic_enabled: true,
                preferred_backend: crate::model::PreferredBackend::Vulkan,
            })
            .unwrap();
        let back = store.get_engine_config().unwrap();
        assert!(back.semantic_enabled);
        assert_eq!(
            back.preferred_backend,
            crate::model::PreferredBackend::Vulkan
        );
    }

    #[test]
    fn engine_config_rejects_unknown_backend_and_non_boolean() {
        let mut store = IndexStore::open_in_memory().unwrap();
        store
            .config_set(super::CONFIG_KEY_PREFERRED_BACKEND, "opencl")
            .unwrap();
        assert!(
            matches!(
                store.get_engine_config().unwrap_err(),
                super::StoreError::InvalidConfig(_)
            ),
            "unknown backend must fail closed"
        );
        store
            .config_set(super::CONFIG_KEY_PREFERRED_BACKEND, "cpu")
            .unwrap();
        store
            .config_set(super::CONFIG_KEY_SEMANTIC_ENABLED, "maybe")
            .unwrap();
        assert!(
            matches!(
                store.get_engine_config().unwrap_err(),
                super::StoreError::InvalidConfig(_)
            ),
            "non-boolean semantic flag must fail closed"
        );
    }

    #[test]
    fn content_extract_priority_orders_small_and_recent_first() {
        use crate::model::{
            compute_extract_priority, UpsertOutcome, SEGMENT_KIND_DOC, SEGMENT_KIND_NAME,
            SEGMENT_KIND_PDF,
        };
        let (mut store, root) = setup();
        let large_old = entry_id_for(&mut store, root, "C:\\data\\large.pdf", "large.pdf", 701);
        let small_old = entry_id_for(
            &mut store,
            root,
            "C:\\data\\small_old.txt",
            "small_old.txt",
            702,
        );
        let small_recent = entry_id_for(
            &mut store,
            root,
            "C:\\data\\small_new.txt",
            "small_new.txt",
            703,
        );

        // Also enqueue a name job to verify claim_content_extract_job ignores kind='name'.
        store
            .enqueue_extract_job(large_old, SEGMENT_KIND_NAME)
            .unwrap();

        let p_large_old =
            compute_extract_priority(20 * 1024 * 1024, 1_000, UpsertOutcome::Inserted);
        let p_small_old = compute_extract_priority(4 * 1024, 1_000, UpsertOutcome::Inserted);
        let p_small_recent = compute_extract_priority(4 * 1024, 2_000, UpsertOutcome::Inserted);
        assert!(p_small_recent > p_small_old);
        assert!(p_small_old > p_large_old);

        store
            .enqueue_extract_job_with_priority(large_old, SEGMENT_KIND_PDF, p_large_old)
            .unwrap();
        store
            .enqueue_extract_job_with_priority(small_old, SEGMENT_KIND_DOC, p_small_old)
            .unwrap();
        store
            .enqueue_extract_job_with_priority(small_recent, SEGMENT_KIND_DOC, p_small_recent)
            .unwrap();

        assert_eq!(store.content_extract_queue_depth().unwrap(), 3);

        // Claim order: small_recent first, then small_old, then large_old.
        let c1 = store.claim_content_extract_job().unwrap().unwrap();
        assert_eq!(c1.entry_id, small_recent);
        let c2 = store.claim_content_extract_job().unwrap().unwrap();
        assert_eq!(c2.entry_id, small_old);
        let c3 = store.claim_content_extract_job().unwrap().unwrap();
        assert_eq!(c3.entry_id, large_old);
        assert!(store.claim_content_extract_job().unwrap().is_none());

        // Name job is still untouched in the queue.
        assert_eq!(
            store
                .extract_queue_depth_for_kind(SEGMENT_KIND_NAME)
                .unwrap(),
            1
        );
    }

    #[test]
    fn content_fts_makes_extracted_text_and_ocr_searchable() {
        use crate::model::{SearchRequest, SEGMENT_KIND_DOC, SEGMENT_KIND_OCR};
        let (mut store, root) = setup();
        let doc_id = entry_id_for(&mut store, root, "C:\\data\\opaque.txt", "opaque.txt", 801);
        let img_id = entry_id_for(&mut store, root, "C:\\data\\shot.png", "shot.png", 802);

        store
            .replace_content_fts(
                doc_id,
                SEGMENT_KIND_DOC,
                &["The indemnification obligation survives termination."],
            )
            .unwrap();
        store
            .replace_content_fts(
                img_id,
                SEGMENT_KIND_OCR,
                &["Fatal error ERR_SSL_PROTOCOL_ERROR in dialog"],
            )
            .unwrap();

        let hits = store
            .search(&SearchRequest {
                text: "indemnification".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_id, doc_id);
        assert_eq!(hits[0].explanation, "Matches extracted content");

        let ocr_hits = store
            .search(&SearchRequest {
                text: "ERR_SSL_PROTOCOL_ERROR".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(ocr_hits.len(), 1);
        assert_eq!(ocr_hits[0].entry_id, img_id);
    }

    #[test]
    fn re_enqueue_after_mark_extract_failed_revives_job_to_pending() {
        use crate::model::{
            EXTRACT_PRIORITY_NEW, EXTRACT_PRIORITY_UPDATED, EXTRACT_STATE_FAILED,
            EXTRACT_STATE_PENDING, SEGMENT_KIND_DOC,
        };
        let (mut store, root) = setup();
        let eid = entry_id_for(&mut store, root, "C:\\data\\revive.txt", "revive.txt", 901);

        store
            .enqueue_extract_job_with_priority(eid, SEGMENT_KIND_DOC, EXTRACT_PRIORITY_NEW)
            .unwrap();
        let claimed = store.claim_content_extract_job().unwrap().unwrap();
        assert_eq!(claimed.entry_id, eid);
        store.mark_extract_failed(claimed.id).unwrap();

        let failed = store.get_extract_job(claimed.id).unwrap().unwrap();
        assert_eq!(failed.state, EXTRACT_STATE_FAILED);
        assert!(!failed.claimed);
        assert_eq!(failed.attempts, 1);
        assert!(store.claim_content_extract_job().unwrap().is_none());

        // File is later edited on disk and re-enqueued: must revive to pending with attempts = 0.
        store
            .enqueue_extract_job_with_priority(eid, SEGMENT_KIND_DOC, EXTRACT_PRIORITY_UPDATED)
            .unwrap();
        let revived = store.get_extract_job(claimed.id).unwrap().unwrap();
        assert_eq!(revived.state, EXTRACT_STATE_PENDING);
        assert!(!revived.claimed);
        assert_eq!(revived.attempts, 0);
        assert_eq!(revived.priority, EXTRACT_PRIORITY_UPDATED);

        let reclaimed = store.claim_content_extract_job().unwrap().unwrap();
        assert_eq!(reclaimed.id, claimed.id);
        assert_eq!(reclaimed.entry_id, eid);
    }

    #[test]
    fn migration_from_pre_priority_v2_adds_column_before_priority_index() {
        let dir = std::env::temp_dir().join(format!(
            "winsearch-prepri-v2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("index.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(super::SCHEMA).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE segments (
                    id          INTEGER PRIMARY KEY,
                    entry_id    INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
                    kind        TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL DEFAULT 0,
                    text        TEXT NOT NULL DEFAULT '',
                    page        INTEGER NULL,
                    start_ms    INTEGER NULL,
                    end_ms      INTEGER NULL,
                    line        INTEGER NULL,
                    vector      BLOB NOT NULL,
                    norm        REAL NOT NULL DEFAULT 0,
                    UNIQUE(entry_id, kind, chunk_index)
                );
                CREATE TABLE extract_jobs (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
                    kind     TEXT NOT NULL,
                    state    TEXT NOT NULL DEFAULT 'pending',
                    claimed  INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(entry_id, kind)
                );
                INSERT INTO meta(key, value) VALUES('schema_version', '2');
                "#,
            )
            .unwrap();
        }
        let mut store = IndexStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), "2");
        let root = store.ensure_root("C:\\data").unwrap();
        let eid = entry_id_for(&mut store, root, "C:\\data\\p.txt", "p.txt", 950);
        store
            .enqueue_extract_job_with_priority(eid, "name", 42)
            .unwrap();
        let claimed = store.claim_extract_job().unwrap().unwrap();
        assert_eq!(claimed.priority, 42);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_path_backfill_is_repaired_atomically() {
        let (mut store, root) = setup();
        let generation = store.begin_generation(root).unwrap();
        store
            .apply_batch(
                root,
                generation,
                &[
                    rec("C:\\alpha\\one.txt", "one.txt", 1, 1),
                    rec("C:\\RÄKNING\\two.txt", "two.txt", 1, 1),
                ],
            )
            .unwrap();
        store
            .conn
            .execute_batch(
                "DELETE FROM meta WHERE key = 'entries_path_tri_backfilled';
            DELETE FROM entries_path_tri WHERE rowid = (SELECT MAX(id) FROM entries);
            CREATE TRIGGER fail_backfill BEFORE INSERT ON meta
            WHEN NEW.key = 'entries_path_tri_backfilled'
            BEGIN SELECT RAISE(FAIL, 'simulated migration failure'); END;",
            )
            .unwrap();
        assert!(IndexStore::ensure_entries_path_tri_backfilled(&store.conn).is_err());
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM entries_path_tri", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "failed migration must roll back its partial work");
        store
            .conn
            .execute_batch("DROP TRIGGER fail_backfill;")
            .unwrap();
        IndexStore::ensure_entries_path_tri_backfilled(&store.conn).unwrap();
        let hits = store
            .search(&crate::model::SearchRequest {
                text: "räkning".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn rename_tree_paths_updates_descendant_fts_keywords() {
        use crate::model::SearchRequest;
        let (mut store, root) = setup();
        let gen = store.begin_generation(root).unwrap();
        let mut folder = rec("C:\\data\\oldparent", "oldparent", 0, 1);
        folder.is_dir = true;
        let child = rec("C:\\data\\oldparent\\quarterly.txt", "quarterly.txt", 10, 1);
        store.apply_batch(root, gen, &[folder, child]).unwrap();
        let folder_id = store
            .find_by_path("C:\\data\\oldparent")
            .unwrap()
            .unwrap()
            .id;

        store
            .move_entry(folder_id, None, "C:\\data\\newparent", "newparent", "")
            .unwrap();
        let updated = store
            .rename_tree_paths("C:\\data\\oldparent", "C:\\data\\newparent")
            .unwrap();
        assert_eq!(updated, 1);

        let hits_new = store
            .search(&SearchRequest {
                text: "newparent quarterly".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits_new.len(), 1);
        assert_eq!(hits_new[0].path, "C:\\data\\newparent\\quarterly.txt");

        let hits_old = store
            .search(&SearchRequest {
                text: "oldparent quarterly".into(),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(
            hits_old.is_empty(),
            "old folder keyword must not match after rename"
        );
    }

    #[test]
    fn apply_batch_detects_app_target_change_and_preserves_catalog_apps_on_crawl() {
        let (mut store, desktop_root) = setup();
        let catalog_root = store.ensure_root("@apps").unwrap();

        // 1. Catalog indexes a shortcut on Desktop.
        let g_cat1 = store.begin_generation(catalog_root).unwrap();
        let mut app_rec = rec("C:\\data\\Acme.lnk", "Acme", 0, 0);
        app_rec.is_app = true;
        app_rec.app_target = "C:\\Program Files\\Acme\\v1\\acme.exe".into();
        app_rec.keywords = "acme --v1".into();
        let out = store
            .apply_batch(catalog_root, g_cat1, &[app_rec.clone()])
            .unwrap();
        assert_eq!(out, vec![UpsertOutcome::Inserted]);

        // 2. Shortcut target changes -> Updated, not Unchanged.
        let g_cat2 = store.begin_generation(catalog_root).unwrap();
        app_rec.app_target = "C:\\Program Files\\Acme\\v2\\acme.exe".into();
        let out = store
            .apply_batch(catalog_root, g_cat2, &[app_rec.clone()])
            .unwrap();
        assert_eq!(out, vec![UpsertOutcome::Updated]);

        // 3. Filesystem crawl visits Desktop and sees `Acme.lnk` as a regular file (`is_app = false`).
        let g_desk = store.begin_generation(desktop_root).unwrap();
        let crawl_rec = rec("C:\\data\\Acme.lnk", "Acme.lnk", 512, 1700);
        let out = store
            .apply_batch(desktop_root, g_desk, &[crawl_rec])
            .unwrap();
        assert_eq!(out, vec![UpsertOutcome::Updated]);

        let row = store.find_by_path("C:\\data\\Acme.lnk").unwrap().unwrap();
        assert!(
            row.is_app,
            "catalog app classification must be preserved when Desktop is crawled"
        );
        assert_eq!(row.name, "Acme", "catalog display name must be preserved");
        assert_eq!(
            row.root_id, catalog_root,
            "catalog root_id must be preserved"
        );
    }
}
