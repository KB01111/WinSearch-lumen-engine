//! Standalone SQLite storage (`code.db`) for the code index: repositories,
//! source files with `(volume_serial, file_id)` rename identity preservation,
//! AST/text chunks with FP16 normalized vectors, symbol occurrences
//! (definitions and references), and FTS5 indexes for symbols and chunks.
//!
//! `code.db` is strictly isolated from `index.db` (`ws_platform::paths::code_db_path()`)
//! so code search results and code vectors never pollute normal desktop search.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

use crate::model::{
    CodeChunk, CodeFileRecord, CodeFileRow, CodeSymbol, DetectedRepo, FileUpsertOutcome,
    NewCodeChunk, NewCodeSymbol, RepoRow, ScoredCodeChunk, CODE_SCHEMA_VERSION,
};

#[derive(Debug, thiserror::Error)]
pub enum CodeStoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("unsupported code schema version: {0}")]
    UnsupportedSchemaVersion(String),
    #[error("invalid code chunk: {0}")]
    InvalidChunk(String),
}

/// Aggregate counters for `code.db`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodeIndexStats {
    pub repos_total: u64,
    pub files_total: u64,
    pub chunks_total: u64,
    pub embedded_chunks_total: u64,
    pub symbols_total: u64,
    pub db_size_bytes: u64,
}

pub struct CodeStore {
    conn: Connection,
}

const CODE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repos (
    id                 INTEGER PRIMARY KEY,
    root_path          TEXT NOT NULL UNIQUE,
    name               TEXT NOT NULL,
    has_git            INTEGER NOT NULL DEFAULT 0,
    enabled            INTEGER NOT NULL DEFAULT 1,
    current_generation INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS code_files (
    id              INTEGER PRIMARY KEY,
    repo_id         INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    path            TEXT NOT NULL UNIQUE,
    rel_path        TEXT NOT NULL,
    name            TEXT NOT NULL,
    name_lower      TEXT NOT NULL,
    ext             TEXT NOT NULL DEFAULT '',
    language        TEXT NOT NULL DEFAULT '',
    size            INTEGER NOT NULL DEFAULT 0,
    mtime           INTEGER NOT NULL DEFAULT 0,
    volume_serial   INTEGER NOT NULL DEFAULT 0,
    file_id         INTEGER NOT NULL DEFAULT 0,
    scan_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_code_files_repo     ON code_files(repo_id, scan_generation);
CREATE INDEX IF NOT EXISTS idx_code_files_identity ON code_files(volume_serial, file_id);
CREATE INDEX IF NOT EXISTS idx_code_files_name     ON code_files(name_lower);
CREATE INDEX IF NOT EXISTS idx_code_files_lang     ON code_files(language);

CREATE TABLE IF NOT EXISTS code_chunks (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL REFERENCES code_files(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    start_line  INTEGER NOT NULL DEFAULT 1,
    end_line    INTEGER NOT NULL DEFAULT 1,
    symbol_name TEXT NOT NULL DEFAULT '',
    symbol_kind TEXT NOT NULL DEFAULT '',
    text        TEXT NOT NULL DEFAULT '',
    vector      BLOB NOT NULL,
    norm        REAL NOT NULL DEFAULT 0,
    UNIQUE(file_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_code_chunks_file ON code_chunks(file_id);

CREATE VIRTUAL TABLE IF NOT EXISTS code_chunks_fts USING fts5(
    symbol_name,
    text,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TABLE IF NOT EXISTS code_symbols (
    id            INTEGER PRIMARY KEY,
    file_id       INTEGER NOT NULL REFERENCES code_files(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    name_lower    TEXT NOT NULL,
    kind          TEXT NOT NULL,
    is_definition INTEGER NOT NULL DEFAULT 1,
    line          INTEGER NOT NULL DEFAULT 1,
    end_line      INTEGER NOT NULL DEFAULT 1,
    signature     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_code_symbols_file ON code_symbols(file_id);
CREATE INDEX IF NOT EXISTS idx_code_symbols_name ON code_symbols(name_lower, is_definition DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS code_symbols_fts USING fts5(
    name,
    signature,
    tokenize = 'unicode61 remove_diacritics 2'
);
CREATE VIRTUAL TABLE IF NOT EXISTS code_symbols_tri USING fts5(
    name,
    tokenize = 'trigram case_sensitive 0'
);
"#;

impl CodeStore {
    /// Opens (or creates) `code.db` at `path`.
    pub fn open(path: &Path) -> Result<Self, CodeStoreError> {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        Self::init(Connection::open(path)?)
    }

    /// Opens an in-memory `code.db` for unit and integration tests.
    pub fn open_in_memory() -> Result<Self, CodeStoreError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, CodeStoreError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "case_sensitive_like", "ON")?;
        conn.execute_batch(CODE_SCHEMA)?;

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
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?1)",
                    params![CODE_SCHEMA_VERSION],
                )?;
            }
            Some(CODE_SCHEMA_VERSION) => {}
            Some(other) => {
                return Err(CodeStoreError::UnsupportedSchemaVersion(other.to_string()));
            }
        }
        Ok(Self { conn })
    }

    pub fn schema_version(&self) -> Result<String, CodeStoreError> {
        let v: String = self.conn.query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )?;
        Ok(v)
    }

    // ---- meta ----

    pub fn meta_get(&self, key: &str) -> Result<Option<String>, CodeStoreError> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    pub fn meta_set(&mut self, key: &str, value: &str) -> Result<(), CodeStoreError> {
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    // ---- repositories ----

    /// Registers or updates a detected repository and returns its `repo_id`.
    pub fn ensure_repo(&mut self, repo: &DetectedRepo) -> Result<i64, CodeStoreError> {
        self.conn.execute(
            "INSERT INTO repos(root_path, name, has_git) VALUES(?1, ?2, ?3)
             ON CONFLICT(root_path) DO UPDATE SET
                 name = excluded.name,
                 has_git = excluded.has_git",
            params![repo.root_path, repo.name, repo.has_git as i64],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT id FROM repos WHERE root_path = ?1",
            params![repo.root_path],
            |r| r.get(0),
        )?;
        Ok(id)
    }

    pub fn repos(&self) -> Result<Vec<RepoRow>, CodeStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, root_path, name, has_git, enabled, current_generation
             FROM repos ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RepoRow {
                id: row.get(0)?,
                root_path: row.get(1)?,
                name: row.get(2)?,
                has_git: row.get::<_, i64>(3)? != 0,
                enabled: row.get::<_, i64>(4)? != 0,
                current_generation: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Increments `current_generation` for `repo_id` and returns the new generation.
    pub fn begin_generation(&mut self, repo_id: i64) -> Result<i64, CodeStoreError> {
        let gen: i64 = self.conn.query_row(
            "UPDATE repos SET current_generation = current_generation + 1
             WHERE id = ?1 RETURNING current_generation",
            params![repo_id],
            |r| r.get(0),
        )?;
        Ok(gen)
    }

    /// Removes files (and their cascading chunks/symbols + FTS rows) in `repo_id`
    /// that were not seen in `scan_generation >= gen`.
    pub fn sweep_generation(&mut self, repo_id: i64, gen: i64) -> Result<u64, CodeStoreError> {
        let tx = self.conn.transaction()?;
        let stale_files = "SELECT id FROM code_files WHERE repo_id = ?1 AND scan_generation < ?2";
        tx.execute(
            &format!(
                "DELETE FROM code_chunks_fts WHERE rowid IN (
                     SELECT id FROM code_chunks WHERE file_id IN ({stale_files})
                 )"
            ),
            params![repo_id, gen],
        )?;
        tx.execute(
            &format!(
                "DELETE FROM code_symbols_fts WHERE rowid IN (
                     SELECT id FROM code_symbols WHERE file_id IN ({stale_files})
                 )"
            ),
            params![repo_id, gen],
        )?;
        tx.execute(
            &format!(
                "DELETE FROM code_symbols_tri WHERE rowid IN (
                     SELECT id FROM code_symbols WHERE file_id IN ({stale_files})
                 )"
            ),
            params![repo_id, gen],
        )?;
        let deleted = tx.execute(
            "DELETE FROM code_files WHERE repo_id = ?1 AND scan_generation < ?2",
            params![repo_id, gen],
        )?;
        tx.commit()?;
        Ok(deleted as u64)
    }

    // ---- code files (with rename identity preservation) ----

    /// Upserts a batch of discovered code files in one transaction.
    ///
    /// Rename preservation:
    /// 1. First checks if a file already exists at `rec.path`.
    /// 2. If not found by `path`, and `(rec.volume_serial != 0 && rec.file_id != 0)`,
    ///    checks for an existing row in the same `repo_id` with matching
    ///    `(volume_serial, file_id)`. When found, updates that row's `path`,
    ///    `rel_path`, `name`, `ext`, and `language` in place — preserving the
    ///    `code_files.id` primary key (and all attached `code_chunks` /
    ///    `code_symbols` when content is unchanged) and returning
    ///    [`FileUpsertOutcome::Renamed`].
    pub fn apply_file_batch(
        &mut self,
        repo_id: i64,
        scan_generation: i64,
        records: &[CodeFileRecord],
    ) -> Result<Vec<FileUpsertOutcome>, CodeStoreError> {
        let tx = self.conn.transaction()?;
        let mut outcomes = vec![None; records.len()];
        {
            let mut by_path = tx.prepare_cached(
                "SELECT id, size, mtime, name_lower, rel_path FROM code_files WHERE path = ?1",
            )?;
            let mut by_identity = tx.prepare_cached(
                "SELECT id, path, scan_generation FROM code_files
                 WHERE repo_id = ?1 AND volume_serial = ?2 AND file_id = ?3
                 LIMIT 1",
            )?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO code_files(
                     repo_id, path, rel_path, name, name_lower, ext, language,
                     size, mtime, volume_serial, file_id, scan_generation
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )?;
            let mut update = tx.prepare_cached(
                "UPDATE code_files SET
                     repo_id = ?2, path = ?3, rel_path = ?4, name = ?5, name_lower = ?6,
                     ext = ?7, language = ?8, size = ?9, mtime = ?10,
                     volume_serial = ?11, file_id = ?12, scan_generation = ?13
                 WHERE id = ?1",
            )?;

            // Move identities to newly seen paths before processing replacements
            // at their old paths, regardless of crawl/batch order.
            let mut ordered = records.iter().enumerate().map(|(index, rec)| {
                by_path.exists(params![rec.path]).map(|exists| (exists, index, rec))
            }).collect::<Result<Vec<_>, _>>()?;
            ordered.sort_by_key(|(exists, _, _)| *exists);
            for (_, index, rec) in ordered {
                let name_lower = rec.name.to_lowercase();
                let existing_path: Option<(i64, i64, i64, String, String)> = by_path
                    .query_row(params![rec.path], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                    })
                    .optional()?;

                if let Some((id, size, mtime, old_name_lower, old_rel_path)) = existing_path {
                    if size == rec.size as i64
                        && mtime == rec.mtime
                        && old_name_lower == name_lower
                        && old_rel_path == rec.rel_path
                    {
                        tx.execute(
                            "UPDATE code_files SET scan_generation = ?2 WHERE id = ?1",
                            params![id, scan_generation],
                        )?;
                        outcomes[index] = Some(FileUpsertOutcome::Unchanged { id });
                    } else {
                        update.execute(params![
                            id,
                            repo_id,
                            rec.path,
                            rec.rel_path,
                            rec.name,
                            name_lower,
                            rec.ext,
                            rec.language,
                            rec.size as i64,
                            rec.mtime,
                            rec.volume_serial as i64,
                            rec.file_id as i64,
                            scan_generation,
                        ])?;
                        outcomes[index] = Some(FileUpsertOutcome::Updated { id });
                    }
                    continue;
                }

                // Not at `rec.path`: check filesystem identity `(volume_serial, file_id)`
                // for a rename/move within the repository (ignoring hard links where the
                // original path still exists on disk or is also present in this batch).
                let renamed_id: Option<i64> = if rec.volume_serial != 0 && rec.file_id != 0 {
                    let cand: Option<(i64, String, i64)> = by_identity
                        .query_row(
                            params![repo_id, rec.volume_serial as i64, rec.file_id as i64],
                            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                        )
                        .optional()?;
                    cand.and_then(|(id, old_path, old_gen)| {
                        let in_batch = records.iter().any(|r| r.path == old_path
                            && r.volume_serial == rec.volume_serial && r.file_id == rec.file_id);
                        let case_only_rename =
                            old_path.to_lowercase() == rec.path.to_lowercase();
                        let old_path = std::path::Path::new(&old_path);
                        let still_exists = !case_only_rename &&
                            ws_platform::identity::file_identity(old_path)
                                .map(|identity| identity == (rec.volume_serial, rec.file_id))
                                .unwrap_or_else(|| old_path.try_exists().unwrap_or(true));
                        if old_gen != scan_generation && !in_batch && !still_exists {
                            Some(id)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

                if let Some(id) = renamed_id {
                    update.execute(params![
                        id,
                        repo_id,
                        rec.path,
                        rec.rel_path,
                        rec.name,
                        name_lower,
                        rec.ext,
                        rec.language,
                        rec.size as i64,
                        rec.mtime,
                        rec.volume_serial as i64,
                        rec.file_id as i64,
                        scan_generation,
                    ])?;
                    outcomes[index] = Some(FileUpsertOutcome::Renamed { id });
                } else {
                    insert.execute(params![
                        repo_id,
                        rec.path,
                        rec.rel_path,
                        rec.name,
                        name_lower,
                        rec.ext,
                        rec.language,
                        rec.size as i64,
                        rec.mtime,
                        rec.volume_serial as i64,
                        rec.file_id as i64,
                        scan_generation,
                    ])?;
                    let id = tx.last_insert_rowid();
                    outcomes[index] = Some(FileUpsertOutcome::Inserted { id });
                }
            }
        }
        tx.commit()?;
        Ok(outcomes.into_iter().map(Option::unwrap).collect())
    }

    /// Explicitly moves/renames a stored file row by `id`, preserving its `id`
    /// and all attached chunks and symbols.
    pub fn move_file(
        &mut self,
        id: i64,
        new_path: &str,
        new_rel_path: &str,
        new_name: &str,
        new_ext: &str,
        new_language: &str,
    ) -> Result<(), CodeStoreError> {
        let name_lower = new_name.to_lowercase();
        self.conn.execute(
            "UPDATE code_files SET
                 path = ?2, rel_path = ?3, name = ?4, name_lower = ?5,
                 ext = ?6, language = ?7
             WHERE id = ?1",
            params![
                id,
                new_path,
                new_rel_path,
                new_name,
                name_lower,
                new_ext,
                new_language
            ],
        )?;
        Ok(())
    }

    pub fn get_file(&self, id: i64) -> Result<Option<CodeFileRow>, CodeStoreError> {
        self.conn
            .query_row(
                "SELECT id, repo_id, path, rel_path, name, name_lower, ext, language,
                        size, mtime, volume_serial, file_id, scan_generation
                 FROM code_files WHERE id = ?1",
                params![id],
                map_code_file_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn find_file_by_path(&self, path: &str) -> Result<Option<CodeFileRow>, CodeStoreError> {
        self.conn
            .query_row(
                "SELECT id, repo_id, path, rel_path, name, name_lower, ext, language,
                        size, mtime, volume_serial, file_id, scan_generation
                 FROM code_files WHERE path = ?1",
                params![path],
                map_code_file_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn find_file_by_identity(
        &self,
        volume_serial: u32,
        file_id: u64,
    ) -> Result<Option<CodeFileRow>, CodeStoreError> {
        self.conn
            .query_row(
                "SELECT id, repo_id, path, rel_path, name, name_lower, ext, language,
                        size, mtime, volume_serial, file_id, scan_generation
                 FROM code_files WHERE volume_serial = ?1 AND file_id = ?2 LIMIT 1",
                params![volume_serial as i64, file_id as i64],
                map_code_file_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_files_for_repo(&self, repo_id: i64) -> Result<Vec<CodeFileRow>, CodeStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, repo_id, path, rel_path, name, name_lower, ext, language,
                    size, mtime, volume_serial, file_id, scan_generation
             FROM code_files WHERE repo_id = ?1 ORDER BY rel_path",
        )?;
        let rows = stmt.query_map(params![repo_id], map_code_file_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn remove_file_by_path(&mut self, path: &str) -> Result<bool, CodeStoreError> {
        let tx = self.conn.transaction()?;
        let id: Option<i64> = tx
            .query_row(
                "SELECT id FROM code_files WHERE path = ?1",
                params![path],
                |r| r.get(0),
            )
            .optional()?;
        let Some(file_id) = id else {
            return Ok(false);
        };
        delete_fts_for_file(&tx, file_id)?;
        let removed = tx.execute("DELETE FROM code_files WHERE id = ?1", params![file_id])? > 0;
        tx.commit()?;
        Ok(removed)
    }

    // ---- code chunks & vectors ----

    /// Inserts or replaces a code chunk for `(file_id, chunk_index)`, keeping a
    /// stable `code_chunks.id` and syncing `code_chunks_fts`.
    pub fn upsert_chunk(&mut self, chunk: NewCodeChunk<'_>) -> Result<i64, CodeStoreError> {
        if chunk.start_line == 0 || chunk.end_line < chunk.start_line {
            return Err(CodeStoreError::InvalidChunk(format!(
                "invalid line range {}..={}",
                chunk.start_line, chunk.end_line
            )));
        }
        let (bytes, norm) = if chunk.vector.is_empty() {
            (Vec::new(), 0.0f32)
        } else {
            let (b, n) = encode_normalized_vector(chunk.vector);
            if n == 0.0 {
                return Err(CodeStoreError::InvalidChunk(
                    "zero-length or non-finite vector".into(),
                ));
            }
            (b, n)
        };

        let tx = self.conn.transaction()?;
        let id: i64 = tx.query_row(
            "INSERT INTO code_chunks(
                 file_id, chunk_index, start_line, end_line, symbol_name, symbol_kind,
                 text, vector, norm
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(file_id, chunk_index) DO UPDATE SET
                 start_line = excluded.start_line,
                 end_line = excluded.end_line,
                 symbol_name = excluded.symbol_name,
                 symbol_kind = excluded.symbol_kind,
                 text = excluded.text,
                 vector = excluded.vector,
                 norm = excluded.norm
             RETURNING id",
            params![
                chunk.file_id,
                chunk.chunk_index,
                chunk.start_line as i64,
                chunk.end_line as i64,
                chunk.symbol_name,
                chunk.symbol_kind,
                chunk.text,
                bytes,
                norm as f64,
            ],
            |r| r.get(0),
        )?;
        tx.execute("DELETE FROM code_chunks_fts WHERE rowid = ?1", params![id])?;
        tx.execute(
            "INSERT INTO code_chunks_fts(rowid, symbol_name, text) VALUES(?1, ?2, ?3)",
            params![id, chunk.symbol_name, chunk.text],
        )?;
        tx.commit()?;
        Ok(id)
    }

    /// Deletes any chunks (and their `code_chunks_fts` entries) for `file_id`
    /// with `chunk_index >= keep`.
    pub fn truncate_chunks_for_file(
        &mut self,
        file_id: i64,
        keep: i64,
    ) -> Result<(), CodeStoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM code_chunks_fts WHERE rowid IN (
                 SELECT id FROM code_chunks WHERE file_id = ?1 AND chunk_index >= ?2
             )",
            params![file_id, keep],
        )?;
        tx.execute(
            "DELETE FROM code_chunks WHERE file_id = ?1 AND chunk_index >= ?2",
            params![file_id, keep],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Updates only the embedding vector of an existing chunk.
    pub fn set_chunk_vector(
        &mut self,
        chunk_id: i64,
        vector: &[f32],
    ) -> Result<bool, CodeStoreError> {
        let (bytes, norm) = encode_normalized_vector(vector);
        if norm == 0.0 {
            return Err(CodeStoreError::InvalidChunk(
                "zero-length or non-finite vector".into(),
            ));
        }
        let n = self.conn.execute(
            "UPDATE code_chunks SET vector = ?2, norm = ?3 WHERE id = ?1",
            params![chunk_id, bytes, norm as f64],
        )?;
        Ok(n > 0)
    }

    pub fn list_chunks_for_file(&self, file_id: i64) -> Result<Vec<CodeChunk>, CodeStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, file_id, chunk_index, start_line, end_line, symbol_name, symbol_kind,
                    text, vector, norm
             FROM code_chunks WHERE file_id = ?1 ORDER BY chunk_index",
        )?;
        let rows = stmt.query_map(params![file_id], map_code_chunk_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Brute-force FP16 cosine top-k over `code_chunks` in `code.db`.
    pub fn search_chunk_vectors(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<ScoredCodeChunk>, CodeStoreError> {
        if top_k == 0 || query.is_empty() {
            return Ok(Vec::new());
        }
        let (normed_query, query_norm) = normalize_vector(query);
        if query_norm == 0.0 {
            return Ok(Vec::new());
        }
        let mut stmt = self.conn.prepare(
            "SELECT id, file_id, chunk_index, start_line, end_line, symbol_name, symbol_kind,
                    text, vector, norm
             FROM code_chunks WHERE norm > 0",
        )?;
        let rows = stmt.query_map([], |row| {
            let bytes: Vec<u8> = row.get(8)?;
            let norm: f64 = row.get(9)?;
            let chunk = CodeChunk {
                id: row.get(0)?,
                file_id: row.get(1)?,
                chunk_index: row.get(2)?,
                start_line: row.get::<_, i64>(3)? as u32,
                end_line: row.get::<_, i64>(4)? as u32,
                symbol_name: row.get(5)?,
                symbol_kind: row.get(6)?,
                text: row.get(7)?,
                vector: decode_fp16_vector(&bytes),
                norm: norm as f32,
            };
            Ok((chunk, bytes))
        })?;

        let mut scored = Vec::new();
        for row in rows {
            let (chunk, bytes) = row?;
            if bytes.len() != normed_query.len() * 2 {
                continue;
            }
            let cos = dot_fp16_row_vs_normalized(&bytes, &normed_query);
            scored.push(ScoredCodeChunk { chunk, score: cos });
        }
        scored.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.chunk.id.cmp(&b.chunk.id))
        });
        scored.truncate(top_k);
        Ok(scored)
    }

    // ---- code symbols & FTS ----

    /// Replaces all symbols for `file_id` in one transaction and updates
    /// `code_symbols_fts` and `code_symbols_tri`.
    pub fn replace_symbols_for_file(
        &mut self,
        file_id: i64,
        symbols: &[NewCodeSymbol<'_>],
    ) -> Result<Vec<i64>, CodeStoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM code_symbols_fts WHERE rowid IN (
                 SELECT id FROM code_symbols WHERE file_id = ?1
             )",
            params![file_id],
        )?;
        tx.execute(
            "DELETE FROM code_symbols_tri WHERE rowid IN (
                 SELECT id FROM code_symbols WHERE file_id = ?1
             )",
            params![file_id],
        )?;
        tx.execute(
            "DELETE FROM code_symbols WHERE file_id = ?1",
            params![file_id],
        )?;

        let mut ids = Vec::with_capacity(symbols.len());
        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO code_symbols(
                     file_id, name, name_lower, kind, is_definition, line, end_line, signature
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            let mut fts_insert = tx.prepare_cached(
                "INSERT INTO code_symbols_fts(rowid, name, signature) VALUES(?1, ?2, ?3)",
            )?;
            let mut tri_insert =
                tx.prepare_cached("INSERT INTO code_symbols_tri(rowid, name) VALUES(?1, ?2)")?;

            for sym in symbols {
                let name_lower = sym.name.to_lowercase();
                insert.execute(params![
                    sym.file_id,
                    sym.name,
                    name_lower,
                    sym.kind,
                    sym.role.is_definition() as i64,
                    sym.line as i64,
                    sym.end_line as i64,
                    sym.signature,
                ])?;
                let id = tx.last_insert_rowid();
                fts_insert.execute(params![id, sym.name, sym.signature])?;
                tri_insert.execute(params![id, name_lower])?;
                ids.push(id);
            }
        }
        tx.commit()?;
        Ok(ids)
    }

    pub fn list_symbols_for_file(&self, file_id: i64) -> Result<Vec<CodeSymbol>, CodeStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, file_id, name, name_lower, kind, is_definition, line, end_line, signature
             FROM code_symbols WHERE file_id = ?1 ORDER BY line, id",
        )?;
        let rows = stmt.query_map(params![file_id], map_code_symbol_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Searches symbols by exact/prefix/trigram/token match, ordering
    /// definitions before references (`is_definition DESC`).
    pub fn search_symbols(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<CodeSymbol>, CodeStoreError> {
        let q = query.trim().to_lowercase();
        if q.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let mut seen = std::collections::HashSet::new();
        let mut results = Vec::new();
        fn push_unique_symbols(
            seen: &mut std::collections::HashSet<i64>,
            results: &mut Vec<CodeSymbol>,
            rows: Vec<CodeSymbol>,
        ) {
            for r in rows {
                if seen.insert(r.id) {
                    results.push(r);
                }
            }
        }

        // 1. Exact or prefix match on name_lower (definitions first)
        if let Some(upper) = prefix_upper_bound(&q) {
            let mut stmt = self.conn.prepare_cached(
                "SELECT id, file_id, name, name_lower, kind, is_definition, line, end_line, signature
                 FROM code_symbols
                 WHERE name_lower >= ?1 AND name_lower < ?2
                 ORDER BY (name_lower = ?1) DESC, is_definition DESC, length(name) ASC, line ASC
                 LIMIT ?3",
            )?;
            let rows = stmt
                .query_map(params![q, upper, limit as i64], map_code_symbol_row)?
                .collect::<Result<Vec<_>, _>>()?;
            push_unique_symbols(&mut seen, &mut results, rows);
        }

        // 2. Trigram substring match for queries >= 3 chars
        if q.chars().count() >= 3 && results.len() < limit {
            let escaped = q.replace('"', "\"\"");
            let mut stmt = self.conn.prepare_cached(
                "SELECT id, file_id, name, name_lower, kind, is_definition, line, end_line, signature
                 FROM code_symbols
                 WHERE id IN (
                     SELECT rowid FROM code_symbols_tri WHERE code_symbols_tri MATCH ?1 LIMIT 200
                 )
                 ORDER BY is_definition DESC, length(name) ASC, line ASC
                 LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![format!("\"{escaped}\""), limit as i64], map_code_symbol_row)?
                .collect::<Result<Vec<_>, _>>()?;
            push_unique_symbols(&mut seen, &mut results, rows);
        }

        // 3. FTS5 token match
        let tokens: Vec<String> = q
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
            .map(|t| format!("\"{t}\"*"))
            .collect();
        if !tokens.is_empty() && results.len() < limit {
            let expr = tokens.join(" ");
            let mut stmt = self.conn.prepare_cached(
                "SELECT id, file_id, name, name_lower, kind, is_definition, line, end_line, signature
                 FROM code_symbols
                 WHERE id IN (
                     SELECT rowid FROM code_symbols_fts WHERE code_symbols_fts MATCH ?1 LIMIT 200
                 )
                 ORDER BY is_definition DESC, length(name) ASC, line ASC
                 LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![expr, limit as i64], map_code_symbol_row)?
                .collect::<Result<Vec<_>, _>>()?;
            push_unique_symbols(&mut seen, &mut results, rows);
        }

        results.truncate(limit);
        Ok(results)
    }

    pub fn stats(&self) -> Result<CodeIndexStats, CodeStoreError> {
        let repos_total: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM repos", [], |r| r.get(0))?;
        let files_total: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM code_files", [], |r| r.get(0))?;
        let chunks_total: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM code_chunks", [], |r| r.get(0))?;
        let embedded_chunks_total: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM code_chunks WHERE norm > 0",
            [],
            |r| r.get(0),
        )?;
        let symbols_total: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM code_symbols", [], |r| r.get(0))?;
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))?;
        Ok(CodeIndexStats {
            repos_total: repos_total as u64,
            files_total: files_total as u64,
            chunks_total: chunks_total as u64,
            embedded_chunks_total: embedded_chunks_total as u64,
            symbols_total: symbols_total as u64,
            db_size_bytes: (page_count * page_size) as u64,
        })
    }
}

fn delete_fts_for_file(tx: &rusqlite::Transaction<'_>, file_id: i64) -> Result<(), rusqlite::Error> {
    tx.execute(
        "DELETE FROM code_chunks_fts WHERE rowid IN (
             SELECT id FROM code_chunks WHERE file_id = ?1
         )",
        params![file_id],
    )?;
    tx.execute(
        "DELETE FROM code_symbols_fts WHERE rowid IN (
             SELECT id FROM code_symbols WHERE file_id = ?1
         )",
        params![file_id],
    )?;
    tx.execute(
        "DELETE FROM code_symbols_tri WHERE rowid IN (
             SELECT id FROM code_symbols WHERE file_id = ?1
         )",
        params![file_id],
    )?;
    Ok(())
}

fn map_code_file_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CodeFileRow> {
    Ok(CodeFileRow {
        id: row.get(0)?,
        repo_id: row.get(1)?,
        path: row.get(2)?,
        rel_path: row.get(3)?,
        name: row.get(4)?,
        name_lower: row.get(5)?,
        ext: row.get(6)?,
        language: row.get(7)?,
        size: row.get::<_, i64>(8)? as u64,
        mtime: row.get(9)?,
        volume_serial: row.get::<_, i64>(10)? as u32,
        file_id: row.get::<_, i64>(11)? as u64,
        scan_generation: row.get(12)?,
    })
}

fn map_code_chunk_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CodeChunk> {
    let bytes: Vec<u8> = row.get(8)?;
    let norm: f64 = row.get(9)?;
    Ok(CodeChunk {
        id: row.get(0)?,
        file_id: row.get(1)?,
        chunk_index: row.get(2)?,
        start_line: row.get::<_, i64>(3)? as u32,
        end_line: row.get::<_, i64>(4)? as u32,
        symbol_name: row.get(5)?,
        symbol_kind: row.get(6)?,
        text: row.get(7)?,
        vector: decode_fp16_vector(&bytes),
        norm: norm as f32,
    })
}

fn map_code_symbol_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CodeSymbol> {
    Ok(CodeSymbol {
        id: row.get(0)?,
        file_id: row.get(1)?,
        name: row.get(2)?,
        name_lower: row.get(3)?,
        kind: row.get(4)?,
        is_definition: row.get::<_, i64>(5)? != 0,
        line: row.get::<_, i64>(6)? as u32,
        end_line: row.get::<_, i64>(7)? as u32,
        signature: row.get(8)?,
    })
}

fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    let last = chars.pop()?;
    let bumped = char::from_u32(last as u32 + 1)?;
    chars.push(bumped);
    Some(chars.into_iter().collect())
}

// ---- FP16 vector helpers (matching ws-index encoding) ----

pub fn normalize_vector(v: &[f32]) -> (Vec<f32>, f32) {
    if v.is_empty() || !v.iter().all(|x| x.is_finite()) {
        return (vec![0.0; v.len()], 0.0);
    }
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 || !norm.is_finite() {
        return (vec![0.0; v.len()], 0.0);
    }
    (v.iter().map(|x| x / norm).collect(), norm)
}

pub fn encode_normalized_vector(v: &[f32]) -> (Vec<u8>, f32) {
    let (normed, norm) = normalize_vector(v);
    let mut out = Vec::with_capacity(normed.len() * 2);
    for &x in &normed {
        out.extend_from_slice(&half::f16::from_f32(x).to_le_bytes());
    }
    (out, norm)
}

pub fn decode_fp16_vector(bytes: &[u8]) -> Vec<f32> {
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let bits = u16::from_le_bytes([chunk[0], chunk[1]]);
        out.push(half::f16::from_bits(bits).to_f32());
    }
    out
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawl::{crawl_repo, detect_repos};
    use crate::model::SymbolRole;
    use std::fs;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-ws-code-store-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn standalone_schema_and_end_to_end_crawl_with_rename_identity_preservation() {
        let dir = TempDir::new("e2e");
        let repo_dir = dir.0.join("sample-repo");
        fs::create_dir_all(repo_dir.join(".git")).unwrap();
        fs::write(
            repo_dir.join(".gitignore"),
            b"*.ignored.rs\n!kept.ignored.rs\n",
        )
        .unwrap();
        fs::create_dir_all(repo_dir.join("src")).unwrap();
        let orig_path = repo_dir.join("src").join("engine.rs");
        fs::write(&orig_path, b"pub struct SearchEngine;\n").unwrap();
        fs::write(repo_dir.join("src").join("drop.ignored.rs"), b"// ignored").unwrap();
        fs::write(repo_dir.join("src").join("kept.ignored.rs"), b"// negated").unwrap();

        let db_path = dir.0.join("code.db");
        let mut store = CodeStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), CODE_SCHEMA_VERSION);

        let repos = detect_repos(&repo_dir).unwrap();
        assert_eq!(repos.len(), 1);
        let repo_id = store.ensure_repo(&repos[0]).unwrap();

        // Generation 1 scan
        let gen1 = store.begin_generation(repo_id).unwrap();
        let crawled1 = crawl_repo(&repo_dir).unwrap();
        let outcomes1 = store.apply_file_batch(repo_id, gen1, &crawled1).unwrap();
        assert!(outcomes1
            .iter()
            .all(|o| matches!(o, FileUpsertOutcome::Inserted { .. })));

        let engine_row_before = store
            .find_file_by_path(&orig_path.to_string_lossy())
            .unwrap()
            .expect("src/engine.rs must be indexed");

        // Attach a symbol and a vector chunk to `engine.rs` before renaming it.
        store
            .replace_symbols_for_file(
                engine_row_before.id,
                &[NewCodeSymbol {
                    file_id: engine_row_before.id,
                    name: "SearchEngine",
                    kind: "struct",
                    role: SymbolRole::Definition,
                    line: 1,
                    end_line: 1,
                    signature: "pub struct SearchEngine",
                }],
            )
            .unwrap();
        let chunk_id = store
            .upsert_chunk(NewCodeChunk {
                file_id: engine_row_before.id,
                chunk_index: 0,
                start_line: 1,
                end_line: 1,
                symbol_name: "SearchEngine",
                symbol_kind: "struct",
                text: "pub struct SearchEngine;",
                vector: &[1.0, 0.0, 0.0, 0.0],
            })
            .unwrap();

        // Rename `src/engine.rs` -> `src/core_engine.rs` on disk.
        let renamed_path = repo_dir.join("src").join("core_engine.rs");
        fs::rename(&orig_path, &renamed_path).unwrap();

        // Generation 2 scan + sweep
        let gen2 = store.begin_generation(repo_id).unwrap();
        let crawled2 = crawl_repo(&repo_dir).unwrap();
        let outcomes2 = store.apply_file_batch(repo_id, gen2, &crawled2).unwrap();
        let swept = store.sweep_generation(repo_id, gen2).unwrap();
        assert_eq!(swept, 0, "renamed file must not be swept as stale");

        // Verify the rename outcome preserved the exact `code_files.id`.
        assert!(
            outcomes2
                .iter()
                .any(|o| *o == FileUpsertOutcome::Renamed { id: engine_row_before.id }),
            "expected Renamed outcome with preserved id {}, got {outcomes2:?}",
            engine_row_before.id
        );

        assert!(store
            .find_file_by_path(&orig_path.to_string_lossy())
            .unwrap()
            .is_none());
        let engine_row_after = store
            .find_file_by_path(&renamed_path.to_string_lossy())
            .unwrap()
            .expect("renamed file must be at new path");
        assert_eq!(engine_row_after.id, engine_row_before.id);
        assert_eq!(engine_row_after.rel_path, "src/core_engine.rs");
        assert_eq!(engine_row_after.name, "core_engine.rs");

        // Attached chunks and symbols survive the rename with the same file_id!
        let chunks = store.list_chunks_for_file(engine_row_after.id).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].id, chunk_id);
        let syms = store.list_symbols_for_file(engine_row_after.id).unwrap();
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "SearchEngine");
    }

    #[test]
    fn symbols_rank_definitions_above_references_and_vector_search_works() {
        let mut store = CodeStore::open_in_memory().unwrap();
        let repo_id = store
            .ensure_repo(&DetectedRepo {
                root_path: "C:\\code\\demo".into(),
                name: "demo".into(),
                has_git: true,
            })
            .unwrap();
        let gen = store.begin_generation(repo_id).unwrap();
        let outcomes = store
            .apply_file_batch(
                repo_id,
                gen,
                &[
                    CodeFileRecord {
                        path: "C:\\code\\demo\\src\\caller.rs".into(),
                        rel_path: "src/caller.rs".into(),
                        name: "caller.rs".into(),
                        ext: "rs".into(),
                        language: "rust".into(),
                        size: 100,
                        mtime: 1000,
                        volume_serial: 1,
                        file_id: 10,
                    },
                    CodeFileRecord {
                        path: "C:\\code\\demo\\src\\def.rs".into(),
                        rel_path: "src/def.rs".into(),
                        name: "def.rs".into(),
                        ext: "rs".into(),
                        language: "rust".into(),
                        size: 200,
                        mtime: 1000,
                        volume_serial: 1,
                        file_id: 20,
                    },
                ],
            )
            .unwrap();
        let caller_file_id = outcomes[0].file_id();
        let def_file_id = outcomes[1].file_id();

        store
            .replace_symbols_for_file(
                caller_file_id,
                &[NewCodeSymbol {
                    file_id: caller_file_id,
                    name: "dispatch_query",
                    kind: "call",
                    role: SymbolRole::Reference,
                    line: 42,
                    end_line: 42,
                    signature: "dispatch_query(&req)",
                }],
            )
            .unwrap();
        store
            .replace_symbols_for_file(
                def_file_id,
                &[NewCodeSymbol {
                    file_id: def_file_id,
                    name: "dispatch_query",
                    kind: "function",
                    role: SymbolRole::Definition,
                    line: 10,
                    end_line: 28,
                    signature: "pub fn dispatch_query(req: &Request)",
                }],
            )
            .unwrap();

        let hits = store.search_symbols("dispatch_query", 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits[0].is_definition, "definition must rank before reference");
        assert_eq!(hits[0].file_id, def_file_id);
        assert_eq!(hits[0].line, 10);
        assert!(!hits[1].is_definition);

        // Vector search over code chunks
        store
            .upsert_chunk(NewCodeChunk {
                file_id: caller_file_id,
                chunk_index: 0,
                start_line: 40,
                end_line: 45,
                symbol_name: "run",
                symbol_kind: "function",
                text: "fn run() { dispatch_query(&req); }",
                vector: &[0.0, 1.0, 0.0, 0.0],
            })
            .unwrap();
        store
            .upsert_chunk(NewCodeChunk {
                file_id: def_file_id,
                chunk_index: 0,
                start_line: 10,
                end_line: 28,
                symbol_name: "dispatch_query",
                symbol_kind: "function",
                text: "pub fn dispatch_query(req: &Request) {}",
                vector: &[1.0, 0.0, 0.0, 0.0],
            })
            .unwrap();

        let vhits = store.search_chunk_vectors(&[1.0, 0.1, 0.0, 0.0], 5).unwrap();
        assert_eq!(vhits.len(), 2);
        assert_eq!(vhits[0].chunk.symbol_name, "dispatch_query");
        assert!(vhits[0].score > vhits[1].score);

        let stats = store.stats().unwrap();
        assert_eq!(stats.repos_total, 1);
        assert_eq!(stats.files_total, 2);
        assert_eq!(stats.chunks_total, 2);
        assert_eq!(stats.embedded_chunks_total, 2);
        assert_eq!(stats.symbols_total, 2);
    }

    #[test]
    fn reused_old_path_preserves_moved_identity_in_either_batch_order() {
        for replacement_first in [false, true] {
            let dir = std::env::temp_dir().join(format!("ws-code-reuse-{}-{}-{}",
                std::process::id(), replacement_first,
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            fs::create_dir_all(&dir).unwrap();
            let old = dir.join("old.rs");
            let moved = dir.join("moved.rs");
            fs::write(&old, "fn original() {}").unwrap();
            let mut store = CodeStore::open_in_memory().unwrap();
            let repo = store.ensure_repo(&DetectedRepo {
                root_path: dir.to_string_lossy().into_owned(), name: "reuse".into(), has_git: false,
            }).unwrap();
            let record = crate::crawl::record_for_code_path(&dir, &old).unwrap().unwrap();
            let g1 = store.begin_generation(repo).unwrap();
            let id = store.apply_file_batch(repo, g1, &[record]).unwrap()[0].file_id();
            fs::rename(&old, &moved).unwrap();
            fs::write(&old, "fn replacement() {}").unwrap();
            let mut records = vec![
                crate::crawl::record_for_code_path(&dir, &moved).unwrap().unwrap(),
                crate::crawl::record_for_code_path(&dir, &old).unwrap().unwrap(),
            ];
            if replacement_first { records.reverse(); }
            let g2 = store.begin_generation(repo).unwrap();
            store.apply_file_batch(repo, g2, &records).unwrap();
            assert_eq!(store.find_file_by_path(&moved.to_string_lossy()).unwrap().unwrap().id, id);
            assert_ne!(store.find_file_by_path(&old.to_string_lossy()).unwrap().unwrap().id, id);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn case_only_rename_on_disk_preserves_file_row_and_symbols() {
        let dir = std::env::temp_dir().join(format!(
            "ws-code-case-rename-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let old_file = dir.join("Engine.rs");
        let new_file = dir.join("engine.rs");
        std::fs::write(&old_file, "pub struct SearchEngine;").unwrap();

        let mut store = CodeStore::open_in_memory().unwrap();
        let repo_id = store
            .ensure_repo(&DetectedRepo {
                root_path: dir.to_string_lossy().into_owned(),
                name: "demo".into(),
                has_git: true,
            })
            .unwrap();

        let g1 = store.begin_generation(repo_id).unwrap();
        let out1 = store
            .apply_file_batch(
                repo_id,
                g1,
                &[crate::crawl::record_for_code_path(&dir, &old_file)
                    .unwrap()
                    .unwrap()],
            )
            .unwrap();
        let initial_id = out1[0].file_id();
        store
            .replace_symbols_for_file(
                initial_id,
                &[NewCodeSymbol {
                    file_id: initial_id,
                    name: "SearchEngine",
                    kind: "struct",
                    role: SymbolRole::Definition,
                    line: 1,
                    end_line: 1,
                    signature: "pub struct SearchEngine;",
                }],
            )
            .unwrap();

        // On Windows, `old_file.exists()` remains true even after a case-only rename.
        std::fs::rename(&old_file, &new_file).unwrap();
        let g2 = store.begin_generation(repo_id).unwrap();
        let out2 = store
            .apply_file_batch(
                repo_id,
                g2,
                &[crate::crawl::record_for_code_path(&dir, &new_file)
                    .unwrap()
                    .unwrap()],
            )
            .unwrap();
        assert_eq!(out2, vec![FileUpsertOutcome::Renamed { id: initial_id }]);
        assert_eq!(store.sweep_generation(repo_id, g2).unwrap(), 0);
        assert_eq!(store.list_symbols_for_file(initial_id).unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
