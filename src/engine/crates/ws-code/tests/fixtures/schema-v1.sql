-- Frozen schema from WinSearch commit 1c0445f, before native code parsing.

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
