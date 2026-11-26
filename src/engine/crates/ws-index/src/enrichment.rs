use crate::{model::ContentChunk, IndexStore, StoreError};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn initialize(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS enrichment_receipts (
      entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
      kind TEXT NOT NULL, request_key TEXT NOT NULL, source TEXT NOT NULL,
      revision INTEGER NOT NULL, model_generation INTEGER NOT NULL,
      PRIMARY KEY(entry_id,kind));",
    )?;
    Ok(())
}
pub(crate) fn preview_kind(kind: &str) -> &str {
    match kind {
        "enriched_ocr" => "ocr",
        "enriched_transcript" => "transcript",
        other => other,
    }
}
pub struct EnrichmentBatch<'a> {
    pub entry_id: i64,
    pub expected_revision: i64,
    pub model_generation: i64,
    pub kind: ws_protocol::EnrichmentKind,
    pub request_key: &'a str,
    pub source: &'a str,
    pub source_sha256: &'a str,
    pub chunks: &'a [ContentChunk],
}
#[derive(Debug, PartialEq, Eq)]
pub struct EnrichmentCommit {
    pub revision: i64,
    pub duplicate: bool,
}
impl IndexStore {
    pub fn ingest_enrichment(
        &mut self,
        batch: EnrichmentBatch<'_>,
    ) -> Result<Option<EnrichmentCommit>, StoreError> {
        let EnrichmentBatch {
            entry_id,
            expected_revision,
            model_generation,
            kind,
            request_key,
            source,
            source_sha256,
            chunks,
        } = batch;
        if !ws_protocol::valid_content_sha256(source_sha256)
            || !ws_protocol::valid_content_sha256(request_key)
            || source.is_empty()
            || chunks.is_empty()
            || chunks.len() > 256
            || chunks.iter().map(|chunk| chunk.text.len()).sum::<usize>() > 128 * 1024
            || chunks.iter().enumerate().any(|(ordinal, chunk)| {
                chunk.entry_id != entry_id
                    || chunk.kind != kind.storage_kind()
                    || chunk.job_kind != kind.storage_kind()
                    || chunk.chunk_index != ordinal as i64
                    || chunk.text.trim().is_empty()
                    || chunk.visual
                    || !chunk.embed
            })
        {
            return Err(StoreError::InvalidConfig("invalid enrichment batch".into()));
        }
        if self.content_revision(entry_id)? != expected_revision
            || self.model_generation()? != model_generation
            || !self.entry_visible(entry_id)?
        {
            return Ok(None);
        }
        let stored: Option<(String,String,i64,i64)> = self.conn.query_row("SELECT request_key,source,revision,model_generation FROM enrichment_receipts WHERE entry_id=?1 AND kind=?2",params![entry_id,kind.storage_kind()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
        if stored
            .as_ref()
            .is_some_and(|(key, proof, revision, generation)| {
                key == request_key
                    && proof == source
                    && *revision == expected_revision
                    && *generation == model_generation
            })
        {
            return Ok(Some(EnrichmentCommit {
                revision: expected_revision,
                duplicate: true,
            }));
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or(StoreError::InvalidIdentity)?;
        let tx = self.conn.transaction()?;
        tx.execute("INSERT INTO content_revisions(entry_id,revision) VALUES(?1,?2) ON CONFLICT(entry_id) DO UPDATE SET revision=excluded.revision",params![entry_id,revision])?;
        // Only separately attested cache content can share the new revision.
        // Hashing a provider source must not upgrade an unknown legacy cache.
        tx.execute("UPDATE content_progress SET revision=?3,source_sha256_revision=?3 WHERE entry_id=?1 AND revision=?2 AND source=?4 AND source_sha256=?5 AND source_sha256_revision=?2",params![entry_id,expected_revision,revision,source,source_sha256])?;
        tx.execute("UPDATE enrichment_receipts SET revision=?3 WHERE entry_id=?1 AND revision=?2 AND source=?4 AND model_generation=?5 AND EXISTS(SELECT 1 FROM content_progress p WHERE p.entry_id=enrichment_receipts.entry_id AND p.kind=enrichment_receipts.kind AND p.revision=?3 AND p.source_sha256=?6)",params![entry_id,expected_revision,revision,source,model_generation,source_sha256])?;
        tx.execute(
            "DELETE FROM content_fts WHERE entry_id=?1 AND kind=?2",
            params![entry_id, kind.storage_kind()],
        )?;
        tx.execute(
            "DELETE FROM content_chunks WHERE entry_id=?1 AND job_kind=?2",
            params![entry_id, kind.storage_kind()],
        )?;
        tx.execute(
            "DELETE FROM segments WHERE entry_id=?1 AND kind=?2",
            params![entry_id, kind.storage_kind()],
        )?;
        tx.execute("INSERT INTO content_progress(entry_id,kind,cursor,complete,revision,source,model_generation,source_sha256,source_sha256_revision) VALUES(?1,?2,?3,1,?4,?5,?6,?7,?4) ON CONFLICT(entry_id,kind) DO UPDATE SET cursor=excluded.cursor,complete=1,revision=excluded.revision,source=excluded.source,model_generation=excluded.model_generation,source_sha256=excluded.source_sha256,source_sha256_revision=excluded.revision",params![entry_id,kind.storage_kind(),chunks.len() as i64,revision,source,model_generation,source_sha256])?;
        for chunk in chunks {
            tx.execute("INSERT INTO content_chunks(entry_id,job_kind,kind,chunk_index,text,page,start_ms,end_ms,line,embed,visual,embedding_input) VALUES(?1,?2,?2,?3,?4,?5,?6,?7,?8,1,0,'\"Text\"')",params![entry_id,kind.storage_kind(),chunk.chunk_index,chunk.text,chunk.location.page,chunk.location.start_ms.map(|n|n as i64),chunk.location.end_ms.map(|n|n as i64),chunk.location.line])?;
            let id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO content_fts(rowid,entry_id,kind,text) VALUES(?1,?2,?3,?4)",
                params![id, entry_id, kind.storage_kind(), chunk.text],
            )?;
        }
        tx.execute("INSERT INTO enrichment_receipts(entry_id,kind,request_key,source,revision,model_generation) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(entry_id,kind) DO UPDATE SET request_key=excluded.request_key,source=excluded.source,revision=excluded.revision,model_generation=excluded.model_generation",params![entry_id,kind.storage_kind(),request_key,source,revision,model_generation])?;
        tx.commit()?;
        Ok(Some(EnrichmentCommit {
            revision,
            duplicate: false,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentMatch, DraftEmbeddingInput, EntryRecord};
    fn fixture() -> (IndexStore, i64, Vec<ContentChunk>) {
        let mut store = IndexStore::open_in_memory().unwrap();
        let root = store.ensure_root("C:/enrichment").unwrap();
        store
            .apply_batch(
                root,
                1,
                &[EntryRecord {
                    root_id: root,
                    path: "C:/enrichment/image.png".into(),
                    name: "image.png".into(),
                    ext: "png".into(),
                    ..Default::default()
                }],
            )
            .unwrap();
        let id = store
            .find_by_path("C:/enrichment/image.png")
            .unwrap()
            .unwrap()
            .id;
        let kind = ws_protocol::EnrichmentKind::Ocr.storage_kind();
        let chunks = vec![ContentChunk {
            id: 0,
            entry_id: id,
            job_kind: kind.into(),
            kind: kind.into(),
            chunk_index: 0,
            text: "source OCR text".into(),
            location: ContentMatch::default(),
            embed: true,
            visual: false,
            embedding_input: DraftEmbeddingInput::Text,
            attempts: 0,
        }];
        (store, id, chunks)
    }
    fn batch<'a>(
        entry: i64,
        revision: i64,
        chunks: &'a [ContentChunk],
        key: &'a str,
        hash: &'a str,
    ) -> EnrichmentBatch<'a> {
        EnrichmentBatch {
            entry_id: entry,
            expected_revision: revision,
            model_generation: 0,
            kind: ws_protocol::EnrichmentKind::Ocr,
            request_key: key,
            source: "current-source",
            source_sha256: hash,
            chunks,
        }
    }
    #[test]
    fn enrichment_commits_once_rejects_stale_claims_and_rolls_back_interrupted_writes() {
        let (mut store, id, chunks) = fixture();
        let key = "a".repeat(64);
        let hash = "b".repeat(64);
        assert_eq!(
            store
                .ingest_enrichment(batch(id, 0, &chunks, &key, &hash))
                .unwrap(),
            Some(EnrichmentCommit {
                revision: 1,
                duplicate: false
            })
        );
        assert_eq!(
            store
                .ingest_enrichment(batch(id, 1, &chunks, &key, &hash))
                .unwrap(),
            Some(EnrichmentCommit {
                revision: 1,
                duplicate: true
            })
        );
        assert_eq!(store.list_content_chunks(id).unwrap().len(), 1);
        assert!(store
            .ingest_enrichment(batch(id, 0, &chunks, &key, &hash))
            .unwrap()
            .is_none());
        store.conn.execute_batch("CREATE TRIGGER abort_enrichment BEFORE INSERT ON content_chunks WHEN NEW.job_kind='enriched_ocr' BEGIN SELECT RAISE(ABORT,'injected interruption'); END;").unwrap();
        assert!(store
            .ingest_enrichment(batch(id, 1, &chunks, &"c".repeat(64), &hash))
            .is_err());
        assert_eq!(store.content_revision(id).unwrap(), 1);
        assert_eq!(
            store.list_content_chunks(id).unwrap()[0].text,
            "source OCR text"
        );
        store.invalidate_content_source(id).unwrap();
        assert!(store
            .ingest_enrichment(batch(id, 1, &chunks, &key, &hash))
            .unwrap()
            .is_none());
        assert!(store.list_content_chunks(id).unwrap().is_empty());
    }
    #[test]
    fn native_parse_of_the_same_source_keeps_enrichment_current() {
        let (mut store, id, chunks) = fixture();
        let key = "a".repeat(64);
        let hash = "b".repeat(64);
        store
            .ingest_enrichment(batch(id, 0, &chunks, &key, &hash))
            .unwrap();
        let native = store
            .begin_content_batch(id, "image", "current-source")
            .unwrap();
        let enrichment = store.content_progress(id, "enriched_ocr").unwrap().unwrap();
        assert_eq!(enrichment.revision, native.revision);
        assert_eq!(
            store
                .ingest_enrichment(batch(id, native.revision, &chunks, &key, &hash))
                .unwrap(),
            Some(EnrichmentCommit {
                revision: native.revision,
                duplicate: true
            })
        );
    }

    #[test]
    fn provider_attestation_does_not_upgrade_unknown_legacy_cached_text() {
        let (mut store, id, chunks) = fixture();
        store
            .conn
            .execute(
                "INSERT INTO content_revisions(entry_id,revision) VALUES(?1,1)",
                [id],
            )
            .unwrap();
        store.conn.execute("INSERT INTO content_progress(entry_id,kind,cursor,complete,revision,source,model_generation) VALUES(?1,'image',1,1,1,'current-source',0)",[id]).unwrap();
        store.conn.execute("INSERT INTO content_chunks(entry_id,job_kind,kind,chunk_index,text,embed,visual,embedding_input) VALUES(?1,'image','ocr',0,'UNATTESTED_LEGACY_TEXT',0,0,'\"Text\"')",[id]).unwrap();
        store
            .ingest_enrichment(batch(id, 1, &chunks, &"a".repeat(64), &"b".repeat(64)))
            .unwrap();
        let (preview, _) = store.preview_text(id, None).unwrap();
        assert_eq!(preview.len(), 1);
        assert_eq!(preview[0].text, "source OCR text");
        assert_eq!(
            store
                .content_progress(id, "image")
                .unwrap()
                .unwrap()
                .revision,
            1
        );
    }
}
