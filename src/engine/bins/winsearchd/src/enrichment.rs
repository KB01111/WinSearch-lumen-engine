use crate::engine::EngineService;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use ws_protocol::{EnrichmentIngested, EnrichmentKind, EnrichmentRequest, ErrorCode};

pub(crate) fn ingest(
    engine: &EngineService,
    request: &EnrichmentRequest,
    cancel: &Arc<AtomicBool>,
) -> Result<EnrichmentIngested, ErrorCode> {
    ingest_after_attestation(engine, request, cancel, || {})
}
fn ingest_after_attestation(
    engine: &EngineService,
    request: &EnrichmentRequest,
    cancel: &Arc<AtomicBool>,
    after_attestation: impl FnOnce(),
) -> Result<EnrichmentIngested, ErrorCode> {
    if !request.is_valid() {
        return Err(ErrorCode::InvalidRequest);
    }
    let entry = engine.resolve_entry_snapshot(&request.reference)?;
    let path = Path::new(&entry.path);
    let kind = ws_extract::modality_kind_for_ext(
        path.extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default(),
    );
    if !matches!(
        (request.kind, kind),
        (EnrichmentKind::Ocr, Some("image")) | (EnrichmentKind::Transcript, Some("audio"))
    ) {
        return Err(ErrorCode::Unsupported);
    }
    let epoch = engine.indexer.policy_epoch();
    let current = || {
        !cancel.load(Ordering::Acquire)
            && engine.indexer.policy_epoch() == epoch
            && engine.indexer.indexing_pause_reason().is_none()
            && engine.indexer.content_path_allowed(path)
    };
    if !current() {
        return Err(ErrorCode::NotReady);
    }
    let id = match request.reference.indexed_id() {
        Some(ws_protocol::IndexedEntryId::File(id)) => id,
        _ => return Err(ErrorCode::InvalidRequest),
    };
    let (revision, generation, duration) = {
        let store = engine.indexer.store_lock();
        if store.database_id() != request.index_id {
            return Err(ErrorCode::NotReady);
        }
        let duration = store
            .content_progress(id, "audio")
            .map_err(|_| ErrorCode::Internal)?
            .and_then(|p| p.media_plan.map(|plan| plan.duration_ms));
        (
            store
                .content_revision(id)
                .map_err(|_| ErrorCode::Internal)?,
            store.model_generation().map_err(|_| ErrorCode::Internal)?,
            duration,
        )
    };
    if generation != request.model_generation {
        return Err(ErrorCode::NotReady);
    }
    for part in &request.parts {
        if let Some(ws_protocol::MatchLocation::Timestamp { end_seconds, .. }) = part.location {
            if duration.is_none_or(|duration| end_seconds * 1000.0 > duration as f64) {
                return Err(ErrorCode::InvalidRequest);
            }
        }
    }
    let guard =
        ws_platform::source_guard::SourceReadGuard::open(path).ok_or(ErrorCode::NotReady)?;
    let source = crate::extract::source_snapshot(path);
    let budget = ws_extract::ExtractBudget::new(
        ws_extract::source::MAX_SOURCE_HASH_BYTES,
        Duration::from_secs(10),
        256,
    );
    let hash = ws_extract::source::source_sha256_while(path, &budget, current)
        .map_err(|_| ErrorCode::NotReady)?
        .ok_or(ErrorCode::NotReady)?;
    if hash != request.expected_sha256
        || !current()
        || !guard.intact()
        || crate::extract::source_snapshot(path) != source
    {
        return Err(ErrorCode::NotReady);
    }
    after_attestation();
    engine.resolve_entry_snapshot(&request.reference)?;
    let key_bytes = serde_json::to_vec(&(
        &request.index_id,
        &request.reference.result_id,
        &request.expected_sha256,
        request.kind,
        request.model_generation,
        &request.parts,
    ))
    .map_err(|_| ErrorCode::InvalidRequest)?;
    let key: String = Sha256::digest(key_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut chunks = Vec::new();
    for part in &request.parts {
        for text in ws_extract::chunk::chunk_text_default(&part.text) {
            let (start_ms, end_ms) = match part.location {
                Some(ws_protocol::MatchLocation::Timestamp {
                    seconds,
                    end_seconds,
                }) => (
                    Some((seconds * 1000.0) as u64),
                    Some((end_seconds * 1000.0) as u64),
                ),
                _ => (None, None),
            };
            chunks.push(ws_index::model::ContentChunk {
                id: 0,
                entry_id: id,
                job_kind: request.kind.storage_kind().into(),
                kind: request.kind.storage_kind().into(),
                chunk_index: chunks.len() as i64,
                text,
                location: ws_index::model::ContentMatch {
                    start_ms,
                    end_ms,
                    ..Default::default()
                },
                embed: true,
                visual: false,
                embedding_input: ws_index::model::DraftEmbeddingInput::Text,
                attempts: 0,
            });
        }
    }
    let committed = {
        let mut store = engine.indexer.store_lock();
        if !current() || !guard.intact() || store.database_id() != request.index_id {
            return Err(ErrorCode::NotReady);
        }
        store
            .ingest_enrichment(ws_index::EnrichmentBatch {
                entry_id: id,
                expected_revision: revision,
                model_generation: generation,
                kind: request.kind,
                request_key: &key,
                source: &source,
                source_sha256: &hash,
                chunks: &chunks,
            })
            .map_err(|_| ErrorCode::Internal)?
            .ok_or(ErrorCode::NotReady)?
    };
    engine.indexer.invalidate_vector_cache();
    if !current() || !guard.intact() {
        return Err(ErrorCode::NotReady);
    }
    let refreshed = engine.resolve_entry_snapshot(&ws_protocol::EntryReference {
        result_id: request.reference.result_id.clone(),
        source_revision: None,
    })?;
    Ok(EnrichmentIngested {
        index_id: request.index_id.clone(),
        reference: ws_protocol::EntryReference {
            result_id: request.reference.result_id.clone(),
            source_revision: Some(refreshed.source_revision),
        },
        content_revision: committed.revision,
        model_generation: generation,
        grant_generation: request.grant_generation,
        duplicate: committed.duplicate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    fn fixture() -> (Arc<EngineService>, std::path::PathBuf, EnrichmentRequest) {
        let root = std::env::temp_dir().join(format!(
            "winsearch-enrichment-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let source = root.join("source.png");
        std::fs::write(&source, b"generated source byte fixture").unwrap();
        let indexer = crate::indexer::Indexer::open(
            ws_index::IndexStore::open_in_memory().unwrap(),
            std::slice::from_ref(&root),
        )
        .unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(16) > 0 {}
        let engine = EngineService::new_with_model_dir(indexer.clone(), root.join("models"));
        let store = indexer.store_lock();
        let id = store
            .find_by_path(&source.to_string_lossy())
            .unwrap()
            .unwrap()
            .id;
        let index_id = store.database_id().to_owned();
        let generation = store.model_generation().unwrap();
        drop(store);
        let entry = engine
            .resolve_entry_snapshot(&ws_protocol::EntryReference {
                result_id: format!("e{id}"),
                source_revision: None,
            })
            .unwrap();
        let hash = Sha256::digest(std::fs::read(source).unwrap())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let request = EnrichmentRequest {
            index_id,
            reference: ws_protocol::EntryReference {
                result_id: entry.result_id,
                source_revision: Some(entry.source_revision),
            },
            expected_sha256: hash,
            model_generation: generation,
            grant_generation: 7,
            kind: EnrichmentKind::Ocr,
            parts: vec![ws_protocol::EnrichmentPart {
                text: "consented fixture OCR text".into(),
                location: None,
            }],
        };
        (engine, root, request)
    }
    #[test]
    fn enrichment_attests_current_bytes_and_retries_without_duplicate_content() {
        let (engine, root, mut request) = fixture();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut wrong = request.clone();
        wrong.expected_sha256 = "f".repeat(64);
        assert!(ingest(&engine, &wrong, &cancel).is_err());
        wrong = request.clone();
        wrong.index_id = "f".repeat(32);
        assert!(ingest(&engine, &wrong, &cancel).is_err());
        let first = ingest(&engine, &request, &cancel).unwrap();
        assert!(!first.duplicate);
        assert!(
            ingest(&engine, &request, &cancel).is_err(),
            "old source proof must expire after revision change"
        );
        request.reference = first.reference;
        let duplicate = ingest(&engine, &request, &cancel).unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(first.content_revision, duplicate.content_revision);
        assert_eq!(
            engine
                .indexer
                .store_lock()
                .list_content_chunks(
                    request
                        .reference
                        .indexed_id()
                        .map(|id| match id {
                            ws_protocol::IndexedEntryId::File(id) => id,
                            _ => 0,
                        })
                        .unwrap()
                )
                .unwrap()
                .len(),
            1
        );
        let mut policy = engine.indexer.get_indexing_config().unwrap();
        policy.roots.clear();
        engine.indexer.set_indexing_config(&policy).unwrap();
        assert!(ingest(&engine, &request, &cancel).is_err());
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn enrichment_rechecks_source_root_model_and_cancel_after_attestation_before_commit() {
        for change in ["source", "root", "model", "cancel"] {
            let (engine, root, request) = fixture();
            let cancel = Arc::new(AtomicBool::new(false));
            let result = ingest_after_attestation(&engine, &request, &cancel, || match change {
                "source" => {
                    std::fs::write(root.join("source.png"), b"changed fixture bytes").unwrap()
                }
                "root" => {
                    let mut policy = engine.indexer.get_indexing_config().unwrap();
                    policy.roots.clear();
                    engine.indexer.set_indexing_config(&policy).unwrap();
                }
                "model" => {
                    engine.indexer.store_lock().reset_content_vectors().unwrap();
                }
                _ => cancel.store(true, Ordering::Release),
            });
            assert!(result.is_err(), "stale {change} was ingested");
            let mut policy = engine.indexer.get_indexing_config().unwrap();
            policy.roots.clear();
            engine.indexer.set_indexing_config(&policy).unwrap();
            drop(engine);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn enrichment_delivery_rechecks_grant_cancel_model_and_request_identity() {
        use ws_protocol::ServerMessage;
        let (engine, root, request) = fixture();
        let cancel = engine.begin_request_for_client(17, 1);
        let epoch = engine.indexer.policy_epoch();
        let result = ingest(&engine, &request, &cancel).unwrap();
        let deliver =
            || engine.enrichment_response_for_delivery(17, 1, &cancel, epoch, Ok(result.clone()));
        assert!(matches!(
            deliver(),
            Some(ServerMessage::EnrichmentIngested { .. })
        ));
        cancel.store(true, Ordering::Release);
        assert!(matches!(
            deliver(),
            Some(ServerMessage::Error {
                code: ErrorCode::Cancelled,
                ..
            })
        ));
        cancel.store(false, Ordering::Release);
        engine.indexer.store_lock().reset_content_vectors().unwrap();
        assert!(matches!(
            deliver(),
            Some(ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            })
        ));
        let mut policy = engine.indexer.get_indexing_config().unwrap();
        policy.roots.clear();
        engine.indexer.set_indexing_config(&policy).unwrap();
        assert!(matches!(
            deliver(),
            Some(ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            })
        ));
        let _replacement = engine.begin_request_for_client(17, 1);
        assert!(deliver().is_none());
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn public_enrichment_pipe_returns_a_bound_revision_and_current_cached_ocr_preview() {
        use ws_protocol::{read_message, write_message, ClientMessage, ServerMessage};
        let (engine, root, request) = fixture();
        let name = format!(
            r"\\.\pipe\winsearch-enrichment-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let serving = name.clone();
        let handler = engine.clone();
        std::thread::spawn(move || {
            let _ = ws_platform::pipe::serve(&serving, move |stream| {
                crate::server::handle_connection(stream, handler.clone())
            });
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut peer = loop {
            match ws_platform::pipe::PipeStream::connect(&name) {
                Ok(peer) => break peer,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => panic!("{error}"),
            }
        };
        write_message(
            &mut peer,
            &ClientMessage::Hello {
                protocol_version: 1,
                client: "enrichment-fixture".into(),
                client_version: "1".into(),
            },
        )
        .unwrap();
        let Some(ServerMessage::HelloAck { features, .. }) =
            read_message::<_, ServerMessage>(&mut peer).unwrap()
        else {
            panic!("handshake")
        };
        assert!(features.iter().any(|feature| feature == "entry.enrichment"));
        let _ = read_message::<_, ServerMessage>(&mut peer).unwrap();
        write_message(
            &mut peer,
            &ClientMessage::IngestEnrichment { id: 11, request },
        )
        .unwrap();
        let Some(ServerMessage::EnrichmentIngested { id: 11, result }) =
            read_message::<_, ServerMessage>(&mut peer).unwrap()
        else {
            panic!("ingestion acknowledgment")
        };
        assert!(result.reference.is_bound());
        write_message(
            &mut peer,
            &ClientMessage::PreviewEntry {
                id: 12,
                request: ws_protocol::PreviewRequest {
                    reference: result.reference,
                    location: None,
                },
            },
        )
        .unwrap();
        let Some(ServerMessage::EntryPreview { id: 12, preview }) =
            read_message::<_, ServerMessage>(&mut peer).unwrap()
        else {
            panic!("provider text preview")
        };
        assert_eq!(preview.state, ws_protocol::PreviewState::Ready);
        assert_eq!(preview.parts[0].kind, "ocr");
        assert_eq!(preview.parts[0].text, "consented fixture OCR text");
        assert!(preview.entry.content_sha256.is_some());
        let mut policy = engine.indexer.get_indexing_config().unwrap();
        policy.roots.clear();
        engine.indexer.set_indexing_config(&policy).unwrap();
        drop(peer);
        std::fs::remove_dir_all(root).unwrap();
    }
}
