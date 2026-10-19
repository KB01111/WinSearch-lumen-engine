//! Run by tools/test-search-parity.py --compare-client in its owned isolated job.
//! This fixture changes that instance's root policy and restores it before exit.
use std::{error::Error, time::Duration};
use ws_client::{
    protocol::{ErrorCode, IndexingConfig, SearchMode, SearchQuery, SearchResult,SearchDisposition,SemanticPhase},
    Client, EngineEvent,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

async fn search(
    client: &Client,
    text: &str,
    mode: SearchMode,
    include_semantic: bool,
) -> Result<Vec<SearchResult>> {
    search_query(client,SearchQuery {
            text: text.into(),
            mode,
            limit: 5,
            include_semantic,
            ..Default::default()
        }).await
}

async fn search_query(client:&Client,query:SearchQuery)->Result<Vec<SearchResult>> {
    let requested_semantic=query.include_semantic;
    let mut stream=client.search(query).await?;
    let mut results = Vec::new();
    let mut terminal = false;
    while let Some(frame) = stream.next().await? {
        terminal |= frame.done;
        if frame.done {
            let outcome=frame.outcome.expect("native engine must supply terminal outcomes");
            eprintln!("typed search outcome: {}",serde_json::to_string(&outcome)?);
            assert_eq!(outcome.disposition,SearchDisposition::Complete,"native acceptance requires a completed snapshot; no implicit retry");
            assert_eq!(outcome.semantic.unwrap().phase,if requested_semantic {SemanticPhase::Ready} else {SemanticPhase::Disabled});
        }
        results.extend(frame.batch);
    }
    assert!(terminal, "search must deliver a terminal frame");
    Ok(results)
}

async fn exercise(client: &Client, original: &IndexingConfig) -> Result<serde_json::Value> {
    let info = client.info().await?;
    for feature in ["search.entry_references","entry.resolve.code","entry.preview.code","search.filters","search.outcomes","search.related"] {assert!(info.supports(feature));}
    let status = client.status().await?;
    let model = client.model_status().await?;
    let engine_config = client.config().await?;
    let mut events = client.subscribe().await?;
    assert!(matches!(events.next().await?, Some(EngineEvent::Status(_))));
    let mut paused = original.clone();
    paused.paused = true;
    assert!(client.set_indexing_config(paused).await?.paused);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(events.next().await?, Some(EngineEvent::IndexingConfig(config)) if config.paused) {
                return Ok::<_, ws_client::Error>(());
            }
        }
    }).await??;
    client.set_indexing_config(original.clone()).await?;
    drop(events);

    let document = search(client, "amberparitytoken", SearchMode::Files, false).await?;
    if document.is_empty() {
        let status=client.status().await?;
        let repeated=search(client,"amberparitytoken",SearchMode::Files,false).await?;
        eprintln!("document admission diagnostic: {}",serde_json::json!({"status":status,"repeated":repeated}));
    }
    assert!(
        !document.is_empty(),
        "actual document extraction must be ready"
    );
    let code = search(client, "parityneedle", SearchMode::Code, false).await?;
    assert!(
        code.iter().any(|hit| hit.location.is_some()),
        "actual code locations must survive IPC"
    );
    let mut extension_filters=Vec::new();
    for (text,mode,extension,expected) in [
        ("pdfparitytoken",SearchMode::Files,".PDF",Some("item-b.pdf")),
        ("pdfparitytoken",SearchMode::Files,"txt",None),
        ("parityneedle",SearchMode::Code,"rs",Some("源 code.rs")),
        ("parityneedle",SearchMode::Code,"py",None),
    ] {
        let hits=search_query(client,SearchQuery {text:text.into(),mode,limit:5,
            extensions:vec![extension.into()],..Default::default()}).await?;
        if let Some(name)=expected {assert!(hits.iter().any(|hit|hit.path.as_deref()
            .is_some_and(|path|std::path::Path::new(path).file_name().unwrap()==name)));}
        else {assert!(hits.is_empty());}
        extension_filters.push(serde_json::json!({"query":text,"extension":extension,"results":hits}));
    }
    let mut file_kind_filters=Vec::new();
    for (text,mode,kind,expected) in [
        ("pdfparitytoken",SearchMode::Files,ws_client::protocol::FileKind::Pdf,Some("item-b.pdf")),
        ("pdfparitytoken",SearchMode::Files,ws_client::protocol::FileKind::Document,None),
        ("parityneedle",SearchMode::Code,ws_client::protocol::FileKind::Source,Some("源 code.rs")),
        ("parityneedle",SearchMode::Code,ws_client::protocol::FileKind::Pdf,None),
        ("notebookparity",SearchMode::Code,ws_client::protocol::FileKind::Unknown,Some("cells.ipynb")),
        ("notebookparity",SearchMode::Code,ws_client::protocol::FileKind::Source,None),
    ] {
        let hits=search_query(client,SearchQuery {text:text.into(),mode,limit:5,file_kinds:vec![kind],..Default::default()}).await?;
        if let Some(name)=expected {assert!(hits.iter().any(|hit|hit.path.as_deref()
            .is_some_and(|path|std::path::Path::new(path).file_name().unwrap()==name)));}
        else {assert!(hits.is_empty());}
        file_kind_filters.push(serde_json::json!({"query":text,"file_kind":kind,"results":hits}));
    }
    let semantic_code = if engine_config.semantic_enabled {
        let query = "a function named parityneedle and its caller";
        assert!(search(client, query, SearchMode::Code, false).await?.is_empty());
        let hits = search(client, query, SearchMode::Code, true).await?;
        assert!(
            !hits.is_empty(),
            "semantic-only code must work through the typed client"
        );
        assert!(search(
            client,
            "asteroid orbital mechanics and interstellar propulsion",
            SearchMode::Code,
            true,
        ).await?.is_empty());
        Some(hits)
    } else {
        None
    };
    let reference=document[0].entry_reference.clone().expect("use the engine's bound document reference");
    assert!(reference.is_bound());
    let entry = client.resolve_entry(reference.clone()).await?;
    assert!(entry.index_revision.is_some_and(|revision| revision > 0));
    assert_eq!(client.resolve_entry(reference.clone()).await?, entry);
    let notebook=search(client,"notebookparity",SearchMode::Code,false).await?;
    assert!(!notebook.is_empty());
    let mut code_entries=Vec::new();
    for hit in code.iter().chain(notebook.iter()).chain(semantic_code.iter().flatten()) {
        let reference=hit.entry_reference.clone().expect("query-specific code IDs must not be parsed by consumers");
        assert!(reference.is_bound() && reference.result_id.starts_with('c'));
        let resolved=client.resolve_entry(reference).await?;
        assert_eq!(resolved.path,hit.path.clone().unwrap());
        assert!(resolved.index_revision.is_some_and(|n|n>0));
        assert!(resolved.content_sha256.as_deref().is_some_and(ws_client::protocol::valid_content_sha256));
        code_entries.push(resolved);
    }
    let code_reference=code[0].entry_reference.clone().unwrap();
    let document_preview = client.preview_text(ws_client::protocol::PreviewRequest {
        reference: reference.clone(),
        location: Some(ws_client::protocol::MatchLocation::Line { line: 2 }),
    }).await?;
    assert!(document_preview.parts.iter().any(|part| part.text.contains("amberparitytoken")));
    assert!(document_preview.entry.content_sha256.as_deref().is_some_and(ws_client::protocol::valid_content_sha256));
    assert!(document_preview.extraction.iter().all(|progress| progress.source_units.is_none() && progress.capped.is_none()));
    let pdf = search(client, "pdfparitytoken", SearchMode::Files, false).await?;
    let pdf_preview = client.preview_text(ws_client::protocol::PreviewRequest {
        reference: pdf[0].entry_reference.clone().expect("use the engine's PDF reference"),
        location: Some(ws_client::protocol::MatchLocation::Page { page: 2 }),
    }).await?;
    assert!(pdf_preview.parts.iter().any(|part| part.text.contains("pdfparitytoken")
        && part.location == Some(ws_client::protocol::MatchLocation::Page { page: 2 })));
    assert!(pdf_preview.entry.content_sha256.as_deref().is_some_and(ws_client::protocol::valid_content_sha256));
    let code_preview=client.preview_text(ws_client::protocol::PreviewRequest {
        reference:code_reference.clone(),location:code[0].location,
    }).await?;
    assert!(code_preview.parts.iter().any(|part|part.kind=="code" && part.text.contains("parityneedle")));
    let notebook_preview=client.preview_text(ws_client::protocol::PreviewRequest {
        reference:notebook[0].entry_reference.clone().unwrap(),location:notebook[0].location,
    }).await?;
    assert!(notebook_preview.parts.iter().any(|part|part.text.contains("notebookparity")
        && matches!(part.location,Some(ws_client::protocol::MatchLocation::Notebook {cell_ordinal:1,line:1,end_line:2}))));
    assert!(notebook_preview.parts.iter().all(|part|!part.text.contains("PRIVATE_OUTPUT_SENTINEL")));

    let mut related_checks=Vec::new();
    for source in [&reference,&code_reference] {
        let mut stream=client.related(ws_client::protocol::RelatedQuery {source:source.clone(),limit:5,
            categories:Vec::new(),extensions:Vec::new(),file_kinds:Vec::new()}).await?;
        if engine_config.semantic_enabled {
            let mut frames=Vec::new();
            while let Some(frame)=stream.next().await? {
                assert!(frame.batch.iter().all(|hit|hit.entry_reference.as_ref().is_some_and(|target|
                    target.is_bound() && target.result_id!=source.result_id && target.result_id.as_bytes()[0]==source.result_id.as_bytes()[0])));
                if frame.done {
                    let outcome=frame.outcome.unwrap();assert_eq!(outcome.disposition,SearchDisposition::Complete);
                    assert_eq!(outcome.semantic.unwrap().phase,SemanticPhase::Ready);
                }
                frames.push(serde_json::json!({"results":frame.batch,"done":frame.done,"outcome":frame.outcome}));
            }
            assert!(frames.last().is_some_and(|frame|frame["done"]==true));
            related_checks.push(serde_json::json!({"source":source,"frames":frames}));
        } else {
            assert!(matches!(stream.next().await,Err(ws_client::Error::Remote {code:ErrorCode::NotReady,..})),
                "Related must not become ordinary exact search without semantic inference");
            related_checks.push(serde_json::json!({"source":source,"error":"not_ready"}));
        }
    }

    // Disconnect before consuming results, then reconnect for an independent operation.
    drop(
        client
            .search(SearchQuery {
                text: "amberparitytoken".into(),
                mode: SearchMode::Files,
                limit: 5,
                ..Default::default()
            })
            .await?,
    );
    client.status().await?;

    let mut revoked = original.clone();
    revoked.roots.clear();
    revoked.code_enabled=false;
    client.set_indexing_config(revoked).await?;
    assert!(matches!(client.preview_text(ws_client::protocol::PreviewRequest {
        reference:code_reference.clone(),location:code[0].location,
    }).await,Err(ws_client::Error::Remote {code:ErrorCode::NotReady,..})));
    assert!(matches!(client.preview_text(ws_client::protocol::PreviewRequest {
        reference: reference.clone(), location: None,
    }).await, Err(ws_client::Error::Remote { code: ErrorCode::NotReady, .. })));
    assert!(matches!(
        client.resolve_entry(reference.clone()).await,
        Err(ws_client::Error::Remote {
            code: ErrorCode::NotReady,
            ..
        })
    ));
    assert!(search(client, "amberparitytoken", SearchMode::Files, false)
        .await?
        .is_empty());
    client.set_indexing_config(original.clone()).await?;
    assert!(matches!(client.preview_text(ws_client::protocol::PreviewRequest {
        reference:code_reference.clone(),location:code[0].location,
    }).await,Err(ws_client::Error::Remote {code:ErrorCode::NotReady,..})));
    assert!(matches!(client.resolve_entry(code_reference).await,
        Err(ws_client::Error::Remote {code:ErrorCode::NotReady,..})));
    assert!(matches!(client.preview_text(ws_client::protocol::PreviewRequest {
        reference: reference.clone(), location: None,
    }).await, Err(ws_client::Error::Remote { code: ErrorCode::NotReady, .. })));
    assert!(matches!(
        client.resolve_entry(reference).await,
        Err(ws_client::Error::Remote {
            code: ErrorCode::NotReady,
            ..
        })
    ));
    let fresh_hits=search(client,"amberparitytoken",SearchMode::Files,false).await?;
    assert!(!fresh_hits.is_empty(),"root restoration must preserve committed exact document text without a model or a retry");
    let fresh=client.resolve_entry(fresh_hits[0].entry_reference.clone().unwrap()).await?;
    assert_ne!(
        fresh.source_revision, entry.source_revision,
        "revoked references must not revive"
    );
    Ok(serde_json::json!({
        "engine_version": info.engine_version, "features": info.features,
        "initial_status": status, "model_status": model, "engine_config": engine_config,
        "document": document, "code": code, "semantic_code": semantic_code,
        "extension_filters":extension_filters,
        "file_kind_filters":file_kind_filters,
        "extension_checks":["pdf_extension_and_case", "excluded_document_extension", "code_extension", "excluded_code_extension"],
        "outcome_checks":["advertised_terminal_state_required", "completed_snapshot_without_retry", "actual_semantic_phase"],
        "related_checks":related_checks,
        "notebook":notebook,"code_entries":code_entries,
        "reference_checks":["bound_document_and_pdf_refs", "bound_code_and_notebook_refs",
            "resolved_code_hashes", "code_proof_stays_invalid_after_restore"],
        "document_preview": document_preview, "pdf_preview": pdf_preview,
        "code_preview":code_preview,"notebook_preview":notebook_preview,
        "code_preview_checks":["actual_cached_code", "actual_notebook_cell_and_no_outputs",
            "code_preview_rejects_revoked_root", "code_preview_proof_stays_invalid_after_restore"],
        "preview_checks": ["current_metadata_bound_document_text", "actual_pdf_page_two",
            "preview_rejects_revoked_root", "preview_proof_stays_invalid_after_restore"],
        "semantic_checks": if semantic_code.is_some() { vec!["exact_only_control_empty", "semantic_only_code", "semantic_negative_empty"] } else { vec![] },
        "entry_size_bytes": fresh.size_bytes, "entry_index_revision": fresh.index_revision,
        "checks": ["typed_status_and_configuration", "paused_policy_pushed_subscription",
            "native_document_and_code_locations", "current_entry_source_proof",
            "drop_disconnect_then_reconnect", "root_revocation_hides_results",
            "revoked_proof_stays_invalid_after_restore", "fresh_reference_after_restore"]
    }))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let instance = std::env::var("WINSEARCH_INSTANCE_ID")?;
    let pipe = std::env::var("WS_PIPE")?;
    let data = std::env::var("WINSEARCH_DATA_DIR")?;
    assert!(
        instance.starts_with("parity-")
            && pipe.ends_with(&format!("-{instance}"))
            && std::path::Path::new(&data).is_absolute(),
        "this fixture only operates on the harness's explicit isolated instance"
    );
    let client = Client::new(pipe)?;
    let original = client.indexing_config().await?;
    assert!(
        !original.roots.is_empty(),
        "harness must admit its generated roots first"
    );
    let result = exercise(&client, &original).await;
    let restored = client.set_indexing_config(original.clone()).await;
    let report = result?;
    assert_eq!(restored?, original, "restore the original fixture policy");
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
