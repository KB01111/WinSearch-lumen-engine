//! Actual CLI process against controlled public-pipe frames, independent of indexing.
use std::{process::Command, sync::mpsc, time::Duration};
use ws_protocol::{
    read_message, write_message, ClientMessage, ErrorCode, MatchLocation, MatchingMoment,
    ResultKind, SearchResult, ServerMessage, PROTOCOL_VERSION,
    SearchDisposition,SearchOutcome,SemanticIssue,SemanticOutcome,
};

fn run_search(
    label: &str,
    reply: impl Fn(u64) -> Vec<ServerMessage> + Send + Sync + 'static,
) -> std::process::Output {
    run_search_with_features(label,vec![],reply)
}

fn run_search_with_features(label:&str,features:Vec<String>,reply:impl Fn(u64)->Vec<ServerMessage>+Send+Sync+'static)->std::process::Output {
    run_search_with_args(label, features,
        &["search", "needle", "--json", "--semantic", "--mode", "code"],
        move |id, _| reply(id))
}

fn run_search_with_args(
    label: &str,
    features: Vec<String>,
    args: &[&str],
    reply: impl Fn(u64, ws_protocol::SearchQuery) -> Vec<ServerMessage> + Send + Sync + 'static,
) -> std::process::Output {
    let pipe = format!(
        r"\\.\pipe\winsearch-cli-json-{}-{label}",
        std::process::id()
    );
    let server_pipe = pipe.clone();
    let (ready, wait) = mpsc::channel();
    std::thread::spawn(move || {
        ready.send(()).unwrap();
        let _ = ws_platform::pipe::serve(&server_pipe, move |mut stream| {
            assert!(matches!(
                read_message::<_, ClientMessage>(&mut stream).unwrap(),
                Some(ClientMessage::Hello { .. })
            ));
            write_message(
                &mut stream,
                &ServerMessage::HelloAck {
                    protocol_version: PROTOCOL_VERSION,
                    engine_version: "test".into(),
                    features: features.clone(),
                },
            )
            .unwrap();
            if let Some(ClientMessage::Search { id, query }) = read_message(&mut stream).unwrap() {
                // An unrelated request's frame must not leak into the CLI JSON.
                write_message(
                    &mut stream,
                    &ServerMessage::Results { outcome:None,
                        id: id + 99,
                        batch: vec![hit("unrelated")],
                        done: true,
                    },
                )
                .unwrap();
                for message in reply(id, query) {
                    write_message(&mut stream, &message).unwrap();
                }
            }
        });
    });
    wait.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    Command::new(env!("CARGO_BIN_EXE_winsearch-cli"))
        .args(args)
        .env("WS_PIPE", pipe)
        .output()
        .unwrap()
}

#[test]
fn json_extension_options_reach_the_wire_without_changing_query_or_output() {
    let (sent, received) = mpsc::channel();
    let output = run_search_with_args("extensions-wire", vec!["search.filters".into()],
        &["search", "needle", "--json", "--extension", ".RS", "--extension=pdf", "--limit", "3"],
        move |id, query| {
            sent.send(query).unwrap();
            vec![ServerMessage::Results { id, batch: vec![hit("selected")], done: true, outcome: None }]
        });
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let query = received.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(query.text, "needle");
    assert_eq!(query.extensions, vec![".RS", "pdf"]);
    assert_eq!(query.limit, 3);
    assert_eq!(serde_json::from_slice::<Vec<SearchResult>>(&output.stdout).unwrap(), vec![hit("selected")]);
}

#[test]
fn json_extension_options_fail_closed_when_the_peer_has_no_filter_capability() {
    let (sent, received) = mpsc::channel();
    let output = run_search_with_args("extensions-legacy", vec![],
        &["search", "needle", "--json", "--extension", "rs"],
        move |id, query| {
            sent.send(query).unwrap();
            vec![ServerMessage::Results { id, batch: vec![], done: true, outcome: None }]
        });
    assert!(!output.status.success(), "an unsupported filter cannot silently become an unfiltered query");
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("search.filters"));
    assert!(received.try_recv().is_err(), "the filtered request must not be sent to a legacy peer");
}

#[test]
fn json_extension_options_reject_unselected_or_pathless_results_in_any_batch() {
    for case in 0..8 {
        let output = run_search_with_args(&format!("extensions-invalid-{case}"), vec!["search.filters".into()],
            &["search", "needle", "--json", "--extension", "rs"],
            move |id, _| {
                let mut invalid = hit("unselected");
                match case % 4 {
                    0 => invalid.path = Some(r"C:\generated\unselected.png".into()),
                    1 => invalid.path = None,
                    2 => invalid.kind = ResultKind::Folder,
                    _ => invalid.path = Some(r"C:\generated\no-extension".into()),
                }
                if case < 4 {
                    vec![
                        ServerMessage::Results { id, batch: vec![hit("partial")], done: false, outcome: None },
                        ServerMessage::Results { id, batch: vec![invalid], done: true, outcome: None },
                    ]
                } else {
                    vec![
                        ServerMessage::Results { id, batch: vec![invalid], done: false, outcome: None },
                        ServerMessage::Results { id, batch: vec![hit("selected")], done: true, outcome: None },
                    ]
                }
            });
        assert!(!output.status.success(), "case {case} cannot publish unselected results");
        assert!(output.stdout.is_empty(), "a rejected final batch cannot publish earlier partial hits as success");
        assert!(String::from_utf8_lossy(&output.stderr).contains("extension filter"));
    }
}

fn hit(id: &str) -> SearchResult {
    SearchResult {
        id: id.into(),
        kind: ResultKind::Code,
        title: "源 code.rs".into(),
        subtitle: r"C:\generated\源 code.rs".into(),
        path: Some(r"C:\generated\源 code.rs".into()),
        location: Some(MatchLocation::Line { line: 7 }),
        excerpt: Some("fn needle() {}".into()),
        score: 0.95,
        matching_moments: vec![MatchingMoment {
            excerpt: "definition".into(),
            location: Some(MatchLocation::Line { line: 7 }),
            thumbnail_path: None,
        }],
        ..Default::default()
    }
}

#[test]
fn json_preserves_order_and_complete_dtos_across_progressive_batches() {
    let output = run_search("progressive", |id| {
        vec![
            ServerMessage::Results { outcome:None,
                id,
                batch: vec![hit("exact")],
                done: false,
            },
            ServerMessage::Results { outcome:None,
                id,
                batch: vec![hit("semantic")],
                done: true,
            },
        ]
    });
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Vec<SearchResult> =
        serde_json::from_slice(&output.stdout).expect("--json must emit only a JSON result array");
    assert_eq!(actual, vec![hit("exact"), hit("semantic")]);
}

#[test]
fn json_rejection_is_an_empty_array() {
    let output = run_search("empty", |id| {
        vec![ServerMessage::Results { outcome:None,
            id,
            batch: vec![],
            done: true,
        }]
    });
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Vec<SearchResult>>(&output.stdout).unwrap(),
        vec![]
    );
}

#[test]
fn json_server_error_fails_without_a_success_payload() {
    let output = run_search("error", |id| {
        vec![
            ServerMessage::Results { outcome:None,
                id,
                batch: vec![hit("partial")],
                done: false,
            },
            ServerMessage::Error {
                id: Some(id),
                code: ErrorCode::Cancelled,
                message: "cancelled test request".into(),
            },
        ]
    });
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cancelled test request"));
}

#[test]
fn json_requires_advertised_terminal_outcome_and_rejects_premature_or_malformed_state() {
    for case in 0..3 {
        let output=run_search_with_features(&format!("bad-outcome-{case}"),vec!["search.outcomes".into()],move |id| {
            let ready=Some(SearchOutcome::complete(SemanticOutcome::READY));
            if case==1 {return vec![ServerMessage::Results {id,batch:vec![],done:false,outcome:ready},
                ServerMessage::Results {id,batch:vec![],done:true,outcome:ready}];}
            vec![ServerMessage::Results {id,batch:vec![],done:true,outcome:if case==0 {None} else {
                Some(SearchOutcome::interrupted(SearchDisposition::Complete))}}]
        });
        assert!(!output.status.success(),"case{case} must reject invalid outcome");
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn json_does_not_publish_partial_hits_as_success_after_interruption() {
    for disposition in [SearchDisposition::Superseded,SearchDisposition::Canceled,SearchDisposition::Failed] {
        let output=run_search_with_features(&format!("interrupted-{disposition:?}"),vec!["search.outcomes".into()],move |id|vec![
            ServerMessage::Results {id,batch:vec![hit("partial")],done:false,outcome:None},
            ServerMessage::Results {id,batch:vec![],done:true,outcome:Some(SearchOutcome::interrupted(disposition))},
        ]);
        assert!(!output.status.success(),"{disposition:?} cannot be reported as a successful result array");
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).to_lowercase().contains(&format!("{disposition:?}").to_lowercase()));
    }
}

#[test]
fn json_preserves_exact_hits_and_reports_bounded_optional_semantic_failure() {
    let output=run_search_with_features("degraded-outcome",vec!["search.outcomes".into()],|id|vec![
        ServerMessage::Results {id,batch:vec![hit("exact")],done:false,outcome:None},
        ServerMessage::Results {id,batch:vec![],done:true,outcome:Some(SearchOutcome::complete(
            SemanticOutcome::degraded(SemanticIssue::EmbeddingFailed)))},
    ]);
    assert!(output.status.success());
    assert_eq!(serde_json::from_slice::<Vec<SearchResult>>(&output.stdout).unwrap(),vec![hit("exact")]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("embedding_failed"));
}
