//! Actual CLI process against controlled public-pipe frames, independent of indexing.
use std::{process::Command, sync::mpsc, time::Duration};
use ws_protocol::{
    read_message, write_message, ClientMessage, ErrorCode, MatchLocation, MatchingMoment,
    ResultKind, SearchResult, ServerMessage, PROTOCOL_VERSION,
};

fn run_search(
    label: &str,
    reply: impl Fn(u64) -> Vec<ServerMessage> + Send + Sync + 'static,
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
                    features: vec![],
                },
            )
            .unwrap();
            if let Some(ClientMessage::Search { id, .. }) = read_message(&mut stream).unwrap() {
                // An unrelated request's frame must not leak into the CLI JSON.
                write_message(
                    &mut stream,
                    &ServerMessage::Results {
                        id: id + 99,
                        batch: vec![hit("unrelated")],
                        done: true,
                    },
                )
                .unwrap();
                for message in reply(id) {
                    write_message(&mut stream, &message).unwrap();
                }
            }
        });
    });
    wait.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    Command::new(env!("CARGO_BIN_EXE_winsearch-cli"))
        .args(["search", "needle", "--json", "--semantic", "--mode", "code"])
        .env("WS_PIPE", pipe)
        .output()
        .unwrap()
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
            ServerMessage::Results {
                id,
                batch: vec![hit("exact")],
                done: false,
            },
            ServerMessage::Results {
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
        vec![ServerMessage::Results {
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
            ServerMessage::Results {
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
