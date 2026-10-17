//! Real decoder/worker tests; generated public/offline fixtures, no model stub.
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use ws_extract::worker::{
    encode_request, validate_response, ExtractRequest, ExtractResponse, WorkerHello,
};
use ws_extract::ExtractionStatus;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .unwrap()
}

fn run(name: &str, kind: &str, cursor: u64, units: u32) -> ExtractResponse {
    let repo = root();
    let source = if name == "two-passages-65s.wav" {
        "spoken-fixtures"
    } else {
        "media-fixtures"
    };
    let scratch = repo.join("artifacts/media-native").join(format!(
        "{}-{kind}-{cursor}-{units}-{}",
        std::process::id(),
        name.replace('.', "-")
    ));
    std::fs::create_dir_all(&scratch).unwrap();
    let request = ExtractRequest {
        path: repo.join("artifacts").join(source).join(name),
        kind: kind.into(),
        scratch_dir: scratch,
        cursor,
        max_units: units,
    };
    assert!(
        request.path.is_file(),
        "generate offline media fixtures first: {:?}",
        request.path
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_winsearch-extract"))
        .env("WINSEARCH_TEST_DECODER_DIR", repo.join("artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin"))
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&encode_request(&request).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "worker failed: {output:?}");
    let mut bytes = output.stdout.as_slice();
    let _: WorkerHello = ws_protocol::read_message(&mut bytes).unwrap().unwrap();
    let response: ExtractResponse = ws_protocol::read_message(&mut bytes).unwrap().unwrap();
    validate_response(&request, &response).unwrap();
    response
}

#[test]
fn audio_native_windows_keep_source_times_and_resume_in_new_worker() {
    let first = run("two-passages-65s.wav", "audio", 0, 1);
    assert_eq!(
        first.output.status,
        ExtractionStatus::Extracted,
        "audio must use the real decoder"
    );
    assert_eq!(first.total_units, Some(3));
    assert_eq!(first.next_cursor, Some(1));
    assert_eq!(first.output.segments.len(), 1);
    let second = run("two-passages-65s.wav", "audio", 1, 1);
    let third = run("two-passages-65s.wav", "audio", 2, 1);
    assert_eq!(second.next_cursor, Some(2));
    assert_eq!(third.next_cursor, None);
    for (response, start, end) in [
        (&first, 0, 30000),
        (&second, 25000, 55000),
        (&third, 50000, 65000),
    ] {
        assert_eq!(response.output.status, ExtractionStatus::Extracted);
        let draft = &response.output.segments[0];
        assert_eq!((draft.start_ms, draft.end_ms), (Some(start), Some(end)));
        assert!(
            draft.text.is_empty(),
            "decoding must not fabricate a transcript"
        );
        assert!(draft.embed);
    }
}

#[test]
fn audio_native_silence_completes_without_fake_drafts() {
    let response = run("-silence.wav", "audio", 0, 30);
    assert_eq!(response.output.status, ExtractionStatus::Extracted);
    assert_eq!(response.total_units, Some(1));
    assert_eq!(response.next_cursor, None);
    assert!(response.output.segments.is_empty());
}

#[test]
fn audio_native_compressed_audio_routes_without_video() {
    let response = run("audio only.m4a", "video", 0, 30);
    assert_eq!(response.output.status, ExtractionStatus::Extracted);
    assert_eq!(response.output.segments.len(), 3);
    assert!(response
        .output
        .segments
        .iter()
        .all(|s| s.kind == "audio" && s.start_ms.is_some() && s.text.is_empty()));
}

#[test]
fn audio_native_long_silence_advances_across_batches_without_giant_artifacts() {
    let name = format!("long silence-{}.wav", std::process::id());
    let source = root().join("artifacts/media-fixtures").join(&name);
    let size = 1200u32 * 16000 * 2;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&source)
        .unwrap();
    let mut header = Vec::new();
    header.extend_from_slice(b"RIFF");
    header.extend_from_slice(&(size + 36).to_le_bytes());
    header.extend_from_slice(b"WAVEfmt ");
    header.extend_from_slice(&16u32.to_le_bytes());
    header.extend_from_slice(&1u16.to_le_bytes());
    header.extend_from_slice(&1u16.to_le_bytes());
    header.extend_from_slice(&16000u32.to_le_bytes());
    header.extend_from_slice(&32000u32.to_le_bytes());
    header.extend_from_slice(&2u16.to_le_bytes());
    header.extend_from_slice(&16u16.to_le_bytes());
    header.extend_from_slice(b"data");
    header.extend_from_slice(&size.to_le_bytes());
    file.write_all(&header).unwrap();
    file.set_len(u64::from(size) + 44).unwrap();
    drop(file);
    let first = run(&name, "audio", 0, 30);
    assert_eq!(first.output.status, ExtractionStatus::Extracted);
    assert_eq!(first.total_units, Some(48));
    assert_eq!(first.next_cursor, Some(30));
    assert!(first.output.segments.is_empty());
    let second = run(&name, "audio", 30, 30);
    assert_eq!(second.output.status, ExtractionStatus::Extracted);
    assert_eq!(second.next_cursor, None);
    assert!(second.output.segments.is_empty());
    std::fs::remove_file(source).unwrap();
}

#[test]
fn audio_native_disguised_playlist_cannot_connect_to_network() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let name = format!("disguised playlist-{}.wav", std::process::id());
    let source = root().join("artifacts/media-fixtures").join(&name);
    std::fs::write(&source, format!("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:30\n#EXTINF:30,\nhttp://{}/segment.ts\n#EXT-X-ENDLIST\n", listener.local_addr().unwrap())).unwrap();
    let response = run(&name, "audio", 0, 30);
    assert!(
        matches!(response.output.status, ExtractionStatus::Failed(_)),
        "{response:?}"
    );
    assert!(response.output.segments.is_empty());
    assert!(matches!(listener.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock));
    std::fs::remove_file(source).unwrap();
}
