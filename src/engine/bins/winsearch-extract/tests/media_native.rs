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
    run_with_plan(name,kind,cursor,units,None)
}

fn run_with_plan(
    name: &str,
    kind: &str,
    cursor: u64,
    units: u32,
    plan: Option<ws_extract::worker::MediaBatch>,
) -> ExtractResponse {
    let mut response = run_raw(name, kind, cursor, units, plan.clone(), None);
    if kind == "video" && plan.is_none() && cursor == 0 {
        while let Some(state) = response.video_selection.clone() {
            if let Some(plan) = response.media.clone() {
                response = run_raw(name, kind, 0, units, Some(plan), None);
                break;
            }
            response = run_raw(
                name,
                kind,
                response.next_cursor.unwrap(),
                units,
                None,
                Some(state),
            );
        }
    }
    response
}

fn run_raw(
    name: &str,
    kind: &str,
    cursor: u64,
    units: u32,
    plan: Option<ws_extract::worker::MediaBatch>,
    selection: Option<ws_extract::worker::VideoSelection>,
) -> ExtractResponse {
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
        media_plan: plan,
        video_selection: selection,
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

#[test]
fn video_native_short_visual_track_keeps_full_soundtrack() {
    let repo = root();
    let name = format!("short-visual-long-audio-{}.mp4", std::process::id());
    let source = repo.join("artifacts/media-fixtures").join(&name);
    let decoder=repo.join("artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin/ffmpeg.exe");
    let generated = Command::new(decoder)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-n",
            "-f",
            "lavfi",
            "-i",
            "color=c=0x486878:s=160x100:r=1:d=8",
            "-i",
        ])
        .arg(repo.join("artifacts/spoken-fixtures/two-passages-65s.wav"))
        .args([
            "-map", "0:v:0", "-map", "1:a:0", "-c:v", "mpeg4", "-q:v", "4", "-threads", "2",
            "-c:a", "aac", "-t", "65",
        ])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let mut response = run_raw(&name, "video", 0, 1, None, None);
    for _ in 0..5 {
        if response.media.is_some() {
            break;
        }
        let state = response.video_selection.clone().unwrap();
        response = run_raw(
            &name,
            "video",
            response.next_cursor.unwrap(),
            1,
            None,
            Some(state),
        );
    }
    let plan = response
        .media
        .expect("selection must finish while only audio remains");
    assert_eq!(plan.duration_ms, 65_000);
    assert!(plan.has_audio);
    assert_eq!(plan.frames_ms.last(), Some(&7_000));
    let audio = run_raw(
        &name,
        "video",
        ws_extract::video::audio_cursor(2),
        1,
        Some(plan),
        None,
    );
    assert!(audio
        .output
        .segments
        .iter()
        .any(|segment| segment.kind == "audio"
            && segment.start_ms == Some(50_000)
            && segment.end_ms == Some(65_000)));
    std::fs::remove_file(source).unwrap();
}

#[test]
fn video_native_commits_selection_before_rendering_frames() {
    let first = run_raw("時刻 clip.mp4", "video", 0, 1, None, None);
    assert_eq!(first.output.status, ExtractionStatus::Extracted);
    assert!(
        first.output.segments.is_empty(),
        "initial scan must not render frames"
    );
    assert!(
        first.media.is_none(),
        "a partial scan must not freeze the plan"
    );
    let mut state = first.video_selection.unwrap();
    assert_eq!(state.next_ms, 15_000);
    assert!(state.sample_count > 0);
    let mut cursor = first.next_cursor.unwrap();
    let mut batches = 1;
    loop {
        let response = run_raw(
            "時刻 clip.mp4",
            "video",
            cursor,
            1,
            None,
            Some(state.clone()),
        );
        batches += 1;
        assert!(response.output.segments.is_empty());
        if let Some(plan) = response.media {
            assert_eq!(response.next_cursor, Some(0));
            assert!(plan.frames_ms.iter().any(|t| t.abs_diff(32750) <= 500));
            assert!(batches > 1);
            let rendered = run_raw("時刻 clip.mp4", "video", 0, 1, Some(plan), None);
            assert!(rendered.output.segments.iter().any(|s| s.kind == "video"));
            break;
        }
        state = response.video_selection.unwrap();
        cursor = response.next_cursor.unwrap();
        assert_eq!(cursor, ws_extract::video::selection_cursor(state.next_ms));
    }
}

#[test]
fn video_native_keeps_nonperiodic_shot_and_deferred_audio_phase() {
    let first = run("時刻 clip.mp4", "video", 0, 30);
    assert_eq!(first.output.status, ExtractionStatus::Extracted);
    let frames: Vec<_> = first
        .output
        .segments
        .iter()
        .filter(|s| {
            matches!(
                s.embedding_input,
                ws_extract::DraftEmbeddingInput::Image { .. }
            )
        })
        .collect();
    assert!(
        !frames.is_empty(),
        "a video must produce genuine visual inputs"
    );
    assert!(
        frames.iter().any(|s| s
            .start_ms
            .is_some_and(|t| t.abs_diff(32750) <= 500 && t % 5000 != 0)),
        "the mid-interval shot transition must survive selection"
    );
    assert!(frames
        .windows(2)
        .all(|pair| pair[1].start_ms.unwrap() - pair[0].start_ms.unwrap() > 500));
    assert!(frames.len() <= 1200);
    for frame in frames {
        let ws_extract::DraftEmbeddingInput::Image { path, token_budget } = &frame.embedding_input
        else {
            unreachable!()
        };
        assert!(matches!(token_budget, 70 | 140));
        let raster = ws_platform::wic::decode_image_wic(path, 1280).unwrap();
        assert!(raster.scaled_width <= 1280 && raster.scaled_height <= 1280);
    }
    assert!(
        first.next_cursor.is_some(),
        "a full soundtrack needs a durable subsequent phase"
    );
}

#[test]
fn video_native_without_audio_still_emits_searchable_frames() {
    let response = run("no audio clip.mp4", "video", 0, 30);
    assert_eq!(response.output.status, ExtractionStatus::Extracted);
    assert!(response.output.segments.iter().any(|s| matches!(
        s.embedding_input,
        ws_extract::DraftEmbeddingInput::Image { .. }
    )));
    assert!(response.output.segments.iter().all(|s| s.kind != "audio"));
}

#[test]
fn video_native_restart_keeps_selection_and_all_soundtrack_windows() {
    let mut response=run("時刻 clip.mp4","video",0,2);
    let plan=response.media.clone().unwrap();
    let mut times=Vec::new();
    let mut ocr=String::new();
    loop {
        assert_eq!(response.media.as_ref(),Some(&plan));
        assert_eq!(response.output.status,ExtractionStatus::Extracted);
        for draft in &response.output.segments {
            assert_ne!(draft.kind,"audio","visual and soundtrack phases remain distinct");
            if matches!(draft.embedding_input,ws_extract::DraftEmbeddingInput::Image { .. }) {
                times.push(draft.start_ms.unwrap());
            }
            if draft.kind=="ocr" { ocr.push_str(&draft.text); ocr.push(' '); }
        }
        let next=response.next_cursor.expect("soundtrack follows every visual frame");
        if next==ws_extract::video::audio_cursor(0) {
            response=run_with_plan("時刻 clip.mp4","video",next,1,Some(plan.clone()));
            break;
        }
        response=run_with_plan("時刻 clip.mp4","video",next,2,Some(plan.clone()));
    }
    assert_eq!(times,plan.frames_ms,"restarts must not duplicate/omit selected frames");
    println!("actual native video OCR: {ocr:?}");
    let mut windows=Vec::new();
    loop {
        assert_eq!(response.output.status,ExtractionStatus::Extracted);
        assert_eq!(response.media.as_ref(),Some(&plan));
        for draft in &response.output.segments {
            assert_eq!(draft.kind,"audio");
            assert!(draft.text.is_empty());
            windows.push((draft.start_ms.unwrap(),draft.end_ms.unwrap()));
        }
        let Some(next)=response.next_cursor else { break; };
        response=run_with_plan("時刻 clip.mp4","video",next,1,Some(plan.clone()));
    }
    assert_eq!(windows,vec![(0,30000),(25000,55000),(50000,65000)]);
}

#[test]
fn video_native_short_soundtrack_finishes_without_fake_tail_vectors() {
    let first=run("short audio track.mp4","video",0,1);
    let plan=first.media.unwrap();
    let audio=run_with_plan("short audio track.mp4","video",ws_extract::video::audio_cursor(0),30,Some(plan));
    assert_eq!(audio.output.status,ExtractionStatus::Extracted);
    assert_eq!(audio.next_cursor,None);
    assert_eq!(audio.output.segments.len(),1,"all trailing windows are genuinely silent");
    assert_eq!((audio.output.segments[0].start_ms,audio.output.segments[0].end_ms),(Some(0),Some(30000)));
}

#[test]
fn video_native_late_soundtrack_preserves_pcm_position_inside_window() {
    let first=run("late audio track.mp4","video",0,1);
    let audio=run_with_plan("late audio track.mp4","video",ws_extract::video::audio_cursor(1),1,first.media);
    assert_eq!(audio.output.status,ExtractionStatus::Extracted);
    let draft=&audio.output.segments[0];
    assert_eq!((draft.start_ms,draft.end_ms),(Some(25000),Some(55000)));
    let ws_extract::DraftEmbeddingInput::Audio { path }=&draft.embedding_input else { panic!("audio"); };
    assert_eq!(ws_extract::audio::validate_window_wav(path).unwrap(),480000);
    let bytes=std::fs::read(path).unwrap();
    let first_energy=bytes[44..].chunks_exact(2).position(|p| i16::from_le_bytes([p[0],p[1]]).unsigned_abs()>300).unwrap();
    assert!((14*16000..17*16000).contains(&first_energy),"late speech shifted within source window: {first_energy}");
}

#[test]
fn video_native_variable_rate_uses_actual_pts_and_frame_ocr_is_real() {
    let variable=run("variable rate.mp4","video",0,30);
    assert_eq!(variable.output.status,ExtractionStatus::Extracted);
    let times=&variable.media.as_ref().unwrap().frames_ms;
    assert!(times.iter().all(|t| [0,233,1566,2300,4666,6700].contains(t)), "{times:?}");
    assert!(times.contains(&1566) && times.contains(&6700),"{times:?}");
    let screen=run("screen OCR.mp4","video",0,30);
    assert_eq!(screen.output.status,ExtractionStatus::Extracted);
    let text=screen.output.segments.iter().filter(|s| s.kind=="ocr").map(|s|s.text.as_str()).collect::<Vec<_>>().join(" ");
    println!("actual generated screen OCR: {text}");
    assert!(text.contains("E4821") && text.contains("DEVICE"),"{text}");
    assert!(screen.output.segments.iter().filter(|s|s.kind=="video").all(|s|
        matches!(s.embedding_input,ws_extract::DraftEmbeddingInput::Image { token_budget:140,.. })));
}
