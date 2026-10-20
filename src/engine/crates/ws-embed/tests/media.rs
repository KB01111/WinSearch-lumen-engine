//! Native CPU tests using independently pinned public synthetic inputs/vectors.
use std::path::PathBuf;
use ws_embed::vector::cosine;
use ws_embed::{BackendPreference, EmbedRuntime, LoadOptions};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/media")
        .join(name)
}
fn runtime() -> EmbedRuntime {
    let model =
        PathBuf::from(std::env::var_os("WS_EMBED_TEST_MODEL").expect("WS_EMBED_TEST_MODEL"));
    let mmproj = model.with_file_name("mmproj-embeddinggemma-2-Q8_0.gguf");
    EmbedRuntime::load(
        &model,
        &LoadOptions {
            backend: BackendPreference::Cpu,
            n_threads: 8,
            mmproj: Some(mmproj),
        },
    )
    .expect("CPU multimodal model")
}
fn image(rt: &mut EmbedRuntime, name: &str) -> Vec<f32> {
    let img = ws_platform::wic::decode_image_wic(&fixture(name), 4096).unwrap();
    rt.embed_image(
        &ws_platform::wic::bgra8_to_rgb8(&img.bgra8),
        img.scaled_width,
        img.scaled_height,
    )
    .unwrap()
}
fn reference(name: &str) -> Vec<f32> {
    let raw = std::fs::read(fixture("reference-media-vectors.json")).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    serde_json::from_value(json["vectors"][name].clone()).unwrap()
}
#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL and sibling public mmproj"]
fn distinct_images_and_pinned_screen_reference() {
    let mut rt = runtime();
    let screen = image(&mut rt, "screen.png");
    let photo = image(&mut rt, "photo.jpg");
    let similarity = cosine(&screen, &photo).unwrap();
    eprintln!("screen/photo cosine={similarity:.10}");
    assert!(
        similarity < 0.95,
        "unrelated images collapsed: {similarity}"
    );
    let agreement = cosine(&screen, &reference("screen-280")).unwrap();
    eprintln!("screen-280 reference cosine={agreement:.10}");
    assert!(
        agreement >= 0.999,
        "independent image reference mismatch: {agreement}"
    );
    assert_eq!(screen, image(&mut rt, "screen.png"));
}
#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL and sibling public mmproj"]
fn public_synthetic_audio_matches_pinned_reference() {
    let mut rt = runtime();
    for invalid in [
        vec![],
        vec![f32::NAN],
        vec![f32::INFINITY],
        vec![0.0; 480_001],
    ] {
        assert!(rt.embed_audio(&invalid).is_err());
    }
    let wav = std::fs::read(fixture("speech.wav")).unwrap();
    let pcm: Vec<f32> = wav[44..]
        .chunks_exact(2)
        .map(|s| i16::from_le_bytes([s[0], s[1]]) as f32 / 32768.0)
        .collect();
    let actual = rt.embed_audio(&pcm).expect("real PCM audio embedding");
    let agreement = cosine(&actual, &reference("speech")).unwrap();
    eprintln!("synthetic-speech reference cosine={agreement:.10}");
    assert!(
        agreement >= 0.999,
        "independent audio reference mismatch: {agreement}"
    );
    assert_eq!(actual, rt.embed_audio(&pcm).unwrap());
}

#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL and sibling public mmproj"]
fn reduced_frame_budgets_match_independent_references() {
    let mut rt = runtime();
    let img = ws_platform::wic::decode_image_wic(&fixture("photo.jpg"), 4096).unwrap();
    let rgb = ws_platform::wic::bgra8_to_rgb8(&img.bgra8);
    for (budget, name) in [(140, "photo-140"), (70, "frame-70")] {
        let vector = rt
            .embed_image_with_budget(&rgb, img.scaled_width, img.scaled_height, budget)
            .unwrap();
        let agreement = cosine(&vector, &reference(name)).unwrap();
        eprintln!("{name} reference cosine={agreement:.10}");
        assert!(
            agreement >= 0.999,
            "independent {name} mismatch: {agreement}"
        );
    }
    assert!(rt
        .embed_image_with_budget(&rgb, img.scaled_width, img.scaled_height, 71)
        .is_err());
}
