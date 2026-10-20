//! Run one CPU thread-count probe against the pinned public media fixtures.
use std::{io::Write, path::PathBuf, time::Instant};
use ws_embed::{vector::cosine, BackendPreference, EmbedRuntime, LoadOptions};

fn record(threads: i32, phase: &str, start: Instant, agreement: Option<f64>) {
    println!(
        "{}",
        serde_json::json!({
            "threads": threads, "phase": phase, "seconds": start.elapsed().as_secs_f64(),
            "reference_cosine": agreement
        })
    );
    std::io::stdout().flush().unwrap();
}

fn main() {
    let argv: Vec<_> = std::env::args_os().skip(1).collect();
    assert_eq!(
        argv.len(),
        3,
        "media-throughput MODEL FIXTURE_DIRECTORY THREADS"
    );
    let model = PathBuf::from(&argv[0]);
    let fixtures = PathBuf::from(&argv[1]);
    let threads: i32 = argv[2].to_str().unwrap().parse().unwrap();
    assert!((1..=64).contains(&threads));
    let start = Instant::now();
    let mut rt = EmbedRuntime::load(
        &model,
        &LoadOptions {
            backend: BackendPreference::Cpu,
            n_threads: threads,
            mmproj: Some(model.with_file_name("mmproj-embeddinggemma-2-Q8_0.gguf")),
        },
    )
    .expect("real CPU multimodal runtime");
    record(threads, "load", start, None);
    let reference: serde_json::Value = serde_json::from_slice(
        &std::fs::read(fixtures.join("reference-media-vectors.json")).unwrap(),
    )
    .unwrap();
    let image = ws_platform::wic::decode_image_wic(&fixtures.join("screen.png"), 4096).unwrap();
    let start = Instant::now();
    let vector = rt
        .embed_image(
            &ws_platform::wic::bgra8_to_rgb8(&image.bgra8),
            image.scaled_width,
            image.scaled_height,
        )
        .unwrap();
    let expected: Vec<f32> =
        serde_json::from_value(reference["vectors"]["screen-280"].clone()).unwrap();
    let agreement = cosine(&vector, &expected).unwrap();
    record(threads, "image280", start, Some(agreement));
    assert!(agreement >= 0.999);
    let wav = std::fs::read(fixtures.join("speech.wav")).unwrap();
    // This benchmark consumes the committed canonical PCM fixture, not arbitrary WAVs.
    assert_eq!(&wav[0..4], b"RIFF");
    assert_eq!(&wav[8..16], b"WAVEfmt ");
    assert_eq!(&wav[20..24], &[1, 0, 1, 0]);
    assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16000);
    assert_eq!(&wav[36..40], b"data");
    assert_eq!(
        wav.len() - 44,
        u32::from_le_bytes(wav[40..44].try_into().unwrap()) as usize
    );
    let mut pcm: Vec<f32> = wav[44..]
        .chunks_exact(2)
        .map(|s| i16::from_le_bytes([s[0], s[1]]) as f32 / 32768.0)
        .collect();
    for phase in ["speech", "speech_repeat"] {
        let start = Instant::now();
        let vector = rt.embed_audio(&pcm).unwrap();
        let expected: Vec<f32> =
            serde_json::from_value(reference["vectors"]["speech"].clone()).unwrap();
        let agreement = cosine(&vector, &expected).unwrap();
        record(threads, phase, start, Some(agreement));
        assert!(agreement >= 0.999);
    }
    pcm.resize(480_000, 0.0);
    let start = Instant::now();
    let vector = rt.embed_audio(&pcm).unwrap();
    assert_eq!(vector.len(), 768);
    assert!(vector.iter().all(|v| v.is_finite()));
    assert!((vector.iter().map(|v| v * v).sum::<f32>() - 1.0).abs() < 1e-3);
    record(threads, "speech_padded30s", start, None);
}
