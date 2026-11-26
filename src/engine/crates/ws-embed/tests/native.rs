//! Native integration tests.
//!
//! Tests here link the real llama.cpp build. The model-gated tests read the
//! model path from `WS_EMBED_TEST_MODEL` and skip when it is unset, so no model
//! file is committed to the repository.

use std::path::PathBuf;

use ws_embed::backends::BackendKind;
use ws_embed::runtime::{available_kinds, enumerate_devices, init_backends};
use ws_embed::{EmbedRuntime, LoadOptions};

fn model_path() -> Option<PathBuf> {
    let p = std::env::var_os("WS_EMBED_TEST_MODEL")?;
    let p = PathBuf::from(p);
    if p.is_file() {
        Some(p)
    } else {
        eprintln!("WS_EMBED_TEST_MODEL set but not a file: {}", p.display());
        None
    }
}

/// Native smoke test: the DLLs load and the CPU device is enumerated.
/// Needs no model.
#[test]
fn backends_load_and_cpu_device_is_present() {
    init_backends();
    let devices = enumerate_devices();
    let kinds = available_kinds(&devices);
    assert!(
        kinds.contains(&BackendKind::Cpu),
        "CPU backend must always be present; found {kinds:?}"
    );
}

/// Model-gated: load on CPU, embed, check dimension and unit norm, release.
#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL (run with --ignored)"]
fn cpu_load_embed_release_roundtrip() {
    let Some(model) = model_path() else {
        eprintln!("skipping: WS_EMBED_TEST_MODEL not set");
        return;
    };
    let opts = LoadOptions {
        backend: ws_embed::BackendPreference::Cpu,
        ..LoadOptions::default()
    };
    let mut rt = EmbedRuntime::load(&model, &opts).expect("load on CPU");
    assert_eq!(rt.backend(), BackendKind::Cpu);
    assert!(rt.n_embd() > 0);

    let vecs = rt
        .embed_texts(&["budget spreadsheet", "receipt from the hardware store"])
        .expect("embed");
    assert_eq!(vecs.len(), 2);
    for v in &vecs {
        assert_eq!(v.len(), rt.n_embd());
        let norm: f64 = v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        assert!((norm - 1.0).abs() < 1e-3, "vector not unit-norm: {norm}");
    }
    rt.release_gpu().expect("release_gpu on CPU is a no-op");
}

/// Model-gated: identical input must give identical output (determinism).
#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL (run with --ignored)"]
fn embedding_is_deterministic_on_cpu() {
    let Some(model) = model_path() else {
        eprintln!("skipping: WS_EMBED_TEST_MODEL not set");
        return;
    };
    let opts = LoadOptions {
        backend: ws_embed::BackendPreference::Cpu,
        ..LoadOptions::default()
    };
    let mut rt = EmbedRuntime::load(&model, &opts).expect("load");
    let a = rt.embed_texts(&["hello world"]).expect("embed a");
    let b = rt.embed_texts(&["hello world"]).expect("embed b");
    assert_eq!(a, b);
}

/// Missing model file is a typed error, not a crash.
#[test]
fn missing_model_is_reported() {
    let err = EmbedRuntime::load(
        &PathBuf::from("does-not-exist.gguf"),
        &LoadOptions::default(),
    )
    .err()
    .expect("must fail");
    assert!(matches!(err, ws_embed::EmbedError::ModelNotFound(_)));
}
