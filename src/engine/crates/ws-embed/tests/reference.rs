//! Reference-vector test for EmbeddingGemma 2.
//!
//! `tests/fixtures/embeddinggemma2-reference.json` holds pinned input texts and
//! the vectors llama.cpp (pinned commit in the fixture) produced for them. Each
//! runtime vector must match its reference within cosine tolerance 1e-3.
//!
//! Model-gated: set `WS_EMBED_TEST_MODEL` to the EmbeddingGemma 2 text GGUF
//! (`embeddinggemma-2-Q8_0.gguf`). The model is never committed.

use std::path::PathBuf;

use serde::Deserialize;
use ws_embed::vector::{cosine, REFERENCE_COSINE_TOLERANCE};
use ws_embed::{BackendPreference, EmbedRuntime, LoadOptions};

#[derive(Deserialize)]
struct Fixture {
    model: String,
    llama_cpp_rev: String,
    pooling: String,
    dim: usize,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    text: String,
    vector: Vec<f32>,
}

fn load_fixture() -> Fixture {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("embeddinggemma2-reference.json");
    let raw = std::fs::read_to_string(&path).expect("read reference fixture");
    serde_json::from_str(&raw).expect("parse reference fixture")
}

fn model_path() -> Option<PathBuf> {
    let p = PathBuf::from(std::env::var_os("WS_EMBED_TEST_MODEL")?);
    p.is_file().then_some(p)
}

/// Fixture integrity: runs without a model, so a bad fixture fails in default CI.
#[test]
fn fixture_is_well_formed() {
    let f = load_fixture();
    assert_eq!(f.dim, 768, "EmbeddingGemma 2 output dimension");
    assert_eq!(f.pooling, "mean");
    assert!(f.model.contains("embeddinggemma-2"));
    assert_eq!(f.llama_cpp_rev.len(), 40, "pinned commit SHA");
    assert!(f.cases.len() >= 8, "enough reference cases");
    for c in &f.cases {
        assert_eq!(c.vector.len(), f.dim, "case {:?}", c.text);
        assert!(c.vector.iter().all(|x| x.is_finite()));
    }
}

/// Model-gated: every runtime vector reproduces its reference within tolerance.
#[test]
#[ignore = "requires WS_EMBED_TEST_MODEL (EmbeddingGemma 2 GGUF)"]
fn embeddinggemma2_matches_reference_vectors() {
    let Some(model) = model_path() else {
        eprintln!("skipping: WS_EMBED_TEST_MODEL not set");
        return;
    };
    let fixture = load_fixture();
    let opts = LoadOptions {
        backend: BackendPreference::Cpu,
        ..LoadOptions::default()
    };
    let mut rt = EmbedRuntime::load(&model, &opts).expect("load EmbeddingGemma 2 on CPU");
    assert_eq!(rt.n_embd(), fixture.dim, "runtime dim must match reference");

    let texts: Vec<&str> = fixture.cases.iter().map(|c| c.text.as_str()).collect();
    let got = rt.embed_texts(&texts).expect("embed reference inputs");
    assert_eq!(got.len(), fixture.cases.len());

    let mut worst = (0.0f64, String::new());
    for (case, actual) in fixture.cases.iter().zip(&got) {
        // Reference is raw (un-normalized); cosine is scale-invariant, so compare directly.
        let c = cosine(actual, &case.vector).expect("comparable vectors");
        let drift = 1.0 - c;
        if drift > worst.0 {
            worst = (drift, case.text.clone());
        }
        assert!(
            drift <= REFERENCE_COSINE_TOLERANCE,
            "reference mismatch for {:?}: cosine={c:.6}, drift={drift:.2e} > {REFERENCE_COSINE_TOLERANCE:.0e}",
            case.text
        );
    }
    eprintln!(
        "reference check: {} cases, worst drift {:.2e} ({:?})",
        fixture.cases.len(),
        worst.0,
        worst.1
    );
}
