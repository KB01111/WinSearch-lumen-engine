//! Requesting real inference must never quietly select a fixture embedder.
use std::{
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn run_eval(stub: bool) -> std::process::Output {
    let token = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let data = std::env::temp_dir().join(format!("winsearch-eval-provenance-{token}"));
    std::fs::create_dir(&data).unwrap();
    let model = data.join("requested.gguf");
    if stub {
        // Stub mode bypasses model parsing: the handshake must still reject it.
        std::fs::write(&model, b"invalid model must not be hidden by stub output").unwrap();
    }
    let suite = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../evals/names.json");
    let output = Command::new(env!("CARGO_BIN_EXE_winsearch-cli"))
        .args(["eval", suite.to_str().unwrap()])
        .env("WINSEARCH_DATA_DIR", &data)
        .env("WINSEARCH_INSTANCE_ID", format!("eval-{token}"))
        .env("WS_EMBED_TEST_MODEL", &model)
        .env("WS_EMBED_STUB", if stub { "1" } else { "0" })
        .env("WS_EMBED_BACKEND", "cpu")
        .output()
        .unwrap();
    assert!(data.parent() == Some(std::env::temp_dir().as_path()));
    std::fs::remove_dir_all(data).unwrap();
    output
}

#[test]
fn explicitly_missing_model_is_an_error_not_a_passing_fixture_run() {
    let output = run_eval(false);
    assert!(
        !output.status.success(),
        "missing real model was accepted: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("WS_EMBED_TEST_MODEL"),
        "missing explicit model was not diagnosed: {error}"
    );
}

#[test]
fn requested_real_model_rejects_stub_worker_provenance() {
    let output = run_eval(true);
    assert!(
        !output.status.success(),
        "requested real run accepted stub vectors"
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("stub"),
        "expected a provenance error before ranking, got: {error}"
    );
}
