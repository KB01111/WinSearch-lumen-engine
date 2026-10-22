//! An explicit export with a fixed allowlist; never serialize raw status/errors.

use crate::ipc::{DaemonClient, IpcError, CLI_VERSION};
use serde_json::{json, Value};

fn backend(value: &str) -> Option<&str> {
    matches!(value, "auto" | "cpu" | "npu" | "cuda" | "vulkan").then_some(value)
}
fn summary(version: &str, status: &ws_protocol::EngineStatus) -> Value {
    let version = if version.len() <= 32
        && version.split('.').count() == 3
        && version
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
    {
        version
    } else {
        "unknown"
    };
    let embedding = status.embedding.as_ref().map(|value| {
        json!({
            "requested_backend":backend(&value.requested_backend),
            "text_backend":value.text_backend.as_deref().and_then(backend),
            "image_backend":value.image_backend.as_deref().and_then(backend),
            "audio_backend":value.audio_backend.as_deref().and_then(backend),
            "npu":value.npu,
        })
    });
    json!({
        "schema_version":1,"available":true,"cli_version":CLI_VERSION,
        "engine_version":version,"state":status.state,
        "files_discovered":status.files_discovered,"files_indexed":status.files_indexed,
        "segments_indexed":status.segments_indexed,"queue_depth":status.queue_depth,
        "index_size_bytes":status.index_size_bytes,"model_loaded":status.model_loaded,
        "embedding":embedding,
    })
}
fn error_code(error: IpcError) -> &'static str {
    match error {
        IpcError::NotConnected { .. } => "not_running",
        IpcError::ServerError { .. } => "engine_error",
        IpcError::Framing(_) => "protocol_error",
        _ => "transport_error",
    }
}
pub(crate) fn collect() -> (Value, bool) {
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let launched = std::thread::Builder::new()
        .name("diagnostic-status".into())
        .spawn(move || {
            let result = (|| {
                let mut client = DaemonClient::connect().map_err(error_code)?;
                let status = client.get_status().map_err(error_code)?;
                Ok(summary(&client.engine_version, &status))
            })();
            let _ = send.send(result);
        });
    let result = if launched.is_err() {
        Err("worker_unavailable")
    } else {
        receive
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap_or(Err("timeout"))
    };
    match result {
        Ok(value) => (value, true),
        Err(code) => (
            json!({"schema_version":1,"available":false,"cli_version":CLI_VERSION,"error_code":code}),
            false,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_export_excludes_paths_source_credentials_and_untrusted_backend_strings() {
        let status = ws_protocol::EngineStatus {
            current_activity: "C:/private/source secret-token".into(),
            index_id: Some("profile-private-identity".into()),
            files_indexed: 123,
            embedding: Some(ws_protocol::EmbeddingDiagnostics {
                requested_backend: "secret-token".into(),
                text_backend: Some("cpu".into()),
                image_backend: Some("C:/private/source".into()),
                audio_backend: None,
                npu: ws_protocol::NpuState::Quarantined,
            }),
            ..Default::default()
        };
        let value = summary("credential-instead-of-version", &status);
        let encoded = value.to_string();
        for forbidden in [
            "private",
            "secret-token",
            "credential",
            "current_activity",
            "index_id",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "diagnostics exported {forbidden}"
            );
        }
        assert_eq!(value["engine_version"], "unknown");
        assert_eq!(value["files_indexed"], 123);
        assert_eq!(value["embedding"]["text_backend"], "cpu");
        assert_eq!(value["embedding"]["npu"], "quarantined");
        assert_eq!(summary("0.1.0", &status)["engine_version"], "0.1.0");
    }
}
