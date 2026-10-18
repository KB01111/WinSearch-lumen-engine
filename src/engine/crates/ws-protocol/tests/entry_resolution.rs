use serde_json::json;
use ws_protocol::{ClientMessage, ServerMessage};

#[test]
fn indexed_entry_resolution_is_typed_and_carries_a_source_revision() {
    let request = json!({"type":"resolve_entry","id":41,
        "reference":{"result_id":"e23","source_revision":null}});
    let parsed: ClientMessage = serde_json::from_value(request.clone())
        .expect("native consumers need identity-based entry resolution");
    assert_eq!(serde_json::to_value(parsed).unwrap(), request);
    let reply = json!({"type":"resolved_entry","id":41,"entry":{
        "result_id":"e23","root_path":"C:/fixture","path":"C:/fixture/report.pdf",
        "name":"report.pdf","kind":"document","size_bytes":1234,
        "modified_unix_seconds":1700000000,"index_revision":7,
        "source_revision":"opaque-native-source-proof"}});
    let parsed: ServerMessage = serde_json::from_value(reply.clone())
        .expect("resolved metadata must cross the existing typed wire");
    assert_eq!(serde_json::to_value(parsed).unwrap(), reply);
}

#[test]
fn entry_resolution_does_not_accept_caller_supplied_paths() {
    let request = json!({"type":"resolve_entry","id":41,
        "reference":{"result_id":"e23","path":"C:/outside/secret.txt"}});
    assert!(serde_json::from_value::<ClientMessage>(request).is_err());
}
