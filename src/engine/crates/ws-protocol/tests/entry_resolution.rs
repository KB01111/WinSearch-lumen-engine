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

#[test]
fn search_results_preserve_a_bound_canonical_entry_reference_separate_from_hit_id() {
    let wire = json!({"type":"results","id":7,"done":true,"batch":[{
        "id":"code:23:2:true:long_symbol_name","kind":"code","title":"long_symbol_name",
        "subtitle":"C:/code/lib.rs","match_explanation":"definition","score":1.0,
        "path":"C:/code/lib.rs","location":{"kind":"line","line":2},"thumbnail_path":null,
        "excerpt":"fn long_symbol_name() {}","matching_moments":[],
        "entry_reference":{"result_id":"c23","source_revision":"native-bound-source"}
    }]});
    let parsed: ServerMessage = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
}

#[test]
fn code_metadata_keeps_actual_sha256_separate_from_metadata_source_proof() {
    let wire = json!({"type":"resolved_entry","id":8,"entry":{
        "result_id":"c23","root_path":"C:/code","path":"C:/code/lib.rs","name":"lib.rs",
        "kind":"code","size_bytes":13,"modified_unix_seconds":1,"index_revision":7,
        "source_revision":"metadata-native-proof","content_sha256":"a".repeat(64)}});
    let parsed: ServerMessage = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
}

#[test]
fn indexed_reference_ids_and_bound_proofs_reject_ambiguous_or_unbounded_values() {
    use ws_protocol::{EntryReference, IndexedEntryId};
    for (value, expected) in [
        ("e23", IndexedEntryId::File(23)),
        ("c23", IndexedEntryId::Code(23)),
    ] {
        let mut reference = EntryReference {
            result_id: value.into(),
            source_revision: None,
        };
        assert_eq!(reference.indexed_id(), Some(expected));
        assert!(reference.is_valid());
        assert!(!reference.is_bound());
        reference.source_revision = Some("native-proof".into());
        assert!(reference.is_bound());
    }
    for value in [
        "e0",
        "c-1",
        "e+1",
        "c01",
        "e1/../../secret",
        "code:23:1:true:symbol",
        "s23",
        "e9223372036854775808",
        "é23",
        "",
    ] {
        assert!(
            !EntryReference {
                result_id: value.into(),
                source_revision: Some("proof".into())
            }
            .is_valid(),
            "{value}"
        );
    }
    for proof in ["".into(), " ".into(), "p".repeat(65537)] {
        assert!(!EntryReference {
            result_id: "c23".into(),
            source_revision: Some(proof)
        }
        .is_bound());
    }
    assert!(ws_protocol::valid_content_sha256(&"a".repeat(64)));
    for value in [
        "".into(),
        "a".repeat(63),
        "g".repeat(64),
        "A".repeat(64),
        "a".repeat(65),
    ] {
        assert!(!ws_protocol::valid_content_sha256(&value));
    }
}
