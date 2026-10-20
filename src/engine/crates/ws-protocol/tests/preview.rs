use ws_protocol::{ClientMessage, ServerMessage};

#[test]
fn code_preview_locations_use_real_code_or_notebook_coordinates_only() {
    use ws_protocol::{MatchLocation,valid_code_preview_location,valid_preview_location};
    assert!(valid_code_preview_location(None));
    assert!(valid_code_preview_location(Some(MatchLocation::Line {line:1})));
    assert!(valid_code_preview_location(Some(MatchLocation::Line {line:64*1024*1024})));
    let notebook=MatchLocation::Notebook {cell_ordinal:2,line:1,end_line:3};
    assert!(valid_code_preview_location(Some(notebook)));
    assert!(!valid_preview_location(Some(notebook)),"ordinary preview capability must not change");
    for location in [MatchLocation::Page {page:1},MatchLocation::Timestamp {seconds:0.0,end_seconds:1.0},
        MatchLocation::Line {line:0},MatchLocation::Line {line:64*1024*1024+1},
        MatchLocation::Notebook {cell_ordinal:0,line:0,end_line:1},
        MatchLocation::Notebook {cell_ordinal:0,line:3,end_line:2},
        MatchLocation::Notebook {cell_ordinal:0,line:1,end_line:64*1024*1024+1}] {
        assert!(!valid_code_preview_location(Some(location)),"{location:?}");
    }
}

#[test]
fn preview_contract_uses_indexed_identity_and_preserves_actual_location_and_partial_state() {
    let request=serde_json::json!({"type":"preview_entry","id":17,"request":{
        "reference":{"result_id":"e12","source_revision":"current-proof"},
        "location":{"kind":"page","page":8}
    }});
    let decoded:ClientMessage=serde_json::from_value(request.clone()).expect("indexed preview request");
    assert_eq!(serde_json::to_value(decoded).unwrap(),request);
    let response=serde_json::json!({"type":"entry_preview","id":17,"preview":{
        "entry":{"result_id":"e12","root_path":"C:/Docs","path":"C:/Docs/source.pdf",
            "name":"source.pdf","kind":"document","size_bytes":19,"modified_unix_seconds":1,
            "index_revision":3,"source_revision":"current-proof"},
        "state":"ready","parts":[{"kind":"pdf","text":"actual eighth page",
            "location":{"kind":"page","page":8}}],"truncated":false,
        "extraction":[{"kind":"pdf","complete_within_limit":true,
            "source_units":null,"indexed_units":8,"capped":null}]
    }});
    let decoded:ServerMessage=serde_json::from_value(response.clone()).expect("bounded preview response");
    assert_eq!(serde_json::to_value(decoded).unwrap(),response);
}

#[test]
fn preview_request_cannot_smuggle_a_path_or_extra_reference_field() {
    for request in [
        serde_json::json!({"reference":{"result_id":"e12"},"path":"C:/private.txt"}),
        serde_json::json!({"reference":{"result_id":"e12","path":"C:/private.txt"}}),
    ] {
        assert!(serde_json::from_value::<ClientMessage>(serde_json::json!({
            "type":"preview_entry","id":1,"request":request
        })).is_err());
    }
}
