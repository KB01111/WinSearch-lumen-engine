use ws_protocol::{ClientMessage, IndexingConfig, IndexingMode, ServerMessage};

#[test]
fn per_root_policies_round_trip_without_changing_legacy_defaults() {
    let rules = serde_json::json!([{"root":"C:/Data","include_hidden":true,
        "max_file_size_bytes":1048576,"exclusions":["cache","*.tmp"]}]);
    let config: IndexingConfig = serde_json::from_value(serde_json::json!({"roots":["C:/Data"],"root_policies":rules})).unwrap();
    assert_eq!(serde_json::to_value(config).unwrap()["root_policies"], rules);
    let legacy: IndexingConfig = serde_json::from_str("{}").unwrap();
    assert!(serde_json::to_value(legacy).unwrap().get("root_policies").is_none());
}
#[test]
fn indexing_wire_defaults_and_full_contract() {
    let default: IndexingConfig = serde_json::from_str("{}").unwrap();
    assert!(default.roots.is_empty());
    assert!(!default.code_enabled);
    assert!(default.index_on_battery && default.respect_battery_saver);
    let mut config = default;
    config.roots = vec![r"C:\Data".into()];
    config.code_enabled = true;
    config.code_roots = vec![r"D:\Code".into()];
    config.excluded_paths = vec![r"C:\Data\Skip".into()];
    config.excluded_extensions = vec!["pdf".into()];
    config.mode = IndexingMode::Scheduled;
    config.hours_start = 1320;
    config.hours_end = 360;
    let request = ClientMessage::SetIndexingConfig {
        id: 42,
        config: config.clone(),
    };
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["type"], "set_indexing_config");
    assert_eq!(json["config"]["mode"], "scheduled");
    assert_eq!(
        serde_json::from_value::<ClientMessage>(json).unwrap(),
        request
    );
    let response = ServerMessage::IndexingConfig {
        id: Some(42),
        config,
    };
    assert_eq!(
        serde_json::from_str::<ServerMessage>(&serde_json::to_string(&response).unwrap()).unwrap(),
        response
    );
}
