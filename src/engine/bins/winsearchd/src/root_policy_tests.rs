use super::*;
use std::io::Write;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

struct Tree(PathBuf);
impl Tree {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "winsearch-root-policy-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        path
    }
    fn hidden_file(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .attributes(0x2)
            .open(&path)
            .unwrap();
        file.write_all(b"x").unwrap();
        assert_ne!(
            std::fs::symlink_metadata(&path).unwrap().file_attributes() & 0x2,
            0
        );
        path
    }
    fn config(
        &self,
        include_hidden: bool,
        maximum: u64,
        exclusions: &[&str],
    ) -> ws_protocol::IndexingConfig {
        serde_json::from_value(
            serde_json::json!({"roots":[self.0],"respect_battery_saver":false,
            "root_policies":[{"root":self.0,"include_hidden":include_hidden,
                "max_file_size_bytes":maximum,"exclusions":exclusions}]}),
        )
        .unwrap()
    }
}
impl Drop for Tree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn per_root_policies_stay_deferred_after_database_reopen() {
    let tree = Tree::new();
    let selected = tree.config(false, 1024, &["private"]);
    let mut store = IndexStore::open_in_memory().unwrap();
    store.set_indexing_config(&selected).unwrap();
    let indexer = Indexer::open_deferred(store).unwrap();
    let effective = indexer.get_indexing_config().unwrap();
    assert!(effective.roots.is_empty());
    assert!(
        effective.root_policies.is_empty(),
        "saved folder rules are not current host consent"
    );
    assert_eq!(
        indexer
            .store_lock()
            .get_or_migrate_indexing_config()
            .unwrap(),
        selected
    );
}

#[test]
fn per_root_policy_filters_exact_and_cached_vectors_before_candidate_limits() {
    let tree = Tree::new();
    for n in 0..100 {
        tree.file(&format!("needle-{n:03}.txt"), b"larger than host policy");
    }
    let wanted = tree.file("needle-wanted.txt", b"x");
    let indexer = Indexer::open(
        IndexStore::open_in_memory().unwrap(),
        std::slice::from_ref(&tree.0),
    )
    .unwrap();
    indexer.enqueue_initial_scan().unwrap();
    while indexer.scan_available(64) > 0 {}
    {
        let mut store = indexer.store_lock();
        for path in std::fs::read_dir(&tree.0)
            .unwrap()
            .map(|entry| entry.unwrap().path())
        {
            let entry = store
                .find_by_path(&path.to_string_lossy())
                .unwrap()
                .unwrap();
            let vector = if path == wanted {
                [0.8, 0.6]
            } else {
                [1.0, 0.0]
            };
            store
                .upsert_segment(ws_index::NewSegment {
                    entry_id: entry.id,
                    kind: "name",
                    chunk_index: 0,
                    text: &entry.name,
                    page: None,
                    start_ms: None,
                    end_ms: None,
                    line: None,
                    vector: &vector,
                })
                .unwrap();
        }
    }
    assert!(
        !indexer
            .search_vectors(&[1.0, 0.0], 1, None)
            .unwrap()
            .is_empty(),
        "warm the actual vector cache"
    );
    indexer
        .set_indexing_config(&tree.config(false, 4, &[]))
        .unwrap();
    let hits = indexer
        .search(&SearchRequest {
            text: "needle".into(),
            limit: 1,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].path,
        wanted.to_string_lossy(),
        "over-size prefix must not consume the lexical limit"
    );
    let hits = indexer.search_vectors(&[1.0, 0.0], 1, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        indexer
            .store_lock()
            .get_entry(hits[0].segment.entry_id)
            .unwrap()
            .unwrap()
            .path,
        wanted.to_string_lossy(),
        "over-size higher-scoring vectors must not consume top-k"
    );
}

#[test]
fn per_root_policy_rechecks_hidden_size_and_relative_exclusions() {
    let tree = Tree::new();
    let visible = tree.file("visible.txt", b"x");
    let dot = tree.file(".private/note.txt", b"x");
    let suffix = tree.file("scratch.tmp", b"x");
    let nested = tree.file("sub/cache/note.txt", b"x");
    let oversized = tree.file("large.txt", b"larger");
    let hidden = tree.hidden_file("windows-hidden.txt");
    let indexer = Indexer::open_deferred(IndexStore::open_in_memory().unwrap()).unwrap();
    indexer
        .set_indexing_config(&tree.config(false, 4, &["cache", "*.tmp"]))
        .unwrap();
    assert!(indexer.name_path_allowed(&visible));
    for path in [&dot, &suffix, &nested, &oversized, &hidden] {
        assert!(
            !indexer.name_path_allowed(path),
            "current root policy must reject {}",
            path.display()
        );
    }
    indexer
        .set_indexing_config(&tree.config(true, 64, &[]))
        .unwrap();
    assert!(indexer.name_path_allowed(&dot) && indexer.name_path_allowed(&hidden));
}

#[test]
fn per_root_hidden_opt_in_reaches_initial_scan_and_keeps_sensitive_files_excluded() {
    let tree = Tree::new();
    let hidden = tree.hidden_file("opt-in-hidden.txt");
    let secret = tree.file(".env", b"never index credentials");
    let indexer = Indexer::open_deferred(IndexStore::open_in_memory().unwrap()).unwrap();
    indexer
        .set_indexing_config(&tree.config(true, 1024, &[]))
        .unwrap();
    while indexer.scan_available(64) > 0 {}
    let store = indexer.store_lock();
    assert!(
        store
            .find_by_path(&hidden.to_string_lossy())
            .unwrap()
            .is_some(),
        "explicit hidden opt-in must reach the scanner"
    );
    assert!(store
        .find_by_path(&secret.to_string_lossy())
        .unwrap()
        .is_none());
    drop(store);
    let late = tree.hidden_file("watcher-hidden.txt");
    let root_id = indexer.root_snapshot()[0].id;
    indexer.handle_event(root_id, WatchEvent::Created(late.clone()));
    assert!(
        indexer
            .store_lock()
            .find_by_path(&late.to_string_lossy())
            .unwrap()
            .is_some(),
        "hidden opt-in must also reach point updates"
    );
}

#[test]
fn per_root_policy_uses_the_most_specific_normalized_root() {
    let tree = Tree::new();
    let selected = tree.0.join("chosen");
    let path = tree.file("chosen/allowed.txt", b"x");
    let mut config = tree.config(false, 1024, &["chosen"]);
    config.roots[0].push_str(&"\\".repeat(32));
    config.roots.push(selected.to_string_lossy().into_owned());
    config.root_policies.push(ws_protocol::RootPolicy {
        root: selected.to_string_lossy().into_owned(),
        include_hidden: false,
        max_file_size_bytes: 1024,
        exclusions: vec![],
    });
    let config = crate::policy::validate(&config).unwrap();
    assert!(
        crate::policy::path_allowed(&config, &path.to_string_lossy()),
        "trailing separators cannot make a parent override a more specific selected root"
    );
}

#[test]
fn per_root_policy_validation_rejects_unselected_duplicates_escapes_and_zero_budget() {
    let tree = Tree::new();
    let valid = serde_json::to_value(tree.config(false, 16, &[])).unwrap();
    let mut invalid = valid.clone();
    invalid["root_policies"] = serde_json::json!([{"root":tree.0.join("unselected"),"include_hidden":false,"max_file_size_bytes":16,"exclusions":[]}]);
    let mut cases = vec![invalid];
    for pattern in ["../outside", "C:/outside", "/outside", ""] {
        let mut invalid = valid.clone();
        invalid["root_policies"] = serde_json::json!([{"root":tree.0,"include_hidden":false,"max_file_size_bytes":16,"exclusions":[pattern]}]);
        cases.push(invalid);
    }
    let mut invalid = valid.clone();
    invalid["root_policies"] = serde_json::json!([{"root":tree.0,"include_hidden":false,"max_file_size_bytes":0,"exclusions":[]}]);
    cases.push(invalid);
    let mut invalid = valid;
    let rule = serde_json::json!({"root":tree.0,"include_hidden":false,"max_file_size_bytes":16,"exclusions":[]});
    invalid["root_policies"] = serde_json::json!([rule, rule]);
    cases.push(invalid);
    for value in cases {
        let config: ws_protocol::IndexingConfig = serde_json::from_value(value).unwrap();
        assert!(
            crate::policy::validate(&config).is_err(),
            "invalid per-root policy must fail closed"
        );
    }
}
