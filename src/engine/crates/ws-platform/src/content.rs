//! Metadata-only guards for local content opens. No content handle is opened.
use std::os::windows::fs::MetadataExt;
use std::path::Path;

pub fn content_attributes_allowed(flags: u32) -> bool {
    flags & (0x400 | 0x1000 | 0x40000 | 0x400000) == 0
}

/// Rejects reparse traversal; ordinary files, including cloud placeholders,
/// may still be discovered by name. Recall/offline flags apply to the file;
/// volume-root attributes are not a file's cloud residency state.
pub fn metadata_path_allowed(path: &Path, content: bool) -> bool {
    if path.components().any(|component| match component {
        std::path::Component::ParentDir => true,
        std::path::Component::Normal(value) => value.to_string_lossy().contains([':', '\0']),
        _ => false,
    }) {
        return false;
    }
    for (i, p) in path.ancestors().enumerate() {
        match std::fs::symlink_metadata(p) {
            Ok(m) => {
                let flags = m.file_attributes();
                if (flags & 0x400 != 0 && (content || i != 0 || flags & 0x10 != 0))
                    || (content && i == 0 && !content_attributes_allowed(flags))
                {
                    return false;
                }
            }
            Err(e) if !content && e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return false,
        }
    }
    true
}

pub fn local_content_allowed(path: &Path) -> bool {
    metadata_path_allowed(path, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_guard_rejects_parent_traversal_and_alternate_streams() {
        let base =
            std::env::temp_dir().join(format!("winsearch-lexical-metadata-{}", std::process::id()));
        let allowed = base.join("allowed");
        let private = base.join("private");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&private).unwrap();
        std::fs::write(private.join("fixture.txt"), b"generated private fixture").unwrap();
        let escape = allowed.join("..").join("private").join("fixture.txt");
        assert!(
            std::fs::metadata(&escape).is_ok(),
            "fixture must resolve to a real escaped file"
        );
        for content in [false, true] {
            assert!(
                !metadata_path_allowed(&escape, content),
                "parent components must not cross a consented lexical root"
            );
            assert!(!metadata_path_allowed(
                &allowed.join("fixture.txt:alternate"),
                content
            ));
        }
        std::fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn cloud_recall_and_reparse_flags_are_name_only() {
        for flag in [0x400, 0x1000, 0x40000, 0x400000] {
            assert!(!content_attributes_allowed(flag));
        }
        assert!(content_attributes_allowed(0x20));
    }
    #[test]
    fn actual_junction_and_ancestor_are_rejected_without_opening_content() {
        let base =
            std::env::temp_dir().join(format!("winsearch-junction-policy-{}", std::process::id()));
        let target = base.join("target");
        let link = base.join("link");
        if std::fs::symlink_metadata(&link).is_ok() {
            std::fs::remove_dir(&link).unwrap();
        }
        std::fs::create_dir_all(target.join("sub")).unwrap();
        std::fs::write(target.join("sub\\fixture.txt"), b"local").unwrap();
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&target)
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "junction creation failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        assert!(local_content_allowed(&target.join("sub\\fixture.txt")));
        assert!(!local_content_allowed(&link.join("sub\\fixture.txt")));
        assert!(!metadata_path_allowed(&link, false));
        assert!(!metadata_path_allowed(&link.join("sub"), false));
        std::fs::remove_dir(&link).unwrap();
        std::fs::remove_dir_all(&base).unwrap();
    }
}
