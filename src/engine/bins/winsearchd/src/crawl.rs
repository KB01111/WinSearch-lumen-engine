//! Directory scanning: one directory at a time, metadata-only, with the
//! exclusion policy applied.
//!
//! The scanner never opens file contents (and never opens handles at all:
//! metadata comes from directory enumeration), so OneDrive/other cloud
//! placeholders are indexed by name without ever being hydrated. Reparse
//! points are indexed as files but never recursed into, which also rules out
//! junction cycles.

use std::io;
use std::os::windows::fs::MetadataExt;
use std::path::Path;
use std::time::UNIX_EPOCH;

use ws_index::model::{extension_of, file_name, EntryRecord};

/// Directory names never indexed (hidden/system attributes are filtered too;
/// this list covers non-hidden development trees).
const EXCLUDED_DIRS: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    ".gradle",
    ".cargo",
    ".rustup",
    "target",
];

/// File names never indexed (case-insensitive), covering credential stores
/// and machine-private files.
const EXCLUDED_FILES: &[&str] = &[
    "ntuser.dat",
    "ntuser.dat.log",
    "ntuser.ini",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    ".env",
    "credentials",
    "credentialprovider",
    "master.key",
    "secrets.json",
];

/// File extensions never indexed: private keys, certificate/keystore stores.
const EXCLUDED_EXTS: &[&str] = &[
    "pem", "key", "pfx", "p12", "ppk", "kdbx", "asc", "jks", "keystore", "der", "p8",
];

/// Hard recursion cap (Windows path limits make deeper trees unreachable).
pub const MAX_DEPTH: usize = 64;

/// The result of scanning one directory: its visible children as index
/// records plus the subdirectories that still need scanning.
#[derive(Debug, Default)]
pub struct DirScan {
    pub entries: Vec<EntryRecord>,
    pub subdirs: Vec<String>,
    /// True when every entry in `read_dir` was enumerated and inspected without
    /// non-`NotFound` I/O errors.
    pub complete: bool,
}

/// Scans `dir_path`, producing records for its children (`parent_id` is the
/// entry id of `dir_path` itself and is stamped on every record).
pub fn scan_directory(
    root_id: i64,
    dir_path: &str,
    parent_id: Option<i64>,
    depth: usize,
) -> io::Result<DirScan> {
    scan_directory_with_hidden(root_id, dir_path, parent_id, depth, false)
}

pub fn scan_directory_with_hidden(
    root_id: i64,
    dir_path: &str,
    parent_id: Option<i64>,
    depth: usize,
    include_hidden: bool,
) -> io::Result<DirScan> {
    let mut scan = DirScan {
        entries: Vec::new(),
        subdirs: Vec::new(),
        complete: true,
    };
    if depth >= MAX_DEPTH {
        return Ok(scan);
    }
    if !ws_platform::content::metadata_path_allowed(Path::new(dir_path), false) {
        return Ok(scan);
    }
    if depth > 0 {
        match std::fs::symlink_metadata(dir_path) {
            Ok(meta) => {
                let attrs = meta.file_attributes();
                // Reject reparse-point, hidden, or system directories queued via
                // watcher events so scanning never escapes the configured root.
                if (attrs & (0x400 | 0x4)) != 0 || (!include_hidden && attrs & 0x2 != 0) {
                    return Ok(scan);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(scan),
            Err(e) => return Err(e),
        }
    }
    let rd = match std::fs::read_dir(dir_path) {
        Ok(rd) => rd,
        Err(e) => {
            // A directory can vanish or become unreadable between queueing and
            // scanning; that is not fatal for the crawl.
            return if e.kind() == io::ErrorKind::NotFound {
                Ok(scan)
            } else {
                Err(e)
            };
        }
    };

    for entry in rd {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    scan.complete = false;
                }
                continue; // transient per-entry failure: skip
            }
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path().to_string_lossy().into_owned();
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    scan.complete = false;
                }
                continue;
            }
        };
        let attrs = metadata.file_attributes();

        let is_dir = metadata.is_dir();
        let is_reparse = attrs & 0x400 != 0; // FILE_ATTRIBUTE_REPARSE_POINT

        if is_excluded(&name, is_dir) {
            continue;
        }
        // Hidden and system items are excluded by policy (also covers
        // junctions such as "Application Data").
        if attrs & 0x4 != 0 || (!include_hidden && attrs & 0x2 != 0) {
            // FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM
            continue;
        }
        if is_dir && is_reparse {
            // Never recurse through reparse points (cycle + scope safety).
            continue;
        }

        if is_dir {
            scan.subdirs.push(path.clone());
        }

        let (volume_serial, file_id) = file_identity(Path::new(&path), is_dir);
        scan.entries.push(EntryRecord {
            root_id,
            parent_id,
            name: name.clone(),
            ext: if is_dir {
                String::new()
            } else {
                extension_of(&name)
            },
            path,
            is_dir,
            size: if is_dir { 0 } else { metadata.file_size() },
            mtime: unix_seconds(&metadata),
            volume_serial,
            file_id,
            is_app: false,
            app_target: String::new(),
            keywords: String::new(),
        });
    }
    Ok(scan)
}

/// True for names the exclusion policy never indexes.
pub fn is_excluded(name: &str, is_dir: bool) -> bool {
    let lower = name.to_lowercase();
    if is_dir {
        if EXCLUDED_DIRS.iter().any(|d| name.eq_ignore_ascii_case(d)) {
            return true;
        }
        // ".env.local", "id_rsa" style sidecars inside any folder.
        if lower == ".env" || lower.starts_with(".env.") {
            return true;
        }
        return false;
    }
    if EXCLUDED_FILES.iter().any(|f| lower == *f) {
        return true;
    }
    // id_rsa, id_ed25519.pub etc.
    if lower.starts_with("id_rsa")
        || lower.starts_with("id_dsa")
        || lower.starts_with("id_ecdsa")
        || lower.starts_with("id_ed25519")
    {
        return true;
    }
    if lower.starts_with(".env.") {
        return true;
    }
    let ext = extension_of(&lower);
    if EXCLUDED_EXTS.contains(&ext.as_str()) {
        return true;
    }
    false
}

fn unix_seconds(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Filesystem identity (volume serial + file id) used for rename/move
/// detection across events and rescans.
///
/// The file is opened with `FILE_READ_ATTRIBUTES` only — a metadata query that
/// never hydrates cloud placeholders — and the handle is closed immediately.
pub fn file_identity(path: &Path, is_dir: bool) -> (u32, u64) {
    use std::ffi::c_void;
    use std::mem::size_of;

    use windows::core::PCWSTR;
    use windows::Win32::Foundation::{CloseHandle, HANDLE};
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FileIdInfo, GetFileInformationByHandleEx, FILE_FLAGS_AND_ATTRIBUTES,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    let wide: Vec<u16> = path
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    unsafe {
        let handle: HANDLE = match CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            if is_dir {
                FILE_FLAG_BACKUP_SEMANTICS
            } else {
                FILE_FLAGS_AND_ATTRIBUTES(0)
            },
            None,
        ) {
            Ok(h) => h,
            Err(_) => return (0, 0),
        };
        let mut info: FILE_ID_INFO = std::mem::zeroed();
        let result = GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            &mut info as *mut FILE_ID_INFO as *mut c_void,
            size_of::<FILE_ID_INFO>() as u32,
        );
        let _ = CloseHandle(handle);
        if result.is_err() {
            return (0, 0);
        }
        let file_id = u64::from_le_bytes(info.FileId.Identifier[..8].try_into().unwrap_or([0; 8]));
        (info.VolumeSerialNumber as u32, file_id)
    }
}

/// Builds the record for a single path (used by the watcher for point
/// updates). Returns `None` for excluded or vanished paths.
#[cfg(test)]
pub fn record_for_path(
    root_id: i64,
    parent_id: Option<i64>,
    path: &Path,
) -> io::Result<Option<EntryRecord>> {
    record_for_path_with_hidden(root_id, parent_id, path, false)
}

pub fn record_for_path_with_hidden(
    root_id: i64,
    parent_id: Option<i64>,
    path: &Path,
    include_hidden: bool,
) -> io::Result<Option<EntryRecord>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let name = file_name(path);
    let is_dir = metadata.is_dir();
    if is_excluded(&name, is_dir) {
        return Ok(None);
    }
    let attrs = metadata.file_attributes();
    if attrs & 0x4 != 0 || (!include_hidden && attrs & 0x2 != 0) {
        return Ok(None);
    }
    let (volume_serial, file_id) = file_identity(path, is_dir);
    Ok(Some(EntryRecord {
        root_id,
        parent_id,
        name: name.clone(),
        ext: if is_dir {
            String::new()
        } else {
            extension_of(&name)
        },
        path: path.to_string_lossy().into_owned(),
        is_dir,
        size: if is_dir { 0 } else { metadata.file_size() },
        mtime: unix_seconds(&metadata),
        volume_serial,
        file_id,
        is_app: false,
        app_target: String::new(),
        keywords: String::new(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct TempTree(std::path::PathBuf);

    impl TempTree {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-crawl-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn scans_files_and_subdirectories() {
        let tree = TempTree::new();
        fs::write(tree.0.join("a.txt"), b"hello").unwrap();
        fs::create_dir(tree.0.join("sub")).unwrap();
        fs::write(tree.0.join("sub").join("b.md"), b"x").unwrap();

        let scan = scan_directory(1, &tree.path(), Some(7), 0).unwrap();
        let names: Vec<&str> = scan.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"sub"));
        assert_eq!(scan.subdirs.len(), 1);
        assert!(scan.entries.iter().all(|e| e.parent_id == Some(7)));

        let a = scan.entries.iter().find(|e| e.name == "a.txt").unwrap();
        assert_eq!(a.size, 5);
        assert_eq!(a.ext, "txt");
        assert!(!a.is_dir);
    }

    #[test]
    fn excludes_dev_trees_and_sensitive_files() {
        let tree = TempTree::new();
        for d in [".git", "node_modules", "target", ".venv"] {
            fs::create_dir(tree.0.join(d)).unwrap();
            fs::write(tree.0.join(d).join("junk.bin"), b"x").unwrap();
        }
        for f in [
            "id_rsa",
            "id_ed25519.pub",
            ".env",
            ".env.local",
            "server.pem",
            "store.pfx",
        ] {
            fs::write(tree.0.join(f), b"secret").unwrap();
        }
        fs::write(tree.0.join("notes.txt"), b"fine").unwrap();

        let scan = scan_directory(1, &tree.path(), None, 0).unwrap();
        let names: Vec<&str> = scan.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["notes.txt"]);
        assert!(scan.subdirs.is_empty());
    }

    #[test]
    fn hidden_items_are_skipped() {
        use windows::core::PCWSTR;
        use windows::Win32::Storage::FileSystem::SetFileAttributesW;
        use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_HIDDEN;

        let tree = TempTree::new();
        let hidden = tree.0.join("secret-folder");
        fs::create_dir(&hidden).unwrap();
        fs::write(tree.0.join("visible.txt"), b"x").unwrap();

        let wide: Vec<u16> = hidden
            .to_string_lossy()
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        unsafe {
            SetFileAttributesW(PCWSTR(wide.as_ptr()), FILE_ATTRIBUTE_HIDDEN).unwrap();
        }

        let scan = scan_directory(1, &tree.path(), None, 0).unwrap();
        let names: Vec<&str> = scan.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["visible.txt"]);
        assert!(scan.subdirs.is_empty());
    }

    #[test]
    fn depth_cap_stops_recursion() {
        let tree = TempTree::new();
        let scan = scan_directory(1, &tree.path(), None, MAX_DEPTH).unwrap();
        assert!(scan.entries.is_empty());
        assert!(scan.subdirs.is_empty());
    }

    #[test]
    fn missing_directory_is_not_an_error() {
        let tree = TempTree::new();
        let gone = format!("{}\\does-not-exist", tree.path());
        let scan = scan_directory(1, &gone, None, 0).unwrap();
        assert!(scan.entries.is_empty());
    }

    #[test]
    fn record_for_path_captures_metadata() {
        let tree = TempTree::new();
        let file = tree.0.join("doc.pdf");
        fs::write(&file, b"1234").unwrap();

        let rec = record_for_path(1, Some(3), &file).unwrap().unwrap();
        assert_eq!(rec.name, "doc.pdf");
        assert_eq!(rec.ext, "pdf");
        assert_eq!(rec.size, 4);
        assert_eq!(rec.parent_id, Some(3));
        assert!(!rec.is_dir);
        assert!(rec.file_id != 0, "identity should be captured");

        // Excluded paths return None.
        let secret = tree.0.join("id_rsa");
        fs::write(&secret, b"x").unwrap();
        assert!(record_for_path(1, None, &secret).unwrap().is_none());

        // Vanished paths return None.
        assert!(record_for_path(1, None, &tree.0.join("nope.txt"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn identity_survives_rename() {
        let tree = TempTree::new();
        let before_path = tree.0.join("before.txt");
        fs::write(&before_path, b"x").unwrap();
        let (vol_before, id_before) = file_identity(&before_path, false);
        assert_ne!(id_before, 0);

        let after_path = tree.0.join("after.txt");
        fs::rename(&before_path, &after_path).unwrap();
        let (vol_after, id_after) = file_identity(&after_path, false);

        assert_eq!((vol_before, id_before), (vol_after, id_after));
    }
}
