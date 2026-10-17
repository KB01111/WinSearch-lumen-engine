//! Repository detection and `.gitignore`-aware source code crawling.
//!
//! Unlike the general filesystem crawler (`winsearchd::crawl`), the code
//! crawler operates on user-configured code folders, detects `.git`
//! repositories, and evaluates `.gitignore` rules (including nested
//! `.gitignore` files and `!negation` patterns) via the `ignore` crate while
//! applying code-specific exclusions (vendor, generated, minified,
//! `node_modules`, `target`, `.git`, binary files, and secret keys).

use std::fs::{self, File};
use std::io::{self, Read};
use std::os::windows::fs::MetadataExt;
use std::path::Path;
use std::time::UNIX_EPOCH;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::sync::Arc;

use crate::model::{language_for_file, CodeFileRecord, DetectedRepo};

/// Maximum file size indexed as source code (2 MiB). Larger files are almost
/// always generated dumps, bundled assets, or data fixtures.
pub const MAX_CODE_FILE_BYTES: u64 = 2 * 1024 * 1024;

const SENSITIVE_CODE_DIRS: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    ".ssh",
    ".aws",
    ".azure",
    ".gnupg",
    ".kube",
    ".docker",
    ".password-store",
];

pub(crate) fn sensitive_code_ancestor(path: &Path) -> bool {
    path.ancestors()
        .filter_map(Path::file_name)
        .any(|name| is_sensitive_code_dir(&name.to_string_lossy()))
}

fn is_sensitive_code_dir(name: &str) -> bool {
    SENSITIVE_CODE_DIRS
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s))
        || name.to_ascii_lowercase().starts_with(".env")
}

/// Directory names excluded from code indexing even when not listed in `.gitignore`.
pub const EXCLUDED_CODE_DIRS: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    "target",
    "vendor",
    "third_party",
    "third-party",
    "generated",
    "__generated__",
    "dist",
    "build",
    "out",
    ".next",
    ".nuxt",
    "coverage",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "venv",
    ".gradle",
    ".cargo",
    ".rustup",
    "bin",
    "obj",
];

/// Exact file names (case-insensitive) never indexed in `code.db`: secrets,
/// credentials, and lockfiles.
pub const EXCLUDED_CODE_FILES: &[&str] = &[
    ".git",
    ".git-credentials",
    ".gitconfig",
    ".npmrc",
    ".yarnrc",
    ".yarnrc.yml",
    ".pypirc",
    ".netrc",
    "_netrc",
    ".env",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "credentials",
    "credentialprovider",
    "master.key",
    "secrets.json",
    "ntuser.dat",
    "ntuser.dat.log",
    "ntuser.ini",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "cargo.lock",
    "poetry.lock",
    "gemfile.lock",
    "composer.lock",
];

/// File extensions (lowercase, without dot) excluded from code indexing:
/// private keys/certificates, compiled artifacts, archives, media, and fonts.
pub const EXCLUDED_CODE_EXTS: &[&str] = &[
    // Keys & certificates
    "pem", "key", "pfx", "p12", "ppk", "kdbx", "asc", "jks", "keystore", "der", "p8", "crt", "cer",
    // Compiled / binary / native artifacts
    "exe", "dll", "so", "dylib", "lib", "a", "o", "obj", "pdb", "ilk", "exp", "wasm", "class",
    "pyc", "pyo", "pyd", "rlib", "rmeta", // Archives & packages
    "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", "nupkg", "jar", "war", "whl",
    // Images, audio, video, fonts, documents, DBs
    "png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "tiff", "tif", "heic", "avif", "mp3", "wav",
    "flac", "ogg", "m4a", "mp4", "mkv", "mov", "avi", "webm", "ttf", "otf", "woff", "woff2", "eot",
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "sqlite", "sqlite3", "db", "db-wal",
    "db-shm", "lock", "map",
];

/// Generated or minified filename suffixes (matched case-insensitively).
pub const EXCLUDED_CODE_SUFFIXES: &[&str] = &[
    ".min.js",
    ".min.mjs",
    ".min.cjs",
    ".min.css",
    ".bundle.js",
    ".bundle.css",
    ".generated.rs",
    ".generated.ts",
    ".generated.cs",
    ".Designer.cs",
    ".g.cs",
    ".g.i.cs",
    ".pb.go",
    ".pb.cc",
    ".pb.h",
    "_pb2.py",
    "_pb2_grpc.py",
];

/// Detects `.git` repositories under `root`.
///
/// - If `root` itself contains `.git` (directory or worktree `.git` file),
///   returns a single [`DetectedRepo`] with `has_git: true`.
/// - Otherwise walks subdirectories up to `max_depth` looking for `.git`
///   markers. When one or more nested repositories are found, returns them
///   sorted by `root_path`.
/// - If no `.git` is found anywhere under `root`, returns `root` itself with
///   `has_git: false` so standalone source folders configured by the user are
///   still indexed.
pub fn detect_repos(root: &Path) -> io::Result<Vec<DetectedRepo>> {
    detect_repos_with_depth(root, 4)
}

/// Detects repositories under `root` searching up to `max_depth` directory levels.
pub fn detect_repos_with_depth(root: &Path, max_depth: usize) -> io::Result<Vec<DetectedRepo>> {
    if sensitive_code_ancestor(root)
        || !ws_platform::content::metadata_path_allowed(root, false)
        || !root.exists()
    {
        return Ok(Vec::new());
    }
    if has_git_marker(root)? {
        return Ok(vec![DetectedRepo {
            root_path: root.to_string_lossy().into_owned(),
            name: dir_name(root),
            has_git: true,
        }]);
    }

    let mut repos = Vec::new();
    find_git_repos_recursive(root, 0, max_depth, &mut repos)?;
    if repos.is_empty() {
        repos.push(DetectedRepo {
            root_path: root.to_string_lossy().into_owned(),
            name: dir_name(root),
            has_git: false,
        });
    } else {
        repos.sort_by(|a, b| a.root_path.cmp(&b.root_path));
    }
    Ok(repos)
}

fn find_git_repos_recursive(
    dir: &Path,
    depth: usize,
    max_depth: usize,
    out: &mut Vec<DetectedRepo>,
) -> io::Result<()> {
    if depth >= max_depth {
        return Ok(());
    }
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e)
            if e.kind() == io::ErrorKind::NotFound
                || e.kind() == io::ErrorKind::PermissionDenied =>
        {
            return Ok(())
        }
        Err(e) => return Err(e),
    };
    for entry in rd {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !metadata.is_dir() {
            continue;
        }
        let attrs = metadata.file_attributes();
        if attrs & 0x400 != 0 {
            // Skip reparse points / junctions.
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_excluded_code_dir(&name) {
            continue;
        }
        let sub = entry.path();
        if has_git_marker(&sub)? {
            out.push(DetectedRepo {
                root_path: sub.to_string_lossy().into_owned(),
                name,
                has_git: true,
            });
        } else {
            find_git_repos_recursive(&sub, depth + 1, max_depth, out)?;
        }
    }
    Ok(())
}

/// Returns `Ok(true)` if `dir/.git` exists (either a standard `.git/` directory or
/// a git worktree/submodule `.git` file).
pub fn has_git_marker(dir: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(dir.join(".git")) {
        Ok(_) => Ok(true),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Crawls a single repository (or code folder) root, honoring `.gitignore`
/// (including nested `.gitignore` files and `!negation` patterns) as well as
/// code exclusions (vendor, generated, minified, `node_modules`, `target`,
/// `.git`, binary files, and secret keys).
pub fn crawl_repo(repo_root: &Path) -> io::Result<Vec<CodeFileRecord>> {
    crawl_repo_while(repo_root, &|_| true)
}

/// Authorization is checked again immediately before every ignore/sniff open.
pub fn crawl_repo_while(repo_root: &Path, allowed: &dyn Fn(&Path)->bool) -> io::Result<Vec<CodeFileRecord>> {
    let mut crawler=CodeCrawler::open(repo_root,allowed)?;
    let mut records=Vec::new();
    loop {let (batch,done)=crawler.next_batch(512,allowed)?;records.extend(batch);if done {break;}}
    records.sort_by(|a,b|a.rel_path.cmp(&b.rel_path));
    Ok(records)
}

/// Resumable metadata traversal: each turn visits at most the supplied entries.
pub struct CodeCrawler { root:std::path::PathBuf, stack:Vec<CrawlFrame> }
impl CodeCrawler {
pub fn open(repo_root:&Path,allowed:&dyn Fn(&Path)->bool)->io::Result<Self> {
    if !repo_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "code root must be absolute",
        ));
    }
    if !allowed(repo_root) || sensitive_code_ancestor(repo_root)
        || !ws_platform::content::metadata_path_allowed(repo_root, false)
        || !repo_root.exists()
    {
        return Ok(Self {root:repo_root.into(),stack:Vec::new()});
    }
    let mut inherited = Vec::new();
    if let Some(rule) = guarded_ignore(repo_root, &repo_root.join(".git/info/exclude"), allowed)? {
        inherited.push(rule);
    }
    Ok(Self {root:repo_root.into(),stack:vec![CrawlFrame::open(repo_root,0,inherited,Vec::new(),allowed)?]})
}
pub fn next_batch(&mut self,max_entries:usize,allowed:&dyn Fn(&Path)->bool)->io::Result<(Vec<CodeFileRecord>,bool)> {
    let repo_root=&self.root;
    let stack=&mut self.stack;
    let mut records = Vec::new();
    let mut visited=0;
    while visited<max_entries.max(1) {
        let Some(frame)=stack.last_mut() else {break};
        let Some(entry) = frame.entries.next() else {
            stack.pop();
            continue;
        };
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        visited+=1;
        let path = entry.path();
        if !allowed(&path) || !ws_platform::content::metadata_path_allowed(&path, false) {
            continue;
        }
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let is_dir = metadata.is_dir();
        let name = entry.file_name().to_string_lossy().into_owned();
        if (is_dir && is_excluded_code_dir(&name)) || (!is_dir && is_excluded_code_file(&name)) {
            continue;
        }
        // .ignore has priority over .gitignore; nearest matching scope wins.
        let excluded = rule_match(&frame.ignore_rules, &path, is_dir)
            .or_else(|| rule_match(&frame.git_rules, &path, is_dir))
            .unwrap_or(false);
        if excluded {
            continue;
        }
        if is_dir && frame.depth < 63 {
            let child = CrawlFrame::open(
                &path,
                frame.depth + 1,
                frame.git_rules.clone(),
                frame.ignore_rules.clone(),
                allowed,
            )?;
            stack.push(child);
        } else if metadata.is_file() {
            if let Some(record) = record_for_code_path_while(repo_root, &path, allowed)? {
                records.push(record);
            }
        }
    }
    Ok((records,stack.is_empty()))
}
}

struct CrawlFrame {
    depth: usize,
    entries: fs::ReadDir,
    git_rules: Vec<Arc<Gitignore>>,
    ignore_rules: Vec<Arc<Gitignore>>,
}

impl CrawlFrame {
    fn open(
        path: &Path,
        depth: usize,
        mut git_rules: Vec<Arc<Gitignore>>,
        mut ignore_rules: Vec<Arc<Gitignore>>,
        allowed: &dyn Fn(&Path)->bool,
    ) -> io::Result<Self> {
        if let Some(rule) = guarded_ignore(path, &path.join(".gitignore"), allowed)? {
            git_rules.push(rule);
        }
        if let Some(rule) = guarded_ignore(path, &path.join(".ignore"), allowed)? {
            ignore_rules.push(rule);
        }
        Ok(Self {
            depth,
            entries: fs::read_dir(path)?,
            git_rules,
            ignore_rules,
        })
    }
}

fn rule_match(rules: &[Arc<Gitignore>], path: &Path, is_dir: bool) -> Option<bool> {
    for rule in rules.iter().rev() {
        match rule.matched_path_or_any_parents(path, is_dir) {
            ignore::Match::Ignore(_) => return Some(true),
            ignore::Match::Whitelist(_) => return Some(false),
            ignore::Match::None => (),
        }
    }
    None
}

/// Fixed control filenames only; no library-triggered parent/global/worktree reads.
fn guarded_ignore(scope: &Path, path: &Path, allowed: &dyn Fn(&Path)->bool) -> io::Result<Option<Arc<Gitignore>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(None)
        }
        Err(error) => return Err(error),
    };
    if !metadata.is_file()
        || metadata.len() > MAX_CODE_FILE_BYTES
        || !ws_platform::content::local_content_allowed(path)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "ignore configuration is unavailable or exceeds 2 MiB",
        ));
    }
    if !allowed(path) { return Err(io::Error::new(io::ErrorKind::PermissionDenied,"code policy changed")); }
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_CODE_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if !allowed(path) || bytes.len() as u64 > MAX_CODE_FILE_BYTES
        || metadata.len() != after.len()
        || metadata.modified()? != after.modified()?
        || !ws_platform::content::local_content_allowed(path)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ignore configuration changed during read",
        ));
    }
    let source = String::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut builder = GitignoreBuilder::new(scope);
    for (ordinal, line) in source.lines().enumerate() {
        if ordinal >= 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many ignore patterns",
            ));
        }
        builder
            .add_line(Some(path.to_path_buf()), line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    builder
        .build()
        .map(Arc::new)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Builds a [`CodeFileRecord`] for a single file inside `repo_root`, returning
/// `Ok(None)` if the file vanished, is excluded by policy, exceeds
/// [`MAX_CODE_FILE_BYTES`], or appears to be binary.
pub fn record_for_code_path(repo_root: &Path, path: &Path) -> io::Result<Option<CodeFileRecord>> {
    record_for_code_path_while(repo_root,path,&|_|true)
}

pub fn record_for_code_path_while(repo_root: &Path, path: &Path, allowed:&dyn Fn(&Path)->bool) -> io::Result<Option<CodeFileRecord>> {
    let Ok(relative) = path.strip_prefix(repo_root) else {
        return Ok(None);
    };
    if !allowed(path) || !path.is_absolute()
        || sensitive_code_ancestor(path)
        || relative
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        || !ws_platform::content::local_content_allowed(path)
    {
        return Ok(None);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    let attrs = metadata.file_attributes();
    // Skip reparse points and system files.
    if attrs & (0x400 | 0x4) != 0 {
        return Ok(None);
    }
    let size = metadata.file_size();
    if size > MAX_CODE_FILE_BYTES {
        return Ok(None);
    }

    let name = file_name(path);
    if is_excluded_code_file(&name) {
        return Ok(None);
    }
    // Check ancestor directory components relative to repo_root against excluded dirs.
    if let Ok(rel) = path.strip_prefix(repo_root) {
        if let Some(parent) = rel.parent() {
            for comp in parent.components() {
                let comp_str = comp.as_os_str().to_string_lossy();
                if is_excluded_code_dir(&comp_str) {
                    return Ok(None);
                }
            }
        }
    }

    if is_probably_binary_while(path,allowed)? {
        return Ok(None);
    }

    let ext = extension_of(&name);
    let language = language_for_file(&name, &ext).to_string();
    let rel_path = relative_slash_path(repo_root, path);
    let (volume_serial, file_id) = file_identity(path, false);

    Ok(Some(CodeFileRecord {
        path: path.to_string_lossy().into_owned(),
        rel_path,
        name,
        ext,
        language,
        size,
        mtime: unix_seconds(&metadata),
        volume_serial,
        file_id,
    }))
}

/// True for directory names excluded from code indexing (`node_modules`,
/// `target`, `vendor`, `generated`, `.git`, etc.).
pub fn is_excluded_code_dir(name: &str) -> bool {
    if is_sensitive_code_dir(name) {
        return true;
    }
    if EXCLUDED_CODE_DIRS
        .iter()
        .any(|d| name.eq_ignore_ascii_case(d))
    {
        return true;
    }
    false
}

/// True for file names excluded from code indexing (secrets/keys, minified
/// bundles, generated code, lockfiles, and binary extensions).
pub fn is_excluded_code_file(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if EXCLUDED_CODE_FILES.iter().any(|f| lower == *f) {
        return true;
    }
    if lower.starts_with("id_rsa")
        || lower.starts_with("id_dsa")
        || lower.starts_with("id_ecdsa")
        || lower.starts_with("id_ed25519")
    {
        return true;
    }
    if lower.starts_with(".env") {
        return true;
    }
    if EXCLUDED_CODE_SUFFIXES
        .iter()
        .any(|s| lower.ends_with(&s.to_ascii_lowercase()))
    {
        return true;
    }
    let ext = extension_of(&lower);
    if EXCLUDED_CODE_EXTS.contains(&ext.as_str()) {
        return true;
    }
    false
}

/// Inspects the first 8 KiB of `path` and returns `true` if it contains a NUL
/// byte (standard Git/ripgrep binary heuristic).
pub fn is_probably_binary(path: &Path) -> io::Result<bool> {
    is_probably_binary_while(path,&|_|true)
}

fn is_probably_binary_while(path: &Path, allowed:&dyn Fn(&Path)->bool) -> io::Result<bool> {
    crate::parse::guard_source_path(path)?;
    if !allowed(path) { return Err(io::Error::new(io::ErrorKind::PermissionDenied,"code policy changed")); }
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e)
            if e.kind() == io::ErrorKind::NotFound
                || e.kind() == io::ErrorKind::PermissionDenied =>
        {
            return Ok(true)
        }
        Err(e) => return Err(e),
    };
    let before = file.metadata()?;
    let mut buf = [0u8; 8192];
    let n = file.read(&mut buf)?;
    crate::parse::guard_source_path(path)?;
    let after = file.metadata()?;
    if !allowed(path) || before.len() != after.len() || before.modified()? != after.modified()? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source changed during binary sniff",
        ));
    }
    Ok(buf[..n].contains(&0))
}

/// Windows filesystem identity `(volume_serial, file_id)` used to preserve file
/// identity across renames and moves.
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

fn unix_seconds(metadata: &fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn dir_name(path: &Path) -> String {
    path.file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn extension_of(name: &str) -> String {
    Path::new(name)
        .extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

fn relative_slash_path(repo_root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(repo_root).unwrap_or(path);
    rel.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_record_never_reads_a_source_outside_the_selected_root() {
        let tree = TempDir::new("outside-root");
        let selected = tree.0.join("selected");
        fs::create_dir(&selected).unwrap();
        let outside = tree.0.join("outside.rs");
        fs::write(&outside, "fn outside() {}\n").unwrap();
        assert!(
            record_for_code_path(&selected, &outside).unwrap().is_none(),
            "outside-root code source was accepted"
        );
        assert!(
            record_for_code_path(&selected, &selected.join("../outside.rs"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn oversized_ignore_file_fails_closed_instead_of_unfiltered_crawl() {
        let tree = TempDir::new("ignore-limit");
        fs::write(tree.0.join("source.rs"), "fn visible() {}\n").unwrap();
        fs::write(
            tree.0.join(".gitignore"),
            vec![b'#'; MAX_CODE_FILE_BYTES as usize + 1],
        )
        .unwrap();
        assert!(
            crawl_repo(&tree.0).is_err(),
            "oversized ignore configuration was read or ignored without a bound"
        );
    }

    #[test]
    fn secret_configurations_and_sensitive_ancestors_are_never_read_as_code() {
        let tree = TempDir::new("secret-configs");
        for relative in [
            ".npmrc",
            ".pypirc",
            ".netrc",
            ".git-credentials",
            ".yarnrc.yml",
            ".aws/config",
            ".ssh/config",
            ".docker/config.json",
            ".git/config",
            ".hg/hgrc",
            ".svn/auth/private.py",
            ".env/private.py",
            ".environment/private.py",
        ] {
            let path = tree.0.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, "SYNTHETIC_PRIVATE_FIXTURE=not-source").unwrap();
            assert!(
                record_for_code_path(&tree.0, &path).unwrap().is_none(),
                "credential configuration accepted: {relative}"
            );
            assert!(
                crate::parse_file(&path).is_err(),
                "direct source parse bypassed credential rule: {relative}"
            );
        }
    }

    #[test]
    fn guarded_ignore_rules_keep_nested_negations_and_priority_inside_selected_root() {
        let tree = TempDir::new("ignore-priority");
        let selected = tree.0.join("selected");
        fs::create_dir_all(selected.join(".git/info")).unwrap();
        fs::create_dir_all(selected.join("sub")).unwrap();
        fs::write(tree.0.join(".gitignore"), "*.rs\n").unwrap();
        fs::write(selected.join(".git/info/exclude"), "from-exclude.rs\n").unwrap();
        fs::write(selected.join(".gitignore"), "only-gitignore.rs\n").unwrap();
        fs::write(selected.join(".ignore"), "!only-gitignore.rs\n*.tmp.rs\n").unwrap();
        fs::write(
            selected.join("sub/.gitignore"),
            "!from-exclude.rs\n!blocked.tmp.rs\n",
        )
        .unwrap();
        fs::write(selected.join("sub/.ignore"), "!wins.tmp.rs\n").unwrap();
        for relative in [
            "root.rs",
            "only-gitignore.rs",
            "from-exclude.rs",
            "sub/from-exclude.rs",
            "sub/blocked.tmp.rs",
            "sub/wins.tmp.rs",
        ] {
            fs::write(selected.join(relative), "fn source() {}\n").unwrap();
        }
        let files = crawl_repo(&selected)
            .unwrap()
            .into_iter()
            .filter(|r| r.ext == "rs")
            .map(|r| r.rel_path)
            .collect::<Vec<_>>();
        assert_eq!(
            files,
            [
                "only-gitignore.rs",
                "root.rs",
                "sub/from-exclude.rs",
                "sub/wins.tmp.rs"
            ]
        );
    }

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "winsearch-ws-code-crawl-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn detect_repos_finds_single_or_nested_git_repos() {
        let workspace = TempDir::new("detect");
        let repo_a = workspace.0.join("repo-a");
        let repo_b = workspace.0.join("nested").join("repo-b");
        fs::create_dir_all(repo_a.join(".git")).unwrap();
        fs::create_dir_all(&repo_b).unwrap();
        // Worktree-style `.git` file is also recognized.
        fs::write(repo_b.join(".git"), b"gitdir: ../.git/worktrees/repo-b\n").unwrap();

        let detected = detect_repos(&workspace.0).unwrap();
        assert_eq!(detected.len(), 2);
        assert_eq!(detected[0].name, "repo-b");
        assert!(detected[0].has_git);
        assert_eq!(detected[1].name, "repo-a");
        assert!(detected[1].has_git);

        // Direct repo root returns just that repo.
        let direct = detect_repos(&repo_a).unwrap();
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].name, "repo-a");
        assert!(direct[0].has_git);
        assert!(has_git_marker(&repo_a).unwrap());
        assert!(has_git_marker(&repo_b).unwrap());
        assert!(!has_git_marker(&workspace.0).unwrap());
    }

    #[test]
    fn crawl_honors_nested_gitignore_negation_and_code_exclusions() {
        let repo = TempDir::new("gitignore");
        fs::create_dir_all(repo.0.join(".git")).unwrap();
        fs::write(repo.0.join(".git").join("HEAD"), b"ref: refs/heads/main\n").unwrap();

        // Root .gitignore: ignore *.log and build_out/, but un-ignore important.log!
        fs::write(
            repo.0.join(".gitignore"),
            b"*.log\n!important.log\nignored_dir/\n",
        )
        .unwrap();

        // Root files
        fs::write(repo.0.join("debug.log"), b"ignored log").unwrap();
        fs::write(repo.0.join("important.log"), b"kept via negation").unwrap();
        fs::write(repo.0.join("README.md"), b"# Repo").unwrap();

        // Ignored dir from root .gitignore
        fs::create_dir_all(repo.0.join("ignored_dir")).unwrap();
        fs::write(repo.0.join("ignored_dir").join("skip.rs"), b"fn skip() {}").unwrap();

        // Nested directory with its own .gitignore + negation
        let src_dir = repo.0.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join(".gitignore"),
            b"*.tmp.rs\n!keep.tmp.rs\nsub_ignored/\n",
        )
        .unwrap();
        fs::write(src_dir.join("lib.rs"), b"pub fn hello() {}").unwrap();
        fs::write(src_dir.join("drop.tmp.rs"), b"fn drop_me() {}").unwrap();
        fs::write(src_dir.join("keep.tmp.rs"), b"pub fn keep_me() {}").unwrap();
        fs::create_dir_all(src_dir.join("sub_ignored")).unwrap();
        fs::write(
            src_dir.join("sub_ignored").join("hidden.rs"),
            b"fn hidden() {}",
        )
        .unwrap();

        // Excluded code trees (even without .gitignore entries)
        for dir in ["vendor", "generated", "node_modules", "target"] {
            let d = repo.0.join(dir);
            fs::create_dir_all(&d).unwrap();
            fs::write(d.join("lib.rs"), b"pub fn excluded() {}").unwrap();
        }

        // Excluded minified, generated suffix, secret keys, and binary files
        fs::write(src_dir.join("app.min.js"), b"function a(){}").unwrap();
        fs::write(src_dir.join("styles.min.css"), b"body{}").unwrap();
        fs::write(src_dir.join("schema.generated.rs"), b"struct Gen;").unwrap();
        fs::write(repo.0.join(".env"), b"SECRET=1").unwrap();
        fs::write(repo.0.join(".env.production"), b"SECRET=2").unwrap();
        fs::write(repo.0.join("id_rsa"), b"PRIVATE KEY").unwrap();
        fs::write(repo.0.join("server.pem"), b"CERT").unwrap();
        fs::write(repo.0.join("signing.key"), b"KEY").unwrap();
        fs::write(src_dir.join("blob.dat"), b"hello\0world\0binary").unwrap();

        let files = crawl_repo(&repo.0).unwrap();
        let rel_paths: Vec<&str> = files.iter().map(|f| f.rel_path.as_str()).collect();

        assert_eq!(
            rel_paths,
            vec![
                ".gitignore",
                "README.md",
                "important.log",
                "src/.gitignore",
                "src/keep.tmp.rs",
                "src/lib.rs",
            ]
        );

        let lib_rs = files.iter().find(|f| f.rel_path == "src/lib.rs").unwrap();
        assert_eq!(lib_rs.language, "rust");
        assert_ne!(lib_rs.file_id, 0);
        assert_ne!(lib_rs.volume_serial, 0);
    }
}
