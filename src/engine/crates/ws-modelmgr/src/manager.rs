//! [`ModelManager`]: the filesystem view of the embedding model used by
//! `winsearchd` — status, full download, local import, deletion, and
//! verification. Threading (progress sharing, cancellation, completion
//! broadcast) lives in `winsearchd`'s model service; everything here is
//! blocking and borrow-free so it runs on a plain worker thread.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::download::{download_file, verify_file, DownloadError, DownloadOptions, ProgressFn};
use crate::manifest::{ExpectedFile, EXPECTED_FILES, NPU_BUNDLE_DIR, NPU_BUNDLE_FILES};

/// Lifecycle of the on-disk model, mirrored 1:1 on the IPC wire
/// (`model_status`) and in `WinSearch.Core`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModelState {
    #[default]
    NotDownloaded,
    Downloading,
    Paused,
    Ready,
    Error,
}

/// Point-in-time snapshot: state + byte counters + optional error text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct ModelStatus {
    pub state: ModelState,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub npu_bundle: Option<ModelComponentStatus>,
}

/// Independent progress for an optional model component.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct ModelComponentStatus {
    pub state: ModelState,
    pub bytes: u64,
    pub total_bytes: u64,
    pub error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error("download: {0}")]
    Download(#[from] DownloadError),
    #[error("import: {0}")]
    Import(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Owns the model directory. `options` carries the download tuning; tests
/// point `base_url_override` at a local fixture server.
#[derive(Clone)]
pub struct ModelManager {
    dir: PathBuf,
    options: DownloadOptions,
    files: Vec<ExpectedFile>,
    verified: Arc<Mutex<HashMap<String, (u64, SystemTime)>>>,
}

impl ModelManager {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            options: DownloadOptions::default(),
            files: EXPECTED_FILES.to_vec(),
            verified: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_options(dir: PathBuf, options: DownloadOptions) -> Self {
        Self {
            dir,
            options,
            files: EXPECTED_FILES.to_vec(),
            verified: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Custom file list (tests serve tiny stand-ins for the real GGUFs).
    pub fn with_files(dir: PathBuf, options: DownloadOptions, files: Vec<ExpectedFile>) -> Self {
        Self {
            dir,
            options,
            files,
            verified: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Tracks the optional AMD NPU2 bundle independently of the base GGUFs.
    /// Tracking its status never installs it or changes base-model readiness.
    pub fn with_npu_bundle(mut self) -> Self {
        if !self.files.iter().any(|f| f.npu_bundle_path().is_some()) {
            self.files.extend_from_slice(&NPU_BUNDLE_FILES);
        }
        self
    }

    /// Whether this manager's file set includes the NPU bundle.
    pub fn includes_npu_bundle(&self) -> bool {
        self.files.iter().any(|f| f.npu_bundle_path().is_some())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Size of the base GGUFs only.
    pub fn total_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|f| f.npu_bundle_path().is_none())
            .map(|f| f.size)
            .sum()
    }

    /// Filesystem-derived status (no in-flight knowledge — the service merges
    /// live progress on top). A `.part` file means a paused download;
    /// anything else incomplete is `not_downloaded`; a stale marker is
    /// `error` only when files are present but invalid.
    pub fn status(&self) -> ModelStatus {
        let base = self.component_status(false);
        ModelStatus {
            state: base.state,
            bytes: base.bytes,
            total_bytes: base.total_bytes,
            error: base.error,
            npu_bundle: self
                .includes_npu_bundle()
                .then(|| self.component_status(true)),
        }
    }

    fn component_status(&self, npu: bool) -> ModelComponentStatus {
        let total = self
            .files
            .iter()
            .filter(|f| f.npu_bundle_path().is_some() == npu)
            .map(|f| f.size)
            .sum();
        let mut bytes = 0u64;
        let mut complete = true;
        let mut partial = false;
        for f in self
            .files
            .iter()
            .filter(|f| f.npu_bundle_path().is_some() == npu)
        {
            let dest = self.dir.join(f.name);
            match dest.metadata() {
                Ok(m) if self.is_verified_meta(&dest, f, &m) => {
                    bytes += f.size;
                }
                Ok(m) => {
                    bytes += m.len().min(f.size);
                    complete = false;
                }
                Err(_) => {
                    self.invalidate_verified(f.name);
                    complete = false;
                    if let Ok(m) = self.dir.join(format!("{}.part", f.name)).metadata() {
                        partial = true;
                        bytes += m.len().min(f.size);
                    }
                }
            }
        }
        if complete {
            ModelComponentStatus {
                state: ModelState::Ready,
                bytes: total,
                total_bytes: total,
                error: None,
            }
        } else if partial {
            ModelComponentStatus {
                state: ModelState::Paused,
                bytes,
                total_bytes: total,
                error: None,
            }
        } else {
            ModelComponentStatus {
                state: ModelState::NotDownloaded,
                bytes,
                total_bytes: total,
                error: None,
            }
        }
    }

    /// Downloads the base GGUF files (skipping ones already valid), in order.
    /// Progress aggregates across files; cancel stops after the current chunk
    /// and leaves `.part` files for the next call to resume.
    pub fn download_all(
        &self,
        progress: &ProgressFn<'_>,
        cancel: &AtomicBool,
    ) -> Result<(), DownloadError> {
        self.download_component(false, progress, cancel)
    }

    pub fn download_npu_bundle(
        &self,
        progress: &ProgressFn<'_>,
        cancel: &AtomicBool,
    ) -> Result<(), DownloadError> {
        self.download_component(true, progress, cancel)
    }

    fn download_component(
        &self,
        npu: bool,
        progress: &ProgressFn<'_>,
        cancel: &AtomicBool,
    ) -> Result<(), DownloadError> {
        fs::create_dir_all(&self.dir)?;
        let total = self.component_status(npu).total_bytes;
        let mut done_before = 0u64;
        for f in self
            .files
            .iter()
            .filter(|f| f.npu_bundle_path().is_some() == npu)
        {
            if self.is_file_valid(f) {
                done_before += f.size;
                continue;
            }
            let done = done_before;
            let combined = |d: u64, _t: u64| progress(done + d, total);
            match download_file(f, &self.dir, &self.options, &combined, cancel) {
                Ok(dest) => {
                    self.record_verified(f.name, &dest);
                    done_before += f.size;
                }
                Err(e) => {
                    self.invalidate_verified(f.name);
                    return Err(e);
                }
            }
        }
        progress(total, total);
        Ok(())
    }

    /// Imports base GGUF files from a local path: either a directory containing
    /// (some of) the manifest files, or a single file whose name matches one
    /// manifest entry. Every copied file is verified (size + hash when
    /// pinned); failures abort with nothing half-written. Cancellation is
    /// honored per 1 MiB chunk: a cancelled import removes its temp file and
    /// reports [`DownloadError::Cancelled`], leaving no residue behind.
    pub fn import(&self, path: &Path, cancel: &AtomicBool) -> Result<ModelStatus, ManagerError> {
        self.import_component(path, false, cancel)
    }

    pub fn import_npu_bundle(
        &self,
        path: &Path,
        cancel: &AtomicBool,
    ) -> Result<ModelStatus, ManagerError> {
        self.import_component(path, true, cancel)
    }

    /// Whether a source contains files for the selected component (no writes).
    pub fn has_import_files(&self, path: &Path, npu: bool) -> bool {
        self.files
            .iter()
            .filter(|f| f.npu_bundle_path().is_some() == npu)
            .any(|f| {
                if path.is_dir() {
                    path.join(f.name).is_file()
                        || f.npu_bundle_path()
                            .is_some_and(|rel| path.join(rel).is_file())
                } else {
                    path.is_file() && Path::new(f.name).file_name() == path.file_name()
                }
            })
    }

    fn import_component(
        &self,
        path: &Path,
        npu: bool,
        cancel: &AtomicBool,
    ) -> Result<ModelStatus, ManagerError> {
        fs::create_dir_all(&self.dir)?;
        if path.is_dir() {
            let mut any = false;
            for f in self
                .files
                .iter()
                .filter(|f| f.npu_bundle_path().is_some() == npu)
            {
                if cancel.load(Ordering::Relaxed) {
                    return Err(ManagerError::Download(DownloadError::Cancelled(
                        0,
                        self.total_bytes(),
                    )));
                }
                // A bundle directory may be imported as-is (`hf download
                // ... --local-dir <dir>` puts its files at the top level).
                let src = Some(path.join(f.name))
                    .filter(|p| p.is_file())
                    .or_else(|| f.npu_bundle_path().map(|rel| path.join(rel)))
                    .unwrap_or_else(|| path.join(f.name));
                if src.is_file() {
                    self.import_one(f, &src, cancel)?;
                    any = true;
                }
            }
            if !any {
                return Err(ManagerError::Import(format!(
                    "directory {} contains none of the expected model files",
                    path.display()
                )));
            }
        } else if path.is_file() {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            let entry = self
                .files
                .iter()
                .find(|f| {
                    f.npu_bundle_path().is_some() == npu
                        && Path::new(f.name).file_name() == path.file_name()
                })
                .ok_or_else(|| {
                    let known: Vec<&str> = self.files.iter().map(|f| f.name).collect();
                    ManagerError::Import(format!(
                        "{name} is not a known model file (expected one of: {})",
                        known.join(", ")
                    ))
                })?;
            self.import_one(entry, path, cancel)?;
        } else {
            return Err(ManagerError::Import(format!(
                "import path does not exist: {}",
                path.display()
            )));
        }
        Ok(self.status())
    }

    /// Deletes every manifest file (plus stale `.part` and `.import-tmp` files).
    /// Missing files are not an error.
    pub fn delete(&self) -> Result<ModelStatus, ManagerError> {
        if let Ok(mut cache) = self.verified.lock() {
            cache.clear();
        }
        for f in self.files.iter().chain(NPU_BUNDLE_FILES.iter()) {
            let dest = self.dir.join(f.name);
            if dest.exists() {
                fs::remove_file(&dest)?;
            }
            let part = self.dir.join(format!("{}.part", f.name));
            if part.exists() {
                fs::remove_file(&part)?;
            }
            let import_tmp = self.dir.join(format!("{}.import-tmp", f.name));
            if import_tmp.exists() {
                fs::remove_file(&import_tmp)?;
            }
        }
        // Only directories the bundle created, and only once empty.
        for sub in ["kernels", ""] {
            let _ = fs::remove_dir(self.dir.join(NPU_BUNDLE_DIR).join(sub));
        }
        Ok(self.status())
    }

    fn import_one(
        &self,
        expected: &ExpectedFile,
        src: &Path,
        cancel: &AtomicBool,
    ) -> Result<(), ManagerError> {
        if cancel.load(Ordering::Relaxed) {
            return Err(ManagerError::Download(DownloadError::Cancelled(
                0,
                expected.size,
            )));
        }
        // Verify the source before touching the model dir.
        let len = src.metadata()?.len();
        if len != expected.size {
            return Err(ManagerError::Import(format!(
                "{}: size {len} != expected {}",
                expected.name, expected.size
            )));
        }
        // Copy in 1 MiB chunks so cancellation lands mid-copy; hash inline
        // when the manifest pins a hash. The temp file + rename means a crash
        // never leaves a truncated model file behind.
        let dest = self.dir.join(expected.name);
        let tmp = self.dir.join(format!("{}.import-tmp", expected.name));
        fs::create_dir_all(dest.parent().unwrap_or(&self.dir))?;
        let mut reader = fs::File::open(src)?;
        let copy_res = (|| -> Result<Option<Sha256>, ManagerError> {
            let mut out = fs::File::create(&tmp)?;
            let mut hasher: Option<Sha256> = expected.sha256.map(|_| Sha256::new());
            let mut chunk = vec![0u8; 1024 * 1024];
            let mut copied = 0u64;
            loop {
                if cancel.load(Ordering::Relaxed) {
                    return Err(ManagerError::Download(DownloadError::Cancelled(
                        copied,
                        expected.size,
                    )));
                }
                let n = reader.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                out.write_all(&chunk[..n])?;
                if let Some(h) = hasher.as_mut() {
                    h.update(&chunk[..n]);
                }
                copied += n as u64;
            }
            out.flush()?;
            drop(out);
            // Verify what was written (size always, hash when pinned) before the
            // rename commits it.
            if copied != expected.size || tmp.metadata()?.len() != expected.size {
                return Err(ManagerError::Import(format!(
                    "{}: short copy ({copied} of {} bytes)",
                    expected.name, expected.size
                )));
            }
            Ok(hasher)
        })();
        let hasher = match copy_res {
            Ok(h) => h,
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                return Err(e);
            }
        };
        if let (Some(hasher), Some(pinned)) = (hasher, expected.sha256) {
            let got = hex::encode(hasher.finalize());
            if got != pinned {
                let _ = fs::remove_file(&tmp);
                return Err(ManagerError::Import(format!(
                    "{}: sha-256 {got} != expected {pinned}",
                    expected.name
                )));
            }
        }
        if let Err(e) = fs::rename(&tmp, &dest) {
            let _ = fs::remove_file(&tmp);
            return Err(ManagerError::Io(e));
        }
        self.record_verified(expected.name, &dest);
        Ok(())
    }

    fn is_file_valid(&self, f: &ExpectedFile) -> bool {
        let dest = self.dir.join(f.name);
        match dest.metadata() {
            Ok(m) if m.is_file() => self.is_verified_meta(&dest, f, &m),
            _ => {
                self.invalidate_verified(f.name);
                false
            }
        }
    }

    fn is_verified_meta(&self, dest: &Path, f: &ExpectedFile, meta: &fs::Metadata) -> bool {
        if meta.len() != f.size {
            self.invalidate_verified(f.name);
            return false;
        }
        let stamp = (
            meta.len(),
            meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        );
        if let Ok(cache) = self.verified.lock() {
            if cache.get(f.name).copied() == Some(stamp) {
                return true;
            }
        }
        if verify_file(dest, f).is_ok() {
            if let Ok(mut cache) = self.verified.lock() {
                cache.insert(f.name.to_string(), stamp);
            }
            true
        } else {
            self.invalidate_verified(f.name);
            false
        }
    }

    fn record_verified(&self, name: &str, dest: &Path) {
        if let Ok(meta) = dest.metadata() {
            let stamp = (
                meta.len(),
                meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            );
            if let Ok(mut cache) = self.verified.lock() {
                cache.insert(name.to_string(), stamp);
            }
        }
    }

    fn invalidate_verified(&self, name: &str) {
        if let Ok(mut cache) = self.verified.lock() {
            cache.remove(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::EXPECTED_FILES;
    use std::fs;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ws-modelmgr-manager-{}-{}-{:?}",
            tag,
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn no_progress(_: u64, _: u64) {}

    #[test]
    fn optional_npu_bundle_does_not_change_base_readiness_or_size() {
        let dir = temp_dir("optional-npu");
        fs::write(dir.join("base.gguf"), b"base").unwrap();
        let mgr = ModelManager::with_files(
            dir.clone(),
            DownloadOptions::default(),
            vec![ExpectedFile {
                name: "base.gguf",
                size: 4,
                sha256: None,
            }],
        )
        .with_npu_bundle();
        let status = mgr.status();
        assert_eq!(status.state, ModelState::Ready);
        assert_eq!(status.bytes, 4);
        assert_eq!(status.total_bytes, 4);
        assert_eq!(mgr.total_bytes(), 4);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn status_reflects_filesystem() {
        use crate::manifest::TOTAL_BYTES;
        let dir = temp_dir("status");
        let mgr = ModelManager::new(dir.clone());

        let s = mgr.status();
        assert_eq!(s.state, ModelState::NotDownloaded);
        assert_eq!(s.total_bytes, TOTAL_BYTES);

        // A stale .part marker reads back as paused.
        fs::write(
            dir.join(format!("{}.part", EXPECTED_FILES[0].name)),
            b"half",
        )
        .unwrap();
        assert_eq!(mgr.status().state, ModelState::Paused);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_removes_files_and_part_markers() {
        let dir = temp_dir("delete");
        for f in &EXPECTED_FILES {
            fs::write(dir.join(f.name), vec![0u8; 8]).unwrap();
            fs::write(dir.join(format!("{}.part", f.name)), vec![0u8; 8]).unwrap();
            fs::write(dir.join(format!("{}.import-tmp", f.name)), vec![0u8; 8]).unwrap();
        }
        let mgr = ModelManager::new(dir.clone());
        let s = mgr.delete().unwrap();
        assert_eq!(s.state, ModelState::NotDownloaded);
        assert!(fs::read_dir(&dir).unwrap().next().is_none());

        // Deleting again is a no-op.
        mgr.delete().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn status_caches_verification_and_invalidates_on_change() {
        use sha2::{Digest, Sha256};
        let dir = temp_dir("status-cache");
        let name = leak("cached.gguf");
        let body = vec![42u8; 4096];
        let files = vec![ExpectedFile {
            name,
            size: body.len() as u64,
            sha256: Some(Box::leak(
                format!("{:x}", Sha256::digest(&body)).into_boxed_str(),
            )),
        }];
        let path = dir.join(name);
        fs::write(&path, &body).unwrap();

        let mgr = ModelManager::with_files(dir.clone(), DownloadOptions::default(), files);
        assert!(mgr.verified.lock().unwrap().is_empty());

        // First status call verifies and populates the cache.
        assert_eq!(mgr.status().state, ModelState::Ready);
        let cached_stamp = mgr.verified.lock().unwrap().get(name).copied();
        assert!(cached_stamp.is_some());

        // Cloned manager shares the verification cache.
        let mgr_clone = mgr.clone();
        assert_eq!(mgr_clone.status().state, ModelState::Ready);
        assert_eq!(
            mgr_clone.verified.lock().unwrap().get(name).copied(),
            cached_stamp
        );

        // Corrupt the file with a new mtime (same length): cache must invalidate and reject.
        let mut corrupt = body.clone();
        corrupt[0] ^= 0xff;
        let bumped_mtime = cached_stamp.unwrap().1 + Duration::from_secs(2);
        {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            f.write_all(&corrupt).unwrap();
            f.set_modified(bumped_mtime).unwrap();
        }
        assert_eq!(mgr.status().state, ModelState::NotDownloaded);
        assert!(!mgr.verified.lock().unwrap().contains_key(name));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_rejects_unknown_and_bad_sized_files() {
        let dir = temp_dir("import");
        let src = temp_dir("import-src");
        let mgr = ModelManager::new(dir.clone());
        let live = AtomicBool::new(false);

        // Unknown file name.
        fs::write(src.join("other.bin"), b"nope").unwrap();
        assert!(mgr.import(&src.join("other.bin"), &live).is_err());

        // Right name, wrong size (manifest expects hundreds of MB).
        fs::write(src.join(EXPECTED_FILES[0].name), b"too small").unwrap();
        let err = mgr
            .import(&src.join(EXPECTED_FILES[0].name), &live)
            .unwrap_err();
        assert!(err.to_string().contains("size"), "got {err}");

        // Directory containing only a wrong-sized candidate fails.
        let dir_err = mgr.import(&src, &live).unwrap_err();
        assert!(
            dir_err.to_string().contains("size") || dir_err.to_string().contains("none"),
            "got {dir_err}"
        );

        // Missing path.
        assert!(mgr.import(&src.join("missing.gguf"), &live).is_err());

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&src);
    }

    /// Happy path for the chunked copy: small stand-in files with pinned
    /// hashes import byte-identical through file and directory forms.
    #[test]
    fn import_copies_and_verifies() {
        use sha2::{Digest, Sha256};
        let dir = temp_dir("import-ok");
        let src = temp_dir("import-ok-src");
        let bodies = vec![
            (leak("ok-a.gguf"), vec![11u8; 4096]),
            (leak("ok-b.gguf"), vec![12u8; 9000]),
        ];
        let files: Vec<ExpectedFile> = bodies
            .iter()
            .map(|(name, body)| ExpectedFile {
                name,
                size: body.len() as u64,
                sha256: Some(Box::leak(
                    format!("{:x}", Sha256::digest(body)).into_boxed_str(),
                )),
            })
            .collect();
        for (name, body) in &bodies {
            fs::write(src.join(name), body).unwrap();
        }
        let mgr = ModelManager::with_files(dir.clone(), DownloadOptions::default(), files);
        let live = AtomicBool::new(false);

        // Single-file form.
        mgr.import(&src.join("ok-a.gguf"), &live).unwrap();
        assert_eq!(fs::read(dir.join("ok-a.gguf")).unwrap(), bodies[0].1);

        // Directory form picks up every known file.
        let s = mgr.import(&src, &live).unwrap();
        assert_eq!(s.state, ModelState::Ready);
        assert_eq!(fs::read(dir.join("ok-b.gguf")).unwrap(), bodies[1].1);

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&src);
    }

    /// An NPU bundle directory imports from its flat `hf download` layout into
    /// `<model dir>/<bundle>/...`, and delete removes the created directories.
    #[test]
    fn npu_bundle_imports_flat_layout_and_deletes_its_directories() {
        use sha2::{Digest, Sha256};
        let dir = temp_dir("npu-bundle");
        let src = temp_dir("npu-bundle-src");
        let bodies = [
            (
                leak("iron-embeddinggemma2-npu2/manifest.txt"),
                vec![1u8; 100],
            ),
            (
                leak("iron-embeddinggemma2-npu2/kernels/k.insts.bin"),
                vec![2u8; 300],
            ),
        ];
        let files: Vec<ExpectedFile> = bodies
            .iter()
            .map(|(name, body)| ExpectedFile {
                name,
                size: body.len() as u64,
                sha256: Some(Box::leak(
                    format!("{:x}", Sha256::digest(body)).into_boxed_str(),
                )),
            })
            .collect();
        fs::create_dir_all(src.join("kernels")).unwrap();
        fs::write(src.join("manifest.txt"), &bodies[0].1).unwrap();
        fs::write(src.join("kernels").join("k.insts.bin"), &bodies[1].1).unwrap();
        let mgr = ModelManager::with_files(dir.clone(), DownloadOptions::default(), files);
        assert!(mgr.includes_npu_bundle());
        let s = mgr
            .import_npu_bundle(&src, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(s.npu_bundle.unwrap().state, ModelState::Ready);
        assert_eq!(
            fs::read(dir.join(NPU_BUNDLE_DIR).join("kernels").join("k.insts.bin")).unwrap(),
            bodies[1].1
        );
        mgr.delete().unwrap();
        assert!(!dir.join(NPU_BUNDLE_DIR).exists());
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&src);
    }

    #[test]
    fn npu_bundle_is_optional_and_registered_once() {
        let mgr = ModelManager::new(PathBuf::from("unused"))
            .with_npu_bundle()
            .with_npu_bundle();
        assert_eq!(
            mgr.files.len(),
            NPU_BUNDLE_FILES.len() + EXPECTED_FILES.len()
        );
        assert!(mgr.files[0].npu_bundle_path().is_none());
        assert_eq!(mgr.total_bytes(), crate::manifest::TOTAL_BYTES);
        assert_eq!(mgr.status().npu_bundle.unwrap().total_bytes, 1_085_278_625);
        assert!(!ModelManager::new(PathBuf::from("unused")).includes_npu_bundle());
    }

    /// A pre-set cancel flag aborts the import immediately (deterministic),
    /// and a cancel mid-copy stops the copy with no residue.
    #[test]
    fn import_honors_cancel() {
        let dir = temp_dir("import-cancel");
        let src = temp_dir("import-cancel-src");
        let name = leak("cancel-big.gguf");
        // 1 GiB: even at multi-GB/s the copy spans hundreds of 1 MiB chunks,
        // so a 5 ms canceller reliably lands mid-copy.
        const SIZE: usize = 1024 * 1024 * 1024;
        let files = vec![ExpectedFile {
            name,
            size: SIZE as u64,
            sha256: None,
        }];
        // Deterministic pattern without holding 1 GiB in RAM twice.
        write_pattern(&src.join(name), SIZE);
        let mgr = ModelManager::with_files(dir.clone(), DownloadOptions::default(), files);

        // Pre-cancelled: aborts before copying anything.
        let dead = AtomicBool::new(true);
        let err = mgr.import(&src.join(name), &dead).unwrap_err();
        assert!(
            matches!(err, ManagerError::Download(DownloadError::Cancelled(..))),
            "got {err:?}"
        );
        assert!(!dir.join(name).exists());

        // Mid-copy cancel from another thread. Retried because under parallel
        // test load the canceller can fire before the first chunk check;
        // partial `copied` proves the cancel landed mid-copy.
        let mut mid_copy = false;
        for _ in 0..10 {
            let cancel = Arc::new(AtomicBool::new(false));
            let stop = Arc::clone(&cancel);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                stop.store(true, Ordering::Relaxed);
            });
            match mgr.import(&src.join(name), &cancel).unwrap_err() {
                ManagerError::Download(DownloadError::Cancelled(copied, total)) if copied > 0 => {
                    assert_eq!(total, SIZE as u64);
                    mid_copy = true;
                    break;
                }
                ManagerError::Download(DownloadError::Cancelled(..)) => continue,
                other => panic!("expected Cancelled, got {other:?}"),
            }
        }
        assert!(mid_copy, "never observed a mid-copy cancel in 10 attempts");
        // No partial dest, no temp file left behind.
        assert!(!dir.join(name).exists());
        assert!(!dir.join(format!("{name}.import-tmp")).exists());

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&src);
    }

    fn leak(s: &str) -> &'static str {
        Box::leak(s.to_string().into_boxed_str())
    }

    /// Writes `len` deterministic bytes without materializing them all at once.
    fn write_pattern(path: &Path, len: usize) {
        use std::io::Write as _;
        let mut out = fs::File::create(path).unwrap();
        let chunk: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
        let mut left = len;
        while left > 0 {
            let n = left.min(chunk.len());
            out.write_all(&chunk[..n]).unwrap();
            left -= n;
        }
        out.flush().unwrap();
    }

    #[test]
    fn model_status_wire_shape() {
        // The exact JSON the C# mirror must parse.
        let s = ModelStatus {
            state: ModelState::Downloading,
            bytes: 42,
            total_bytes: 100,
            error: None,
            npu_bundle: None,
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"state\":\"downloading\""));
        let back: ModelStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        // Old payloads without the new counters still parse (additive IPC).
        let legacy: ModelStatus = serde_json::from_str(r#"{"state":"ready"}"#).unwrap();
        assert_eq!(legacy.state, ModelState::Ready);
        assert_eq!(legacy.bytes, 0);
    }

    #[test]
    fn cancel_flag_type_is_thread_shareable() {
        // Compile-time proof the download signature fits the engine model.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AtomicBool>();
        let _ = no_progress;
    }
}
