//! Blocking download with `Range` resume, progress, cancellation, and
//! streamed SHA-256 verification (exact size always, hash whenever the
//! manifest pins one).
//!
//! Chunks are fed to the hasher as they arrive — including any resumed `.part`
//! prefix, which is hashed before appending — so commit-time verification
//! needs no second pass over the file.
//!
//! The engine is `std::thread`-only, so this is deliberately blocking: the
//! caller runs it on a worker thread and polls a shared [`ProgressFn`] /
//! [`std::sync::atomic::AtomicBool`] cancel flag. Partial downloads live next
//! to the destination as `<name>.part`; a later call resumes them.
//!
//! Stall semantics: the cancel flag is checked between socket reads, so a
//! fully stalled connection blocks cancellation until the transfer-level
//! `timeout_global` (30 min) fires; resume then recovers. ureq 3 offers no
//! per-read timeout (only a total-body budget, which would break slow
//! multi-hundred-MB downloads), so there is deliberately no read-timeout knob.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::manifest::ExpectedFile;

/// Progress callback: `(downloaded_bytes, total_bytes)`. Called after every
/// chunk on the downloading thread; keep it cheap (it only records counters
/// in `winsearchd`).
pub type ProgressFn<'a> = dyn Fn(u64, u64) + Send + Sync + 'a;

/// Tuning for one file download. `base_url_override` is test-only: it
/// replaces the `https://huggingface.co/<id>/resolve/main` prefix so tests
/// run against a local fixture server instead of Hugging Face.
#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub base_url_override: Option<String>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            base_url_override: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("cancelled after {0} of {1} bytes")]
    Cancelled(u64, u64),
    #[error("http error: {0}")]
    Http(String),
    #[error("http status {0} for {1}")]
    Status(u16, String),
    #[error("size mismatch for {name}: got {got} bytes, expected {expected}")]
    SizeMismatch {
        name: String,
        got: u64,
        expected: u64,
    },
    #[error("sha-256 mismatch for {name}: got {got}, expected {expected}")]
    HashMismatch {
        name: String,
        got: String,
        expected: String,
    },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Downloads one manifest file into `dir`, resuming `<name>.part` when
/// present. Returns the final path. Verifies exact size always, and SHA-256
/// whenever the manifest pins one (hashed inline during the transfer; see
/// module docs).
pub fn download_file(
    expected: &ExpectedFile,
    dir: &Path,
    options: &DownloadOptions,
    progress: &ProgressFn<'_>,
    cancel: &AtomicBool,
) -> Result<PathBuf, DownloadError> {
    let dest = dir.join(expected.name);
    let part = dir.join(format!("{}.part", expected.name));
    // Bundle files live in subdirectories (`<bundle>/kernels/...`).
    fs::create_dir_all(dest.parent().unwrap_or(dir))?;

    // Already downloaded and valid: skip the network entirely.
    if dest.is_file() && verify_file(&dest, expected).is_ok() {
        info!(
            file = expected.name,
            "already present and valid; skipping download"
        );
        progress(expected.size, expected.size);
        return Ok(dest);
    }
    // A complete-but-invalid leftover can never be resumed.
    if dest.is_file() {
        warn!(
            file = expected.name,
            "existing file failed verification; re-downloading"
        );
        fs::remove_file(&dest)?;
    }

    let url = match &options.base_url_override {
        Some(base) => format!("{}/{}", base.trim_end_matches('/'), expected.name),
        None => expected.url(),
    };

    let mut resume_from = part.metadata().map(|m| m.len()).unwrap_or(0);
    if resume_from > expected.size {
        warn!(
            file = expected.name,
            got = resume_from,
            expected = expected.size,
            "oversized .part file; removing and restarting download"
        );
        fs::remove_file(&part)?;
        resume_from = 0;
    } else if part.is_file() && resume_from == expected.size {
        if verify_file(&part, expected).is_ok() {
            info!(
                file = expected.name,
                "complete .part file verified; committing without download"
            );
            fs::rename(&part, &dest)?;
            progress(expected.size, expected.size);
            return Ok(dest);
        }
        warn!(
            file = expected.name,
            "complete-sized .part file failed verification; removing and restarting"
        );
        fs::remove_file(&part)?;
        resume_from = 0;
    }
    if resume_from > 0 {
        info!(
            file = expected.name,
            bytes = resume_from,
            "resuming partial download"
        );
    }

    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(30 * 60)))
        .build()
        .into();
    let mut request = agent.get(&url);
    if resume_from > 0 {
        request = request.header("Range", format!("bytes={resume_from}-"));
    }
    let mut response = request.call().map_err(|e| match e {
        // Attach the URL to bare status errors for diagnosability.
        ureq::Error::StatusCode(code) => DownloadError::Status(code, url.clone()),
        other => DownloadError::Http(other.to_string()),
    })?;

    let status = response.status().as_u16();
    let (mut downloaded, truncate) = match status {
        // Partial content: append to what is already on disk.
        206 => (resume_from, false),
        // Server ignored (or we sent no) Range: start over.
        200 => {
            if resume_from > 0 {
                info!(file = expected.name, "server ignored Range; restarting");
            }
            (0, true)
        }
        other => return Err(DownloadError::Status(other, url)),
    };

    let total = total_bytes(&response, expected, downloaded);
    let mut out = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(truncate)
        .append(!truncate)
        .open(&part)?;
    // If we restarted, the stale prefix must go even when the file existed.
    if truncate && resume_from > 0 {
        out.set_len(0)?;
    }

    // Streamed SHA-256 over exactly the bytes committed: when appending to a
    // resumed prefix, hash what is already on disk first, then every network
    // chunk below. No hasher when the manifest pins no hash (zero overhead).
    let mut hasher: Option<Sha256> = expected.sha256.map(|_| Sha256::new());
    if let Some(h) = hasher.as_mut() {
        if !truncate && resume_from > 0 {
            hash_existing(&part, h)?;
        }
    }

    progress(downloaded, total);
    let mut reader = response.body_mut().as_reader();
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        if cancel.load(Ordering::Relaxed) {
            // Leave `.part` behind: the next call resumes it (state: paused).
            return Err(DownloadError::Cancelled(downloaded, total));
        }
        let n = reader.read(&mut chunk).map_err(DownloadError::Io)?;
        if n == 0 {
            break;
        }
        out.write_all(&chunk[..n])?;
        if let Some(h) = hasher.as_mut() {
            h.update(&chunk[..n]);
        }
        downloaded += n as u64;
        progress(downloaded, total);
    }
    out.flush()?;
    drop(out);

    // Commit, then verify the committed file: exact size always, then the
    // streamed digest when the manifest pins a hash. A rejected file is
    // deleted so the next attempt restarts clean instead of tripping over
    // (or byte-counting) an unusable leftover.
    fs::rename(&part, &dest)?;
    let got_len = dest.metadata()?.len();
    if got_len != expected.size {
        let _ = fs::remove_file(&dest);
        return Err(DownloadError::SizeMismatch {
            name: expected.name.to_string(),
            got: got_len,
            expected: expected.size,
        });
    }
    if let (Some(hasher), Some(pinned)) = (hasher, expected.sha256) {
        let got = hex::encode(hasher.finalize());
        if got != pinned {
            let _ = fs::remove_file(&dest);
            return Err(DownloadError::HashMismatch {
                name: expected.name.to_string(),
                got,
                expected: pinned.to_string(),
            });
        }
    }
    progress(expected.size, expected.size);
    info!(
        file = expected.name,
        bytes = downloaded,
        "download complete"
    );
    Ok(dest)
}

/// Feeds an already-on-disk prefix (a resumed `.part`) through the running
/// digest so the final hash covers the whole committed file.
fn hash_existing(path: &Path, hasher: &mut Sha256) -> Result<(), DownloadError> {
    let mut file = fs::File::open(path)?;
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        hasher.update(&chunk[..n]);
    }
    Ok(())
}

/// Best-effort total for progress: `Content-Range` total on 206, else the
/// manifest size (servers may omit or lie about lengths; the size check after
/// commit is authoritative).
fn total_bytes(
    response: &ureq::http::Response<ureq::Body>,
    expected: &ExpectedFile,
    resume_from: u64,
) -> u64 {
    if let Some(range) = response
        .headers()
        .get("content-range")
        .and_then(|v| v.to_str().ok())
    {
        // `bytes 123-456/789` — the part after `/` is the full length.
        if let Some(total) = range.rsplit('/').next().and_then(|s| s.parse::<u64>().ok()) {
            return total;
        }
    }
    if response.status().as_u16() == 200 {
        if let Some(len) = response.body().content_length() {
            return len.max(expected.size);
        }
        let _ = resume_from;
    }
    expected.size
}

/// Authoritative post-commit check: exact byte size, then SHA-256 when the
/// manifest pins one. A mismatch deletes nothing here (the caller decides);
/// download callers remove the bad file so the next attempt restarts clean.
pub fn verify_file(path: &Path, expected: &ExpectedFile) -> Result<(), DownloadError> {
    let len = path.metadata().map(|m| m.len()).unwrap_or(0);
    if len != expected.size {
        return Err(DownloadError::SizeMismatch {
            name: expected.name.to_string(),
            got: len,
            expected: expected.size,
        });
    }
    if let Some(pinned) = expected.sha256 {
        let got = sha256_of(path)?;
        if got != pinned {
            return Err(DownloadError::HashMismatch {
                name: expected.name.to_string(),
                got,
                expected: pinned.to_string(),
            });
        }
    }
    Ok(())
}

pub fn sha256_of(path: &Path) -> Result<String, DownloadError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        hasher.update(&chunk[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// Tiny in-memory HTTP server for download tests: serves fixed byte blobs,
    /// honors `Range` (206 + `Content-Range`), and can drip-feed slowly so a
    /// test can cancel mid-stream. Records the `Range` headers it saw.
    struct Fixture {
        base_url: String,
        seen_ranges: Arc<Mutex<Vec<Option<String>>>>,
        _thread: std::thread::JoinHandle<()>,
    }

    impl Fixture {
        fn serve(files: HashMap<String, Vec<u8>>, drip_delay: Duration) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let seen_ranges: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&seen_ranges);
            let thread = std::thread::Builder::new()
                .name("ws-modelmgr-fixture".into())
                .spawn(move || {
                    for stream in listener.incoming().take(64) {
                        let mut stream = match stream {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        // Read one request head (small fixture requests fit).
                        let mut head = vec![0u8; 8192];
                        let n = match std::io::Read::read(&mut stream, &mut head) {
                            Ok(n) => n,
                            Err(_) => continue,
                        };
                        head.truncate(n);
                        let head = String::from_utf8_lossy(&head).into_owned();
                        let mut lines = head.lines();
                        let request_line = lines.next().unwrap_or("");
                        let path = request_line
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or("/")
                            .trim_start_matches('/')
                            .to_string();
                        let mut range: Option<String> = None;
                        for line in lines {
                            if line.is_empty() {
                                break;
                            }
                            if let Some(v) = line.strip_prefix("Range:") {
                                range = Some(v.trim().to_string());
                            }
                            if let Some(v) = line.strip_prefix("range:") {
                                range = Some(v.trim().to_string());
                            }
                        }
                        seen.lock().unwrap().push(range.clone());

                        let body = files.get(&path).cloned().unwrap_or_default();
                        let found = files.contains_key(&path);
                        // Suffix of the blob selected by `bytes=<start>-`.
                        let start = range
                            .as_deref()
                            .and_then(|r| r.strip_prefix("bytes="))
                            .and_then(|r| r.strip_suffix('-'))
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        if !found {
                            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            continue;
                        }
                        let start = start.min(body.len());
                        let slice = &body[start..];
                        let status = if range.is_some() {
                            format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{}/{}\r\n",
                                body.len().saturating_sub(1),
                                body.len()
                            )
                        } else {
                            "HTTP/1.1 200 OK\r\n".to_string()
                        };
                        let header = format!(
                            "{status}Content-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                            slice.len()
                        );
                        if stream.write_all(header.as_bytes()).is_err() {
                            continue;
                        }
                        // Drip-feed so cancellation can land mid-stream.
                        for piece in slice.chunks(1024) {
                            if stream.write_all(piece).is_err() {
                                break;
                            }
                            if drip_delay > Duration::ZERO {
                                std::thread::sleep(drip_delay);
                            }
                        }
                    }
                })
                .unwrap();
            Self {
                base_url,
                seen_ranges,
                _thread: thread,
            }
        }

        fn options(&self) -> DownloadOptions {
            DownloadOptions {
                base_url_override: Some(self.base_url.clone()),
                ..DownloadOptions::default()
            }
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ws-modelmgr-test-{}-{}-{:?}",
            tag,
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn fixture_bytes(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| seed.wrapping_add((i % 251) as u8))
            .collect()
    }

    fn expected_for(name: &'static str, bytes: &[u8]) -> ExpectedFile {
        // Leak a per-test hex string: tests are short-lived; the manifest
        // struct borrows `&'static str` for its pinned constants.
        let hex: &'static str = Box::leak(format!("{:x}", Sha256::digest(bytes)).into_boxed_str());
        ExpectedFile {
            name,
            size: bytes.len() as u64,
            sha256: Some(hex),
        }
    }

    fn last_progress() -> (Arc<Mutex<Vec<(u64, u64)>>>, &'static ProgressFn<'static>) {
        let calls: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let calls2 = Arc::clone(&calls);
        let cb: &'static ProgressFn<'static> = Box::leak(Box::new(move |d: u64, t: u64| {
            calls2.lock().unwrap().push((d, t));
        }));
        let _ = &calls;
        (calls, cb)
    }

    #[test]
    fn full_download_verifies_size_and_hash() {
        let bytes = fixture_bytes(7, 32 * 1024);
        let mut files = HashMap::new();
        files.insert("tiny.gguf".to_string(), bytes.clone());
        let fx = Fixture::serve(files, Duration::ZERO);
        let expected = expected_for("tiny.gguf", &bytes);

        let dir = temp_dir("full");
        let (calls, progress) = last_progress();
        let cancel = AtomicBool::new(false);
        let dest = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap();

        assert_eq!(fs::read(&dest).unwrap(), bytes);
        // Progress reached 100% and totals match the manifest size.
        {
            let calls = calls.lock().unwrap();
            assert!(!calls.is_empty());
            assert_eq!(
                *calls.last().unwrap(),
                (bytes.len() as u64, bytes.len() as u64)
            );
        }

        // Second call is a no-op (already valid) and performs no HTTP.
        let before = fx.seen_ranges.lock().unwrap().len();
        let cancel2 = AtomicBool::new(false);
        download_file(&expected, &dir, &fx.options(), progress, &cancel2).unwrap();
        assert_eq!(fx.seen_ranges.lock().unwrap().len(), before);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn kill_mid_stream_then_resume_via_range() {
        let bytes = fixture_bytes(13, 256 * 1024);
        let mut files = HashMap::new();
        files.insert("big.gguf".to_string(), bytes.clone());
        // 2 ms per KB ≈ 0.5 s for the whole blob: plenty of room to cancel.
        let fx = Fixture::serve(files, Duration::from_millis(2));
        let expected = expected_for("big.gguf", &bytes);
        let dir = temp_dir("resume");
        let (_, progress) = last_progress();

        // First attempt: cancel mid-stream from another thread.
        let cancel = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&cancel);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(80));
            stop.store(true, Ordering::Relaxed);
        });
        let err = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap_err();
        assert!(
            matches!(err, DownloadError::Cancelled(_, _)),
            "expected cancel, got {err:?}"
        );
        // The partial file survived the kill.
        let part_len = dir
            .join("big.gguf.part")
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(part_len > 0 && part_len < bytes.len() as u64);

        // Second attempt resumes: the server must see a Range request, and the
        // committed file must be byte-identical.
        let resume = AtomicBool::new(false);
        let dest = download_file(&expected, &dir, &fx.options(), progress, &resume).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), bytes);
        assert!(
            fx.seen_ranges
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.as_deref().is_some_and(|v| v.starts_with("bytes="))),
            "server never saw a Range resume"
        );
        assert!(!dir.join("big.gguf.part").exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tampered_file_is_rejected() {
        let bytes = fixture_bytes(29, 16 * 1024);
        let mut tampered = bytes.clone();
        tampered[100] ^= 0xff;
        tampered[9000] ^= 0x01;
        let mut files = HashMap::new();
        files.insert("tampered.gguf".to_string(), tampered);
        let fx = Fixture::serve(files, Duration::ZERO);
        // Manifest pins the hash of the *untampered* bytes.
        let expected = expected_for("tampered.gguf", &bytes);

        let dir = temp_dir("tamper");
        let (_, progress) = last_progress();
        let cancel = AtomicBool::new(false);
        let err = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap_err();
        assert!(
            matches!(err, DownloadError::HashMismatch { .. }),
            "expected hash rejection, got {err:?}"
        );
        // Nothing usable left behind.
        assert!(!dir.join("tampered.gguf").exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn size_mismatch_is_rejected() {
        let bytes = fixture_bytes(5, 4096);
        let mut files = HashMap::new();
        files.insert("short.gguf".to_string(), bytes.clone());
        let fx = Fixture::serve(files, Duration::ZERO);
        let mut expected = expected_for("short.gguf", &bytes);
        expected.size += 100; // manifest expects more than the server sends

        let dir = temp_dir("sizemismatch");
        let (_, progress) = last_progress();
        let cancel = AtomicBool::new(false);
        let err = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap_err();
        assert!(
            matches!(err, DownloadError::SizeMismatch { .. }),
            "expected size rejection, got {err:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn complete_valid_part_file_commits_without_http() {
        let bytes = fixture_bytes(41, 16 * 1024);
        let fx = Fixture::serve(HashMap::new(), Duration::ZERO);
        let expected = expected_for("complete-part.gguf", &bytes);
        let dir = temp_dir("complete-part-valid");
        fs::write(dir.join("complete-part.gguf.part"), &bytes).unwrap();

        let (calls, progress) = last_progress();
        let cancel = AtomicBool::new(false);
        let dest = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap();

        assert_eq!(fs::read(&dest).unwrap(), bytes);
        assert!(!dir.join("complete-part.gguf.part").exists());
        assert!(
            fx.seen_ranges.lock().unwrap().is_empty(),
            "complete valid .part file must not trigger HTTP requests"
        );
        assert_eq!(
            *calls.lock().unwrap().last().unwrap(),
            (bytes.len() as u64, bytes.len() as u64)
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_or_corrupt_complete_part_file_is_discarded_and_redownloaded() {
        let bytes = fixture_bytes(53, 16 * 1024);
        let mut files = HashMap::new();
        files.insert("recover.gguf".to_string(), bytes.clone());
        let fx = Fixture::serve(files, Duration::ZERO);
        let expected = expected_for("recover.gguf", &bytes);
        let dir = temp_dir("part-recover");
        let (_, progress) = last_progress();
        let cancel = AtomicBool::new(false);

        // 1. Corrupt .part of exact expected.size: discarded and downloaded fresh without Range.
        let mut corrupt = bytes.clone();
        corrupt[0] ^= 0xff;
        fs::write(dir.join("recover.gguf.part"), &corrupt).unwrap();
        let dest = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), bytes);
        assert!(!dir.join("recover.gguf.part").exists());
        assert_eq!(fx.seen_ranges.lock().unwrap().as_slice(), &[None]);

        // 2. Oversized .part (> expected.size): discarded and downloaded fresh without Range.
        fs::remove_file(&dest).unwrap();
        let mut oversized = bytes.clone();
        oversized.extend_from_slice(&[0xabu8; 512]);
        fs::write(dir.join("recover.gguf.part"), &oversized).unwrap();
        let dest2 = download_file(&expected, &dir, &fx.options(), progress, &cancel).unwrap();
        assert_eq!(fs::read(&dest2).unwrap(), bytes);
        assert!(!dir.join("recover.gguf.part").exists());
        assert_eq!(fx.seen_ranges.lock().unwrap().as_slice(), &[None, None]);

        let _ = fs::remove_dir_all(&dir);
    }
}
