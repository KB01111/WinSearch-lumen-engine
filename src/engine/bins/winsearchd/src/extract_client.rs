//! A fresh, job-owned extraction child per batch; no in-process fallback.
use std::io::{Read, Write};
use std::os::windows::io::AsRawHandle;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows::Win32::System::Threading::WaitForSingleObject;
use windows::Win32::System::IO::CancelSynchronousIo;
use ws_extract::worker::{
    encode_request, validate_response, ExtractRequest, ExtractResponse, WorkerHello,
};
use ws_extract::{ExtractOutput, ExtractionStatus};
use ws_platform::child_job::ChildJob;

#[derive(Debug, thiserror::Error)]
pub enum ExtractClientError {
    #[error("extraction batch timed out")]
    Timeout,
    #[error("extraction cancelled by policy or source change")]
    Cancelled,
    #[error("malformed extraction output: {0}")]
    Malformed(String),
    #[error("extraction worker failed: {0}")]
    Worker(String),
    #[error("unsupported extraction input: {0}")]
    Unsupported(String),
}

impl ExtractClientError {
    fn worker(error: impl std::fmt::Display) -> Self {
        Self::Worker(error.to_string())
    }
}

pub struct ExtractClient {
    worker_bin: PathBuf,
    cache_dir: PathBuf,
    #[cfg(test)]
    worker_args: Vec<String>,
}

impl ExtractClient {
    pub fn new(worker_bin: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            worker_bin,
            cache_dir,
            #[cfg(test)]
            worker_args: Vec::new(),
        }
    }

    pub fn production() -> Self {
        let worker_bin = std::env::current_exe()
            .expect("daemon executable path")
            .with_file_name("winsearch-extract.exe");
        Self::new(worker_bin, ws_platform::paths::cache_dir())
    }

    pub fn request(&self, path: PathBuf, kind: &str, max_units: u32) -> ExtractRequest {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        ExtractRequest {
            path,
            kind: kind.into(),
            scratch_dir: self.cache_dir.join("extract").join(format!(
                "{}-{stamp}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
            cursor: 0,
            max_units,
        }
    }

    #[allow(dead_code)] // Public one-request API; production adds policy cancellation.
    pub fn extract(
        &self,
        request: &ExtractRequest,
        timeout: Duration,
    ) -> Result<ExtractResponse, ExtractClientError> {
        self.extract_while(request, timeout, || true)
    }

    pub fn extract_while(
        &self,
        request: &ExtractRequest,
        timeout: Duration,
        allowed: impl Fn() -> bool,
    ) -> Result<ExtractResponse, ExtractClientError> {
        let deadline = Instant::now() + timeout;
        let frame = encode_request(request).map_err(ExtractClientError::Malformed)?;
        if !allowed() {
            return Err(ExtractClientError::Cancelled);
        }
        // Scratch is created exclusively. Never remove a directory supplied by
        // another request, even if a caller accidentally reuses its pathname.
        let parent = self.cache_dir.join("extract");
        if request.scratch_dir.parent() != Some(parent.as_path())
            || !request.scratch_dir.is_absolute()
        {
            return Err(ExtractClientError::Malformed(
                "scratch directory outside app cache".into(),
            ));
        }
        std::fs::create_dir_all(&parent).map_err(ExtractClientError::worker)?;
        if !ws_platform::content::local_content_allowed(&parent) {
            return Err(ExtractClientError::Malformed(
                "unsafe scratch parent".into(),
            ));
        }
        std::fs::create_dir(&request.scratch_dir).map_err(ExtractClientError::worker)?;
        let mut scratch = Scratch(Some(request.scratch_dir.clone()));
        let mut promoted = ArtifactLease::default();
        let result = (|| {
            let mut command = Command::new(&self.worker_bin);
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            #[cfg(test)]
            command.args(&self.worker_args);
            let mut job = ChildJob::spawn(&mut command).map_err(ExtractClientError::worker)?;
            let mut stdin = job.child_mut().stdin.take().expect("piped stdin");
            let mut stdout = job.child_mut().stdout.take().expect("piped stdout");
            let (sender, receiver) = mpsc::channel();
            let writer_sender = sender.clone();
            let writer = thread::spawn(move || {
                let result = stdin
                    .write_all(&frame)
                    .and_then(|_| stdin.flush())
                    .map_err(ExtractClientError::worker);
                let _ = writer_sender.send(Event::Written(result));
            });
            let kind = request.kind.clone();
            let reader = thread::spawn(move || {
                let result = (|| {
                    let hello: WorkerHello = ws_protocol::read_message(&mut stdout)
                        .map_err(|e| ExtractClientError::Malformed(e.to_string()))?
                        .ok_or_else(|| {
                            ExtractClientError::Worker("no capability greeting".into())
                        })?;
                    if hello.supported_kinds.len() > 16
                        || hello.supported_kinds.iter().any(|s| s.len() > 32)
                    {
                        return Err(ExtractClientError::Malformed("invalid capabilities".into()));
                    }
                    if !hello.supported_kinds.contains(&kind) {
                        return Err(ExtractClientError::Unsupported(kind));
                    }
                    ws_protocol::read_message(&mut stdout)
                        .map_err(|e| ExtractClientError::Malformed(e.to_string()))?
                        .ok_or_else(|| {
                            ExtractClientError::Worker("worker exited without a response".into())
                        })
                })();
                let _ = sender.send(Event::Response(result));
            });
            let mut exchange = Exchange {
                job,
                writer: Some(writer),
                reader: Some(reader),
                closed: false,
            };
            let outcome = (|| {
                let (mut written, mut response) = (false, None);
                loop {
                    if !allowed() {
                        return Err(ExtractClientError::Cancelled);
                    }
                    if Instant::now() >= deadline {
                        return Err(ExtractClientError::Timeout);
                    }
                    match receiver.recv_timeout(
                        Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    ) {
                        Ok(Event::Written(result)) => {
                            result?;
                            written = true;
                        }
                        Ok(Event::Response(result)) => response = Some(result?),
                        Err(mpsc::RecvTimeoutError::Timeout) => (),
                        Err(mpsc::RecvTimeoutError::Disconnected)
                            if !written || response.is_none() =>
                        {
                            return Err(ExtractClientError::Worker(
                                "pipe task ended unexpectedly".into(),
                            ))
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            thread::sleep(Duration::from_millis(1))
                        }
                    }
                    if written && response.is_some() {
                        if let Some(status) = exchange
                            .job
                            .child_mut()
                            .try_wait()
                            .map_err(ExtractClientError::worker)?
                        {
                            if !status.success() {
                                return Err(ExtractClientError::Worker(format!("exit {status}")));
                            }
                            break;
                        }
                    }
                }
                Ok(response.expect("received response"))
            })();
            // All child writers are gone before validating or promoting artifacts.
            exchange.close()?;
            let mut response = outcome?;
            validate_response(request, &response).map_err(ExtractClientError::Malformed)?;
            match &response.output.status {
                ExtractionStatus::Unsupported => {
                    return Err(ExtractClientError::Unsupported(request.kind.clone()))
                }
                ExtractionStatus::Failed(message) => {
                    return Err(ExtractClientError::Worker(message.clone()))
                }
                _ => (),
            }
            if !allowed() {
                return Err(ExtractClientError::Cancelled);
            }
            self.promote(request, &mut response, &mut promoted)?;
            if !allowed() {
                return Err(ExtractClientError::Cancelled);
            }
            Ok(response)
        })();
        scratch.clean().map_err(ExtractClientError::worker)?;
        let response = result?;
        promoted.keep();
        Ok(response)
    }

    fn promote(
        &self,
        request: &ExtractRequest,
        response: &mut ExtractResponse,
        promoted: &mut ArtifactLease,
    ) -> Result<(), ExtractClientError> {
        let scratch = request
            .scratch_dir
            .canonicalize()
            .map_err(ExtractClientError::worker)?;
        let destination = self.cache_dir.join("thumbs");
        std::fs::create_dir_all(&destination).map_err(ExtractClientError::worker)?;
        if !ws_platform::content::local_content_allowed(&destination) {
            return Err(ExtractClientError::Malformed("unsafe cache path".into()));
        }
        let mut moved: std::collections::HashMap<PathBuf, PathBuf> =
            std::collections::HashMap::new();
        for (index, segment) in response.output.segments.iter_mut().enumerate() {
            let Some(source) = segment.thumbnail_hint.as_ref() else {
                continue;
            };
            if !ws_platform::content::local_content_allowed(source) {
                return Err(ExtractClientError::Malformed(
                    "unsafe thumbnail source".into(),
                ));
            }
            let canonical = source
                .canonicalize()
                .map_err(|e| ExtractClientError::Malformed(e.to_string()))?;
            if !canonical.starts_with(&scratch) || !canonical.is_file() {
                return Err(ExtractClientError::Malformed(
                    "thumbnail outside scratch".into(),
                ));
            }
            if let Some(path) = moved.get(&canonical) {
                segment.thumbnail_hint = Some(path.clone());
                continue;
            }
            let name = format!(
                "extract-{}-{index}.png",
                request
                    .scratch_dir
                    .file_name()
                    .expect("private scratch name")
                    .to_string_lossy()
            );
            let path = destination.join(name);
            let mut target = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(ExtractClientError::worker)?;
            promoted.paths.push(path.clone());
            let source_file =
                std::fs::File::open(&canonical).map_err(ExtractClientError::worker)?;
            let bytes = std::io::copy(&mut source_file.take(16 * 1024 * 1024 + 1), &mut target)
                .map_err(ExtractClientError::worker)?;
            if bytes > 16 * 1024 * 1024 {
                return Err(ExtractClientError::Malformed("thumbnail too large".into()));
            }
            moved.insert(canonical, path.clone());
            segment.thumbnail_hint = Some(path);
        }
        Ok(())
    }
}

/// The daemon keeps this lease until its final policy/source check and commit.
/// A stale or failed commit removes promoted files instead of orphaning them.
#[derive(Default)]
pub struct ArtifactLease {
    paths: Vec<PathBuf>,
}
impl ArtifactLease {
    pub fn for_output(output: &ExtractOutput) -> Self {
        Self {
            paths: output
                .segments
                .iter()
                .filter_map(|s| s.thumbnail_hint.clone())
                .collect(),
        }
    }
    pub fn keep(&mut self) {
        self.paths.clear();
    }
}
impl Drop for ArtifactLease {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

enum Event {
    Written(Result<(), ExtractClientError>),
    Response(Result<ExtractResponse, ExtractClientError>),
}
struct Scratch(Option<PathBuf>);
impl Scratch {
    fn clean(&mut self) -> std::io::Result<()> {
        if let Some(path) = self.0.as_ref() {
            std::fs::remove_dir_all(path)?;
            self.0 = None;
        }
        Ok(())
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.clean();
    }
}
struct Exchange {
    job: ChildJob,
    writer: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    closed: bool,
}
impl Exchange {
    fn close(&mut self) -> Result<(), ExtractClientError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let child_result = self
            .job
            .terminate_and_reap()
            .map_err(ExtractClientError::worker);
        let deadline = Instant::now() + Duration::from_secs(1);
        // Repeated cancellation also covers a task scheduled after the first
        // cancellation call. Never join a still-running synchronous I/O task.
        let stopped = |task: &JoinHandle<()>| unsafe {
            WaitForSingleObject(HANDLE(task.as_raw_handle()), 0) == WAIT_OBJECT_0
        };
        while [&self.writer, &self.reader]
            .iter()
            .any(|task| task.as_ref().is_some_and(|t| !stopped(t)))
            && Instant::now() < deadline
        {
            for task in [&self.writer, &self.reader].into_iter().flatten() {
                unsafe {
                    let _ = CancelSynchronousIo(HANDLE(task.as_raw_handle()));
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        let mut complete = true;
        for task in [&mut self.writer, &mut self.reader] {
            if let Some(task) = task.take() {
                if stopped(&task) {
                    complete &= task.join().is_ok();
                } else {
                    complete = false;
                }
            }
        }
        child_result?;
        if !complete {
            return Err(ExtractClientError::Worker(
                "pipe task cleanup did not complete".into(),
            ));
        }
        Ok(())
    }
}
impl Drop for Exchange {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    use std::time::Instant;
    use windows::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };

    struct Tree(PathBuf);
    impl Tree {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "ws-extract-process-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Tree {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn fixture() -> PathBuf {
        static BINARY: OnceLock<PathBuf> = OnceLock::new();
        BINARY
            .get_or_init(|| {
                let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../target/debug")
                    .join(format!(
                        "extract-transport-fixture-{}.exe",
                        std::process::id()
                    ));
                let result = Command::new("rustc")
                    .arg(
                        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                            .join("tests/fixtures/extract_transport.rs"),
                    )
                    .arg("-o")
                    .arg(&path)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
                path
            })
            .clone()
    }

    fn dead(pid_file: &Path) {
        let pid = std::fs::read_to_string(pid_file)
            .unwrap()
            .parse::<u32>()
            .unwrap();
        unsafe {
            if let Ok(handle) = OpenProcess(PROCESS_SYNCHRONIZE, false, pid) {
                assert_eq!(
                    WaitForSingleObject(handle, 1000),
                    WAIT_OBJECT_0,
                    "owned process {pid} survived cleanup"
                );
                CloseHandle(handle).unwrap();
            }
        }
    }

    #[test]
    fn native_extract_rejects_hostile_frames_and_outside_scratch() {
        for mode in ["oversized", "mid-frame", "outside"] {
            let tree = Tree::new();
            let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
            let pid = tree.0.join("pid");
            let outside = tree.0.join("outside.png");
            std::fs::write(&outside, "task-owned outside artifact").unwrap();
            client.worker_args = vec![
                mode.into(),
                pid.to_string_lossy().into_owned(),
                outside.to_string_lossy().into_owned(),
            ];
            let request = client.request(tree.0.join("fixture.txt"), "doc", 256);
            let started = Instant::now();
            assert!(
                matches!(
                    client.extract(&request, Duration::from_secs(3)),
                    Err(ExtractClientError::Malformed(_))
                ),
                "{mode}"
            );
            assert!(started.elapsed() < Duration::from_secs(4));
            dead(&pid);
            assert!(!request.scratch_dir.exists());
            assert_eq!(
                std::fs::read_to_string(&outside).unwrap(),
                "task-owned outside artifact"
            );
        }
    }

    #[test]
    fn native_extract_deadline_unblocks_a_request_larger_than_pipe_capacity() {
        let tree = Tree::new();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        let pid = tree.0.join("pid");
        client.worker_args = vec!["never-read".into(), pid.to_string_lossy().into_owned()];
        let mut request = client.request(tree.0.join("fixture.txt"), "doc", 256);
        request.path = PathBuf::from("界".repeat(50_000));
        assert!(encode_request(&request).unwrap().len() > 64 * 1024);
        let started = Instant::now();
        assert!(matches!(
            client.extract(&request, Duration::from_millis(600)),
            Err(ExtractClientError::Timeout)
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        dead(&pid);
        assert!(!request.scratch_dir.exists());
    }

    #[test]
    fn native_extract_timeout_kills_owned_descendants_and_cancel_cleans_scratch() {
        let tree = Tree::new();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        let pid = tree.0.join("pid");
        let descendant = tree.0.join("descendant");
        client.worker_args = vec![
            "tree".into(),
            pid.to_string_lossy().into_owned(),
            descendant.to_string_lossy().into_owned(),
        ];
        let request = client.request(tree.0.join("fixture.txt"), "doc", 256);
        assert!(matches!(
            client.extract(&request, Duration::from_millis(700)),
            Err(ExtractClientError::Timeout)
        ));
        dead(&pid);
        dead(&descendant);
        assert!(!request.scratch_dir.exists());
        std::fs::remove_file(&descendant).unwrap();
        let request = client.request(tree.0.join("fixture.txt"), "doc", 256);
        let started = Instant::now();
        assert!(matches!(
            client.extract_while(&request, Duration::from_secs(3), || started.elapsed()
                < Duration::from_millis(600)),
            Err(ExtractClientError::Cancelled)
        ));
        dead(&pid);
        dead(&descendant);
        assert!(!request.scratch_dir.exists());
    }

    fn real_client(tree: &Tree) -> ExtractClient {
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/winsearch-extract.exe");
        assert!(
            worker.is_file(),
            "build winsearch-extract before integration tests"
        );
        ExtractClient::new(worker, tree.0.join("cache"))
    }

    #[test]
    fn real_extract_binary_returns_actual_text_lines_and_explicit_unsupported() {
        let tree = Tree::new();
        let path = tree.0.join("actual.txt");
        std::fs::write(&path, "actual generated fixture text\nsecond line").unwrap();
        let client = real_client(&tree);
        let request = client.request(path.clone(), "doc", 256);
        let response = client.extract(&request, Duration::from_secs(10)).unwrap();
        assert_eq!(
            response.output.segments[0].text,
            "actual generated fixture text\nsecond line"
        );
        assert_eq!(response.output.segments[0].line, Some(1));
        assert!(!request.scratch_dir.exists());
        let request = client.request(path, "audio", 256);
        assert!(matches!(
            client.extract(&request, Duration::from_secs(5)),
            Err(ExtractClientError::Unsupported(_))
        ));
        let mut continuation = client.request(tree.0.join("actual.txt"), "doc", 256);
        continuation.cursor = 1;
        assert!(matches!(
            client.extract(&continuation, Duration::from_secs(5)),
            Err(ExtractClientError::Unsupported(_))
        ));
        std::fs::write(
            tree.0.join("actual.txt"),
            "generated chunk words ".repeat(200),
        )
        .unwrap();
        let capped = client.request(tree.0.join("actual.txt"), "doc", 1);
        let response = client.extract(&capped, Duration::from_secs(5)).unwrap();
        assert_eq!(response.output.segments.len(), 1);
        assert_eq!(
            response.total_units, None,
            "legacy truncation cannot establish a total"
        );
    }

    #[test]
    fn real_extract_image_promotes_thumbnail_and_cleans_private_directory() {
        let tree = Tree::new();
        let path = tree.0.join("fixture.png");
        ws_platform::wic::write_bgra_thumbnail_png(&vec![120; 32 * 32 * 4], 32, 32, 32, &path)
            .unwrap();
        let client = real_client(&tree);
        let request = client.request(path, "image", 256);
        let response = client.extract(&request, Duration::from_secs(20)).unwrap();
        let thumb = response
            .output
            .segments
            .iter()
            .find_map(|s| s.thumbnail_hint.as_ref())
            .unwrap();
        assert!(thumb.is_file());
        assert!(thumb.starts_with(tree.0.join("cache/thumbs")));
        assert!(!request.scratch_dir.exists());
    }

    #[test]
    fn missing_worker_fails_without_in_process_fallback() {
        let tree = Tree::new();
        let client = ExtractClient::new(tree.0.join("missing.exe"), tree.0.join("cache"));
        let request = client.request(tree.0.join("fixture.txt"), "doc", 256);
        assert!(matches!(
            client.extract(&request, Duration::from_secs(1)),
            Err(ExtractClientError::Worker(_))
        ));
        assert!(!request.scratch_dir.exists());
    }
}
