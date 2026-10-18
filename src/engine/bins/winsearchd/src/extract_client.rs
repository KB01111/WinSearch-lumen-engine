//! A fresh, job-owned extraction child per batch; no in-process fallback.
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FileIdInfo, GetFileInformationByHandleEx, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows::Win32::System::Threading::WaitForSingleObject;
use windows::Win32::System::IO::CancelSynchronousIo;
use ws_extract::worker::{
    encode_request, validate_response, ExtractRequest, ExtractResponse, WorkerHello,
};
use ws_extract::{ExtractOutput, ExtractionStatus};
use ws_platform::child_job::ChildJob;

#[derive(Debug, thiserror::Error)]
pub enum ExtractClientError {
    #[error("extraction admission unavailable while parser slots or cleanup are busy")]
    AdmissionUnavailable,
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

const MAX_EXTRACTION_JOBS: usize = 2;

struct ExtractionSlots {
    active: Mutex<usize>,
    ready: Condvar,
}

fn extraction_slots() -> &'static ExtractionSlots {
    static SLOTS: OnceLock<ExtractionSlots> = OnceLock::new();
    SLOTS.get_or_init(|| ExtractionSlots { active: Mutex::new(0), ready: Condvar::new() })
}

struct ExtractionPermit;
impl ExtractionPermit {
    fn try_acquire() -> Option<Self> {
        let mut active = extraction_slots().active.lock().unwrap_or_else(|p| p.into_inner());
        if *active >= MAX_EXTRACTION_JOBS { return None; }
        *active += 1;
        Some(Self)
    }
}
impl Drop for ExtractionPermit {
    fn drop(&mut self) {
        let mut active = extraction_slots().active.lock().unwrap_or_else(|p| p.into_inner());
        *active -= 1;
        extraction_slots().ready.notify_all();
    }
}

pub struct ExtractClient {
    worker_bin: PathBuf,
    cache_dir: PathBuf,
    cleanup_pending: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(crate) worker_args: Vec<String>,
    #[cfg(test)]
    pub(crate) worker_env: Vec<(String, PathBuf)>,
}

impl ExtractClient {
    pub fn admission_available(&self) -> bool {
        self.cleanup_pending.load(Ordering::Acquire) == 0
            && *extraction_slots().active.lock().unwrap_or_else(|p| p.into_inner()) < MAX_EXTRACTION_JOBS
    }

    pub(crate) fn wait_for_admission(&self, timeout: Duration) {
        let slots = extraction_slots();
        let active = slots.active.lock().unwrap_or_else(|p| p.into_inner());
        let _guard = slots.ready.wait_timeout_while(active, timeout, |active| {
            *active >= MAX_EXTRACTION_JOBS || self.cleanup_pending.load(Ordering::Acquire) != 0
        }).unwrap_or_else(|p| p.into_inner());
    }

    pub fn new(worker_bin: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            worker_bin,
            cache_dir,
            cleanup_pending: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            worker_args: Vec::new(),
            #[cfg(test)]
            worker_env: Vec::new(),
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
            media_plan: None,
            video_selection: None,
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
        if !self.admission_available() {
            return Err(ExtractClientError::AdmissionUnavailable);
        }
        let permit = Arc::new(ExtractionPermit::try_acquire().ok_or(ExtractClientError::AdmissionUnavailable)?);
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
            let anchor = ScratchAnchor::capture(&request.scratch_dir)?;
            let mut command = Command::new(&self.worker_bin);
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            #[cfg(test)]
            command.args(&self.worker_args);
            #[cfg(test)]
            command.envs(self.worker_env.iter().map(|(name, value)| (name, value)));
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
                job: Some(job),
                writer: Some(writer),
                reader: Some(reader),
                closed: false,
                _permit: Some(Arc::clone(&permit)),
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
                            .as_mut()
                            .expect("owned extraction job")
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
            if let Err(error) = exchange.close() {
                if !exchange.closed {
                    self.retain_cleanup(exchange.take_owned(), Scratch(scratch.0.take()));
                }
                return Err(error);
            }
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
            self.promote(request, &anchor, &mut response, &mut promoted)?;
            if !allowed() {
                return Err(ExtractClientError::Cancelled);
            }
            Ok(response)
        })();
        if scratch.clean().is_err() {
            self.retain_cleanup(Exchange {
                job: None, writer: None, reader: None, closed: true,
                _permit: Some(Arc::clone(&permit)),
            }, Scratch(scratch.0.take()));
            return Err(ExtractClientError::AdmissionUnavailable);
        }
        let response = result?;
        promoted.keep();
        Ok(response)
    }

    fn retain_cleanup(&self, exchange: Exchange, scratch: Scratch) {
        self.cleanup_pending.fetch_add(1, Ordering::AcqRel);
        retain_cleanup(RetainedCleanup {
            exchange,
            scratch,
            pending: Some(Arc::clone(&self.cleanup_pending)),
        });
    }

    fn promote(
        &self,
        request: &ExtractRequest,
        anchor: &ScratchAnchor,
        response: &mut ExtractResponse,
        promoted: &mut ArtifactLease,
    ) -> Result<(), ExtractClientError> {
        anchor.validate(&request.scratch_dir)?;
        let destination = self.cache_dir.join("thumbs");
        std::fs::create_dir_all(&destination).map_err(ExtractClientError::worker)?;
        if !ws_platform::content::local_content_allowed(&destination) {
            return Err(ExtractClientError::Malformed("unsafe cache path".into()));
        }
        let mut moved: std::collections::HashMap<PathBuf, PathBuf> =
            std::collections::HashMap::new();
        let mut promote_one = |source: &Path, extension: &str, cap: u64| -> Result<PathBuf, ExtractClientError> {
            if !ws_platform::content::local_content_allowed(source) {
                return Err(ExtractClientError::Malformed(
                    "unsafe thumbnail source".into(),
                ));
            }
            let canonical = source
                .canonicalize()
                .map_err(|e| ExtractClientError::Malformed(e.to_string()))?;
            if !canonical.starts_with(&anchor.canonical) || !canonical.is_file() {
                return Err(ExtractClientError::Malformed(
                    "thumbnail outside scratch".into(),
                ));
            }
            if let Some(path) = moved.get(&canonical) {
                return Ok(path.clone());
            }
            let name = format!(
                "extract-{}-{}.{extension}",
                request
                    .scratch_dir
                    .file_name()
                    .expect("private scratch name")
                    .to_string_lossy(),
                moved.len()
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
            let bytes = std::io::copy(&mut source_file.take(cap + 1), &mut target)
                .map_err(ExtractClientError::worker)?;
            if bytes > cap {
                return Err(ExtractClientError::Malformed("derived artifact too large".into()));
            }
            moved.insert(canonical, path.clone());
            Ok(path)
        };
        for segment in &mut response.output.segments {
            if let Some(source) = segment.thumbnail_hint.as_ref() {
                segment.thumbnail_hint = Some(promote_one(source, "png", 16 * 1024 * 1024)?);
            }
            match &mut segment.embedding_input {
                ws_extract::DraftEmbeddingInput::Text => (),
                ws_extract::DraftEmbeddingInput::Image { path, .. } => {
                    *path = promote_one(path, "png", 16 * 1024 * 1024)?;
                }
                ws_extract::DraftEmbeddingInput::Audio { path } => {
                    *path = promote_one(path, "wav", ws_extract::decoder::MAX_PCM_BYTES as u64 + 44)?;
                    let samples = ws_extract::audio::validate_window_wav(path)
                        .map_err(|e| ExtractClientError::Malformed(e.to_string()))?;
                    let span = segment.end_ms.unwrap() - segment.start_ms.unwrap();
                    if (samples * 1000).div_ceil(16000).abs_diff(span) > 1 {
                        return Err(ExtractClientError::Malformed("audio timestamp/WAV duration mismatch".into()));
                    }
                }
            }
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
                .flat_map(|s| {
                    let input = match &s.embedding_input {
                        ws_extract::DraftEmbeddingInput::Text => None,
                        ws_extract::DraftEmbeddingInput::Image { path, .. }
                        | ws_extract::DraftEmbeddingInput::Audio { path } => Some(path.clone()),
                    };
                    s.thumbnail_hint.clone().into_iter().chain(input)
                })
                .collect(),
        }
    }
    pub fn keep(&mut self) {
        self.paths.clear();
    }
    pub fn keep_input(&mut self,input:&ws_extract::DraftEmbeddingInput) {
        if let ws_extract::DraftEmbeddingInput::Image { path,.. } | ws_extract::DraftEmbeddingInput::Audio { path }=input {
            self.paths.retain(|p|p!=path);
        }
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
struct ScratchAnchor {
    canonical: PathBuf,
    identity: (u64, [u8; 16]),
}
impl ScratchAnchor {
    fn capture(path: &Path) -> Result<Self, ExtractClientError> {
        if !ws_platform::content::local_content_allowed(path) {
            return Err(ExtractClientError::Malformed("unsafe scratch root".into()));
        }
        Ok(Self {
            canonical: path.canonicalize().map_err(ExtractClientError::worker)?,
            identity: directory_identity(path).map_err(ExtractClientError::worker)?,
        })
    }

    fn validate(&self, path: &Path) -> Result<(), ExtractClientError> {
        let current = Self::capture(path)?;
        if current.canonical != self.canonical || current.identity != self.identity {
            return Err(ExtractClientError::Malformed(
                "scratch root was replaced".into(),
            ));
        }
        Ok(())
    }
}

fn directory_identity(path: &Path) -> std::io::Result<(u64, [u8; 16])> {
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    unsafe {
        let raw = CreateFileW(
            windows::core::PCWSTR(wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
        .map_err(std::io::Error::other)?;
        let handle = OwnedHandle::from_raw_handle(raw.0);
        let mut info = FILE_ID_INFO::default();
        GetFileInformationByHandleEx(
            HANDLE(handle.as_raw_handle()),
            FileIdInfo,
            &mut info as *mut _ as _,
            std::mem::size_of_val(&info) as u32,
        )
        .map_err(std::io::Error::other)?;
        Ok((info.VolumeSerialNumber, info.FileId.Identifier))
    }
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
    job: Option<ChildJob>,
    writer: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    closed: bool,
    // The outer request and exceptional cleanup share one admission lifetime.
    _permit: Option<Arc<ExtractionPermit>>,
}
impl Exchange {
    fn close(&mut self) -> Result<(), ExtractClientError> {
        if self.closed {
            return Ok(());
        }
        let child_result = self
            .job
            .as_mut()
            .map(|job| job.terminate_and_reap())
            .unwrap_or(Ok(()))
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
        let mut panicked = false;
        for task in [&mut self.writer, &mut self.reader] {
            if let Some(handle) = task.as_ref() {
                if stopped(handle) {
                    panicked |= task.take().expect("verified pipe task").join().is_err();
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
        self.closed = true;
        if panicked {
            return Err(ExtractClientError::Worker("pipe task panicked".into()));
        }
        Ok(())
    }

    fn take_owned(&mut self) -> Self {
        Self {
            job: self.job.take(),
            writer: self.writer.take(),
            reader: self.reader.take(),
            closed: false,
            _permit: self._permit.take(),
        }
    }
}
impl Drop for Exchange {
    fn drop(&mut self) {
        if self.close().is_err() && !self.closed {
            retain_cleanup(RetainedCleanup {
                exchange: self.take_owned(),
                scratch: Scratch(None),
                pending: None,
            });
        }
    }
}

struct RetainedCleanup {
    exchange: Exchange,
    scratch: Scratch,
    pending: Option<Arc<AtomicUsize>>,
}
struct CleanupQueue {
    items: Mutex<Vec<RetainedCleanup>>,
    ready: Condvar,
}
struct CleanupReaper {
    queue: Arc<CleanupQueue>,
    _thread: JoinHandle<()>,
}

fn retain_cleanup(cleanup: RetainedCleanup) {
    // One permanent owner retains both queued work and the reaper's native
    // thread handle. A client's pending count prevents repeated failed jobs
    // from accumulating while its original pipe tasks are still alive.
    static REAPER: OnceLock<CleanupReaper> = OnceLock::new();
    let reaper = REAPER.get_or_init(|| {
        let queue = Arc::new(CleanupQueue {
            items: Mutex::new(Vec::new()),
            ready: Condvar::new(),
        });
        let owned_queue = Arc::clone(&queue);
        let thread = thread::spawn(move || loop {
            let mut batch = {
                let mut items = owned_queue.items.lock().unwrap_or_else(|p| p.into_inner());
                while items.is_empty() {
                    items = owned_queue
                        .ready
                        .wait(items)
                        .unwrap_or_else(|p| p.into_inner());
                }
                std::mem::take(&mut *items)
            };
            let mut pending = Vec::new();
            for mut cleanup in batch.drain(..) {
                if cleanup.exchange.close().is_ok() && cleanup.scratch.clean().is_ok() {
                    let counter = cleanup.pending.take();
                    drop(cleanup);
                    if let Some(counter) = counter {
                        let _active = extraction_slots().active.lock().unwrap_or_else(|p| p.into_inner());
                        counter.fetch_sub(1, Ordering::AcqRel);
                        extraction_slots().ready.notify_all();
                    }
                } else {
                    pending.push(cleanup);
                }
            }
            if !pending.is_empty() {
                owned_queue
                    .items
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend(pending);
                thread::sleep(Duration::from_millis(25));
            }
        });
        CleanupReaper {
            queue,
            _thread: thread,
        }
    });
    reaper
        .queue
        .items
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(cleanup);
    reaper.queue.ready.notify_one();
}

#[cfg(test)]
pub(crate) mod tests {
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

    pub(crate) struct CleanupFixture {
        release: Option<mpsc::Sender<()>>,
        pending: Arc<AtomicUsize>,
    }

    #[test]
    fn media_artifact_is_promoted_and_owned_until_durable_commit() {
        let tree = Tree::new();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        let request = client.request(tree.0.join("source.wav"), "audio", 30);
        let original = request.scratch_dir.join("window.wav");
        client.worker_args = vec!["audio-cache".into(), tree.0.join("worker.pid").to_string_lossy().into(), original.to_string_lossy().into()];
        let response = client.extract(&request, Duration::from_secs(5)).unwrap();
        let ws_extract::DraftEmbeddingInput::Audio { path } = &response.output.segments[0].embedding_input else { panic!("typed audio missing"); };
        assert!(path.is_file(), "audio artifact was lost when private scratch was cleaned");
        assert!(!path.starts_with(&request.scratch_dir));
        assert!(path.starts_with(&client.cache_dir));
        assert_eq!(&std::fs::read(path).unwrap()[..4], b"RIFF");
        drop(ArtifactLease::for_output(&response.output));
        assert!(!path.exists(), "uncommitted derived audio lease must clean up");
    }

    #[test]
    fn code_native_child_parses_source_under_shared_owned_transport() {
        let tree=Tree::new();
        let source=tree.0.join("example.rs");
        std::fs::write(&source,"pub fn first() { helper(); }\n").unwrap();
        let worker=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/winsearch-extract.exe");
        let client=ExtractClient::new(worker,tree.0.join("cache"));
        let request=client.request(source,"code",64);
        let response=client.extract(&request,Duration::from_secs(10)).unwrap();
        assert!(matches!(response.output.status,ws_extract::ExtractionStatus::Extracted),
            "real child did not parse code: {:?}",response.output.status);
        let batch=response.code.expect("typed code batch");
        assert!(batch.units.iter().any(|unit|matches!(unit,ws_extract::code::CodeUnit::Symbol { symbol,.. }
            if symbol.name=="first" && symbol.role.is_definition() && symbol.line==1)));
        assert!(batch.units.iter().any(|unit|matches!(unit,ws_extract::code::CodeUnit::Chunk { chunk,.. }
            if chunk.text=="pub fn first() { helper(); }\n")));
    }

    #[test]
    fn code_native_child_continuations_preserve_the_complete_source_and_symbol_tail() {
        use ws_extract::code::CodeUnit;
        let tree=Tree::new();
        let source=tree.0.join("many.rs");
        let text=(0..300).map(|n|format!("pub fn f{n}() {{ helper(); }}\n")).collect::<String>();
        std::fs::write(&source,&text).unwrap();
        let worker=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/winsearch-extract.exe");
        let client=ExtractClient::new(worker,tree.0.join("cache"));
        let mut request=client.request(source,"code",64);
        let mut reconstructed=String::new();
        let (mut definitions,mut references,mut batches,mut consumed)=(0,0,0,0);
        let mut expected_total=None;
        loop {
            let response=client.extract(&request,Duration::from_secs(10)).unwrap();
            assert!(response.diagnostics.is_empty());
            if let Some(total)=expected_total { assert_eq!(response.total_units,Some(total)); }
            else { expected_total=response.total_units; }
            let batch=response.code.unwrap();
            assert!(!batch.units.is_empty() && batch.units.len()<=64);
            for unit in batch.units {
                assert_eq!(unit.ordinal(),consumed);
                consumed+=1;
                match unit {
                    CodeUnit::Chunk { chunk,.. }=>reconstructed.push_str(&chunk.text),
                    CodeUnit::Symbol { symbol,.. } if symbol.role.is_definition()=>definitions+=1,
                    CodeUnit::Symbol { symbol,.. } if symbol.name=="helper"=>references+=1,
                    _=>(),
                }
            }
            batches+=1;
            match response.next_cursor {
                Some(next)=> { assert_eq!(next,consumed); request.cursor=next; }
                None=>break,
            }
            assert!(batches<30,"continuation failed to make bounded progress");
        }
        assert!(batches>1);
        assert_eq!(Some(consumed),expected_total);
        assert_eq!(reconstructed,text);
        assert_eq!((definitions,references),(300,300));
    }

    #[test]
    fn code_native_child_keeps_notebook_coordinates_and_excludes_outputs() {
        use ws_extract::code::CodeUnit;
        let tree=Tree::new();
        let source=tree.0.join("example.ipynb");
        let notebook=serde_json::json!({"cells":[
            {"cell_type":"markdown","source":"Introduction\n"},
            {"cell_type":"code","source":"%%python\ndef greet():\n    helper()\n",
             "outputs":[{"text":"OUTPUT_MUST_NOT_BE_INDEXED"}],"attachments":{"fake":"ATTACHMENT_MUST_NOT_BE_INDEXED"}}
        ]});
        std::fs::write(&source,serde_json::to_vec(&notebook).unwrap()).unwrap();
        let worker=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/winsearch-extract.exe");
        let client=ExtractClient::new(worker,tree.0.join("cache"));
        let response=client.extract(&client.request(source,"code",64),Duration::from_secs(10)).unwrap();
        assert!(response.diagnostics.is_empty());
        let batch=response.code.unwrap();
        let serialized=serde_json::to_string(&batch).unwrap();
        assert!(!serialized.contains("MUST_NOT_BE_INDEXED"));
        let symbol=batch.units.iter().find_map(|unit|match unit {
            CodeUnit::Symbol { symbol,.. } if symbol.name=="greet" && symbol.role.is_definition()=>Some(symbol),
            _=>None,
        }).unwrap();
        assert_eq!((symbol.line,symbol.end_line),(3,4));
        let cell=symbol.notebook.unwrap();
        assert_eq!((cell.cell_ordinal,cell.start_line,cell.end_line),(1,2,3));
    }

    #[test]
    fn code_native_child_denies_sensitive_and_oversized_sources() {
        let tree=Tree::new();
        let sensitive=tree.0.join(".env");
        std::fs::write(&sensitive,"SYNTHETIC_PRIVATE_FIXTURE=not-to-be-read").unwrap();
        let oversized=tree.0.join("oversized.rs");
        std::fs::File::create(&oversized).unwrap().set_len(ws_code::MAX_CODE_FILE_BYTES+1).unwrap();
        let worker=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/winsearch-extract.exe");
        let client=ExtractClient::new(worker,tree.0.join("cache"));
        for (path,expected) in [(sensitive,"code source policy denied"),(oversized,"source exceeds 2 MiB")] {
            match client.extract(&client.request(path,"code",64),Duration::from_secs(10)) {
                Err(ExtractClientError::Worker(message))=>assert!(message.contains(expected),"{message}"),
                other=>panic!("unsafe code source was not explicitly rejected: {other:?}"),
            }
        }
        for relative in [".git/config", ".hg/hgrc", ".svn/auth/private.py", ".env/private.py", ".environment/private.py"] {
            let path=tree.0.join("repo").join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path,"SYNTHETIC_PRIVATE_FIXTURE=not-to-be-read").unwrap();
            match client.extract(&client.request(path,"code",64),Duration::from_secs(10)) {
                Err(ExtractClientError::Worker(message))=>assert!(message.contains("code source policy denied"),"{message}"),
                other=>panic!("sensitive ancestor {relative} bypassed child source policy: {other:?}"),
            }
        }
    }

    #[test]
    fn media_artifact_outside_private_scratch_is_rejected() {
        let tree = Tree::new();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        client.worker_args = vec!["audio-outside".into(), tree.0.join("worker.pid").to_string_lossy().into(), tree.0.join("outside.wav").to_string_lossy().into()];
        let request = client.request(tree.0.join("source.wav"), "audio", 30);
        assert!(matches!(client.extract(&request, Duration::from_secs(5)), Err(ExtractClientError::Malformed(_))));
        assert!(tree.0.join("outside.wav").is_file(), "external data must be preserved");
    }
    impl Drop for CleanupFixture {
        fn drop(&mut self) {
            self.release.take();
            let deadline = Instant::now() + Duration::from_secs(3);
            while self.pending.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    pub(crate) fn cleanup_trigger(
        client: &ExtractClient,
        root: PathBuf,
    ) -> impl Fn() -> CleanupFixture + Send + Sync + 'static {
        let pending = Arc::clone(&client.cleanup_pending);
        let binary = fixture();
        move || {
            let permit = Arc::new(ExtractionPermit::try_acquire().expect("fixture parser admission"));
            let mut command = Command::new(&binary);
            command
                .arg("never-read")
                .arg(root.join("cleanup-fixture.pid"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped());
            let mut job = ChildJob::spawn(&mut command).unwrap();
            let stdin = job.child_mut().stdin.take().unwrap();
            let stdout = job.child_mut().stdout.take().unwrap();
            let (release, released) = mpsc::channel();
            let task = thread::spawn(move || {
                let _ = released.recv();
                drop((stdin, stdout));
            });
            let mut exchange = Exchange {
                job: Some(job),
                writer: Some(task),
                reader: None,
                closed: false,
                _permit: Some(permit),
            };
            assert!(exchange.close().is_err());
            let scratch = root.join("retained-cleanup-scratch");
            std::fs::create_dir(&scratch).unwrap();
            pending.fetch_add(1, Ordering::AcqRel);
            retain_cleanup(RetainedCleanup {
                exchange: exchange.take_owned(),
                scratch: Scratch(Some(scratch)),
                pending: Some(Arc::clone(&pending)),
            });
            CleanupFixture {
                release: Some(release),
                pending: Arc::clone(&pending),
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
    fn native_extract_rejects_replaced_scratch_root_and_preserves_external_file() {
        let tree = Tree::new();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        let external_dir = tree.0.join("external");
        std::fs::create_dir(&external_dir).unwrap();
        let external_file = external_dir.join("artifact.png");
        std::fs::write(&external_file, "generated external artifact").unwrap();
        let pid = tree.0.join("pid");
        let request = client.request(tree.0.join("fixture.txt"), "doc", 256);
        client.worker_args = vec![
            "replace-root".into(),
            pid.to_string_lossy().into_owned(),
            external_file.to_string_lossy().into_owned(),
            request.scratch_dir.to_string_lossy().into_owned(),
        ];
        let result = client.extract(&request, Duration::from_secs(5));
        dead(&pid);
        assert_eq!(
            std::fs::read_to_string(&external_file).unwrap(),
            "generated external artifact"
        );
        assert!(!request.scratch_dir.exists());
        assert!(
            matches!(result, Err(ExtractClientError::Malformed(_))),
            "replacement accepted: {result:?}"
        );
    }

    #[test]
    fn native_cleanup_failure_retains_live_thread_and_pipe_ownership() {
        let tree = Tree::new();
        let permit = Arc::new(ExtractionPermit::try_acquire().expect("fixture parser admission"));
        let mut command = Command::new(fixture());
        command
            .arg("never-read")
            .arg(tree.0.join("pid"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let mut job = ChildJob::spawn(&mut command).unwrap();
        let stdin = job.child_mut().stdin.take().unwrap();
        let stdout = job.child_mut().stdout.take().unwrap();
        let (release, released) = mpsc::channel();
        let (done, completed) = mpsc::channel();
        let task = thread::spawn(move || {
            let _ = released.recv();
            drop((stdin, stdout));
            let _ = done.send(());
        });
        let mut exchange = Exchange {
            job: Some(job),
            writer: Some(task),
            reader: None,
            closed: false,
            _permit: Some(permit),
        };
        let started = Instant::now();
        let result = exchange.close();
        let retained = !exchange.closed && exchange.writer.is_some();
        let client = ExtractClient::new(tree.0.join("missing.exe"), tree.0.join("cache"));
        let pending = Arc::clone(&client.cleanup_pending);
        let retained_scratch = tree.0.join("retained-scratch");
        std::fs::create_dir(&retained_scratch).unwrap();
        client.retain_cleanup(
            exchange.take_owned(),
            Scratch(Some(retained_scratch.clone())),
        );
        assert_eq!(pending.load(Ordering::Acquire), 1);
        let denied = client.request(tree.0.join("fixture.txt"), "doc", 256);
        let denied_result = client.extract_while(&denied, Duration::from_secs(1), || true);
        assert!(matches!(
            denied_result,
            Err(ExtractClientError::AdmissionUnavailable)
        ));
        assert!(
            !denied.scratch_dir.exists(),
            "pending cleanup must block another child before setup"
        );
        drop(client);
        release.send(()).unwrap();
        completed.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(result, Err(ExtractClientError::Worker(_))));
        assert!(
            retained,
            "cleanup failure detached the task owning both pipe endpoints"
        );
        let finish_deadline = Instant::now() + Duration::from_secs(3);
        while pending.load(Ordering::Acquire) != 0 && Instant::now() < finish_deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            pending.load(Ordering::Acquire),
            0,
            "retained cleanup did not finish after task release"
        );
        assert!(!retained_scratch.exists());
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

    struct RunningExtractions {
        allowed: Arc<std::sync::atomic::AtomicBool>,
        tasks: Vec<JoinHandle<Result<ExtractResponse, ExtractClientError>>>,
    }
    impl RunningExtractions {
        fn stop(&mut self) -> Vec<Result<ExtractResponse, ExtractClientError>> {
            self.allowed.store(false, Ordering::Release);
            self.tasks.drain(..).map(|task| task.join().unwrap()).collect()
        }
    }
    impl Drop for RunningExtractions {
        fn drop(&mut self) { let _ = self.stop(); }
    }

    #[test]
    fn process_wide_parser_slots_block_fresh_clients_until_owned_children_finish() {
        let tree = Tree::new();
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut running = RunningExtractions { allowed: allowed.clone(), tasks: vec![] };
        let mut pids = Vec::new();
        for number in 0..2 {
            let pid = tree.0.join(format!("pid-{number}"));
            pids.push(pid.clone());
            let mut client = ExtractClient::new(fixture(), tree.0.join(format!("cache-{number}")));
            client.worker_args = vec!["never-read".into(), pid.to_string_lossy().into()];
            let request = client.request(tree.0.join("fixture.txt"), "doc", 1);
            let allowed = allowed.clone();
            running.tasks.push(thread::spawn(move || client.extract_while(&request,
                Duration::from_secs(10), || allowed.load(Ordering::Acquire))));
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while pids.iter().any(|pid| !pid.exists()) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert!(pids.iter().all(|pid| pid.exists()), "both owned parsers must actually start");
        let mut third = ExtractClient::new(fixture(), tree.0.join("third-cache"));
        let third_pid = tree.0.join("third.pid");
        third.worker_args = vec!["oversized".into(), third_pid.to_string_lossy().into()];
        let denied = third.request(tree.0.join("fixture.txt"), "doc", 1);
        let result = third.extract(&denied, Duration::from_secs(1));
        assert!(matches!(result, Err(ExtractClientError::AdmissionUnavailable)), "{result:?}");
        assert!(!denied.scratch_dir.exists() && !third_pid.exists(),
            "capacity denial must precede scratch creation and process startup");
        let started = Instant::now();
        for result in running.stop() {
            assert!(matches!(result, Err(ExtractClientError::Cancelled)), "{result:?}");
        }
        assert!(started.elapsed() < Duration::from_millis(500));
        for pid in pids { dead(&pid); }
        let admitted = third.request(tree.0.join("fixture.txt"), "doc", 1);
        assert!(matches!(third.extract(&admitted, Duration::from_secs(2)),
            Err(ExtractClientError::Malformed(_))), "a freed slot must admit another owned parser");
        dead(&third_pid);
        assert!(!admitted.scratch_dir.exists());
    }

    #[test]
    fn retained_parser_slots_cannot_be_bypassed_with_a_new_client() {
        let first_tree = Tree::new();
        let second_tree = Tree::new();
        let third_tree = Tree::new();
        let first = ExtractClient::new(fixture(), first_tree.0.join("cache"));
        let second = ExtractClient::new(fixture(), second_tree.0.join("cache"));
        let first_cleanup = cleanup_trigger(&first, first_tree.0.clone())();
        let second_cleanup = cleanup_trigger(&second, second_tree.0.clone())();
        let fresh = ExtractClient::new(third_tree.0.join("missing.exe"), third_tree.0.join("cache"));
        let request = fresh.request(third_tree.0.join("source.txt"), "doc", 1);
        let result = fresh.extract(&request, Duration::from_secs(1));
        assert!(matches!(result, Err(ExtractClientError::AdmissionUnavailable)), "{result:?}");
        assert!(!request.scratch_dir.exists(), "retained slots must block new setup");
        drop((first_cleanup, second_cleanup));
        assert!(fresh.admission_available(), "finished cleanup must release shared admission");
    }

    #[test]
    fn scratch_cleanup_failure_retains_parser_capacity_until_the_owned_cache_is_clean() {
        use std::os::windows::fs::OpenOptionsExt;
        let tree = Tree::new();
        let parked = ExtractClient::new(fixture(), tree.0.join("parked-cache"));
        let parked_cleanup = cleanup_trigger(&parked, tree.0.clone())();
        let mut client = ExtractClient::new(fixture(), tree.0.join("cache"));
        let pid = tree.0.join("parser.pid");
        let release = tree.0.join("release");
        client.worker_args = vec!["code-wait".into(), pid.to_string_lossy().into(), release.to_string_lossy().into()];
        let client = Arc::new(client);
        let request = client.request(tree.0.join("source.rs"), "code", 1);
        let scratch = request.scratch_dir.clone();
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut running = RunningExtractions { allowed: allowed.clone(), tasks: vec![] };
        let active = client.clone();
        running.tasks.push(thread::spawn(move || active.extract_while(&request,
            Duration::from_secs(10), || allowed.load(Ordering::Acquire))));
        let deadline = Instant::now() + Duration::from_secs(3);
        while !pid.exists() && Instant::now() < deadline { thread::sleep(Duration::from_millis(2)); }
        assert!(pid.exists(), "worker must actually start before locking its scratch");
        let locked = std::fs::OpenOptions::new().write(true).create_new(true).share_mode(0)
            .open(scratch.join("held.tmp")).unwrap();
        std::fs::write(&release, b"release").unwrap();
        let result = running.tasks.pop().unwrap().join().unwrap();
        assert!(matches!(result, Err(ExtractClientError::AdmissionUnavailable)), "{result:?}");
        dead(&pid);
        assert!(scratch.exists(), "the fixture must actually prevent scratch deletion");
        let fresh = ExtractClient::new(tree.0.join("missing.exe"), tree.0.join("fresh-cache"));
        let request = fresh.request(tree.0.join("source.rs"), "code", 1);
        let result = fresh.extract(&request, Duration::from_secs(1));
        assert!(matches!(result, Err(ExtractClientError::AdmissionUnavailable)), "{result:?}");
        assert!(!request.scratch_dir.exists(), "failed cleanup cannot be bypassed");
        drop(locked);
        drop(parked_cleanup);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !client.admission_available() && Instant::now() < deadline {
            client.wait_for_admission(Duration::from_millis(20));
        }
        assert!(client.admission_available() && fresh.admission_available());
        assert!(!scratch.exists(), "retained scratch must actually be cleaned");
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
        let request = client.request(path, "archive", 256);
        assert!(matches!(
            client.extract(&request, Duration::from_secs(5)),
            Err(ExtractClientError::Unsupported(_))
        ));
        let mut continuation = client.request(tree.0.join("actual.txt"), "doc", 256);
        continuation.cursor = 1;
        let completed = client
            .extract(&continuation, Duration::from_secs(5))
            .unwrap();
        assert!(completed.output.segments.is_empty());
        assert_eq!(completed.next_cursor, None);
        assert_eq!(completed.total_units, Some(1));
        std::fs::write(
            tree.0.join("actual.txt"),
            "generated chunk words ".repeat(200),
        )
        .unwrap();
        let capped = client.request(tree.0.join("actual.txt"), "doc", 1);
        let response = client.extract(&capped, Duration::from_secs(5)).unwrap();
        assert_eq!(response.output.segments.len(), 1);
        assert!(response.total_units.is_some_and(|n| n > 1));
        assert_eq!(response.next_cursor, Some(1));
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
