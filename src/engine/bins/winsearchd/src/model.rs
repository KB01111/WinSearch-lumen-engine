//! Embedding-model lifecycle for `winsearchd`: download/import run on a
//! single background thread (the engine is `std::thread`-only), progress is
//! shared through a mutex snapshot, and completion pushes `model_status` to
//! all IPC clients through a notifier installed by `main.rs`.
//!
//! The on-disk truth lives in [`ws_modelmgr::ModelManager`]; this service
//! adds the in-flight dimension (downloading/paused/error) plus the DB
//! compat record (`meta` key `embedding_model`). A stored record whose model
//! id, pinned commit, or embedding width differs from the manifest invalidates every stored
//! vector — [`ModelService::check_compat`] reports that, and construction
//! purges model vectors and requeues name/content jobs per entry so the
//! new model re-embeds everything (P3.5 pipeline consumes the queue).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};
use ws_modelmgr::{
    DownloadError, ManagerError, ModelCompat, ModelManager, ModelState, ModelStatus,
    EMBEDDING_MODEL_META_KEY,
};
use ws_protocol::ServerMessage;

use crate::indexer::Indexer;

/// Pushes a server message to all connected clients (wired to
/// `EngineService::push_to_all` in `main.rs`; unset in unit tests).
pub type PushFn = Arc<dyn Fn(ServerMessage) + Send + Sync>;

/// Minimum interval between progress pushes (the counter itself updates per
/// chunk; only the broadcast is throttled).
const PUSH_THROTTLE: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Download,
    Import,
}

struct ActiveOp {
    id: u64,
    kind: OpKind,
    cancel: Arc<AtomicBool>,
    include_npu: bool,
}

#[derive(Default)]
struct Notifications {
    pending: VecDeque<ModelStatus>,
    delivering: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    Cancelled,
    Failed(String),
}

/// Typed worker result: cancellation is matched on the error variant, never
/// on message text, so an unrelated failure that merely mentions "cancelled"
/// cannot be misclassified as a clean cancel.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpError {
    Cancelled,
    Failed(String),
}

impl OpError {
    fn from_download(e: DownloadError) -> Self {
        match e {
            DownloadError::Cancelled(..) => Self::Cancelled,
            other => Self::Failed(other.to_string()),
        }
    }

    fn from_manager(e: ManagerError) -> Self {
        match e {
            ManagerError::Download(DownloadError::Cancelled(..)) => Self::Cancelled,
            other => Self::Failed(other.to_string()),
        }
    }
}

/// What the stored compat record says about the current manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatCheck {
    /// No record yet (fresh database): nothing to invalidate.
    NoRecord,
    /// Stored vectors (once they exist) are still usable.
    Compatible,
    /// Model id, pinned weights, or embedding width changed: re-embed vectors.
    Incompatible(ModelCompat),
}

pub struct ModelService {
    manager: ModelManager,
    indexer: Arc<Indexer>,
    active: Mutex<Option<ActiveOp>>,
    next_op_id: Mutex<u64>,
    /// Live byte counters while an op runs.
    progress: Mutex<ModelStatus>,
    /// Terminal state of the last op (cleared when the next one starts).
    outcome: Mutex<Option<Outcome>>,
    notifier: Mutex<Option<PushFn>>,
    notifications: Mutex<Notifications>,
}

impl ModelService {
    /// Track the optional bundle on every PC; only an explicit request installs it.
    pub fn new(model_dir: PathBuf, indexer: Arc<Indexer>) -> Arc<Self> {
        let manager = ModelManager::new(model_dir).with_npu_bundle();
        Self::with_manager(manager, indexer)
    }

    pub fn with_manager(manager: ModelManager, indexer: Arc<Indexer>) -> Arc<Self> {
        let this = Arc::new(Self {
            manager,
            indexer,
            active: Mutex::new(None),
            next_op_id: Mutex::new(1),
            progress: Mutex::new(ModelStatus::default()),
            outcome: Mutex::new(None),
            notifier: Mutex::new(None),
            notifications: Mutex::new(Notifications::default()),
        });
        // Changed model identity, weights, or width invalidates every stored
        // vector: purge and enqueue name/content jobs per entry so the
        // new model re-embeds everything. Fresh DBs (`NoRecord`) and
        // compatible installs are no-ops.
        match this.check_compat() {
            Ok(CompatCheck::Incompatible(stored)) => {
                warn!(
                    model_id = %stored.model_id,
                    embedding_dim = stored.embedding_dim,
                    "embedding model changed: purging model vectors and requeueing for re-embed"
                );
                match this.indexer.purge_model_vectors_and_requeue() {
                    Ok((purged, enqueued)) => {
                        let _ = this.record_compat();
                        info!(
                            purged,
                            enqueued,
                            "model swap: stale vectors purged, re-embed queued"
                        );
                    }
                    Err(e) => warn!(error = %e, "model-swap purge/requeue failed"),
                }
            }
            Err(e) => warn!(error = %e, "cannot read embedding compat record"),
            _ => {}
        }
        this
    }

    pub fn set_notifier(&self, push: PushFn) {
        *self.notifier.lock().unwrap_or_else(|p| p.into_inner()) = Some(push);
    }

    fn push(&self, status: &ModelStatus) {
        let push = self.notifier.lock().unwrap_or_else(|p| p.into_inner()).clone();
        if let Some(push) = push {
            push(ServerMessage::ModelStatus {
                id: None,
                status: status.clone(),
            });
        }
    }

    // Enqueue while holding `active` so messages follow lifecycle changes across operations.
    fn queue_notification(&self, status: ModelStatus) {
        self.notifications
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pending
            .push_back(status);
    }

    fn push_pending_notifications(&self) {
        let mut notifications = self.notifications.lock().unwrap_or_else(|p| p.into_inner());
        if notifications.delivering {
            return;
        }
        notifications.delivering = true;
        loop {
            let Some(status) = notifications.pending.pop_front() else {
                notifications.delivering = false;
                return;
            };
            // Callbacks may read state or enqueue more work; no mutex may span delivery.
            drop(notifications);
            self.indexer.notify_work();
            self.push(&status);
            notifications = self.notifications.lock().unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Current lifecycle snapshot: live progress while an op runs, the
    /// terminal error after a failure, otherwise the filesystem truth.
    pub fn status(&self) -> ModelStatus {
        if self.active.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
            return self.progress.lock().unwrap_or_else(|p| p.into_inner()).clone();
        }
        if let Some(Outcome::Failed(_)) = self.outcome.lock().unwrap_or_else(|p| p.into_inner()).clone() {
            let failed = self
                .progress
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let mut current = self.manager.status();
            if failed.state == ModelState::Error {
                current.state = ModelState::Error;
                current.error = failed.error;
            }
            if let (Some(failure), Some(component)) =
                (failed.npu_bundle, current.npu_bundle.as_mut())
            {
                if failure.state == ModelState::Error {
                    component.state = ModelState::Error;
                    component.error = failure.error;
                }
            }
            return current;
        }
        self.manager.status()
    }

    /// Starts (or resumes) the download. Idempotent while running: returns
    /// the live snapshot instead of spawning a second thread.
    pub fn start_download(self: &Arc<Self>) -> ModelStatus {
        self.start_download_with_npu(false)
    }

    pub fn start_download_with_npu(self: &Arc<Self>, include_npu: bool) -> ModelStatus {
        self.start_op(OpKind::Download, None, include_npu)
    }

    pub fn try_start_download_with_npu(
        self: &Arc<Self>,
        include_npu: bool,
    ) -> Result<ModelStatus, String> {
        if !include_npu {
            // Legacy downloads are idempotent during any active operation.
            return Ok(self.start_download());
        }
        self.start_op_result(OpKind::Download, None, include_npu)
    }

    /// Signals the running op to stop; partial files stay for resume.
    /// No-op when idle.
    pub fn cancel_download(&self) -> ModelStatus {
        if let Some(op) = self.active.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            if op.kind == OpKind::Download {
                info!("cancelling model download");
                op.cancel.store(true, Ordering::Relaxed);
            }
        }
        self.status()
    }

    /// Imports from a local path on the background thread (copies can be
    /// hundreds of MB; never block the IPC loop on them).
    #[cfg(test)]
    pub fn start_import(self: &Arc<Self>, path: PathBuf) -> Result<ModelStatus, String> {
        self.start_import_with_npu(path, false)
    }

    pub fn start_import_with_npu(
        self: &Arc<Self>,
        path: PathBuf,
        include_npu: bool,
    ) -> Result<ModelStatus, String> {
        self.start_op_result(OpKind::Import, Some(path), include_npu)
    }

    /// Deletes the model files. Refused while an op runs.
    pub fn delete(&self) -> Result<ModelStatus, String> {
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        if active.is_some() {
            return Err("a model download/import is already running".into());
        }
        *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) = None;
        let status = self.manager.delete().map_err(|e| e.to_string())?;
        self.queue_notification(status.clone());
        drop(active);
        info!("model files deleted");
        self.push_pending_notifications();
        Ok(status)
    }

    /// Compares the stored compat record against this build's manifest.
    pub fn check_compat(&self) -> Result<CompatCheck, String> {
        let raw = self.indexer.meta_get(EMBEDDING_MODEL_META_KEY).map_err(|e| e.to_string())?;
        let Some(raw) = raw else {
            return Ok(CompatCheck::NoRecord);
        };
        let Some(stored) = ModelCompat::from_json(&raw) else {
            // Unparseable record: fail closed (vectors untrusted).
            warn!("unparseable embedding_model compat record; treating as incompatible");
            return Ok(CompatCheck::Incompatible(ModelCompat::current()));
        };
        if ModelCompat::current().is_compatible_with(&stored) {
            Ok(CompatCheck::Compatible)
        } else {
            Ok(CompatCheck::Incompatible(stored))
        }
    }

    fn start_op(self: &Arc<Self>, kind: OpKind, path: Option<PathBuf>,
        include_npu: bool,
    ) -> ModelStatus {
        self.start_op_result(kind, path, include_npu)
            .unwrap_or_else(|e| {
                if self
                    .active
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_some()
                {
                    return self.status();
                }
                ModelStatus {
                state: ModelState::Error,
                error: Some(e),
                ..self.manager.status()
                }
            })
    }

    fn start_op_result(
        self: &Arc<Self>,
        kind: OpKind,
        path: Option<PathBuf>,
        include_npu: bool,
    ) -> Result<ModelStatus, String> {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(op) = active.as_ref() {
            return if kind == OpKind::Download && op.kind == kind && op.include_npu == include_npu {
                    Ok(self.progress.lock().unwrap_or_else(|p| p.into_inner()).clone())
                } else {
                Err("a different model download/import is already running".into())
            };
        }
        if kind == OpKind::Import && path.is_none() {
            return Err("import needs a local path".into());
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let id = {
            let mut next = self.next_op_id.lock().unwrap_or_else(|p| p.into_inner());
            let id = *next;
            *next += 1;
            id
        };
        *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) = None;
        let mut started = self.manager.status();
        let npu_first = include_npu && started.state == ModelState::Ready;
        set_component(&mut started, npu_first, ModelState::Downloading, None);
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = started.clone();
        let this = Arc::clone(self);
        let thread_cancel = Arc::clone(&cancel);
        if let Err(e) = std::thread::Builder::new()
            .name("winsearch-model".into())
            .spawn(move || this.run_op(id, kind, path.as_deref(), include_npu, &thread_cancel))
        {
            let msg = format!("cannot spawn model thread: {e}");
            let mut failed_status = started.clone();
            set_component(
                &mut failed_status,
                npu_first,
                ModelState::Error,
                Some(msg.clone()),
            );
            *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = failed_status.clone();
            *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(Outcome::Failed(msg.clone()));
            *active = None;
            self.queue_notification(failed_status);
            drop(active);
            self.push_pending_notifications();
            return Err(msg);
        }
        *active = Some(ActiveOp { id, kind, cancel,
            include_npu,
        });
        self.queue_notification(started.clone());
        drop(active);
        self.push_pending_notifications();

        info!(?kind, "model operation started");
        Ok(started)
    }

    fn run_op(&self, id: u64, kind: OpKind, path: Option<&Path>,
        include_npu: bool,
        cancel: &AtomicBool) {
        let last_push = Mutex::new(Instant::now());
        let progress_cb = |npu: bool, downloaded: u64, total: u64| {
            let mut snapshot = self.manager.status();
            set_component(&mut snapshot, npu, ModelState::Downloading, None);
            if npu {
                if let Some(component) = snapshot.npu_bundle.as_mut() {
                    component.bytes = downloaded;
                    component.total_bytes = total;
                }
            } else {
                snapshot.bytes = downloaded;
                snapshot.total_bytes = total;
            }
            let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
            if !active.as_ref().is_some_and(|op| op.id == id) {
                return;
            }
            *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = snapshot.clone();
            let mut last = last_push.lock().unwrap_or_else(|p| p.into_inner());
            if last.elapsed() >= PUSH_THROTTLE {
                *last = Instant::now();
                self.queue_notification(snapshot);
                drop(last);
                drop(active);
                self.push_pending_notifications();
            }
        };

        let mut npu_phase = false;
        let mut result: Result<(), OpError> = match kind {
            OpKind::Download => self
                .manager
                .download_all(&|d, t| progress_cb(false, d, t), cancel)
                .map_err(OpError::from_download),
            OpKind::Import => {
                let path = path.expect("import path checked at start");
                if include_npu && !self.manager
                    .has_import_files(path, false) {
                    Ok(())
                } else {
                    self.manager
                        .import(path, cancel)
                    .map(|_| ())
                    .map_err(OpError::from_manager)
            }
        }
        };
        if result.is_ok() && include_npu {
            if self.manager.status().state == ModelState::Ready {
                if let Err(e) = self.record_compat() {
                    warn!(error = %e, "cannot write base embedding compat record");
                }
            }
            npu_phase = true;
            let snapshot = self.manager.status();
            let component = snapshot.npu_bundle.as_ref().cloned().unwrap_or_default();
            progress_cb(true, component.bytes, component.total_bytes);
            result = match kind {
                OpKind::Download => self
                    .manager
                    .download_npu_bundle(&|d, t| progress_cb(true, d, t), cancel)
                    .map_err(OpError::from_download),
                OpKind::Import => self
                    .manager
                    .import_npu_bundle(path.expect("import path checked at start"), cancel)
                    .map(|_| ())
                    .map_err(OpError::from_manager),
            };
        }

        // Publish terminal bookkeeping before allowing another operation to start.
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        if !active.as_ref().is_some_and(|op| op.id == id) {
            return;
        }

        let status = match result {
            Ok(()) => {
                *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) = Some(Outcome::Ok);
                if self.manager.status().state == ModelState::Ready {
                    if let Err(e) = self.record_compat() {
                    warn!(error = %e, "cannot write embedding compat record");
                }
                }
                let status = self.manager.status();
                info!("model operation complete: {:?}", status.state);
                status
            }
            Err(OpError::Cancelled) => {
                *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) = Some(Outcome::Cancelled);
                // Downloads keep `.part` files (state: paused); cancelled
                // imports leave no residue (state: not_downloaded).
                let status = self.manager.status();
                info!("model operation cancelled");
                status
            }
            Err(OpError::Failed(msg)) => {
                *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(Outcome::Failed(msg.clone()));
                warn!(error = %msg, "model operation failed");
                let mut status = self.manager.status();
                set_component(&mut status, npu_phase, ModelState::Error, Some(msg));
                status
            }
        };
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = status.clone();
        *active = None;
        self.queue_notification(status);
        drop(active);
        self.push_pending_notifications();
    }

    fn record_compat(&self) -> Result<(), String> {
        self.indexer
            .meta_set(EMBEDDING_MODEL_META_KEY, &ModelCompat::current().to_json())
            .map_err(|e| e.to_string())
    }
}

fn set_component(status: &mut ModelStatus, npu: bool, state: ModelState, error: Option<String>) {
    if npu {
        if let Some(component) = status.npu_bundle.as_mut() {
            component.state = state;
            component.error = error;
        }
    } else {
        status.state = state;
        status.error = error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::sync::Mutex as StdMutex;
    use ws_index::IndexStore;
    use ws_modelmgr::{DownloadOptions, ExpectedFile};

    fn test_indexer() -> Arc<Indexer> {
        let store = IndexStore::open_in_memory().unwrap();
        Indexer::open(store, &[]).unwrap()
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "winsearch-model-test-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Tiny fixture server: serves fixed blobs per file name, honoring
    /// `Range`, with an optional per-KB drip delay for cancel tests.
    struct Fixture {
        base_url: String,
        _thread: std::thread::JoinHandle<()>,
    }

    impl Fixture {
        fn serve(files: HashMap<String, Vec<u8>>, drip: Duration) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let thread = std::thread::Builder::new()
                .name("winsearch-model-fixture".into())
                .spawn(move || {
                    for stream in listener.incoming().take(32) {
                        let mut stream = match stream {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        let mut head = vec![0u8; 4096];
                        let n = std::io::Read::read(&mut stream, &mut head).unwrap_or(0);
                        head.truncate(n);
                        let head = String::from_utf8_lossy(&head).into_owned();
                        let mut range: Option<usize> = None;
                        for line in head.lines().skip(1) {
                            if line.is_empty() {
                                break;
                            }
                            if let Some(v) = line
                                .strip_prefix("Range:")
                                .or_else(|| line.strip_prefix("range:"))
                            {
                                range = v
                                    .trim()
                                    .strip_prefix("bytes=")
                                    .and_then(|r| r.strip_suffix('-'))
                                    .and_then(|s| s.parse().ok());
                            }
                        }
                        let path = head
                            .lines()
                            .next()
                            .unwrap_or("")
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or("/")
                            .trim_start_matches('/')
                            .to_string();
                        let body = files.get(&path).cloned().unwrap_or_default();
                        if !files.contains_key(&path) {
                            let _ = stream.write_all(
                                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            );
                            continue;
                        }
                        let start = range.unwrap_or(0).min(body.len());
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
                        for piece in slice.chunks(1024) {
                            if stream.write_all(piece).is_err() {
                                break;
                            }
                            if drip > Duration::ZERO {
                                std::thread::sleep(drip);
                            }
                        }
                    }
                })
                .unwrap();
            Self {
                base_url,
                _thread: thread,
            }
        }
    }

    /// A service wired to tiny stand-in files served by `fx`.
    fn tiny_service(
        dir: PathBuf,
        fx: &Fixture,
        blobs: &[(String, Vec<u8>)]) -> Arc<ModelService> {
        use sha2::{Digest, Sha256};
        let files: Vec<ExpectedFile> = blobs
            .iter()
            .map(|(name, body)| ExpectedFile {
                // Leaked: tests are short-lived; the manifest borrows static.
                name: Box::leak(name.clone().into_boxed_str()),
                size: body.len() as u64,
                sha256: Some(Box::leak(
                    format!("{:x}", Sha256::digest(body)).into_boxed_str(),
                )),
            })
            .collect();
        let options = DownloadOptions {
            base_url_override: Some(fx.base_url.clone()),
            ..DownloadOptions::default()
        };
        let manager = ModelManager::with_files(dir, options, files);
        ModelService::with_manager(manager, test_indexer())
    }

    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while !cond() {
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        true
    }

    #[test]
    fn blocked_start_notification_does_not_hold_active_or_stop_import() {
        use std::sync::mpsc;
        let blobs = vec![("tiny.gguf".to_string(), vec![3u8; 1024])];
        let fx = Fixture::serve(HashMap::new(), Duration::ZERO);
        let srcdir = temp_dir("blocked-start-src");
        std::fs::write(srcdir.join("tiny.gguf"), &blobs[0].1).unwrap();
        let dir = temp_dir("blocked-start");
        let service = tiny_service(dir.clone(), &fx, &blobs);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = StdMutex::new(release_rx);
        let statuses = Arc::new(StdMutex::new(Vec::new()));
        let recorded = Arc::clone(&statuses);
        service.set_notifier(Arc::new(move |msg| {
            if let ServerMessage::ModelStatus { status, .. } = msg {
                if recorded.lock().unwrap().is_empty() {
                    entered_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                recorded.lock().unwrap().push(status.state);
            }
        }));
        let other = Arc::clone(&service);
        let source = srcdir.join("tiny.gguf");
        let caller = std::thread::spawn(move || other.start_import(source));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let active_unlocked = service.active.try_lock().is_ok();
        let imported = wait_until(Duration::from_secs(2), || dir.join("tiny.gguf").exists());
        release_tx.send(()).unwrap();
        caller.join().unwrap().unwrap();
        assert!(wait_until(Duration::from_secs(2), || statuses.lock().unwrap().len() >= 2));
        assert!(active_unlocked, "broadcast held the operation lock");
        assert!(imported, "broadcast prevented the import from running");
        assert_eq!(*statuses.lock().unwrap(), vec![ModelState::Downloading, ModelState::Ready]);
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(srcdir);
    }

    #[test]
    fn blocked_first_notification_cannot_publish_terminal_after_second_start() {
        use std::io::Read as _;
        use std::sync::mpsc;

        let body = vec![3u8; 1024];
        let blobs = vec![("tiny.gguf".to_string(), body.clone())];
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (download_started_tx, download_started_rx) = mpsc::channel();
        let (download_release_tx, download_release_rx) = mpsc::channel();
        let fx = Fixture {
            base_url,
            _thread: std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 8192];
                assert!(stream.read(&mut request).unwrap() > 0);
                download_started_tx.send(()).unwrap();
                download_release_rx.recv().unwrap();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                stream.write_all(&body).unwrap();
            }),
        };
        let srcdir = temp_dir("notification-order-src");
        std::fs::write(srcdir.join("tiny.gguf"), &blobs[0].1).unwrap();
        let dir = temp_dir("notification-order");
        let service = tiny_service(dir.clone(), &fx, &blobs);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = StdMutex::new(release_rx);
        let statuses = Arc::new(StdMutex::new(Vec::new()));
        let recorded = Arc::clone(&statuses);
        let first = AtomicBool::new(true);
        let weak = Arc::downgrade(&service);
        service.set_notifier(Arc::new(move |msg| {
            // Reading lifecycle state from a callback must remain deadlock-free.
            let _ = weak.upgrade().unwrap().status();
            if let ServerMessage::ModelStatus { status, .. } = msg {
                recorded.lock().unwrap().push(status.state);
                if first.swap(false, Ordering::Relaxed) {
                    entered_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
            }
        }));
        let other = Arc::clone(&service);
        let source = srcdir.join("tiny.gguf");
        let caller = std::thread::spawn(move || other.start_import(source));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let imported = wait_until(Duration::from_secs(2), || {
            service.active.lock().unwrap().is_none()
        });
        std::fs::remove_file(dir.join("tiny.gguf")).unwrap();
        assert_eq!(service.start_download().state, ModelState::Downloading);
        download_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        release_tx.send(()).unwrap();
        caller.join().unwrap().unwrap();
        let delivered = wait_until(Duration::from_secs(2), || {
            statuses.lock().unwrap().len() >= 3
        });
        let observed = statuses.lock().unwrap().clone();
        download_release_tx.send(()).unwrap();
        assert!(wait_until(Duration::from_secs(2), || service.status().state == ModelState::Ready));
        assert!(imported, "blocked delivery stopped the first import");
        assert!(delivered, "ordered lifecycle notifications were not drained");
        assert_eq!(observed, vec![ModelState::Downloading, ModelState::Ready, ModelState::Downloading]);
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(srcdir);
    }

    #[test]
    fn notification_callback_can_delete_finished_import() {
        let blobs = vec![("tiny.gguf".to_string(), vec![3u8; 1024])];
        let fx = Fixture::serve(HashMap::new(), Duration::ZERO);
        let srcdir = temp_dir("reentrant-notify-src");
        std::fs::write(srcdir.join("tiny.gguf"), &blobs[0].1).unwrap();
        let dir = temp_dir("reentrant-notify");
        let service = tiny_service(dir.clone(), &fx, &blobs);
        let weak = Arc::downgrade(&service);
        let (deleted_tx, deleted_rx) = std::sync::mpsc::channel();
        let statuses = Arc::new(StdMutex::new(Vec::new()));
        let recorded = Arc::clone(&statuses);
        service.set_notifier(Arc::new(move |msg| {
            if let ServerMessage::ModelStatus { status, .. } = msg {
                recorded.lock().unwrap().push(status.state);
                if status.state == ModelState::Ready {
                    let result = weak.upgrade().unwrap().delete();
                    deleted_tx.send(result).unwrap();
                }
            }
        }));
        service.start_import(srcdir.join("tiny.gguf")).unwrap();
        assert_eq!(deleted_rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap().state,
            ModelState::NotDownloaded);
        assert!(wait_until(Duration::from_secs(2), || statuses.lock().unwrap().len() == 3));
        assert_eq!(*statuses.lock().unwrap(),
            vec![ModelState::Downloading, ModelState::Ready, ModelState::NotDownloaded]);
        assert!(!dir.join("tiny.gguf").exists());
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(srcdir);
    }

    #[test]
    fn superseded_operation_cannot_overwrite_newer_outcome() {
        let dir = temp_dir("superseded-op");
        let service = ModelService::new(dir.clone(), test_indexer());
        *service.active.lock().unwrap() = Some(ActiveOp {
            id: 2,
            kind: OpKind::Import,
            cancel: Arc::new(AtomicBool::new(false)),
            include_npu: false,
        });
        *service.outcome.lock().unwrap() = Some(Outcome::Ok);
        service.run_op(1, OpKind::Import, Some(&dir.join("absent")),
            false, &AtomicBool::new(false));
        assert_eq!(*service.outcome.lock().unwrap(), Some(Outcome::Ok));
        assert_eq!(service.active.lock().unwrap().as_ref().unwrap().id, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn compat_record_written_and_checked() {
        let service = ModelService::new(temp_dir("compat"), test_indexer());

        assert_eq!(service.check_compat().unwrap(), CompatCheck::NoRecord);

        // Simulate a finished download's bookkeeping.
        service.record_compat().unwrap();
        assert_eq!(service.check_compat().unwrap(), CompatCheck::Compatible);

        // An incompatible record is detected (vectors would be invalidated).
        service
            .indexer
            .meta_set(
                EMBEDDING_MODEL_META_KEY,
                &ModelCompat {
                    embedding_dim: 384,
                    ..ModelCompat::current()
                }
                .to_json(),
            )
            .unwrap();
        assert!(matches!(
            service.check_compat().unwrap(),
            CompatCheck::Incompatible(_)
        ));
    }

    #[test]
    fn incompatible_compat_purges_vectors_and_requeues_on_restart() {
        // Task3-S4: seed name vectors, flip the compat record, and take the
        // restart path (`with_manager` again): stale vectors are purged and
        // every entry is requeued so the new model re-embeds everything.
        let tree = std::env::temp_dir().join(format!(
            "winsearch-model-incompat-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&tree);
        std::fs::create_dir_all(&tree).unwrap();
        std::fs::write(tree.join("keep.txt"), b"x").unwrap();

        let dir = temp_dir("incompat-restart");
        let store = ws_index::IndexStore::open_in_memory().unwrap();
        let indexer =
            crate::indexer::Indexer::open(store, &[tree.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        // Seed one name vector through the pipeline's fake path.
        let out = indexer
            .embed_once_with(|texts| {
                Ok(ws_embed_protocol::stub_embed_texts(
                    texts,
                    ws_embed_protocol::STUB_DIM,
                ))
            })
            .unwrap();
        assert!(out.embedded >= 1);
        assert!(indexer.segments_indexed().unwrap() >= 1);
        let entry_id = indexer.store_lock().find_by_path(&tree.join("keep.txt").to_string_lossy()).unwrap().unwrap().id;
        {
            let mut store = indexer.store_lock();
            store.upsert_segment(ws_index::model::NewSegment {
                entry_id, kind: "doc", chunk_index: 0, text: "old model content",
                page: None, start_ms: None, end_ms: None, line: None,
                vector: &[1.0, 0.0],
            }).unwrap();
            for job in store.claim_extract_jobs_batch_for_kind("doc", 10).unwrap() {
                store.complete_extract_job(job.id).unwrap();
            }
        }

        let first = ModelService::with_manager(
            ws_modelmgr::ModelManager::new(dir.clone()),
            Arc::clone(&indexer),
        );
        first.record_compat().unwrap();
        assert_eq!(first.check_compat().unwrap(), CompatCheck::Compatible);
        // Different pinned weights with the same model id and width.
        indexer
            .meta_set(
                EMBEDDING_MODEL_META_KEY,
                &ModelCompat {
                    repo_commit: "other-weights".into(),
                    ..ModelCompat::current()
                }
                .to_json(),
            )
            .unwrap();
        // Restart path: constructing the service again purges + requeues.
        let _second = ModelService::with_manager(
            ws_modelmgr::ModelManager::new(dir.clone()),
            Arc::clone(&indexer),
        );
        assert_eq!(
            indexer.segments_indexed().unwrap(),
            0,
            "stale vectors must be purged"
        );
        assert!(
            indexer.embed_queue_depth().unwrap() >= 1,
            "re-embed jobs must be queued"
        );
        {
            let store = indexer.store_lock();
            assert!(store.list_segments_for_entry(entry_id).unwrap().is_empty());
            assert_eq!(store.extract_queue_depth_for_kind("doc").unwrap(), 1);
        }
        // The queued job re-embeds through the pipeline's fake path.
        let out = indexer
            .embed_once_with(|texts| {
                Ok(ws_embed_protocol::stub_embed_texts(
                    texts,
                    ws_embed_protocol::STUB_DIM,
                ))
            })
            .unwrap();
        assert!(out.embedded >= 1);
        let _ = std::fs::remove_dir_all(&tree);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_media_pipeline_requeues_vectors_and_preserves_exact_chunks() {
        for pipeline in [None, Some(1)] {
        let dir = temp_dir("legacy-media-pipeline");
        let root = dir.join("source");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("keep.txt"), b"durableexact").unwrap();
        let mut store = ws_index::IndexStore::open_in_memory().unwrap();
        let mut config = store.get_or_migrate_indexing_config().unwrap();
        // This fixture tests migration; host Battery Saver must not skip its scan.
        config.respect_battery_saver = false;
        store.set_indexing_config(&config).unwrap();
        let indexer = crate::indexer::Indexer::open(
            store,
            &[root.clone()])
        .unwrap();
        assert!(indexer.indexing_pause_reason().is_none(), "fixture scan paused: {:?}", indexer.indexing_pause_reason());
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let (entry, before, generation) = {
            let mut store = indexer.store_lock();
            let entry = store
                .find_by_path(&root.join("keep.txt").to_string_lossy())
                .unwrap()
                .unwrap()
                .id;
            let job = store
                .claim_extract_jobs_batch_for_kind("doc", 1)
                .unwrap()
                .pop()
                .unwrap();
            let progress = store
                .begin_content_batch(entry, "doc", &crate::extract::source_snapshot(&root.join("keep.txt")))
                .unwrap();
            let mut chunk = ws_index::model::ContentChunk {
                id: 0,
                entry_id: entry,
                job_kind: "doc".into(),
                kind: "doc".into(),
                chunk_index: 0,
                text: "durableexact".into(),
                location: Default::default(),
                embed: true,
                visual: false,
                embedding_input: Default::default(),
                attempts: 0,
            };
            chunk.location.line = Some(4);
            assert!(store
                .commit_content_batch(&job, &progress, &[chunk], None)
                .unwrap());
            let chunks = store.claim_content_vectors(Some(entry)).unwrap();
            assert_eq!(chunks.len(), 1);
            let generation = store.model_generation().unwrap();
            assert!(store
                .commit_content_vectors(&chunks, &[vec![1.0, 0.0]], progress.revision, generation)
                .unwrap());
            (
                entry,
                store.list_content_chunks(entry).unwrap(),
                store.model_generation().unwrap(),
            )
        };
        let mut legacy = serde_json::to_value(ModelCompat::current()).unwrap();
        if let Some(version) = pipeline {
            legacy["vector_pipeline_version"] = serde_json::json!(version);
        } else {
            legacy.as_object_mut().unwrap().remove("vector_pipeline_version");
        }
        indexer
            .meta_set(EMBEDDING_MODEL_META_KEY, &legacy.to_string())
            .unwrap();
        let _service = ModelService::with_manager(
            ws_modelmgr::ModelManager::new(dir.join("models")),
            Arc::clone(&indexer),
        );
        let mut store = indexer.store_lock();
        let retained = store.list_content_chunks(entry).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].id, before[0].id);
        assert_eq!(retained[0].text, "durableexact");
        assert_eq!(retained[0].location.line, Some(4));
        assert_eq!(
            store
                .content_matches(entry, &["durableexact".into()])
                .unwrap()
                .len(),
            1
        );
        assert!(store.list_segments_for_entry(entry).unwrap().is_empty());
        assert!(store.model_generation().unwrap() > generation);
        assert!(
            store
                .content_progress(entry, "doc")
                .unwrap()
                .unwrap()
                .complete
        );
        let pending = store.claim_content_vectors(Some(entry)).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, before[0].id);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn cancel_download_does_not_cancel_an_import() {
        let service = ModelService::new(temp_dir("import-cancel"), test_indexer());
        let cancel = Arc::new(AtomicBool::new(false));
        *service.active.lock().unwrap() = Some(ActiveOp {
            id: 1, kind: OpKind::Import, cancel: Arc::clone(&cancel),
            include_npu: false,
        });
        service.cancel_download();
        assert!(!cancel.load(Ordering::Relaxed));
        assert!(service.try_start_download_with_npu(false).is_ok());
        assert!(service.try_start_download_with_npu(true).is_err());
        assert!(!cancel.load(Ordering::Relaxed));
    }

    #[test]
    fn cancel_without_active_op_is_noop() {
        let service = ModelService::new(temp_dir("idle-cancel"), test_indexer());
        assert_eq!(service.status().state, ModelState::NotDownloaded);
        assert_eq!(service.cancel_download().state, ModelState::NotDownloaded);
    }

    #[test]
    fn optional_npu_download_failure_keeps_base_ready() {
        let base = ("base.gguf".to_string(), b"base".to_vec());
        let npu = (
            "iron-embeddinggemma2-npu2/fixture.bin".to_string(),
            b"npu".to_vec(),
        );
        let fx = Fixture::serve([base.clone()].into_iter().collect(), Duration::ZERO);
        let dir = temp_dir("optional-npu-error");
        let service = tiny_service(dir.clone(), &fx, &[base.clone(), npu.clone()]);
        service.start_download();
        assert!(wait_until(Duration::from_secs(10), || service
            .status()
            .state
            == ModelState::Ready));
        assert!(
            !dir.join(&npu.0).exists(),
            "default request must not install the bundle"
        );
        assert_eq!(service.status().total_bytes, 4);
        assert_eq!(
            service.status().npu_bundle.unwrap().state,
            ModelState::NotDownloaded
        );

        service.start_download_with_npu(true);
        assert!(wait_until(Duration::from_secs(10), || service
            .status()
            .npu_bundle
            .as_ref()
            .is_some_and(|s| s.state == ModelState::Error)));
        let status = service.status();
        assert_eq!(status.state, ModelState::Ready);
        assert_eq!(status.bytes, 4);
        assert!(status.error.is_none());
        assert!(status.npu_bundle.unwrap().error.is_some());
        assert_eq!(service.check_compat().unwrap(), CompatCheck::Compatible);
        std::fs::remove_file(dir.join(&base.0)).unwrap();
        assert_eq!(
            service.status().state,
            ModelState::NotDownloaded,
            "an optional failure must not freeze the base filesystem snapshot"
        );
        service.delete().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn explicit_npu_request_cannot_be_silently_merged_into_base_only_operation() {
        let service = ModelService::new(temp_dir("npu-busy-opt-in"), test_indexer());
        *service.active.lock().unwrap() = Some(ActiveOp {
            id: 1,
            kind: OpKind::Download,
            cancel: Arc::new(AtomicBool::new(false)),
            include_npu: false,
        });
        let result = service.start_op_result(OpKind::Download, None, true);
        assert!(
            result.is_err(),
            "an opt-in must not acknowledge base-only work as an NPU install"
        );
    }

    #[test]
    fn optional_npu_cancel_resumes_and_import_rejects_corrupt_bundle() {
        let base = ("base.gguf".to_string(), b"base".to_vec());
        let npu = (
            "iron-embeddinggemma2-npu2/fixture.bin".to_string(),
            vec![5; 512 * 1024],
        );
        let blobs = [base.clone(), npu.clone()];
        let fx = Fixture::serve(
            blobs.clone().into_iter().collect(),
            Duration::from_millis(2),
        );
        let dir = temp_dir("optional-npu-resume");
        std::fs::write(dir.join(&base.0), &base.1).unwrap();
        let service = tiny_service(dir.clone(), &fx, &blobs);
        service.start_download_with_npu(true);
        assert!(wait_until(Duration::from_secs(10), || service
            .status()
            .npu_bundle
            .as_ref()
            .is_some_and(|s| s.bytes > 0)));
        service.cancel_download();
        assert!(wait_until(Duration::from_secs(10), || service
            .status()
            .npu_bundle
            .as_ref()
            .is_some_and(|s| s.state == ModelState::Paused)));
        let paused = service.status();
        assert_eq!(paused.state, ModelState::Ready);
        assert!(paused.npu_bundle.unwrap().bytes > 0);
        service.start_download_with_npu(true);
        assert!(wait_until(Duration::from_secs(20), || service
            .status()
            .npu_bundle
            .as_ref()
            .is_some_and(|s| s.state == ModelState::Ready)));
        assert_eq!(std::fs::read(dir.join(&npu.0)).unwrap(), npu.1);

        let source = temp_dir("optional-npu-corrupt");
        std::fs::write(source.join("fixture.bin"), vec![9; npu.1.len()]).unwrap();
        service.start_import_with_npu(source.clone(), true).unwrap();
        assert!(wait_until(Duration::from_secs(10), || service
            .status()
            .npu_bundle
            .as_ref()
            .is_some_and(|s| s.state == ModelState::Error)));
        assert_eq!(service.status().state, ModelState::Ready);
        assert_eq!(
            std::fs::read(dir.join(&npu.0)).unwrap(),
            npu.1,
            "failed import must preserve verified files"
        );
        service.delete().unwrap();
        assert!(!dir.join(&base.0).exists());
        assert!(!dir.join(&npu.0).exists());
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(source);
    }

    #[test]
    fn cancelled_word_in_other_errors_is_not_cancel() {
        // A failure that merely mentions "cancelled" must stay a failure.
        let e = OpError::from_download(DownloadError::Http("upstream cancelled by admin".into()));
        assert!(matches!(e, OpError::Failed(_)), "got {e:?}");
        let e = OpError::from_manager(ManagerError::Import("source cancelled mid-read".into()));
        assert!(matches!(e, OpError::Failed(_)), "got {e:?}");
        // ...while the real variants still classify as cancel.
        assert_eq!(
            OpError::from_download(DownloadError::Cancelled(1, 2)),
            OpError::Cancelled
        );
        assert_eq!(
            OpError::from_manager(ManagerError::Download(DownloadError::Cancelled(3, 4))),
            OpError::Cancelled
        );
    }

    #[test]
    fn busy_import_refused_while_download_runs() {
        let body = vec![5u8; 512 * 1024];
        let blobs = vec![("slow-busy.gguf".to_string(), body)];
        let files: HashMap<String, Vec<u8>> = blobs.iter().cloned().collect();
        let fx = Fixture::serve(files, Duration::from_millis(2));
        let dir = temp_dir("svc-busy-import");
        let service = tiny_service(dir.clone(), &fx, &blobs);

        service.start_download();
        assert!(wait_until(Duration::from_secs(10), || service.status().bytes > 0));
        // Refused (engine maps this to Error{invalid_request}), not merged.
        let err = service.start_import(dir.join("whatever.gguf")).unwrap_err();
        assert!(err.contains("already running"), "got {err}");
        service.cancel_download();
        assert!(
            wait_until(Duration::from_secs(10), || service.status().state == ModelState::Paused),
            "cancel never settled, got {:?}",
            service.status()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn busy_second_import_refused_while_import_runs() {
        // 96 MB: the first import is still copying when the second arrives
        // (copy + debug-build hashing take seconds).
        let body = vec![7u8; 96 * 1024 * 1024];
        let blobs = vec![("huge.gguf".to_string(), body)];
        let fx = Fixture::serve(HashMap::new(), Duration::ZERO);
        let srcdir = temp_dir("svc-import-big-src");
        std::fs::write(srcdir.join("huge.gguf"), &blobs[0].1).unwrap();
        let dir = temp_dir("svc-import-big");
        let service = tiny_service(dir.clone(), &fx, &blobs);

        let first = service.start_import(srcdir.join("huge.gguf")).unwrap();
        assert_eq!(first.state, ModelState::Downloading);
        // A concurrent import is refused...
        let err = service.start_import(srcdir.join("huge.gguf")).unwrap_err();
        assert!(err.contains("already running"), "got {err}");
        // ...while a download start stays idempotent-ok (no second thread).
        assert_eq!(service.start_download().state, ModelState::Downloading);

        // The first import still completes byte-identical with compat written.
        assert!(
            wait_until(Duration::from_secs(90), || service.status().state == ModelState::Ready),
            "import never reached ready, got {:?}",
            service.status()
        );
        assert_eq!(
            std::fs::read(dir.join("huge.gguf")).unwrap(),
            std::fs::read(srcdir.join("huge.gguf")).unwrap()
        );
        assert_eq!(service.check_compat().unwrap(), CompatCheck::Compatible);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&srcdir);
    }

    #[test]
    fn delete_while_idle_clears_files() {
        let dir = temp_dir("delete");
        let service = ModelService::new(dir.clone(), test_indexer());
        std::fs::write(dir.join(ws_modelmgr::TEXT_FILE), b"x").unwrap();
        let s = service.delete().unwrap();
        assert_eq!(s.state, ModelState::NotDownloaded);
        assert!(!dir.join(ws_modelmgr::TEXT_FILE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn full_download_thread_reports_ready_and_writes_compat() {
        let blobs = vec![
            ("tiny-a.gguf".to_string(), vec![9u8; 8192]),
            ("tiny-b.gguf".to_string(), vec![3u8; 16384]),
        ];
        let files: HashMap<String, Vec<u8>> = blobs.iter().cloned().collect();
        let fx = Fixture::serve(files, Duration::ZERO);
        let dir = temp_dir("svc-full");
        let service = tiny_service(dir.clone(), &fx, &blobs);

        let pushes: Arc<StdMutex<Vec<ModelStatus>>> = Arc::new(StdMutex::new(Vec::new()));
        let pushes2 = Arc::clone(&pushes);
        service.set_notifier(Arc::new(move |msg: ServerMessage| {
            if let ServerMessage::ModelStatus { status, .. } = msg {
                pushes2.lock().unwrap().push(status);
            }
        }));

        let started = service.start_download();
        assert_eq!(started.state, ModelState::Downloading);
        // Second start while running is idempotent (no second thread).
        assert_eq!(service.start_download().state, ModelState::Downloading);

        assert!(
            wait_until(Duration::from_secs(30), || service.status().state == ModelState::Ready),
            "download never reached ready"
        );
        assert_eq!(service.check_compat().unwrap(), CompatCheck::Compatible);
        // Start + completion pushes arrived (progress pushes are throttled).
        let pushes = pushes.lock().unwrap();
        assert!(pushes.iter().any(|s| s.state == ModelState::Downloading));
        assert!(pushes.iter().any(|s| s.state == ModelState::Ready));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cancel_mid_download_then_resume_to_ready() {
        let body = vec![5u8; 512 * 1024];
        let blobs = vec![("slow.gguf".to_string(), body.clone())];
        let files: HashMap<String, Vec<u8>> = blobs.iter().cloned().collect();
        // ~1 s for the whole blob: the cancel below lands mid-stream.
        let fx = Fixture::serve(files, Duration::from_millis(2));
        let dir = temp_dir("svc-resume");
        let service = tiny_service(dir.clone(), &fx, &blobs);

        service.start_download();
        assert!(wait_until(Duration::from_secs(10), || {
            service.status().bytes > 0
        }));
        service.cancel_download();
        assert!(
            wait_until(Duration::from_secs(10), || service.status().state == ModelState::Paused),
            "cancel never settled to paused, got {:?}",
            service.status()
        );
        assert!(dir.join("slow.gguf.part").exists());

        // Resume on a fresh op completes byte-identical.
        service.start_download();
        assert!(
            wait_until(Duration::from_secs(30), || service.status().state == ModelState::Ready),
            "resume never reached ready"
        );
        assert_eq!(std::fs::read(dir.join("slow.gguf")).unwrap(), body);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
