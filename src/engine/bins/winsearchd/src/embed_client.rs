//! Supervisor for the `winsearch-embed` worker process.
//!
//! The supervisor owns the worker's lifetime on behalf of the engine and is
//! the worker's only client (one connection at a time, enforced by holding
//! [`Slot`] across a whole request). It:
//! - spawns the worker on the first embed request (never before —
//!   `EngineService::status` must stay spawn-free),
//! - handshakes every connection (`hello` doubles as the health ping and
//!   refreshes the cached `model_loaded`/`stub` flags),
//! - restarts with backoff after a transport failure and retries the
//!   in-flight job exactly once,
//! - never retries definitive worker answers (`model_not_available`,
//!   `unsupported`, ...).
//!
//! `EngineStatus.model_loaded` reads the cached flags: true when real weights
//! are resident **or** the worker serves deterministic stub vectors (test
//! setups). Production never sets the stub flag, so the overlay hint logic
//! is unaffected there.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};
use windows::Win32::System::IO::CancelSynchronousIo;
use windows::Win32::System::Threading::{
    GetCurrentThreadId, OpenThread, THREAD_ALL_ACCESS,
};
use ws_embed_protocol::{
    EmbedErrorCode, EmbedRequest, EmbedResponse, EMBED_PROTOCOL_VERSION, MAX_TEXTS_PER_JOB,
};
use ws_platform::{identity, pipe::PipeStream};
use ws_protocol::{read_message, write_message};

use crate::server::ENGINE_VERSION;

/// Base backoff before respawning a crashed worker; doubles per consecutive
/// failure, capped at [`MAX_BACKOFF`]. Fast enough to not stall queries,
/// slow enough to not hot-loop a poisoned worker.
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// How long to wait for a freshly spawned worker's pipe to appear. Generous:
/// process start + DLL paging on a loaded machine takes seconds; the success
/// path returns on the first poll either way.
const SPAWN_WAIT: Duration = Duration::from_secs(30);
/// Connect retries inside one request. Covers the pipe-busy race: a readiness
/// probe (or a previous request) consumes the worker's current instance, and
/// the next instance appears a breath later — or seconds later on a loaded
/// machine. Bounded so a truly dead worker still surfaces as `Transport`
/// (restart path) instead of hanging the caller.
const CONNECT_RETRY: Duration = Duration::from_secs(15);
const CONNECT_POLL: Duration = Duration::from_millis(25);

/// Bounded job-IO timeout for the semantic pipeline's embed calls (P3.5,
/// Task2-I1 carried requirement).
///
/// The pipe transport itself is blocking with no per-read deadline, so the
/// scheduler never calls [`EmbedSupervisor::embed_texts`] directly: it calls
/// [`EmbedSupervisor::embed_texts_bounded`] with this timeout. A worker that
/// hangs mid-job (never answers) fails the batch as `Transport` (claims are
/// released for retry) instead of stalling the scheduler thread forever.
///
/// Value: **120 s** per `embed_texts` call (up to 256 texts; the pipeline
/// batches 128). Budget: spawn wait (30 s) + connect retries (15 s) + model
/// lazy-load on a cold worker (tens of seconds for ~865 MB) + CPU embedding
/// of the batch (seconds). The success path returns in seconds; only a truly
/// wedged worker ever hits the cap.
pub const EMBED_JOB_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("embed worker transport: {0}")]
    Transport(String),
    #[error("embed protocol violation: {0}")]
    Protocol(String),
    #[error("embed protocol version mismatch: {0}")]
    VersionMismatch(String),
    #[error("embedding model not available: {0}")]
    ModelNotAvailable(String),
    #[error("embedding input unavailable: {0}")]
    InputUnavailable(String),
    #[error("embedding failed: {0}")]
    EmbedFailed(String),
    #[error("unsupported by embed worker: {0}")]
    Unsupported(String),
    #[error("invalid embed request: {0}")]
    InvalidRequest(String),
    #[error("cannot start embed worker: {0}")]
    Spawn(String),
}

impl EmbedError {
    fn transport(e: impl std::fmt::Display) -> Self {
        Self::Transport(e.to_string())
    }

    fn from_worker(code: EmbedErrorCode, message: String) -> Self {
        match code {
            EmbedErrorCode::VersionMismatch => Self::VersionMismatch(message),
            EmbedErrorCode::InvalidRequest => Self::InvalidRequest(message),
            EmbedErrorCode::ModelNotAvailable => Self::ModelNotAvailable(message),
            EmbedErrorCode::EmbedFailed => Self::EmbedFailed(message),
            EmbedErrorCode::Unsupported => Self::Unsupported(message),
        }
    }
}

/// Snapshot of the worker's self-reported state (answers [`EmbedSupervisor::ping`]).
#[derive(Debug, Clone)]
pub struct WorkerSnapshot {
    pub model_loaded: bool,
    pub backend: Option<String>,
    pub stub: bool,
    pub dim: usize,
    pub uptime_secs: u64,
    pub jobs_served: u64,
}

#[derive(Debug, Clone, Default)]
pub struct EmbedStats {
    pub spawns: u64,
    pub restarts: u64,
    pub retries: u64,
    pub jobs: u64,
}

/// How to reach and launch the worker. Explicit config beats env beats
/// compiled defaults, so integration tests can run in parallel with unique
/// pipes and CLI args instead of mutating process-global env.
#[derive(Clone)]
pub struct EmbedConfig {
    pub worker_bin: PathBuf,
    pub pipe_name: String,
    pub worker_args: Vec<String>,
    pub backoff_base: Duration,
    /// Test hook: when set, backoff waits call this instead of blocking the
    /// thread, so tests assert the sleep schedule deterministically.
    /// `None` (production) blocks on the calling thread.
    pub sleeper: Option<std::sync::Arc<dyn Fn(Duration) + Send + Sync>>,
}

impl std::fmt::Debug for EmbedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbedConfig")
            .field("worker_bin", &self.worker_bin)
            .field("pipe_name", &self.pipe_name)
            .field("worker_args", &self.worker_args)
            .field("backoff_base", &self.backoff_base)
            .field("sleeper_set", &self.sleeper.is_some())
            .finish()
    }
}

impl EmbedConfig {
    pub fn from_env_or_default() -> Self {
        let worker_bin = match std::env::var("WS_EMBED_BIN") {
            Ok(p) if !p.is_empty() => PathBuf::from(p),
            _ => default_worker_bin(),
        };
        let pipe_name = match std::env::var("WS_EMBED_PIPE") {
            Ok(p) if !p.is_empty() => p,
            _ => identity::embed_pipe_name().unwrap_or_else(|_| {
                // Identity lookup cannot realistically fail; a pid-suffixed
                // fallback keeps the supervisor usable if it ever does.
                format!(r"\\.\pipe\winsearch-embed-unknown-{}", std::process::id())
            }),
        };
        let backoff_base = match std::env::var("WS_EMBED_BACKOFF_MS") {
            Ok(v) => v
                .parse::<u64>()
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_BACKOFF_BASE),
            Err(_) => DEFAULT_BACKOFF_BASE,
        };
        Self {
            worker_bin,
            pipe_name,
            worker_args: Vec::new(),
            backoff_base,
            sleeper: None,
        }
    }
}

/// The worker ships next to the daemon (`winsearch-embed.exe` beside
/// `winsearchd.exe`, same layout as the installer target).
fn default_worker_bin() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
        .join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX))
}

/// Owns the worker child (when we spawned it). Separate from the connection
/// mutex below so an admin/test kill lands even mid-request; the in-flight
/// job then fails over to the restart path.
struct Slot {
    child: Option<Child>,
}

#[derive(Default)]
struct BoundedCalls {
    active: usize,
    epoch: u64,
}

pub struct EmbedSupervisor {
    config: EmbedConfig,
    /// Serializes whole requests on the legacy direct path (`transact`,
    /// used by `embed_texts`/`ping`). The pipeline's bounded path
    /// (`transact_bounded`) holds this only across ensure+connect+send —
    /// never across the blocking reply read — so a hung worker cannot wedge
    /// later batches behind a leaked holder.
    conn: Mutex<()>,
    slot: Mutex<Slot>,
    /// Cancel registrations for in-flight bounded calls, keyed by `call_id`
    /// (`call_id -> os_thread_id`). The watchdog cancels only the timed-out
    /// caller's thread via `CancelSynchronousIo`, which unblocks the helper
    /// even for foreign (unkillable-by-us) workers without disrupting other
    /// concurrent bounded callers.
    inflight_tids: Mutex<HashMap<u64, u32>>,
    bounded_calls: Mutex<BoundedCalls>,
    worker_gen: AtomicU64,
    preferred_backend: Mutex<Option<String>>,
    cancel_seq: AtomicU64,
    next_id: AtomicU64,
    failures: AtomicU32,
    model_loaded: AtomicBool,
    stub: AtomicBool,
    stats: Mutex<EmbedStats>,
}

impl EmbedSupervisor {
    pub fn new() -> Self {
        Self::with_config(EmbedConfig::from_env_or_default())
    }

    pub fn with_config(config: EmbedConfig) -> Self {
        Self {
            config,
            conn: Mutex::new(()),
            slot: Mutex::new(Slot { child: None }),
            inflight_tids: Mutex::new(HashMap::new()),
            bounded_calls: Mutex::new(BoundedCalls::default()),
            worker_gen: AtomicU64::new(1),
            preferred_backend: Mutex::new(None),
            cancel_seq: AtomicU64::new(1),
            next_id: AtomicU64::new(1),
            failures: AtomicU32::new(0),
            model_loaded: AtomicBool::new(false),
            stub: AtomicBool::new(false),
            stats: Mutex::new(EmbedStats::default()),
        }
    }

    /// Updates the preferred inference backend (`auto|cuda|vulkan|cpu`) to pass
    /// to spawned workers when `--backend` is not already hardcoded in
    /// `config.worker_args`. If the preference changes and no bounded calls are
    /// in flight, any idle owned worker is terminated so the next request
    /// launches with the new backend.
    pub fn set_preferred_backend(&self, backend: &str) {
        let normalized = backend.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return;
        }
        let changed = {
            let mut guard = self
                .preferred_backend
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if guard.as_deref() == Some(normalized.as_str()) {
                false
            } else {
                *guard = Some(normalized);
                true
            }
        };
        let idle = self.bounded_calls.lock().unwrap_or_else(|p| p.into_inner()).active == 0;
        if changed && idle {
            self.terminate_worker();
        }
    }

    pub fn worker_bin(&self) -> &PathBuf {
        &self.config.worker_bin
    }

    pub fn pipe_name(&self) -> &str {
        &self.config.pipe_name
    }

    /// Cached worker state: real weights resident, or stub vectors flowing.
    /// Updated by every handshake; never spawns a worker by itself.
    pub fn model_loaded(&self) -> bool {
        self.model_loaded.load(Ordering::Relaxed) || self.stub.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> EmbedStats {
        self.stats.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// True while our spawned child is alive (reaps it once it exits, e.g.
    /// after its idle timeout). Never probes the pipe: a foreign worker is
    /// not ours to report on.
    pub fn is_worker_running(&self) -> bool {
        let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
        match slot.child.as_mut() {
            Some(child) => match child.try_wait() {
                Ok(None) => true,
                _ => {
                    slot.child = None;
                    false
                }
            },
            None => false,
        }
    }

    /// Kill our worker, if any, and forget it. The next request respawns.
    /// Test hook for the kill-mid-job test (and a future admin path).
    /// Kill failures are logged explicitly (a wedged child that refuses to
    /// die must be visible; the watchdog path depends on this break).
    pub fn terminate_worker(&self) {
        let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
        match kill_locked(&mut slot) {
            Ok(true) => {
                self.worker_gen.fetch_add(1, Ordering::Release);
                info!("embed worker killed");
            }
            Ok(false) => {}
            Err(e) => warn!(error = %e, "embed worker kill failed"),
        }
    }

    fn begin_bounded(&self) {
        let mut calls = self.bounded_calls.lock().unwrap_or_else(|p| p.into_inner());
        calls.active += 1;
        calls.epoch += 1;
    }

    fn finish_bounded(self: &std::sync::Arc<Self>, timed_out: bool) {
        let mut calls = self.bounded_calls.lock().unwrap_or_else(|p| p.into_inner());
        calls.active -= 1;
        if !timed_out || calls.active != 0 {
            return;
        }
        let epoch = calls.epoch;
        let worker_gen = self.worker_gen.load(Ordering::Acquire);
        drop(calls);
        // Startup may hold `slot` for 30s. Cleanup must never extend the caller's
        // deadline, and a newer caller must invalidate deferred cleanup.
        let sup = std::sync::Arc::clone(self);
        if let Err(e) = std::thread::Builder::new().name("winsearch-embed-cleanup".into()).spawn(move || {
            sup.retire_timed_out_worker(epoch, worker_gen);
        }) {
            warn!(error = %e, "cannot spawn embed timeout cleanup");
        }
    }

    fn retire_timed_out_worker(&self, epoch: u64, worker_gen: u64) {
        let child = {
            let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
            let calls = self.bounded_calls.lock().unwrap_or_else(|p| p.into_inner());
            if calls.active != 0 || calls.epoch != epoch
                || self.worker_gen.load(Ordering::Acquire) != worker_gen {
                return;
            }
            let child = slot.child.take();
            if child.is_some() {
                self.worker_gen.fetch_add(1, Ordering::Release);
            }
            child
        };
        if let Some(mut child) = child {
            if let Err(e) = child.kill().and_then(|()| child.wait().map(|_| ())) {
                warn!(error = %e, "embed worker kill failed");
            }
        }
    }

    /// Embed texts, one job per [`MAX_TEXTS_PER_JOB`] chunk, in order.
    /// Spawns the worker on first use; restarts it and retries once when the
    /// connection breaks mid-job.
    pub fn embed_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Err(EmbedError::InvalidRequest(
                "cannot embed an empty batch".into(),
            ));
        }
        let mut all = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(MAX_TEXTS_PER_JOB) {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let req = EmbedRequest::EmbedTexts {
                id,
                texts: chunk.to_vec(),
            };
            match self.transact(req) {
                Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                    self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                    all.extend(vecs);
                }
                Ok(other) => {
                    return Err(EmbedError::Protocol(format!(
                        "unexpected reply to embed_texts: {}",
                        response_kind(&other)
                    )));
                }
                Err(EmbedError::Transport(_)) => {
                    // The worker died mid-job (or never answered): restart
                    // with backoff and retry the chunk exactly once.
                    self.stats.lock().unwrap_or_else(|p| p.into_inner()).retries += 1;
                    info!("embed job interrupted; restarting worker and retrying once");
                    let retry_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                    let retry = EmbedRequest::EmbedTexts {
                        id: retry_id,
                        texts: chunk.to_vec(),
                    };
                    match self.transact_after_restart(retry) {
                        Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                            self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                            all.extend(vecs);
                        }
                        Ok(other) => {
                            return Err(EmbedError::Protocol(format!(
                                "unexpected reply to retried embed_texts: {}",
                                response_kind(&other)
                            )));
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(all)
    }

    /// Bounded embed path for the semantic pipeline (Task2-I1, fix round
    /// 1/5: leak-free watchdog).
    ///
    /// Runs the whole call on a helper thread and waits `timeout`
    /// ([`EMBED_JOB_TIMEOUT`] in production). A hung worker — handshake done
    /// but no `embeddings` answer — surfaces as `Transport("... timed out
    /// after ...")` instead of stalling the scheduler thread forever.
    /// Takes `Arc<Self>` so the helper thread owns a share.
    ///
    /// Leak-free guarantees (fix round 1/5):
    /// - The helper uses [`Self::transact_bounded`], which holds the
    ///   supervisor `conn` mutex only across ensure+connect+handshake+send
    ///   and releases it before the blocking reply read. A timed-out helper
    ///   therefore never wedges later batches behind a leaked `conn` holder.
    /// - Before the blocking read the helper registers its OS thread id;
    ///   the watchdog cancels exactly that thread's synchronous pipe IO via
    ///   `CancelSynchronousIo`, so the helper unblocks even for foreign
    ///   (unkillable-by-us) workers. Stale generations are ignored, so a
    ///   recycled thread id is never cancelled.
    /// - On timeout the owned child (if any) is still killed to break the
    ///   hung pipe IO, with kill failures logged explicitly.
    ///
    /// `std::thread`-only (no async): one helper thread + `mpsc::recv_timeout`.
    pub fn embed_texts_bounded(
        self: &std::sync::Arc<Self>,
        texts: &[String],
        timeout: Duration,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        use std::sync::mpsc;
        if texts.is_empty() {
            return Err(EmbedError::InvalidRequest(
                "cannot embed an empty batch".into(),
            ));
        }
        let call_id = self.cancel_seq.fetch_add(1, Ordering::Relaxed);
        self.begin_bounded();
        let sup = std::sync::Arc::clone(self);
        let owned: Vec<String> = texts.to_vec();
        let cancel = std::sync::Arc::new(AtomicBool::new(false));
        let cancel2 = std::sync::Arc::clone(&cancel);
        let deadline = Instant::now() + timeout;
        let (tx, rx) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("winsearch-embed-bounded".into())
            .spawn(move || {
                let r = sup.embed_texts_cancelable(call_id, &owned, &cancel2, deadline);
                let _ = tx.send(r);
            });
        if let Err(e) = spawned {
            self.finish_bounded(false);
            return Err(EmbedError::Transport(format!(
                "cannot spawn embed helper: {e}"
            )));
        }
        let outcome = rx.recv_timeout(timeout);
        match outcome {
            Ok(r) => {
                self.finish_bounded(false);
                r
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Stop the helper promptly: flag it and cancel only this
                // caller's synchronous pipe IO, then defer owned-worker cleanup
                // if this was the last active bounded caller.
                cancel.store(true, Ordering::Relaxed);
                self.cancel_inflight_read_for(call_id);
                self.finish_bounded(true);
                warn!("embed job timed out after {timeout:?}; helper cancelled, batch fails for retry");
                Err(EmbedError::Transport(format!(
                    "embed job timed out after {timeout:?}"
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.finish_bounded(false);
                Err(EmbedError::Transport(
                    "embed helper died before answering".into(),
                ))
            }
        }
    }

    /// Helper body for [`Self::embed_texts_bounded`]: like [`Self::embed_texts`]
    /// (one job per chunk, exactly one restart+retry on transport failure) but
    /// every job goes through [`Self::transact_bounded`], which never holds
    /// `conn` across the blocking read and honors `cancel`/`deadline`.
    fn embed_texts_cancelable(
        &self,
        call_id: u64,
        texts: &[String],
        cancel: &AtomicBool,
        deadline: Instant,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut all = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(MAX_TEXTS_PER_JOB) {
            if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
                return Err(EmbedError::Transport("embed job cancelled".into()));
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let req = EmbedRequest::EmbedTexts {
                id,
                texts: chunk.to_vec(),
            };
            let mut used_gen = self.worker_gen.load(Ordering::Acquire);
            match self.transact_bounded(call_id, req, cancel, deadline, &mut used_gen) {
                Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                    self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                    all.extend(vecs);
                }
                Ok(other) => {
                    return Err(EmbedError::Protocol(format!(
                        "unexpected reply to embed_texts: {}",
                        response_kind(&other)
                    )));
                }
                Err(EmbedError::Transport(_)) => {
                    self.stats.lock().unwrap_or_else(|p| p.into_inner()).retries += 1;
                    info!("embed job interrupted; restarting worker and retrying once");
                    let retry_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                    let retry = EmbedRequest::EmbedTexts {
                        id: retry_id,
                        texts: chunk.to_vec(),
                    };
                    match self.transact_bounded_after_restart(
                        call_id,
                        retry,
                        cancel,
                        deadline,
                        used_gen,
                    ) {
                        Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                            self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                            all.extend(vecs);
                        }
                        Ok(other) => {
                            return Err(EmbedError::Protocol(format!(
                                "unexpected reply to retried embed_texts: {}",
                                response_kind(&other)
                            )));
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(all)
    }

    /// Bounded single-image embed path (`EmbedRequest::EmbedImage { id, path }`)
    /// for the Phase 4 image extractor. Uses the same leak-free helper +
    /// `CancelSynchronousIo` watchdog as [`Self::embed_texts_bounded`].
    pub fn embed_image_bounded(
        self: &std::sync::Arc<Self>,
        path: &std::path::Path,
        timeout: Duration,
    ) -> Result<Vec<f32>, EmbedError> {
        self.embed_image_with_budget_bounded(path, 280, timeout)
    }

    pub fn embed_image_with_budget_bounded(
        self: &std::sync::Arc<Self>,
        path: &std::path::Path,
        token_budget: u32,
        timeout: Duration,
    ) -> Result<Vec<f32>, EmbedError> {
        if !matches!(token_budget, 70 | 140 | 280) {
            return Err(EmbedError::InvalidRequest(
                "invalid image token budget".into(),
            ));
        }
        self.embed_media_bounded(
            EmbedRequest::EmbedImage {
                id: 0,
                path: path.to_string_lossy().into_owned(),
                token_budget: Some(token_budget),
            },
            timeout,
        )
    }

    pub fn embed_audio_bounded(
        self: &std::sync::Arc<Self>,
        path: &std::path::Path,
        window_ms: Option<(u64, u64)>,
        timeout: Duration,
    ) -> Result<Vec<f32>, EmbedError> {
        if window_ms.is_some_and(|(s, e)| e <= s || e - s > 30_000 || e.checked_mul(16).is_none()) {
            return Err(EmbedError::InvalidRequest("invalid audio window".into()));
        }
        self.embed_media_bounded(
            EmbedRequest::EmbedAudio {
                id: 0,
                path: path.to_string_lossy().into_owned(),
                window_ms,
            },
            timeout,
        )
    }

    fn embed_media_bounded(
        self: &std::sync::Arc<Self>,
        request: EmbedRequest,
        timeout: Duration,
    ) -> Result<Vec<f32>, EmbedError> {
        use std::sync::mpsc;
        let path = match &request {
            EmbedRequest::EmbedImage { path, .. } | EmbedRequest::EmbedAudio { path, .. } => path,
            _ => unreachable!(),
        };
        if path.is_empty() {
            return Err(EmbedError::InvalidRequest("empty media path".into()));
        }
        let call_id = self.cancel_seq.fetch_add(1, Ordering::Relaxed);
        self.begin_bounded();
        let sup = std::sync::Arc::clone(self);
        let cancel = std::sync::Arc::new(AtomicBool::new(false));
        let cancel2 = std::sync::Arc::clone(&cancel);
        let deadline = Instant::now() + timeout;
        let (tx, rx) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("winsearch-embed-media".into())
            .spawn(move || {
                let r = sup.embed_media_cancelable(call_id, request, &cancel2, deadline);
                let _ = tx.send(r);
            });
        if let Err(e) = spawned {
            self.finish_bounded(false);
            return Err(EmbedError::Transport(format!(
                "cannot spawn media embed helper: {e}"
            )));
        }
        let outcome = rx.recv_timeout(timeout);
        match outcome {
            Ok(r) => {
                self.finish_bounded(false);
                r
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                cancel.store(true, Ordering::Relaxed);
                self.cancel_inflight_read_for(call_id);
                self.finish_bounded(true);
                warn!("media embed job timed out after {timeout:?}; helper cancelled");
                Err(EmbedError::Transport(format!(
                    "media embed job timed out after {timeout:?}"
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.finish_bounded(false);
                Err(EmbedError::Transport(
                    "media embed helper died before answering".into(),
                ))
            }
        }
    }

    fn embed_media_cancelable(
        &self,
        call_id: u64,
        request: EmbedRequest,
        cancel: &AtomicBool,
        deadline: Instant,
    ) -> Result<Vec<f32>, EmbedError> {
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            return Err(EmbedError::Transport("media embed job cancelled".into()));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let make_request = |id| {
            let mut req = request.clone();
            match &mut req {
                EmbedRequest::EmbedImage { id: request_id, .. }
                | EmbedRequest::EmbedAudio { id: request_id, .. } => *request_id = id,
                _ => unreachable!(),
            }
            req
        };
        let req = make_request(id);
        let extract_first = |vecs: Vec<Vec<f32>>| -> Result<Vec<f32>, EmbedError> {
            if vecs.len() != 1 {
                return Err(EmbedError::Protocol(
                    "media embedding returned wrong vector count".into(),
                ));
            }
            Ok(vecs.into_iter().next().unwrap())
        };
        let mut used_gen = self.worker_gen.load(Ordering::Acquire);
        match self.transact_bounded(call_id, req, cancel, deadline, &mut used_gen) {
            Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                extract_first(vecs)
            }
            Ok(other) => Err(EmbedError::Protocol(format!(
                "unexpected reply to media request: {}",
                response_kind(&other)
            ))),
            Err(EmbedError::Transport(_)) => {
                self.stats.lock().unwrap_or_else(|p| p.into_inner()).retries += 1;
                info!("media embed job interrupted; restarting worker and retrying once");
                let retry_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let retry = make_request(retry_id);
                match self
                    .transact_bounded_after_restart(call_id, retry, cancel, deadline, used_gen)
                {
                    Ok(EmbedResponse::Embeddings { vecs, .. }) => {
                        self.stats.lock().unwrap_or_else(|p| p.into_inner()).jobs += 1;
                        extract_first(vecs)
                    }
                    Ok(other) => Err(EmbedError::Protocol(format!(
                        "unexpected reply to retried media request: {}",
                        response_kind(&other)
                    ))),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Health ping: handshake + `status` round-trip, spawning the worker if
    /// needed. Refreshes the cached `model_loaded` flags.
    pub fn ping(&self) -> Result<WorkerSnapshot, EmbedError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        match self.transact(EmbedRequest::Status { id }) {
            Ok(EmbedResponse::WorkerStatus {
                model_loaded,
                backend,
                stub,
                dim,
                uptime_secs,
                jobs_served,
                ..
            }) => {
                self.model_loaded.store(model_loaded, Ordering::Relaxed);
                self.stub.store(stub, Ordering::Relaxed);
                Ok(WorkerSnapshot {
                    model_loaded,
                    backend,
                    stub,
                    dim,
                    uptime_secs,
                    jobs_served,
                })
            }
            Ok(other) => Err(EmbedError::Protocol(format!(
                "unexpected reply to status: {}",
                response_kind(&other)
            ))),
            Err(e) => Err(e),
        }
    }

    /// One attempt: ensure a worker, handshake, send, receive. Holds the
    /// connection mutex for the whole request (single client); the child
    /// slot is only taken briefly, so `terminate_worker` can kill mid-job.
    /// Transport failures leave the dead child in place for the caller to
    /// restart.
    fn transact(&self, req: EmbedRequest) -> Result<EmbedResponse, EmbedError> {
        let _conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        {
            let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
            self.ensure_worker_locked(&mut slot)?;
        }
        match self.round_trip(&req) {
            Ok(resp) => {
                self.failures.store(0, Ordering::Relaxed);
                Ok(resp)
            }
            Err(e) => {
                if matches!(e, EmbedError::Transport(_)) {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// Restart path: drop the (presumed dead) child and run one attempt.
    /// Used exactly once per job after a transport failure. The backoff wait
    /// lives in `ensure_worker_locked` below, so the retry path sleeps
    /// exactly once (single sleep, `backoff_for` shape).
    fn transact_after_restart(&self, req: EmbedRequest) -> Result<EmbedResponse, EmbedError> {
        let _conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        {
            let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = kill_locked(&mut slot) {
                debug!(error = %e, "restart-path kill of presumed-dead child failed");
            }
        }
        {
            let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
            self.ensure_worker_locked(&mut slot)?;
        }
        match self.round_trip(&req) {
            Ok(resp) => {
                self.failures.store(0, Ordering::Relaxed);
                Ok(resp)
            }
            Err(e) => {
                if matches!(e, EmbedError::Transport(_)) {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// The worker is usable when our child is alive or a foreign worker
    /// answers the pipe (e.g. left idling by a previous daemon). Otherwise
    /// spawn and wait for the pipe.
    fn ensure_worker_locked(&self, slot: &mut Slot) -> Result<(), EmbedError> {
        if let Some(child) = slot.child.as_mut() {
            match child
                .try_wait()
                .map_err(|e| EmbedError::Spawn(e.to_string()))?
            {
                None => return Ok(()),
                Some(status) => {
                    info!(%status, "embed worker exited; will respawn on demand");
                    slot.child = None;
                }
            }
        }
        if probe(&self.config.pipe_name) {
            // Someone else's worker is serving; use it without owning it.
            return Ok(());
        }
        let failures = self.failures.load(Ordering::Relaxed);
        if failures > 0 {
            // The single backoff sleep on every (re)spawn path: `transact`
            // counts the failure, the retry path lands here exactly once.
            self.backoff_sleep(failures);
        }
        let mut cmd = Command::new(&self.config.worker_bin);
        cmd.args(&self.config.worker_args);
        if !self.config.worker_args.iter().any(|a| a == "--backend") {
            if let Some(ref pref) = *self
                .preferred_backend
                .lock()
                .unwrap_or_else(|p| p.into_inner())
            {
                cmd.arg("--backend").arg(pref);
            }
        }
        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                // The pre-spawn probe may have missed a live worker
                // (instance-turnover race): a patient re-probe distinguishes
                // "worker serving" from "launch broken" before failing.
                if patient_probe(&self.config.pipe_name) {
                    info!("embed pipe serving without a spawnable worker; using it");
                    return Ok(());
                }
                return Err(EmbedError::Spawn(format!(
                    "cannot launch {}: {e}",
                    self.config.worker_bin.display()
                )));
            }
        };
        info!(bin = %self.config.worker_bin.display(), "embed worker spawned");
        slot.child = Some(child);
        self.worker_gen.fetch_add(1, Ordering::Release);
        {
            let mut stats = self.stats.lock().unwrap_or_else(|p| p.into_inner());
            stats.spawns += 1;
            if failures > 0 {
                stats.restarts += 1;
            }
        }
        wait_for_pipe(&self.config.pipe_name)
    }

    /// Connect, handshake (health ping + version check + flag refresh), send
    /// the job, read the reply. Any IO/EOF problem is `Transport` (retryable);
    /// worker `Error` answers are definitive.
    fn round_trip(&self, req: &EmbedRequest) -> Result<EmbedResponse, EmbedError> {
        let mut stream = self.connect_and_send(req)?;
        Self::read_reply(&mut stream)
    }

    /// Phase A of a round trip: ensure a worker, connect, handshake, send.
    /// Callers that hold `conn` across this must release it before the
    /// blocking reply read (see `transact_bounded`).
    fn connect_and_send(&self, req: &EmbedRequest) -> Result<PipeStream, EmbedError> {
        let mut stream = connect_with_retry(&self.config.pipe_name)?;
        let hello = EmbedRequest::Hello {
            protocol_version: EMBED_PROTOCOL_VERSION,
            client: "winsearchd".to_string(),
            client_version: ENGINE_VERSION.to_string(),
        };
        write_message(&mut stream, &hello).map_err(EmbedError::transport)?;
        let handshake: Option<EmbedResponse> =
            read_message(&mut stream).map_err(EmbedError::transport)?;
        match handshake {
            Some(EmbedResponse::HelloAck {
                protocol_version,
                model_loaded,
                stub,
                ..
            }) => {
                if protocol_version != EMBED_PROTOCOL_VERSION {
                    return Err(EmbedError::VersionMismatch(format!(
                        "worker speaks v{protocol_version}, supervisor speaks v{EMBED_PROTOCOL_VERSION}"
                    )));
                }
                self.model_loaded.store(model_loaded, Ordering::Relaxed);
                self.stub.store(stub, Ordering::Relaxed);
            }
            Some(EmbedResponse::Error { code, message, .. }) => {
                return Err(EmbedError::from_worker(code, message));
            }
            Some(other) => {
                return Err(EmbedError::Protocol(format!(
                    "unexpected handshake reply: {}",
                    response_kind(&other)
                )));
            }
            None => {
                return Err(EmbedError::Transport(
                    "worker closed during handshake".into(),
                ))
            }
        }
        write_message(&mut stream, req).map_err(EmbedError::transport)?;
        Ok(stream)
    }

    /// Phase B of a round trip: blocking read of the reply on an already-sent
    /// stream. Never called with `conn` held on the bounded path.
    fn read_reply(stream: &mut PipeStream) -> Result<EmbedResponse, EmbedError> {
        let reply: Option<EmbedResponse> =
            read_message(stream).map_err(EmbedError::transport)?;
        match reply {
            Some(EmbedResponse::Error { code, message, .. }) => {
                Err(EmbedError::from_worker(code, message))
            }
            Some(resp) => Ok(resp),
            None => Err(EmbedError::Transport("worker closed mid-job".into())),
        }
    }

    /// Bounded single attempt: `conn` is held only across ensure + phase A
    /// (connect/handshake/send, all internally bounded); the blocking reply
    /// read runs WITHOUT `conn`, registered for watchdog cancellation across
    /// both Phase A and Phase B, so a hung worker never wedges later batches
    /// behind a leaked holder.
    fn transact_bounded(
        &self,
        call_id: u64,
        req: EmbedRequest,
        cancel: &AtomicBool,
        deadline: Instant,
        used_gen: &mut u64,
    ) -> Result<EmbedResponse, EmbedError> {
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            return Err(EmbedError::Transport("embed job cancelled".into()));
        }
        self.register_inflight_for(call_id);
        let phase_a = {
            let _conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
            if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
                Err(EmbedError::Transport("embed job cancelled".into()))
            } else {
                let ensure = {
                    let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
                    self.ensure_worker_locked(&mut slot)
                };
                *used_gen = self.worker_gen.load(Ordering::Acquire);
                ensure.and_then(|()| self.connect_and_send(&req))
            }
        };
        let mut stream = match phase_a {
            Ok(s) => s,
            Err(e) => {
                self.unregister_inflight(call_id);
                return Err(e);
            }
        };
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            self.unregister_inflight(call_id);
            return Err(EmbedError::Transport("embed job cancelled".into()));
        }
        let out = match Self::read_reply(&mut stream) {
            Ok(resp) => {
                self.failures.store(0, Ordering::Relaxed);
                Ok(resp)
            }
            Err(e) => {
                if matches!(e, EmbedError::Transport(_)) {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        };
        self.unregister_inflight(call_id);
        out
    }

    /// Bounded restart path: kill + ensure + send under `conn`, read outside
    /// it (same leak-free shape as [`Self::transact_bounded`]). Used exactly
    /// once per job after a transport failure. Only kills the child if
    /// `worker_gen` still matches `failed_worker_gen` (preventing a stale
    /// retry from killing a replacement worker another caller already started).
    fn transact_bounded_after_restart(
        &self,
        call_id: u64,
        req: EmbedRequest,
        cancel: &AtomicBool,
        deadline: Instant,
        failed_worker_gen: u64,
    ) -> Result<EmbedResponse, EmbedError> {
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            return Err(EmbedError::Transport("embed job cancelled".into()));
        }
        self.register_inflight_for(call_id);
        let phase_a = {
            let _conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
            if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
                Err(EmbedError::Transport("embed job cancelled".into()))
            } else {
                if self.worker_gen.load(Ordering::Acquire) == failed_worker_gen {
                    let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
                    match kill_locked(&mut slot) {
                        Ok(true) => {
                            self.worker_gen.fetch_add(1, Ordering::Release);
                        }
                        Ok(false) => {}
                        Err(e) => {
                            debug!(error = %e, "bounded-restart kill of presumed-dead child failed");
                        }
                    }
                }
                let ensure = {
                    let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
                    self.ensure_worker_locked(&mut slot)
                };
                ensure.and_then(|()| self.connect_and_send(&req))
            }
        };
        let mut stream = match phase_a {
            Ok(s) => s,
            Err(e) => {
                self.unregister_inflight(call_id);
                return Err(e);
            }
        };
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            self.unregister_inflight(call_id);
            return Err(EmbedError::Transport("embed job cancelled".into()));
        }
        let out = match Self::read_reply(&mut stream) {
            Ok(resp) => {
                self.failures.store(0, Ordering::Relaxed);
                Ok(resp)
            }
            Err(e) => {
                if matches!(e, EmbedError::Transport(_)) {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        };
        self.unregister_inflight(call_id);
        out
    }

    /// Registers the calling thread for watchdog cancellation of its in-flight
    /// synchronous pipe IO under `call_id`.
    fn register_inflight_for(&self, call_id: u64) {
        let tid = unsafe { GetCurrentThreadId() };
        self.inflight_tids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(call_id, tid);
    }

    /// Clears `call_id`'s cancel registration (normal completion path).
    fn unregister_inflight(&self, call_id: u64) {
        self.inflight_tids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&call_id);
    }

    /// Watchdog: cancel the registered helper thread's synchronous pipe IO for
    /// `call_id`. Removes only `call_id`'s entry so other concurrent bounded
    /// callers are unaffected and a recycled thread id is never cancelled.
    fn cancel_inflight_read_for(&self, call_id: u64) {
        let tid = self
            .inflight_tids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&call_id);
        let Some(tid) = tid else {
            debug!(call_id, "watchdog: no in-flight read registered; helper already settled");
            return;
        };
        unsafe {
            match OpenThread(THREAD_ALL_ACCESS, false, tid) {
                Ok(handle) => {
                    if let Err(e) = CancelSynchronousIo(handle) {
                        warn!(tid, call_id, error = %e, "watchdog: CancelSynchronousIo failed");
                    } else {
                        info!(tid, call_id, "watchdog: cancelled hung pipe read");
                    }
                    if let Err(e) = windows::Win32::Foundation::CloseHandle(handle) {
                        warn!(tid, call_id, error = %e, "watchdog: CloseHandle(thread) failed");
                    }
                }
                Err(e) => {
                    debug!(tid, call_id, error = %e, "watchdog: OpenThread failed, helper gone");
                }
            }
        }
    }
}

impl Default for EmbedSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

fn response_kind(resp: &EmbedResponse) -> &'static str {
    match resp {
        EmbedResponse::HelloAck { .. } => "hello_ack",
        EmbedResponse::Embeddings { .. } => "embeddings",
        EmbedResponse::WorkerStatus { .. } => "worker_status",
        EmbedResponse::Error { .. } => "error",
        EmbedResponse::ShutdownAck { .. } => "shutdown_ack",
    }
}

/// Instant readiness check: one connect attempt, immediately dropped. Note
/// this consumes a pipe instance — never assert on two back-to-back probes
/// (the successor needs a scheduling quantum); `connect_with_retry` and the
/// spaced polls below are the turnover-safe primitives.
fn probe(pipe_name: &str) -> bool {
    PipeStream::connect(pipe_name).is_ok()
}

/// Spaced re-poll after a failed instant probe: distinguishes a live worker
/// in instance turnover from a truly absent pipe. Only used on paths where
/// spawning already failed, so the success path never pays for it.
fn patient_probe(pipe_name: &str) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if probe(pipe_name) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Connect with bounded retries (see `CONNECT_RETRY`). The first attempt
/// usually succeeds; retries cover the just-spawned / instance-turnover
/// windows without ever blocking the caller indefinitely.
fn connect_with_retry(pipe_name: &str) -> Result<PipeStream, EmbedError> {
    let start = Instant::now();
    loop {
        match PipeStream::connect(pipe_name) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                if start.elapsed() >= CONNECT_RETRY {
                    return Err(EmbedError::transport(e));
                }
                std::thread::sleep(CONNECT_POLL);
            }
        }
    }
}

fn wait_for_pipe(pipe_name: &str) -> Result<(), EmbedError> {
    // Spaced polls: each success consumes an instance, so back-to-back
    // probes would chase turnover; 100 ms spacing lets the server listen
    // again. The final gap is covered by `connect_with_retry`.
    let start = Instant::now();
    while start.elapsed() < SPAWN_WAIT {
        if probe(pipe_name) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // The child may still come up (slow model dir, cold DLLs); a later
    // request retries. Report honestly rather than hanging the caller.
    warn!("embed worker pipe did not appear within {SPAWN_WAIT:?}");
    Err(EmbedError::Spawn(format!(
        "worker pipe {pipe_name} did not appear within {SPAWN_WAIT:?}"
    )))
}

/// `base * 2^(failures-1)` capped at 5 s. Pure for tests.
fn backoff_for(failures: u32, base: Duration) -> Duration {
    if failures == 0 {
        return Duration::ZERO;
    }
    let shift = failures.saturating_sub(1).min(10);
    let scaled = base.saturating_mul(1 << shift);
    scaled.min(MAX_BACKOFF)
}

impl EmbedSupervisor {
    /// Waits out the backoff for `failures` consecutive transport failures
    /// (`backoff_for` shape). Honors the test `sleeper` hook when set.
    fn backoff_sleep(&self, failures: u32) {
        let d = backoff_for(failures, self.config.backoff_base);
        if d.is_zero() {
            return;
        }
        match &self.config.sleeper {
            Some(sleeper) => sleeper(d),
            None => std::thread::sleep(d),
        }
    }
}

/// Kills the owned child (if any) and forgets it. Returns `Ok(true)` when a
/// child was reaped, `Ok(false)` when there was nothing to kill. Kill/wait
/// failures are returned (never swallowed) so watchdogs can log them.
fn kill_locked(slot: &mut Slot) -> std::io::Result<bool> {
    if let Some(mut child) = slot.child.take() {
        child.kill()?;
        child.wait()?;
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    /// Scripted in-process fake worker: answers handshakes, then behaves per
    /// `mode` so retry/backoff paths are deterministic without a real binary.
    struct FakeWorker {
        pipe_name: String,
        jobs_seen: Arc<Mutex<Vec<usize>>>,
        _thread: std::thread::JoinHandle<()>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mode {
        /// First job connection dies after the handshake (crash mid-job);
        /// later ones serve stub vectors.
        DropFirstJob,
        /// Every job is answered with `model_not_available` (definitive).
        AlwaysUnavailable,
        /// Serve stub vectors immediately.
        Serve,
        /// Handshake then never answer the job (hung worker): the client
        /// blocks on the reply until the pipeline watchdog fires.
        Hang,
        /// First job connection hangs (watchdog must fire and free `conn`
        /// for the next batch); later ones serve stub vectors immediately.
        HangFirst,
    }

    #[test]
    fn task2_supervisor_model_recovery_embeds_persisted_exact_without_reparse() {
        use crate::extract::ExtractorRegistry;
        use crate::indexer::Indexer;
        use ws_index::{IndexStore, SearchRequest};
        struct CountParser(Arc<AtomicUsize>);
        impl ws_extract::Extractor for CountParser {
            fn extract(
                &self,
                path: &std::path::Path,
                budget: &ws_extract::ExtractBudget,
            ) -> Result<ws_extract::ExtractOutput, ws_extract::ExtractError> {
                self.0.fetch_add(1, AtomicOrdering::SeqCst);
                ws_extract::DocumentExtractor.extract(path, budget)
            }
        }
        let dir = std::env::temp_dir().join(format!(
            "winsearch-task2-model-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("invoice.txt"),
            "Header\nWINSEARCH_INVOICE_42 deferred genuine stage",
        )
        .unwrap();
        let indexer = Indexer::open(IndexStore::open_in_memory().unwrap(), &[dir.clone()]).unwrap();
        indexer.enqueue_initial_scan().unwrap();
        while indexer.scan_available(64) > 0 {}
        let parses = Arc::new(AtomicUsize::new(0));
        let mut registry = ExtractorRegistry::default();
        registry.register("doc", Arc::new(CountParser(parses.clone())));
        let unavailable = FakeWorker::start(Mode::AlwaysUnavailable);
        let sup = Arc::new(test_supervisor(unavailable.pipe_name.clone()));
        for _ in 0..5 {
            indexer
                .extract_once_with_supervisor(&registry, &sup)
                .unwrap();
        }
        let hits = indexer
            .search(&SearchRequest {
                text: "WINSEARCH_INVOICE_42".into(),
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        let id = hits[0].entry_id;
        let chunks = indexer.store_lock().list_content_chunks(id).unwrap();
        assert_eq!(chunks[0].attempts, 0);
        assert!(indexer
            .store_lock()
            .list_segments_for_entry(id)
            .unwrap()
            .is_empty());
        assert_eq!(parses.load(AtomicOrdering::SeqCst), 1);
        let available = FakeWorker::start(Mode::Serve);
        let sup = Arc::new(test_supervisor(available.pipe_name.clone()));
        let outcome = indexer
            .extract_once_with_supervisor(&registry, &sup)
            .unwrap();
        assert_eq!(outcome.segments_stored, 1);
        assert_eq!(parses.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            indexer.store_lock().list_content_chunks(id).unwrap()[0].id,
            chunks[0].id
        );
        assert_eq!(
            indexer
                .store_lock()
                .list_segments_for_entry(id)
                .unwrap()
                .len(),
            1
        );
        // An incompatible space preserves exact rows and schedules fresh vectors.
        indexer.purge_model_vectors_and_requeue().unwrap();
        assert!(indexer
            .store_lock()
            .list_segments_for_entry(id)
            .unwrap()
            .is_empty());
        assert_eq!(
            indexer
                .search(&SearchRequest {
                    text: "WINSEARCH_INVOICE_42".into(),
                    limit: 1,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
        indexer
            .extract_once_with_supervisor(&registry, &sup)
            .unwrap();
        assert_eq!(parses.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            indexer
                .store_lock()
                .list_segments_for_entry(id)
                .unwrap()
                .len(),
            1
        );
        drop(indexer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    static PIPE_SEQ: AtomicUsize = AtomicUsize::new(0);

    impl FakeWorker {
        fn start(mode: Mode) -> Self {
            let n = PIPE_SEQ.fetch_add(1, AtomicOrdering::Relaxed);
            let pipe_name = format!(r"\\.\pipe\winsearch-embed-fake-{}-{n}", std::process::id());
            let jobs_seen = Arc::new(Mutex::new(Vec::new()));
            let jobs2 = Arc::clone(&jobs_seen);
            let name2 = pipe_name.clone();
            let dropped = Arc::new(AtomicBool::new(false));
            let dropped2 = Arc::clone(&dropped);
            let hung = Arc::new(AtomicBool::new(false));
            let hung2 = Arc::clone(&hung);
            let thread = std::thread::Builder::new()
                .name("fake-embed-worker".into())
                .spawn(move || {
                    if let Err(e) = ws_platform::pipe::serve(&name2, move |mut stream| {
                        let hello: Option<EmbedRequest> = read_message(&mut stream).unwrap_or(None);
                        if !matches!(hello, Some(EmbedRequest::Hello { .. })) {
                            return;
                        }
                        write_message(
                            &mut stream,
                            &EmbedResponse::HelloAck {
                                protocol_version: EMBED_PROTOCOL_VERSION,
                                worker_version: "fake".into(),
                                features: vec!["embed_texts".into()],
                                model_loaded: false,
                                stub: true,
                            },
                        )
                        .ok();
                        let req: Option<EmbedRequest> = read_message(&mut stream).unwrap_or(None);
                        match req {
                            Some(EmbedRequest::Status { id }) => {
                                write_message(
                                    &mut stream,
                                    &EmbedResponse::WorkerStatus {
                                        id,
                                        model_loaded: false,
                                        backend: Some("stub".into()),
                                        stub: true,
                                        dim: ws_embed_protocol::STUB_DIM,
                                        uptime_secs: 7,
                                        jobs_served: jobs2.lock().unwrap().len() as u64,
                                    },
                                )
                                .ok();
                            }
                            Some(EmbedRequest::EmbedTexts { id, texts }) => {
                                jobs2.lock().unwrap().push(texts.len());
                                match mode {
                                    Mode::Hang => {
                                        // Hung worker: sleep past any test
                                        // watchdog, then close without
                                        // answering (client must time out).
                                        let _ = id;
                                        std::thread::sleep(Duration::from_secs(30));
                                    }
                                    Mode::HangFirst
                                        if !hung2.swap(true, AtomicOrdering::SeqCst) =>
                                    {
                                        // First job hangs past the watchdog;
                                        // later jobs serve immediately, so a
                                        // second bounded batch must proceed
                                        // while the first is still stuck.
                                        let _ = id;
                                        std::thread::sleep(Duration::from_secs(30));
                                    }
                                    Mode::AlwaysUnavailable => {
                                        write_message(
                                            &mut stream,
                                            &EmbedResponse::Error {
                                                id: Some(id),
                                                code: EmbedErrorCode::ModelNotAvailable,
                                                message: "no model".into(),
                                            },
                                        )
                                        .ok();
                                    }
                                    Mode::DropFirstJob
                                        if !dropped2.swap(true, AtomicOrdering::SeqCst) =>
                                    {
                                        // Crash: close without answering.
                                    }
                                    _ => {
                                        write_message(
                                            &mut stream,
                                            &EmbedResponse::Embeddings {
                                                id,
                                                vecs: ws_embed_protocol::stub_embed_texts(
                                                    &texts,
                                                    ws_embed_protocol::STUB_DIM,
                                                ),
                                                dim: ws_embed_protocol::STUB_DIM,
                                                backend: "stub".into(),
                                                stub: true,
                                            },
                                        )
                                        .ok();
                                    }
                                }
                            }
                            // Anything else on a test double: close quietly.
                            _ => {}
                        }
                    }) {
                        eprintln!("fake serve exited: {e}");
                    }
                })
                .unwrap();
            // Wait until the accept loop listens. Spaced connect-polls (see
            // `probe`): the first success may consume the only instance, so
            // settle, then re-poll rather than asserting back-to-back.
            let start = Instant::now();
            let mut up = false;
            while start.elapsed() < Duration::from_secs(60) {
                if probe(&pipe_name) {
                    up = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            assert!(up, "fake worker pipe never appeared (first sighting)");
            std::thread::sleep(Duration::from_secs(3));
            let start = Instant::now();
            let mut steady = false;
            while start.elapsed() < Duration::from_secs(30) {
                if probe(&pipe_name) {
                    steady = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            assert!(steady, "fake worker pipe never settled");
            Self {
                pipe_name,
                jobs_seen,
                _thread: thread,
            }
        }
    }

    fn test_supervisor(pipe_name: String) -> EmbedSupervisor {
        test_supervisor_with_sleeper(pipe_name, Duration::from_millis(1), None)
    }

    fn test_supervisor_with_sleeper(
        pipe_name: String,
        backoff_base: Duration,
        sleeper: Option<std::sync::Arc<dyn Fn(Duration) + Send + Sync>>,
    ) -> EmbedSupervisor {
        EmbedSupervisor::with_config(EmbedConfig {
            // Never spawned in these tests: the fake is already serving, so
            // `ensure` takes the probe path.
            worker_bin: PathBuf::from(r"C:\definitely-not-a-worker.exe"),
            pipe_name,
            worker_args: Vec::new(),
            backoff_base,
            sleeper,
        })
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let base = Duration::from_millis(250);
        assert_eq!(backoff_for(0, base), Duration::ZERO);
        assert_eq!(backoff_for(1, base), Duration::from_millis(250));
        assert_eq!(backoff_for(2, base), Duration::from_millis(500));
        assert_eq!(backoff_for(3, base), Duration::from_secs(1));
        assert_eq!(backoff_for(99, base), MAX_BACKOFF);
    }

    #[test]
    fn live_worker_reuse_never_sleeps() {
        // Retrying against an already-serving worker costs no backoff: the
        // single sleep lives on the spawn path (see the respawn test below).
        // The recorder proves the fast path waits never.
        let fake = FakeWorker::start(Mode::DropFirstJob);
        let sleeps = std::sync::Arc::new(Mutex::new(Vec::new()));
        let sleeps2 = std::sync::Arc::clone(&sleeps);
        let sup = test_supervisor_with_sleeper(
            fake.pipe_name.clone(),
            Duration::from_millis(250),
            Some(std::sync::Arc::new(move |d: Duration| {
                sleeps2.lock().unwrap().push(d);
            })),
        );

        let vecs = sup.embed_texts(&["retry me".to_string()]).expect("retry succeeds");
        assert_eq!(vecs.len(), 1);
        assert_eq!(sup.stats().retries, 1);
        assert!(
            sleeps.lock().unwrap().is_empty(),
            "live-worker retry must not back off"
        );
    }

    #[test]
    fn crash_mid_job_restarts_and_retries_once() {
        let fake = FakeWorker::start(Mode::DropFirstJob);
        let sup = test_supervisor(fake.pipe_name.clone());

        let texts = vec!["alpha".to_string(), "beta".to_string()];
        let vecs = sup.embed_texts(&texts).expect("retry must succeed");
        assert_eq!(vecs.len(), 2);
        assert_eq!(
            vecs[0],
            ws_embed_protocol::stub_vector("alpha", ws_embed_protocol::STUB_DIM)
        );

        let stats = sup.stats();
        assert_eq!(
            stats.retries, 1,
            "one interrupted job retried, got {stats:?}"
        );
        assert_eq!(stats.spawns, 0, "fake was already serving; nothing spawned");
        // Both attempts reached a worker (first died, second served).
        assert_eq!(*fake.jobs_seen.lock().unwrap(), vec![2, 2]);
        // The handshake refreshed the cache: stub counts as servable.
        assert!(sup.model_loaded());
    }

    #[test]
    fn definitive_worker_errors_are_not_retried() {
        let fake = FakeWorker::start(Mode::AlwaysUnavailable);
        let sup = test_supervisor(fake.pipe_name.clone());

        let err = sup.embed_texts(&["x".to_string()]).unwrap_err();
        assert!(
            matches!(err, EmbedError::ModelNotAvailable(_)),
            "got {err:?}"
        );
        assert_eq!(sup.stats().retries, 0);
        assert_eq!(fake.jobs_seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn timeout_cleanup_preserves_newer_worker_or_bounded_caller() {
        for newer_caller in [false, true] {
            let sup = Arc::new(test_supervisor("unused-cleanup-pipe".into()));
            sup.begin_bounded();
            sup.finish_bounded(false);
            let epoch = sup.bounded_calls.lock().unwrap().epoch;
            let worker_gen = sup.worker_gen.load(Ordering::Acquire);
            let child = Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 30"])
                .spawn().unwrap();
            sup.slot.lock().unwrap().child = Some(child);
            if newer_caller {
                sup.begin_bounded();
                sup.finish_bounded(false);
            } else {
                sup.worker_gen.fetch_add(1, Ordering::Release);
            }
            sup.retire_timed_out_worker(epoch, worker_gen);
            let preserved = sup.slot.lock().unwrap().child.as_mut()
                .is_some_and(|child| child.try_wait().unwrap().is_none());
            sup.terminate_worker();
            assert!(preserved, "stale timeout cleanup killed a newer worker/caller");
        }
    }

    #[test]
    fn bounded_calls_return_by_deadline_while_worker_slot_is_held() {
        for image in [false, true] {
            let sup = Arc::new(test_supervisor("unused-deadline-pipe".into()));
            let slot = sup.slot.lock().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let other = Arc::clone(&sup);
            let caller = std::thread::spawn(move || {
                let result = if image {
                    other.embed_image_bounded(std::path::Path::new("fixture.png"), Duration::from_millis(50))
                } else {
                    other.embed_texts_bounded(&["fixture".into()], Duration::from_millis(50))
                        .map(|_| Vec::new())
                };
                tx.send(result.is_err()).unwrap();
            });
            let outcome = rx.recv_timeout(Duration::from_secs(1));
            drop(slot);
            caller.join().unwrap();
            assert_eq!(outcome.unwrap(), true, "caller outlived its deadline waiting for slot");
        }
    }

    #[test]
    fn bounded_accounting_never_waits_for_worker_startup_slot() {
        use std::sync::mpsc;
        let sup = Arc::new(test_supervisor("unused-test-pipe".into()));
        let slot = sup.slot.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        let other = Arc::clone(&sup);
        let helper = std::thread::spawn(move || {
            other.begin_bounded();
            other.finish_bounded(false);
            other.begin_bounded();
            other.finish_bounded(true);
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(1));
        drop(slot);
        helper.join().unwrap();
        assert!(result.is_ok(), "bounded accounting waited on the worker slot");
    }

    #[test]
    fn bounded_call_times_out_on_hanging_worker() {
        // Task2-I1: a worker that handshakes but never answers must fail the
        // bounded call instead of stalling the caller forever. The fake hangs
        // 30 s; the watchdog fires in 500 ms.
        let fake = FakeWorker::start(Mode::Hang);
        let sup = Arc::new(test_supervisor(fake.pipe_name.clone()));

        let start = Instant::now();
        let err = sup
            .embed_texts_bounded(&["never answered".to_string()], Duration::from_millis(500))
            .unwrap_err();
        let elapsed = start.elapsed();
        assert!(
            matches!(err, EmbedError::Transport(ref msg) if msg.contains("timed out")),
            "got {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(15),
            "bounded call stalled the caller for {elapsed:?}"
        );
        // The job reached the worker (it is hanging on it); the watchdog, not
        // a successful reply, ended the call.
        assert_eq!(fake.jobs_seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn bounded_second_batch_proceeds_while_first_hangs() {
        // Fix round 1/5 (I1): the timed-out helper must not hold `conn`
        // across its blocked read — a second bounded batch starts and
        // completes while the first is still stuck, and the watchdog cancels
        // the first helper's IO (no leaked holder wedges the pipeline).
        let fake = FakeWorker::start(Mode::HangFirst);
        let sup = Arc::new(test_supervisor(fake.pipe_name.clone()));

        let sup2 = Arc::clone(&sup);
        let first = std::thread::Builder::new()
            .name("test-first-hung-batch".into())
            .spawn(move || {
                sup2.embed_texts_bounded(
                    &["hung batch".to_string()],
                    Duration::from_millis(800),
                )
            })
            .unwrap();
        // Let the first batch get through its handshake+send and block in the
        // reply read (phase A is ms; the hang is 30 s).
        std::thread::sleep(Duration::from_millis(1500));

        let start = Instant::now();
        let vecs = sup
            .embed_texts_bounded(&["second batch".to_string()], Duration::from_secs(20))
            .expect("second batch must proceed while the first hangs");
        let elapsed = start.elapsed();
        assert_eq!(vecs.len(), 1);
        assert_eq!(
            vecs[0],
            ws_embed_protocol::stub_vector("second batch", ws_embed_protocol::STUB_DIM)
        );
        assert!(
            elapsed < Duration::from_secs(20),
            "second batch stalled behind the hung first batch for {elapsed:?}"
        );

        let err = first.join().expect("helper thread panicked").unwrap_err();
        assert!(
            matches!(err, EmbedError::Transport(ref msg) if msg.contains("timed out")),
            "first batch must fail via the watchdog, got {err:?}"
        );
        assert_eq!(fake.jobs_seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn large_batches_split_into_capped_jobs() {
        let fake = FakeWorker::start(Mode::Serve);
        let sup = test_supervisor(fake.pipe_name.clone());

        let texts: Vec<String> = (0..(MAX_TEXTS_PER_JOB + 44))
            .map(|i| format!("t{i}"))
            .collect();
        let vecs = sup.embed_texts(&texts).expect("chunked embed");
        assert_eq!(vecs.len(), texts.len());
        assert_eq!(
            *fake.jobs_seen.lock().unwrap(),
            vec![MAX_TEXTS_PER_JOB, 44],
            "one capped job plus the remainder"
        );
    }

    #[test]
    fn empty_batch_is_rejected_before_touching_the_worker() {
        let fake = FakeWorker::start(Mode::Serve);
        let sup = test_supervisor(fake.pipe_name.clone());
        assert!(matches!(
            sup.embed_texts(&[]),
            Err(EmbedError::InvalidRequest(_))
        ));
        assert!(fake.jobs_seen.lock().unwrap().is_empty());
    }

    #[test]
    fn ping_reports_worker_snapshot() {
        let fake = FakeWorker::start(Mode::Serve);
        let sup = test_supervisor(fake.pipe_name);
        let snap = sup.ping().expect("ping");
        assert!(snap.stub);
        assert!(!snap.model_loaded, "fake serves no real weights");
        assert_eq!(snap.backend.as_deref(), Some("stub"));
        assert_eq!(snap.dim, ws_embed_protocol::STUB_DIM);
        assert!(snap.uptime_secs <= 120);
        assert!(sup.model_loaded());
    }

    #[test]
    fn config_env_fallbacks() {
        // No WS_EMBED_* set by these tests (explicit config everywhere), so
        // the defaults resolve to the shipped layout.
        let cfg = EmbedConfig::from_env_or_default();
        assert!(cfg.pipe_name.starts_with(r"\\.\pipe\winsearch-embed-"));
        assert_eq!(
            cfg.worker_bin.file_name().unwrap().to_string_lossy(),
            format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX)
        );
    }

    /// Real-binary tests: drive the actual `winsearch-embed` process on a
    /// unique pipe with the deterministic stub path (no model file needed).
    mod real_binary {
        use super::*;
        use std::sync::OnceLock;

        static SEQ: AtomicUsize = AtomicUsize::new(0);

        /// The worker takes a machine-global single-instance mutex, so only
        /// one real-binary test may run at a time (they would otherwise kill
        /// each other's workers at startup).
        static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

        fn serial() -> std::sync::MutexGuard<'static, ()> {
            SERIAL
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|p| p.into_inner())
        }

        fn worker_bin() -> PathBuf {
            // Test executables live in target/<profile>/deps/; the worker
            // binary is built next to it by any workspace build.
            let mut p = std::env::current_exe().expect("current exe");
            p.pop(); // deps
            p.pop(); // <profile>
            let bin = p.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
            assert!(
                bin.is_file(),
                "worker binary missing at {} — run a workspace build first (`cargo build --workspace`)",
                bin.display()
            );
            bin
        }

        fn real_supervisor(extra_args: &[&str]) -> EmbedSupervisor {
            real_supervisor_traced(extra_args).0
        }

        /// Real supervisor plus a recorder for its backoff waits (no real
        /// waiting when the worker binary is exercised).
        fn real_supervisor_traced(
            extra_args: &[&str],
        ) -> (EmbedSupervisor, std::sync::Arc<Mutex<Vec<Duration>>>) {
            let n = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
            let pipe = format!(r"\\.\pipe\winsearch-embed-int-{}-{n}", std::process::id());
            let mut args = vec!["--pipe".to_string(), pipe.clone()];
            args.extend(extra_args.iter().map(|s| s.to_string()));
            let sleeps = std::sync::Arc::new(Mutex::new(Vec::new()));
            let sleeps2 = std::sync::Arc::clone(&sleeps);
            let sup = EmbedSupervisor::with_config(EmbedConfig {
                worker_bin: worker_bin(),
                pipe_name: pipe,
                worker_args: args,
                backoff_base: Duration::from_millis(10),
                sleeper: Some(std::sync::Arc::new(move |d: Duration| {
                    sleeps2.lock().unwrap().push(d);
                })),
            });
            (sup, sleeps)
        }

        fn unit_norm(v: &[f32]) -> f64 {
            v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt()
        }

        fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
            let start = Instant::now();
            while !cond() {
                if start.elapsed() > timeout {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            true
        }

        #[test]
        fn real_worker_round_trips_a_text_batch() {
            let _serial = serial();
            let sup = std::sync::Arc::new(real_supervisor(&["--stub", "--idle-secs", "60"]));
            assert!(!sup.is_worker_running(), "nothing spawned yet (lazy)");

            let texts = vec![
                "budget spreadsheet".to_string(),
                "receipt from the hardware store".to_string(),
                "quarterly review notes".to_string(),
            ];
            let vecs = sup.embed_texts(&texts).expect("stub round-trip");
            assert!(sup.is_worker_running(), "first request spawned the worker");
            assert_eq!(vecs.len(), 3);
            for v in &vecs {
                assert_eq!(v.len(), ws_embed_protocol::STUB_DIM);
                assert!((unit_norm(v) - 1.0).abs() < 1e-5, "stub not unit-norm");
            }
            // Deterministic: same batch, same vectors.
            let again = sup.embed_texts(&texts).expect("second batch");
            assert_eq!(vecs, again);
            assert!(sup.model_loaded(), "stub servable counts as loaded");

            let snap = sup.ping().expect("health ping");
            assert!(snap.stub);
            assert!(snap.jobs_served >= 2);
            let image = sup.embed_image_with_budget_bounded(std::path::Path::new(r"C:\fixture.png"), 70, Duration::from_secs(5)).unwrap();
            let audio = sup.embed_audio_bounded(std::path::Path::new(r"C:\fixture.wav"), Some((0,30_000)), Duration::from_secs(5)).unwrap();
            assert_eq!(image.len(), ws_embed_protocol::STUB_DIM);
            assert_eq!(audio.len(), ws_embed_protocol::STUB_DIM);
            assert_ne!(image, audio);
            assert!(sup.embed_image_with_budget_bounded(std::path::Path::new(r"C:\fixture.png"), 71, Duration::from_secs(5)).is_err());
            assert!(sup.embed_audio_bounded(std::path::Path::new(r"C:\fixture.wav"), Some((0,30_001)), Duration::from_secs(5)).is_err());
            sup.terminate_worker();
        }

        #[test]
        fn killing_the_worker_mid_job_restarts_and_retries() {
            let _serial = serial();
            let (sup, sleeps) =
                real_supervisor_traced(&["--stub", "--idle-secs", "60", "--stub-delay-ms", "10"]);
            // ~3 s of stub work: the kill below lands mid-job even on a
            // heavily loaded machine.
            let texts: Vec<String> = (0..300).map(|i| format!("mid-job item {i}")).collect();

            let worker = std::thread::scope(|s| {
                let handle = s.spawn(|| sup.embed_texts(&texts));
                // Wait for the spawn, then kill well inside the ~1 s stub job.
                assert!(
                    wait_until(Duration::from_secs(15), || sup.is_worker_running()),
                    "worker never started"
                );
                std::thread::sleep(Duration::from_millis(300));
                sup.terminate_worker();
                handle.join().expect("embed thread panicked")
            });
            let vecs = worker.expect("job retried after restart must succeed");
            assert_eq!(vecs.len(), texts.len());
            let stats = sup.stats();
            assert!(stats.retries >= 1, "expected a retry, got {stats:?}");
            assert!(stats.restarts >= 1, "expected a restart, got {stats:?}");
            // Single backoff on the respawn path (I2): exactly one wait of
            // `backoff_for(1, base)` for the whole kill → restart → retry.
            assert_eq!(
                *sleeps.lock().unwrap(),
                vec![backoff_for(1, Duration::from_millis(10))],
                "exactly one backoff sleep with the backoff_for shape"
            );

            // The respawned worker serves follow-up requests.
            let follow = sup
                .embed_texts(&["after restart".to_string()])
                .expect("follow-up");
            assert_eq!(follow.len(), 1);
            sup.terminate_worker();
        }

        #[test]
        fn idle_worker_exits_and_respawns_on_demand() {
            let _serial = serial();
            let sup = real_supervisor(&["--stub", "--idle-secs", "2"]);
            sup.embed_texts(&["warm".to_string()]).expect("warmup job");
            assert!(sup.is_worker_running());

            assert!(
                wait_until(Duration::from_secs(90), || !sup.is_worker_running()),
                "worker did not idle-exit (no lingering process)"
            );
            let spawns_before = sup.stats().spawns;

            // Next request transparently respawns.
            let vecs = sup
                .embed_texts(&["after idle".to_string()])
                .expect("respawn");
            assert_eq!(vecs.len(), 1);
            assert!(sup.stats().spawns > spawns_before);
            sup.terminate_worker();
        }
    }
}
