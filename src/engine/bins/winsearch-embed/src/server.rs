//! Pipe server for the internal embed-worker protocol.
//!
//! Lifetime: load the model lazily on the first job, serve until `shutdown`
//! or the idle timeout, then exit the process — process exit returns all GPU
//! memory to the driver, which is the full GPU release (no `release_gpu`
//! dance needed on the way out). One client (winsearchd) at a time:
//! concurrent connections serialize on a slot mutex.
//!
//! The message handlers ([`dispatch`]) are pure over [`Shared`] state and
//! never exit the process themselves, so unit tests exercise them directly;
//! only [`run`] installs the watchdog that calls `process::exit`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};
use ws_embed_protocol::{
    stub_embed_texts, stub_vector, EmbedErrorCode, EmbedRequest, EmbedResponse,
    EMBED_PROTOCOL_VERSION, MAX_TEXTS_PER_JOB, STUB_DIM,
};
use ws_modelmgr::{MMPROJ_FILE, TEXT_FILE};
use ws_platform::{
    identity, paths,
    pipe::{self, PipeStream},
};
use ws_protocol::{read_message, write_message};

pub use ws_embed::backends::BackendPreference;

/// Idle timeout before the worker exits: 5 minutes.
pub const DEFAULT_IDLE_SECS: u64 = 300;

/// Watchdog poll interval (also bounds idle-exit overshoot).
const WATCHDOG_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub pipe_name: String,
    pub idle_timeout: Duration,
    /// Deterministic fake embeddings; no model file needed.
    pub stub: bool,
    /// Explicit text-GGUF path. `None` = `model_dir()/TEXT_FILE`.
    pub model_path: Option<PathBuf>,
    pub backend: BackendPreference,
    /// Artificial per-text delay in stub mode (widens the kill-mid-job test window).
    pub stub_delay: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_getter(&|k| std::env::var(k).ok())
    }

    /// CLI tokens (`--pipe/--idle-secs/--stub/--model/--backend/
    /// --stub-delay-ms`) override the env-derived config. Minimal hand parser:
    /// no CLI framework in the engine.
    pub fn apply_args(&mut self, raw: &[String]) -> anyhow::Result<()> {
        let mut i = 0;
        while i < raw.len() {
            match raw[i].as_str() {
                "--pipe" => self.pipe_name = take_value(raw, &mut i, "--pipe")?,
                "--idle-secs" => {
                    self.idle_timeout = Duration::from_secs(
                        take_value(raw, &mut i, "--idle-secs")?
                            .parse::<u64>()
                            .map_err(|e| anyhow::anyhow!("--idle-secs: {e}"))?,
                    );
                }
                "--stub" => self.stub = true,
                "--model" => self.model_path = Some(take_value(raw, &mut i, "--model")?.into()),
                "--backend" => {
                    self.backend = take_value(raw, &mut i, "--backend")?
                        .parse()
                        .map_err(|e: String| anyhow::anyhow!("--backend: {e}"))?;
                }
                "--stub-delay-ms" => {
                    self.stub_delay = Duration::from_millis(
                        take_value(raw, &mut i, "--stub-delay-ms")?
                            .parse::<u64>()
                            .map_err(|e| anyhow::anyhow!("--stub-delay-ms: {e}"))?,
                    );
                }
                other => anyhow::bail!("unknown argument: {other}"),
            }
            i += 1;
        }
        Ok(())
    }

    /// Env-backed constructor behind a getter so tests inject values without
    /// touching process-global state.
    fn from_getter(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let pipe_name = match get("WS_EMBED_PIPE") {
            Some(p) if !p.is_empty() => p,
            _ => identity::embed_pipe_name().map_err(|e| anyhow::anyhow!("{e}"))?,
        };
        let idle_secs = match get("WS_EMBED_IDLE_SECS") {
            Some(v) if !v.is_empty() => v
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("WS_EMBED_IDLE_SECS: {e}"))?,
            _ => DEFAULT_IDLE_SECS,
        };
        let stub = matches!(get("WS_EMBED_STUB").as_deref(), Some("1"));
        let model_path = match get("WS_EMBED_MODEL") {
            Some(p) if !p.is_empty() => Some(PathBuf::from(p)),
            _ => None,
        };
        let backend = match get("WS_EMBED_BACKEND") {
            Some(p) if !p.is_empty() => p
                .parse()
                .map_err(|e: String| anyhow::anyhow!("WS_EMBED_BACKEND: {e}"))?,
            _ => BackendPreference::Auto,
        };
        let stub_delay = match get("WS_EMBED_STUB_DELAY_MS") {
            Some(v) if !v.is_empty() => Duration::from_millis(
                v.parse::<u64>()
                    .map_err(|e| anyhow::anyhow!("WS_EMBED_STUB_DELAY_MS: {e}"))?,
            ),
            _ => Duration::ZERO,
        };
        Ok(Self {
            pipe_name,
            idle_timeout: Duration::from_secs(idle_secs),
            stub,
            model_path,
            backend,
            stub_delay,
        })
    }
}

fn take_value(raw: &[String], i: &mut usize, flag: &str) -> anyhow::Result<String> {
    *i += 1;
    raw.get(*i)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))
}

/// Mutable worker state shared by the connection threads and the watchdog.
pub struct Shared {
    cfg: WorkerConfig,
    activity: Mutex<Activity>,
    engine: Mutex<EngineSlot>,
}

struct Activity {
    last: Instant,
    active_jobs: u32,
    shutdown: bool,
}

struct EngineSlot {
    runtime: Option<ws_embed::EmbedRuntime>,
    backend: Option<String>,
    jobs_served: u64,
    started: Instant,
}

impl Shared {
    fn for_config(cfg: WorkerConfig) -> Self {
        Self {
            cfg,
            activity: Mutex::new(Activity {
                last: Instant::now(),
                active_jobs: 0,
                shutdown: false,
            }),
            engine: Mutex::new(EngineSlot {
                runtime: None,
                backend: None,
                jobs_served: 0,
                started: Instant::now(),
            }),
        }
    }

    fn touch(&self) {
        self.activity.lock().unwrap_or_else(|p| p.into_inner()).last = Instant::now();
    }
}

/// What [`dispatch`] wants the connection loop to do after replying.
enum Action {
    Respond(EmbedResponse),
    /// Reply, then flag shutdown so the watchdog exits the process.
    RespondAndExit(EmbedResponse),
}

/// Runs the pipe server until idle-exit or `shutdown`. Only returns on pipe
/// creation failure; every other exit path terminates the process.
pub fn run(config: &WorkerConfig) -> anyhow::Result<()> {
    let shared = Arc::new(Shared::for_config(config.clone()));
    spawn_watchdog(Arc::clone(&shared), config.idle_timeout);

    // One client at a time: a second connection waits on the slot until the
    // first disconnects (winsearchd is the only client; this is backstop).
    let slot = Arc::new(Mutex::new(()));
    let name = config.pipe_name.clone();
    info!(pipe = %name, "embed worker serving");
    pipe::serve(&name, move |stream| {
        let _held = slot.lock().unwrap_or_else(|p| p.into_inner());
        handle_connection(stream, Arc::clone(&shared));
    })
    .map_err(|e| anyhow::anyhow!("pipe server failed: {e}"))
}

fn spawn_watchdog(shared: Arc<Shared>, idle: Duration) {
    std::thread::Builder::new()
        .name("winsearch-embed-idle".into())
        .spawn(move || loop {
            std::thread::sleep(WATCHDOG_POLL);
            let done = {
                let a = shared.activity.lock().unwrap_or_else(|p| p.into_inner());
                a.shutdown || (a.active_jobs == 0 && a.last.elapsed() >= idle)
            };
            if done {
                // Process exit IS the full GPU release: the driver reclaims
                // every allocation. No per-handle teardown needed (or wanted —
                // a CPU `release_gpu` reload before dying would be pure waste).
                let idle = shared
                    .activity
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .shutdown;
                info!(shutdown = idle, "embed worker exiting");
                std::process::exit(0);
            }
        })
        .expect("spawn idle watchdog");
}

fn handle_connection(mut stream: PipeStream, shared: Arc<Shared>) {
    loop {
        let req: Option<EmbedRequest> = match read_message(&mut stream) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(error = %e, "embed connection read failed");
                break;
            }
        };
        let Some(req) = req else {
            break; // clean disconnect
        };
        shared.touch();
        let (resp, exit) = match dispatch(&req, &shared) {
            Action::Respond(r) => (r, false),
            Action::RespondAndExit(r) => (r, true),
        };
        if write_message(&mut stream, &resp).is_err() {
            break;
        }
        if exit {
            shared
                .activity
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .shutdown = true;
            // Let the watchdog do the exiting so the ack above is flushed.
            std::thread::sleep(WATCHDOG_POLL * 2);
            std::process::exit(0);
        }
    }
}

fn dispatch(req: &EmbedRequest, shared: &Shared) -> Action {
    let reply = |r| Action::Respond(r);
    match req {
        EmbedRequest::Hello {
            protocol_version,
            client,
            client_version,
        } => {
            if *protocol_version != EMBED_PROTOCOL_VERSION {
                return reply(EmbedResponse::Error {
                    id: None,
                    code: EmbedErrorCode::VersionMismatch,
                    message: format!(
                        "worker speaks embed protocol v{EMBED_PROTOCOL_VERSION}, client speaks v{protocol_version}"
                    ),
                });
            }
            info!(client = %client, version = %client_version, "embed client hello");
            reply(hello_ack(shared, EMBED_PROTOCOL_VERSION))
        }
        EmbedRequest::EmbedTexts { id, texts } => {
            if texts.is_empty() || texts.len() > MAX_TEXTS_PER_JOB {
                return reply(EmbedResponse::Error {
                    id: Some(*id),
                    code: EmbedErrorCode::InvalidRequest,
                    message: format!(
                        "batch must hold 1–{MAX_TEXTS_PER_JOB} texts, got {}",
                        texts.len()
                    ),
                });
            }
            job_guard(shared, || embed_texts_job(shared, *id, texts))
        }
        EmbedRequest::EmbedAudio {
            id,
            path,
            window_ms,
        } => job_guard(shared, || embed_audio_job(shared, *id, path, *window_ms)),
        EmbedRequest::EmbedImage {
            id,
            path,
            token_budget,
        } => job_guard(shared, || {
            embed_image_job(shared, *id, path, token_budget.unwrap_or(280))
        }),
        EmbedRequest::Status { id } => reply(status_response(shared, *id)),
        EmbedRequest::Shutdown { id } => {
            Action::RespondAndExit(EmbedResponse::ShutdownAck { id: *id })
        }
    }
}

/// Marks a job active so the watchdog cannot idle-exit mid-job, and refreshes
/// the activity clock when it finishes.
fn job_guard(shared: &Shared, job: impl FnOnce() -> EmbedResponse) -> Action {
    {
        shared
            .activity
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_jobs += 1;
    }
    let resp = job();
    {
        let mut a = shared.activity.lock().unwrap_or_else(|p| p.into_inner());
        a.active_jobs = a.active_jobs.saturating_sub(1);
        a.last = Instant::now();
    }
    Action::Respond(resp)
}

fn hello_ack(shared: &Shared, id_version: u32) -> EmbedResponse {
    let eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    EmbedResponse::HelloAck {
        protocol_version: id_version,
        worker_version: env!("CARGO_PKG_VERSION").to_string(),
        features: features(&eng, shared.cfg.stub),
        model_loaded: eng.runtime.is_some(),
        stub: shared.cfg.stub,
    }
}

fn status_response(shared: &Shared, id: u64) -> EmbedResponse {
    let eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    EmbedResponse::WorkerStatus {
        id,
        model_loaded: eng.runtime.is_some(),
        backend: eng.backend.clone(),
        stub: shared.cfg.stub,
        dim: eng.runtime.as_ref().map_or(STUB_DIM, |r| r.n_embd()),
        uptime_secs: eng.started.elapsed().as_secs(),
        jobs_served: eng.jobs_served,
    }
}

fn features(eng: &std::sync::MutexGuard<'_, EngineSlot>, stub: bool) -> Vec<String> {
    let mut f = vec!["embed_texts".to_string()];
    if stub
        || eng
            .runtime
            .as_ref()
            .is_some_and(|r| r.mtmd_supports_audio())
    {
        f.push("embed_audio".to_string());
    }
    if stub
        || eng
            .runtime
            .as_ref()
            .is_some_and(|r| r.mtmd_supports_vision())
    {
        f.push("embed_image".to_string());
    }
    f
}

fn embed_texts_job(shared: &Shared, id: u64, texts: &[String]) -> EmbedResponse {
    if shared.cfg.stub {
        if !shared.cfg.stub_delay.is_zero() {
            std::thread::sleep(shared.cfg.stub_delay * texts.len() as u32);
        }
        let vecs = stub_embed_texts(texts, STUB_DIM);
        served(shared);
        return EmbedResponse::Embeddings {
            id,
            vecs,
            dim: STUB_DIM,
            backend: "stub".to_string(),
            stub: true,
        };
    }
    let mut eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    // Borrow the runtime only inside this block: the backend label and the
    // served-counter live on the same slot and are touched after it ends.
    let (dim, result) = match ensure_runtime(&mut eng, shared) {
        Ok(rt) => {
            let dim = rt.n_embd();
            (dim, rt.embed_texts(texts).map_err(|e| e.to_string()))
        }
        Err(message) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::ModelNotAvailable,
                message,
            };
        }
    };
    let backend = eng.backend.clone().unwrap_or_else(|| "?".into());
    match result {
        Ok(vecs) => {
            eng.jobs_served += 1;
            EmbedResponse::Embeddings {
                id,
                vecs,
                dim,
                backend,
                stub: false,
            }
        }
        Err(message) => EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::EmbedFailed,
            message,
        },
    }
}

fn embed_image_job(shared: &Shared, id: u64, path_str: &str, token_budget: u32) -> EmbedResponse {
    if !matches!(token_budget, 70 | 140 | 280) {
        return EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::InvalidRequest,
            message: "image token budget must be70,140or280".into(),
        };
    }
    if shared.cfg.stub {
        if !shared.cfg.stub_delay.is_zero() {
            std::thread::sleep(shared.cfg.stub_delay);
        }
        served(shared);
        let seed = format!("image:{path_str}");
        return EmbedResponse::Embeddings {
            id,
            vecs: vec![stub_vector(&seed, STUB_DIM)],
            dim: STUB_DIM,
            backend: "stub".to_string(),
            stub: true,
        };
    }

    let path = std::path::Path::new(path_str);
    if !path.is_absolute() || !ws_platform::content::local_content_allowed(path) {
        return EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::InvalidRequest,
            message: "image is not an allowed local file".into(),
        };
    }
    let wic = match ws_platform::wic::decode_image_wic(path, 4096) {
        Ok(img) => img,
        Err(e) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::EmbedFailed,
                message: format!("WIC image decode failed for {path_str}: {e}"),
            };
        }
    };
    let rgb = ws_platform::wic::bgra8_to_rgb8(&wic.bgra8);

    let mut eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    let (dim, result) = match ensure_runtime(&mut eng, shared) {
        Ok(rt) if rt.has_mtmd() && rt.mtmd_supports_vision() => {
            let dim = rt.n_embd();
            (
                dim,
                rt.embed_image_with_budget(&rgb, wic.scaled_width, wic.scaled_height, token_budget)
                    .map_err(|e| e.to_string()),
            )
        }
        Ok(_) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::Unsupported,
                message: "image embedding requires the multimodal vision projector (mmproj)".into(),
            };
        }
        Err(message) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::ModelNotAvailable,
                message,
            };
        }
    };
    let backend = eng.backend.clone().unwrap_or_else(|| "?".into());
    match result {
        Ok(vec) => {
            eng.jobs_served += 1;
            EmbedResponse::Embeddings {
                id,
                vecs: vec![vec],
                dim,
                backend,
                stub: false,
            }
        }
        Err(message) => EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::EmbedFailed,
            message,
        },
    }
}

fn embed_audio_job(
    shared: &Shared,
    id: u64,
    path: &str,
    window: Option<(u64, u64)>,
) -> EmbedResponse {
    if window.is_some_and(|(s, e)| e <= s || e - s > 30_000 || e.checked_mul(16).is_none()) {
        return EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::InvalidRequest,
            message: "invalid audio window".into(),
        };
    }
    if shared.cfg.stub {
        if !shared.cfg.stub_delay.is_zero() {
            std::thread::sleep(shared.cfg.stub_delay);
        }
        served(shared);
        return EmbedResponse::Embeddings {
            id,
            vecs: vec![stub_vector(&format!("audio:{path}:{window:?}"), STUB_DIM)],
            dim: STUB_DIM,
            backend: "stub".to_string(),
            stub: true,
        };
    }
    let pcm = match crate::wav::read_pcm16(std::path::Path::new(path), window) {
        Ok(pcm) => pcm,
        Err(error) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::InvalidRequest,
                message: format!("invalid PCM16 mono16k WAV: {error}"),
            }
        }
    };
    let mut eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    let rt = match ensure_runtime(&mut eng, shared) {
        Ok(rt) if rt.mtmd_supports_audio() => rt,
        Ok(_) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::Unsupported,
                message: "audio embedding requires a usable audio projector".into(),
            }
        }
        Err(message) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::ModelNotAvailable,
                message,
            }
        }
    };
    let dim = rt.n_embd();
    let result = rt.embed_audio(&pcm);
    let backend = eng.backend.clone().unwrap_or_else(|| "?".into());
    match result {
        Ok(vector) => {
            eng.jobs_served += 1;
            EmbedResponse::Embeddings {
                id,
                vecs: vec![vector],
                dim,
                backend,
                stub: false,
            }
        }
        Err(error) => EmbedResponse::Error {
            id: Some(id),
            code: EmbedErrorCode::EmbedFailed,
            message: error.to_string(),
        },
    }
}

fn served(shared: &Shared) {
    shared
        .engine
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .jobs_served += 1;
}

/// Lazily loads the model on first use. The failure is returned as a message
/// (the worker stays alive — the model may be downloaded later).
fn ensure_runtime<'a>(
    eng: &'a mut std::sync::MutexGuard<'_, EngineSlot>,
    shared: &Shared,
) -> Result<&'a mut ws_embed::EmbedRuntime, String> {
    if eng.runtime.is_none() {
        let path = shared
            .cfg
            .model_path
            .clone()
            .unwrap_or_else(|| paths::model_dir().join(TEXT_FILE));
        if !path.is_file() {
            return Err(format!(
                "embedding model not found: {} (download it in Settings → AI model)",
                path.display()
            ));
        }
        let mmproj = path.with_file_name(MMPROJ_FILE);
        let opts = ws_embed::LoadOptions {
            backend: shared.cfg.backend,
            mmproj: mmproj.is_file().then_some(mmproj),
            ..ws_embed::LoadOptions::default()
        };
        info!(path = %path.display(), backend = ?opts.backend, threads = opts.n_threads, "loading embedding model");
        match ws_embed::EmbedRuntime::load(&path, &opts) {
            Ok(rt) => {
                eng.backend = Some(rt.backend().to_string());
                eng.runtime = Some(rt);
            }
            Err(e) => return Err(format!("embedding model failed to load: {e}")),
        }
    }
    eng.runtime
        .as_mut()
        .ok_or_else(|| "embedding model unavailable".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn getter(vars: HashMap<String, String>) -> impl Fn(&str) -> Option<String> {
        move |k: &str| vars.get(k).cloned()
    }

    fn test_config() -> WorkerConfig {
        WorkerConfig {
            pipe_name: r"\\.\pipe\winsearch-embed-test".into(),
            idle_timeout: Duration::from_secs(300),
            stub: true,
            model_path: None,
            backend: BackendPreference::Auto,
            stub_delay: Duration::ZERO,
        }
    }

    #[test]
    fn config_defaults_match_spec() {
        let cfg = WorkerConfig::from_getter(&getter(HashMap::new())).unwrap();
        assert_eq!(cfg.idle_timeout, Duration::from_secs(DEFAULT_IDLE_SECS));
        assert_eq!(DEFAULT_IDLE_SECS, 300, "idle exit is 5 minutes");
        assert!(!cfg.stub);
        assert!(cfg.model_path.is_none());
        assert_eq!(cfg.backend, BackendPreference::Auto);
    }

    #[test]
    fn config_env_overrides() {
        let vars = HashMap::from([
            ("WS_EMBED_PIPE".to_string(), r"\\.\pipe\custom".to_string()),
            ("WS_EMBED_IDLE_SECS".to_string(), "7".to_string()),
            ("WS_EMBED_STUB".to_string(), "1".to_string()),
            ("WS_EMBED_MODEL".to_string(), "C:\\m.gguf".to_string()),
            ("WS_EMBED_BACKEND".to_string(), "cpu".to_string()),
            ("WS_EMBED_STUB_DELAY_MS".to_string(), "12".to_string()),
        ]);
        let cfg = WorkerConfig::from_getter(&getter(vars)).unwrap();
        assert_eq!(cfg.pipe_name, r"\\.\pipe\custom");
        assert_eq!(cfg.idle_timeout, Duration::from_secs(7));
        assert!(cfg.stub);
        assert_eq!(cfg.model_path, Some(PathBuf::from("C:\\m.gguf")));
        assert_eq!(cfg.backend, BackendPreference::Cpu);
        assert_eq!(cfg.stub_delay, Duration::from_millis(12));
    }

    #[test]
    fn args_override_env() {
        let mut cfg = WorkerConfig::from_getter(&getter(HashMap::new())).unwrap();
        cfg.apply_args(&[
            "--pipe".to_string(),
            r"\\.\pipe\arg".to_string(),
            "--idle-secs".to_string(),
            "9".to_string(),
            "--stub".to_string(),
            "--backend".to_string(),
            "vulkan".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.pipe_name, r"\\.\pipe\arg");
        assert_eq!(cfg.idle_timeout, Duration::from_secs(9));
        assert!(cfg.stub);
        assert_eq!(cfg.backend, BackendPreference::Vulkan);
        assert!(cfg.apply_args(&["--bogus".to_string()]).is_err());
    }

    #[test]
    fn stub_round_trip_through_dispatch() {
        let shared = Shared::for_config(test_config());
        let hello = EmbedRequest::Hello {
            protocol_version: EMBED_PROTOCOL_VERSION,
            client: "winsearchd".into(),
            client_version: "test".into(),
        };
        let Action::Respond(EmbedResponse::HelloAck {
            stub,
            model_loaded,
            features,
            ..
        }) = dispatch(&hello, &shared)
        else {
            panic!("expected hello_ack");
        };
        assert!(stub);
        assert!(!model_loaded, "stub mode never claims real weights");
        assert!(features.contains(&"embed_audio".to_string()));

        let texts = vec!["hello world".to_string(), "second text".to_string()];
        let Action::Respond(EmbedResponse::Embeddings {
            vecs,
            dim,
            backend,
            stub,
            ..
        }) = dispatch(
            &EmbedRequest::EmbedTexts {
                id: 3,
                texts: texts.clone(),
            },
            &shared,
        )
        else {
            panic!("expected embeddings");
        };
        assert_eq!(dim, STUB_DIM);
        assert_eq!(backend, "stub");
        assert!(stub);
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0], stub_vector(&texts[0], STUB_DIM));
        assert_ne!(vecs[0], vecs[1]);

        // Media plumbing answers in stub mode too.
        let Action::Respond(EmbedResponse::Embeddings { vecs, .. }) = dispatch(
            &EmbedRequest::EmbedImage {
                id: 4,
                path: "C:\\p.png".into(),
                token_budget: None,
            },
            &shared,
        ) else {
            panic!("expected image embeddings");
        };
        assert_eq!(vecs.len(), 1);

        let Action::Respond(EmbedResponse::WorkerStatus { jobs_served, .. }) =
            dispatch(&EmbedRequest::Status { id: 5 }, &shared)
        else {
            panic!("expected worker_status");
        };
        // One text job + one image job served.
        assert_eq!(jobs_served, 2);

        let Action::RespondAndExit(EmbedResponse::ShutdownAck { id }) =
            dispatch(&EmbedRequest::Shutdown { id: 6 }, &shared)
        else {
            panic!("expected shutdown_ack");
        };
        assert_eq!(id, 6);
    }

    #[test]
    fn audio_worker_rejects_malformed_and_oversized_wav_before_loading() {
        let path =
            std::env::temp_dir().join(format!("winsearch-audio-red-{}.wav", std::process::id()));
        let mut cfg = test_config();
        cfg.stub = false;
        cfg.model_path = Some(PathBuf::from("missing-model-for-wav-test.gguf"));
        let shared = Shared::for_config(cfg);
        for bytes in [b"not a wave".to_vec(), {
            let mut wav = b"RIFF".to_vec();
            wav.extend(u32::MAX.to_le_bytes());
            wav.extend(b"WAVEfmt ");
            wav.extend(u32::MAX.to_le_bytes());
            wav
        }] {
            std::fs::write(&path, bytes).unwrap();
            let Action::Respond(EmbedResponse::Error { code, .. }) = dispatch(
                &EmbedRequest::EmbedAudio {
                    id: 12,
                    path: path.to_string_lossy().into_owned(),
                    window_ms: None,
                },
                &shared,
            ) else {
                panic!("expected malformed WAV error")
            };
            assert_eq!(code, EmbedErrorCode::InvalidRequest);
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn audio_worker_rejects_invalid_windows_before_model_load() {
        let mut cfg = test_config();
        cfg.stub = false;
        cfg.model_path = Some(PathBuf::from("missing-model-for-window-test.gguf"));
        let shared = Shared::for_config(cfg);
        for window_ms in [
            Some((10, 10)),
            Some((20, 10)),
            Some((0, 30_001)),
            Some((u64::MAX - 1, u64::MAX)),
        ] {
            let Action::Respond(EmbedResponse::Error { code, .. }) = dispatch(
                &EmbedRequest::EmbedAudio {
                    id: 11,
                    path: "missing.wav".into(),
                    window_ms,
                },
                &shared,
            ) else {
                panic!("expected invalid audio request")
            };
            assert_eq!(code, EmbedErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn bad_batches_are_rejected() {
        let shared = Shared::for_config(test_config());
        for texts in [vec![], vec!["x".to_string(); MAX_TEXTS_PER_JOB + 1]] {
            let Action::Respond(EmbedResponse::Error { code, .. }) =
                dispatch(&EmbedRequest::EmbedTexts { id: 1, texts }, &shared)
            else {
                panic!("expected error");
            };
            assert_eq!(code, EmbedErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn hello_version_mismatch_is_an_error() {
        let shared = Shared::for_config(test_config());
        let Action::Respond(EmbedResponse::Error { code, id, .. }) = dispatch(
            &EmbedRequest::Hello {
                protocol_version: EMBED_PROTOCOL_VERSION + 1,
                client: "x".into(),
                client_version: "x".into(),
            },
            &shared,
        ) else {
            panic!("expected error");
        };
        assert_eq!(code, EmbedErrorCode::VersionMismatch);
        assert_eq!(id, None);
    }

    #[test]
    fn missing_model_is_reported_not_fatal() {
        let mut cfg = test_config();
        cfg.stub = false;
        cfg.model_path = Some(PathBuf::from(r"C:\does-not-exist-12345.gguf"));
        let shared = Shared::for_config(cfg);
        let Action::Respond(EmbedResponse::Error { code, .. }) = dispatch(
            &EmbedRequest::EmbedTexts {
                id: 1,
                texts: vec!["hi".into()],
            },
            &shared,
        ) else {
            panic!("expected error");
        };
        assert_eq!(code, EmbedErrorCode::ModelNotAvailable);
        // The worker is still alive and answering status.
        let Action::Respond(EmbedResponse::WorkerStatus { model_loaded, .. }) =
            dispatch(&EmbedRequest::Status { id: 2 }, &shared)
        else {
            panic!("expected status");
        };
        assert!(!model_loaded);
    }

    /// Model-gated: the real binary loads real weights lazily, serves real
    /// vectors, and shuts down cleanly. No model file is committed; set
    /// `WS_EMBED_TEST_MODEL` to a GGUF (e.g. the EmbeddingGemma 2 text model)
    /// and run with `--ignored`.
    #[test]
    #[ignore = "requires WS_EMBED_TEST_MODEL (run with --ignored)"]
    fn real_model_round_trip_through_worker_binary() {
        use std::process::Command;
        use ws_platform::pipe::PipeStream;
        use ws_protocol::{read_message, write_message};

        let Some(model) = std::env::var_os("WS_EMBED_TEST_MODEL")
            .map(PathBuf::from)
            .filter(|p| p.is_file())
        else {
            eprintln!("skipping: WS_EMBED_TEST_MODEL not set");
            return;
        };
        let mut exe = std::env::current_exe().expect("current exe");
        exe.pop(); // deps
        exe.pop(); // <profile>
        let bin = exe.join(format!("winsearch-embed{}", std::env::consts::EXE_SUFFIX));
        assert!(
            bin.is_file(),
            "build the worker binary first: {}",
            bin.display()
        );

        let pipe = format!(r"\\.\pipe\winsearch-embed-real-{}", std::process::id());
        let mut child = Command::new(&bin)
            .args([
                "--pipe",
                pipe.as_str(),
                "--model",
                model.to_str().expect("model path"),
                "--backend",
                "cpu",
                "--idle-secs",
                "120",
            ])
            .spawn()
            .expect("spawn worker");

        let cleanup = |child: &mut std::process::Child| {
            let _ = child.kill();
            let _ = child.wait();
        };
        let mut stream = None;
        for _ in 0..200 {
            match PipeStream::connect(&pipe) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        let mut stream: PipeStream = match stream {
            Some(s) => s,
            None => {
                cleanup(&mut child);
                panic!("worker pipe never appeared");
            }
        };
        write_message(
            &mut stream,
            &EmbedRequest::Hello {
                protocol_version: EMBED_PROTOCOL_VERSION,
                client: "test".into(),
                client_version: "test".into(),
            },
        )
        .unwrap();
        let ack: EmbedResponse = read_message(&mut stream).unwrap().unwrap();
        let EmbedResponse::HelloAck { stub, .. } = ack else {
            cleanup(&mut child);
            panic!("expected hello_ack, got {ack:?}");
        };
        assert!(!stub);

        write_message(
            &mut stream,
            &EmbedRequest::EmbedTexts {
                id: 1,
                texts: vec!["budget spreadsheet".into(), "receipt".into()],
            },
        )
        .unwrap();
        let resp: EmbedResponse = read_message(&mut stream).unwrap().unwrap();
        let EmbedResponse::Embeddings {
            vecs, dim, stub, ..
        } = resp
        else {
            cleanup(&mut child);
            panic!("expected embeddings, got {resp:?}");
        };
        assert!(!stub);
        assert_eq!(vecs.len(), 2);
        assert!(dim > 0);
        for v in &vecs {
            assert_eq!(v.len(), dim);
            let norm: f64 = v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
            assert!((norm - 1.0).abs() < 1e-3, "not unit-norm: {norm}");
        }

        write_message(&mut stream, &EmbedRequest::Shutdown { id: 2 }).unwrap();
        let ack: EmbedResponse = read_message(&mut stream).unwrap().unwrap();
        assert!(matches!(ack, EmbedResponse::ShutdownAck { .. }));
        drop(stream);
        let status = child.wait().expect("wait for worker exit");
        assert!(status.success(), "worker exited {status}");
    }
}
