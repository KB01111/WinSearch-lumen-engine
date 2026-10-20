//! Pipe server for the internal embed-worker protocol.
//!
//! Lifetime: load the model lazily on the first job, serve until `shutdown`
//! or the idle timeout, then exit the process — process exit returns all GPU
//! memory to the driver, which is the full GPU release (no `release_gpu`
//! dance needed on the way out). One client (winsearchd) at a time:
//! concurrent connections serialize on a slot mutex.
//!
//! The message handlers ([`dispatch`]) are pure over [`Shared`] state and
//! may retire a broken NPU process before native teardown; [`run`] also
//! installs an independent watchdog for blocked NPU initialization/execution.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};
use ws_embed_protocol::{
    stub_embed_texts, stub_vector, EmbedErrorCode, EmbedRequest, EmbedResponse,
    EMBED_PROTOCOL_VERSION, MAX_TEXTS_PER_JOB, NPU_RETIRE_EXIT_CODE, STUB_DIM,
};
use ws_modelmgr::{MMPROJ_FILE, NPU_BUNDLE_DIR, TEXT_FILE};
use ws_platform::{
    identity, paths,
    pipe::{self, PipeStream},
};
use ws_protocol::{read_message, write_message};

pub use ws_embed::backends::BackendPreference;

/// Idle timeout before the worker exits: 5 minutes.
pub const DEFAULT_IDLE_SECS: u64 = 300;

/// NPU share of one text job. Well under winsearchd's 120 s job timeout:
/// texts left when it runs out are embedded on GPU/CPU in the same job.
const NPU_TEXT_JOB_BUDGET: Duration = Duration::from_secs(45);

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
    /// Explicit IRON NPU bundle directory. `None` = `NPU_BUNDLE_DIR` next to
    /// the text GGUF.
    pub npu_bundle: Option<PathBuf>,
    /// Supervisor quarantine: preserve the preference while bypassing NPU use.
    pub disable_npu: bool,
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
                "--disable-npu" => self.disable_npu = true,
                "--model" => self.model_path = Some(take_value(raw, &mut i, "--model")?.into()),
                "--npu-bundle" => {
                    self.npu_bundle = Some(take_value(raw, &mut i, "--npu-bundle")?.into())
                }
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
        let npu_bundle = match get("WS_EMBED_NPU_BUNDLE") {
            Some(p) if !p.is_empty() => Some(PathBuf::from(p)),
            _ => None,
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
            npu_bundle,
            disable_npu: false,
            stub_delay,
        })
    }

    fn text_model_path(&self) -> PathBuf {
        self.model_path
            .clone()
            .unwrap_or_else(|| paths::model_dir().join(TEXT_FILE))
    }

    fn npu_bundle_dir(&self) -> PathBuf {
        self.npu_bundle
            .clone()
            .unwrap_or_else(|| self.text_model_path().with_file_name(NPU_BUNDLE_DIR))
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
    /// Independent of `engine`: the driver can block while holding that mutex.
    npu_deadline: Option<Instant>,
}

impl Activity {
    fn exit_code_at(&self, now: Instant, idle: Duration) -> Option<i32> {
        if self.npu_deadline.is_some_and(|deadline| now >= deadline) {
            Some(NPU_RETIRE_EXIT_CODE)
        } else if self.shutdown || (self.active_jobs == 0 && now.duration_since(self.last) >= idle)
        {
            Some(0)
        } else {
            None
        }
    }
}

/// The NPU runtime is tried once, lazily, and stays off after a failure:
/// every later job goes straight to the GPU/CPU runtime.
enum NpuSlot {
    Untried,
    Ready(ws_embed::NpuRuntime),
    Unavailable(&'static str),
}

struct EngineSlot {
    /// Primary text/image runtime on an AMD NPU2 (IRON bundle).
    npu: NpuSlot,
    /// llama.cpp runtime (CUDA > Vulkan > CPU): audio, and the fallback.
    runtime: Option<ws_embed::EmbedRuntime>,
    backend: Option<String>,
    last_backend: Option<String>,
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
                npu_deadline: None,
            }),
            engine: Mutex::new(EngineSlot {
                npu: NpuSlot::Untried,
                runtime: None,
                backend: None,
                last_backend: None,
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
            let exit_code = {
                let a = shared.activity.lock().unwrap_or_else(|p| p.into_inner());
                a.exit_code_at(Instant::now(), idle)
            };
            if let Some(code) = exit_code {
                // Process exit IS the full GPU release: the driver reclaims
                // every allocation. No per-handle teardown needed (or wanted —
                // a CPU `release_gpu` reload before dying would be pure waste).
                info!(code, "embed worker exiting");
                if code == NPU_RETIRE_EXIT_CODE {
                    ws_platform::child_job::terminate_current_process(code as u32);
                }
                std::process::exit(code);
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
    let mut resp = job();
    if let EmbedResponse::Embeddings { npu, backend, .. } = &mut resp {
        let mut engine = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
        engine.last_backend = Some(backend.clone());
        *npu = npu_observation(&engine);
    }
    {
        let mut a = shared.activity.lock().unwrap_or_else(|p| p.into_inner());
        a.active_jobs = a.active_jobs.saturating_sub(1);
        a.last = Instant::now();
        a.npu_deadline = None;
    }
    Action::Respond(resp)
}

fn hello_ack(shared: &Shared, id_version: u32) -> EmbedResponse {
    let eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    EmbedResponse::HelloAck {
        protocol_version: id_version,
        worker_version: env!("CARGO_PKG_VERSION").to_string(),
        features: features(),
        model_loaded: model_loaded(&eng),
        stub: shared.cfg.stub,
        npu: npu_observation(&eng),
    }
}

fn status_response(shared: &Shared, id: u64) -> EmbedResponse {
    let eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    let npu_dim = match &eng.npu {
        NpuSlot::Ready(npu) => Some(npu.n_embd()),
        _ => None,
    };
    EmbedResponse::WorkerStatus {
        id,
        model_loaded: model_loaded(&eng),
        backend: eng.last_backend.clone(),
        stub: shared.cfg.stub,
        dim: npu_dim
            .or_else(|| eng.runtime.as_ref().map(|r| r.n_embd()))
            .unwrap_or(STUB_DIM),
        uptime_secs: eng.started.elapsed().as_secs(),
        jobs_served: eng.jobs_served,
        npu: npu_observation(&eng),
    }
}

fn npu_observation(eng: &EngineSlot) -> Option<String> {
    match &eng.npu {
        NpuSlot::Untried => None,
        NpuSlot::Ready(_) => Some("ready".into()),
        NpuSlot::Unavailable(reason) => Some((*reason).into()),
    }
}

fn model_loaded(eng: &EngineSlot) -> bool {
    eng.runtime.is_some() || matches!(eng.npu, NpuSlot::Ready(_))
}

fn features() -> Vec<String> {
    // These are implemented requests, not model-residency claims. Lazy audio
    // must remain callable after NPU-only text/image jobs. The job returns a
    // typed availability/unsupported error when its projector cannot load.
    ["embed_texts", "embed_audio", "embed_image"]
        .map(String::from)
        .to_vec()
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
            npu: None,
        };
    }
    let mut eng = shared.engine.lock().unwrap_or_else(|p| p.into_inner());
    // The NPU takes texts in order until its time budget is spent, so a batch
    // of long texts cannot outrun the client's job timeout; GPU/CPU embeds
    // whatever is left (same model, same space).
    let mut vecs = try_npu(&mut eng, shared, "texts", |npu| {
        let mut done = Vec::with_capacity(texts.len());
        for text in texts {
            let remaining = shared
                .activity
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .npu_deadline
                .unwrap()
                .saturating_duration_since(Instant::now());
            // Leave watchdog headroom before choosing the safe partial-batch fallback.
            if !done.is_empty() && remaining <= WATCHDOG_POLL * 2 {
                break;
            }
            done.extend(npu.embed_texts(std::slice::from_ref(text))?);
        }
        Ok(done)
    })
    .unwrap_or_default();
    if vecs.len() == texts.len() {
        eng.jobs_served += 1;
        return EmbedResponse::Embeddings {
            id,
            dim: vecs.first().map_or(0, Vec::len),
            vecs,
            backend: "npu".to_string(),
            stub: false,
            npu: None,
        };
    }
    let on_npu = vecs.len();
    // Borrow the runtime only inside this block: the backend label and the
    // served-counter live on the same slot and are touched after it ends.
    let (dim, result) = match ensure_runtime(&mut eng, shared) {
        Ok(rt) => {
            let dim = rt.n_embd();
            (
                dim,
                rt.embed_texts(&texts[on_npu..]).map_err(|e| e.to_string()),
            )
        }
        Err(message) => {
            return EmbedResponse::Error {
                id: Some(id),
                code: EmbedErrorCode::ModelNotAvailable,
                message,
            };
        }
    };
    let mut backend = eng.backend.clone().unwrap_or_else(|| "?".into());
    if on_npu > 0 {
        backend = format!("npu+{backend}");
    }
    match result {
        Ok(rest) => {
            eng.jobs_served += 1;
            vecs.extend(rest);
            EmbedResponse::Embeddings {
                id,
                vecs,
                dim,
                backend,
                stub: false,
                npu: None,
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
            npu: None,
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
    if let Some(vec) = (token_budget != 70)
        .then(|| {
            try_npu(&mut eng, shared, "image", |npu| {
                npu.embed_image_with_budget(&rgb, wic.scaled_width, wic.scaled_height, token_budget)
            })
        })
        .flatten()
    {
        let dim = vec.len();
        eng.jobs_served += 1;
        return EmbedResponse::Embeddings {
            id,
            vecs: vec![vec],
            dim,
            backend: "npu".to_string(),
            stub: false,
            npu: None,
        };
    }
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
                npu: None,
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
            npu: None,
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
                npu: None,
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

/// Runs `job` on the NPU when it is (or can now be made) resident. `None`
/// means "serve this job on GPU/CPU": no NPU, a preference that excludes it,
/// or a failed job. A device-level failure also retires the NPU for the rest
/// of the worker's life, releasing its hardware contexts.
fn try_npu<T>(
    eng: &mut std::sync::MutexGuard<'_, EngineSlot>,
    shared: &Shared,
    what: &str,
    job: impl FnOnce(&mut ws_embed::NpuRuntime) -> Result<T, ws_embed::EmbedError>,
) -> Option<T> {
    ensure_npu(eng, shared);
    let NpuSlot::Ready(npu) = &mut eng.npu else {
        return None;
    };
    if let Err(error) = arm_npu_budget(shared) {
        warn!(%error, "NPU budget could not be configured");
        ws_platform::child_job::terminate_current_process(NPU_RETIRE_EXIT_CODE as u32);
    }
    let result = job(npu);
    shared
        .activity
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .npu_deadline = None;
    match result {
        Ok(out) => Some(out),
        Err(e @ ws_embed::EmbedError::NpuFailed(_)) => {
            warn!(error = %e, job = what, "NPU failed; retiring worker before native teardown");
            ws_platform::child_job::terminate_current_process(NPU_RETIRE_EXIT_CODE as u32);
        }
        Err(e) => {
            warn!(error = %e, job = what, "NPU rejected input; serving this job on GPU/CPU");
            None
        }
    }
}

/// Decides once whether the NPU serves text/image jobs: the preference must
/// allow it (`auto`/`npu`), an AMD NPU2 must be present, and the bundle must
/// load. Every "no" is logged once and leaves the GPU/CPU runtime in charge.
fn ensure_npu(eng: &mut EngineSlot, shared: &Shared) {
    if !matches!(eng.npu, NpuSlot::Untried) {
        return;
    }
    eng.npu = match probe_npu(shared).and_then(|bundle| {
        arm_npu_budget(shared).map_err(|error| {
            warn!(%error, "NPU initialization unavailable");
            "load_failed"
        })?;
        match ws_embed::NpuRuntime::load(&bundle) {
            Ok(runtime) => Ok(runtime),
            Err(error @ ws_embed::EmbedError::NpuFailed(_)) => {
                warn!(%error, "NPU initialization failed; retiring worker before native teardown");
                ws_platform::child_job::terminate_current_process(NPU_RETIRE_EXIT_CODE as u32);
            }
            Err(error) => {
                warn!(%error, "NPU model unavailable");
                Err("load_failed")
            }
        }
    }) {
        Ok(rt) => {
            info!(bundle = %rt.bundle_dir().display(), "serving text and image embeddings on the AMD NPU");
            NpuSlot::Ready(rt)
        }
        Err(reason) => {
            info!(%reason, "NPU not used; GPU/CPU fallback serves every job");
            NpuSlot::Unavailable(reason)
        }
    };
    if !matches!(eng.npu, NpuSlot::Ready(_)) {
        shared
            .activity
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .npu_deadline = None;
    }
}

fn arm_npu_budget(shared: &Shared) -> Result<(), ws_embed::EmbedError> {
    let armed = {
        let mut activity = shared.activity.lock().unwrap_or_else(|p| p.into_inner());
        if activity.npu_deadline.is_some() {
            false
        } else {
            activity.npu_deadline = Some(Instant::now() + NPU_TEXT_JOB_BUDGET);
            true
        }
    };
    if armed {
        ws_embed::NpuRuntime::set_job_budget(NPU_TEXT_JOB_BUDGET)?;
    }
    Ok(())
}

fn probe_npu(shared: &Shared) -> Result<PathBuf, &'static str> {
    let cfg = &shared.cfg;
    if cfg.disable_npu {
        return Err("quarantined");
    }
    if !cfg.backend.wants_npu() {
        return Err("disabled");
    }
    if !ws_embed::npu_runtime_compiled() {
        return Err("build_missing");
    }
    let device = ws_platform::npu::detect_npu2().ok_or("no_npu")?;
    let bundle = cfg.npu_bundle_dir();
    if !bundle.join("manifest.txt").is_file() {
        return Err("bundle_missing");
    }
    arm_npu_budget(shared).map_err(|_| "load_failed")?;
    verify_npu_bundle(&bundle)?;
    info!(device = %device.instance, bundle = %bundle.display(), "loading NPU embedding model");
    Ok(bundle)
}

/// Kernel binaries and weights are untrusted until all pinned bytes match.
/// This runs only during lazy initialization, never on public status reads.
fn verify_npu_bundle(bundle: &std::path::Path) -> Result<(), &'static str> {
    for expected in ws_modelmgr::NPU_BUNDLE_FILES {
        let relative = expected.npu_bundle_path().ok_or("load_failed")?;
        if ws_modelmgr::download::verify_file(&bundle.join(relative), &expected).is_err() {
            return Err("bundle_missing");
        }
    }
    Ok(())
}

/// Lazily loads the model on first use. The failure is returned as a message
/// (the worker stays alive — the model may be downloaded later).
fn ensure_runtime<'a>(
    eng: &'a mut std::sync::MutexGuard<'_, EngineSlot>,
    shared: &Shared,
) -> Result<&'a mut ws_embed::EmbedRuntime, String> {
    if eng.runtime.is_none() {
        let path = shared.cfg.text_model_path();
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

    #[test]
    fn manifest_alone_or_corrupted_files_are_not_a_verified_npu_bundle() {
        let dir = std::env::temp_dir().join(format!("ws-npu-invalid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("manifest.txt"), b"corrupt kernel references").unwrap();
        assert_eq!(verify_npu_bundle(&dir), Err("bundle_missing"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cold_capabilities_allow_lazy_audio_without_starting_any_runtime() {
        let mut cfg = test_config();
        cfg.stub = false;
        cfg.model_path = Some(PathBuf::from("missing-cold-capability-model.gguf"));
        let shared = Shared::for_config(cfg);
        let EmbedResponse::HelloAck {
            features,
            model_loaded,
            stub,
            npu,
            ..
        } = hello_ack(&shared, 1)
        else {
            panic!("hello ack")
        };
        assert!(!model_loaded && !stub && npu.is_none());
        for name in ["embed_texts", "embed_audio", "embed_image"] {
            assert!(features.iter().any(|feature| feature == name));
        }
        let engine = shared.engine.lock().unwrap();
        assert!(engine.runtime.is_none() && matches!(engine.npu, NpuSlot::Untried));
    }

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
            npu_bundle: None,
            disable_npu: false,
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
    fn quarantine_flag_preserves_the_requested_backend() {
        let mut cfg = test_config();
        cfg.backend = BackendPreference::Npu;
        assert!(cfg.apply_args(&["--disable-npu".into()]).is_ok());
        assert_eq!(cfg.backend, BackendPreference::Npu);
        assert!(probe_npu(&Shared::for_config(cfg)).is_err());
    }

    #[test]
    fn npu_deadline_takes_priority_over_active_jobs_and_idle_shutdown() {
        let now = Instant::now();
        let mut activity = Activity {
            last: now,
            active_jobs: 1,
            shutdown: false,
            npu_deadline: Some(now + Duration::from_secs(45)),
        };
        assert_eq!(
            activity.exit_code_at(now + Duration::from_secs(44), Duration::from_secs(1)),
            None
        );
        assert_eq!(
            activity.exit_code_at(now + Duration::from_secs(45), Duration::from_secs(1)),
            Some(75)
        );
        activity.npu_deadline = None;
        assert_eq!(
            activity.exit_code_at(now + Duration::from_secs(60), Duration::from_secs(1)),
            None
        );
        activity.active_jobs = 0;
        assert_eq!(
            activity.exit_code_at(now + Duration::from_secs(60), Duration::from_secs(1)),
            Some(0)
        );
    }

    #[test]
    fn completed_job_clears_npu_watchdog_before_cpu_or_idle_time() {
        let shared = Shared::for_config(test_config());
        shared.activity.lock().unwrap().npu_deadline =
            Some(Instant::now() - Duration::from_secs(1));
        dispatch(
            &EmbedRequest::EmbedTexts {
                id: 1,
                texts: vec!["x".into()],
            },
            &shared,
        );
        let activity = shared.activity.lock().unwrap();
        assert_eq!(activity.npu_deadline, None);
        assert_eq!(activity.active_jobs, 0);
        assert_eq!(
            activity.exit_code_at(Instant::now(), Duration::from_secs(300)),
            None
        );
    }

    #[test]
    fn watchdog_retires_a_stalled_job_without_taking_the_engine_lock() {
        if std::env::var_os("WS_TEST_LOCKED_NPU_CHILD").is_some() {
            use std::os::windows::ffi::OsStrExt;
            #[link(name = "kernel32")]
            unsafe extern "system" {
                fn LoadLibraryW(path: *const u16) -> *mut std::ffi::c_void;
            }
            let dll = std::env::var_os("WS_TEST_LOCKED_NPU_DLL").unwrap();
            let path: Vec<u16> = dll.encode_wide().chain(Some(0)).collect();
            // SAFETY: absolute parent-owned fixture DLL, with a terminated path.
            assert!(!unsafe { LoadLibraryW(path.as_ptr()) }.is_null());
            let shared = Arc::new(Shared::for_config(test_config()));
            {
                let mut activity = shared.activity.lock().unwrap();
                activity.active_jobs = 1;
                // Fixture budget only; the production deadline remains45s.
                activity.npu_deadline = Some(Instant::now() + Duration::from_millis(100));
            }
            spawn_watchdog(Arc::clone(&shared), Duration::from_secs(300));
            let _engine = shared.engine.lock().unwrap();
            std::thread::sleep(Duration::from_secs(10));
            panic!("watchdog blocked behind a stalled native job");
        }
        let fixture = std::env::temp_dir().join(format!(
            "ws-blocked-detach-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&fixture).unwrap();
        let source = fixture.join("blocked_detach.rs");
        let dll = fixture.join("blocked_detach.dll");
        std::fs::write(&source, r#"
            #[link(name="kernel32")]
            unsafe extern "system" { fn Sleep(ms:u32); }
            #[no_mangle]
            pub extern "system" fn DllMain(_: *mut core::ffi::c_void, reason:u32, _: *mut core::ffi::c_void)->i32 {
                if reason==0 { unsafe { Sleep(30000); } }
                1
            }
        "#).unwrap();
        assert!(std::process::Command::new("rustc")
            .args(["--edition=2021", "--crate-type=cdylib"])
            .arg(&source)
            .arg("-o")
            .arg(&dll)
            .status()
            .unwrap()
            .success());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "server::tests::watchdog_retires_a_stalled_job_without_taking_the_engine_lock",
                "--nocapture",
            ])
            .env("WS_TEST_LOCKED_NPU_CHILD", "1")
            .env("WS_TEST_LOCKED_NPU_DLL", dll)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(NPU_RETIRE_EXIT_CODE));
                let _ = std::fs::remove_dir_all(&fixture);
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_dir_all(&fixture);
                panic!("worker retirement exceeded its independent parent deadline");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
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
    fn npu_bundle_defaults_next_to_the_text_model_and_is_configurable() {
        let mut cfg = test_config();
        cfg.model_path = Some(PathBuf::from("models").join("m.gguf"));
        assert_eq!(
            cfg.npu_bundle_dir(),
            PathBuf::from("models").join(NPU_BUNDLE_DIR)
        );
        cfg.apply_args(&["--npu-bundle".into(), "custom-bundle".into()])
            .unwrap();
        assert_eq!(cfg.npu_bundle_dir(), PathBuf::from("custom-bundle"));
        let vars = HashMap::from([("WS_EMBED_NPU_BUNDLE".to_string(), "env-bundle".to_string())]);
        let cfg = WorkerConfig::from_getter(&getter(vars)).unwrap();
        assert_eq!(cfg.npu_bundle, Some(PathBuf::from("env-bundle")));
        let mut cfg = test_config();
        cfg.apply_args(&["--backend".into(), "npu".into()]).unwrap();
        assert_eq!(cfg.backend, BackendPreference::Npu);
    }

    /// Without a usable NPU the worker falls through to the GPU/CPU runtime
    /// and says why in `worker_status.npu`; a CPU preference never probes it.
    #[test]
    fn unusable_npu_falls_back_and_reports_why() {
        for (backend, expect) in [
            (BackendPreference::Cpu, "disabled"),
            (BackendPreference::Auto, ""),
        ] {
            let mut cfg = test_config();
            cfg.stub = false;
            cfg.backend = backend;
            cfg.model_path = Some(PathBuf::from("missing-model-for-npu-test.gguf"));
            let shared = Shared::for_config(cfg);
            let Action::Respond(EmbedResponse::Error { code, message, .. }) = dispatch(
                &EmbedRequest::EmbedTexts {
                    id: 1,
                    texts: vec!["hi".into()],
                },
                &shared,
            ) else {
                panic!("expected the GPU/CPU fallback's missing-model error");
            };
            assert_eq!(code, EmbedErrorCode::ModelNotAvailable);
            assert!(message.contains("embedding model not found"), "{message}");
            let Action::Respond(EmbedResponse::WorkerStatus {
                npu,
                model_loaded,
                backend: label,
                ..
            }) = dispatch(&EmbedRequest::Status { id: 2 }, &shared)
            else {
                panic!("expected status");
            };
            let reason = npu.expect("the first job probes the NPU");
            assert_ne!(reason, "ready", "no NPU bundle exists in tests");
            assert!(reason.contains(expect), "{reason}");
            assert!(!model_loaded);
            assert_eq!(label, None);
        }
    }

    #[test]
    fn stub_mode_never_probes_the_npu() {
        let shared = Shared::for_config(test_config());
        dispatch(
            &EmbedRequest::EmbedTexts {
                id: 1,
                texts: vec!["x".into()],
            },
            &shared,
        );
        let Action::Respond(EmbedResponse::WorkerStatus { npu, .. }) =
            dispatch(&EmbedRequest::Status { id: 2 }, &shared)
        else {
            panic!("expected status");
        };
        assert_eq!(npu, None);
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
