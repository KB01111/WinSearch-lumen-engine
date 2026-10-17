use serde::{Deserialize, Serialize};

use ws_modelmgr::ModelStatus;

/// Bumped on breaking wire changes. A client and engine with different
/// versions refuse to work together (the engine answers `Error` with
/// `ErrorCode::ProtocolVersionMismatch` and closes the connection).
pub const PROTOCOL_VERSION: u32 = 1;

pub type RequestId = u64;

/// Messages sent by a client (WinUI frontend, CLI) to the engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Must be the first message on a connection.
    Hello {
        protocol_version: u32,
        client: String,
        client_version: String,
    },
    /// Heartbeat; answered with `Pong`.
    Ping {
        id: RequestId,
    },
    /// Execute a search. Results stream back as `Results` frames with the same
    /// id; the last frame has `done: true`. Superseded/cancelled requests are
    /// closed early with `done: true`.
    Search {
        id: RequestId,
        query: SearchQuery,
    },
    /// Cancel the in-flight request with this id.
    Cancel {
        id: RequestId,
    },
    /// Snapshot of engine/indexing status.
    GetStatus {
        id: RequestId,
    },
    /// Begin (or resume) the embedding-model download. Answered by
    /// `ModelStatus` with `id` set; progress arrives as pushed `ModelStatus`
    /// frames (`id: None`) until the download completes or is cancelled.
    ModelDownloadStart {
        id: RequestId,
    },
    /// Cancel the in-flight model download; the partial files stay on disk
    /// so a later start resumes (status `paused`).
    ModelDownloadCancel {
        id: RequestId,
    },
    /// Copy model files from a local `path` (file or directory) into the
    /// model dir, verifying size (and SHA-256 when pinned).
    ModelImport {
        id: RequestId,
        path: String,
    },
    /// Delete the downloaded model files.
    ModelDelete {
        id: RequestId,
    },
    /// Snapshot of the model lifecycle state.
    GetModelStatus {
        id: RequestId,
    },
    /// Snapshot of the engine search policy (`semantic_enabled`,
    /// `preferred_backend`). Answered by `Config` with `id` set.
    GetConfig {
        id: RequestId,
    },
    /// Persist the engine search policy. Answered by `Config` with `id` set;
    /// an unknown `preferred_backend` spelling is refused with
    /// `Error{invalid_request}` (the typed store only holds
    /// `auto|cuda|vulkan|cpu`).
    SetConfig {
        id: RequestId,
        config: EngineConfig,
    },
    GetIndexingConfig {
        id: RequestId,
    },
    SetIndexingConfig {
        id: RequestId,
        config: IndexingConfig,
    },
}

/// Messages sent by the engine to a client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    IndexingConfig {
        id: Option<RequestId>,
        config: IndexingConfig,
    },
    HelloAck {
        protocol_version: u32,
        engine_version: String,
        /// Capability tags, e.g. "search.exact", "search.semantic", "code".
        features: Vec<String>,
    },
    Pong {
        id: RequestId,
    },
    Results {
        id: RequestId,
        batch: Vec<SearchResult>,
        done: bool,
    },
    /// `id: Some(_)` answers a `GetStatus` request; `id: None` is a pushed
    /// update (e.g. indexing progress) sent proactively to all clients.
    Status {
        id: Option<RequestId>,
        status: EngineStatus,
    },
    /// `id: Some(_)` answers a model request (`ModelDownloadStart/Cancel`,
    /// `ModelImport`, `ModelDelete`, `GetModelStatus`); `id: None` is a
    /// pushed progress/completion update sent proactively to all clients.
    ModelStatus {
        id: Option<RequestId>,
        status: ModelStatus,
    },
    /// `id: Some(_)` answers `GetConfig`/`SetConfig`. Additive like every
    /// other frame: `PROTOCOL_VERSION` stays 1.
    Config {
        id: Option<RequestId>,
        config: EngineConfig,
    },
    Error {
        /// `None` means the error is connection-scoped (e.g. handshake).
        id: Option<RequestId>,
        code: ErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IndexingMode {
    #[default]
    Always,
    Scheduled,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct IndexingConfig {
    pub roots: Vec<String>,
    pub excluded_paths: Vec<String>,
    pub excluded_extensions: Vec<String>,
    pub code_enabled: bool,
    pub code_roots: Vec<String>,
    pub paused: bool,
    pub mode: IndexingMode,
    pub index_on_battery: bool,
    pub respect_battery_saver: bool,
    pub hours_start: u16,
    pub hours_end: u16,
}
impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            roots: vec![],
            excluded_paths: vec![],
            excluded_extensions: vec![],
            code_enabled: false,
            code_roots: vec![],
            paused: false,
            mode: IndexingMode::Always,
            index_on_battery: true,
            respect_battery_saver: true,
            hours_start: 60,
            hours_end: 420,
        }
    }
}

/// Engine search policy (P3.6): the semantic-stage kill switch plus the
/// recorded inference-backend preference.
///
/// `preferred_backend` is deliberately a plain string on the wire (canonical
/// `auto|cuda|vulkan|cpu`) rather than an enum: an unknown spelling from a
/// newer peer still parses, and validation happens at application time
/// (`set_config` refuses it with `invalid_request`). The typed
/// `ws_index::PreferredBackend` is the store-side counterpart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EngineConfig {
    /// Whether the semantic query stage may run (default true; the stage
    /// additionally requires the embedding model to be ready).
    #[serde(default = "default_semantic_enabled")]
    pub semantic_enabled: bool,
    /// Canonical `auto|cuda|vulkan|cpu`.
    #[serde(default = "default_preferred_backend")]
    pub preferred_backend: String,
}

fn default_semantic_enabled() -> bool {
    true
}

fn default_preferred_backend() -> String {
    "auto".to_string()
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            semantic_enabled: true,
            preferred_backend: "auto".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ProtocolVersionMismatch,
    InvalidRequest,
    Internal,
    NotReady,
    Cancelled,
    Unsupported,
}

/// What a search is targeting. `Auto` blends all sources; explicit modes
/// restrict to one source (e.g. `Code` is only ever searched when requested).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    #[default]
    Auto,
    Apps,
    Files,
    Code,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct SearchQuery {
    pub text: String,
    pub mode: SearchMode,
    /// Maximum number of results the client wants to see (engine may stream
    /// more for the expanded window).
    pub limit: u32,
    /// Ask for semantic (embedding) retrieval in addition to exact matching.
    /// The engine never blocks exact results on this; semantic results arrive
    /// in later `Results` frames.
    pub include_semantic: bool,
    /// Restrict to these result kinds; empty means all.
    pub categories: Vec<ResultKind>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ResultKind {
    Application,
    File,
    Folder,
    Image,
    Screenshot,
    Document,
    Audio,
    Video,
    Code,
    Calculator,
    Settings,
    Action,
}

/// Where inside a result the match lives (page, timestamp, line). Drives
/// "open at the match" behaviour and the match-explanation text.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MatchLocation {
    Page { page: u32 },
    Timestamp { seconds: f64, end_seconds: f64 },
    Line { line: u32 },
    Notebook { cell_ordinal:u32, line:u32, end_line:u32 },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MatchingMoment {
    pub excerpt: String,
    pub location: Option<MatchLocation>,
    pub thumbnail_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SearchResult {
    /// Stable within one search; used for selection tracking across updates.
    pub id: String,
    pub kind: ResultKind,
    pub title: String,
    /// Path, URI, or short description depending on kind.
    pub subtitle: String,
    /// Human-readable reason this result matched ("Matches PDF page 14").
    pub match_explanation: String,
    /// Ranking score in [0, 1]; results stream in descending order.
    pub score: f64,
    pub path: Option<String>,
    pub location: Option<MatchLocation>,
    /// Absolute path of a cached thumbnail, when one exists.
    pub thumbnail_path: Option<String>,
    pub excerpt: Option<String>,
    pub matching_moments: Vec<MatchingMoment>,
}

impl Default for SearchResult {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: ResultKind::File,
            title: String::new(),
            subtitle: String::new(),
            match_explanation: String::new(),
            score: 0.0,
            path: None,
            location: None,
            thumbnail_path: None,
            excerpt: None,
            matching_moments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EngineRunState {
    /// No indexing running and nothing pending.
    #[default]
    Idle,
    Indexing,
    Paused,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct EngineStatus {
    pub state: EngineRunState,
    /// Short human-readable activity line ("Indexing Pictures", "Paused").
    pub current_activity: String,
    pub files_discovered: u64,
    pub files_indexed: u64,
    pub segments_indexed: u64,
    pub queue_depth: u64,
    /// Whether the embedding model is loaded in the inference worker.
    pub model_loaded: bool,
    pub index_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ws_modelmgr::ModelState;

    #[test]
    fn client_message_json_round_trip() {
        let msg = ClientMessage::Search {
            id: 42,
            query: SearchQuery {
                text: "a dog on the beach".into(),
                mode: SearchMode::Auto,
                limit: 20,
                include_semantic: true,
                categories: vec![ResultKind::Image, ResultKind::Video],
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"search\""));
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn server_message_json_round_trip() {
        let msg = ServerMessage::Results {
            id: 7,
            batch: vec![SearchResult {
                id: "r1".into(),
                kind: ResultKind::Document,
                title: "rental-agreement.pdf".into(),
                subtitle: "C:\\Docs\\rental-agreement.pdf".into(),
                match_explanation: "Matches PDF page 14".into(),
                score: 0.91,
                path: Some("C:\\Docs\\rental-agreement.pdf".into()),
                location: Some(MatchLocation::Page { page: 14 }),
                thumbnail_path: None,
                excerpt: Some("rental clause".into()),
                matching_moments: vec![MatchingMoment {
                    excerpt: "rental clause".into(),
                    location: Some(MatchLocation::Page { page: 14 }),
                    thumbnail_path: None,
                }],
            }],
            done: true,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"results\""));
        assert!(json.contains("\"kind\":\"page\""));
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn timestamp_location_round_trip() {
        let msg = ServerMessage::Results {
            id: 1,
            batch: vec![SearchResult {
                location: Some(MatchLocation::Timestamp {
                    seconds: 155.5,
                    end_seconds: 185.5,
                }),
                ..Default::default()
            }],
            done: false,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn model_messages_round_trip() {
        for msg in [
            ClientMessage::ModelDownloadStart { id: 1 },
            ClientMessage::ModelDownloadCancel { id: 2 },
            ClientMessage::ModelImport {
                id: 3,
                path: "C:\\models".into(),
            },
            ClientMessage::ModelDelete { id: 4 },
            ClientMessage::GetModelStatus { id: 5 },
        ] {
            let json = serde_json::to_string(&msg).unwrap();
            let back: ClientMessage = serde_json::from_str(&json).unwrap();
            assert_eq!(msg, back);
        }

        let status = ServerMessage::ModelStatus {
            id: None,
            status: ModelStatus {
                state: ModelState::Downloading,
                bytes: 10,
                total_bytes: 100,
                error: None,
            },
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"type\":\"model_status\""));
        assert!(json.contains("\"state\":\"downloading\""));
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }

    #[test]
    fn old_clients_still_parse_new_server_frames() {
        // Additive IPC: unknown-to-old-clients frames carry only `#[serde(default)]`
        // fields, and new client messages parse from the exact wire JSON.
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type":"model_import","id":9,"path":"x"}"#).unwrap();
        assert_eq!(
            msg,
            ClientMessage::ModelImport {
                id: 9,
                path: "x".into()
            }
        );
    }

    #[test]
    fn config_messages_round_trip() {
        for msg in [
            ClientMessage::GetConfig { id: 11 },
            ClientMessage::SetConfig {
                id: 12,
                config: EngineConfig {
                    semantic_enabled: false,
                    preferred_backend: "cuda".into(),
                },
            },
        ] {
            let json = serde_json::to_string(&msg).unwrap();
            let back: ClientMessage = serde_json::from_str(&json).unwrap();
            assert_eq!(msg, back);
        }
        let answer = ServerMessage::Config {
            id: Some(11),
            config: EngineConfig::default(),
        };
        let json = serde_json::to_string(&answer).unwrap();
        assert!(json.contains("\"type\":\"config\""));
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(answer, back);
    }

    #[test]
    fn config_defaults_are_additive() {
        // A bare `{}` (or a frame from an older peer that never sent the new
        // fields) reads back as "semantic on, auto backend" — never a parse
        // error, and `PROTOCOL_VERSION` stays 1.
        let cfg: EngineConfig = serde_json::from_str("{}").unwrap();
        assert!(cfg.semantic_enabled);
        assert_eq!(cfg.preferred_backend, "auto");
        // Unknown backend spellings still parse on the wire; the engine
        // refuses them at `set_config` application time.
        let cfg: EngineConfig =
            serde_json::from_str(r#"{"semantic_enabled":true,"preferred_backend":"opencl"}"#)
                .unwrap();
        assert_eq!(cfg.preferred_backend, "opencl");
        let msg: ClientMessage = serde_json::from_str(r#"{"type":"get_config","id":3}"#).unwrap();
        assert_eq!(msg, ClientMessage::GetConfig { id: 3 });
    }
}
