//! Internal embed-worker protocol: the typed messages `winsearchd` (the only
//! client) and `winsearch-embed` (the inference worker) exchange over the
//! second per-user pipe `\\.\pipe\winsearch-embed-<SID>`.
//!
//! This protocol is **internal**: it has its own version constant
//! ([`EMBED_PROTOCOL_VERSION`], independent of `ws-protocol`'s
//! `PROTOCOL_VERSION`) and is **never mirrored to the C# frontend**.
//! Framing is the same length-prefixed JSON codec as the frontend pipe
//! (`ws_protocol::{read_message, write_message}`), so either side reuses it.
//!
//! Jobs (winsearchd batches 32–256 items per job; see `docs/ipc-protocol.md`):
//! - `embed_texts {id, texts[]}` → `embeddings {id, vecs[], dim, backend, stub}`
//! - `embed_audio {id, path, window_ms?}` → real bounded PCM16 mono16k embeddings
//! - `embed_image {id, path, token_budget?}` → real70/140/280token image embeddings
//! - `hello` / `status` / `shutdown` for handshake, health, and teardown.
//!
//! `backend` names what served a job: `npu` (AMD NPU, IRON bundle), `cuda`,
//! `vulkan`, `cpu`, or `npu+<gpu/cpu>` when the NPU's time budget ran out
//! mid-batch and the rest fell back.
//!
//! [`stub_vector`] provides the deterministic fake-embedding path used when no
//! model file is available (`WS_EMBED_STUB=1` / `--stub`): seeded by the input
//! text, L2-normalized, [`STUB_DIM`] wide. Tests use it; production never does.

use serde::{Deserialize, Serialize};

/// Wire version of the internal embed-worker protocol. Bumped only on breaking
/// changes to the messages below; independent of the frontend `PROTOCOL_VERSION`.
pub const EMBED_PROTOCOL_VERSION: u32 = 1;

/// Retire the process without dropping potentially broken native NPU objects.
/// The owning supervisor quarantines NPU use before its one fresh-worker retry.
pub const NPU_RETIRE_EXIT_CODE: i32 = 75;

/// Embedding width produced by [`stub_vector`]. Matches the production
/// `EMBEDDING_DIM` (768, unified EmbeddingGemma 2 space) so stub vectors flow
/// through the same vector store without shape special-cases.
pub const STUB_DIM: usize = 768;

/// Maximum texts in one `embed_texts` job. Bounds the JSON response frame
/// (256 × 768 f32 ≈ 1.6 MB, well under the 16 MiB frame cap). The supervisor
/// splits larger batches into one job per chunk.
pub const MAX_TEXTS_PER_JOB: usize = 256;

pub type RequestId = u64;

/// winsearchd → worker requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmbedRequest {
    /// Must be the first message on a connection. A version mismatch is
    /// answered with `error{version_mismatch}` and the connection is closed.
    Hello {
        protocol_version: u32,
        client: String,
        client_version: String,
    },
    /// Embed a batch of texts (1–[`MAX_TEXTS_PER_JOB`] items).
    EmbedTexts { id: RequestId, texts: Vec<String> },
    /// Embed a bounded PCM16 mono16k WAV window.
    EmbedAudio {
        id: RequestId,
        path: String,
        /// Optional `[start_ms, end_ms]` window; `None` means the whole file.
        window_ms: Option<(u64, u64)>,
    },
    /// Embed a resident local image file.
    EmbedImage {
        id: RequestId,
        path: String,
        /// Omitted by older clients: full-image280tokens. Frames use70/140.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_budget: Option<u32>,
    },
    /// Health probe; answered by `worker_status`.
    Status { id: RequestId },
    /// Ask the worker to acknowledge and exit (idle-exit stays the normal path).
    Shutdown { id: RequestId },
}

/// worker → winsearchd responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmbedResponse {
    HelloAck {
        protocol_version: u32,
        worker_version: String,
        /// Capability tags, e.g. `embed_texts`. Audio/image tags appear only
        /// when the worker can actually serve them (explicit test stub mode,
        /// or a loaded supporting projector).
        features: Vec<String>,
        /// True once real weights are resident (lazy on first job). Always
        /// false in stub mode — see `stub`, which the supervisor ORs in.
        model_loaded: bool,
        /// True when vectors are deterministic fakes ([`stub_vector`]).
        stub: bool,
        /// Closed availability code (see frontend `NpuState`), never a path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        npu: Option<String>,
    },
    Embeddings {
        id: RequestId,
        /// One L2-normalized vector per input, in order. Plain JSON f32
        /// arrays (debuggable default; a binary encoding only if profiling
        /// demands it — same policy as the frontend pipe).
        vecs: Vec<Vec<f32>>,
        dim: usize,
        backend: String,
        stub: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        npu: Option<String>,
    },
    WorkerStatus {
        id: RequestId,
        model_loaded: bool,
        backend: Option<String>,
        stub: bool,
        dim: usize,
        uptime_secs: u64,
        jobs_served: u64,
        /// AMD NPU use: `"ready"` while it serves text/image jobs, otherwise
        /// why it does not (no NPU, preference, bundle missing, load or run
        /// failure). Absent before the first job probes it, and from workers
        /// that predate the NPU runtime.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        npu: Option<String>,
    },
    Error {
        id: Option<RequestId>,
        code: EmbedErrorCode,
        message: String,
    },
    ShutdownAck {
        id: RequestId,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbedErrorCode {
    /// Worker speaks a different [`EMBED_PROTOCOL_VERSION`].
    VersionMismatch,
    /// Malformed job (empty batch, oversized batch, unknown shape).
    InvalidRequest,
    /// No usable model (file missing, load failed). Definitive for the job:
    /// the supervisor does NOT restart on this.
    ModelNotAvailable,
    /// Load succeeded but this input failed (tokenize/decode).
    EmbedFailed,
    /// Loaded runtime without a projector supporting the requested modality.
    Unsupported,
}

impl EmbedResponse {
    /// The request id this response answers, if any.
    pub fn id(&self) -> Option<RequestId> {
        match self {
            Self::HelloAck { .. } => None,
            Self::Embeddings { id, .. }
            | Self::WorkerStatus { id, .. }
            | Self::ShutdownAck { id } => Some(*id),
            Self::Error { id, .. } => *id,
        }
    }
}

/// Deterministic stub embedding for `seed`: FNV-1a hash → xorshift64* stream
/// mapped to `[-1, 1]`, then L2-normalized. Same input always yields the same
/// unit vector; different inputs (almost surely) yield different vectors.
/// `dim` is [`STUB_DIM`] in the worker; exposed for tests.
pub fn stub_vector(seed: &str, dim: usize) -> Vec<f32> {
    let mut state = fnv1a64(seed);
    // xorshift64* must never run on a zero state (it would stick at zero).
    if state == 0 {
        state = 0x9e37_79b9_7f4a_7c15;
    }
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let bits = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        // Top 24 bits → [0, 1) → [-1, 1].
        let unit = ((bits >> 40) as f32) / 16_777_216.0;
        v.push(unit * 2.0 - 1.0);
    }
    l2_normalize(&mut v);
    v
}

/// One stub vector per text, in order.
pub fn stub_embed_texts(texts: &[String], dim: usize) -> Vec<Vec<f32>> {
    texts.iter().map(|t| stub_vector(t, dim)).collect()
}

fn fnv1a64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_4842_2225;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn l2_normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x = (*x as f64 / norm) as f32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_responses_preserve_optional_npu_observation() {
        for json in [
            r#"{"type":"hello_ack","protocol_version":1,"worker_version":"test","features":[],"model_loaded":false,"stub":false,"npu":"bundle_missing"}"#,
            r#"{"type":"embeddings","id":1,"vecs":[],"dim":768,"backend":"cpu","stub":false,"npu":"bundle_missing"}"#,
        ] {
            let response: EmbedResponse = serde_json::from_str(json).unwrap();
            assert_eq!(
                serde_json::to_value(response).unwrap()["npu"],
                "bundle_missing"
            );
        }
    }

    #[test]
    fn request_response_json_round_trip() {
        for req in [
            EmbedRequest::Hello {
                protocol_version: EMBED_PROTOCOL_VERSION,
                client: "winsearchd".into(),
                client_version: "0.1.0".into(),
            },
            EmbedRequest::EmbedTexts {
                id: 7,
                texts: vec!["budget spreadsheet".into()],
            },
            EmbedRequest::EmbedAudio {
                id: 8,
                path: "C:\\a.wav".into(),
                window_ms: Some((0, 30_000)),
            },
            EmbedRequest::EmbedImage {
                id: 9,
                path: "C:\\p.png".into(),
                token_budget: None,
            },
            EmbedRequest::Status { id: 10 },
            EmbedRequest::Shutdown { id: 11 },
        ] {
            let json = serde_json::to_string(&req).unwrap();
            let back: EmbedRequest = serde_json::from_str(&json).unwrap();
            assert_eq!(req, back);
        }
        let resp = EmbedResponse::Embeddings {
            id: 7,
            vecs: vec![vec![0.5, -0.25]],
            dim: 2,
            backend: "cpu".into(),
            stub: true,
            npu: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"embeddings\""));
        let back: EmbedResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, back);
        assert_eq!(back.id(), Some(7));
    }

    #[test]
    fn worker_status_npu_field_is_optional_on_the_wire() {
        let legacy = r#"{"type":"worker_status","id":1,"model_loaded":true,"backend":"cpu","stub":false,"dim":768,"uptime_secs":3,"jobs_served":2}"#;
        let EmbedResponse::WorkerStatus { npu, .. } = serde_json::from_str(legacy).unwrap() else {
            panic!("expected worker_status");
        };
        assert_eq!(npu, None);
        let current = EmbedResponse::WorkerStatus {
            id: 1,
            model_loaded: true,
            backend: Some("npu".into()),
            stub: false,
            dim: 768,
            uptime_secs: 3,
            jobs_served: 2,
            npu: Some("ready".into()),
        };
        let json = serde_json::to_string(&current).unwrap();
        assert!(json.contains(r#""npu":"ready""#));
        assert_eq!(serde_json::from_str::<EmbedResponse>(&json).unwrap(), current);
    }

    #[test]
    fn error_carries_optional_id() {
        let err = EmbedResponse::Error {
            id: None,
            code: EmbedErrorCode::VersionMismatch,
            message: "speak v1".into(),
        };
        let json = serde_json::to_string(&err).unwrap();
        let back: EmbedResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
        assert_eq!(back.id(), None);
    }

    #[test]
    fn stub_vectors_are_deterministic_unit_and_distinct() {
        let a = stub_vector("hello world", STUB_DIM);
        let b = stub_vector("hello world", STUB_DIM);
        assert_eq!(a.len(), STUB_DIM);
        assert_eq!(a, b);
        let norm: f64 = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "stub not unit-norm: {norm}");

        let c = stub_vector("completely different text", STUB_DIM);
        assert_ne!(a, c);
        // Near-orthogonal for distinct inputs (sanity, not a guarantee).
        let cos: f64 = a
            .iter()
            .zip(&c)
            .map(|(x, y)| f64::from(*x) * f64::from(*y))
            .sum();
        assert!(cos.abs() < 0.25, "suspicious similarity: {cos}");
    }

    #[test]
    fn batch_stub_preserves_order() {
        let texts = vec!["one".to_string(), "two".to_string(), "three".to_string()];
        let vecs = stub_embed_texts(&texts, STUB_DIM);
        assert_eq!(vecs.len(), 3);
        assert_eq!(vecs[0], stub_vector("one", STUB_DIM));
        assert_eq!(vecs[2], stub_vector("three", STUB_DIM));
    }
}
