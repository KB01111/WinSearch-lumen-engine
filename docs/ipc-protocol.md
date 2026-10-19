# WinSearch IPC Protocol

Transport: named pipe, one instance per user session.
Pipe name: `\\.\pipe\winsearch-<user SID>` (Rust side uses the full path;
the .NET client passes the bare name `winsearch-<SID>` to
`NamedPipeClientStream`).

Security: the pipe is created with an explicit DACL granting access to the
current user's SID only, plus `PIPE_REJECT_REMOTE_CLIENTS`. No network listener
exists. Protocol version: **1** (`PROTOCOL_VERSION` in `ws-protocol`,
`ProtocolJson.Version` in `WinSearch.Core`).

## Framing

Each message is a frame:

```
+-------------------+---------------------------+
| u32 LE length     | UTF-8 JSON payload        |
+-------------------+---------------------------+
```

- Maximum frame size: 16 MiB (`MAX_FRAME_BYTES`).
- A clean end-of-stream (0 bytes at a frame boundary) means the peer closed.
- A truncated frame is an error and tears down the connection.

The payload encoding is JSON for debuggability; a compact binary encoding is
only adopted if profiling shows a measurable benefit.

Content results add optional `excerpt` and default-empty `matching_moments`
without changing protocol version 1. Each moment contains `excerpt`, optional
`location` (the existing typed `page`, `timestamp`, or `line` union), and
optional `thumbnail_path`. The primary `location`/`excerpt` come from the first
matching chunk; metadata is never borrowed from an unrelated chunk in that
file. Exact content results can include these fields with no embedding model.
Old result payloads deserialize to a null excerpt and empty moments in both
Rust and C#. Native page/media extraction and explorer preview consumers are
separate implementation tasks.

## Message envelope

All messages carry a `type` discriminator (snake_case). Requests carry `id`
(`u64`, monotonically increasing per connection). Responses echo the `id`.

### Client → engine (`ClientMessage`)

| type | fields | description |
|---|---|---|
| `hello` | `protocol_version`, `client`, `client_version` | Must be the first message. Version mismatch → `error` + disconnect. |
| `ping` | `id` | Heartbeat; answered by `pong`. |
| `search` | `id`, `query` | Execute a search; results stream as `results` frames. |
| `related` | `id`, `query` | Find content related to a current bound indexed source; requires `search.related`, `search.semantic` and `search.outcomes`. |
| `cancel` | `id` | Cancels the in-flight request with this id. |
| `get_status` | `id` | Snapshot request; answered by `status` with `id` set. |
| `model_download_start` | `id` | Begin (or resume) the embedding-model download; answered by `model_status` with `id` set. Idempotent while running. |
| `model_download_cancel` | `id` | Cancel the in-flight model download; partial files stay for resume (`paused`). |
| `model_import` | `id`, `path` | Import model files from a local file or directory (copied + verified). |
| `model_delete` | `id` | Delete the downloaded model files. |
| `get_model_status` | `id` | Snapshot request; answered by `model_status` with `id` set. |
| `get_config` | `id` | Snapshot of engine search policy; answered by `config` with `id` set. |
| `set_config` | `id`, `config` | Persist engine search policy; answered by `config` with `id` set (`invalid_request` on unknown `preferred_backend`). |
| `get_indexing_config` | `id` | Read durable folder, exclusion, code and pause policy. |
| `set_indexing_config` | `id`, `config` | Validate and atomically save indexing policy and root visibility; answers `indexing_config` and pushes it to connected clients. Invalid paths, hours, extensions or list limits return `invalid_request`. |

### Engine → client (`ServerMessage`)

| type | fields | description |
|---|---|---|
| `hello_ack` | `protocol_version`, `engine_version`, `features[]` | Capability discovery, including `search.exact`, `search.outcomes` and optional operations described below. Code availability still follows the opt-in policy. |
| `pong` | `id` | Heartbeat reply. |
| `results` | `id`, `batch[]`, `done`, `outcome?` | Streamed result batches; terminal frame has `done: true`. Peers advertising `search.outcomes` include a typed terminal outcome. |
| `status` | `id?`, `status` | `id: null` is a pushed update (indexing progress); `id` set answers `get_status`. |
| `model_status` | `id?`, `status` | `id: null` is a pushed update (download progress / completion); `id` set answers a model request. |
| `config` | `id?`, `config` | `id` set answers `get_config` or `set_config`. |
| `indexing_config` | `id?`, `config` | `id` set answers get/set; absent/null is a policy change notification. |
| `error` | `id?`, `code`, `message` | `id: null` is connection-scoped (e.g. handshake failure). |

### Payload models

- `SearchQuery`: `text`, `mode` (`auto|apps|files|code`), `limit`,
  `include_semantic`, `categories[]`, `extensions[]`.
- `RelatedQuery`: bound `source` (`EntryReference`), `limit` (default 20,
  range 1..10000), `categories[]` (at most 32), `extensions[]` (same search filter bounds).
- `SearchResult`: `id`, `kind`, `title`, `subtitle`, `match_explanation`,
  `score`, `path?`, `location?`, `thumbnail_path?`.
- `MatchLocation` (discriminator `kind`):
  `page {page}` | `timestamp {seconds, end_seconds}` | `line {line}` |
  `notebook {cell_ordinal, line, end_line}`. Notebook cells are zero-based;
  cell-local line ranges are one-based and inclusive.
- `EngineStatus`: `state` (`idle|indexing|paused|error`), `current_activity`,
  `files_discovered`, `files_indexed`, `segments_indexed`, `queue_depth`,
  `model_loaded`, `index_size_bytes`.
- `ModelStatus`: `state` (`not_downloaded|downloading|paused|ready|error`),
  `bytes`, `total_bytes`, `error?`.
- `EngineConfig`: `semantic_enabled` (default `true`), `preferred_backend`
  (`auto|cuda|vulkan|cpu`, default `"auto"`).
- `IndexingConfig`: `roots[]`, `excluded_paths[]`, `excluded_extensions[]`,
  `code_enabled`, `code_roots[]`, `paused`, `mode` (`always|scheduled|manual`),
  `index_on_battery`, `respect_battery_saver`, `hours_start`, `hours_end`.
  Defaults: empty lists, code and pause false, mode `always`, both power flags
  true, hours 60 and 420. Missing fields use these defaults; protocol stays v1.
- `ErrorCode`: `protocol_version_mismatch`, `invalid_request`, `internal`,
  `not_ready`, `cancelled`, `unsupported`.

Indexing minutes range from 0 through 1439; windows include the start and
exclude the end. Overnight windows wrap; equal endpoints mean all day.
Manual mode runs when the user has not paused it. Pause precedence is manual
pause, battery saver, disallowed battery power, then schedule. An unknown
battery state pauses when `index_on_battery` is false; otherwise it permits
work unless another rule pauses it. Unknown saver state alone permits work.
Paused status includes the actual reason; exact interactive searches continue.

Root and excluded-path membership uses case-insensitive Windows component
boundaries; an explicitly selected subroot can override an excluded ancestor.
Paths must be absolute, without NUL or parent traversal. Limits are 128 roots
per kind, 512 excluded paths and 256 extension rules. Extensions normalize one
leading dot and ASCII case, then must contain only ASCII letters/digits.
Missing/offline roots remain stored. Fresh installations select no personal
roots. Legacy enabled roots migrate once, excluding the synthetic `@apps`
catalog; no extra standard folders are added. The complete policy is one JSON
record at `meta.indexing_config`, in the same transaction as `roots.enabled`.
Removing roots preserves rows but hides names/content/vectors immediately and
prevents worker claims/commits. Adding them back starts reconciliation.

Reparse ancestors and configured reparse roots are never traversed. Before
content and visual-embedding opens, the shared metadata-only guard rejects
OFFLINE, RECALL_ON_OPEN and RECALL_ON_DATA_ACCESS file attributes and reparse
paths. Those files can remain searchable by name. Ordinary content inside
recognized projects is restricted to screenshots; secrets/generated files
remain excluded. Non-explicit media folders with at least 300 files of one
kind and at least 70% machine-like names are name-only. Classification examines
at most 4096 directory entries without reading content; explicit roots bypass
dataset classification. Camera/screenshot prefixes are not machine-like.

For native tests, `WINSEARCH_DATA_DIR` overrides the whole data root with an
absolute path; relative values fail clearly. `WINSEARCH_INSTANCE_ID` optionally
adds an instance suffix and must be 1..48 ASCII letters/digits/underscore/hyphen.
Pipes retain their existing per-user SID and append `-<id>`; overridden engine,
embed and app mutexes/show events use `<existing-name>-<SID>-<id>`.
Without the override all default names remain compatible. Child processes
inherit both environment variables. `scripts/test-indexing-isolation.ps1`
exercises two generated-data daemon instances and independent shutdown.

## Semantics

- **Progressive search**: exact matches stream first; semantic results append
  in later batches under the same `id` when `include_semantic` is set,
  `semantic_enabled` is true, and the embedding model is ready. Code mode also
  requires code opt-in and a category filter that admits code. Semantic hits
  are deduplicated against already-streamed
  exact entry IDs, scored in the hybrid band (`0.45 + 0.45·cosine` with
  cosine ≥ `0.65`, `+0.10` exact-token-in-name boost, `+0.02` recency), and
  carry `match_explanation` (`"semantic name match · similarity 0.XX"`). The
  client discards batches from superseded requests (sequence guard) so older
  searches never overwrite newer ones.
- **Code search**: `mode: code` uses only the separate code index. Exact
  definitions precede references and chunk FTS. Semantic chunks append after
  exact results, suppressing chunks containing an exact hit at the same file
  and line. Results carry the actual excerpt, line/cell location and stable
  code result ID. Disabled/removed roots are hidden before candidate limits;
  policy and parse revisions fence in-flight queries through delivery.
- **Cancellation**: cancelling the client-side token sends `cancel`; the engine
  cooperatively stops and closes the stream with `done: true`.
- **Liveness**: clients ping periodically; a dropped pipe triggers automatic
  reconnect (with engine restart if needed) and a fresh handshake.
- **Engine independence**: `winsearchd` runs with or without any client
  attached; closing the UI never interrupts indexing.
- **Model lifecycle**: `model_download_start` resumes `<name>.part` files via
  HTTP `Range` and is idempotent while running; progress and completion
  arrive as pushed `model_status` frames (throttled to ~4 Hz server-side).
  A concurrent `model_import` is refused with `error: invalid_request`
  (like `model_delete`, which also refuses while a download/import is in
  flight). Every message above is additive: `PROTOCOL_VERSION` stays **1**.

## Versioning

Breaking wire changes bump `PROTOCOL_VERSION`. Both sides refuse mismatched
versions at handshake with `error: protocol_version_mismatch` and disconnect.
Additive fields use `#[serde(default)]` semantics and do not bump the version.

## Indexed entry resolution

Peers advertising `entry.resolve` support a metadata-only operation for native
preview and action consumers. It resolves current ordinary filesystem IDs
(`e<positive entry id>`). Peers additionally advertising `entry.resolve.code`
resolve opted-in code-file IDs (`c<positive code-file id>`). IDs are canonical
positive signed 64-bit decimals: no signs, leading zeros, paths or query-specific
semantic/symbol IDs. Catalog application resolution remains separate work.
The caller supplies an indexed identity, never a source path:

```json
{"type":"resolve_entry","id":41,"reference":{"result_id":"e23","source_revision":null}}
```

`source_revision` is optional/nullable for initial resolution. Supplying a prior
value requires an exact match to the current source, content revision, model
generation, engine incarnation and access-policy epoch. Source replacement,
engine restart or revoked/restored grants expire it. A stored native file identity
must still match the current source before inventory reconciliation. Unrelated
vector-cache updates and unchanged policy saves do not. A supplied proof must be
nonblank and at most 64KiB UTF-8. If an incarnation cannot be created, metadata is
`not_ready`; exact name search remains available without bound references.
Unknown fields inside `reference`, including a caller-supplied `path`, are refused.
Malformed IDs produce `invalid_request`; unavailable, denied or changed sources
produce `not_ready`, with a sanitized message and no entry payload.

The correlated `resolved_entry` reply contains `entry`: `result_id`, `root_path`,
`path`, `name`, `kind`, current `size_bytes`, nullable `modified_unix_seconds`,
nullable `index_revision`, opaque `source_revision` and optional `content_sha256`.
The latter is a canonical lowercase SHA-256 of the complete admitted source bytes
only when established by current extraction; it is currently available for code
and notebook files. Notebook hashes include the complete original JSON bytes,
while indexed text excludes outputs. Ordinary extraction can also establish a
complete-source byte hash within its64MiB hash budget. Existing32MiB document/
image/PDF admission remains unchanged; larger streaming media and legacy rows
without established provenance retain an unknown/omitted hash. A byte hash does
not imply that every page/window/chunk was extracted.
Metadata-only entries
have no `index_revision`; stale extracted content cannot supply one for a replaced
source. A positive revision does not imply complete extraction or uncapped page
coverage. **The source revision is a metadata/identity proof, not a content hash.**
It must never be supplied as Lumen's file/content hash.

Native clients must resolve/revalidate references immediately before preview,
open/reveal or media-source access, enforce their own admitted root boundary, and
cancel stale work. This endpoint reads attributes/identity only: it does not read
source content, render, start inference, grant a webview filesystem scope or
implement the content-preview/playback route. Lumen's trusted Rust host keeps
the IPC and native paths out of a direct React transport.

The C# client checks the capability before sending, snapshots mutable caller
arguments, correlates replies and rejects foreign IDs/proofs, missing metadata
and paths outside the returned local root. It also validates hash format and
requires a positive content revision for any hash. Code/file reference domains
must match the entry kind. Those lexical checks complement the
engine's native residency/reparse/current-policy checks; they do not replace them.
The additive operation retains protocol version1. The
[async Rust client](headless-client.md) performs equivalent reply checks and
uses the same shared DTOs. The cached-text routes below are additive;
larger-source/legacy provenance recovery and Lumen host integration remain open.

Peers advertising `search.entry_references` add optional `entry_reference` to
ordinary and code search results, containing the canonical indexed ID and a
nonempty current proof. `SearchResult.id` remains the query-specific selection ID:
consumers must use the supplied reference instead of parsing semantic or symbol
IDs. Synthesized/application results have no reference. References are bound and
source-validated again after waiting for the response writer, with cancellation,
request identity and query epochs checked before delivery. Clients accept absent
fields from older peers but reject malformed/unbound references and code/file
domains inconsistent with the result kind or requested search mode. The trusted
host must still revalidate its grants and resolve again before source access.

## Cached text previews

Peers advertising `entry.preview.text` accept an ordinary indexed identity and
an optional actual page, line or timestamp. No caller source/cache path is accepted:

```json
{"type":"preview_entry","id":42,"request":{"reference":{"result_id":"e23","source_revision":null},"location":{"kind":"page","page":2}}}
```

The correlated `entry_preview` contains `preview.entry` (the same current metadata
as `resolved_entry`), `state`, `parts`, `truncated` and `extraction`. Each text part
has `kind`, UTF-8 `text` and nullable actual `location`. States are `ready`,
`not_indexed`, `no_cached_text` and `location_unavailable`; only `ready` has text.
Requests accept pages1..5000, lines1..67108864 and finite ordered timestamps within
366days. Pages and timestamp pairs select exact stored positions; a line selects
cached text covering that line and returns its actual start line. A trailing newline
terminates the preceding line; it does not make a following empty line available.
A missing position
never causes a fabricated page/line/time or a fallback to unrelated text.

The reply is bounded to **64KiB aggregate UTF-8 text, 64parts and eight extraction
kinds**, preserving codepoint boundaries. `truncated` reports the transport text/part
limit. Each extraction item reports `complete_within_limit`; `source_units`,
`indexed_units` and `capped` are currently null because full source coverage has not
been persisted. Completion within a limit is not full-document completion.

SQLite selects only bounded text prefixes/locations through current source/root/
revision guards before the row limit. It does not load vector inputs, artifact
paths or all content chunks. This route starts no parser or inference and reads no
source content; already cached text remains available while indexing is paused.
Current native source metadata, model generation, revision and policy proof are
revalidated after the query and again after acquiring the response writer. Cancelled,
disconnected or replaced requests cannot reuse an earlier delivery decision.

At most two preview jobs are admitted by the daemon engine; further requests return
`not_ready` with a busy message. Work runs away from the connection reader, so a
database wait does not block in-band Ping/Cancel. Admission is retained through
delivery/cleanup. This bound does not add a server-side deadline to the existing
synchronous pipe writer; a raw client that stops reading can hold admitted work
until its connection closes. Rust/C# preview clients own and close their read
connections on cancellation or the35second total deadline. The C# client has two
local preview slots and leaves its UI/control connection independent.

Both clients require the capability and validate identity/proof, local root,
state, kinds, locations and reply budgets. Capability presence establishes cached
text only. Raster/page thumbnails, media playback, uncached/on-demand rendering,
larger-source/legacy hashes and the consumer UI remain open.
Protocol version remains1. See [ordinary-text validation](validation/2026-10-18-preview-text/README.md).

Peers advertising `entry.preview.code` accept canonical code-file `c` references
through the same `preview_entry` operation, job quota and delivery fences:

```json
{"type":"preview_entry","id":43,"request":{"reference":{"result_id":"c7","source_revision":"<current opaque proof>"},"location":{"kind":"notebook","cell_ordinal":1,"line":1,"end_line":2}}}
```

Code locations are null, a positive source line, or a notebook cell ordinal with
positive ordered cell-local lines, bounded to67108864. Cell ordinals are zero-based.
Page/timestamp coordinates are rejected for code; notebook coordinates are rejected
for ordinary entries. Code parts always have `kind: "code"` and actual line/notebook
locations. A requested notebook range selects chunks that cover that range in the
requested cell. Replies retain actual cached ranges, clipped to returned text if
the transport budget truncates a part; missing cells/ranges never select another
cell. Notebook outputs are excluded even though the source byte hash covers the
complete notebook JSON.

The code store checks enabled repository, current parsed revision/provenance and
native source admission before selecting bounded text prefixes. SQL joins the
current source snapshot/hash and parse version again before applying location and
row limits. No vectors, parser inputs or source bytes are loaded by this route.
The same64KiB/64part bounds and final current-source checks apply to both domains.
Current indexed code reports completion within parser limits; full source totals
and cap state remain null. Rust/C# require the separate capability, validate code
parts and actual ranges, and refuse legacy peers before sending a preview request.
See [code-preview validation](validation/2026-10-18-code-preview/README.md).

Ordinary source hashing runs within the existing owned extraction child, using
64KiB buffers, the same deadline and resident/reparse/native-identity checks.
Supported sources are hashed before and after parsing. An advisory native
read-cache break notification guards parsing and the final digest, without
requiring write acknowledgement. Opened identity and metadata checks remain;
timestamp equality alone cannot detect every rapid change/restore. Notification
buffers and handles remain owned through cancellation completion inside the
bounded extraction worker. Missing/ungrantable notifications or native
identity/change time leave the hash unknown. Observed source changes return the
internal `SourceChanged` outcome. The unchanged change/restore assertion and
native parser evidence are recorded in the
[outcome/integrity checkpoint](validation/2026-10-18-search-outcomes/README.md).
The daemon discards/requeues that work without
charging a poison attempt. Hashes are committed with chunks/cursor and explicitly
bound to the parsed content revision, so a retained hash from an older writer
cannot describe a later revision. Different or missing established hashes across
continuations invalidate old derived content. Media cache restoration also checks
the established digest before replacing an artifact. Model-only vector resets
preserve valid byte provenance. Resolution/preview still read metadata and cached
content only; they do not rehash source bytes. The trusted host must keep its own
current grants and content-access freshness checks.

### Extension filters and candidate admission

Peers advertising `search.filters` accept optional `SearchQuery.extensions`.
It selects indexed source-file extensions in ordinary and code queries, including
semantic retrieval. An empty list preserves existing behavior. Each of up to64
values has1..32 ASCII letters/digits/`+`/`-`/`_`, with an optional leading dot;
matching is case-insensitive. Wildcards, path syntax and malformed/oversized lists
return `invalid_request`. Folders and synthetic calculator/settings hits cannot
satisfy a nonempty extension filter. Application filtering uses the indexed
shortcut/executable extension, not its launch target's extension.

Mode, category and extension admission precede each exact/name/FTS/content/path
candidate cap. Semantic entry filtering precedes vector top-k; code exact and
semantic filters reuse the existing admitted-file predicates before their limits.
The connection-local request scope does not alter native source/root admission or
metadata/preview access. A subsequent search replaces its previous request scope.
Candidate/result bounds, model space, ranking and semantic thresholds are unchanged.

Rust/C# clients reject nonempty extension filters on peers without the capability,
validate request budgets and reject replies outside the selected extensions.
C# snapshots the query and its lists before awaiting writes. Omitted extension
fields remain compatible with older clients. Finer file-kind
scopes remain separate unfinished contracts.

### Related content search

`search.related` adds a separate `related` request; it never sends an empty or
filename query through ordinary `search`. The caller supplies a current bound
`EntryReference`, not a path, query text, vector or model override. Unknown query
fields are rejected. Code sources additionally require `code`; nonempty extension
filters require `search.filters`. Capability support does not establish model or
source-text availability.

```json
{"type":"related","id":8,"query":{"source":{"result_id":"e42","source_revision":"current-proof"},"limit":20,"categories":["document"],"extensions":["pdf"]}}
```

The engine obtains admitted indexed text through the existing cached preview
contract, bounded to 64 KiB and 64 parts. It preserves Lumen's text-based Related
behavior: sources without indexed text are unavailable, including media without
OCR/text. It does not parse or hydrate source content, seed with a filename,
download a model or start a second inference runtime. Cached text can be partial;
this operation does not claim a whole-source embedding. Code/notebook sources
use current parsed code text and the existing corroborated code-query recipe.

Source exclusion and category/extension/source-policy admission precede vector
top-k or code candidate limits. Ordinary candidates use content vectors, not
name-only vectors. Code remains in its isolated scope. Related has no exact
stage or exact-hit suppression and ranks admitted candidates by similarity,
retaining existing semantic thresholds and model-space requirements.

The daemon admits at most two Related workers. Cached lookup and inference run
outside the connection read loop so controls and cancellation remain readable.
Missing model/text, disabled policy and unavailable resources produce a fixed
public error plus a failed empty terminal outcome. Inference/SQL failures do not
fall back to ordinary results. No eligible scoped vectors produce complete /
degraded(`no_vectors`); a healthy below-threshold lookup is complete / ready with
zero hits. Current request identity, cancellation, index/model epochs and the
bound seed are rechecked under the final writer, even for an empty result.
Superseded/canceled requests terminate empty; reused request IDs cannot receive
the old worker's terminal frame.

Both typed clients require outcomes and semantic support, reject a returned seed,
unbound references, wrong domains/filters and disabled semantic completion, and
bound delivery to the requested limit and 64 frames. The .NET client also bounds
pending Related frames to two and reports overflow instead of dropping a frame.
The Rust client retains its owned read lane and a total deadline of 35 seconds. Installed
consumer migration and native performance acceptance remain separate gates.

### Terminal search outcomes

Peers advertising `search.outcomes` include `outcome` on every terminal
`results` frame and omit it on progressive frames. Legacy omission remains
unknown; clients must not infer successful semantic retrieval from it.

```json
{"type":"results","id":7,"batch":[],"done":true,"outcome":{"disposition":"complete","semantic":{"phase":"degraded","reason":"no_vectors"}}}
```

`disposition` is one of `complete`, `superseded`, `canceled` or `failed`.
Only `complete` carries `semantic`; interrupted terminal batches are empty and
have no semantic state. Previously streamed hits belong to the interrupted
request and must not become a successful snapshot. Request-ID reuse suppresses
old work entirely so it cannot terminate the replacement request.

For completed requests, `semantic.phase` is `disabled`, `ready` or `degraded`.
Disabled/ready have no reason; degraded requires exactly one closed reason:
`model_unavailable`, `no_vectors`, `embedding_failed`, `retrieval_failed` or
`index_changed`. No worker exception text or paths are reason codes. Without
requested semantics, only disabled is valid. Readiness requires a successful
query embedding and lookup against eligible, current, dimension-compatible
vectors. Zero matches after the unchanged cosine threshold or duplicate
suppression can still be ready; model-file presence alone cannot establish it.
No eligible vectors in the requested scope is degraded/no_vectors.

Optional semantic failure preserves exact results. Exact database failure is
an `error` followed by an empty failed terminal frame, with a fixed public
message. Cancellation and source/model/query checks are repeated after obtaining
the writer. A changed epoch or removed candidate closes as superseded with no
terminal hits; it is not silently reported as a genuine empty match. Existing
native source checks, limits, model recipes and ranking thresholds remain in
force. A newer in-band search cancels the preceding request through the existing
cancel path. Transport failure may prevent any terminal frame from arriving.

Rust/C# clients and the CLI validate presence, terminal placement, phase/reason
pairings and request consistency. Unknown fields/codes and numeric outcome enums
are rejected. CLI text includes the terminal outcome; `--json` preserves the
existing result array on stdout and emits the typed outcome on stderr. An
interrupted CLI request exits unsuccessfully without a success array. This
contract does not establish the cause or fix of the retained native pause/resume
failure; native and installed consumer acceptance remain separate gates.

## Internal embed-worker protocol (P3.2)

`winsearchd` talks to its inference worker `winsearch-embed` over a **second**
per-user pipe, `\\.\pipe\winsearch-embed-<SID>`, built on the same
`ws-platform` DACL machinery (`PIPE_REJECT_REMOTE_CLIENTS`, current user
only). Framing is the same length-prefixed JSON codec (16 MiB cap), but the
message set and version are independent: `EMBED_PROTOCOL_VERSION` (currently
**1**) lives in the `ws-embed-protocol` crate, which is the single definition
both sides code against. This protocol is **internal**: it is never mirrored
to `WinSearch.Core` and `PROTOCOL_VERSION` above is untouched by it.

`winsearchd` is the only client (one connection at a time; concurrent
connections serialize). The semantic pipeline (P3.5) batches **128 names**
per scheduler round-trip through the supervisor's bounded call; larger
`embed_texts` inputs are still split into one `embed_texts` job per 256-text
chunk. The `extract_jobs` claim order is `priority DESC, id DESC` (new files
first, LIFO within priority). Every scheduler batch is bounded by
**`EMBED_JOB_TIMEOUT` = 120 s** (`embed_texts_bounded`: helper thread +
`recv_timeout`, `conn` held only across ensure+connect+send; on timeout the
watchdog cancels the helper's registered pipe read and kills the owned
worker, failing the batch as `Transport` so claims release for retry instead
of stalling the scheduler). Batch failures increment attempts; at
**5 attempts** the job is poison-pilled (`failed`, never auto-claimed). A
mid-batch entry change requeues the fresh data (attempts untouched) instead
of upserting a stale vector.

| direction | `type` | fields | description |
|---|---|---|---|
| → worker | `hello` | `protocol_version`, `client`, `client_version` | First message per connection; doubles as the health ping. Mismatch → `error{version_mismatch}` + close. |
| → worker | `embed_texts` | `id`, `texts[1–256]` | Embed a batch; answered by `embeddings`. |
| → worker | `embed_audio` | `id`, `path`, `window_ms?` | Plumbing lands in P3.2; vectors in Phase 4 (real worker answers `unsupported` until mtmd is wired). |
| → worker | `embed_image` | `id`, `path` | Same Phase-4 note as audio. |
| → worker | `status` | `id` | Health probe; answered by `worker_status`. |
| → worker | `shutdown` | `id` | Acknowledge with `shutdown_ack`, then exit (idle-exit stays the normal path). |
| worker → | `hello_ack` | `protocol_version`, `worker_version`, `features[]`, `model_loaded`, `stub` | `features` always has `embed_texts`; audio/image tags appear only when servable (stub mode, or mtmd from Phase 4). |
| worker → | `embeddings` | `id`, `vecs[]`, `dim`, `backend`, `stub` | One L2-normalized vector per input, in order; plain JSON f32 arrays (binary encoding only if profiling demands it). |
| worker → | `worker_status` | `id`, `model_loaded`, `backend?`, `stub`, `dim`, `uptime_secs`, `jobs_served` | Self-reported liveness. |
| worker → | `error` | `id?`, `code`, `message` | `code`: `version_mismatch` \| `invalid_request` \| `model_not_available` \| `embed_failed` \| `unsupported`. Only transport failures trigger a supervisor restart; `error` answers are definitive for the job. |
| worker → | `shutdown_ack` | `id` | Shutdown accepted. |

Worker lifecycle: single-instance mutex `Local\WinSearch.Embed.v1`; model
loads lazily on the first job (missing file → `model_not_available`, worker
stays alive for a later retry after download); idle exit after 5 minutes
without work — process exit returns all GPU memory, which is the full GPU
release. Supervisor: spawn on first request (never before), handshake every
connection (refreshes the cached `model_loaded`/`stub` behind
`EngineStatus.model_loaded`), restart with backoff (`250 ms · 2ⁿ`, 5 s cap)
after a transport failure and retry the in-flight job exactly once.

Test/ops knobs (CLI flags win over env): `--pipe/--idle-secs/--stub/
--model/--backend/--stub-delay-ms`, else `WS_EMBED_PIPE/WS_EMBED_IDLE_SECS/
WS_EMBED_STUB/WS_EMBED_MODEL/WS_EMBED_BACKEND/WS_EMBED_STUB_DELAY_MS`, else
compiled defaults. Supervisor side: `WS_EMBED_BIN` (worker exe override),
`WS_EMBED_PIPE`, `WS_EMBED_BACKOFF_MS`. `WS_EMBED_STUB=1` serves
deterministic stub vectors (FNV-1a seeded, 768-d, unit-norm) with no model
file; `WS_EMBED_TEST_MODEL` gates the real-model tests (never committed).
