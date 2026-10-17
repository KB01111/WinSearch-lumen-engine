# WinSearch Architecture

WinSearch is a native Windows launcher + universal search app, ported from
DigUp (macOS/Swift, MIT) and rebuilt on Windows-native stacks: a Rust
indexing/search engine (`winsearchd`) and a WinUI 3 (.NET 8) frontend, talking
over a per-user named pipe.

```
+---------------------------------------------------------------+
|  WinSearch.App (WinUI 3)                                      |
|  OverlayWindow · SettingsWindow · TrayIcon · HotkeyManager     |
+------------------------------+--------------------------------+
                               | WinSearch.ViewModels (MVVM)
                               | WinSearch.Core (IPC + protocol + settings)
+------------------------------v--------------------------------+
|  \\.\pipe\winsearch-<SID>   (per-user DACL, no network)         |
+------------------------------+--------------------------------+
                               | ws-protocol (framed JSON, v1)
+------------------------------v--------------------------------+
|  winsearchd (Rust)                                            |
|  ┌────────────┐ ┌──────────┐ ┌──────────────┐ ┌────────────┐  |
|  │ crawl.rs   │ │watcher.rs│ │ catalog.rs   │ │ engine.rs  │  |
|  │ dir scan + │ │ReadDir.. │ │ Start Menu + │ │ search     │  |
|  │ exclusions │ │ ChangesW │ │ .lnk resolve │ │ pipeline   │  |
|  └─────┬──────┘ └────┬─────┘ └──────┬───────┘ └─────┬──────┘  |
|        └────────── indexer.rs ──────┘               │         |
|                  (queue, workers, drives)           │         |
+--------------------------+--------------------------+---------+
                           | ws-index (rusqlite, FTS5)
                           v
              %LOCALAPPDATA%\WinSearch\db\index.db
```

## Components

### `ws-protocol` (crates/ws-protocol)

Typed IPC messages (`ClientMessage`/`ServerMessage`) and the framing codec
(4-byte LE length + UTF-8 JSON, 16 MiB cap). Protocol version is **1**; see
`ipc-protocol.md` for the wire contract.

### `ws-platform` (crates/ws-platform)

Win32 plumbing: named pipe with a per-user DACL and
`PIPE_REJECT_REMOTE_CLIENTS`, current-user SID, data paths under
`%LOCALAPPDATA%\WinSearch`, and a named-mutex single-instance guard.

### `ws-index` (crates/ws-index)

The search index — no Windows dependencies, fully unit-tested anywhere.

- **Storage**: SQLite (WAL, `synchronous=NORMAL`) with an FTS5 table over
  names + path keywords. Schema in `store.rs`; upserts are batched per
  directory in one transaction (crash-safe: any interruption leaves the DB
  consistent and the crawl resumable).
- **Identity**: every entry stores `(volume_serial, file_id)`, so renames and
  moves can be matched to their previous row (`find_by_file_id`).
- **Crawl queue**: durable work list with atomic claims; claims from a
  crashed run are released on startup and re-processed (idempotent upserts).
- **Retrieval** (`search.rs`): FTS5 prefix candidates + a bounded substring
  fallback (for mid-word matches like `code` → `VSCode`), merged and ranked
  deterministically (exact name > prefix > substring > path; small boosts for
  apps and recency; small penalty for deep paths). Every hit carries its match
  explanation.
- **Calculator** (`calc.rs`): recursive-descent arithmetic evaluator
  (+ - * / % ^, functions, constants). Only arithmetic syntax is accepted —
  nothing is ever executed. Queries like `report-2.pdf` are recognized as
  ordinary text, not math.
- **Settings catalog** (`settings.rs`): `ms-settings:` URI pages with
  name/keyword matching.

### `winsearchd` (bins/winsearchd)

- `crawl.rs` — per-directory scan with the exclusion policy: hidden/system  items, dev trees (`.git`, `node_modules`, `target`, …), and sensitive files
  (keys, certs, credential stores). Metadata comes from directory enumeration;
  identity lookups open files with `FILE_READ_ATTRIBUTES` only, so cloud
  placeholders (OneDrive) are never hydrated. Reparse points are indexed as
  files but never recursed into (no junction cycles).
- `watcher.rs` — `ReadDirectoryChangesW` per root (64 KiB buffer, subtree).
  Rename OLD/NEW pairs are fused into one `Renamed` event so the index
  preserves entry identity across renames and moves; buffer overflow triggers
  a subtree rescan.
- `indexer.rs` — orchestrator: bounded crawl worker pool (4 threads) draining
  the crash-safe queue, watcher event handling, the application catalog
  refresh, and a drive supervisor that preserves the index of disconnected
  volumes (`missing` flag) and reconciles them on reconnect. Plus the
  semantic pipeline (P3.5): `UpsertOutcome != Unchanged` (from `process_dir`
  batches, `point_upsert`, renames, and catalog refreshes) enqueues
  `extract_jobs(kind='name')` — `Inserted` at high priority (new files first),
  `Updated`/requeues low; claim order is `priority DESC, id DESC` (LIFO
  within priority). One scheduler thread (`winsearch-embed-sched`) drains the
  queue in batches of 128 names through the supervisor's bounded call
  (**120 s** job-IO watchdog: a hung worker fails the batch as `Transport`,
  claims are released for retry, the scheduler never stalls). The bounded
  path holds the supervisor `conn` mutex only across ensure+connect+send and
  releases it before the blocking reply read, so a timed-out helper never
  wedges later batches; the watchdog cancels the stuck helper's synchronous
  pipe IO (`CancelSynchronousIo` on exactly the registered thread) even for
  foreign workers, with owned-child kill failures logged. Results upsert
  `segments(kind='name', chunk 0)` idempotently (stable id, replace prior
  vector). After each embed the entry is re-resolved under lock and a
  mid-batch change (name/`mtime`/priority mismatch) requeues the fresh data
  instead of upserting a stale vector. Batch failures increment attempts;
  at **5 attempts** the job is poison-pilled (`failed`, never auto-claimed)
  with a health log. Deletes cascade via FK (`sweep`/`remove_tree`); claims left by a
  crash are released on open (`release_extract_claims`). A model swap
  (`check_compat == Incompatible`) purges all model vectors and requeues
  names and deferred content vectors, preserving exact chunks and locations.
- `catalog.rs` — Start Menu/Desktop scan; `.lnk` targets resolved via the
  shell's `IShellLinkW`.
- `engine.rs` — the search pipeline: calculator → settings → index hits,
  streamed as progressive batches with cooperative cancellation. `code:`
  queries map to the code mode (index lands in Phase 5) and return nothing
  rather than mixing in normal results. `status()` reports `model_loaded`
  from the embed supervisor's cache (real weights resident, or stub vectors
  flowing in test setups); `segments_indexed` counts `segments(kind='name')`;
  `current_activity` is `"Indexing"` while the crawl queue drains and
  `"Embedding names…"` while only the embed queue remains. The engine reaches
  the supervisor through `embed_supervisor()` (an `Arc` shared with the
  indexer's scheduler); the pipeline embeds through its bounded call.
- `embed_client.rs` — supervisor for the inference worker: spawns
  `winsearch-embed` on the first embed request (never before), handshakes
  every connection (health ping + version check + flag refresh), restarts
  with backoff after a transport failure and retries the in-flight job once.
  Definitive worker errors are never retried. Links no native inference code.
  The pipeline never calls `embed_texts` directly: `embed_texts_bounded`
  (helper thread + `recv_timeout`, **`EMBED_JOB_TIMEOUT` = 120 s**) bounds
  every scheduler batch; the bounded transaction holds `conn` only across
  ensure+connect+handshake+send and reads the reply without it, and on
  timeout the watchdog cancels the helper's registered pipe read
  (`CancelSynchronousIo`) plus kills the owned child (kill failures logged),
  so the batch fails for retry with no leaked holder and no stuck thread.

### `winsearch-embed` (bins/winsearch-embed) + `ws-embed-protocol`

- `winsearch-embed` — crash-isolated inference process serving the internal
  embed-worker protocol on `\\.\pipe\winsearch-embed-<SID>` (one client,
  `winsearchd`). Loads the model lazily on the first job (stays alive on
  `model_not_available` so a later download can succeed), warms up via
  `EmbedRuntime`, and exits after 5 minutes idle — process exit is the full
  GPU release. Single-instance mutex `Local\WinSearch.Embed.v1`.
  `--stub` serves deterministic 768-d unit vectors with no model file.
- `ws-embed-protocol` — the single definition of that protocol (own version
  `EMBED_PROTOCOL_VERSION`, never mirrored to the frontend): `hello`,
  `embed_texts`/`embed_audio`/`embed_image` jobs, `status`/`shutdown`, and
  the deterministic stub-vector math. See `ipc-protocol.md`.

### Frontend (src/app)

- `WinSearch.Core` — `EngineClient` / `IEngineClient` (async pipe client:
  correlation ids, streamed batches, cancellation, auto-reconnect with engine
  restart), protocol models, `AppSettings` (UI-owned preferences only).
- `WinSearch.ViewModels` — `MainOverlayViewModel` (debounce, superseded-query
  cancellation, append-only semantic streaming with `IsSemanticPending` and
  missing-model hint), `SettingsViewModel` (theme/hotkey/query memory plus AI
  model download/pause/resume/import/delete state machine, progress + speed
  mapping, and `get_config`/`set_config` policy sync), result item VMs.
- `WinSearch.App` — overlay window (one instance, reused; DPI/monitor-aware
  placement, acrylic, composition animations respecting reduced motion,
  semantic pending indicator + missing-model hint), tray icon, global hotkey
  with conflict detection and fallback, settings window (incl. AI model group).
- `WinSearch.Tests` — xUnit unit tests for ViewModel logic and state machines.

## Data locations

| Path | Contents |
|---|---|
| `%LOCALAPPDATA%\WinSearch\db\index.db` | entries + FTS + crawl queue + checkpoints + vector segments + embed queue |
| `%LOCALAPPDATA%\WinSearch\db\code.db` | code index (Phase 5) |
| `%LOCALAPPDATA%\WinSearch\models` | embedding models (Phase 3) |
| `%LOCALAPPDATA%\WinSearch\cache\thumbs` | thumbnail cache |
| `%LOCALAPPDATA%\WinSearch\logs` | engine logs (rolling daily) |
| `%LOCALAPPDATA%\WinSearch\settings.json` | UI-owned settings |

## Indexing lifecycle

1. Startup: open DB, release claims left by a crash (crawl + embed queues),
   enqueue every present root, start workers/watchers/catalog/drive
   supervisor and the embed scheduler. A model swap (`check_compat ==
   Incompatible`) first purges model vectors and requeues name/content
   embeddings while preserving exact text and durable parser progress.
2. Crawl workers claim directories, scan them, upsert children and enqueue
   subdirectories in per-directory batches, then mark the directory done.
   Every changed entry also enqueues a name-embed job (new files first, LIFO
   within priority).
3. The embed scheduler claims up to 128 name jobs, embeds the current entry
   names through the bounded supervisor call, and upserts one segment per
   entry (replace prior vector, stable id); failures release claims for retry.
4. When a root's queue drains, a generation sweep deletes entries that were
   not seen in this scan (files removed while indexing was away) — segments
   and pending jobs cascade via FK.
5. Watchers keep the index fresh between scans; renames keep row identity
   (and requeue the new name); deletes cascade to segments.
6. Disconnected volumes: entries are flagged `missing` (hidden from search,
   never deleted). On reconnect the root is rescanned and unflagged.

## Performance posture

- Bounded everything: 4 crawl workers, per-directory transactions, capped
  candidate sets (500 FTS + 500 substring) per query, capped results.
- The UI thread never touches the index; searches run on the connection thread
  and stream batches.
- Measurements live in `benchmarks.md` (kept up to date with the code).

## Privacy

Local-only by design: per-user named pipe, no network listener, no telemetry
anywhere in the codebase. The engine never reads file contents in Phase 2 and
never hydrates cloud placeholders. Sensitive files are excluded at the index
level.

## Durable content batches and deferred embeddings

The two content slots use the sibling `winsearch-extract` process. Text jobs
request 30 chunks per batch, with the existing 1200-character target and 15%
overlap. A cursor is a stable chunk ordinal for the current source revision.
The worker returns a truthful total and `next_cursor`; only a null cursor
finishes extraction. Continuations receive lower priority than new/short jobs.
Document parsing currently repeats within the existing 32 MiB input / 16 MiB
archive XML bounds to reconstruct the requested window; it does not hydrate
cloud placeholders. PDF page and phased media cursors are described below.

Additive tables `content_revisions`, `content_progress`, and `content_chunks`
extend schema version 2. The migration marker is `meta.content_batches_v1`.
Old FTS text is preserved, including numeric rows without vector segments.
Legacy locations are copied only when a segment's text actually equals that
FTS row; unavailable locations remain absent. Fresh chunks have unique
`(entry_id, job_kind, chunk_index)` identities, text, page/time/line/thumbnail,
an embedding flag, and durable embedding attempts/state. `content_fts.rowid`
references the chunk identity. Entries own these rows through cascading FKs.

`IndexStore::begin_content_batch` binds the precise filesystem source snapshot
(path, length, modification time, and file identity), durable source revision,
and model-space generation. `commit_content_batch` transactionally writes
exact text/locations, cursor/completion, and claim release. Replays and stale
revisions cannot advance a cursor. `content_progress` and `list_content_chunks`
expose the persisted state to later extractors. Watcher Modified events call
`invalidate_content_source` even for equal-length, same-second changes; the
existing entry `mtime` retains its seconds-based recency semantics.

`claim_content_vectors`, `release_content_vectors`, and `commit_content_vectors`
implement the separate deferred stage. Exact text is searchable before a
model exists. Production uses typed `EmbedError::ModelNotAvailable` to release
claims without attempts; transport/protocol/invalid-vector failures have the
existing three-attempt cap and retain exact text. Model changes call
`reset_content_vectors`, advancing the durable space generation while keeping
valid exact rows and parse cursors. The normal supervisor resumes vectors
without reparsing. Source/revision/policy/space checks guard both commits.
Empty, zero, and nonfinite vectors never enter `segments` from this stage.

`IndexStore::content_matches` joins matching FTS rows to their own locations.
Its query-time `ContentMatch.match_byte` is the original UTF-8 byte offset of
the first highlighted FTS token. Line and excerpt use this same anchor, so
accent-normalized queries retain the actual matching source text. Literal
highlight-marker data uses an absent marker whose unique start character also
prevents a false match across the source/inserted-marker boundary.
`SearchHit.matches` and the additive wire excerpts/matching moments preserve
multiple matching chunks for a grouped file result. These interfaces carry
up to 32 moments on the primary result path; `content_matches_page` exposes
subsequent matching chunks with an offset and a per-page cap of 32. Exact text
and all locations remain durable regardless of that retrieval cap. They carry
locations for PDF/media work; audio, video, and frontend explorer support have
their own acceptance gates.

### PDF worker pages

`ws_extract::pdf::extract_pdf_batch(&ExtractRequest, &ExtractBudget)` uses the
same owned extraction child, deadlines, job memory limit, admission permits,
scratch validation and artifact leases as document/image batches. PDFium is
loaded only in that child from an absolute sibling DLL path with Windows
`LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_SYSTEM32`. Debug builds
alone accept an explicit absolute `WINSEARCH_TEST_PDFIUM_DIR` for fixtures.
The archive version/hash and upstream URL are pinned in
`src/engine/third_party/pdfium.json`; `tools/fetch-pdfium.ps1` verifies before
extracting, rejects unsafe archive paths/links, preserves all licenses and
never changes PATH. Package assembly must copy the verified DLL and notices.

The cursor is opaque: `(zero_based_page << 32) | draft_within_page`. Decode
validation accepts page ordinals through 5000 and draft ordinals below 8192;
page 5000 is terminal and cannot have a draft offset. Requests also validate
against the real capped page count and the actual reconstructed page drafts.
`total_units` is the actual page count capped at 5000, never a chunk count.
Each batch covers at most 30 pages and 256 drafts. A continued large page
finishes its remaining text/OCR/visual drafts before advancing. Blank pages
have an empty, non-embeddable page draft with their real thumbnail, allowing
durable progress without fabricating text. Terminal completion is represented
by `next_cursor = null`; the persisted completion flag is authoritative.

All PDF drafts carry one-based `page`. Allowed kinds are `pdf` selectable
text, `ocr` raster-recognized text, and `pdf_visual` with empty text and a
rendered PNG. Text uses the shared chunker and numeric exact-only routing.
Draft ordinals are stable packed page/chunk identities, so replay/restart
cannot duplicate them. Line ordering resets per page. Per-page locations
retain their own artifacts; the first page thumbnail remains the file's
primary thumbnail until source invalidation. Deferred visual embeddings open
the promoted page PNG rather than the source PDF. Real visual-model quality
is a separate media-runtime gate.

Every first-batch PDF replacement transaction removes previous `pdf`, `ocr`
and `pdf_visual` vectors, including snapshot-driven replacement or a completed
job requeued without a watcher event. Failed/unsupported initial replacements
clear the same derived kinds. Before a PDF parse, a changed persisted source
snapshot uses durable source invalidation to remove stale exact/derived content
and invalidate the semantic cache even if the new parse fails. An unchanged
source requeued with a missing dependency/transient failure retains its valid
prior content; a later continuation failure retains committed earlier pages.
Parent transport validation also bounds cumulative per-page
draft ordinals when completing or advancing a page, not only when continuing it.

PDFium handles are RAII-managed under a serial lock. Unicode paths use native
File reads and bounded `FPDF_LoadCustomDocument` callbacks (Windows unsigned
long is 32 bits), without buffering the whole source. Text extraction allocates
`count + 1` UTF-16 values and rejects more than 500000 page characters. Finite,
positive page sizes and checked raster dimensions/stride/area precede render;
the raster has at most 2048 pixels per edge. Grossly pathological aspect ratios
fail explicitly. No JavaScript/forms handlers are initialized or invoked.

Protected, corrupt, unreadable, missing-dependency and OCR-unavailable inputs
return explicit failures; genuinely blank pages complete normally. A later
failure leaves previously committed batches searchable and progress incomplete.
The worker's additive `diagnostics: Vec<String>` is capped at eight entries of
1024 bytes each. Sources above the page limit emit
`PDF page limit: source_pages=N indexed_pages=5000; remaining pages excluded by product limit`.
The daemon records this warning; completion means all pages within the product
limit, not uncapped full-file coverage. A future source-status UI must surface
that distinction. Search visibility, native cancellation, exact and vector
commits continue to use current source/policy/model generations.

Native fixture setup: run `tools/generate-pdf-fixtures.py` with Python containing
Pillow/reportlab, then the fetcher with an isolated output (or use the independently
verified archive cache). Worker acceptance tests use the debug dependency override;
daemon native tests use the verified `pdfium.dll` beside the rebuilt debug worker.
`tools/test-fetch-pdfium.ps1` checks the fetcher against the pinned cached archive.

### Native media embeddings

The pinned mtmd helper incrementally DECODEs chunks, which loses the shared
bidirectional context for this encoder. The shim now collects special/text
tokens and each media encoder row in one `llama_batch_ext`, assigns positions
and embedding outputs, then calls `llama_process(ENCODE)` once. Rows use the
model's input width512, independently of its768-dimensional pooled output.
Mean pooling and L2 normalization include the complete sequence. Media contexts
reserve2048 logical/physical tokens; existing short-text batching stays512.
Native RAII frees bitmaps/chunks/batches on every return; invalid/nonfinite or
zero vectors fail rather than becoming semantic content.

`EmbedRuntime::embed_image_with_budget` accepts70/140/280 tokens (default280
through `embed_image`). Reference sizing fills the budget in48-pixel multiples
using attributed Pillow-compatible bicubic resampling before mtmd; the projector
does not resize those already-budgeted pixels again. Image input is bounded to
16M pixels. `embed_audio` accepts1..480000 finite mono16k float samples. The
worker's PCM16 mono16k WAV reader scans bounded headers, validates sizes/formats,
seeks to an optional forward window of at most30seconds, and reads at most960000
PCM bytes. Longer complete files are rejected, not silently cut. Compressed
formats and recording windows belong to the subsequent extraction slice.

Internal `EmbedImage.token_budget` is optional/default280; no public C# wire
change is needed. Image/audio share the supervisor's bounded cancellation and
restart path. An explicit text model uses its own sibling projector. Capability
tags require a loaded supporting projector (or explicit test-only stub mode).
`ModelCompat.vector_pipeline_version=2` participates in compatibility; a missing
field deserializes as legacy0, pipeline1 is incompatible, and malformed/partial identities fail closed.
Vector replacement preserves durable exact text, locations and parser progress.

Code syntax now uses pinned native Tree-sitter grammars for all twenty listed
languages plus the TSX variant. SQL uses SQLite syntax. Iterative node walking
finds definitions and identifier occurrences while skipping comments/strings;
references do not imply cross-file type resolution. Unsupported languages retain
bounded text chunks; malformed syntax retains useful text with explicit recovery
diagnostics. Code reads are limited to2MiB after residency/sensitive/ancestor
checks. Chunks are bounded to16KiB and60lines without nested-body multiplication.
The code crawler uses an iterative64-level walk with guarded reads of local
`.gitignore`, `.ignore` and selected-root `.git/info/exclude`. It reuses the
ignore crate's pattern matcher, preserving nested negation and `.ignore`
priority. Control files are capped at2MiB/8192lines and unavailable controls
fail closed. No automatic parent/global/worktree-external ignore-file reads
occur. Selected-root containment and credential-config/sensitive-ancestor
checks also apply to direct code reads, before binary sniffing.

`winsearch-extract` accepts internal kind `code` under the existing owned-child
deadline/memory/two-slot admission path. The optional typed `CodeBatch` is
separate from ordinary content drafts. Its cursor covers chunks followed by
symbols, at most256units and2MiB encoded payload per response. Parent validation
rejects missing/skipped units and invalid ranges/cell metadata. Native tests
reconstruct300definitions plus300references over multiple batches without losing
source text. The opt-in code scheduler uses this transport; normal queries have
no code results and there is no active in-process fallback.

`code.db` schema3 stores zero-based notebook cell ordinal and one-based local
lines alongside virtual source lines for chunks/symbols. Notebook source cells,
including language magics, are retained; outputs/attachments/raw JSON are excluded.
Migration preserves repo/file identity and exact text, clears stale heuristic
symbols/vectors and marks files for native reparse. Successful persistence records
the native parse revision and diagnostics; metadata changes reset that revision.

The code coordinator persists source snapshots, revisioned claims and scan
staging in `code.db`. Initial selected roots reconcile immediately; watcher
changes hide stale source and persist a 60-second quiet deadline across pause,
resume and restart. One signalled coordinator walks at most512 entries or applies
at most256 files per turn. Parser children return at most256 units per batch;
a whole-file transaction publishes exact chunks/symbols before deferred vectors.
Failed parse jobs have three poison attempts; missing models do not consume them.
Precise metadata/native identity, current policy, claim revision and model recipe
are checked around work and commits. Power notifications wake the coordinator;
scheduled openings use the next daily boundary rather than a polling loop.

Code queries filter current roots/exclusions/residency before candidate limits.
Exact identifier definitions precede references, then chunk FTS, followed by
optional semantic chunks using the shared supervisor/recipe/admission floor.
Policy changes and new parse publication advance a request revision so an old
stream cannot resume after revoke/regrant or return an excerpt from a prior parse.
Notebook results retain zero-based cell ordinal and one-based cell-local lines.
The Windows editor preference detects known editor installations, launches with
individual arguments, and falls back to the associated viewer with an explanation.
Notebook opening never treats virtual source lines as JSON file lines. Native
editor interaction and full preview acceptance remain part of the shell gate.

Application formatting lives in `ws-modelmgr::recipe`: normal queries use
`task: search result | query: ...`, code queries use the code-retrieval task,
and text/code documents use `title: ... | text: ...`. Filename inputs retain
their original spelling; development measurements rejected document prompts
and extension stripping for that path. Image/audio inputs and raw runtime
reference inputs carry no added text prefix. `ws-index::retrieval` owns common
finite-score admission (cosine >=0.65 for recognized segment kinds) and hybrid
ranking for daemon and CLI. Query cache keys include the model generation;
generation changes during embedding, ranking or before semantic delivery
suppress that stage. In-flight name batches also requeue across a model change.

Independent public synthetic reference inputs/vectors are pinned with license
and hashes in `ws-embed/tests/fixtures/media`. `cargo test -p ws-embed --test media
-- --ignored --nocapture --test-threads=1` requires verified model files. The
upstream WAV is formant noise, not intelligible speech. Generate separate local
spoken fixtures with `tools/generate-spoken-fixtures.ps1` and
`tools/assemble-spoken-fixtures.py`; these write files without playback, external
services or language installation. `tools/test-media-runtime.py` tests the actual
worker plus image descriptions/spoken retrieval. These checks do not establish
long-media extraction, UI playback, full-format decoding or final GPU/package
acceptance; those remain required downstream gates.

The engine is also intended to power [Lumen](https://github.com/KB-Helios/Lumen).
Its Rust host should consume the typed headless engine contract and dedicated
profile; the current WinUI app remains another consumer. Lumen's React
SearchService, Zod validation, consent, cancellation and native process/model
ownership remain boundaries. Actual client/host integration and engine-only
package verification are required work, not established by the media tests.

### Bounded audio and video extraction

The extraction child now decodes compressed/local recordings through private
sibling ffmpeg/ffprobe executables, never PATH. Debug fixtures alone can set
WINSEARCH_TEST_DECODER_DIR. Both tools use a local-file protocol and explicit
container whitelist that excludes playlists, concat and image sequences; MOV
external references remain disabled. Probe JSON is capped at 64 KiB, ordinary stderr at 32 KiB,
stream count at 32, source duration at 366 days, and decoded PCM at 960000 bytes.
Each decoder runs in an owned nested job under the existing worker's outer job,
deadline and 1 GiB tree limit. Timeout, crash and capture-overflow tests exercise
the real process owner. Streaming media has a 64 GiB metadata size cap; ordinary
document/image/PDF input keeps its 32 MiB bound.

Audio cursors are window ordinals, with windows of 30 seconds and a hop of 25 seconds.
Each batch processes at most 30 windows and the final partial window ends at
the source duration. RMS below .003 advances progress without a draft, transcript
or vector. Duration decimal parsing avoids floating-point millisecond rounding.
AAC delay, late-starting and short soundtracks retain source time through bounded
resampling/silence padding; a truncated or corrupt decode fails accurately.
The internal MediaPlan lets the parent validate processed source units separately
from emitted drafts, including windows that are genuinely silent.

Video selection combines actual source frames at an adaptive periodic interval
(normally five seconds), low-resolution shot changes and an actual late frame.
The shot scan samples at least one second apart, adapting to at most 14,400 samples.
Mean absolute grayscale change of 38/255 marks a shot candidate. Sorted candidates
within 500 ms collapse; an evenly spaced subsample of the union caps selection at
1,200 frames and retains first/last coverage. Integer PTS/time-base conversion
preserves variable-rate source time; no synthetic fps clock supplies locations.
Shot raw pixels/logs are bounded separately (about 8 MiB each), frame PNGs at 16 MiB,
rendered sides at 1280 pixels. Each batch emits at most 30 frame units/256 drafts.
Visual inputs use 70 tokens, or 140 when useful OCR is present; numeric OCR stays
exact-only. Videos without sound retain visual search; audio-only containers use
the soundtrack route.

Frame cursors encode `frame_index << 16 | within_frame_draft`; the independent
soundtrack phase uses `1 << 62 | window_ordinal`. The revision-scoped selection
and cursor commit with chunks and the extraction claim in one transaction.
Changing a continued plan, replaying a claim, or skipping phases is rejected.
The 1,200-frame cap never truncates the soundtrack's 30 second/25 second-hop windows.
Initial shot selection still decodes the source sequentially under the 120 second
budget. The sparse two-hour fixture passes; expensive long/high-resolution scans
have not established resumable selection and remain a final acceptance concern.

DraftEmbeddingInput distinguishes text, image budgets and derived audio paths;
it is persisted on each content chunk independently of searchable text.
The parent confines, caps and promotes artifacts before removing scratch.
Audio additionally requires a canonical bounded PCM16/mono 16k WAV whose sample
duration agrees with its location. Artifact leases cover typed inputs as well
as thumbnails. Silent-only progress commits atomically and stale/replayed claims
cannot advance it. A missing deferred WAV or frame is regenerated from its permitted
source in a bounded child, then attached to the existing claimed chunk without
changing exact row IDs or source locations. Generation/revision/policy checks
precede replacement and vector commits; changed sources invalidate old vectors
before a decoder failure can preserve stale media results. Restoring a video
frame repairs same-time OCR thumbnail references, including drafts rendered in
different worker requests. Once vectors are complete, on-demand preview/cache
regeneration still belongs to the pending preview/maintenance slice.

Semantic results preserve the actual scored segment's page/line/time, up to 32
deduplicated matching moments and matching cached thumbnails. Sound matches have
no invented transcript. Existing entry filters also cover a video's soundtrack.
The current admission threshold still returns unrelated results; the required
shared retrieval recipe/confidence work and negative-control gate remain open.

Native fixtures cover Unicode/spaced/dash filenames, three windows in a 65 second recording,
compressed audio, network-playlist rejection without a loopback connection,
and a 20 minute silent recording continued across workers. Native video checks add
shot selection, variable-rate PTS, OCR, phase restart, cache restoration and
source/pause/root changes. The genuine daemon run indexes later speech at 50..65s
and retrieves a matching frame at 40s; unrelated negatives still fail. A two-hour
160x100/1fps fixture verifies 1,200 frames plus 288 independent audio windows and
owned decoder cancellation; it is not high-resolution throughput evidence.
See benchmarks for measured resources. Full retrieval quality, completed-vector
cache maintenance, installed payload and UI acceptance remain open.
