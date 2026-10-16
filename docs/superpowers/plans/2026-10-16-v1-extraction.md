# WinSearch v1 bounded extraction and resumable content plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Run untrusted parsers in a bounded child process and make exact content/location search work independently of model availability, with durable incremental progress for long files.

**Architecture:** Keep two extraction coordinators in winsearchd, but move actual document/image/OCR/native parsing to winsearch-extract. A typed internal request/response protocol carries bounded drafts and continuation cursors. Commit extracted text and match metadata before optional vector generation; failed inference must not erase valid exact text.

**Tech Stack:** Existing Rust/serde/framing/SQLite/windows-rs; Windows Job Objects for child resource ownership. Existing extractor implementations and model supervisor remain in use.

## Global Constraints

- Work only in C:\Users\kevin\.codex\worktrees\winsearch-v1\WinSearch on codex/winsearch-v1.
- The approved full v1 design remains binding; preserve the original checkout and prior dirty worktree.
- Prefix shell commands with rtk. No weakened tests/thresholds, production stub vectors, or claims based on skipped tests.
- Never alter source documents or hydrate cloud placeholders. Use the policy slice's current path/metadata guard before content opens and recheck before committing.
- No UI or external public publishing in this slice. Use generated fixtures and isolated data directories for native checks.
- This is a native Windows x64 app. Keep Rust responsible for parser/model execution and policy; C# receives typed state.

## Task 1: Isolate existing parsers and enforce parent-side limits

**Files:**
- Create: src/engine/bins/winsearch-extract/Cargo.toml and src/main.rs
- Create: src/engine/crates/ws-extract/src/worker.rs
- Modify: src/engine/crates/ws-extract/src/{lib,budget}.rs
- Create: src/engine/bins/winsearchd/src/extract_client.rs
- Create: src/engine/crates/ws-platform/src/child_job.rs
- Modify: src/engine/bins/winsearchd/src/{main,extract}.rs
- Modify: workspace Cargo/Windows features, lockfile, ws-platform exports only as required
- Test: worker protocol, child_job ownership, real extract-process integration, existing extraction regressions

**Interfaces:**

Add serializable types in ws-extract::worker:

```rust
pub struct ExtractRequest {
    pub path: PathBuf,
    pub kind: String,
    pub scratch_dir: PathBuf,
    pub cursor: u64,
    pub max_units: u32,
}
pub struct ExtractResponse {
    pub output: ExtractOutput,
    pub next_cursor: Option<u64>,
    pub total_units: Option<u64>,
}
```

Use std::path::PathBuf. Serialize existing ExtractOutput/SegmentDraft/status
without changing their meaning. A zero cursor means first unit; each modality
defines units (text chunks, PDF pages, audio windows, or video frame/audio
phases in later slices). Worker reports its supported kinds; missing support
must be an explicit response, never a fabricated successful extraction.

Use existing 4-byte little-endian length-prefixed JSON framing with a maximum
16MiB response and at most256 drafts per batch. Request paths/configuration
come only from the daemon's allowed job snapshot, not a new arbitrary-file UI
IPC endpoint. Parent validates returned counts, string lengths, finite
metadata, page/time ordering, forward-only cursor progress, and scratch paths.
Do not trust an output path outside the supplied scratch directory.

Create one private scratch directory per request under the app cache. Worker
does no input parsing until assigned to its Windows job and sent the request.
The job owns child decoder processes too, enforces a1GiB job memory limit,
and kills descendants when its handle closes. Default elapsed deadline is
120seconds per batch. Parent must bound request writes, output reads and waits and kill,
reap, close pipes, and clean scratch artifacts on timeout/crash/cancel. Never
kill an unrelated process by executable name. Limit concurrency to the
existing2 extraction slots. The worker launches without a visible console.

On success, promote referenced thumbnail artifacts from that request's scratch
directory into the existing app-owned cache before returning usable paths to
the caller; validate source containment first and use unique cache names. Do
not return paths and then delete the files behind them. Clean unreferenced
scratch artifacts and remove the private directory after promotion. Failure,
timeout and stale-policy paths must not retain an abandoned scratch tree.

Bound encoded requests to256KiB as well. A child that never reads stdin must
not trap the parent while a long Unicode path fills the pipe: use cancellable
owned writer/read tasks or an independent job deadline that closes the child
tree and unblocks both sides. Include a transport fixture whose bounded request
exceeds pipe capacity and whose child never reads it; timeout must still kill,
reap and close handles within the configured deadline.

Keep existing per-format byte/decompression/pixel/segment limits. Streaming
PDF/media work in later slices needs per-batch limits rather than rejecting
all large input files on the old32MiB metadata check. Do not loosen document
archive limits or read an entire media file into memory. This task still
supports the existing text/document/image/OCR extractors; PDF legacy behavior
is replaced in the PDF slice.

Expose `ExtractClient::extract(&ExtractRequest, Duration) -> Result<ExtractResponse, ...>`
from extract_client.rs (a small explicit error enum distinguishes timeout,
malformed output, worker failure, and unsupported input). Production extract
workers must call it; tests can still inject deterministic extractor providers
without process spawning. Do not leave an in-process production fallback that
bypasses the safety limit when the worker is missing.

- [ ] Write protocol roundtrip/bounds tests and native worker fixture tests first.
  Assert a tiny text file returns its actual text and line metadata. A worker
  that writes an oversized length, exits mid-frame, hangs, or returns a path
  outside scratch must fail within the specified bound. A deliberately hung
  child tree must be gone after timeout. Record red failures before fixes.
- [ ] Implement the job owner and typed worker framing. Use documented native
  API declarations/current Context7 docs where applicable. Test ownership and
  failure cleanup using task-owned child processes only.
- [ ] Route production document/image/OCR extraction through the child. Preserve
  budgets and policy snapshots; file changes, root removal, pause/cancel,
  unsupported codecs, and missing executables must have deterministic outcomes.
- [ ] Run selected Rust workspace packages plus real worker-process tests.
  Build the actual new binary before running integration tests. Run .NET tests
  and x64 WinUI build if public protocol/project files change. Run diff check.
- [ ] Self-review trust boundaries/handle lifetime/cleanup and commit this task.
  Report red/green commands, exact counts, produced interfaces, and limitations.

## Task 2: Durable batches and exact match locations without a model

**Files:**
- Modify: src/engine/crates/ws-index/src/{model,store,search}.rs
- Modify: src/engine/crates/ws-extract/src/{lib,docs,chunk,worker}.rs
- Modify: src/engine/bins/winsearchd/src/{extract,indexer,engine,model}.rs
- Modify: src/engine/bins/winsearch-extract/src/main.rs
- Test: migrations, long text batches, extraction restart/failure, lexical locations, no-model recovery
- Modify: src/engine/crates/ws-protocol/src/messages.rs and C# Core/Protocol/Messages.cs plus serialization/consumer tests if excerpts/moments require additive public result fields
- Modify: docs/architecture.md and docs/ipc-protocol.md if result fields grow

**Interfaces:**

Keep ExtractRequest/ExtractResponse from Task1. Persist a per-job cursor and
source snapshot (entry identity/path/size/mtime/model-space generation) so
restarts resume, while source or model changes invalidate stale work.
Do not rely only on the existing Unix-second mtime. A watcher Modified event
must invalidate/requeue content even when file size and whole-second mtime
are unchanged. Use a durable source/job revision and precise metadata as
needed; keep the existing seconds field's recency semantics. Recheck the
actual source and current revision before committing a batch or its vectors.
Transactionally commit the batch's exact text, page/time/line/thumbnail
metadata and next cursor before releasing its claim. Retries must not duplicate
chunks, erase prior batches, or advance progress without content. A null
next_cursor completes extraction. Lower continuation priority after the first
batch so short/new files are not starved by long files.

Reuse existing segments/FTS/queue machinery where possible. Store text/location
drafts without requiring nonempty vectors (zero/empty vectors must not enter
cosine results), or use a small content-record table if existing invariants
require it. Do not duplicate the whole index service. In the report name the
actual schema and public store methods used by later PDF/media tasks.

Add serializable match metadata to SearchHit or a small index-local location
type and map it to existing protocol MatchLocation. Exact content matches must
return the relevant chunk excerpt and page/time/line, including when there is
no downloaded model. Multiple occurrences in one file remain available as
matching moments for later previews; the primary list can still group by file.
Do not select metadata from an unrelated chunk just because it shares a file.

Text chunks retain existing1200-character/~15% overlap behavior and line
numbers. First batch is30 chunks; continuation batches are at most30 chunks,
up to existing file byte/decompression bounds. A bounded response must never
silently label a truncated long document complete. Preserve numeric exact-only
routing: numeric text is searchable but does not enqueue semantic vectors.

Make semantic embedding a durable deferred stage. Missing/unready models do
not increment poison attempts or discard extracted exact text; a later model
download/import/restart resumes pending vectors. Genuine transport/decode
failures retain bounded retries and preserve correct exact text. File deletes,
renames, modality changes, policy removal and incompatible model commits must
remove/requeue the correct state without mixing embedding spaces.

- [ ] Reproduce the no-model bug: extract a generated document containing
  "WINSEARCH_INVOICE_42" with an embedding callback returning a model-unavailable
  error; exact content search must still find it with its actual excerpt/line.
  Assert no fabricated semantic result appears. Observe red before fixing.
- [ ] Add a file with75 text chunks. Verify first30 are searchable promptly,
  restart the indexer, resume at30, and finish exactly75 stable chunks without
  duplicates. Interrupt a commit and verify progress/content roll back together.
  Change the file during a batch and verify stale drafts cannot overwrite it.
- [ ] Add a same-size, same-second edit regression: index a generated text file,
  overwrite it with a different equal-length token, and deliver the actual
  watcher Modified event while preserving the whole-second timestamp (distinct
  subsecond timestamps are allowed). The new token must replace the old exact
  text after re-extraction. Repeat inside an in-flight callback and prove the
  earlier batch cannot commit. Observe the current unchanged-metadata queue
  failure before fixing it; do not just sleep until the next timestamp second.
- [ ] Add exact location tests with distinct tokens on different pages/times,
  plus numeric content without vectors. Verify policy-revoked chunks are absent
  and cannot crowd permitted hits out of the limit.
- [ ] Implement durable batching and deferred vectors. After model availability
  changes, verify the same exact chunks gain genuine/injected-test vectors
  through the existing supervisor path without parsing every file from scratch.
- [ ] Verify old DB migration preserves entries and searchable text, including
  numeric exact-only rows that never had vector segments. Run full selected
  Rust tests and all new worker/store tests, then .NET tests/WinUI build and
  diff check. Label fixtures versus native process versus model evidence.
- [ ] Commit and report concrete storage/cursor/location APIs for PDF/media,
  along with red/green/restart evidence. Do not claim PDF/audio/video support
  until their dedicated implementations and native fixtures pass.
