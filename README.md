# WinSearch

A Windows local search and indexing engine built with Rust, with a standalone
WinUI 3 app. WinSearch is being completed as the dedicated search engine behind
[Lumen](https://github.com/KB-Helios/Lumen), using local EmbeddingGemma 2 inference.
The feature reference is [ARahim3/DigUp](https://github.com/ARahim3/DigUp).

**Status: v1.0 is in progress.** The implementation PR is
[PR #2](https://github.com/KB01111/WinSearch-lumen-engine/pull/2). Existing engine
features and passing subsystem tests do not establish complete or installed v1.0
acceptance. CPU inference works locally; GPU calibration remains pending. On PCs
with an AMD Ryzen AI NPU2, the [IRON EmbeddingGemma 2 bundle](docs/hx370-npu.md)
is the primary text/image model, with automatic GPU/CPU fallback. Its hardware
acceptance is still pending.

## v1.0 TODO

Remaining work as of 2026-10-20. Keep a task open until its implementation and
relevant acceptance checks are complete. An unchecked item can include working
subsystems whose final integration or acceptance is still pending. The detailed
source/test evidence lives
in the [gap matrix](docs/v1-gap-matrix.md); the
[approved design](docs/superpowers/specs/2026-10-16-v1-design.md) and
[Lumen integration plan](docs/superpowers/plans/2026-10-17-v1-lumen-engine.md)
define the full scope.

The immediate blockers are large-result search latency,
Lumen's search adapter and data migration, the remaining standalone UI, and
final performance, recovery and installed-app acceptance. NPU support is not a
release requirement.

The [Related/cache-retention checkpoint](docs/validation/2026-10-19-related/README.md)
records the typed Related operation, a reproduced exact-cache fix and native
CPU/no-model checks with retained failures. Consumer integration and the tasks
below still require implementation or final acceptance.
The [CLI filter checkpoint](docs/validation/2026-10-19-cli-filters/README.md)
adds repeated extension arguments and rejects unsupported peers and mismatched replies.
The [file-kind checkpoint](docs/validation/2026-10-19-file-kinds/README.md)
adds typed source classifications before candidate limits, including PDF,
spreadsheet, presentation, source and unknown kinds.
The [owned-startup checkpoint](docs/validation/2026-10-19-owned-startup/README.md)
adds process-bound Rust connections and deferred folder admission.
[Lumen PR #27](https://github.com/KB01111/Lumen/pull/27) now wires verified owned
startup, PID authentication, deferred admission and shutdown into the Rust host;
its real two-profile native test passes. Search/root-policy forwarding, migration
and installed-consumer acceptance remain open below.
The [engine-only package guide](docs/engine-payload.md) covers verified local
assembly and Lumen staging. Final release delivery, native consumer wiring and
installed acceptance remain open.
The [database-identity checkpoint](docs/validation/2026-10-19-index-identity/README.md)
adds durable ordinary/code index namespaces and prevents deleted code file IDs
from being recycled. Lumen still needs to consume these identities in its host
ledger and search adapter; its staged release engine now exposes the matching
identity contract.
The [per-folder policy checkpoint](docs/validation/2026-10-19-root-policies/README.md)
adds hidden/size/relative exclusion admission before lexical/vector limits,
including code traversal and capability checks. The
[search-limit checkpoint](docs/validation/2026-10-19-search-limits/README.md)
adds capability-gated totals up to 10,000, progressive bounded batches and
receive budgets. The Lumen mapper, native forwarding and installed consumer
acceptance remain open.
The [metadata-identity checkpoint](docs/validation/2026-10-20-metadata-identity/README.md)
preserves metadata-only entry references without reviving old content proofs.
The [rebuilt-runtime checkpoint](docs/validation/2026-10-20-engine-contract/README.md)
retains earlier actual-model, retrieval and native checks, including rejected
debug packages.
The [extraction-claim checkpoint](docs/validation/2026-10-20-extraction-claims/README.md)
reduces repeated policy work in the queue; the later release checkpoint below
supplies rebuilt-package evidence.
The [release-runtime checkpoint](docs/validation/2026-10-20-release-runtime/README.md)
now passes both native profiles, actual packaged CPU inference, the five frozen
retrieval suites and the outside-checkout 1,000/1,100-result gate with unchanged
deadlines. [Lumen PR #27](https://github.com/KB01111/Lumen/pull/27) now pins and
stages that release with a passing two-profile native contract/PID/Stop test.
Consumer integration and controlled performance acceptance remain open; earlier
failures are retained.
[Lumen PR #35](https://github.com/KB01111/Lumen/pull/35) (open) implements the typed
native consumer: current folder grants and per-folder policies are forwarded to the owned
engine and re-sent after restarts, and launcher searches stream from it with native
cancellation and a local-index fallback. The earlier statements above that search/root-policy
forwarding is open describe released builds and stay in force until that PR is merged and
the consumer passes installed acceptance. The pinned runtime `85a915d` is reachable from this
repository's `main` through [PR #4](https://github.com/KB01111/WinSearch-lumen-engine/pull/4).
Recent/Related, pins/history, identity migration, the single crawler and installed
acceptance remain open below.
The [IPC writer checkpoint](docs/validation/2026-10-20-ipc-writer/README.md)
bounds server frame writes, closes partial transports and removes failed
broadcast clients without holding the global client registry. Its native
regressions pass; the staged release still uses the earlier verified engine.

### Lumen integration and native contract

- [ ] Complete remaining provenance contracts and verify finer file-kind scopes
  through Lumen's native adapter. Preserve per-folder hidden-file, size and
  exclusion policies before candidate limits, and reconcile consumer result
  limits without silently truncating them. Complete large-result latency and
  concurrent indexing validation, and verify Lumen's native forwarding against
  its pinned release while preserving current metadata/source identities and
  the unchanged package gates.
- [ ] Consume and verify the engine's terminal outcomes in Lumen and the
  standalone UI, preserving complete/superseded/canceled/failed and semantic
  disabled/ready/degraded states, exact results on optional failure and legacy
  peers' unknown state. Validate the closed outcome/reason codes, then complete
  independent review and installed acceptance.
- [ ] Verify consumer handling of empty/superseded results and the full
  pause/resume/revoke matrix after the cache-retention fix; preserve current
  source fences and retained failure evidence.
- [ ] Implement the typed native WinSearch adapter behind Lumen's existing
  SearchService. Connect it to the
  [verified catalogue/launcher streaming consumer](https://github.com/KB01111/Lumen/blob/codex/winsearch-engine-adapter/docs/reports/2026-10-20-winsearch-stream-consumer.md),
  preserving latest-request selection, native cancellation and empty Recent
  queries. Keep secured IPC, grants and actions in Lumen's Rust host.
- [ ] Complete consumer/restart/installed verification of process-bound pipe
  authentication, retaining the verified process handle through every connection.
- [ ] Launch Lumen's engine with deferred root admission and verify current
  folder grants on startup/restart, including empty roots and revoked access.
- [ ] Consolidate to one authoritative crawler, index and embedding lifecycle;
  transactionally migrate identities, metadata, pins and history with bindings
  that distinguish engine profiles and index rebuilds. Metadata-only entries
  must remain pinnable without fabricated content hashes or extraction records.
- [ ] Preserve Recent/Related results, pins, history and answer-context admission,
  with current source/root checks before previews, actions and answers. Preserve
  consented OCR/transcription enrichment in the authoritative engine path;
  complete source-bound enrichment ingestion and explicit engine index reset
  before retiring their legacy routes.
- [ ] Integrate owned engine startup/shutdown, streaming, cancellation, status,
  indexing progress, model management and recoverable errors. Verify launcher
  input, result selection and native Stop remain responsive.

### Previews, documents, media and code

- [ ] Complete source-byte hashes for larger files and existing indexes, and
  report accurate extraction totals and limits.
- [ ] Complete source-attestation device/recovery and independent review after
  the native change/restore fix. Unavailable read-change notifications must
  retain an unknown hash; timestamp equality alone can miss rapid edits.
- [ ] Complete confined raster/image/PDF-page thumbnails, rich previews,
  uncached rendering and cache maintenance within existing host resource bounds.
- [ ] Wire PDF page, source line and notebook-cell navigation into consumer
  previews/actions; verify open-at-line in the supported installed editors.
- [ ] Finish audio playback/seek and the video player/timeline with actual source
  timestamps, frame navigation and soundtrack handling.
- [ ] Verify every advertised image/EXIF, document, audio and video format on
  Windows, including malformed inputs, large sources and truthful unsupported states.
- [ ] Complete final code retrieval, notebook, editor and large-repository
  acceptance using the unchanged quality and source-policy gates.

### Standalone Windows app

- [ ] Build the expanded explorer with list/grid views, category filters,
  keyboard navigation and the bounded preview pane.
- [ ] Implement the six-step onboarding flow, selected-root consent, folder
  estimates, model download/import progress and shortcut setup.
- [ ] Complete General, Search, Folders, Code, AI, Performance, Storage and About
  settings, including durable save, validation and reconnect behavior.
- [ ] Expose pause/schedule/battery/performance controls and storage/cache
  maintenance through the existing engine policy.
- [ ] Verify native accessibility, focus, keyboard shortcuts, high contrast,
  reduced motion, startup/tray behavior and location-aware result actions.

### Reliability, performance and model acceptance

- [ ] Revalidate final binaries against the pinned actual EmbeddingGemma 2
  text/code/image/audio references, shared 768-dimensional space and unchanged
  numerical and retrieval thresholds.
- [ ] Measure and calibrate supported CPU/Vulkan/CUDA routes: cold load, warm
  queries, batch throughput, memory, backend failure recovery and CPU fallback.
- [ ] Benchmark 1,000 / 10,000 / 50,000 / 200,000 files: exact, semantic and hybrid
  p50/p95/p99 latency, indexing/incremental throughput, memory/disk use, startup
  and recovery. Separate synthetic, real filesystem and real-model measurements.
- [ ] Measure idle CPU/GPU and background behavior on AC/battery, battery saver,
  fullscreen/gaming and CPU/GPU pressure while preserving search responsiveness.
- [ ] Complete the combined crash/restart, worker cancellation, interrupted model
  download/resumption, database migration, low-disk and disconnected-drive matrix.
- [ ] Rebuild and stage the bounded IPC transport, then complete packaged,
  concurrent-client and installed control/cancellation/resource checks. Native
  frame-deadline and failed-broadcast cleanup regressions now pass; client
  deadlines alone do not prove server-side responsiveness.
- [ ] Run the final unchanged Rust workspace, .NET, Windows IPC/watcher,
  real-model/reference and retrieval suites, including negative controls.

### Packaging, installation and delivery

- [ ] Produce self-contained standalone and engine-only payloads containing the
  required workers, native DLLs, pinned dependencies, notices and hash manifests.
  Complete assembly and fixed-path Lumen staging, independently pin the manifest
  hash, validate staged bytes at build/startup and reject incomplete release
  packages. Fixture-only verifier tests do not establish a runnable payload.
- [ ] Implement the per-user installer, startup registration, data-preserving
  upgrades, update checks and uninstall lifecycle; document signing/WinGet steps.
- [ ] Test outside the checkout and through clean install/upgrade/uninstall on
  Windows, including the installed Lumen consumer and missing/corrupt dependencies.
- [ ] Resolve valid outstanding review findings and complete the broad final
  security, privacy, reliability and accessibility review. Clear the recorded
  strict Clippy failures in unchanged model-manager and daemon code.
- [ ] Reconcile the feature checklist, benchmarks and PR with final-source
  evidence and remaining limitations before declaring v1.0 complete.

## Architecture and verification

See [architecture](docs/architecture.md), [IPC protocol](docs/ipc-protocol.md),
[headless Rust client](docs/headless-client.md),
[feature parity](docs/feature-parity.md) and [benchmarks](docs/benchmarks.md).
Lumen owns the application UI and trusted native boundary; WinSearch owns search,
indexing and retrieval. WinUI remains available for standalone development.
