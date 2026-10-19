# WinSearch

A Windows local search and indexing engine built with Rust, with a standalone
WinUI 3 app. WinSearch is being completed as the dedicated search engine behind
[Lumen](https://github.com/KB-Helios/Lumen), using local EmbeddingGemma 2 inference.
The feature reference is [ARahim3/DigUp](https://github.com/ARahim3/DigUp).

**Status: v1.0 is in progress.** The implementation PR is
[PR #2](https://github.com/KB01111/WinSearch-lumen-engine/pull/2). Existing engine
features and passing subsystem tests do not establish complete or installed v1.0
acceptance. CPU inference works locally; GPU calibration remains pending. NPU
acceleration is optional and outside the current focus.

## v1.0 TODO

Remaining work as of 2026-10-19. Keep a task open until its implementation and
relevant acceptance checks are complete. The detailed source/test evidence lives
in the [gap matrix](docs/v1-gap-matrix.md); the
[approved design](docs/superpowers/specs/2026-10-16-v1-design.md) and
[Lumen integration plan](docs/superpowers/plans/2026-10-17-v1-lumen-engine.md)
define the full scope.

The [Related/cache-retention checkpoint](docs/validation/2026-10-19-related/README.md)
records the typed Related operation, a reproduced exact-cache fix and native
CPU/no-model checks with retained failures. Consumer integration and the tasks
below still require implementation or final acceptance.
The [CLI filter checkpoint](docs/validation/2026-10-19-cli-filters/README.md)
adds repeated extension arguments and rejects unsupported peers and mismatched replies.
The [file-kind checkpoint](docs/validation/2026-10-19-file-kinds/README.md)
adds typed source classifications before candidate limits, including PDF,
spreadsheet, presentation, source and unknown kinds.

### Lumen integration and native contract

- [ ] Complete remaining provenance contracts and verify finer file-kind scopes
  through Lumen's native adapter.
- [ ] Consume and verify the engine's terminal outcomes in Lumen and the
  standalone UI, preserving complete/superseded/canceled/failed and semantic
  disabled/ready/degraded states, exact results on optional failure and legacy
  peers' unknown state. Complete independent review and installed acceptance.
- [ ] Verify consumer handling of empty/superseded results and the full
  pause/resume/revoke matrix after the cache-retention fix; preserve current
  source fences and retained failure evidence.
- [ ] Implement the typed WinSearch adapter behind Lumen's existing SearchService.
  Keep secured IPC, filesystem grants and native actions in Lumen's Rust host.
- [ ] Authenticate every pipe connection against the engine process owned by
  Lumen before sending the handshake or requests.
- [ ] Defer scanning persisted roots until Lumen admits its current folder
  policy, including explicitly empty roots and revoked access.
- [ ] Consolidate to one authoritative crawler, index and embedding lifecycle;
  migrate identities and metadata without losing existing user data.
- [ ] Preserve Recent/Related results, pins, history and answer-context admission,
  with current source/root checks before previews, actions and answers.
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
- [ ] Bound stalled raw IPC writers and verify control/cancellation/resource
  behavior; client deadlines alone do not prove server-side responsiveness.
- [ ] Run the final unchanged Rust workspace, .NET, Windows IPC/watcher,
  real-model/reference and retrieval suites, including negative controls.

### Packaging, installation and delivery

- [ ] Produce self-contained standalone and engine-only payloads containing the
  required workers, native DLLs, pinned dependencies, notices and hash manifests.
- [ ] Implement the per-user installer, startup registration, data-preserving
  upgrades, update checks and uninstall lifecycle; document signing/WinGet steps.
- [ ] Test outside the checkout and through clean install/upgrade/uninstall on
  Windows, including the installed Lumen consumer and missing/corrupt dependencies.
- [ ] Resolve valid outstanding review findings and complete the broad final
  security, privacy, reliability and accessibility review.
- [ ] Reconcile the feature checklist, benchmarks and PR with final-source
  evidence and remaining limitations before declaring v1.0 complete.

## Architecture and verification

See [architecture](docs/architecture.md), [IPC protocol](docs/ipc-protocol.md),
[headless Rust client](docs/headless-client.md),
[feature parity](docs/feature-parity.md) and [benchmarks](docs/benchmarks.md).
Lumen owns the application UI and trusted native boundary; WinSearch owns search,
indexing and retrieval. WinUI remains available for standalone development.
