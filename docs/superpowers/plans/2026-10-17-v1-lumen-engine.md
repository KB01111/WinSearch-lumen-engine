# WinSearch as Lumen's native search engine

User-directed addition2026-10-17 to the full approved v1.0 goal. Existing
PDF/media/code/explorer/onboarding/settings/release requirements remain binding.
Prioritize the engine, retrieval and this consumer integration before standalone
WinUI polish. This plan is pending implementation and proves no integration yet.

Reference: KB-Helios/Lumen main `3b60ba43104ec7c450423c13db4bc5ee06662e68`,
refreshed against GitHub and the local origin/main ref on 2026-10-18. Compared
with the earlier `8ab5a6f` audit, production search contracts are unchanged; the
search embedding diff changes only a deadline-test fixture. CI/performance-test
changes in that interval must remain in the eventual consumer baseline.
The user's existing Lumen
UI branch differs; preserve it. Recheck current main before implementation.
Its native index, hybrid/Related/Recent search, pins/history, answer context,
consent and bounded previews already work in source and must be preserved.

## Constraints and architecture

- WinSearch remains the headless Rust daemon with owned extraction/inference
  workers. WinUI and Lumen are clients of that engine. No Tauri or UI dependency
  enters the engine/model/parser crates; no second search/model service.
- Lumen's Rust host owns the fixed staged executable, child Job Object/lifetime,
  per-host data/instance identity, typed pipe client, admission and cancellation.
  React retains SearchService/Zod contracts and receives typed events only.
- Use a dedicated absolute WINSEARCH_DATA_DIR and valid WINSEARCH_INSTANCE_ID;
  preserve the normal WinSearch app's data, pipes, startup and shortcut.
- Hydrated Lumen roots, including an explicitly empty set, govern admission.
  No default-root crawl, implicit model download or new cloud consent.
- Preserve stable identity, current-query IDs, aborts, bounded streaming,
  exact-first behavior, exact without AI, filters before limits and structured
  sanitized failures. Never infer semantic readiness from model-file presence.
- Preserve Lumen's64KiB aggregate UTF-8 text,4MiB passive-raster and40x12 XLSX
  preview bounds. Rich media requires a confined native cache/stream route;
  no unrestricted path endpoint, webview shell permission or arbitrary asset scope.
- Source/policy/model changes invalidate old results/previews/answer context.
  Root configuration mutations are ordered and every delivery/action rechecks
  current grants. Existing credentials/provider/cloud controls remain in Lumen.

## Task 1: Reusable Rust headless client and missing consumer contracts

Run after reviewed media/code/retrieval. Add a small ws-client crate using the
actual ws-protocol types, framing limits and Windows pipe transport. It must
build without ws-embed/CMake/model weights or any frontend. Choose the smallest
async integration matching Lumen's existing native runtime after reading its
current Cargo/API sources; query current documentation before library API work.

Expose bounded connect/handshake, correlated requests, streamed search,
cancellation, configuration, status and indexed-entry preview/metadata. Reuse
the shell preview protocol work where present; implement it once in the engine
if this priority reaches it first. Add only the typed engine operations needed
for Lumen's existing Related, current entry resolution, filters and truthful
semantic outcomes. Recent/pin/history policy can stay host-owned; do not create
another copy without need. Configuration and read lanes must not deadlock each
other or permit stale root admission.

The [2026-10-19 Related/cache checkpoint](../../validation/2026-10-19-related/README.md)
records the distinct bound-source operation, typed Rust/C# clients and current
native CPU/no-model checks. It also retains a native failure and the regression
for unavailable inference erasing committed exact cache during policy changes.
The [CLI filter checkpoint](../../validation/2026-10-19-cli-filters/README.md)
exposes the existing extension contract through arguments with capability/reply
checks. The [file-kind checkpoint](../../validation/2026-10-19-file-kinds/README.md)
adds bounded typed source-kind selectors before candidate caps and keeps omitted
fields compatible with older peers. Lumen's scope/filter intersection mapping,
expanded provenance, independent review and consumer/installed gates below
remain open; these checkpoints do not complete Task1.

- [ ] RED malformed/version/ID/frame/timeout/disconnect and cancellation cases.
- [ ] Real isolated rebuilt daemon queries/configuration/pause/revoke/reconnect,
  exact without a model, actual semantic/code locations and preview cancellation.
- [ ] Verify no stub leakage, stale results or denied-path reads; client depends
  only on engine contract/transport, not GUI or native inference build tooling.
- [ ] Cover affected Rust/.NET protocol callers, commit and independently review.

## Task 2: Lumen Rust adapter, identity and existing feature preservation

Create an isolated Lumen checkout from verified main; follow its actual AGENTS
and Bun/Rust/Tauri gates. Keep the user's existing branch and untracked files.
Integrate through its native service boundary and composition root. Default the
packaged app to the staged WinSearch engine; make an unavailable engine an honest
recoverable state with confined exact fallback, without a second active content
indexer/model pipeline.

Map streamed engine results into current SearchService/Zod payloads without
rewriting React search/selection/settings components. Keep native ordering,
metadata, provenance, actual page/time/cell and bounded excerpts. Preserve
Recent/Related/pins/history and answer-context admission. Migrate existing
identity/metadata transactionally or retain a stable host mapping; do not erase
indexes, pins or history. Never manufacture file hashes/revisions from entry IDs.
Route preview/open through current admitted identities and native confinement.

- [ ] RED adapter tests for current IDs/abort/status, root hydration/empty roots,
  config ordering, latest delivery, filters/limits and malformed native payloads.
- [ ] Native tests for actual daemon lifetime, restart, two-profile isolation,
  exact/no-model, semantic/code search, root revocation and answer/preview safety.
- [ ] Prove existing Recent/Related/pins/history and granted provider/app-content
  features survive; no UI-side process/model/credential execution is introduced.
- [ ] Full required Lumen tests/typecheck/lint/native build and isolated native
  UI evidence; commit and independently review before publication.

## Task 3: Engine-only payload and installed Lumen acceptance

Reuse the final WinSearch release assembly's daemon/workers/DLLs/notices/hashes
for an engine-only payload. It must run without WinUI, developer PATH or .NET.
Lumen's pinned staging/build consumes that exact artifact or explicit local
verified build output. Do not invent a published release URL, upload a release,
silently download a model or change trust/consent to make staging pass.

- [ ] Missing/corrupt executable/DLL/hash REDs and clean engine payload manifest.
- [ ] Run the installed Lumen payload outside both checkouts, prove the owned
  headless engine connection and responsive typed search/preview/settings flow.
- [ ] Verify cancellation, reconnect/exit, two apps/profiles, source/root changes,
  model-unavailable exact behavior and cleanup/data preservation.
- [ ] Record actual runtime/backend/installed/fixture evidence separately; all
  existing v1 release/hardware gates still apply. Publish/attach every requested
  PR with both integration and standalone acceptance limits stated accurately.
