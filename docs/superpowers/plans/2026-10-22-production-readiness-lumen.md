# WinSearch production readiness and Lumen integration plan

> **For agentic workers:** Use `executing-plans` to implement this plan task by task. Work directly by default. Each task needs its own regression checks and review before advancing its dependent tasks.

**Goal:** Deliver a reproducible Windows engine package and an installed Lumen app that uses WinSearch as its authoritative local search, indexing and embedding engine without losing user data or weakening source admission.

**Architecture:** Keep the Rust daemon, SQLite indexes, extraction/inference workers and typed `ws-client`. Lumen's Rust host owns verified startup, process handles, grants, cancellation, user metadata and native actions; its existing SearchService remains the UI boundary. Engine content belongs in WinSearch; pins/history belong in a small host ledger.

**Tech stack:** Rust, Windows named pipes and Job Objects, SQLite/FTS5, EmbeddingGemma 2, existing native extractors, Python acceptance tools, and the existing Tauri/React Lumen consumer.

## Global constraints

- This is an engine-and-Lumen release plan. The approved standalone WinUI roadmap remains open and separately tracked; this plan does not redefine full WinSearch v1 as complete.
- Preserve one authoritative crawler, content index and embedding lifecycle per host profile. The ordinary/code databases are separate namespaces within that engine, not competing application pipelines.
- Preserve per-user pipe security, process-bound authentication, retained process handles, kill-on-close jobs, deferred admission and explicit empty roots.
- Keep credentials, provider calls, file reads, process execution and consent in the existing Rust native boundaries. No new frontend execution permissions or automatic model downloads.
- Preserve exact-first search and no-model exact search; typed terminal outcomes; latest-query selection; bounded delivery; filters before candidate limits; source/policy/model fences.
- Preserve the 768-dimensional embedding space, pipeline 2, current numerical tolerances, semantic floor .65, frozen retrieval labels and Recall@5 >= .90 / MRR >= .85 / negative false positives == 0. Report known category misses.
- Preserve CPU fallback. Qualified NPU text and 140/280-token images remain distinct from 70-token images/audio, which use the existing CPU/GPU path. Do not change automatic backend preference on the strength of one timing sample.
- Preserve Lumen's aggregate 64 KiB UTF-8 text, 4 MiB passive-raster and 40x12 spreadsheet preview limits; existing Edge and 120/240 Hz performance gates remain binding.
- A source proof, database ID, file ID or path is not a content hash or a grant. Unknown hashes stay unknown.
- Keep recoverable backups and legacy content until migration/rollback checks pass. Never directly copy a live SQLite main file while ignoring its WAL.
- All shell commands use `rtk`; fetch and compare both repositories before implementation. Preserve unrelated checkouts and user work. Publication, merging and release deployment are separate actions.

## Audited baseline: 2026-10-22

Assumed load: one Windows desktop user, Lumen plus occasional CLI/standalone clients, concurrent indexing and interactive search, up to 200,000 files with multiple extracted segments per file. This is not a multi-tenant network service.

| Component | Verified current state | Implication |
| --- | --- | --- |
| WinSearch main | Local clean `b12a452d7db43b5f6086e57d637c7a23d7599fb0`, equal to freshly fetched `origin/main` | Main includes PRs #2/#3 and bounded server writes, but not the later NPU completion branch |
| WinSearch PR #4 | Open, non-draft, mergeable; remote head `41c8dd344f21a6f668e0be76b2fd6f6ae102dfd3` | Current review statuses are not Windows build/runtime CI. No checked-in GitHub workflow exists on main or this branch |
| NPU local checkout | `codex/npu-embeddinggemma2-completion` checkout at `5f4b7d3`; remote branch has advanced | Do not execute from this stale checkout without reconciling it |
| Qualified NPU payload | Checked-in evidence binds runtime `85a915d460b1fa751bbe86f289ff77c18c032c25`, manifest `171aedbb142c712520b768f524ca848a79aab519081cfc0209e8781e96d7390d` | Actual hardware/package evidence exists for this checkpoint; it does not certify subsequent runtime changes |
| Later PR #4 changes | `b176b44` and `41c8dd3` change worker/supervisor/model behavior and tools after `85a915d` | Rebuild and requalify the chosen candidate; the evidence README's blanket claim that later changes are tooling-only is now stale |
| Lumen main | Freshly fetched `a61ca3c6b890dbed765fb2ba6a4fd15dfa76d76a` | Inspect this baseline, not only the old integration checkout |
| Lumen PR #27 | Merged 2026-10-20; hosted Verify and package and CodeQL checks passed | Verified staging/startup is delivered. Reimplementing this foundation would duplicate work |
| Lumen payload pin | Main still pins engine `85a915d` and manifest `171aedbb...` | Synchronize the next engine artifact, client dependency and compiled pin together |
| Lumen live search | `EngineRuntime` exposes startup/status; `IndexRuntime::open` starts its inventory/content worker and `start_index_lifecycle` starts its additional lifecycle | Live search cutover and retirement of the legacy pipeline are still required |

Sources: [WinSearch PR #4](https://github.com/KB01111/WinSearch-lumen-engine/pull/4), [Lumen PR #27](https://github.com/KB01111/Lumen/pull/27), `docs/v1-gap-matrix.md`, `docs/engine-payload.md`, `docs/headless-client.md`, and `docs/validation/2026-10-20-npu-completion/README.md` on the NPU branch. Lumen source was read in the main checkout at `C:/Users/kevin/Projekt/Lumen/.worktrees/lumen-tailwind-einui` and compared with fetched main.

Checks run for this audit:

- `rtk proxy python -m unittest discover -s tools -p test_*.py -v`: 16 passed on current WinSearch main. These are package/media helper contract tests, not native engine acceptance.
- `rtk cargo fmt --all -- --check` from `src/engine`: failed, exit 1, with existing differences across engine and vendored source. No formatting changes were applied.
- Git source/branch comparison and live PR status queries were performed. The engine and Lumen were not installed or launched for this audit.
- Historical NPU evidence reports 721 Rust passes/11 ignored, 170 .NET passes, actual CPU/NPU references and copied-package runs at its frozen checkpoint. Those suites were not rerun here and are not current-head acceptance.

## Findings and required decisions

### Must fix before production

1. **Source, package and evidence describe different candidates.** Main's assembler pins `d4f1448`; Lumen pins `85a915d`; PR #4 is now `41c8dd3`. A build from the latest source is not automatically covered by the older artifact's tests. Freeze one candidate, regenerate its proof and update every consumer pin atomically. See `tools/assemble-engine-payload.py:17` and Lumen `src-tauri/winsearch-payload.lock.json:3`.

2. **Server work can grow beyond the client bounds.** `ws-platform/src/pipe.rs:291` starts a thread per accepted connection and uses unlimited pipe instances at line 318. `server.rs:183` polls idle reads every millisecond without a handshake/partial-frame deadline. `engine.rs:1112` starts semantic threads without an ordinary-search admission cap. Connection churn, partial frames or superseded searches can retain threads/work even though one `ws-client` has semaphores. Bound server admission and frame progress; prove cancellation and control responsiveness under overload. These are source findings, not a reproduced resource-exhaustion benchmark.

3. **A stalled model download cannot promptly honor Cancel.** `ws-modelmgr/src/download.rs:141` sets a 30-minute global timeout; line 198 performs a blocking body read and checks cancellation only between reads. A server that sends headers and then stops can leave the operation active and prevent replacement/deletion. Add an interruptible transport boundary and a stalled-server regression. Context7's current upstream documentation mentions per-read options, but the locally installed locked `ureq 3.4.2` source has no `timeout_per_read` setter; do not paste an API from newer upstream into this build.

4. **Lumen startup is wired; native search and folder-policy forwarding are not.** Lumen `lib.rs:238-246` creates both runtimes, while `winsearch_engine.rs:635` offers status only. Forward search/Related/configuration through the owned client, then replace the legacy worker startup. Otherwise the app either keeps searching its old index or creates two competing pipelines.

5. **Cutover would strand existing controls and user metadata.** The current engine `ClientMessage` has no source-bound external enrichment ingestion or explicit full-index reset operation. Lumen already has enrichment, history, pins and reset-related behavior. Implement the required engine operations and transactional host mapping before retiring those routes; preserve every existing caller, including answers, MCP and Windows AI.

6. **Reproducible engine CI and final installed acceptance are missing.** Review-bot success is not a Windows build, parser/pipe test, actual-model run or signed upgrade test. Add engine-owned hosted checks, then test the exact installed Lumen artifact and recoverability.

### Should finish in the same release program

7. **Current scale and power evidence is incomplete.** The documented native 1k/10k/50k/200k matrix is open. The vector store scans FP16 segments; file count alone hides memory growth from document/media chunks. Historical offline and whole-harness timings cannot establish current query percentiles or concurrent-indexing behavior. Measure first; add a new vector/index architecture only if these measurements require it.

8. **Source/preview limits must survive the adapter.** Ordinary complete-byte hashing stops at 64 MiB (`ws-extract/src/source.rs:8,97`); extraction totals can be unknown; rich uncached/raster/media previews remain unfinished. Preserve truthful unknown/partial/unsupported states and current-source checks. Complete missing functionality inside the existing confined native preview boundary rather than broadening webview access.

9. **Recovery needs a combined test matrix.** Durable queues and isolated workers already exist, but final-candidate interrupted migration, low disk, source replacement, disconnected drives, sleep/resume, malformed media and model cancellation still need combined native/package evidence. Treat a rebuilt index as a new identity namespace and prevent old mappings from silently resolving to different files.

10. **Documentation and lint baselines need reconciliation.** README still describes merged PR #2 as the active implementation PR; headless-client prose still quotes a 4,096-result ceiling despite capability-gated 10,000 support. Correct these together with formatting/Clippy debt in a separately reviewable baseline change. Do not hide failures with warning suppression.

No wholesale rewrite, network service, new UI search abstraction or replacement vector database is justified by this audit. The existing architecture fits the intended integration.

## Milestones and dependency order

| Milestone | Tasks | Exit condition |
| --- | --- | --- |
| A: Engine candidate ready for integration | 1-5 | One reviewed source candidate, bounded server/model lifecycle, reset/enrichment contracts, repeatable Windows checks and verified payload |
| B: Lumen uses the engine | 6-8 | Typed native routing, acknowledged grants, metadata preservation and one active content/index/model lifecycle; real two-profile integration passes |
| C: Production release candidate | 9-11 | Scale/quality/recovery gates, synchronized payload, final-head hosted checks and installed signed upgrade acceptance pass |

Task 6 can begin against the existing client while engine work proceeds, but its native acceptance must use the final candidate. Task 7's metadata-only startup must be selected before normal live cutover; never temporarily enable two content pipelines as the default.

## Task 1: Establish one candidate and executable Windows verification

**Owner:** WinSearch; coordinate the final artifact contract with Lumen.

**Files:** `src/engine/Cargo.toml`, `src/engine/Cargo.lock`; affected Rust files with formatting/lint failures; create `.github/workflows/ci.yml`, `tools/verify-engine.ps1`, `docs/validation/2026-10-22-production-baseline/README.md`; update `README.md`, `docs/headless-client.md`, `docs/v1-gap-matrix.md`.

**Consumes:** fetched main and the reviewed PR #4 delta. **Produces:** one immutable source candidate and a machine-readable verification ledger; no automatic merge or publication.

- [ ] Compare current main, local NPU checkout and remote PR #4. Reconcile all valid review fixes, especially post-launch XRT retirement, timeout quarantine, optional import and download progress behavior. Use a dedicated implementation checkout when isolation is needed.
- [ ] Correct the stale evidence claim about later runtime changes. Preserve old reports as historical evidence and create a new report for the chosen source.
- [ ] Establish a separately reviewed formatting/lint baseline. Run fmt, strict Clippy and tests as separate commands so one exit code cannot conceal another. Inspect generated/vendored changes rather than blindly reformatting the tree.
- [ ] Add hosted Windows MSVC jobs for locked build/tests, native pipe/watcher tests, package helper contracts and .NET protocol tests/build. Record toolchain and pinned native dependency versions and upload failure logs.
- [ ] Separate deterministic CI from explicit model/hardware qualification. A skipped hardware gate must remain pending for any advertised backend that requires it.
- [ ] Verify `ws-client` can build independently without CMake, inference weights or a frontend. Update the 10,000-result capability documentation.

**Validation:** engine `rtk cargo fmt --all -- --check`; `rtk cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`; `rtk cargo test --locked --workspace --all-targets --all-features`; root `rtk dotnet test WinSearch.slnx --nologo` and Release x64 WinUI build; Python discovery command above. Resolve platform/native prerequisites explicitly. List every ignored test and its separate acceptance disposition.

**Done:** the selected head has repeatable Windows gates and no concealed failing mandatory check. Review/commit this baseline before new behavior.

## Task 2: Bound server connections, reads and search work

**Files:** modify `src/engine/crates/ws-platform/src/pipe.rs`, `pipe_tests.rs`, `src/engine/bins/winsearchd/src/server.rs`, `engine.rs`, `ipc_writer_tests.rs`; create `src/engine/bins/winsearchd/src/ipc_admission_tests.rs`; update `docs/ipc-protocol.md`.

**Consumes:** existing framed protocol, cancellation flags and five-second write deadline. **Produces:** bounded daemon-side admission with existing typed error/outcome behavior.

- [ ] Add failing native tests for silent pre-Hello peers, trickled headers/bodies, repeated canceled searches, disconnect during work and a healthy control client beside stalled clients.
- [ ] Use a starting design limit of 32 connected clients and 8 active ordinary searches per daemon, with no unbounded pending queue. Keep existing two-preview/two-Related limits. Reject excess work with the existing recoverable NotReady behavior; release permits on every completion, cancellation, disconnect and spawn failure. Count work until it actually retires.
- [ ] Bound Hello to five seconds and each started inbound frame to a five-second absolute deadline; trickle traffic must not extend it. Idle established status subscribers can remain connected. Retire partial transports and drain canceled native I/O safely.
- [ ] Replace the one-millisecond idle polling path with waitable/native cancellation-aware reads. Ensure independent Stop and configuration handling remain reachable during slow exact planning as well as inference.
- [ ] Retain existing absolute write deadlines, no I/O under the global registry lock, source fences and typed interrupted-search outcomes.
- [ ] Exercise 33 connections and 9 active searches, return to baseline thread/handle counts after cleanup, and verify a healthy query/configuration path recovers. Record sampled peak memory and handles; do not equate these tests with a large-corpus benchmark.

**Validation:** focused pipe/server/admission tests, existing IPC writer regressions, then affected workspace tests and outside-checkout package lifecycle tests. Preserve the unchanged 90-second package watchdog.

**Done:** overload and partial frames cannot retain unbounded work; cancellation/Stop is proven with actual pipes. Review/commit before package certification.

## Task 3: Make model cancellation responsive during network stalls

**Files:** `src/engine/crates/ws-modelmgr/src/download.rs`, `manager.rs`; add `download_transport.rs` if the interruptible transport cannot remain focused in `download.rs`; daemon `model.rs`; corresponding client/protocol tests if a public state changes.

**Consumes:** verified `.part` files, Range resume and the current model operation state. **Produces:** a bounded cancellation result with resumable bytes and no later success from a canceled operation.

- [ ] Extend the local HTTP fixture to send valid headers and a prefix, then stop sending while keeping the socket open. Cancel after the blocked read starts. Also cover a stall before headers and a slow transfer that continues making progress.
- [ ] Implement an interruptible network boundary with at most one second between cancellation observations. Use the locked ureq transport's actual supported interfaces; inspect its connector/transport source and current docs before coding. An upstream-only per-read setter is not an implementation. Keep the existing overall transfer bound and resumable verification.
- [ ] Require cancellation to reach a terminal paused/canceled state within two seconds in the controlled stalled-server test; preserve `.part` and verify resumed bytes against the original manifest. A continuing slow transfer must succeed without a short total-body timeout.
- [ ] Verify canceled download/import/replacement/delete operations cannot overwrite newer lifecycle state, report backend readiness after worker retirement, or leave an indefinitely active operation. Keep current download-only Cancel semantics explicit unless a separately tested additive operation extends them.
- [ ] Recheck missing/corrupt models, interrupted import, no-model exact search, NPU quarantine/reset, CPU fallback and post-launch retirement guards on the selected candidate.

**Validation:** ws-modelmgr stalled-server/resume tests, daemon model and embed supervisor suites; current NPU dispatch guard fixture; explicit real-worker/native package tests. Review/commit the cancellation fix independently.

## Task 4: Add an explicit, recoverable engine index reset

**Files:** create `src/engine/crates/ws-protocol/src/reset.rs`; modify protocol `lib.rs`, `messages.rs`; daemon `server.rs`, `engine.rs`, `indexer.rs`, `code.rs`; ordinary/code stores; Rust client; C# protocol/client compatibility tests; `docs/ipc-protocol.md`.

**Consumes:** current ordinary/code database identities and acknowledged folder configuration. **Produces:** capability-gated typed reset with fresh identities and a clear completion boundary.

- [ ] Write contract/native regressions for reset during indexing/query/preview, crash between ordinary/code reset stages, old references, repeated requests and a lost acknowledgment.
- [ ] Define a typed `ResetIndex` request and `IndexReset` response under `index.reset`. Scope it to engine-owned ordinary/code content and durable jobs; preserve model files and saved folder policy. Return the new independent database identities after successful reset.
- [ ] Fence admission first, cancel/drain old work, durably record reset intent and recover to a coherent reset after restart. Only acknowledge once both namespaces are coherent; SQLite transactions in separate files are not one cross-file transaction.
- [ ] Do not delete host pins/history. Invalidate their old engine bindings and re-resolve through current admitted sources. A missing acknowledgment uses the existing mutation-unknown restart/reconciliation rule rather than blindly retrying destructive work.
- [ ] Prove another host profile and standalone WinSearch data remain unchanged.

**Validation:** protocol round trips, store restart/crash fixtures, Rust/C# unsupported-peer tests and owned two-profile native tests. Review/commit independently of ingestion.

## Task 5: Preserve consented source-bound enrichment in the engine

**Files:** create `src/engine/crates/ws-protocol/src/enrichment.rs`; protocol exports/messages, daemon server/engine/extraction/indexer/store, `ws-client`; Lumen `src-tauri/src/search/enrichment.rs`, `enrichment_tests.rs`; protocol documentation.

**Consumes:** an existing bound entry reference, an expected complete-source SHA-256, extraction kind/location, bounded text and the host's current consent/admission generation. **Produces:** capability-gated idempotent ingestion into the same engine content lifecycle, with a new current content revision.

- [ ] Write rejection tests for changed/replaced source, stale entry reference, missing expected hash, revoked root/consent, oversized text, invalid location, duplicate result, interrupted commit and cross-profile IDs.
- [ ] Define `IngestEnrichment`/`EnrichmentIngested` under `entry.enrichment`. Restrict the first contract to the existing consented OCR/transcription outputs; cap a request at 64 KiB aggregate UTF-8 text and validate actual page/time/line locations. Keep jobs and vectors engine-owned.
- [ ] Keep credentials/network execution in Lumen's current consented native provider path. Have the engine independently validate the current source and expected bytes before atomic commit; revalidate the host grant before sending and before acknowledging delivery.
- [ ] Make retries idempotent using source identity, hash, enrichment kind and revision; reject stale responses after reset/model/root changes. No text may be attached to an arbitrary raw path.
- [ ] Preserve failures as honest unavailable/partial states. Larger or unattested sources cannot be accepted by inventing a hash or silently lifting read budgets; use a bounded background source-attestation job before permitting that enrichment.

**Validation:** store/daemon/client contract tests and actual consented test-fixture ingestion through Lumen's Rust adapter. Fixture provider output tests admission, not a live cloud provider. Review/commit this contract before legacy enrichment retirement.

## Task 6: Wire Lumen's existing native service boundary

**Lumen files:** `src-tauri/src/search/winsearch_engine.rs`, `indexing.rs`, `root_policy.rs`, `types.rs`, `mod.rs`, `lib.rs`; create `src-tauri/src/search/winsearch_adapter.rs` and `winsearch_adapter_tests.rs`; existing `src/services/search/development-file-search-service.ts`, its tests, `catalogue-search-service.ts`, search schemas and selection tests only where mapping requires changes.

**Consumes:** `Client::for_process`, `search`, `related`, `resolve_entry`, `preview_text`, indexing/model configuration and typed status subscriptions. **Produces:** the existing Lumen SearchService payloads backed by the verified engine.

- [ ] Add adapter tests for mode/category/kind/extension intersections, requested limits through 10,000, exact-first accumulation, final empty batches, and every terminal/semantic state including legacy unknown.
- [ ] Add root-policy tests for hydration, explicitly empty roots, exclusions/hidden/size settings, ordered grant changes, pause/resume/revoke and restart. Keep the daemon unadmitted until current settings are acknowledged; current folder grants override saved engine policy.
- [ ] Forward search through the native owned client and stream accumulated snapshots using the already implemented consumer. Carry request IDs/AbortSignal to native cancellation; reject old delivery after query/grant/model generation changes. Do not replace the SearchService interface with engine internals.
- [ ] Preserve empty-query Recent via the host ledger, Related via the typed engine operation, and application/settings/calculator behavior through the existing catalogue composition. Preserve limits before candidate selection instead of filtering a truncated host reply.
- [ ] Extend EngineRuntime to expose controlled query/configuration/subscription operations while retaining process ownership. On `MutationOutcomeUnknown`, close admission, stop/restart the owned child, reconcile state and then create a fresh client.
- [ ] Map status, model progress, retries and recoverable errors into current settings/activity contracts. Model-file presence must not imply usable inference. Missing payload/model must leave a recoverable state; no second content indexer starts as a fallback.

**Validation:** adapter unit/native tests against the rebuilt owned daemon, no-model exact search, semantic/code results, actual locations, empty/superseded queries, two-profile wrong-PID checks, Stop and restart; existing frontend service/selection suites. Review/commit before changing the default composition root.

## Task 7: Migrate the host ledger and retire the duplicate lifecycle

**Lumen files:** `src-tauri/src/search/index.rs`, `indexing.rs`, `index_worker.rs`, `embedding.rs`, `enrichment.rs`, `lib.rs`; create `search/engine_bindings.rs` and `search/engine_migration_tests.rs`; affected native answer, MCP and Windows AI source consumers discovered by a full caller search.

**Consumes:** existing stable IDs, file identity/metadata, pins, open/query history and fresh ordinary/code engine namespaces. **Produces:** metadata-only Lumen startup with durable two-way engine bindings.

- [ ] Add a metadata-only open path that starts neither inventory/content workers nor the embedding/enrichment lifecycle. Keep its stop/generation state independent from the active engine's grant/request state.
- [ ] Take a recoverable SQLite-consistent backup and verify integrity before migration. Simulate termination before/inside/after the migration transaction; reopening must produce either the intact old ledger or the committed new one.
- [ ] Preserve existing host stable IDs, pin timestamps, history preferences and open counts. Store bindings using host profile + ordinary/code database identity + engine entry ID, with both lookup directions. A matching path alone does not prove the same file after replacement.
- [ ] Preserve metadata-only pins without fabricated extraction/hash fields. Represent disconnected/deleted/unresolved entries honestly; require current engine resolution and grants before any read/action.
- [ ] Switch the default composition root only after Tasks 4-6 are available. Remove both legacy worker startup sites, not only `start_index_lifecycle`; verify thread joins and no legacy content/vector writes after cutover.
- [ ] Exercise upgrade, repeat migration, engine rebuild/reset and rollback to the recoverable ledger. Never silently reinterpret an old engine ID under a new database namespace.

**Validation:** migration fault injection, pin/history/Recent regression suite, profile isolation, restart/reset, content/vector write counters and a live query during indexing. Review/commit the cutover separately from the adapter.

## Task 8: Finish every source-consuming path and preserve previews

**Engine files:** `crates/ws-extract/src/source.rs`, platform `source_guard.rs`, daemon extraction/entry/preview paths; protocol preview/entry contracts if additive fields are required.

**Lumen files:** `search/preview.rs`, `opening.rs`, metadata and answer-context paths, `engine_bindings.rs`; existing native MCP/Windows AI callers, preview service tests and installed editor action tests.

**Consumes:** freshly resolved source references, current grants, real hashes where available and actual locations. **Produces:** safe preview/action/answer behavior after cutover with truthful partial/unsupported states.

- [ ] Search all callers of the legacy content/location lookup methods. Route Recent, Related, pins, preview, open, containing folder, answer context, MCP and Windows AI through current bindings and admission.
- [ ] Test same-size/mtime change-and-restore, file replacement, rename, reparse/cloud-placeholder paths, root revoke during delivery, engine restart and unknown hashes. Unknown source attestation must not become a stale trusted answer or cached preview.
- [ ] Complete bounded background attestation for larger/legacy sources when required by a supported consumer. Preserve the 64 MiB synchronous hash budget and cancellation; commit a hash only when complete bytes and change detection are verified. Expose exact indexed/source totals or explicit unknown/capped state.
- [ ] Keep existing text/raster/spreadsheet previews functional. Add engine-backed rich image/PDF-page/media cache or stream support only through confined native handles/cache identities with size/eviction/cancellation bounds. No raw filesystem URL or unrestricted webview asset route.
- [ ] Map PDF pages, real audio/video times, source lines and notebook cell-local locations. Verify supported editor open-at-line with installed editors. Unsupported playback/navigation must be visibly unavailable rather than claim success.
- [ ] Preserve extraction behavior for every advertised format using valid, malformed, oversized and interrupted inputs. Track standalone rich-player/explorer UX separately; it cannot be claimed complete from a headless preview test.

**Validation:** existing provenance/source notification regressions, bounded preview tests, negative denied-path tests across all native callers, actual PDF/media/native parser cases and installed actions. Review/commit at each independently testable preview or consumer boundary.

## Task 9: Establish current quality, scale and resource acceptance

**Files:** existing `tools/test-search-parity.py`, `test-retrieval.py`, `test-media-runtime.py`, `test-media-resources.py`; create `tools/test-engine-scale.py`; benchmark hooks in `crates/ws-index/examples/bench.rs` only when needed; `docs/benchmarks.md` and new dated validation directory.

**Consumes:** final engine candidate and integrated Lumen native path. **Produces:** reproducible reports with source/binary/model/corpus/driver/host identity and separate synthetic/native/installed scopes.

- [ ] Build fixed 1k/10k/50k/200k corpora; report both file counts and extracted segment counts. Record bytes, modality mix, filters/exclusions and warm/cold definitions. Separate generated indexes from real filesystem/model runs.
- [ ] Measure exact first-batch and complete search p50/p95/p99, warm semantic/hybrid latency, 1,000/10,000-result delivery, code/Related latency, incremental indexing, startup/recovery, memory/private bytes, handles, disk/scratch and idle behavior.
- [ ] Repeat with indexing/extraction active, rapid query replacement and limited CPU/GPU availability. Measure native revocation and independent Stop latency. Test AC, battery, battery saver, schedule, fullscreen and sleep/resume.
- [ ] Use these proposed initial release budgets as explicit targets: warm exact first-batch p95 <=100 ms / p99 <=250 ms; warm semantic terminal p95 <=750 ms / p99 <=1,500 ms; cancellation/independent Stop <=1 s; quiescent daemon average CPU <1% over ten minutes. Declare the reference host/configuration before collecting results. These are proposed product targets, not existing measured guarantees or permission to relax earlier gates; cold model load and bulk-result completion are reported separately.
- [ ] Set and record numeric memory/disk ceilings for each supported modality/backend before executing its acceptance run. Base them on the declared corpus segment count, the 1,536-byte raw FP16 vector cost per 768-dimensional segment and actual worker peaks; file count alone is insufficient. Preserve existing media-specific memory/scratch gates. Do not claim a supported maximum until the declared ceilings pass.
- [ ] Run all five frozen retrieval suites and negative controls, native parser/pipe parity, actual text/image/audio references and backend fallback on identical candidate binaries. Required numerical cosine remains >=.999 where specified by existing references.
- [ ] Qualify CPU as the universal route; independently qualify every CUDA/Vulkan/NPU route advertised in the release. Missing hardware stays an explicit unsupported/pending release claim. Keep the known 70-token NPU exclusion and live driver-loss limits explicit.
- [ ] Optimize only measured failing paths. First examine policy/source rechecks, lock duration, repeated candidate work, batching and segment residency. Any ANN/mmap/new dependency proposal needs a demonstrated failing envelope and a separate design review.

**Done:** all declared supported profiles meet unchanged quality/resource gates and the recorded latency targets. Retain failed samples and category misses; no causal speed claims from unrelated whole-harness timings.

## Task 10: Prove recovery and bounded diagnostics

**Files:** ordinary/code store recovery tests, daemon watcher/indexer/embed/extract/model tests; create `tools/test-engine-recovery.py`; Lumen migration/recovery tests; daemon logging setup and diagnostics documentation.

**Consumes:** durable queues, profile identities, owned processes and migration backups. **Produces:** recovery evidence without stale results, lost user metadata or escaped workers.

- [ ] Kill daemon/extractor/embed worker during each durable stage, then restart. Prove claims recover, no stale source commits, current policy is acknowledged again and no duplicate engine/worker survives.
- [ ] Inject low disk, failed writes, interrupted schema migration/reset, locked/corrupt DB, corrupted native DLL/payload, interrupted model transfer/import, missing bundle and retired NPU state. Surface recoverable errors and preserve exact search whenever its prerequisites remain valid.
- [ ] Disconnect/reconnect drives, overflow watchers and sleep/resume during indexing/search. Verify identity continuity or explicit new namespace and cache invalidation as appropriate.
- [ ] Prove another profile and standalone WinSearch remain unaffected by Stop, reset, failed migration or uninstall of Lumen.
- [ ] Add bounded log retention and a sanitized diagnostic summary using existing tracing/status: versions/backend, queue counts, resource failures and error codes. Do not export source text, credentials or user paths without explicit user action. Verify logging failure does not break search.
- [ ] Enumerate every ignored hardware/installed test and run the required subset explicitly. A skip is not a passing recovery result.

**Done:** the combined native/package matrix passes with empty owned jobs, intact ledger/history and recorded recovery timings. Review/commit fixes with their fault regressions.

## Task 11: Rebuild, stage and accept the exact installed release

**WinSearch files:** `tools/assemble-engine-payload.py`, its tests, native/NPU package tools, third-party pins/notices, payload guide and final validation ledger.

**Lumen files:** `src-tauri/Cargo.toml`, `src-tauri/Cargo.lock`, `src-tauri/src/search/winsearch_engine.rs`, `src-tauri/winsearch-payload.lock.json`, `scripts/lib/winsearch-payload.mjs`, staging tests and current release/signature/smoke scripts.

**Consumes:** final tested source, dependency closure and completed integration. **Produces:** a reviewable signed installed release candidate; publication remains a separate step.

- [ ] Build the final source in the selected release profile. Apply required runtime signatures before recording the final executable/DLL hashes and generating the payload manifest; signing changes bytes. Generate source/binary/native dependency provenance, third-party notices and the model-free engine payload. Fail on missing, stale, stub or unexpected components, and never sign or modify payload bytes after pinning their manifest without regenerating and revalidating it.
- [ ] Requalify copied-package CPU and every advertised accelerated route outside both checkouts with a minimal PATH. Preserve the original 90-second lifecycle watchdog and 1,000/1,100-result assertions; separately report contention behavior.
- [ ] Assemble the corresponding-source archive, pinned build recipes/patches and dependency license materials required by `src/engine/third_party/ffmpeg-source.md` alongside the distributable candidate. Verify their correspondence to shipped binaries; upstream links alone do not satisfy the repository's delivery requirement.
- [ ] Update Lumen's client revision/lockfile, compiled engine commit, strict allowlist and independent manifest hash together. Stage to the fixed resource path, then verify staged and build-copied bytes. Reject corrupt/missing resources and retain the previous valid staged package on staging failure.
- [ ] Run Lumen's current mandatory typecheck, lint, unit, Edge functional, production profiling, strict Rust and locked Tauri release build. Run the explicit native engine test even if the broad Rust suite ignores it.
- [ ] Install the candidate on a clean supported Windows environment without developer tools, models or AMD runtime assumptions. Test empty-root startup, explicit model import/download, exact/no-model and actual semantic search, settings, pause/resume, preview/action/context, Stop, restart and two-profile isolation.
- [ ] Test data-preserving upgrade from the existing Lumen schema and the currently staged engine, interrupted upgrade and recoverable rollback; verify signatures, installer resource closure, startup registration and uninstall/data-retention policy. Use a disposable test profile/environment for uninstall.
- [ ] Capture keyboard/focus/accessibility and the existing native UI performance gates during integrated search. A browser fixture run cannot certify installed native behavior.
- [ ] Inspect all required hosted checks on the final engine and Lumen heads, protection/review requirements and unresolved review threads. Keep readiness open while a required check or supported-profile gate is pending.
- [ ] Update README/gap matrix/package guide/PR descriptions with the exact candidate and remaining scope. No automatic merge, release upload or public announcement is part of this planning task.

**Done:** the artifact tested is the artifact installed and staged, all required checks pass, recovery preserves user data, and supported backend/platform claims match actual evidence.

## Commands and evidence conventions

Use each implementation checkout's actual paths; `WinSearch` below means the engine repository and `Lumen` means the consumer repository. Run commands independently, inspect their exit codes, and attach the command/cwd/head to the report.

```powershell
# WinSearch/src/engine: deterministic source gates
rtk cargo fmt --all -- --check
rtk cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
rtk cargo test --locked --workspace --all-targets --all-features
rtk cargo build --locked --release --workspace
rtk cargo check --locked -p ws-client

# WinSearch root: consumer protocol and helper gates
rtk dotnet test WinSearch.slnx --nologo
rtk dotnet build src/app/WinSearch.App/WinSearch.App.csproj -c Release -p:Platform=x64 --nologo
rtk proxy python -m unittest discover -s tools -p test_*.py -v

# Lumen root, after verified payload staging
rtk bun run typecheck
rtk bun run lint
rtk bun run test
rtk bun run test:e2e
rtk bun run test:performance
rtk bun run tauri build

# Lumen/src-tauri: native source gates
rtk cargo fmt --all -- --check
rtk cargo clippy --locked --all-targets --all-features -- -D warnings
rtk cargo test --locked --all-features
```

Hardware/reference/package acceptance additionally uses the existing native tools with explicit external model/fixture paths and fresh report destinations. These inputs must be recorded from the verified local environment; this plan intentionally does not hard-code another session's artifact paths or run commands with invented model inputs.

Every evidence record must include source commit, relevant source hashes, executable/DLL/manifest hashes, corpus/model/reference hashes, OS/driver/backend, command, timestamps, exit status, metrics, skips and retained failures. Label fixture, native, copied-package, installed, hosted and physical-hardware evidence separately.

## Release acceptance checklist

- [ ] Candidate identity agrees across source, manifest, client, compiled pin, staged bytes and installed payload.
- [ ] Server/worker/network work is bounded; cancellation, admission and Stop remain responsive under load.
- [ ] Current folder grants are acknowledged before results; revoked/stale sources cannot reach preview, actions, answers, MCP or Windows AI.
- [ ] One crawler/content/vector lifecycle runs per host profile; the host ledger preserves pins/history and independent index namespaces.
- [ ] Reset/enrichment/model/settings behaviors remain available and recoverable after cutover.
- [ ] Actual model references, frozen retrieval and negative controls pass without changing denominators or tolerances.
- [ ] Declared scale, segment, backend, latency, memory, disk and power envelopes pass; unsupported hardware is named honestly.
- [ ] Crash/migration/upgrade/rollback and clean installed behavior pass on the exact artifact.
- [ ] Final-head hosted checks and required reviews pass; signing and license/distribution obligations are satisfied.
- [ ] Standalone WinUI roadmap items remain separately open until their own installed acceptance passes.

## Recommended first execution slice

Start with Task 1 and the native regressions in Tasks 2-3. They make the candidate reproducible and close concrete lifecycle gaps before changing live Lumen search. Then deliver reset/enrichment and the native adapter as separate reviewable changes, perform ledger migration, and finish with integrated scale/recovery/installed acceptance. Do not spend this first slice building the standalone explorer or replacing existing UI architecture.

## Audit limits

This was a source/contract/evidence audit of startup, transport, search scheduling, model downloads, storage identity/provenance, package tools and Lumen's current composition root. Existing documentation and selected tests were inspected; every parser, ranking branch, third-party native implementation and UI interaction was not re-audited line by line. Full Rust/.NET/native/model/hardware/installer suites were not rerun in this turn. The proposed budgets and new operation names are planned requirements, not claims about implemented or measured behavior.
