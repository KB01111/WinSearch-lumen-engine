# Task 1 implementation report

Status: DONE. Scope: bounded existing document/image/OCR extraction only.

## Implemented

- Added the real `winsearch-extract` workspace binary. It reports `doc`, `pdf` (legacy IFilter), and `image` capabilities, waits for its framed assignment before opening input, uses the existing extractors, and exits after one response. Audio/video and nonzero continuation cursors return explicit unsupported outcomes.
- Added the exact `ws_extract::worker::{ExtractRequest, ExtractResponse}` interfaces from the brief, serde on existing output/status/draft types, `WorkerHello`, request encoding/reading, and parent response validation. Existing 4-byte little-endian JSON framing is reused, with 256 KiB encoded-request and 16 MiB response bounds, 256 drafts, 64 KiB draft/error strings, bounded kind/path strings, matching modality kinds, ordered positive page/line metadata, ordered valid integer time ranges representable in storage, and forward-only cursor checks. Numeric wire fields are integers; nonfinite or incorrectly typed JSON cannot deserialize into them.
- Added `ExtractClient::extract(&ExtractRequest, Duration)` and the production-required `extract_while(..., allowed)` cancellation variant. Explicit errors distinguish timeout, cancellation, malformed output, worker failure, and unsupported input. Missing executables fail; there is no production in-process fallback.
- Added `ws_platform::child_job::ChildJob`, using current Windows API declarations and Context7 documentation. Each job has a 1 GiB job-wide memory limit, kill-on-last-job-handle-close, and no breakaway flags. Workers launch with `CREATE_NO_WINDOW`. Assignment completes before the parent starts the request writer.
- Changed the daemon's production registry to the child client. Existing injected-provider fixtures still use the original registry path. The existing two extraction slots remain unchanged. The default batch deadline is 120 seconds.
- Polling cancellation checks current pause/content/root/consent policy and actual source length, precise filesystem modification time, and file identity. The source snapshot is checked again at final commit. Existing entry/job commit checks remain. Cancellation requeues rather than poisoning the job.
- Added exclusive per-request directories under `cache/extract`. The parent validates every referenced thumbnail's metadata and canonical component containment, rejects reparse traversal, copies at most 16 MiB into unique `cache/thumbs` names using exclusive creation, and removes scratch before returning success. Referenced output files survive scratch deletion. A daemon artifact lease removes newly promoted files on stale policy/source state or failed commit. Unreferenced scratch files are removed. Scratch cleanup errors are surfaced rather than reported as successful extraction.

## Ownership and bounded cleanup

The `ChildJob` owns the real `std::process::Child` and an RAII `OwnedHandle` for its unnamed, noninherited Job Object. Native assignment failure kills only that child. A direct child is reaped only after a finite native `WaitForSingleObject` confirms exit. Cleanup queries job accounting until `ActiveProcesses == 0`, including decoder descendants, within the same one-second process/tree cleanup grace.

The exchange owns one stdin writer thread and one stdout reader thread. The deadline runs on the parent independently of either blocking pipe operation. All ordinary completion, crash, malformed-output, timeout, and cancellation results pass through explicit cleanup: terminate the owned job, verify/reap the direct child, verify the job is empty, cancel synchronous pipe I/O, then verify both actual Windows thread handles have terminated before joining. Pipe-task cleanup has a separate one-second grace. A cleanup failure is an explicit worker failure; the code neither force-kills Rust threads nor claims cleanup succeeded if Windows termination cannot be verified. RAII provides a second cleanup attempt for unwinding/failure paths.

The scratch owner is outside the exchange, so process/pipe cleanup precedes scratch deletion. Promoted artifacts are retained only after explicit scratch cleanup succeeds. No process is killed by executable name or by a discovered unrelated PID.

## Red/green evidence

All commands were run from `src/engine` unless noted; shell invocations were RTK-prefixed after reading RTK instructions.

1. RED, before protocol implementation: `rtk cargo test -p ws-extract worker::tests --lib` failed compilation with 16 missing-type/helper errors (new request/response/validation API did not exist). GREEN: the same command passed **3 tests**, 22 filtered out.
2. RED, before client implementation: `rtk cargo test -p winsearchd extract_client::tests --bin winsearchd` failed compilation with 32 missing imports/API references in the new test scaffold. These were compile-red failures, not claimed runtime assertion failures. GREEN after implementation and building the real binary: **6 tests passed**, including actual native hostile fixtures and worker processes.
3. RED for the native job slice: `rtk cargo test -p ws-platform child_job::tests --lib` failed with two feature-gated `JobObjects` import errors before the required workspace Windows feature was available. GREEN after the feature/owner implementation: **1 test passed**, 14 filtered out. It queries the actual memory/kill-on-close configuration and verifies the owned process terminates on owner drop.
4. Runtime RED during expanded verification: the selected-suite run failed with **119 daemon tests passed, 1 failed**. The real worker's explicit nonzero-cursor unsupported response had `total_units: 0`; parent validation correctly rejected a total below the requested cursor. Fixed the worker to omit totals for unsupported/failed responses and for capped legacy results. GREEN: `rtk cargo test -p winsearchd real_extract_binary_returns_actual_text_lines_and_explicit_unsupported --bin winsearchd` passed **1 test**, 119 filtered out, after rebuilding `winsearch-extract`.

## Final verification

- `rtk cargo build -p winsearch-extract` passed before native worker integration testing. The actual generated-text and PNG tests use that binary, not a substitute provider.
- `rtk cargo build -p winsearch-extract -p winsearchd` passed. The daemon build reported seven existing dead-code warnings in catalog/embed/engine/extraction fixture entrypoints/indexer; the new unused-import warning found during iteration was fixed. No warnings were suppressed to hide a failure.
- With process-local `WINSEARCH_DATA_DIR=C:/Users/kevin/.codex/worktrees/winsearch-v1/WinSearch/src/engine/target/task1-tests/data` and `WINSEARCH_INSTANCE_ID=task1-extraction-tests`, ran:

  `cargo test -p ws-protocol -p ws-platform -p ws-index -p ws-extract -p ws-embed-protocol -p winsearchd -p winsearch-extract -- --test-threads=1`

  through `rtk proxy powershell -NoProfile -Command ...`. Final result: **246 passed, 0 failed, 0 ignored**: winsearchd 120, ws-embed-protocol 4, ws-extract 25, ws-index 70, ws-platform 15, ws-protocol 11 plus one protocol integration test; new binary/doc-test targets have zero tests. Raw final output is in ignored `src/engine/target/task1-tests/selected-suite.log`.
- After the final explicit scratch-cleanup error propagation change, `rtk cargo test -p winsearchd extract --bin winsearchd` passed **17 tests**, 103 filtered out. This includes the six client/native-process tests and existing extraction regressions.
- `rtk git diff --check` passed; focused `rustfmt --check` passed for all five new Rust source/fixture files.

Native transport cases use a separately compiled task-owned fixture, generated temporary content, and unique cache/scratch roots. Covered: an oversized response length, mid-frame exit, an existing task-owned artifact outside scratch, a 150 KB Unicode request whose child never reads stdin (600 ms deadline, return/cleanup under two seconds), a hung real descendant tree (700 ms deadline), active cancellation, missing binaries, actual text/line metadata, explicit unsupported kinds/cursors, capped-total truthfulness, and usable promoted native PNG thumbnails. Returning the expected timeout also requires successful explicit native process/thread cleanup. The existing ignored real `winsearch-embed.exe` and DLLs were reused unchanged by the preexisting real-binary tests.

## Self-review and limits

- Reviewed framing allocation bounds, JSON types, modality/count/string/cursor validation, canonical path containment and reparse rejection, unique destination creation, cleanup ordering, early returns, process/thread handle ownership, descendant termination, stale-policy/source disposal, and production routing without fallback. No threshold or existing test was weakened.
- `extract_client.rs` remains one planned module, approximately 400 implementation lines plus native fixture tests. It owns transport/artifact lifetime; no additional framework or dependency was introduced.
- Continuation persistence and complete extraction beyond the legacy 256-draft cap are Task 2. Nonzero cursors are deliberately unsupported now; capped results do not fabricate a known total. PDF remains the existing legacy IFilter behavior until the PDF slice. Audio/video are unsupported. Existing byte/archive/decompression/pixel limits remain; no entire-media-file loading was added.
- Entry rows still persist modification time in seconds, and resting watcher/queue generation semantics remain unchanged. The precise active-request/final-commit snapshot does not solve same-size, same-second changes that never enqueue a job. Task 2 must address durable source generations.
- Native tests verify configured Job Object memory limits and actual lifecycle cleanup; they do not deliberately exhaust 1 GiB. Native PNG/WIC and actual WinRT OCR invocation are exercised, while deterministic OCR-word assertions still use the existing injected-provider fixtures. This is not a claim about OCR quality or codec availability on every machine.
- Installer/package distribution must include the new sibling `winsearch-extract.exe` in its later packaging slice. No public frontend IPC schema, .NET project, or UI file changed, so .NET/WinUI checks were not required or run for this task. No normal user daemon/app was launched, personal content was not opened, and no live model inference rebuild was attempted.

## Files changed

`src/engine/{Cargo.toml,Cargo.lock}`; new `bins/winsearch-extract/{Cargo.toml,src/main.rs}`; `bins/winsearchd/src/{main.rs,extract.rs,extract_client.rs}`; new `bins/winsearchd/tests/fixtures/extract_transport.rs`; `crates/ws-extract/{Cargo.toml,src/lib.rs,src/budget.rs,src/worker.rs}`; `crates/ws-platform/src/{lib.rs,child_job.rs}`; this report. Future code-slice planning documents are intentionally outside this commit.

## Review fix round 1

Addressed Important findings 1 and 2 from `task-1-review.md`; the exceptional ownership statement in the initial report is superseded by the retained-owner implementation below.

- Anchored the freshly created scratch directory **before launch** using its canonical pathname, full 64-bit volume serial, and full 128-bit Windows file identity. Directory metadata handles use `FILE_READ_ATTRIBUTES`, `FILE_FLAG_BACKUP_SEMANTICS`, and `FILE_FLAG_OPEN_REPARSE_POINT`, and close through `OwnedHandle`. After the job is verified empty, promotion rechecks the scratch root's metadata/reparse guard, canonical path, and identity before opening any artifact. Source containment compares against the original anchor. A real hostile fixture replaces the root with a junction and returns an external file's real path; it is rejected, the junction is removed, and the generated external file remains intact.
- Cleanup only takes a thread handle for joining after its native termination is verified. Incomplete tasks and the job remain owned; `closed` becomes true only after actual cleanup completes. A single permanent retained owner/reaper holds the cleanup queue and its own native thread handle. Explicit cleanup failure transfers the job, pipe tasks, and scratch owner into that queue, independently of the client's lifetime. The reaper retries finite cleanup operations and deletes scratch only after process/thread cleanup succeeds. The client refuses another extraction while its retained cleanup is pending, preventing repeated failures from accumulating new tasks across the two production slots. No Rust thread is force-killed and no unverified thread is joined or detached.
- The regression deliberately keeps a real child's stdin/stdout endpoints owned by a delayed thread beyond the one-second cleanup grace. It verifies the error is bounded, the live thread stays owned, another extraction is blocked before scratch/process setup, dropping the client does not abandon cleanup, and releasing the delayed task lets the retained owner join it, close handles, remove scratch, and clear pending state. Ordinary timeout/crash/cancel and real-worker cases remain covered.

RED, before either fix:

1. `rtk cargo test -p winsearchd native_extract_rejects_replaced_scratch_root --bin winsearchd`: **0 passed, 1 failed, 121 filtered out**; assertion output began `replacement accepted: Ok(ExtractResponse ... thumbnail_hint: ...cache\\thumbs...)`. This executed the full owned-worker exploit using generated content, rather than only the predicate probe.
2. `rtk cargo test -p winsearchd native_cleanup_failure_retains --bin winsearchd`: **0 passed, 1 failed, 121 filtered out**; assertion output was `cleanup failure detached the task owning both pipe endpoints`.

GREEN after the respective fixes: each command above passed **1 test**, 121 filtered out. After the retained-owner/client-drop/admission checks and final ownership review, `rtk cargo test -p winsearchd extract --bin winsearchd` passed **19 tests, 0 failures, 0 ignored, 103 filtered out**, in 1.83 seconds. `rtk git diff --check` and focused `rustfmt --check` passed. No full-suite, build, .NET, or unrelated checks were repeated solely for this scoped review round; the initial full-suite evidence remains above. This fix changes only `extract_client.rs`, the task-owned transport fixture, and this report.

The seven previously captured daemon warning diagnostics were at `catalog.rs:65` (`scan_catalog`), `embed_client.rs:111` (`WorkerSnapshot`), `embed_client.rs:300` (unused supervisor methods, also lines 307/386/706/742/766/861), `engine.rs:103` (unused entrypoints/hooks, also 113/153/158/261), `extract.rs:95` (`set_budget_factory`), `extract.rs:119` (`extract_once_with`), and `indexer.rs:473` (fixture statistics methods, also 478/486). Their cleanup is deferred per controller instruction; no blanket warning suppression or extra warning-only build was added.

If Windows cannot verify termination, cleanup remains explicitly pending and owned, and that client stays blocked; this is not reported as successful cleanup. Later batching/media/package acceptance remains outside Task 1.

## Review fix round 2

Addressed the new Important queue-integration finding from `task-1-rereview-1.md` without changing store schema or failure thresholds.

- Added a distinct `ExtractClientError::AdmissionUnavailable` result and a read-only admission query. Pending retained cleanup is no longer classified as an extraction/IO failure.
- The indexer checks admission before claiming and again under the store lock. If cleanup starts between claim and client admission, the existing no-failure `requeue_extract_job` operation releases the claim without incrementing attempts or touching FTS, vectors, or thumbnails. The explicit `ExtractStepOutcome::admission_unavailable` flag keeps this state separate from idle work and actual per-file failures.
- Both production extraction slots take a 100 ms backoff on admission unavailability, then recheck readiness. They neither consume claims in a busy loop nor wait for an unrelated index notification to resume.
- Added two indexer-level regressions using an actual owned native child, a delayed thread owning its real pipe endpoints, and the retained reaper. One holds cleanup before the claim; the other triggers it from the budget factory after a real claim. Each repeatedly attempts unrelated queued files, verifies exact unchanged job state/claimed flags/attempts/priority, preserves complete stored vector segments and searchable FTS artifacts, releases cleanup, and verifies successful actual-worker processing of both files afterward. The fixture utility exists only under `cfg(test)` and does not change production providers.

RED: `rtk cargo test -p winsearchd native_retained_cleanup_ --bin winsearchd -- --test-threads=1` produced **0 passed, 2 failed, 122 filtered out**. Both assertions reported `admission unavailability consumed unrelated job state/attempts`: the actual jobs had **attempts: 3** instead of their original **attempts: 0**, reproducing the caller/store failure-budget bug before either production fix.

GREEN: the same command passed **2 tests, 0 failures, 122 filtered out** in 3.63 seconds. After final outcome assertions, `rtk cargo test -p winsearchd extract --bin winsearchd` passed **21 tests, 0 failures, 0 ignored, 103 filtered out** in 2.20 seconds, covering the native transport/retained-owner/junction cases and the new queue integration regressions. `rtk git diff --check` and focused `rustfmt --check` for `extract_client.rs` passed. No unrelated suite/build or warning-only cleanup was added.

Changed source is limited to `extract_client.rs`, `extract.rs`, and this report. Existing seven build warnings and later batching/media/package gates remain deferred. Review of the final error path confirms admission unavailability returns before `fail_extract_job_capped` and artifact clearing; existing actual extraction failures retain their previous retry/poison policy.
