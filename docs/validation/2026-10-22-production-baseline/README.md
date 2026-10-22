# Production implementation baseline

Implementation starts from PR #4 head `41c8dd344f21a6f668e0be76b2fd6f6ae102dfd3`,
including runtime changes after the older qualified `85a915d` artifact. That artifact
remains historical evidence; no new source or package is certified by reusing it.

The formatting baseline is a separate commit with no intended behavior change.
Baseline `ws-modelmgr` ran 23 tests and `ws-platform` ran 28 tests, all passing.
The implementation checkpoint adds reproducible source gates, bounded pipe/search
admission, interruptible model downloads, typed match sources and recoverable reset.
It is not production or installed-Lumen acceptance.

## Local verification

- Rust workspace/all-targets/all-features: **753 passed, 11 ignored**, 28 suites.
  This includes the native PDF/media workers, pipe overload/deadlines, search
  admission, download stalls/address fallback, and reset recovery/native pipe tests.
- Strict workspace Clippy (`-D warnings`) and rustfmt: passed. Build notices explicitly
  report CUDA compiled, Vulkan absent, and XRT absent; compilation is not backend quality.
- Independent Rust client, 21 Python contracts and **181 .NET tests**: passed.
- Native prerequisite preparation: passed twice, including dependency hash inventory.
- The final reset namespace-generation error cleanup also passed its two focused
  recovery tests after the full workspace run.

Raw checks are retained locally in `artifacts/production-tests-reset` and
`artifacts/production-contracts-reset`; structured ledgers capture source revision,
dirty diff/untracked hashes, commands, timestamps and exit codes. The full source
run excludes any later enrichment or Lumen migration change.

## Enrichment contract checkpoint

The subsequent engine implementation adds bounded image OCR/audio transcript
ingestion through a current ordinary source reference, independently hashes the
source, and atomically records text, pending vectors and idempotency receipts.
Unknown legacy caches cannot acquire provenance from the new provider hash.
The acknowledgment is fenced again after waiting for the peer writer. Source
hash reads now check cancellation/admission between 64 KiB reads.

The enrichment workspace snapshot passed **764 Rust tests, 11 ignored** across
28 suites in the isolated `engine-validation` Cargo profile. Later delivery and
cancellable-hash changes passed 257 protocol/client/store/extractor tests and four
daemon enrichment tests. The latter include actual named-pipe ingestion and
cached OCR preview, plus source/root/model/cancel/request-reuse rejection. The
provider outputs and sources are generated fixtures, not live provider accuracy.
These changes still require Lumen consented fixture delivery and final rebuilt
payload/native/installed acceptance. Current builds use the XRT stub; the real
SDK and models must be selected and requalified separately.

## Retained scale failure and diagnostics work

The [failed debug scale run](exact-scale-debug-failure.json) used 1,000 generated
files (800 extracted text chunks), with no semantic model. It retained 100 warm
20-result samples and 37 bulk samples before the one-hour watchdog stopped the
owned process tree. Warm 20-result first-batch p50/p95/p99 were
5,509/8,725/10,716 ms, far above the proposed 100/250 ms p95/p99 targets. Independent
Stop took 28.48 ms and the owned job emptied. Peaks were 26,234,880 private bytes,
89,923,584 working-set bytes and 10,540,525 data bytes. This was a debug harness
validation run under changing host load, not release or installed acceptance.
It is a measured failure requiring profiling; no specific cause is established.

Bounded logging and a sanitized CLI diagnostics export have focused source tests.
Testing also identified mandatory log-directory creation before the best-effort
logger; the shared layout now treats logging failure as optional while retaining
mandatory database/model/cache errors. The focused logging, layout and diagnostics
tests and strict workspace Clippy pass.

The subsequent full source gate passed **771 Rust tests, 11 ignored**, 28 suites;
the command/source ledger and raw logs are in `artifacts/production-tests-diagnostics`.
The [copied native recovery fixture](native-reset-recovery.json) passed blocked-log
startup/search, independent ordinary/code namespace reset, stale-reference/replay
refusal, model/policy preservation, other-profile isolation, Stop, restart with
fresh grant admission, sanitized CLI export and four SQLite integrity checks.
Its hashes include the runtime source files, executables and adjacent libraries.
It used the debug profile and stub XRT build, with no semantic model.

The first harness attempt masked its reset error by deleting the deployment
before stopping processes; cleanup is now ordered before temporary-file removal.
The corrected failure exposed a real ten-second reset-drain timeout while shell
catalog enumeration held a work ticket. A controlled regression failed before
moving read-only enumeration outside the commit ticket. Its captured policy epoch
now rejects a catalog snapshot that crossed reset before it can mutate the store.
The later controlled regression, all 30 indexer tests and the native fixture pass.
The timeout and workload were retained rather than enlarged or skipped.

Release containment now uses OS `CREATE_SUSPENDED` plus Job Object assignment
before resumption. An actual process/descendant fixture verifies this boundary;
the former debug-only stdin gate could not establish it for optimized binaries.
The broader low-disk/crash/device/installed Task 10 matrix remains open.

## Review contract regressions

Review exposed two valid enrichment gaps: post-commit-capable errors previously
left later client mutations enabled, and multiple transcript ranges could be
out of order or overlap. Both regressions failed before the fixes and now pass.
The client poisons its control lane before releasing queued writers; protocol
validation retains the existing timestamp/duration bounds and additionally checks
ordering across parts.

The proposed proxy-connector reorder was rejected after inspecting pinned ureq
3.4.2 and its current upstream documentation. CONNECT recursively invokes the
same chain without proxy configuration to obtain the bounded TCP transport.
An actual local proxy fixture passes a tunneled HTTP body and cancellation during
CONNECT response, real TLS handshake and body stalls with the original order.
The proposed TCP-first order failed that fixture by bypassing the proxy; the
original order was restored. These are local transport fixtures, not live
corporate-proxy or public TLS certificate qualification.

## Copied release checkpoint and measured latency failure

The [release runtime checkpoint](release-runtime-checkpoint.json) binds the clean
`ae7c8c6` CPU/NPU candidate to its native-source, executable, library, dependency
and model hashes. Its copied native recovery fixture passed, including independent
Stop in 97.98 ms, fresh grant admission after restart, and four SQLite integrity
checks. An explicitly invoked real-NPU reference test passed unchanged cosine
thresholds of at least 0.999. CPU/NPU media reference fixtures and ten frozen
retrieval suites passed; all owned process jobs emptied. These are local copied
candidate checks. The two media runs overlapped, so their timings do not qualify
quiet performance. They do not establish installed Lumen acceptance or signing.

The [retained release scale failure](exact-scale-release-failure.json) uses the
same candidate and 1,000 generated files. Its 100 warm 20-result samples measured
p50/p95/p99 of 4,581/5,804/7,998 ms, failing the unchanged 100/250 ms p95/p99 targets.
After those samples and one bulk query, the operator stopped only the verified
owned daemon to begin profiling. Bulk percentiles and larger corpora remain
unmeasured; cleanup timing after that stop is not independent Stop evidence.

Aggregate source profiling found repeated live source-policy checks dominating
lexical planning. Query-pass reuse now has bounded memory and independent local
directory-change/policy fences, with live delivery/source checks retained. A
three-query debug diagnostic reduced guard calls from 5,654 to 1,473, but still
took seconds. This is diagnostic evidence, not release latency acceptance. The
copied candidate above predates these changes and cannot qualify them.

The [query-source checkpoint](query-source-checkpoint.json) records the subsequent
source gate: **782 Rust tests passed, 11 ignored, 28 suites**, strict workspace
Clippy and build passed. Corrupt generated PDB/object files blocked the first
attempt; its logs remain intact, and the successful retry disabled incremental
compilation. Review then found a watcher callback shutdown could reuse a closed
numeric handle. A deterministic regression reproduced that read; the loop now
exits on closed state. All four watcher tests, strict daemon Clippy and build
passed after that final change. No other runtime source changed after the full
suite. Rustfmt, diff checks and all 29 Python contracts pass.

The rebuilt unoptimized copied-native fixture passed reset/restart, fresh grants,
two-profile isolation, sanitized diagnostics and four integrity checks. Independent
Stop took 23.76 ms and both owned jobs emptied. This qualifies the recovery fixture
for those recorded bytes; it does not qualify new release latency or installed
Lumen behavior.

The clean `9b7b01d` release rebuild also passed. Its
[100-sample top-20 release run](exact-scale-query-cache-release-failure.json)
measured p50/p95/p99 of 1,984/2,324/3,023 ms on the same generated 1,000-file corpus,
still failing the unchanged latency targets. It completed without an operator
interruption; independent Stop took 22.76 ms and the owned job emptied. Peak job
private/working-set bytes were 20,353,024/82,800,640, and data peaked at 10,059,326
bytes, below the declared 2 GiB private/4 GiB data ceilings. These are separate
host runs, so they do not isolate a causal speedup. Bulk-result and larger-corpus
acceptance remain unmeasured. This candidate's separate copied recovery fixture
passed with independent Stop in 24.95 ms.

The [updated native-input proof](release-native-inputs.json) records independent
CPU/NPU references, all ten frozen retrieval suites and copied recovery for the
`9b7b01d` candidate. The assembler now pins that source and those bytes. Its
unsigned model-free manifest is
`059bea49c03e889de604dbd2c29af2deb410c2694c6d19099309295387fbce9b`.
The [copied package fixture](payload-native-9b7b01d.json) passed in 33.39 seconds
under the unchanged 90-second watchdog, outside the checkout with System32-only
PATH. It checked native text/PDF extraction, per-folder admission and bounded
1,000/1,100-result delivery. These functional checks do not override the failed
latency gate. Lumen's local development pins/staging match this manifest; its
expanded native adapter test still fails on queued code work and is under
investigation. Default cutover remains disabled.

The [code-event checkpoint](code-event-checkpoint.json) identifies that failure:
a notification on the explicitly excluded `private` folder retired the initial
code scan and deferred the entire root for 60 seconds. Excluded-only events now
leave admitted work alone. Admitted deletions, either side of crossing renames,
overflow and governing crawl controls still invalidate; even explicitly denied
controls must retire previously captured rules for an admitted directory. Both
regressions failed before their fixes. All 17 code tests, strict daemon Clippy and
build pass, and a controlled native diagnostic changes from no result on the old
release to a result on the corrected debug daemon. The corrected release and its
Lumen integration still need revalidation; these diagnostics do not qualify them.

## Remaining acceptance

Tasks 5-11 remain open. The Task 6 opt-in Lumen adapter has source regressions,
but its expanded native code-search test currently fails against the historical
85a915d payload. Default cutover remains disabled. The scale harness has retained
debug and release failures; neither passes production latency. Final source,
hosted, backend/model quality, package recovery, signing, installed upgrade/rollback
and strict UI/resource acceptance still need evidence on the final pinned candidate.
Existing 11 ignored
model/hardware/benchmark tests are not passes.

`tools/verify-engine.ps1` runs contracts, lint, test or release checks independently
and writes command, revision, timestamps and exit status alongside raw logs. Failure
stops the selected suite and still writes the ledger. GitHub's Windows jobs exercise
these source gates. Model/hardware tests that are ignored in those jobs still need
explicit qualification; a hosted source check cannot establish installed acceptance.
