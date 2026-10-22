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

## Remaining acceptance

Tasks 5 and 7-11 remain open. The Task 6 opt-in Lumen adapter has source regressions,
but its expanded native code-search test currently fails against the historical
85a915d payload. Default cutover remains disabled. The scale harness is a draft and
has no measured result yet. Fresh hosted checks, backend/model quality, two-profile
reset/package recovery, signing, installed upgrade/rollback and strict UI/resource
acceptance still need evidence on the final pinned candidate. Existing 11 ignored
model/hardware/benchmark tests are not passes.

`tools/verify-engine.ps1` runs contracts, lint, test or release checks independently
and writes command, revision, timestamps and exit status alongside raw logs. Failure
stops the selected suite and still writes the ledger. GitHub's Windows jobs exercise
these source gates. Model/hardware tests that are ignored in those jobs still need
explicit qualification; a hosted source check cannot establish installed acceptance.
