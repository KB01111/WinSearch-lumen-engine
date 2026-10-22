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
