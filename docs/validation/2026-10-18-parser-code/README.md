# Shared parser capacity and genuine code retrieval

Every extraction client now shares two parser slots. Owned child/pipe/private
scratch cleanup retains its slot, including exceptional cleanup after client
Drop. Code jobs wait without charging source poison attempts. The engine and
background workers share one extractor registry, which future previews must reuse.

Actual managed-model testing exposed unrelated code hits that earlier worker-only
checks missed: overriding the worker's model path had left ModelManager and native
code vectors unavailable. The stronger harness imports the pinned artifacts,
checks their staged hashes and requires complete native 768-dimensional code vectors.
Code search now corroborates the code query with the general-search task before
candidate truncation. Both use the unchanged .65 floor; original code cosine and
hybrid ranking, stored vectors, model and dimension remain unchanged.

## Current local evidence

- [Rust workspace and embedding-library checks](workspace-tests.txt): 508 passed,
  zero failed, including one API doctest; the worker's real-model case is ignored
  by default. It subsequently [passed explicitly](worker-roundtrip.txt).
  [Native embedding integration tests](native-integration-references.txt), including
  their model-gated cases, add nine passed, zero failed/ignored. Thus 518 checks
  executed successfully across these scopes; see [counts](test-counts.json).
  The daemon contributes 197 tests, including real owned-worker/Windows file-lock,
  durable queue, code admission, model-generation and source-revocation regressions.
  Native tests use the documented serialized method. This is no default-parallel,
  .NET, installed-app or hosted-CI claim.
- [Coherent CPU build](coherent-build.txt) passed for daemon/CLI/extractor/embedding
  worker and examples, with seven existing daemon dead-code warnings. GPU backends
  were explicitly disabled for this checkpoint. The established CMake cache uses
  a separate, verified clean pinned llama.cpp source checkout; build/install outputs
  stay in this worktree. An initial [cache-source mismatch](cpu-cache-path-failure.txt)
  and an intentionally [interrupted CUDA kernel build](cuda-build-interrupted.txt)
  are retained. Neither establishes CUDA acceptance.
- [No-model native run](native-no-model.json): ten public CLI/pipe comparisons,
  seven offline snapshot cases and eight typed-client checks pass, with zero code
  vectors and an empty owned job; 6.507 s total.
- [Managed CPU native run](native-managed-cpu.json): ModelManager ready, four of four
  native FP16 code vectors at width768, non-stub CPU worker. Twenty-one public
  comparisons and seventeen offline snapshot cases agree. The typed client retains
  its eight checks and adds three semantic-only/exact-control/negative checks;
  its semantic code DTOs equal the CLI/pipe results. Both requested Rust source
  lines and literal excerpts survive; the astronomy query returns no code hits.
  Cancellation and root revocation return empty terminal batches. All owned jobs
  finish empty. Index readiness 62.495 s; total harness 74.867 s, including staging,
  hashing and comparisons, rather than an installed latency measurement.
- Original native positive cases still meet Recall@5 .90 / MRR .85; the expanded
  group is .9091/.8636, and all ten empty controls pass. The screenshot miss and
  rank-two audio result remain visible. Adding code cases cannot dilute the
  separately enforced original positive subgroup.
- Production CLI [development](cli-code-development.json) and
  [held-out](cli-code-held-out.json) each pass Recall@5/MRR1 and negative FP0:
  eight positives and eight negatives per split, against sixteen generated code
  snippets. These are actual worker/store/ranker fixtures, separate from native
  parsing and installed editor behavior. Timings are 3.807/3.820 s per harness.
- Existing [names](cli-original-names.json),
  [development](cli-original-development.json) and
  [held-out](cli-original-held-out.json) suites were rerun on the same current CLI:
  Recall/MRR .933/.933, .938/.938 and .938/.906, respectively, with FP0 each.
  The cooking/application misses and rank-two soundtrack remain visible. No old
  labels, cutoffs or references were changed; these held-out results were not
  used to retune the new policy.
- [Media worker references](media-worker-results.json) preserve the .999 agreement
  gate; the minimum of four comparisons is .999698. The owned
  [run record](media-worker-run.json) retains current worker/model hashes.
  The explicit worker test passed during an earlier helper invocation whose
  subsequent media step had a [wrong fixture directory](reference-setup-failure.txt).
  That [setup failure](reference-setup-failure.json) is preserved; the corrected
  media run and all nine Rust integration cases pass unchanged.

## Frozen calibration and functional failures

The [source-text fixture](calibration-fixture.json) was frozen before measurement;
the [policy lock](query-policy-lock.json) predates held-out evaluation. Diagnostic
[development](calibration-development.json) and [held-out](calibration-heldout.json)
retain per-query outcomes and top-five scores. Both-task corroboration yields
Recall/MRR1 and FP0 on each split. Code-only admission yields FP1 on both;
normalized-mean queries fail held-out FP.625 and were not selected or retuned.
Duplicate full score matrices remain in local raw artifacts.

Functional REDs reproduce [third-client quota bypass](parser-capacity-red.txt),
[locked-scratch slot reuse](scratch-cleanup-red.txt),
[durable scheduling/poison charging](scheduler-admission-red.txt),
[daemon task affinity/cache omission](code-daemon-red.txt),
[CLI false positives](code-cli-red.txt) and the actual
[managed native astronomy failure](native-code-negative-red.json).
No quality/reference/resource threshold or old fixture label was relaxed.

Run the native harness from the root with a fresh artifacts output directory:

```powershell
python tools/test-search-parity.py --output artifacts/code-native-new --real-model --compare-offline --compare-client
python tools/test-retrieval.py code-development artifacts/code-development-new
python tools/test-retrieval.py code-held-out artifacts/code-held-out-new
```

These require the built binaries and pinned local models/fixture bundles. The
workspace command is `cargo test --workspace --exclude ws-embed -- --test-threads=1`,
followed by `cargo test -p ws-embed --lib`. Native reference tests use
`cargo test -p ws-embed --test native --test media --test reference -- --include-ignored --test-threads=1`
with a valid `WS_EMBED_TEST_MODEL`. CPU reproduction sets `WS_EMBED_NO_CUDA=1` and
`WS_EMBED_NO_VULKAN=1`; an existing cache also needs its matching `LLAMA_CPP_DIR`.

Published copies normalize private checkout/fixture paths and redact opaque native
identity proofs. Input/binary hashes and outcomes remain. Independent review could
not start because the agent thread limit was reached; no independent approval is
claimed. Full consumer previews/code identity/hash/Related/filters, Lumen migration,
WinUI flows, required scale/device/performance and installed release gates remain
open in the [gap matrix](../../v1-gap-matrix.md). This checkpoint is not full v1.
