# Terminal search outcomes checkpoint

This checkpoint adds `search.outcomes` to the existing version1 secured pipe.
It does not establish complete v1, Lumen integration, installed behavior or
hosted CI acceptance. The [README TODO](../../../README.md#v10-todo) remains open.

## Behavior

Terminal frames distinguish complete, superseded, canceled and failed requests.
Completed requests carry semantic disabled/ready/degraded and a closed reason
code. Progressive frames carry no outcome. Legacy omission stays unknown.
Rust/C# clients and the CLI enforce presence, placement and state consistency.
C# accepts the same exact lowercase wire codes as Rust, without numeric or
alternate-case aliases. Optional inference failure preserves exact results;
exact SQL failure emits a fixed public error and a failed terminal frame.

Readiness requires a successful embedding and eligible current vectors, before
the unchanged cosine thresholds and duplicate suppression. A completed lookup
with zero admitted hits can be ready; absent or incompatible vectors cannot.
Code availability is measured in its existing scan and checks the current model
fingerprint. Database/source-proof lookup errors are no longer swallowed in the
production code search path. Ordinary cached-vector errors propagate as well.

Delivery retains cancellation, native identity, source/root/policy, model and
request checks after acquiring the writer. Model generation is captured with
the request. Invalidated batches close empty as superseded/canceled; reused
request IDs suppress old work. An interrupted CLI request cannot publish earlier
hits as successful JSON. Complete CLI `--json` retains its stdout array and
reports the typed outcome on stderr.

## Verification status

Local verification now covers617 executed Rust checks and105 .NET tests. The
x64 app build passed with zero warnings/errors. The initial broader workspace
run failed an unchanged source change/restore integrity
test: parsed transient bytes were attested with the restored file's hash. It
also retained one model-gated ignored worker test. The failed run is preserved;
later passes cannot erase it. The explicit model-gated results and final native
profiles are recorded below.

The integrity failure reproduced in13/30 diagnostic runs. The fix adds an
advisory read-cache break notification across hashing/parsing, preserving the
original strict assertion and all existing digest, identity and metadata checks.
Ten isolated native probe pairs observed change/restore events, compatible reads
and writes without acknowledgement. Current53 extractor and20 platform checks
pass, including100 canceled-guard lifetimes with no handle-count increase.
Unsupported/ungrantable notifications retain an unknown hash; established
continuations already reject a missing previously established digest. The guard
and its pending I/O buffers are owned until cancellation completes inside the
existing bounded worker lifetime. This requires broader native validation.
See Microsoft's [read-oplock contract](https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_request_oplock)
and [completion-lifetime contract](https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/System/IO/fn.GetOverlappedResult.html).

The corrected workspace run passes591 tests with one model-gated worker case
ignored by default. That case was subsequently run explicitly with the actual
pinned model and passed. All25 embedding tests also pass with model-gated cases
included: ten text references have worst cosine drift1.32e-4; image, audio and
reduced-frame references retain their .999 floor (observed cosines .999942,
.999751, .999815 and .999698). The unchanged fast change/restore assertion then
passes30/30 independent samples without retrying a failed sample. These amount
to617 executed Rust checks across the workspace and explicit model runs; the
initial failed workspace run and default ignored state remain recorded.

The final coherent build passes with seven existing daemon dead-code warnings.
Native no-model passes10 public/seven offline cases in8.204s (index3.115s).
Actual managed CPU passes21 public/17 offline in138.776s (index120.334s), four
real768d code vectors and a ready ModelManager. Both retain the eight lifecycle,
four ordinary preview, four reference, four code-preview and four extension
checks, plus three new outcome checks; CPU retains three semantic checks. Native
source SHA256/revision checks pass and both owned jobs end empty.

Original positive gates remain Recall@5 .90/MRR .85; expanded positives
.9091/.8636 and ten empty controls FP0. The screenshot miss and audio rank2
remain. Whole-harness times include staging/hashes/indexing/inference and
comparisons. The CPU run is slower than the previous85.869s/index66.469s sample;
these different executable/backend-build and uncontrolled host samples do not
establish a causal performance change. Controlled overhead and scale gates
remain open.

Published logs/JSON and raw-versus-published byte hashes are listed in
`evidence-hashes.json`. Paths/SIDs/opaque proofs are redacted; model, source and
executable hashes remain. The verbose embedding trace is condensed to actual
test/reference lines, with the full local raw hash recorded. The host record
is reused, not freshly captured. No Rust build occurred between native profiles.

Functional failing controls cover absent/malformed terminal outcomes, premature
placement, invalid enum aliases, request-inconsistent states, model absence,
embedding/retrieval failures, code vector availability and CLI interruption.
Temporarily restoring swallowed exact SQL errors makes the new exact-failure
regression fail. Held-writer tests cover source/policy/model changes,
cancellation and reused IDs. Fake-vector unit fixtures remain separate from
actual model/native evidence.

The native smoke consumer now requires the advertised outcome, logs each
terminal state and requires a complete snapshot with the expected semantic
phase. It adds no implicit retry. The earlier pause/resume failure remains
[retained and unconfirmed](../2026-10-18-query-filters/README.md); later passing
runs or improved signaling cannot establish that failure's cause or fix.

No ranking, model recipe, reference tolerance, candidate cap or retrieval gate
was relaxed. The synchronous raw pipe writer still lacks a deadline. Independent
review remains unavailable after the earlier workspace-credit failure; this
checkpoint receives author review only. Final scale/resource/recovery, GPU
calibration, installed consumer, release and hosted review gates remain open.
NPU work remains outside the focus.

Lumen main was refreshed to `3b60ba4` without modifying its existing UI branch
or untracked files. Its production search contracts are unchanged from the
earlier audit; current CI/performance-test changes must remain in its future
integration baseline.

## Reproduce

Use the pinned model/dependency setup in the preceding
[code-provenance checkpoint](../2026-10-18-code-provenance/README.md).
From src/engine, run tests before the final coherent build:

```powershell
rtk proxy cargo test --workspace --exclude ws-embed --all-targets -- --test-threads=1
# Set WS_EMBED_TEST_MODEL to the actual pinned GGUF; sibling mmproj must exist.
rtk proxy cargo test -p ws-embed --all-targets -- --include-ignored --test-threads=1 --nocapture
rtk proxy cargo build -p winsearch-embed --bin winsearch-embed
rtk proxy cargo test -p winsearch-embed --bin winsearch-embed real_model_round_trip_through_worker_binary -- --ignored --test-threads=1 --nocapture
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, use fresh output directories and no Rust build
between the two native profiles:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/outcome-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/outcome-cpu-repro --real-model --compare-offline --compare-client
```
