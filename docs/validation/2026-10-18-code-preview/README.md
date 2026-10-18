# Cached code and notebook previews

This checkpoint adds cached code/notebook text to the existing native preview
operation. Full v1, Lumen integration, rich previews and installed acceptance
remain open. The [root README TODO](../../../README.md#v10-todo) tracks that work.

`entry.preview.code` accepts canonical code-file references and actual source-line
or notebook cell-local ranges. Page/time coordinates are rejected for code;
notebook coordinates remain invalid for ordinary entries. The engine reuses the
existing two-job preview admission, owned client reads and final source/policy/
request/writer checks. It starts no parser/inference and reads no source content.

The code store admits current native identity, enabled repository and current
parsed revision/snapshot/hash before selecting bounded cached text. SQL reads only
text prefixes/byte counts/actual locations, with matching provenance and location
filters before its row limit; vectors and parser inputs are not loaded. Aggregate
text remains64KiB UTF8 and64parts, preserving codepoint boundaries. Notebook outputs
never enter indexed/preview text even though the source hash includes full JSON.
Unknown source totals/cap states remain null. No schema migration is added.

Rust/C# negotiate the code capability separately, reject legacy peers before
sending a preview frame, and validate actual kinds, metadata/hash, locations,
cell ranges and budgets. C# copies notebook arguments before awaiting a handshake.
No new parser/inference/GUI dependency enters the Rust client; ws-code adds only
the existing shared ws-protocol dependency.

## Verification

The final selected Rust run passes **464 checks**, zero failed/ignored:210 daemon,
41 client,44 code,46 extraction,86 index,17 platform and20 protocol/contract.
The coherent CPU workspace binaries/examples build passes with seven existing
daemon dead-code warnings. **76 .NET tests** pass, zero failed/skipped; the x64
WinUI build passes with zero warnings/errors. These are local debug checks,
not one final-source full-workspace or installed-app acceptance run.

The rebuilt no-model native run passes10 public/seven offline snapshot cases in
**7.174s** (index3.879s). The managed CPU run passes21 public/17 offline cases in
**78.381s** (index63.504s), with four actual768-dimensional code vectors and a ready
ModelManager. Both include eight typed-client lifecycle checks, four ordinary
preview checks, four reference checks and four new code-preview checks. The CPU
run also retains three semantic checks. Actual code/notebook source-byte hashes
match independent Python digests; both owned jobs end empty.

The smoke client previews engine-supplied bound code/notebook references and
locations. It checks actual cached source text, the notebook's cell1/lines1..2,
exclusion of a generated private-output sentinel, root revocation and refusal of
old proofs after restore. Same-engine CLI/pipe DTO equality remains complete;
the existing intentional cross-policy client comparison excludes only the expired
source proof. Canonical IDs, ranking, excerpts and coordinates remain compared.

The original CPU positives remain Recall@5 .90/MRR .85; expanded positives
.9091/.8636, ten empty controls FP0. The screenshot miss and audio rank2 remain
recorded. No model, ranking or acceptance threshold changed. Whole-harness timings
include fixture staging/hashing/indexing/inference/comparisons; the preceding
separate76.239s sample establishes no causal performance change or scale acceptance.
The host record is reused from the preceding checkpoint; it is not a fresh
hardware/driver inventory. Published evidence
redacts private paths and opaque native identity proofs, retaining actual source,
binary and model hashes. The manifest distinguishes raw and published byte hashes.

The scoped independent GPT-6.1 Sol source review found no remaining confirmed
blocker. Its notebook newline finding was reproduced using five production-sized
chunks below16KiB each: aggregate truncation ending at a newline incorrectly
claimed the next line. The same defect was reproduced in the ordinary store and
Rust/C# clients. All four now count actual returned lines consistently; functional
RED logs and final passing regressions are retained. The reviewer ran no builds
or native/model tasks; this is not broad final-v1 security approval. No GPT-6 Sol
agent was used for this checkpoint.

Other functional RED logs retain missing code selection/routing/capabilities and
malformed-client response rejection. Separate new-test fixture failures are also
retained: adding a notebook after an unchanged configuration did not trigger an
immediate scan; active watchers deferred that scan by the existing60second retry;
one test attempted private-field access; and a later attempt waited for only one
of two files to parse. The final fixture creates both files before configuration
and waits for both committed provenance records. Production scheduling, timeouts,
source policies and resource/quality thresholds were unchanged.

The first broad run exposed five native media fixture failures: their pinned
decoder overrides contained parent traversal, intentionally rejected by the
existing path guard. Canonicalizing the shared fixture fixed four; a separate
audio override was then corrected. Both failed runs are retained. Production
decoder/worker paths use adjacent executables and remain unchanged; source review
found no confirmed production-path regression. A local shell invocation also
failed to find follow-up scripts after changing directories; absolute driver paths
were then used, and each final build/test log was checked independently of the
wrapper exit status.

## Reproduce and remaining work

Use the pinned CPU environment from the
[provenance checkpoint](../2026-10-18-code-provenance/README.md). From src/engine:

```powershell
rtk proxy cargo test -p ws-code -p ws-extract -p winsearchd -p ws-protocol -p ws-index -p ws-client -p ws-platform --all-targets -- --test-threads=1
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, use fresh output directories:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/code-preview-native-new --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/code-preview-native-cpu-new --real-model --compare-offline --compare-client
```

The synchronous raw server writer still lacks a deadline. Shared preview admission
and client deadlines do not establish stalled-writer/control or installed native
Stop acceptance. Ordinary byte provenance, rich/uncached preview, real totals/caps,
Related/filter/semantic outcomes, Lumen migration/one crawler, engine-only payload,
WinUI flows,1k/10k/50k/200k performance, CPU/GPU calibration and installed/release
gates remain required. No GPU/NPU, installed Lumen, interactive UI, scale or hosted
CI pass is claimed here.
