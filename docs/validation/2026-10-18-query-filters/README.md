# Request scope and extension filters

This checkpoint fixes candidate starvation in existing app/file/category searches
and adds typed source-extension filters. Full v1.0, Related search, remaining
file-kind scopes, explicit outcomes and Lumen integration remain open in the
[README TODO](../../../README.md#v10-todo).

Previously, excluded entries could fill the exact8 or other500 candidate slots
before the engine applied its requested mode/category. Three crowded-index tests
reproduced a missing allowed result. A connection-local request view now filters
each exact/prefix/substring/token/content/path candidate source before its own
cap, including the content-FTS DISTINCT subquery. Native source/root admission
remains separate and unchanged. A new request replaces its previous filter;
metadata/preview admission does not inherit search scope. There is no persistent
schema migration, new dependency, vector-space change or cap increase.

`search.filters` advertises optional `SearchQuery.extensions`: up to64 single
extension components,1..32 ASCII letters/digits/`+`/`-`/`_`, optional leading dot,
case-insensitive. Wildcards/path syntax are rejected. Nonempty filters exclude
folders and synthetic calculator/settings results. Application filters select the
indexed shortcut/executable extension rather than its launch target. Ordinary
semantic entry IDs are filtered before vector top-k. Code exact/semantic reuse
their current admitted-file predicates before limits and exact-hit suppression.

Rust/C# require the separate capability for nonempty filters, validate bounds
before dispatch and reject out-of-filter/null-path/folder replies. Positive `.PDF`
replies remain usable. C# snapshots the query and its lists before awaits.
Legacy omitted extensions preserve existing behavior. The trusted Lumen host
must still enforce current grants; a search selector grants no filesystem access.

## Verification and limits

Final selected Rust checks: **493 passed**, zero failed/ignored:216 daemon,
44 client,44 code,53 extraction,96 index,18 platform and22 protocol.
The actual extractor suite separately passes18 tests (13 media/five PDF).
**89 .NET tests** pass, zero failed/skipped. The coherent CPU workspace binaries/
examples build retains seven existing daemon dead-code warnings; the x64 WinUI
build has zero warnings/errors. These are scoped local debug checks, not final
whole-workspace/installed acceptance.

The final coherent no-model run passes in **14.218s** (index5.642s). Managed CPU
passes in **85.869s** (index66.469s), with four actual768-dimensional code vectors
and a ready ModelManager. Both owned jobs end empty.
Five earlier same-source no-model samples and the earlier failure are kept
separately. The native test target rebuilt the extractor with a different byte
hash; the final executable set was rebuilt afterward before both final profiles.
Those final profiles must match the current binaries/harness. Earlier samples
do not establish repeatable latency or explain the failure.

The unchanged frozen native cases remain10 public/seven offline without a model
and21 public/17 offline with the CPU model. Separate extension cases cover PDF
case/dot normalization, excluded document extensions, ordinary text, actual
Unicode-named Rust source, excluded code extensions, notebook files and synthetic
result suppression. CPU adds filtered document/code semantic queries. A malformed
raw request must return `invalid_request`. Four typed-client filter checks add
positive PDF/code cases and two negative controls. Existing lifecycle, source
hashes, actual locations, previews, cancellation, revocation and owned-job cleanup
remain asserted. New filter cases do not change frozen quality denominators.

The semantic starvation regression uses fixed vectors on1,609 excluded real
files and one lower-scoring allowed PDF; it is a functional test, not real-model
quality or scale-performance evidence. Original CPU Recall@5 .90/MRR .85,
expanded.9091/.8636 and ten empty controls FP0 remain the acceptance baseline.
The screenshot miss and audio rank2 remain recorded. No model, ranker, numerical
or retrieval threshold was changed.

Functional REDs retain the three existing mode/category misses, ignored extension
selection, missing new-API validation/matching, legacy capability handling,
bad client replies, synthetic/code leakage and semantic top-k starvation.
The initial negative native test used a helper that asserts every error reply
is a failure. The daemon had returned the expected invalid-request response;
the helper now reads/asserts that response on its own disposable connection.

One subsequent native run returned an empty document result immediately after
the typed client's pause/resume sequence. **Its cause remains unconfirmed and
the failure is retained.** The original assertion remains strict. Diagnostics
now record discarded query epochs and one failure-only query/status probe.
One diagnostic run and five fresh exact-only runs then passed; no production
behavior fix was inferred from those passes. The five final samples and the
failed native JSON/log are retained separately. Superseded-query versus genuine
empty-result signaling remains a required contract task in the README.

Request admission can inspect more filtered rows than the old prematurely capped
search. Callback/row-inspection overhead is uncalibrated. Whole-harness times
include staging, hashes, indexing, inference and comparisons; they are uncontrolled
and establish no causal speed change or1k/10k/50k/200k latency acceptance.
GPU, idle/power, installed native Stop and raw stalled-writer gates remain open.

This checkpoint received author source review. Independent review remains
unavailable after the preceding workspace-credit failure; no cached-preview
review is inherited as approval for these changes. Broad final review and hosted
CI remain open. Published paths/SIDs/opaque proofs are redacted; actual source,
binary and model hashes remain. The manifest distinguishes raw local and
sanitized published byte hashes. The host record is reused, not freshly captured.

## Reproduce

Use the pinned CPU environment in the
[code-provenance checkpoint](../2026-10-18-code-provenance/README.md).
From src/engine, rebuild the final executable set after native tests, then run
both profiles without another Rust build between them:

```powershell
rtk proxy cargo test -p ws-code -p ws-extract -p winsearchd -p ws-protocol -p ws-index -p ws-client -p ws-platform --all-targets -- --test-threads=1
rtk proxy cargo test -p winsearch-extract --tests -- --test-threads=1
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, using fresh output directories:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/query-filter-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/query-filter-cpu-repro --real-model --compare-offline --compare-client
```

Related/semantic-outcome and finer scope contracts, the Lumen adapter/migration/
one indexing lifecycle, rich previews, standalone flows, release payloads and
installed acceptance remain unfinished. NPU work remains outside the focus.
