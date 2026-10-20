# Bound result references and code metadata

This checkpoint extends the draft v1 branch with current native result references
and code metadata. Full Task1, Lumen integration and v1 acceptance remain open.

Search hit IDs and ranking are unchanged. A separate optional `entry_reference`
contains canonical `e`/`c` IDs and a bounded current source proof. The proof includes
a native per-engine incarnation, policy/model/content revision and native source
snapshot. Existing files must match their stored native identity before and after
metadata reads; a newly created file at the old path cannot inherit the old row.
Code resolution also requires the enabled repository, current code opt-in/root,
sensitive/reparse/residency policy and current parsed source. Code/notebook byte
hashes come only from the existing complete-source parser provenance record;
ordinary-source hashes remain unknown. Metadata proofs are never content hashes.

The final selected Rust run passed **452 checks**, zero failed/ignored (209 daemon,
37 client,39 code,46 extraction,85 index,17 platform and19 protocol/contract).
All workspace CPU binaries/examples built successfully, retaining seven existing
daemon dead-code warnings. **60 .NET tests** passed, zero failed/skipped; the x64
WinUI build passed with zero warnings/errors. These are local debug checks.

The rebuilt native no-model run passes10 public/seven offline snapshot cases,
eight typed-client lifecycle checks, four preview and four bound-reference checks
in **7.450s** (index4.217s). The managed CPU run passes21 public/17 offline cases,
the same client checks plus three semantic checks in **76.239s** (index60.919s).
Both independently verify actual code/notebook byte hashes and end with empty
owned jobs; managed CPU has four actual768-dimensional code vectors and a ready
ModelManager. The original positive Recall@5 .90/MRR .85 and ten empty controls
with FP0 remain unchanged. Expanded positive scores are .9091/.8636; the screenshot
miss and audio rank2 remain visible. No model, ranking or acceptance gate changed.

These whole-harness timings include generated-source staging, hashing, indexing,
inference and comparisons. The previous separate code-provenance CPU sample was
133.656s; this is an uncontrolled comparison and establishes neither a causal
speedup nor scale acceptance. The host record is reused from the preceding run
on this HX370/Windows11 host and is labeled with its original capture time.
Published JSON preserves actual binary/model/input hashes; private paths and
opaque native identity proofs are redacted. The evidence manifest distinguishes
raw local bytes from published bytes. This is local generated-fixture/native
debug evidence, not hosted CI or an installed release.

Search and metadata delivery acquire the writer before final validation. Search
also checks request-token identity and source/query epochs through delivery; exact
duplicate suppression uses only IDs actually sent. Name hits retain their guarded
identity when content is unavailable, with stale body/location/cache data removed.
If native incarnation creation fails, exact search remains usable without a bound
reference. This does not add a deadline to the synchronous raw server writer.

Protocol1 remains additive. `search.entry_references` and `entry.resolve.code`
advertise the new surfaces; `entry.preview.text` remains ordinary cached text only.
Rust/C# clients validate canonical IDs, UTF-8 proof budgets, code/file domains,
hash format and positive content revision before delivery. Legacy omitted fields
remain usable. The native smoke consumer uses supplied references rather than
parsing semantic or long code-symbol hit IDs. Its code and notebook hashes are
independently compared with actual source-file digests by Python.

The independent GPT-6.1 Sol source review found no confirmed correctness blocker.
It requested actual held-writer and code-delivery coverage; both were added. The
writer regression deliberately holds the mutex while source/policy/request
identity changes. Moving validation before the mutex produced the retained
[functional RED](writer-order-red.txt); correct lock order is restored. The review
did not run builds or native/model tasks and is not full-v1 security acceptance.

Functional RED logs also retain missing serialization, cross-engine proof reuse,
physical replacement rebinding, parent traversal, missing code capability and
client trust-boundary failures. The initial new semantic replacement test expected
all results to disappear, although the fixture contains a second unchanged file.
Its corrected assertion explicitly requires the replaced source to be present
initially and verifies that only its hits disappear. No production filtering,
source policy, ranking, timeout or retrieval threshold was relaxed.

The first combined platform run also exposed a test-fixture precision error:
subtracting 1ns from Windows `SystemTime` does not create the assumed negative
fractional instant. The test now first asserts an actual representable 100ns
offset before checking negative-second flooring, following the
[standard library's platform precision](https://doc.rust-lang.org/stable/std/time/struct.SystemTime.html#platform-specific-behavior).
The production conversion was unchanged. Both initial fixture failures are
retained separately from the final passing suite.

The first managed native run reached a remaining full-reference comparison for
semantic code across the smoke client's deliberate policy revoke/restore. Its
only differing field was the expected changed `source_revision`; ordered hit IDs,
canonical reference IDs, scores, excerpts and locations were equal. That one
comparison now uses the same proof-only projection as exact cross-policy cases.
The [failed native run](native-policy-proof-comparison-failure.json) remains
retained, with its owned job empty. Fresh native runs use the final harness hash.

## Reproduce and remaining work

Use the pinned CPU runtime environment from the
[code provenance checkpoint](../2026-10-18-code-provenance/README.md). From src/engine:

```powershell
rtk proxy cargo test -p ws-code -p ws-extract -p winsearchd -p ws-protocol -p ws-index -p ws-client -p ws-platform --all-targets -- --test-threads=1
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, use fresh output directories:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/bound-references-native-new --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/bound-references-native-cpu-new --real-model --compare-offline --compare-client
```

Same-engine public CLI/pipe DTO comparisons include the full reference. The typed
client intentionally pauses/restores/revokes policy, so its cross-policy comparison
excludes only `source_revision` and retains the canonical reference ID plus every
ranking/location/excerpt field. Offline ranker snapshots continue comparing the
same ordered IDs/paths/locations/moments. These are distinct checks; none changes
retrieval acceptance thresholds.

Code/notebook cached previews, ordinary document/media byte provenance, rich or
uncached previews and persisted real totals/cap states remain required. Related,
filters, semantic outcomes, the Lumen adapter/migration/single crawler, engine-only
payload, WinUI flows,1k/10k/50k/200k native performance, CPU/GPU calibration,
installation, updates and full final acceptance remain open. No GPU/NPU, installed
Lumen, interactive UI, scale or hosted-CI pass is claimed here.
