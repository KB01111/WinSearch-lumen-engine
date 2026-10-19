# Source file-kind selection checkpoint

Ordinary and Related queries now accept optional typed `file_kinds`, gated by
`search.file_kinds`. The13 closed codes follow Lumen's directory/extension rules
at main `3b60ba43104ec7c450423c13db4bc5ee06662e68`, fetched and reverified during
this checkpoint. No Lumen checkout files changed.

PDF, text documents, spreadsheets, presentations, source, images, video, audio,
archives, executables, 3D models, folders and unknown sources remain distinct.
Directory flags take precedence over suffixes. Notebooks currently classify as
unknown sources while keeping code-domain results and notebook locations. This
classification is independent of extraction support and retrieval category.

The list is bounded to13 alternatives and intersects mode/categories/extensions.
SQLite candidate admission and ordinary/code vector admission apply it before
their existing caps; Related uses the same target admission. Empty lists are
omitted for legacy compatibility, including older Related peers that reject
unknown fields. Rust/C#/CLI typed clients check capability, budgets and reply
classification. C# copies caller lists before awaiting writes; numeric/aliased
file-kind codes and invalid classified result kinds are rejected. The CLI command
surface retains its extension arguments; kinds are a typed native operation.

There is no database migration, reindex, new dependency, model-space change or
candidate-cap increase. Current source/root/residency, request and model fences
remain in force. A selector grants no filesystem access, and the trusted native
consumer still owns current grants and action/preview admission.

## Verification

| Check | Result |
| --- | --- |
| Rust workspace, all targets, ws-embed excluded |619 pass; zero failures; one model worker ignored in this invocation |
| Actual pinned ws-embed tests with ignored cases included |25 pass; zero failures/ignored |
| Explicit actual-model worker replay |1 pass; zero failures/ignored |
| Executed Rust tests across those disjoint scopes |645 |
| .NET suite |152 pass; zero failures/skips |
| x64 WinUI build |Pass; zero warnings/errors |
| Final coherent binaries/examples |Pass; seven existing daemon dead-code warnings |

The default ignored worker is counted only after its explicit real-model replay.
Repeated focused runs are not added to the645 total. A focused daemon replay
also checks the code semantic branch after strengthening its kind assertion.
Contract/crowding tests use fixed vectors; actual model/reference and native
retrieval runs are separate evidence classes.

Actual CPU text references retain worst drift1.32e-4 across10 cases; public
image/audio/photo/frame references pass the unchanged .999 cosine floor. The
actual worker loads the pinned Q8_0 weights/projector and reports CPU execution.
CUDA compilation does not establish CUDA execution or calibration. Vulkan/GPU,
scale and installed acceptance remain open.

| Final native profile | Original public/offline cases | Raw kind cases | Typed kind cases | Whole harness | Initial indexing |
| --- | --- | --- | --- | --- | --- |
| [No model](native-no-model.json) |10 /7 |11 |6 |10.389s |3.194s |
| [Actual managed CPU](native-managed-cpu.json) |21 /17 |13 |6 |91.556s |60.224s |

Native kind cases cover PDF/document separation, exact/code/notebook selection,
union/intersection behavior, synthetic exclusion and malformed14-value rejection.
CPU adds document/code semantic cases plus a filtered Related positive and an
explicit no-vector outcome. Related corpus additions occur after original
quality queries/snapshots. Existing extension, provenance, preview, source/policy
lifecycle and cancellation assertions remain. Both owned jobs end empty, with
identical executable/DLL/helper/comparator/harness hashes and no Rust build
between final profiles.

Original Recall@5/MRR remain **.90/.85**; expanded positives .9091/.8636 and ten
empty controls FP0. Screenshot miss and audio rank2 remain. Added filter/Related
cases do not alter those denominators. These uncontrolled whole-harness samples
include additional work and establish no causal speed comparison, scale budget
or idle/power acceptance. Host details are the reused host record linked from
the [preceding Related checkpoint](../2026-10-19-related/README.md).

## Retained failures and limits

Functional REDs reproduce missing kind serialization, ignored exact/semantic
selection, code/synthetic leakage, unsupported Related fields, legacy dispatch,
missing client validation and invalid numeric result-kind acceptance. See
[protocol](protocol-red.txt), [daemon](daemon-red.txt),
[Rust client](rust-client-red.txt), [.NET contract](dotnet-contract-red.txt),
[.NET client](dotnet-client-red.txt) and
[invalid result kind](dotnet-invalid-result-red.txt).

The first CPU native run failed in the fixture classifier on the unknown/auto
`1+2` case:20 indexed application shortcuts had `.lnk`, a valid unknown extension
absent from the fixture's initial lookup table. Their numeric name matches are
allowed by auto mode. The [retained failure summary](retained-fixture-failure.json)
records counts/hashes and cleanup while omitting machine application names/paths.
The corrected fixture classifies every returned source and strictly forbids
calculator/settings/action results; it permits valid indexed matches in that
one synthetic-exclusion case. The original frozen quality cases, named positive
expectations and other empty-result controls remain unchanged. No engine or quality-threshold change was needed for this fixture
failure; both final profiles reran with the same corrected harness.

These checks received author source review. Independent review remains incomplete
after the earlier workspace-credit failure; prior quota/skipped reviews do not
approve this diff. Full Task1, Lumen scope/filter mapping, expanded provenance,
adapter/data migration/single index lifecycle, raw stalled-writer bounds, rich
previews/UI, packaging and installed/release acceptance remain open in the
[README TODO](../../../README.md#v10-todo). No release or merge is claimed.

## Reproduce and evidence

Use the pinned model/dependency setup in the
[Related checkpoint](../2026-10-19-related/README.md). Set
`WS_EMBED_TEST_MODEL` to the actual pinned GGUF with its sibling projector. From
`src/engine`, run:

```powershell
rtk proxy cargo test --workspace --exclude ws-embed --all-targets -- --test-threads=1
rtk proxy cargo test -p ws-embed --all-targets -- --include-ignored --test-threads=1 --nocapture
rtk proxy cargo build --workspace --bins --examples
rtk proxy cargo test -p winsearch-embed --bin winsearch-embed real_model_round_trip_through_worker_binary -- --ignored --test-threads=1 --nocapture
rtk proxy cargo build --workspace --bins --examples
```

From the root, use fresh output directories and do not rebuild Rust between
native profiles:

```powershell
rtk proxy dotnet test src/app/WinSearch.Tests/WinSearch.Tests.csproj --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/kind-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/kind-cpu-repro --real-model --compare-offline --compare-client
```

[Evidence hashes](evidence-hashes.json) distinguish raw local and published bytes.
Native summaries retain fixture kind DTOs, while unrelated machine application
inventory is counts/hashes only. Original queries, extension comparisons, offline
and full typed-client DTOs are condensed to structural hashes using Python JSON,
UTF-8, sorted keys, compact separators and `ensure_ascii=False`. Private paths,
SID and opaque identities are redacted. Model/reference traces retain selected
test/reference lines and full raw hashes. Models, binaries and databases stay
local. This is local Windows/debug evidence, separate from hosted CI and installed
Lumen/WinUI behavior.
