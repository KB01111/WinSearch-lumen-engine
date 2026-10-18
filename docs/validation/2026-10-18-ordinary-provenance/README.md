# Ordinary document and media source hashes

This checkpoint adds actual source-byte SHA-256 for admitted ordinary documents,
PDFs, images, audio and video within a **64MiB hash budget**. Full v1.0, Lumen
integration and installed acceptance remain open in the
[README TODO](../../../README.md#v10-todo).

The existing owned extraction child hashes the original file before and after
parsing, using64KiB buffers and the original deadline. Original32MiB document/
image/PDF and64GiB streaming-media admission remain unchanged. Larger streaming
sources and missing native identity/change time retain an unknown hash. Code and
notebooks keep their existing same-read provenance and bypass the ordinary hash
wrapper. No additional parser, model, worker pool or frontend dependency is added;
the extractor reuses the workspace's existing sha2 version.

The wrapper compares bytes, length, precise modification time, native identity
and opened-handle native change time around reads/parsing. Normal A-to-B-to-A
edits with restored modification time are rejected. This is a bounded before/
after check, not an immutable parser snapshot. No-recall/open-reparse-point flags
complement residency/reparse checks; all-provider cloud/remote-file acceptance
remains unverified.

Canonical hashes commit atomically with chunks, cursor and an explicit matching
content-revision tag. An older writer changing only its known revision/source
columns cannot attach a retained hash to the new revision. A resumed legacy
partial parse cannot attest its earlier chunks or media plan with a newly seen
digest; that hash stays unknown until a fresh parse establishes it. Additive
nullable columns preserve IDs, FTS, vectors, durable jobs and media plans.
Automatic legacy backfill and larger-source hashing remain unfinished.

Source changes and established continuation-hash mismatches clear derived content
and requeue without charging poison attempts. Completed media cache regeneration
checks the same established hash before replacing an artifact. Failure/unavailable
decoder states do not alone clear valid cached content. Model-only vector resets
preserve valid source hashes. Metadata resolution and cached previews read the
persisted current hash; neither reads source content. Lumen must still enforce its
own grants and content-access freshness at its Rust boundary.

## Verification

The final selected source passes **481 Rust checks**, zero failed/ignored:
213 daemon,41 client,44 code,53 extraction,92 index,18 platform and20 protocol.
The separate actual extractor suite passes18 tests:13 native media and5 PDF,
zero failed/ignored. The coherent CPU workspace binaries/examples build has seven
existing daemon dead-code warnings. **76 .NET tests** pass, zero failed/skipped;
the x64 WinUI build has zero warnings/errors. These are local debug checks,
not full-workspace, installed-app or final-v1 acceptance.

The rebuilt no-model run passes10 public/seven offline snapshot cases in
**8.194s** (index3.578s). Managed CPU passes21 public/17 offline cases in
**83.146s** (index64.291s). Both retain eight typed-client lifecycle checks,
four ordinary-preview, four reference and four code-preview checks; CPU also
retains three semantic checks. The ordinary source hash/revision/metadata check
covers two sources without a model and all five modalities with the CPU model.

Native checks independently hash the original source files with Python, require
matching positive content/hash revisions in SQLite, resolve the actual metadata,
and compare hashes returned by the typed client's document/PDF previews. The CPU
run covers all five ordinary modalities and retains actual768-dimensional code
vectors, ModelManager readiness and the existing semantic checks. Both runs keep
CLI/pipe DTO equality, offline ranking comparisons, cancellation, root revocation,
code/notebook previews and empty-owned-job assertions.

Original CPU positive gates remain Recall@5 .90/MRR .85 and ten empty controls FP0;
expanded positives remain.9091/.8636. The screenshot miss and audio rank2 stay
recorded. No model, ranking or acceptance threshold changed. Hashing adds up to
two bounded complete-source reads per ordinary parser batch, including
continuations. These uncontrolled whole-harness samples include staging, hashing,
indexing, inference and comparisons; they establish no causal performance change.
Required1k/10k/50k/200k and device/power/installed measurements remain open.

Functional failure records cover actual byte hashing, opened-handle identity,
atomic persistence/rollback, worker response validation, native worker/store
metadata, continuation retry, media cache restoration, old-writer revision
binding, legacy continuation and change/restore with original mtime. The
change/retry RED intentionally disabled the new handler to demonstrate that
regression; it is not a failure of the final source. An initial exact-filter
invocation selected zero tests and is retained separately; the corrected legacy
regression ran and failed before the fix, then passed.

Two existing test fixtures needed deterministic setup. The code replacement
delivery fixture now stops only its watcher before asserting native identity;
separate live watcher/epoch tests remain. Retained source/DB observations support
a callback-epoch race, but the exact failed scheduler epoch was not captured.
The background vector-recovery fixture now verifies the actual initial worker
failure, then uses the production-style condition-variable retry loop rather
than assuming one wake completes recovery. Its due bounds, vector assertions and
absence of an external signal remain unchanged. Neither change alters production
scheduling or acceptance thresholds.

A new cache test initially attempted private cross-crate connection access; the
compile error is retained and the fixture now uses existing public production
store APIs. The locked-PDF native test exposed a diagnostic regression: the new
source-open error omitted the existing unreadable classification. That diagnostic
was restored without changing the test. Failed runs are retained separately from
the final passing logs.

This checkpoint received author source review. A requested independent ordinary-
provenance review could not run because workspace credits were exhausted; the
preceding cached-preview review does not approve these changes. Broad final
security/reliability review and hosted CI remain open.

Evidence redacts private paths, user SIDs and opaque native identity/provenance
proofs, retaining actual source, binary and model hashes. The manifest separates
raw local and sanitized published byte hashes. The host record is reused from
the preceding checkpoint; it is not a fresh hardware/driver inventory.

## Reproduce

Use the pinned CPU environment from the
[code-provenance checkpoint](../2026-10-18-code-provenance/README.md).
From src/engine:

```powershell
rtk proxy cargo build --workspace --bins --examples
rtk proxy cargo test -p ws-code -p ws-extract -p winsearchd -p ws-protocol -p ws-index -p ws-client -p ws-platform --all-targets -- --test-threads=1
rtk proxy cargo test -p winsearch-extract --tests -- --test-threads=1
```

From the repository root, using fresh output directories:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/ordinary-source-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/ordinary-source-cpu-repro --real-model --compare-offline --compare-client
```

Rich/uncached previews, extraction totals/caps, Related/filters/semantic outcomes,
Lumen migration and one crawler, native Stop/stalled-writer acceptance, standalone
flows, engine-only packaging, supported GPU calibration and installed release
gates remain required. NPU work remains outside the current focus.
