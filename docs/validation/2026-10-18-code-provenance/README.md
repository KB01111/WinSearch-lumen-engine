# Exact code-byte provenance

The owned code parser now returns SHA256 of the exact admitted bytes used for its
parse. It reuses the existing bounded read: UTF8 bytes, BOM, CRLF and complete
notebook JSON contribute to the digest, while notebook outputs/attachments still
never enter search chunks. No metadata, path or extracted-text hash is substituted.
The existing2MiB, sensitive-path, reparse and residency guards remain in force.

Each internal code batch requires a canonical lowercase64-character hash. The
daemon rejects changes across continuation and atomically publishes parsed
chunks/symbols, source snapshot, hash and a durable per-file content revision.
The revision is distinct from parser compatibility. Code schema4 migration keeps
IDs, exact rows, vectors and durable jobs; a current legacy parse starts at revision1
with hashNULL. Reconciliation establishes unknown hashes by real parsing. A parse
without byte provenance clears an earlier hash.

This checkpoint persists internal code provenance. Public canonical code references,
hash metadata, bounded notebook/code preview and ordinary document/media hashes
remain unfinished. The existing opaque source_revision remains metadata, not SHA256.

## Current local evidence

- [Affected Rust suite](rust-tests.txt): **421 passed, zero failed/ignored**:
  202daemon,33client,39code,46extract,85index and16protocol/contract checks.
  Native cases use the established serialized method. Source/revoke/cancel,
  parser quotas, exact locations and the earlier cached-preview regressions pass.
- [Coherent CPU build](coherent-build.txt): all workspace bins/examples pass with
  seven existing daemon dead-code warnings. The worker and daemon must be rebuilt
  together; older internal batches missing the required hash are refused.
  No public protocol version, ranking, model or acceptance threshold was changed.
- Parser known-vector cases cover ordinary bytes, BOM/CRLF, empty files and full
  notebook JSON containing excluded output. Expected digests were independently
  computed with Python hashlib; the hello-world vector also matches RustCrypto's
  published SHA256 example. Existing oversized-source assertions cover both parser
  entry points. The [worker test](worker-green.txt) additionally walks actual
  continuation batches and rejects malformed hashes.
- [Store checks](store-green.txt): an actual SQLite trigger aborts the final
  provenance write after parsed replacement begins. The transaction retains old
  chunk IDs/text/vectors, revision and hash. Successful replacement/reopen advances
  the content revision; non-provenance parsing clears the old digest. Schema3
  migration preserves native identity, exact FTS, vectors and pending jobs and
  leaves the legacy digest unknown. The existing schema1 migration is also rerun.
- [Native parser check](native-green.txt) proves the independently known digest
  of actual generated Rust bytes is persisted and a second parse advances the
  revision without changing the hash. The strengthened [restart case](legacy-green.txt),
  also included in the final421suite, proves a missing legacy hash is established
  from a real owned parser after restart.
- [No-model native run](native-no-model.json): ten public CLI/pipe cases, seven
  offline comparisons, eight client lifecycle and four preview checks pass.
  Both actual code/notebook digests equal independent Python file digests, with
  positive content revisions. Four original positive cases have Recall/MRR1;
  six declared empty controls are empty. Index phase4.275s, total8.459s; owned job empty.
- [Managed CPU run](native-managed-cpu.json): ModelManager stages/verifies pinned
  artifacts and creates four actual768-dimensional code vectors. Twenty-one public
  cases and seventeen offline comparisons agree; eight client lifecycle, three
  semantic-code and four preview checks pass. Both source-file digests match.
  Original ten-positive **Recall@5=.90/MRR=.85** and ten empty controls FP0 remain
  unchanged. Expanded eleven-positive group .9091/.8636; the screenshot miss and
  soundtrack rank2 are retained. Index phase118.678s, total133.656s; owned job empty.
  The earlier preview checkpoint's separate run took67.592s total. These are
  uncontrolled whole-harness samples including staging/hashing/indexing/inference;
  they establish neither causal performance change nor an acceptable scale budget.

The [host record](host.json) identifies Ryzen AI9 HX370/12cores/24threads, Windows11
Pro build26300, rustc1.95.0 and .NET SDK10.0.402. It was captured at the preceding
preview run on this host; no GPU/NPU execution is claimed. C# source did not change
here; the last40 .NET tests and x64 WinUI build belong to
[that checkpoint](../2026-10-18-preview-text/README.md), not a new .NET run.

## Functional failures retained

- [Parser RED](parser-red.txt): empty digest instead of the known byte hash.
- [Worker RED](worker-red.txt): missing digest and malformed digest accepted;
  [GREEN](worker-green.txt) preserves actual continued-batch and malformed cases.
- Initial [migration fixture setup failure](store-fixture-setup-failure.txt)
  attempted to drop columns before they existed. Its setup now conditionally
  removes only the new columns to create a genuine schema3 database.
  [Functional store RED](store-red.txt) then failed on missing provenance and
  schema3 instead of4; [GREEN](store-green.txt) retains data/rollback assertions.
- [Native RED](native-red.txt): real parser bytes were indexed without storing
  their digest; [GREEN](native-green.txt) checks actual persistence.
- [Legacy restart RED](legacy-red.txt): an unchanged file with a current metadata
  snapshot kept hashNULL indefinitely. [GREEN](legacy-green.txt) schedules an
  admitted reparse and establishes the real digest. No existing assertion, timeout,
  source policy or retrieval gate was relaxed.

## Reproduce and remaining work

Use the CPU env/pinned source described in the
[parser/code checkpoint](../2026-10-18-parser-code/README.md). From src/engine:

```powershell
rtk proxy cargo test -p ws-code -p ws-extract -p winsearchd -p ws-protocol -p ws-index -p ws-client --all-targets -- --test-threads=1
rtk proxy cargo build --workspace --bins --examples
```

From the root, with fresh output directories:

```powershell
rtk proxy python tools/test-search-parity.py --output artifacts/code-provenance-native-new --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/code-provenance-native-cpu-new --real-model --compare-offline --compare-client
```

Published JSON retains actual model/binary/input/source hashes. Private paths are
normalized; opaque native identity proofs are redacted. [Hash records](evidence-hashes.json)
distinguish local raw from published bytes. These are local debug/generated-fixture
and native results, not hosted CI, installed Lumen, accessibility or scale acceptance.

Canonical bound code references, public source hashes/preview, ordinary-file
provenance, rich/uncached preview and PDF totals/caps remain required. Related,
filters, semantic outcomes, the Lumen adapter/migration/single index lifecycle,
engine payload, WinUI flows,1k/10k/50k/200k performance, CPU/GPU calibration and
installation/update/final acceptance remain open. Independent review remains pending
after approved GPT6.1Sol dispatch hit the thread limit; no GPT6Sol fallback was used.
Full Task1 and v1 acceptance are not complete.
