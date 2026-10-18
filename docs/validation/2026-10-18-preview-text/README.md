# Bounded cached-text previews

This checkpoint adds `entry.preview.text` to the existing version1 contract and
Rust/C# clients. An ordinary indexed reference returns current metadata and at
most64KiB aggregate UTF-8 text/64parts with actual page, covered-line or exact
timestamp locations. SQLite applies current source/root/revision guards before
the row limit and reads no vectors, artifact inputs or source content. Cached
previews need no model/parser and work while indexing is paused.

Two admitted jobs retain capacity through delivery/cleanup. Database waits run
away from the connection reader. Cancellation, client disconnect and replacement
retire request tokens without removing newer requests. After acquiring the pipe
writer, delivery rechecks the request and current native source proof. Rust owns
each read connection; C# previews use separate one-shot connections with two local
slots and a35second total deadline. The main UI/control connection stays separate.

## Current local evidence

[Host/runtime record](host.json): Ryzen AI9 HX370,12cores/24threads, Windows11
Pro build26300, rustc1.95.0, .NET SDK10.0.402 and Python3.12.14. Only the CPU
route was executed; listed display devices do not establish GPU acceptance.

- [Affected Rust checks](rust-tests.txt): **335 passed, zero failed/ignored**:
  201daemon,33client,85index and16protocol/contract checks. Native tests use the
  established serialized method. The public named-pipe case holds SQLite while
  two previews wait, proves Ping and in-band Cancel still work, refuses a third
  job, releases capacity and rejects revoked/restored proofs and replaced sources.
  A delivery-gate unit separately rejects cancellation/source changes/reused IDs
  after preparing a reply; its production caller takes the writer before that gate.
  This does not claim a raw stalled-writer timing test or full-workspace acceptance.
- [C# tests](dotnet-tests.txt): **40 passed, zero failed/skipped**. Actual pipe
  fixtures cover mutable arguments, malformed replies, capability rejection before
  sending, owned-connection EOF and caller cancellation releasing admission.
  Invalid numeric locations reject before connecting. [x64 WinUI build](winui-build.txt)
  passed with zero warnings/errors; this is build evidence, not interactive UI acceptance.
- [Coherent CPU build](coherent-build.txt) passed for the daemon, CLI, extractor,
  embedding worker and examples. Seven existing daemon dead-code warnings remain.
  GPU backends were disabled; no CUDA/Vulkan/NPU execution is claimed.
- [No-model native run](native-no-model.json): ten public CLI/pipe comparisons,
  seven offline snapshot cases, eight typed-client lifecycle checks and **four
  preview checks** pass in6.790s. Preview checks require actual cached document
  text, actual PDF page2, denied roots and stale proof after grant restoration.
  No model/code vectors are required and the owned process job finishes empty.
- [Managed CPU run](native-managed-cpu.json): ModelManager imports and verifies
  pinned artifacts, reports ready and produces four real768-dimensional code
  vectors. Twenty-one public cases and seventeen offline snapshot comparisons
  agree; the typed client retains eight lifecycle, three semantic-code and four
  preview checks. The original ten-positive group remains **Recall@5=.90/MRR=.85**;
  the expanded eleven-positive group is .9091/.8636. All ten declared empty-result
  controls remain empty. The existing screenshot miss and soundtrack rank2 are
  retained. Total67.592s includes staging/hashing/comparison, not an inference-only
  benchmark. Every owned process exits. No ranking/model/gate was changed here.

## Regression history retained

Functional REDs are published for [protocol](protocol-red.txt), [bounded store](store-red.txt),
[engine cancellation/unsupported route](engine-red.txt), [Rust client](client-red.txt),
[C# codec](dotnet-codec-red.txt) and [C# client](dotnet-client-red.txt).
The earlier compile/setup corrections remain in local raw artifacts.

The additional [timestamp RED](timestamp-red.txt) proves submillisecond requests
were rounded into a different stored location. [GREEN](timestamp-green.txt) uses
exact stored-second comparison and preserves actual locations. The
[delivery RED](delivery-red.txt) proves a prepared reply could outlive cancellation;
the [passing gate regression](delivery-green.txt) and final335-check suite retain
the fix. No timeout, retrieval threshold or expected content was relaxed.

Three broader/fixture failures are preserved separately, not counted as passes:

- [Fresh-source fixture](affected-source-fixture-failure.txt) and earlier
  [public-pipe failure](native-pipe-transient-failure.txt): a restored real watcher
  can advance revision during fresh metadata resolution after a write. Fixtures
  allow at most5seconds for a new unbound request to settle; old-proof denial,
  empty old text and source/root assertions remain unchanged. No production retry
  revives a request across a policy change. The focused
  [native case passed](native-pipe-green.txt) and is rerun by the final suite.
- [Exclusion fixture](affected-watcher-fixture-failure.txt): its first policy save
  started a watcher that could index the next case's file before revocation.
  Each exclusion case now owns a fresh tree/indexer; the original post-revocation
  raw-entry and queue assertions are preserved. Production policy is unchanged.
- [Invalid pipe fixture](affected-invalid-test-pipe-failure.txt): the new numeric
  input test supplied a name outside the client's existing per-user namespace.
  It now uses an existing valid fixture and closes its peer before testing;
  no production namespace validation was weakened.

## Reproduce and interpret

From `src/engine`, with the CPU build's pinned llama.cpp source and GPU-disable
environment documented in the preceding [parser/code checkpoint](../2026-10-18-parser-code/README.md):

```powershell
rtk proxy cargo test -p ws-protocol -p ws-client -p ws-index -p winsearchd --all-targets -- --test-threads=1
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, using the configured Python runtime and fresh outputs:

```powershell
rtk proxy dotnet test WinSearch.slnx --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/preview-native-new --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/preview-native-cpu-new --real-model --compare-offline --compare-client
```

The JSON records binary/model/input hashes and generated-fixture outcomes.
User/checkout/temporary paths are normalized and opaque native identity proofs
redacted; [hash records](evidence-hashes.json) distinguish local raw and published
bytes. These are local debug/native/generated-fixture results, not hosted CI,
installed Lumen, accessibility, backend calibration or required scale benchmarks.

Full preview/Task1/v1 acceptance remain open. Actual file hashes, code/notebook
identities, raster/PDF thumbnails, media playback, uncached/on-demand rendering,
source totals/cap metadata, consumer UI and Lumen adapter/migration remain required.
Extraction completion means within product limits; unknown totals/cap state stay
null. The source proof remains metadata, never SHA256. New parser work must reuse
the existing shared registry/quota. The synchronous raw server writer still has no
deadline; an uncooperative raw client can retain admitted work until disconnect.
Independent review remains pending because approved-model dispatch hit the agent
thread limit; no GPT6Sol fallback or review-configuration change was used.
