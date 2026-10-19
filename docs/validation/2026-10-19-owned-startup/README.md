# Owned engine startup prerequisite

Local Windows/debug evidence for the native Lumen integration prerequisite.
This does not complete the Lumen adapter, installed acceptance or full v1.0.

`Client::for_process` checks the server PID on the same connected pipe before
Hello, for every read, control and subscription connection. The caller supplies
the verified owned process ID and retains its process handle/Job lifetime.
`Client::new` remains namespace-only for compatibility. No dependency, wire
version, parser quota, model/ranking policy or acceptance threshold changed.

`winsearchd --defer-root-admission` retains saved policy, identities, vectors and
durable jobs without admitting saved ordinary/code folders or catalog work.
The effective policy is denied until the current host submits a valid policy.
Watchers attach before queued scans can run, and admission remains closed if
generation, queue or checkpoint setup fails. Invalid policies do not unlock it;
an explicitly empty valid policy is accepted. Standalone startup is unchanged.
See the [host contract](../../headless-client.md).

## Verification

| Scope | Result |
| --- | --- |
| Workspace all targets, excluding ws-embed |625 pass; one actual-model worker initially ignored |
| Final daemon replay after watcher-order correction |238 pass; replaces the earlier238, not additional tests |
| Other unchanged workspace tests |387 pass, including53 client tests |
| Actual pinned model tests, including ignored cases |25 pass; no failures or ignored cases |
| Explicit actual worker replay |1 pass; no failures or ignored cases |
| Executed Rust tests across those disjoint scopes |651 |
| Additional client documentation test |1 pass |
| .NET |152 pass; no failures or skips |
| x64 WinUI build |Zero warnings/errors |
| Coherent binaries/examples |Pass; seven existing daemon dead-code warnings |

The complete workspace run preceded the final watcher-order correction. All238
affected daemon tests were then rebuilt and replayed; other tested component
sources stayed unchanged. Final model/worker builds and native profiles followed.
The [source lock](source-lock.json) records raw local and Git-normalized bytes.
Repeated focused and daemon runs are not added to the651 total.

| Native profile | Public queries | Raw/typed source-kind cases | Initial indexing / whole harness |
| --- | --- | --- | --- |
| No model |10 |11 /6 |3.461s /11.122s |
| Actual managed CPU |21 |13 /6 |82.073s /98.175s |

Both native profiles start deferred, reject an invalid policy without opening
admission, then index the explicitly admitted generated roots. The typed client
uses the harness-owned daemon PID throughout search/configuration/subscriptions,
entry resolution, previews, pause/revoke/restore and reconnect. Original source,
cache-retention, code/notebook, extension/kind, Related and cancellation checks
remain. Both owned jobs finish empty. Their executable, native DLL, client,
comparator and harness hashes match; no Rust build ran between profiles.

Actual pinned text references retain worst drift1.32e-4 across10 cases. The four
image/audio/photo/frame reference cosines remain above the unchanged .999 floor.
CUDA compiled but reported unavailable initialization on this runner; execution
used CPU. GPU calibration remains open. NPU is outside the current focus.

Original CPU Recall@5/MRR remain **.90/.85**, expanded positives .9091/.8636,
and all ten empty controls have FP0. The screenshot miss and audio rank2 remain
visible in the [validation summary](validation.json). Whole-harness timings are
uncontrolled observations, not causal speed comparisons or scale/idle acceptance.
Model identity, pinned weights/projector and remaining inputs are recorded in
[native inputs](native-inputs.json).

## Retained failures and review

Functional REDs reproduced handshake bytes reaching a mismatched process,
saved-root/catalog admission before current host policy, and a SQLite write
failure returning an error after admission had opened. The final failure test
forces generation, queue and checkpoint failures and verifies closed admission,
no retained watchers and successful retries. Code jobs/watchers remain inactive
until current policy is admitted.

An initial parallel workspace run also failed57 daemon tests, mainly with
`AdmissionUnavailable` against the existing two process-wide parser slots.
The repository's previously documented serial test command passed. Parser
capacity, test assertions and acceptance thresholds were not relaxed. Raw logs
are retained locally with [digests and byte counts](local-evidence-hashes.json);
published summaries exclude machine application inventory, private paths and
opaque source identities.

Scoped independent GPT-6.1 Sol source review found the admission-on-error and
watcher-order races; both were fixed, and the final ordering was reviewed again
without further findings. This does not replace the broad final branch/security
review or installed consumer acceptance.

## Reproduce

Use the existing pinned model/dependency setup from the
[Related checkpoint](../2026-10-19-related/README.md). Set `WS_EMBED_TEST_MODEL`
to the actual pinned GGUF and sibling projector. From `src/engine`:

```powershell
rtk proxy cargo test --locked --workspace --exclude ws-embed --all-targets -- --test-threads=1
rtk proxy cargo test --locked -p winsearchd --bin winsearchd -- --test-threads=1
rtk proxy cargo test --locked -p ws-client --doc
rtk proxy cargo test --locked -p ws-embed --all-targets -- --include-ignored --test-threads=1 --nocapture
rtk proxy cargo build --locked --workspace --bins --examples
rtk proxy cargo test --locked -p winsearch-embed --bin winsearch-embed real_model_round_trip_through_worker_binary -- --ignored --test-threads=1 --nocapture
rtk proxy cargo build --locked --workspace --bins --examples
```

From the repository root, use fresh output directories and do not rebuild Rust
between native profiles:

```powershell
rtk proxy dotnet test src/app/WinSearch.Tests/WinSearch.Tests.csproj --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/owned-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/owned-cpu-repro --real-model --compare-offline --compare-client
```

The actual Lumen lifecycle/adapter, stable metadata migration, one production
index, preview/answer admission, streaming UI, scale/resource/device work and
standalone/engine-only packaging remain in the [README TODO](../../../README.md#v10-todo).
No release, merge, installed or full-v1 acceptance is claimed.
