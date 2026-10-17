# Native media-selection checkpoint

Source: `f056bd1`, Windows x64, HX370 development machine. These are local native
measurements and selected tests, separate from hosted CI and installed acceptance.

The [test log](selected-tests.txt) contains the full terminal output for:

```powershell
cd src/engine
cargo test -p ws-extract -p ws-index -p winsearch-extract -p winsearchd -p winsearch-cli -- --test-threads=1
```

353 passed, zero failed or ignored: 20 CLI, 13 native media, five native PDF,
188 daemon, 45 extraction and 82 index tests. Dependencies, sibling workers and
generated native fixtures were already prepared by the repository's existing
fetch/generation tools. This is not proof of a fresh self-contained installation.
The separate [coherent debug build log](selected-build.txt) retains seven existing
daemon dead-code warnings. They do not appear in the selected test log.

Resource results preserve all measured batches, including the baseline's 480
scan workers. [Fixed-slice results](fixed15s-resources.json) and
[adaptive results](adaptive-resources.json) use identical generated source hashes
and pinned decoder binaries. The final adaptive run had no competing build,
native test, inference or formatting process. Local checkout prefixes are
replaced with `<checkout>`; log line endings and trailing whitespace are normalized.
Measurements, hashes and per-run PIDs are unchanged.

The [baseline input record](fixed15s-input-lock.json) was collected **after** its
run. The [adaptive input lock](adaptive-input-lock.json) was written **before**
the repeat. Its initially empty `batches` is an input-lock snapshot; completed
measurements are in adaptive-resources.json. HD identity was verified from the
baseline before execution and is also in the completed adaptive results.

The two-hour sparse fixture selected 1,200 visual frames and 288 independent
audio windows. It checks early silence and one actual late audio artifact;
it does not decode or embed every window. The other fixture is 120 seconds of
1080p/15fps video with GOP450. Cancellation observes an actual decoder child and
asserts an empty owned job. Resource limits and deadlines were unchanged.

See [benchmark methodology and interpretation](../../benchmarks.md#resumable-video-selection-resources-2026-10-18)
for timings, bounds and limitations. These generated sources do not establish
arbitrary-codec/4k throughput, real-model quality, UI or Lumen behavior. Earlier
failed/overlapped runs remain in local artifacts and are described in benchmarks.
Independent review of this media checkpoint and final whole-branch gates remain.
