# CLI extension selection checkpoint

The CLI now sends the existing bounded extension filter instead of treating its
arguments as query text. Repeat `--extension <ext>` / `-e <ext>`, or use
`--extension=<ext>`. Values are alternatives, intersect the selected mode and
retain the engine's current source/root policies. Optional leading dots and ASCII
case-insensitive matching follow the existing protocol. There is no new wire
field, dependency, schema migration, ranking change or increased candidate cap.

Invalid/missing values, wildcard/path syntax and more than64 extensions fail
before connecting. `--extension --json` cannot consume the next option as its
value. Unknown long options fail explicitly; `--` admits literal query text that
starts with `--`. Existing negative-number query text remains usable.

Nonempty filters require the peer's `search.filters` capability before dispatch.
The synchronous CLI client validates every correlated batch's file extension and
rejects folder/null-path/unselected results. An invalid early or final batch
cannot publish a successful JSON array or earlier partial hits. Existing terminal
outcome validation and legacy unfiltered behavior remain intact.

## Verification

- [CLI all-target tests](cli-tests.txt):42 executions pass, zero failures/ignored
  (19 binary, two evaluation-provenance, nine public-pipe subprocess and12 offline
  example tests). The example reuses evaluation tests; this is an execution count,
  not42 distinct behaviors. [CLI build](cli-build.txt) passes.
- Actual owned daemon [without a model](native-no-model.json):10 existing public/
  seven offline cases and **nine filtered CLI/pipe comparisons** pass. Total7.996s,
  initial indexing1.176s; owned job ends empty.
- Actual pinned model/projector [managed CPU](native-managed-cpu.json):21 existing
  public/17 offline cases and **11 filtered CLI/pipe comparisons** pass. Total
  99.365s, initial indexing73.146s; four real768-dimensional code vectors and an
  empty owned job. CPU execution is separate from CUDA compilation.

Every existing native extension case now compares the full ordered CLI DTO array
with flattened pipe batches: PDF dot/case matching, rejected document/code
extensions, ordinary text, Unicode source, notebook and synthetic-result exclusion.
Two added cases prove both alternatives in a repeated PDF/text filter; CPU adds
the existing document/code semantic cases. These separate filter cases do not
change the frozen retrieval-quality denominators. Existing source/provenance,
preview, Related, policy lifecycle and cancellation checks remain asserted.

Original CPU Recall@5/MRR remain **.90/.85**; expanded positives .9091/.8636 and
ten empty controls FP0. The screenshot miss and audio rank2 remain. No numerical,
retrieval, source, timeout or resource threshold was relaxed. Whole-harness times
include additional CLI processes/queries and are uncontrolled; they establish no
causal performance comparison or scale acceptance.

Both profiles use identical executable/DLL/helper/comparator/harness hashes, with
no Rust build between them. The daemon, embedding/extraction workers, adjacent
DLLs and model/projector also match the preceding
[Related checkpoint](../2026-10-19-related/README.md) byte-for-byte. Only the CLI
application code changed here. That checkpoint's broad Rust/.NET/WinUI/reference
results are historical evidence, not newly rerun suites. Host details remain the
reused host record linked there. This is local Windows/debug and generated-fixture
evidence; installed consumers, GPU calibration and hosted CI are separate gates.

Functional failures are retained: [parser RED](parser-red.txt),
[option/separator RED](separator-red.txt) and [wire RED](wire-red.txt) reproduce
query contamination, accepted invalid options, silent legacy fallback and accepted
unselected results. The first implementation also failed on `--extension --json`;
the original assertion remains and now passes after rejecting an option token as
a separate extension value. The tests exercise actual CLI subprocesses on public
pipes; their controlled frames are contract fixtures, not actual daemon indexing.

## Reproduce and remaining work

From `src/engine`:

```powershell
rtk proxy cargo test -p winsearch-cli --all-targets
rtk proxy cargo build -p winsearch-cli --bin winsearch-cli
```

From the root, with the unchanged daemon/workers/native DLLs and pinned model
setup from the preceding checkpoint, use fresh output directories. Do not build
Rust between profiles:

```powershell
rtk proxy python tools/test-search-parity.py --output artifacts/cli-filter-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/cli-filter-cpu-repro --real-model --compare-offline --compare-client
```

[Evidence hashes](evidence-hashes.json) distinguish raw local and published bytes.
Native summaries retain full filtered CLI DTOs, equal CLI/pipe hashes and terminal
outcomes; original queries are condensed to equal structural hashes (Python JSON,
UTF-8, sorted keys, compact separators,
`ensure_ascii=False`). Private paths/SID/opaque identity fields are redacted. No
models, binaries or databases are published.

Lumen main was fetched and verified at `3b60ba43104ec7c450423c13db4bc5ee06662e68`.
Its `search/metadata.rs` classifies13 file kinds and `search/indexing.rs` applies
multiple native filters as an intersection. The extension list above is an OR
selector; the eventual adapter must preserve Lumen's intersection semantics and
finer kinds rather than blindly forward those filters. No Lumen files changed.

Finer kinds/provenance, the Lumen adapter/migration/single index lifecycle, rich
previews/UI, raw stalled-writer bounds, broad independent review, resource/scale,
packaging and installed/release acceptance remain open in the
[README TODO](../../../README.md#v10-todo). This checkpoint received author source
review only. Independent review remains incomplete after the earlier workspace
credit failure. No full Task1/v1 completion, release or merge is claimed.
