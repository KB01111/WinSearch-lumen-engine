# WinSearch Benchmarks

Measured, not estimated. Every number below was produced by running the
benchmarks in this repository on the machine described and pasting the real
output. Re-run them with:

```sh
cargo run --release -p ws-index --example bench 50000     # index + search + semantic vector scan
cargo run --release -p ws-index --example bench 200000
cargo run -p winsearch-cli -- eval names                  # retrieval eval suite (Recall@5 + MRR)
target\release\winsearchd.exe --bench-crawl 20000         # crawl pipeline
```

- Date: 2026-10-13
- Machine: AMD64 Family 26 (Zen 5), 24 logical CPUs, Windows
- Build: `cargo build --release` (LTO thin, codegen-units 1)

## Search query latency (interactive path)

500 queries (10 realistic queries × 50 runs) against a synthetic but
realistically named index (report-2024-03-….pdf, meeting notes ….txt,
IMG_….png, mixed depths and extensions).

| Index size | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 50,000 entries | **0.87 ms** | 3.8 ms | 4.0 ms | 4.7 ms |
| 200,000 entries | **3.15 ms** | 11.0 ms | 13.6 ms | 16.0 ms |

Per-source median at 200k entries (all index-driven; no table scans):

| Source | Median |
|---|---|
| name prefix (range seek on name index) | 0.04 ms |
| substring via trigram FTS index | 0.14 ms |
| word/token FTS match | 0.99 ms |

Earlier revisions of this pipeline were measured at p50 5.9 ms / p95 12.9 ms
(50k) and p50 26 ms for the prefix source alone (200k). The two fixes that
moved the numbers: dropping `ORDER BY rank` from the FTS candidate query
(5.7 ms → 0.3 ms median) and replacing a `LIKE '%…%'` table scan with a
trigram index plus a `>= / <` prefix range seek (2.3 ms → 0.006 ms median).

## Index write throughput

| Operation | 50k entries | 200k entries |
|---|---|---|
| Bulk insert (new entries) | ~50,000 entries/s | ~49,600 entries/s |
| Re-upsert, nothing changed (rescan) | ~241,000 entries/s | ~160,000 entries/s |
| Re-upsert, 10% modified | ~145,000 entries/s | ~70,000 entries/s |

Index size: 25.5 MiB at 50k entries, 101.5 MiB at 200k entries (~0.5 MiB per
1,000 entries — names + two FTS indexes + identity metadata).

## Crawl pipeline (scan → identity → upsert)

`winsearchd --bench-crawl` creates a real tree on disk (20,000 files in 100
directories) and runs the production path: directory enumeration, exclusion
policy, per-file identity lookup (`FILE_READ_ATTRIBUTES` open only — no
content reads, no cloud hydration), and batched index upserts.

| Phase | Result |
|---|---|
| Full crawl, 20,100 entries | 2.46 s (**8,140 files/s**) |
| Incremental rescan (all unchanged) | 1.55 s (**12,911 files/s**) |

Notes:

- This is a single-threaded measurement of the per-directory pipeline. The
  daemon runs 4 bounded crawl workers, so wall-clock throughput on a real
  tree scales with I/O concurrency (the queue is per-directory).
- ~640 bytes of index per file on disk.

## Semantic vector search & hybrid ranking (Phase 3)

Measured via `cargo run --release -p ws-index --example bench 50000` on 768-dimensional FP16-encoded vectors (`EMBEDDING_DIM = 768`, 1,536 bytes per segment in the contiguous FP16 plane; `SEMANTIC_TOP_K = 200`, `MIN_SEMANTIC_COSINE = 0.35`):

| Vector store path | Segments | FP16 memory | p50 | p95 | Scan throughput |
|---|---|---|---|---|---|
| `VectorIndex::search` (in-memory FP16 plane) | 1,000 | 1.46 MiB | **2.14 ms** | 2.46 ms | ~467,300 vecs/s |
| `VectorIndex::search` (in-memory FP16 plane) | 10,000 | 14.65 MiB | **21.81 ms** | 30.52 ms | ~458,600 vecs/s |
| `VectorIndex::search` (in-memory FP16 plane) | 50,000 | 73.24 MiB | **99.80 ms** | 108.09 ms | ~501,000 vecs/s |
| `IndexStore::search_vectors` (warm SQLite `segments` scan) | 1,000 | 1,536 B/row | **6.25 ms** | 6.58 ms | ~160,000 vecs/s |
| `IndexStore::search_vectors` (warm SQLite `segments` scan) | 10,000 | 1,536 B/row | **64.83 ms** | 78.98 ms | ~154,200 vecs/s |

### Deterministic pipeline fixture (`evals/names.json`)

Run via `cargo run -p winsearch-cli -- eval names` (24 fixture entries, 18 queries across semantic paraphrases, German/Japanese multilingual queries, exact matches, and negative controls):

| Suite | Queries | Recall@5 (gate ≥ 0.90) | MRR (gate ≥ 0.85) | Negative-control FP rate (gate ≤ 0.00) |
|---|---|---|---|---|
| `names`, ConceptStubEmbedder | 15 positive + 3 negative | **1.000** | **1.000** | **0.000** |

That default embedder is a deterministic concept fixture, not a real model.
The original real CPU baseline passed all15positives but failed all3negative
controls (FP1.000). With pipeline2 application query/document formatting and
shared admission, the unchanged names suite passes Recall@5/MRR0.933 and
negativeFP0.000 (`artifacts/retrieval-recipe-names.log`, real CPU/stub:false,
768dimensions, same pinned Q8_0). The cooking-recipe paraphrase remains a miss.
The separate development and held-out suites were frozen before calibration;
full added modality/code/held-out and daemon parity acceptance remain open.

### Tuned semantic & hybrid ranking thresholds

- `MIN_SEMANTIC_COSINE = 0.65` is the common application floor after adding
  task-aware text formatting. Frozen development query-prefix negative maxima
  were0.5931(name),0.5912(code), and<=0.5747(other modalities). Its application
  paraphrase "play a movie" remains a measured miss; it is not relabeled.
  Filename stripping/normalization/document prompts were also measured and
  rejected. These development measurements are not held-out acceptance.
- `SEMANTIC_TOP_K = 200`: candidate pool before per-entry deduplication and hybrid scoring.
- Hybrid score band `0.45 + 0.45 * cosine` (`[0.45, 0.90]`): places semantic matches below exact/prefix matches (`>= 0.92`) and above weak FTS token matches (`0.50`), with `+0.10` exact-token boost and `+0.02` 30-day recency boost.
- `QUERY_CACHE_CAP = 64`: LRU cache for query embeddings so repeated/backspaced queries in an overlay session avoid re-embedding.

The genuine production-daemon media check now passes all3positive timestamp/
thumbnail cases and all3unrelated negative controls with pipeline2
(`artifacts/indexed-media-recipe-final/results.json`): 38.906s indexing,
39.765s total, 15video/7audio/565name vectors, actual CPU/stub:false/dim768.
Sampled job peak working set was1,975,136,256B and private bytes2,322,423,808B,
including the loaded model. The owned job was emptied and a separate process
inventory found no remaining WinSearch helpers. This check uses the earlier
generated media corpus, not the unopened held-out suite. It does not close the
long-HD, cache-preview, installation, UI, code or Lumen consumer gates.

## Envelope and interpretation

- Typing in the standalone overlay debounces at 120 ms. The historical 200k
  exact-query measurements above do not establish current native source-guard,
  semantic/hybrid, concurrent-indexing or Lumen input responsiveness.
- Each candidate source returns at most 500 rows. Policy/source admission runs
  before that cap; the cap does not bound all candidate inspection or vector-scan
  work. Final native scaling measurements remain required.
- The historical p95 gap at 200k came from two-token substring phrases (e.g.
  `presentation final`) with thousands of matches in the trigram index;
  measured at ~11 ms worst case.
- Idle paths use work-queue waits and `ReadDirectoryChangesW`, with timed
  wakeups for policy/catalog checks. Actual idle CPU/GPU measurements remain
  an acceptance gate; blocking primitives alone do not prove zero resource use.

## Active-policy scale check (2026-10-16)

The reviewed policy implementation initially materialized unrelated visible
entry IDs inside search sources. At 50,000 generated rows, `report` took
1.3–1.7 seconds, `report-2024` about 7.85–9.72 seconds, and a no-hit phrase
15.35 seconds. That baseline run was stopped; it has no reported p95.
Correlated candidate/segment permission checks fixed that regression while
preserving filtering before the existing caps.

After commit `9673ee8`, the same active-policy release probe completed:

| Path | Size | p50 | p95 |
|---|---:|---:|---:|
| Exact/name queries | 50,000 entries | 11.01 ms | 18.62 ms |
| SQLite vector search | 1,000 segments | 7.03 ms | 7.59 ms |
| SQLite vector search | 10,000 segments | 73.78 ms | 78.26 ms |
| Cached FP16 VectorIndex | 50,000 segments | 58.13 ms | 62.22 ms |

This probe derives from `ws-index/examples/bench.rs`, adding an active
IndexingConfig and using five iterations of each of ten queries (50 samples).
It uses an optimized standalone Cargo profile rather than the older thin-LTO
500-sample run, so the historical 3.8 ms p95 is not established by this check.
Vectors and filenames are generated fixtures; these are latency measurements,
not real-model retrieval evidence. Large denied-root populations, directory
classification cost, installed-app contention and broader hardware remain
in the final performance acceptance scope.

## Targets (Phase 7 power/performance items)

Current source-consistency checkpoint (2026-10-17, source `52dd9fa`) reran
the same active-policy standalone 50k probe. It measured exact/name p50
19.05ms, p95 39.55ms and p99 43.48ms. The 50k cached FP16 scan measured
p50 76.29ms/p95 122.83ms. Raw log:
`artifacts/retrieval-source-policy-bench/run.log`. This is slower than the
earlier run above; these are separate host-time samples, not a controlled A/B
attribution. The probe uses virtual paths and the offline source callback;
it exercises current SQLite policy/query views but does not measure native
filesystem source-validation overhead. Native 1k/10k/50k/200k query, inference,
hybrid, idle and recovery measurements remain required.

The rebuilt daemon/public CLI comparison used actual generated documents,
PDF pages, code/notebook sources, images, sound and video. Its CPU run
`artifacts/retrieval-parity-cpu-v3/results.json` compared complete ordered DTOs
for 18 queries and passed the unchanged overall Recall@5=.90/MRR=.85/zero-empty-
control gates. The full-size screenshot query was a miss (visual cosine.642428
below unchanged.65); the same scene in a resized video frame scored.691191.
The audio file ranked2 behind the video's soundtrack. These outcomes remain
reported; they are not converted to perfect modality results. Cancellation
and live root revocation yielded empty terminal frames with a live transport.
Indexing took 36.726s, total 43.391s; CPU/stub:false/dim768 was verified and the
owned job emptied. This establishes public client/daemon DTO parity on this
corpus, not identity with the offline fixture evaluator or NPU execution.
The ten positive queries include four document, three code and one each of
image/audio/video. Per-modality Recall@5/MRR are document 1/1, code 1/1,
image 0/0, audio 1/.5 and video 1/1. The eight empty-result controls include
isolation and root revocation; they are not eight independent semantic negatives.
The no-model native run (`artifacts/retrieval-parity-native-v2/results.json`)
passed ten queries in 2.103s with zero code vectors. These are small generated
corpora, not the mandatory large-index or installed-consumer measurements.

The subsequent `9491262` checkpoint additionally runs the CLI's shared offline
normal/code query functions on SQLite backups of that actual native index.
`artifacts/retrieval-offline-native-v1/` has seven equal no-model query cases;
`artifacts/retrieval-offline-cpu-v2/` has fourteen equal genuine CPU cases.
Ordered public IDs, paths, primary and matching-moment locations match exactly,
including code definition/reference roles and notebook cell ranges. The latter
also passes eighteen public queries with the same .90/.85 quality and retained
image miss/audio rank2. Index87.911s/total110.816s includes an extra offline worker
load/query phase; no latency improvement is inferred. Binary/native/model,
comparator, manifest, generated-source and snapshot hashes are retained. This
compares distinct query code on the same native vectors; independently seeded
fixtures and native extraction remain separately classified evidence.
The initial CPU-v1 run failed indexing with a newly rebuilt extractor and an
older daemon; it is retained, and the pair was rebuilt together before v2.

The 2026-10-17 CPU worker follow-up reproduced all four independent media
reference cosines (>=.999) but hit the unchanged watchdog at 180 seconds during
repeated audio requests, before retrieval. This is an incomplete acceptance run,
not a passing benchmark. Retained results: artifacts/media-review-followup/.
The host's native power policy independently reported Battery Saver during
extraction tests; that association does not establish the slowdown's cause.
That timeout is retained as intermittent evidence. A fresh unchanged helper run
passed all four media references and four retrieval pairs in 20.422s
(`artifacts/media-calibration-baseline/`); it does not prove the earlier cause.

Controlled CPU probes used the same pinned model/native build, public screen and
speech inputs, then speech padded to 30s. All reference cosines stayed >=.999:

| Threads | Total seconds | Image 280 seconds | Short speech seconds | Audio 30s seconds |
| --- | ---: | ---: | ---: | ---: |
| 4 | 15.672 | 6.876 | 0.586 | 4.766 |
| 8 | 16.235 | 7.127 | 0.485 | 5.497 |
| 12 | 15.515 | 7.281 | 0.604 | 5.128 |
| 24 | 21.469 | 10.525 | 1.503 | 5.277 |

`ws-embed/examples/media-throughput.rs` reproduces one thread-count probe; its
executable needs the same sibling native DLLs as the worker. The shared runtime
default now caps available logical threads at 8; explicit LoadOptions still allow
calibration. This is a conservative measured default, not a universal optimum.
The actual daemon's three-file media corpus plus its installed-app catalog left
15 frame vectors pending at the unchanged 120s check with 24 threads. Rebuilt with
8 threads it completed 15 frame/7 audio/565 name vectors in 60.750s. Both later-spoken
queries returned 50..65s, and the screenshot query returned a genuine 40s cached
frame. All three unrelated queries returned false positives; this run fails the
required retrieval gate. Results: `artifacts/indexed-media-threads8/`. Its older
harness also flagged the valid 40s frame because it expected exactly 33s; the
corrected harness checks the screenshot's full 32.75..65s interval. This has not
been represented as a fresh passing full run.

The corrected harness was then rerun with a native stdin startup gate, ensuring
job assignment precedes any eager daemon helpers. This final source run completed
indexing in 45.250s and passed all three positive location/thumbnail checks. The
same three negatives still fail, so the whole quality run exits1. Resources and
full results are retained in `artifacts/indexed-media-native-gate/`; there were
no WinSearch processes left in the subsequent independent process inventory.

`tools/test-media-resources.py` generated a two-hour 160x100/1fps video with an
eight-second soundtrack near 7100s. The first 30-frame batch selected exactly 1,200
times plus 288 independent audio windows in 2.830s, sampled peak job-tree working
set 54,505,472 bytes and scratch 10,110 bytes. Early silence emitted no draft; the
7100..7130s audio artifact was 960,044 bytes (sampled tree working set 36,360,192).
Cancellation waited until the real ffmpeg child was observed, then terminated
and independently read back an empty owned job in 6.521ms on this run. Timing uses
QueryPerformanceCounter (100ns resolution). The older GetTickCount64 clock had
15.625ms resolution and cannot support its apparent zero-duration result.
Memory was sampled every 20ms; scratch includes final file counts. These sparse-source
measurements do not establish long/high-resolution decode throughput, GPU
behavior or the final installed resource gates. Raw results:
`artifacts/media-long-resource-qpc/results.json`.

- Idle CPU/GPU ≈ 0: resource-behavior measurements remain on the Phase 7 checklist.
- Power-aware indexing policies (AC / battery saver / quiet hours): Phase 7.

## Retrieval-quality checkpoint, 2026-10-17

The shared application recipe now uses search/code task queries, title/text
documents, raw filenames/media, and finite cosine admission at0.65. Pipeline2
invalidates incompatible vectors while retaining durable exact text. The
previous native media false-positive run above is retained as historical RED;
the corrected production-daemon check passed three media positives and three
unrelated negatives in39.765s (`artifacts/indexed-media-recipe-final/`).

Expanded CLI fixture-index runs use actual CPU image/audio/text worker inputs,
strict non-stub768-dimensional provenance and unchanged overall gates
Recall@5>=.90,MRR>=.85,negativeFP0:

| Frozen suite | Recall@5 | MRR | Negative FP |
| --- | ---: | ---: | ---: |
| Original names | .933 | .933 | 0/3 |
| Development | .938 | .938 | 0/7 |
| Held-out | .938 | .906 | 0/7 |

The cooking query in names and one application paraphrase in each added suite
remain explicit misses. Held-out soundtrack ranks second; other added positive
categories rank first. Category results are printed separately. The recipe was
frozen before held-out outcomes; no expected answer or gate was changed.

`tools/test-retrieval.py` records executable/model/projector and generated input
hashes, actual backend/recipe, failed cases and owned-job cleanup. Refreshed runs
are `artifacts/retrieval-expanded-{names-v2,development-v3,held-out-v2}/`;
CLI SHA2d3ba4b... was identical across those three. Their wall times include the
wrapper and concurrent development activity, not an installed-app latency claim.
Fixture-index results do not establish native extraction, ordered daemon/CLI
parity, final reviewed-source or complete v1 acceptance.

The measured CPU cap8 remains the conservative HX370 default above. Native
Windows XRT detects the laptop's Strix NPU, but neither checked EmbeddingGemma2
bundle provides verified Windows model execution. See [availability check](hx370-npu.md).

## Resumable video-selection resources, 2026-10-18

Initial shot selection now commits progress before frame rendering. A fixed
15-second slice baseline made inexpensive sparse video spend most of its time
starting workers. Adaptive slices use a decoded-pixel estimate and persist the
chosen span, from 15 to 450 seconds. The owned 120-second watchdog still bounds
actual work, including GOP preroll. Unknown rates retain 15-second slices.

The final adaptive run reused the exact source files and pinned decoder binaries
from the baseline, verifying their SHA256 before execution. No competing build,
native test, inference or formatting process ran during this repeat.

| Native fixture measurement | Fixed 15-second baseline | Adaptive repeat |
| --- | ---: | ---: |
| Two-hour 160×100/1fps selection batches | 480 | 16 |
| Sum of selection-worker wall time | 142.443 s | 3.997 s |
| Selected visual frames / independent audio windows | 1,200 / 288 | 1,200 / 288 |
| Maximum batch, including separately checked render/audio | 3.354 s | 2.798 s |
| Peak sampled owned working set across those batches | 114,536,448 B | 62,074,880 B |
| Peak sampled owned private bytes | 93,585,408 B | 51,867,648 B |
| Maximum scratch | 960,044 B | 960,044 B |
| Actual decoder cancellation to empty owned job | 8.244 ms | 6.647 ms |
| 120-second 1080p/15fps/GOP450 selection batches | 8 | 8 |
| HD sum of selection-worker wall time | 8.712 s | 8.010 s |
| HD decoder cancellation to empty owned job | 7.040 ms | 6.726 ms |

The HD repeat peaked at 72,450,048 B owned working set, 51,494,912 B private bytes
and zero scratch during selection. Memory is sampled every 20 ms; scratch also
counts final files. Cancellation starts only after observing the actual ffmpeg
child and uses QPC. Every batch's owned job was empty after cleanup, with no
monitoring errors. The existing 1 GiB sampled working-set, 2 MiB scratch and
120-second per-worker assertions were unchanged.

The two-hour fixture checks early silence and a real late 7100..7130s mono16k WAV,
plus the complete selection plan; it does not decode/embed all 288 audio windows.
The improvement is consistent with reducing worker/decoder launches on cheap
sources. These individual host-time samples do not establish a general speedup
for other codecs, 4k/high-frame-rate or pathological long-GOP media. No model is
loaded, so this is extraction evidence, separate from retrieval quality.

Raw results and input locks are under
`artifacts/media-selection-resources-v1/` and
`artifacts/media-selection-resources-adaptive-v2/`. An earlier adaptive run
measured 4.588 s sparse/7.598 s HD; it overlapped controller formatting cleanup
near the end and is retained separately. Baseline hashes were recorded after
that original run; adaptive runs record their own inputs before execution.

Reproduce from the repository root, with the pinned decoder bundle available:

```powershell
python tools/test-media-resources.py --output artifacts/media-selection-new
python tools/test-media-resources.py --output artifacts/media-selection-repeat --reuse-sources artifacts/media-selection-resources-v1
```

The current selected Rust packages passed 353 tests, zero ignored, including
13 actual media fixtures, five actual PDF fixtures and 188 daemon tests:
`artifacts/media-selection-adaptive-covering-serial.log`. An earlier parallel
run exposed an obsolete visual-first test setup and a pre-existing 700 ms hostile
worker readiness race; both failure records are retained. The setup now completes
selection before testing the same six source/pause/root revocation cases. The
unchanged selected tests passed serialized. Final whole-workspace, independent
review, completed-vector preview/maintenance, UI, Lumen and installed gates remain.

## Shared parser admission and code quality, 2026-10-18

Process-wide two-slot parser admission now covers code and ordinary extraction,
including retained child/pipe/scratch cleanup. A denied code job preserves its
durable revision and poison attempts. Five owned-worker/Windows file-lock/queue
regressions exercise actual admission and cancellation. The engine shares one
extractor registry with background workers; pending consumer previews must reuse it.

The stronger native harness imports the actual pinned artifacts through the model
manager and waits for real code vectors. Earlier worker-only overrides left the
code pipeline unavailable, so those earlier runs do not establish semantic code
acceptance. The managed pipeline exposed an unrelated astronomy query returning
code hits. Both supported query tasks now corroborate a code candidate at the
unchanged .65 floor before candidate limits; code-task cosine and hybrid ranking
remain unchanged. Document vectors/model/dimension are unchanged.

Sixteen generated source snippets and independent development/held-out splits
(eight positives/eight negatives each) were frozen before measurement. The chosen
policy was locked before held-out evaluation. Actual native CPU diagnostic scores
and the final production CLI/store/ranker each report Recall@5/MRR1 and negative
FP0 on both splits. Code-only diagnostic admission returns FP1; a normalized-mean
alternative fails held-out FP.625 and was not selected or retuned. CLI harness
times are 3.807/3.820 s; these small fixture measurements are not code-index scale
or installed latency evidence. Existing suite labels and thresholds are unchanged.

The current rebuilt managed CPU daemon has four of four native code vectors at
768 dimensions. Twenty-one CLI/pipe and seventeen offline snapshot cases agree,
including actual semantic-only function/caller lines/excerpts and an empty
astronomy control. The typed Rust client adds three matching semantic checks to
its eight lifecycle/policy checks. Original positive cases separately preserve
Recall.90/MRR.85; the expanded group is .9091/.8636. The screenshot miss and audio
rank2 remain visible; all ten empty controls pass. Native index readiness is
62.495 s and total harness time74.867 s, including model staging/hashing and all
comparisons. This differs in scope from earlier worker-only timings and is not
a causal performance comparison. No-model native parity passes ten public/seven
offline/eight client checks in6.507 s with no code vectors. Owned jobs finish empty.

CPU verification executes518 Rust checks, including one API doctest, the explicitly
run normally ignored worker test and nine native text/media/reference cases.
The media worker separately retains four reference agreements>=.999 (minimum
.999698). The coherent CPU build passes with seven existing daemon dead-code
warnings. An intentionally interrupted CUDA rebuild, a CMake cache-source path
failure and a media-helper fixture-path error are retained as separate records;
they are not quality/GPU passes. Full GPU/scale/installed acceptance and independent
review remain open. [Evidence, functional failures and reproduction](validation/2026-10-18-parser-code/README.md).
