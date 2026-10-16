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
An actual CPU Q8_0 worker evaluation on 2026-10-16 passed all 15 positive
queries (Recall@5/MRR 1.000) but failed all three negative controls (FP 1.000
against the unchanged 0.000 gate). Prompt formatting alone did not resolve
the failure. Real retrieval acceptance remains pending its dedicated fix;
the earlier fixture result must not be presented as model-quality evidence.

### Tuned semantic & hybrid ranking thresholds

- `MIN_SEMANTIC_COSINE = 0.35`: the deterministic fixture's negative controls
  score below 0.15. Real unrelated filename matches scored approximately
  0.66–0.73, so this cutoff does not establish real-model rejection quality.
- `SEMANTIC_TOP_K = 200`: candidate pool before per-entry deduplication and hybrid scoring.
- Hybrid score band `0.45 + 0.45 * cosine` (`[0.45, 0.90]`): places semantic matches below exact/prefix matches (`>= 0.92`) and above weak FTS token matches (`0.50`), with `+0.10` exact-token boost and `+0.02` 30-day recency boost.
- `QUERY_CACHE_CAP = 64`: LRU cache for query embeddings so repeated/backspaced queries in an overlay session avoid re-embedding.

## Envelope and interpretation

- Typing in the overlay debounces at 120 ms; a query costs well under a frame
  budget even at 200k entries, so the interactive path stays responsive while
  indexing runs in the background.
- Query cost is bounded by design: each candidate source is capped at 500
  rows, so worst-case latency does not depend on how many files match.
- The remaining p95 gap at 200k comes from two-token substring phrases (e.g.
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

- Idle CPU/GPU ≈ 0: resource-behavior measurements remain on the Phase 7 checklist.
- Power-aware indexing policies (AC / battery saver / quiet hours): Phase 7.
