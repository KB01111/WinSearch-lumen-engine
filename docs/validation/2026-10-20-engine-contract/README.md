# Engine contract runtime verification — 2026-10-20

The rebuilt development candidate uses engine source
`04018c7733dbb58731c0b06308d49bf031676726`, containing database identities,
per-folder source rules and shared search result limits. Its native inputs are
recorded in [native-inputs.json](native-inputs.json). These are verified runtime
inputs, not complete package or installed-consumer acceptance.

## Passed on the frozen candidate

The actual pinned weights/projector passed all 25 `ws-embed` tests, including
previously ignored reference/native cases, and the separate actual-model worker
test passed. Text references retained a worst drift of 1.32e-4 across ten cases;
image, audio, photo and frame reference cosines passed the unchanged 0.999 floor.
CUDA compilation was enabled but successful acceleration is not established;
the tested inference path used CPU. Vulkan was not built in that environment.

No-model and managed CPU native profiles passed 10 and 21 public queries,
respectively, with the same executable/DLL/client/comparator hashes and no
intervening Rust build. Both owned jobs ended empty. These include actual
document/media/code extraction, typed client parity, source references,
revocation, cache retention, Related and in-flight cancellation.

Original CPU Recall@5/MRR stayed .90/.85; expanded positives stayed
.9091/.8636. Ten empty controls had zero false positives. The screenshot miss
and audio rank two remain. The five frozen names, retrieval development/held-out
and code development/held-out suites also passed their existing gates with the
actual worker. These are distinct fixture/native measurements, not controlled
scale or installed performance acceptance.

## Package rejection retained

Assembly produced 62 files totaling 269,181,630 bytes, excluding the manifest.
Candidate manifest SHA-256:
`866d35390fc5093b8d7dc1605d6e014e2eb320cc64b8d79f427909e7721ca036`.
The older payload failed the expanded native test because all three new
capabilities were absent, and its owned job ended empty.

The new candidate starts its binaries outside the checkout with only System32
on PATH, exposes the new capabilities/ordinary database identity and extracts
the generated text/PDF. Its 1,100-file fixture exposed a failure: a request for
1,000 results produced no result frame before the existing 90-second watchdog
expired. The diagnostic confirms `timedOut: true` and an empty owned job after
termination. Small text/PDF matches completed before the large request.

The initial expanded fixture also assumed every query would complete during
inventory. It now retries only explicit empty `superseded` termination within
the existing deadline, and still requires complete results for every assertion.
Failed runs are retained locally. A diagnostic SQLite handle cleanup error was
corrected with explicit connection closure.

The resolver was also found to reject metadata-only identity for ordinary files
whose content becomes reserved for project/code search. A new functional RED
reproduces that behavior. The [metadata identity fix](../2026-10-20-metadata-identity/README.md) avoids content-policy work for
metadata-only bindings while retaining root, native file, residency and stale
content-proof checks. Eight focused reference cases pass; final disjoint Rust
coverage is recorded in that checkpoint. New-source package performance
validation remains required.

Lumen remains at consumer commit `8865f159b5f1a00e06fcf46e55e901631ea7e052`, with
its earlier `de3d186` payload/client pin. This rejected candidate has not been
staged or published as a release. The SearchService cutover, metadata migration,
one authoritative crawler/index/model lifecycle, independent final review and
full v1.0 acceptance remain open.
