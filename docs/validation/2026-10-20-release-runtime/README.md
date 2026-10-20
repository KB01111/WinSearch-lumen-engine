# Optimized CPU engine package — 2026-10-20

The unchanged Rust source at `d4f1448068d93d51120f7ec1e2a17395f88089f7`
now has a verified release-profile engine package. This clears the functional
outside-checkout package gate that the earlier debug builds failed. It does not
complete the Lumen search adapter, controlled performance or installed acceptance.

## Verification

- Release actual-model/reference tests: 25 pass, followed by the explicit real
  worker round trip: 1 pass. A final release build supplies the coherent binaries.
- Complete no-model and actual managed-CPU native profiles: 10 and 21 public
  queries pass, including typed-client/offline parity, root revocation, code and
  notebook identity, cached content and cancellation. Their whole-harness times
  are 12.455 and 52.059 seconds. Source, binary and DLL hashes remain fixed.
- The CPU profile retains original Recall@5 .90/MRR .85, expanded positives
  .9091/.8636 and ten empty controls with zero false positives. The screenshot
  miss and audio rank two remain documented.
- Outside-checkout package: text/PDF extraction, source policies, explicit
  deferred admission and 1,000/1,100-result bounded streams pass. The unchanged
  90-second watchdog does not fire; the owned job finishes empty. Whole-harness
  elapsed time is 45.492 seconds. These observations are not controlled latency
  percentiles, indexing-under-load or scale acceptance.
- Actual copied package CPU worker: non-stub 768-dimensional outputs, four media
  reference cosines above the unchanged .999 floor and four cross-modal ranking
  positives pass. The worker exits normally. Verified local model/projector
  files are explicit inputs; models are not bundled in the package.
- All five frozen retrieval suites pass with zero negative-control false
  positives: names .933/.933 Recall@5/MRR, retrieval development .938/.938,
  retrieval held-out .938/.906 and both code suites 1/1. Existing allowed misses
  remain in the evidence; fixture chunks are not native extraction proof.
- Static dependency closure: all 36 PE files have their non-system imports
  supplied locally. This is separate from actual model execution.
- Final assembly regressions: 13 pass. Matching archive libraries already staged
  beside release binaries are copied once; a native-proof/archive mismatch
  rejects publication. Independent GPT-6.1 Sol read-only review found no scoped
  issue in the harness options, release sequence or assembly fix.

## Package and retained failures

The package contains 62 files, 220,906,174 bytes excluding its manifest. Its
independent manifest SHA-256 is
`d4da84bfcc0ed1ae709963614911a6a1051162589bf666f72d5177c0d6a7c9e9`.
The [manifest](payload.json), [PE imports](pe-imports.json),
[native input proof](../2026-10-20-extraction-claims/release-native-inputs.json)
and [source/test/runtime evidence](evidence.json) are published without binaries,
models, databases, machine application inventory or private source paths.

Earlier debug failures remain retained, including the readiness-first replay
that reached the watchdog. The order-only harness correction puts policy
readiness before the load queries, retaining the same 45-second readiness
deadline, 90-second watchdog and assertions. It does not establish post-load
reconfiguration or controlled concurrency acceptance.

The initial release runs caught missing staged PDFium and FFmpeg dependencies.
Those failed reports/log hashes remain retained. Pinned archives then supplied
the complete release-local PDFium, decoder and CRT files before both final native
profiles. No decoder override was enabled in production code, and no Rust source
or acceptance threshold changed for this release checkpoint.

## Remaining work

Lumen now pins the matching client/package at consumer `79460cc`, with the actual
two-profile contract/PID/Stop gate passing. Complete its SearchService
adapter, policy forwarding, data migration and single crawler/index/model
lifecycle. The standalone UI, controlled latency/scale/resource/recovery, GPU
calibration, signing/licensing review, installer/update/uninstall and installed
consumer gates remain open. This CPU package excludes CUDA; NPU remains optional.
All 33 README tasks retain their full implementation and acceptance scope.
