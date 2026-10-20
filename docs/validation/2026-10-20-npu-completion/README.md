# Windows NPU completion evidence

This report separates actual NPU inference, CPU references, synthetic recovery
fixtures and the Lumen consumer. Runtime source is frozen at
`85a915d460b1fa751bbe86f289ff77c18c032c25`; subsequent evidence/tooling commits
do not change that source. The independently pinned package manifest SHA-256 is
`171aedbb142c712520b768f524ca848a79aab519081cfc0209e8781e96d7390d`.

## Actual hardware and model

- AMD Ryzen AI 9 HX 370; PCI `VEN_1022&DEV_17F0`, `NPU Strix`.
- Installed XRT 2.21.0; AMD driver 32.0.20102.3930; firmware 1.1.2.64.
- Build SDK 2.21.75, pinned in `src/engine/third_party/xrt-sdk.json`.
- IRON revision `052b6a44fe2478d86aec2cd65fbd03ea24279a9e`: 142 files,
  1,085,278,625 bytes, every size and SHA-256 checked before native execution.
- CPU reference: pinned llama.cpp 4fbc76de, EmbeddingGemma 2 Q8_0 and projector.

## Qualified vector routes

The real Windows `ws-embed` acceptance passed 18 application text comparisons,
17 in-contract independent HF embedding references, 4 independent image
references and 8 CPU/NPU application image comparisons. All use cosine >=.999.
Application text agreement: .999597068–.999940220. Full tokenizer IDs include
special tokens and the 1421-token independent case; that case is truncated to
512 for application inference (cosine .999814460). Its unbounded upstream
inference hit the 45-second budget; it is outside the application contract.
Image preprocessing pixels/positions match the independent fixtures exactly.

| Image | 140 tokens | 280 tokens |
| --- | ---: | ---: |
| car | .999350440 | .999667278 |
| cat_laptop | .999433004 | .999716459 |
| cats | .999521450 | .999770533 |
| kitchen | .999282063 | .999185401 |

70-token NPU images failed the unchanged gate for car (.998327092) and
kitchen (.998424925), despite shared application resize preprocessing. On the
user's explicit approval, 70-token jobs always use the existing CPU/GPU path;
the NPU wrapper refuses this budget and the worker bypasses NPU initialization
for it. Text and 140/280-token images remain the only qualified NPU routes.
No tolerance, vector recipe, dimension, pooling rule or pipeline version was
lowered or replaced. Audio remains on llama.cpp.

## Verification ledger

- .NET 170 tests passed, no skips.
- Download cancellation retains resumable component partials. Import
  cancellation/resume is covered through the model manager and replacement/
  deletion; the existing download-cancel IPC remains download-only.
- Python package/media/native-guard contract 21 tests passed; includes stub/missing/stale shim,
  changed source/commit, incomplete qualification and copied runtime-info checks.
- Full final-source Rust workspace: 721 passed, 11 ignored; final focused lifecycle and
  supervisor suites also pass. A residency-handshake regression was corrected
  without allowing retired responses to record a successful backend.
- Final source-checkpoint release reference test passed in 48.57 seconds, including CPU
  comparisons; this duration is not a latency benchmark.
- All five frozen retrieval suites pass on NPU-requested and alternating
  CPU/NPU indexes. Negative-control false positives remain zero; pipeline 2,
  semantic floor .65 and every frozen acceptance threshold are unchanged.
- The final native supervisor recovery test passed in 49.20 seconds: simulated
  initialization resource failure, CPU retry/quarantine, deliberate reset to
  actual NPU, simulated device loss, 64 long-text inputs retried on CPU,
  quarantine/reset back to NPU and independent Stop. Faults terminate exact
  retained owned process handles; they do not claim physical device removal.
- A DLL with deliberately blocked process-detach cleanup made ordinary worker
  exit fail its 5-second independent parent deadline. Forced self-termination
  passed in .54 seconds. This fixture tests driver/loader-lock recovery; it does
  not claim a physical driver failure was induced.
- Final independent review found that a native wait error could destroy an
  active cached run while Rust propagated the error. Windows start/wait and
  terminal failure guards now force 75 before caller run/buffer destruction.
  A shared-header MSVC fixture failed all eight unprotected failure cases;
  all 11 guarded child cases pass, including success/pre-launch controls and
  stack/run/global destructor sentinels. Linux retains its error behavior.
- Clippy completes successfully with existing workspace warnings. Global
  rustfmt already fails on refreshed `b12a452`; native runtime files are
  formatted, and unrelated compact source formatting is preserved.

The [validation ledger](validation-evidence.json) records scope and hashes of
the raw logs, including negative controls and the baseline formatting failure.
Local raw logs are under ignored `artifacts/npu-*`. No model weights, AMD
driver, or build SDK is redistributed with the engine. This payload contains
CPU and NPU runtimes. CUDA/Vulkan fallback source paths are preserved; live
CUDA/Vulkan execution is not part of this hardware/package evidence.

## Exact package and measurements

[Native proof](release-native-inputs.json) binds 150 source files, the four Rust
executables, 32 adjacent libraries, SDK pin, reference log and all 10 frozen
retrieval runs. [Manifest](payload.json) declares 69 files / 221,486,715 bytes;
[dependency closure](package-dependencies.json) audits all 37 PE files, including
FFmpeg/ffprobe, with zero unresolved imports. XRT is a trusted system prerequisite.
The real shim and taconite/model-runtime license notices are included. Assembly
rejects stub, absent/stale shim, mismatched source and incomplete qualification.

[Copied-package results](package-inference.json) run the exact package from a
temporary directory outside WinSearch and Lumen. Weights and test media are
separate external copies. PATH contains only System32 and developer overrides
are removed. Actual NPU text and 140/280 images, CPU 70 images/audio, CPU-only
references and four cross-modal retrieval rankings pass; responses explicitly
report non-stub actual routes. Missing shim, missing bundle and corrupt bundle
each retain usable CPU inference on repeated requests. Every owned Job Object
is empty after shutdown; original package/model bytes remain unchanged.

Fixed short-query process-cold and warm measurements on this shared HX370 host:

| Measurement | NPU requested | CPU |
| --- | ---: | ---: |
| First text request, process cold | 4.257 s | 2.279 s |
| Warm text, median of 5 | 107.29 ms | 24.13 ms |
| Four-text batch throughput | 9.30 items/s | 36.10 items/s |
| Peak working set, complete mixed-media run | 3,443,261,440 B | 1,352,183,808 B |
| Peak private memory, complete mixed-media run | 3,897,962,496 B | 1,793,654,784 B |

CPU is faster for this short text input. Cold means a fresh worker, with the OS
file cache uncontrolled. The NPU-requested memory peak includes CPU residency
for fallback images/audio. Worker traces separate host processing/attention
from NPU operator dispatch; warm text dispatch is approximately 76-92 ms. These
are host wall times including driver waits, not device cycle/utilization data.
No hardware timing counter or exclusive-machine performance claim is made.

Reproduce using `tools/test-npu-package.py` with `--payload`, independently pinned
`--manifest-sha256`, external `--models`, `--fixtures`, `--spoken`,
`--consumer-root` and fresh `--output`. The original no-model native package
test retains its 90-second runtime watchdog and unchanged 1,000/1,100-result checks.
The [final idle-host run](package-native.json) passed in 66.443 seconds: all nine
checks passed, no timeout, owned job empty. No test inputs or deadlines changed.
The prior-source 98ba190 [compilation-contention timeout](package-native-concurrent-timeout.json)
and its [66.016-second isolated pass](package-native-before-native-guard.json)
are preserved. Final-source 85a915d also [passed under compilation load](package-native-under-load.json)
in 91.502 seconds including setup before the runtime watchdog, then
[timed out during installer compression](package-native-installer-timeout.json).
All attempts emptied their owned jobs. This gate is sensitive to concurrent
host load; the idle-host pass does not establish a search-latency guarantee
under build/installer contention. Neither the watchdog nor result assertions
were relaxed.

## Lumen handoff and remaining acceptance

Lumen staging accepts the exact pinned manifest. Its explicit two-profile native
test passes against the updated payload, retaining wrong-PID authentication
refusal, distinct process/index identities, paused/unadmitted roots and
independent Stop (13.09 seconds). Its final locked full Rust suite passed 413
tests / 27 ignored; strict Clippy, formatting, 22 staging tests and the final
locked Tauri/NSIS build passed. UI sources/configuration are unchanged from
the 787 Vitest, 131 Edge and 4 profiling passes. The
[consumer evidence](https://github.com/KB01111/Lumen/blob/codex/winsearch-engine-adapter/docs/reports/2026-10-20-winsearch-npu-handoff.md)
records source/log/binary hashes and the exact staged/debug/release payloads.
Lumen live SearchService routing, migration, local answer generation, signing,
installed-app acceptance and broader v1 acceptance are outside this delivery.
