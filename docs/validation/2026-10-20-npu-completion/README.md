# Windows NPU completion evidence

This report separates actual NPU inference, CPU references, synthetic recovery
fixtures and the Lumen consumer. Package/consumer acceptance is pending until
the final source, binary and independently pinned payload hashes are recorded.

## Actual hardware and model

- AMD Ryzen AI9 HX370; PCI `VEN_1022&DEV_17F0`, `NPU Strix`.
- Installed XRT2.21.0; AMD driver32.0.20102.3930; firmware1.1.2.64.
- Build SDK2.21.75, pinned in `src/engine/third_party/xrt-sdk.json`.
- IRON revision `052b6a44fe2478d86aec2cd65fbd03ea24279a9e`:142 files,
  1,085,278,625 bytes, every size and SHA-256 checked before native execution.
- CPU reference: pinned llama.cpp4fbc76de, EmbeddingGemma2 Q8_0 and projector.

## Qualified vector routes

The real Windows `ws-embed` acceptance passed18 application text comparisons,
17 in-contract independent HF embedding references,4 independent image
references and8 CPU/NPU application image comparisons. All use cosine>=.999.
Application text agreement: .999597068–.999940220. Full tokenizer IDs include
special tokens and the1421-token independent case; that case is truncated to
512 for application inference (cosine .999814460). Its unbounded upstream
inference hit the45second budget; it is outside the application contract.
Image preprocessing pixels/positions match the independent fixtures exactly.

| Image | 140 tokens | 280 tokens |
| --- | ---: | ---: |
| car | .999350440 | .999667278 |
| cat_laptop | .999433004 | .999716459 |
| cats | .999521450 | .999770533 |
| kitchen | .999282063 | .999185401 |

70-token NPU images failed the unchanged gate for car(.998327092) and
kitchen(.998424925), despite shared application resize preprocessing. On the
user's explicit approval,70-token jobs always use the existing CPU/GPU path;
the NPU wrapper refuses this budget and the worker bypasses NPU initialization
for it. Text and140/280-token images remain the only qualified NPU routes.
No tolerance, vector recipe, dimension, pooling rule or pipeline version was
lowered or replaced. Audio remains on llama.cpp.

## Verification ledger

- .NET170tests passed, no skips.
- Python package/media contract18tests passed; includes stub/missing/stale shim,
  changed source/commit, incomplete qualification and copied runtime-info checks.
- Full Rust workspace:716passed,11ignored; final focused lifecycle and
  supervisor suites also pass. A residency-handshake regression was corrected
  without allowing retired responses to record a successful backend.
- Actual release reference test passed in105.60seconds, including CPU
  comparisons; this duration is not a latency benchmark.
- All five frozen retrieval suites pass on NPU-requested and alternating
  CPU/NPU indexes. Negative-control false positives remain zero; pipeline2,
  semantic floor.65 and every frozen acceptance threshold are unchanged.
- A real worker survived simulated device loss through CPU retry/quarantine,
  deliberate reset back to NPU and Stop (103seconds, including a64-input
  long-text fallback). Final exact-package recovery is recorded separately.
- A DLL with deliberately blocked process-detach cleanup made ordinary worker
  exit fail its5second independent parent deadline. Forced self-termination
  passed in.54seconds. This fixture tests driver/loader-lock recovery; it does
  not claim a physical driver failure was induced.
- Clippy completes successfully with existing workspace warnings. Global
  rustfmt already fails on refreshed `b12a452`; native runtime files are
  formatted, and unrelated compact source formatting is preserved.

Local raw logs are under ignored `artifacts/npu-*`. Final checked-in proof will
record exact binary/source hashes and compact measured results. No model
weights, AMD driver, or build SDK is redistributed with the engine.

## Open acceptance

Frozen NPU/mixed retrieval, real worker recovery/deadline tests, final copied
package inference, measured cold/warm behavior, strict payload staging and
Lumen's two-profile PID/admission/independent Stop test remain pending here.
Lumen live SearchService routing, migration, local answer generation, signing,
installed-app acceptance and broader v1 acceptance are outside this delivery.
