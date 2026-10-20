# NPU EmbeddingGemma 2 completion

Approved by the user on 2026-10-20. Work starts at `b12a452` (the merged
initial IRON integration). Lumen's existing PR #27 is the package consumer.

## Required outcomes

- [x] Real Windows XRT shim build and verified, pinned NPU2 model execution.
- [x] Existing 768-dimensional recipe v2, text cap512 and image budgets70/140/280;
      actual NPU and CPU agreement >=.999, unchanged retrieval thresholds.
- [x] Independent base/NPU model readiness; explicit opt-in download/import,
      resumable cancellation and complete deletion.
- [x] Additive Rust/C# diagnostics, cached status, actual modality backends.
- [x] Bounded NPU initialization/execution, worker retirement and fallback
      quarantine, existing120second supervisor limit and independent Stop.
- [ ] Verified engine-only package with real shim, licenses, native binary
      proof, independent manifest pin and outside-checkout execution.
- [ ] PR #27 dependency/lock/package pin and allowlist update with native
      two-profile ownership checks and required Lumen verification.

## Boundaries

No live Lumen search cutover, data migration, local generation, driver changes,
model bundling, release publication or automatic merge. CPU/GPU fallback,
current source consent, deferred roots and owned process lifetimes remain.
Failed parity blocks NPU release and vector mixing; it does not justify
changing recipes or lowering thresholds.

User-approved qualification adjustment:70-token image requests use CPU/GPU.
Only text and140/280-token images may use NPU after the unchanged parity and
retrieval gates pass. Preserve the failed70-token comparisons in the report.

## Verification

Use regression tests for model components/protocol defaults, malformed vectors,
backend selection, timeout/reconfiguration quarantine and stale workers.
Run real NPU text/image references, all frozen retrieval suites, copied-package
CPU/NPU and no-model profiles, cancellation/recovery and cold/warm measurements.
Run affected Rust/.NET tests, fmt/Clippy/release builds and all Lumen AGENTS.md
checks. Record final source/binary/model hashes and fixture/CPU/NPU/consumer
evidence separately. Installed-app, signing and unrelated v1 gates stay open.
