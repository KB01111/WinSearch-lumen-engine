# WinSearch v1 real retrieval quality implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Make the daemon and evaluation runner use the same model-aware retrieval behavior and reject unrelated results while preserving useful multilingual/content/media matches.

**Architecture:** Reuse the existing exact-first and semantic stages. Move their duplicated admission/ranking and application input formatting into small shared modules, version the resulting vector recipe, and calibrate real-model admission against separate development and held-out data. Keep raw low-level runtime references intact.

**Tech Stack:** Existing Rust ws-index/ws-modelmgr/daemon/CLI and the pinned real EmbeddingGemma2 worker. No new model/provider/reranker service.

## Global Constraints

- Work only in C:\Users\kevin\.codex\worktrees\winsearch-v1\WinSearch on codex/winsearch-v1. Approved full v1 design remains binding.
- Begin after reviewed PDF/media runtime wiring. The genuine media daemon exposed three unrelated-query false positives, so fail-closed evaluation/provenance and shared recipe/admission now precede code parsing. Keep this task open until real code wiring and the complete held-out suite pass; code and all original modality gates remain required. Preserve prior checkouts, exact-first response/cancellation, code opt-in and policy visibility before limits.
- Prefix shell commands with rtk. No weakened Recall@5/MRR/negative-FP gates, changed expected answers to hide failures, regenerated native references or relaxed cosine tolerance1e-3.
- Never branch on evaluation queries/corpus/expected labels in production. No production stub vectors or silent fallback to a deterministic embedder when a real model was requested.
- Use isolated generated/public fixtures and explicit CPU/native settings. Benchmark device/backend separately; do not call fixture smoke tests model-quality evidence.

## Task 1: Shared model recipe, confidence admission and honest evaluations

**Files:**
- Create: src/engine/crates/ws-index/src/retrieval.rs
- Modify: src/engine/crates/ws-index/src/lib.rs
- Modify: src/engine/bins/winsearchd/src/{engine,indexer,extract}.rs and code query integration
- Modify: src/engine/bins/winsearch-cli/src/eval.rs and CLI reporting
- Modify: src/engine/crates/ws-modelmgr/src/manifest.rs for application recipe/fingerprint
- Modify: evaluation fixtures only to add independent coverage, retaining existing gates/cases
- Test: shared ranking, daemon/CLI parity, true worker provenance, recipe migration and held-out retrieval
- Modify: docs/{architecture,benchmarks,feature-parity}.md

**Interfaces:**

Produce shared functions in ws-index::retrieval for semantic admission and hybrid
ranking; all engine/eval callers must use them. Admission takes segment kind and
finite cosine, not query text or evaluation metadata. Keep filtering/exact
deduplication separate from admission. Name explicit model-specific cutoffs in
one place, retain their development/held-out measurements, and reject invalid
vectors. It is acceptable to tighten an overly permissive product admission
cutoff; acceptance test thresholds and native tolerances must remain unchanged.

Application-level input-format functions live with the model recipe (ws-modelmgr)
and distinguish normal query, code query, filename, document and code document.
They must be pure and shared by index/query/eval paths. Consult official model
card guidance, then measure filename formatting rather than assuming full
document prompts suit filenames. Media carries no text prefix. Low-level raw
EmbedRuntime APIs/reference inputs retain their meaning. Advance the media
slice's pipeline-version fingerprint whenever document/name formatting changes;
legacy vectors cannot mix with a new query recipe. Preserve exact text and
resume deferred vectors without reparsing everything unnecessarily.

Known real baseline red is recorded in artifacts/baseline-names-eval.log:
15positive queries all rank1, all3negative controls return unrelated hits,
FP1.0 against max0.0. Ignored artifacts/retrieval-probe has actual raw/prompt
vectors and scores; prefixes alone did not fix negative controls. This is
diagnosis, not a license to choose a cutoff solely around these18queries.

Define extra development and held-out sets before calibration, covering unseen
multilingual paraphrases, generic filenames, near-topic confounders, absent
exact identifiers, receipts/screenshots, PDF-only terms, speech and video-only
moments and opt-in code definitions. Keep the held-out set out of parameter
selection. Report Recall/MRR/FP per modality and overall, including failed
cases. The original names suite must pass unchanged with the actual model.
If no uniform cutoff works, use measured per-kind admission; do not substitute
query-specific hardcoding or an external provider for a root-cause fix.

`WS_EMBED_TEST_MODEL` being set to a missing/invalid file must fail explicitly.
When real inference is selected, verify handshake and each response stub:false,
dimension768, finite unit vectors, expected ID/count and actual backend before
claiming a live model report. An unset model can keep deterministic pipeline
smoke tests, clearly labeled and distinct from real acceptance. Add diagnostic
negative top hits/cosines to evaluation failure output so future failures can
be investigated without weakening gates or rebuilding scratch probes.

- [ ] Add red tests for an explicitly invalid model path and a worker returning
  stub:true under a requested real-model run. Add shared daemon/eval ranking and
  formatting parity regressions, plus old fingerprint migration. Preserve every
  existing native reference case/tolerance and fixture smoke test.
- [ ] Run unchanged names suite on the real CPU worker to reproduce its negative
  red, then define development/held-out additions and record their baseline.
  Inspect actual similarities and source segments, including the corrected
  real image/audio pipeline. Choose the smallest evidence-backed correction.
- [ ] Implement shared recipe/admission/ranking, vector requeue migration and
  fail-closed evaluation provenance. Exact hits/locations remain available when
  semantic confidence is insufficient or no model is downloaded. Code results
  remain isolated from normal mode and definitions rank before references.
- [ ] Run original names and all added modality/held-out suites with the real
  worker; preserve all existing gates. Verify a generated isolated daemon over
  its public pipe returns the same ordered IDs, source locations and rejection
  behavior as the CLI shared pipeline. Include root revocation and cancellation.
- [ ] Run focused plus selected full Rust tests and unchanged real native
  references, .NET tests/x64 build if protocol changes, then diff check. Record
  model/dependency hashes, pipeline version, CPU/backend, query metrics and any
  remaining failures. Commit and report real versus fixture evidence precisely.
