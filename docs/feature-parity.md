# WinSearch Feature-Parity Checklist (vs. DigUp)

Maintained continuously during implementation. A feature is checked only when
implemented **and** verified. Legend: `[x]` done+verified · `[~]` partial ·
`[ ]` planned.

## Reference

- DigUp (MIT, ARahim3/DigUp) — macOS/Swift reference. Architecture notes and
  behaviors ported are documented in `architecture.md`; attribution ships in
  `THIRD-PARTY-NOTICES.txt`.

## Core launcher

- [~] Global hotkey search overlay (default Alt+Space, customizable, conflict
      detection + fallback) — implemented; onboarding surfacing pending (Phase 6)
- [x] Overlay reuses one window (no rebuild per invocation)
- [x] Escape closes instantly; focus restored to previous window
- [x] Multi-monitor + DPI-aware placement (opens on the active monitor)
- [x] Arrow-key navigation, Enter opens, click opens
- [~] Keyboard conventions (Ctrl+Enter reveal, Space preview, Ctrl+O expanded) —
      Ctrl+Enter reveal + Ctrl+C copy path wired; Space preview and
      Ctrl+O expanded await the Phase 6 preview pane / explorer window
- [x] Progressive search response (exact results stream first)
- [x] Superseded-search cancellation + stable updates

## Search & indexing engine

- [x] Rust engine runs independently of the UI (`winsearchd`)
- [x] Per-user named-pipe IPC, typed requests, streaming, cancellation,
      protocol versioning, auto-reconnect
- [x] SQLite database + FTS5 exact search (Phase 2) — exact/prefix/trigram/
      token sources, deterministic ranking with match explanations
- [x] File/folder discovery + incremental crawler (Phase 2) — generation
      sweeps, exclusion policy, identity capture
- [x] Filesystem watching (ReadDirectoryChangesW) + rename/move identity
      (Phase 2) — paired rename events preserve entry ids; overflow triggers
      rescan
- [x] Application catalog search + launching (Phase 2) — Start Menu/Desktop,
      IShellLinkW target resolution, shell launch
- [x] Calculator + Windows settings URI search (Phase 2) — real arithmetic
      evaluator (nothing executed) + ms-settings catalog
- [x] Persistent checkpoints; crash/restart-safe indexing (Phase 2) — durable
      claim queue, idempotent upserts, verified crash/resume
- [x] Disconnected-drive preservation + reconnect reconciliation (Phase 2) —
      verified end-to-end with a mapped drive (subst)

## Semantic search (EmbeddingGemma 2)

- [x] Model download (865 MB: text Q8_0 + mmproj Q8_0), SHA-256, resume, import (Phase 3)
- [x] llama.cpp integration, CUDA > Vulkan > CPU backend detection (Phase 3)
- [x] Lazy load / warm-up / idle unload / explicit GPU release (Phase 3)
- [x] Vector store (FP16, 768-d) + cosine scan (Phase 3)
- [~] Hybrid ranking + exact boost + explanations + thresholds (Phase 3) —
      deterministic fixture passes; actual model fails negative controls
- [x] Query-embedding cache, cancellation-aware semantic pass (Phase 3)

## Multimodal content

- [~] Image semantic search (JPEG/PNG/WebP/HEIC/AVIF/TIFF/BMP/GIF/RAW) + EXIF (Phase 4) —
      real CPU mixed encoder/reference and image retrieval pass; full format/EXIF,
      final installed payload and UI acceptance remain required
- [~] Screenshot search: visual + OCR text, exact error codes (Phase 4) —
      actual OCR and genuine visual discrimination/reference checks pass;
      final combined screenshot retrieval/UI acceptance remains required
- [~] PDF page-level search, scanned-page OCR, page numbers/thumbnails,
      first-30-pages fast path, 5,000-page cap, resumable (Phase 4) — native
      PDFium text/raster and installed WinRT OCR verified with generated PDFs;
      exact pages survive restart without a model. Rendered-page visual artifacts
      reach the deferred embedding path; real visual-model quality and packaged
      distribution acceptance remain separate gates.
- [~] Document search (txt/md/rtf/doc/docx/odt/html) with chunking, excerpts,
      numeric-content exact-only routing (Phase 4) — no-model exact persistence
      and durable long-document continuation verified; final UI/package gates pending
- [~] Audio: 30 s overlapping windows, timestamp matches, player with
      seek-to-match (Phase 4) — real bounded PCM runtime/reference/spoken retrieval
      verified. Native bounded decoding, silence-only continuation and durable
      typed/deferred WAV inputs now pass. The genuine daemon retrieves the later
      passage at 50..65s; unrelated-query rejection, player, packaging and final
      acceptance remain required
- [~] Video: periodic + shot-change frames, audio-track indexing, timestamp
      navigation, timeline markers (Phase 4) — native selection/PTS/OCR/phases and
      source-policy/cache recovery verified; genuine frame and later soundtrack
      locations recorded. Negative controls still fail; timeline/player, complete
      cache maintenance, expensive long-source scanning and packaging remain open

## Code search (opt-in)

- [x] Separate code index (never mixes with normal results) (Phase 5)
- [x] Repository detection, .gitignore, language detection (Phase 5)
- [ ] Tree-sitter chunking + symbols, definitions over references (Phase 5) —
      existing parser uses line heuristics; no Tree-sitter dependency yet
- [x] Notebook cells (without outputs), line-number preservation (Phase 5)
- [ ] `code:` queries (semantic + exact identifier) (Phase 5)
- [ ] Editor detection + open-at-line (VS Code, Cursor, VS, JetBrains,
      Sublime, Zed, configurable) (Phase 5)

## Windows experience

- [x] Tray icon: open/settings/exit, indexing tooltip
- [x] Settings window (theme, hotkey rebind w/ conflict detection, query memory)
- [ ] Expanded search explorer: list + grid, category filters, preview pane (Phase 6)
- [ ] Onboarding (6 steps incl. model download, folder estimates, shortcut) (Phase 6)
- [ ] Full settings sections (General/Search/Folders/Code/AI/Performance/Storage/About) (Phase 6)
- [ ] Animations (overlay, selection, category), reduced-motion + high contrast (Phase 6)

## Power & performance

- [ ] Power-aware indexing policies (AC/battery saver/quiet hours/manual) (Phase 7)
- [~] Idle CPU/GPU ≈ 0, bounded queues, measured benchmarks (Phase 7) —
      bounded queues/workers in place, idle paths block (no polling),
      measurements in `benchmarks.md`; resource-behavior tests remain

## Privacy & security

- [x] Local-only IPC (per-user DACL, no network listener)
- [x] No telemetry anywhere in the codebase
- [x] Sensitive-file exclusions (keys, certs, credential stores, hidden dirs) (Phase 2)
- [x] No cloud-file hydration (OneDrive placeholders) (Phase 2) — metadata
      from directory enumeration; attribute-only identity opens
- [ ] Untrusted-content parser limits (Phase 4)

## Distribution

- [ ] Inno Setup installer, startup registration, clean uninstall (Phase 7)
- [ ] Code-signing documentation, WinGet manifest prep (Phase 7)
- [ ] Update check (no file data uploaded) (Phase 7)

## Tests & evaluation

- [x] IPC round-trip tests (real named pipe), framing, protocol serialization
- [x] Crawler/watcher/filesystem behavior tests (Phase 2) — 68 tests across
      the workspace, incl. real ReadDirectoryChangesW events, shell-created
      .lnk round-trips, crash/resume, and subst-based drive disconnects
- [~] Retrieval evals (exact, OCR, PDF page, audio/video timestamps, code) (Phases 3–5) —
      `winsearch-cli eval names` runs Recall@5, MRR and negative controls.
      Deterministic fixtures pass; the actual-model name baseline fails negative
      controls. Full indexed OCR/PDF/media/code quality remains required.
- [ ] Resource-behavior tests (Phase 7)
- [ ] UI verification matrix (Phase 7)
