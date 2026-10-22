# Engine-only package for Lumen

`tools/assemble-engine-payload.py` assembles a model-free Windows x64 package
without WinUI or .NET. It consumes explicit local inputs; it does not download,
compile, install, start or publish them. The current binary pin is the tested
**optimized release-profile** checkpoint `d4f1448068d93d51120f7ec1e2a17395f88089f7`.
This is not a signed release package or complete installed-Lumen acceptance.

## Contents and trust

The flat runtime directory contains `winsearchd.exe`, `winsearch-embed.exe`,
`winsearch-extract.exe`, the CPU llama.cpp/ggml/mtmd DLLs, PDFium, FFmpeg/FFprobe,
their shared DLLs and the Microsoft x64 release CRT. Notices retain the PDFium
dependency licenses, FFmpeg source instructions, llama.cpp license, code-parser
licenses and DigUp license. CUDA is not included in this CPU package;
supported GPU calibration, signing and installed acceptance remain open.

The assembler verifies engine binary/DLL hashes against the published
[native inputs](validation/2026-10-20-extraction-claims/release-native-inputs.json), and
dependency archives against the repository's FFmpeg, PDFium and MSVC pins.
The llama.cpp license is read from the pinned Git object, avoiding checkout
line-ending changes. Source links/reparse points, unsafe archive names, special
entries, duplicate case aliases and excessive expansion are rejected. Existing
output directories are never overwritten. A fresh candidate is published only
after every copy and notice succeeds.
Libraries already staged beside release binaries are sourced once from their
verified archives and checked against the native proof before publication.

`payload.json` records schema version, engine commit, Windows target, and each
file's relative path, SHA-256 and byte length. Models, indexes, databases, WinUI
files and developer executables are excluded. The manifest excludes itself;
its SHA-256 must be pinned separately by the consumer. Never establish trust by
hashing an arbitrary input manifest and silently accepting that value.

## Assemble and test

Use Python 3.11 or newer. Supply the archive paths and pinned llama.cpp checkout
explicitly; create the output's parent first. The current pin accepts only the
documented tested binary checkpoint, rather than labeling arbitrary binaries as
that commit.

```powershell
python tools/assemble-engine-payload.py `
  --binaries <verified-binary-directory> `
  --ffmpeg-archive <pinned-ffmpeg-zip> `
  --pdfium-archive <pinned-pdfium-tgz> `
  --msvc-archive <pinned-microsoft-crt-vsix> `
  --llama-source <pinned-llama.cpp-checkout> `
  --engine-commit d4f1448068d93d51120f7ec1e2a17395f88089f7 `
  --output <new-output-directory>

python tools/test_engine_payload.py -v
python tools/test-engine-payload-native.py `
  --payload <assembled-directory> `
  --manifest-sha256 <independently-recorded-manifest-hash> `
  --output <new-test-report.json>
```

The native test copies the actual payload to a temporary directory outside the
checkout, removes inherited engine overrides and developer PATH entries, and
uses an isolated data/pipe identity and kill-on-close process job. It starts the
decoders and embedding executable, then runs actual text/PDF extraction and
public exact search with no model. It checks deferred folder admission, source
policies, 1,000/1,100-result bounded streams and that the owned job finishes empty.
The test alone does not establish model inference, GPU,
installed UI, crash-recovery or performance acceptance.

The [current release checkpoint](validation/2026-10-20-release-runtime/README.md)
also passes actual copied-package CPU inference and all five frozen retrieval
suites. Its 62-file manifest SHA-256 is
`d4da84bfcc0ed1ae709963614911a6a1051162589bf666f72d5177c0d6a7c9e9`.
It uses Rust's existing release profile; no source guard or threshold was relaxed.
For native/retrieval development checks, `tools/test-search-parity.py` and
`tools/test-retrieval.py` accept `--binary-dir src/engine/target/release` while
retaining their debug default. Stage the pinned native dependencies alongside
release executables; production decoding does not honor the debug override.

## Lumen staging

The isolated [Lumen integration PR](https://github.com/KB01111/Lumen/pull/27)
adds `bun run stage:winsearch --source <package>`.
Its checked-in `src-tauri/winsearch-payload.lock.json` independently pins the
manifest hash. Staging verifies the source, every copied byte and the final
fixed `src-tauri/binaries/winsearch` directory. It rejects unknown files and
retains the previous staged package if source verification fails.

The integration's build hook verifies the staged package and provides
`LUMEN_WINSEARCH_MANIFEST_SHA256` to the Rust compiler; Tauri maps the directory to
`resource_dir/binaries/winsearch`. Missing/empty development resources produce
an empty anchor and an unavailable engine; corrupt staged resources fail.
Release verification requires a complete package. These changes are isolated
integration work. [Lumen PR #27](https://github.com/KB01111/Lumen/pull/27) now
owns verified lazy startup/status, retained process/PID authentication, deferred
root admission and job shutdown. Its running SearchService still uses its
existing backend. Root-policy forwarding, migration, streaming and the
single-index cutover remain open. At consumer commit `79460cc`, the staged
payload/client use the optimized release checkpoint above. Its actual
two-profile contract/PID/Stop gate passes; all 62 staged and build-copied files
match the independently pinned manifest. The
[consumer report](https://github.com/KB01111/Lumen/blob/codex/winsearch-engine-adapter/docs/reports/2026-10-20-winsearch-release-pin.md)
records 413 full-suite Rust passes, 27 ignored gates and the separately passed
owned-native test, plus staging, formatting, Clippy, typecheck and lint checks.

Before final delivery, reconcile any later binary changes, finish single-index
ownership and final runtime validation, and run clean
installed Lumen/standalone, signing, licensing, update and uninstall gates.
All [README TODOs](../README.md#v10-todo) remain open until their complete scope
and acceptance checks are satisfied.
# Building a current candidate

`tools/build-engine-candidate.py` builds a model-free CPU/NPU release snapshot
from a clean checkout. It requires explicit repository, target directory, XRT SDK,
pinned SDK archive and fresh output paths. The SDK inventory must match the pinned
archive, and source/SDK hashes must remain unchanged through the build. The tool
retains the exact recipe, command, toolchain, raw log and copied executable/library
hashes, and refuses a stub NPU runtime. CUDA and Vulkan are disabled for this
declared candidate profile.

This creates an unsigned, unqualified candidate. Existing package assembly still
requires fresh reference/retrieval proof bound to the selected source and binary
hashes. Candidate compilation or runtime-info output does not replace those gates,
the dependency/license closure, signing or installed acceptance.

