# Engine-only package for Lumen

`tools/assemble-engine-payload.py` assembles a model-free Windows x64 package
without WinUI or .NET. It consumes explicit local inputs; it does not download,
compile, install, start or publish them. The current binary pin is the tested
**debug/development** checkpoint `de3d18645dca3a47e4e5bbb6167c3165b7678093`.
This is not a signed release package or complete installed-Lumen acceptance.

## Contents and trust

The flat runtime directory contains `winsearchd.exe`, `winsearch-embed.exe`,
`winsearch-extract.exe`, the CPU llama.cpp/ggml/mtmd DLLs, PDFium, FFmpeg/FFprobe,
their shared DLLs and the Microsoft x64 release CRT. Notices retain the PDFium
dependency licenses, FFmpeg source instructions, llama.cpp license, code-parser
licenses and DigUp license. CUDA is not included in this CPU development package;
supported GPU calibration and release packaging remain open.

The assembler verifies engine binary/DLL hashes against the published
[native inputs](validation/2026-10-19-owned-startup/native-inputs.json), and
dependency archives against the repository's FFmpeg, PDFium and MSVC pins.
The llama.cpp license is read from the pinned Git object, avoiding checkout
line-ending changes. Source links/reparse points, unsafe archive names, special
entries, duplicate case aliases and excessive expansion are rejected. Existing
output directories are never overwritten. A fresh candidate is published only
after every copy and notice succeeds.

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
  --engine-commit de3d18645dca3a47e4e5bbb6167c3165b7678093 `
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
public exact search with no model. It checks deferred folder admission and that
the owned job finishes empty. It does not establish model inference, GPU,
installed UI, crash-recovery or performance acceptance.

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
single-index cutover remain open; the staged payload/client still uses the
earlier pinned checkpoint.

Before release, replace the development binary pin with verified final release
artifacts, finish runtime validation and single-index ownership, and run clean
installed Lumen/standalone, signing, licensing, update and uninstall gates.
All [README TODOs](../README.md#v10-todo) remain open until their complete scope
and acceptance checks are satisfied.
