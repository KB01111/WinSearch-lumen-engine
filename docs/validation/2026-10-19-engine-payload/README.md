# Engine-only payload checkpoint — 2026-10-19

This checkpoint implements actual local package assembly and isolated Lumen
staging/build verification. It does **not** complete the Lumen adapter, installed
consumer, GPU, release or full v1.0 gates. All 33 README TODOs remain open.

The [Lumen packaging PR](https://github.com/KB01111/Lumen/pull/27) contains the
consumer staging/build changes at `cdc1e57a849e391b022f9aa5f76b6a04a536d971`.
It remains a draft while the native adapter and complete consumer gates are open.

## Actual payload and native evidence

- Engine binaries: the unchanged tested debug checkpoint
  `de3d18645dca3a47e4e5bbb6167c3165b7678093`. Their hashes are checked against the
  [owned-startup native inputs](../2026-10-19-owned-startup/native-inputs.json).
- 62 payload files, 268,999,358 bytes, excluding the manifest itself. The
  [manifest](payload.json) SHA-256 is
  `6865b4bf80fd97b761d71affe186104ec6c2e150b942ab5532c12da9a1ec7fc6`.
- Decoder/PDFium archives retain their existing pins. Import inspection found
  previously omitted Microsoft C++ runtime dependencies. The assembler now
  includes the complete release x64 CRT from the separately pinned Microsoft
  VSIX, without installing it or copying System32 files.
- The [PE import audit](pe-imports.json) covers all 36 packaged PE files. All
  non-system static imports are supplied app-locally. Windows API sets and
  Windows 11 system DLLs remain OS dependencies. This is an import-table check,
  not proof of every optional dynamic-loading path or a clean-machine run.
- The [native no-model result](native-no-model.json) uses a fresh deployment
  outside the checkout, a System32-only PATH, isolated data/instance identity
  and an owned process job. FFmpeg, FFprobe and the embedding executable start;
  the actual daemon/extraction worker index generated text and a two-page PDF
  and return public exact results. Deferred admission starts closed, then
  accepts the explicit generated root. The job finishes empty.

The native helper uses the existing raw protocol fixture. PID-bound typed
connections retain their separate prior evidence; this package run does not
replace Lumen's required owned-process authentication and native lifetime tests.

## Focused checks

| Check | Evidence |
| --- | --- |
| Python assembly tests | 11 pass, zero failures/skips |
| Lumen verifier/staging tests | 21 pass, zero failures |
| Focused staging-script ESLint | Pass, zero warnings |
| Real assembly and Lumen fixed-path staging | Pass; independent manifest hash matches |
| Lumen explicit release verification | Pass against the staged development payload |
| Lumen `cargo check --locked --all-features` | Pass after preserving incomplete adapter drafts; zero warnings |
| Tauri debug resource copy | All 62 files re-verified against the independent manifest hash |
| Outside-checkout no-model smoke | Pass; six recorded checks and empty owned job |

Commands:

```powershell
python tools/test_engine_payload.py -v
python tools/test-engine-payload-native.py --payload <assembled-package> --manifest-sha256 6865b4bf80fd97b761d71affe186104ec6c2e150b942ab5532c12da9a1ec7fc6 --output <report.json>
# In the isolated Lumen checkout:
bun test scripts/stage-winsearch.test.mjs
bun x eslint scripts/stage-winsearch.mjs scripts/stage-winsearch.test.mjs scripts/lib/winsearch-payload.mjs --max-warnings 0
bun run stage:winsearch --source <assembled-package>
bun scripts/stage-winsearch.mjs verify-build --release
```

The initial assembly stub failed four rejection tests. Additional REDs covered
unimplemented archive/staging operations, missing empty-development resource
behavior and a reparse parent accepted when no engine directory existed.
Actual assembly also exposed CRLF differences in the
llama.cpp license and a Windows directory-rename lock; the license now comes
from its exact pinned Git object and publication retries only bounded Windows
access/share failures. Existing output directories are never overwritten.

The package contains real binaries; script unit tests use explicitly synthetic
file bytes. No model, database, binary, developer path or application inventory
is committed. The package is local development output, not an uploaded release.

## Remaining gates

Finish Lumen's owned native runtime and startup byte validation, streaming and
cancellation, transactional user-data migration, one production index lifecycle,
root/source policies and retained preview/action/answer safety. Then execute
the complete unchanged Lumen native/UI/performance and installed-consumer gates.
Final release binaries, supported GPU calibration, redistribution review,
signing, clean install/upgrade/uninstall and updates remain required. The
[package guide](../../engine-payload.md) and [README](../../../README.md#v10-todo)
retain the implementation/acceptance distinction.
