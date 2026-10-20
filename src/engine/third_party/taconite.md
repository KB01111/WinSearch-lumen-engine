# taconite / taconite-embeddinggemma2 (AMD NPU runtime)

The AMD NPU path runs the IRON export of EmbeddingGemma 2
([`brishen/iron-embeddinggemma2-npu2`](https://huggingface.co/brishen/iron-embeddinggemma2-npu2),
pinned in `crates/ws-modelmgr/src/manifest.rs`) through these crates:

| Crate | Version | Source | Licence |
| --- | --- | --- | --- |
| `taconite-embeddinggemma2` | 0.2.0 | crates.io (checksum `2b9abd0596b3674612bd545b04faf1f0fd6094b8fcc83488bcfcdd139d36b76e`) | Apache-2.0 AND BSD-3-Clause AND MIT-CMU |
| `taconite-bundle` | 0.1.0 | crates.io | Apache-2.0 |
| `taconite` | 0.1.1 | **vendored** at `third_party/taconite`, applied with `[patch.crates-io]` | Apache-2.0 |

Upstream repository: https://github.com/Brishen/taconite.

## Why `taconite` is vendored

Upstream `taconite`'s build script compiles its XRT shim with `g++`/`ar`
against Linux XRT, so it cannot build on Windows. The vendored copy keeps
upstream's Rust and C++ shim sources and changes only:

- `build.rs`, rewritten. On MSVC with an XRT SDK (`XRT_ROOT`, default
  `C:\Xilinx\XRT`, from XRT's `xrt_windows_sdk.zip`), it builds the shim as
  `iron_xrt_shim.dll` (exports in `shim/iron_xrt_shim.def`, functions only)
  and stages it next to the profile's binaries. On Linux with XRT it builds
  a static shim linked to `libxrt_coreutil.so`. With no SDK, or with
  `TACONITE_XRT_STUB=1`, it links `shim/iron_xrt_stub.c`, where every call
  fails cleanly. `XRT_LINKED` reports which build you have.
- `src/winload.rs` (new). Before the first shim call, `Session::open`
  loads `xrt_coreutil.dll` from the Windows system directory (or its AMD
  directory) and the shim from the executable directory explicitly. PATH,
  environment overrides and the current directory cannot supply these DLLs.
  `winsearch-embed` and the `ws-embed`
  test binaries delay-load `iron_xrt_shim.dll`, so a PC without the AMD NPU
  driver still starts and falls back to GPU/CPU.
- Removed: the `gpu` feature/module (Vulkan through `ash`/`naga`), upstream's
  static-XRT link mode, and the `direct` feature. The `direct` amdxdna ioctl
  path is Linux-only and is not enabled.

## Windows build and qualification

`xrt-sdk.json` pins AMD's native Windows guide SDK: release 2.21.75,
70,834,080 bytes, SHA-256
`ccc244c2c423588972ade76142cdc01049477aaa39a35be97e782b97eb7c5295`.
Verify the archive before extraction, and set `XRT_ROOT` to its `xrt_sdk/xrt`
directory. Build `winsearch-embed`, `winsearchd`, `winsearch-extract` and the CLI
with this SDK. `/Zc:__cplusplus` is required with MSVC's C++17 switch for XRT's
headers. A locked stale shim fails staging instead of silently surviving it.
The installed driver is a system prerequisite; its DLLs and model weights
are excluded from the payload. SDK/header license: `licenses/XRT-LICENSE`.

The shim bounds XRT waits in 100 ms slices and carries a job deadline. XRT can
mask a timed wait while another command completes, so the worker also has an
independent 45-second process watchdog covering initialization and execution.
Device errors retire the process before native buffer teardown; the supervisor
retries once through fallback and quarantines NPU until deliberate model/backend
reconfiguration or restart. The overall supervisor deadline remains 120 seconds.

Real Windows/HX370 execution is recorded in `docs/validation/2026-10-20-npu-completion/`.
Qualified application routes are text and 140/280-token images. 70-token images
retain CPU/GPU execution because two pinned comparisons failed the unchanged
.999 gate. The public recipe remains 768-dimensional normalized vectors,
512 text tokens and pipeline version 2. The assembler requires real-shim
runtime information, matching binary/source hashes and reference/retrieval
qualification before an NPU payload can be published locally.
