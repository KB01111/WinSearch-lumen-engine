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
  loads `xrt_coreutil.dll` (`%XILINX_XRT%`, `%SystemRoot%\System32\AMD`,
  then PATH) and the shim explicitly. `winsearch-embed` and the `ws-embed`
  test binaries delay-load `iron_xrt_shim.dll`, so a PC without the AMD NPU
  driver still starts and falls back to GPU/CPU.
- Removed: the `gpu` feature/module (Vulkan through `ash`/`naga`), upstream's
  static-XRT link mode, and the `direct` feature. The `direct` amdxdna ioctl
  path is Linux-only and is not enabled.

## Status

The Windows shim build and actual NPU inference have **not** been verified on
hardware yet. The acceptance steps are in `docs/hx370-npu.md`. Engine payload
assembly (`tools/assemble-engine-payload.py`) does not yet ship
`iron_xrt_shim.dll`. Until a verified pin adds it, packaged engines use the
GPU/CPU path.
