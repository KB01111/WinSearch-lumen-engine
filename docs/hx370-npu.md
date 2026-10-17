# EmbeddingGemma 2 on Windows / HX 370

Checked 2026-10-17. The development laptop's AMD Ryzen AI 9 HX 370 has a healthy
NPU device. The installed AMD driver is32.0.20102.3930; native `xrt-smi examine`
reports XRT2.21.0, firmware1.1.2.64 and an accessible `NPU Strix` device.
Device discovery is not evidence of EmbeddingGemma 2 inference.

[FastFlowLM's Windows embedding documentation](https://fastflowlm.com/docs/models/embeddinggemma/)
lists the older `google/embeddinggemma-300m` model. That does not establish
EmbeddingGemma 2 support and cannot replace its shared text/image/audio space.

Neither of the two requested bundles currently documents a ready native Windows
model runtime:

| Bundle | Published support | Windows status |
| --- | --- | --- |
| [Halogen NPU EmbeddingGemma 2](https://huggingface.co/peonist-ai/halogen-npu-embeddinggemma-2) | Custom HNPW/ELF artifacts for Halogen's Linux Strix Halo container | No native Windows/HX370 execution established |
| [IRON EmbeddingGemma 2 NPU2](https://huggingface.co/brishen/iron-embeddinggemma2-npu2) | NPU2 including HX370; open Rust taconite text/image runtime | Runtime currently documents Linux x86-64 and amdxdna |

The IRON option is a possible port rather than a verified Windows package.
[Taconite's requirements](https://github.com/Brishen/taconite#requirements) specify
Linux; its build script uses Linux XRT link/build conventions, and the direct
backend uses Linux device ioctls. The EmbeddingGemma runtime documents text and
images, so it also does not establish the required audio path on Windows.

[AMD's native Windows IRON/XRT guide](https://github.com/Xilinx/mlir-aie/blob/main/docs/buildHostWinNative.md)
supports running compiled kernels entirely on Windows. This establishes a
potential platform route, not compatibility of either model bundle. A port would
need a Windows XRT build/loader, actual bundle execution, unchanged reference
cosines and multimodal retrieval tests, cancellation/resource ownership and
redistribution review before being called supported.

The final user request is to check availability and optimize WinSearch for this
laptop; it does not require developing that port. Keep the existing CPU/GPU
backends and measured CPU configuration while their native acceptance proceeds.
Do not label CPU results as NPU results or silently enable an unsupported bundle.
No drivers, firmware, power policy or system runtime were changed for this check.
