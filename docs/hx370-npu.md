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

Official platform checks reach the same limited conclusion:

- [AMD's Ryzen AI 1.8 model list](https://ryzenai.docs.amd.com/en/latest/llm_list.html)
  lists `embeddinggemma-300m`, not EmbeddingGemma 2. Its generative Gemma models
  do not establish compatibility with this embedding architecture.
- [AMD's Windows ML model support](https://ryzenai.docs.amd.com/projects/WinML/en/latest/model_support.html)
  describes ONNX float/quantized Transformer support and prepared LLMs.
  [Microsoft's Windows ML overview](https://learn.microsoft.com/en-us/windows/ai/new-windows-ml/overview)
  describes execution-provider management; neither provides an EG2 package.
- [ONNX Runtime's Vitis AI provider](https://onnxruntime.ai/docs/execution-providers/Vitis-AI-ExecutionProvider.html)
  supports Ryzen AI on Windows. [AMD's deployment guide](https://ryzenai.docs.amd.com/en/latest/modelrun.html)
  requires a compatible ONNX graph and can partition unsupported operations onto
  CPU. Its operator-assignment report is needed to establish actual NPU placement.
- [llama.cpp's backend list](https://github.com/ggml-org/llama.cpp#supported-backends)
  includes CPU and GPU routes; it does not list an AMD XDNA NPU backend.
  [Google's EG2 card](https://huggingface.co/google/embeddinggemma-2) does not
  establish a Windows AMD NPU export or validated execution path.

| Evidence stage | Windows/HX370 EG2 result |
| --- | --- |
| Model in supported AMD catalogue | Older 300M listed; EG2 not found in the checked list |
| Compatible downloadable artifact/runtime | Linux bundles found; ready Windows pair not verified |
| Model/runtime initialization | XRT detects the NPU; no EG2 model loaded on it |
| Actual NPU operators executed | Not demonstrated |
| Correct embeddings/reference agreement | Not demonstrated on NPU; CPU evidence is separate |
| Repeated latency/memory/cancellation acceptance | Not demonstrated on NPU |

This is an availability assessment, not proof that a custom Windows port is
impossible. NPU acceleration remains unavailable for production selection until
all execution stages pass. It is optional and does not block v1.0.
