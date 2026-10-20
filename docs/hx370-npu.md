# EmbeddingGemma 2 on Windows / HX 370

## Current integration (2026-10-20)

The IRON bundle
[`brishen/iron-embeddinggemma2-npu2`](https://huggingface.co/brishen/iron-embeddinggemma2-npu2)
(revision `052b6a44`) is now the **primary text/image model** whenever an AMD
NPU2 is present. The llama.cpp GGUFs serve as the GPU/CPU fallback and handle
every audio job.

| Step | Where | Behavior |
| --- | --- | --- |
| Detect | `ws-platform::npu` | Present PnP PCI IDs: `VEN_1022&DEV_17F0` is NPU2 (Strix Point/Halo, Krackan). `DEV_1502` is NPU1 (Phoenix/Hawk Point) and cannot run the kernels. |
| Download | `ws-modelmgr` (`NPU_BUNDLE_FILES`, 142 files, 1.09 GB, size + SHA-256 pinned) | With an NPU2, the bundle downloads first into `<model dir>\iron-embeddinggemma2-npu2\`, then the GGUFs. `Ready` needs both. Import accepts a flat `hf download` directory. |
| Load | `ws-embed::NpuRuntime` over `taconite-embeddinggemma2` | XRT (`xrt_coreutil.dll` from the AMD driver) and the delay-loaded `iron_xrt_shim.dll` are loaded explicitly, then the bundle loads and runs a warm-up text. |
| Serve | `winsearch-embed` | With `auto`/`npu`: text and image jobs go to the NPU. Audio, and any job once the NPU is unavailable, goes to llama.cpp (CUDA > Vulkan > CPU). |
| Fall back | `winsearch-embed` | Missing NPU, missing bundle, a build without the XRT SDK, a load failure or a device failure retires the NPU for the worker's lifetime. The reason appears in `worker_status.npu`. A rejected input falls back for that job only. A text batch that runs past 45 s on the NPU finishes on GPU/CPU (`backend: "npu+cpu"`). |

The vectors stay in the existing index space. Both runtimes use the same
`google/embeddinggemma-2` weights and the same application inputs: recipe
prompts as raw text with `<bos>`/`<eos>`, a 512-token text cap and 70/140/280
image-token budgets. The model card reports cosine ≥0.999 against float32 for
the NPU bundle. `ModelCompat` is unchanged, so moving between NPU and GPU/CPU
does not trigger a re-embed.

Building with NPU support needs the XRT SDK (`xrt_windows_sdk.zip` from XRT's
releases, extracted to `C:\Xilinx\XRT` or pointed to by `XRT_ROOT`). Without
it, the build links a stub and always uses GPU/CPU. See
`src/engine/third_party/taconite.md`.

### Hardware acceptance still required

None of these steps has run on HX 370 hardware yet:

1. Build the Windows shim against the XRT SDK and load the bundle (`npu: "ready"`).
2. Embed the reference texts and images in `ws-embed/tests/fixtures`. Check
   cosine against the llama.cpp vectors and cross-backend retrieval parity
   with the frozen retrieval suites.
3. Measure latency, memory, idle exit and cancellation, plus fallback after
   driver removal and while another process holds the NPU's hardware contexts.
4. Add a verified `iron_xrt_shim.dll` pin to the engine payload assembler.

Until these steps pass, NPU results are not release evidence. The fallback
keeps the measured CPU/GPU behavior.

## Availability assessment (2026-10-17, kept for history)

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
