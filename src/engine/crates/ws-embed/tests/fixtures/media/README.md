# Independent media references

Public synthetic inputs and vectors from ARahim3/DigUp at
`ec28973268b81f836be72187ff23b3a16ef4fd97`, MIT (LICENSE-DigUp.txt).
Original paths: `Tests/LlamaRuntimeTests/Reference/{screen.png,photo.jpg,speech.wav}`
and `Tests/LlamaRuntimeTests/Reference/vectors.json` (renamed locally).
The vector JSON records `made_with: llama.cpp b11461`, `vector_version: 1`.
Inputs are synthetic application/landscape graphics and four seconds of formant
noise; `speech.wav` has no intelligible transcript and cannot establish spoken
word retrieval. Real spoken acceptance uses separately generated local OS voice
fixtures, never reference noise relabelled as speech content.

SHA-256:

- screen.png: a8a0c831fe4746d00c95e3ab436d53e4cc9046a774fe4f0f6edb73d1a7d55f9b
- photo.jpg: 243df6bca3dfce7888ba872d9a774b7d030f5f6d4c5f92bd11d0f3402c026133
- speech.wav: bfbc029af6c0bacdbaccfa6c695d6772373453249386fa6a62a7b02a31d33e1f
- reference-media-vectors.json: 0ec9336aa53fb0d5bafa074cd56122e2fd914cb73f080b4f98332872089f6dd8

`ImageSizing.swift` from `Sources/LlamaRuntime/` at the same snapshot is the
source for the attributed Pillow-compatible Rust resampler. No model weights
are committed. CPU acceptance runs `cargo test -p ws-embed --test media --
--ignored --nocapture --test-threads=1` with WS_EMBED_TEST_MODEL and its sibling
mmproj; the cosine gate remains >=0.999. `tools/test-media-runtime.py` additionally
checks the actual worker protocol, budgets, projector resolution and real
image/spoken retrieval. See its required arguments; generated inputs/results
belong in ignored artifacts directories.
