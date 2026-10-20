The original `names.json` and its gates are retained unchanged. `retrieval-development.json` and `retrieval-held-out.json` were frozen before selecting the application recipe or semantic cutoffs. All use Recall@5 >= 0.90, MRR >= 0.85 and negative false-positive rate == 0.

Use development scores for calibration. Open held-out scores only after recording the chosen recipe and cutoffs; a failed held-out gate remains a failure. Do not change expected answers to make a candidate pass. Production admission depends on segment kind and cosine only.

The added suites include multilingual names, applications, generic filenames with document/OCR/PDF-only terms, absent exact identifiers, unrelated and near-topic negatives, actual generated image/audio/video-moment inputs, and code definitions isolated to code mode. Media generation text describes synthetic fixtures; it must never substitute for audio/image embeddings. CLI segment fixtures assess retrieval; parser/extractor, public-pipe, policy/cancellation and full video timestamp tests are separate native gates.

The CLI supports declared document/OCR/PDF chunks, real image/audio assets, timed video frames/soundtracks, and a separate code fixture store. It rejects unknown schema fields, invalid source locations and mixed code/ordinary entries. Code mode uses the shared code-store search functions and code recipe; code fixture snippets do not establish native parsing or opt-in scheduler acceptance. Results retain matching pages/times/lines, and the report prints category counts and Recall/MRR/FP, with `n/a` when that category has no positive or negative queries. The frozen suite thresholds apply to the overall suite; category misses remain visible.

Generate the synthetic assets with `python tools/generate-retrieval-fixtures.py` before media evaluation. With `WS_EMBED_TEST_MODEL` set to a valid pinned text model and its sibling projector installed, `winsearch-cli eval retrieval-development` uses the actual worker for every media embedding. Without a real media-capable worker the added suite fails explicitly; generation text is never an embedding fallback. `winsearch-cli eval names` retains its labeled deterministic smoke mode when the variable is unset. A missing explicitly selected model, stub worker or malformed real reply is an error.

The genuine CPU development run records Recall@5/MRR 0.938 and FP0 at the unchanged gates; the application paraphrase “play a movie” is an explicit miss (application category Recall/MRR0). All other development positives pass. This is fixture-index retrieval evidence with actual worker vectors, not a pass of native extraction, daemon/CLI parity, held-out, installed-app or full-v1 acceptance.

The frozen held-out run with the same executable and unchanged recipe records Recall@5 0.938, MRR 0.906 and FP0. Its application query “edit a sound recording” misses the expected app, and the video soundtrack query ranks second. Other positive categories pass at rank one. These results pass the declared overall gates while preserving the category failures; final reviewed-source/native/installed acceptance remains separate.

On Windows, `python tools/test-retrieval.py retrieval-development artifacts/retrieval-dev-run`
runs the built debug CLI and actual CPU worker under an owned Windows job.
Choose `names` or `retrieval-held-out` for the other frozen suites and a new
output directory for each run. The harness verifies fixture hashes, printed
recipe version/admission floor, and non-stub worker provenance; it records
CLI/worker/model/projector/generated-asset hashes, misses, metrics and descendant
cleanup under `artifacts/`.
It uses the existing pinned GGUF/projector in `artifacts/test-models`; this is
separate from the Windows NPU model availability check.

Fixture locations must apply to their kind: doc/code use lines, PDF uses pages,
audio/video use paired source times, images have no location, and OCR may use
one page, line or time range. Video/OCR frame point timestamps are valid.
Declared line spans are bounded by the fixture's 2 MiB source-line envelope.

`code-development.json` and `code-held-out.json` add 16 generated source snippets
in Rust, Python, TypeScript, C#, Go, Java, SQL and PowerShell. Each split contains
eight natural-language positives and eight unrelated negatives, with the same
.90/.85/zero-FP gates. Labels were frozen before model measurement; the chosen
two-task corroboration policy was recorded before evaluating held-out queries.
It retains the common .65 floor for each query view and the original code-task
ranking score. The code-only baseline's prompt affinity admitted every negative
in these sets. This supplements the existing suites and changes none of their
labels or thresholds. Run either suite through `tools/test-retrieval.py` using
its suite name and a fresh output directory. These are source-text fixtures,
separate from native parser/model-manager/public-pipe acceptance.
