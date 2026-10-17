The original `names.json` and its gates are retained unchanged. `retrieval-development.json` and `retrieval-held-out.json` were frozen before selecting the application recipe or semantic cutoffs. All use Recall@5 >= 0.90, MRR >= 0.85 and negative false-positive rate == 0.

Use development scores for calibration. Open held-out scores only after recording the chosen recipe and cutoffs; a failed held-out gate remains a failure. Do not change expected answers to make a candidate pass. Production admission depends on segment kind and cosine only.

The added suites include multilingual names, applications, generic filenames with document/OCR/PDF-only terms, absent exact identifiers, unrelated and near-topic negatives, actual generated image/audio/video-moment inputs, and code definitions isolated to code mode. Media generation text describes synthetic fixtures; it must never substitute for audio/image embeddings. CLI segment fixtures assess retrieval; parser/extractor, public-pipe, policy/cancellation and full video timestamp tests are separate native gates.

The added segment/media/code schema is frozen input for the ongoing retrieval task. Its CLI integration is still pending; the current names evaluator rejects unsupported fields instead of silently ignoring them. Development diagnostics used the actual worker directly and do not constitute a pass of the complete added suites.
