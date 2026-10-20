# Task 2 implementation report

Status: DONE. Initial implementation selected-suite verification passed at `c142d96`; the subsequent review fix and its scoped verification are recorded below. Scope is durable existing text/image extraction, exact match locations, and deferred vectors. Native PDF pages, audio/video, and frontend explorer are separate tasks.

## Changes

- Added `Extractor::extract_batch(path, budget, cursor)` while preserving Task 1's `ExtractRequest`/`ExtractResponse` framing and owned process/client implementation. Production passes the durable cursor to the actual sibling worker. Each response contains at most 30 chunks; complete text parsing reconstructs the requested window within the original byte/archive/deadline bounds. The old 256-draft truncation cannot silently complete a long text document. The worker supplies a truthful total and continuation, with document-specific cursor/returned-chunk validation.
- Preserved the existing 1200-character target and approximately 15% overlap. Document chunking now receives original text so leading whitespace does not remove line offsets. FTS-highlighted token positions produce the relevant match line, including normalized FTS tokens.
- Kept legacy IFilter decoded text bounded when collecting beyond the old draft cap: its character limit is also capped by the existing byte budget, decoded UTF-8 must fit that budget, and a cap-hit result is explicitly incomplete rather than silently complete. This guard does not implement native PDF pages.
- Exact chunks and locations commit before optional embeddings. The cursor, completion flag, FTS/chunk rows, thumbnail reference, and extraction claim release commit in one SQLite transaction. Replays cannot duplicate chunks. Continuations use priority `-1000000000000`, behind fresh/short files. Failure during a later batch retains previously committed exact content.
- Added a durable source revision independent of the existing whole-second entry mtime. Actual Modified events always invalidate and enqueue supported content even for equal-size/equal-second edits. Precise source snapshots and current revisions are checked after parsing, during native cancellation polling, and before exact/vector commits. Renames invalidate source state; obsolete modality jobs complete without clearing a newer modality's content. Entry deletion cascades content/progress/revision state.
- Embedding is a separate claimed durable stage using the existing supervisor. Production preserves typed `EmbedError::ModelNotAvailable`/`Unsupported` distinctions rather than parsing error strings. Model unavailable releases content-vector claims without attempts; genuine failures retain the existing three-attempt cap without deleting exact content. Empty/zero/nonfinite vectors cannot enter segments through this path. Model-space reset increments the durable generation, rejects in-flight old-space vectors, and requeues vectors on the same exact chunk IDs without reparsing. Recovery uses the existing extraction workers' bounded work wait, including restart; it needs no new service.
- Retained Task 1 `AdmissionUnavailable`, no-attempt claim release, 100 ms admission backoff, native source/policy checks, and retained process/pipe/artifact ownership. Exact persistence does not release newly promoted artifact ownership on stale policy/source state. Synthetic image visual vectors use a separate negative chunk ordinal and do not enter FTS or advance the extraction cursor.
- Added default-compatible typed `SearchResult.excerpt` and `matching_moments` in Rust/C#, using the existing `MatchLocation` union. The primary result remains grouped by file. Matching metadata joins its own FTS row identity, never an unrelated segment from that file. The primary path has at most 32 matching moments, each with a 240-character excerpt; later previews can page all persisted matching chunks through the store API.

## Storage and APIs for later PDF/media tasks

The database remains additive schema version `2`, using the idempotent migration marker `meta.content_batches_v1`:

| Storage | Meaning |
| --- | --- |
| `content_revisions(entry_id PRIMARY KEY, revision)` | Durable source/job invalidation generation; FK cascades on entry deletion. |
| `content_progress(entry_id, kind PRIMARY KEY, cursor, complete, revision, source, model_generation)` | Source-bound extraction progress. `source` records path, byte length, precise filesystem mtime, and file identity. |
| `content_chunks(id AUTOINCREMENT PRIMARY KEY, entry_id, job_kind, kind, chunk_index, text, page, start_ms, end_ms, line, thumbnail_path, embed, visual, embed_state, attempts)` | Exact drafts and durable optional-vector state; unique `(entry_id, job_kind, chunk_index)`. No vector is required. |
| `content_fts.rowid = content_chunks.id` | Exact FTS row identity for matching text/location joins. Chunk deletion removes its FTS row. |
| `meta.content_model_generation` | Embedding-space invalidation generation. |

Public index-local value types: `ContentProgress`, `ContentChunk`, serializable `ContentMatch`, and `SearchHit.matches` in `ws_index::model`. The `ContentMatch` carries excerpt/page/time/line/thumbnail. Existing `segments` remain vector-only.

Public `IndexStore` methods:

- `content_revision(entry)` / `invalidate_content_source(entry)`.
- `begin_content_batch(entry, kind, source)` / `content_progress(entry, kind)` / `commit_content_batch(job, progress, chunks, next_cursor)`.
- `list_content_chunks(entry)` for all durable raw drafts, including numeric exact-only rows. Synthetic image visual records have `visual = true`, `chunk_index = -1` and no FTS row.
- `claim_content_vectors(preferred_entry)` / `release_content_vectors(chunks, failed)` / `commit_content_vectors(chunks, vectors, revision, generation)`. Claims are reset on database reopen. Optional unsupported visual records use `complete_optional_visual`.
- `model_generation()` / `reset_content_vectors()`; the existing `Indexer::purge_model_vectors_and_requeue` integrates the model service's incompatible-model path without reparsing completed content.
- `content_matches(entry, query_tokens)` returns the first 32 matching chunks; `content_matches_page(entry, query_tokens, offset, limit)` retrieves subsequent moments, capped at 32 per page, under current policy visibility.

Cursor units in this implementation are chunk ordinals. `ExtractResponse.next_cursor = None` completes extraction. Document validation checks forward progress agrees with returned chunk count and known totals. PDF page units, multiple drafts/page, PDF rendered visuals/OCR, phased video cursors, and modality-specific allowed-kind/response bounds must be implemented by their dedicated tasks; this report does not claim those semantics already work. Location storage already accepts page and integer millisecond metadata.

Legacy migration preserves entries and every existing searchable FTS row, including numeric rows that never had vectors. An old location is copied only from a segment with the same entry and exact text; combined legacy FTS with no equal segment honestly retains absent location. The migration is transactional and idempotent. Existing v1/pre-priority migration assertions remain intact.

## Red/green evidence

Shell commands were RTK-prefixed. Commands below ran from `src/engine` unless otherwise indicated. Fixtures use isolated generated roots and databases. Final selected suite uses `WINSEARCH_DATA_DIR=<worktree>/src/engine/target/task2-tests/data` and `WINSEARCH_INSTANCE_ID=task2-durable-batches-tests`.

1. Before production changes, `rtk cargo test -p winsearchd task2_ --bin winsearchd -- --test-threads=1` produced runtime **0 passed, 4 failed, 124 filtered out**:
   - no model: `model failure discarded exact content`, actual hits `0`, expected `1`;
   - first batch: actual chunks `75`, expected `30`;
   - real production Modified handler: `same metadata Modified never queued replacement`;
   - in-flight Modified: `old batch committed over actual Modified event`.
   GREEN after the implementation: **4 passed**, 124 filtered out. The no-model test also asserts actual excerpt/line 2 and no fabricated vector result. A later engine-level test verifies the public primary excerpt/location/moment with a model-error callback.
2. Native watcher runtime RED was established by temporarily restoring the original Modified branch, running `rtk cargo test -p winsearchd task2_native_modified_same_second_and_inflight --bin winsearchd -- --test-threads=1`, then restoring the fix in a `finally` block. Actual `ReadDirectoryChangesW` -> Modified -> production handler produced **0 passed, 1 failed, 130 filtered out**, `assertion failed: step.claimed`. GREEN runs cover both resting and in-flight edits without advancing the persisted timestamp second. The earlier deterministic tests directly deliver the real event type to the production handler; they are handler fixtures, distinct from this native notification evidence. Ignored helper/log: `target/task2_native_red.py`, `target/task2-native-watcher-red.log`.
3. `rtk cargo test -p ws-extract text_batches_reject_empty_progress_and_false_completion --lib`: runtime RED **0 passed, 1 failed, 25 filtered out**, accepting a false complete total; GREEN **1 passed**, 25 filtered out after document cursor validation.
4. Full selected run initially stopped at the daemon with **131 passed, 1 failed**. Existing `revoked_inflight_name_and_content_jobs_cannot_commit` found a discarded batch reporting FTS count `1` instead of `0`. The count now reflects retained content; the unchanged regression then passed **1 test**, 131 filtered out. No policy assertion was relaxed.
5. Final image cursor runtime RED: `rtk cargo test -p winsearchd image_exif_ocr_screenshot_semantic_and_thumbnail_end_to_end --bin winsearchd` produced **0 passed, 1 failed, 132 filtered out**, terminal cursor `2` instead of actual extraction units `1`. Synthetic visual-vector records now neither enter FTS nor count as extraction units. GREEN: `rtk cargo test -p winsearchd extract --bin winsearchd -- --test-threads=1` passed **28 tests**, 105 filtered out; `rtk cargo test -p ws-index -- --test-threads=1` passed **74 tests**.
6. Runtime atomicity RED used the final rollback regression with the batch-content transaction deliberately committed before the cursor transaction (temporary mutation, restored in `finally`). `rtk cargo test -p ws-index task2_content_cursor_and_text_roll_back_together_and_reject_replay --lib` failed **0 passed, 1 failed, 73 filtered out**: actual retained chunks `60`, expected `30` after interruption. The real single-transaction implementation is restored; final full-suite GREEN includes this regression. Ignored helper/log: `target/task2_atomic_red.py`, `target/task2-atomic-red.log`.
7. Runtime native restart RED rebuilt the actual extraction binary from its Task 1 `main.rs` while preserving the new daemon/indexer test, then restored and rebuilt the new worker in `finally`. `rtk cargo test -p winsearchd task2_native_worker_seventy_five_chunks_resume_after_restart --bin winsearchd -- --test-threads=1` failed **0 passed, 1 failed, 132 filtered out**: restart yielded `0` chunks instead of the next `30` because the old worker prematurely completed the capped file. Final full-suite GREEN uses the rebuilt new actual worker. Ignored helper/log: `target/task2_restart_red.py`, `target/task2-restart-red.log`.
8. `rtk cargo test -p ws-extract task2_ifilter --lib`: runtime RED **0 passed, 2 failed, 26 filtered out**. The unbounded draft collection gave IFilter character limit `18446744073709551615` instead of `33554432`, and a cap-hit output was accepted as complete. GREEN **2 passed**, 26 filtered out after preserving the decode bound and explicitly rejecting incomplete output. These are deterministic decoded-text boundary fixtures, not a native PDF/IFilter quality claim.

Additional green behavior in the selected suite:

- Actual rebuilt native extraction worker: generated exactly-75-chunk document commits 30 promptly, survives indexer/database reopen, resumes at 30, finishes exactly 75 ordinals `0..74`, preserves first-30 IDs, and lets a newly discovered short file precede continuation.
- Actual rebuilt native extraction worker: generated text beyond 256 chunks reaches `finalsentinelafterlegacycap` through bounded continuations; exact search finds the final excerpt. No silent legacy-cap completion.
- SQL interrupted-commit trigger aborts while updating the cursor after chunk/FTS writes: cursor remains 30, exactly 30 original chunks remain, second-batch token is absent, and the original claim stays held. Retrying after removing the trigger succeeds; replay returns false. Source invalidation rejects stale drafts.
- Distinct tokens select their own page 2/page 9/time 42000..48000 locations. Repeated moments remain available, including paged retrieval. Numeric 98017 has no pending vector. More than 500 revoked content candidates cannot crowd the permitted exact hit out of limit 1.
- Real supervisor/named-pipe path with **injected deterministic test worker vectors**: five typed model-unavailable steps leave attempts at zero and exact content usable; changing to a serving test worker adds vectors to the same chunk IDs with parser count exactly 1. Incompatible-space reset preserves exact content and re-embeds without reparsing. This is supervisor/transport/stage evidence, not model inference quality evidence.
- Repeated invalid empty/zero vector attempts preserve exact text and stop at three attempts. A generation change rejects in-flight vectors; current generation commits successfully. Existing deletion/rename/consent/pause/admission/artifact regressions remain enforced.

There were also development-only compile/setup failures (test handler visibility/import fixes, new additive fields missing from one old Rust literal, a selective formatting import reorder, PowerShell stderr redirection, a Python encoding/newline issue in the temporary native-red helper, and one overlapping Windows test executable linker lock). These were corrected; none is represented as a behavioral RED or a passing gate. No test was disabled or ignored. The old native assertions that nonzero document cursors were unsupported and capped totals unknown were replaced with stronger continuation/known-total assertions because those Task 1 limitations are precisely removed here. The small-file scheduling test now explicitly drains 30-chunk continuations before its original zero-backlog assertion.

## Initial implementation verification (`c142d96`)

- `rtk cargo build -p winsearch-extract -p winsearchd`: passed before native batch tests; seven preexisting daemon dead-code warning groups remain. No blanket warning suppression or warning-only scope added.
- `rtk proxy powershell -NoProfile -File target/task2-checks.ps1` runs the build and isolated selected suite: `cargo test -p ws-protocol -p ws-platform -p ws-index -p ws-extract -p ws-embed-protocol -p winsearchd -p winsearch-extract -- --test-threads=1`.
- Selected full suite on the final source: **266 passed, 0 failed, 0 ignored** (daemon 133 in 60.88 seconds; index 74; extract 28; embed protocol 4; platform 15; protocol 11 plus one integration test). This includes the final image-unit correction and IFilter decode guard, plus restored single-transaction/native continuation implementations after their runtime RED probes. Final raw output is in ignored `src/engine/target/task2-tests/selected-suite.log`.
- From repository root, `rtk dotnet test src/app/WinSearch.Tests/WinSearch.Tests.csproj --nologo`: **17 passed**, 0 warnings.
- `rtk dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo`: **3 projects built**, 0 errors, 0 warnings.
- `rtk git diff --check`: passed. New/changed Rust blocks were formatted without reformatting unrelated baseline source. The final report does not claim the entire preexisting workspace passes a new whole-workspace formatting gate.

## Self-review and limits

Reviewed transaction rollback/replay behavior, FK/FTS cleanup and legacy numeric migration, source revision vs whole-second recency, precise source/policy checks, stale modality jobs, generation isolation, vector claim lifecycle/retries, no-model exact availability, independent artifact ownership, actual native batching beyond 256, matching-row metadata identity, policy visibility before caps, primary response bounds, Rust/C# defaults, and unchanged Task 1 retained cleanup/admission guarantees.

Generated fixtures and injected vectors are explicitly identified above. Existing real-binary embedding transport tests reuse the unchanged ignored `winsearch-embed.exe`/DLLs and their existing stub modes; no embedding inference binary was rebuilt and no new live-model inference claim is made. No normal personal-content daemon or UI was launched. No personal content was read, no download was needed, and no provider data was sent.

Current document continuation reconstructs and rechunks bounded source text for each batch rather than saving parser-internal state. The durable chunk cursor prevents duplicate commits and full reparsing for deferred embeddings, but does not make long-document parsing itself streaming. Exact legacy rows with no authoritative location retain a missing location. The primary result carries the first 32 matching moments; the remaining persisted moments require the index-local paged API. Dedicated PDF/native audio/video, explorer consumers, packaging, real model quality/latency and other tasks retain their own acceptance gates.

## Review fix round 1

Addressed Important I1 from `task-2-review.md` against fix base `c142d96cfb3360f54edebe54cb156713f7e202bd`. The review correctly identified that FTS `unicode61 remove_diacritics 2` can match `cafe` against late source `café`, while the literal query-based excerpt selector falls back to the opening text. FTS normalization, ranking, thresholds, and policy filtering are unchanged.

- Added optional, default-compatible index-local `ContentMatch.match_byte`, computed from the first FTS-highlighted token in original UTF-8 text. The same original-text prefix determines the actual match line. If source text contains the normal highlight marker, the query uses an absent marker before deriving that offset; review round 2 below corrects that marker's boundary construction. This is query metadata rather than persisted schema or new public IPC content.
- Added `ws_extract::select_excerpt_at` and reused the existing excerpt renderer for bounded 240-character excerpts and whitespace collapse. The authoritative original byte offset maps through that collapse without searching the query again. Both the primary excerpt and every `matching_moments` excerpt use this common mapping. Existing literal query-based selection remains available for callers without an authoritative anchor.
- The public engine regression uses a generated file with the match near character 1000, unrelated `decafélate` substrings in earlier padding, multibyte leading text, literal highlight-marker data, composed `café` and decomposed `cafe\u{301}` variants, and matching moments on actual lines 2 and 4. It asserts actual matching source text in the primary excerpt and all moments, the relevant line, shared primary/first-moment metadata, and the 240-character bound. A focused selector regression verifies original byte anchoring through collapsed Unicode/ASCII whitespace, plus a zero-length excerpt budget.

Runtime RED, before the fix: `rtk cargo test -p winsearchd task2_normalized_fts_excerpts_anchor_primary_and_all_moments --bin winsearchd -- --test-threads=1` produced **0 passed, 1 failed, 133 filtered out**. Assertion: `primary excerpt omitted normalized match`, with the returned excerpt consisting of opening `decafélate intro` padding. Runtime GREEN after the fix: the same command passed **1 test**, 133 filtered out, in 0.07 seconds.

Focused covering verification on the amended source:

- `rtk cargo test -p ws-index -p ws-extract --lib -- --test-threads=1`: **103 passed, 0 failed, 0 ignored** (index 74, extract 29), 1.25 seconds.
- `rtk cargo test -p winsearchd engine::tests --bin winsearchd -- --test-threads=1`: **27 passed, 0 failed, 0 ignored**, 107 filtered out, 0.36 seconds.
- `rtk git diff --check`: passed.

No unchanged broad/native worker/model suite or .NET/WinUI checks were repeated for this index-local excerpt correction; the earlier checks remain identified by their implementation revision above. No C# or wire schema changed. The seven baseline warning groups remain ledgered for final whole-v1 review as requested. Native PDF/media/model-quality and complete-v1 acceptance stay with their downstream tasks. No new scratch report or planning files were added to Git.

## Review fix round 2

Addressed Important N1 from `task-2-rereview-1.md` against fix base `6217d8e56330b5d9cab35b9243764ff45b7db294`. The original I1 normalization fix remains in place. An absent delimiter made only from repeated U+0001 could still overlap a literal source suffix and the newly inserted delimiter, so its first occurrence did not necessarily identify the actual highlight boundary.

The delimiter now starts with a single U+0001 and grows using only U+0002 until absent from the source. Its start character occurs nowhere else inside the delimiter: an earlier literal source suffix cannot complete a false delimiter across the inserted-start boundary, because that boundary presents U+0001 where a continuation requires U+0002. The ordinary no-literal-marker fast path and FTS matching/normalization, ranking, limits, typed locations, and shared primary/all-moment excerpt mapping are unchanged.

Added a generated public engine regression for the reported adjacent U+0001 run of length 600, a line-2 variant, repeated literal U+0001/U+0002 pairs, and a long literal U+0002 suffix following U+0001. It asserts the actual `café invoice` excerpt, actual line, primary/first-moment agreement and the 240-character bound. Existing composed/decomposed accent, multibyte padding, literal-marker, multiple-moment and whitespace-collapse assertions remain intact.

Runtime RED before the correction: `rtk cargo test -p winsearchd task2_adjacent_literal_marker_runs_do_not_shift_public_match_anchor --bin winsearchd -- --test-threads=1` produced **0 passed, 1 failed, 134 filtered out**, with `adjacent literal marker run displaced the excerpt` (0.04 seconds). Runtime GREEN after the correction: the same command passed **1 test**, 134 filtered out, in 0.15 seconds.

Focused covering verification on the amended source:

- `rtk cargo test -p ws-index --lib -- --test-threads=1`: **74 passed, 0 failed, 0 ignored**, 1.13 seconds.
- `rtk cargo test -p winsearchd engine::tests --bin winsearchd -- --test-threads=1`: **28 passed, 0 failed, 0 ignored**, 107 filtered out, 0.46 seconds, including both public normalization and adjacent-marker regressions.
- `rtk cargo test -p ws-extract anchored_excerpt_preserves_original_byte_position_through_whitespace_collapse --lib`: **1 passed, 0 failed, 0 ignored**, 28 filtered out, 0.00 seconds.
- `rtk git diff --check`: passed.

No unchanged broad/native/.NET/WinUI suites were rerun. No schema, public wire fields, dependency, threshold, or test gate changed. Baseline warnings and downstream v1/native modality/model acceptance retain their previously assigned scope. Only the existing tracked report was amended; no new scratch planning/report files were added to Git.
