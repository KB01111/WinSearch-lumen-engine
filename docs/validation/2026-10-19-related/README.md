# Related search and exact-cache retention checkpoint

This is a local Windows checkpoint for the Related engine/client contract and a
reproduced cache-retention race. Tests/reference checks were run October18–19;
the final native profiles ran October19. It does not establish full v1.0,
installed Lumen/WinUI, hosted CI, scale or GPU acceptance.

## Resulting behavior

`search.related` is a distinct operation with a current bound ordinary/code
source reference. Rust and C# require the Related, semantic and terminal-outcome
capabilities, plus code/extension capabilities when applicable. Paths, query text
and vectors cannot be supplied as the seed. Returned seed entries, unbound or
wrong-domain references, wrong filters, disabled semantic fallback and excessive
results/frames fail validation. The .NET pending Related queue is bounded to two
frames; Rust retains its owned read lane and total request deadline.

The engine uses up to64KiB/64parts of current cached indexed text and its existing
inference runtime. It preserves Lumen's existing text-based Related behavior:
missing text/model or disabled policy fails explicitly, including audio without
indexed text. No parser, filename seed, implicit download or second model runtime
is introduced. Cached seed text can be partial; this is not a whole-source
embedding guarantee.

Ordinary candidates use content vectors; code stays isolated and corroborated.
The source is excluded before top-k/code candidate limits. Related has no exact
stage or exact-hit suppression and ranks admitted results by similarity while
retaining existing semantic admission thresholds. A healthy lookup with no
above-threshold hits is complete/ready; no eligible scoped vectors is explicitly
degraded. Inference/SQL failures never become ordinary search results.

Two owned Related workers keep cached lookup/inference outside the connection
read loop. Controls remain serviceable during a deliberately blocked store;
excess work is rejected. Request identity, cancellation, source/index/model
epochs and the bound seed are checked under the final writer, even for empty
results and failed inference. Obsolete work terminates superseded/canceled;
old work cannot terminate a reused request ID.

The native run also exposed empty exact results after root revoke/restore. A
deterministic regression then reproduced committed exact-cache deletion when
unavailable inference overlaps pause or root-policy changes. The fix retains
already committed cache when the source snapshot is unchanged and the model is
unavailable. Root revocation still hides results immediately; source changes and
successful embeddings interrupted by pause/root changes retain their existing
discard behavior. Existing source, media cleanup and identity assertions remain
unchanged. The new native assertion checks that cache revisions and actual byte
hashes survive the consumer's read-only policy lifecycle without reparsing.

Metadata/proof reads do not rehash source bytes. This change does not turn an
opaque metadata source proof into a content hash or remove the native host's
current grants and freshness responsibilities.

## Verification

| Check | Result |
| --- | --- |
| Rust workspace excluding ws-embed, all targets |603 pass, zero failures; one model-gated worker ignored in this invocation |
| ws-embed all targets with ignored cases included and pinned actual weights/projector |25 pass, zero failures/ignored |
| Explicit actual-model worker replay |1 pass, zero failures/ignored |
| Executed Rust tests across those disjoint scopes |629 |
| .NET suite |118 pass, zero failures/skips |
| x64 WinUI build |Pass, zero warnings/errors |
| Final coherent workspace binaries/examples |Pass; seven existing daemon dead-code warnings |

The default ignored worker case is not counted as a pass until its separate real
model replay. Repeated focused runs are not added to the629 total. The12 offline
example tests are included in the all-target workspace count. Earlier default
workspace/doctest invocations are separate and not added again.

Contract, race and crowded-candidate unit fixtures use fixed vectors/test hooks.
Native profiles use generated files through the actual owned daemon/parser
workers; only the CPU profile uses real model inference. These are separate
evidence classes and do not claim live-provider or installed UI acceptance.

Text reference drift remains1.32e-4 across10 cases. Image, audio, photo and frame
reference cosines are .999942/.999751/.999815/.999698, above the unchanged .999
floor. Actual CPU profiles use the pinned shared768 model/projector, with four
real768-dimensional code vectors and a ready ModelManager. The embedding worker
binary is unchanged between the retained failed native profile and final runs.
CUDA compilation does not establish GPU execution; these model runs use CPU.

| Final same-binary profile | Original public/offline queries | Whole harness | Initial indexing |
| --- | --- | --- | --- |
| No model |10 /7 |5.628s |2.417s |
| Actual managed CPU |21 /17 |87.910s |68.405s |

Both profiles retain the existing lifecycle, bound-reference, preview, source
hash, extension and terminal-state checks. Each adds two typed-client Related
sources; raw transport adds three no-model controls and six CPU cases. The CPU
positive uses a separate twin-document corpus added **after** the original
retrieval queries and offline snapshots. The distinct target ranks first at
cosine .855829; the seed is excluded. This is a positive integration case, not
a calibrated Related-quality benchmark. Source text also retrieves actual
audio/video candidates; audio without indexed text remains unavailable as a seed.

Original CPU Recall@5/MRR remain .90/.85; expanded positives remain
.9091/.8636. All ten empty controls have FP0. The screenshot miss and audio rank2
remain. Related cases do not change those quality denominators or thresholds.
Both owned jobs finish empty. No Rust build occurred between final profiles;
binary, DLL, harness and headless-example hashes are recorded.

These timings include different checks from preceding checkpoints, including
Related lookups and a later corpus addition. They are uncontrolled whole-harness
samples and establish no causal speedup/regression or1k/10k/50k/200k acceptance.
Host details are reused from the preceding
[host record](../2026-10-18-search-outcomes/host-reused.json), not freshly captured.

## Retained failures and controls

- Protocol/client/daemon RED logs retain actual stub/validation failures, seed
  occupation of top-k and stale empty completion. A separate failed-inference
  source-change control reproduced Internal instead of supersession before its fix.
- A later SQL-callback barrier reproduced80bytes of old error output after a
  request ID was replaced during seed lookup. Delivery now rechecks identity
  after the lookup before error/terminal writes; the control observes zero old
  bytes and preserves the new token. Result delivery also has a final token check.
- The first broad run passed226 daemon tests and failed the new public fixture.
  A diagnostic located its timeout in the ready lookup over the crowded source,
  rather than Ping/cancellation. The public control fixture now has two vectors
  and the same5second control bound. The separate crowded test still has1,609
  seed chunks and the unchanged candidate limits. Changing test embedding hooks
  also clears their query cache so each failure/zero-result case runs its hook.
- The first native no-model run failed after root restore, before the fresh
  reference assertion could index a result. Its stored cache had revision3,
  incomplete progress and no exact rows after earlier successful extraction.
  Its JSON, client failure and cache-state diagnostic are retained. The new
  policy-transition regression failed before the cache fix and passes afterward,
  including a source-change rejection and immediate root hiding.
- The preceding checkpoint's older transient result failure remains recorded
  there. This checkpoint proves the reproduced cache-loss race and final profiles;
  it does not supply a complete causal trace for every historical intermittent case.

No source/hash, cosine, quality, timeout, resource or existing assertion threshold
was relaxed. These are author-reviewed local results. Independent review has not
been completed; the earlier attempt failed because workspace credits were
exhausted. Prior skipped/quota bot reviews and outstanding structural findings
still need review; configuration and
thresholds were not changed.

## Reproduce and evidence

Use the pinned model and dependency setup in the
[preceding checkpoint](../2026-10-18-search-outcomes/README.md). Set
`WS_EMBED_TEST_MODEL` to the actual pinned GGUF with its sibling projector; use
CPU for the model replay. From `src/engine`, run:

```powershell
rtk proxy cargo test --workspace --exclude ws-embed --all-targets -- --test-threads=1
rtk proxy cargo test -p ws-embed --all-targets -- --include-ignored --test-threads=1 --nocapture
rtk proxy cargo build -p winsearch-embed --bin winsearch-embed
rtk proxy cargo test -p winsearch-embed --bin winsearch-embed real_model_round_trip_through_worker_binary -- --ignored --test-threads=1 --nocapture
rtk proxy cargo build --workspace --bins --examples
```

From the repository root, use fresh output directories and do not build Rust
between the native profiles:

```powershell
rtk proxy dotnet test src/app/WinSearch.Tests/WinSearch.Tests.csproj --nologo
rtk proxy dotnet build src/app/WinSearch.App/WinSearch.App.csproj -p:Platform=x64 --nologo
rtk proxy python tools/test-search-parity.py --output artifacts/related-repro --compare-offline --compare-client
rtk proxy python tools/test-search-parity.py --output artifacts/related-cpu-repro --real-model --compare-offline --compare-client
```

[Evidence hashes](evidence-hashes.json) distinguish raw local logs from published
sanitized/condensed files. Source identities, user paths and SID are redacted;
fixture byte/model/binary hashes remain. Model output is condensed to test and
reference lines with the full raw hash retained. Native summaries retain Related
frames, lifecycle/provenance and original-query parity hashes instead of repeated
DTO copies. Structural hashes use Python `json.dumps` with `ensure_ascii=False`,
sorted keys, compact separators and UTF-8; full raw JSON hashes remain in the manifest. No models,
binaries or databases are published. Native summaries: [no model](native-no-model.json),
[CPU](native-managed-cpu.json), [retained failure](retained-native-no-model-failure.json).

Lumen adapter/identity migration, installed Related behavior, finer file-kind
scopes/CLI filters, richer provenance/previews, resource/scale/device calibration,
standalone UI, packaging/release and broad final review remain open in the
[README TODO](../../../README.md#v10-todo) and
[gap matrix](../../v1-gap-matrix.md). No release or merge is performed.
