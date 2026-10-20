# Search result limit checkpoint

Lumen requests 500 results by default and allows up to 10,000. The previous
daemon request path capped exact candidates at 120, the storage layer capped
each candidate source at 500, and the Rust client rejected limits above 4,096.
The daemon could also send exact overflow and semantic results beyond the
requested total. Raising one client constant could not preserve consumer behavior.

The engine now advertises `search.limits`. Ordinary queries share their total
between exact and optional semantic results, up to 10,000. Zero retains the
existing effective minimum of one. Larger lexical requests raise the bounded
candidate budget while small requests retain the 500-row storage baseline.
ID deduplication uses a set with the original insertion/ranking order preserved.
Exact results remain first; a full exact budget avoids unused semantic inference
and reports that stage as disabled.

Delivery splits at 512 results or 8 MiB, rechecking request identity, current
source/model policy and cancellation after acquiring the writer for every
piece. The aggregate budget is 64 MiB and 64 result frames, with room reserved
for a bounded error and failed terminal response. Oversized single results and
exhausted streams fail explicitly. Existing wire framing, quotas and deadlines
are unchanged; the synchronous server writer still has no deadline.

Both clients enforce capable peers' requested total and retain the legacy
ordinary 4,096-result compatibility bound. Larger requests require the new
capability before dispatch. Rust counts raw received bytes, including ignored
additive fields and interleaved pushes. C# checks correlated result bytes,
frames and count before queueing, and releases receive accounting on failed
requests, disconnect and disposal, including a paused consumer.

## Verification

Retained functional failures reproduced the 120- and 500-result truncation,
exact/semantic overflow, rejection of capable 10,000-result peers, accepted
over-budget replies, unsupported request dispatch, unbounded aggregate receive
bytes, unnecessary inference and retained disposal accounting. An initial build
error in the sizing helper and initial test-helper errors are recorded as build
errors, not functional RED evidence.

Focused checks passed five daemon cases, four Rust-client cases and five .NET
cases. Daemon tests include 1,100 actual generated files with 1,000 delivered
results, count/byte splitting, explicit aggregate/individual failure and no
unused query inference. Pipe-peer cases exercise client bounds separately from
real model acceptance. An additional Rust case covers raw ignored fields and
interleaved push traffic in the final suite.

The final serial workspace passed **652 Rust tests**, with **1 actual-model
worker case ignored**. All **160 .NET tests passed** without skips, and the
x64 WinUI build completed with zero warnings or errors.

Final regression commands and tested source/log hashes are recorded in
[test-evidence.json](test-evidence.json).

## Remaining work

Lumen must forward the requested total through its native adapter and accumulate
progressive snapshots while preserving grants, latest-request cancellation and
selection. The currently staged client/payload remains pinned to `de3d186`;
rebuild, independently verify and pin the new identity/root/limit contract before
consumer cutover. The host metadata migration and one authoritative crawler,
index and model lifecycle remain required.

This is functional source verification with author review. Independent final
review, real-model/reference/retrieval replay on the final coherent payload,
controlled 1k/10k/50k/200k resource/latency tests and installed consumer acceptance
remain open. No complete v1.0 or production acceptance is claimed.
