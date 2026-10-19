# Headless Rust client

`src/engine/crates/ws-client` is the asynchronous native transport for Rust hosts,
including the planned Lumen adapter. It uses Tokio 1 and the actual `ws-protocol`
types. It does not start an engine, crawl files, load a model or depend on WinUI,
Tauri or `ws-embed`. Protocol types are re-exported as `ws_client::protocol`.

The native host must verify and own the daemon/workers and their Job Object,
derive its own user SID and instance, and supply the local pipe name. The client
accepts `\\.\pipe\winsearch-S-1-…` names; remote paths, worker pipes and unrelated
namespaces are rejected. Namespace validation does not authenticate a server
process. The host must preserve executable and instance ownership; the engine
retains its per-user pipe DACL. Tokio's `SECURITY_IDENTIFICATION` default is kept.

```rust,no_run
use ws_client::{Client, protocol::{SearchMode, SearchQuery}};

async fn example(pipe_name: String) -> Result<(), ws_client::Error> {
let client = Client::new(pipe_name)?;
let mut search = client.search(SearchQuery {
    text: "invoice".into(),
    mode: SearchMode::Files,
    limit: 20,
    include_semantic: false,
    ..Default::default()
}).await?;
while let Some(frame) = search.next().await? {
    // Forward typed results only after the host rechecks current query/root grants.
    for result in frame.batch {
        println!("{}", result.title);
    }
}
Ok(())
}
```

`search` preserves ordered progressive batches, actual locations/excerpts and
the engine's terminal flag. `SearchStream::cancel(self)` sends the correlated
cancel and disconnects, with a one-second send bound. Dropping a stream or a
pending `next()` read closes its connection; a subsequent `next()` on that
interrupted stream reports the closed connection once, then returns `None`.
An error or terminal batch also closes the connection and releases admission.
There are no detached client tasks, retries of applied requests or queued result
batches. Polling/owning and dropping these futures remains the host's responsibility.

`SearchBatch.outcome` preserves the optional typed terminal state. Peers with
`search.outcomes` must supply it on terminal frames; missing, premature or
inconsistent outcomes fail validation. Older peers retain `None` (unknown).
Complete requests expose semantic disabled/ready/degraded with closed reasons;
superseded/canceled/failed requests have empty terminal batches and no semantic
state. The host must retire any earlier hits from an interrupted request. An
optional semantic failure can preserve exact hits as complete/degraded. See the
[wire contract](ipc-protocol.md#terminal-search-outcomes). The client does not
retry interrupted searches or infer availability from model files.

`status`, `model_status`, `config`, `set_config`, `indexing_config`,
`set_indexing_config`, `resolve_entry`, `preview_text` and explicit model download/import/delete
methods return shared typed DTOs. Calling the client does not implicitly download
or import a model. A mutation may have applied before its reply is lost; the host
must reconcile current state before deciding whether to issue another mutation.
A cancelled, interrupted or unacknowledged mutation fences all further mutations
through that client and its clones with `MutationOutcomeUnknown`. Reads and
subscriptions remain available. The host must stop/restart its owned engine,
reconcile persisted policy/model state and construct a fresh client before
resuming mutations; recreating a client alone cannot order an older server task.
A correlated server rejection completes the request and does not fence the lane.

Each operation uses one owned connection. Clones share four read slots, one
serialized mutation slot and one separate status subscription slot. Admission,
connect, handshake, writes and ordinary request reads share a 35-second total
deadline; connect/handshake also has a five-second bound. Only a missing/busy
pipe is retried, before any request is sent. Each request ID is positive and
unique across clones. Wrong IDs, unexpected response types or invalid versions
fail closed. Known unsolicited status/configuration frames cannot extend the
deadline. A status broadcast can race the handshake; it is discarded until the
version acknowledgement is validated.

The shared 16 MiB frame cap is checked before allocating an incoming body.
Search accepts at most 64 KiB of query text and a requested limit of 4,096;
delivery is bounded to 4,096 total results and 64 result frames. These client
resource bounds do not change engine ranking or retrieval acceptance thresholds.
`subscribe()` delivers typed status/model/configuration events on its own
connection. Idle subscriptions can remain quiet indefinitely, but a started
frame must finish within 35 seconds. Interrupted, expired or malformed reads
and partial writes close their owned connection instead of reusing partial bytes.

Exact search requires `search.exact`; semantic requests also require
`search.semantic`, code mode requires `code`, and entry resolution requires
`entry.resolve` for ordinary `e` IDs or `entry.resolve.code` for code-file `c` IDs;
cached ordinary text preview requires `entry.preview.text`; code/notebook cached
preview requires `entry.preview.code`.
Capability presence is not semantic/model readiness. Existing
`EngineStatus.model_loaded` and `ModelStatus` are returned unchanged; the richer
semantic outcome/provenance contract remains part of the unfinished Lumen work.

`resolve_entry` checks the requested identity/source proof, nonempty metadata,
positive optional index revision, canonical optional SHA-256 and lexical local-disk root containment before
returning a reply. The [source proof](ipc-protocol.md#indexed-entry-resolution)
is opaque metadata/policy identity, **not a file hash or permission to open a
file**. Native consumers must revalidate their own currently authorized roots,
source identity and residency before preview/open/read. Search results from peers
advertising `search.entry_references` carry a separate canonical bound
`entry_reference`; pass that reference to the resolver/preview rather than parsing
the query-specific hit ID. Present references are validated against the kind and
requested search mode; older peers may omit them. Code/notebook `c` metadata
includes its current complete-source byte hash when established by native parsing;
ordinary hashes are available when established by current extraction within the
64MiB hash budget; legacy/larger-source hashes remain unknown.
`preview_text(PreviewRequest)` returns bounded
cached text and actual locations through the same owned read lane. Dropping the
future closes that connection; identity/proof, local root, state, kinds, aggregate
64KiB/64part and eight extraction-kind bounds are checked before delivery.
See the [cached-text contract](ipc-protocol.md#cached-text-previews). Extraction
completion is within product limits; full source totals and cap state remain
unknown. Code previews use the same method/read admission with canonical `c`
references and actual source-line or notebook cell-local coordinates. Ordinary
entries reject notebook locations; code entries reject page/timestamp locations.
Code parts require real locations and notebook ranges cannot exceed returned text.
A trailing line terminator does not expose a following line. Full-byte code hashes
include notebook outputs, while preview text excludes them. Raster/media/uncached
preview, larger-source/legacy provenance recovery and expanded filters
remain open.

`SearchQuery.extensions` selects source-file extensions in ordinary/code exact
and semantic searches. It accepts at most64 single ASCII components of1..32
letters/digits/`+`/`-`/`_`, with an optional leading dot and case-insensitive
matching. An empty list preserves existing behavior. Nonempty filters require
`search.filters`; the client rejects unsupported peers before sending a query,
malformed input, mismatched reply extensions and folder/null-path replies.
Mode/category/extension filters precede engine candidate caps and semantic top-k.
Finer file-kind scopes and expanded source provenance remain open; typed terminal
semantic outcomes are described above.

The standalone CLI exposes the same extension selection with repeatable
`--extension <ext>` / `-e <ext>` or `--extension=<ext>` arguments:

```powershell
winsearch-cli search invoice --mode files --extension pdf -e .docx --json
```

Repeated values are alternatives; they intersect the selected search mode and
the engine's existing source/root policy. Invalid or excessive values fail before
connecting. Filtered queries require `search.filters`; an older peer cannot
silently run an unfiltered search. Every correlated result batch is checked for
the selected extensions before the CLI publishes its final JSON array. Unknown
long options are rejected; use `--` before literal query text beginning with
`--`. Omitted extensions preserve the existing unfiltered/legacy behavior.

`related(RelatedQuery)` requires a current bound indexed source and the separate
`search.related`, `search.semantic` and `search.outcomes` capabilities. Code seeds
also require `code`; extension filters require `search.filters`. It obtains bounded
cached indexed text through the engine and uses its existing local inference.
No caller paths, query text, vectors or implicit model downloads are accepted.
Missing text/model or disabled policy fails explicitly. Source media without text
is unavailable, preserving Lumen's existing text-based Related behavior.

Related uses the same `SearchStream`, cancellation, owned read connection and
35second total deadline. Requests permit1..10000 results; delivery cannot exceed
the requested limit or64frames. Ordinary search retains its existing4096 budget.
The client rejects a returned seed, missing bound references, mismatched domains
or filters and ordinary disabled-semantic fallback. Zero valid above-threshold
results can still be complete/ready; absent eligible vectors are explicitly
degraded. See the [Related wire contract](ipc-protocol.md#related-content-search).
Lumen's adapter, identity migration and installed Related acceptance remain open.

From `src/engine`, `cargo test -p ws-client --all-targets` runs framing and real
Windows-pipe transport fixtures without models or a daemon. The
`native-client-smoke` example is a separate assertion-based fixture for the
actual owned daemon. Build it and the parity binaries, then from the repository
root run:

```powershell
python tools/test-search-parity.py --output artifacts/client-native-new --compare-offline --compare-client
```

Use a fresh output directory. The harness creates its generated sources,
isolated data/instance and kill-on-close job, exercises current native indexes,
and requires the job to be empty afterward. The example changes only that
explicit fixture instance's root/pause policy and restores it. Its document and
code DTOs must equal the existing public CLI results; revocation must hide
ordinary results and invalidate old proofs even after root restoration.
The example requires four ordinary preview checks: actual cached document text,
PDF page2 and rejection of revoked roots/previous proofs after restoration. Four
separate code-preview checks cover actual source/notebook text, exclusion of output
sentinels, and refusal of revoked/expired proofs after restoration. Code preview
uses the reference and actual location supplied by search, without parsing hit IDs.
These are native transport/extraction checks, not installed Lumen, model quality
or launcher responsiveness acceptance. See the
[gap matrix](v1-gap-matrix.md) for the remaining full v1 work.
