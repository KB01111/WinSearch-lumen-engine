# Per-folder source policy checkpoint

The engine now accepts explicit per-folder hidden-file, maximum indexed-file
size and relative exclusion rules. These are additional source restrictions,
applied before lexical/FTS candidate limits and cached-vector top-k. Rules apply
to ordinary and opt-in code roots; the most specific normalized selected root
owns its rule. Existing callers that omit the additive field retain their
previous configuration behavior.

`IndexingConfig.root_policies` contains objects with `root`, `exclusions`,
`include_hidden` and `max_file_size_bytes`. Each rule must name a currently
selected ordinary or code root. Size must be positive. Duplicate normalized
roots, unselected roots, absolute/traversing/NUL exclusions, unknown rule fields
and excessive lists are rejected. Up to 256 rules and 512 aggregate exclusion
patterns are supported, with at most 1,024 UTF-8 bytes per pattern.

Exclusion semantics preserve Lumen's current relative policy: an ordinary name
matches a relative subtree/component, and a leading `*` matches a suffix. This
field is not an arbitrary glob/regular-expression engine. Hidden filtering
includes relative dot components and Windows hidden attributes within the root.
Folder sizes do not consume the file-size limit. Metadata checks do not read
source content or hydrate cloud placeholders; existing reparse/residency and
sensitive/generated-file guards remain.

Hidden opt-in reaches both initial crawl and watcher point updates. Current
metadata is checked again for retrieval, extraction and code admission, including
cached vectors. Saved rules stay deferred with saved roots until current host
admission. Rules remain persisted in the database while effective deferred
configuration omits them.

Code crawling reads fixed `.gitignore`, `.ignore` and `.git/info/exclude` controls
through its separately fenced, bounded control lane even when hidden source
files are not indexed. Those controls retain the existing 2 MiB bound and local
source/root/Stop checks. An explicit exclusion of a required ignore control
fails the scan instead of silently disabling ignore rules. Code parsing retains
its independent 2 MiB source cap.

The engine advertises `index.root_policies`. Rust and C# clients reject a
nonempty policy mutation before dispatch when this capability is unavailable;
older engines cannot silently discard restrictions. Protocol version remains 1.

## Verification

Functional REDs reproduced discarded Rust/C# fields, ignored source restrictions,
oversized lexical/vector candidates consuming limits, hidden opt-in missing the
crawler, invalid rules being accepted and requests sent to unsupported peers.
Additional REDs reproduced saved rules escaping deferred configuration and
trailing separators letting a parent rule override an explicitly selected child.
No quotas or acceptance thresholds were relaxed.

The first focused run passed all four original native root tests, the protocol
round-trip, Rust capability dispatch and both C# cases. The final serial suite
also covers deferred/reopen, watcher updates, nested rules and native code
ignore-control behavior. It passed **642 Rust tests**, with one actual-model
worker case ignored, and **155 .NET tests** without skips. The x64 WinUI build
had zero warnings or errors. Commands, tested source hashes and local-log hashes
are recorded in [test-evidence.json](test-evidence.json).

## Remaining work

Lumen must explicitly map every admitted folder's settings to these rules,
including its additional generated/private-data exclusions and checked conversion
from MiB to bytes. Keep current canonical roots/grants, ordered configuration,
source checks and closed native DTO validation. The host must require the new
capability and must not emulate source restrictions by filtering capped results.
Reuse existing engine hard exclusions such as `.git`, `node_modules` and
`target`; duplicating `.git` as an explicit control-read revocation would prevent
reading `.git/info/exclude` in an opted-in repository.

The staged Lumen payload/client is still pinned to the earlier `de3d186`
checkpoint. Rebuilding/staging the new contract, resolving consumer result-limit
parity, stable host metadata migration and the SearchService cutover remain open.
No new packaged/installed consumer, model/retrieval quality, scale/resource or
independent final-review acceptance is claimed by this checkpoint.
