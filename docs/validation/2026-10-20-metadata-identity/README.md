# Metadata-only identity checkpoint

Ordinary filenames remain admitted when their content is unavailable or reserved
for code search. The resolver previously required full content admission even
for metadata-only bindings, dropping the canonical identity needed for native
actions and host metadata/pins. It also repeated project/dataset directory
classification for those bindings.

The resolver now uses current name/root, residency/reparse and native-file
identity checks for metadata-only entries. Content-bearing entries retain their
existing content-policy checks. Both paths are revalidated before returning.
Metadata-only responses expose no content revision or content hash. The opaque
source proof now includes content availability, so an earlier content-bearing
proof cannot become valid again after content admission is revoked.

## Verification

A functional RED extracted a generated document, introduced a project marker,
confirmed the former content proof was refused, then reproduced the missing
metadata-only identity. The corrected case and seven other reference tests pass.
The initial short fixture/hash assertion was corrected before the functional RED;
legacy in-process extraction does not fabricate a source-byte hash.

The full workspace run passed 250 daemon cases and caught a native fixture race:
the watcher advanced content revision 2 to 3 during metadata capture. The resolver
correctly returned `NotReady`. That fixture now uses the existing five-second
bounded `NotReady` retry pattern for a fresh unbound request; size, old-proof,
model-generation, native identity and grant-revocation assertions are unchanged.
The case passed when replayed with workspace features.

The remaining 402 workspace cases passed separately, with one actual-model
worker case ignored by that command. Final disjoint coverage is **653 Rust tests
passed**, counting the 251 daemon cases once and excluding repeated focused
checks. This is not a claim that the initial failed workspace command passed.
Commands, source/log hashes and scope notes are in
[test-evidence.json](test-evidence.json).

## Remaining acceptance

The [earlier rebuilt candidate](../2026-10-20-engine-contract/README.md) passed
actual model/reference, native CPU/no-model and frozen retrieval gates but
failed the expanded outside-checkout 1,000-result test at its existing watchdog.
The performance effect of this resolver fix is unproven until new coherent
binaries are built and that package test passes. Do not relabel the old candidate
with the new source commit or advance Lumen's pin on unit tests alone.

Independent final review, controlled scale/resource measurements, Lumen's
SearchService/metadata migration and installed acceptance remain open. The full
v1.0 goal and all 33 README TODOs remain incomplete.
