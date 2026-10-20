# Durable database identity checkpoint

Lumen's future host ledger needs a durable namespace for canonical `eN` and `cN`
entry IDs. Process IDs and source-reference incarnations cannot supply that
namespace: process IDs change, and entry counters can restart after deleting a
database. The isolated code database also reused a deleted highest file ID.

Each store now creates and validates its own non-secret `meta.database_id`.
Ordinary and code identities are separate, persist across reopen, and change
when a deleted database is recreated. `EngineStatus.index_id` and `code_index_id`
carry them under the additive `index.identity` capability. Legacy/unavailable
values remain absent. Rust and C# status DTOs preserve the fields.

Code file inserts allocate IDs from a durable high-water mark in the same
savepoint as the file batch. Initialization accounts for existing rows and never
decreases a concurrently advanced mark. Deleting all files, pruning a repository
or reopening cannot reuse a committed code identity. Existing rows, parsed text,
symbols and vectors keep their IDs. Overflow fails without publishing a partial
batch. Ordinary entries already use SQLite AUTOINCREMENT.

## Reproduced failures and checks

Before implementation, actual tests demonstrated missing database identities,
acceptance of corrupt identities, Rust/C# status fields being discarded, and
reuse of a deleted code file ID after reopen. No assertions or acceptance
thresholds were relaxed.

The complete focused storage/protocol run passed 176 tests: 49 code, 98 ordinary
index, 20 protocol unit and 9 protocol integration cases. Additional coverage
checks legacy code migration with preserved parsed content, database recreation,
failed-batch rollback, overflow and unknown legacy status fields.

The final serial workspace run passed 633 Rust tests with one real-model worker
case ignored. All 153 .NET tests passed without skips, and the x64 WinUI build
had zero warnings or errors. Commands, final-source hashes and local-log hashes
are recorded in [test-evidence.json](test-evidence.json). Raw local logs are
`artifacts/index-identity-red.log`, `index-identity-contract-red.log`,
`index-identity-green.log` and `index-identity-final-regression.log`.

## Limits and next integration work

These values are bookkeeping, not content hashes, source proofs or filesystem
grants. Consumers still need their host profile, the corresponding database
identity, canonical entry ID, fresh source resolution and current root policy.
Database cloning, arbitrary snapshot rollback, an older writer without the new
allocator and manual database edits require explicit reconciliation; this
checkpoint does not establish restore/downgrade acceptance.

The Lumen owned-startup change is published in
[PR #27](https://github.com/KB01111/Lumen/pull/27), at consumer commit
`8865f159b5f1a00e06fcf46e55e901631ea7e052`. Its separately verified pinned
development package/client still uses engine commit `de3d186`; this new contract
has not yet been rebuilt, staged and consumed by Lumen. Search streaming,
per-folder source-policy parity, host metadata migration and the single-index
cutover remain open. No new installed/package/model/scale acceptance is claimed.

This checkpoint has author review only. An attempted independent GPT-6.1 Sol
review could not start because the agent thread limit was reached. Broad final
review and the [remaining v1.0 TODOs](../../../README.md#v10-todo) remain required.

The SQLite behavior used here is documented in the official
[AUTOINCREMENT](https://www.sqlite.org/autoinc.html) and
[core functions](https://www.sqlite.org/lang_corefunc.html) references.
