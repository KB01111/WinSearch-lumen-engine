# Async headless Rust client checkpoint

This checkpoint adds the reusable Tokio client using the existing version1
protocol. It supports typed search batches/cancel, status subscriptions,
configuration, model controls and ordinary indexed-entry metadata resolution.
The native host still owns the verified engine process, roots, source/action
grants and user-facing behavior. See [contract and limits](../../headless-client.md).
Lumen's adapter, content previews, actual content provenance, Related/expanded
filters and installed full-v1 acceptance remain open.

Local evidence has distinct scopes:

- [Affected Rust tests](rust-tests.txt): 234 passed, zero failed/ignored
  (190 daemon, 30 client, 11 protocol unit, two entry-contract and one
  indexing-contract). The example's zero-test harness is not counted as native
  acceptance. The [public API documentation example](rust-doctest.txt)
  separately compiles, one passed. This uses the repository's documented
  serialized native test method: `cargo test -p ws-client -p ws-protocol
  -p winsearchd --all-targets -- --test-threads=1`.
- The client cases use Tokio duplex streams and actual Windows named pipes:
  version/capability/ID/type checks, malformed/oversized/truncated frames,
  bounded connect, cancellation and partial-write cleanup, ordered/terminal
  streams, separate bounded read/mutation lanes, typed pushed events and
  no replay after a lost mutation reply. Unknown mutation outcomes fence further
  writes across clones until the owning host restarts/reconciles the engine;
  reads remain usable. An acknowledged rejection permits a later valid write.
  Quiet subscriptions stay connected;
  a started frame is bounded. They require no engine or model and are transport
  fixtures, not inference evidence.
- [Current native results](native-results.json): a rebuilt owned daemon/CLI/
  extractor and client example used generated document/PDF/code/notebook sources.
  Ten public query comparisons and seven offline/native-snapshot cases agree.
  The typed client returns identical ordered document/code DTOs and actual
  line locations, receives a paused-policy event, resolves current metadata,
  reconnects after dropping search, and rejects revoked roots/proofs. Restoring
  roots does not revive an old proof; fresh resolution succeeds. Model status
  was not-downloaded, model-loaded was false, code had zero model vectors.
  All assertions passed, owned job empty, 3.730 seconds total harness time.
  This excludes real-model/installed-app performance. The earlier first native
  runs also passed (6.039 and 3.255 seconds); final rebuilt-example evidence is published.
- [Dependency tree](dependency-tree.txt): no ws-embed, llama/CMake, ws-platform
  or frontend dependency. [Coherent build](coherent-build.txt) passed with seven
  existing debug daemon dead-code warnings; the final
  [client example build](client-example-build.txt) passed.
- [Client-only Clippy](client-clippy.txt) passed with warnings denied and
  dependencies excluded. The earlier dependency-inclusive command
  [failed](dependency-clippy-failure.txt) on the existing manual Default
  implementation in ws-modelmgr. It is retained as a failed check, not presented
  as a full-workspace lint pass.

Two default-parallel attempts are retained separately: a
[code-vector recovery failure](parallel-recovery-failure.txt), and a
[hostile-worker PID/readiness failure](parallel-readiness-failure.txt).
The unchanged recovery test subsequently [passed in isolation](isolated-recovery.txt).
The latter parallel failure reached its short timeout/cancellation fixture
before a required PID file was available. No production code, assertions,
timeouts or resource/quality thresholds were changed to make these pass.
Temporary failure diagnostics were removed before the final serialized suite.
These parallel results are not counted as passes or claimed to be permanently
repaired by a successful serialized run.

Functional REDs are retained: missing [framing](framing-red.txt),
[owned session](session-red.txt), [unary transport](unary-red.txt),
[typed operations](typed-red.txt), [stream/subscription](stream-red.txt), and
[pre-ack broadcast](preack-red.txt). [Mutation-outcome REDs](mutation-fence-red.txt)
also reproduce later writes overtaking an unacknowledged/cancelled mutation.
The pre-ack case reproduces a permissible daemon
registration/broadcast race; unsolicited snapshots are discarded until a valid
version acknowledgement arrives. An earlier unbounded RED fixture had to be
stopped; its peer wait was bounded before the published functional unary RED.
That interrupted run remains in raw local artifacts.

Published copies normalize checkout/temporary fixture prefixes and trailing
whitespace. Native filesystem identifiers inside the opaque source proof are
redacted; binary/input hashes and outcomes are retained. Prior .NET/x64 evidence
belongs to the preceding entry-resolution checkpoint; no .NET source or wire DTO
changed here and it was not rerun for this transport-only change.

Independent client review and final-source security/performance, genuine-model,
Lumen/WinUI and installed package gates remain required. Approved-model reviewer
dispatch is currently limited by the agent thread limit; no unavailable review
is counted as an approval. The draft PR is not full v1 acceptance.
