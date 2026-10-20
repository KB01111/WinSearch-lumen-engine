# Server frame deadlines and failed-client cleanup

The raw Windows server transport now limits a frame's header, payload and codec
flush to one five-second deadline. A peer which stops reading or only drains
small chunks cannot retain its writer indefinitely. Timeout cancels the specific
overlapped operation, waits for completion before releasing borrowed memory, and
closes the endpoint. Subsequent writes fail and the connection reader observes
closure. A completed frame permits normal idle time before the next frame.

The change applies to the shared server pipe implementation used by the daemon
and embedding worker. Synchronous client endpoints and protocol version 1 stay
compatible. No dependency, model or retrieval threshold changed.

Broadcasts copy the client list under its short registry lock, then perform I/O
outside that lock. A failed peer is unregistered and its outstanding request
tokens are canceled. Concurrently registered healthy clients remain registered.
Broadcast delivery to the snapshot remains serial; the deadline is per frame,
not an aggregate multi-client fan-out budget.

## Verification

| Check | Observed result |
| --- | --- |
| Original native writer regressions before the fix | Two functional failures: non-reading and trickling peers exceeded the eight-second observation bound; idle-frame control passed |
| Broadcast regression before the fix | Functional failure: the stalled writer held the global client registry |
| Elapsed-before-flush regression before the fix | Functional failure: expired frame reported success |
| Final affected serial Rust suite | 365 passed, zero failed, zero ignored, across `ws-platform`, `ws-protocol`, `ws-client` and `winsearchd`, all targets |
| Strict Clippy including dependencies | Failed in unchanged `ws-modelmgr/src/download.rs` (`derivable_impls`) |
| Strict Clippy excluding dependencies | Failed on existing daemon code, including dead-code, Boolean simplification and legacy test style findings; no diagnostic points to the new writer/test code |
| Strict platform Clippy, all targets, excluding dependencies | Passed with `-D warnings` |
| Formatting | The transport and both new test modules pass scoped rustfmt checks; dense legacy daemon source was not reformatted |
| Scoped independent GPT-6.1 Sol review | Flush-expiry finding fixed and regression reproduced; final rereview found no remaining scoped finding |

The final suite includes the five new native regressions plus existing search,
preview, policy, source-revocation, cancellation and worker transport checks.
The native tests use actual Windows pipe handles and oversized payloads relative
to the pipe buffer. Failed TDD runs clean up their blocked writer threads by
closing the peer. The broadcast test follows the existing daemon test listener
pattern; it does not establish production listener-resource or admission limits.

Exact commands, source hashes, log hashes and lint/format results are recorded
in [evidence.json](evidence.json). Raw logs are retained locally under
`artifacts/ipc-writer-*.log`; public hashes identify them without uploading logs,
models, binaries or user data. Commands ran serially on Windows x64.

The implementation follows Microsoft's
[overlapped pipe I/O](https://learn.microsoft.com/en-us/windows/win32/ipc/synchronous-and-overlapped-input-and-output)
and [cancellation lifetime requirements](https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-cancelioex).
Canceling an operation does not itself prove completion; its event, OVERLAPPED
structure and buffer remain alive until the completion result is collected.

## Still open

The previously verified release package and Lumen staging remain pinned to
`d4f1448068d93d51120f7ec1e2a17395f88089f7`; they do not yet contain this transport
change. Rebuild, actual-model worker/package gates and consumer staging are
required before adopting the new bytes. This checkpoint does not rerun the full
workspace, .NET/WinUI, real-model, retrieval, scale, GPU or installed matrix.
Strict branch Clippy remains failing; these failures are retained and tracked
in the final review TODO, without suppressions or a lint-pass claim.

Lumen's native query/policy adapter, user-data migration and single-index cutover
remain unfinished. Concurrent multi-peer fairness, aggregate resource bounds,
installed control/Stop behavior and broad final review remain open. The README
retains all 33 open TODOs and full v1.0 acceptance is not claimed.
