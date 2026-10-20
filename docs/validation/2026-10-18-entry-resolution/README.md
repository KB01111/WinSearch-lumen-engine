# Indexed entry resolution checkpoint

This adds the ordinary filesystem identity-resolution contract needed by native
preview/actions. It does not complete content previews or the Lumen adapter.
No model, parser, dependency, retrieval threshold or reference vector changed.

The protocol's functional RED rejected the previously unknown `resolve_entry`
variant. The native pipe RED then showed the operation was unavailable. Current
wire coverage verifies current metadata, no fabricated content revision before
extraction, a valid revision after exact extraction, and no stale content revision
after replacement. It rejects stale source/model/grant proofs and malformed IDs;
unchanged policy saves and unrelated vector-cache changes retain valid references.

The .NET regressions additionally reproduced missing dispatch, mutable caller
arguments, out-of-root/missing-proof replies, null payloads and use without an
advertised capability. They now reject malformed replies with a structured engine
error and release pending requests. C# lexical path checks perform no content reads.

Validation is local, with separate scopes:

- Rust daemon/protocol: 204 passed, zero failed/ignored, including the actual
  secured pipe fixture. [Log](rust-tests.txt).
- Full .NET suite: 35 passed, zero failed/skipped. The x64 WinUI build passed
  with zero warnings/errors; see [tests](dotnet-tests.txt) and
  [build](winui-build.txt). This is a build, not native UI interaction acceptance.
- Rebuilt actual daemon/CLI plus native document/PDF/code parsing: ten public
  query comparisons, seven shared-offline/native-snapshot cases, current entry
  resolution and root-revocation rejection; no model loaded. All checks passed
  and the owned job was empty, 3.708 s total harness time. This is not an inference
  or installed-app performance measurement. [Results](native-results.json).

The full-process RED used the older standalone daemon, which closed the pipe on
the unknown operation. A coherent rebuild supplied the new contract; the passing
run is separate. New request/reply behavior is gated by `entry.resolve` for older
engines. Checkout prefixes and log whitespace are normalized in published copies.
Generated temporary fixture prefixes are also normalized, and the opaque source
proof is redacted to avoid publishing native filesystem identifiers. Binary/input
hashes and outcomes are retained; raw local logs remain under artifacts.

Independent review, code identity/content preview, cryptographic content provenance,
headless Rust client, Lumen/UI/installed behavior and remaining full-v1 gates are open.
