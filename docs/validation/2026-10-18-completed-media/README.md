# Completed media cache repair

The existing bounded restoration helper rejected evicted artifacts after vectors
were marked complete. Its compare-and-swap now admits claimed or completed rows,
while preserving current revision/model generation and old-input checks. It does
not enqueue inference or alter exact rows, vector IDs/values or progress.

The [functional RED](red.txt) reproduces the completed-WAV failure. The new native
regression then restores actual generated audio and video artifacts, validates
their WAV/PNG data, checks preserved rows/vectors/progress and rejects a changed
source before starting an intentionally unavailable parser. Test vectors are
protocol fixtures; this is not real-model quality evidence.

The [affected-suite log](affected-tests.txt) records:

```powershell
cd src/engine
cargo test -p ws-index -p winsearchd -- --test-threads=1
```

271 passed, zero failed/ignored (189 daemon, 82 index). This is separate from the
earlier 353-test selection checkpoint. Checkout prefixes, line endings and trailing
whitespace are normalized in the logs; test outcomes remain unchanged.

The public identity-based preview/maintenance route and its cancellation behavior
remain to be implemented. Current production calls reach restoration through the
pending-vector path; the completed-state path is directly native-tested here.
Independent review and final whole-branch/installed/Lumen gates remain open.
