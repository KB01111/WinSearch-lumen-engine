# Engine diagnostics and log retention

`winsearch-cli diagnostics` prints an explicit JSON summary on stdout and exits
nonzero when status is unavailable. Collection has a ten-second total deadline.
Its schema contains the CLI/engine versions, closed engine/NPU states, allowlisted
backend names, file/segment/queue counts, model-loaded state and index byte count.
An unsupported engine version or backend remains unknown. Failure output contains
only a fixed error code, rather than the remote message or transport exception.

The export excludes activity text, source paths/text, credentials, pipe/user/profile
identities, model errors and raw logs. It performs no upload. Model-loaded state is
a residency observation, not a model/backend quality certificate.

Local daemon logs rotate through `winsearchd.log.0` through `.3`, at most 8 MiB
each. Formatting is capped at 8 KiB per event before the lossy nonblocking queue,
which holds at most 256 chunks. Oversized events receive a truncation marker.
Startup retires only the previous engine's exact `winsearchd.log.YYYY-MM-DD`
daily-file names; unrelated files are preserved. Sharing, rotation and write
failures disable file logging. A blocked log directory does not prevent creation
of the mandatory database/model/cache layout or daemon startup.

Raw local logs can contain operational paths. They are not included in the
sanitized export. Forced termination can lose queued logs. Retention/failure
fixtures and export allowlist tests do not establish the full crash, low-disk,
installed rollback or device-removal acceptance matrix.

For local query profiling, the optional tracing targets
`winsearchd::query_profile=debug` and `winsearchd::query_profile_detail=debug`
record aggregate planning/delivery time, store-lock wait, guard-call count/time,
result count and policy-classification time. These events contain no query,
source path/text or entry identifier. Enable them only for a diagnostic run;
instrumented timings do not establish quiet release performance. They use the
same bounded local logger and are excluded from the sanitized CLI export.
