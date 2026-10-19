# Extraction claim work — 2026-10-20

The expanded outside-checkout payload test still fails readiness/search checks.
Investigation found unnecessary work in all three ordinary extraction claim
paths: an `IN` subquery materialized the visible index for each claimed job while
the caller held the index store lock.

Use a correlated `EXISTS` lookup for the candidate entry instead. Admission still
precedes the claim limit; the atomic `UPDATE ... RETURNING`, pending/claimed
state, kind filters and priority/LIFO order remain unchanged. This adds no cache,
index, migration or relaxed source policy.

## Regression evidence

The initial functional failure measured 13,221 `policy_fold` evaluations per
claim with 1,102 entries, for generic, name-kind and content claims. With the
lookup change, each path performs 21 evaluations. A subsequent stronger fixture
also queues all 1,100 unrelated entries and retains the same count. It requires
the allowed higher-priority job to win while the excluded highest-priority job
cannot consume the claim limit. The deterministic path-normalization function
is preserved by the counting probe.

All 99 index tests passed before the additional competing-job fixture; that
expanded regression then passed separately. The final unchanged-source serial
workspace run passes 654 tests, with one actual-model worker test ignored.
Repeated focused checks are not added to that count. Commands, source and log
hashes are recorded in [test-evidence.json](test-evidence.json). Raw logs remain
local.

Independent GPT-6.1 Sol read-only review found no scoped actionable issue after
checking correlation scope, visibility, ordering, atomicity, state transitions
and production callers. The reviewer did not run tests or native workloads.

## Remaining acceptance

These counts establish reduced policy work in the tested claim paths. They do
not establish package throughput, latency under contention, or controlled scale
and resource acceptance. Rebuild coherent binaries and rerun the existing native
profiles and expanded outside-checkout test without changing its deadlines or
requirements. Lumen's pinned payload must not advance on this regression alone.

The full v1.0 scope, Lumen search/data migration, installed acceptance and broad
final review remain incomplete. All 33 README TODOs remain open.
