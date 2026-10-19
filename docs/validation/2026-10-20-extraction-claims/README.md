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

## Rebuilt ef56d84 runtime

Coherent debug binaries from `ef56d84b2c19c86cb296a0d3d2868ee54eb7c1b6`
pass all 25 actual embedding/reference cases and the explicit real-worker round
trip. The same frozen binaries pass both no-model and CPU native profiles,
including the typed client and offline comparator. Original CPU Recall@5/MRR
remain .90/.85; expanded positives remain .9091/.8636 and ten empty controls
have zero false positives. The screenshot miss and audio rank two remain.

The rebuilt 62-file package has manifest SHA-256
`c3688466db06103467be42a17af8588a9ff3323b6e20069b8401cc9734c27346`.
Outside the checkout with System32-only PATH, it now completes text/PDF readiness
and the 1,000-result and 1,100-result streams in bounded frames. The full test
still fails: the large searches take tens of seconds, and the hidden-file
reconfiguration check reaches the existing readiness deadline. The 90-second
watchdog did not fire; cleanup left the owned job empty. The package remains
rejected and Lumen's pin is unchanged. Neither deadlines nor requirements were
relaxed.

[Runtime evidence](runtime-evidence.json) and [native inputs](native-inputs.json)
record this candidate separately from later source changes. Whole-harness times
are observations, not controlled performance comparisons. The CLI bytes changed,
so the older five retrieval-suite runs do not validate this candidate.

## Remaining acceptance

These counts establish reduced policy work in the tested claim paths. They do
not establish package throughput, latency under contention, or controlled scale
and resource acceptance. Finish the remaining large-result latency work, rebuild
coherent binaries and rerun native profiles, frozen retrieval suites and the
expanded outside-checkout test without changing deadlines or requirements.
Lumen's pinned payload must not advance on this regression alone.

The full v1.0 scope, Lumen search/data migration, installed acceptance and broad
final review remain incomplete. All 33 README TODOs remain open.
