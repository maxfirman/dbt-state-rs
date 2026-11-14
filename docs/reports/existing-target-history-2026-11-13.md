# Existing Target relation without State history — 2026-11-13

The #158 lifecycle capture found that a source-less SQL request reporting its
Target relation with a known epoch must build with the generic rebuild
description, an existing-table Explain line, an empty query-analysis section
and no upstream dependencies. The previous implementation described the table
as absent. No reuse is granted by this correction.

`golden/fixtures/databricks_existing_target_without_history.jsonl` is the
unchanged complete 24-RPC hosted control campaign: twelve crafted submits and
twelve full Explain responses, covering Databricks and Snowflake FULL, APPEND,
SNAPSHOT, VIEW, DBT_CUSTOM and data-test requests. All statuses succeeded. There
are no hidden prior builds, confirmations, SKIPs or warehouse writes in this
synthetic control. The data-test neighbor adds an empty query-analysis section only for its
separately persisted existing-result/no-history reason.

The new persisted `existing_target_without_history` reason admits only the
captured FULL, APPEND, SNAPSHOT, VIEW and DBT_CUSTOM kinds, when the sole input
is the exact Target relation with a known epoch and no prior scoped build,
and parsing the SQL in its Warehouse dialect establishes that it reads no
relation. Missing upstream metadata alone does not establish source-less SQL.
Seeds, unknown execution kinds and other incremental kinds retain their
existing treatment. A data test with exactly that first-build/sole-known-result
and source-less SQL condition uses `data_test_existing_target_without_history`,
preserving existing no-target, cached, upstream and audit cases. Existing real-client data-test
fixtures have no Target relation; the new crafted controls supply one. Old persisted `first_build` rows keep their original
Explain behavior. The decision engine remains pure.

Source patch was prepared by #158 and consumed by #159 as a separate shared
dependency, SHA-256
`f1953cca1f8c65bc677d6a38568e65ea9afc7e79e2627714d033dbb318104eac`.
Original secret-free capture and experiment driver are retained at
`/tmp/databricks-lifecycle-158-existing-target/` and
`/tmp/dbt-state-132-context/lifecycle158-existing-target.py`. Real managed Delta
existing-target evidence is separately retained by #158 at
`/tmp/databricks-lifecycle-158-atomic-explain/`; that broader lifecycle slice
remains unfinished.

The public production-router conformance replay exercises the complete fixture,
including both status and full response/Explain comparison, with zero gaps,
unknown baselines or partial-history exceptions. Validation is recorded in
`/tmp/dbt-state-132-context/isolation159-dependency-conformance.log` and the
associated server-library check. This dependency does not resolve #159's
separate missing-namespace older-match diagnostics or imply full Databricks
support.

CI run `43496999258` exposed the metadata-only admission error: the existing
`differential::explain_rederives_tree_and_selector_is_implemented` request is a
VIEW reading `ANALYTICS_DB.raw.raw_customers` through a CTE, despite reporting
only its Target relation. It must retain the previous `first_build` reason and
Explain tree. The follow-up passes optional SQL relation-read evidence from the
submit boundary into the pure engine; relation-reading or unclassified queries
retain that earlier behavior for models and data tests. The 24-RPC capture is
unchanged and does not establish hosted policy for those uncaptured queries.
The same relation-reading submit is entry 14 of
`golden_20261008T222744.061Z.jsonl`. Its pre-existing, narrowly pinned
missing-history rebuild-description exemption is restored: the source-less
capture did not resolve it. No exemption logic or new-capture assertions change.

Follow-up validation on 2026-11-14: the unchanged failing differential passes;
server library tests pass 309/309 with an invalid `DATABASE_URL`; full workspace
`cargo test` passes (605 passed, 32 opt-in ignored), including all 12 conformance
tests and the 79-corpus replay. Strict workspace/all-targets/fuzz Clippy and
format checks pass. Logs are retained under
`/tmp/dbt-state-132-context/ci-fix-{differential,server-lib,workspace,clippy,fmt}.log`.
The initial conformance run in `ci-fix-conformance.log` records the precise
legacy-exemption-set failure before its restoration; the final workspace run
records the passing conformance gate.
