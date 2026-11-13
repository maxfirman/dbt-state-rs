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
is the exact Target relation with a known epoch and no prior scoped build.
Seeds, unknown execution kinds and other incremental kinds retain their
existing treatment. A data test with exactly that first-build/sole-known-result
condition uses `data_test_existing_target_without_history`, preserving existing
no-target, cached, upstream and audit cases. Existing real-client data-test
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
