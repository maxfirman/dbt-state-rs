# Managed seed acceptance

The opt-in `experiments/databricks_seeds.py` runner exercises pinned dbt 2.0.11
on serverless Unity Catalog with ordinary managed Delta seed relations. It uses
an explicit ownership lease and fresh exact relation allowlist inside
`cdo_demo_db`; it preserves every preexisting relation and creates no schemas.
Run hosted first, replay the complete certified fixture through the production
gRPC router, then run the same managed operations against Rust with a fresh
relation prefix and isolated State history.

The workload declares duplicate rows and NULLs, checks complete typed row
multisets, ordered column names/types and MANAGED/Delta properties, and compares
with an independent State-disabled dbt project. It changes CSV contents,
`column_types` (INT to BIGINT, alongside BIGINT/STRING columns), `delimiter`
(comma to pipe) and `quote_columns` (false to true) individually. Every unchanged
seed and downstream control must reuse. First-call Explain and full successful
confirmations are captured for each decision; downstream requests preserve the
actual seed epochs and producer evidence.

```bash
uv run --with pyyaml python experiments/databricks_seeds.py \
  --dbt /path/to/dbt-2.0.11 --proxy /path/to/record-proxy \
  --probe /path/to/state-probe --schema-lease /path/to/owned-lease.json \
  --work-dir /fresh/artifact/directory --run-id YYYYMMDDabcdef
# Rust phase adds --rust-url http://127.0.0.1:PORT for an owned production router.
python3 experiments/databricks_seeds_test.py
```

The ignored `real_dbt_managed_seeds_match_independent_reference` test owns an
isolated production-router/Postgres server for the Rust phase, using the same
`DBT_STATE_DATABRICKS_*` executable, Python, artifact and lease variables as the
baseline acceptance tests. Ordinary CI runs the independent oracle fault probes
and exact captured-traffic replay without cloud credentials.

This ordinary managed-seed flow does not resolve the mandatory copied-seed
rebuild blocker: Databricks `CREATE OR REPLACE` over a managed shallow clone
fails with `CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR` in the pinned scope.
No automatic Databricks seed Result copy is enabled by this ticket, and no
whole-scope Databricks support claim follows from these seed checks.
