# Databricks upstream origin and served rows

On 2026-11-21, dbt 2.0.11 asked hosted State whether to build a FULL model in dev. Hosted granted a Result copy when dev read the same physical upstream as prod (A) and when dev read a confirmed copy of that upstream (B). It required a build when dev had independently built its upstream, including when the upstream SQL and rows matched prod (C). In the distinguishable-row case (D), dev served the expected `D_dev` marker. These are the Databricks observations for [#238](https://github.com/maxfirman/dbt-state-rs/issues/238), the native experiment in [#236](https://github.com/maxfirman/dbt-state-rs/issues/236).

| Case and evidence | Upstream read by dev | First dev downstream submit | Dev row | State-disabled dev row | Next unchanged submit and row |
|---|---|---|---|---|---|
| [A](evidence/databricks-upstream-origin/A.json) | Same prod relation | CLONE prod downstream | `A_prod` | `A_prod` | SKIP, `A_prod` |
| [B](evidence/databricks-upstream-origin/B.json) | Dev relation, confirmed CLONE of prod upstream | CLONE prod downstream | `B_prod` | `B_prod` | SKIP, `B_prod` |
| [C](evidence/databricks-upstream-origin/C.json) | Dev relation, independent BUILD with identical SQL and rows | BUILD | `C_prod` | `C_prod` | SKIP, `C_prod` |
| [D](evidence/databricks-upstream-origin/D.json) | Dev relation, independent BUILD with identical SQL and a changed raw row | BUILD | `D_dev` | `D_dev` | SKIP, `D_dev` |

## What varied

Each case used a fresh project and UUID-prefixed managed Delta tables in two existing `cdo_demo_db` schemas. Both schema inventories showed the names were free. One prod upstream and one prod downstream were built and confirmed before the dev downstream submit. The output object name and model unique ID matched across targets within each case. The downstream model always selected from `ref('upstream')`.

In A the upstream model resolved to the prod schema for both targets. In B, C and D it resolved to each target's own schema. B's dev upstream received an ordinary submit grant naming the prod upstream. The client confirmed that grant with `failed_to_clone=false`, and the physical Delta operation was `CLONE`. C and D disabled copying for the dev upstream build. Each ran the exact SELECT that its prod upstream had executed, and Delta recorded `CREATE OR REPLACE TABLE AS SELECT`. In D, after both prod builds, a single UPDATE changed the raw marker from `D_prod` to `D_dev` before dev built its upstream. The read of the materialized prod downstream returned `D_prod`.

## How the decisions and rows were checked

Every downstream manifest set `pre_clone=never`. Every ordinary downstream submit carried execution type 1 (FULL) and zero seconds of Lag tolerance. Prod sent `allow_clones=false` and chain limit 0. Dev sent `allow_clones=true` and chain limit 1 in all four cases, with `defer_to_target=prod`. The recorded input names identify the upstream each submit actually read. A's speculative submits returned `undecided`. The actionable decisions in all four cases came from `SubmitEnrichedSQL`. There were zero `RegisterClone` calls.

For every grant the client's Confirm request referenced the grant ID, the service returned success, and the warehouse recorded a `CLONE` at Delta version 0. Both downstream BUILD decisions had skip and clone rejection codes 6/6. Every write completed on its first attempt. The first and repeat physical reads each selected the whole dev output table. The independent `dbt show --select downstream --target dev --no-manage-state` executed the same dev SELECT against the intended upstream, returned all rows, and sent zero State RPCs. All eight dev table reads matched their case's Reference execution.

## Inspect or reproduce

Each linked JSON file contains the pre-run inventory, raw input setup, per-step physical rows and column types, Delta operation and version, effective model state, and RPC counts. It includes complete request and response bodies, gRPC status and response metadata for ordinary submits, confirmations, deferral and speculative submits. The recorder certified 118 RPCs, including version checks and telemetry. A and B each span two certified sessions because the collector initially expected a copy confirmation response to echo the grant ID. The **Confirm request** carries that ID. The continuation used the original tables and issued only the outstanding commands.

From the repository root, run `python3 experiments/databricks_upstream_origin_test.py` to check the retained decisions and row comparisons offline. The test also substitutes `D_prod` for the first dev read and checks that the driver reports the wrong marker. To list the dev decisions, grant sources and physical rows, run the following command.

```bash
jq -r '.case as $case | .steps | to_entries[] | select(.key | startswith("dev_")) |
  . as $step | .value.rpcs[] | select(.method == "SubmitEnrichedSQL") |
  .response.response | to_entries[] |
  [$case, $step.key, .key, (.value.clone_source // ""), $step.value.physical.rows[0][0]] | @tsv' \
  docs/reports/evidence/databricks-upstream-origin/*.json
```

To repeat the live experiment, use the [Databricks setup](../development.md#databricks-live-setup), hosted State credentials, and two existing demo schemas. The driver creates fresh names and retains the tables and private raw captures. Run A first to establish automatic copy on the setup. [Experiment driver](../../experiments/databricks_upstream_origin.py)

```bash
cargo build -p dbt-state-harness --bin record-proxy
work=$(mktemp -d)
for arm in A B C D; do
  uv run --with pyyaml python experiments/databricks_upstream_origin.py --case "$arm" --work-dir "$work/$arm" || exit 1
done
```
