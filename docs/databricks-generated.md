# Opt-in generated Databricks replay

The credential-free `databricks_generated` test remains the modeled safety gate.
Live replay consumes its exact closed semantic plan: FULL table, zero Lag
tolerance, direct source projection with bounded INT addition or literal VALUES,
duplicates and NULLs. It does not import production identity/decision helpers.
Complete hosted captures remain the conformance oracle; symbolic ledger results
and actual warehouse Reference executions check Reuse safety separately.

Export a reproducible plan without accessing Postgres or a warehouse:

```sh
DBT_STATE_GENERATED_SEED=0 DBT_STATE_GENERATED_LITERAL=false \
DBT_STATE_GENERATED_PLAN=/tmp/generated-plan.json \
cargo test -p dbt-state-harness --test databricks_generated export_live_plan -- --ignored --exact
```

Replay through hosted State first, then use a different empty work directory and
fresh object UUID with `--rust-url http://127.0.0.1:PORT` for the Rust production
router. Supply the pinned dbt 2.0.11 binary, existing proxy/probe binaries, a demo
Databricks profile and an evidenced existing schema lease (same format as the
freshness campaign). `--schema` may select another existing prod/ref container
listed in that lease’s digest-verified ownership evidence when the default is
full. Current complete inventory and the two-object capacity bound still apply.
No schemas are created. Each phase collision-checks and
owns exactly two fresh UUID-named tables inside `cdo_demo_db`; reference `dbt show`
creates no objects. All preexisting inventory is retained. Clone is disabled via
`pre_clone: never` and the client copy permissions used by the retained graph.
No server copy switch exists; [current policy](databricks-copy-preview.md) applies.

```sh
python3 experiments/databricks_generated.py \
  --dbt /path/to/dbt-2.0.11 --proxy /path/to/record-proxy --probe /path/to/state-probe \
  --schema-lease /path/to/schema-lease.json --plan /tmp/generated-plan.json \
  --work-dir /tmp/generated-hosted --observations 4
```

Omit `--observations` to replay the full bounded plan (44 source observations or
six literal observations). A prefix is explicitly recorded in operations; a
four-observation pilot covers BUILD, mandatory unchanged SKIP, INSERT and SKIP.
The retained `plan.json` plus seed makes semantic replay explicit, while each new
run uses fresh relation names and State history. There is no arbitrary SQL input.

`operations.json` retains SQL and dbt commands with outcomes. Certified complete
captures include actual client metadata/epochs, confirmations and Explain trees;
epochs are never synthesized in live mode. Every observation compares physical R
with an independently advanced symbolic source ledger F, complete typed SQL API
rows/schema, measured MANAGED/Delta properties and State-disabled dbt `show`.
Row checks precede decision checks, so a stale SKIP cannot pass because hosted
State emitted it. An unchanged BUILD also fails. `summary.json` records failure;
transport/auth/SQL errors never count as a successful mutation check. Existing
modeled wrong-SKIP and always-BUILD mutants remain mandatory offline tests.

Retain secret-free work directories outside the repository. Replay complete
hosted recordings with the existing supplemental conformance capture interface;
do not promote partial histories or claim that live safety proves hosted policy.
A general shrinker and broader SQL grammar are separate extensions, unnecessary
for replaying the bounded seed/operation journal. Live access is explicit only;
ordinary CI has no credentials or scheduled cloud access.

The 2026-11-15 seed-0 source pilot passed all four observations on hosted State
and Rust with pinned dbt 2.0.11. Each complete capture contains 27 RPCs (four
submits, two confirmations, four Explain calls plus speculative/validation/
telemetry calls). All eight physical observations passed typed duplicate/NULL
multiset, ordered schema, MANAGED/Delta and independent read-only dbt checks.
Only the four named UUID tables were added in `cdo_demo_db.ds_20261112a134ef_prod`:
hosted `17486fb44e734bacabce160e07d9cb0d`, Rust
`cf2d6ff605d0454da87d7302a5a939c4`, each prefixed `g161_` with suffixes
`_inputs` and `_consumer`. No existing tables were changed. The first attempt
refused full reference containers before DDL; that failed receipt is retained.
