# Databricks Volatile evaluation capture (#198)

Status on 2026-11-19: prepared, live capture blocked. No new Databricks cell is
verified and no rendering, identity, Upstream producer or Reuse qualification
rule has changed. The existing query-changed FULL rebuild remains the only
captured rendering cell; see the [support matrix](protocol.md#volatile-evaluation).

`experiments/databricks_volatile.py` prepares a minimal isolated project with
fresh UUID-named objects and the pinned dbt 2.0.11 binary (including its digest
check). Two builds cover a true first build and an unchanged rerun of a table
calling `rand()` with evaluation requested, a passing singular data test with
evaluation requested, and a consumer table with a one-hour Lag tolerance. A
deterministic table supplies the test's input. Each build is followed by native
`dbt state explain --verbose` using that build's artifacts.

The runner reads each model's complete typed result and relation properties
through the SQL API. It checks the one-row `INT, DOUBLE` RAND result, the
consumer's current or retained value according to its observed decision, and
managed Delta relation properties. The passing test writes no Target relation;
its actual outcome is retained in `run_results.json`. The runner retains the
rendered State values alongside the Warehouse reads for subsequent comparison.
It does not substitute those values into SQL or infer hosted policy from a
physical result.

## Run after restoring compute

Build `record-proxy` first. Supply an existing schema lease in the format used
by `databricks_baseline.py`: `catalog: cdo_demo_db`, distinct `prod` and `ref`
roles naming existing `ds_<run-id>_ref` containers, `preexisting_objects` for
each schema, and `source_evidence` paths with SHA-256 digests. Each evidence
file must list `retained_schemas` and, for each schema, its observed
`full_name`, `owner` and `schema_id`. A fresh read of Unity Catalog metadata
can supply this evidence; no old machine's files are required. The runner
checks that ownership still matches, refuses collisions including temporary
names, and verifies that preexisting inventory remains present afterwards.

```bash
cargo build -p dbt-state-harness --bin record-proxy
python3 experiments/databricks_volatile.py \
  --dbt /path/to/dbt-2.0.11 --proxy target/debug/record-proxy \
  --schema-lease /path/to/lease.json --work-dir /path/to/fresh-capture
python3 experiments/databricks_volatile_test.py
```

The default profile is `databricks` in `~/.dbt/profiles.yml`; the default SQL
warehouse is `47fd3fa312624d91`. Override the profile, profile path and warehouse
explicitly when needed. Tokens remain in the existing local Databricks CLI
configuration and secret environment variables. Only the runner's own proxy
process is stopped. No schema creation or cleanup is performed. The runner is
offline-tested preparation; its native build/Explain path still needs execution.

The attempted live run read ownership and inventories successfully. The
warehouse was `STOPPED`; even `SELECT current_version()` was rejected. Its
start endpoint returned HTTP 400, `BAD_REQUEST`, with reason
`DENY_NEW_AND_EXISTING_RESOURCES`, domain `resource-gatekeeper`, and
`denyReason: INACTIVE`. A separate `SELECT 1` was also rejected. No dbt
invocation, warehouse mutation, or hosted State capture occurred. Local attempt
artifacts are at `~/projects/databricks-volatile-198-hosted`; ownership evidence
and the lease are at `~/.dbt/leases/volatile198/` on the capture machine.

## Remaining acceptance

Re-enable compute, use a fresh work directory, and capture both rounds. Compare
the State-rendered values with the independent Warehouse values and preserve
complete causal histories. Promote fixtures only after inspecting every cell.
Then add failing full-response replay and public protocol scenarios, including
Explain after a server restart, and implement only the observed response rules.
If captures require identity or Upstream producer changes, or relaxing a reuse
refusal, stop for the maintainer's decision under #198's check-in rule. Any
remaining mismatch is unfinished work under ADR 0009. No replay exception or
support claim is added by this preparation.

Preparation validation: the two new offline tests and thirteen shared runner
tests pass, as do workspace typechecking and formatting. The workspace suite
hits an existing failure in
`conformance::explain_skip_tree_matches_hosted_oracle_when_history_seeded`, also
reproduced in isolation: the unchanged server returns the "at least one upstream
dependency is unchanged" description where the unchanged test expects "within
lag tolerance". This preparation changes neither Rust code nor fixtures.
