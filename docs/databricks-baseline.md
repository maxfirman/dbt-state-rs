# Databricks table/view acceptance

The #135 baseline uses the two #132-confirmed seams: hosted captures replayed
through the production gRPC router with isolated Postgres history, and the
pinned real dbt client running against that router and Databricks. It is one
table/view lifecycle slice; the [capability survey](reports/databricks-capabilities-2026-11-12.md)
and its copy/snapshot blockers remain separate.

[The runner](../experiments/databricks_baseline.py) extends the survey's guarded
profiles, schema ownership, SQL Statement Execution, process ownership and
artifact retention. It pins dbt 2.0.11 by SHA-256, checks the CLI/profile
workspace and warehouse agree, and normally creates two fresh run-owned schemas
under `cdo_demo_db`. A schema collision fails before building. It retains
successes and failures; it does not delete warehouse relations or overwrite an
existing artifact directory. Schema quota failures are environment failures,
not hosted-policy observations.

When schema quota prevents new containers, `--schema-lease /path/to/ledger.json`
uses two explicitly leased prior campaign reference schemas. The ledger names
the exact catalog/schema allowlist, hashed original ownership configurations
and preexisting object inventory. The runner verifies those hashes and actual
inventory before starting. It permits only the new run's prefixed table/view
aliases, refuses any existing run-prefixed object (including intermediate
names), and checks the original inventory remains present afterward. It never
creates/drops schemas or modifies prior objects in lease mode. Fresh physical
relations and State histories are still required. The integration test accepts
the same ledger via `DBT_STATE_DATABRICKS_SCHEMA_LEASE`.

Both a managed Delta table and a view run through first build, unchanged
reuse, changed-query rebuild, and unchanged reuse of the changed query. The
literal workload declares BIGINT/INT/STRING columns, duplicate rows and NULLs.
Its string literal contains a fresh query identity, preventing earlier campaign
SQL from becoming hidden hosted copy history. `--query-identity` (or
`DBT_STATE_DATABRICKS_QUERY_IDENTITY` in the integration test) lets the Rust
phase use the hosted phase's exact declared data while keeping fresh physical
relation names and isolated State history.
Every step requires the expected node-level decision and successful build
confirmations, and captures Explain for both actual decision IDs. Always
building cannot satisfy the unchanged steps.
The unchanged step also sends the same Explain IDs in reversed order. Hosted
returns entries in ascending decision-ID order; the shared handler and replay
assert that contract separately from each entry's complete tree.

Each managed step is checked against a separate project/profile target running
dbt with State disabled, an independent warehouse SELECT, and literal expected
rows/schema. The checker compares complete row multisets, ordered column
metadata and required relation properties separately: MANAGED/Delta for the
table, VIEW for the view. Missing schema/count/chunk metadata, truncated
results and inconsistent typed cells fail physical reads; successful DDL may
have no result. The runner finally corrupts its own table and requires the
precise `physical:rows` failure. Credential-free probes additionally reject a
wrong SKIP after the query change, lost duplicate, changed NULL, changed value,
wrong column name/order/type, wrong relation kind and wrong provider.

Run the hosted phase first:

```bash
cargo build -p dbt-state-harness --bin record-proxy --bin state-probe
uv run --with pyyaml python experiments/databricks_baseline.py \
  --dbt /path/to/pinned/dbt-2.0.11 \
  --proxy "$PWD/target/debug/record-proxy" \
  --probe "$PWD/target/debug/state-probe" \
  --work-dir /path/to/new-hosted-baseline
```

The selected `databricks` dbt profile must target `cdo_demo_db`; the Databricks
CLI token comes from its existing configuration. Hosted recording additionally
uses the existing dbt Cloud credential. Raw captures retain all RPCs, including
telemetry durations; a clean proxy shutdown must certify identical started,
recorded and durable counts. Only a complete focused capture is promoted to
`golden/fixtures/`, where ordinary `cargo test --workspace` replays it without
cloud access. New Databricks replay permits no unknown baseline, known gap or
partial-history exception, and compares full Explain text and relation names.

After hosted replay passes, run real dbt against a fresh isolated production
router with the opt-in integration test:

```bash
uv venv /path/to/acceptance-python
uv pip install --python /path/to/acceptance-python/bin/python pyyaml
export DBT_STATE_DATABRICKS_PYTHON=/path/to/acceptance-python/bin/python
export DBT_STATE_DATABRICKS_DBT=/path/to/pinned/dbt-2.0.11
export DBT_STATE_DATABRICKS_ARTIFACTS=/path/to/new-rust-baseline
cargo test -p dbt-state-harness --test databricks_baseline \
  real_dbt_table_and_view_match_independent_reference -- --ignored --nocapture
```

The test owns the server's ephemeral listener and Postgres schema. Its recording
proxy uses `PROXY_LOCAL_UPSTREAM=http://127.0.0.1:PORT`, an explicit local mode
that bypasses cloud credential loading and token exchange. Only canonical
literal loopback HTTP addresses with an explicit nonzero port are accepted;
hostnames, external destinations and URL credentials/paths are rejected. Hosted
TLS/authentication defaults are preserved. Databricks credentials are still
needed for the real warehouse. Ordinary CI runs the replay and stdlib Python
fault probes, and leaves this cloud test ignored.
