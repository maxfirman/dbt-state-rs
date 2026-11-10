# dbt-state-duckdb-local

A DuckDB extension for the pinned `dbt-state` Python client, so real dbt Core
projects can use this repository's State server against a local DuckDB file.
Start with the [DuckDB workflow guide](../../docs/duckdb.md) for the quickstart,
pinned versions, support matrix and operations.

## Install and run

From the repository root, either sync the locked environment:

```sh
uv sync --project integrations/duckdb --locked
uv run --project integrations/duckdb dbt-state-duckdb build --profiles-dir /path/to/profiles
```

or install the package into your own Python 3.12 environment with
`uv pip install ./integrations/duckdb` and run `dbt-state-duckdb`.

`dbt-state-duckdb` installs the extension and then runs the normal dbt CLI;
plain `dbt` does not load it. `dbt-state-duckdb reset --database <file>`
resets State evidence without contacting the server.

Set these for local mode:

```sh
export DBT_STATE_LOCAL=1
export RUN_CACHE_API_URL=127.0.0.1:50051   # localhost, 127.0.0.1 or [::1] with a port
export RUN_CACHE_API_SECURE=false
```

Without `DBT_STATE_LOCAL=1`, the extension does nothing and the client's
own adapter and authentication behavior apply. With it, `client.py::install`
refuses a non-loopback URL or a secure channel, registers the extension for
the `duckdb` adapter, skips the client's hosted SSO step for DuckDB runs,
replaces the warehouse namespace with the file/generation scope, and adds the
local qualification, completion and recovery steps. The profile must point at
a persistent local file; in-memory and remote paths are refused. The client
logs an UNIMPLEMENTED warning for ClientTelemetry, which this server does not
serve.

## Modules

| Module | Responsibility |
| --- | --- |
| `__init__.py` | `DuckDBAdapterExtension`, `install()`, `main()` (CLI and `reset`) |
| `client.py` | Every patch to the pinned client, in order |
| `qualification.py` | SQL, seed and RegisterClone request qualification; workload admission |
| `evidence.py` | Ordinal evidence store and table fingerprints |
| `views.py` | View fingerprints, dependency traversal, SQL eligibility subset, execution context |
| `hooks.py` | Hook contract marker and the supported post-hook subset |
| `incremental.py` | `delete+insert` contract, self-read proof, initialization identity |
| `recovery.py` | File lock, generation, dirty marker, `reset` |
| `completion.py` | Completion evidence before confirmation |
| `data_tests.py` | Completed data-test invocation outcomes |
| `copy.py` | CTAS capability checks and source inspection |
| `result_copy.py` | Result copy attempt lifecycle for both entry paths |

## Tests

```sh
# Postgres must be running; DATABASE_URL defaults to localhost:55441/dbtstate.
./scripts/duckdb-smoke.sh     # duckdb_python real-dbt tests
./scripts/duckdb-ci.sh --jobs 4   # the full DuckDB gate
```

The harness sets `PYTHONPATH` and the local-mode variables itself. To run one
target directly, set `DBT_STATE_DUCKDB_PYTHON` (and `DBT_STATE_SERVER_BIN` for
`duckdb_generated`). The scripts under `tests/` are drivers for the Rust
targets in `crates/harness/tests`.

## Reference

- [Evidence and eligibility](../../docs/duckdb-evidence.md)
- [SQL identity and result copies](../../docs/duckdb-identity-and-copy.md)
- [Incremental models](../../docs/duckdb-incremental.md)
- [Generated checks](../../docs/duckdb-generated.md)
