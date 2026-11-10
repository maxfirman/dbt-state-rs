# Test harness

`crates/harness` captures real traffic between the official dbt client and the
hosted dbt State service (`api.state.dbt.com`), and provides tools to compare
this server against it. Wire semantics and the captured protocol facts live in
[protocol.md](protocol.md); test strategy and the replay rules live in
[testing.md](testing.md).

## Components

| Binary (`crates/harness/Cargo.toml`) | Source | Purpose |
|---|---|---|
| `record-proxy` | `src/bin/record_proxy.rs` | Recording proxy (below). |
| `state-probe` | `src/bin/state_probe.rs` | Sends one crafted RPC (`submit`, `values`, `confirm`, `explain`) read as golden-format JSON from stdin, through the proxy, and prints the response. Used by `experiments/lag_matrix.py` and `seed_upstream.py` for synthetic-epoch sequences that need no warehouse. |
| `diff-fuzz` | `src/bin/diff_fuzz.rs` | Opt-in differential discovery against the hosted service; needs `--features fuzz` ([testing.md](testing.md#hosted-discovery-differential-fuzz)). |

### Recording proxy

An insecure local tonic server for the SQL, Execution, Clone,
ClientValidation, Explain, SelectorService and ClientTelemetry services. The
dbt client points at it. Each call is forwarded over TLS to the hosted service
with the client's own metadata (`x-submitted-at-epoch`, `x-dbt-invocation-id`,
and so on; transport and auth headers excluded) plus a minted Bearer token. The
proxy returns the hosted response unchanged and appends one JSON line per call
to `golden_<timestamp>.jsonl`.

- `src/auth.rs` reads the active host and token from `~/.dbt/dbt_cloud.yml`,
  performs the dbt platform token exchange, caches the token with 60s expiry
  slack, and extracts the organization ID from the
  `runcache:scope:org:<ORG_ID>:...` scope.
- `src/golden.rs` defines `GoldenEntry` {`service`, `method`, `recorded_at`,
  `metadata`, `request`, `response`} and the append-only writer.
- Environment: `PROXY_LISTEN` (default `127.0.0.1:50099`), `PROXY_UPSTREAM`
  (default `https://api.state.dbt.com:443`), `GOLDEN_DIR` (default `./golden`).

Older fixtures were captured before the proxy forwarded client metadata, so
the hosted service did not receive those headers. Their recorded `metadata`
still shows what the client sent.

## Running a capture

```bash
cargo build -p dbt-state-harness
RUST_LOG=info GOLDEN_DIR=./golden PROXY_LISTEN=127.0.0.1:50099 ./target/debug/record-proxy

# In the dbt project (a Snowflake target; DuckDB does not trigger dbt State):
DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL=127.0.0.1:50099 RUN_CACHE_API_SECURE=false \
  dbt build --profile snowflake --target prod_demo --select <model> --skip-semantic-manifest-validation
```

To force a build, make a semantic SQL change such as adding a column. Comment
or whitespace edits are normalized away and still SKIP
([sql-normalization.md](sql-normalization.md)). For an incremental model,
`--full-refresh` makes the client bypass dbt State (no submit).

Promote a useful capture into `golden/fixtures/` and drive it from a test
([golden/README.md](../golden/README.md)).

## Experiment drivers (`experiments/`)

The drivers start from a clean `jaffle-shop` tree, run one focused scenario
against `--target prod_demo` or `dev_clone` (`defer_to_target: prod_demo`),
print the newly captured decisions and `git restore` the project. They only
ever mutate `CDO_DEMO_DB`.

| Area | Drivers |
|---|---|
| Proxy and generic runs | `proxy_up.sh` (start or restart the proxy; captures go to `$GOLDEN_DIR`, default `/tmp/conf_golden`), `run.sh LABEL SELECT [EDIT_PY] [EXTRA…]` |
| Result copy (clone) | `clone_experiment.sh LABEL {incremental\|table\|snapshot} [--fresh-source] [--no-clones]`, `clone_logic_sensitivity.sh`, `clone_eligibility.sh`, `clone_fallback_xenv.sh`, `clone_fallback_control.sh`, `clone_fallback_freshness.sh`, `clone_chain_provenance.sh`, `registerclone_source_age.sh`, `p0_fallback_over_existing.sh`, `physical_state_exec_type.sh` |
| Dependency and SQL identity | `finding3_*.sh`, `quoted_identifier_case.sh`, `resolution_context.sh` |
| Lag tolerance | `lag_experiment.sh LABEL {any\|all} LAG {none\|a\|b\|both}`, `lag_buildage.sh`, `lag_isolate.sh`, `lag_docs_example.sh` (`DIRECT=1` runs without the proxy as a control), `lag_matrix.py ARM…`, `copy_candidates.py`, `seed_upstream.py` |
| Data tests and templates | `data_test_freshness.sh`, `data_test_freshness_control.sh`, `data_test_template.sh`, `template_comparison.sh` |
| Selectors and deferral | `selector_experiment.sh {baseline\|query CRIT\|mutate CRIT EDIT\|all-clean}`, `selector_last_write_wins.sh`, `deferral_experiment.sh` |
| Explain | `explain_experiment.sh`, `explain_*_history.sh`, `explain_tolerance.sh`, `explain_clone_deferral.sh`, `explain_dependency_set_changed.sh`, `explain_dep_set_*.sh`; `explain_slice.py` renders captures |
| Inspection and validation | `golden_tail.py FILE N [PREFIX]`, `lag_show.py`, `lag_rule_check.py [files]`, `trace_validate.py` and `oracles.py` (correlate captured source, clone and confirm evidence; tested by `trace_validate_test.py`) |

`lag_experiment.sh` bumps upstreams by External modification of dbt-built
tables, which hosted treats as an upstream query change; `lag_matrix.py`
covers the representative Lag tolerance cases with crafted calls.

## Live clone runs

Forcing the dbt client to take a result copy against this server live depends
on warehouse state (the client only asks to clone when the target relation is
absent). Replay of captured traffic is evidence for the captured cases only;
see [testing.md](testing.md#captured-traffic-replay).
