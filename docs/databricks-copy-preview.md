# Databricks preview: Result copy disabled

The preview grants no Databricks Result copy by default. `RegisterClone`
returns successful gRPC transport with `unable_to_clone`, decision `1`
(BUILD), clone rejection reason `10`, empty description, no `ready_to_clone`
and no deprecated `ready_to_clone_v1` mirror. This uses the existing
copy-disabled response shape; it is a local preview policy, not a claim that
hosted Databricks returns that refusal. No copy execution is published.
Ordinary SQL and seed submits retain their existing no-automatic-copy policy.
Other warehouses retain their existing policies.

## Pinned client configuration

Use dbt **2.0.11**, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`,
with pre-copy disabled on the models and snapshots you run. The exact node
configuration used by existing native Databricks campaigns is:

```jinja
{{ config(state={'pre_clone': 'never'}) }}
```

Merge `pre_clone: never` into any existing `state` mapping so its other
settings are retained. Native Databricks table/view and incremental runs use this setting; see
[`databricks_baseline.py`](../experiments/databricks_baseline.py) and
[`databricks_append.py`](../experiments/databricks_append.py). The retained
snapshot campaign uses same-target runs and does not independently validate
a cross-target snapshot refusal fallback.
For a profile Target that enables cloning, also set `allow_clones: false`
on that output in `profiles.yml`:

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      # Keep the existing Databricks connection settings here.
      allow_clones: false
```

The pinned 2.0.11 wire evidence for `allow_clones: false` is retained in
[`explain_copy_logic_mismatch.jsonl`](../golden/fixtures/explain_copy_logic_mismatch.jsonl)
and [`explain_seed_copy_disabled.jsonl`](../golden/fixtures/explain_seed_copy_disabled.jsonl):
ordinary submits carry `allow_clones: false` and `clone_chain_depth_limit: 0`.
Those configuration captures are Snowflake; the native Databricks campaign
establishes `pre_clone: never`, rather than assuming that profile setting
alone suppresses Databricks pre-copy.

The default does **not** return gRPC `UNIMPLEMENTED`. The existing
[protocol contract](protocol.md#clone-cloneregisterclone) describes the normal
BUILD fallback for `unable_to_clone` reason `10`. The public gRPC regression
proves refusal followed by ordinary SQL submit returns BUILD, including when
the source would qualify for the retained copy path. A native Databricks
2.0.11 invocation consuming this new local refusal has not been run; use the
explicit no-pre-copy configuration above for this preview. No new live Cloud,
Databricks or DuckDB campaign was run for this policy change.

## Isolated experimental opt-in

`DBT_STATE_EXPERIMENTAL_DATABRICKS_DIRECT_COPY=true` enables only the retained
narrow `RegisterClone` implementation. It is outside preview support and
does not enable ordinary-submit copying. Turning it off prevents new copy
grants; it does not invalidate existing copy history, and previously recorded
opt-in copies can still qualify for narrow adoption. That history remains
outside preview support. The default is false; `1`, `true`
and `yes` (case insensitive) enable it. Embedded servers opt in through
`AppState::with_databricks_direct_copy(true)`.

Existing source qualification, provenance, confirmation and adoption checks
remain required under opt-in. Exact native copy fixtures and their public
regressions opt in explicitly; no fixture assertions are relaxed. Ordinary
FULL submits are separately tested to BUILD with a cross-target source under
both default and direct-copy opt-in configurations. The production migrations
and persisted provenance formats are unchanged by this policy switch.
