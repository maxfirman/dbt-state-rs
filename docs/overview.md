# Overview

## What dbt State is

dbt State (GA ~2026, also called "state-aware orchestration") makes dbt build on
*change* instead of on schedule. On every run, for each node, dbt consults a
decision service: has the node's **logic** (its SQL) or its **upstream data**
changed since a prior recorded build? If not, dbt reuses the previous result —
it **skips** the build (a "no-op"), **clones** an existing object from another
environment, or **defers** to it.

dbt Labs ships this as a hosted, metered gRPC service at `api.state.dbt.com`.
The dbt client (dbt Core's Python client and the Rust "Fusion"/dbt v2 engine) is
only a thin client; with no credentials it disables itself and dbt runs vanilla.
The decision engine — the interesting part — is server-side and closed-source.

`dbt-state-rs` is an independent, self-hostable reimplementation of that server.

## The client/server split (and where SQL is parsed)

The protocol is internally called the **"query cache"** (proto package
`com.fivetran.query_cache`). The split is deliberate and important:

- **The client** computes most things and sends them pre-digested:
  - `node_body_hash`, `node_hash`, `node_configs_hash`, `node_contract_hash`
    (fingerprints of the model) inside a `dbt_node_state` message;
  - a `table_namespace` grouping hash (an adapter-level id);
  - per-input-table freshness (`last_modified_epoch`);
  - the **raw compiled SQL** and a `semantic_extras` map of config;
  - for seeds, an md5 `values_hash` of the seed bytes.
- **The server** owns the decision: it derives the node's *logic identity* from
  the **rendered SQL** (not the client's `node_body_hash`, which it ignores for
  reuse) plus the allowlisted `semantic_extras`, matches that against recorded
  history, evaluates upstream freshness, and returns BUILD / SKIP / CLONE. It
  then records confirmed outcomes so future runs skip.

Because the logic identity is the rendered SQL, the server **does normalize
SQL** — but we established by live experiment that this is an *AST
canonicalization* (comments, whitespace, case, operator/cast/type/function
synonyms), **not** a logical-plan comparison. We reproduce it with
`sqlparser` (apache/datafusion-sqlparser-rs); see
[protocol.md](protocol.md) §"SQL normalization" and
[`experiments/SQL_NORMALIZATION.md`](../experiments/SQL_NORMALIZATION.md). The
CLONE DDL is the only SQL the server *generates*, from a simple dialect template.

## Project goals

1. Faithfully reproduce the hosted service's decisions (skip/execute/clone/defer).
2. Keep state in a backend you control (Postgres here).
3. Be validatable against the real service, not just against our own assumptions.

## How faithfulness is ensured

Three independent checks (see [testing.md](testing.md)):

- **Conformance replay** of real captured traffic (regression gate).
- **Property-based invariants** over the pure decision function.
- **Differential fuzzing** that sends mutated requests to both the real service
  and ours and diffs the decisions (opt-in discovery tool).

## Scope and known limits

Implemented and validated (live, against the real service): `SubmitEnrichedSQL`
(skip/execute with AST-canonicalized SQL + `semantic_extras` matching),
`SubmitValues` (seeds, `values_hash`), data tests (matched by `node_unique_id`),
snapshots/incremental/view/custom-materialization semantics, `ConfirmExecution`,
`RecordExecutions`, `RegisterClone` (with clone-DDL generation),
cross-environment reuse by `table_namespace`, `compare_unrendered_code`,
node-type-specific `decision_description` strings, `ClientValidation`, `Health`,
and conservative defaults for `Explain`, `SelectorService`,
`ResolveDeferredRelations`, and speculative submits.

Known bounded gaps (all safe-directional — we over-execute, never serve stale):
- The SQL type/function **synonym catalogs** cover the common Snowflake aliases
  verified live but are not exhaustive; an unlisted synonym falls through.
- A plain `SubmitEnrichedSQL` answered with `ready_to_clone` by the hosted
  service depends on physical warehouse state not in the protocol (characterized,
  not reproduced).
- Rich `Explain` text, `transformed_nodes_by_query`, `query_hash_metadata_info`,
  and non-Snowflake clone DDL verified against the real service.

See [protocol.md](protocol.md#coverage) for the full matrix and
[`experiments/SCENARIOS.md`](../experiments/SCENARIOS.md) for the live-verified
scenario table.
