# Architecture

Cargo workspace with four crates. The server depends on the proto crate; the
harness depends on both to drive replay and differential tests.

```
crates/proto     generated tonic stubs (server + client) from the .proto files
crates/server    the dbt State server (decision engine + Postgres store + gRPC)
crates/harness   recording proxy, state probe, golden fixtures, differential + fuzz tooling, tests
crates/ui        optional read-only console, scoped to one configured organization
```

## proto crate

`build.rs` compiles the vendored `.proto` files with `tonic-prost-build`,
generating **both** server and client stubs (server for us, client for the proxy
and differential tests). Two notable build steps:

- **serde derives** are added to every message (`type_attribute`) so the proxy
  can emit JSON golden files that are human-diffable and replayable. The one
  `google.protobuf.Duration` field (`SessionEndRequest.session_duration`) uses
  a lossless optional seconds/nanoseconds serializer. Older captures omitted
  it; the omission still deserializes as absent.
- **`Clone` service collision patch.** The proto has a service literally named
  `Clone`; tonic generates a trait `clone_server::Clone` that shadows
  `std::clone::Clone` and makes the std-derive impl fail to compile. `build.rs`
  rewrites that one generated impl to `impl<T> std::clone::Clone for
  CloneServer<T>`, preserving the on-wire service name
  `com.fivetran.query_cache.Clone`.

Modules: `dbt_state_proto::query_cache` and `dbt_state_proto::grpc_health`.

## server crate

| Module | Responsibility |
|---|---|
| `config.rs` | Env config: `DBT_STATE_LISTEN` (default `127.0.0.1:50051`), `DATABASE_URL`, `DBT_STATE_REQUIRE_ORG_HEADER` (reject requests without `x-organization-id`). |
| `lib.rs` | `AppState` (`store`, `require_org_header`, `clock`), `Clock` (`System`, test-only `Replay`), `build_router()`. `AppState::connect` opens the pool and runs the migrations. |
| `main.rs` | Binary: tracing setup, `Config::from_env`, `AppState::connect`, serve. |
| `services.rs` | gRPC handlers. They extract org and metadata, call `submit_decision` or the store, map outcomes to the wire (both clone oneofs) and spawn capture. Re-exports `sql_execution` for upgrade tests. |
| `submit_decision.rs` | How one SQL or values submit is decided (flow below), and the speculative submits that are decided through the same code. Returns a `SubmitOutcome` carrying its `DecisionRecord`. |
| `reuse_candidate.rs` | Pure Reuse candidate selection on the latest row (strict identity match or copy-origin adoption), then Reuse qualification. Reports why there is no candidate, and which build an execution type change is judged against. |
| `decision.rs` | The pure decision engine (`decide`, `reusable`), per-upstream freshness (`upstream_freshness`), decision reasons and hosted descriptions. |
| `decision_proptest.rs` | Property tests for `decide` (a `#[cfg(test)]` submodule of `decision`). |
| `result_copy.rs` | Pure Result copy planning for the submit fallback, the Snowflake seed copy on submit (#91) and RegisterClone, and the rejection codes of a submit that is not copied. Asks the Warehouse rules which execution types a submit may copy, and for chain limits, relation checks, source evidence, the incremental contract and DDL. |
| `execution.rs` | Execution evidence: identity construction, pending SQL/seed/copy rows, and atomic RecordExecutions publication with selector/deferral state. |
| `explain.rs` | Pure re-derivation of `dbt state explain` trees from a persisted node decision and its matched execution ([explain-catalog.md](explain-catalog.md)). |
| `selector.rs` | Pure engine for `state:*` selectors (`SelectorService.GetStateSelection`). |
| `capture.rs` | Best-effort, non-blocking write of the UI domain rows and one `node_decisions` row per decision. |
| `fingerprint.rs` | Versioned SQL/dependency/context, seed and schema-abstracted logic fingerprints shared by Submit and Record. Computes identities only. |
| `sql_identity.rs` | Shared SQL parsing/traversal feeding the dialect identity rules. |
| `sql_norm.rs` | Restricted Snowflake AST canonicalization; unsupported syntax stays exact ([sql-normalization.md](sql-normalization.md)). |
| `warehouse.rs`, `warehouse/` | Interprets the wire dialect. The Warehouse rules answer each warehouse-specific question (fingerprint version tags, namespace identity, Result copy eligibility, DDL, relation checks, chain and candidate limits, provenance walk, incremental contract, build time), with `warehouse/snowflake.rs` and `warehouse/duckdb.rs`; other warehouses get conservative defaults (ADR 0004). |
| `store.rs` | Postgres via sqlx: pending inserts, confirm-once, Record batches with `node_states` in one transaction, `node_states` upsert/fetch for selectors, confirmed execution scope reads for deferral, and `fetch_explain_decisions` (node decisions with their matched execution) for Explain. |
| `store/history.rs` | Scoped State history reads: a Target relation's current physical state (one or many), its history, a data test's latest scoped outcome, copy candidates, copy-chain depth and copy-source lookups. Warehouse rule outcomes arrive as parameters; the store holds no dialect names. |

### Registered services

`build_router()` registers, against one shared `AppState`:

| Service | Methods |
|---|---|
| `grpc.health.v1.Health` | reports `SERVING` |
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` (`skip_execution` only for an unchanged data test without upstreams, otherwise `undecided`) |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` |
| `Clone` | `RegisterClone` |
| `ClientValidation` | `ValidateClientVersion` |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` (empty stub) |
| `SelectorService` | `GetStateSelection` |

The binary and the in-process integration tests share `build_router()`, so tests
exercise the production wiring. Wire semantics are in [protocol.md](protocol.md).

### Submit flow

`submit_decision::submit_sql`:

1. Record the node's selector state in `node_states` (best effort; a failure
   is counted, never fails the submit).
2. Build the execution evidence and resolve each input's Upstream producer
   from State history (`resolve_producers`).
3. Read State history once: the Target relation's current physical state, or a
   data test's latest outcome by scoped UID. `reuse_candidate` decides on that
   row alone whether it is a Reuse candidate.
4. With no Reuse candidate (and no External modification of the Target
   relation), try the Result copy fallback. A copy overrides the verdict.
   Only the execution types the Warehouse rules copy are copied (FULL; DuckDB
   also delete+insert under its contract). Another kind with an equivalent
   keeps the ordinary verdict and reports the equivalent in its rejection
   codes (`with_refused_copy`).
5. Otherwise call `decide()`, with the build age measured on `AppState::clock`.
6. If the Reuse candidate must rebuild for upstream freshness, try a Result
   copy of a same-upstream equivalent over the existing Target relation.
7. For a rebuild, report an older build that would still qualify
   (`report_older_match`).
8. `settle`: mint the ids, write the pending row for every outcome except SKIP,
   and return the `SubmitOutcome`.

`submit_values` (seeds) follows steps 1, 3, 5, 7 and 8: no producers, no Result
copy, no Lag tolerance, ANY policy.

`submit_decision::speculative_sql` (#103) runs the same code for a data test
with no Target relation and no reported tables, and keeps only a SKIP: it
stops, before anything is written, after step 3 when there is no Reuse
candidate and after step 5 when the verdict is not SKIP, and answers
`undecided`. It records the node state (step 1) only for the SKIP. Every
other speculative submit is `undecided` without a read.

### Physical scope rule

The physical execution scope is organization + warehouse family + client
`table_namespace` + exact Target relation name. dbt derives the namespace from
an adapter-specific warehouse identifier. Selecting the latest row *within that
scope* stops a same-named relation in another account, project or host from
shadowing this one; execution type, dialect options and logic are checked only
after the current row is selected, so an older row is never resurrected after a
later build. One exception for which row a rebuild is judged against: when the
current row has the submit's identity under another execution type and
recorded the submit's genuine upstream names, the latest
row of the submit's type is (`submit_decision::select_sql`, ADR 0007); it is
never reused. Other upstream names are another identity (#116). A client without a namespace gets an isolated absent-namespace
scope per family. Empty strings share that physical scope, including legacy
confirmed and pending rows. Source-less Databricks rejection/Explain reads can
return query diagnostics from outside a physical reuse scope; those reads
return only booleans or a name and never feed a reuse/copy candidate, producer
or freshness anchor. A hosted absent-to-named fallback conflicts with this
required self-hosted isolation and remains [an explicit incomplete
contract](reports/databricks-isolation-2026-11-13.md). Data tests select by org, project, warehouse scope and UID.
Every store query is org-scoped; neither this nor the UI's
`DBT_STATE_UI_ORG_ID` filter is authentication.

### Store tables

Schema is defined by `crates/server/migrations/` (applied on startup).

| Table | Contents |
|---|---|
| `executions` | State history: pending and confirmed rows with identity/context hashes, generated `warehouse_family`, `table_namespace`, recorded inputs with producers, outcome, cached result bytes, copy provenance (`clone_source_table`, `node_namespace_logic_hash`; a RegisterClone copy also keeps its source build's runtime and recorded upstream names, `clone_source_inputs`). |
| `node_states` | Latest client-supplied node hashes per (org, project, Target, node) for `state:*` selectors, plus retained `target_table` snapshot metadata. |
| `node_decisions` | One row per decision: class, description, rejection codes, `is_stale`, `decision_reason`, `matched_execution_id`, Lag tolerance and build age, decision-time inputs; for a view, whether its SQL reads a relation (`reads_relation`, #117). Read by Explain and the UI. |
| `organizations`, `projects`, `environments`, `invocations` | UI capture domain. An `environments` row is a dbt Target. |
| `actors`, `roles`, `role_bindings` | Reserved for future auth; nothing writes them. |

### Lifecycle

1. A submit that returns SKIP writes no `executions` row. EXECUTE, CLONE
   (`ready_to_clone`) and the chain-limit EXECUTE write a `pending` row keyed
   by `request_id`; `RegisterClone` does too.
2. `ConfirmExecution(request_id, …)` flips the row to `confirmed` with the
   outcome and the server receipt time, exactly once. A repeat confirm returns
   INTERNAL (`Database operation failed`) without rewriting the outcome. A
   missing valid UUID returns NOT_FOUND; malformed IDs return INTERNAL.
3. `RecordExecutions` writes a batch of confirmed rows and their `node_states`
   in one transaction, after validating the complete batch. A physical target
   requires an outcome epoch; an absent target requires an absent epoch.
4. Later submits consider only confirmed rows. A SKIP echoes the recorded
   `execution_runtime_ms`. Physical current state uses the greatest completion
   epoch, with confirmation receipt order breaking equal epochs. A legacy
   confirmed row with an unknown epoch blocks physical reuse and copying even
   when external modifications are ignored; later delivery cannot prove recovery.
   Logical test/deferral histories and historical copy provenance retain their
   separate ordering rules.

## ui crate

`crates/ui` (bin `dbt-state-ui`) reads the capture tables read-only. See
[ui.md](ui.md).

## harness crate

The recording proxy (`record-proxy`), the single-RPC `state-probe` used by
live characterization drivers, the opt-in `diff-fuzz` tool (`--features fuzz`),
golden/auth/diff support modules and the integration tests. See
[harness.md](harness.md) and [testing.md](testing.md).

### Submit clock for Explain

Databricks SQL submits retain nullable `display_submitted_at_ms` on their
execution and node-decision rows (migration 0022). Explain uses this frozen
instant for temporal text, including identity-change rebuilds without a reuse
anchor. A captured FULL upstream with a missing current epoch displays the
current submit instant; a missing recorded epoch uses the original build
submit instant only when it was retained. Both raw epochs remain unknown.
These columns never participate in freshness, producer selection, physical
history ordering or fingerprints, and legacy rows are never backfilled.
