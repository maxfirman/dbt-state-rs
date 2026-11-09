# Architecture

Cargo workspace with four crates. The server depends on the proto crate; the
harness depends on both (dev/opt-in) to drive differential tests.

```
crates/proto     generated tonic stubs (server + client) from the .proto files
crates/server    the dbt State server (decision engine + Postgres store + gRPC)
crates/harness   recording proxy, golden fixtures, differential + fuzz tooling
crates/ui        optional read-only console, scoped to one configured organization
```

## proto crate

`build.rs` compiles the vendored `.proto` files with `tonic-prost-build`,
generating **both** server and client stubs (server for us, client for the proxy
and differential tests). Two notable build steps:

- **serde derives** are added to every message (`type_attribute`) so the proxy
  can emit JSON golden files that are human-diffable and replayable. The one
  `google.protobuf.Duration` field (`SessionEndRequest.session_duration`, in the
  unused ClientTelemetry service) is `serde(skip)`-ed.
- **`Clone` service collision patch.** The proto has a service literally named
  `Clone`; tonic generates a trait `clone_server::Clone` that shadows
  `std::clone::Clone` and makes the std-derive impl fail to compile. `build.rs`
  rewrites that one generated impl to `impl<T> std::clone::Clone for
  CloneServer<T>` — preserving the on-wire service name
  `com.fivetran.query_cache.Clone`.

Modules: `dbt_state_proto::query_cache` and `dbt_state_proto::grpc_health`.

## server crate

| Module | Responsibility |
|---|---|
| `config.rs` | Env config (`DBT_STATE_LISTEN`, `DATABASE_URL`). |
| `store.rs` | Postgres writes via sqlx: pending→confirmed lifecycle and atomic batch publication. |
| `store/history.rs` | Scoped State history reads: a target's current physical state, a data test's latest scoped outcome, copy candidates and historical-provenance lookups. Warehouse rule outcomes (candidate limit, provenance-walk depth) arrive as plain parameters; the store contains no dialect names. |
| `submit_decision.rs` | The Submit decision module: decides one SQL or values submission (node-state write, State history selection, `decide()`, the Result copy fallback and its precedence, including the clone-equivalent lookup driven by the Warehouse rules, id minting and the pending-row write) and returns one `SubmitOutcome` carrying its `DecisionRecord`. |
| `reuse_candidate.rs` | Pure reuse candidate selection: strict identity match or clone-origin adoption on the latest row, then reuse qualification. |
| `result_copy.rs` | Pure Result copy planning for the submit fallback and client-requested copies (RegisterClone). It asks the Warehouse rules for chain limits, relation checks, source evidence, the incremental contract and DDL. |
| `execution.rs` | Shared execution evidence, pending SQL/seed/copy recording, and atomic Record publication with selector/deferral state. |
| `decision.rs` | The pure decision engine (`decide()`), freshness, policy, descriptions. |
| `sql_identity.rs` | Shared parser/traversal and conservative DuckDB SQL identity policy. |
| `sql_norm.rs` | Snowflake SQL canonicalization (exact SQL on parse failure); strict + schema-abstracted (cross-environment) variants. |
| `warehouse.rs` | Interprets wire dialect and options; selects conservative SQL identity and physical warehouse family. The Warehouse rules answer each warehouse-specific question (fingerprint version tags and namespace identity; Result copy eligibility, DDL, relation checks, copy-chain limit, candidate limit, provenance walk, incremental contract), dispatching to `warehouse/snowflake.rs` and `warehouse/duckdb.rs`; other warehouses get conservative defaults (ADR 0004). |
| `fingerprint.rs` | Versioned SQL/dependency/context, seed, and schema-abstracted cross-environment logic fingerprints shared by Submit/Record. It computes identities only; the Warehouse rules choose version tags and decide copy eligibility. |
| `clone.rs` | Dialect-string entry point to the Warehouse rules' clone DDL. |
| `services.rs` | gRPC service impls. Submit handlers extract org/metadata, call `submit_decision`, map the outcome to the wire and capture. |
| `lib.rs` | `AppState` (pool + store), `build_router()` registering all services + health. |
| `main.rs` | Binary: connect, migrate, serve. |

`build_router()` is shared by the binary and the in-process integration tests,
so tests exercise the exact same wiring as production.

### Decision engine and persistence

`decide(ctx, confirmed) -> Verdict` stays pure, and so does the reuse candidate
selection that feeds it. Every verdict names exactly one decision reason.
Reuse candidate selection reports why the latest row was not a reuse
candidate, and `decide` reports the freshness outcome when there is one.
`submit_decision.rs` owns a whole submit: it records
the node's selector state (best effort), makes one scoped store read and lets
`reuse_candidate.rs` decide, on that row alone, whether it matches the
submit's identity and passes reuse qualification (physical target evidence or
complete cached test results). A Result copy is considered only when there is
no reuse candidate, and when one applies it overrides the verdict;
`result_copy.rs` decides whether and how a result is copied. Otherwise the
module calls `decide()`. It mints the request and execution decision ids,
writes the pending row and returns a `SubmitOutcome` (Skip, Execute, Copy or
the chain-limit Execute) carrying the matched execution, the Explain freshness
anchor, and a `DecisionRecord`: class, decision reason (including result copy
and copy chain limit), hosted description, both rejection codes, `is_stale`
and the matched execution id. SQL and seed submits share that tail. The
handlers build the wire response and the capture input from the record and
the request; nothing re-reads the response. Execution evidence owns identity construction and
pending-row contents; Record uses the same identity constructors and publishes
its complete batch with selector/deferral state atomically. The engine
compares exact physical input names and optional epochs;
unknown/missing/duplicate metadata builds, and the own target is excluded only
from upstream drift. Freshness drift is an upstream newer than the node's own
last-build epoch (ANY/ALL policy), verified live; `freshness_tolerance_seconds`
is intentionally not applied. See [protocol.md](protocol.md#current-decision-semantics)
and [correctness-handoff.md §C](correctness-handoff.md).

Physical history lookups share one query constructor in `store/history.rs`. It
selects the latest confirmed row for one target, or per target for copy candidates,
inside organization/warehouse-family/namespace scope. Data-test UID lookups and
historical copy-provenance walks retain their separate selection rules.

Model and seed submits make one read: the target's current physical state. The
strict identity match, clone-origin adoption and prior-build existence are all
decided on that row by `reuse_candidate.rs`, so an older row can never be
resurrected after a later build. Cached results and recorded inputs are decoded
only once the row is selected: result-serving matches still report corrupt
results as an internal error, while adoption and non-matching rows ignore them.
The Submit decision module returns database errors for the handler to
translate to the existing wire error. `services::sql_execution` remains a public re-export for upgrade tests.

`fingerprint.rs` length-frames a versioned SHA-256 fingerprint of SQL (or template
mode), dialect/default context, sorted semantic extras and supplied dependency
SQL/context. It also derives a schema-abstracted cross-environment logic hash
(`namespace_logic_hash`) used, scoped by `table_namespace`, for Snowflake clone reuse: the
same model built in different environments differs only by the physical schema
qualifier, which the abstraction collapses. DuckDB normalizes a small [validated formatting subset](duckdb-sql-identity.md);
other dialects retain exact SQL. A separate checked
[DuckDB table copy slice](duckdb-result-copy.md) substitutes only supported
environment context while retaining exact physical base inputs. Seeds share their own versioned configuration hash.
Unsupported SQL is retained exactly; old hashes cold miss rather than being
reinterpreted.

The physical execution scope is organization + warehouse family + client
`table_namespace` + exact target name. The namespace is derived by dbt from an
adapter-specific warehouse identifier. Selecting latest history *within that
physical scope* prevents a same-named table in another account/project/host from
shadowing this one; execution type, dialect options and logic are checked only
after selecting the current row. The namespace may be absent for nonstandard
clients, in which case that family has an isolated absent-namespace scope.

Migrations 0001–0012 define execution history, UI capture tables, selector and
deferral `node_states`, template fingerprints, and the generated
`warehouse_family` column/index. `executions` stores identity/context hashes
(including the cross-environment `node_namespace_logic_hash` and clone
provenance `clone_source_table` from 0008), optional freshness snapshots,
confirmed outcomes and complete protobuf result bytes. Models/seeds select the
latest confirmed physical target in the warehouse scope before matching its
fingerprint; Snowflake cross-environment clone reuse additionally matches by
namespace + abstracted logic. Data tests select by org/project/warehouse
scope/UID and then check logic/results. Pending rows do not permit reuse.
Confirm writes an outcome once; Record batches and their selector/deferral
state updates are transactional.

Every execution store query is org scoped from x-organization-id (default local).
UI reads use DBT_STATE_UI_ORG_ID (default local) in every query. These scope
boundaries do not provide authentication. Cross-target provenance, authoritative
selector/deferral state and concurrent physical lifecycle modelling remain in
[correctness-handoff.md](correctness-handoff.md).

### Lifecycle (submit → confirm → skip)

1. `SubmitEnrichedSQL`: look up confirmed history → `decide()`. On EXECUTE, mint
   a `request_id`, insert a `pending` row, return `ready_to_execute`.
2. The client builds the model, then `ConfirmExecution(request_id, …)` flips the
   row to `confirmed` with the outcome.
3. A later identical submit now finds a confirmed match and (if upstream fresh)
   returns `skip_execution`, echoing the recorded `execution_runtime_ms`.

## harness crate

See [harness.md](harness.md) and [testing.md](testing.md). Contains the
recording proxy (`bin/record_proxy.rs`), the opt-in differential-fuzz tool
(`bin/diff_fuzz.rs`, `--features fuzz`), golden/auth/diff support modules, and
the integration tests.
