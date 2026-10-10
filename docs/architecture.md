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
| `store.rs` | Postgres access via sqlx: lookups, pending→confirmed lifecycle, batch hydrate. |
| `decision.rs` | The pure decision engine (`decide()`), freshness, policy, descriptions. |
| `sql_norm.rs` | Snowflake SQL canonicalization (exact SQL on parse failure) for the logic-identity match. |
| `fingerprint.rs` | Versioned SQL/dependency/context and seed fingerprints shared by Submit/Record. |
| `clone.rs` | Dialect-aware clone-DDL generation. |
| `services.rs` | gRPC service impls binding the store + decision engine to the wire. |
| `lib.rs` | `AppState` (pool + store), `build_router()` registering all services + health. |
| `main.rs` | Binary: connect, migrate, serve. |

`build_router()` is shared by the binary and the in-process integration tests,
so tests exercise the exact same wiring as production.

### Decision engine and persistence

`decide(ctx, confirmed) -> Verdict` stays pure. The store/service validate the
candidate fingerprint, physical target and cached test result before invoking
it. The engine compares exact physical input names and optional epochs;
unknown/missing/duplicate metadata builds, and the own target is excluded only
from upstream drift. Existing ANY/ALL and lag-tolerance policy remain unchanged.
See [protocol.md](protocol.md#current-decision-semantics).

`fingerprint.rs` length-frames a versioned SHA-256 fingerprint of SQL (or template
mode), dialect/default context, sorted semantic extras and supplied dependency
SQL/context. Seeds share their own versioned configuration hash. Unsupported SQL
is retained exactly; old hashes cold miss rather than being reinterpreted.

Migrations 0001–0007 provide execution history and separate UI capture tables.
`executions` stores identity/context hashes, optional freshness snapshots,
confirmed outcomes and complete protobuf result bytes. Models/seeds select the
latest confirmed physical target before matching its fingerprint. Tests select
by org/project/namespace/UID and then check logic/results. Pending rows do not
permit reuse. Confirm writes an outcome once; Record batches are transactional.

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
