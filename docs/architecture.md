# Architecture

Cargo workspace with three crates. The server depends on the proto crate; the
harness depends on both (dev/opt-in) to drive differential tests.

```
crates/proto     generated tonic stubs (server + client) from the .proto files
crates/server    the dbt State server (decision engine + Postgres store + gRPC)
crates/harness   recording proxy, golden fixtures, differential + fuzz tooling
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
| `decision.rs` | The pure decision engine (`decide()`), freshness, policy, normalization. |
| `clone.rs` | Dialect-aware clone-DDL generation. |
| `services.rs` | gRPC service impls binding the store + decision engine to the wire. |
| `lib.rs` | `AppState` (pool + store), `build_router()` registering all services + health. |
| `main.rs` | Binary: connect, migrate, serve. |

`build_router()` is shared by the binary and the in-process integration tests,
so tests exercise the exact same wiring as production.

### Decision engine (`decision.rs`)

`decide(ctx, confirmed) -> Verdict` is a **pure function** — no I/O — which makes
it unit- and property-testable. Inputs are distilled into a `SubmitContext`
(execution_type, node_body_hash, input_tables, freshness tolerance,
target_table, stale_upstream_policy). Key helpers:

- `is_stale()` — freshness evaluation honoring `StaleUpstreamPolicy` (ANY/ALL),
  excluding the node's own target table, comparing upstreams by logical identity.
- `logical_relation_key()` — strips the environment (schema) component of a
  fully-qualified relation so `"DB"."PROD"."T"` and `"DB"."DEV"."T"` compare
  equal (cross-environment reuse).

See [protocol.md](protocol.md#decision-semantics-as-reproduced) for the exact
rules. The verdict is turned into the proto response shape in `services.rs`.

### Store & schema (`store.rs`, `migrations/`)

Single table `executions` (see `migrations/0001_init.sql`, `0002_values_hash.sql`):

- Identity/match columns: `org_id`, `target_table`, `execution_type`,
  `node_hash`, `node_body_hash`, `node_configs_hash`, `node_contract_hash`,
  `node_unique_id`, `table_namespace`, `values_hash`, `dialect`.
- Recorded outcome: `last_modified_epoch`, `table_type`, `execution_runtime_ms`.
- Upstream freshness snapshot: `input_tables` (JSONB array of
  `{name, last_modified_epoch}`).
- Lifecycle: `status` (`pending` after a verdict, `confirmed` after
  `ConfirmExecution`), `request_id` (unique, correlates confirm), timestamps.

Indexes: physical-fingerprint lookup, unique `request_id`, node-uid, and the
values-hash lookup. Lookups:

- `find_confirmed_by_namespace()` — primary: logical cross-env match on
  `table_namespace`+`node_body_hash`+`execution_type` (org-scoped).
- `find_confirmed()` — fallback: physical `target_table`+`node_body_hash`.
- `find_confirmed_by_unique_id()` — data-test nodes (`execution_type=8`), keyed
  on `node_unique_id` (their `node_body_hash` collides across tests of the same
  type and `target_table` is empty).
- `find_confirmed_values()` — seeds, keyed on `values_hash`.
- `insert_pending()` / `confirm()` — the submit→confirm lifecycle.
- `insert_confirmed_batch()` — `RecordExecutions` atomic hydrate (single tx).

**Org isolation** is enforced in every query (`org_id` from the
`x-organization-id` metadata header, defaulting to `local` for insecure local
runs). State never crosses orgs.

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
