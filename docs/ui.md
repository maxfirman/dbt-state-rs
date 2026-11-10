# UI console

A read-only web console for the dbt State server, built with
[Topcoat](https://tokio.rs/blog/2026-07-22-announcing-topcoat) (server-rendered
Rust, no WASM). It shows what dbt State decided across projects and Targets. It
never executes dbt and never writes to the database.

A Target is the dbt profile output name the client sends (`target_name`). The
UI shows it literally rather than inventing a server-side environment model;
the underlying table is still called `environments`.

## Pages

Routes come from the module tree under `crates/ui/src/app/` (`module_router!`).

| Route | Source | Shows |
|---|---|---|
| `/` | `app.rs::home` | Reuse rate, built/reused/cloned totals, recent invocations. |
| `/projects` | `app/projects.rs::index` | Every project with Target counts and totals. |
| `/projects/{project_id}` | `app/projects.rs::project_id::detail` | The project's Targets with dbt State flags and per-Target totals. |
| `/invocations` | `app/invocations.rs::index` | Recent runs. |
| `/invocations/{invocation_id}` | `app/invocations.rs::invocation_id::detail` | Every node decision of a run with its description, hashes, timing and generated copy SQL; `?decision=build\|skip\|clone` filters it. |
| `/environments/{environment_id}` | `app/environments.rs::environment_id::catalog` | Target lineage: the latest decision per node as a server-rendered SVG graph (`dagre-rs`) plus a text node list. |

## Running it

The UI reads the database the server writes to.

```bash
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
cargo run -p dbt-state-server                   # captures decisions
HOST=127.0.0.1 PORT=4000 cargo run -p dbt-state-ui
# open http://127.0.0.1:4000
```

`HOST` and `PORT` default to `127.0.0.1` and `3000`. During development,
`topcoat dev` (from `crates/ui`) rebuilds and reloads automatically.

## Organization scope

Every UI query is filtered by `DBT_STATE_UI_ORG_ID` (default `local`). Set it to
the organization ID the gRPC requests use. It is a single-organization filter,
not authentication or tenant selection.

## Data

The server's best-effort capture (`crates/server/src/capture.rs`) writes
`organizations`, `projects`, `environments`, `invocations` and
`node_decisions`. The UI reads `projects`, `environments`, `invocations` and
`node_decisions` (`crates/ui/src/db.rs`).

## Known limitation

The UI shows each decision's `decision_description`, not the persisted Decision
reason (`node_decisions.decision_reason`).
