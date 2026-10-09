# UI console (Phase 1)

A read-only web console for the dbt State server, built with
[Topcoat](https://tokio.rs/blog/2026-07-22-announcing-topcoat) (server-rendered
Rust, no WASM). It presents an audit log of what dbt State decided across
projects and environments. It never executes dbt and never mutates state.

See [ui-plan.md](ui-plan.md) for the full plan, research, and roadmap.

## What it shows

- **Overview** (`/`) — reuse rate, built/reused/cloned totals, recent invocations.
- **Projects** (`/projects`) — every project; drill into a project to see its
  environments with dbt State flags and per-environment built/reused/cloned.
- **Invocations** (`/invocations`) — the audit log of runs. Each invocation
  (`/invocations/{id}`) lists every node decision with its human-readable reason,
  hashes, timing, and (for clones) the generated clone SQL; filterable by
  decision.
- **Lineage / catalog** (`/environments/{id}`) — a backend-rendered lineage
  graph of the environment's latest node state (SVG laid out with `dagre-rs`),
  with an accessible text node-list fallback.

## How the data gets there

The dbt State **server** records a forward-compatible domain model on every
decision (best-effort, non-blocking — see `crates/server/src/capture.rs`):
organization → project → environment → invocation → `node_decisions`
(the append-only audit log). The UI reads that same Postgres read-only via sqlx.
Migrations: `crates/server/migrations/0003_ui_domain.sql`.

## Running it

The UI reads the same database the server writes to.

```bash
# 1. Postgres (shared with the server) + run the gRPC server so it captures state.
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
cargo run -p dbt-state-server            # gRPC on 127.0.0.1:50051 (+ migrations)

# 2. Point dbt at the server so decisions get captured (see docs/development.md).
#    DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL=127.0.0.1:50051 RUN_CACHE_API_SECURE=false dbt build ...

# 3. Run the UI (reads the same DATABASE_URL).
HOST=127.0.0.1 PORT=4000 cargo run -p dbt-state-ui
# open http://127.0.0.1:4000
```

During development, `topcoat dev` (from `crates/ui`) gives automatic rebuilds and
live reload.

## Design notes

- **Multipage, server-rendered.** Each page is a Topcoat `#[page]`; the root
  `#[layout]` is the app shell. No client-side framework; `dagre-rs` lays out
  lineage on the server and we emit inline SVG.
- **Accessibility.** Semantic landmarks (`header`/`nav`/`main`/`footer`), a skip
  link, `aria-current` on the active nav item, visible focus rings, status shown
  with text + colour (never colour alone), and the lineage SVG carries
  `role="img"` + `<title>`/`<desc>` with a parallel text node list.
- **Styling.** A small, self-contained high-contrast stylesheet (no build step)
  lives in `crates/ui/src/app.rs` (`STYLES`). It can be swapped for Tailwind +
  Topcoat UI components (`topcoat ui add`) later.
- **Auth is deferred.** The domain model already carries the seams
  (`organizations`/`actors`/`roles`/`role_bindings`, org-scoped rows, nullable
  `actor_id` on audit rows) so SSO + RBAC drop in additively.

## Status

Phase 1 complete: capture + Overview + Projects/Environments + Invocations audit
log + backend lineage. Deferred to later phases: environment "clear state"
action, built-vs-reused time-series charts, authenticated client parity, SSO +
RBAC enforcement, and manifest/catalog ingestion for the richer Catalog.
