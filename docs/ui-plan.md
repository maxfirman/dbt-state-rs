# UI plan: a minimal, read-only console for the dbt State server

Status: PHASE 1 IMPLEMENTED (see ui.md). This document captures research into the
real dbt Platform and a proposed plan for a companion UI to the dbt State server.

## Goal & non-goals

A minimal, modern web console that is an **alternative/replacement for dbt
Platform**, but tightly scoped to dbt State. It is an **audit log and
observability surface**, not a control plane.

Non-goals (explicit): no remote execution, no job/schedule configuration, no
code editing, no IDE. We never run dbt; we only observe what clients did and
expose the state the protocol gives us.

## What the real dbt Platform looks like (researched via its Admin API + docs)

### Object model (to mirror)
- **Account / Organization** — top-level tenant. Has groups, SSO config
  (`enterprise_authentication_method`, `enterprise_login_slug`), seat/license
  types (developer, read_only, analyst, it, explorer), plan.
- **Project** — belongs to an account; has a connection, repository, default
  dbt version, environments.
- **Environment** — belongs to a project; `type` = development | deployment;
  carries `dbt_version`, a primary profile/credentials, a connection, and —
  crucially for us — **`enable_dbt_state`** and **`is_deferrable`** flags.
- **Run / Invocation** — an execution against an environment (dbt Platform also
  has Jobs/Schedules, which we deliberately omit). Each run has per-node results.
- **Node** — model / test / seed / snapshot / etc.

### dbt State UI surfaces (the parts worth mirroring)
From docs/deploy/state-aware-interface + dbt Explorer/Catalog:
1. **Account home**: a "models built vs reused" chart over time; reused-model
   count per project. The headline value prop (compute saved by skipping).
2. **Per-run structured log view**: each node tagged built / skipped / reused /
   cloned, with the human-readable **reason** (our `decision_description`),
   filterable (All/Success/Skipped/Reused/…), searchable.
3. **Run/build history** + total run duration and models built/reused over 7/14/
   30 days.
4. **Catalog / Explorer**: browse resources (models, sources, tests, metrics),
   **lineage graph**, **column-level lineage**, per-resource detail pages
   (description, columns, code, execution metadata, latest status).
5. **"Latest status" lineage lens**: nodes tagged with their latest execution
   status (incl. "Reused").
6. **Clear cache / clear state** per environment (the one write action State
   exposes — resets the environment's recorded state so the next run rebuilds).

### RBAC model (to mirror, simplified)
- Permissions are grouped into **permission sets** (owner, member, …) assigned
  to **groups**; a group's grant is scoped to all projects or a specific project.
- Fine-grained permission statements exist (e.g. `metadata_read`,
  `environments_read`, `audit_log_read`, `dbt_state_account_credentials_*`).
- We will implement a small, read-leaning subset (see RBAC below).

## What our server/protocol already gives us (data inventory)

From the gRPC traffic (see docs/protocol.md), every decision carries:
- Identity: `project_id`, `project_name`, `target_name` (environment),
  `profile_name`, `resource_type`, node `unique_id` / `fqn` / `name`,
  `table_namespace`, `default_schema`, `dialect`.
- Logic/data: `node_hash`, `node_body_hash`, `node_configs_hash`,
  `node_contract_hash`, `node_macros_hash`, `node_persisted_descriptions_hash`,
  the **raw compiled SQL**, `query_dependencies` (upstream SQL), per-input
  freshness `tables[]`, `values_hash` (seeds).
- Decision: variant (skip/execute/clone), `decision_description` (human reason),
  `is_stale`, rejection reasons, `execution_runtime_ms`, `execution_decision_id`,
  `request_id`, and (clone) the generated `clone_sqls`, source/target.
- Org: `x-organization-id`, plus session/invocation ids in metadata
  (`x-session-id`, `x-dbt-invocation-id`).

This is enough to reconstruct: org → project → environment → invocation → node
decision, with reasons, timing, lineage (from query_dependencies), and the SQL.
If a full `manifest.json` / `catalog.json` is also ingested (optional), we can
offer the richer Catalog (descriptions, columns, column-level lineage).

### Gap vs current persistence
Today we persist only the `executions` table. To power the UI we must also
model/capture: organizations, projects, environments, invocations, and
per-node decision rows with reasons (today we discard the decision log after
responding). This is additive — the server keeps the same wire behavior and
additionally records richer rows for the UI.

## Proposed architecture

```
dbt client ──gRPC──> dbt-state-server (unchanged wire behavior)
                           │  also records: orgs, projects, envs, invocations,
                           │                node decisions (append-only audit log)
                           ▼
                        Postgres
                           ▲
                           │ read-only queries
          ┌────────────────┴───────────────┐
          │  UI backend (REST/JSON over HTTP, in the same Rust server) │
          └────────────────┬───────────────┘
                           ▼
                     Web UI (SPA)
```

- **Backend**: add an HTTP/JSON read API to the existing Rust server (axum —
  already in the tonic dependency tree), served alongside gRPC. Read-only
  endpoints for orgs/projects/environments/invocations/nodes/decisions +
  aggregates (built-vs-reused over time). One write endpoint: "clear state for
  environment" (mirrors Clear cache), permission-gated.
- **State capture**: extend the server to upsert org/project/environment on
  every submit (derived from request fields) and append a `node_decisions` row
  per decision (invocation id from `x-dbt-invocation-id`, reason, timing,
  status). Keep it additive and non-blocking to the decision path.
- **Frontend**: a small SPA (React + TypeScript + Vite, a component kit like
  shadcn/ui, a graph lib like React Flow for lineage). Minimal, read-only.
- **Optional manifest ingestion**: an endpoint to upload/point at
  `manifest.json`/`catalog.json` per environment to unlock the full Catalog.

## Authentication & RBAC

Two distinct auth surfaces, matching dbt Platform:

1. **Machine/client auth (must be indistinguishable from dbt Platform).**
   The dbt client already does OAuth token-exchange against
   `auth.state.dbt.com` using the `~/.dbt/dbt_cloud.yml` credential. To mirror
   this *precisely and frictionlessly*, our deployment hosts a compatible
   token-exchange + gRPC endpoint so a user only points `dbt_cloud.yml` /
   `RUN_CACHE_API_URL` at our host — setup is identical to dbt Platform. (Today
   the local insecure path already works; this adds the authenticated path.)
2. **Human/UI auth (modern).** OIDC / SSO (e.g. Auth0/Okta/Entra/Google) via
   authorization-code + PKCE, plus optional username/password for local dev.
   Sessions via short-lived JWT + refresh. This mirrors dbt Platform's
   enterprise SSO.

**RBAC (simplified, read-leaning):**
- Roles as permission sets: `owner`, `admin`, `member` (read + clear-cache),
  `read_only` (read), scoped to **all projects** or a **specific project**
  (mirrors dbt's group→permission_set→project scoping).
- Permissions enforced server-side on every API call; the UI hides what the
  user can't see. An `audit_log_read`-style gate protects the raw decision log.
- Users belong to groups; groups carry the scoped permission set. SSO group
  claims can map to groups (like dbt's `sso_mapping_groups`).

## Proposed information architecture (screens)

1. **Home / Overview** — built-vs-reused chart over time (account-wide), reused
   count per project, recent invocations, headline "builds skipped / est. time
   saved".
2. **Projects** — list; each project → its environments.
3. **Environment detail** — flags (dbt State enabled, deferrable, dbt_version,
   dialect), latest invocations, built/reused trend, **Clear state** action.
4. **Invocations (runs)** — list + detail. Detail = the structured decision log:
   every node with status icon (built/skipped/reused/cloned), reason
   (`decision_description`), timing, and a drill-in to the node decision
   (hashes, SQL, upstream freshness, clone SQL if any).
5. **Catalog** — resources across the latest state of an environment: searchable
   list + lineage graph (from `query_dependencies`; richer with manifest). Node
   detail: description, columns (if catalog ingested), compiled SQL, latest
   status, column-level lineage.
6. **Audit log** — raw, filterable decision stream (the core "what happened").
7. **Settings** — members/groups/roles (RBAC), SSO config, environment state
   management.

## Phasing

- **Phase 1 (MVP, highest value):** state-capture of org/project/env/invocation/
  decision; read API; Home overview + Invocations list/detail (the audit log).
  Local dev auth + basic RBAC (owner/read_only). This alone replaces the most
  valuable dbt State screens.
- **Phase 2:** Environment detail + Clear state; built-vs-reused charts; proper
  OIDC/SSO; full RBAC with groups + project scoping.
- **Phase 3:** Catalog + lineage graph from query_dependencies; node detail with
  SQL + freshness; "latest status" lens.
- **Phase 4:** Optional manifest/catalog ingestion → full Catalog with columns +
  column-level lineage; compatible authenticated client endpoint (token-exchange
  parity) so setup is indistinguishable from dbt Platform.

## Open questions for the user

## Confirmed decisions (2026-10-09)

- **Scope:** single-tenant deployment (one organization per server instance) for
  now; the domain model is designed to be forwards-compatible with multi-tenant.
- **Auth:** deferred. Build with no auth initially, but the domain model carries
  the seams (organization scoping, actor/principal on audit rows, role tables
  stubbed) so SSO/RBAC drops in later without migrations beyond additive ones.
- **Tech stack:** **Topcoat** (tokio-rs) — server-rendered, multipage, no WASM.
  Tailwind + Topcoat UI (copy-in components) for a clean, accessible,
  visually-pleasing interface. Backend-rendered **lineage** via the `dagre-rs`
  crate (`dagrers`: Sugiyama layout over a petgraph `Graph`) → emit SVG in a
  Topcoat `view!`. No client-side graph library.
- **Interface:** multipage app, strong accessibility (semantic HTML, ARIA,
  keyboard nav, focus states, color-contrast), clean and intuitive.
- **Coexistence:** the gRPC server (tonic) and the Topcoat UI run together.
  Topcoat's `tower` feature can mount the tonic service, or they bind separate
  ports; decided at scaffold time.

## Forwards-compatible domain model (Phase 1 schema)

New tables, additive to the existing `executions` table. All carry `org_id`
from day one (single value today, real tenancy later). Timestamps on everything.

- `organizations(id, external_id, name, created_at)` — one row today; the server
  derives/creates it from the `x-organization-id` header (default `local`).
- `projects(id, org_id, external_id (project_id from protocol), name, dialect?,
  created_at, updated_at)` — upserted from `dbt_node_state.project_id/project_name`.
- `environments(id, project_id, name (target_name), profile_name?, dialect?,
  dbt_state_enabled, is_deferrable, first_seen_at, last_seen_at)` — upserted from
  `target_name`/`profile_name`; flags inferred/overridable.
- `invocations(id, org_id, environment_id, external_invocation_id
  (x-dbt-invocation-id), session_id (x-session-id), started_at, last_seen_at,
  built_count, reused_count, cloned_count)` — grouped from request metadata.
- `node_decisions(id, invocation_id, environment_id, org_id, node_unique_id,
  node_name, node_fqn, resource_type, execution_type, decision (enum:
  build|skip|clone), is_stale, decision_description, request_id,
  execution_decision_id, node_body_hash, table_namespace, target_table,
  clone_source?, clone_sqls?, execution_runtime_ms?, created_at)` — the append-
  only audit log; one row per decision. This is the UI's core data source.
- Auth seams (created now, unused until Phase 2): `actors(id, org_id, kind
  (user|service|system), external_subject?, display_name?)`,
  `roles(id, org_id, name, permission_set)`, `role_bindings(id, role_id,
  actor_id, scope (all|project), project_id?)`. `node_decisions`/`invocations`
  gain a nullable `actor_id` so audit rows attribute an actor once auth exists.

The capture path is additive and best-effort: recording a decision must never
block or fail the gRPC decision response. Writes are fire-and-forget (bounded
channel) or in the same tx but tolerant of errors.

## Rendering lineage (backend)

Per environment (latest state) or per invocation, build a petgraph `Graph` of
`node_unique_id`s with edges derived from `query_dependencies` (and, when a
manifest is ingested later, from the manifest DAG). Run `dagrers::DagreLayout`
with `RankDir::LR` to get node coordinates + edge paths, then render an
accessible inline SVG (with `<title>`/`<desc>`, role=img, and a parallel
text/list fallback) inside a Topcoat page. Node fill encodes latest status
(built/skipped/reused/cloned).
