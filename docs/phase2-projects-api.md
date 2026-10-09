# Phase 2 (revised): honest audit log, not a pretend hierarchy

Status: DECIDED (supersedes the a-priori-projects plan). Based on experiments
against the real `api.state.dbt.com` (see "Clone experiment findings" below), we
will NOT build first-class, a-priori projects/environments. They are not a dbt
State correctness concern — the deferral/clone "environment" is a client-side
concept, and clone correctness is governed by client-side deferral resolution
plus a server-side freshness-epoch gate, never by a server notion of an
"environment". So an authoritative hierarchy would be scaffolding that misleads.

Instead the UI is an **honest log of what happened**. We stop calling the dbt
profile `target_name` an "environment"; we present it as what it literally is —
the **target name** — and additionally surface the **target database** and
**target schema** that the decision actually touched.

## Clone experiment findings (verified against the real API)

1. The server validates a clone's **source freshness** via a required epoch.
   Observed verbatim: `clone source was modified since the cache decision ...
   required epoch <= <X>, found <Y>; falling back to execution`. At clone-decision
   time the server may return `clone_required_last_modified_epoch`; if the source
   table is modified past it before the clone runs, the clone is rejected and the
   client **falls back to executing**.
2. `ConfirmExecution{failed_to_clone=true}` round-trips so the server records the
   node was executed, not cloned.
3. Happy-path clone: `ready_to_clone` (decision=3, skip_rejection_reason=5), with
   concrete `clone_sqls`; `clone_required_last_modified_epoch` null when the
   source is freshly built, set when a staleness window exists.
4. Clone source is selected **client-side** from deferral (node `unique_id`
   resolved in the `defer_to_target` profile target). The server receives an
   already-chosen `clone_source_table` and validates it (freshness), rather than
   searching candidates.
5. Validation observed is **temporal (epoch)**, not hash/lineage of the source.
   No `unable_to_clone` message was observed; rejection manifested as the
   client-side epoch check + fallback.
6. Upstream *logic* changes reach the server via each node's
   `query_dependencies` (the upstream's compiled SQL is transmitted and reflects
   the change) — not via any lineage hash in `dbt_node_state`.

Correctness takeaway: identity (project/target names) does NOT enter the
skip/build/clone decision match. So auto-created, name-based attribution cannot
corrupt a verdict; it can only mislabel the *log*. Hence: make the log honest and
precise about what each field is, rather than inventing authority.

## What changes

- **Terminology/model:** rename the UI concept "environment" → "target". A
  target is identified by `(org, target_name)` but we are explicit that this is a
  dbt profile target alias (per-developer, not an authoritative environment).
- **Surface the real relation coordinates:** for every decision, show/store the
  **target database** (`default_catalog`), **target schema** (`default_schema`),
  and the fully-qualified **target table** (`target_table`) when present.
- **Keep auto-capture** (no a-priori registration). Projects remain a grouping by
  `project_name`/`project_id` purely for the log; no "create project" CRUD, no
  membership, no REST/CLI for provisioning in this phase. (Those were motivated by
  authority we've decided not to assert.)
- **Honest framing in the UI copy:** label things "Target", "Database", "Schema";
  avoid implying these are governed environments.

## Schema delta (migration 0004, additive)

- `environments` table → treat as "targets": add `database` and `schema` columns
  (nullable), captured from `default_catalog`/`default_schema`. Keep the table
  name for now to avoid a destructive rename; expose it as "Target" in the UI and
  via a view/alias. (A later migration can rename if desired.)
- `node_decisions` already stores `target_table` and `default_schema`; add
  `default_catalog` (database) if not already present so each decision row is
  self-describing.

## Not doing (and why)
- A-priori projects/environments, membership, RBAC-for-provisioning, REST/CLI for
  CRUD: dropped. They implied an authority dbt State does not require and we would
  have to fake. If a future need arises (multi-tenant governance), revisit — but
  it is explicitly out of scope for an honest-log console.
- Read-only auth/SSO for *viewing* the console may still be added later; that is
  independent of the (now-cancelled) provisioning model.
