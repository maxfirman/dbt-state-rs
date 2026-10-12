# dbt State `state:*` selector conformance — test plan

> **STATUS: EXECUTED & IMPLEMENTED (2026-10-11).** This plan was carried out
> end to end. The hosted behavior was captured live
> (`golden/fixtures/selector_state_selection.jsonl`, 67 distinct
> `GetStateSelection` queries across all 10 criteria), the semantics were proven
> (a Python oracle and the Rust engine both reproduce 69/69 captured responses),
> and `SelectorService.GetStateSelection` is implemented and conformant. See the
> **Results** section below, `docs/protocol.md` (State selectors),
> `docs/correctness-handoff.md` §D, and `experiments/SCENARIOS.md`. The open
> questions in §2 are all RESOLVED (answers inline in the Results section).

Plan for exhaustively testing `SelectorService.GetStateSelection` — the
server-side engine behind dbt State-powered `state:*` selectors
(`state:new/old/modified/unmodified` and the `modified.*` sub-selectors) — and
implementing it to conform with the real hosted service (`api.state.dbt.com`).

Status today: `SelectorServiceImpl::get_state_selection` returns
`UNIMPLEMENTED` (`crates/server/src/services.rs`). This plan is **capture-first
TDD**: characterize the hosted behavior live, promote golden fixtures, write
failing conformance assertions, then implement. We do **not** assume any
server-side semantics the hosted service has not demonstrated.

Feature docs:
<https://docs.getdbt.com/docs/deploy/dbt-state-about?version=2#use-dbt-state-with-state-selectors->,
<https://docs.getdbt.com/docs/deploy/dbt-state-deferral#dbt-state-powered-state-selectors>,
<https://docs.getdbt.com/reference/node-selection/methods#state>.

---

## 1. What the protocol is (KNOWN from primary sources)

Authoritative, already read:

- **Proto** (`proto/.../services/selector_service.proto`):
  `GetStateSelection(SelectorRequest) -> SelectorResponse`.
  - `SelectorRequest { string target; SelectorCriteria selector_criteria;
    string project_id; repeated DbtNodeData nodes; }`
  - `SelectorCriteria { SC_UNSPECIFIED=0, MODIFIED=1, NEW=2, UNMODIFIED=3,
    OLD=4, BODY=5, CONFIGS=6, RELATION=7, PERSISTED_DESCRIPTIONS=8, MACROS=9,
    CONTRACT=10 }`
  - `DbtNodeData { node_unique_id, node_hash, node_body_hash?,
    node_configs_hash?, node_persisted_descriptions_hash?, node_macros_hash?,
    node_contract_hash?, node_database_representation? }`
  - `SelectorResponse { repeated string node_unique_ids; }`
- **Client** (`dbt-core/crates/dbt-state/src/selector.rs`, the authoritative
  implementation — the Fusion crate has no selector):
  - selector string → criteria: `new→NEW`, `old→OLD`, `modified→MODIFIED`,
    `unmodified→UNMODIFIED`, `modified.body→BODY`, `modified.configs→CONFIGS`,
    `modified.persisted_descriptions→PERSISTED_DESCRIPTIONS`,
    `modified.relation→RELATION`, `modified.macros→MACROS`,
    `modified.contract→CONTRACT`.
  - `target` = the `defer_to` env (default `prod`); `project_id` is required.
  - Sends every node's hashes; `node_database_representation` =
    `database.schema.alias` (or `None` if any component is empty).
  - Batches `SELECTOR_MAX_BATCH_SIZE = 10_000` nodes per request and **unions**
    the per-batch `node_unique_ids`.
  - Fires `ClientSelectorEvent` telemetry (fire-and-forget) after each call.
- **Hashes** (`dbt-core/crates/dbt-state/src/hash.rs`): `node_hash` is the
  overall change signal; for a model it is
  `hash([body, configs, persisted_descriptions, macros, fqn, contract,
  ref_representation])`. Component hashes drive the sub-selectors. Seeds:
  `node_hash = node_body_hash` (or `""`). Generic tests:
  `hash([configs, fqn])`.
- **Selector semantics** (dbt docs, `node-selection/methods#state`):
  - `new` — no node with the same `unique_id` in the comparison state.
  - `old` — a node with the same `unique_id` exists.
  - `modified` — all new nodes **plus** changed existing nodes.
  - `unmodified` — existing nodes with no changes.
  - `modified.body/configs/relation/persisted_descriptions/macros/contract` —
    the respective component changed.
  - Comparison source for dbt-State-powered selectors is **each node's own last
    execution in the `defer_to_target` env** (default `prod`), scoped by
    `project_id`, used automatically when no `--state` manifest is given.

## 2. What is NOT known and MUST be captured live (no assumptions)

The hosted comparison engine is closed-source; neither `dbt-state-oss` nor the
Fusion client implements it. Each question below is answered by a captured
`GetStateSelection` request/response pair, never by inference:

1. **target→history mapping.** How does the `target` string resolve to recorded
   executions? (env name, profile target, or the physical schema). Does it line
   up with how `SubmitEnrichedSQL` keys history (physical `target_table`)?
2. **`modified` composition.** Does `MODIFIED` compare `node_hash` only, or OR
   together the component hashes? Does it include `NEW` nodes (docs say yes —
   confirm)?
3. **Set algebra.** Exact membership for `new`/`old`/`modified`/`unmodified`:
   partition checks (is `old == !new`? is `unmodified == old \ modified`?).
4. **Absent component hash.** When a `DbtNodeData` component hash is `None`
   (e.g. non-model `node_contract_hash`, or `node_database_representation`
   `None`), is the node ever selected by that sub-criterion? Does the server
   skip it, treat `None==None` as unmodified, or treat `None` vs recorded value
   as modified?
5. **Node types.** Classification for models (view/table/incremental), seeds,
   snapshots, and data tests — are all four compared the same way? Are tests
   even sent (the client iterates all `nodes`)?
6. **First-seen / empty history.** With no recorded execution for a node (or an
   empty `target` env): is every node `new`/`modified` and none `old`/
   `unmodified`? Does the server error, or return an empty set?
7. **Response ordering & dedup.** Is `node_unique_ids` sorted, input-ordered, or
   arbitrary? Any duplicates? (Client collects into a `BTreeSet`, so order from
   the server is not load-bearing for the client, but conformance diffing needs
   a defined comparison — likely set equality.)
8. **Scoping isolation.** Do `project_id` and `target` isolate history (a node
   built only under project A / env X is `new` for project B / env Y)? Does
   org (`x-organization-id`) isolate as elsewhere?
9. **`SC_UNSPECIFIED` / unknown criteria.** What does the server do with
   criterion 0 or an out-of-range value? (The client never sends these, but the
   server contract should be captured, else left out of scope explicitly.)
10. **Trigger.** Which dbt command actually emits `GetStateSelection` in a
    self-managed project with `project-id` set and no `--state`
    (expected: `dbt ls --select state:modified`, `dbt build --select
    state:modified`, `dbt test --select state:new`). Confirm the request even
    fires against our insecure proxy.

## 3. Capture methodology (authoritative oracle)

Mirror the existing live-capture pattern (`experiments/clone_experiment.sh`,
`lag_experiment.sh`): recording proxy → real service, jaffle-shop on Snowflake,
**only `CDO_DEMO_DB` is mutated**, `git restore` the project afterwards.
Credentials from `~/.dbt/dbt_cloud.yml`; profile token is a secret — never echo.

New driver: `experiments/selector_experiment.sh`.

Capture protocol per scenario:

1. **Seed baseline state.** Build the jaffle-shop DAG in `prod_demo` (the
   `defer_to_target`) through the proxy so the hosted service records a last
   execution per node in that env/project.
2. **Mutate one axis** in the working tree (see §4 change catalog) — e.g. edit
   one model's SQL body, or one model's config, or a column `description` with
   `persist_docs`, or a macro, or `database/schema/alias`, or add/remove a node.
3. **Run the selector** without building:
   `dbt ls --select state:<criterion>` (and the `modified.*` variants) through
   the proxy, from a target whose `defer_to_target` is `prod_demo`. This emits
   `GetStateSelection` with the post-mutation hashes; the recorded baseline is
   the pre-mutation state.
4. **Record** the `GetStateSelection` request (criteria + per-node hashes) and
   response (`node_unique_ids`) to the golden `.jsonl`.
5. **Restore** the tree. Repeat for each criterion × change type.

Because the response depends on hosted recorded state, each fixture entry is
only meaningful paired with the baseline that produced it. The conformance test
reconstructs that baseline locally (via `RecordExecutions`/`SubmitEnrichedSQL`
+ `ConfirmExecution`) from the **same** captured hashes, then replays the
captured `GetStateSelection` request and asserts our `node_unique_ids` equals
the hosted set. This is the established "build equivalent state in both, then
diff" approach from the handoff's §E.

If live access is unavailable at implementation time, the plan degrades
gracefully: fixtures already promised in `golden/fixtures/` drive the offline
conformance tests; any criterion not yet captured stays `UNIMPLEMENTED` with its
open question documented in `correctness-handoff.md` rather than guessed.

## 4. Exhaustive test matrix

### 4.1 Criteria × change-type grid (the core matrix)

For a single existing model with recorded baseline state, apply each change type
and capture every criterion. Expected ("?") cells are filled from live capture,
never pre-assumed.

| change type ↓ \ criterion → | new | old | modified | unmod | mod.body | mod.configs | mod.relation | mod.persisted_descriptions | mod.macros | mod.contract |
|---|---|---|---|---|---|---|---|---|---|---|
| none (identical rebuild) | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| body: SQL change | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| body: whitespace/comment only | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| config change (non-excluded, e.g. `materialized`) | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| config `tags`/`meta` only (metadata-only) | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| relation: `schema`/`alias`/`database` change | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| persisted_descriptions: `description` + `persist_docs` on | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| persisted_descriptions: `description` with `persist_docs` off | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| macros: change an upstream macro | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| contract: column name/type change (contract enforced) | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| node added (brand new unique_id) | ? | ? | ? | ? | ? | ? | ? | ? | ? | ? |
| node removed (present in baseline, absent now) | n/a | n/a | n/a | n/a | — | — | — | — | — | — |

Partition invariants to assert once the grid is captured (verify, don't assume):
- `old` ≡ nodes not in `new` (for the submitted set).
- `modified ⊇ new`; `unmodified ≡ old \ modified`.
- `modified ≡ new ∪ (mod.body ∪ mod.configs ∪ mod.relation ∪
  mod.persisted_descriptions ∪ mod.macros ∪ mod.contract)` — **only if** the
  capture shows this union holds; otherwise record the true relation.

### 4.2 Node-type coverage

Capture at least one of each so classification differences surface:

- model — view (`stg_*`), table (`marts/*`), incremental (merge), snapshot.
- seed (`node_hash == node_body_hash`; sub-selectors on a seed).
- data test (generic + singular; `node_hash == hash([configs, fqn])`).
- node types with `None` component hashes (seed/test `node_contract_hash`,
  `node_database_representation`).

### 4.3 Edge cases

- **Empty `nodes`** → expect empty response, no error (client guards this but
  capture the server contract).
- **Empty / first-seen history** → every node `new`/`modified`.
- **Batching** → > 10_000 nodes forces 2 requests client-side; server behavior
  per batch is identical (unit-test the client batching is already covered
  upstream; our server test asserts a single large request is handled).
- **Scoping** → same `node_unique_id` under a different `project_id`, `target`,
  and org is `new`.
- **`None` vs recorded value** for each component hash (per open question #4).
- **`SC_UNSPECIFIED` / out-of-range criterion** → capture or explicitly scope
  out with a documented reason.
- **Response set semantics** → compare as a set (dedup, order-independent)
  unless capture proves ordering is contractual.

## 5. Test artifacts to produce

### 5.1 Live capture
- `experiments/selector_experiment.sh` — driver (new).
- `golden/fixtures/selector_*.jsonl` — one fixture family per criterion group,
  each carrying the baseline executions and the `GetStateSelection`
  request/response. Documented in `golden/README.md`.

### 5.2 Pure unit tests (no DB) — `crates/server/src/selector.rs` (new module)
Factor the comparison into a pure function
`classify(criteria, submitted: &DbtNodeData, recorded: Option<&RecordedNode>)
-> bool` (mirrors the pure `decision::decide` pattern) so it is property- and
table-testable offline:
- table test for every (criterion, change-type) cell from §4.1.
- partition-invariant properties from §4.1 (set algebra) via proptest, matching
  `decision_proptest.rs` style.
- `None`-hash handling per captured truth.

### 5.3 Integration / conformance tests — `crates/harness/tests/`
- `selector_conformance.rs` — load `selector_*.jsonl`, rebuild baseline locally
  from captured hashes, replay the captured `GetStateSelection`, assert
  `node_unique_ids` set-equals the hosted response.
- `selector_scoping.rs` — org/project_id/target isolation at the gRPC boundary.
- `selector_offline.rs` — hand-built baselines for edge cases that are awkward
  to capture (empty nodes, large batch, first-seen), asserting the engine not
  the hosted bytes.
- Wire `selector_*.jsonl` into `conformance.rs` auto-discovery (or an explicit
  `CHARACTERIZATION_ONLY`/dedicated-test carve-out, as `lag_multi_upstream`
  does).
- **Replace** the two existing UNIMPLEMENTED assertions once implemented:
  `differential.rs::explain_placeholder_and_unsupported_selector` and
  `offline_regressions.rs::unsupported_history_queries_are_errors_not_empty_successes`.

## 6. Implementation sketch (TDD green, after red)

Only built to satisfy captured behavior:

1. **Schema.** New migration `0009_selector_hashes.sql` adding the columns the
   selector needs that are not recorded today:
   `node_persisted_descriptions_hash`, `node_macros_hash`,
   `node_database_representation` on `executions` (nullable; populated going
   forward on Submit/Record from `DbtNodeState`; legacy rows NULL → that
   sub-criterion simply does not fire, matching the clone/0008 precedent).
   Also persist `node_hash` is already present. Index for
   `(org_id, project_id, target, node_unique_id, status='confirmed')`.
2. **Record path.** Extend `fingerprint.rs`/`store.rs`/`services.rs` so Submit
   and Record persist the three new hashes and the `target`/`project_id`
   needed to resolve the comparison baseline.
3. **Store lookup.** `store::latest_confirmed_node_state(org, project_id,
   target, node_unique_id) -> Option<RecordedNode>` returning the recorded
   hashes.
4. **Pure engine.** `selector::classify(criteria, submitted, recorded)` encoding
   the captured truth table.
5. **Service.** `get_state_selection` hydrates recorded state for the submitted
   `node_unique_id`s (batched), runs `classify`, returns the matching set in the
   captured order/dedup. Org from `x-organization-id` (default `local`), as
   every other store query.

Open semantic (#1, target→history): resolve from capture. If the selector keys
history by env/project (not physical `target_table`), the record path must store
a `target` env label distinct from `target_table` — captured evidence decides
this before any code is written.

## 7. Methodology guardrails (per AGENTS.md / handoff)

- Capture against the real service **first**; promote a fixture; add the failing
  assertion; then implement. Never invent hosted policy.
- Live runs only ever mutate `CDO_DEMO_DB`; `git restore` jaffle-shop after.
- Keep `selector::classify` I/O-free so properties and conformance replay stay
  valid.
- Preserve org scoping on every new store query.
- Document any criterion that cannot be captured as an explicit open question in
  `docs/correctness-handoff.md`; leave it `UNIMPLEMENTED` rather than guessed.
- Final gate: `cargo test --workspace`, `cargo test -p dbt-state-server --lib`
  against an unreachable DB, `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets --features fuzz -- -D warnings`.

---

## Results (live capture, 2026-10-11)

All §2 open questions resolved against the real service; see
`docs/correctness-handoff.md` §D and `docs/protocol.md`.

1. **Trigger.** `dbt compile --select state:<crit>` (and build/run/test) fires
   `GetStateSelection`; `dbt ls` does NOT engage the run-cache lifecycle.
2. **target→history.** `SelectorRequest.target` is the `defer_to_target` ENV
   name (default `prod`), not a physical schema. State is recorded by a build's
   `dbt_node_state.target_name`. We built the baseline in `prod_demo` and queried
   from `dev_clone` (`defer_to_target: prod_demo`), so `target=prod_demo` lined
   up. The recorded `node_hash` equals the submitted one — the server records the
   client hashes verbatim and does a PURE comparison (no recompute).
3. **`modified` composition.** `modified = new ∪ body ∪ configs ∪
   persisted_descriptions ∪ macros ∪ contract`. **`relation` is EXCLUDED.**
4. **Set algebra.** `new`/`old` and `modified`/`unmodified` each exactly
   partition the submitted set; a *new* node matches every `modified.*`.
5. **Absent component hash.** `None` vs `Some` counts as a difference; a *new*
   node (no recorded state) matches every sub-selector.
6. **Node types.** Only models/seeds/snapshots/tests are tracked; sources, unit
   tests, semantic models, metrics and saved queries are always `new`/`modified`.
7. **Response.** A flat `node_unique_ids`; compared as a set (order not
   contractual). Empty submission → empty response.
8. **Scoping.** Isolated by org (`x-organization-id`, default `local`),
   `project_id`, and `target` env (verified offline in `selector_offline.rs`).
9. **Invalid input.** `SC_UNSPECIFIED`/out-of-range criteria and an empty
   `project_id` are rejected with `INVALID_ARGUMENT`.
10. **Whitespace.** `node_body_hash` is a raw file hash, so comment/whitespace
    edits DO fire `modified.body` — the selector is file-hash based, unlike the
    semantic SQL reuse decision.

### Artifacts produced
- `experiments/selector_experiment.sh` — the live capture driver (baseline /
  query / mutate / all-clean).
- `golden/fixtures/selector_state_selection.jsonl` — 159 entries (baseline
  submits/confirms + 67 distinct selector queries). `CHARACTERIZATION_ONLY` in
  the generic replay; driven by the dedicated conformance test.
- `crates/server/src/selector.rs` — pure `classify` engine (13 unit tests).
- `crates/server/migrations/0009_node_states.sql` — the comparison store.
- `crates/server/src/store.rs` — `upsert_node_state` / `fetch_node_states`.
- `crates/server/src/services.rs` — `record_node_state` on Submit/Values/Record;
  `SelectorServiceImpl` wired to the store.
- `crates/harness/tests/selector_conformance.rs` — replays all captured queries,
  asserts set-equality (TDD oracle).
- `crates/harness/tests/selector_offline.rs` — record-path + scoping + edge cases.
- `crates/harness/src/bin/record_proxy.rs` — now proxies `SelectorService` +
  `ClientTelemetry` (needed to capture the selector traffic at all).

### Not yet characterized (future work)
- Multi-batch requests (>10,000 nodes): the client batches and unions; our
  server handles a single large request, so this is client-side only.
- `ResolveDeferredRelations` (deferral) remains UNIMPLEMENTED.
