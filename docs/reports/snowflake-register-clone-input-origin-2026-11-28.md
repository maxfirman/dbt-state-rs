# Snowflake reuse after RegisterClone over dev's own upstream — 2026-11-28

Live characterization and fix for #271 (part of #270, follow-up to #93 and
#264). The question: after the client's dev-clone pre-step copies prod's
downstream into dev with `RegisterClone`, can the downstream's own submit
SKIP over a dev upstream that dev built for itself, and so keep serving
prod's rows? Each fact is marked **Observed** (read from a capture or the
warehouse) or **Inferred**.

## Outcome

- **Observed, correctness finding (this server before #271).** Yes. On
  server `03f3087` the first post-copy submit of a MERGE incremental and of
  a snapshot SKIPped over dev's independently built upstream. Dev kept
  `(1, 'prod')` after that submit and after the unchanged rerun, where the
  independent-schema Reference produced `(1, 'dev')`.
- **Observed (hosted).** Hosted rebuilt both post-copy submits (6 / 6), so
  dev served `(1, 'dev')`, and reused the rebuild on the unchanged rerun.
  In the shared-input control, where both downstreams read prod's
  upstream, hosted reused both copies and served the right rows.
- **Observed (this server with #271, `9e5e8d6`).** Decisions, rows and
  Explain trees match hosted in every case: the first post-copy submit
  rebuilds and serves `dev`, the rerun SKIPs with `dev`, and the control
  SKIPs with `prod`.
- **Cause (from the code).** A RegisterClone copy records no inputs.
  Adoption gave it the submit's own inputs at the copy's epoch
  (`copy_lifecycle::adopt_current_inputs`), so any upstream older than the
  copy looked fresh, whatever the copied build had read. #264 had closed
  the same gap for the submit's Result copy fallback but kept this rule for
  client-requested copies.

## Provenance

| | Hosted | This server, before | This server, with #271 |
|---|---|---|---|
| Server | `api.state.dbt.com` | `03f3087` (main), Postgres `i271_main1` | `9e5e8d6`, Postgres `i271_fix1` |
| Run id | `11172745418a` | `111727798fed` | `121759c0f2d3` |
| Window (UTC) | 2026-11-28 11:17:38–11:19:11 | 11:17:38–11:19:01 | 12:18:12–12:19:36 |
| Proxy upstream | hosted | `PROXY_LOCAL_UPSTREAM=http://127.0.0.1:50371` | `…:50372` |

- Scenario: `python3 experiments/explain_capture.py register_clone_input_origin --port <p> --profiles <private profiles.yml> --work-dir <dir>`
  ([scenario](../../experiments/capture_scenarios/register_clone_input_origin.py)).
  The hosted and pre-fix runs ran concurrently with their own ports, work
  directories and names.
- Client: `dbt 2.0.11` at `~/.local/bin/dbt`, SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`. Every
  `ValidateClientVersion` sent `2.0.11`. Each capture was certified complete
  (`capture_integrity.verify_capture`) and every Explain step passed the
  driver oracle.
- Fusion commands of each run, in order (`--profiles-dir <private dir>
  --profile snowflake --skip-semantic-manifest-validation` on each):
  1. `dbt run-operation` creating the inputs
  2. `dbt build --target prod_demo --select ri_up rs_up`
  3. `dbt build --target prod_demo --select ri_dm ri_ds rs_dm rs_ds`
  4. `dbt build --target dev_noclone --select ri_up` (dev's own upstream,
     no copy permission)
  5. `dbt build --target dev_clone --select ri_up ri_dm ri_ds` (case I: the
     native `RegisterClone` pre-step of both downstreams, their
     confirmations, then their post-copy submits)
  6. `dbt build --target dev_clone --select rs_dm rs_ds` (case S, likewise)
  7. steps 5 and 6 again (the unchanged rerun)
  8. `dbt run-operation` copying dev's input into `ref_state_demo`, then
     `dbt build --no-manage-state --target ref_demo --select ri_up ri_dm ri_ds rs_dm rs_ds --full-refresh`
     (the Reference)

  Each name carries the run id suffix. State-free `dbt show --no-manage-state`
  reads follow each round. All 21 State-free invocations of each run exited
  0 with zero State RPCs recorded by the run's proxy (`state_free.json`).
- Targets: `dev_clone` defers to `prod_demo` with `allow_clones: true`;
  `dev_noclone` is `dev_clone` with `allow_clones: false`; `ref_demo` is
  `CDO_DEMO_DB.ref_state_demo` without deferral. The last two were added to
  a private copy of `~/.dbt/profiles.yml` for the runs. Writes: only per-run
  objects in `CDO_DEMO_DB.PROD_STATE_DEMO`, `DEV_CLONE_DEMO` and
  `REF_STATE_DEMO` (created for #271).
- Evidence in the tree: the hosted capture is
  `golden/fixtures/explain_register_clone_input_origin.jsonl` (61 rows; 17
  telemetry batches dropped). [`evidence/snowflake-register-clone-input-origin.json`](evidence/snowflake-register-clone-input-origin.json)
  holds, for all three runs, every RegisterClone, confirmation and
  downstream submit with its inputs, decision and Explain tree, and every
  read by phase, written by `experiments/register_clone_origin.py extract`.
  Raw windows and dbt logs remain out of the tree.

## Setup

Per run, case I has an input `ri_in` in each Target's own schema (prod
`(1, 'prod')`, dev `(1, 'dev')`), an upstream table `ri_up` over it
(`lag_tolerance: 0s`) and two downstreams over `ref('ri_up')`: a MERGE
incremental `ri_dm` (`unique_key='id'`, `lag_tolerance: 0s`) and a snapshot
`ri_ds` (`check` on `v`). Case S has an input and upstream in prod only and
the same two downstreams reading prod's upstream through a source fixed to
`prod_state_demo`. Each downstream has the same SQL and configuration in
prod and dev. A snapshot cannot set its Lag tolerance on 2.0.11, so the
snapshots ran with the client's 45 minutes ([catalog](../explain-catalog.md#snapshot)).
The dev downstreams are absent before the dev round, so the pre-step
copies them.

## Results

Codes are `skip_rejection_reason` / `clone_rejection_reason`. "Rows" is the
marker for `id = 1` (for a snapshot, its current row): prod / after the
first post-copy submit / after the rerun / Reference.

| Case | Downstream | Service | RegisterClone, confirmed | First post-copy submit | Rerun | Rows |
|---|---|---|---|---|---|---|
| I | MERGE | hosted | 5 / -, prod → dev, `success` | EXECUTE 6 / 6 | SKIP | prod / dev / dev / dev |
| I | MERGE | before #271 | same | **SKIP** | SKIP | prod / **prod** / **prod** / dev |
| I | MERGE | with #271 | same | EXECUTE 6 / 6 | SKIP | prod / dev / dev / dev |
| I | snapshot | hosted | same | EXECUTE 6 / 6 | SKIP | prod / dev / dev / dev |
| I | snapshot | before #271 | same | **SKIP** | SKIP | prod / **prod** / **prod** / dev |
| I | snapshot | with #271 | same | EXECUTE 6 / 6 | SKIP | prod / dev / dev / dev |
| S | MERGE, snapshot | all three | same | SKIP | SKIP | prod / prod / prod / prod |

- **The copy (Observed).** Each downstream's `RegisterClone` named its prod
  relation as `clone_source_table` and the dev relation as `target_table`,
  was granted (`ready_to_clone`, 5 / unset) and confirmed with
  `success: true` and `failed_to_clone: false`: no client fallback. Its
  `clone_source_last_modified_epoch` was the prod relation's warehouse
  metadata epoch, 192–645 ms before the build's confirmed epoch, in every
  copy of every run (and 202–361 ms in `explain_post_copy_identity.jsonl`).
- **The first post-copy submit (Observed).** In case I it listed dev's own
  upstream (hosted run: `"CDO_DEMO_DB"."DEV_CLONE_DEMO"."RI_UP_11172745418A"`
  at 1795864690602, built on `dev_noclone` and confirmed at 1795864690820,
  before the copies were confirmed at 1795864699257 and 1795864699304) and
  the copy itself. The copied prod builds had read prod's upstream. In case
  S both sides listed prod's upstream at the same epoch.
- **Hosted's tree for the rebuild (Observed, entries 36, 37).** The table
  exists, the query and upstream model queries have not changed, and dev's
  upstream is listed with its `last updated` line only: no badge, no
  `no updates since` line, no summary. This server with #271 renders the
  same tree; before #271 it rendered its SKIP tree.
- **The Reference (Observed).** Dev's input was copied into
  `ref_state_demo` (read back as `(1, 'dev')`), and every dev node was built
  there with `--no-manage-state` as first builds. Its rows are the intended
  inputs' rows: `dev` for case I, `prod` for case S.

## Inferred

- Hosted reuses a client-requested copy only when the build it holds read
  what the submit reads, counting a dev upstream that is itself a copy of
  the upstream the build read (`c93st`, `c93it`) but not one dev built
  itself. This isolates the #93 question in
  [correctness-handoff.md](../correctness-handoff.md): "the upstream is a
  copy" was the distinguishing fact.
- Not captured: whether hosted reuses a copy whose source's upstream changed
  between the source build and the copy. This server now rebuilds it: the
  copy's rows were computed before the change.

## The fix

[ADR 0007, Client-requested copies](../adr/0007-lag-tolerance-and-upstream-producers.md#client-requested-copies):

1. Registration records the build the copy holds
   (`clone_source_execution_id`) when the request's source epoch is no
   later than that build's confirmation (or absent). Before, only an equal
   epoch did, so none of these copies recorded it. A DuckDB incremental
   copy records the build at the exact epoch it already required.
2. As the Reuse candidate, the copy is judged as that build
   (`submit_decision::judge_registered_copy`): reused only when the build
   read the submit's inputs, each by name or as a Stand-in at the exact
   recorded epoch (`copy_search::reads_as_recorded`), over the epochs it
   recorded (`copy_search::held_inputs`). Otherwise, or without a recorded
   build, it is refused at Reuse qualification (6 / 6).
3. Explain reads the inputs the copy was judged with
   (`node_decisions.anchor_input_tables`, migration 0038) and renders
   hosted's tree for the refusal (MERGE and SNAPSHOT).

Regressions: the hosted capture replays in `conformance.rs` (with no new
gap); `register_clone_inputs.rs` covers case I (both kinds, the exact tree
and the rerun SKIP), case S, an upstream changed between the source build
and the copy, and a copy without source ancestry.

## Reproduce

```bash
cargo build -p dbt-state-harness --bin record-proxy --bin state-probe
python3 experiments/explain_capture_test.py
python3 experiments/register_clone_origin_test.py
# a private profiles.yml with dev_noclone and ref_demo; ref_state_demo must exist
python3 experiments/explain_capture.py register_clone_input_origin --port <p> \
  --profiles <private profiles.yml> --work-dir <dir>
# against this server: start dbt-state-server on <server port>, then
PROXY_LOCAL_UPSTREAM=http://127.0.0.1:<server port> python3 experiments/explain_capture.py \
  register_clone_input_origin --port <p2> --profiles <private profiles.yml> --work-dir <dir2>
python3 experiments/register_clone_origin.py <dir>
python3 experiments/register_clone_origin.py extract <out.json> "<label>=hosted=<dir>" \
  "<label>=dbt-state-server=<dir2>"
```
