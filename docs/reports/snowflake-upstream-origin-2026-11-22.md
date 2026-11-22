# Snowflake upstream origin and served rows — 2026-11-22

Live characterization for #237 (spec #236, Experiment 1). The question: does
the origin of a dev upstream change hosted State's Result copy decision for
an ordinary FULL submit, and which rows does the dev Target then serve? Each
fact is marked **Observed** (read from the capture or the warehouse) or
**Inferred** (the likeliest rule, not proven). This note changes no server
code. The [cross-warehouse comparison](result-copy-source-selection-2026-11-22.md)
provides the evidence handoff for the #243 refactor.

**Update (#264, 2026-11-22).** This server now builds the dev downstream in
B, C and D and copies it in A, as hosted does. The "This server" column and
the Replay section below describe the server before that change. See
[ADR 0007](../adr/0007-lag-tolerance-and-upstream-producers.md#result-copy-over-different-inputs).
A native rerun of `upstream_origin_table` against the patched server
(run `232206b6851a`, 2026-11-22 23:22–23:24 UTC, dbt 2.0.11 with the SHA-256
above, Postgres `i264_native`) confirms it:

| Case | Dev downstream, first | Next | Rows: prod / served / after next / verified Reference |
|---|---|---|---|
| A | CLONE from `PROD_STATE_DEMO.OA_DN`, confirmed | SKIP | prod / prod / prod / prod |
| B | EXEC 6 / 6, confirmed | SKIP | prod / prod / prod / prod |
| C | EXEC 6 / 6, confirmed | SKIP | same / same / same / same |
| D | EXEC 6 / 6, confirmed | SKIP | prod / **dev** / dev / dev |

C and D's dev upstream, built on `dev_noclone`, got 6 / 6 as on hosted (it
got 1 / 10 before). The verified Reference ran `dbt build --no-manage-state
--target dev_clone --select <the run's dev nodes> --full-refresh` with the
State URL pointing at the run's own proxy; its capture holds zero RPCs and
all 14 invocations exited 0. Sanitized per-case records with the verified
Reference are in
[`evidence/snowflake-upstream-origin-264/`](evidence/snowflake-upstream-origin-264/).

## Outcome

- **Observed.** Hosted copied the dev downstream only in case A, where prod
  and dev read the same physical upstream and submit byte-identical SQL.
  Hosted built the dev downstream in B, where the dev upstream is a confirmed
  copy of prod's, and in C and D, where the dev upstream was built
  independently. This held with a merge-incremental upstream (run 1) and a
  table upstream (run 2). Origin did not separate the cases: hosted refused
  the confirmed copy (B) just as it refused the independent builds.
- **Observed.** Hosted served the right rows in every case. Every dev
  downstream held the rows of the State-disabled Reference execution, after
  both the first and the next dev build. Hosted shows no wrong-marker
  result. The Reference was verified with State explicitly disabled
  (`--no-manage-state`, no State RPC) in a follow-up of each run (below).
- **Observed, correctness finding (this server).** This server copies prod's
  downstream in B, C and D in both runs. In D that serves the prod marker:
  replaying run 2 natively against this server (dbt 2.0.11, Snowflake)
  served `v='prod'` where the Reference execution serves `v='dev'`. This is
  the dev-input case #236 user story 2 describes. In #239's crafted histories,
  this server granted a copy of a source that read another relation of the
  same name (in `CSC_UP_ALT`), where hosted built. Those histories captured
  RPC decisions. The rehearsal's reads, phases and RPCs support the physical
  finding. [Evidence extract](evidence/snowflake-upstream-origin/D.json),
  [policy review #264](https://github.com/maxfirman/dbt-state-rs/issues/264).

## Provenance

| | Run 1 | Run 2 | Rust rehearsal of run 2 |
|---|---|---|---|
| Scenario | `explain_capture.py upstream_origin` | `explain_capture.py upstream_origin_table` | as run 2, proxy `PROXY_LOCAL_UPSTREAM` → `dbt-state-server` (Postgres `i237_rehearsal`) |
| Upstream | merge incremental (`unique_key='id'`) | table | table |
| Run id | `234451b8ea12` | `000858964de8` | `002752a5958d` |
| Window (UTC) | 2026-11-21 23:45:13–23:47:21 | 2026-11-22 00:09:13–00:11:31 | 2026-11-22 00:27:55–00:30:20 |
| Fixture | `golden/fixtures/explain_upstream_origin_incremental.jsonl` (70 rows; 23 telemetry batches dropped) | `golden/fixtures/explain_upstream_origin_table.jsonl` (83 rows; 27 telemetry batches dropped) | not promoted (this server's own output) |
| Metered submits (`SubmitEnrichedSQL`) | 22 | 24 | — |
| Other calls | 8 speculative, 1 RegisterClone, 15 confirms, 14 `state-probe` Explains | 16 speculative, 15 confirms, 16 `state-probe` Explains | — |

- Client: `dbt 2.0.11` at `~/.local/bin/dbt`, SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`. Every
  `ValidateClientVersion` sent `2.0.11` (`client.json`). Every capture was
  certified complete (`capture_integrity.verify_capture`), and every Explain
  step passed the driver oracle.
- Writes: only per-run objects in `CDO_DEMO_DB.PROD_STATE_DEMO` and
  `CDO_DEMO_DB.DEV_CLONE_DEMO`. The fixtures carry no credential: the proxy
  drops transport and auth headers.
- Run 2 builds C and D's dev upstream on the profile output `dev_noclone`.
  It is `dev_clone` with `allow_clones: false` (it sends `clone_chain_depth_limit: 0`),
  added to the local `~/.dbt/profiles.yml` for this run.
- **Evidence in the tree.** The fixtures hold every hosted request and
  response quoted below. The rows come from `dbt show` reads.
  [`evidence/snowflake-upstream-origin/`](evidence/snowflake-upstream-origin/)
  holds one document per case (`A.json` … `D.json`), written by
  `experiments/upstream_origin.py extract`. For hosted run 1, hosted run 2 and
  the Rust rehearsal, each document lists every planned read with its run
  step, phase (`prod`, `served`, `after_next`, `reference`), relation and
  rows. It also lists the dev upstream's origin, the dev downstream
  decisions, the verdict, and the case's RPCs without call metadata. The
  rehearsal was not promoted as a fixture; its document is its only
  in-tree record.
- Raw windows, dbt logs and `reads.json` remain out of the tree at
  `~/projects/worktrees/dbt-state-rs/notes-237/`.

## Setup (both runs)

Every case x in a..d has a per-run input `ox_in`, upstream `ox_up` and
downstream `ox_dn` ([`_upstream_origin.py`](../../experiments/capture_scenarios/_upstream_origin.py)).

- **Downstream:** `table`, `select id, v from <upstream>`, with
  `state={'pre_clone': 'never', 'lag_tolerance': '0s'}`. Prod builds it once;
  that build is the one FULL candidate. The dev relation is absent before the
  first dev build.
- **Copy permissions (Observed):** every downstream submit, prod and dev,
  carried `allow_clones: true`, `clone_chain_depth_limit: null` and
  `freshness_tolerance_seconds: 0`. No downstream was the subject of a
  `RegisterClone`. Its decision always came from an ordinary
  `SubmitEnrichedSQL`; the speculative submits for it all answered `undecided`.
- **A:** the downstream reads prod's upstream through a source fixed to
  `prod_state_demo`. Dev builds only the downstream. The prod and dev
  submits carry identical `sql`, `tables` and `table_namespace`
  (`fc38a5ba…`).
- **B:** the upstream reads a fixed prod input, so its dev SQL is prod's.
  Run 1: the client's pre-step copies it with `RegisterClone` (default
  `pre_clone`, dev relation absent). Run 2: hosted copies it on submit.
- **C, D:** the upstream reads an input in its Target's own schema
  (`schema: "{{ target.schema }}"`). C's prod and dev inputs both hold
  `(1, 'same')`. D's prod input holds `(1, 'prod')` and its dev input
  `(1, 'dev')`. Run 1 sets `pre_clone: never` on the upstream; run 2 builds it
  on `dev_noclone`.
- After the next unchanged dev build, the original driver ran a full refresh
  of every dev node and read the rows back. It cleared the State environment
  override and the recording proxy URL, but did not pass `--no-manage-state`,
  and the effective project and user State settings of those runs were not
  retained. That original Reference is therefore not the one the verdicts
  rest on.
- **Verified Reference (2026-11-22 20:14–20:17 UTC).** Each run was followed
  by a State-free re-execution of the same run's nodes (`--run-id`):
  `upstream_origin_reference` for run 1, `upstream_origin_table_reference`
  for run 2 and for the rehearsal. dbt 2.0.11 (same SHA-256) first read the
  case inputs back. It then ran `dbt build --no-manage-state --target
  dev_clone --select <the run's dev nodes> --full-refresh` and read every dev
  relation back with `dbt show --no-manage-state`. The State URL pointed at
  the run's own recording proxy as a trap. The driver fails a step during
  which the proxy records an RPC. All 14 invocations of each follow-up
  exited 0, and none recorded an RPC; each follow-up capture holds zero
  RPCs. Each case’s `verified_reference` section retains the sanitized build
  command, the input rows and the Reference rows by original run ID,
  relation and phase. The follow-up work directory contains the individual
  read commands and exits in `state_free.json`.

## Results

Codes are `skip_rejection_reason` / `clone_rejection_reason`. **CLONE** =
`ready_to_clone`, 1 / -, "an equivalent model exists under another name so
we cloned that one". **EXEC** = `ready_to_execute`, 6 / 6, "model was
executed because its table did not exist". **SKIP** = "model was a no-op
because both its query and its upstream data are up to date". I*n* =
`explain_upstream_origin_incremental.jsonl` entry *n*; T*n* =
`explain_upstream_origin_table.jsonl` entry *n*.

| Case | Dev upstream (hosted) | Dev downstream, first (hosted) | Next (hosted) | Rows: prod / served / after next / Reference | This server |
|---|---|---|---|---|---|
| A run 1 | prod's, read in place | CLONE from `PROD_STATE_DEMO.OA_DN` (I21), confirmed (I22) | SKIP (I50) | prod / prod / prod / prod | same response; copy tree lacks `<FRESH>` (gap I23) |
| A run 2 | prod's, read in place | CLONE from `PROD_STATE_DEMO.OA_DN` (T27), confirmed (T28) | SKIP (T63) | prod / prod / prod / prod | same response; copy tree lacks `<FRESH>` (gap T29) |
| B run 1 | `RegisterClone` 5 / - from `PROD_STATE_DEMO.OB_UP` (I25), confirmed (I26), post-copy SKIP (I27) | EXEC (I28) | SKIP (I55) | prod / prod / prod / prod | CLONE from prod `OB_DN` (gap I28) |
| B run 2 | CLONE on submit from `PROD_STATE_DEMO.OB_UP` (T32), confirmed (T33) | EXEC (T34) | SKIP (T68) | prod / prod / prod / prod | CLONE (gap T34; rehearsal served prod) |
| C run 1 | EXEC (I34), confirmed (I35) | EXEC (I36) | SKIP (I61) | same / same / same / same | CLONE (gap I36) |
| C run 2 | EXEC on `dev_noclone` (T39), confirmed (T40); SKIP on `dev_clone` (T44) | EXEC (T45) | SKIP (T74) | same / same / same / same | CLONE (gap T45; rehearsal served same) |
| D run 1 | EXEC (I42), confirmed (I43) | EXEC (I44) | SKIP (I67) | prod / **dev** / dev / dev | CLONE (gap I44) |
| D run 2 | EXEC on `dev_noclone` (T51), confirmed (T52); SKIP on `dev_clone` (T56) | EXEC (T57) | SKIP (T80) | prod / **dev** / dev / dev | CLONE (gap T57); **rehearsal served prod, wrong marker** |

The Reference column is the verified Reference. In every run and case it
equals the original Reference rows. D's dev input holds `(1, 'dev')` and
its prod input `(1, 'prod')` (`verified_reference.inputs`). The verified
Reference of D produced `(1, 'dev')` for both the dev upstream and the dev
downstream in all three runs.

| Case | Hosted run 1 | Hosted run 2 | dbt-state-server rehearsal |
|---|---|---|---|
| A | served prod / Reference prod | prod / prod | prod / prod |
| B | prod / prod | prod / prod | prod / prod |
| C | same / same | same / same | same / same |
| D | dev / dev | dev / dev | **prod / dev (wrong marker)** |

An offline check agrees. Each captured downstream SQL selects only `id, v`
from the dev upstream. Running those SELECTs in DuckDB against the retained
upstream rows reproduced the Reference rows in all three runs, both after
the first build and after the unchanged submit.

Every confirmation answered `success: true` with `failed_to_clone: false`,
so no copy fell back to a build on the client.

- **Explain (Observed).** Hosted's tree for each EXEC downstream in B, C and
  D, in both runs, is only the table analysis: "the model table does not
  exist" [FAIL] and "there is no other table that the model could be cloned
  from" [FAIL]. Hosted does not name an equivalent it refused. A's CLONE
  tree names prod's downstream as the source, and lists prod's upstream
  `<FRESH>` with "no updates since … last executed".
- **The dev upstreams (Observed).** In C and D hosted also found no copy
  source for the dev upstream itself, even without copy permission. Run 2
  sent `allow_clones: false` / `clone_chain_depth_limit: 0` and got 6 / 6,
  not 1 / 10 ("cloning is disabled"). A different physical input is not an
  equivalent for hosted. A same-input upstream (B) was copied.
- **Epochs (Observed).** The upstream epoch a downstream submit lists is
  sometimes the epoch the client confirmed for the upstream's build and
  sometimes the warehouse's, a few hundred ms earlier. Prod downstreams
  listed the warehouse epoch (727–774 ms earlier in run 1, 211–281 ms in run
  2), and so did A's dev submits (I21, T27), which were copied. The first dev
  downstream submit listed the confirmed epoch exactly in run 1 B, C and D
  (I28, I36, I44) and run 2 B (T34), and the warehouse epoch in run 2 C and D
  (T45, T57, 936 and 201 ms earlier). Every next submit listed the warehouse
  epoch. Since hosted refused under both epoch sources and copied under the
  warehouse epoch, the epoch source does not explain the refusals.

## Inferred

- Hosted's 2.0.11 submit-copy search does not count a dev upstream as prod's
  upstream, whether it is a confirmed copy (B) or an independent build of the
  same logic (C, D). In these runs hosted copied only a candidate that had
  read the physical upstream the submit reads (A). This agrees with #239,
  where hosted never copied a source reading another relation of the same
  name. Older captures contradict this as a general rule (next bullet).
- Older captures show hosted copying a downstream over a dev-schema upstream:
  `explain_clone_history.jsonl` entry 12 (client 2.0.6, 2026-11-08, upstream
  copied on submit), `clone_fallback_zero_tolerance.jsonl` entry 2
  (2026-10-12) and `clone_chain_upstream_history.jsonl` entry 13 (client
  2.0.11, 2026-11-10). In the first two, the granted source was built and
  confirmed in the same capture (entries 3–4 and 0–1); `explain_clone_history`
  has B's run-2 shape, with the dev upstream copied on submit from prod's
  (entry 7). In the third, the granted source `CF2_MODEL_164819` was built in
  an earlier session the capture lacks. This run does not explain why those
  were copied while B was not. Observed differences, none shown to be the
  cause: the client (2.0.6 in `explain_clone_history`), the Lag tolerance
  (2700 s there, 0 s here, though `clone_fallback_zero_tolerance` copied at
  0 s), the downstream's configuration (`node_configs_hash` differs:
  `pre_clone` and `lag_tolerance` are set here), and the capture dates
  (hosted may have changed since 2026-11-10). The question is open; see
  [correctness-handoff.md](../correctness-handoff.md#open-hosted-questions).
- This server's copy of D serves rows computed from prod's input. From the
  code, not from a capture: `submit_decision/copy_search.rs` takes an
  equivalent recorded under another Target when the schema-abstracted logic
  identity holds, and nothing ties the candidate's recorded upstream read to
  the physical upstream the submit reads. #239's grant from the middle source
  also has different input bindings. The experiments leave their shared cause
  open.

## Replay

Both fixtures replay in `conformance.rs`. 27 keyed open gaps
(`golden/replay_gaps.json`) cover the differences; everything else matches
exactly.

- **Decision (6, with 12 Explains):** I28, I36, I44, T34, T45 and T57. At each, hosted builds
  and this server copies prod's downstream. The hosted confirmation and the
  next SKIP after each have no local counterpart and are not compared (six
  unknown baselines per fixture). Their Explains (I31, I39, I47, I57, I63,
  I69; T37, T48, T60, T70, T76, T82) each explain the other side's decision.
- **Upstream rejection codes (4 field-scoped, 2 Explains):** I34 and I42
  (C and D's dev upstream, run 1), T39 and T51 (the same, built on
  `dev_noclone`). Hosted reports no equivalent (6 / 6). This server finds
  prod's upstream, which read its input in `prod_state_demo`, as an
  equivalent under another name (skip reason 1). With `allow_clones: false`
  it answers 1 / 10, "cloning is disabled", and its Explains (T41, T53) name
  prod's upstream. Both build.
- **Copy trees (3):** I23, T29 (A) and T36 (B's dev upstream). Both copy.
  Hosted lists the upstream `<FRESH>` with "no updates since … last
  executed"; this server omits the badge and the line. #239 reports the same
  open Snowflake rendering gap. T36's source is prod's upstream
  (`OB_UP`), the others' prod's downstream.

6 + 12 + 4 + 2 + 3 = 27.

## Reproduce

```bash
cargo build -p dbt-state-harness --bin record-proxy --bin state-probe
python3 experiments/explain_capture_test.py        # driver self-test, incl. the Reference step
python3 experiments/upstream_origin_test.py        # per-case record self-test
# run 2 needs a `dev_noclone` output: `dev_clone` with `allow_clones: false`
python3 experiments/explain_capture.py upstream_origin --port <p> --work-dir <dir1>
python3 experiments/explain_capture.py upstream_origin_table --port <p> --work-dir <dir2>
python3 experiments/upstream_origin.py <dir>       # per-case record (--json for all fields)
# the State-free Reference follow-ups, each with the run id of the run it verifies
python3 experiments/explain_capture.py upstream_origin_reference --port <p> --run-id <run 1 id> --work-dir <ref1>
python3 experiments/explain_capture.py upstream_origin_table_reference --port <p> --run-id <run 2 id> --work-dir <ref2>
python3 experiments/explain_capture.py upstream_origin_table_reference --port <p> --run-id <rehearsal id> --work-dir <ref3>
python3 experiments/upstream_origin.py extract docs/reports/evidence/snowflake-upstream-origin \
  "hosted run 1 (merge-incremental upstream)=hosted=<dir1>=<ref1>" \
  "hosted run 2 (table upstream)=hosted=<dir2>=<ref2>" \
  "dbt-state-server rehearsal of run 2=dbt-state-server=<rehearsal dir>=<ref3>"
```

For a rehearsal against this server, start `dbt-state-server` on another
port and run the driver with
`PROXY_LOCAL_UPSTREAM=http://127.0.0.1:<server port>`. A Databricks run
(#238) can keep the case design, the downstream config and the step order,
and change only the targets, dialect and input DDL.
