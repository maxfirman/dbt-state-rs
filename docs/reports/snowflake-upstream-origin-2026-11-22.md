# Snowflake upstream origin and served rows — 2026-11-22

Live characterization for #237 (spec #236, Experiment 1). The question: does
the origin of a dev upstream change hosted State's Result copy decision for
an ordinary FULL submit, and which rows does the dev Target then serve? Each
fact is marked **Observed** (read from the capture or the warehouse) or
**Inferred** (the likeliest rule, not proven). This note changes no server
code. The comparison with Databricks (#238) and any policy change belong to
#242 and #243.

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
  result.
- **Observed, correctness finding (this server).** This server copies prod's
  downstream in B, C and D in both runs. In D that serves the prod marker:
  replaying run 2 natively against this server (dbt 2.0.11, Snowflake)
  served `v='prod'` where the Reference execution serves `v='dev'`. This is
  the dev-input case #236 user story 2 describes, and it matches the #239
  finding that this server copies a source in the environment pattern that
  hosted does not consider
  ([copy source choice](snowflake-copy-source-choice-2026-11-21.md), entry 10).
  Keyed as open gaps, not accepted divergences, for policy review in #242 and
  #243.

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
- Raw windows, dbt logs, `reads.json` and the per-case records
  (`run*.record.{txt,json}`) are out of the tree at
  `~/projects/worktrees/dbt-state-rs/notes-237/`. Everything below is
  reproducible from the fixtures. The rows come from the State-free `dbt show`
  reads in `reads.json`.

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
- Then, per case: the next unchanged dev build. Last: a State-disabled
  `dbt build --full-refresh` of every dev node (the Reference execution, the
  driver's `Reference` step), read back.

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
- **Epochs (Observed).** Each downstream submit listed its upstream with the
  epoch the warehouse reported. For prod's upstreams, that epoch is earlier
  than the epoch the client confirmed for the build: by 727–774 ms in run 1
  and 211–281 ms in run 2. Run 2's B dev
  downstream (T34) listed the dev upstream at exactly the confirmed copy
  epoch.

## Inferred

- Hosted's 2.0.11 submit-copy search does not count a dev upstream as prod's
  upstream, whether it is a confirmed copy (B) or an independent build of the
  same logic (C, D). The candidate must have read the physical upstream the
  submit reads. Only A met that. This agrees with #239, where hosted never
  copied a source reading another relation of the same name.
- Older captures show hosted copying a downstream over a dev-schema upstream:
  `explain_clone_history.jsonl` entry 12 (client 2.0.6, 2026-11-08, upstream
  copied on submit), `clone_fallback_zero_tolerance.jsonl` entry 2
  (2026-10-12) and `clone_chain_upstream_history.jsonl` entry 13 (client
  2.0.11, 2026-11-10). This run does not explain why those were copied while
  B was not. Possible factors are the client version, the source's own
  history (those sources were themselves copies) and a hosted change since
  2026-11-10. The question is open; see
  [correctness-handoff.md](../correctness-handoff.md#open-hosted-questions).
- This server's copy of D serves rows computed from prod's input. The cause
  is the same as for #239 entry 10. `submit_decision/copy_search.rs` takes an
  equivalent in another environment, the schema-abstracted logic identity
  holds, and nothing ties the candidate's recorded upstream read to the
  physical upstream the submit reads.

## Replay

Both fixtures replay in `conformance.rs`. 27 keyed open gaps
(`golden/replay_gaps.json`) cover the differences; everything else matches
exactly.

- **Decision (6):** I28, I36, I44, T34, T45 and T57. At each, hosted builds
  and this server copies prod's downstream. The hosted confirmation and the
  next SKIP after each have no local counterpart and are not compared (six
  unknown baselines per fixture). Their Explains (I31, I39, I47, I57, I63,
  I69; T37, T48, T60, T70, T76, T82) each explain the other side's decision.
- **Upstream rejection codes (4, field-scoped):** I34 and I42 (C and D's dev
  upstream, run 1), T39 and T51 (the same, built on `dev_noclone`). Hosted
  reports no equivalent (6 / 6). This server finds prod's upstream, which
  read its input in `prod_state_demo`, as an equivalent under another name
  (skip reason 1). With `allow_clones: false` it answers 1 / 10, "cloning is
  disabled", and its Explains (T41, T53) name prod's upstream. Both build.
- **Copy trees (3):** I23, T29 (A) and T36 (B's dev upstream). Both copy.
  Hosted lists the upstream `<FRESH>` with "no updates since … last
  executed"; this server omits the badge and the line. This is the same open
  Snowflake rendering gap as #239 (`copy_source_choice.jsonl` entry 3).

## Reproduce

```bash
cargo build -p dbt-state-harness --bin record-proxy --bin state-probe
python3 experiments/explain_capture_test.py        # driver self-test, incl. the Reference step
python3 experiments/upstream_origin_test.py        # per-case record self-test
# run 2 needs a `dev_noclone` output: `dev_clone` with `allow_clones: false`
python3 experiments/explain_capture.py upstream_origin --port <p> --work-dir <dir1>
python3 experiments/explain_capture.py upstream_origin_table --port <p> --work-dir <dir2>
python3 experiments/upstream_origin.py <dir>       # per-case record (--json for all fields)
```

For a rehearsal against this server, start `dbt-state-server` on another
port and run the driver with
`PROXY_LOCAL_UPSTREAM=http://127.0.0.1:<server port>`. A Databricks run
(#238) can keep the case design, the downstream config and the step order,
and change only the targets, dialect and input DDL.
