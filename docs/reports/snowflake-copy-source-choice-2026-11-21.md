# Snowflake copy source choice among competing sources — 2026-11-21

Live characterization for #239 (part of #236): which existing physical result
hosted State offers to copy when several Snowflake builds of one model could
be the source of an ordinary FULL submit into an absent Target relation. Each
fact is marked **Observed** (read from the capture or its replay) or
**Inferred** (the likeliest rule, not proven). This note changes no server
code. The [cross-warehouse comparison](result-copy-source-selection-2026-11-22.md)
provides the evidence handoff for the #243 refactor.

**Update (#264, 2026-11-22).** This server now agrees with hosted in all 13
histories: it passes over an equivalent that read another relation. The
"This server" section below describes the server before that change. See
[ADR 0007](../adr/0007-lag-tolerance-and-upstream-producers.md#result-copy-over-different-inputs).

## Provenance

| | Main run | Completion-order controls | Middle-epoch control |
|---|---|---|---|
| Driver (`experiments/copy_source_choice.py`) | `run` (the nine histories then planned) | `run old_mid_late old_new_late old_mid_new_late` | `run mid_current_alone` |
| Run id | `2346130f59d0`, preceded by the control-only run `23455314d580` | `0012045393fe` | `00352880a044` |
| Window (UTC) | 2026-11-21 23:45:56–23:48:10 | 2026-11-22 00:12:07–00:12:45 | 2026-11-22 00:35:32–00:35:40 |
| Fixture entries (`golden/fixtures/copy_source_choice.jsonl`) | 0–55 | 56–75 | 76–79 |
| Metered submits (`SubmitEnrichedSQL`) | 28 | 10 | 2 |
| Other calls | 18 confirms, 10 Explains | 7 confirms, 3 Explains | 1 confirm, 1 Explain |

- Every request is crafted with `state-probe` through a recording proxy on
  this machine; no dbt client and no warehouse is involved. The requests
  copy the shape client 2.0.11 sends for a FULL table model
  (`data_test_history.jsonl` entry 27) and name never-created relations in
  `CDO_DEMO_DB`. Nothing was written to Snowflake.
- All three proxy sessions are certified complete
  (`capture_integrity.verify_capture`: 56/56, 20/20 and 4/4 RPCs). The
  fixture is the three sessions concatenated, unedited. It carries no
  credential: the proxy drops transport and auth headers.
- `experiments/copy_source_choice.py table golden/fixtures/copy_source_choice.jsonl`
  re-derives the hosted column of the results table from the fixture alone.
  It names the source a grant copies (`clone_source`) and checks that the
  grant's `clone_required_last_modified_epoch` and `execution_runtime_ms` are
  that source's confirmed completion epoch and runtime. It flags a history as
  inconclusive, and exits 1, when the history is not in the plan, a planned
  source was never submitted or another source appears, a source submit is
  not a first build, a source was not confirmed successfully before the
  destination submitted, the destination reports its own Target relation, or
  an epoch is later than the moment its request was sent. No history was
  flagged. `copy_source_choice_test.py` runs it over the fixture, and over
  the fixture with destination entry 46 moved ahead of confirms 43–45 and with
  entries 42 and 44 removed; both mutations flag `old_mid_new`.
- The "This server" column comes from the conformance replay of the fixture
  (`conformance.rs::conformance_replay_all_corpora`, which asserts each keyed
  local variant), and agrees with two rehearsals of the plan against this
  server at `2f61c43` (runs `2343306f73ca`, `0009160c2b2c`). Raw sessions,
  driver output and the rehearsals are out of the tree at
  `~/projects/worktrees/dbt-state-rs/notes-239/239-evidence/`.

## Setup

One history per row of the results table. Each history has its own model
(`model.jaffle_shop.csc<run>_<history>`) and its own upstream names, so no
history can see another's builds. Within a history every submit has the same
model unique id, output object name (`CSC<RUN>_<HISTORY>`), catalog
(`CDO_DEMO_DB`) and connection namespace (`fc38a5ba0eceb701c6ab7c6b882e28b0`,
the demo namespace). Each role has its own schema, default schema and
`target_name` (`csc_old`, `csc_mid`, `csc_new`, `csc_dest`):

| Role | Target relation schema | Reads | Role in the question |
|---|---|---|---|
| `old` | `CSC_OLD` | `CDO_DEMO_DB.csc_up.<src>` after its change, at the epoch the destination reports | the fresh source: same physical input as the destination |
| `mid` | `CSC_MID` | `CDO_DEMO_DB.csc_up_alt.<src>`: same object name, another schema, at B − 60 s (in `mid_current_alone`, at the destination's epoch for `csc_up.<src>`) | the different-input source: same SQL once schemas are abstracted, different physical input |
| `new` | `CSC_NEW` | `CDO_DEMO_DB.csc_up.<src>` before its change | the stale source: same physical input, stale for the destination at Lag tolerance 0 |
| destination | `CSC_DEST` | `CDO_DEMO_DB.csc_up.<src>` at its current epoch | absent: its Target relation is not in `tables` and has no history |

**Freshness settings and copy permissions**, identical on every submit:
`execution_type` 1 (FULL), `freshness_tolerance_seconds: 0`,
`stale_upstream_policy: 0` (ANY), `allow_clones: true`,
`defer_enabled: true`, `is_defer_to_profile: false`,
`clone_chain_depth_limit: null`, `clone_time_travel_limit: null`,
`ignore_external_modifications: false`, `compare_unrendered_code: false`,
`tolerate_nondeterminism: true`. Every source confirm reports
`failed_to_clone: false`, `table_type: "TRANSIENT TABLE"`,
`execution_results: null` and a runtime distinct within its run.

**Planned timeline** from a history’s base B. The fixture records the
request timestamp after the probe starts. At entry 76, for example, the
submit was 411 ms later than planned and preceded completion by 589 ms.

| Step | When |
|---|---|
| `new` submits, reporting `csc_up.<src>` at B − 60 s | B |
| `csc_up.<src>` changes (no request) | B + 0.5 s |
| `old` submits, reporting `csc_up.<src>` at B + 0.5 s | B + 1.5 s |
| `mid` submits, reporting `csc_up_alt.<src>` | B + 3 s |
| completion epochs, in the history's completion order | B + 4 s, B + 5 s, B + 6 s |
| confirms, in the history's confirmation order, each after its epoch | from B + 7 s, 1.5 s apart |
| destination submits, then is explained 2 s later | 2 s after the last confirm |

Every source is submitted before any is confirmed, so no source could be
offered another source's result (each answered 6 / 6, "model was executed
because its table did not exist"). A source that read the upstream before
its change was necessarily submitted first: submit order is the same in
every history and was not varied.

## Results

Completion order is oldest first; confirmation order is first to last.
Entries are the destination's submit and Explain.

| History | Sources | Completion | Confirmation | Hosted | Entries | This server |
|---|---|---|---|---|---|---|
| `old_alone` (positive control) | old | old | old | CLONE `old` | 2–3, 6–7 | CLONE `old` |
| `mid_alone` | mid | mid | mid | BUILD 6 / 6 | 10–11 | CLONE `mid` |
| `mid_current_alone` | mid (read at the destination's epoch) | mid | mid | BUILD 6 / 6 | 78–79 | CLONE `mid` |
| `new_alone` (control) | new | new | new | BUILD 6 / 6 | 14–15 | BUILD 6 / 6 |
| `old_mid` | old, mid | old < mid | old, mid | CLONE `old` | 20–21 | CLONE `mid` |
| `old_mid_rev` | old, mid | old < mid | mid, old | CLONE `old` | 26–27 | CLONE `mid` |
| `old_mid_late` | old, mid | mid < old | mid, old | CLONE `old` | 60–61 | CLONE `old` |
| `old_new` | old, new | old < new | old, new | CLONE `old` | 32–33 | CLONE `old` |
| `old_new_rev` | old, new | old < new | new, old | CLONE `old` | 38–39 | CLONE `old` |
| `old_new_late` | old, new | new < old | new, old | CLONE `old` | 66–67 | CLONE `old` |
| `old_mid_new` | old, mid, new | old < mid < new | old, mid, new | CLONE `old` | 46–47 | BUILD 6 / 6 |
| `old_mid_new_rev` | old, mid, new | old < mid < new | new, mid, old | CLONE `old` | 54–55 | BUILD 6 / 6 |
| `old_mid_new_late` | old, mid, new | new < mid < old | new, mid, old | CLONE `old` | 74–75 | CLONE `old` |

Every hosted CLONE is `ready_to_clone`, decision 3, codes 1 / null,
`is_stale: false`, "an equivalent model exists under another name so we
cloned that one". Each grant's `clone_source` names its own history's
`CSC_OLD` relation, and its `clone_required_last_modified_epoch` and
`execution_runtime_ms` are the epoch and runtime confirmed for that history's
`old`. They differ between histories (for example 51,011 ms at entry 6 and
51,511 ms at entry 32). Every hosted BUILD is `ready_to_execute`, decision 1,
codes 6 / 6, `is_stale: false`, "model was executed because its table did
not exist".

A `_rev` history and its base share relative completion and submit times and
differ only in confirmation order. A `_late` history and the `_rev` one
share confirmation order and relative send times and differ only in completion epochs
(the fresh source completes last). The `_late` histories ran 24 minutes
after their `_rev` partners, in their own run. The fourth combination (the
fresh source completes last and is confirmed first) was not run.

## Findings

1. **Observed: the positive control copies the oldest fresh source.** Both
   runs of `old_alone` granted `CSC_OLD`.
2. **Observed: each competitor alone builds.** `mid_alone`,
   `mid_current_alone` and `new_alone` answered BUILD 6 / 6, and their
   Explain trees say "the model table does not exist" [FAIL] and "there is
   no other table that the model could be cloned from" [FAIL] (entries 11,
   15, 79). No tree names the candidate, so the refusals look the same on
   the wire.
3. **Observed: with competitors, hosted copies the fresh source in every
   history.** Adding the stale `new`, the different-input `mid`, or both
   yields the same decision, codes and description as `old_alone`. Each
   grant names its own `old` source and echoes that build’s epoch and
   runtime across the tested completion and confirmation orders.
4. **Observed: neither ordering decides the winner.** Holding completion
   order fixed and reversing confirmation order (`_rev`) leaves the grant on
   `old`; so does then reversing completion order (`_late`). `old` wins as
   the first and the last confirmation, and as the oldest and the newest
   completion.
5. **Inferred: hosted passes over an ineligible candidate in either
   position.** A rule that considers only the newest completion would have
   built in `old_new` and `old_new_rev`, only the oldest completion in the
   `_late` histories, only the latest confirmation in `old_new` and `old_mid`,
   and only the first confirmation in `old_new_rev` and `old_mid_rev`.
   Eligibility (finding 2) explains every winner.
6. **Inconclusive: why the different-input source is ineligible.** In
   `mid_alone` `mid` read its relation 60 s before the destination's
   upstream changed, so a comparison of schema-abstracted inputs would find
   it stale. `mid_current_alone` removes that: `mid` read its relation at
   the epoch the destination reports for its own, and hosted still builds.
   Two explanations remain: hosted does not treat SQL that reads
   `csc_up_alt` as the same logic as SQL that reads `csc_up` (its schema
   abstraction is narrower than "every schema"), or it requires a copy
   source to have read the destination's own upstream relations. The two
   SQL texts are identical once schemas are abstracted, and a crafted
   request cannot separate the relation a build read from the relation its
   SQL names, so these histories cannot tell the two apart. Earlier
   Snowflake captures in which hosted copied between Targets had each
   upstream in its reader's own schema (`explain_clone_history.jsonl` entry
   12); here neither upstream schema is the reader's.

**Not covered** by this design: a second fresh source competing with `old`,
which would expose the order in which hosted examines eligible candidates;
the fourth ordering combination above; a destination with its own Target
relation or history; and upstreams in each reader's own schema. Physical
rows and the client's handling of a grant are covered by the native
experiment (#237).

## This server

**Observed (conformance replay of the fixture):** this server agrees with
hosted in 7 of the 13 histories and differs in 6. Each difference is
keyed in `golden/replay_gaps.json` as an open divergence:

- `mid_alone`, `mid_current_alone` (entries 10, 78): it copies `mid` where
  hosted builds. `submit_decision/copy_search.rs` copies an equivalent whose
  recorded upstream reads differ from the submit's unjudged when it is the
  first candidate offered. The grant names the result whose recorded input is
  `csc_up_alt.<src>` for a destination that reads `csc_up.<src>`. These crafted
  RPCs execute no copy and record no warehouse rows.
- `old_mid`, `old_mid_rev` (entries 20, 26): it copies `mid`, the newest
  completion, where hosted copies `old`. Decision, codes and description
  agree, so these gaps are field-scoped to `clone_source`, `clone_sqls`,
  `clone_required_last_modified_epoch` and `execution_runtime_ms`.
- `old_mid_new`, `old_mid_new_rev` (entries 46, 54): it passes over the
  stale `new`, then stops at `mid` and builds, because after a freshness
  rejection it accepts only another equivalent with the submit's upstream
  reads. Hosted copies `old`.

**Inferred:** its candidate order is the completion epoch, newest first.
`old_mid_rev` and `old_mid_late` differ only in completion order, and it
copies `mid` in the first and `old` in the second; confirmation order
changes none of its results.

**Explain, outside #239's source-choice scope.** In each of the 11 hosted
CLONE trees the upstream is listed `<FRESH>` with "no updates since
«destination» last executed", because `old` read the destination's upstream
at the epoch the destination reports. On Snowflake this server lists that
upstream without a badge or anchor line
([explain-catalog.md](../explain-catalog.md#clone-model)); its Databricks
copy tree matches hosted's `<FRESH>` listing (`databricks_submit_copy.jsonl`
entry 13). The seven CLONE trees whose decisions agree are keyed as Explain
gaps (entries 3, 7, 33, 39, 61, 67, 75). Tracked in #252.

Replay: `copy_source_choice.jsonl` is in the generic conformance replay
(corpus 153). Each divergent destination gap pins the local answer
(`local_payload_fields`: the copied source, its clone SQL, epoch and
runtime), so a change of the source this server chooses fails the replay.
The seven Explain-only gaps clear only hosted's `<FRESH>` badge on the
upstream line and its `no updates since «FQN» last executed` line; the
decision, description and the rest of each tree must match hosted. Every
other response, confirmation and Explain matches hosted exactly.

## Reproduce

```bash
cargo build -p dbt-state-harness --bin state-probe --bin record-proxy
python3 experiments/copy_source_choice_test.py      # warehouse-free self-test
python3 experiments/copy_source_choice.py plan      # 13 histories, 38 submits, no RPC

GOLDEN_DIR=<dir> PROXY_LISTEN=127.0.0.1:<port> ./target/debug/record-proxy & PROXY=$!
until (exec 3<>/dev/tcp/127.0.0.1/<port>) 2>/dev/null; do sleep 1; done   # wait for the proxy
PROBE_URL=http://127.0.0.1:<port> python3 experiments/copy_source_choice.py run [HISTORY ...]
RUN=$?                                              # 1: an RPC failed or a history is inconclusive
kill -TERM $PROXY; wait $PROXY                      # drains calls and writes the sidecar
python3 experiments/copy_source_choice.py table --certified <dir>/golden_*.jsonl   # 1 unless certified and conclusive
```

`run` stops at the first failed RPC and prints `{"failed": <history>, "error": …,
"not_run": […]}`. A nonzero `run` or `table --certified` means the recording
is not evidence; promote only a capture for which both exit 0.

For a rehearsal against this server, start `dbt-state-server` on another
port and give the proxy `PROXY_LOCAL_UPSTREAM=http://127.0.0.1:<server port>`.
A Databricks run (#240) can keep the same roles, timeline and histories and
change only the dialect, names and request shape.
