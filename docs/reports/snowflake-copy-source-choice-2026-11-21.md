# Snowflake copy source choice among competing sources — 2026-11-21

Live characterization for #239 (part of #236): which existing physical result
hosted State offers to copy when several Snowflake builds of one model could
be the source of an ordinary FULL submit into an absent Target relation. Each
fact is marked **Observed** (read from the capture) or **Inferred** (the
likeliest hosted rule, not proven). This note changes no server code. The
comparison with Databricks (#240) and any policy change belong to #242 and
#243.

## Provenance

| | Main run | Completion-order controls |
|---|---|---|
| Driver | `experiments/copy_source_choice.py run` | `experiments/copy_source_choice.py run old_mid_late old_new_late old_mid_new_late` |
| Run id | `2346130f59d0` (preceded by the control-only run `23455314d580`) | `0012045393fe` |
| Window (UTC) | 2026-11-21 23:45:56–23:48:10 | 2026-11-22 00:12:07–00:12:45 |
| Fixture | `golden/fixtures/copy_source_choice.jsonl` entries 0–55 | same file, entries 56–75 |
| Metered submits (`SubmitEnrichedSQL`) | 28 | 10 |
| Other calls | 18 confirms, 10 `state-probe` Explains | 7 confirms, 3 Explains |

- Every request is crafted with `target/debug/state-probe` through a
  recording proxy on this machine; no dbt client and no warehouse is
  involved. The requests copy the shape client 2.0.11 sends for a FULL table
  model (`data_test_history.jsonl` entry 27) and name never-created relations
  in `CDO_DEMO_DB`. Nothing was written to Snowflake.
- Both proxy sessions are certified complete
  (`capture_integrity.verify_capture`: 56/56 and 20/20 RPCs). The fixture is
  the two sessions concatenated, unedited. It carries no credential: the
  proxy drops transport and auth headers.
- Rehearsals against this server at `2f61c43` (same plan, runs
  `2343306f73ca` and `0009160c2b2c`, Postgres database `i239_rehearsal`)
  give the "Rust" column below. Raw sessions, driver output and rehearsals
  are out of the tree at `~/projects/worktrees/dbt-state-rs/notes-239/239-evidence/`.
- `experiments/copy_source_choice.py table golden/fixtures/copy_source_choice.jsonl`
  re-derives the results table from the fixture alone. It names the source a
  grant copies (`clone_source`) and checks that the grant's
  `clone_required_last_modified_epoch` and `execution_runtime_ms` are that
  source's confirmed completion epoch and runtime. It flags a history as
  inconclusive when a source submit is not a first build, a source confirm is
  refused, the destination reports its own Target relation, or any epoch is
  later than the moment its request was sent. No history was flagged.

## Setup

One history per row of the results table. Each history has its own model
(`model.jaffle_shop.csc<run>_<history>`) and its own upstream names, so no
history can see another's builds. Within a history every submit has the same
model unique id, output object name (`CSC<RUN>_<HISTORY>`), catalog
(`CDO_DEMO_DB`) and connection namespace (`fc38a5ba0eceb701c6ab7c6b882e28b0`,
the demo namespace). Each role has its own schema, default schema and
`target_name`:

| Role | Target relation schema | Reads | Role in the question |
|---|---|---|---|
| `old` | `CSC_OLD` | `CDO_DEMO_DB.csc_up.<src>`, after its change, at the epoch the destination reports | the eligible source: fresh, same physical input as the destination |
| `mid` | `CSC_MID` | `CDO_DEMO_DB.csc_up_alt.<src>`: same object name, another schema | same SQL once schemas are abstracted, different physical input |
| `new` | `CSC_NEW` | `CDO_DEMO_DB.csc_up.<src>`, before its change | same physical input, stale for the destination at Lag tolerance 0 |
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

**Timeline** from a history's base B (each request was sent at most 7 ms after
its planned time):

| Step | When |
|---|---|
| `new` submits, reporting `csc_up.<src>` at B − 60 s | B |
| `csc_up.<src>` changes (no request) | B + 0.5 s |
| `old` submits, reporting `csc_up.<src>` at B + 0.5 s | B + 1.5 s |
| `mid` submits, reporting `csc_up_alt.<src>` at B − 60 s | B + 3 s |
| completion epochs, in the history's completion order | B + 4 s, B + 5 s, B + 6 s |
| confirms, in the history's confirmation order, each after its epoch | from B + 7 s, 1.5 s apart |
| destination submits, then is explained 2 s later | 2 s after the last confirm |

Every source is submitted before any is confirmed, so no source could be
offered another source's result (each answered 6 / 6, "model was executed
because its table did not exist"). A source that read the upstream before
its change was necessarily submitted first: submit order is the same in
every history and was not varied.

## Results

Completion order and confirmation order are oldest or first on the left.
Entries are the destination's submit and Explain.

| History | Sources | Completion | Confirmation | Hosted | Entries | Rust (rehearsal) |
|---|---|---|---|---|---|---|
| `old_alone` (positive control) | old | old | old | CLONE `old` | 2–3, 6–7 | CLONE `old` |
| `mid_alone` | mid | mid | mid | BUILD 6 / 6 | 10–11 | CLONE `mid` |
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
cloned that one", with `clone_source` the `CSC_OLD` relation and its
`clone_required_last_modified_epoch` and `execution_runtime_ms` equal to
`old`'s confirm. Every hosted BUILD is `ready_to_execute`, decision 1,
codes 6 / 6, `is_stale: false`, "model was executed because its table did
not exist".

`_rev` and its base history share completion epochs and submit times and
differ only in confirmation order. `_late` and the `_rev` history share
confirmation order and send times and differ only in completion epochs (the
fresh source completes last). The `_late` histories ran 24 minutes after
their `_rev` partners, in their own run.

## Findings

1. **Observed: the positive control copies the oldest fresh source.** Both
   runs of `old_alone` granted `CSC_OLD`.
2. **Observed: each competitor alone builds.** `mid_alone` and `new_alone`
   both answered BUILD 6 / 6, and both Explain trees say "the model table
   does not exist" [FAIL] and "there is no other table that the model could
   be cloned from" [FAIL] (entries 11, 15). Neither tree names the candidate,
   so the two refusals look the same on the wire.
3. **Observed: with competitors, hosted copies the eligible source in every
   history.** Adding the stale `new`, the different-input `mid`, or both
   changes nothing observable: the same source, epoch, runtime, codes and
   description as `old_alone`. Neither competitor blocks the copy, whether
   it completes before or after `old` and whether it is confirmed before or
   after it.
4. **Observed: neither ordering decides the winner.** Holding completion
   order fixed and reversing confirmation order (`_rev`) leaves the grant on
   `old`; so does then reversing completion order (`_late`). `old` wins as
   the first and the last confirmation, and as the oldest and the newest
   completion.
5. **Inferred: hosted searches past an ineligible candidate in either
   position.** A rule that considers only the newest completion would have
   built in `old_new` and `old_new_rev`, only the oldest completion in the
   `_late` histories, only the latest confirmation in `old_new` and `old_mid`,
   and only the first confirmation in `old_new_rev` and `old_mid_rev`.
   Eligibility (finding 2) explains every winner. With one eligible source per
   history, the order in which hosted examines candidates is not observable.
6. **Inconclusive: why the middle source is ineligible.** The `mid`
   refusal fits two rules that this design cannot tell apart: hosted does not
   treat SQL that reads `csc_up_alt` as the same logic as SQL that reads
   `csc_up` (its schema abstraction is narrower than "every schema"), or it
   treats the build as an equivalent whose recorded inputs cannot prove the
   destination's upstream fresh. Both read the same SQL text, and a crafted
   request cannot separate the relation a build read from the relation its
   SQL names. Earlier Snowflake captures in which hosted copied across
   environments had each upstream in its reader's own schema
   (`explain_clone_history.jsonl` entry 12); here neither upstream schema is
   the reader's.

Unreachable here: a second eligible source competing with `old` (which
would expose search order), and a destination with its own Target relation
or history. Physical rows and the client's handling of a grant are covered by
the native experiment (#237).

## This server

**Observed (rehearsals, `2f61c43`):** this server agrees in 7 of the 12
histories and differs in five, all keyed in `golden/replay_gaps.json` as
open divergences:

- `mid_alone` (entry 10): it copies `mid`, an equivalent in another
  environment copied unjudged (`submit_decision/copy_search.rs`), where
  hosted builds. That copy serves a result computed from
  `csc_up_alt.<src>` to a destination that reads `csc_up.<src>`.
- `old_mid`, `old_mid_rev` (entries 20, 26): it copies `mid`, the newest
  completion, where hosted copies `old`. Same decision, codes and
  description, so these gaps are field-scoped to `clone_source`,
  `clone_sqls`, `clone_required_last_modified_epoch` and
  `execution_runtime_ms`.
- `old_mid_new`, `old_mid_new_rev` (entries 46, 54): it passes over the
  stale `new`, then stops at `mid` and builds, because after a freshness
  rejection it copies only another same-upstream equivalent. Hosted copies
  `old`.

Its candidate order is the completion epoch, newest first: `old_mid_rev`
and `old_mid_late` differ only in completion order and it copies `mid` in
the first and `old` in the second. Confirmation order changes none of its
results.

**Explain (outside #239's source-choice scope).** In each of the 11 hosted
CLONE trees the upstream is listed `<FRESH>` with "no updates since
«destination» last executed", because `old` read the destination's upstream
at the epoch the destination reports. This server lists that upstream
without a badge or anchor line on Snowflake
([explain-catalog.md](../explain-catalog.md#clone-model)), while its
Databricks copy tree matches hosted's `<FRESH>` listing
(`databricks_submit_copy.jsonl` entry 13). The seven CLONE trees whose
decisions agree are keyed as Explain gaps (entries 3, 7, 33, 39, 61, 67, 75).

Replay: `copy_source_choice.jsonl` is in the generic conformance replay
(corpus 148). Every other response, confirmation and Explain matches hosted
exactly.

## Reproduce

```bash
cargo build -p dbt-state-harness --bin state-probe --bin record-proxy
python3 experiments/copy_source_choice_test.py      # warehouse-free self-test
python3 experiments/copy_source_choice.py plan      # 12 histories, 36 submits, no RPC
GOLDEN_DIR=<dir> PROXY_LISTEN=127.0.0.1:<port> ./target/debug/record-proxy &
PROBE_URL=http://127.0.0.1:<port> python3 experiments/copy_source_choice.py run [HISTORY ...]
kill -TERM <proxy pid>                              # writes the integrity sidecar
python3 experiments/copy_source_choice.py table <dir>/golden_*.jsonl
```

For a rehearsal against this server, start `dbt-state-server` on another
port and give the proxy `PROXY_LOCAL_UPSTREAM=http://127.0.0.1:<server port>`.
A Databricks run (#240) can keep the same roles, timeline and histories and
change only the dialect, names and request shape.
