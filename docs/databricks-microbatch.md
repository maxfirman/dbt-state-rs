# Databricks microbatch windows

The supported bounded microbatch cases pass with pinned dbt 2.0.11 on the
tested serverless Unity Catalog warehouse, using managed Delta in `cdo_demo_db`.
Configured predicates across multiple batches have a reproducible client
capability limitation, described below. This is bounded feature evidence, not
a full hosted-conformance claim under ADR 0009.

The [original campaign](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-microbatch-2026-11-15.md)
already verifies fixed and explicitly shifted/overlapping/empty daily windows,
timestamp moves and deletions, duplicate/NULL keys, source NULL-time exclusion,
target NULL-time retention and five required own-runtime SKIPs per backend.
Its complete hosted and Rust histories each contain 70 RPCs and nine daily
batches. Those observations are retained; this supplement addresses lookback,
implicit scheduling and configured predicates without repeating that campaign.

## Window and result contract

The pinned [batch builder](https://github.com/dbt-labs/dbt/blob/v2.0.11/crates/dbt-tasks-sa/src/microbatch/mod.rs)
rounds the actual UTC end upward to the daily boundary. The pinned
[model runner](https://github.com/dbt-labs/dbt/blob/v2.0.11/crates/dbt-tasks-sa/src/runnable/model.rs)
passes that end as the incremental checkpoint: lookback N processes the current
day plus N preceding days, clamped to `begin`. Explicit CLI start overrides
that implicit start. The campaign records actual command bounds, native SQL,
manifest configuration and UTC before/after each managed invocation. Its
2026-11-15 day guard refuses rollover or starting near midnight. It does not
fake system time or source epochs, and does not claim an observed midnight
crossing. Earlier explicit shifts cover physical window movement; this run
checks actual implicit scheduling within the sampled UTC day.

An independent operation ledger declares the complete source, cumulative
target R and current selected input F before execution. A successful operation
retains target rows outside its replacement condition and adds the freshly
selected source bag. Expected rows are never read back from the target. Source,
R and F each receive typed SQL API checks and a separate literal, SELECT-only
State-disabled `dbt show` Reference execution, with a zero-RPC recorder.

| Build, followed by required unchanged reuse | Actual daily window | Independent result |
| --- | --- | --- |
| Recovery of the failed initial command | Explicit Nov 12–16 | Complete five-row bag, including current duplicates; source NULL-time and exact Nov16 boundary excluded |
| Changed source, implicit `lookback=0` | Nov 15–16 | Refresh current rows; retain old Nov12 and Nov13 rows |
| Same source, implicit `lookback=2` | Nov 13–16 | Refresh Nov13; retain old Nov12 |
| One batch with `incremental_predicates=['id >= 3']` and source SQL `WHERE id >= 3` | Nov 15–16 | Actual DML combines predicate AND time bounds; retain excluded current id1 and replace duplicate id3 |
| Widen predicate/source filter to `id >= 1` | Nov 15–16 | Replace current id1 as well; retain older days and duplicate id3 |

The successful ten-command history executes ten daily batches, five Confirms
and five nonstale own-runtime SKIPs. Each SKIP preserves the entire Delta
history and performs no native write. Builds preserve ordered schema and
managed/Delta properties. Fault probes reject fresh-source substitution, lost
retained rows, ignored lookback or predicate changes, and lost duplicates.

## Configured multiple-batch predicate limitation

The initial native command used the same graph with
`incremental_predicates=['id >= 0']` and explicit Nov12–16 bounds. It compiled
before DDL. The pinned adapter macro mutates that list by appending each batch's
time predicates. Later batches consequently carry earlier, contradictory date
ranges. The first three batches completed, including an empty third batch;
the fourth failed with dbt1308 and `Table does not support overwrite by
expression`. Exact emitted DML and the full client error are retained. There
was no Confirm, Record or Clone. The independent partial R contains only the
two earlier-day rows, and is checked separately from current F.

The same UUID graph was inspected and recovered using native default
predicates. No target reset, new UUID, adapter macro override or cleanup hid
the failure. Both original failed traffic and the continuation remain in the
whole fixture. Rust acceptance recreates the failed command and the recovery
on its own fresh pair. Single-batch configured predicates are supported by the
observed native SQL/results; the multiple-batch configured combination remains
an explicit pinned-client capability limit. These observations do not establish
every predicate expression, schema option, concurrency/failure combination or
full-refresh/Record behavior for microbatch.

## State and provenance

The complete hosted fixture is `databricks_moving_windows.jsonl`: 77 RPCs,
SHA256 `cf7f7accfa26535e02bce1d702750d41deb703797e4bf4b10b9dc585ac97650f`.
It is the byte-exact chronological concatenation of the independently certified
six-RPC failed attempt and 71-RPC same-graph continuation, including eleven
full Explain calls, validation, speculative submissions, telemetry, statuses
and metadata. No replay mask, gap or missing-history exemption is added.

Whole-history public replay first found the recovery BUILD described as a
missing table despite positive current target evidence and no confirmed build.
The correction is limited to that Databricks microbatch presentation: it still
BUILDs with rejection reasons 6/6, records no freshness anchor, and lists source
timestamps without freshness badges or an invented last execution. Unknown or
nonpositive epochs, absent targets, other tested dialects and uncharacterized
policies retain their prior outputs. Strict whole-history replay and public
negative controls preserve the original red receipts separately.

Raw secret-free artifacts are retained under
`~/.local/state/dbt-state-rs/databricks-150-moving-20261115/`. The original hosted
and continuation entrypoint snapshots remain distinct from strengthened final
guards; offline validation checks those guards against original saved native
SQL, complete typed/reference results, statuses, epochs and history. This
attribution does not imply the final guards ran in the earlier process.

The Rust client omitted an optional speculative probe on `late_zero_repeat`;
its ordinary response was the required own-runtime nonstale SKIP. The original
harness required a speculative probe and stopped before its read-only checks.
Its complete 33-RPC capture and actual audit clocks are retained separately.
The guard now permits zero or one speculative call, still requiring exactly
one ordinary submission and the same status/runtime/no-write assertions.

The test server's isolated State schema had already been cleaned up. A second
current TestServer restores only the five genuine prior ordinary requests and
two successful Confirms through public gRPC. Only generated request IDs are
mapped; original payloads, source epochs and genuine Confirm outcomes/runtime
are unchanged. This is explicitly public history hydration, using current
System-clock metadata, with a retained ID/request/response receipt. It performs
no warehouse command. The saved SKIP receives its missing physical/reference
checks and Explain through the mapped decision; only the remaining six native
commands run. No completed managed command is repeated. The original 33-RPC
prefix passes strict targeted replay with its actual audit clocks. The separate
50-RPC hydrated/native session, SHA256
`58a8ebe75d1183edd7bb02fe0cd3cffb93e40f12321f44f7f224fda588b63935`, retains
its own actual audit clocks for the final combined gate. The sessions are never
concatenated into an invented uninterrupted history. Native continuation passed
in 182.24 seconds; final offline validation checks all 22 hosted/Rust physical
checkpoints, including the partial result and resumed saved SKIP. Production
inventory is 57→59 hosted and 59→61 Rust; the reference inventory stays 100.

Only `mw150_<UUID>_{inputs,consumer}` in the existing owned
`ds_20261112a134ef_prod` schema are writable. Hosted UUID:
`8673784da9c243cc81b349a7c3a26f13`; Rust UUID:
`a437b28633cb4cafad32667a9b30bae3`. Paginated inventory and ownership evidence
protect all previous graphs. Exact staging/backup names and session daily
temporary views are reserved separately. No new schema, persistent reference,
old-object write, copy or cleanup is part of the campaign.
