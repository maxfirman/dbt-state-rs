# Freshness against recorded upstream epochs and producers, gated by lag tolerance

## Status

Accepted (2026-11-10). Supersedes the earlier own-build freshness anchor (see
[correctness-handoff.md](../correctness-handoff.md)), which was characterized
only with External modifications of dbt-managed upstreams. Amended by
#83/#90 (2026-11-11): which build a submit is judged against when the node's
execution type changed ([below](#execution-type-change)), and by #116
(2026-11-11): only over the submit's genuine upstream names. Amended by
#158 (2026-11-14): physical completion ordering and unknown-epoch safety.
Amended by #200 (2026-11-21, maintainer decision option A): a Snowflake data
test's State history is its history key ([below](#data-test-history-key)).
Amended by #264 (2026-11-22): a Result copy on submit requires the physical
inputs the equivalent recorded ([below](#result-copy-over-different-inputs)).

## Decision

A model's reuse candidate is assessed upstream by upstream against what its
last confirmed build recorded:

- **Drift**: an upstream drifted when its current epoch is later than the epoch
  recorded for it at that build. The node's own build epoch is not the
  comparison point.
- **Producer**: each recorded input stores the logic identity of the build in
  State history that produced the relation's contents when it was read, or that
  no recorded build did (a source, or an External modification). If the current
  producer differs from the recorded one, the upstream counts as an upstream
  query change and the node rebuilds regardless of the stale-upstream policy
  and Lag tolerance.
- **Lag tolerance**: when the policy requires a rebuild, the node is reused
  instead while its build age is under the tolerance. Build age is the server
  clock minus the build's ConfirmExecution epoch; on DuckDB, whose epochs are
  ordinals, it is the server clock minus the time the server received the
  confirmation.

## Why

The hosted service behaves this way (live characterization 2026-11-10:
`experiments/lag_matrix.py`, the docs' worked example through the real client,
fixtures `lag_tolerance_*.jsonl`, scored by `experiments/lag_rule_check.py`).
The own-build anchor missed upstream changes made between a node's submit and
its confirmation, and could not express why an External modification of a
consumed build rebuilds under ALL while a recorded rebuild of that upstream is
reused within tolerance.

## Considered Options

- **Store the producing execution id** instead of its logic identity. Rejected:
  hosted treats a same-logic rebuild of an upstream as a data change (reused
  within tolerance) and a changed-logic rebuild as an upstream query change, so
  the comparison must be on logic identity.
- **A ledger of what earlier skips saw.** Rejected: hosted rebuilds under ALL
  once every upstream has drifted since the build, even if an earlier skip
  already saw one of them.
- **Derive producers at decision time from history ordering.** Rejected: it
  needs the upstream's build history as of the node's submit, which later
  confirms can obscure; recording it at submit time is exact.

## Consequences

- Inputs recorded before producers were stored read as unrecorded and never
  count as an upstream query change; they keep the drift and tolerance rules.
- A RecordExecutions batch resolves producers against State history and the
  batch itself, as the batch's builds are not yet in history. A consumer
  records the batch build that produced what it read: the earliest build of
  that relation not older than its read. A read later than every batch build,
  records no build, as a later submit would resolve it too. Physical batch
  outcomes without an epoch are rejected atomically (#158); legacy unknown
  physical rows cannot establish a producer.
- Copies registered with RegisterClone record no inputs. Their freshness stays
  anchored on the copy's own build epoch, so a copy is never treated as
  fresher than when it was made.
- A seed's producer identity includes its values, so a reloaded seed with
  changed values or config counts as an upstream query change, and a
  same-values reload as a data change. Characterized live on 2026-11-10
  (`lag_seed_upstream.jsonl`): hosted does the same.
- The same freshness judgment decides which other build may stand in when the
  target's own build must rebuild (2026-11-10, `lag_copy_candidates.jsonl`).
  A same-environment equivalent under another name is judged as if it were
  the reuse candidate, on its own build age, and copied over the target (or
  into it, when dropped) when it qualifies. An older build of the target
  itself is never reused (the latest build wins), but when one would qualify,
  the rebuild reports LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH as
  hosted does; a dropped target whose latest build would qualify reports
  TARGET_TABLE_DOES_NOT_EXIST / TARGET_TABLE_MATCH.

## Execution type change

Amendment (#83, #90; approved by the maintainer 2026-11-11).

**Rule.** The SQL identity a submit is compared on excludes its execution
type. When the latest build in scope has the submit's identity but another
execution type, and recorded the submit's genuine upstream names (its inputs
other than its own Target relation), the submit is judged against the latest
build in scope with its identity *and* execution type:

- if there is one, it is judged as the reuse candidate would be for its own
  target: a Target relation modified after it rebuilds with "the table was
  modified after the cached execution" (6 / 6), a dropped one as a first
  build;
- if there is none, the rebuild is EXECUTION_TYPE_MISMATCH (2 / 2) with the
  generic rebuild description, against the latest build.

A RecordExecutions row is a build like any other: a `--full-refresh` run of an
incremental is recorded as FULL, so the next APPEND/MERGE submit is judged
against the last incremental build, and a later FULL submit with the same
identity reuses the recorded row.

A RegisterClone copy has the submit's identity when the submit has the logic
the copy inherited from its source build, as for its adoption (#93). So a
table submitted over an incremental's copy, after a materialization change,
with the same SQL and semantic extras is EXECUTION_TYPE_MISMATCH (2 / 2)
against the copy (clarification, #104, live:
`explain_copy_logic_mismatch.jsonl` `kf4`). Other logic, including other
semantic extras (a view, a custom node), is
`no_reuse_candidate.copy_logic_mismatch` (6 / 6).

**Other upstream names** (amendment, #116, 2026-11-11). A latest build
with the submit's identity under another execution type that recorded other
genuine upstream names is another identity, as hosted treats it, not an
execution type change: `no_reuse_candidate.upstream_names_changed`, with
the identity-mismatch answer (6 / 6, or 8 / 7 when an older build of the
submit's type would qualify) and that latest build as the unmatched build
Explain judges against. On client 2.0.11 this is the same SQL switched
between a view, which reports no upstreams, and a custom node, which shares
its semantic extras, over an upstream (live:
`explain_reason_kind_cells.jsonl` `vc`, `cv`; with no upstream on either
side it stays 2 / 2, `vc0`, `cv0`). Both answers rebuild, so no reuse
changes. A RegisterClone copy records no inputs of its own and keeps the
rule above.

**Evidence** (live, client 2.0.11): `explain_snapshot_history.jsonl`
entries 58–60 (FULL over SNAPSHOT history: 2 / 2);
`explain_incremental_append_merge_history.jsonl` entries 69–79 (APPEND and
MERGE after a recorded full refresh: 6 / 6, "modified after");
`explain_execution_type_change.jsonl` (`experiments/explain_capture.py
execution_type_change` then `execution_type_change_append --run-id`): FULL →
APPEND and MERGE → DELETE_INSERT give 2 / 2; switching back gives 6 / 6
"modified after"; FULL → VIEW is an identity change (a view sends other
semantic extras), so switching back to FULL reports the older FULL build as
LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH (8 / 7), the existing
older-match rule; a FULL submit over a recorded FULL full refresh of an
APPEND model SKIPs; APPEND over FULL, VIEW, FULL history gives 2 / 2.

**What stays.** Only the latest build in scope is ever reused. The older
build of the submit's type is a reference for the rebuild's reason, codes and
Explain anchor ([ADR 0005](0005-re-derive-explain-tree-on-demand.md)). When
nothing is against it (its Target relation unchanged since, which a newer
build of the same relation should make impossible), the submit is still
rebuilt, as the execution type change against the latest build: a local,
conservative choice no capture contradicts. The lookup reads the target's 50
newest builds (`Store::target_history`); a build of the submit's type older
than that is not found, and the rebuild is the execution type change. The
identity comparison is
unchanged: on DuckDB the schema-abstracted logic identity includes the
execution type, so a DuckDB execution type change stays an identity
mismatch.

**Considered.** Leaving RecordExecutions rows out of decisions also explains
#90, but not the SKIP after a recorded FULL full refresh. Preferring the
latest build of the submit's type whatever the newer builds' identity does
not explain FULL → VIEW → FULL, where hosted judged the latest (view) build.


## Physical completion ordering and unknown epochs

Amendment (#158, 2026-11-14). Complete hosted Snowflake and Databricks histories
vary completion epochs independently from confirmation delivery, with different
SQL identities and equal-epoch controls (`databricks_lifecycle_ordering.jsonl`).
Independent Databricks writes verify the relation contents in both orders.

**Rule.** Within a physical scope, the greatest completion epoch is current;
confirmation receipt and insertion order break equal-epoch ties. Select the
current row before identity and eligibility. This definition is shared by own
Target relation history, current upstream producers and current copy sources.
Logical UID histories and immutable copy ancestry at a child's submit time
retain their separate ordering semantics.

**Unknown evidence.** Physical Confirm/Record outcomes must include an epoch.
Confirm rejection leaves pending evidence unpublished and permits a valid
retry; an invalid Record batch publishes neither executions nor selector state.
Existing confirmed rows without epochs remain a conservative physical cold
miss, including with `ignore_external_modifications=true`, and cannot supply
copies. This is a legacy/corruption safety floor, not a claim about hosted
recovery of unknown physical evidence.

**Considered.** Putting unknown epochs last can resurrect an older matching
identity after an untracked replacement. Recovering on a later publication is
also insufficient: a known pending execution may have started before the
unknown replacement and confirm afterward; historical Record may arrive late
as well. Neither delivery proves a new warehouse build after the barrier.
The cold-miss limitation preserves safety without inventing chronology.
Genuine matching-version frozen histories and current-version storage-fault
public tests cover both paths.

## Narrow Databricks direct-copy provenance (#152)

The native MERGE and check-snapshot pre-copy captures have no source type or
format field. A successful warehouse `SHALLOW CLONE` attempt can establish a
copy without declaring its source provider. This does not authorize a SKIP.
For the captured single fully-qualified `SELECT * FROM` input, new versioned
provenance binds exact logic/config/full UID and retains the actual source
context. RegisterClone freezes that source's recorded input epochs and producers
onto its pending copy. Adoption judges freshness against those inputs, including
changes before the copy, rather than synthesizing a baseline at the copy epoch.
Snowflake and DuckDB retain their existing copy-age rule.

Confirm clears this adoption evidence when the latest scoped source ID/epoch
changed after registration, any source write remains pending, or the client
reports failed cloning. Registration likewise refuses a source with any
pending write. Unknown
versions, legacy NULL, unsupported SQL/template/dependency shapes and missing
source input epochs/producers never authorize direct adoption. The nullable
migration does not reinterpret old fingerprints or backfill missing evidence.

Unobserved physical source modification during the unversioned DDL remains an
unresolved protocol boundary: Confirm and the adopter carry no new observation
of the source relation. This narrow offline implementation is not a completed
Databricks copy-safety or hosted-conformance claim; see the
[implementation report](../reports/databricks-direct-precopy-2026-11-15.md).


## Databricks volatile producer after External modification (#213)

The paired hosted history showed a zero-tolerance consumer rebuild and a
one-hour consumer reuse after an ordinary volatile FULL rebuild over unchanged
inputs. After an External modification to that model's only managed input, both
consumers rebuilt and Explain reported an upstream query change. On the next
rerun with the edited input unchanged, the one-hour consumer reused again.
The producer's original SQL and the consumers' original SQL were unchanged.

The maintainer approved this specific Databricks transition. An ordinary FULL
`rand()` projection over one directly read managed input can establish a
producer revision if the Target relation is as built, the sole input's epoch
advanced, and the same ordinary FULL input producer still has a confirmed
row at the recorded epoch. There must be no pending State write for the
input. The Delta requirement follows the tested Databricks scope, since the
pinned client omitted the table type when confirming this managed input.
The producing model's pending row stores the revision and publishes it on
confirmation. Ordinary same-identity builds retain an established revision
across further input edits or missing evidence. A same-identity
`RecordExecutions` build at or after the current build's epoch retains its
revision. Consumers recorded in the same batch resolve against the revised
producer identity. History before migration 0030 has no revision.
Revisions leave SQL identity and Reuse qualification unchanged and carry no
rendered values. The usual producer comparison then explains the upstream
query change and applies the same Lag-tolerance rule to consumers. Broader
transitive producer rules remain uncaptured.

[Hosted capture and physical readback](../reports/databricks-volatile-priority-2026-11-21.md)

## Data-test history key

Amendment (#200, maintainer decision option A, 2026-11-21). It amends reuse
invariant 1 (AGENTS.md) for data tests.

**Evidence.** Live on Snowflake with client 2.0.11
(`golden/fixtures/data_test_history.jsonl`,
`data_test_history_probes.jsonl`;
[characterization note](../reports/snowflake-data-test-history-2026-11-21.md)).
Hosted reuses the latest confirmed run of the same normalized SQL over the
same input relations, with the same `severity`, in the same
`table_namespace`, whatever the unique id, `project_id`, `target_name`,
`default_schema` (fully qualified SQL), evaluation flag and client node
hashes. A dev relation hosted copied from prod, unchanged since, counts as
the prod relation. Within one key only the later-confirmed run is served,
failures included (P66, P74, F79); an older run under another key stays
reusable (F56, P30, P44, P52, P58).

**Rule.** For a Snowflake data test (`Warehouse::keys_data_test_history`):

- *The unit of history is the key.* State history is scoped by
  organization, warehouse family, `table_namespace` and the versioned key
  `dbt-state-data-test-key-v1` (`analyzed_workload::identity::data_test_history_key`,
  `executions.data_test_key`, migration 0029). The key holds the normalized
  SQL with each input relation read as the relation it stands for (below),
  every `semantic_extras` entry, and the query dependencies (name,
  definition and context). `severity` is captured; `fail_calc`, `warn_if`,
  `error_if`, `store_failures`, `store_failures_as`, `limit`, `where` and any
  other entry are kept conservatively, since over-separating only costs a
  rebuild. The default catalog and schema are in the key unless the SQL is
  context free: it parses, names every relation in three parts (a CTE
  reference does not count), and calls no function, table function or
  sequence, any of which can resolve through the session's schema.
  Unparseable SQL keys its exact text with its context. The unique id and
  `project_id` are not in the key; the organization always is.
- *Latest run per key.* Only the latest confirmed run under the key
  (`confirmed_at`, as before for data tests) is a reuse candidate. Nothing
  older in the key is resurrected: a newer failing run is served as such.
  The ordinary per-input freshness check then applies (invariant 2).
- *Other keys stay reusable.* An older run of the same unique id under
  another key stays reusable after a newer run under this key. A data test
  has no Target relation a later run overwrites, so an older run still
  describes the inputs it read, and its inputs are judged exactly as before.
- *Copies stand for their source only while unchanged.* An input whose
  current physical state is a copy this server recorded (a Result copy on
  submit or a RegisterClone) with its source build recorded
  (`executions.clone_source_execution_id`, migration 0029) stands for that
  build, at its epoch and with its Upstream producer, while the copy is
  unchanged: its reported epoch is known and no later than the copy's
  confirmation epoch, the rule a Target relation as built already follows
  (invariant 3). The source must be an ordinary confirmed build of the copy's
  source relation in the same organization, warehouse family and namespace;
  a copy of a copy stands for itself. A stand-in proves freshness only at
  exactly the epoch recorded for its source: an earlier one means the copy
  holds an older build than the run read, which is unprovable
  (`decision::upstream_freshness`). Any other input stands for itself;
  names and schema abstraction never make two relations one (P48–P59).
- *`compare_unrendered_code`* does not change the key (F35, F80, P54–P59).
  The dual rendered/template identity data tests had before #200 (reuse
  invariant 5) is retained, restricted to keyed rows: when the key has no
  run, the latest run of the submit's unique id in its project, organization,
  warehouse family and `table_namespace` is a reuse candidate when it was
  recorded with a key and has the submit's template identity
  (`submit_decision::select_sql`,
  `reuse_candidate::data_test_identity_matches`). This alternative is not
  captured under the key: the one #200 cell that changes the rendered SQL
  under `compare_unrendered_code` with the client hashes unchanged (P54–P59)
  also changes `default_schema`, which the server's template identity
  holds, so both sides build there. It keeps the
  unique-id and project scope because a generic test's unrendered body is
  shared by every test of its kind. A row recorded before the key never
  matches through it
  (`data_test_history.rs::a_template_match_under_another_key_reuses_the_unique_ids_keyed_run`).
- *Rows recorded before the key.* A data test without a key never matches,
  through the template alternative either, and a copy without a recorded
  source build stands for itself: each rebuilds once. No row is rehashed
  (ADR 0004).
- *Other Warehouses.* Databricks, DuckDB and every other Warehouse keep one
  latest run per unique id in its project and physical scope (uncaptured).

**Considered.**

- *Keep `project_id` as a stricter scope.* Rejected by the decision: hosted
  reuses across projects (P12–P15), and the organization boundary already
  isolates tenants.
- *Unchanged since the copy as an exact epoch match.* Rejected: Snowflake
  reports a copy's metadata epoch slightly before its confirmation on later
  reads (F50, F76: 1795290449321 against 1795290449575), and hosted still
  reads the copy as its source. The as-built rule is the bound this server
  already trusts for a Target relation.
- *Schema-abstracted SQL, as the Snowflake Result copy logic identity.*
  Rejected: hosted separates a dev relation of the prod relation's name and
  epoch that it did not copy (P50, P56).
- *Epochs in the key (a content address).* Not distinguished by the capture
  from choosing the latest run and then judging freshness; they differ only
  under Lag tolerance, which clients send as 0 for data tests. The decision
  takes the latest run first.

**Consequences.** Explain lists an input read through a copy by the copy's
name, dated at the build it stands for, without a verdict (F36–F39, F57,
F81, F83, F84), and explains a reuse under `compare_unrendered_code` by the
unrendered code ([ADR 0005](0005-re-derive-explain-tree-on-demand.md) "Amended
by #200"). Two blind spots of copies carry over as accepted risks:

- *A source modified before a submit-time copy.* An External modification of
  the source between its build and a Result copy on submit is not visible to
  the server, so the copy stands for the source build and a data test over
  it reuses the source's run over the contents before the change. The
  server has no evidence of the source's epoch at copy time: the submit
  reports no epoch of the source (the captured dev submit sends no
  `tables`, `data_test_history.jsonl` entry 27), and the copy's confirmation
  reports the copy's own epoch, its clone time (1795290449575 at entry 32,
  against the source build's 1795290435994 at entry 9).
  `clone_required_last_modified_epoch` is what the server asks of the
  client, not evidence that the client checked it. This is the risk model
  copy adoption already accepts for a FULL copy and a seed copy (ADR 0004,
  [correctness-handoff.md](../correctness-handoff.md) "Seed copy on
  submit"). A RegisterClone names the source epoch it copied
  (`clone_source_last_modified_epoch`), and records the source build only
  when that epoch is the build's. A run of the data test that read the
  modified source is the latest in the key, and then the copy no longer
  proves freshness
  (`data_test_history.rs::a_source_modified_before_a_submit_time_copy_is_not_visible`).
- *A source rewritten between the copy decision and the client's clone DDL*
  is not detected (the accepted unpinned source race, ADR 0004).

## Result copy over different inputs

Amendment (#264). It amends reuse invariant 4 (AGENTS.md) for the SQL
submit's Result copy fallback on Snowflake and DuckDB. Databricks already
requires exact current inputs (`copy_lifecycle::source_admits`).

**Evidence.** A native Snowflake run of #237 case D against this server
(dbt 2.0.11, run `002752a5958d`) copied a prod downstream for a dev
downstream whose dev upstream had been built independently from a dev
input. The dev downstream then served the prod rows `(1, 'prod')` where
the State-disabled Reference execution served `(1, 'dev')`. The schema-
abstracted logic identity held, and nothing tied the equivalent's recorded
upstream read to the relation the submit reads. Hosted built that
downstream in both runs, and in cases B and C
(`explain_upstream_origin_*.jsonl`,
[report](../reports/snowflake-upstream-origin-2026-11-22.md)). In #239's
crafted histories it never copied a source that read another relation of
the same name, and copied an older source that read the destination's
upstream past it (`copy_source_choice.jsonl`,
[report](../reports/snowflake-copy-source-choice-2026-11-21.md)). Hosted
copied case A, where both downstreams read the one physical prod upstream.

**Rule.** An equivalent under another name qualifies for a submit's Result
copy only when the build whose result it holds was computed from the
physical inputs the submit reads (`submit_decision::copy_search`):

- *A copy is judged by the build it holds.* An ordinary build holds its own
  result. A copy this server recorded (a Result copy on submit or a
  RegisterClone) holds the result of the build it was copied from, followed
  through any copies between (`clone_source_execution_id`,
  `submit_decision::held_build`). Its inputs are the input epochs that build
  recorded, with that build's own Target relation set aside (not the
  copy's). A copy whose build cannot be established is passed over. Before
  this, a CI copy registered after its upstream changed was judged by its
  later confirmation and looked fresh.
- *Each input is itself, or its Stand-in.* An input that build recorded by
  the same canonical name is itself. An input it did not record, but that is
  an unchanged copy this server recorded, is read as its Stand-in (that
  source build, at its epoch and with its Upstream producer;
  `stand_in::input_reads`, the rule of
  [Data-test history key](#data-test-history-key)) when the build recorded
  the Stand-in's relation at exactly the Stand-in's epoch. This is a
  qualification, checked before any freshness policy, so Lag tolerance
  never admits a Stand-in of another build. A copy of a copy stands for
  itself, and so does a copy of a relation the submit also reads directly.
- *Each reference keeps its binding.* With a Stand-in, the submit's SQL and
  its query dependency definitions, with each copy read as its source (and
  its Target relation's schema read as the build's), must have the build's
  copy logic identity. Two copies swapped between the references of a join,
  in the root SQL or inside a view's definition, read the same relations but
  bind them the other way, so they differ. Only Snowflake's
  schema-abstracted identity supports the check; elsewhere no Stand-in is
  used.
- *The submit's own Target relation is the only read set aside.* The
  build's Target relation is a genuine input of a submit that reads it.
- *Every base relation the SQL reads is reported* (Snowflake, for a kind the
  Warehouse rules copy; `copy_search::sql_reads_are_reported`). A base
  relation is one the root SQL, or the definition of a query dependency it
  reads, names in FROM or JOIN and that is no query dependency itself (a
  view read through its definition, which the copy logic identity binds to
  its name); a CTE reference or a view's declared name is none. A read
  nothing reports proves nothing, also when another input is reported. A
  read of the submit's own Target relation is never covered: its prior
  contents differ between the two Target relations. A kind that only
  reports its equivalent in its codes keeps reporting it (client 2.0.11
  reports no reads for a view). DuckDB's checked copy graph already binds
  each physical input exactly.
- *Then it is judged as the reuse candidate,* over the build's recorded
  epochs and with the equivalent's own build age (a copy's is its copy
  time; hosted copied a copy within Lag tolerance whose source build was
  older, `lag_copy_candidates` `wba`).
- *A copy records the inputs of the build it holds*
  (`copy_search::held_inputs`): each upstream at the epoch and with the
  Upstream producer that build recorded, or as read through a Stand-in.
  Its next submit is then judged against what its rows were computed from,
  so a copy made within Lag tolerance is reused only while the tolerance
  still holds, and a stricter resubmit rebuilds it.
- *Any other equivalent is passed over.* The search goes on to the next one
  in the shared order (newest build first, `COPY_CANDIDATE_LIMIT`). Before
  #264 the first equivalent that read other relations was copied without a
  freshness judgment.
- *A copy already granted is revalidated before reuse.* A Result copy on
  submit that is the Target relation's Reuse candidate is reused only while
  the build it holds qualifies for the submit by the rules above and the
  copy recorded that build's inputs
  (`submit_decision::revalidate_copy_candidate`,
  `copy_search::records_held_inputs`). One granted by an earlier server over
  another input, or recorded at the newer epochs its submit read, is refused
  at Reuse qualification and rebuilds (6 / 6, the generic rebuild
  description, no Explain tree). A
  RegisterClone copy keeps its adoption rule (#93), and Databricks its
  publication checks.

The copy's pending row records the submit's own inputs as read, so its next
unchanged submit is judged over them.

**Considered.**

- *Keep copying the first equivalent that read other relations, unjudged.*
  Rejected: it serves rows computed from another input (case D).
- *Match inputs by schema-abstracted name.* Rejected: it is the rule that
  served the wrong rows; hosted builds over an independently built dev
  upstream with the same row (case C).
- *Compare the set of inputs only.* Rejected: two copies swapped between a
  join's references have the same set and epochs but project the other
  input's values (review of #266).
- *Judge a copy's resubmit over the epochs it recorded at copy time.*
  Rejected: a copy made within Lag tolerance then looked fresh forever
  (second review of #266).
- *Invalidate every copy recorded before #264.* Not taken: revalidating
  each copy against the build it holds keeps the ones that qualify and
  needs no migration.
- *Admit a copied upstream at the copied build's identity, whatever epoch
  the equivalent recorded.* Not taken: the exact-epoch rule is the one
  Stand-ins already follow, and hosted built case B, where the equivalent
  recorded the copied upstream at its metadata epoch, earlier than the
  build's confirmation (T34, I28). It copied the same shape when the
  equivalent recorded the build's epoch (`explain_clone_history.jsonl` entry
  12). Whether hosted's deciding condition is this epoch is open
  ([correctness-handoff.md](../correctness-handoff.md#open-hosted-questions)).

**Consequences.** Captures from 2026-10 in which hosted copied a source over
a dev input no recorded build or copy produced
(`clone_fallback_stale_source.jsonl` and `clone_fallback_zero_tolerance.jsonl`,
entry 2 each) are keyed replay gaps: the server builds there. A copy over an
existing Target relation follows the same rule, since every copy is now
judged. A Result copy recorded before migration 0029 has no recorded source
build (`clone_source_execution_id`): it is passed over as a copy source and,
as its own Target relation's Reuse candidate, rebuilds once. A copy over a Stand-in is recorded and explained as a judged copy,
with the submit's inputs as read; the replayed tree of
`explain_clone_history.jsonl` entry 14 matches hosted.

Open questions are in
[correctness-handoff.md](../correctness-handoff.md#open-hosted-questions).
