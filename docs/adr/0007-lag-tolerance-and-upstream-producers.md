# Freshness against recorded upstream epochs and producers, gated by lag tolerance

## Status

Accepted (2026-11-10). Supersedes the earlier own-build freshness anchor (see
[correctness-handoff.md](../correctness-handoff.md)), which was characterized
only with External modifications of dbt-managed upstreams. Amended by
#83/#90 (2026-11-11): which build a submit is judged against when the node's
execution type changed ([below](#execution-type-change)), and by #116
(2026-11-11): only over the submit's genuine upstream names. Amended by
#158 (2026-11-14): physical completion ordering and unknown-epoch safety.

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
