# MERGE settings preparation, 2026-11-14

This is offline preparation for a bounded next slice of #145, not acceptance
or a live-run grant. #145 remains incomplete, including schema evolution,
configured refresh, restart/upgrade, generated lifecycle controls and native
#138/#140 dependencies. Existing hosted/Rust baseline MERGE evidence is in
[the accepted partial report](databricks-merge-2026-11-14.md).

`experiments/databricks_merge_settings.py` owns a separate graph and imports
canonical helpers without editing them. Its complete literal typed bags and
config variants are frozen in `databricks_merge_settings_ledger.json`; its
proposed exact object names are in `databricks_merge_settings_contract.json`.
The operation model uses nullable unique keys; the actual native MERGE must
use `<=>`. Source-missing rows remain in cumulative R. Fresh F is checked
separately from R, with multiplicity and all ordered column types retained.

| Invocations | Config | R rows | F rows | Policy expectation |
|---|---|---:|---:|---|
| first / first_repeat | default | 4 | 4 | BUILD / SKIP |
| null_update / null_repeat | default | 5 | 5 | BUILD / SKIP |
| config_include | update amount only | 5 | 5 | Hosted discovery |
| include_delta / include_repeat | update amount only | 6 | 5 | BUILD / SKIP |
| config_exclude | exclude tag from updates | 6 | 5 | Hosted discovery |
| exclude_delta / exclude_repeat | exclude tag from updates | 7 | 6 | BUILD / SKIP |
| predicate_delta / predicate_repeat | exclude tag; destination ON predicate | 9 | 7 | BUILD / SKIP |

The first update changes the actual NULL key in place. Include/exclude updates
preserve existing tags but populate every field on newly inserted rows. Key 2
is removed from source and retained in R. The final parenthesized predicate
`(DBT_INTERNAL_DEST.id >= 2 OR DBT_INTERNAL_DEST.id IS NULL)` excludes key 1
from matching while admitting NULL. The incoming key-1 row is deliberately
identical to the old target row, producing **two exact duplicate rows** in R;
F has one. Nothing applies a later positive update to duplicate target keys.
Those update semantics remain uncharacterized; no native ambiguity error is
assumed for one source row matching multiple target rows.

Config changes live only in schema YAML. Raw model body and compiled SELECT
must stay identical, source epochs must remain unchanged during config-only
calls, and every target epoch must match the campaign's own latest accepted
Confirm. Actual semantic_extras must carry exactly the declared settings.
The two config-only decisions remain discovery until the complete hosted
capture passes strict public replay and independent physical checks. A Rust
run requires that complete certified capture and a hashed receipt with
`strict_replay_passed`, `physical_campaign_passed`, `complete_cells: 12`, the
exact capture digest and `operation_ledger_sha256`. It derives decisions from
that prior hosted history, never from its own responses.

Each of the 12 managed calls is followed by complete source/R/F typed reads,
MANAGED/Delta checks, full source/target Delta history, and an isolated
State-disabled literal SELECT reference through the canonical reference
validator seam. Reference RPC counts must be zero. SKIP must preserve full
Delta history and report the prior own Confirm's runtime. A failure stops the
whole phase; failed Confirm, Record/copy paths, unexpected SQL or incomplete
results cannot become accepted reusable evidence. There is no version-advance
requirement for a successful no-op write and no invented commit.

The exact proposed fresh pairs are `ms_20261114a145cc_{inputs,consumer}` and
`ms_20261114b145dd_{inputs,consumer}` in existing
`cdo_demo_db.state_20261114_append146` (schema UUID
`6f14be2c-a920-4309-8240-caf8e8df7888`). Each consumer reserves exact
`__dbt_stg` / `__dbt_backup` spellings sharing one extra persistent peak, plus
one exact unqualified `__dbt_tmp` session view. No reference tables, schema
allocation, old graph writes or cleanup are included. Root must separately
verify current protected inventory, ownership and capacity and grant each
backend. These names are a proposal, not an allocation or capacity observation.

The native wrapper records PID, argv, executable path and digest before the
campaign, then retains actual audit clocks using canonical `native_clock`
before the isolated State schema drops. The complete immutable native capture
and that sidecar must subsequently pass public replay. No migration or
production change is proposed before hosted characterization.

Primary macro evidence is the actual pinned dbt 2.0.11 manifest retained at
`~/.local/state/dbt-state-rs/databricks-145-20261114/hosted/targets/012_next_repeat/manifest.json`
(SHA256 `f72109d4df38998ee2cd521e49b26e4ffd8cd384bc25093f3b74678a6804a1f3`).
It establishes predicate placement in ON, nullable `<=>`, update include/exclude
selection and unrestricted inserts. It does not establish hosted config-only
policy or unexecuted serverless capability. Macro hashes and schema-evolution
limits are retained in the coordinator's
`ticket145-remaining-settings-preflight.md`.

Add-column `append_new_columns` and `fail` recovery are a separate next graph:
that work needs an error-aware native execution boundary and a frozen expanded
schema ledger, including untouched-row NULL backfill and failure publication
controls. This slice does not claim those faults or capabilities are covered.
