# Experimental Databricks preview

This is a bounded preview, not full hosted dbt State conformance. It targets
**dbt 2.0.11**, the tested Databricks Free Edition serverless workspace,
Unity Catalog and managed Delta tables, plus the characterized view behavior.
Other versions, warehouses and formats are unverified. Evidence was collected
on 2026-11-12 through 2026-11-15 (DBSQL 2026.44 at the capability survey).

The normal server setup is in [development.md](development.md). Use the
[copy configuration](databricks-copy-preview.md) for this preview: Databricks
copying is off by default; ordinary same-target execution and reuse are the
intended workflow. Treat the copy opt-in as unsupported experimental work.

## Coverage and follow-ups

Each row describes tested cases, not every possible setting of that feature.
Evidence links point to the preserved archive; production tests and captured
histories remain in the main tree. Unfinished issues remain open after landing.

| Area | Tested boundary / remaining work | Evidence | Issues |
| --- | --- | --- | --- |
| Tables and views | First build, unchanged reuse, changed logic and independent typed physical results. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-baseline-2026-11-12.md) | #135 |
| Names and SQL identity | Characterized quoting/context, bounded SQL equivalence; unsupported syntax remains exact. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-sql-equivalence-2026-11-14.md) | #136, #137 |
| Seeds | Ordinary managed seed contents/configuration and downstream invalidation; copying excluded. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-seeds-2026-11-13.md) | #139 |
| Freshness and producers | Bounded input/target changes, producer changes, lag and ANY/ALL controls; unknown evidence rebuilds. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-policy-2026-11-14.md) | #140–#142 |
| Data tests and audits | Bounded complete cached outcomes/configuration/audit errors; UID isolation retained. Broader template/audit parity remains incomplete. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-cached-test-results-2026-11-14.md) | #138, #143, #144 |
| MERGE and append | Characterized baseline histories and independent results; broad settings/schema evolution/transition matrices deferred. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-merge-2026-11-14.md) | #145, #146 |
| insert_overwrite | Bounded hosted/Rust replacement history; broader configuration/error coverage deferred. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-insert-overwrite-2026-11-14.md) | #147 |
| replace_where | 13 hosted and 13 Rust checkpoints, complete results and independent references. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-replace-where-2026-11-14.md) | #148 |
| delete+insert | 13 hosted and 13 Rust checkpoints; duplicate/NULL/empty input, replacement and full refresh cases. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-delete-insert-2026-11-15.md) | #149 |
| Microbatch | 10 checkpoints per backend, daily windows and independent references; broader settings deferred. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-microbatch-2026-11-15.md) | #150 |
| Snapshots | Timestamp/ignore:12 managed and12 native reference checkpoints per backend. Older timestamps/nonzero-microsecond SCD hashes and other modes remain unverified. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-snapshot-timestamp-ignore-2026-11-15.md) | #151 |
| Deferral and auxiliary RPCs | Actual pinned-client deferral and validation/telemetry; full selector breadth deferred. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-deferral-2026-11-13.md) | #155–#157 |
| History, isolation and upgrades | Bounded failure/retry/restart, namespace/isolation and genuine legacy-history cases; full matrices remain open. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-lifecycle-2026-11-14.md) | #158–#160 |

## Deliberate limitations

- No automatic Databricks submit-time Result copy. Direct RegisterClone is
  disabled by default until physical acceptance is completed (#152–#154).
  Narrow opt-in copy/adoption remains outside preview support and has public
  protocol/safety tests, not a complete physical safety claim. Disabling the
  opt-in does not invalidate previously recorded copy history.
- Preserve org/warehouse/namespace/full-UID isolation even where hosted reuses
  across those boundaries. Extra BUILDs or speculative undecided responses are
  accepted preview differences. Targetless qualification remains conservative.
- Unknown/conflicting freshness evidence cannot justify reuse. The crafted
  omitted-source hosted SKIP that retained stale rows is not reproduced.
- FULL/seed replacement over existing shallow-clone targets fails in the
  tested client/warehouse even with State disabled. Such targets and workflows
  are outside the preview; do not use copy opt-in to bypass this limitation.
- Snapshot `new_record` deletes produced NULL business values in State-disabled
  controls. This mode is outside the preview; ordinary dbt execution cannot
  make that client/warehouse behavior safe.
- Complete selector coverage, all incremental settings/schema transitions,
  all snapshot modes, broad generated/live testing and full upgrade parity are
  follow-ups, not preview release claims. Precise descriptions/Explain/error
  parity outside retained regression cases is also unfinished.

A known wrong result caused by this server remains a defect. Accepted preview
limits do not waive isolation, latest-physical-history selection or the
upstream producer/epoch rules in [correctness-handoff.md](correctness-handoff.md).

## Evidence and maintenance

The archive branch is
[archive/databricks-132-evidence](https://github.com/maxfirman/dbt-state-rs/tree/archive/databricks-132-evidence),
pinned at `d4ac553cdd09becb77c7555b143e5f4bedaeb2fb`.
[databricks-evidence.tsv](databricks-evidence.tsv) lists original paths and
SHA-256 hashes for archived reports and unexecuted MERGE settings/schema
scaffolds. These outputs are not runtime dependencies. Accepted campaign
drivers and their tests remain under `experiments/`; golden and database
upgrade fixtures remain under `golden/`. External raw execution artifacts stay
at their original retained locations; repository reports are not substitutes
for those artifacts.

The pre-consolidation production baseline `45e94c3` passed709 workspace tests,
including10 direct-copy tests, and CI build/tests plus strict formatting/Clippy.
The existing131-corpus whole replay passed separately before final direct-path
guard strengthening. These counts describe that source, not future edits.
Final landing checks are recorded on the PR. Local/integration-PR real-dbt
DuckDB acceptance is deferred until after landing by maintainer request.

The [landing plan](databricks-landing.md) governs this preview. #132 tracks
landing; #162 and the unfinished children track the future full verified-support
claim. ADR0009 remains the standard for that larger claim.
