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

On 2026-11-15 the maintainer accepted **documented conservative differences**
for completing implementation tickets: extra BUILDs or speculative undecided
responses may preserve UID, namespace and freshness safety instead of matching
unsafe hosted reuse. A ticket can close when its supported behavior is tested
and these limits are explicit. Missing features and untested behavior remain
open. Exact retained fixtures are not weakened, and this does not create a
full hosted-conformance claim under ADR 0009.

| Area | Tested boundary / remaining work | Evidence | Issues |
| --- | --- | --- | --- |
| Tables and views | First build, unchanged reuse, changed logic and independent typed physical results. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-baseline-2026-11-12.md) | #135 |
| Names and SQL identity | Characterized quoting/context, bounded SQL equivalence; unsupported syntax remains exact. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-sql-equivalence-2026-11-14.md) | #136, #137 |
| Seeds | Ordinary managed seed contents/configuration and downstream invalidation; copying excluded. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-seeds-2026-11-13.md) | #139 |
| Freshness and producers | Bounded input/target changes, producer changes, lag and ANY/ALL controls; unknown evidence rebuilds. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-policy-2026-11-14.md) | #140–#142 |
| Data tests and audits | Complete source-less and upstream pass/fail/recovery cached outcomes, configuration and audit errors; UID isolation retained. [Cached-test evidence](databricks-cached-tests.md). Conservative cross-UID template differences are documented; broader audit coverage remains incomplete. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-cached-test-results-2026-11-14.md) | #138, #143, #144 |
| MERGE | Unique-key upserts, destination predicate effects, schema failure/retry/append-column recovery, reuse and full-refresh Record/type transitions with independent cumulative results. Bounded keys/settings. | [Evidence](databricks-merge.md) | #145 |
| APPEND | Duplicate-preserving cumulative history, additive schema setting, required reuse, full refresh and bounded strategy/table/view transitions with independent results. | [Evidence](databricks-append.md) | #146 |
| insert_overwrite | Single/composite NULL-safe partition replacement, duplicate/empty input retention, schema failure/retry/add-column recovery, reuse and full-refresh Record/type transition. Bounded settings. | [Evidence](databricks-insert-overwrite.md) | #147 |
| replace_where | Predicate/input changes, retained/replaced NULL/duplicate bags, empty-range deletion, schema failure/retry/add-column recovery, native predicate constraint failure/recovery and full-refresh11 transition. | [Evidence](databricks-replace-where.md) | #148 |
| delete+insert | Completed bounded scalar-key replacement/input/full-refresh behavior: 13 hosted and 13 Rust checkpoints, duplicate/NULL/empty bags and exact Explain. Composite keys and broader settings/schema cases are unverified. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-delete-insert-2026-11-15.md) | #149 |
| Microbatch | 10 checkpoints per backend, daily windows and independent references; broader settings deferred. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-microbatch-2026-11-15.md) | #150 |
| Snapshots | Timestamp/ignore:12 managed and12 native reference checkpoints per backend. Older timestamps/nonzero-microsecond SCD hashes and other modes remain unverified. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-snapshot-timestamp-ignore-2026-11-15.md) | #151 |
| State selectors | All ten pinned-client criteria, first/unchanged and changed-component/data baselines, client graph expansion; two-model graph. | [Evidence](databricks-selectors.md) | #156 |
| Deferral and auxiliary RPCs | Actual pinned-client deferral and validation/telemetry. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-deferral-2026-11-13.md) | #155, #157 |
| History, isolation and upgrades | Verified physical failure/retry/restart, request-scope isolation/concurrent projects and genuine SQL/seed/copy upgrades. Targetless logical reuse and physical two-warehouse checks are not claimed. | [Report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-lifecycle-2026-11-14.md) | #158–#160 |
| Generated table/freshness safety | Four-seed modeled CI traces and explicit seed/plan live replay; four actual hosted/Rust observations per backend with typed independent references. Closed grammar only. | [Evidence](databricks-generated.md) | #161 |

## Completed conservative implementation slices

The original technical criteria below have existing public and native evidence.
Under the accepted completion policy, their documented conservative differences
are compatibility limits, not missing implementations. This does not establish
full ADR 0009 hosted conformance or every configuration/graph combination.

| Tickets | Verified behavior | Explicit remaining limit |
| --- | --- | --- |
| #138 | Dependency binding/reordering versus swaps, rendered/template/config/context identity, Record transitions and genuine old-version invalidation. | Fresh UID tests BUILD/return undecided where hosted can reuse another UID. |
| #140 | Native source DML/schema/metadata and recorded producer changes; observed epoch precision/convergence; crafted equal/missing/delayed boundaries. | Unknown/conflicting/omitted inputs BUILD. Real changed-data equal epochs and general delayed visibility were not observed. |
| #141 | Drop/recreation/replacement, external DML/DDL, table/view changes and latest A→B→A physical history with independent results. | Unknown freshness follows #140's conservative rules; no new metadata phenomena are inferred. |
| #142 | ANY/ALL partial/all drift and same/changed producers, native policy-permitted stale/fresh references, deterministic T−1/T/T+1 and zero-tolerance checks. | Exact hosted equality is unobserved; the local tested equality rule and unknown-evidence BUILD remain explicit. |
| #158 | Failed/incomplete execution, delayed/duplicate Confirm, retry, atomic Record and restart through public calls. | Targetless FULL/VIEW logical reuse is unsupported. Selected show probes do not establish universal client unreachability. |
| #159 | Org/family/namespace/target collisions, known aliases, latest history and actual concurrent independent owned projects. | None/empty→named namespace fallback is refused; physical two-warehouse collision behavior is not claimed. |

Detailed dated reports remain in the [evidence archive](databricks-evidence.tsv).
Their strict-era INCOMPLETE headings record the earlier completion policy.
The current implementation status is this matrix; the original diagnostics and
exact fixture assertions remain unchanged.

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
- Broader selector node-kind/graph combinations, all incremental settings/schema transitions,
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
