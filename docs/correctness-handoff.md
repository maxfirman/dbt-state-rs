# Open questions and known divergences

This page lists what is not settled: hosted behavior we have not characterized,
differences from hosted that the tests pin, and gaps that block a production
deployment. Read it before changing decision logic. The wire contract and the
server's current behavior are in [protocol.md](protocol.md); validation commands
and the evidence rules are in [testing.md](testing.md).

Two goals are kept apart (see [GLOSSARY.md](../GLOSSARY.md)):

- **Hosted conformance**: agreement with the official dbt State service's
  observable behavior for a given client, warehouse and feature scope.
- **Reuse safety**: reusing a result satisfies the workload's declared logic,
  freshness and isolation requirements.

When the two conflict, or hosted behavior is unknown, the server chooses reuse
safety: it builds. Over-execution is a conformance gap; serving a wrong result
is a defect.

## Databricks implementation completion

The maintainer accepted documented conservative differences on 2026-11-15.
The tested original slices #138/#140/#141/#142/#158/#159 therefore complete their
implementation criteria; the [current scope matrix](databricks-preview.md#completed-conservative-implementation-slices)
records the evidence and limits. Missing features and untested behavior remain
work. Known hosted differences below are still real; no diagnostic or exact
fixture was changed, and no full hosted-conformance claim is made. Dated package
notes use their original evidence bounds, with current completion status above.

## Reuse invariants

Do not weaken these without new evidence.

- **Current state before identity.** Within the physical scope (organization,
  warehouse family, `table_namespace`, target relation), the server selects the
  latest confirmed build first and only then compares fingerprints, execution
  type and dialect. An older matching build never stands in for a relation
  overwritten since. When the latest build has the submit's identity under
  another execution type, and recorded the submit's genuine upstream names,
  the rebuild is judged against the latest build of
  the submit's type, which is never reused
  ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change)). See
  [architecture.md](architecture.md#physical-scope-rule).
- **Data-test history key (#200).** A data test has no Target relation. On
  Snowflake its State history is its key: organization, warehouse family,
  `table_namespace` and the versioned key over its normalized SQL (each
  input read through an unchanged copy this server recorded as that copy's
  source build), every `semantic_extras` entry and its query dependencies,
  plus its default catalog and schema unless the SQL is context free. Only
  the latest confirmed run under the key is reusable, failures included; an
  older run under another key stays reusable, because no later run
  overwrites what it read. A copy stands for its source only while unchanged
  since the copy (reported epoch no later than its confirmation), only for
  an ordinary source build, and proves freshness only at exactly the
  source's recorded epoch. Rows without a key, and copies without a recorded
  source build, cold-miss once. Under `compare_unrendered_code` a key without
  a run falls back, as before #200, to the latest keyed run of the unique id
  in its project and physical scope with the submit's template identity
  (reuse invariant 5's dual identity, retained; uncaptured under the key).
  Databricks, DuckDB and other Warehouses keep one latest run per unique id
  ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#data-test-history-key)).
  **Accepted risk:** an External modification of a source between its build
  and a submit-time Result copy is not visible, so a data test over the
  copy reuses the source's run over the contents before the change. Neither
  the submit nor the copy's confirmation reports the source's epoch at copy
  time (the confirmation reports the clone time). This is the blind spot of
  a FULL copy and a seed copy below, inherited. A RegisterClone records the
  source build only when its `clone_source_last_modified_epoch` is the
  build's.
- **Physical completion order defines current state.** The greatest completion
  epoch wins; confirmation receipt then row id break equal-epoch ties
  (`databricks_lifecycle_ordering.jsonl`; the equal-epoch cases in
  `crates/harness/tests/concurrency.rs`). A legacy confirmed unknown epoch is a
  conservative physical cold miss, including for copies and with external
  modification tolerance; delivery alone cannot prove safe recovery
  ([#158 evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-lifecycle-2026-11-14.md)).
- **Target relation as built.** Reuse needs exactly one known epoch for the
  target relation with `current_epoch <= confirmed_epoch` (a later epoch is
  accepted only with `ignore_external_modifications`). Missing, duplicated
  or unknown metadata builds. This assumes the confirmation epoch is an upper
  bound for the build's warehouse write time; timestamps alone cannot prove
  object lineage.
- **Unknown evidence builds.** Unknown or conflicting upstream epochs, recorded
  duplicate inputs, a changed set of physical input names, empty or duplicated dependency definitions and
  corrupt stored evidence (`INTERNAL`) never become a reuse. A node's own
  relation is not an input: a model's own target, and a `store_failures`
  data test's audit relation when the request identifies it exactly (#88)
  or, under a configured alias, as the only unnamed table in the test's
  schema (#123, [ADR 0008](adr/0008-store-failures-audit-relation-by-elimination.md)).
- **Bounded current duplicate normalization (#140).** Databricks rendered FULL
  submits without query dependencies may collapse only exactly identical raw
  upstream entries with known epochs and a supported physical Target relation.
  Other aliases of that physical name, conflicting/unknown entries and duplicate
  Target evidence remain ambiguous. Pending builds retain their uncollapsed
  inputs; stored duplicates are never retroactively normalized. Explain submit
  clocks are separate nullable display data and never freshness evidence.
- **Versioned fingerprints.** Each fingerprint starts with a version tag
  (`crates/server/src/warehouse.rs`, `analyzed_workload/identity.rs`). A change to identity
  semantics takes a new tag, so older State history cold-misses and rebuilds
  once. Never rehash old rows in place
  ([ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md)).
- **Freshness.** Drift is measured against the epoch recorded for each upstream,
  an Upstream producer change always rebuilds, and Lag tolerance compares Build
  age only ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md)).
- **No cross-target SKIP.** A `table_namespace` match alone never justifies a
  SKIP. Cross-target reuse is a Result copy on Snowflake (and the DuckDB copy
  subset) only, and a submit-time Result copy is FULL only (#101), except a
  seed on Snowflake (#91, below).
- **Seed copy on submit (#91).** A Snowflake seed without a reuse candidate
  is copied from the latest load of another Target relation in its scope
  (organization, warehouse family, `table_namespace`) that has its
  `values_hash` and seed logic hash (dialect, catalog and every seed
  `semantic_extras` entry) and differs from it in the schema only
  (`result_copy::seed_lookup`, `Store::find_seed_copy_candidates`). It is
  copied into an absent table, or over its table when that table's latest
  load has other values; never over a table modified after a load with its
  values (an accepted divergence, #129, below). A submit with
  `allow_clones: false` is loaded with the FULL `copy_disabled` codes
  (1 / 10) and its ordinary description, and Explain names the load it
  could have copied (`explain_seed_copy_disabled.jsonl`); one with
  `clone_chain_depth_limit: 0` alone is not copied either (uncaptured,
  below). **Blind spot:** at submit time an
  External modification of the prod source after its load cannot be seen.
  The submit reports only its own target's epoch, and a seed has no upstream
  listing, so the copy serves the source as modified. A FULL copy from
  another environment has the same blind spot: it is copied unjudged.
- **Copy adoption needs the copy's logic.** A later submit reuses a
  RegisterClone copy only when its schema-abstracted logic equals the logic
  the copy inherited from its source build, directly or with the submit's
  Target relation schema read as the copy source's schema (Snowflake only,
  `analyzed_workload::identity::rehomed_copy_logic_hash`, #93). Either way each dependency's
  name stays bound to its definition
  (`analyzed_workload/identity/tests.rs::rehomed_identity_keeps_dependency_bindings`). No
  persisted fingerprint changed. A submit of another execution type with that
  logic is never reused: it is an execution type change (2 / 2) judged
  against the copy, as hosted does (`explain_copy_logic_mismatch.jsonl`
  `kf4`, #104).
- **Submit-time equivalents read in another schema.** The Result copy
  fallback finds an equivalent by the submit's schema-abstracted logic, or by
  that logic with the submit's Target relation schema read as another schema
  the request names (the #93 rule, `analyzed_workload::identity::rehomed_copy_logic_hashes`,
  Snowflake only, #107). A candidate found the second way must have its
  Target relation in that schema: one elsewhere read other relations. That
  schema does not show where the build resolved its references (a build into
  the prod schema may have used another default schema), so a candidate that
  may be copied must also have recorded exactly the upstream reads of the
  submit so read, and those reads must name each schema the reading names;
  otherwise it is not an equivalent (PR #127 review,
  `clone_submit_fallback.rs::a_build_into_the_prod_schema_that_read_another_schema_is_not_copied`).
  Kinds that only report the equivalent in their codes (views record no
  reads) admit on the Target relation's schema. Upstream reads compare by
  canonical name both here and when the fallback decides whether an
  equivalent read the submit's upstreams, so a shared relation spelled
  differently is judged for drift, never copied unjudged
  (`a_differently_spelled_shared_source_is_judged_before_a_copy`). Each
  dependency's name stays bound to its definition, so a dev dependency whose
  definition differs from the prod build's is not an equivalent
  (`clone_submit_fallback.rs::a_submit_reading_a_fixed_prod_schema_copies_only_a_same_logic_prod_build`).
  Copying stays FULL only; other kinds only report the equivalent in their
  codes. A candidate in a schema the request does not name is found only by
  the plain logic, which is conservative (not captured).

## Open hosted questions

Each item needs a controlled capture before the server changes. Until then the
server keeps the conservative behavior described.

- Which source does a Databricks FULL submit copy when two eligible builds
  have the same physical completion epoch? With distinct epochs, hosted
  selected the later one in all three two-source ordering controls.
  [Captured decisions and confirmation order](reports/databricks-copy-candidates-2026-11-21.md)

- **Speculative decisions beyond a data test without upstreams (#103).**
  Hosted SKIPs an unchanged data test with no upstreams on the
  `SubmitEnrichedSQLSpeculative` call (`explain_rebuild_reason_gaps.jsonl`
  entry 39) and answers `undecided` to the other 313 speculative calls in
  `golden/fixtures/`. Whether hosted ever SKIPs, copies or answers
  `ready_to_execute_untracked` for another workload (for example a data test
  whose upstream epochs were all prefetched) is not captured. The server
  decides only the captured case, through the ordinary submit's code, and
  answers `undecided` otherwise, after which the client sends the ordinary
  submit (`crates/harness/tests/speculative_submit.rs`).
- **Data-test history key, remaining cells (#200).** Implemented as decided
  (option A); these cells stay open and the server keeps the conservative
  choice named:
  - whether hosted's key holds the upstream epochs (a content address) or
    picks the latest run and then judges freshness. They differ only under
    Lag tolerance, which clients send as 0 for data tests; the server picks
    the latest run first;
  - whether copy provenance or the same producer at the same build makes a
    dev relation stand for the prod one. The server uses only copies it
    recorded (Result copy on submit, RegisterClone) of an ordinary build;
    a copy of a copy, a copy recorded before #200 and a relation built
    rather than copied stand for themselves;
  - whether `fail_calc`, `warn_if`, `error_if`, `store_failures`, `limit`,
    `where` and query dependencies are in hosted's key. The server keeps
    them all in the key;
  - whether hosted ignores `default_schema` for SQL that names a relation
    without its schema, reads a CTE, or calls a function or sequence. The
    server keeps the default catalog and schema in the key there, so a dev
    test of a generic `unique` or `accepted_values` test (which calls
    `count`) does not reuse the prod run through a copy;
  - the 2.0.6 shape with its dev runs captured: a dev upstream built, not
    copied, with a newer prod run (`golden_20261009T083754.534Z.jsonl`
    entries 13–16 stay Missing history);
  - more than one organization (one was captured), and Databricks and DuckDB,
    which keep one latest run per unique id;
  - whether hosted reuses a data test under `compare_unrendered_code` whose
    rendered SQL changed (another key) and whose template did not. The
    server keeps the pre-#200 dual identity for it: the latest keyed run of
    the same unique id in its project and physical scope, with the same
    template identity, is reused.
- **Resolution context of dependencies.** Whether hosted keys dependency
  equivalence on anything beyond the definition body and freshness. A
  context-only change could not be isolated from upstream freshness through the
  client (`experiments/resolution_context.sh`). The schema-abstracted identity
  is unchanged (`analyzed_workload/identity/tests.rs::ns_uniform_schema_rename_is_cross_env_positive`).
- **Volatile evaluation: uncaptured cells.** The supported functions,
  cells and Warehouses, with their client 2.0.11 evidence, are in
  [protocol.md](protocol.md#volatile-evaluation) (#67, #201). Values are
  returned only in the captured cells (`Rules::cells`). Each cell below
  keeps the conservative behavior unless stated: an empty map, no volatile
  Explain lines and, with evaluation on, the rebuild of volatile SQL. The
  Warehouse executes the original SQL in every case. #202 captured the
  Snowflake cells #191 left open (`golden/fixtures/explain_volatile_cells.jsonl`,
  [characterization note](reports/snowflake-volatile-cells-2026-11-19.md)),
  and #203–#207 implemented them; every entry of that capture replays,
  entry 146 since #200.
  - Maintainer decisions on the #202 check-in (#201, 2026-11-19).
    Decision 1: hosted's SKIP of volatile SQL with evaluation on under
    `compare_unrendered_code` (entry 140) is matched for a Snowflake model
    with an unchanged template (#206;
    [ADR 0005](adr/0005-re-derive-explain-tree-on-demand.md) "Amended by
    #201"). Decision 2: the data-test SKIP that echoes another data test's
    runtime (entry 146, `cto`) was not matched pending #200; since #200
    (option A) the server echoes `rt`'s later-confirmed run of the same key,
    as hosted does, and the entry replays. Decision 3: a model's rebuild refused at Reuse
    qualification records the refused build as its Explain anchor (ADR 0005
    "Amended by #201").
  - Databricks (#198): #198 captured a FULL table's first build and
    unchanged rerun, a data test without a Target relation's first execution
    and rerun, their exact Explain, and a downstream table's reuse within Lag
    tolerance (`databricks_volatile_evaluation.jsonl`,
    [characterization note](databricks-volatile.md)); with the query-changed
    FULL rebuild (controls entries 85, 86) these are the cells that render.
    The reruns render only as captured, unchanged: the refused build's
    Target relation as built, a data test's cached results complete, and
    every upstream unchanged since it (`Selection::unchanged_rerun`).
    Uncaptured (#213): other execution types and rebuild reasons, refusals for
    another cause (among them under `compare_unrendered_code`), the same
    volatile refusal after the table was dropped or modified externally or
    over changed inputs, a data test's logic change, a data test with a Target relation, volatile query
    dependencies, evaluation off, and functions other than unseeded
    `rand()`. Databricks never reuses with evaluation
    requested, because it has no checked Volatile function detector.
  - Snowflake, not reachable through client 2.0.11: a data test with a
    Target relation. The client sends `target_table: null` even for a
    `store_failures` test (cells entries 61, 145), so this server renders
    nothing for one.
  - Snowflake, reachable through client 2.0.11 but not captured (#211):
    the map stays empty for
    - the call forms `current_timestamp(n)` with another precision than 3,
      `current_time(n)`, and `uniform`, `randstr` or `normal` with other
      arguments. Whether a constant-generator call alone, such as
      `uniform(1, 100, 1234)`, blocks hosted's reuse is not isolated (entries
      36 and 131 also call `random()`); the detector rebuilds it;
    - a data test with values in both its own SQL and a query dependency;
    - evaluation off for a data test over a volatile view, a table with
      values in both its SQL and a query dependency, and APPEND, snapshot
      and custom nodes;
    - APPEND, snapshot and custom rebuilds other than a first build and a
      rerun refused at Reuse qualification, their values in a query
      dependency, and the MERGE, INSERT_OVERWRITE, DELETE_INSERT and
      MICROBATCH strategies;
    - any build for another decision reason: upstream data or Upstream
      producer changes, an External modification with evaluation off, an
      execution type change;
    - a Result copy over an existing Target relation or with values in a
      query dependency, and the chain-limit build;
    - a reuse under `compare_unrendered_code` (decision 1) with values only
      in a query dependency, or of another kind than FULL; a data test under
      `compare_unrendered_code`, which keeps the refusal;
    - under `compare_unrendered_code`, a table whose template changed while
      its rendered SQL did not: refused at Reuse qualification for its
      volatile SQL, explained with no tree. Only the unchanged template was
      captured (entry 140);
    - a rerun refused at Reuse qualification for another cause besides its
      volatile SQL (a query dependency without a definition, a missing
      template body): only the refusal of volatile SQL alone was captured
      (`reuse_candidate::refuses_volatile_sql_alone`);
    - a speculative submit, which this server never renders for (the
      #202 capture's 36 speculative submits were all `undecided`).
  - Snowflake Explain, reachable but not captured (#211):
    the decision's own tree without volatile lines for an evaluation-off
    rebuild other than a table's query change without upstreams; the model
    rerun tree or the volatile-dependency tree over an upstream table that
    drifted or changed producer since the refused build, or over both an
    upstream table and a query dependency; the model rerun tree of a custom
    node with upstreams, of an APPEND incremental or snapshot without
    upstreams, of any non-FULL kind with query dependencies, and of a view
    over a `ref`; the volatile-dependency tree of another node kind; the
    evaluation-off SKIP over a volatile query dependency with an upstream
    table, within Lag tolerance or of another kind; a data test's
    evaluation-off query analysis on anything but a first execution and a
    reuse. A data test whose only input is a deterministic query dependency
    keeps `no upstream dependencies`: no capture shows whether hosted's
    bare `data freshness analysis` line (`ctv`, entries 105, 201) is
    specific to volatile dependencies. A copy that reads nothing says
    `no upstream dependencies` whether or not its SQL is volatile; only the
    volatile copy (entry 209) was captured.
  - Relative-time wording (#210): for `lz`'s rerun (entry 200) hosted words
    a 94.992 s gap "updated 2 minutes after …"; this server rounds down to
    "a minute" outside Databricks. The replay and the `lz` scenario test mask
    the line (`«gap»`), so it is no replay gap.
  - Hosted reads the clock for each function; this server reads it once per
    submit. The replay compares clock values by format only.
  - `query_hash_metadata_info` stays unset; hosted's use of it is
    uncharacterized.
- **RegisterClone source-age chain, entry 4.** Hosted SKIPs a post-copy submit
  that differs from the chain's first submit in schema, an added incremental
  predicate and epochs at once; the local server does not. The cause is not
  isolated
  (`clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay`,
  fixture `clone_chain_source_age.jsonl`). It is not the #93 cause: its
  schema difference is one the copy adoption already matched before #93
  (the upstream resolved per target). What remains is the added
  `is_incremental()` self-reference predicate, the Target relation's epoch
  being newer than the copy's confirmation, and a runtime (4074 ms) that is
  not the source build's (2600 ms).
- **Explain verdicts over a copy (#93).** In the post-copy SKIP tree hosted
  gives a FRESH verdict only to an upstream the copy's source build recorded,
  and lists a per-target upstream with its `last updated` line only
  (`explain_post_copy_identity.jsonl` `c93st`, `c93it`). The server keeps the
  source build's upstream names on the copy and renders the same. Not
  isolated: in that capture the per-target upstream was itself a submit-time
  copy, so "the upstream is a copy" fits the capture too.
- **An older build of the submit's type that would qualify (#83, #90).**
  Hosted judges a submit whose latest build has its identity under another
  execution type against the latest build of its own type, RecordExecutions
  rows included (resolved, [ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change),
  `explain_execution_type_change.jsonl`). Every capture had that build's
  Target relation modified since. Whether hosted would reuse it when its
  Target relation is unchanged since is not characterized; the server
  rebuilds, as the execution type change against the latest build (2 / 2,
  `offline_regressions.rs::execution_type_change_is_judged_against_the_latest_build_of_that_type`).
- **Same SQL under another execution type, other upstream names (#116,
  resolved).** Hosted treats a view ↔ custom node switch over an upstream
  (the view reports none, the custom node does) as another identity: 6 / 6,
  and 8 / 7 over an older build of the submit's type
  (`explain_reason_kind_cells.jsonl` `vc`, `cv`); with no upstream on either
  side it is an execution type change (`vc0`, `cv0`, 2 / 2). The server
  judges an execution type change only over a latest build that recorded
  the submit's genuine upstream names, otherwise it records
  `no_reuse_candidate.upstream_names_changed` and renders hosted's trees
  ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change)).
  Both answers rebuild, so reuse is unchanged. Not characterized, and
  unreachable through client 2.0.11: whether hosted also compares upstream
  names between builds of the same execution type (the server's
  `dependency_set_changed`). FULL, the incrementals and snapshots report the
  same upstreams for the same SQL.
- **Incremental CLONE (#97, resolved by #73).** Hosted 2.0.11 has no
  explainable incremental CLONE decision: its `RegisterClone` copy has an
  empty `execution_decision_id`, and it rebuilds incrementals on submit. This
  server now copies only FULL on submit (#101). Still open (#101 question 3):
  whether hosted 2.0.11 copies an incremental over an existing relation that
  has no State history of its own, the situation of the 2.0.6 copies
  (`clone_failed_fallback.jsonl`, `clone_submit_equivalent_fallback.jsonl`,
  whose target histories are not captured; keyed divergences below).
- **Non-FULL submits with an equivalent, uncaptured cases (#86, #92, #101).**
  The server reports 1 / 3, 1 / 6 or 1 / 2 only where the captures show them
  ([protocol.md](protocol.md#result-copy-on-submit)). Not captured: a view,
  snapshot or custom node whose newest equivalent has another execution type
  (the server sends 1 / 2); a non-FULL submit whose own reuse candidate must
  rebuild for an Upstream producer change (the server keeps the ordinary
  codes; for upstream data drift hosted reports the equivalent, 1 / 3 or
  1 / 6, captured since in `explain_freshness_cells.jsonl` `fd_sn` and
  `explain_rebuild_tolerance_line.jsonl`, and so does the server); a non-FULL
  submit that forbids copies (the server sends its non-FULL codes, not
  1 / 10; a FULL one is `copy_disabled`, 1 / 10, "cloning is disabled",
  captured, `explain_copy_logic_mismatch.jsonl` `cf4`); and a view or
  snapshot rebuild over an existing Target relation that has an equivalent
  elsewhere (#92 question 2). Captured since (#104,
  `explain_freshness_cells.jsonl` `fr_*`): when the target's own older build
  matches and would be reused, hosted reports that older match (8 / 7,
  `is_stale` as it would be reused) and not the equivalent elsewhere, for
  the five incrementals and a snapshot. The server does the same
  (`submit_decision::older_match` goes ahead of a refused copy).
- **External modification of the target relation in older captures.** In
  `lag_multi_upstream_policy.jsonl` (client 2.0.6) hosted skipped a target
  relation modified after its build; current hosted rebuilds it
  (`lag_tolerance_matrix.jsonl` `ot*_u`). The server follows current hosted, so
  that fixture stays characterization-only (`lag_policy.rs`).
- **First-build rejection codes with prior hosted history.** Hosted reports
  4 / 7 from builds in sessions the capture lacks. The rule is implemented; the
  replay lacks the history. Keyed gaps: `lag_external_modification_linked.jsonl`
  entries 3–4.
- **Stale reuse versus a fresh alternative, and copies with Upstream
  producers.** Uncharacterized: whether hosted prefers copying a fresh
  equivalent over a stale same-target reuse within tolerance; copying an
  equivalent from another target after a same-upstream one was rejected on
  freshness (the server rebuilds); and the full clone chain with the upstream's
  own copies. `clone_chain_upstream_history.jsonl` is characterization-only
  because every copy source predates the capture
  (`lag_tolerance.rs::clone_chain_external_modification_of_consumed_upstream_rebuilds`).
- **Seed copy, uncaptured cases (#91).** A seed whose target forbids copies
  with `clone_chain_depth_limit: 0` alone (client 2.0.11 sends it only with
  `allow_clones: false`, which is captured: 1 / 10, the ordinary load
  description, and a tree naming the equivalent,
  `explain_seed_copy_disabled.jsonl`): the server sends the ordinary load
  (6 / 6) and records `copy_chain_limit`, which renders no tree. A
  seed whose existing table has no load in scope, or whose latest row is not
  a seed load: the server reloads it. A source in another database or under
  another object name (an alias): never copied. A source modified after its
  load (above).
- **Copy limits.** `clone_time_travel_limit` and `OUTSIDE_TIME_TRAVEL_WINDOW`
  (code 9) are never applied. Outside DuckDB, a `clone_chain_depth_limit`
  above 0 is ignored.
- **Selector snapshot timing.** Selector snapshots still publish at submit
  time. Deferral now independently reads confirmed execution history: fresh
  Snowflake/Databricks protocol captures and actual Databricks failed attempts
  prove that unconfirmed attempts are not deferrable (2026-11-13; #155,
  [report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-deferral-2026-11-13.md)). This supersedes the
  earlier shared selector/deferral timing assumption. Hosted selector failure
  timing remains unobserved.
- **Wildcard views.** Column evidence for `SELECT *` over views with added or
  removed columns, nested views and transitive definition changes.
- **`lenient_dependencies`.** The server ignores the field. Define it from
  client source or docs, then capture.
- **Template comparison matrix.** The four cells with the flag both ways,
  transitions of `compare_unrendered_code`, macro/var changes, and how
  `RecordExecutions` (no flag) should seed the template identity.
- **Copy with no recorded inputs.** A submit-path copy that recorded an empty
  input list rebuilds when a later submit has upstreams; hosted is unobserved
  (`reuse_candidate.rs::sql_submit_copy_with_no_recorded_upstreams_rebuilds_when_upstreams_appear`).
- **Data tests and Lag tolerance.** Clients send tolerance 0 for data tests, so
  data tests as Lag tolerance subjects are uncharacterized.
- **Data-test EXECUTE trees not captured.** Hosted's data-test EXECUTE tree
  (#75) is the same for a first execution, a logic change, upstream data
  drift and an upstream whose logic changed, for passing and failing tests,
  over one or two upstreams; a test without upstreams says `no upstream
  dependencies` (`explain_rebuild_reason_gaps.jsonl`, #40 code review). A
  test whose own SQL rendered Volatile function values with evaluation
  requested is captured on its first execution and rerun
  (`explain_volatile_evaluation.jsonl` entries 24, 46, #195) and after its
  logic changed (`explain_volatile_cells.jsonl` entry 204, #206). A test that
  fails reuse qualification without rendered values, for example over
  `seq4()` with evaluation requested, was not captured; the server renders
  nothing for it ([explain-catalog.md](explain-catalog.md#data-test)).
- **`store_failures` audit relation recognition (#88, #106, #123).** Hosted's own
  rule is not observable: every capture is also explained by "compare only
  the inputs recorded at the build". The server recognizes the audit relation
  from the request alone and drops it from the test's inputs
  (`execution.rs::is_own_audit_relation`): a Snowflake data test whose
  semantic extras set `store_failures: true`, and a relation exactly
  `default_catalog`.`default_schema`.`dbt_node_name`. Every other unrecorded
  table, except the aliased case below, still changes the dependency set and
  rebuilds. The request does not
  carry the test's alias, so a relation the test reads can have exactly that
  name (test `orders` aliased `orders_audit`, reading `orders`; Codex review
  on #109): a name that the test's SQL or one of its query dependencies
  mentions as a word is never the audit relation and stays an input.
  `default_schema` is the test's own schema, also in a separate audit schema
  (VERIFIED LIVE 2026-11-11, #106: a schema macro putting tests in
  `<target>_dbt_test__audit`, `explain_store_failures_audit_schema.jsonl`).
  A configured `alias` is carried by no request field
  (`explain_store_failures_audit_alias.jsonl`), so that audit relation is
  recognized by elimination instead (#123, maintainer decision 2026-11-11,
  [ADR 0008](adr/0008-store-failures-audit-relation-by-elimination.md),
  `execution.rs::unnamed_audit_relation`): the only reported table in
  `default_catalog`.`default_schema` that neither the test's SQL nor any query
  dependency mentions as a word. Two or more such tables (counting one the
  exact-name rule drops) are ambiguous and all stay inputs. This is an
  inference, not hosted's observed rule. It rests on the client: 2.0.11
  reports a table only when the SQL or a view definition it sends as a query
  dependency names it, or when it is the node's own relation (dbt-fusion
  `collect_table_modified_infos`), and a relation the query never reads
  cannot change the test's result. Its limits: a read that names no relation
  (a function body, a view definition the request omits) would be missed,
  which 2.0.11 never reports; before the audit relation first exists a lone
  unnamed table is dropped, and the next run, reporting it next to the audit
  relation, rebuilds. Both rules apply where the evidence is built, so the
  decision, the stored rows and Explain agree (the Explain capture's fallback
  for decisions that record no inputs filters the same way). Not captured: a
  long generic test name, whose alias is dbt's truncated name (the same
  mechanism, so the same rule). Other warehouses recognize nothing.
- **Unit tests.** Hosted reuse of unit tests is not characterized; Explain shows
  "details unavailable" ([hosted-behavior.md](hosted-behavior.md)).
- **Explain evidence gaps (#78).** Resolved by #76: VIEW × CLONE is fenced as
  unreachable on 2.0.11 (hosted answers a non-FULL submit copy candidate with
  1 / 3, `explain_copy_probes.jsonl` entry 13). The SHAPE 7 query-changed
  tree is replayed from `explain_snapshot_history.jsonl` (FULL, VIEW and
  snapshot, client 2.0.11).
- **SHAPE 7 without a matched build (resolved).** Hosted judges an
  identity-mismatch rebuild's freshness listing against the latest build in
  scope, whose identity did not match: `<FRESH>` and `upstream data is up to
  date` when the upstream epoch is unchanged
  (`explain_incremental_append_merge_history.jsonl`,
  `explain_incremental_microbatch.jsonl` entry 40, #89), `<FRESH>` and no
  summary when it moved back (`explain_snapshot_history.jsonl`), `<OUTDATED>`
  with the `updated N seconds after …` line and `upstream data is outdated`
  when it drifted (`explain_rebuild_reason_gaps.jsonl` entry 33). The
  decision persists that build's id (`node_decisions.unmatched_execution_id`,
  migration 0017) and Explain re-derives the listing from it. A rebuild
  recorded before the migration has no such build: its listing keeps the
  default `<FRESH>` badges and omits the summary, since unknown evidence
  never proves freshness
  ([explain-catalog.md](explain-catalog.md#execute-node-query-changed-shape-7)).
- **Reason × node kind cells without a capture (#104, settled).** Every
  Decision reason × node kind that renders a tree is backed by a tracked
  capture; the cells without one render nothing, and each has a status in
  [explain-catalog.md](explain-catalog.md#decision-reason-coverage) (no cell
  is left → #104). The freshness cells are captured
  (`explain_freshness_cells.jsonl`): the
  ALL-policy reuse with partial drift for the incrementals and DBT_CUSTOM,
  drift past the Lag tolerance for insert_overwrite, delete+insert,
  microbatch and a snapshot, and the stale SHAPE 7 rebuild with its Lag
  tolerance line, whose age is the older matching build's (the decision now
  records it). The last cell, `copy_logic_mismatch` over a dropped
  RegisterClone copy, is captured for every model kind (crafted
  `explain_copy_logic_crafted.jsonl` `x11`–`x19`): hosted rebuilds with
  6 / 6 and the first-build tree, as this server does.
- **A view that reads no relation (#117), resolved.** Client 2.0.11 reports
  no upstreams for any view, yet hosted's VIEW tree says `no upstream
  dependencies` (and omits `upstream model queries have not changed`) when
  the view's SQL reads no relation, and `upstream data is up to date` when it
  reads one (`explain_view_reads_no_relation.jsonl`: no FROM, a CTE without
  FROM, VALUES and FLATTEN read none, a `ref` reads one; SKIP and SHAPE 7;
  `explain_reason_kind_cells.jsonl` `vc0`, `cv0`). The decision now records
  whether a view's SQL reads a relation (`node_decisions.reads_relation`,
  migration 0019, ADR 0005 amendment), and Explain words the tree from it.
  Still open: views whose SQL hosted parses differently from this server.
  A CTE hides a table of the same name only within its own query (#125
  review); a view whose SQL does not parse as one query, or whose CTE names
  itself or a later sibling without RECURSIVE, renders nothing.
- **Snapshot SKIP wording past the Lag tolerance.** Hosted words every
  observed snapshot SKIP "both … up to date", including drift within the
  tolerance (`explain_snapshot_history.jsonl` entry 40). The `ALL`-policy
  reuse past the tolerance is unreachable for snapshots on 2.0.11: the
  client reads `state` config (`require_fresh_data_from`, `lag_tolerance`)
  from models only (dbt-fusion `run_cache_service.rs::model_state_for_node`),
  so a snapshot always gets the ANY policy. The server keeps the
  same string ([protocol.md](protocol.md#decision-descriptions)), a local
  choice pinned by
  `decision.rs::unobserved_all_policy_snapshot_skip_past_tolerance_keeps_the_observed_wording`.
- **Non-Snowflake copy DDL.** BigQuery and Databricks templates follow published
  syntax but were never run against hosted or the warehouses. View copies need
  the view definition, not `CREATE VIEW ... CLONE`.
- **Client and warehouse compatibility.** Wire schemas match the reviewed Rust
  client; older Python client schemas and identity representations differ.
  Supported client versions and warehouses need a pinned matrix.
- **Proxy fidelity.** The recording proxy forwards client metadata but does not
  record raw protobuf bytes. It now retains error/status metadata and checks
  capture completeness (`crates/harness/src/bin/record_proxy.rs`).

## Known divergences

The #155 crafted protocol diagnostic using Databricks backticks as Snowflake
Target relation metadata is unresolved: hosted accepts it and resolves each
component as a doublequoted identifier containing literal backticks; this
server returns the submitted spelling. The dated
[deferral report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-deferral-2026-11-13.md) retains the exact
failed replay and original capture. The scoped fixtures use each warehouse's
own quoting; this diagnostic has no replay exemption and establishes no
cross-dialect spelling compatibility.

Databricks #140 missing-evidence diagnostics are an **accepted conservative
implementation difference**, not a matching hosted replay ([dated evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-freshness-2026-11-12.md)).
Hosted SKIPs a crafted request that omits a source still read by its SQL;
independent warehouse reads prove the cached target is stale. Missing recorded
epochs also SKIP hosted. The later follow-up matches missing-current and
both-missing epoch Explain while retaining conservative freshness checks. The
bounded normalization of identical known current duplicates is integrated for
Databricks rendered FULL submits
without query dependencies and with a supported physical Target relation
([follow-up report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-freshness-followup-2026-11-14.md)).
Conflicting or unknown epochs, mixed physical aliases, duplicate Target relation
evidence and recorded/legacy duplicates remain outside that normalization;
ambiguous freshness evidence remains unresolved. Snowflake audit rules are
unchanged. These captures stay in the durable archive with exact red replay,
without a Databricks gap or golden exclusion. The tested conservative behavior
completes #140 under the accepted policy; it does not prove hosted parity.

The #158 crafted untargeted FULL/VIEW diagnostic remains **unsupported**. Fresh
Snowflake and Databricks FULL controls repeatedly reuse the logical outcome
after a null-epoch confirmation, independently of subsequent physical target
history. Both FULL and VIEW accept `target_table: null` submits and null-epoch
confirmations. A later [targetless follow-up](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-targetless-2026-11-14.md)
also captures repeated FULL/VIEW SKIPs. Safe local logical qualification, result
scope, isolation and actual-client reachability remain incomplete for both
FULL and VIEW. The ordinary Rust non-test absent-target guard remains.
Pinned read-only dbt show/operation probes did not send those execution RPCs;
this does not establish general unreachability. This limitation does not leave
the tested physical-publication/restart criteria of #158 incomplete. These
complete diagnostic histories remain archived, without a replay exemption or overall support claim ([report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-lifecycle-2026-11-14.md)).

Accepted historical corpus divergences are pinned per fixture entry in
[`golden/replay_gaps.json`](../golden/replay_gaps.json), with its reason and,
where only part of a response differs, the `ignore_fields` it exempts. The
replay fails when an entry becomes unused or resolved
([testing.md](testing.md#goldenreplay_gapsjson)). The categories are:

- **Missing history.** The capture copies from, or depends on, a build made in
  an earlier session, so an empty local store builds instead. This includes
  the four dev data tests of `golden_20261009T083754.534Z.jsonl` (entries
  13–16), which are not reuse across Targets: each hosted SKIP echoes a
  runtime (2140, 1340, 13906, 16155 ms) that matches none of the prod
  confirmations (2088, 2164, 2148, 1511 ms), the dev SQL reads
  `dev_clone.customers` at an epoch about 2.5 days older than the capture,
  and no copy occurs (#190).
- **Conservative identity.** A dependency sent as `SELECT` text in one submit
  and as view DDL in another rebuilds locally where hosted reused.
- **Safety floor.** The externally modified seed table is reloaded
  (AGENTS.md reuse invariant 3) where hosted skips it, serving the edited
  table: crafted, `lag_seed_upstream.jsonl` entries 22–23. Seen live
  through client 2.0.11 (#129), an accepted divergence (see
  [below](#accepted-divergences)).
- **Snapshot and custom node over a modified Target relation (#115).** An
  accepted divergence (see [below](#accepted-divergences)).
- **2.0.6 submit copies of a MERGE incremental (deliberate, #101).** Client
  2.0.6 got `ready_to_clone` for a MERGE incremental over an existing target
  (`clone_failed_fallback.jsonl` entry 2, `clone_submit_equivalent_fallback.jsonl`
  entry 1). The server follows hosted 2.0.11, which rebuilt every incremental
  strategy on submit (`explain_incremental_copy_over.jsonl`,
  `explain_incremental_submit_copy.jsonl`), and copies only FULL on submit
  (maintainer decision 2026-11-11), so it builds there. Whether 2.0.11 would
  copy over a relation with no State history of its own is open (#101
  question 3, above). The copy sources' histories are not in those captures
  either. Keyed gaps: those two entries (local `ready_to_execute`).
- **Malformed requests (#114, crafted only).** An accepted divergence (see
  [below](#accepted-divergences)).
- **Crafted ambiguous Target relation evidence** (#71 probes,
  `experiments/ambiguous_rebuilds.py`). Client 2.0.11 sends none of these
  requests ([explain-catalog.md](explain-catalog.md#ambiguous-rebuild-reasons)).
  The Target relation listed twice or with no epoch (#80) remains an
  accepted divergence (see [below](#accepted-divergences)). The original #81
  no-target model submits failed hosted (`Internal` when unset,
  `InvalidArgument` "Failed to process SQL" when `""`); the server's
  absent-target guard is pinned by
  `offline_regressions.rs::model_submit_without_target_is_invalid_argument_not_a_result_copy`.
  Later #158 controls accept absent-target FULL/VIEW logical outcomes; that
  separate qualification/isolation blocker is described above.

  The proxy did not record the failed #81 submits or #82 no-epoch confirm
  (see "Proxy fidelity"), so their original record is the driver's stdout,
  without fixture rows or keyed gaps. Fresh #158 captures resolve #82 below.

**Resolved 2026-11-14 — target-bearing ConfirmExecution without an epoch
(#82 / #158).** The earlier server accepted this crafted confirmation and
later rebuilt with `no_build_anchor`. It now matches hosted `Internal` (13),
`Database operation failed`, leaving the physical execution pending so a valid
retry can publish. Fresh Databricks and Snowflake controls are retained in
[`databricks_lifecycle_null_confirm.jsonl`](../golden/fixtures/databricks_lifecycle_null_confirm.jsonl);
invalid target/epoch pairs in RecordExecutions reject the whole batch in
[`databricks_lifecycle_null_record_batch.jsonl`](../golden/fixtures/databricks_lifecycle_null_record_batch.jsonl).
An absent target with an outcome epoch is also invalid; this does not settle
absent-target, null-epoch FULL/VIEW logical reuse (#158). Raw legacy confirmed
physical rows with NULL epochs still prevent physical reuse and copying, even
after later valid deliveries ([lifecycle report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-lifecycle-2026-11-14.md)).

Characterization-only fixtures, which the generic replay excludes, are listed
in `CHARACTERIZATION_ONLY` in `crates/harness/tests/conformance.rs` with the
test that pins each.

### Accepted divergences

Settled differences from hosted, accepted by the maintainer: the server's
rule is the conservative one, so it stays. Each needs a request client 2.0.11
never sends, except #115 and #129, which the client sends and where the
server keeps reuse invariant 3. Their keyed gaps stay in `golden/replay_gaps.json`, with
reasons that start `ACCEPTED` and point here. Each rebuild below is
local-only and renders no Explain tree.

- **Ambiguous Target relation evidence (#80, crafted d1 / d2).** A Target
  relation listed twice in `tables` with the build's epoch (hosted SKIPs), or
  listed with no epoch (hosted rebuilds it as missing, codes 4 / 7, "did not
  exist"): the server rebuilds with `own_target_changed_or_missing` (6 / 6,
  generic description). Rationale: crafted-only (client 2.0.11 sends one
  entry per relation and drops a Target relation of unknown epoch), and the
  "Target relation as built" invariant above, which needs exactly one known
  epoch, is the conservative rule. Keyed gaps:
  `explain_ambiguous_rebuilds.jsonl` entries 2, 3, 6 and 7 (`empty_tree`).
- **Data test with incomplete cached results (#87, crafted,
  `experiments/incomplete_test_results.py`).** A data test confirmed with
  `execution_results: null`, or with only `failures`, then resubmitted
  unchanged: hosted SKIPs and echoes the confirmed results (`{"fields": {}}`
  or `failures` alone); the server rebuilds with
  `no_reuse_candidate.incomplete_cached_test_results`. Rationale:
  crafted-only (client 2.0.11 always confirms `failures`, `should_warn` and
  `should_error`), and AGENTS.md invariant 5 requires complete cached
  results. Keyed gaps: `explain_incomplete_test_results.jsonl` entries 2, 6
  (decision) and 3, 7 (`empty_tree`).
- **Upstream with an unknown epoch (#113, crafted,
  `experiments/unknown_epoch_probes.py`).** A FULL model built and
  resubmitted with an upstream whose `last_modified_epoch` is `null` at the
  build, at the resubmit, or both: hosted SKIPs within the Lag tolerance
  (`is_stale` unless only the build's epoch was unknown) and explains it with
  the ordinary SKIP tree, reading an unknown current epoch as `now`. The
  server rebuilds with `upstream_outdated` (6 / 6) and renders no tree,
  because SHAPE 9 needs a proven drift. Rationale: crafted-only (client
  2.0.11 leaves a table of unknown epoch out of `tables` and skips the State
  submit), and AGENTS.md invariant 2 says unknown epochs never prove
  freshness. Keyed gaps: `explain_unknown_epoch.jsonl` entries 2, 6, 10, 14
  (decision) and 3, 7, 11, 15 (`empty_tree`).
- **Malformed requests (#114, crafted,
  `experiments/malformed_requests.py`).** For every node kind, hosted fails a
  submit with empty SQL with `INVALID_ARGUMENT` "Failed to process SQL", and
  a model whose query dependency has an empty name or query the same way. It
  builds a request whose query dependency is listed twice, then SKIPs the
  identical resubmit. The server accepts all of these, builds them, and on
  every resubmit fails reuse qualification
  (`no_reuse_candidate.failed_reuse_qualification`, 6 / 6). Rationale:
  crafted-only (no captured client request has these shapes), and refusing
  to reuse a request whose dependency evidence is empty or ambiguous is the
  conservative rule. Keyed gaps: `explain_malformed_requests.jsonl` entries
  2, 6, 10, 14, 19, 23, 27 (decision) and 3, 7, 11, 15, 20, 24, 28
  (`empty_tree`). The rejected hosted submits have no fixture rows (see
  "Proxy fidelity").
- **Snapshot and custom node over a modified Target relation (#115, client
  2.0.11).** Hosted reuses a snapshot whose Target relation a later
  `--full-refresh` (FULL) build rewrote or an External modification changed,
  and a custom node whose relation was modified externally or replaced by a
  view build in between (`explain_reason_kind_cells.jsonl` `sn_c`, `sn_cz`,
  `sn_e`, `sn_ez`, `vc`, `cv0`). The incrementals, FULL and VIEW rebuild in
  the same situations, as the server does for every kind, with
  `own_target_modified` (6 / 6). Rationale: AGENTS.md reuse invariant 3
  checks the Target relation's External modifications, and a relation
  changed after its build no longer holds the cached result. Keyed gaps:
  `explain_reason_kind_cells.jsonl` entries 171, 187, 297, 311, 313, 323
  (decision) and 225, 231, 369, 375, 376, 379 (`empty_tree`).
- **Seed over a modified Target relation (#129, client 2.0.11).** Hosted
  skips an unchanged-values seed whose table was modified outside dbt after
  its load, serving the modified table, here a dev seed copied from prod
  (`explain_seed_copy_existing.jsonl` `s91b`); it neither reloads nor copies
  it again. The server reloads it with `own_target_changed_or_missing`
  (6 / 6). Rationale: AGENTS.md reuse invariant 3, the same choice as #115.
  Keyed gaps: `explain_seed_copy_existing.jsonl` entries 13 (decision) and
  17 (`empty_tree`).
- **Historical unset-target error (#81, crafted d3).** That particular
  capture returned hosted `Internal`, versus local `InvalidArgument`, and had
  no keyed gap. It does not establish a general absent-target contract: fresh
  #158 FULL/VIEW controls accept logical outcomes. Their unresolved behavior
  is the blocking diagnostic above; the safety guard remains.

## Deployment gaps

- **Auth and TLS.** In the default `DBT_STATE_AUTH=none` mode,
  header-based organization selection is isolation, not authentication, and
  the server refuses a non-loopback address unless
  `DBT_STATE_ALLOW_INSECURE=true`. `DBT_STATE_AUTH=oidc` verifies a
  Token-broker-signed token and its Organization on every RPC except health
  ([protocol.md](protocol.md#authentication-oidc-mode)). The Token broker,
  TLS, session checks and Principal recording are still open (#218–#226). The
  UI reads one configured organization (`DBT_STATE_UI_ORG_ID`).
- **ClientTelemetry** acknowledges pinned 2.0.11 `SubmitTelemetryBatch` calls
  ([dated evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-client-auxiliary-2026-11-12.md)) without
  hosted metering persistence. The deprecated standalone `RegisterSessionStart`
  has no characterized contract and remains unimplemented.
- **Health** always reports `SERVING`; it is not a database readiness check.
- **Capture** runs as unbounded background tasks (`services.rs::spawn_capture`)
  with no retention policy. Bound both before heavy load.
- **ClientValidation** accepts every client version.
- **Speculative submits** are decided only for an unchanged data test with
  no upstreams (#103); every other one is `Undecided`, which costs the
  client one more round trip but leaves its decision to the ordinary submit.

## Primary sources

- dbt docs: [dbt State overview](https://docs.getdbt.com/docs/deploy/dbt-state-about),
  [dbt State configs](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs),
  [deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).
- Clients: [Rust client](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac)
  (`crates/dbt-tasks-core/src/run_cache/`, `crates/dbt-state/`),
  [Python client](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae)
  (`clients/dbt_state/src/dbt_state/run_cache.py`).


## Databricks dependency/template identity (#138, evidence 2026-11-14)

[#138's report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-dependency-template-2026-11-14.md) records
whole-binding identity, exact runtime extras versus declared body/config/macro
signatures, scoped Record evidence, captured errors and template Explain INFO
lines. New fingerprints use `dbt-state-databricks-sql-v4`; genuine v3 pending and
confirmed histories invalidate with current-version positive controls. This adds
no copy eligibility and preserves latest scoped physical history, producers,
unknown-epoch/external-modification checks and exact template contexts.

**#138 is complete with a documented conservative UID difference.** The actual pinned
client's fresh `test.survey_20261114d138df.template_identity` speculative submit
reuses after a different full UID's complete confirmed history, with the same
source-less SQL/body/config and a 3498 ms runtime echo. Both authoritative state
UID and fallback label differ. Exact backend-row attribution is inferred, not
exposed, and equal literal rows are not a wrong-row demonstration. The required
scoped UID floor remains intact, with fresh-UID UNDECIDED/BUILD and within-UID
positive reuse regressions. The certified four-RPC fresh-UID failure stays
outside matching fixtures, with no gap, mask or unknown-baseline exemption.
[Full evidence and limits](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-data-test-uid-boundary-2026-11-14.md).
The preview has landed; the conservative #140/#158/#159 boundaries remain
documented compatibility limits, not shared implementation-ticket blockers.

## Databricks partition replacement scoped acceptance (#147, 2026-11-15)

The pinned serverless client reaches null-safe `REPLACE ON` for a managed Delta
incremental configured with insert_overwrite and partition_by=['p']. The complete
thirteen-cell hosted history and fresh independent Rust execution establish the
bounded fixed-render single-partition-key path, including duplicate/NULL typed
rows, removal of a represented row, disappearance of a NULL partition, empty F
retaining old R, restoration, FULL1 Record and subsequent ordinary type4 rebuild
and reuse. The entire hosted fixture replays without Databricks gaps. A helper
INSERT INTO TABLE parser stop and its separately cleared continuation preserve
every original RPC and successful physical write; delayed first Explain timing
is retained rather than masked. See the [dated report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-insert-overwrite-2026-11-14.md).

The [native supplement](databricks-insert-overwrite.md) adds composite STRING
partition keys, NULL in either/both components, schema failure/retry and additive
recovery, complementary replacement and empty-input retention. Its independent
cumulative bags distinguish unrepresented siblings from fresh source results.
Every cell has complete typed/schema/partition/history checks and isolated
State-disabled literal References. The complete 70-RPC hosted fixture retains
all ten Explain responses and both failed attempts without publication.

This completes the original #147 partition-settings/schema-evolution acceptance
within the demonstrated pinned-client scope. It does not establish all column
removal/type/reordering policies, liquid clustering, legacy cluster overwrite or
restart scenarios. Broader node-kind transitions and Result copy remain separate
work. #138/#140 conservative differences no longer block it. No safety or
isolation floor changed.

The initial supplemental native replay differed by1s in the unchanged Explain:
the handler sampled1794690806996ms and its UUID was minted5ms later across the
second boundary. All12 actual audit clocks were independently recovered from
private original WAL and verified against complete tuple/CRC/full-page evidence.
A test-only hash-bound, org/decision-scoped sidecar now replays the entire original
89-RPC capture exactly with all125 hosted corpora (19passed,0failed). No payload
mask, known gap, clock guess or warehouse rerun was used. The historical red is
retained; live production clock behavior is unchanged. Future147 harness runs
retain audit clocks postflight before test-schema cleanup. The actual original
native executable/source snapshots remain separately pinned. The new native
supplement retains postflight audit clocks before its test State schema is removed.

## Databricks audit ordinary-submit partial (#144, 2026-11-14)

[Bounded primary evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-audit-hosted-rejection-2026-11-14.md)
records aliased and default-name source-less native audit-table histories.
Both BUILD/Confirm a complete failing result and positively reuse it; a later
body-changing pass reaches ordinary INVALID_ARGUMENT 3, `Failed to process SQL`,
and native fallback. The original pre-guard public replay failures and campaign
labels remain immutable. Separately attributed historical transport controls
repeat the rejection, accept a genuine relation read, and rule out configured
alias, UNION syntax and changed-body cache path as necessary conditions.
No synthetic control publishes a confirmed result or claims physical execution.

The narrow ordinary status guard now matches both entire causal histories
(30/21 rows, including all controls) without new replay exemptions. It requires
parsed source-less literal SELECT/UNION ALL, true template/table-store mode and
one checked same-native-audit-context positive-epoch table. Genuine or unknown
reads, CTEs, functions, dependencies, other modes/contexts and other dialects
keep their prior handling. No audit input is removed, no history or identity is
widened, and speculative reuse is unchanged. Snowflake ADR 0008 stays scoped.

Fresh independent Rust aliased/default-name counterparts match all five/three
cells, complete statuses, runtime echoes, Explain and typed physical results.
Their final expected error has no result publication; pinned native fallback
passes with empty physical F while last confirmed State R remains failing.
Every compile/reference canary proves zero State RPCs. Original inventories
and host outputs are preserved; root separately retires only owned outputs
between phases with full identity/actor/history provenance.

Views, alternate audit schemas, genuine-upstream native audit behavior, external
modifications, drop/recreation and post-error reuse remain uncovered in #144.
#143 cached outcomes are complete, including the later genuine-upstream
pass/fail/recovery cases and BUILD Explain fix. The #138 UID difference remains
explicit; broader audit feature gaps still belong to #144.

## Databricks predicate replacement scoped acceptance (#148, 2026-11-15)

The pinned client sends replace_where as DBT_CUSTOM11, including native
full-refresh Record input. The complete hosted90-RPC history and a fresh Rust
counterpart cover first→steady→fresh reuse, inside update/duplicate NULL rows,
outside update/delete retaining the original target row, empty incoming
replacement, a destination-only predicate widening, and full refresh→ordinary
SQL transition→fresh reuse. Both backends independently check complete typed
source/cumulative R/selected F multisets, schema, MANAGED Delta/history, accepted
runtimes and zero-RPC literal references. The initial helper's FULL1 assumption
stopped after successful refresh; the original76/source remain preserved, and
only missing checks and the final two invocations continued under a separate
grant. No State history or expected rows were fabricated. The [bounded report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-replace-where-2026-11-14.md)
records whole-trace replay and actual native audit-clock provenance.

The [schema/predicate supplement](databricks-replace-where.md) adds native
schema failure/retry with no publication, `append_new_columns` recovery and
fresh own-runtime reuse. Outside rows survive bounded replacement with NULL
backfill. Actual out-of-range input fails twice with
`DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION` and
`DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`; neither attempt changes the target's
complete rows/schema/history or sends Confirm/Record. Widening the destination
predicate recovers and removes the previously retained outside row. Independent
literal source/target bags, typed schemas, native SQL and State-disabled
SELECT-only References check every checkpoint.

This completes the original #148 predicate/data, supported additive schema,
full-refresh and exact-error criteria within the pinned scope. It does not
claim every predicate expression, column removal/type change, legacy runtime
mode or all strategy transitions. No generic full-refresh type mapping,
unknown temporal encoding or unsupported error policy is inferred. Accepted
UID/namespace/unknown-freshness conservative differences remain explicit and
never permit unsafe reuse. The full Databricks support claim remains separate.

## Databricks DELETE_INSERT bounded completion (#149, 2026-11-15)

The complete real-client hosted88-RPC history and a fresh Rust counterpart
cover stable first/ordinary SQL, non-NULL replacement with duplicate keys,
absent NULL keys retaining old rows, selected NULL keys replacing old NULL
rows through native `<=>`, empty input retaining cumulative R, and native
FULL1 full refresh followed by ordinary5 rebuild/reuse. Both backends verify
complete typed source/R/F multisets, ordered schema, MANAGED Delta and full
histories, full statuses, own confirmed runtimes and zero-RPC literal references.
Whole hosted and native histories replay exactly, including12 Explain trees
each; native uses independently retained actual handler audit clocks.

Final offline review corrected a validation-helper lookup to require literal
`skip_execution.explained_decision.is_stale=false`. All12 raw SKIP flags were
independently rechecked as false; original captures and executed frozen sources
remain unchanged. No production policy or live execution was modified or rerun.

The original #149 replacement/input/full-refresh and complete response/Explain
criteria are complete in this bounded native scalar-key history. Native SQL is
NULL-safe `REPLACE ON`, not an assumed legacy DELETE/IN implementation. Current
completion does not claim alternate/empty/invalid/composite keys, predicate or
alias settings, schema/name-binding changes, failure/restart/upgrade cross-products
or generated strategy breadth. Legacy fallback and additional temporal encodings
remain unverified. These limits stay outside the supported claim; no fixture or
safety rule changes. See the
[bounded report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-delete-insert-2026-11-15.md).


## Bounded MICROBATCH counterpart (#150, 2026-11-15)

Both pinned hosted and Rust ten-command histories pass independent complete
source/R/F typed multisets, duplicate/NULL keys, explicit daily half-open bounds,
source timestamp moves/deletes, overlapping/empty replacements and exact own
NULL-event-time target-row retention. Each has nine daily batches, five
Confirms, five fresh own-runtime SKIPs, ten zero-RPC references and full Explain.
The entire hosted70 and native70 replay exactly; native uses ten actual audit
clocks retained before schema Drop. The original description-only red remains
preserved alongside its narrowly reviewed correction.

The [real-clock/lookback supplement](databricks-microbatch.md) adds implicit
UTC daily scheduling, lookback0/2, required unchanged reuse, and actual
single-batch custom predicates with independent retained/replaced results.
Configured multiple-batch predicates expose a pinned-client limitation:
the native macro accumulates contradictory batch bounds, causing a failed
command without Confirm. The whole failed attempt and same-graph recovery
remain replayed; no adapter workaround or invented freshness anchor is used.
This distinguishes supported bounded #150 behavior from that explicit client
capability limit. Broader full-refresh/Record, schema/concurrency/lifecycle and
isolated Target-only modification combinations are not claimed. See the
[bounded report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-microbatch-2026-11-15.md).

## Databricks direct pre-copy bounded (#152, 2026-11-15)

The narrow native MERGE/check-snapshot RegisterClone/adoption path now has
credential-free public gRPC coverage. Its versioned direct provenance freezes
source inputs/producers and guards State-recorded source replacement before
Confirm; it does not grant automatic namespace copy eligibility. Its fresh
physical MERGE/check-snapshot acceptance is documented in
[direct pre-copy](databricks-precopy.md). Unobserved physical source races and
broader copying remain outside that quiescent scope.

## Databricks automatic submit copy bounded (#153, 2026-11-15)

The original default-off experimental path supported deterministic fully-qualified
SELECT-star FULL models over one exact current source. Its new versioned identity
binds UID, client signatures, exact SQL/config and output catalog/name; source
inputs/build age and publication provenance remain frozen. Invalid origins cannot
fall through strict SQL or namespace adoption. MERGE is measured to BUILD1/6.
The independently completed whole75 primary history replays exactly, while the
separately authorized whole91 controls retain explicit hosted RAND rewrite and
unknown-epoch display-clock limits. No runtime-field masks or replay gaps are
added. Quiescent managed-Delta scope, unsupported positive chains and
unobservable unversioned physical source races keep full ADR0009 unresolved.
See [scope and evidence](databricks-submit-copy.md).

## Databricks copy recovery bounded (#154, 2026-11-15)

The [serialized recovery supplement](databricks-copy-recovery.md) retains the
entire hosted151 history: source removal, chain refusals, target modification,
the exact FULL platform failure and retry, automatic client-checked fallback,
and direct source rejection/build. Independent full results, properties and
later history are checked through the real client; native whole replay requires
all actual decision audit clocks. Positive chains and physical races stay excluded.

The complete hosted boundary pins nine differing local payloads: entries26/30,
115/119,127/128,129/133 and141. These are BUILD/Explain presentation and the
conservative stale direct-source refusal; its raw hosted confirmation gets
NotFound without publishing anything. No response field or timestamp is masked,
no pending row is invented, and existing exact corpora remain unchanged.
This accepted conservative behavior is distinct from full ADR0009 conformance.

## Databricks default-on rollout (#188, 2026-11-16)

Both bounded shared-v2 copy paths now run without private server switches.
The maintainer accepts unpinned physical source races shared with Snowflake
and the Unity Catalog shallow-clone FULL rebuild failure also observed with
hosted and State-disabled execution. [Current copy policy](databricks-copy-preview.md)
records these limitations and hosted-compatible client controls. Unknown input
evidence, incompatible identities, externally modified sources observed during
validation, and invalid publication still refuse reuse. No snapshot pinning,
automatic DROP or new materialization support is introduced.
