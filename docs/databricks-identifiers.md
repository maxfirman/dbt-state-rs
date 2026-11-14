# Databricks identifiers and resolution context

The #136 campaign characterizes identifiers through the pinned dbt 2.0.11
client and production gRPC router, with independent Databricks physical checks.
The [dated evidence report](reports/databricks-identifiers-2026-11-13.md)
records exact capture digests and validation. This extends the
[table/view baseline](databricks-baseline.md); broader #132 copy/snapshot
blockers remain unchanged.

A managed Delta result reads a three-column input with duplicate rows and
NULLs. Actual client cases change only the source relation spelling: three-part,
schema-qualified, unqualified, backticks, and case of each component separately.
Hosted reuses all of these when they resolve to the same source. Switching to
the same basename in another owned schema returns different rows and requires
a build; returning to the original also builds against latest physical history.
A source view supplies an actual query-dependency definition and context.
Every step repeats unchanged and requires reuse. Each result is compared to
State-disabled dbt, an independent SELECT and declared complete typed rows,
ordered column schema, and MANAGED/Delta properties.

Crafted boundary calls are labeled separately. They derive from the actual
confirmed requests, changing one context or physical metadata field. Fully
qualified root/dependency SQL ignores unused default contexts. Unqualified
root SQL distinguishes different default schemas/catalogs, folds their plain
ASCII case, and uses the target schema when the default schema is absent.
Independent SQL Statement Execution reads of identical unqualified SQL under
both owned contexts verify the different complete results. Fully qualified
plain physical metadata names match across bare/backtick and case variants.
Boundary build proposals remain pending; they never confirm invented epochs.
Every call has a complete captured Explain response.

Warehouse rules implement only these demonstrated identifier forms. A simple
SELECT with named-column projections and one named input resolves its input
against its own catalog/schema. Only that input's exact source span is changed;
all other SQL bytes remain intact at this resolution stage. The subsequent
[bounded SQL identity rules](databricks-sql-equivalence.md) admit captured
formatting, table-alias, literal-projection and single-CTE forms; other shapes
retain exact SQL and resolution context. Template/configuration policy (#138)
remains a separate slice. No Result copy eligibility is added.

A single Warehouse ingress rule canonicalizes demonstrated fully qualified
physical metadata before history lookup, freshness, producer resolution,
own-target checks and Explain capture; SQL execution recording uses the same
rule. Raw SQL and wire target spelling remain available for DDL. Organization,
warehouse family, namespace, and distinct catalog/schema/relation scope remain
separate; only latest physical history can qualify.

Migration 0020 adds a generated `physical_target_key` and current-history index.
The raw target spelling and old fingerprints stay intact for both old and new
rows. SQL evidence carries its original target separately for persistence;
decoded decision rows retain that spelling alongside the physical name.
Selectors/deferral record the original submitted target, and a public confirmed
build→deferral regression verifies it survives later output. Both preexisting
confirmed aliases and old pending aliases confirmed after upgrade participate
in the same latest-row selection. Canonicalizing only new publications would
miss older alias history; equal-epoch frozen regressions demonstrate why target
freshness cannot repair that lookup error. Producer and historical reads use
the same key. PostgreSQL's projection admits exactly the Warehouse boundary's
three plain ASCII components with independently optional backticks. ASCII
`translate`, explicit `C` collation and exact unsupported forms avoid
locale/Unicode/quoted-dot collisions. Only ordinary spaces around the known
dialect family are admitted; tab/newline/Unicode whitespace stays exact.

Explain distinguishes a changed physical binding from a changed rendered
projection by reconstructing the prior build's full versioned identity, never
by trusting a client body hash. This affects explanation only: the current
physical input or context change still requires a build. Source view changes
retain their external-table and view-ignored branches; relative source ages
round minutes as captured.

Current Databricks SQL identities use `dbt-state-databricks-sql-v3`, building on
the earlier identifier-v1 and rendered Spark-alias-v2 rules (the aliases are
covered by [#159 isolation evidence](reports/databricks-isolation-2026-11-13.md)).
Representative rendered-mode v1/v2 frozen histories cold-miss, including an old
pending execution confirmed after upgrade. Existing `compare_unrendered_code`
template alternatives can still qualify under the preserved policy. Other
warehouse versions are unchanged.

Reproduce hosted characterization first:

```bash
uv run --with pyyaml==6.0.3 python experiments/databricks_identity.py \
  --dbt /path/to/pinned/dbt-2.0.11 --proxy /path/to/record-proxy \
  --probe /path/to/state-probe --schema-lease /path/to/owned-lease.json \
  --work-dir /path/to/new-hosted-identity
```

The runner permits exactly its new `i_<run>_input`, `result`, and `input_view`
names in the two explicitly evidenced leased schemas. It verifies original
ownership digests and preexisting inventories, refuses fresh-name collisions,
and retains every prior object. It never creates/drops a schema. The proxy is
owned and certifies complete capture; inherited local-upstream settings are
cleared by the shared runner. Successful and failed artifacts are retained.

After public replay passes, run the ignored real-client acceptance with a fresh
artifact directory and the hosted query identity:

```bash
export DBT_STATE_DATABRICKS_PYTHON=/path/to/python-with-pyyaml
export DBT_STATE_DATABRICKS_DBT=/path/to/pinned/dbt-2.0.11
export DBT_STATE_DATABRICKS_SCHEMA_LEASE=/path/to/owned-lease.json
export DBT_STATE_DATABRICKS_QUERY_IDENTITY=YYYYMMDDxxxxxx
export DBT_STATE_DATABRICKS_ARTIFACTS=/path/to/new-rust-identity
cargo test -p dbt-state-harness --test databricks_identity \
  real_dbt_identifiers_match_independent_reference -- --ignored --nocapture
```

Ordinary PR checks replay the complete hosted corpus and run public safety,
recording, frozen-upgrade and credential-free ownership regressions.
