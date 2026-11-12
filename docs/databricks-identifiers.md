# Databricks identifiers and resolution context

The #136 campaign characterizes identifiers through the pinned dbt 2.0.11
client and production gRPC router, with independent Databricks physical checks.
The [dated evidence report](reports/databricks-identifiers-2026-11-12.md)
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
all other SQL bytes remain intact. Quoted dots, escaped backticks, CTEs,
functions, expressions and other uncharacterized shapes retain exact SQL and
resolution contexts. Formatting/expression policy (#137) and template/config
policy (#138) remain separate slices. No Result copy eligibility is added.

A single Warehouse ingress rule canonicalizes demonstrated fully qualified
physical metadata before history lookup, freshness, producer resolution,
own-target checks and Explain capture; SQL execution recording uses the same
rule. Raw SQL and wire target spelling remain available for DDL. Organization,
warehouse family, namespace, and distinct catalog/schema/relation scope remain
separate; only latest physical history can qualify. New Databricks identities
use `dbt-state-databricks-identifiers-v1`; independently captured old-server
hash bytes must cold-miss, including an old pending execution confirmed after
upgrade. Existing warehouse versions are unchanged.

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
