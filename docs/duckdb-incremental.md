# DuckDB incremental models

The local integration supports explicit `incremental_strategy='delete+insert'`
with a single identifier `unique_key`, non-NULL keys, and explicit
`on_schema_change='fail'`. The tested versions remain dbt Core 1.10.15,
dbt-duckdb 1.10.0, DuckDB 1.4.1, dbt-state 2.62.6 and query-cache packages
2.7.0, pinned by the Python lockfile. Normal cold initialization is wire
DELETE_INSERT (5); only an explicit full refresh uses FULL (1).

This strategy deletes target rows whose key appears in the temporary query
result, then inserts that result. Rows omitted from the result remain in the
target. Source deletion therefore does not delete a retained target row. Changed
old keys are updated only when the incremental query selects them again. This
is why the real test uses literal results for each update sequence instead of
an unconditional full-refresh oracle. A timestamp/watermark predicate must
select updates to old keys. Duplicate result keys remain duplicates. NULL keys
accumulate under ordinary dbt execution; existing targets with NULL keys bypass
State, and such sources cannot initialize another environment.

Self-reading incremental SQL requires a fixed-point proof before reuse. The
initial supported shape is a single base-table SELECT of unchanged, unqualified
columns with exactly `watermark > (SELECT MAX(watermark) FROM {{ this }})`,
optionally wrapping MAX in COALESCE with a literal fallback. The watermark must
be projected unchanged under its original name. Joins, aliases, transformed
projections, extra predicates and view inputs fall outside this narrow proof.
After successful execution, unchanged inputs cannot pass this strict bound
again. Other direct or indirect self reads execute ordinarily on every run;
their completed target alone does not prove another execution would be a no-op.
Both rendered branches must satisfy this rule before development initialization
or automatic copying is eligible. External-input-only incrementals retain their
existing reuse behavior.

Schema evolution requires an explicit full refresh or ordinary execution with a
different schema policy. The supported `fail` mode lets dbt reject added,
removed or incompatible columns; State does not implement schema migration.
Incremental predicates (including the `predicates` alias), other strategies,
implicit strategy defaults, snapshots, Python models and custom materializations
execute through dbt without State compile, submit or cached-outcome handling.
User overrides of the selected delete/insert strategy macros also bypass State.
Successful ordinary writes still pass through the completion/evidence lifecycle.

## Development initialization

With one persistent file and `prod`/`dev` profile schemas, the actual upstream
client calls RegisterClone before compiling an absent incremental target.
CTAS can therefore make `is_incremental()` true in that compilation. This route
has its own source/target/epoch correlation context; it does not pass through
the client's SQL-response handler.

Before CTAS, the server requires the current confirmed source in the same
organization, file/generation and DuckDB family, DELETE_INSERT execution type,
inspected source table properties, exact current source epoch, and a matching
versioned initialization identity. Pending registrations, unknown source
metadata, external source edits and later incompatible overwrites cannot supply
this provenance. This first initialization slice uses an ordinary source;
a source whose latest outcome is itself a copy is not eligible here.

The identity binds the SQL execution-contract version, node UID, template text,
effective configuration (except
physical relation location and nonsemantic labels) and supplied macro contents.
It is stored in the existing opaque local `node_hash` field and transmitted in
RegisterClone labels and SQL semantic extras. Binding it to automatic SQL
copying also prevents a rejected initialization template from re-entering through
an identical rendered cold branch. If this identity is unavailable, automatic
incremental copying is disabled; strict same-target reuse remains possible.
No wire field or service name changes. Snowflake hashes
and RegisterClone policy are unaffected. A changed template/configuration is
rejected before CTAS: copying first and discovering the mismatch in subsequent
SQL would let a delta query retain obsolete source rows.

Initialization accepts only `config`, `is_incremental()` and `this` template
primitives. Both branches must pass the existing deterministic local SQL
reference validation, with fully qualified base-table inputs in the local
catalog and known evidence. Inputs in other environment schemas, views,
`ref`/`source`, arbitrary Jinja, dynamic target/variables/macros, random functions,
order-sensitive SQL and unknown evidence take ordinary absent-target execution.
This deliberately narrower contract avoids equating unchanged template text
with unchanged physical input resolution or deterministic results.
Named type casts in either branch also prevent initialization: their mutable
definitions are not part of the relation evidence. The execution-context check
requires strict scalar-subquery cardinality in the actual worker session and
rejects configured default collations and nonempty or unknown worker collations
before initialization as well as SQL reuse. Cursor plugin settings are included
in this check. The SQL contract version is bound into initialization identities,
so earlier sources that lack this proof must rebuild before supplying a copy.
Timezone-aware casts in either branch also prevent initialization because their
intermediate values can depend on timezone/calendar settings that are not bound to
the workload. This includes TIMESTAMPTZ, TIMETZ and their aliases even when the
output columns have supported ordinary types. The versioned SQL contract also
invalidates earlier unchecked SQL outcomes and initialization source identities.
The same recursive validation rejects FLOAT/DOUBLE inside nested cast targets
in either branch, including ARRAY/STRUCT and TRY_CAST forms. Supported outer
types and an exact final output do not certify approximate intermediates.

CTAS validates the exact plan, source/target environments, source properties and
source evidence in the transaction that reads the source. It records completion
evidence after commit and confirms that registration separately. Subsequent SQL
must still match its rendered/configuration/dependency identity. A template with
an `is_incremental()` branch may BUILD once after initial creation or copying
to record its new rendered identity; the next stable run must SKIP. The simple
unconditional fixture adopts a validated copied identity immediately.

A failed or uncertain compile-time copy leaves its registration pending. The
following SQL submission records the actual ordinary outcome under its own
request ID. Uncertainty invalidates the evidence generation at restart. Neither
a returned copy proposal nor a failed CTAS certifies a reusable target. Copy
disabled settings, absent sources and unsupported properties retain ordinary
dbt behavior. Source validity remains distinct from upstream freshness; the
shared policy can permit a physically valid older source result.

## Validation

```sh
DBT_STATE_DUCKDB_PYTHON=integrations/duckdb/.venv/bin/python \
  cargo test -p dbt-state-harness --test duckdb_incremental_python -- --ignored --nocapture
cargo test -p dbt-state-harness --test duckdb_incremental
```

The real dbt gate crosses production gRPC and checks cold BUILD, unchanged SKIP,
controlled old-key updates, retained deleted-source rows, actual profile-based
RegisterClone/CTAS/confirmation, subsequent SQL submission and restart reuse,
logic mismatch before initialization, disabled copying, edited/deleted sources,
CTAS-plan failure with ordinary fallback, conditional branch transitions, and
ordinary unsupported workloads. Literal expected rows and exact column types
also cover self-updating models, indirect self reads and unsafe transformed
watermarks alongside the supported fixed-point watermark.
They are independent of production fingerprint and decision code. Existing
Snowflake clone regressions and captured conformance replay remain required.
