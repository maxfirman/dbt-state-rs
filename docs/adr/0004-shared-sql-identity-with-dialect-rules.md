# Share SQL identity machinery with explicit warehouse semantics

## Decision

Use one fingerprinting and dependency-binding pipeline for Snowflake and
DuckDB, sharing traversal, provenance and execution lifecycle machinery.
Warehouse-specific rules supply parsing, identifier semantics and validated
SQL transformations. This creates a shared implementation exercised by cheap
DuckDB integration tests while preserving Snowflake behavior through its
existing conformance tests and explicit dialect rules.

Begin DuckDB cross-environment matching with a conservative, tested SQL subset
that recognizes supported prod/dev schema substitutions. Preserve the binding
between each dependency reference and its definition, structured identifier
components, and distinct physical inputs. Unknown syntax or uncertain
semantics must prevent automatic copying. A parser accepting syntax does not
establish equivalence. Do not enable Snowflake normalization wholesale for
DuckDB: for example, DuckDB's double-quoted identifiers are case-insensitive,
whereas Snowflake's are case-sensitive.

Copy eligibility, source validation, recording and subsequent reuse share the
same lifecycle. DuckDB CTAS and Snowflake CLONE remain warehouse-specific
execution capabilities. Adding CTAS DDL alone is insufficient: automatic
cross-environment selection and safe copy-to-SKIP adoption must also work.
Changes to persisted identity semantics require versioned fingerprints and
safe invalidation rather than reinterpretation of old history.

Evidence: current [SQL normalization](../../crates/server/src/sql_norm.rs),
[fingerprinting](../../crates/server/src/analyzed_workload/identity.rs), and
[DuckDB identifier rules](https://duckdb.org/docs/2.0/sql/dialect/keywords_and_identifiers).

Current rules: [DuckDB SQL identity and result copies](../duckdb-identity-and-copy.md).

## Shared analyzed workload amendment (#181, 2026-11-15)

Extend the existing seam to Databricks through one analyzed workload and one
copy qualification/provenance lifecycle. The [architecture contract](../architecture.md#shared-analyzed-workload-contract-181186)
specifies the small interface, first grammar, responsibility replacements and
compatibility gate. #183 extracted the shared analysis and copy lifecycle without
changing identity bytes or behavior; #184/#185 replaced the Databricks analyzers
and origin framing with shared structural analysis and versioned copy proofs.
The [focused acceptance matrix](../databricks-shared-copy.md) states the evidence
and remaining limits.

Keep a structural representation with explicit reference-to-definition bindings,
node-kind/configuration evidence and context/function effects. Raw SQL and strict
exact fallback survive opaque analysis. Physical references and logical copy
identity are distinct; State history, physical observations, producers and
wall-clock build age are separate decision inputs. The adapter can establish
identifier and function semantics but cannot manufacture column types, a catalog
or missing session settings. Unknown proof refuses cross-target adoption.

We choose structural equivalence over query optimization: no algebraic rewrites,
arbitrary schema removal or universal logical plan. This permits useful FULL
projections, filters, joins, CTEs and checked aggregation while keeping semantic
uncertainty visible. The same analysis serves already-supported direct MERGE and
check snapshots; their accumulated result still requires matching node-kind and
configuration evidence. Permission to attempt clone DDL never proves permission
to SKIP afterward.

The shared lifecycle replaces parallel analyzers, identity framing and
publication/adoption checks; it is not a new facade over permanent duplicates.
Dialect adapters retain their characterized identity bytes, naming, observation
semantics and copy execution capabilities. Snowflake/DuckDB keep current policy;
Databricks v2 copy proofs cold-miss incompatible legacy evidence, including
pending v1 rows confirmed after upgrade. Ordinary strict identities are unaffected.
Both Databricks copy paths initially stayed default off (superseded by the
[rollout amendment](#databricks-rollout-amendment-2026-11-16) below); positive chains remain unsupported,
and the documented conservative stale-source refusal is retained even where
hosted grants. Unversioned physical copy races remain outside this decision.

## Databricks rollout amendment (2026-11-16)

Following the focused acceptance, enable both bounded Databricks copy paths
without separate server switches. This supersedes the default-off rollout
condition above, not its eligibility, identity or provenance rules. Preserve
hosted-compatible client copy controls. The maintainer accepts unpinned physical
source races (also present in Snowflake) and the documented Unity Catalog
shallow-clone FULL rebuild limitation, which also affects hosted/client-only
execution. Neither blocks cloning acceptance. Keep the limitation and supported
client workarounds explicit in [current copy policy](../databricks-copy-preview.md);
do not automatically drop relations or broaden materialization support.
