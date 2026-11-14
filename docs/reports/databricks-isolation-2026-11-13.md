# Databricks isolation characterization — 2026-11-13–14

**Partial: #159 remains incomplete.** Hosted absent-namespace fallback can
reuse a result for a fresh named namespace; this conflicts with the required
self-hosted physical isolation contract. The Rust server retains that isolation.
This crafted wire finding is not a two-warehouse wrong-row proof; only one
Databricks workspace is authorized for these experiments.

Ticket #159 covers physical history scope and concurrent campaign ownership.
This evidence uses the pinned dbt 2.0.11 binary (SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`),
Free Edition serverless warehouse `47fd3fa312624d91`, Unity Catalog and managed
Delta tables. Writes use only exact fresh run-owned relations in the two
previously evidenced `cdo_demo_db` leased containers. No schema is created or
dropped in Databricks.

## Hosted observations and classifications

`databricks_isolation_scopes.jsonl` is a complete 113-RPC crafted public-gRPC
history: 49 submits, 15 successful confirmations and 49 full Explain responses.
It starts from the pinned client's VIEW request (wire execution type 10); the
original control records synthetic `MANAGED` confirmations. These
accepted boundary declarations are not physical view/table evidence. The later
61-RPC FULL control explicitly selects execution type 1 and coherent MANAGED
confirmations, and the 35-RPC alias/namespace control uses coherent VIEW
confirmations. Synthetic epochs,
confirmations, namespaces and dialect substitutions are not claimed as actual
client execution or physical warehouse evidence.

The Rust implementation keeps connection namespaces, warehouse families and
Target relations independent. Hosted positive controls preserve an already
recorded named scope's required SKIP after another scope records different
logic; the newly discovered absent-to-fresh-named fallback is separately
unresolved below. Absent and empty
namespace spellings share the absent connection scope. Equal-epoch confirmed
changed SQL blocks resurrection of the older identity. The only new rendered
SQL dialect aliases admitted are the exact strings `spark` and
`spark, normalization_strategy = lowercase`, equivalent to `databricks` in the
captured cases. Both aliases have changed-SQL, namespace and Target relation
negative controls. This does not establish arbitrary dialect option, seed or
template-mode equivalence.

Hosted OAuth chooses the organization. Arbitrary `x-organization-id` headers
cannot characterize hosted organization isolation. Public production-router
regressions independently label organization checks as self-hosted safety,
retaining the existing self-hosted authentication model.

The original 90-RPC discovery includes an unsupported uppercase `Databricks`
dialect that returned hosted `INTERNAL`. It remains diagnostic evidence outside
the pinned client's actual `databricks` spelling and the admitted exact alias
set; it is not hidden by a replay gap. A first expanded protocol attempt used
comment-only cell tags: hosted ignored those comments, so two cells sharing the
absent namespace collided. The failed attempt is retained. The promoted fresh
campaign uses distinct string-literal identities and includes all causal
confirmations.

The early seven “partial-history” differences were not seven alias failures.
Six were existing-target/no-State-history description or Explain differences,
addressed by the separately integrated #158 reason dependency and its SQL-read
safety gate. The remaining original entry 19 was a cross-namespace rejection
and query diagnostic (8/7), characterized separately here. The actual alias
identity defect is independently demonstrated by the frozen v1 public history.
No new complete synthetic history receives a partial-history exemption.

## Concurrent real-client evidence

`databricks_isolation_concurrent_a.jsonl` and
`databricks_isolation_concurrent_b.jsonl` contain 15 and 21 hosted RPCs.
Two independent projects ran concurrently against separate run-prefixed
relations in the same leased containers, using separate profiles, projects,
artifacts, ports and owned proxy processes. Seven dbt invocations include two
State-disabled independent references. First builds must execute, unchanged
runs must SKIP, and the second project must still SKIP after the first project's
cleanup. Always rebuilding cannot satisfy these checks.

Every observed relation is compared against declared BIGINT/INT/STRING column
metadata, complete typed duplicate/NULL row multisets, required MANAGED/Delta
properties, a State-disabled dbt result and an independent literal SELECT.
Campaign-specific string values make a peer result fail even when schema and
row counts agree. Cleanup first rejects the peer's exact relations without
issuing SQL, then drops only successfully created own relations. Before/after
inventories prove every peer and preexisting object remains. The surviving
peer's full result and reference are read again before required reuse; then its
own cleanup is checked the same way. No broad schema cleanup is permitted.

## Implementation and upgrade behavior

Shared Warehouse policy admits the two exact evidenced aliases for rendered
SQL. Other warehouse fingerprints and template dialect framing retain their
existing behavior. Public conservative controls reject unsupported option
strings and changed rendered SQL under an alias even when the template body
hash is stable; this does not override the existing dual rendered/template
matching contract for an unchanged rendered query. Databricks uses
`dbt-state-databricks-identifiers-v2`, invalidating earlier identities safely.
The physical-key projection and raw-name preservation from #136 remain intact.
No history ordering rule is introduced here; #158 owns completion ordering.

The frozen `databricks_alias_identifiers_v1_aa89b6a.json` was read from genuine
public Submit/Confirm history produced by clean #136 source `aa89b6a`, server
SHA-256 `d9cc53c64f8b3fbae300ed625648ffa2b2dcdaf8999c3027039aee92fa5ab84e`.
The old server demonstrably reused its canonical request but rebuilt the two
hosted-supported Spark aliases. Upgrade regressions cover confirmed v1 history
and pending v1 history confirmed after a new-version build, including equal
epochs. Old fingerprints are never recomputed using the new implementation.

## Reproduction and evidence

Durable secret-free archive:
`~/.local/state/dbt-state-rs/databricks-159-20261113/`.
`protocol-discovery`, `protocol-scopes`, `protocol-scopes-v2`,
`hosted-concurrent` (invalid run-ID preflight), `hosted-concurrent-v2`,
`pre-alias-upgrade*`, `pre-alias-red`, `frozen-empty-namespace-v2`,
`default-definition-controls` and the dated diagnostic/reduction directories
retain successes and failures, ordered
operations, actual upstream provenance, exact requests/responses/statuses,
complete physical results, commands, invocation counts and executable digests.

```bash
uv run --with pyyaml python experiments/databricks_isolation_protocol.py \
  --dbt "$PINNED_DBT" --proxy "$PROXY" --probe "$PROBE" \
  --node-kind view --schema-lease "$LEASE" --template golden/fixtures/databricks_table_view_baseline.jsonl \
  --work-dir "$FRESH_PROTOCOL_ARTIFACTS"
uv run --with pyyaml python experiments/databricks_isolation.py \
  --dbt "$PINNED_DBT" --proxy "$PROXY" --probe "$PROBE" \
  --schema-lease "$LEASE" --work-dir "$FRESH_CONCURRENT_ARTIFACTS"
# Repeat the second command with --rust-url http://127.0.0.1:PORT after exact replay.
```

The concurrent command's optional run ID is YYYYMMDD plus five hexadecimal
digits; independent child runs append `a` and `b`. It refuses nonempty artifact
directories or existing run-prefixed relations. Its credential-free ownership,
fixture completeness and peer-result fault checks run in ordinary CI through
`databricks_isolation.rs`; the ignored real-client test is the opt-in Rust gate.

The new exact replay corpus consists of nine isolation fixtures: 378 RPCs,
including 157 complete Explain responses. Source-metadata, dialect/namespace,
strict literal/context identity, coherent FULL aliases, None/empty namespaces
and refused-build confirmation controls supplement the original 113-RPC trace
and actual-client concurrency captures. Causal bookkeeping now recognizes only
the exact three admitted dialect strings and the captured None/empty physical
scope equivalence, so the corresponding SKIPs receive full payload comparison
instead of being counted as unknown history. Product requests are unmodified.

An empty-string namespace previously persisted as SQL `''` while physical reads
searched SQL NULL. Writes now normalize only the empty string; legacy reads use
`NULLIF(table_namespace, '')` across physical history, producers, data-test
outcomes and copy provenance. Nonempty namespaces, organizations, warehouse
families and Target relations remain distinct. Genuine v2 rows exported from
the immutable pre-fix server cover both confirmed legacy empty rows and old
pending rows confirmed after a newer NULL-namespace build. This fixes the
missed-history problem without rewriting old fingerprints or execution rows.

Diagnostic reads return only booleans or a source name, never an execution row.
An omitted namespace can report query evidence under the same physical name in
other namespaces (8/7); other-name evidence selects current physical state
before strict identity and reports VIEW type refusal (1/3) or the captured FULL
copy-disabled case (1/10). These remain ordinary pending builds. Public and
hosted confirmation controls require subsequent same-SQL reuse, changed-SQL
rebuild and continued reuse after the merely named source changes. No copy is
granted, copy origin adopted, upstream producer inferred or cross-scope
freshness evaluated by this diagnostic path. A fresh named namespace may report only a changed definition from the absent
scope; matching definitions still cannot grant reuse or an unchanged-query hint.

Snowflake Explain presentation now double-quotes the captured crafted
three-component backtick metadata spelling while keeping raw physical names
and own-input comparisons separate. Existing valid Snowflake and Databricks
name presentation stays intact.

The two 28-RPC `default-definition-controls` (VIEW) and
`full-default-definition-controls` (FULL) captures remain outside the acceptance
corpus as an explicit unresolved contract conflict: after None or empty-scope
confirmation, hosted State SKIPs a fresh named namespace for the same query.
For a changed query it builds with a query-changed Explain line. This was
isolated after twenty logical UID/project/profile/pending controls and eight
coherent-context/alias controls produced no pending-specific effect; a fresh
complete 113-RPC rerun reproduced the original two Explain lines. No replay gap,
unknown-history allowance, trace pruning or unsafe fallback was introduced to
claim completion. The source-less scoped safety test intentionally requires a
BUILD where this hosted fallback SKIPs.

Actual Rust concurrency passed all seven pinned-client invocations with the
complete physical/reference/ownership checks described above; the immutable v2
server and its dirty source manifest are retained under
`survey-bin/isolation-159-v2-pre-diagnostic/`, with the Rust campaign under
`rust-concurrent/`. This earlier independent real-client slice does not claim
that crafted absent-namespace traffic is client emitted. The final current-source rerun under `rust-concurrent-final/` independently
passed the same seven invocations and all physical checks. Its immutable server
is under `bin-final/`, SHA-256
`40371798b1aa8066b9988bdb096d278571a7072b14864302c0d414371f0ae5d7`.
`bin-final/provenance.json` records the dirty source snapshot and build manifest;
it does not claim that this binary was built from a clean commit. This report does not claim full
#159 or #132 completion.
The existing copy-rebuild, snapshot and #140 freshness safety blockers remain
unchanged.


## Validation of the safe partial

The final source passed the full workspace (615 passed, 33 ignored), exact
conformance (88 corpora; all nine new fixtures have zero unknown-baseline, gap or
partial-history counters), ten public isolation tests, 309 server library tests
with an unusable database URL, strict workspace/all-targets/fuzz Clippy, formatting
and 25 Databricks Python tests. The complete DuckDB gate passed all 25
acceptance cases (596.68 seconds), including generated physical/oracle mutations.
All experiment Python tests also passed with the required PyYAML dependency
(33 unittest cases plus the scripts’ top-level fault checks); the initial bare
Python attempt lacking PyYAML is retained as an environment failure. The public isolation suite has one additional
ignored real-client test; its separate final live run passed as described above.
The machine-readable companion index records fixture hashes, retained evidence
and exact check logs. These checks validate the bounded implementation; they do
not resolve the documented hosted namespace conflict.
