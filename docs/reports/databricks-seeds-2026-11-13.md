# Databricks managed seeds — 2026-11-13

This report characterizes ordinary managed seeds for #139 using hosted capture,
exact public replay and real pinned-client Rust acceptance. All final integration
checks pass. No general Databricks support claim is made. The known managed shallow-clone
seed rebuild, copied FULL-table rebuild, snapshot deletion and #140 freshness
Safety blockers remain unchanged; unresolved #158 logical-target and #159
default-namespace diagnostics remain incomplete too.

## Hosted characterization

The pinned dbt 2.0.11 campaign completed 40 invocations and 121 certified RPCs:
20 validation, 41 telemetry, ten SubmitValues, ten ordinary SQL submits, ten
speculative submits, ten successful confirmations and 20 first-call complete
Explain responses. The complete unfiltered fixture is
`golden/fixtures/databricks_managed_seeds.jsonl`, SHA-256
`3f8d4f47748353efc7d44ac24e5254b399d3a6026247da135b33561d5a9ae33d`.
No Databricks replay gap, exclusion or unknown baseline is authorized.

Each seed step is followed by unchanged downstream SQL selecting its complete
rows. Initial load, content change, INT→BIGINT column configuration, comma→pipe
delimiter and false→true `quote_columns` each build and confirm both relations;
each intervening unchanged seed and downstream control must SKIP. Actual
SubmitValues carries `column_types`, `delimiter` and `quote_columns` in semantic
extras. The column configuration change retains the same values hash; delimiter
changes the CSV hash; quoting changes extras while retaining the CSV hash.
Downstream Explain identifies each of the four seed changes as an upstream
query change, alongside its actual changed freshness epoch.

Each step independently checks ordered column names/types, the full typed row
multiset (including duplicate and NULL multiplicities), MANAGED relation kind
and Delta provider. An isolated State-disabled dbt project executes the same
controlled CSV/config operations in a separate leased schema. Neither expected
rows nor schemas depend on State's fingerprints, response or actual output.
The INT→BIGINT cell requires the wider schema even when values remain equal.

The Explain census contains five FULL/SKIP, five FULL/EXECUTE, five VALUES/SKIP
and five VALUES/EXECUTE trees, no empty trees, INFO, unspecified or unmapped
vocabulary. Production-router replay reproduces all 121 RPCs, including exact Explain trees,
with 101 ordinary assertions and 20 Explain comparisons; unknown baselines,
known gaps and partial-history exemptions are all zero.

## Isolation, provenance and retained evidence

Archive: `/home/maxfirman/.local/state/dbt-state-rs/databricks-139-20261113`.
`hosted-1/summary.json`, `capture-certification.json`, operation manifest,
ordered operation log, exact source runner, CLI output, request/response/status
capture, independent physical reads and before/after inventory are retained.
`artifact-digests.json` pins the archived files. A setup-only failure from Python
without PyYAML is recorded separately; the successful invocation used
`uv run --with pyyaml python`.

All writes use the exact four fresh `s_20261113a139a1_` relations across the
leased `cdo_demo_db.ds_20261112a134ef_ref` and
`cdo_demo_db.ds_20261112e134ef_ref` schemas. No schemas were created. Before
inventory counted 66 prod and 53 reference relations; after counted 71 and 55,
including concurrent peer additions. Every before object was preserved; this
campaign creates two relations per schema and does not claim ownership of peer
additions. The seed and downstream are ordinary managed tables, never shallow
clones. Automatic Databricks seed Result copy remains disabled.

Both hosted and Rust report DBSQL `2026.44` with the same observed Unity
Catalog/serverless build hashes; full `current_version()` results are retained
in each campaign’s `warehouse.json`. This is the tested environment, not a
promise for other clients, type configurations or deployment variants.

The pinned client SHA-256 is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The runner retains private copies and digests of the immutable #155 recording
proxy and #136 protocol probe. Effective hosted upstream is explicitly recorded.
The discovery runner admitted configuration decisions before freezing the
observed expectations; the final runner requires all observed builds and reuses
and adds a State-disabled manifest/allowlist preflight. Its snapshot and digest
are separate from the unchanged capture-generating runner.

## Current-source acceptance — 2026-11-14

The seed worktree was updated to integration `a2330b0` before validation. A fresh
independent Cargo target (`/home/maxfirman/.cache/dbt-state-rs/target-139`) builds
with debug symbols and incremental compilation disabled, four build jobs and
two test threads. The original shared target is untouched. Exact public replay
passes all 12 conformance tests and all 100 causally classified corpora, including
the complete managed-seed fixture, in 183.30 seconds of test execution.

The final real-dbt Rust campaign passes 41 invocations in 278.88 seconds: one
State-disabled manifest/allowlist preflight plus the same 40 managed/reference
operations as hosted. Its certified capture contains 121 RPCs, SHA-256
`0138ef944f56a765728132e62d36a3a6813051b8e126688eb1a3dd06f61aeae7`.
All ten builds are confirmed, ten eligible unchanged seed/downstream decisions
must SKIP, and every one of 20 first-call Explain responses contains its complete
tree. Every step passes the full independent physical contract and isolated
State-disabled reference comparison. No client polling/retry override is used.

Evidence is retained in archive `rust-final-1`: exact run prefix
`s_20261114568a7b_`, complete operations and source snapshots, actual local
upstream, requests/responses/statuses, certification, SQL results, schema and
properties, and inventories. The production router runs in the integration
test's owned Postgres schema. Before inventory was 83 prod / 59 reference;
after is exactly 85 / 61, and every preexisting object is preserved. No Databricks schemas are created
and no prior warehouse relations are mutated. `current-source-binaries` retains
the actual test executable containing the production router, source hashes,
build settings, and its `a2330b0` base; the campaign separately retains actual
proxy/probe copies and digests. The source snapshot is explicitly dirty because
it includes the new acceptance tests and fixture.

No Rust production behavior or fingerprint semantics need changing: the shared
seed identity, selection and producer machinery already matches this capture.
The change freezes that behavior at the agreed public and end-to-end seams.
No identity version changes or reinterpretation of existing history are made.

The credential-free Python checks pass 23 tests, including precise faults for
stale contents, lost duplicates, changed NULL, wrong type/name/order,
shallow-clone relation kind and wrong provider. The complete current census has
644 trees across 111 tracked fixtures, no empty trees or vocabulary findings;
this refresh includes lifecycle fixtures added since the earlier catalog count.
The final credential-free workspace suite passes 629 tests with zero failures
and 35 expected opt-in ignores. Strict workspace/all-targets/fuzz-feature Clippy
passes with warnings denied; formatting and `git diff --check` pass. The latest
integration tip was merged before completion and remains `a2330b0`.

The just-completed #158 gate supplies 25 successful actual fixed/generated
DuckDB cases, including seed reuse/change propagation, at identical production
source. This is reused evidence, **not a new #139 DuckDB run**. The accepted
source commit `df8461a` and integration `a2330b0` have no server/proto diff; this
seed change adds no production/proto diff either. The retained #158 log SHA-256 is
`cadecda7598fb83aae3d8f2e3ce33b8c2d369c2a77b18b3ca0c9b4bf5be7bd2e`.
`reused-duckdb-evidence.json` records that source comparison and the original
25-case timing/results. No separate slow rerun was performed solely for the
seed runner, fixture and documentation additions.

The companion JSON report pins each final log and capture hash. The ordinary
managed-seed slice is complete for the specified matrix; cross-target copied
seed rebuild and the broader #132 limitations remain required blockers.

## Preparatory public tracer — 2026-11-14

After updating the seed worktree to integration `a67fd13`, a decision-only
tracer passed 15 public calls using the immutable #159 final server and probe:
ten captured seed submits (five builds and five required unchanged SKIPs),
with five successfully correlated confirmations. Every seed request field is
retained verbatim; confirmation request IDs are remapped to the returned local
opaque IDs. The tracer runs against a fresh owned Postgres schema and removes
it after stopping its owned server. No cloud call or Cargo command was run.

Evidence is retained in archive `public-tracer-final159`: ordered full inputs
and outputs, source driver, server log, summary, digests and original binary
provenance. The server SHA-256 is
`40371798b1aa8066b9988bdb096d278571a7072b14864302c0d414371f0ae5d7`;
the probe SHA-256 is
`e3d87cd7d852f25664fbf9ebafd88efe8ccded4f642b9ea551cdf409fbf9cd5c`.
Both match the inspected #159 `bin-final/provenance.json`, which explicitly
records its dirty source snapshot based on `837bab5`; this is not presented as
a freshly built integration binary. Its documented #159 incomplete scope
remains unchanged.

The standalone server uses the System clock. That tracer asserted decision
variants and successful confirmation correlation; it did not establish exact
full response/status/metadata replay, Explain equivalence, or warehouse
correctness against Rust. Full compiled replay and real-dbt/Rust physical
acceptance were still required at that preparatory stage; the current-source
acceptance above now supplies both. All broader capability and Safety blockers
remain unchanged.
