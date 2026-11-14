# Databricks identifier evidence — 2026-11-13

This report covers #136 identifier and resolution behavior under #132's pinned
dbt 2.0.11, serverless Unity Catalog and managed Delta scope. It establishes no
general Databricks support claim. The copied-target rebuild and snapshot
new_record blockers from #134 and unresolved freshness Safety divergences from
#140 remain unchanged.

## Hosted characterization

The complete hosted campaign succeeded with 45 dbt invocations and 182 certified
RPCs: 22 ValidateClientVersion, 46 SubmitTelemetryBatch, 22 speculative submits,
44 ordinary submits, four confirmations and 44 complete Explain responses.
The fixture SHA-256 is
`5a058c42142447f8a80f24805625e9faae9584173d1089fe65f397aaa25021d7`.
Actual client cases include qualified, schema-qualified and unqualified SQL,
backticks, case changes in each component, same-basename source collisions,
and source-view query dependencies. Eighteen actual reuse decisions are required;
four actual builds are confirmed. Twenty-two crafted boundary submits are
separately labeled and never confirm invented epochs.

Independent Statement Execution reads verify that the same unqualified SQL reads
different complete rows under the two owned schema contexts. Every actual model
result is compared with State-disabled dbt, an independent SELECT, and literal
complete typed row multisets, ordered schema and MANAGED/Delta properties.
All writes use new run-prefixed relations in the guarded demo schema lease;
preexisting objects are retained. The pinned executable digest is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.

## Implementation and regressions

[Identifier rules](../databricks-identifiers.md) describe the bounded SQL and
physical-name forms. The original exact-SQL implementation fails complete hosted
replay (`replay-red.log`). Frozen pre-change hash bytes prove version invalidation;
confirmed and late-confirmed alias histories with equal epochs prove latest-row
selection across upgrade. Public regressions also exercise producer lookup,
organization/family/namespace/schema/catalog separation, unsupported identifiers
and dialect whitespace, RecordExecutions and original-spelling deferral output.
Coverage includes both frozen hash fixtures (ordinary input query and literal
SQL), confirmed-before-upgrade and pending-confirmed-after-upgrade paths,
latest frozen alias versus eligible canonical history at equal epochs, aliased
producer changes, and malformed/Unicode/escaped/quoted-dot names retaining exact
scope. The additive projection's rationale is [ADR 0010](../adr/0010-preserve-raw-identifiers-with-physical-history-keys.md).

Full Explain compares exact trees without a Databricks gap, exclusion, comparator
mask or unknown baseline. The shared test-only UUIDv7 clock is bounded by each
captured request/response interval. Production decision IDs remain opaque.

The physical-key review confirms migration 0020 only adds a generated column
and index: original `executions.target_table` and fingerprints are never rewritten.
Submit and RecordExecutions persistence bind the original pre-ingress target;
ExecutionRow canonicalization is in memory with `raw_target_table` retained.
Selector/deferral direct SQL and CopyCandidate decoding retain raw spelling.
After merging #155, confirmed deferral reads persisted `executions.target_table`
directly. Migration 0021 adds named deferral scope evidence without modifying
that raw target or the physical-key projection. The final suite validates both
migrations and both tickets' public regressions together.
The public `new_sql_builds_preserve_the_original_target_spelling_for_later_deferral`
regression confirms uppercase original spelling through a confirmed build.

## Retained attempts

Durable root:
`~/.local/state/dbt-state-rs/databricks-136-20261112/`.
`hosted/` holds the accepted hosted evidence; `exploratory-hosted/` and
`context-diagnostics/` retain earlier investigations. `rust/` is the failed first
Rust attempt: physical and decision checks passed, but the first Explain response
was empty. `rust-2/` and `rust-live-2.log` are interrupted evidence, explicitly
unaccepted because that attempt used an unapproved Explain polling override;
`rust-2-interruption.json` records the interruption. No artifacts were removed.
The final campaign uses inherited strict Baseline.explain, exactly one request per
check, after the independently characterized server capture-visibility fix.

## Final validation

`cargo clippy --workspace --all-targets --features fuzz -- -D warnings` passed
(`clippy-exclusive-final.log`, repeated after merging #155). `cargo fmt --all --check` and `git diff --check` passed.
Credential-free identifier/baseline/survey/capture-integrity Python checks passed
22 tests (`python-final.log`); the combined #155 check passed 25 tests
(`python-combined-final.log`).

The strict Rust campaign passed all 45 dbt invocations in 468.13 seconds
(`rust-live-3.log`, artifacts `rust-3/`): 179 certified RPCs, including 44
ordinary submits, four confirmations and exactly 44 complete first-call Explain
responses. It requires 18 actual reuses and verifies four actual builds, with
all physical/reference checks passing. Capture SHA-256:
`2746e537fe986d99dac82b2e0dfcd827f4268bb69d3f4168ed5571575e7d404f`.
The client emitted different optional speculative submission counts (17 Rust,
22 hosted); both complete captures and telemetry remain retained. The ordinary
44-submit lifecycle and all required decisions match.

`compare-phases-strict.py` passed (`cross-phase-strict.log`): 22 actual steps,
44 complete actual/reference physical results, 22 crafted boundary decisions,
and 44 Explain structures: decision enum, decision description and recursively
each line's text, metadata and children. Only owned run prefixes are mapped;
`last updated:` lines carry a placeholder in this diagnostic because separate
warehouse histories have distinct source/target epochs. Opaque request/decision
IDs and generated owned relation names also differ between those histories.
Every original response remains retained. Exact hosted fixture replay checks the
complete correlated response and every Explain character at its captured bounded
decision clock, including physical timestamps; no diagnostic substitutions are
used there. The cross-phase diagnostic does not replace
the exact public replay gate or add a comparison exception. It requires exactly
44 Explain calls and never filters empty responses.

`workspace-final-2.log` is explicitly unaccepted: the shared Cargo target was
released after compilation while tests remained unlaunched, allowing another
branch to replace later executables. The owned command was stopped, and
`workspace-final-2-unaccepted.json` records the provenance issue. This does not
affect the stable copied live binaries or Clippy completed before release.
The final exclusive full workspace command runs after merging integration
`cd79d3c5b3392924ae7dba194877780907ddd545` (#155), including 78 corpora.
`workspace-exclusive-final.log` passed every harness/public suite and exact
78-corpus replay, then failed one stale warehouse unit assertion expecting the
pre-change shared Databricks SQL tag. The existing assertion now pins each
warehouse's independent literal version contract; all 309 server-library tests
pass (`serverlib-version-final.log`). The final exclusive full workspace rerun passed: **605 passed, zero failed,
32 explicitly ignored** across 77 result blocks (`workspace-exclusive-final-2.log`).
It includes all 12 conformance tests and 78 corpora, all 11 ordinary identifier
acceptance tests, confirmed-deferral coverage and both additive migrations.
Strict combined workspace/all-targets/fuzz Clippy passed afterward. Formatting
and whitespace checks passed.

The long opt-in DuckDB real-client acceptance campaign was not rerun for this
slice. Ordinary workspace tests preserve existing Snowflake/DuckDB contracts;
broader #132 release validation and the named blockers remain the integration
owner's responsibility. No issue closure, PR readiness or full-scope completion
is implied.

Machine-readable accepted capture/log digests are in
[the artifact summary](databricks-identifiers-2026-11-13.json). The durable
`artifact-index.json` contains every retained evidence file's size and SHA-256,
including rejected attempts. `final-command.json` records the exact live command
without credentials. `binary-build-final.jsonl` and `validated-binaries.json`
record the stable server/probe build provenance for future frozen-history tests.
