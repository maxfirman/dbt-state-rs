# Databricks experimental preview acceptance

This report completes the bounded preview publication in #162 under the
maintainer's 2026-11-15 decision to accept **documented conservative differences**.
The [feature matrix](databricks-preview.md) defines the tested cases; a closed
implementation ticket does not establish every configuration of its feature.
The full zero-known-mismatch claim in [ADR 0009](adr/0009-databricks-scoped-hosted-conformance.md)
remains unfulfilled. Unsupported capabilities are excluded, not counted as passes.

## Environment and configuration

Evidence was collected on 2026-11-12 through 2026-11-15 with dbt **2.0.11**,
SHA-256 `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`,
on the tested Databricks Free Edition serverless workspace, Unity Catalog and
managed Delta. The initial capability survey observed DBSQL 2026.44; later
campaigns do not establish that every execution used that same runtime version.
Other clients, deployments and storage formats remain unverified.

Use the normal [server setup](development.md). Databricks copy is enabled by
default for the bounded shared-analysis subset; the two historical experimental
server switches have been removed. [Current client controls and accepted limits](databricks-copy-preview.md)
replace the original no-copy preview configuration. Original 2026-11-15 copy
campaigns explicitly enabled their then-default-off server paths; their source
attribution and captured assertions remain unchanged.

## Acceptance evidence and attribution

The feature matrix links the retained hosted captures, public gRPC replay,
real dbt/Rust-server execution and independent Reference executions. Campaigns
check complete typed bags, NULLs/duplicates, ordered schemas and relevant
Delta/Unity Catalog history and properties; incremental references preserve
cumulative history, and snapshot references preserve full SCD history. Explain,
errors and negative controls remain part of the specific linked evidence.

Native campaigns were completed as separate feature slices on their recorded
source revisions. Subsequent replay/guard corrections are tested separately;
this is **not one new live campaign on a uniform final revision**. In particular:

- Cached upstream tests (#143) ran natively before the BUILD Explain display
  correction; the corrected display is verified by complete public replay,
  not an invented second native campaign.
- Automatic submit-copy (#153) ran natively on `301de04`. The later ordinary
  same-target identity guard has focused regressions; that fixed-metadata native
  campaign did not traverse the changed UID/config branch. Its full 91-RPC
  native history is replayed on the final library with all 20 persisted clocks.
- The exact hosted submit-copy corpus has 75 RPCs. A separately completed
  boundary phase retains the full 91-RPC characterization, including two known
  display/rewriting differences; that boundary history is not represented as
  an exact hosted conformance corpus.
- [Copy recovery](databricks-copy-recovery.md) (#154) retains the complete hosted
  151-RPC history as boundary
  characterization. All calls are dispatched; nine full local outputs are pinned
  explicitly (four BUILD diagnostics, three Explain outputs, a stale-source refusal
  and the resulting orphan-confirmation NotFound/no-publication). Other outputs
  must match hosted. This is not an exact hosted replay claim.
  The native campaign passed 31 CLI invocations (25 managed attempts, including
  the expected FULL failure, plus six compiles) and 24 independent native references.
  Its whole 151-RPC capture replays exactly with all 25 persisted decision clocks.
  It ran recorded dirty source based on `7600927`; production, driver and native-runner/support bytes
  match frozen source `25f7577`, as distinct from a final-commit binary claim.

Native requests used the real server clock. Supplementary replay uses actual
persisted decision clocks saved before isolated test-schema removal, without
altering the raw captures. Existing exact fixture assertions, complete causal
history and unknown-baseline checks remain intact. The historical evidence
[archive and hash index](databricks-evidence.tsv) preserves earlier diagnostics;
strict-era INCOMPLETE headings are not rewritten as passing full conformance.

Live mutations were confined to run-owned objects inside `cdo_demo_db`.
Earlier campaigns' fresh run-owned containers are documented in their feature
evidence. The final bounded copy campaigns (#152–#154) use existing schemas,
ownership/ID checks, inventories and exact plans. Their intentional copy-failure
DROP/UPDATE operations apply only to that campaign's fresh result objects with
saved beforeimages and immediate identity checks. Older graphs remain preserved.
No credentials or raw profiles are published.

## Final implementation and CI receipts

Implementation landed at `1310918bb8077c657f16f781c8e7bc8114de9cff` in
[PR #179](https://github.com/maxfirman/dbt-state-rs/pull/179).
[Source-head CI](https://github.com/maxfirman/dbt-state-rs/actions/runs/43760960353)
passed the full workspace and strict formatting/Clippy gates at `25f7577`;
automated Codex review completed with no findings, alongside independent
Standards/Spec review with no remaining bounded-scope issue.
The [merged implementation main run](https://github.com/maxfirman/dbt-state-rs/actions/runs/43761798138)
records workspace, lint and real-dbt DuckDB acceptance on that implementation.
This publication requires its successful completion and the publication PR's
normal checks before merge. Source and evidence attribution above remains
per-campaign, rather than implying a new uniform final-head cloud run.

## Regression commands

The implementation PRs run the credential-free workspace and strict lint gates:

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
```

These include retained Snowflake/DuckDB protocol and property regressions,
mandatory whole Databricks corpora and fixed/generated safety coverage. They
are not fresh live Snowflake characterization. Individual evidence pages name
the focused commands and explicit opt-in native drivers; running ordinary
`cargo test` does not launch cloud acceptance.

Slow local real-dbt DuckDB acceptance was deferred by maintainer request.
During implementation only the Databricks integration PR branches skipped that
CI step; main, other PRs and manual/scheduled runs retained normal coverage.
This final publication removes that temporary branch exception, restoring the
normal PR gate as the effort ends. The regular gate executes:

```sh
./scripts/duckdb-ci.sh --jobs 4
```

Its fixed cases plus four seeds/four generated episodes provide actual
post-merge DuckDB acceptance without another local campaign. The larger
scheduled/manual generated run is a separate gate, not claimed by a main run.

## Compatibility limits

- Cached data tests retain full-UID identity. Namespace fallback and targetless
  logical reuse remain conservative. Ordinary model/snapshot reuse follows
  scoped physical identity. Extra BUILD/undecided responses are accepted limits.
- Unknown/conflicting upstream epochs cannot prove freshness, and changed
  producers rebuild. Unsafe hosted stale reuse is not reproduced.
- Incremental claims cover the exact settings and cumulative cases in the
  matrix. Multi-day microbatch configured predicates retain a pinned-client
  mutation defect; composite delete+insert keys and broader settings are unverified.
- Snapshot claims use explicit source timestamps with `ignore`. Generated clocks,
  invalidate modes and `new_record` NULL-business-row behavior remain unsupported
  or unverified, as detailed in the snapshot evidence.
- A physically modified direct-copy source is conservatively refused where
  hosted grants a copy before an ordinary MERGE repair. This is an intentional
  refusal followed by ordinary execution, rather than exact hosted grant parity;
  unsafe source eligibility is not expanded to reproduce that optimization.
- Copy is enabled by default with accepted limitations. Unversioned SHALLOW CLONE
  cannot expose every external source race at confirmation. Quiescent, serialized
  experiments do not prove general concurrency safety or copy chains.
- Ordinary FULL/seed replacement over an existing shallow-clone target can fail
  in the tested client/warehouse even with State disabled. A successful fallback
  after an absent-target copy failure does not remove that separate limitation.
- The RAND control returns hosted's `transformed_nodes_by_query` shape with a
  locally rendered value and prints it in Explain (Volatile evaluation, #192);
  it still BUILDs over the original SQL. One crafted unknown-input Explain
  differs by one second because the captured client submit and UUIDv7 decision
  cross a second boundary. That boundary grants no copy, and no timestamp or
  response field is masked.

A wrong result attributable to this server remains a defect. Conservative
completion does not relax org/warehouse/namespace isolation, latest physical
history selection, complete cached test results or upstream epoch/producer rules.
