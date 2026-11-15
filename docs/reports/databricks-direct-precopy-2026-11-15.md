# Databricks direct pre-copy: narrow offline implementation, 2026-11-15

This implements the retained native MERGE/check-snapshot direct RegisterClone
path and narrow adoption. #152 remains open. No new warehouse writes, live dbt
runs, or local DuckDB execution were performed for this slice.

Primary source: the preserved #134 archive
`~/.local/state/dbt-state-rs/databricks-134-20261112/zero-lag/golden/golden_20261112T014013.365Z_6728e5f8c1bc48a4884fd9d720a16cd5.jsonl`
(SHA256 `6a7b16a989da7fb8ae628b41927ddc3c712de96794b0459ec09f1bfeec7730c8`).
The focused test fixture copies lines4/6/10/11/12/17/22/24 and
29/31/35/36/37/42/47/49 byte-for-byte. These contain each source Submit/Confirm,
RegisterClone/Confirm, adoption/repeat and changed-input Submit/Confirm. It is a
causal test input under `crates/harness/tests/fixtures`, not a newly accepted
whole hosted conformance corpus. Only opaque transport IDs are remapped.

## Implemented and checked

- Exact hosted RegisterClone response for both native node kinds: both mirrors,
  OR REPLACE SHALLOW CLONE DDL including whitespace, NULL epoch/runtime and empty
  decision ID. Native requests have NULL source type/properties and omit format.
- Latest physical source selection before qualification, scoped by organization,
  warehouse family, nonblank namespace and physical relation. Exact source epoch,
  full UID and node kind must match. Pending, copied, unknown or incompatible
  sources cannot expose older eligible history.
- Version1 immutable direct-copy provenance, independent of automatic namespace
  identity. Only the observed backtick-qualified single-input SELECT-star subset
  without query dependencies/templates is admitted. It binds rendered identity,
  physical input, full UID and client body/config/macro/contract evidence, retaining
  the actual default catalog/schema. Unsupported SQL stays BUILD after copying.
- Copy publication freezes source input epoch/producer snapshots and inherits
  runtime. Native adoption/repeat/changed-input responses match completely after
  ID remapping; a changed-input build can confirm and reuse its own fresh result.
- Source input changes before the copy cannot be erased by copy time. Changed
  producers still rebuild with large lag tolerance. Changed SQL/config/full UID,
  physical input, target epoch, unknown input epoch and template mode rebuild.
- Confirm atomically removes adoption evidence if the latest scoped source ID or
  epoch differs from registration, or any source write remains pending.
  Registration also refuses a source with any pending write. Failed cloning removes it. Legacy NULL,
  malformed payloads and unknown versions cannot establish direct equivalence.
  Existing Snowflake/DuckDB namespace-copy semantics remain separate.

DDL eligibility does not infer MANAGED=Delta. Databricks itself validates the
attempted shallow-copy operation; successful confirmation is still required.
Official [CREATE TABLE CLONE documentation](https://docs.databricks.com/aws/en/sql/language-manual/delta-clone)
distinguishes supported formats, and
[Parquet/foreign Iceberg conversion](https://docs.databricks.com/aws/en/ingestion/data-migration/clone-parquet)
means DDL success is not proof that the historical source provider was Delta.
Explicit incompatible source types and retention properties remain unsupported.

## Validation and remaining work

Single-reviewer follow-up found and fixed two safety gaps: pending source writes
are barriers regardless of submission order (an earlier submit can finish later),
and template mode without usable signatures cannot establish direct provenance.
Two new public regressions failed against the original compiled server with the
exact unwanted grant/SKIP, establishing the RED baseline for these corrections.
The final-source direct-copy suite passes all ten tests (8.94s). The reviewer
ran `cargo test --workspace -- --skip conformance` on final source: 709 passed,
zero failed, 56 ignored, one whole-corpus replay filtered out. All four crates
were rebuilt; no ignored real-dbt cases ran. Formatting and diff checks pass.

The original direct case was RED with UNIMPLEMENTED before history; the adoption
case then was RED with BUILD instead of the native SKIP. Eight focused public
gRPC/Postgres tests now pass (11.14s). Existing hosted corpus replay passes all
19 tests/131 corpora (255.04s); this executable predates only the final
Databricks-specific pending-source strengthening, which the final focused tests
exercise. Existing corpora contain no Databricks RegisterClone calls. Strict
server/harness Clippy with all targets and fuzz enabled, formatting and diff
whitespace checks pass. The final broader Rust gate is recorded above. Original commands, source/file/tool hashes and
replay/lint outcomes are in the compact external
`/tmp/dbt-state-132-context/ticket152-implementation-checkpoint.md` manifest.

No fresh independent warehouse Reference executions or new physical acceptance
are claimed. An external/unrecorded source change during unversioned SHALLOW
CLONE is not observable from these requests: Confirm and adoption contain no
source re-observation. The State-recorded registration/confirmation race is
covered, but physical concurrency/removal/modification needs further evidence
or a stronger client/warehouse contract. General SQL/dependencies/templates,
source chains, lifecycle breadth and Explain acceptance remain open. Automatic
submit Result copy (#153) stays disabled; #154 and copied FULL/seed replacement
and snapshot new_record blockers are unaffected. Local DuckDB validation is
explicitly deferred until after landing by the maintainer.
