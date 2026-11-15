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

## Persisted-history upgrade coverage (#160, 2026-11-15)

`golden/upgrade/databricks_copy_v1_45e94c3.json` freezes six complete
36-column `row_to_json(executions)` exports from clean historical commit
`45e94c3e33a6fccfbaf79efb1a143b794f0ddb33`: each native MERGE/check-snapshot
source, pending copy and confirmed copy. These are historical **local-server**
rows produced by replaying the causal native requests above, not hosted stored
rows or a new Warehouse execution. Before direct-copy support in `20d7685`,
Databricks RegisterClone refused this protocol; no fictitious pre-support copy
has been constructed. The fixture includes the original rendered fingerprints,
versioned copy provenance, source context/full UID, input epochs, timestamps,
NULLs and source links. Its input `producer: null` genuinely means no recorded
producer; it is preserved, not replaced with a current identity or omitted.

The small exporter records source commit, executable digests, pinned toolchain,
build command, native-fixture digest and its own digest. Reproduce after building
the historical checkout with the fixture's command, then run from this checkout:

```bash
python3 golden/upgrade/freeze_databricks_copy.py \
  --historical-source /tmp/dbt-state-160-historical \
  --bin-dir /home/maxfirman/.cache/dbt-state-152-target/debug
```

This uses only an isolated Postgres schema and a local server; it sends no
Warehouse or hosted requests. Do not overwrite the historical executables with
a current build before exporting. Original historical public calls establish
SKIP / SKIP / changed-input BUILD for both node kinds. Current upgrade tests
import every semantic column, allocating fresh surrogate IDs and remapping
only `direct_copy_provenance.source_execution_id` to the restored source.
A prior current-protocol allocation forces IDs to differ; fingerprints, producer
identities, epochs, context and transport UUIDs remain verbatim. Generated
lookup columns are recomputed by current migrations.

`databricks_legacy_history` now covers these copy histories alongside the existing
genuine v3 SQL Submit/Confirm/Record and v2 seed histories. Both confirmed copies
and old pending copies confirmed after upgrade retain compatible reuse with new
copy grants OFF, reject changed SQL/config/full UID/org/namespace/input evidence,
and preserve source input freshness even for a change before copy time. Missing
epoch publication remains pending and permits a valid retry; duplicate outcomes,
failed copying, pending source writes and a newer source are handled conservatively.
Explicitly labelled storage faults cover NULL/malformed/unknown-version/unknown
identity/missing-source/missing-UID/missing-producer evidence. They are not genuine
historical output. A damaged latest copy hides valid older history at the same
epoch, rebuilds, then reuses its newly confirmed current result. Explain responses
are checked for the observed builds and skips.

Validation: all six `databricks_legacy_history` tests pass (30.07s, two test
threads); formatting and diff whitespace checks pass. No production policy
changed. This completes the bounded persisted-history
upgrade matrix for #160; it does not establish fresh physical copy acceptance
for #152 or full Databricks support for #162. Existing physical copy/concurrency
limitations above remain open.
