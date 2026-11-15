# Databricks old SQL and seed history: bounded offline upgrade slice

Issue #160 remains open. This slice restores complete old self-hosted State
rows and verifies current public gRPC behavior. It does not supply Databricks
Result copy evidence, close the feature graph, or establish hosted/private
State-database migration behavior. No warehouse, Cloud, authentication, GitHub
or native dbt calls were made for this slice.

## Evidence

[`databricks_legacy_160_provenance.json`](../../golden/upgrade/databricks_legacy_160_provenance.json)
pins the source, executables, historical Cargo.lock, immutable fixture/capture
checksums, zero-based source entry indexes and transport-ID mappings. Existing
rendered/template/Record SQL exports are reused byte-for-byte. They were made
by historical source `302156014b182df2f545a4c9bc3d128926bdf11c`, server SHA256
`52afbea7916f0ff5bd32fb77fd78f229811ff4d4cfeb19cb6ce7ee5d4dc7ab15`, from
actual retained dbt 2.0.11 hosted request bodies. Those bodies retain original
SQL, configs, namespace, UID, inputs and epochs. The original binary manifest
pins source and executable; it does not record the original compiler version
or build command. No reproducible-binary claim is added here.

[`databricks_legacy_freeze.py`](../../experiments/databricks_legacy_freeze.py)
reuses that verified historical executable and its pinned probe. Run with
`python experiments/databricks_legacy_freeze.py` only when those exact retained
binaries are available. It uses a fresh `legacy160_freeze_<uuid>` schema on
local Postgres port 55441, sets that schema alone as server search_path, and
terminates the server before dropping only that owned schema. It never builds
an old server or computes a fingerprint itself. The frozen runner SHA and
exact process commands/environment settings are in the seed fixture. Probe
transport metadata is not replayed; organization defaults to `local`.

The runner replays all ten SubmitValues requests and their five seed confirms
from `databricks_managed_seeds.jsonl`, preserving their original order and
bodies except the explicitly mapped ConfirmExecution request IDs. It exports
all 34 old columns immediately after each BUILD and its confirmation. The
fixture retains the complete fifteen local operations, original hosted-ID to
local-ID mapping, five pending rows, five confirmed rows and five unchanged
request bodies. Source SubmitValues/Confirm/unchanged entry indexes are
2/3/16, 27/28/40, 51/52/64, 75/76/88 and 99/100/112. These local observations
are not relabeled as hosted expectations or hosted persisted rows.

## Import and public behavior

The test-only `legacy::import_complete_execution` rejects incomplete or
unexpected old exports. It inserts every semantic column, preserving original
creation/confirmation times, hashes, values, input JSON, outcomes, runtime,
UID and context. It allocates a fresh database surrogate ID; these exports
contain no execution FK IDs requiring further remapping. Transport UUIDs stay
unchanged. Only `id` and the generated `warehouse_family`/`physical_target_key`
columns are omitted. Original generated values remain in the fixtures; current
migrations compute current lookup keys. The later nullable `node_macros_hash`
column stays NULL, with no backfill. The importer verifies all 31 imported
fields against their original export after insertion and never deletes rows.
The older `FrozenExecution` helper is explicitly documented as selected-field
setup with synthetic confirmation time, rather than a verbatim row importer.

`databricks_legacy_history` tests use the production router:

- Genuine rendered/template v3 pending and confirmed SQL rows, plus the genuine
  confirmed RecordExecutions row, each cold-miss after upgrade. Pending rows
  receive the original frozen confirmation through current gRPC. Each ordinary
  miss then BUILDs, Confirms a fresh known outcome, and SKIPs with its own new
  runtime. BUILD and SKIP Explain messages are checked.
- All five old seed states reuse unchanged, both after importing the original
  confirmed row and after confirming its genuine old pending row. The seed-v2
  identity was already compatible; old history is not invalidated wholesale.
  Actual captured value, column-type, delimiter and quote_columns changes
  rebuild, then reuse their own fresh confirmed result/runtime with Explain.
- Compatible seed positives are paired with organization, absent/empty/other
  namespace and other warehouse-family negative controls before changing the
  baseline history.

Current SQL/seed observations use public BUILD responses and gRPC
confirmations of explicit test outcomes; they do not execute warehouse SQL.
No old semantic field is rewritten to a current hash, time or protocol value.
Original old projection behavior after Record remains separately documented
as BUILD versus hosted SKIP. Existing lifecycle/isolation/identity/fault suites
retain latest-physical, unknown-epoch, UID/cached-result and corruption floors;
this slice does not require unknown-epoch faults to heal.

## Remaining work

Databricks Result copy/adoption prerequisites are unavailable. No Snowflake
copy row is reused as Databricks evidence. Full #160 copy coverage, remaining
legacy-version/fault/generated matrices, and the parent native/hosted release
contract remain open. The bounded fixture tests add no production seam,
fingerprint change, migration, capability enablement or support promotion.

## Validation

With `CARGO_TARGET_DIR=/home/maxfirman/.cache/dbt-state-target160`, dev/test
debug info disabled, incremental compilation disabled, three build jobs and
two test threads:

- Focused TDD: the two public tests first failed at the missing complete-row
  importer; both passed after implementing it. Final workspace execution,
  including the added scope controls, passed both tests in 14.42 seconds.
- `cargo test --workspace`: 689 passed, zero failed, 52 live tests ignored;
  includes all 19 conformance tests (242.45 seconds), all retained corpora,
  existing SQL/seed/copy/isolation/unknown-epoch/cached-result regressions, and
  all 310 server library tests.
- `cargo clippy --workspace --all-targets --features fuzz -- -D warnings`:
  passed in 67 seconds. Fuzz tooling was compiled, never executed.
- `cargo fmt --all --check`, Python syntax compilation and fixture/primary
  capture/runner checksum integrity checks: passed.

Production server, migrations, protocol, harness production code, UI and
Cargo dependency files are byte-identical to canonical `a54f4da`. No new live
DuckDB, Snowflake or Databricks acceptance run is claimed for this test-only
slice.
