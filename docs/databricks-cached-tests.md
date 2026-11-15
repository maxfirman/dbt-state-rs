# Databricks cached data-test outcomes

The bounded dbt 2.0.11 serverless Unity Catalog/managed Delta coverage for #143
includes the existing source-less passing/failing, configuration and template
histories, plus a genuine upstream data-change history captured on 2026-11-15.
This completes #143's ordinary/speculative cached-outcome scope under the
maintainer's 2026-11-15 decision to accept documented conservative differences.
The known #138 cross-full-UID true-template difference remains: hosted can reuse
another UID's result while Rust returns UNDECIDED/BUILD and preserves its full-UID
isolation floor. This is an accepted conservative difference, not exact hosted
parity or a full Databricks support claim. Audit objects are separately #144.

Existing complete public histories cover [source-less pass/fail](../golden/fixtures/databricks_cached_test_results.jsonl),
[configuration/threshold changes](../golden/fixtures/databricks_cached_test_thresholds.jsonl)
and [true-template cached failures](../golden/fixtures/databricks_cached_test_template_failure.jsonl).
Their native hosted/Rust Reference executions and limitations remain in the
[archived reports](databricks-preview.md#evidence-and-maintenance).

The new workload is one singular test, `store_failures: false`, rendered-code
comparison, zero Lag tolerance, over one fresh owned Delta source. The SQL stays
unchanged throughout: `SELECT id, v FROM <source> WHERE v < 0`. Initial source
rows are `(1,2), (1,2), (2,NULL)`. Only `v` for `id=1` changes to `-2`, then `2`.
The test creates no audit relation and invokes no Result copy.

| Observation | Ordinary decision | Failures | Native status / exit |
| --- | --- | --- | --- |
| First pass | BUILD | 0 | pass / 0 |
| Unchanged pass | SKIP | 0 | reused / 0 |
| Upstream data changed | BUILD | 2 | fail / 1 |
| Unchanged failure | SKIP | 2 | fail / 1 |
| Upstream data recovered | BUILD | 0 | pass / 0 |
| Unchanged recovery | SKIP | 0 | reused / 0 |

Hosted and Rust match all six decision/result/status observations. Every ordinary request carries
the exact source and an actual known Delta epoch; all six speculative requests
carry its unknown epoch and return undecided. Each cell checks a State-disabled
native `dbt test` Reference execution, full typed SQL API failure rows (including
the duplicate), ordered INT schema, independent count/error/warn aggregate,
complete confirmed/cached result fields and runtime, raw client status and exit,
and captures a complete Explain tree. MANAGED/Delta properties are measured separately.
The certified [whole 39-RPC hosted fixture](../golden/fixtures/databricks_cached_test_upstream.jsonl)
retains telemetry, statuses, confirmations and Explain. The original whole-history
public replay failed on three BUILD Explain timestamps: Rust rendered a raw epoch
where hosted rendered relative time plus UTC. The renderer now uses the retained
Databricks submit clock for these data-test lines, without adding freshness
verdicts or changing other warehouses/legacy rows. The corrected public replay
checks the exact trees; no timestamp is masked and no new exemption is added.
The original Rust live run uses the production-library `TestServer`, avoiding the
historical standalone executable left by the upgrade campaign. Its raw BUILD
Explain mismatch is preserved; the display fix is verified by public replay,
not claimed as a second native client campaign. No further warehouse writes
were made for the fix.

Credential-free public gRPC controls additionally reject reuse after an unknown
or changed source epoch and under another full UID, with a positive original
scope control. Existing source-less restart/org/namespace and complete payload
controls remain. Unit tests are accounted for separately from cached data tests:
the [protocol inventory](protocol.md#state-selectors-selectorservicegetstateselection)
does not record them as cached workloads. Hosted unit-test reuse remains
[uncharacterized](correctness-handoff.md); this work adds no unit-test caching
or native unit-test support claim.

Use `experiments/databricks_cached_upstream.py` with the existing cached-test
runner arguments plus `--schema` and a fresh UUID `--object-id`; run hosted first,
then use the observed contract and a different UUID/work directory for Rust.
The ignored integration test wires the current Rust library to the pinned client.
The driver verifies lease ownership digests, pages the entire existing inventory,
checks collision/capacity and compiles before DDL. Per backend it allows one
CREATE, one INSERT and two UPDATE statements against its exact UUID table; no
schema creation, cleanup or writes to earlier graphs. All previous objects stay.

Secret-free raw evidence is retained outside the repo at
`~/.local/state/dbt-state-rs/databricks-143-20261115/{hosted-3,rust}`. Each capture
has 39 RPCs: hosted SHA-256 `7bce456acf4140f2ddc4174934d8dcf23082ae43483f4be9602eb51879c36934`,
Rust `c4ab66573df1286964bc55ab62c24e900650c6f240ff03d9415eb5a78b1060f6`.
Inventory was 39→40 and 40→41 in existing `cdo_demo_db.ds_20261112a134ef_prod`;
owned names are `t143_0b04ca9a77314896b84e62749ff28549_inputs` and
`t143_db1fbe412f284b9aacb09ca8fc46cbdb_inputs`. Two earlier stopped attempts
remain in sibling `hosted`/`hosted-2`, with both created tables preserved.
Final exact client-probe/pagination/speculation guards were strengthened after
live execution; the original twelve native/reference query logs per backend
were revalidated offline against those guards. A new BUILD Explain guard rejects
raw epochs and wrong absolute times; offline it accepts the original hosted
trees and reproduces all three original Rust mismatches. Future live summary
success requires this check. Live snapshots and captures stay unchanged.
