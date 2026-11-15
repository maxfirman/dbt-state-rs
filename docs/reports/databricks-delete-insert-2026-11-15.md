# Databricks DELETE_INSERT bounded acceptance — 2026-11-15

**Validated safe partial; whole #149 and dependencies #138/#140 remain open.**
The November14 campaign verifies the pinned native null-safe key-replacement
path against hosted State and a fresh Rust server. Final offline validation and
packaging completed November15. No production decision, eligibility, persistence,
identity or response policy changed.

The scope is dbt2.0.11, the configured Databricks Free Edition serverless SQL
warehouse, Unity Catalog, SQL incremental models and MANAGED Delta tables.
The exact dbt binary SHA is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
No new warehouse-version query was added; this does not broaden the pinned
client/environment claim or validate legacy adapter paths.

## Complete causal sequence

The original immutable ledger predates live execution and is unchanged:
`ace0a361bdd87c5676959367939ef957ef99ded675c3757f0637756753080a2f`.
Expected complete source, cumulative result R and fresh selected result F are
literal worked multisets, independent of State, warehouse output and production
fingerprints. Ordinary SQL is fixed from first through steady invocations:
`WHERE p = DATE '2026-11-01' OR p IS NULL`. Only full refresh selects the full
source. No configuration predicate is involved.

Both backends independently pass this sequence. Counts summarize complete typed
bag comparisons; counts or sums alone are never used to accept a result.

| Cell | Source rows | R rows | F rows | Observed behavior |
|---|---:|---:|---:|---|
| first | 6 | 6 | 6 | BUILD/Confirm, including duplicate NULL-key rows |
| first repeat | 6 | 6 | 6 | fresh SKIP with own confirmed runtime |
| non-NULL update + duplicate | 6 | 7 | 5 | replace key1, preserve incoming duplicates; retain old absent/nonselected keys |
| update repeat | 6 | 7 | 5 | fresh SKIP |
| remove source NULL | 4 | 7 | 3 | both old NULL-key rows retained |
| remove NULL repeat | 4 | 7 | 3 | fresh SKIP |
| restore selected NULL | 5 | 6 | 4 | one incoming NULL key replaces both old NULL-key rows |
| restore NULL repeat | 5 | 6 | 4 | fresh SKIP |
| empty selected input | 1 | 6 | 0 | no incoming key removes old target rows |
| empty repeat | 1 | 6 | 0 | fresh SKIP |
| full refresh | 1 | 1 | 1 | native CTAS and acknowledged FULL1 Record |
| next ordinary | 1 | 1 | 0 | DELETE_INSERT5 BUILD/Confirm; empty input retains refreshed row |
| next repeat | 1 | 1 | 0 | fresh SKIP with the new ordinary build's runtime |

Every cell verifies complete typed source/R/F bags and ordered
`id BIGINT, v STRING, p DATE, ts TIMESTAMP` schema, exact duplicate/NULL values,
MANAGED Delta properties and full consecutive source/target histories. Each
phase retains source versions0–5 and target versions0–7. The native query logs
show the actual observed operation:

```sql
insert into table <own consumer> as target
replace on (target.id <=> temp.id)
(select id, v, p, ts from <own session temporary view>) as temp;
```

NULL keys match NULL through `<=>`. Key membership removes every matching old
occurrence; incoming occurrences are never deduplicated. An absent key retains
old rows. This is the observed native `REPLACE ON` branch; a legacy scalar
`DELETE ... IN` interpretation would have different NULL behavior.

## Fractional timestamp codec and independent references

The retained #147/#148 proof covered whole-second UTC values, not the nonzero
fractions in this ledger. Before any new source/consumer DDL, the separately
cleared hosted codec gate executed one pinned six-row literal SELECT twice:
one typed API execution and one State-disabled `dbt show`. It independently
proves exact `.123Z`, `.456Z`, `.789Z`, `.012Z` encodings, the ordered schema and
duplicate NULL-key rows. Its isolated recorder certifies zero State RPCs.
Unknown precision, offset, spacing, types, SQL or encoding would stop before DDL;
no generic normalization or ledger change is allowed. Whole-second `Z` is
accepted only as the identical evidenced `.000Z`; nonzero fractions are exact.

Each backend also executes13 separate State-disabled SELECT-only literal R
references with isolated complete zero-RPC canaries, and independently reads
selected F from its own source. The combined archive therefore certifies27
zero-RPC canaries:14 hosted including the codec gate,13 native. No persistent
reference relation is created. Native does not repeat codec discovery.

## Full refresh, statuses and actual native Explain clocks

Hosted full refresh is discovery before it becomes an expectation: original
zero-based entry72 (line73) records execution typeFULL1, positive epoch1794699386000,
runtime3490ms, full input/outcome and `records_stored=1`. The future native grant
locks literal expected kind1 to that original record and the complete strict
hosted replay. The native phase independently observes1; any changed kind,
including another valid enum, rejects. Ordinary submissions remain strictly5.
This is not generic acceptance of all Record kinds.

Each complete capture has88 RPCs:13 validation,32 telemetry,12 speculative,
12 ordinary,6 Confirm,12 Explain and1 Record. Client statuses are7 `success`
and6 `reused` per backend. Each fresh SKIP returns its own most recent confirmed
runtime, with unchanged source/target evidence and unchanged target Delta
version. Each phase has12 complete Explain trees and no invented Record decision.

The fresh Rust wrapper retains actual organization/decision/audit sampled clocks
for all12 ordinary decisions after proxy drain and before its isolated State
schema is dropped. The sidecar is bound to the entire immutable native capture,
with exact ID/org coverage and captured request-time bounds. Live traffic uses
actual clocks. Supplemental replay uses these retained samples, including every
original status, response and Explain field. No response mask, gap, unknown
baseline, fabricated time or accepted prefix is introduced.

## Preserved source attribution and post-use guard correction

Hosted execution used clean source `8a79629` with646 tracked input hashes.
Native execution used clean `d95a94d` with647 hashes, after byteexact promotion
of the whole hosted fixture as corpus129. Both original source archives, phase
grants, tool/executable copies, actual PID/argv/executable receipts and all hashes
remain immutable. Both processes and every recorder have exited; no retry,
source reset, warehouse cleanup or source-history import occurred.

Final offline review found a validation-helper lookup at the wrong JSON level:
the original frozen driver used `skip.get('is_stale')`; the actual field is
`skip.explained_decision.is_stale`. Post-use helper commit `f24c7a0` adds a
literal-false check at the public submit-validation boundary. Negative tests for true, missing and wrong-level flags failed before the fix
because the old guard accepted them; after the fix the guard rejects all three.
All24 retained ordinary submissions pass the corrected guard; all12 raw SKIP
flags are independently verified false. The complete public replay already
compared those original responses exactly. No production policy, capture or
live execution was modified or rerun, and this later helper is not attributed
to the earlier executed source.

## Validation and footprint

Fresh results are:

- all128 prior hosted corpora plus the ENTIRE new hosted88 trace:233.41s;
- after promotion, all129 hosted corpora plus the ENTIRE native88 trace and
  its actual12-clock sidecar:253.76s;
- each new trace:76 non-Explain response assertions and12 full Explain trees,
  zero unknown baselines, gaps or partial-history allowances;
- the separate other18 conformance guards:18 passed,1 filtered,9.87s;
- focused Python47 checks including16 own; credential-free wrapper1 passed with
  its live test ignored, and that exact live test separately passed in279.29s;
- strict workspace/all-targets/fuzz Clippy, formatting and whitespace checks.

The whole-corpus log alone reports1 passed and18 filtered; it is not presented
as19 tests. A retained offline audit checks all26 complete cells independently.
The Explain census is990 trees in140 files, with12 new DELETE_INSERT trees
(six EXECUTE/six SKIP), no empty tree, unspecified value or new vocabulary.

Earlier root results of681 workspace tests,310 database-free library tests and
25 DuckDB cases are reused, **not rerun for #149**. Exact hash proof matches68
production and57 DuckDB integration/script inputs to the accepted uniform gate
manifest. Current Cargo.lock is identical to the accepted #148 input proof;
resolved package versions/sources/checksums remain unchanged, with the earlier
canonical sha2 harness dev-dependency edge already accounted for. New #149
fixture, helper, replay, lint and live results are fresh as stated above.

Only exact `di_20261114{a149aa,b149bb}_{inputs,consumer}` pairs are added in the
existing owned `cdo_demo_db.state_20261114_append146` container. Root receipts
verify its UUID/owner/creation, protected12 and14 inventories and fresh names.
GetQuota counters were STALE10 against actual12 before hosted, and STALE12
against actual14 before native, both with limit100; no fresh matching quota
counter is claimed. The final inventory has16 permanent objects. One shared
persistent auxiliary slot was reserved (peak17), but observed usage is0;
serialized SQL uses the exact session temporary view. No feature/macro override,
new warehouse schema, prior-object data read/write, permanent reference or warehouse cleanup occurs.

The machine-readable [report](databricks-delete-insert-2026-11-15.json) pins the
private evidence index, source/tool/grant receipts, complete ledger and clocks.
The archive is `~/.local/state/dbt-state-rs/databricks-149-20261114`.

## Unfinished scope

Whole #149 remains open. Alternate/empty/invalid/composite native unique keys,
predicate settings and aliases, schema evolution and name binding, warehouse
failure/atomicity/retry publication, broader strategy/node-kind transitions,
generated cases and restart/upgrade breadth remain required. The offline
composite-key oracle is not native composite-key acceptance. Legacy fallback,
uncharacterized temporal precision and unsupported error policy are not inferred.
#138/#140 remain incomplete; no dependency or whole Databricks claim is promoted.
