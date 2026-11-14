# Databricks true-template cached failure — 2026-11-14

This is new bounded independent partial #143 evidence. Native #143 remains
incomplete and blocked by #138's cross-full-UID hosted reuse conflict. No
isolation floor, dependency status or production rule is widened. This does
not establish full Databricks support, unit-test caching or #144 audit behavior.
The earlier false-mode failure and configuration histories remain separately
attributed in their dated reports.

The ordinary pinned dbt 2.0.11 executable has SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The serverless Free Edition warehouse and Unity Catalog `cdo_demo_db` are
unchanged. One selected source-less singular test explicitly declares
`store_failures=false`, `compare_unrendered_code=true`, severity ERROR,
`fail_calc=coalesce(sum(v), cast(0 as bigint))`, and both thresholds `> 0`.
Its ordered INT columns `id,v` have the literal multiset
`[(1,2),(1,2),(2,NULL)]` when enabled, and no rows otherwise. The independently
worked aggregate is 4 for enabled rows and 0 for empty rows. Both duplicates
contribute; NULL does not; coalesce makes the empty outcome complete.

A fresh full UID **and fresh body/rendered SQL nonce** establish an actual
initial BUILD and successful computation Confirm carrying the complete failing
triple. A fresh UID alone would not exclude #138's known hosted cross-UID reuse.
This history requires initial BUILD and retains no unknown-baseline exemption.

| Actual client cell | Hosted decision | Complete cached `(failures,error,warn)` | Fresh independent result | Raw managed exit / status |
| --- | --- | --- | --- | --- |
| first failure | BUILD + successful Confirm | `(4,true,true)` | `(4,true,true)` | `1 / fail` |
| unchanged failure | speculative SKIP | `(4,true,true)` | `(4,true,true)` | `1 / fail` |
| render only to pass | speculative SKIP | `(4,true,true)` | `(0,false,false)` | `1 / fail` |
| change body and rendering to pass | BUILD + successful Confirm | `(0,false,false)` | `(0,false,false)` | `0 / pass` |
| unchanged pass | speculative SKIP | `(0,false,false)` | `(0,false,false)` | `0 / reused` |

The render-only transition uses ordinary `--vars` with a byte-identical body
and configuration. Its actual request delta is **exactly `sql`**. The emitted
true mode, all dbt node hashes, full UID, semantic extras, macro/context fields,
organization and connection namespace stay fixed. The body hash is
`04f469f326c9cccbf0638e7ec3ab4db7`, configuration hash
`f729e9e90bd060400efa6636743cde31`. The later nonce change alters both body and
rendered SQL; its body hash is `a1f90ff9de2f20fe66db8814917fce4e`.

The confirmed failing reference R remains immutable while template policy
intentionally reuses it. Fresh F is retained separately; it passes with exit 0.
This permitted R/F difference is not a wrong-result claim. The runner compares
the managed cache with R and the independent execution with F, and switches
both references to the new passing result after the observed body-changing
BUILD. Offline faults reject interchanging those references, retaining the old
failure after invalidation, stale/incomplete/mistyped Struct fields, wrong
counts, wrong scope/mode/hash deltas, runtime errors and unexplained nonzero exits.
Logical failure is accepted only with the complete selected result, matching
wire payload and successful computation confirmation for BUILD.

Every managed cell retains all three typed Struct fields, confirmed runtime
(or its exact reuse echo), complete response and gRPC status, and its full
first-call Explain tree. Ordinary repeat reachability is not claimed: the pinned
client takes speculative SKIP for these repeats. The wire is DBT_DATA_TEST 8,
null target, empty tables/query dependencies and authoritative full UID.
Explicit false emits `store_failures=false` and `store_failures_as=ephemeral`.

Each cell has a complete independent typed row SELECT, a separate aggregate
SELECT, and a fresh State-disabled dbt test with the same declaration. All five
disabled tests and five compile prechecks have separate recording canaries
certifying zero State RPCs. No custom macros, client overrides, hooks, models,
seeds, snapshots, unit/audit objects, schemas, DDL/DML, cleanup or physical
user-data reads are used. The mutation allowlist is empty. Only the exact
absent audit relation's metadata probes are admitted; absence is checked before
invocation. Before/after inventories have exact equality at 100/100 objects.
The coordinator owns subsequent unrelated cleanup outside these intervals.

The complete primary archive is
`~/.local/state/dbt-state-rs/databricks-143-20261114/true-hosted-first/`:
15 dbt invocations, full manifests/run_results, query logs, SQL API envelopes,
operation trace, per-cell R/F ledgers and exact request deltas. The immutable
29-RPC fixture `golden/fixtures/databricks_cached_test_template_failure.jsonl`
has SHA-256
`00954f2570b1bb7c8ca96ae63fb276238a6aae3f8ec1d8e500cbe6f959c8f6b6`.
Sources and freshly built owned proxy/probe executables were copied to
`true-current-source-bin/` and hashed before actual use on integration
`e3f641aa54dd6d2880da698a3a44f973c8699914`; no historical binary hash was
inferred. All owned proxies were stopped before releasing the inventory window.

The first exact production-router replay is green across all 122 hosted corpora
in 221.72 seconds. The new fixture asserts 24 non-Explain responses and five
complete trees, with zero gaps, unknown baselines or partial history. There was
zero production red and no server change. Focused public persistence/scope and
oracle controls pass (four tests, three opt-in ignores); all 21 credential-free
Python groups, strict workspace/all-targets/fuzz Clippy, 310 library tests with
an invalid database URL, formatting and whitespace checks pass. The focused
Explain census has five nonempty trees, three INFO lines and no unspecified
marker/badge. The tracked census is 925 trees / 133 files.

For the unchanged production implementation, the coordinator's final-source
675-test workspace and 25/25 DuckDB gates at
`af6937bee4e1d6f7c5c5422e5fe967100d04fb12` are accurately reused. This is not a
fresh execution of those broad gates. A retained 159-file comparison covers
server/proto/protocol sources, Cargo inputs, harness implementation, DuckDB
integration/tests and all DuckDB scripts, with zero differences. This slice
adds only experiment helpers, acceptance tests, a fixture and documentation.
The newly changed boundaries have the fresh focused checks described above.
The retained reuse receipt is
`/tmp/dbt-state-132-context/true143-identical-production-gate-reuse.json`;
the final-source DuckDB receipt is
`/tmp/dbt-state-132-context/capture141-final-duckdb.json`.

Pinned-client acceptance against current Rust on synced integration
`3bb711dfdc34224e81c3a7e9c10c0db5976ee8ba` passes in 48.38 seconds. Its five
managed cells and ten zero-RPC compile/reference canaries match the observed
hosted decisions, typed outcomes, runtime echoes and raw exit/status contract.
Fresh F stays independently passing while render-only SKIP returns failing R;
the later body change BUILDs and subsequent pass positively reuses.
The complete 29-RPC Rust trace in `true-rust-current/` has SHA-256
`87f72b6cd2f074402af43ad97ac1f613b69559bd04757c44af7fdd1bf4349dac`.
The exact acceptance executable, proxy/probe executables and launch sources
were copied to `true-rust-source-bin/` and hashed before actual use. This is a
pinned-source live run, not an inferred historical binary or future-source run.
The Rust interval also has exact 100/100 before/after inventory equality. All
owned proxies were stopped before explicit release; no further warehouse calls
or cleanup were performed by this slice.

The final template entrypoint SHA-256 is
`e74be6e85c5bd10af4b50ebf7e3c8186e0e54d100d2f244a9afdb1714757ee88`;
the shared guarded runner SHA-256 is
`aadb56e88926732b3ed6e1f950f55cf7bf0e9bc1d5fc346275342bdcb0d59d2a`.
Both are byte-identical to their retained hosted and Rust launch copies.
