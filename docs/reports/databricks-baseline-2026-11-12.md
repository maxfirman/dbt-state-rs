# Databricks table/view baseline — 2026-11-12

The #135 slice passes hosted public replay and real dbt acceptance against the
Rust production router, with independent physical results. This claim covers
the literal managed Delta table/view lifecycle below. The broader #132 scope,
including the survey's copied-target rebuild and snapshot deletion-record
blockers, remains unfinished; see the [capability report](databricks-capabilities-2026-11-12.md).

The environment is official dbt **2.0.11**, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`,
Databricks Free Edition serverless warehouse `47fd3fa312624d91`, Unity Catalog,
DBSQL **2026.44**, and managed Delta tables plus views. Databricks CLI was
1.26.0; Rust 1.98.0; the Rust campaign's controller used Python 3.14.0 and
PyYAML 6.0.3. No additional dbt packages are required for this literal workload.

## Observed lifecycle and independent results

Both phases declared the same literal identity, `2026111257ac3e`, in their
query data. Their separate fresh project copies, relation aliases, invocation
artifacts and State histories prevent shared physical results. The string
literal also prevents previous campaign SQL from becoming hidden hosted copy
history. The result schema is `(id BIGINT, amount INT, tag STRING)`; two
identical rows and a row containing NULLs make multiplicity and NULL handling
observable. The semantic query change changes both duplicate amounts from 10
to 17.

| Step | Hosted table/view | Rust table/view | Independent physical checks |
| --- | --- | --- | --- |
| First build | BUILD / BUILD, accepted confirmations | BUILD / BUILD, accepted confirmations | Complete schema and all three rows match |
| Unchanged | SKIP / SKIP | SKIP / SKIP | Same complete schema and row multiset |
| Changed query | BUILD / BUILD, accepted confirmations | BUILD / BUILD, accepted confirmations | Both duplicate rows contain amount 17 |
| Unchanged changed query | SKIP / SKIP | SKIP / SKIP | Complete changed result remains correct |

Each phase ran nine dbt invocations: one parse, four State-guided runs and four
independent State-disabled reference runs in another project/profile target.
The reference runs emitted **zero State RPCs**. Each step checked the complete
typed row multiset and ordered schema against those references, a separately
executed SELECT and literal expected values. Required properties were checked
separately: MANAGED/Delta for the table and VIEW for the view. Expected SKIP
responses are mandatory, so always building cannot pass.

The final negative probe updated only that run's table to amount 999 and
required the exact `physical:rows` failure. Network/authentication/SQL errors
cannot count as oracle sensitivity. Credential-free probes additionally reject
lost duplicates, changed NULLs/values, a wrong SKIP after a logic change,
column name/order/type changes, wrong relation kind/provider, incomplete SQL
read envelopes and unsafe/colliding lease names. The promoted fixture's
inventory is pinned so dropping lifecycle or Explain calls cannot silently
remove coverage.

## Complete protocol evidence and the ordering fix

The primary hosted campaign retained **38 certified RPCs**, including eight
ordinary submissions, eight speculative submissions, four confirmations, four
Explain requests, validation and complete telemetry durations. Six later
hosted batch probes alternated ascending/reversed Explain requests; all six
returned the same ascending decision-ID order.

The strict replay exposed Rust's previous request-order response as a real
mismatch. A public replay regression failed before the shared Explain handler
was changed to sort entries by decision ID. The promoted **39-RPC fixture**
contains the entire primary campaign plus one opposing-order diagnostic call;
every requested decision and confirmed build is in that fixture. It compares
all ten Explain entries' full text, names, markers, badges and children. There
are **zero unknown baselines, known-gap exemptions or partial histories**.
Opaque IDs are remapped; ascending ordering is checked independently in each
ID domain because concurrent hosted responses can be captured in a different
order from ID allocation.

The Rust live campaign retained **39 certified RPCs** and exercises both
Explain request orders itself. Its proxy uses the explicit local HTTP mode;
`operations.json` records `mode: local` and the actual owned endpoint. Local
forwarding loads no dbt Cloud credential and performs no token exchange.
Hosted TLS/authentication defaults remain unchanged. Public CLI tests reject
nonliteral/nonloopback destinations and verify real forwarding plus clean
capture certification.

An additional retained cross-phase comparison passed for all four steps: full
ordinary response payloads, complete Explain entries, statuses/application
metadata, complete typed rows/schema and SKIP runtime echoes. It maps only the
different role relation names and opaque ID values, verifies runtimes against
each phase's own accepted confirmations, and excludes the five previously
characterized hosted-ingress HTTP headers. Raw captures retain those headers.

## Isolation, retained failures and reproduction

Two initial attempts failed before any dbt invocation because Unity Catalog's
schema quota counter remained 57/50 after verified empty-schema cleanup. Those
failures remain archived. The primary campaigns explicitly leased
`cdo_demo_db.ds_20261112a134ef_ref` and
`cdo_demo_db.ds_20261112e134ef_ref`, whose ownership is proven by hashed #134
configuration records. They use only new `b_<run>_literal_table/view` aliases.
Actual preexisting inventories were checked before and after each phase; no
old object was changed or removed, and lease mode creates/drops no schemas.

An exploratory untagged hosted campaign also succeeded and is retained
separately; the tagged campaign supplies the promoted lifecycle evidence. One
Rust test launch failed before Python started because a temporary `uv run`
environment had expired; its log is retained. The successful invocation uses
an owned persistent Python environment, as shown in the corrected
[reproduction instructions](../databricks-baseline.md).

Artifacts are archived at
`/home/maxfirman/.local/state/dbt-state-rs/databricks-135-20261112`.
The [machine-readable evidence index](databricks-baseline-2026-11-12.json)
records capture/sidecar hashes, exact counts, source composition, ownership
proofs and validation. It includes the external comparison script and report.
Configured Databricks token scans found no token in the retained live
artifacts. Existing populated survey diagnostics are preserved.

Ordinary CI remains credential-free: it runs the complete replay and offline
guard/oracle tests. Real dbt/Databricks acceptance remains explicitly opt-in.

## Validation

- `cargo test --workspace`: **584 passed**, zero failed, 28 opt-in tests ignored.
- Conformance: all **10 tests**, including **71 replay corpora**, pass.
- Explicit real dbt/Rust acceptance: one passed, nine dbt invocations,
  39 certified RPCs; the independent references emitted zero RPCs.
- `cargo clippy --workspace --all-targets --features fuzz -- -D warnings` and
  `cargo fmt --all --check`: pass.
- Offline baseline tests pin the fixture's full lifecycle/Explain inventory,
  the independent checker mutations and lease safety. Survey and capture
  guards also pass in the workspace suite.

The full DuckDB live campaign remains the final integration release gate after
the remaining work packages. This slice ran the complete workspace suite and
all existing hosted public conformance, and has no unresolved additional
DuckDB concern from the shared Explain ordering change.
