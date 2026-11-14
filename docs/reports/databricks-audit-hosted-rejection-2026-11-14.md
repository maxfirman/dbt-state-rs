# Databricks audit ordinary-submit status fidelity — 2026-11-14

A narrow ordinary-submit guard now matches the complete hosted audit histories,
including their observed errors. Two fresh pinned-client Rust counterparts also
match the bounded aliased and default-name contracts, including successful
native fallback and independent physical results after the expected error.
**#144 remains incomplete**, with views, genuine upstream audit histories,
alternate audit schemas and lifecycle coverage outstanding. #143/#138's UID
boundary blocker and the wider #132 blockers remain unchanged.

No audit relation is removed from inputs. The ordinary error boundary adds no
reuse eligibility, fingerprint, history, namespace, producer or freshness rule;
the speculative fastpath and Snowflake audit rules remain unchanged.

## Primary environment and result oracle

The pinned dbt 2.0.11 executable has SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The warehouse is serverless Unity Catalog managed Delta in `cdo_demo_db`.
The allocated default audit schema is
`ds_20261112a134ef_ref_dbt_test__audit`, UUID
`dcabff91-9a47-4ff0-9f53-2b71c06f414a`. Exact hosted names are
`sf_20261114a144aa` / `sf_20261114a144ab`; exact Rust names are
`sf_20261114b144bb` / `sf_20261114b144bc`.
The first name in each pair is managed; the second is an independent
State-disabled reference. No source/model relation participates.

The singular test uses table `store_failures`, true template comparison,
severity ERROR, thresholds `>0`, and
`fail_calc=coalesce(sum(v), cast(0 as bigint))`. Its independently declared INT
`(id,v)` multiset is `[(1,2),(1,2),(2,NULL)]`, yielding `(4,true,true)`.
The empty result yields `(0,false,false)`. Typed rows retain both duplicates,
NULL, column order and types. Separate typed aggregate SELECTs, native disabled
references, MANAGED/Delta properties and Unity Catalog identity establish the
physical results independently of State.

Fresh full UID **and raw/rendered nonce** require an actual initial BUILD;
fresh UID alone would not exclude #138's known hosted cross-UID behavior.
Recorded R remains separate from fresh F. A permitted template SKIP can return
failing R while independently evaluated F passes. Native fallback after a State
error changes physical F without publishing a new confirmed State R.

## Hosted characterization and preserved failures

The original `hosted-first/` attempt completed a disabled compile/reference,
created only exact reference `ab`, then stopped at a conservative allowlist's
scoped `system.information_schema.tables` SELECT guard. Its certified capture
has zero RPCs. Separate root review admitted that exact catalog/schema-scoped
metadata query and continued using the original successful artifacts unchanged.
The original failed summary is retained.

| Aliased native cell | Hosted State response | State result | Fresh F | Raw managed exit/status |
| --- | --- | --- | --- | --- |
| initial failure | BUILD + successful Confirm | `(4,true,true)` | `(4,true,true)` | `1 / fail` |
| unchanged failure | speculative SKIP | `(4,true,true)` | `(4,true,true)` | `1 / fail` |
| render only to pass | speculative SKIP | `(4,true,true)` | `(0,false,false)` | `1 / fail` |
| identical rendered-pass repeat | speculative SKIP | `(4,true,true)` | `(0,false,false)` | `1 / fail` |
| body and render change to pass | speculative Undecided; ordinary INVALID_ARGUMENT 3 | no new State result | `(0,false,false)` | `0 / pass`, native fallback |

The first four cells preserve complete physical R, payload/runtime and full
Explain. The final ordinary error is exactly `Failed to process SQL`, with null
response. The client warns `dbt1410` and executes normally; the driver stops on
the unexpected State status. A later separately authorized read-only snapshot
proves the final managed audit is empty and links its fallback CTAS query ID to
Delta history. This does not rewrite that campaign as successful acceptance.
No view cell ran. Original plus continuation use five compile, five disabled
reference and five managed invocations; all ten canaries certify zero State RPCs.
The entire failed 28-RPC trace has SHA-256
`f43a96f4a470e4469f2979e659d0f67a1276e69cc49773e9722eb569fa38dc1a`.

A separately cleared default-name control then uses singular filenames
`sf_20261114a144aa.sql` / `sf_20261114a144ab.sql`, exactly matching native node
names and physical aliases, with **no alias configuration**. Root first
snapshots and retires only the two owned prior outputs, preserving every other
object. Fresh projects `audit_default_20261114a144ad` and its reference, plus
nonces `20261114a144adtablea/b`, establish independent history. First failure
BUILD/Confirm and unchanged SKIP pass. The third, body-changing pass again gets
ordinary status 3 and native fallback. It stops after three managed/compile/
reference invocations and six zero-RPC canaries. The whole 18-RPC trace is
`8ad1024c8fd4e312639f9d1a2cff1c26795def58b20f6638d8dcd3f3eb8166cd`.
Configured alias is therefore not necessary for this observed rejection.

## Separately attributed historical transport controls

These use a fresh recorder and exact digest allowlists, retaining opaque client
hashes deliberately as **historical/synthetic transport fields**. They do not
claim fresh compilation, current epochs, native declarations or physical rows.
No pending response is confirmed; no dbt or warehouse SQL executes.

| Control | Only changed field relative to its captured request | Hosted result |
| --- | --- | --- |
| A: exact aliased failing ordinary request repeated | none | status 3 |
| B: same request without reported audit evidence | `tables=[]` | status 0, ready-to-execute |
| C: default-name failing request genuinely reads exact managed audit relation | `sql` | status 0, ready-to-execute |
| D: same default request becomes equivalent empty single SELECT without UNION | `sql` | status 3 |
| E: original confirmed default failing-body request adds captured audit table/epoch | `tables` | status 3 |

Every error is `Failed to process SQL`, with null response. In the actual final
native cells, speculative and ordinary request bodies differ **only in tables**:
empty becomes the exact known-epoch managed audit relation; SQL never reads it
and query dependencies are empty. C accepts a genuine relation read, while D/E
show UNION syntax and changed-body cache path are not necessary for the error.
These observations do not establish generic audit elimination or rejection.
A/B's certified two-RPC trace is
`cce7280c8d4d2921d25209c77bf58a33506e09d4ead35112926bdb53e156de84`;
C/D/E's certified three-RPC trace is
`7c08c72a4e15388306ec2eb11f13e59d61b3683966c6da2810d89583c81f1786`.

## Narrow ordinary status boundary and complete public histories

After organization validation and before any State decision, the handler
rejects only this checked subset: Databricks DBT_DATA_TEST, no target, true
template mode, exact native `store_failures=true` / `store_failures_as=table`,
empty query dependencies, exactly one positive known-epoch fully qualified
table in the same default catalog/schema with native `_dbt_test__audit` suffix,
and affirmative parsed source-less SQL. A Databricks-local AST qualifier admits
the captured literal SELECT/CAST/equality/AND/UNION ALL within derived-query
forms. Named table factors, CTEs, table/scalar functions, placeholders,
uncharacterized casts/AST and unknown parsing do not qualify. The shared
relation-read parser stays unchanged.

Public negative controls preserve genuine direct/quoted-CTE/subquery/function
reads, dependencies, false/missing store flags, false compare mode, views,
other dialects, targets, missing/multiple/ambiguous/outside-context names and
unknown/nonpositive epochs. No input is dropped or history widened.

Before implementation, strict whole-history replay fails at aliased entry 26
(status 3 versus Rust 0, 269.69s) and default entry 16 (262.88s). The public
positive regression also first fails on the old successful response. All reds
remain retained. After the guard, whole native28+A/B2 and native18+C/D/E3 replay
green with all then-accepted corpora in 216.57s and 237.15s respectively.
Only then are byte-identical chronological concatenations promoted:

- `databricks_audit_aliased_error_history.jsonl`: all 30 rows, SHA-256
  `6a32d653285fbaf225d2f08f41cde8097eb52527a4fecae523c9abd01d3dca92`.
- `databricks_audit_default_error_history.jsonl`: all 21 rows, SHA-256
  `19f2c1c39fa5e15a8c3ef7951c9a103174dc8703208d6c8531df6e022ea2a4e1`.

Every status, null error response and full Explain remains visible. There is no
prefix selection, new mask, gap, unknown baseline or partial-history allowance.
Original campaign labels, summaries and certified captures remain unchanged.

## Fresh native Rust counterparts

Both phases run the actual production router in fresh isolated State history,
using pinned native dbt and separately reviewed exact-name receipts. The
running server PID/argv/executable/address are bound before warehouse use.
The actual frozen acceptance executable is
`6fe2d15ecc570aba23b96b210b3fbe617e295d5b1a5f8152ff835d7606aa42cb`;
the amended helper is
`2c46f167e53438732a0803ce4e41bc0be41477ce9b6e67a09d59a7755cf286e9`.
The full amended 537-file manifest is
`8b829b31da0418460406138d01789e96f85d15453ee37f94516186b82572498a`.
Sources and actual executable copies are retained before use, not inferred
from historical hashes. Every amended input stays identical through each run.

Aliased phase A passes in **112.41s**: all five managed cells, five compile and
five disabled references match the table above. Full typed physical/schema/
Delta/aggregate checks, complete cache results, own-runtime echoes and four
Explain trees pass. All ten canaries certify zero RPCs. The final expected
status 3/null/Undecided has no ConfirmExecution, RecordExecutions, SubmitValues
or RegisterClone; the pinned client warns and falls back with exit 0/pass and
empty physical F. The prior confirmed State R remains `(4,true,true)`.
The entire 28-RPC native trace is
`5270125295278a1d689f4f8bd31f07af3af2790e466ce572e2a884f00e785dcd`.
Exact supplemental replay with all 126 hosted corpora passes in **237.23s**,
including all statuses and four complete trees, with zero exemptions.

Root then separately snapshots and retires only the exact owned bb/bc table
IDs, verifying owner/actor, typed empty rows, aggregates, properties, full Delta
history and final native CTAS query IDs. The snapshot manifest is
`02ed70840dfe8b3101f682eb6db7866dc01be99158d36f32a5d7cd46766a35a8`.
The original retirement receipt accidentally labels its descriptive status
`aaab`; actual allowlists, UUID/ID guards, DROP statements and inventories are
correctly bb/bc. It stays immutable. A root metadata-only corrected addendum
binds its original digest and changes only that label, preserving all facts;
phase B verifies this chain before use. No cleanup is performed by the agent.

Default-name phase B passes in **77.81s**, with fresh project/nonces
`audit_default_20261114b144bd` / `20261114b144bdtablea/b`, default bb/bc singular
filenames and no alias configuration. Its three cells are initial BUILD/
Confirm4/T/T, unchanged speculative SKIP4/T/T, and the body-changing pass with
expected ordinary status 3 plus successful native fallback/physical F = 0. All six
compile/reference canaries, independent typed/schema/Delta/aggregate oracles,
own runtime and two full Explain trees pass. The entire 18-RPC trace is
`c530d9a716daeb22b91cc7d3218434d95eb73edc7feb193b1d629e7bf222a7fc`.
All126 plus that whole trace replay green in **242.28s**, with zero exemptions.

Every native phase starts with both output names absent. Before/after
inventories preserve host aa/ab and every primary object (a99/e100, including
all original incremental objects); only exact bb/bc are added. Final audit
inventory contains exactly four retained names. No native replay-clock header,
new hosted call, custom macro/client feature, new schema, persistent auxiliary
or agent cleanup is used. Proxies drain before releasing each phase.

## Validation attribution and remaining scope

Production changes require fresh broad validation. Against the original
uniform 537-file manifest
`b3056c795dd75fb3ee7d655b548316db3f1eeece7ec5b0f154c36e98076868b9`,
new workspace validation passes **681 tests**, zero failures, 49 opt-in ignores
across 88 groups in 657.10s, including all 126 public corpora. New no-database
library (310 tests), strict workspace/all-target/fuzz Clippy, formatting and **25/25 local
DuckDB acceptance cases** pass; DuckDB takes 930.67s. Full Python (87 tests) passes in
the provisioned environment after a retained system-Python missing-grpcio stop.
No prior675/25 result is reused for the production change.

Root review then identifies two Python-only oracle gaps: actual publication
methods and ownership-versus-managed-role confusion. Red/green amendments
forbid actual RecordExecutions/SubmitValues and require exact managed relation,
including checked quotation. Only two helper/oracle files change; all other 535
inputs and frozen executables remain byte-identical. New focused Python (88 tests),
frozen harness3-pass/1-ignore and offline revalidation of all eight native cells
with six injected publication/reference-role faults pass. Original537 gate
copies stay immutable; those broad results are not called a uniform final-helper
rerun. The amendment manifest is
`a95c3da28615c0e4f4e3cf5c293dc418d66c2f1ac4bc7c81359a7a772f37ceba`.

After both frozen native phases, canonical partition/clock-collector changes
are integrated separately. They add test-only sampled-clock replay and a
harness dev-dependency on an existing sha2 version; all production/proto source
and resolved package versions/sources/checksums remain identical, as do DuckDB
scripts/test bodies/integration sources. Actual native runs retain the frozen
pre-integration executable/source attribution. Current focused audit/partition checks pass (four tests, two opt-in ignores),
as do all 101 Python tests, strict workspace/all-target/fuzz Clippy, formatting
and whitespace checks. The final conformance group passes all 19 tests in
242.25s, including all 127 hosted corpora and an unfiltered 46-row supplemental
concatenation of the two native histories (40 assertions and six full trees,
zero exemptions). Its constituents remain separately certified independent
State sessions with distinct fresh UID/body scopes; this is not a new continuous
native capture. The supplemental digest is
`e7cd701696ed35d31891d59158ae58e8c923ca79ef08d958f7da269244090813`.

Six audit Explain trees add four existing template INFO lines, no invented
error tree and no unspecified marker/badge. After canonical partition evidence,
the complete census is 966 trees in 138 files. Views, alternate audit schemas,
genuine-upstream native audit behavior, external modifications, drop/recreation
and post-error reuse histories remain uncharacterized. These bounded histories
do not complete #144, its dependencies or full Databricks support.

The complete private archive is
`~/.local/state/dbt-state-rs/databricks-144-20261114/`. Original failures,
transport controls, root-owned allocations/retirements, all before/after
snapshots, source/executable copies, validation receipts and native phases have
separate attribution. The [machine-readable report](databricks-audit-hosted-rejection-2026-11-14.json)
links its file-hash index and exact gate/replay receipts.
