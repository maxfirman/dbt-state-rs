# Databricks cached test configuration — 2026-11-14

This is bounded independent partial #143 evidence. Native #143 remains
incomplete and blocked by #138's hosted cross-full-UID reuse conflict. No
production rule, safety floor, dependency status or accepted scope was changed.
The [prior failing-cache slice](databricks-cached-test-results-2026-11-14.md)
remains separately attributed.

The pinned dbt executable is 2.0.11, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The existing Databricks Free Edition serverless warehouse and Unity Catalog
`cdo_demo_db` are unchanged. A selected singular test reads only the declared
ordered INT columns `id,v` and multiset `[(1,2),(1,2),(2,NULL)]`; explicit
`store_failures=false` / `store_failures_as=ephemeral` creates no audit object.
The write allowlist is empty. No models, seeds, snapshots, unit tests, custom
macros, hooks, DDL/DML, schemas, cleanup or physical relation data reads were
used. Ordinary exact derived absent-audit metadata probes remain recorded.

## First configuration vertical

A fresh project/full UID and SQL/body nonce establish an initial failing
BUILD, followed by required unchanged reuse. The same full UID, organization,
namespace, SQL, body and configuration declaration are kept throughout; only
variables supplying the declared test configuration change. The actual request
audit shows ERROR→WARN changes **only `semantic_extras.severity`**; even the
node config hash stays fixed. This is primary evidence about emitted fields,
not an assumption that configuration variables must change that hash.

| Declaration | Hosted decision pair | Complete raw result `(failures,error,warn)` | Raw exit / status pair |
| --- | --- | --- | --- |
| severity ERROR; count(*); both thresholds >0 | BUILD, speculative SKIP | `(3,true,true)` | `1/fail`, `1/fail` |
| severity WARN; count(*); both thresholds >0 | BUILD, speculative SKIP | `(3,true,true)` | `0/warn`, `0/warn` |

WARN does not suppress the raw `should_error` flag. Each BUILD successfully
confirms the complete result despite the logical failure/warning. Each reuse
echoes all three typed fields and its own confirmed runtime. Every cell retains
ordinary/speculative requests, complete response/status and first-call Explain.
Ordinary unchanged CLI reuse reachability is not claimed.

Each cell independently checks complete typed literal rows, a separate aggregate
SELECT and a State-disabled dbt test with exactly the declared configuration.
Separate owned recording canaries certify zero RPCs for all four references and
all four compile prechecks. Raw exits are preserved; unexpected nonzero,
transport/parse/auth/runtime errors, incomplete payloads and wrong selected
results fail acceptance. Offline fault controls distinguish WARN classification
from the raw error flag and reject stale passing/threshold outcomes.

The complete hosted trace is retained under
`~/.local/state/dbt-state-rs/databricks-143-20261114/config-hosted-first/`, with
12 dbt invocations and 24 certified RPCs. The immutable fixture
`golden/fixtures/databricks_cached_test_config.jsonl` has SHA-256
`4cc17b913c24d4c29530f000ef88913e3d3bf46af2afefd09a29b502c6f9f016`.
Exact public replay against an empty isolated Postgres-backed production router
passes all 115 hosted corpora in 201.99 seconds, including its 20 non-Explain
assertions and four complete Explain trees. There was zero production red.
No masks, gaps or unknown-baseline exceptions were added.

Actual pinned dbt against Rust integration `facbe84547d09a2f6c0c9127dd8661a72b41519e`
passes the same four-cell contract in 40.90 seconds. Its complete 24-RPC capture
under `config-rust-current/` has SHA-256
`2f707278c1b27496e72b7962dc7f9f99f6b6feabbd720d5d17899cfb0f7e54c6`.
`config-current-source-bin/source-provenance.json` and retained executables were
written before launch and pin production, harness, helper and binary hashes.
This is a pinned-source live run, not a claim to rerun the client on future source.
The hosted and Rust guards each establish exact unchanged inventory 100/96;
#142 concurrently changed data only within its existing four names and kept
its next object-addition boundary outside these intervals. Each owned proxy
was stopped before releasing its interval.

## Thresholds, NULLs and duplicate aggregation

After the first vertical was accepted, a second fresh full UID/body nonce
established a complete independent twelve-cell history. Its first ERROR/WARN
pairs reproduce the earlier finding as causal baselines. Each following
configuration is submitted once and then repeated unchanged:

| severity / fail_calc / error_if / warn_if | Raw result `(failures,error,warn)` | First / repeat status; raw exit |
| --- | --- | --- |
| ERROR / count(*) / >0 / >0 | `(3,true,true)` | fail / fail; 1 / 1 |
| WARN / count(*) / >0 / >0 | `(3,true,true)` | warn / warn; 0 / 0 |
| ERROR / count(*) / >3 / >0 | `(3,false,true)` | warn / warn; 0 / 0 |
| ERROR / count(*) / >3 / >3 | `(3,false,false)` | pass / reused; 0 / 0 |
| ERROR / count(v) / >2 / >0 | `(2,false,true)` | warn / warn; 0 / 0 |
| ERROR / sum(v) / >3 / >0 | `(4,true,true)` | fail / fail; 1 / 1 |

Every first declaration BUILDs and successfully confirms its complete typed
result; every repeat speculatively SKIPs and echoes that result and runtime.
A passing threshold result can legitimately retain failures=3. NULL is
excluded from count(v); both duplicate v=2 values contribute to sum(v)=4.
All complete literal row/schema and separate aggregate oracles pass, as do
12 State-disabled selected tests and 12 compile prechecks with certified zero
RPCs. Their raw logs and numeric exits remain retained.

Wire deltas change only `semantic_extras` throughout this history. The exact
transitions are: severity; severity plus error_if; warn_if; fail_calc plus
error_if plus warn_if; and fail_calc plus error_if. Rendered SQL, raw body,
all node hashes, context, scoped full UID and namespace remain unchanged.
The threshold-warning transition changes two configuration fields and the
aggregate transitions change several; they are not isolated single-field
invalidation claims. Per-cell `request-deltas/` stores the full byte-value audit.

The complete hosted archive is `config-hosted-thresholds/`, with 36 dbt
invocations and 72 certified RPCs. Immutable fixture
`golden/fixtures/databricks_cached_test_thresholds.jsonl` has SHA-256
`6b69a51aea6eeafebc628fd6e480a6ef2378fb0df5688f0d594ee93c76f16e2d`.
The independent no-object guards again establish exact 100/96 equality and
stop every owned proxy before releasing the coordinated interval.

True-template failing-cache transitions, unit-test caching and failure audit
objects are outside this new configuration evidence. The audit-object work
remains #144. #138's known cross-full-UID conflict remains unresolved and no
fixture relaxes that safety floor.

The twelve-cell primary trace also replays exactly through the public production
router: all 116 hosted corpora pass in 206.72 seconds, including its 60 complete
non-Explain assertions and twelve complete trees, without new normalization or
exceptions. The pinned real-client Rust matrix on `facbe845` passes in 107.22
seconds with the same decisions, raw exits/statuses, typed payloads, references
and confirmed-runtime echoes. Its certified 72-RPC capture in
`config-rust-thresholds/` has SHA-256
`3c0d7038515bbb61f0cafe1fc2f68b1d9dbe55aa3a6cc0715e959bc37ed04d8d`.
Launch sources and executables are retained in
`config-threshold-current-source-bin/` with a receipt written before actual
use. The exact inventory again remains 100/96, all owned proxies are stopped,
and no further warehouse operations follow this interval.

The final configuration runner SHA-256 is
`730d0a16796bb73dfa414de79cc26ea9be0166abd35510fe9d413db024a9837a`,
byte-identical to both twelve-cell live archives and the four-cell Rust archive.
The first four-cell hosted runner predates metadata-only operation-manifest
and entrypoint-provenance writes; its original source remains archived. The
shared guarded base runner SHA-256 is
`791e602c59dbe08d3bf8aa154c9eec12708758b9023009807df8b825e2814109`,
byte-identical across all four live archives.

All 18 credential-free Python groups pass in the dedicated persistent Python
environment; the final configuration fault group additionally reruns after
expanding status/count mutations. Strict workspace/all-targets/fuzz Clippy,
310 library tests with an invalid DATABASE_URL, formatting and whitespace checks
pass. The pre-sync tracked Explain census is 795 trees / 127 files, with zero
empty or unmapped trees or unspecified enum values; INFO remains 59 lines.

No new DuckDB campaign was run for this helper/test-only delta. At `facbe845`,
production server/proto/protocol sources, Cargo inputs, harness implementation,
DuckDB integration and gate scripts are byte-identical to `1233907`; the prior
final-source 25/25 gate documented in the linked cached-failure report applies.
This does not claim a fresh execution or waive a later production change.

Final workspace validation on synced integration `facbe845` passes 663 tests,
zero failures and 43 opt-in ignores. It includes all 116 hosted corpora and the
supplementary complete 72-RPC Rust threshold trace; the full conformance group
passes in 208.98 seconds. The current integration tip was merged naturally
before these final gates and remained `facbe845` at handoff. A later FULL-only
Explain change must receive its own source review and exact replay attribution;
these receipts do not claim it was used in this live campaign.
