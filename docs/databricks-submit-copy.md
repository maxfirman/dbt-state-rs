# Databricks copy on SQL submission

Ordinary `SubmitEnrichedSQL` Result copy is enabled by default for the bounded
FULL subset below. There is no separate server switch. Hosted-compatible client
copy permissions remain effective; see [client controls and accepted limitations](databricks-copy-preview.md).

The v2 checked subset is FULL models with explicit projections, null predicates,
searched CASE, ordered INNER/LEFT joins, nonrecursive CTEs and grouping/HAVING
with checked numeric expressions. Multiple physical inputs and supplied query
dependencies bind exactly, including each definition's catalog/schema context.
Only the output schema is omitted after resolving every read independently of
it; physical input schemas, output catalog/name, UID and body/config/macro/contract
signatures remain bound. The SQL tree and rendered text retain output names,
order, bags and LEFT null extension; there are no algebraic rewrites.

Unknown physical column types may pass through or participate in null tests.
Numeric literal casts must be lossless and in range. Arithmetic admits checked
literal expressions whose intermediate values also fit INT; it does not infer
numeric physical columns or assume ANSI/timezone defaults. Named physical
columns require complete SQL table-alias column lists; unknown column sets may
resolve missing names to session variables and cannot prove presence. Checked aggregates
require explicit `system.builtin.count`, `min`, `max` or `sum` resolution. COUNT
preserves its documented BIGINT result; MIN/MAX require checked integer input;
SUM currently admits only provably zero integer expressions, avoiding unbounded
overflow. Other functions, unknown coercion/casts/arithmetic, windows, sets,
recursive queries, table functions and unreviewed syntax remain opaque. Decimal
casts retain precision/scale; decimal arithmetic remains opaque without its
rounding/precision context. String
literals also lack proof without their [session/object collation](https://docs.databricks.com/aws/en/sql/language-manual/data-types/string-type);
a physical string column retains its bound input type. Division currently remains
opaque.

[Databricks routine resolution](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-name-resolution)
can resolve temporary routines before builtins through SESSION PATH in SQL and
Runtime 19+. Bare aggregate names therefore lack copy proof without resolution
metadata. [COUNT](https://docs.databricks.com/aws/en/sql/language-manual/functions/count)
returns BIGINT and distinguishes row counting from null-excluding expression
counting. [SUM](https://docs.databricks.com/aws/en/sql/language-manual/functions/sum)
and [ANSI arithmetic](https://docs.databricks.com/aws/en/sql/language-manual/parameters/ansi_mode)
have overflow/error behavior that cannot be inferred from a config hash.

Automatic requests retain `lookback=1`, `on_schema_change="ignore"`, no templates
or additional relation properties, zero lag, ANY policy, external modification
checking, `allow_clones=true` and chain limit1. Source history must be current
in the same organization, warehouse family and nonblank connection namespace.
Every genuine input must retain its exact positive epoch and matching complete
NoBuild or recorded Build producer. Unknown/changed producers, missing epochs,
pending/newer source writes and source copies prevent a grant.

Ordinary source writes persist a separate versioned `analyzed_copy_proof` with
identity, physical/dependency bindings and checked effect domain. Automatic FULL
identity wraps that proof in `databricks-submit-copy-v2`; it never reinterprets
v1 hashes. Confirmed or pending v1 automatic origins cold-miss v2 adoption even
if confirmed after upgrade. Direct copying now uses the same shared proof with
its own `databricks-direct-copy-v2` origin. Ordinary legacy strict reuse is
unchanged. No old source SQL is reconstructed and no legacy proof is backfilled:
a fresh ordinary source write must record v2 evidence before broader copying.
Both copy paths are enabled by default. The [focused shared-copy acceptance](databricks-shared-copy.md)
separates the new evidence from the original native campaigns.

The pending copy freezes the source execution, physical name, epoch and genuine
input evidence. Confirm checks that source again, including pending writes;
invalid publication prevents both strict SQL reuse and namespace adoption. The
source build epoch remains the lag-tolerance age anchor: a recent copy cannot
renew an old build's tolerance. Physical copy completion retains its own epoch.
Existing direct-copy provenance cannot fall through into automatic adoption.

The pinned native configuration sets `state.pre_clone: never` for every node,
source-profile `allow_clones: false` without a defer target, and development-profile
`allow_clones: true` with `defer_to_target: prod`. The hosted primary campaign
completed twelve managed commands and twelve independent read-only native/literal
references: FULL copied into absent and existing development targets, then SKIP;
MERGE built in both positions, then SKIP. Source updates rebuilt the production
models. Complete schema/bags, Delta history, UC provider/type, executed DDL and
all default properties were checked. SQL Submit has a single ready_to_clone
oneof; RegisterClone's duplicate v1 mirror does not apply to this flow.

Rust physical acceptance used production library301de04 for the same twelve
managed commands and twelve references, followed by all eight controls. Its
complete whole91 capture retains twenty actual persisted decision clocks. A
subsequent narrow guard correction preserves ordinary same-target UID/config-only
reuse; the fixed-metadata native graph did not exercise that changed branch.
The complete native outputs are replayed on the final library with their actual
clocks, and focused regressions cover the corrected branch. (They covered both
settings of the since-removed server switch when it existed.) No successful native command or warehouse write was repeated.

The exact primary whole75 corpus is
[`databricks_submit_copy.jsonl`](../golden/fixtures/databricks_submit_copy.jsonl).
A separately authorized eight-control phase retains its complete whole91 history
in the [boundary characterization](../crates/harness/tests/fixtures/databricks_submit_copy_controls.jsonl).
Six crafted requests (SQL/config change, input drift/unknown epoch, volatility,
external target modification) BUILD without copying; two unchanged controls SKIP.
These calls execute no DDL or Confirm, and complete physical bags/history remain
unchanged. The local public regressions assert those decisions and the retained
hosted/local differences; they do not claim exact whole91 hosted replay.

Two boundaries remain explicit. Hosted RAND evaluation returns a sampled
`transformed_nodes_by_query` value and additional Explain lines; this server
conservatively BUILDs without rewriting volatile SQL. The unknown-input control's
hosted display uses the second containing the captured client submit, while its
UUIDv7 decision instant crosses into the next second (119ms later). This server's
replay uses the evidenced decision instant, so that one display differs by1s;
no timestamp is guessed or comparison field masked. Neither boundary grants copy.

As with direct pre-copy, an external physical source change during unversioned
SHALLOW CLONE is unobservable at Confirm. These managed, serialized experiments
require quiescent sources and do not establish universal concurrency safety.
FULL/seed ordinary replacement of an existing managed shallow clone has a pinned
client/platform failure even with State disabled. The
[copy recovery supplement](databricks-copy-recovery.md) establishes actual
source-epoch/missing-source fallback, chain refusal and subsequent native history;
positive chains remain unsupported. This bounded feature is not full ADR0009 conformance.
