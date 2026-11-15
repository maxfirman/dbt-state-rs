# Experimental Databricks copy on SQL submission

`DBT_STATE_EXPERIMENTAL_DATABRICKS_SUBMIT_COPY=true` separately enables a bounded
ordinary `SubmitEnrichedSQL` Result copy. The default is false. Embedded servers
use `AppState::with_databricks_submit_copy(true)`. The direct-copy switch continues
to enable only RegisterClone. Disabling either switch prevents new grants, while
valid previously confirmed copy history can still qualify for reuse.

The checked subset is dbt 2.0.11 FULL models on quiescent managed Delta, with
exact fully-qualified `SELECT * FROM catalog.schema.input`, one genuine input,
`lookback=1`, `on_schema_change="ignore"`, no dependencies/templates/extra relation
properties, zero lag, ANY policy and external-modification checking. Automatic
requests require `allow_clones=true` and chain limit1. Model identity binds the
exact rendered SQL, catalog, node kind, runtime extras, full UID and client
body/config/macro/contract signatures under `databricks-submit-copy-v1`. Only the
output schema is omitted, because every read in this subset is fully qualified.
This identity is distinct from RegisterClone's unchanged direct-copy v1 identity.
The output catalog and relation name remain bound as well.
The UID and exact-input restrictions are conservative limits on automatic copy,
not new identity requirements for ordinary model or snapshot builds.

Source history must be current in the same organization, warehouse family and
nonblank connection namespace. A pending source write, a newer incompatible
source, unknown/nonpositive input or source epochs, unknown/changed producers,
and source copies prevent a grant. This subset requires a known NoBuild producer
for its single input. The source's recorded input epoch must equal the request's
current observation. There is no unjudged copy from another input or automatic
chain. MERGE builds normally and, within the measured fresh-equivalent scope,
reports rejection1/6. Snapshots, seeds, views and other SQL/configuration shapes
are outside automatic-copy eligibility.

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
clocks, and focused regressions cover the corrected branch under both switch
settings. No successful native command or warehouse write was repeated.

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
client/platform failure even with State disabled. Chains and failed-copy fallback
remain separate work. This bounded feature is not full ADR0009 conformance.
