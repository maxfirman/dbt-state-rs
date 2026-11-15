# Databricks State selectors

On 2026-11-15, pinned dbt **2.0.11** (SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`)
reached all ten `GetStateSelection` criteria on serverless Unity Catalog,
managed Delta in `cdo_demo_db`:

| CLI criterion | Wire criterion |
| --- | --- |
| `modified`, `new`, `unmodified`, `old` | 1, 2, 3, 4 |
| `modified.body`, `modified.configs`, `modified.relation` | 5, 6, 7 |
| `modified.persisted_descriptions`, `modified.macros`, `modified.contract` | 8, 9, 10 |

The two-model `root → child` campaign records every criterion before the
first build, after confirmed execution and unchanged reuse, and after body,
tag configuration, alias, macro, persisted-description, contract, effective
materialization configuration and external data changes. The existing selector
engine matched hosted; no production change was needed.

The complete selector responses and command artifacts establish these details:

- New nodes match every criterion except `old` and `unmodified`. Unchanged
  nodes match `old` and `unmodified`.
- Builds submit an absent `node_database_representation`; selector requests
  supply its qualified name. Consequently unchanged built nodes also match
  `modified.relation`, without matching `modified`.
- Body, macro, persisted-description, contract and effective configuration
  edits select the root under their respective component criteria. Adding a
  tag in SQL changes the body hash, while its configuration hash stays equal.
- `state:modified.body+` compiles root and child; `+state:modified.body`
  compiles only root. The RPC itself selects root in both cases: graph
  expansion belongs to the client.
- Updating only the owned root's data from integer `1` to `2` leaves
  `modified` empty and `unmodified` selecting both nodes. Selectors compare
  node state, independently of submit-time freshness decisions.

`compile --select state:*` requires an explicit `dbt-cloud.project-id`.
The retained native #134 baseline has no project ID and all attempted State
selections fail locally with `dbt1600`; it cannot supply this command baseline.
The fresh project ID receives the expected missing-local-cloud-project warning,
but its authenticated proxy calls succeed. `modified.sql`, `modified.shell`
and an unknown criterion fail locally with the client's explicit ten-item valid
list. `dbt ls --select state:modified` fails with “no comparison manifest” and
does not engage State selection. These failures are retained, not counted as
successful State command surfaces. No Explain RPC is emitted by these commands.

## Reproduction and evidence

`experiments/databricks_selectors.py --help` describes the opt-in driver.
It verifies the binary digest, requires a fresh UUID, inventories the existing
demo schema before writes, refuses name collisions and creates only
`s156_<uuid>_{root,child}`. SQL embeds the UUID and pre-cloning is disabled.
It preserves the source archive, exact ownership, commands, logs, typed physical
results, client manifests/compiled selections and gracefully certified captures.
The `certify` function correlates each actual invocation with its complete RPC
response and checks the independent two-node graph closure. Its physical checks
require complete integer rows/schema before and after the owned data update.

Artifacts are retained at
`~/.local/state/dbt-state-rs/databricks-156-20261115/{readonly,hosted,hosted-extension,rust}`.
Hosted UUID is `e725368c606b4a4e812d8f68be9655eb` in
`cdo_demo_db.ds_20261112a134ef_prod`; Rust UUID is
`42e051f5574044ff8dcdaac97a6c7f95` in `cdo_demo_db.ds_20261112a134ef_ref`.
Only those four exact UUID-prefixed tables are written; all are retained.
Hosted uses 88 primary and 20 extension commands. Rust runs the same 108
commands against an isolated production-router State server.

The fixture `crates/harness/tests/fixtures/databricks_selectors156.jsonl` is
the byte concatenation of the two **whole, unfiltered** certified hosted
sessions (394 + 89 RPCs), including telemetry and statuses. Source SHA-256s:

- Primary: `f8d13bc8cc43730557f60a8546f1ece5b8056d9eafb32bb6a6e3ec42d253249b`.
- Extension: `5d53543b5f9c3d1d9fd1dbd9bea3eb1955c8086d6671aea2f4268264f2d93638`.

`cargo test -p dbt-state-harness --test databricks_selectors` replays all
483 RPCs through public gRPC, comparing full responses/statuses/protocol
metadata after correlating lifecycle IDs. It includes 102 full selector
responses and their first-build submit/confirmation history; no raw State
database seeding or policy exemptions are used. The ignored
`real_dbt_selectors_use_rust_state` test runs the real client against Rust.

This completes the reachable criterion inventory and these command baselines
for #156. It does not establish every node-kind/graph/configuration combination,
additional client versions or the broader Databricks release acceptance #162.
