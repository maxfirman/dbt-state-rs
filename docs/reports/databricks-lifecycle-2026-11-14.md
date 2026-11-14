# Execution publication and physical history, 2026-11-14 (#158)

This candidate prevents failed, incomplete or delayed work from replacing valid
physical reuse evidence. The hosted captures precede implementation. Eleven
complete sessions (245 RPCs) replay through the public Rust gRPC service with
full responses, status details, optional presence and Explain trees intact.
**#158 remains INCOMPLETE.** This validates the physical lifecycle slice;
untargeted FULL/VIEW logical qualification and reachability remain blocking.
The broader Databricks support gate remains open.

## Characterized contract

Fresh Databricks and Snowflake controls establish these shared responses:

| Operation | Observable result |
| --- | --- |
| Actual dbt warehouse failure | No Confirm; a repeated attempt still builds |
| First valid Confirm | Publishes once; echoes the submitted request ID |
| Exact or conflicting duplicate Confirm | INTERNAL 13, `Database operation failed`; original runtime/results remain |
| Unknown valid UUID | NOT_FOUND 5, `Execution request with ID {submitted_id} not found` |
| Empty/malformed UUID | INTERNAL 13, `Database operation failed` |
| Uppercase/simple/braced UUID | Correlates to the original request; preserves spelling in the response |
| Physical outcome without completion epoch | INTERNAL 13; pending remains unpublished; valid retry succeeds |
| Absent target with an outcome epoch | INTERNAL 13 |
| Record with missing input | INVALID_ARGUMENT 3, `Record at index N has no input set`; whole batch unpublished |
| Invalid target/epoch pair in Record | INTERNAL 13; whole batch unpublished |
| Explicit empty SQL target on these two families | INVALID_ARGUMENT 3, `Failed to process SQL` |
| Empty Record batch | Success, zero records |

A clone-fallback flag is a successful fallback outcome, not an execution-failure
flag. Local DuckDB uses the shared immutable publication/status behavior; these
controls do not claim hosted DuckDB engagement.

Delivery order does not establish physical completion order. Hosted controls
use different SQL/identities and both confirmation orders. Independent actual
Databricks writes verify the relation contents: the greatest completed target
epoch remains current when its confirmation arrives first. Equal epochs use
confirmation receipt order. Physical current-row selection, upstream producers
and copy-source selection share this ordering. Logical data-test/UID deferral
history and bounded historical copy-provenance walks keep their own semantics.

A confirmed legacy row with an unknown completion epoch conservatively prevents
physical reuse and copying. It remains a cold miss after late confirmations or
new valid deliveries because those events cannot prove warehouse replacement.
The floor applies even with `ignore_external_modifications=true`. Genuine frozen
pre-change v2 rows retain their matching identity, and separate current-version
storage-corruption probes prevent a future identity-version bump from masking
this safety check. Historical fixtures are upgrade tests, not hosted captures.

## Public and warehouse validation

The public lifecycle suite covers exact errors, correlated UUID spellings,
rejected-batch atomicity, valid retries, duplicate immutability, pending/confirmed
persistence across a real router restart, frozen legacy histories and current
storage faults. It uses production routing and isolated Postgres databases.

The pinned actual-client campaign uses dbt 2.0.11, serverless Unity Catalog and
managed Delta. Only exact fresh `l_<run>_...` names in the leased `cdo_demo_db`
containers are writable; no schema creation or existing-object mutation occurs.
Typed complete rows preserve duplicates and NULLs; columns, table type and Delta
provider are checked against State-disabled dbt and independent SELECT results.
Actual dbt invocations and crafted public-RPC controls are labeled separately.
The public `state-probe` requests each complete Explain tree using correlated
decision IDs from those submits.

The fresh Rust campaign passed nine dbt invocations, nine required reuses and
70 complete RPCs, including exact duplicate/atomic errors and delayed history.
The first Rust attempt is retained as incomplete: seven actual-client cells
passed before the crafted Record driver omitted an explicit protobuf default
and failed locally without sending that RPC. The corrected driver sets
`from_speculative_submit: false`.

The merged #159 v2 dependency passes the genuine frozen Databricks known-epoch
positive control. All 12 local lifecycle tests pass, including matching-version
legacy histories and current-version storage faults. The complete workspace
gate passes 628 tests with zero failures and 34 opt-in cases ignored; it includes
exact replay of all 99 sessions and 310 server-library tests. Strict workspace,
all-targets, fuzz-feature Clippy passes. All 25 actual DuckDB fixed/generated
acceptance cases pass with their existing publication expectations preserved.
The final merged-source pinned Rust campaign also passes: nine invocations,
nine required reuses and 70 certified RPCs. The final capture SHA-256 is
`6129add3dbcaf73f2bb4188d4271604692786f799c0291e3b96dc514fc8e8e6c`;
its source/binary provenance and independent physical checks are retained in
`rust-20261114a158c6`. All 25 credential-free Databricks Python oracles and
format/diff checks pass.

The retained failed full runs exposed outdated test setups: fresh synthetic
non-UUID correlations in two frozen clone tests, a targetless data-test outcome
with a physical epoch, and duplicate-success expectations in deferral and
non-finite-result tests. Only the contradicted setup/status assertions changed.
Frozen fingerprints, all upgrade cells, required reuse, confirmed FQN isolation
and complete cached-payload equality remain asserted.

## Retained limitations

Crafted FULL/VIEW requests with `target_table: null` expose separate hosted
logical reuse. Pinned read-only dbt show/operation probes did not send those
execution RPCs. Their logical qualification and isolation are unresolved; the
existing non-test absent-target guard remains. Unknown physical epochs cannot
be copy sources; absent-target histories cannot be relation mappings. These
diagnostic captures remain archived
outside the promoted physical fixtures, with no replay exemption or blanket
acceptance claim.

The parent copied-target rebuild, snapshot deletion and #140 safety blockers
remain unchanged. This candidate does not establish overall Databricks support.

Evidence is retained under
`/home/maxfirman/.local/state/dbt-state-rs/databricks-158-20261113`.
`promoted-physical-fixtures.json` pins original capture paths and SHA-256 hashes;
`capture-index.json` retains discovery, failed attempts and accepted final
runs. The companion JSON report pins all check logs and capture hashes.


The final required commands were `cargo test --workspace --no-fail-fast`,
`cargo clippy --workspace --all-targets --features fuzz -- -D warnings`,
`./scripts/duckdb-ci.sh --jobs 4 --artifacts <durable>/duckdb-final-v2`, the
ignored `databricks_lifecycle` real-dbt acceptance with the pinned executable
and ownership lease, the 25 `experiments/databricks_*_test.py` unit cases, and
`cargo fmt --all -- --check`. Workspace artifacts were cleaned at the exclusive
cross-worktree handoff; immutable binaries and source manifests preserve the
accepted provenance.
