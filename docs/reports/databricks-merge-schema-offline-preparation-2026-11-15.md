# MERGE schema addition and failure/recovery: offline preparation

This is credential-free runner preparation for part of #145 under #132. It does
not establish hosted behavior, warehouse capability, native acceptance or fixture
coverage. #145, #138 and #140 remain open. Existing MERGE baseline and settings
runners and all production decision code are unchanged.

The separate `experiments/databricks_merge_schema.py` runner describes eight
managed cells: first build/reuse, two deliberate schema failures, append-column
recovery/reuse, and a later same-schema delta/reuse. Source keys are unique and
non-NULL. Its complete literal source/R/F ledger distinguishes the failed C3
Target from the current C4 full evaluation. Recovery preserves the untouched
key 2 with NULL note; the later delta preserves source-missing key 1. Complete
row multiplicity, ordered API schema/types, available nullable metadata, managed
Delta properties and full history are checked. A current full evaluation cannot
stand in for the cumulative incremental Result.

The external ledger file is mandatory and compared in full with the independent
literals and the root grant digest before credentials or campaign initialization.
The explicit machine contract reserves two final relations per backend, exact
session/persistent auxiliaries, eight managed invocations, one disabled parse,
eight isolated disabled show references and an 80-RPC ceiling per backend. It
allocates no schema and permits no cleanup. A fresh root inventory, capacity,
allocation identity and separate backend grant are required for any live work.

Campaign and lifecycle failure handling are composed without relaxing successful
client runs. Failed cells preserve artifacts, complete calls and observed status,
then compare unchanged Target rows/schema/properties/full history and prohibit
Confirm/Record/RegisterClone. Reference execution uses the canonical isolated
zero-RPC canary with a schema-aware validator and typed literal SQL.

The runner deliberately stops on unknown failure engagement, client exit or
diagnostic, RPC status, and recovery SQL. No numeric failure exit, transport
shape or ADD/MERGE spelling is invented. An absent reviewed schema diagnostic
contract stops after retaining the first failed checkpoint and available Explain.
Recovery SQL, if reached after a separately reviewed contract, is retained with
physical/reference evidence and stops before the next invocation. That path
needs actual hosted-derived SQL validation before completing this graph. Native
execution remains explicitly refused until a whole certified hosted history,
including both failure intervals and all status/Explain traffic, has passed
strict public replay and supplies the actual native lock. The ignored Rust
wrapper retains process provenance and native audit clocks before teardown when
that future path is made available. It is not current live authorization.

Offline probes use synthetic external artifacts and executables, never
approximate hosted fixtures. They reject failed-state schema upgrades, missing
retained rows or NULLs, backfill, duplicate multiplicity, fresh-F substitution,
schema/type/order drift, altered failed Target history, unrelated nonzero exits,
publication after failure, mismatched Confirm IDs/epochs, altered external
ledgers and extra reference RPCs. Run them with:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 experiments/databricks_merge_schema_test.py
cargo test -p dbt-state-harness --test databricks_merge_schema
python3 experiments/databricks_merge_schema.py --proposal
```

No warehouse, Cloud, State service, dbt client or fixture promotion was used for
this preparation. Live settings acceptance, schema/drop/type breadth, refresh
configuration, ambiguity cases, generated lifecycle, restart/upgrade and native
identity/freshness dependencies remain required work.

Validation on the preparation branch: 11 focused synthetic probes pass, all 27
MERGE helper probes pass, the Rust wrapper has one credential-free pass and one
ignored live test, focused harness-test Clippy passes with warnings denied, and
workspace formatting passes. No whole-workspace or fresh live acceptance claim
is made for this offline-only addition.
