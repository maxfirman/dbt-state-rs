# Databricks snapshots

The bounded snapshot scope is dbt 2.0.11 on the characterized serverless Unity
Catalog workspace with managed Delta tables, scalar unique non-NULL `id`,
`updated_at='ts'`, and `hard_deletes='ignore'`. Timestamp strategy and check
strategy with `check_cols=['v']` use exact source timestamps. This feature scope
is separate from full ADR 0009 hosted conformance.

The original timestamp/ignore history covers twelve managed and twelve native
State-disabled reference executions per backend, including updates/inserts,
ignored deletes, reappearance, equal timestamps and a SQL change. Its
[retained report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-snapshot-timestamp-ignore-2026-11-15.md)
remains unchanged.

The source-clock supplement in `experiments/databricks_snapshot_modes.py` adds:

- Check changes through non-NULL → NULL → non-NULL, retention of the complete old
  business row after a change to unchecked columns, ignored deletion and
  reappearance, and unchanged positive reuse.
- Timestamp changes at older, equal and advancing times, including a one
  microsecond advance and trailing fractional zeroes in native hash formatting.
- A real FULL model that creates the entire valid SCD schema, followed by a
  native SNAPSHOT on the same alias and ordinary snapshot reuse. This tests an
  actual usable SCD relation rather than relabeling an incompatible model table.

Each backend has one fresh source and four independent snapshot targets:
managed/reference pairs for each strategy. References execute with State disabled
at each BUILD checkpoint. The source is immutable between a managed execution
and its reference. The complete reference history is also checked at every
SKIP checkpoint. Neither managed nor reference targets supply expected rows.
The predeclared source ledger independently determines every business value,
SCD hash, updated_at, valid_from, valid_to and NULL. Native `date_format` with
six digits and `unix_micros` cross-check full timestamp precision. Complete
ordered column types, row bags, managed Delta properties and histories are
checked; a no-op MERGE need not create a Delta version. SKIPs retain their own
confirmed runtime, source epoch, target epoch and complete Delta history.

The pinned `dbt.snapshot_check_strategy` macro chooses configured `updated_at`
before `snapshot_get_time`. Actual emitted CTAS and staging DML must use `ts`
for updated_at/from and hash identity. A read-only schema inference query may
mention `current_timestamp()` with `WHERE FALSE LIMIT 0`; this does not feed the
SCD mutation. The guard rejects generated clocks in mutating CTAS/staging/MERGE
SQL and requires the explicit source-clock aliases. Native macro overrides and
fake live clocks are not used.

The complete public Submit/Confirm/Explain histories are replayed without
fixture masks or accepted prefixes. Native replay uses actual organization-scoped
State audit clocks retained before the isolated test schema is dropped. Those
clocks establish relative-age Explain fidelity; they never stand in for
warehouse SCD timestamps. Credential-free modeled public tests additionally
reject reuse with unknown or changed source metadata, an externally modified
or different Target relation, another connection namespace or organization,
and an unconfirmed snapshot outcome. Independent oracle fault probes reject
wrong business rows, lost historical rows, incorrect hashes and microseconds.

The only production change is a bounded rejection diagnostic. A BUILD may
report another confirmed snapshot with the same rendered query/type and identical
current source epoch/producer using `TARGET_TABLE_MISMATCH` /
`EXECUTION_TYPE_NOT_FULL` (1/3). The lookup keeps organization, warehouse family,
exact nonblank connection namespace and latest physical history boundaries.
It requires the direct-source, zero-lag, scalar-key captured request shape and
known source epochs without a recorded producer. It changes only those two
rejection fields. BUILD eligibility, own freshness/Explain anchors, pending and
confirmed history, and disabled automatic copying remain intact.

The hosted supplement is a byte-exact chronological composition of three
certified sessions (15 + 14 + 97 RPCs). Two harness postprocessing checks stopped
after successful commands: a fixed `consumer` label assumption and a blanket
clock ban that included a read-only schema probe. Both corrections continue the
same graph and State history, retaining original statuses, source snapshots and
certificates. Eight completed CLI calls and three Explain calls are reused;
no successful managed execution or source mutation is repeated. The initial
FULL physical observation belongs to its original session; later postprocessing
observes the identical SCD bag after successful no-op snapshot adoption. The
whole 126-RPC fixture has SHA-256
`99690ba641fde28d8950bd42becbf6900f543f05228b7be8257ec84443a0ee58`.
Retained secret-free source, native SQL, physical histories and composition
receipts live under
`~/.local/state/dbt-state-rs/databricks-151-modes-20261115`.

## Compatibility limits

Only ignored hard deletes are included in this bounded implementation. Check
strategy without an explicit `updated_at` and invalidating hard deletes use
warehouse-generated statement clocks; complete independent clock/hash fidelity
has not been established. API millisecond displays and client query start/end
intervals do not prove the exact native microsecond clock. Target-derived hash
inversion is circular and cannot supply an independent expected timestamp.

Both timestamp and check `new_record` controls with State disabled emit NULL
business columns in deletion rows with the pinned client. This is an unsupported
client capability, not valid expected deletion semantics or a State policy fix.
The retained primary staging SQL and native Column-versus-tuple macro evidence
remain in the original capability survey/preflight.

The check ledger advances `ts` on every change to `v`; changed check values at
an equal timestamp, and the resulting SCD hash collision, are unverified.
NULL/duplicate keys, NULL updated_at, schema evolution, alternate check column
sets, renamed SCD columns, current-validity sentinels and Result copy are outside
this supplement. Existing tests and captures remain intact. These limits do not
claim all reachable snapshot modes or full ADR 0009 conformance.
