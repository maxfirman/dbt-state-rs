# Use observed changes as DuckDB freshness evidence

## Implementation status — 2026-11-07

Versioned schema/content fingerprints, ordered evidence, invocation locking,
generation recovery and explicit reset are implemented. The
[integration notes](../../integrations/duckdb/README.md) describe their current
mechanics and supported evidence types. The
[recovery acceptance case](../../crates/harness/tests/duckdb_recovery.rs) and
[interruption acceptance case](../../crates/harness/tests/duckdb_interruptions.rs)
exercise recovery through real dbt processes.

The validation work anticipated below now has those concrete checks. The
observed-change contract and explicit reset requirement after backup restoration
remain in force.

## Original decision

DuckDB freshness may be based on observed differences in relation contents and
schema, together with known writes performed by the integration. A manual
write that leaves identical observed contents and schema need not invalidate
downstream reuse. Scans cannot detect every intervening write or a change
reversed before the next observation; supporting arbitrary between-run edits
does not promise a complete write history.

This is an intentional evidence difference from Snowflake, whose modification
timestamps can invalidate reuse after an identical-content rewrite. Preserve
the shared server freshness policies while documenting the DuckDB evidence
contract. Unknown or unsupported evidence causes a rebuild. This difference
does not change Snowflake behavior or establish hosted conformance for DuckDB.

Known successful builds and copies must advance their recorded write evidence
even when they produce identical contents. An unchanged scan must retain its
previous evidence. The detailed ordering and recovery mechanism still needs
validation; a digest is not itself a modification epoch.

Persist versioned content/schema fingerprints, ordered evidence and a database
generation identifier in a reserved metadata schema inside the DuckDB file.
The warehouse namespace must also distinguish separate file copies at
different paths; an embedded identifier alone is insufficient. Normal
restarts retain evidence. Missing, incompatible or uncertain metadata must
prevent reuse, with a fresh generation where needed.

Provide an explicit reset operation that starts a new evidence generation and
invalidates prior reuse eligibility. Restoring an old database backup requires
this reset before State is used again. Automatic detection of arbitrary backup
restoration is outside the initial support contract. This trades an explicit
local recovery step for avoiding another metadata service and the illusion
that a restored in-file clock can reliably detect its own rollback.
