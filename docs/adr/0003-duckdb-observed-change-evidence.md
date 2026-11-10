# Use observed changes as DuckDB freshness evidence

## Decision

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

Amended by ADR 0007: Lag tolerance needs elapsed time, which ordinals do not
give, so DuckDB Build age is measured from the server's receipt of the build's
confirmation.

Known successful builds and copies must advance their recorded write evidence
even when they produce identical contents. An unchanged scan must retain its
previous evidence. A digest is not itself a modification epoch. The ordering
and recovery mechanism is described in [DuckDB evidence](../duckdb-evidence.md).

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
