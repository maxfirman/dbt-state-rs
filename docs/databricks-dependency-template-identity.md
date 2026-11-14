# Databricks dependency, template and configuration identity (#138)

**Safe partial; #138 remains INCOMPLETE for the [cross-UID test boundary](reports/databricks-data-test-uid-boundary-2026-11-14.md).**

This is the bounded contract captured with official dbt 2.0.11 on serverless
Unity Catalog and managed Delta, 2026-11-14. It adds no Result copy admission.
Parent #132 support remains blocked by the separately recorded copy, snapshot,
freshness, logical-target and namespace limitations.

Rendered identity binds compiled SQL, exact runtime `semantic_extras`, resolution
context and whole `(dependency name, definition, own catalog, own schema)`
bindings. Reordering whole bindings is neutral. Swapping definitions attached
to fixed names is different. Databricks drops only exact repeated whole wire
bindings before physical-name resolution; conflicting definitions/contexts and
mixed aliases retain the ambiguity guard. Unknown dependency epochs still
cannot prove freshness. Empty dependency definitions receive INVALID_ARGUMENT,
`Failed to process SQL`.

Template mode adds an alternative: declared body and config digests, optional
macro digest presence/value, exact template contexts and whole dependency
bindings. It does not replace rendered identity. The actual client reaches it
through project YAML `+state: {compare_unrendered_code: true}` and singular-test
`config(state={'compare_unrendered_code':true})`. The model setting can change
without changing model body bytes. The singular-test state config does not
accept the model's `lag_tolerance` key; that unsuccessful discovery is retained.

| Declared configuration signature | Runtime extras | Hosted decision, otherwise unchanged |
| --- | --- | --- |
| same | same | SKIP |
| same | changed | SKIP through the declaration alternative |
| changed | same | SKIP through the rendered alternative |
| changed | changed | BUILD |

Actual `on_schema_change: ignore` → `fail` changes both config digest and extras,
keeps body/SQL fixed, and rebuilds in rendered and template modes. Tags change
neither transmitted runtime extras nor State hashes in this client; this does
not establish that arbitrary node-state hashes are runtime inputs. Separate
crafted controls establish that node/contract/persisted-description hashes do
not enter the observed template alternative, while macro presence/value does.
Crafted controls establish protocol policy, not client reachability of every
field combination.

Both body and config signatures must be present and nonempty before a template
alternative is constructed. Missing evidence retains rendered-only matching.
The consumed provided signatures use captured 32-hex encoding; malformed or
explicit-empty ordinary-submit signatures return INTERNAL, `Database operation
failed`. Original strings remain stored as evidence. Hosted case controls
justify folding body/config hexadecimal case for matching; macro case remains
exact because only its presence/value was characterized. Explain compares the
original strings, so a case-only body/config change can report a changed string
while the decision still reuses an equivalent declared digest.

A source-less physical FULL execution recorded by the actual write-only client
retains complete body/config/macro declaration evidence for a later true-mode
submit. A false Submit does not create that alternative. Record retention stays
bounded to the captured physical FULL/source-less shape with complete valid
signatures; other Record shapes keep rendered identity. A subsequent true-mode
rendered change can therefore reuse that recorded declared result. References
explicitly use the earlier declared result, rather than accepting arbitrary old
or current rows.

The rendered/template same/changed truth table is captured separately, including
a pure inline Jinja comment that changes body bytes with byte-identical compiled
SQL. An appended Jinja comment also leaves an outer newline around a VALUES
query; rendered fallback strips only outer ASCII SQL whitespace. Unsupported
interior SQL remains exact. Neither change widens rendered dialect aliases to
template-only matches.

The fingerprint version is `dbt-state-databricks-sql-v4`. Genuine v3 rendered,
template and Record histories, including pending and confirmed exports from an
immutable pre-change binary, prove safe cold misses across the boundary. New
current-version positive controls prevent a version bump from hiding failures.
Snowflake and DuckDB fingerprint versions and behavior remain unchanged.

Explain stores raw comparison evidence and optional macro metadata, then derives
the tree against the decision's matched/unmatched row. Dependency display order
follows root references through complete bindings and each definition's own
context. Ambiguous/unknown traversal, more than 32 definition levels or more than 1024
relation visits keeps submitted order. This never reorders
physical State inputs or changes producers, epochs or freshness decisions.
Captured INFO and body/config/macro lines are catalogued in
[explain-catalog.md](explain-catalog.md#databricks-template-comparison-2026-11-14-138).

One refusal-only diagnostic distinguishes an absent target's current equivalent
from a superseded historical query. The former reports code 10 and the nested
clone limit; the latter reports code 9 and ordinary missing-table text. The
historical lookup returns only a boolean within the same organization, warehouse
family and exact named namespace. It supplies no reuse/copy source, producer,
epoch, cached result or freshness/Explain anchor. Physical selection remains
latest-row-first.


Ordinary and speculative provided-signature validation have separate hosted
controls and both reject captured malformed signatures with INTERNAL13. Absent
body evidence permits a proven rendered-only result; changing rendering without
a complete declaration leaves speculative submission UNDECIDED. UID and cached
result completeness gates remain mandatory in every route.
