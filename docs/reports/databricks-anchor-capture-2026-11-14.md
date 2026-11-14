# Databricks Explain anchor capture ordering — 2026-11-14

This additive report corrects a response-visibility race discovered during
review of `b8821b698dc8e2d01504d2b2ceb076088076ec03`. The original
[target lifecycle report](databricks-target-lifecycle-2026-11-14.md) and its
immutable live evidence remain historical evidence for that earlier source.
Neither that live binary nor its old DuckDB gate contains this handler fix.
#141 remains incomplete and blocked by #140; #132 is not promoted.

The independently reproduced public regression submits the actual captured
physical-source request (fixture entry 265), confirms its execution, and
submits the literal-view transition (278). A selective BEFORE INSERT fault
holds only the first request's audit, scoped to organization and exact target;
the later nonempty-dependency audit is independently observed complete.
Before the fix, the later decision's first Explain reports a changed outer
query and no upstream dependencies. Repeating the same Explain after the
prior audit completes produces the captured hosted literal-view tree.
The exact imported reproduction fails in 1.19 seconds. The adapted test,
which releases the held audit 400 ms after starting Explain, fails in 1.58
seconds and passes after the visibility fix in 1.67 seconds. Full tree equality
and the existing complete-response hosted replay oracle establish the expected
display; no hosted policy is inferred from the fault.

In-memory capture tracking now distinguishes decision IDs from request IDs
and scopes both by organization. For the new Databricks FULL
literal-dependency display, Explain waits only for missing auxiliary evidence belonging to its exact
matched/unmatched execution request, then unconditionally rereads the requested
decisions. Rereading even when the pending entry has disappeared closes the
completion/removal race between the initial read and registry lookup. There is
one bounded dependency pass, no polling, no organization-wide capture barrier,
and no older logically matching audit lookup. Submit and Confirm remain
nonblocking on capture. No migration, execution identity, decision policy,
fingerprint, producer, State history selection or Result copy changes.

The regression also explains a persisted unrelated decision, a foreign-org ID
and an unknown ID while the selective anchor hold remains active. They return
without waiting. Existing current-decision visibility controls and legacy,
malformed, null, deleted and foreign-org auxiliary-audit restart controls remain
part of the public gates.

Final validation is performed on the union with the separately characterized
#142 display-only formatter patch. Its provenance is recorded separately; the
patch changes only Databricks minute-gap and relative-minute/hour display
rounding. The final bounded adjacent minute check adds four unfiltered primary trees: 90 and
151-second controls, plus distinguishing 150/270-second ties. The final
complete replay contains 121 corpora; its Explain census contains 920 trees
in 132 files. #142 remains partial under its #138/#140 blockers.

The [companion JSON](databricks-anchor-capture-2026-11-14.json) records final
source hashes, command outcomes and elapsed times. This local public replay
and physical acceptance evidence does not represent a new #141 live warehouse run. No warehouse operations or objects
were requested or created for this repair.

The frozen combined source is
`af6937bee4e1d6f7c5c5422e5fe967100d04fb12`, including canonical test-only
integration `f6a5e973677b901bd5dd1a8dfdb3763779570622`. All 457 recorded source
files match across its public replay, current-source visibility/formatter
controls, invalid-database library check, strict Clippy and formatting gates.
The public gate passes all 17 tests over 121 corpora in 237.99 seconds wall
time. The visibility/formatter gate passes eight tests with one live test
ignored; the library gate passes 310 tests with an invalid database URL.

The repair archive is
`~/.local/state/dbt-state-rs/databricks-141-capture-fix-20261114`. Its
`pre-minute-control/` boundary preserves the prior 120-corpus green candidate,
initial strict-lint failure and test-only correction, initial missing-PyYAML
setup failure and isolated pinned dependency correction, and the imported
review reproduction and red-to-green proof. Final receipts and the new
DuckDB artifacts are recorded separately. Original #141 campaign artifacts
and reports are preserved unchanged.

The full workspace gate passes **675 tests with zero failures and 45 ignored**
in 617.43 seconds. Its 457 recorded source hashes match all final preflight
gates. This includes the complete hosted replay, current-source capture
visibility, auxiliary-audit compatibility and imported #142/#143 controls.

The fresh locked-environment DuckDB gate passes **25/25 cases** in 894.06
seconds, including fixed real-client cases and generated physical-state traces
with independent-oracle mutation probes. Its complete source hash set matches
the other six final gates. The archive retains the build output, server/test
executable hashes, individual case logs, timing report and oracle output.
This is the one final combined-source 25-case run, not reuse of the original
#141 binary's earlier gate.
