# Bounded Databricks timestamp-ignore snapshots — 2026-11-15

Hosted and fresh Rust windows pass the same 12-cell timestamp/ignore history.
Each backend ran 12 managed snapshots, 12 independent State-disabled NATIVE snapshot
references, one disabled parse, 6 Build/Confirm, 6 fresh own-runtime Skips and 12 Explain
requests. This is bounded acceptance, not completion of #151/#138/#140/#132.

## Results and boundaries

Complete literal SCD rows, original business values, MD5 IDs, updated_at,
valid_from/valid_to/nulls, ordered types/schema, multiplicity, source bags,
managed Delta properties and full source/target histories match in every cell.
Cases cover first build/reuse, update+insert/reuse, ignore-delete/reuse,
reappearance/reuse, equal-timestamp business mutation/reuse and SQL projection
change/reuse. Equal or older source timestamps do not create SCD versions.
A Build does not require a no-op MERGE to advance Delta version.

Every ordinary request is native SNAPSHOT7 with zero lag/ANY, compare_unrendered_code=false, direct
owned-source SQL and positive known physical epochs. Each Skip proves its own
accepted Confirm target epoch/runtime, unchanged source epoch/SQL/extras/
namespace and complete target Delta history; nested is_stale is exactly false.
References always execute native snapshots into their own independent source/
target pairs. No Record, Clone, persistent auxiliary, generated-clock strategy,
cleanup, fake live clock or unplanned object was admitted. Two sources and two
targets per backend remain: eight MANAGED Delta tables in the single owned 151
schema, with at most one exact session temporary view at a time.

Before each fresh window's proxy/DDL, Statement API date_format with six digits
plus unix_micros and current_timezone proved the independent UTC literal
2026-11-12 01:50:34.797584 exactly. Actual clock sidecars measure State audit
decision times only; they are never used to infer warehouse SCD/hash time.

## Original STOP and exact continuation

The original 672-file source 4dc06d5 completed parse and its first native CTAS,
ordinary Build and Confirm. Its runner then rejected successful telemetry and
a source-only unknown-epoch speculative Submit returning undecided. The raw
stopped summary, code/tool/archive freeze, all seven RPCs/statuses and certificate
remain unchanged. The narrow correction validates canonical method/service/
status/envelope/identity and permits that undecided speculative subset while
keeping the ordinary complete positive-epoch floor. Unknown or failed RPCs,
speculative reuse and duplicate speculative submits still stop.

The 679-file continuation source bc32f8b resumed first-cell postprocessing only.
Before proxy or any further mutation it verified exact three-object inventory,
allocation identity, both complete sources/history/original query IDs, original
source epoch 1794710691000, first target full SCD/schema/history/CTAS query ID
and own Confirm epoch 1794710706000/runtime8782. It then requested the original
decision's missing Explain, executed the first independent reference and
continued eleven pairs. Exactly 23 new CLI calls occurred; parse, successful first
managed execution, source CREATE and initial INSERT were not repeated.

Original 7 and continuation 77 are byte-exact chronological concatenation:
whole 84 SHA256 06ff6d582bbd0e08f6e2746a7bdac8bb777cc3fee9c07a27b7353b55acf9fc8e. Both source certificates and the composition receipt
remain separate. Only the entire combined 84 is admitted as
golden/fixtures/databricks_snapshot_timestamp_ignore_history.jsonl; the prefix
is never independently admitted and no recorded field is rewritten.

## Fresh Rust and strict replay

Fresh 680-file source 942fe5d admits that entire hosted fixture and raises the
normal corpus count to 131. Its standard fresh Snapshots path used the absent
b151bb graph, not continuation or old IDs. Actual server PID 611705, child 611723
and proxy 611945 were retained with argv/executable hashes; all 680 sources and six
source/frozen executable pairs remained fixed through child/proxy drain.
Twelve actual org-scoped persisted audit clocks were saved before TestServer
Drop, together with the exact extra-replay environment. Live captures contain
no replay-now header. Native 84 SHA256 f1e00f04c29cc6be24acd0e17797982d9bfb493ed2041ba345e5e119316df1d5; actual audit-clock sidecar
SHA256 bec6a988f188ac2f91c54ece0f95b027f1f5910c0438d5f23d65403c5b947334.

Hosted all 130 prior corpora plus ENTIRE hosted 84 passed 253.22s. Native all 131 hosted
corpora plus ENTIRE native 84 and its actual 12 clocks passed 257.55s. Each is one
whole-corpus test, 1 passed/18 filtered, not a fresh execution of all 19 guards.
New histories have zero gaps, unknown baselines or partial histories.
Each raw capture has 84 outcomes. The printed per-fixture asserted=72 counter
includes 12 Validate responses and excludes 12 Explain responses; Explain is checked
by its separate counter. Thus all 84 outcomes are checked. Excluding validation,
there are 72 responses including the 12 Explain responses; there are not 84
non-validation comparisons.

Exact argv, explicit environment/unsets, PID/tool-session, source/capture/
composition or actual-clock bindings, Cargo/test executable, exit and log
hashes were recorded contemporaneously. Unrelated inherited environment is
explicitly labelled unsnapshotted. Original raw captures, certificates and
clock/environment receipts are immutable. Full native data is supplemental
local-server acceptance evidence, not an additional hosted fixture.

## Provenance and verification

The [machine-readable report](databricks-snapshot-timestamp-ignore-2026-11-15.json)
binds all three captures, source 672/679/680 archives/tool freezes, the initial
guard RED/green, bootstrap, both actual command receipts, independent physical/
SQL-history/provenance reviews and the 1607-artifact evidence index.

Production/proto/Cargo and complete DuckDB gate inputs are byte-identical to
root abe843c. Its 698 workspace/310 library/25 DuckDB runs are prior qualified gates,
not fresh final-source runs. Current focused 19 snapshot +35 adjacent Python tests,
wrapper 1 pass/1 ignored, strict Clippy/fmt and tool builds passed. The native wrapper
was rebuilt/verified after the runner correction. The other 18 conformance guards
then passed separately in 9.21s (18 passed/1 filtered), explicitly skipping the
whole replay. Their functional source has 131 fixtures; only reporting docs were
uncommitted. This is a separate guard result, not a consolidated rerun of all 19.
Whole hosted admission changes only test fixture/count/docs, and final reporting changes only documentation.

Check/invalidate clock precision/bounds, new_record's State-disabled NULL deleted
business-column blocker, config/strategy/node-kind transitions, failure/retry,
generated/restart lifecycle, NULL/duplicate keys/timestamps, schema evolution,
full refresh and copy remain required. No full-ticket closure, support waiver,
merge/push or PR-ready transition is made by this report.

Runtime/frozen executable copies remain immutable. Later Cargo replay/guard builds
may replace mutable cache binaries at their source paths; the JSON report records
that post-window cache observation separately from the verified runtime hold.
