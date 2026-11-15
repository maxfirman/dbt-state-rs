# Bounded Databricks microbatch hosted/native evidence — 2026-11-15

The bounded hosted and native physical histories and whole strict replays pass. The reviewed description-only fix passed whole-history replay and all fresh uniform production gates, and root integrated it as canonical `abe843cde530569e923cf1032a66ba4d59a84a78`. This report does not complete #150, #138, #140 or the strict Databricks spec.

## Scope and provenance

One root-cleared hosted attempt ran pinned dbt 2.0.11 from the immutable 665-file source archive at `e1a1eb222bc88dcfd1d0bf457bb6dffd3de5ccb5`, based on canonical `9e6919c1ae6d82c1c6d127cc5cefc379c27fc500`. Executable/source hashes and actual Python/proxy PID, argv and executable hashes were retained; source and frozen tools were rechecked after child exit and proxy drain. No retry, cleanup, full refresh, macro/flag override or fake clock was used.

Archive: `/home/maxfirman/.local/state/dbt-state-rs/databricks-150-20261115`. Original complete capture SHA-256: `cdc76f6a3a47188bcb3026885ca14040a14ca53de474a0a75ad6a37ca583e6a0`. Its sidecar records 70 started, 70 recorded, complete=true, failed=false. The immutable original hosted source remains separate from the later description fix.

Only the fresh hosted pair `cdo_demo_db.state_20261114_append146.mb_20261114a150aa_{inputs,consumer}` was written. The root inventory/capacity receipt protected all sixteen earlier #146–149 relations, confirmed all four proposed #150 names absent, and retained the quota service's cached refresh timestamp rather than claiming an instantaneous refresh. The hosted final inventory is eighteen; the declared hosted persistent peak was nineteen. The later native phase below received its separate fresh eighteen-object check and grant, and ended with twenty permanent relations within its declared peak of twenty-one.

## Native command and result evidence

Ten managed CLI invocations plus one State-disabled parse completed. Each invocation submitted one whole-command MICROBATCH execution type 6, not one per daily batch. Native event-time CLI bounds changed explicitly; `lookback=1` was the actual default. State SQL remained the unwindowed source SELECT; semantic extras bound the whole CLI start/end. Five builds executed nine daily batches, and five unchanged repeats reused their own positive Confirm runtime with nested `skip_execution.explained_decision.is_stale` exactly false. No RecordExecutions or full refresh was attempted.

The initial native command emitted CTAS for its first day and REPLACE WHERE for its second; later builds used the bounded `cast(ts as TIMESTAMP) >= start AND < end` replacement shape. Native SQL and per-day temporary names were checked exactly. A final allowed CLUSTER BY statement advanced the first target to Delta version 2; later versions and unchanged repeats were retained.

| Build and unchanged repeat | Explicit half-open window | Daily batches in build | Cumulative R rows | Current selected F rows | Target Delta version | Own build/reuse runtime ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| first / first_repeat | Nov 1–3 | 2 | 4 | 4 | 2 | 12729 |
| update / update_repeat | Nov 1–3 | 2 | 5 | 4 | 5 | 10686 |
| shifted / shifted_repeat | Nov 2–4 | 2 | 7 | 5 | 7 | 10556 |
| overlap / overlap_repeat | Nov 1–3 | 2 | 4 | 1 | 9 | 10741 |
| empty / empty_repeat | Nov 2–3 | 1 | 3 | 0 | 10 | 5591 |

R is an independently declared cumulative multiset, distinct from the source's current selected projection F. Full BIGINT/STRING/DATE/TIMESTAMP schema, source and target row multiplicities, MANAGED Delta properties and full histories were checked for every cell. Ten separate State-disabled SELECT-only literal R references each had a complete zero-RPC canary; F was separately read from the current source and compared against its independent literal. Duplicate keys and NULL keys were preserved. Boundary rows, source updates, timestamp moves, deletion of duplicate source rows, revisiting overlapping windows and deletion of an empty selected day were exercised. A moved source row's old target version remained until its old window was revisited.

Before the existing update build, the source UPDATE ran and the exact own consumer's UC metadata explicitly reported its TIMESTAMP `ts` column nullable=true, with MANAGED Delta/full-name guards. An independently checked prior R then received exactly one literal row `(99, 'retained-target-null', DATE '2026-11-02', NULL TIMESTAMP)`. Full R+row, properties and history were checked before native execution. This added one target INSERT, one UC GET and five SQL inspection reads per backend plan, with no extra CLI, batch or relation. Both source and target epochs advanced before the required zero-lag BUILD. Row 99 survived every later replacement and empty window; it remained absent from source and selected F. This proves this bounded hosted target-NULL retention history, not generic NULL behavior or isolated external-target policy. Exclusion of a source NULL timestamp was not used as proof of target retention.

Canonical #149's independently captured API/show gate established the same exact three-digit nonzero UTC fractional encoding form, including `.123Z`, `.456Z`, `.789Z` and `.012Z`. It did not observe `.999Z`. This run's routine exact typed API/show comparisons checked `.999Z` losslessly; unknown precision, offsets or rounding were refused. The older whole-second spelling adjustment remains limited to evidenced `Z` ↔ identical `.000Z`; no generic timestamp normalization was introduced.

The complete 70-RPC capture contains 10 ValidateClientVersion, 25 SubmitTelemetryBatch, 10 speculative Submits, 10 ordinary Submits, 5 Confirms and 10 full Explain calls. Ten separate zero-RPC references are outside this main capture.

## Preserved red replay and narrowly reviewed correction

On the original hosted source, replay of all 129 existing corpora plus the entire new 70-RPC history failed after 249.12 seconds. New history had 60 asserted protocol entries, zero unknown baselines, zero known gaps and zero partial history. Exactly two comparisons differed: the ordinary update Submit at entry 17 and its Explain response at entry 21 used different decision-description text. Both services chose BUILD with decision 1, rejection reasons 6/6 and is_stale=false; the complete Explain tree matched. The hosted combined source-change/target-insert cell used the generic upstream/query rebuild text while the original local code used modified-target text. The original red log, capture, source and receipts remain preserved.

A separate specialist made a reviewed correction limited to description selection for the evidenced Databricks MICROBATCH ordinary-build case with matching rendered identity, known positive advancing target and genuine upstream epochs, zero lag/ANY and no copied result. It does not grant reuse or change decisions, reasons, isolation, freshness qualification, anchors or Explain trees. Adjacent offline controls delimit conservative behavior; they do not characterize new hosted policy.

On that reviewed fix source, all original 129 corpora plus the entire immutable new history passed strict replay in 249.26 seconds. The complete capture was admitted byte-for-byte, bringing fixed canonical to 130 hosted corpora. No prefix promotion, masking or new replay gap was introduced. Root completed fresh uniform gates: 698 workspace tests, 310 server-library tests, 25 DuckDB acceptance cases, strict workspace Clippy, formatting, 149 Python tests and all 130 hosted corpora. Final independent merger review was clean. The gate receipts and source boundary are documented in `docs/reports/databricks-microbatch-description-2026-11-15.md`; the corrected binary subsequently completed the bounded native warehouse phase below.

## Native physical counterpart and clock provenance

After the reviewed fix and final #151 offline integration, root granted one fresh native attempt at source `8933e9ecff8a92d899089a8542dcfc7fe4cf82a4`, based on canonical `3c12bfe95e2f843ce37c788d64cd18a9b88a8ed3`. Its separate freeze contains all670 tracked source/helper/header/Cargo inputs and copied native wrapper, proxy, probe, dbt, Databricks CLI and Python executables. A fresh root receipt protected all eighteen existing relations, confirmed the Rust pair absent and retained reported quota18/100 with its cached refresh timestamp. Only `mb_20261114b150bb_{inputs,consumer}` was written. No retry, cleanup or unexpected persistent relation was needed.

All ten native cells, nine daily batches, ten zero-RPC references, five Confirms and five exact nonstale own-runtime SKIPs passed with the same independent R/F/source multisets, typed schema, NULL99 retention and Delta versions as the hosted table above. Native Confirm/reuse runtimes by stage were 12327, 10772, 9955, 10727 and 5498ms. The full native capture also has 70 RPCs with the same method counts, SHA-256 `f901c711bbbcecade91fe4d1f8af18fa64a1f6531b28f5cd557a983930e7fc9c`; its sidecar certifies70/70 complete. Actual executing wrapper/proxy identities and source/tool hashes after child exit and proxy drain are retained.

The wrapper retained actual org-scoped persisted audit clocks for the exact ten ordinary decision IDs before the isolated State schema was dropped. The clock sidecar is hash-bound to the complete native capture, SHA-256 `1f0f0e3b51d086a756f7fae1f4a477ec6768a1f9ae1cefb5ba815ca370d751e1`. Both original raw-capture and supplemental replay environment receipts are retained. Only a byte-identical renamed capture copy was made to satisfy the supplemental filename convention; no wire payload, clock, expected response, mask or gap was modified. Production System clock was used live.

All current130 hosted corpora plus the entire native70 history passed strict supplemental replay with that actual clock sidecar in253.19 seconds:60 new assertions, zero unknown baselines, zero known gaps and zero partial histories. The original hosted source/capture and original red replay remain immutable. No new warehouse operations are planned.

## Validation and limits

The native preparation ran57 related Python checks (15 own), two focused description controls/tests, the credential-free wrapper check, strict focused Clippy and formatting. It did not repeat the full production gates. Hash comparison proves all64 production/protocol/Cargo inputs used by the earlier fresh698-workspace/310-library/25-DuckDB gates remain identical; #151 and #150 additions are helper/test/report-only. Actual current native execution and its whole-history replay supply the fresh bounded integration evidence.

Full #150 and #138/#140 remain incomplete. This bounded run does not establish full-refresh/Record behavior, implicit moving-clock scheduling, nondefault lookback/settings/predicates/schema evolution, concurrency, partial-batch failure, lifecycle, generated acceptance or isolated external-target policy. The frozen preflight ledger's unverified wording is preserved provenance, while this report distinguishes the actual hosted/native observations now obtained.

Primary receipts include the immutable archive's hosted/native audit, process, exit, clock, source freeze, grant and capture certificates; root capacity receipts; original red and reviewed green replay; fresh production gate receipts and a complete evidence index. The accompanying JSON binds exact paths and hashes.

Private evidence index: `/home/maxfirman/.local/state/dbt-state-rs/databricks-150-20261115/evidence-index.json` contains2903 retained files, SHA-256 `fb524cfa53b32e094a9d79c2895133fbfc5fd3dbe1321f182d46df30f36634ce`. The index excludes itself and delivery report copies to avoid self-reference. Hosted source665 and native source670, all six native executable copies, original red and both green replay logs, root grants/capacity receipts, actual pre-Drop clocks and independent review inputs remain retained. This report is a documentation-only postflight addition and was not part of either executing source freeze.
