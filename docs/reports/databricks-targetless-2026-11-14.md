# Targetless FULL/VIEW characterization, 2026-11-14 (#158)

**#158 and strict #132 remain INCOMPLETE.** No production code, absent-target
guard, accepted fixture, replay exemption or support claim changes. This bounded
follow-up closes the missing crafted VIEW-repeat observation and adds exact
ordinary selected-node `show` negatives. It does not establish general client
unreachability, a safe logical history key or wrong warehouse rows.

## Hosted crafted protocol observations

The 198-RPC diagnostic capture contains 22 independent fresh baselines: FULL
(wire 1) and VIEW (wire 10), each across 11 single-field variants. Every baseline
uses a fresh literal marker and named namespace, absent target, no reported
physical tables, clones disabled and explicit defaults. Each sequence is first
submit/Explain, null-epoch Confirm, unchanged repeat/Explain, variant/Explain,
and unchanged positive control/Explain. All 44 unchanged repeat/control SKIPs
preserve the exact submitted cached marker structure and distinctive runtime.
No physical epoch, warehouse write or invented organization was supplied. All
hosted calls use the existing authorized account.

| Single changed field | FULL and VIEW variant response |
| --- | --- |
| Rendered literal SQL | EXECUTE |
| `semantic_extras.on_schema_change`: `"ignore"` to `"fail"` | EXECUTE |
| Execution type FULL to VIEW or VIEW to FULL | EXECUTE |
| Named namespace A to fresh named B | EXECUTE |
| Named namespace A to explicitly empty string | EXECUTE |
| Named namespace A to absent (`null` in capture) | SKIP, original marker/runtime |
| Default catalog, for literal SQL | SKIP, original marker/runtime |
| Default schema, for literal SQL | SKIP, original marker/runtime |
| `dbt_node_state.project_name` only | SKIP, original marker/runtime |
| `dbt_node_state.node_unique_id` only | SKIP, original marker/runtime |
| CTE dependency annotation definition only | SKIP, original marker/runtime |

The UID/project cases retain their baseline labels and every other field; they
are crafted single-field requests, not observed ordinary client transitions.
Model logical UID equivalence is not inferred from data-test UID policy. The
catalog/schema cases contain no unqualified warehouse references. In the last
case, rendered SQL remains a self-contained `literal_input` CTE with definition
`select 1 as id`, while its separately reported dependency query changes to
`select 2 as id`. This inconsistent annotation is an exact input observation;
it does not prove that hosted ignores genuinely changed upstream definitions.

The namespace-absence marker crossing needs further qualification against the
required connection isolation floor. The marker proves a cached protocol result
crossed that request boundary; it is not an independently incorrect physical
result. Neither the prior FULL physical/logical coexistence capture nor these
new targetless-only sequences prove VIEW physical/logical coexistence.

## Ordinary pinned client and read-only acceptance

Pinned dbt 2.0.11 SHA-256 is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The new `experiments/databricks_targetless.py` uses isolated profiles, projects,
artifacts and proxy ports. It permits selected-node `show --output json --limit
-1` only and refuses direct warehouse SQL API operations. Source-less table and view
models and a source-less analysis contain declared literal SELECTs, with no
hooks, macro overrides, client changes, new schemas or persistent outputs.

The accepted hosted run invokes each selection first/repeat with managed State
explicitly enabled, then a State-disabled reference. All nine commands succeed
and return the independently declared literal row. Each of six managed commands
sends one ValidateClientVersion and two telemetry RPCs; all three references
send zero State RPCs. There is no SQL submit, speculative submit, Confirm or
Record. This is a negative for these exact selected-node cells, distinct from
the earlier inline show/operation probes. It cannot establish general targetless
FULL/VIEW unreachability.

The first selected-show attempt is preserved as failed inventory acceptance.
Its before/after files were written at 15:35:42.694474Z and 15:36:11.844610Z.
Schema `a` is unchanged; schema `e` loses only
`f_20261114a141aa_inputs`. The peer #141 campaign reports an authorized source
DROP at 15:36:09.676229–15:36:10.690715Z and CREATE VIEW at
15:36:10.692881–15:36:11.697553Z, overlapping that observation window. The
original failed summary is unchanged; this peer attribution is recorded
separately and does not convert that attempt into acceptance.

The fresh accepted hosted boundary is 15:38:43.321763–15:39:12.390481Z. Both
leased inventories match exactly before/after, including all preexisting
objects. This campaign creates no warehouse objects and performs no cleanup.

The retained Rust run independently passes the same nine selected-show cells, with exact declared rows, State-disabled references and identical
18 auxiliary RPCs. Both leased inventories match exactly. No targetless
execution RPC is reached on either backend.

## Rust diagnostic replay and guard coverage

The retained Rust diagnostic run, attributed to source
`e8e0f8d010215914ddda1f8206fd1c762566b479` by its source manifests, replays the complete crafted capture through the production public router and
an isolated Postgres schema. Requests remain unchanged. **All 198 full-response
or status comparisons differ:** 88 SQL submits return INVALID_ARGUMENT 3,
`target_table is required for a non-test node`; their 22 confirmations return
NOT_FOUND 5; 88 Explain calls return an empty messages vector. These are retained
diagnostic mismatches, including downstream effects of the rejected submits,
not matching conformance. No guard is removed to fit this logical contract.

Two new public-router regressions add coverage beyond existing absent-target
validation. Accepted null-epoch FULL/VIEW Records cannot make a fresh physical
target reusable or shadow an independently recorded physical result, including its complete distinct cached payload, in either
delivery order, including across restart. They preserve ordinary absent/empty
errors and speculative undecided behavior. A targetless outcome with a physical
epoch rejects its whole Record batch; valid null-epoch retry succeeds without
publishing a deferred relation. Existing scoped complete-data-test positives,
unknown-epoch floors and physical lifecycle tests remain required.

Production source is identical to the fresh #138 25-case DuckDB acceptance
reported in [the dependency/template report](databricks-dependency-template-2026-11-14.md).
That prior run is attributed as identical-production evidence; it was not rerun
for this helper/test/report-only change.

## Validation

The workspace gate passes 660 tests with zero failures and 41 opt-in cases
ignored. Its conformance test replays all **113** current accepted corpora
(124 fixture files, 11 existing characterization exclusions), passing all 12
conformance checks in 193.64 seconds. The source and fixture manifest plus the
own-target conformance binary SHA-256 are retained; no fixture is omitted.

After the suite binaries were built, the two new guard tests were strengthened
with complete distinct physical/logical cached-payload assertions. Their final
focused run passes both tests in 6.10 seconds. Final strict workspace,
all-targets, fuzz-feature Clippy, formatting, diff checks and Python syntax
checks pass. All 41 credential-free Databricks Python cases pass in an isolated
persistent environment. The initial Python attempt used an older environment
without grpcio and failed one test-module import; that failed log is retained.

Logs and hashes are indexed in the external `validation-summary.json`. These
gates supplement the prior identical-production DuckDB evidence; they do not
convert the 198 diagnostic mismatches into conformance.

## Retained evidence

Root: `~/.local/state/dbt-state-rs/databricks-158-targetless-20261114`.
All diagnostic traces remain outside accepted fixtures.

| Directory | RPCs | Capture SHA-256 |
| --- | ---: | --- |
| `hosted-crafted` | 198 | `594c43b14e72085a3be425578812f8d27215ba88bf0f4cebec5e0906975c7e79` |
| `hosted-selected` (failed inventory) | 18 | `9127368f6031a4a6f1f4e250673a163006a6058c7ad6ea32ad269d824dfb10b5` |
| `hosted-selected-stable` | 18 | `cab3a03ff684b822451b8b748074a6a4985e2f0c41e9ef138b114c89326d87d1` |
| `rust-public-replay` (diagnostic mismatches) | 198 | `86a5d3d3ba9331cc9b5d41febd1532919dd41f1f5f8d16b558cbed202f09feb6` |
| `rust-selected-stable` | 18 | `2bc69a0dffa3f5c2389d07a008fef126e055bb82c108bd48ab7e3255947b5d3c` |

Each campaign retains the executed helper, configuration, binary digests,
complete capture and certification, per-cell requests/results and inventory
snapshots. `rust-public-replay/full-response-deltas.json` preserves every mismatch.

## Package review correction and bounded revalidation

The original server digest claim
`79ecc4ee25bfac7b8b5cb728df39354915f1f274cb4b5838a80511d0e33146e9`
is withdrawn as historical executable attribution. Retained
`immutable-current/dbt-state-server` actually hashes to
`1eb2ded482cfa5c7d366aff984112af641e88830eaa283d49b3160a450e3d69e`,
matching its retained provenance entry. The provenance file itself hashes to
`34d82babce83bdcbd29f0ac742f619550c34910c85205fb3855f9417d4a3f596`;
its stale `provenance.json` map entry
`9720ba849e2a86fb233e6cfdc79986497c1b38d4136265c0456fd599dae422ca`
is a prior-file input digest, not a self digest. Both retained top-level launch
scripts copy binaries and rewrite that manifest before launch. No historical
PID/argv/executable-byte receipt was found, so neither digest can be assigned
to either historical Rust run. Original files and manifests remain unchanged;
`review-fix/reconciliation.json` records this correction additively.

The 63-file source manifest independently matches this package's production
and build inputs. That source equivalence, the retained build/gate logs and
the independently passing observations are distinct from the unavailable
historical binary receipt. Reinspection confirms all nine retained hosted
`e3` checks and nine Rust `e5` checks have null errors, exact declared literal
rows, six managed invocations with 18 auxiliary RPCs, three references with
zero RPCs, zero execution RPCs and unchanged inventories. The 198 diagnostic
comparisons remain observations with the historical executable-attribution
limitation above.

The helper previously caught a dbt `Refused` error and saved it without failing
the campaign. It now raises that error after saving reachability evidence;
`run()` retains the failure in its summary before raising. An offline command
boundary test injects exit 17 and verifies the original error, result and log
survive and the campaign fails. A second test verifies successful nine-command
acceptance. These two tests pass; the fault test failed against the original
helper. No production, fixture or capability-gap change is included.

Fresh review validation uses the unchanged retained binaries after independently
checking their manifest entries and all 63 production/build source hashes.
`review-fix/current/source-equivalence.json` records that check, without claiming
a reconstructed historical build receipt. `launch-receipts.json` records argv,
PID, prelaunch file digest and `/proc/PID/exe` digest for the server, two proxies,
two client version checks and 18 selected-show commands; all 23 executable
receipts match their prelaunch bytes. The new Rust server launch is therefore
attributed to the `1eb2ded…` executable above. The first local validation attempt
failed before dbt because the system Python lacked PyYAML; its script and launch
receipts remain preserved, and the successful retry used the existing isolated
environment.

Both fresh hosted and Rust runs independently pass all nine literal-row checks,
with six managed invocations, 18 auxiliary RPCs, three zero-RPC references and
zero execution RPCs per backend. Certified capture hashes are respectively
`479f44e9a1bc182f2f25b973faa1a33144432af7ea62d29d749ad725b3ad629b`
and `f6c4477e6f9ef9a4f1f6d5751dc6cc35a3561c65232d022225699c0e6f2ceea8`.
The #141 lifecycle owner granted the stable interval and received its completion
boundary; both inventories remain 100/97 objects before/after. No warehouse
DDL, DML, object creation or cleanup occurred. `review-fix/current/acceptance.json`
indexes these checks and confirms all four original immutable-file hashes are
unchanged. `review-fix/fault-red.log` and `fault-green.log` retain the regression
checks. Existing workspace/Clippy/conformance gates remain prior source-equivalent
evidence; broad Cargo gates were not rerun for this helper/report/test-only fix.
#158 and #132 remain INCOMPLETE, with the absent-target guard retained.
