# Scoped deferral, 2026-11-13 (#155)

The pinned dbt 2.0.11 client resolves and reads recorded Databricks upstreams
through `Execution.ResolveDeferredRelations`. The actual request omits
`project_id`; hosted State nevertheless returns the fully backtick-qualified
production relation. The former Rust handler returned an empty mapping.

This report establishes the bounded deferral slice, not overall Databricks
support. #140 remains incomplete on unsafe omitted-source/missing-epoch and
other unresolved cells; copied-target rebuild and snapshot deletion blockers
from #134 remain unchanged. The authentication model is unchanged.

## Observed contract

Fresh warehouse-free public protocol histories independently cover Snowflake
and Databricks with absent and populated project IDs. Every history starts with
a fresh submit, resolves before confirmation, confirms, then varies the scope.
Both warehouses return the same outcomes:

| Request / lifecycle | Hosted result |
| --- | --- |
| Pending submit, including explicit project ID | Empty mapping |
| Confirmed outcome | Exact submitted physical FQN |
| Profile changed or absent | Empty mapping, even with project ID |
| Nonempty project ID | Matches ID; project name is descriptive |
| Absent/empty project ID | Matches exact profile + project name; includes outcomes originally recorded with an ID |
| Wrong project ID, target, or unknown UID | Empty mapping |
| `failed_to_clone=true` confirmation | Publishes the successful fallback build |

The wire protocol has no general execution-failure flag. Actual dbt warehouse
failures send no confirmation. A fresh failed UID does not resolve; a failed
attempt of a previously successful UID against a different owned physical
relation leaves the prior confirmed FQN and complete prior rows available.
Targetless cached test outcomes cannot become relation mappings.

The request contains no warehouse family or connection namespace. Deferral
therefore applies its supplied logical scope and does not claim additional
physical isolation from absent fields. The physical reuse query and its org,
family, namespace and Target relation scope remain separate. Local public
regressions prove organization isolation. Hosted OAuth credentials identify the
organization; arbitrary local org headers are not represented as hosted auth.

## Actual client and independent warehouse evidence

Scope: pinned executable digest
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`,
Databricks Free Edition serverless, DBSQL 2026.44, Unity Catalog managed Delta.
Only exact new `d_<run>_...` relation allowlists inside the previously leased
`cdo_demo_db` owned containers are writable. Before/after inventories preserve
preexisting objects; no schema creation/deletion occurs.

The final hosted campaign has six actual dbt invocations and 54 certified RPCs:
6 validation, 16 telemetry batches, 6 speculative SQL, 7 ordinary SQL,
2 confirmations, 4 complete Explain calls and 13 deferral calls. It requires
initial upstream/downstream builds and unchanged upstream/downstream SKIPs.
The Rust production-router campaign independently passed the same six
invocations and 54 certified RPCs. Every deferred response identifies the exact
production FQN, and the emitted SQL reads that source. The reference is an independently executed literal
SELECT and explicit BIGINT/INT/STRING schema plus duplicate/NULL row multiset;
MANAGED/Delta properties are checked separately. A different owned decoy has
amount 17 instead of 10 and must fail with exactly `physical:rows` under the
wrong-source oracle, while the successful run preserves it unchanged.

The first discovery deliberately placed the decoy at the development
upstream's exact alias. Pinned dbt preferred that existing development relation
even though State returned the production FQN. The physical checker rejected
the result. This is retained evidence of client selection behavior, not a State
mapping bug. Successful deferred-read acceptance leaves that alias absent and
uses a separate owned decoy; both actual emitted SQL and results must prove the
production read.

The separate final protocol campaign has zero dbt invocations and 72 certified
RPCs: 16 submits, 10 synthetic confirmations and 46 resolves. Six required
SKIPs additionally prove that reusing a physical result under a new logical UID
or profile/project does not publish a new deferral mapping. These are explicitly
crafted warehouse-free protocol evidence, not physical execution claims.
Both complete fixtures replay all calls, including full mappings, Explain
responses, statuses and application metadata, with no exemptions.

Early crafted pending/fallback requests also exposed broader, unimplemented
copy/drop response reasons: a same-SQL new target in the production namespace
received hosted's reserved-dev-copy chain-limit rejection; a synthetic second
build without own-target metadata received hosted's absent-target equivalent
reasons. The first full replay rejected all three payload differences. Those
captures and the failed replay remain in the archive. The focused deferral
cells use fresh physical namespace histories to avoid invoking those separate
copy/drop policies; no comparator exception or implementation waiver was added.
This does not resolve or narrow the parent copy/freshness work.

Final validation also caught an unvalidated supplement: the earlier passing
log covered 56 protocol calls, before 16 additional calls were appended. Those
additional requests used Databricks backticks in Snowflake target metadata.
Hosted accepts that spelling and returns doublequoted components containing
literal backticks; Rust preserves the raw target spelling. The original
72-row bytes and exact failed replay are retained in the archive. The final
16-call supplement was recaptured with each dialect's own quoting, as the
committed runner specifies. The Snowflake cross-dialect spelling divergence
remains an explicit coverage limit; no normalization rule or comparator waiver
was added for it, and no compatibility claim is made for that spelling.

## Implementation and compatibility

Three nullable execution metadata columns persist profile, project name and
Target name on SQL/values Submit and Record through the existing write path.
Deferral reads only confirmed executions under the exact logical scope and
returns the most recently confirmed physical mapping per UID. Pending or failed
attempts do not overwrite it; duplicate confirmation cannot republish an older
outcome. Selector snapshots remain separate and retain their existing timing.

Migration 0021 does not infer missing scope on old execution rows. Older rows
without profile/project/Target evidence cannot resolve until a fresh confirmed
execution records it. Existing fingerprint bytes and physical reuse semantics
are unchanged. The public Record path already commits execution metadata and
selector state atomically; no second non-atomic deferral store is introduced.
The separate Identifier migration 0020 from #136 and this additive migration
0021 operate on different execution columns. Their combined migration check
remains an integration gate after #136 is merged.

## Reproduction

Build `record-proxy` and `state-probe` once. Copy the binaries to owned stable
paths when sharing a Cargo target directory between worktrees. Then run:

```bash
uv run --with pyyaml python experiments/databricks_deferral.py \
  --dbt /path/to/dbt-2.0.11 --proxy /path/to/record-proxy \
  --probe /path/to/state-probe --schema-lease /path/to/owned-lease.json \
  --work-dir /new/empty/hosted-run

uv run --with pyyaml python experiments/databricks_deferral_protocol.py \
  --dbt /path/to/dbt-2.0.11 --proxy /path/to/record-proxy \
  --probe /path/to/state-probe --schema-lease /path/to/owned-lease.json \
  --template golden/fixtures/databricks_deferral.jsonl \
  --work-dir /new/empty/protocol-run
```

For actual Rust acceptance use the ignored
`real_dbt_deferred_reads_match_independent_reference` integration test; it owns
an isolated Postgres schema and the production router and passes its explicit
`--rust-url` to the runner. Ordinary credential-free checks include the two
complete conformance fixtures, public deferral safety tests and the Python
history/ownership/oracle gates. The broader workspace preserves existing
Snowflake/DuckDB checks.

Final credential-free validation after merging integration `fbbd083` passed:
593 workspace tests, all 11 conformance tests across 77 corpora, strict
workspace/all-targets/fuzz Clippy, formatting and diff checks. The focused
Python checks passed 24 tests. Ignored cloud tests were not repeated; the
successful actual Rust campaign remains the retained physical acceptance.

Evidence archive, validation results and immutable digests are recorded in the
adjacent JSON index. Failed/discovery attempts are retained with their exact
failure classification and complete capture sidecars.
