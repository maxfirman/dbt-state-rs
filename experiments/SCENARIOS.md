# dbt State conformance scenario checklist

Derived from the official docs (docs.getdbt.com, Sep 2026) + live capture. Each
row is a behaviour to verify against the real service and, where it differs from
`dbt-state-rs`, to implement. Status: ✅ verified+conformant, ⚠️ divergence
found, ❓ to test, 🔧 implemented this pass.

## Node-type reusability (docs: dbt-state-about)
| node type | reusable? | expected |
|---|---|---|
| SQL model (view/table/incremental) | yes | skip/clone when logic+data unchanged |
| snapshot | yes | reusable |
| seed | yes | match on values_hash |
| data test | yes | reuse prior result if tested nodes unchanged; match by node_unique_id |
| unit test | ❓ | state-explain shows UNKNOWN ("details unavailable") |
| Python model | NO — always build | n/a (not Snowflake-SQL) |
| custom materialization (DBT_CUSTOM=11) | NO — always build | must always EXECUTE |

## Decision rules (docs: dbt-state-about, state-explain)
| rule | expected hosted behaviour | our status |
|---|---|---|
| view, logic unchanged, upstream data CHANGED | SKIP (views reflect new data w/o rebuild) | ❓ likely ⚠️ (we apply freshness to all types) |
| view uses `select *` on ref()/source() | REBUILD (cols unknown at parse) | ❓ server SQL analysis — out of scope |
| table/incremental, upstream data changed beyond lag_tolerance | EXECUTE | ✅ |
| logic (semantic SQL) changed | EXECUTE | ⚠️ C1: we over-execute on cosmetic (body-hash); documented |
| data test, tested node unchanged | SKIP (reuse prior result) | ✅ (uid match) |
| data test, new/changed | EXECUTE | ✅ (uid match, C1c) |
| no prior execution | EXECUTE | ✅ |
| lag_tolerance window not elapsed | SKIP even if upstream changed | ❓ (we compare epoch deltas) |
| require_fresh_data_from any/all | ANY/ALL stale policy | ✅ |
| custom materialization | always EXECUTE | ❓ likely ⚠️ (we treat et=11 like any model) |

## Config-change policy (live-verified, C1b)
| change | hosted | our status |
|---|---|---|
| meta / tags | SKIP | ⚠️ we execute (body-hash) |
| post_hook | SKIP (hook silently dropped) | ⚠️ we execute |
| pre_hook | EXECUTE | ✅ agree |
| grants | EXECUTE | ✅ agree |
| persist_docs | EXECUTE | ✅ agree |
| contract enforce (config change) | EXECUTE | ✅ agree |
| contract data_type (contract hash only) | SKIP | ⚠️ we execute |
| column description (persist_docs off) | SKIP (not sent) | ✅ |

## Materializations to test (this pass)
- view: skip-on-upstream-change behaviour (❓ KEY), logic change -> execute
- table: baseline (✅)
- incremental (merge/append/delete+insert/insert_overwrite): clone-from-prod + run on top; --full-refresh
- microbatch
- ephemeral (no node? inlined)
- snapshot
- materialized_view

## CONFIRMED FINDINGS (live)
- VIEW (et=10) skip-despite-upstream-change: ✅ CONFORMANT (not a divergence).
  The CLIENT sends only the view's OWN target in tables[] (never upstreams;
  verified 0/5 view submits across all captures carry non-own upstreams). Our
  own-table exclusion => considered==0 => skip on logic match. Data TESTS on the
  view DO carry the upstream (raw_customers) and correctly execute when it drifts.
- VIEW definition change (added column) -> node_body_hash changes -> EXECUTE. ✅
- CUSTOM MATERIALIZATION (et=11): docs say "always build, never reused" but the
  REAL service SKIPPED an unchanged custom_table model on rerun (execute ->
  confirm -> skip). => our treat-like-any-node behaviour is CONFORMANT; do NOT
  implement an always-execute rule (would diverge). Fixture
  custom_materialization_reuse.jsonl. Docs are aspirational/outdated here.


## Other scenarios
- seed content change (values_hash), new seed, seed config (delimiter/quote/column_types)
- clone: cross-env freshest candidate; allow_clones:false; RegisterClone
- vars: --vars change -> rendered SQL change -> execute
- env_var in SQL -> execute (unless compare_unrendered_code)
- target name change
- source freshness / external table modification
- singular test (data-test node, singular SQL)
