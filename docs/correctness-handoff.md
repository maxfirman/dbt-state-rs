# Correctness handoff after offline hardening

Updated 2026-10-13 (review #2 5th FOLLOW-UP). Read this before changing
decisions. The goal remains a conformant drop-in dbt State replacement; **the
current server does not yet meet that goal**, but a growing set of behaviors is
now live-verified.

An earlier phase hardened the server offline (unsafe-skip fixes, fingerprinting,
test/seed identity, org isolation — summarized in the table below). A later
phase (2026-10-11) characterized the **clone** and **lag/freshness** paths
against the REAL hosted service (api.state.dbt.com, dbt Fusion client, Snowflake
`prod_demo`/`dev_clone`) via the recording proxy, and implemented/fixed them.
Those live-verified sections are marked inline (see §A and §C). Passing local
tests establish the contracts below; live-verified items additionally reproduce
captured hosted responses, but complete hosted equivalence is still not claimed.

## Completed changes and evidence

| Review finding | Offline change | Main regression coverage |
| --- | --- | --- |
| 1: stale historical matches and own-target validity | Select the newest confirmed physical target before checking its fingerprint or materialization. Require one known current-target epoch. Reject observed external changes unless explicitly ignored. | A→B→A, materialization replacement, missing/changed target, explicit ignore flag |
| 2: unsafe normalization and UTF-8 panic | Preserve TRY/SAFE cast kind, type parameters, qualified function identity. Preserve exact unsupported SQL; normalize only Snowflake. Version the fingerprints. | Negative SQL pairs, arbitrary UTF-8 no-panic property, parser failure |
| 3: test identity, logic, lost results | Match project/namespace/UID plus SQL/config/context; support Python label identity. Persist complete protobuf results through Confirm and Record. Require usable failures/error/warn fields before SKIP. | Passing/failing/warning tests, missing results, changed SQL/severity/project/UID, nested results, int64 MAX, infinity, immutable duplicate confirm |
| 4: dependency definitions and resolution context | Fingerprint all supplied dependency SQL and its catalog/schema, root dialect/catalog/schema, and sorted semantic extras. Empty/ambiguous supplied dependency definitions prevent reuse. Share submit/record fingerprint code. | Changed view definition, changed schema, Record/Submit parity |
| 5: unknown freshness and schema collisions | Keep optional epochs; exact physical dependency names; incomplete or duplicate metadata forces BUILD even with ALL. Corrupt stored evidence returns INTERNAL rather than becoming empty/fresh. | Unknown epochs, two same-named inputs in different schemas, missing/duplicate inputs, invalid JSON/protobuf |
| 6: seed configuration | Match latest physical state, seed bytes and semantic configuration/context; require known current object metadata. | Column types, missing object, content reversion, Record/Submit parity |
| 7: cross-target SKIP without provenance | Remove unconditional namespace reuse. Rebuild another physical target until object/provenance mapping is implemented. | Different target/schema with same namespace cannot SKIP |
| 8: volatile evaluation omitted | Runtime volatile-function VALUES (`transformed_nodes_by_query`) are NOT implemented (documented conformance gap). The cross-name clone fallback is suppressed only when the SQL genuinely contains a volatile function AND `tolerate_nondeterminism=false` — a CONSERVATIVE fallback, not full volatile conformance (4TH FOLLOW-UP finding 3). | Deterministic model clones even with the flag; genuinely volatile model executes; field-scoped conformance gap |
| 9: empty selectors/deferral | **Selectors IMPLEMENTED (live-verified 2026-10-11, see §D).** `GetStateSelection` reproduces the hosted `state:*` verdicts via a recorded `node_states` store + pure `selector::classify`. **Deferral IMPLEMENTED (live-verified 2026-10-12, see §D):** `ResolveDeferredRelations` resolves each node to its defer-to-env physical FQN from `node_states` (migration 0010 adds `target_table`), omitting unbuilt nodes. | `selector_conformance.rs`, `selector_offline.rs`, `selector.rs` unit tests; `deferral.rs` + fixture `deferral_resolve.jsonl` |
| 10: invalid clone DDL | Reject view sources and unknown dialects before inserting pending history. Preserve the captured Snowflake table DDL and both response mirrors. | Exact captured table DDL, view/unknown-dialect errors and no pending rows |
| 13: tenant leaks/logged password | Scope every UI database read to configured `DBT_STATE_UI_ORG_ID` (default `local`); remove startup DSN logging. | Two organizations with identical project names; all UI read methods including direct foreign IDs |
| 12: lag tolerance (live-verified 2026-10-11) | Anchor data-freshness drift on the node's OWN last-build epoch (`upstream_epoch > own_build_epoch`), replacing the `recorded_upstream + tolerance` timestamp-distance rule. `freshness_tolerance_seconds` verified NOT to absorb the gap and intentionally unapplied. Data tests have no own object, so they anchor on the RECORDED UPSTREAM epoch instead (review #2 finding 1, 2026-10-12). See §C. | `decision.rs` `freshness_*` unit tests, proptest oracle, `golden/fixtures/lag_freshness_own_build.jsonl`, `crates/harness/tests/lag_freshness.rs` |
| 10 (extended, live-verified 2026-10-11): clone lifecycle, rejection, cross-name fallback | Reproduce register→confirm→cross-environment reuse (schema-abstracted, logic-sensitive); reject `allow_clones=false` (`clone_chain_depth_limit=0` → `unable_to_clone`, crr=10); emit SQL-submit `ready_to_clone` for an equivalent under another name. Migration 0008 adds `node_namespace_logic_hash` + `clone_source_table`. See §A. | `clone_lifecycle.rs`, `clone.rs::register_clone_rejected_when_allow_clones_false`, `clone_submit_fallback.rs`, fixtures `clone_rejected_allow_clones_false.jsonl` + `clone_submit_equivalent_fallback.jsonl`, `sql_norm`/`fingerprint` unit tests |
| 12b (live-verified 2026-10-11): require_fresh_data_from + is_stale | Verify ALL rebuilds only when every direct parent drifted (SKIP otherwise, even at lag 0); fix `is_stale` to be false on every execute and true only on a tolerated-stale skip. Record evidence that `lag_tolerance` CAN gate under ANY policy (exact rule not yet isolated; engine keeps the safe own-build anchor). See §C. | `decision.rs` `*_is_stale_*` tests, `crates/harness/tests/lag_policy.rs`, fixture `lag_multi_upstream_policy.jsonl` |
| Testing/toolchain | Pin Rust 1.98; capture DB test out of `--lib`; reject malformed trace lines; replay all fixtures with full captured confirms and ID correlation; fail discovery fuzz tool on transport/decode failures or divergences; add live `experiments/` capture drivers (clone/lag). | Workspace tests, no-DB library tests, strict trace parse test, full causal response comparisons, fuzz compilation |
| **#2 finding 1 (live-verified 2026-10-12): data-test upstream freshness** | A data test (execution_type=8) is confirmed with NO own object epoch; the engine returned `should_rebuild=false` for it BEFORE comparing upstreams, reusing a stale cached pass/fail after the tested data changed. Now `decision.rs::data_test_upstream_freshness` anchors a data test's freshness on the RECORDED UPSTREAM epoch (per input, strict `>`, ANY-style; unknown/absent baseline rebuilds). VERIFIED LIVE (`experiments/data_test_freshness{,_control}.sh`): `dt_test` over `dt_up` executes when `dt_up`'s epoch advances and skips when unchanged. | 6 `decision.rs` unit tests, `crates/harness/tests/data_test_freshness.rs`, fixture `data_test_upstream_freshness.jsonl` (CHARACTERIZATION-ONLY) |
| **#2 finding 5 (2026-10-12): RecordExecutions logical-state atomicity** | `RecordExecutions` wrote each record's `node_states` (selector/deferral) row inline — before full batch validation and outside the batch transaction — so a rejected (INVALID_ARGUMENT) or DB-failed batch leaked logical state. Now node-state upserts are collected during row build (fail-fast, no writes) and published only AFTER the executions batch commits. Error-path atomicity only; every successful path is behavior-preserving (conformance replay still passes). The companion publication-point question (an UNCONFIRMED/failed submit being immediately deferrable) is left unchanged and documented open — it needs live pending/failed-build characterization. | `crates/harness/tests/correctness.rs::rejected_record_batch_does_not_leak_node_state` |
| **#2 finding 6 (live-verified 2026-10-12): compare_unrendered_code dual identity** | With `compare_unrendered_code=true` the server used `template=node_body_hash` EXCLUSIVELY, so a template-only change (identical rendered SQL) wrongly EXECUTED. Now `node_sql_hash` ALWAYS carries the rendered identity and `node_template_hash` (migration 0011) carries the template identity; `find_confirmed` reuses when EITHER matches — the documented "rebuild only when BOTH change" contract. VERIFIED LIVE (`experiments/template_comparison.sh`): a Jinja-comment-only edit (template changed, rendered identical) SKIPS. Both hashes fold in deps/context/extras so a genuine dependency change still rebuilds. | 5 `template_comparison.rs` tests, fixture `template_compare_unrendered.jsonl` (CHARACTERIZATION-ONLY), migration 0011 |
| **#2 FOLLOW-UP finding 3 (live-verified 2026-10-12): swapped dependency bindings** | `namespace_logic_hash` hashed only a SORTED MULTISET of dependency definition texts, discarding the (reference → definition) binding. Two models with the same root SQL and the same two definitions SWAPPED between their references hashed identically → one could be cloned into the other despite different output. Now each dependency's schema-abstracted NAME is bound to its definition (`sql_norm::abstract_object_name_schema`, sorted by the pair). VERIFIED LIVE (`experiments/finding3_swapped_bindings.sh`, two oracles): hosted EXECUTES the swapped-binding submit (not clone) and the dev contents are the swapped model's correct value. | `fingerprint.rs::ns_distinguishes_swapped_dependency_bindings` (+ metamorphic order/swap, same-basename, unqualified-name tests), conformance fixture `clone_fallback_swapped_bindings.jsonl`, `clone_submit_fallback.rs::{hosted_swapped_bindings_execute_not_clone,server_does_not_clone_swapped_binding_equivalent}` |
| **#2 FOLLOW-UP finding 6 (live-verified 2026-10-12): data-test template dual identity** | The finding-6 fix threaded `node_template_hash` into the MODEL lookup but not the DATA-TEST lookup (`find_confirmed_test` compared only the rendered hash), so a cuc data test with an unchanged template but changed rendered literal wrongly EXECUTED. `find_confirmed_test` now matches EITHER hash. VERIFIED LIVE (`experiments/data_test_template.sh`): a data-test Jinja-comment-only edit SKIPS. | `data_test_template.rs` (4 cells + hosted pin), conformance fixture `data_test_compare_unrendered.jsonl` |
| **#2 FOLLOW-UP finding 5 (2026-10-12): FULL cross-store atomicity** | The executions batch committed BEFORE a best-effort `node_states` write, so a logical-state write failure left a durable execution row with no logical state. Both tables are now written in ONE transaction (`store::insert_confirmed_batch_with_node_states`); a node_states fault rolls the WHOLE request back. | `correctness.rs::{node_state_write_failure_rolls_back_whole_record_batch,successful_record_batch_publishes_execution_and_node_state_together}` |
| **#2 FOLLOW-UP finding 2 (live-verified 2026-10-12): stale-source clone eligibility** | Captured the clone SOURCE's full provenance (addressing the review's critique). A dev submit whose current upstream is NEWER than the source's build epoch STILL returns `ready_to_clone` from that stale source — hosted TOLERATES source staleness. The server matches (also clones), so it is NOT over-conservative; a freshness gate would regress conformance and is intentionally not added. | conformance fixture `clone_fallback_stale_source.jsonl`, `clone_eligibility.rs::{hosted_clones_stale_source_with_captured_provenance,server_clones_stale_cross_name_equivalent_like_hosted}` |
| **#2 FOLLOW-UP finding 4 (live-verified 2026-10-12): clone-chain provenance** | After a clone is confirmed, bumping the upstream NEWER than the clone's build epoch makes a re-submit `ready_to_execute` ("its upstream data is out of date"). Hosted anchors a post-clone target's freshness on the CLONE's build epoch and REBUILDS when the upstream advances past it — the own-build anchor the engine implements; a clone is NOT perpetually fresh. The server reproduces the whole chain. | conformance fixture `clone_chain_upstream_drift.jsonl`, `clone_chain_provenance.rs::hosted_clone_rebuilds_when_upstream_advances_past_clone` |
| **#2 2ND FOLLOW-UP finding 1 (live-verified 2026-10-12): schema abstraction collapsed distinct bindings — REACHABLE** | The first binding fix still abstracted EVERY schema to a single placeholder, so two same-basename relations in DIFFERENT schemas (reachable via custom `schema=`/`alias=`) collapsed and a swap of their definitions under a constant root SQL hashed identically → unsafe CLONE. Rewrote the abstraction to a RELATIVE schema→token map (`sql_norm::SchemaMap`): distinct schemas → distinct tokens (sorted); a single environment schema → token 0 (cross-env preserved). `namespace_logic_hash` (v2) builds one request-wide map over the root SQL + every dependency name/definition + `default_schema` (resolving unqualified refs). LIVE-VERIFIED with NONEMPTY `query_dependencies` (two VIEW deps, constant root SQL): hosted EXECUTES, dev contents `(2,)`. | `fingerprint.rs::{ns_metamorphic_same_basename_different_schema,ns_blue_red_distinct_physical_inputs_differ,ns_unqualified_default_schema_context_differs}`, conformance fixture `clone_fallback_dep_binding_swap.jsonl`, `clone_submit_fallback.rs::{hosted_dep_binding_swap_executes_shape_is_pinned,server_executes_swapped_dependency_binding_with_views}` |
| **#2 2ND FOLLOW-UP finding 2 (live-verified 2026-10-12): RegisterClone source lookup + clone-age anchor** | CODE DEFECT: `store::find_source_ns_logic_hash` filtered namespace in the SAME WHERE as the ORDER BY, so a source whose newest build moved namespaces leaked a stale same-namespace logic hash onto the clone-origin row. Fixed: select the source's CURRENT physical row FIRST (CTE), then post-filter namespace (a transition → inherit nothing → safe EXECUTE). Separately, a RegisterClone chain isolating `source build < upstream < clone creation` shows hosted SKIPs → it anchors on the CLONE CREATION epoch (clone-age), NOT source-content age; the server's own-build anchor reproduces this, so no speculative source-epoch validation is added. | `clone_history_provenance.rs::clone_source_logic_inherit_must_reflect_current_namespace`, fixture `clone_chain_source_age.jsonl`, `clone_chain_provenance.rs::{hosted_clone_age_not_source_age_shape_is_pinned,server_clone_age_anchor_skips_upstream_between_source_and_clone}` |
| **#2 2ND FOLLOW-UP finding 3 (2026-10-12): real CLONE conformance gating** | The conformance replay's `ready_to_clone` arm only incremented `gaps` — it never compared the local CLONE response or replayed its confirmation, so clone fixtures were not actually gated. Now it asserts the local CLONE payload (minus opaque IDs), correlates the confirmation into `pending` (tracking cloned state), and distinguishes a full causal match from a keyed MISSING-SOURCE-HISTORY gap and from a genuine mismatch. Un-excluded the replayable `template_compare_unrendered.jsonl`. | `conformance.rs` clone arm; 7 keyed clone/missing-history entries in `golden/replay_gaps.json`; corpus 22→24 fixtures |
| **#2 2ND FOLLOW-UP finding 4 (live-verified 2026-10-12): clone eligibility dimensions** | The eligibility experiment used top-level config keys and wrongly declared `lag_tolerance`/`evaluate_volatile_sql` unavailable; they belong under the nested `state={}` config (dbt 2.0.6 parses it). Distinct arms + a wire-field VARCHECK now confirm the independent variable reached the request. LIVE: a STALE cross-name source still returns `ready_to_clone` under zero `freshness_tolerance_seconds` (that dimension does not gate the fallback). NOTE: the companion claim that `tolerate_nondeterminism=false` never gates the fallback was CORRECTED in 3rd/4th follow-up finding 3 — a genuinely volatile model is NOT cloned, while a deterministic one still is. | `clone_eligibility.rs::server_clones_stale_equivalent_under_zero_tolerance`, `experiments/clone_eligibility.sh` |
| **#2 2ND FOLLOW-UP finding 5 (2026-10-12): experiment reliability** | Drivers now check the dbt EXIT STATUS directly (not a loose grep a partial multi-node build could satisfy) and FAIL on a failed mutation (the `CREATE OR REPLACE` run_op no longer swallows errors with `\|\| true`); the swapped-binding driver's oracles are executable assertions (fail on a missing submit, wrong decision, or wrong warehouse contents). | hardened `clone_eligibility.sh`, `clone_chain_provenance.sh`, `finding3_swapped_bindings.sh`; new `finding3_view_dep_swap.sh`, `registerclone_source_age.sh` |
| **#2 3RD FOLLOW-UP finding 1 (2026-10-12): quoted-identifier case in schema abstraction** | `SchemaMap` lowercased + stripped quotes unconditionally, so Snowflake's case-SENSITIVE quoted schemas `DB."X".INPUT` and `DB."x".INPUT` collapsed to one relative token and a swap of their definitions conflated → unsafe clone. Added a quote-aware `canonical_schema_key` (quoted → `q:` case-sensitive keyspace; unquoted → `u:` folded); `SchemaCollector` and the `Canonicalizer` preserve quote state. The resolution-context sub-cases were MEASURED LIVE (`experiments/resolution_context.sh`): hosted keys dependency equivalence on the DEFINITION BODY (executes when the data differs, reuses when it abstracts equal), so a bare leaf-schema rename is the environment qualifier (cross-env positive), not a bug. | `fingerprint.rs::{ns_quoted_case_distinct_schemas_do_not_collapse, ns_dependency_leaf_schema_rename_is_cross_env_positive, ns_uniform_schema_rename_is_cross_env_positive, ns_unqualified_uniform_schema_rename_is_cross_env_positive}`, `clone_submit_fallback.rs::server_executes_quoted_case_swapped_binding` |
| **#2 3RD FOLLOW-UP finding 2 (2026-10-12) — SUPERSEDED by 4TH FOLLOW-UP finding 4** | The earlier synthetic clone-age test did NOT send the captured requests. Replaced with `registerclone_source_age_chain_exact_replay` driving all 5 captured messages; entries 0–3 match. The 3rd-follow-up framing attributed ENTRY 4's mismatch to the INCREMENTAL self-referential predicate — that causal claim is RETRACTED in the 4th follow-up (not isolated; entry 4 differs on three axes at once and hosted's SKIP is a post-clone same-target no-op). Entry 4 is now an UNRESOLVED DIVERGENCE. The "hosted does not validate a source epoch" retraction stands. | `clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay` |
| **#2 3RD FOLLOW-UP finding 3 (2026-10-12) — SUPERSEDED by 4TH FOLLOW-UP finding 3** | Established LIVE that a genuinely volatile model (`current_timestamp()`) EXECUTES (does not clone), correcting the earlier deterministic-query claim. The 3rd-follow-up SERVER FIX gated the cross-name fallback on `tolerate_nondeterminism` alone — the 4th follow-up NARROWS this: a DETERMINISTIC model with the same flag is still cloned by hosted, so the gate now keys on the SQL actually containing a volatile function. The promoted captures (`clone_fallback_zero_tolerance.jsonl`, `clone_fallback_volatile.jsonl`) and the VARCHECK hardening stand; the test `server_executes_not_clones_under_volatile_evaluation` was replaced by `volatile_sql_executes_but_deterministic_clones_under_nondeterminism_flag`. | `services.rs` fallback gate; `clone_eligibility.rs::{server_clones_stale_equivalent_under_zero_tolerance, volatile_sql_executes_but_deterministic_clones_under_nondeterminism_flag, hosted_zero_tolerance_*, hosted_volatile_*}` |
| **#2 3RD FOLLOW-UP finding 4 (2026-10-12): binding-swap regression gate** | The committed view-dep-swap capture's treatment EXECUTE passed partly because the local store had no baseline candidate (missing source history), with no control and inconsistent dependency representation. Added a self-contained gate: build+confirm a prod consumer with two view deps, then a NO-SWAP CONTROL (identical bindings → `ready_to_clone`, proving a compatible candidate exists) and the SWAP (same root SQL, same `create or replace view` representation → `ready_to_execute`). The treatment EXECUTE is now a real gate (a wrong binding hash would clone the treatment too and fail). | `clone_submit_fallback.rs::dep_binding_swap_gated_against_present_baseline_with_control` |
| **#2 3RD FOLLOW-UP finding 5 (2026-10-12): new-driver oracle hardening** | `finding3_view_dep_swap.sh` and `registerclone_source_age.sh` had print-only oracles. Both now EXIT NONZERO on absent prerequisites/measurements: the view-dep driver fails on a nonzero contents query, a missing/≠`ready_to_execute` submit, <2 dependencies, or dev contents ≠ `(2,)`; the source-age driver fails unless a RegisterClone event + both confirms + a dev resubmit are present AND the distinguishing interval (source build < upstream < clone creation) holds. Failure paths are warehouse-free assertions. | hardened `experiments/finding3_view_dep_swap.sh`, `experiments/registerclone_source_age.sh` |
| **#2 4TH FOLLOW-UP finding 1 (2026-10-13): consistent identifier case for ALL components** | The 3rd-follow-up fix only corrected the SCHEMA token; `abstract_object_name_schema_with` still lowercased unquoted catalog/table parts and stripped quotes case-preserved, so unquoted `INPUT` and quoted `"input"` both became `input` → a swapped TABLE-component binding was undetected (unsafe clone). The old `canonical_schema_key` also split `X`/`"X"` into disjoint `q:`/`u:` keyspaces, wrongly making unquoted `X` distinct from `"X"`. FIX: a single `canonical_identifier` applying Snowflake default resolution (unquoted → UPPERCASE, quoted → case-sensitive, ONE keyspace) for catalog/schema/table; the `Canonicalizer`'s `pre_visit_ident` resolves every identifier the same way (bare-uppercase emitted unquoted so `"X"`≡`X`; lowercase/special kept quoted). LIVE-verified Snowflake stores unquoted identifiers uppercased (`experiments/quoted_identifier_case.sh`). | `sql_norm.rs::{canonical_identifier,is_bare_uppercase_identifier}`; `fingerprint.rs::{ns_quoted_case_table_component_binding_swap_differs, ns_unquoted_equals_uppercase_quoted_all_components, ns_unquoted_schema_equals_uppercase_quoted}`; `clone_submit_fallback.rs::{server_gates_table_component_quoted_case_swap_with_control, server_clones_unquoted_baseline_from_uppercase_quoted_submit}` |
| **#2 4TH FOLLOW-UP finding 2 (live-measured 2026-10-13): resolution-context stays UNRESOLVED** | SUPERSEDES the 3rd-follow-up framing that treated the context cases as settled. Reworked `resolution_context.sh` to hold the dependency DEFINITION TEXT byte-constant with a same-body CONTROL and a different-leaf-data TREATMENT. LIVE: control (same data) → `ready_to_clone`; treatment (leaf rebuilt with different data) → `ready_to_execute`. Inspecting the captured submits, the ONLY difference between control and treatment is the leaf's `last_modified_epoch` (…606014 vs …673492) — the dependency name/schema/text are byte-identical. So the treatment EXECUTE is UPSTREAM-FRESHNESS driven, NOT a namespace resolution-context distinction; a pure context-only change cannot be isolated from freshness through Fusion. The context-only policy remains UNRESOLVED / potential-unsafe; `namespace_logic_hash` is unchanged. The two misleadingly named/commented tests (`ns_*_context_only_differs`) were renamed to the cross-env positive they assert. | `fingerprint.rs::{ns_uniform_schema_rename_is_cross_env_positive, ns_unqualified_uniform_schema_rename_is_cross_env_positive}`; `experiments/resolution_context.sh` |
| **#2 4TH FOLLOW-UP finding 3 (live-measured 2026-10-13): EXECUTE payload gating + NARROWED volatile gate** | SUPERSEDES 3rd-follow-up finding 3's blanket `tolerate_nondeterminism` gate. (a) The conformance `ready_to_execute` arm compared only the VARIANT, hiding the volatile fixture's full-response mismatch (hosted populates `transformed_nodes_by_query` with the runtime `CURRENT_TIMESTAMP()` value; local returns `{}`). It now compares the FULL execute payload with a field-scoped `ignore_fields` known gap for that one unimplemented field (`clone_fallback_volatile` entries 0+2 → `known_gaps=2`, not 0) and tolerates the first-build-vs-rebuild `explained_decision` as a documented replay-state artifact. (b) LIVE truth-table: a DETERMINISTIC model with `evaluate_volatile_sql=true` (tnd=false) is STILL cloned by hosted (`experiments/clone_eligibility.sh volatile_det`), so the earlier blanket gate was too broad. The cross-name clone fallback is now suppressed ONLY when the SQL ACTUALLY contains a volatile function (`sql_norm::sql_contains_volatile_function`, a documented Snowflake nondeterministic set), a CONSERVATIVE fallback — NOT full volatile-evaluation conformance. | `services.rs` gate (`volatile_blocks_clone`); `sql_norm.rs::sql_contains_volatile_function`; `conformance.rs` execute arm + `strip_named_fields`/`execute_diff_is_only_rebuild_rationale`; `clone_submit_fallback.rs::{deterministic_model_clones_even_when_nondeterminism_not_tolerated, genuine_volatile_model_executes_when_nondeterminism_not_tolerated}`; `clone_eligibility.rs::volatile_sql_executes_but_deterministic_clones_under_nondeterminism_flag`; `golden/replay_gaps.json` ignore_fields entries |
| **#2 4TH FOLLOW-UP finding 4 (2026-10-13): RegisterClone cause RETRACTED, full-response compare** | SUPERSEDES the 3rd-follow-up claim that the entry-4 mismatch is CAUSED by the incremental self-referential predicate. That attribution is not isolated (per the reviewer's local-variation probes) — entry 4 differs from entry 0 on THREE axes at once (schema prod→dev, the added predicate, and the own-target/upstream epochs); hosted's own wording ("both its query and its upstream data are up to date") shows a post-clone same-target no-op. The test now compares entries 0–3 by FULL response value (both RegisterClone mirrors) and pins entry 4 as an UNRESOLVED DIVERGENCE (no single-cause claim, no speculative predicate-disregard rule). `registerclone_source_age.sh` audits the COMPLETE RPC history and fails on intervening `RecordExecutions`/speculative submits. | `clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay` (+`strip_ids`); `experiments/registerclone_source_age.sh` |
| **#2 4TH FOLLOW-UP finding 5 (2026-10-13): shared trace validator + warehouse-free fault tests** | The experiment oracles accepted uncorrelated evidence: `clone_eligibility.sh` never validated the selected clone source against a captured confirmed build, and `registerclone_source_age.sh` accepted a method name + positional confirm epochs without correlating successful responses/IDs. New `experiments/trace_validate.py` correlates source/clone/confirmation identities + successful outcomes and rejects missing baselines + intervening writes; both drivers use it. `experiments/trace_validate_test.py` adds 9 warehouse-free fault-path tests reproducing the reviewer's matrix (deleted baseline, unsuccessful confirm, `unable_to_clone`, unrelated confirm id, missing source build, intervening `RecordExecutions`) — all now rejected. | `experiments/trace_validate.py`, `experiments/trace_validate_test.py`, `experiments/{clone_eligibility,registerclone_source_age}.sh` |
| **#2 5TH FOLLOW-UP finding 1 (2026-10-13): reparse-invariant identifier canonicalization + fingerprint v3** | REGRESSION I introduced in the 4th follow-up: `pre_visit_ident` emitted every bare-uppercase resolved value UNQUOTED, so a quoted reserved-word column reference `"NULL"` rendered as the bare `NULL` — which reparses as the null LITERAL (a different expression kind), making `select "NULL" …` SKIP after confirming `select NULL …`. FIX: `is_bare_uppercase_identifier` now requires REPARSE-INVARIANCE (parsing `select <word>` must yield exactly an `Expr::Identifier`), so reserved words/literals/functions keep their quotes; ordinary quoted-uppercase names still unify. Both P0 fixes change canonicalization, so fingerprint versions were bumped `dbt-state-sql-v2`→`v3` and `dbt-state-ns-logic-v2`→`v3` to INVALIDATE persisted fingerprints from the unsafe normalizer on upgrade. | `sql_norm.rs::{is_bare_uppercase_identifier,projection_expr}`; `fingerprint.rs` v3 tags; `sql_norm.rs::{quoted_reserved_word_identifier_is_not_bare_keyword,ordinary_quoted_uppercase_still_equals_unquoted}`; `c2_rendered_sql.rs::quoted_reserved_word_change_forces_execute_same_target` |
| **#2 5TH FOLLOW-UP finding 2 (2026-10-13): dotted/escaped quoted object-name parsing** | `abstract_object_name_schema_with`/`schema_of_object_name` split on every `.`, fragmenting a quoted component with an embedded dot (`"X.Y"` vs `"x.y"`) and uppercasing the fragments, so a swapped binding on dotted quoted names CLONED unsafely. FIX: `split_object_name_components` parses real identifier components honoring double-quoted segments and `""` escapes; both functions use it. | `sql_norm.rs::split_object_name_components`; `fingerprint.rs::{ns_dotted_quoted_schema_binding_swap_differs,ns_dotted_quoted_table_binding_swap_differs,ns_escaped_quote_component_binding_swap_differs}`; `sql_norm.rs::object_name_component_split_respects_quotes` |
| **#2 5TH FOLLOW-UP finding 3 (2026-10-13): parse-based volatility over the dependency graph** | The 4th-follow-up gate inspected only `req.sql` (missing a volatile VIEW dependency) and used a raw-text substring fallback (a clock keyword in a COMMENT or STRING LITERAL flipped the decision; unknown functions were implicitly deterministic). FIX: `sql_contains_volatile_function` is now PURELY AST-based (matches a volatile builtin only as a function call or a bare parenless identifier, both unquoted) — comments/literals/similar-named/qualified columns are no longer false positives, unparseable is conservatively volatile. New `request_has_volatile_sql` inspects the root SQL AND every supplied dependency definition. Still a conservative suppression gate, NOT full volatile-evaluation conformance. | `sql_norm.rs::{sql_contains_volatile_function,request_has_volatile_sql}`; `services.rs` gate; `clone_submit_fallback.rs::volatile_dependency_definition_suppresses_clone`; hardened `volatile_function_detection` + `request_volatility_covers_dependency_definitions` |
| **#2 5TH FOLLOW-UP finding 4 (2026-10-13): replay-state EXECUTE gating + rebuild rationale server fix** | The conformance execute arm trusted the LOCAL "did not exist" wording and exempted the WHOLE `explained_decision` except the decision int, silently accepting differing rejection codes / `is_stale` / wording after a confirmed baseline (6 post-baseline cases). FIX (harness): the rebuild-rationale exemption is now gated on `has_history` derived from the REPLAY STATE (a prior ConfirmExecution for this target), and `ignore_fields` asserts the exempted field ITSELF differs (path-aware for `explained_decision.<field>`). FIX (server): `SubmitContext.target_has_prior_build` (`store.target_has_confirmed_history`, fingerprint-agnostic) lets `decide()` report the hosted rebuild rationale ("query didn't match or upstream out of date") when the target existed under a different fingerprint — resolving 5/6. The residual `c1_config_semantics` entry 7 (hosted srr=8/crr=7 vs 6/6) is an EXACT field-scoped gap on those two codes only. `clone_chain_provenance` entry 4 now asserts the exact local variant (not `assert_ne`). | `decision.rs::{SubmitContext.target_has_prior_build,decide}`; `store.rs::target_has_confirmed_history`; `conformance.rs::{execute_diff_is_only_rebuild_rationale,retain_named_fields}`; `golden/replay_gaps.json` entry-7 gap |
| **#2 5TH FOLLOW-UP finding 5 (2026-10-13): event-ordered trace validator** | `trace_validate.py` could let a FUTURE confirmation justify an EARLIER decision, checked the `from_speculative_submit` FLAG instead of the real `SubmitEnrichedSQLSpeculative` METHOD, accepted a `failed_to_clone=true` confirm as a baseline, accepted disagreeing RegisterClone mirrors, and never checked a baseline on the EXECUTE/negative-treatment path. FIX: `before_index` event ordering; method recognition; `_confirm_is_successful` separates physical outcome from RPC ack; `_mirrors_agree`; `require_present_baseline_for_execute`. `trace_validate_test.py` grows 9→15 warehouse-free fault tests. | `experiments/trace_validate.py`, `experiments/trace_validate_test.py`, `experiments/{clone_eligibility,registerclone_source_age}.sh` |

`decision::decide` remains pure. Store queries remain organization scoped.

## Compatibility and upgrade effects

**Fingerprint version bump v2→v3 (review #2 5th follow-up).** The persisted
logic fingerprints are versioned by a leading tag (`dbt-state-sql-v3`,
`dbt-state-ns-logic-v3`). The 5th-follow-up identifier-canonicalization fixes
(reserved-word quoting, dotted quoted names) change what those hashes produce, so
the tags were bumped from `v2` to `v3`. On upgrade, a submit's new-version hash
will not match any pre-upgrade `v2` row, so the server safely REBUILDS rather
than reusing a hash computed by the old (unsafe) normalizer. No migration is
needed; stale `v2` rows simply never match and age out. (`dbt-state-seed-v2` is
unaffected — seeds carry no SQL.)

Migration `0007_execution_results.sql` adds nullable `project_id`, binary
`execution_results`, and an index on current confirmed targets. Results use the
custom protobuf Struct bytes, preserving int64 and non-finite double fields
without JSON conversion. Existing numeric input epochs deserialize as `Some`;
new null epochs preserve unknown evidence. Corrupt rows cause an explicit error.

Migration `0008_cross_env_reuse.sql` adds nullable `node_namespace_logic_hash`
(the schema-abstracted cross-environment logic identity) and `clone_source_table`
(clone provenance), plus a namespace+logic index. Both are populated on new
executions only; pre-0008 confirmed rows have them NULL, so cross-environment
clone adoption simply does not fire for them (they fall back to the existing
strict same-target path — safe, no behavior change for legacy rows).

Migration `0009_node_states.sql` adds a new `node_states` table (keyed by
org/project_id/`target` env/node_unique_id) for the `state:*` selector
comparison store — SEPARATE from the `executions` decision ledger. It is
populated going forward on every Submit/Record that carries a `dbt_node_state`;
it has no effect on the decision path and legacy executions are unaffected. A
`state:*` selector against an env with no recorded node state simply reports
every node as `new`/`modified` until a build (in that `defer_to_target` env)
seeds it.

Migration `0010_node_state_fqn.sql` adds a nullable `target_table` column to
`node_states`, populated going forward on every Submit/Record that carries both
a `dbt_node_state` and a physical target. It powers deferral
(`ResolveDeferredRelations` resolves a node to its defer-to-env FQN); legacy
rows have it NULL and simply do not resolve. No effect on the decision path.

Migration `0011_node_template_hash.sql` adds a nullable `node_template_hash`
column to `executions` for the `compare_unrendered_code` dual identity (review
#2 finding 6). It is written only on `compare_unrendered_code=true` submits;
legacy rows and all Record/seed/clone rows have it NULL and match on the
rendered `node_sql_hash` alone, exactly as before. No backfill is needed.

SQL/seed hashes now have a versioned, length-framed SHA-256 domain. Existing
fingerprints will cold miss and rebuild; **do not rehash old rows in place**:
root SQL, dependency definitions and complete outcomes were not stored there.
Legacy test rows without recorded results also rebuild.

Current-target validation uses `current_epoch <= confirmed_epoch`, with presence
required. The seed capture has a current metadata epoch
`1791624374547` and confirmation `1791624374910` (363 ms later), so exact equality
would reject a real unchanged object. This assumes the recorded confirmation is
an upper bound for that build's warehouse modification time; timestamps alone
cannot prove object lineage. Stronger lineage and late-confirmation handling
remain below. The explicit ignore-external-modifications flag still requires
known existence evidence.

More work may execute: missing metadata, changed physical input names,
dependency representation differences (SELECT versus full view DDL), another
physical target, unsupported syntax, or requested volatile evaluation. These
are intentional conservative fallbacks, **not claims of hosted conformance**.
State selectors are now implemented (see §D). Deferral callers still receive
UNIMPLEMENTED; this is an explicit feature gap and may abort client operations
that previously succeeded incorrectly.

## Remaining work, in priority order

> **Review #2 4TH+5TH FOLLOW-UP — explicitly OPEN (do-not-infer):**
> - **Resolution-context namespace policy** (finding 2): cannot be isolated from
>   upstream freshness through Fusion; whether hosted keys dependency equivalence
>   on anything beyond the definition body + freshness is UNRESOLVED. Treated as
>   potential-unsafe; `namespace_logic_hash` unchanged.
> - **Runtime volatile-function values** (finding 3): the hosted
>   `transformed_nodes_by_query` map (e.g. `CURRENT_TIMESTAMP()` → a timestamp) is
>   NOT evaluated locally; it is a field-scoped conformance gap. The clone
>   suppression is a conservative volatile-SQL gate, not full volatile conformance.
> - **RegisterClone entry-4 divergence** (finding 4): hosted's post-clone
>   same-target no-op SKIP is not reproduced locally; the cause is multi-axis and
>   UNRESOLVED. No speculative rule implemented.
> - **Hosted view-binding fixture** `clone_fallback_dep_binding_swap.jsonl` still
>   replays as `asserted=2 unknown_baseline=1 known_gaps=1` — the prod consumer
>   that returned `ready_to_clone` was from a prior session not in the capture, so
>   it is an unknown baseline. Turning it into an independent HOSTED conformance
>   gate needs a fresh capture of its own confirmed baseline + a no-swap control.
>   (The SELF-CONTAINED gate `dep_binding_swap_gated_against_present_baseline_with_control`
>   covers the server-side property.)
> - **Rebuild rejection-code mapping** (5th follow-up finding 4): after a
>   confirmed baseline, a changed submit that misses the fingerprint now reports
>   the correct rebuild DESCRIPTION, but hosted's rejection CODES vary
>   (`c1_config_semantics` entry 7 uses skip=8/clone=7 while other post-baseline
>   rebuilds use 6/6). The 8/7-vs-6/6 selection is uncharacterized; recorded as an
>   EXACT field-scoped conformance gap on those two codes only.
> - Prior-acknowledged open items (below) remain: stale-same-target vs
>   fresh-alternative, exact lag/time-travel window, the unconfirmed/failed-submit
>   publication point, wildcard-view columns, graph/ordering, proxy fidelity, the
>   client/warehouse compatibility matrix.

### A. Physical state, concurrency and clone provenance (review 1, 7, 10)

**Clone lifecycle, eligibility, rejection and the SQL-submit cross-name fallback —
LIVE-VERIFIED + IMPLEMENTED (2026-10-11).** Characterized against the real service
(api.state.dbt.com, Fusion client, Snowflake `prod_demo`/`dev_clone`) via the
recording proxy; see `experiments/clone_experiment.sh`,
`experiments/clone_logic_sensitivity.sh`, and the fixtures
`clone_rejected_allow_clones_false.jsonl` + `clone_submit_equivalent_fallback.jsonl`.

- **Clone eligibility SPLITS by materialization.** Incremental models and
  snapshots clone via the `Clone.RegisterClone` RPC (the `pre_clone` path, default
  `if_missing`). Table materializations clone via `SQL.SubmitEnrichedSQL` →
  `ready_to_clone` (the cross-name fallback below). Clones require profile
  `defer_to_target` set and the dev object absent.
- **Clone lifecycle (register → confirm → reuse) now reproduced.** After
  `RegisterClone` + `ConfirmExecution`, a later submit for the cloned target
  SKIPS when its logic matches — and reuse is CROSS-ENVIRONMENT (schema-
  abstracted) and LOGIC-SENSITIVE: a dev submit differing from the prod source
  only in the schema qualifier skips; a genuine SQL change executes
  (`LATEST_QUERY_HASH_NOT_MATCH`=8). Implemented via a schema-abstracted
  `node_namespace_logic_hash` (`fingerprint::namespace_logic_hash`,
  `sql_norm::normalize_sql_namespace_abstracted`) recorded on every execution,
  plus clone provenance columns (`clone_source_table`, migration 0008). The
  clone-origin row inherits the source's namespace logic identity at
  RegisterClone (`store::find_source_ns_logic_hash`) and is adopted by a later
  submit only when it is clone-origin and the abstracted logic matches
  (`store::find_latest_confirmed_target`, gated to clone-origin). Pinned by
  `crates/harness/tests/clone_lifecycle.rs`.
- **`allow_clones: false` rejection.** The client encodes it as
  `clone_chain_depth_limit = 0`; `RegisterClone` returns `unable_to_clone`
  (decision=1 READY_TO_EXECUTE, `clone_rejection_reason=10`
  CLONE_CHAIN_LIMIT_EXCEEDED, empty deprecated oneof, no pending row) and the
  client builds normally. Implemented in `services.rs::register_clone`; pinned by
  `clone.rs::register_clone_rejected_when_allow_clones_false`.
- **SQL-submit cross-name `ready_to_clone` fallback.** A submit for an absent
  target with an equivalent (same namespace + schema-abstracted logic +
  `execution_type`) under another physical name returns `ready_to_clone` cloning
  it in (decision=3, `skip_rejection_reason=1` TARGET_TABLE_MISMATCH,
  `clone_required_last_modified_epoch`=source build epoch, source runtime echoed).
  Implemented via `store::find_clone_equivalent` + `services::
  submit_ready_to_clone_response`; gated to real SQL submits with a known
  namespace, `allow_clones != false`, and `execution_type` match. Pinned by
  `crates/harness/tests/clone_submit_fallback.rs`.
- **Historical clone-state must reflect CURRENT physical contents — FIXED
  (review issue #1 P0#1/P0#2, 2026-10-12).** Two clone-state lookups could treat
  a historical execution as evidence of an object's current contents:
  `store::find_clone_equivalent` filtered by (namespace, logic, type) BEFORE
  reducing to the newest row per source, so an obsolete A-logic row for a source
  since rebuilt with B could resurrect stale SQL as a clone source (A→B→A); and
  `store::find_latest_confirmed_target` applied `clone_source_table IS NOT NULL`
  BEFORE selecting the latest row, so a cloned-then-rebuilt target still resolved
  to the old clone-origin row and could be ADOPTED into an unsafe SKIP. Fixed:
  `find_clone_equivalent` now reduces to the latest confirmed row PER target
  (`DISTINCT ON (target_table)`) and only then filters by logic+type;
  `find_latest_confirmed_target` selects the latest confirmed target row of any
  provenance (caller still requires clone-origin + logic match to adopt) and
  omits `execution_results` from that lookup (adoption never serves cached
  results, so a corrupt-but-irrelevant blob can't fail a cold miss). Pinned by
  `crates/harness/tests/clone_history_provenance.rs`.
- **The cross-name fallback fires OVER an existing target — LIVE-VERIFIED
  (2026-10-12, `experiments/p0_fallback_over_existing.sh`).** A table target
  submitted with logic L1 (absent → clone), rebuilt with L2 (execute), then
  submitted with L1 again while physically holding L2 is answered
  `ready_to_clone` AGAIN. The fallback discriminator is purely "does an
  equivalent for THIS logic exist under another name", NOT target existence or
  prior build history. An inferred `!target_has_confirmed_history` gate was
  therefore REMOVED (it contradicted hosted). The P0#2 fix only prevents the
  unsafe stale-clone-origin SKIP; the subsequent clone is correct. Fixture
  `golden/fixtures/clone_fallback_over_existing_target.jsonl` (characterization-
  only), pinned by `clone_submit_fallback.rs::hosted_fallback_fires_over_existing_target`.
- **Current physical state before ALL metadata filters — HARDENED (review
  issue #1 follow-up, 2026-10-12).** The P0 fixes picked the latest row per
  source before matching logic, but three lookups still applied SOME metadata
  filters before `ORDER BY confirmed_at`: a target's current row could be
  skipped and an older matching one returned. All three now select the current
  physical state FIRST, then filter:
  - `find_latest_confirmed_target` applied `execution_type`/`table_namespace`/
    `dialect` before the ORDER BY. **LIVE-VERIFIED client-reachable
    (`experiments/physical_state_exec_type.sh`):** a model switched
    `table`→`incremental` sends `execution_type` 1→3 for the same target; after
    the target holds an et=3 build, re-submitting et=1 of the same logic is
    answered `ready_to_clone`, NOT a skip reusing the stale et=1 clone-origin
    row. Fixed via a `current_state` CTE (latest row, any type) then post-filter.
    Fixture `golden/fixtures/clone_rebuild_execution_type.jsonl`
    (characterization-only).
  - `find_source_ns_logic_hash` applied `node_namespace_logic_hash IS NOT NULL`
    before the ORDER BY, so a source whose newest build stores no logic hash
    (unrendered-comparison or empty SQL — see the `ns_logic_hash` gate in
    `services.rs`) would inherit an OBSOLETE hash onto the clone-origin row. Now
    reads the source's latest row's hash directly (may be NULL → inherit none →
    later submit EXECUTEs; safe).
  - `find_clone_equivalent` reduced to latest-per-target WITHIN the requested
    namespace, so a target whose newest state moved to another namespace could
    surface its older same-namespace row as current. Now computes
    current-state-per-target across ALL namespaces, then filters by namespace.
    `table_namespace` is a stable adapter-level id (no target changes it across
    captured traffic), so this is an OFFLINE invariant with no hosted-parity
    claim (do-not-infer rule).

  All three pinned by `crates/harness/tests/clone_history_provenance.rs`
  (`clone_adoption_must_not_resurrect_row_of_different_execution_type` —
  live-verified; `clone_source_logic_inherit_must_use_current_source_state` and
  `clone_equivalent_must_reflect_current_namespace_of_source` — offline).

Remaining in this area: dropped/recreated sources mid-decision, server restart;
clone row/time limits (`clone_time_travel_limit`, `OUTSIDE_TIME_TRAVEL_
WINDOW`=9) and deeper chain limits (>0); `defer_enabled`/`is_defer_to_profile`
nuances. **Concurrency / late-confirmation / conflicting-batch determinism is
now PINNED (2026-10-12, `crates/harness/tests/concurrency.rs`).** Hosted behavior
under true concurrency is not establishable from a sequential capture (so these
are not hosted-parity claims), but the server's rule is made explicit and
deterministic: CURRENT PHYSICAL STATE = the execution with the newest
`confirmed_at` (tie-broken by id). Confirmation order — NOT submission order —
defines current state (reversed-confirm test); a late confirm deterministically
becomes current (late-confirm test); conflicting confirmed builds for one target
both persist and the last-confirmed wins (conflicting-rows test); and
Submit↔Record agree on the model fingerprint (parity test). This models "the
last build to finish writing the object wins" — the only provenance signal
available without stable warehouse object IDs (stronger lineage remains open).
**`allow_clones=false` suppression of the SQL-submit fallback is now
LIVE-VERIFIED (2026-10-12, `experiments/clone_experiment.sh table --no-clones`):**
the client encodes it as `clone_chain_depth_limit=0` on the SUBMIT (NOT via
`allow_clones`), and the hosted service, instead of cloning the equivalent,
returns `ready_to_execute` with `skip_rejection_reason=1` (TARGET_TABLE_MISMATCH),
`clone_rejection_reason=10` (CLONE_CHAIN_LIMIT_EXCEEDED) and
`decision_description="model was executed because the maximum number of nested
clones has been reached"`. The server now reproduces this exact shape (it
previously gated only on `allow_clones != Some(false)`, which the real client
never sets, and emitted a generic execute). Fixture
`clone_submit_allow_clones_false.jsonl`; pinned by
`clone_submit_fallback.rs::allow_clones_false_suppresses_fallback_with_chain_limit_shape`.
`failed_to_clone=true` (observed with "clone source freshness
unavailable") is still confirmed as a normal build — do not reject on the flag
alone. State selectors and deferral are implemented (§D).

Hosted/warehouse questions: late-confirmation ordering; warehouse clock
semantics; source-freshness enforcement; TABLE types beyond ordinary/transient.
Do not reinstate schema stripping for the STRICT same-target identity or a
namespace-only SKIP (the cross-env path CLONES, it never skips without proven
physical provenance). BigQuery/Databricks/Spark clone templates remain
unverified. Snowflake view cloning reports unsupported; a correct view-copy
needs the actual view definition/context, not `CREATE VIEW ... CLONE`.

### B. Dependency semantics and SQL equivalence (review 2, 4, 5)

**Negative coverage ADDED (2026-10-12, offline).** Dependency-identity
properties are now pinned in `crates/server/src/fingerprint.rs` tests: the
STRICT identity distinguishes a changed dependency definition and the physical
dependency name / resolution catalog / schema (same-target reuse cannot conflate
different physical inputs); the NAMESPACE-ABSTRACTED identity excludes the
physical dependency name/catalog/schema (so the same model's view in prod vs a
dev clone shares identity) BUT still distinguishes a genuine dependency LOGIC
change, a different dependency-set size, and a root projection change; dependency
ORDER is irrelevant in both; and template vs rendered modes never collide. SQL
equivalence negatives are pinned in `crates/server/src/sql_norm.rs` tests:
quoted/qualified function names and UDFs that shadow a builtin are NOT
synonym-folded; unknown functions, cast precision/scale and target-type
differences, TRY_CAST vs CAST, COLLATE, DISTINCT/LIMIT/ORDER-direction and JOIN
kind all stay distinct (the normalizer is syntactic, over-executing is the safe
default). Only the tiny verified synonym set folds.

Offline work remaining: retain a resolved dependency graph; distinguish view DDL
from its query; handle cycles, raw references outside
the DAG, and default resolution context. Add nested views,
transitive definition changes, missing/added columns with SELECT *. Exact
fingerprinting of supplied definitions does not
establish completeness of the supplied graph. `lenient_dependencies` is still
ignored; define its behavior from primary client/docs evidence and then capture
controlled cases. Builtin synonym rules should gain warehouse validation before
being expanded.

Hosted/warehouse questions: actual equivalence catalog, parameterized numeric
and string casts, session settings/collation, SQL header effects, view-definition
representations. See existing SQL normalization experiments for individual
observations. A parser round trip alone is not a semantic oracle.

#### Review #2 findings assessment (updated 2026-10-12 after the follow-up review)

Review #2 raised 9 findings; its FOLLOW-UP review reopened several and flagged a
new regression. The determinations below are CORRECTED from the earlier
assessment where the follow-up was right.

- **Finding 1 (data-test upstream freshness).** Genuine, FIXED + LIVE-VERIFIED
  (see the completed-changes table). A data test anchors freshness on the
  recorded upstream epoch.

- **Finding 3 (abstraction erases dependency identity: BLUE/RED, swapped
  V1/V2) — CORRECTED: this WAS a genuine P0 reuse-safety bug, now FIXED
  (`1a22b8a`).** The earlier assessment wrongly CLOSED this as a non-bug on the
  strength of a two-oracle experiment that changed a SINGLE dependency (which
  changes the definition multiset). The follow-up review was right: the real
  counterexample SWAPS two dependency definitions between their references under
  an UNCHANGED root query — the sorted multiset of definition texts is
  identical, but the output differs. `namespace_logic_hash` hashed only that
  sorted multiset and discarded the (reference → definition) binding, so it
  conflated the two and could clone one into the other. FIXED: the abstracted
  identity now binds each dependency's schema-abstracted NAME to its definition
  (`sql_norm::abstract_object_name_schema`, sorted by the pair). LIVE-VERIFIED
  with two oracles (`experiments/finding3_swapped_bindings.sh`): hosted answered
  `ready_to_execute` for the swapped-binding dev submit and the actual dev
  warehouse contents were the swapped model's correct value (`(2,)`), not the
  baseline's (`(1,)`) — hosted keys on the binding, and so now does the server.
  Pinned by `fingerprint.rs::ns_distinguishes_swapped_dependency_bindings` (+ the
  metamorphic order/swap pair), the conformance fixture
  `clone_fallback_swapped_bindings.jsonl`, and
  `clone_submit_fallback.rs::{hosted_swapped_bindings_execute_not_clone,
  server_does_not_clone_swapped_binding_equivalent}`. The earlier
  single-dependency experiment remains valid as a complementary case
  (`finding3_dependency_definition_keys_clone_eligibility`).
  **2ND FOLLOW-UP CORRECTION:** the binding fix above was INCOMPLETE — it still
  abstracted every schema to a SINGLE placeholder, so two same-basename
  relations in DIFFERENT schemas (REACHABLE via custom `schema=`/`alias=`)
  collapsed and a swap of their definitions still conflated. Fixed by a RELATIVE
  schema→token map (`sql_norm::SchemaMap`; distinct schemas → distinct tokens,
  single env schema → token 0). LIVE-VERIFIED with NONEMPTY `query_dependencies`
  (two VIEW deps, constant root SQL): hosted EXECUTES, dev contents `(2,)`
  (`experiments/finding3_view_dep_swap.sh`, fixture
  `clone_fallback_dep_binding_swap.jsonl`). See the 2ND FOLLOW-UP rows in the
  completed-changes table.

- **Finding 6 (template truth table; data-test regression) — the follow-up's new
  regression is FIXED (`b210e8c`).** The finding-6 dual-identity fix threaded the
  unrendered-template hash into the MODEL lookup but NOT the DATA-TEST lookup
  (`find_confirmed_test` compared only the rendered hash), so a cuc data test
  with an unchanged template and a changed rendered literal wrongly EXECUTED.
  `find_confirmed_test` now takes `node_template_hash` and matches EITHER hash.
  LIVE-VERIFIED (`experiments/data_test_template.sh`): a data-test Jinja-comment-
  only edit SKIPS on hosted. Pinned by `data_test_template.rs` (4 cells + a
  hosted-shape pin) and the conformance fixture
  `data_test_compare_unrendered.jsonl`. The model template cell remains fixed +
  live-verified. The wildcard-view column-evidence sub-claim remains OPEN.

- **Finding 5 (RecordExecutions atomicity) — now FULL cross-store atomicity
  (`b58e30c`), not just the error path.** The earlier fix stopped a rejected
  batch leaking logical state, but the executions batch still committed BEFORE a
  best-effort `node_states` write, so a logical-state write failure left a
  durable execution row with no logical state. Both tables are now written in a
  SINGLE transaction (`store::insert_confirmed_batch_with_node_states`): a
  node_states fault rolls the WHOLE request back. Pinned by the fault-injection
  test `correctness.rs::node_state_write_failure_rolls_back_whole_record_batch`
  (CHECK(false) on node_states) + the happy-path publish test. The separate
  unconfirmed/failed-submit publication-point question (finding 5A) remains OPEN
  (needs live pending/failed-build characterization).

- **Finding 2 (clone fallback eligibility) — determination REFINED with the
  source baseline now captured (`953ea40`).** The follow-up correctly noted the
  earlier `clone_fallback_xenv_fresh.jsonl` did not capture the cloned source's
  provenance. A controlled experiment (`experiments/clone_eligibility.sh`) now
  captures the full chain self-contained: the source builds and CONFIRMS (its
  build epoch recorded), the upstream is bumped, and a dev submit whose CURRENT
  upstream is NEWER than the source build still returns `ready_to_clone` from
  that STALE source. So hosted TOLERATES source staleness and does NOT
  freshness-gate the clone candidate; the server's `find_clone_equivalent` also
  clones here, MATCHING hosted — it is NOT over-conservative, and adding the
  review's implied freshness gate would reuse LESS than hosted (a regression).
  The fallback is intentionally NOT tightened. Fixture
  `clone_fallback_stale_source.jsonl` (conformance-replayed) pinned by
  `clone_eligibility.rs`.
  **2ND FOLLOW-UP:** `lag_tolerance`/`evaluate_volatile_sql` ARE reachable via
  the nested `state={}` config (not top-level keys). With distinct arms and a
  wire-field VARCHECK, a STALE cross-name source STILL returns `ready_to_clone`
  under zero `freshness_tolerance_seconds` (that dimension does not gate the
  fallback; the server matches —
  `clone_eligibility.rs::server_clones_stale_equivalent_under_zero_tolerance`).
  **3RD/4TH FOLLOW-UP CORRECTION:** the companion claim that
  `tolerate_nondeterminism=false` never gates the fallback was WRONG. A GENUINELY
  volatile model (`current_timestamp()`) is NOT cloned (it executes), while a
  DETERMINISTIC model carrying the same flag IS still cloned (live:
  `experiments/clone_eligibility.sh volatile`/`volatile_det`). The server now
  suppresses the clone only when the SQL actually contains a volatile function
  (`sql_norm::sql_contains_volatile_function`).
  The stale-same-target-vs-fresh-alternative suppression dimension and the exact
  clone tolerance window / OUTSIDE_TIME_TRAVEL_WINDOW remain OPEN (need dedicated
  controlled captures).

- **Finding 4 (RegisterClone provenance / content age) — CHARACTERIZED
  (`46e50e7`, `da4d712`).** A live clone-chain (`experiments/clone_chain_provenance.sh`)
  settles the post-clone drift question: after a clone is confirmed, bumping the
  upstream NEWER than the clone's build epoch makes a re-submit for the clone
  target `ready_to_execute` ("… its upstream data is out of date"). So hosted
  anchors a post-clone target's freshness on the CLONE's build epoch and REBUILDS
  when an upstream advances past it — the own-build anchor the engine implements;
  a clone is NOT perpetually fresh.
  **2ND FOLLOW-UP (corrections):** (a) the upstream-drift capture alone could not
  distinguish source-age from clone-age; a dedicated RegisterClone chain
  isolating `source build < upstream < clone creation` shows hosted SKIPs → it
  anchors on the CLONE CREATION epoch (clone-age), NOT source-content age
  (`experiments/registerclone_source_age.sh`, fixture `clone_chain_source_age.jsonl`,
  `clone_chain_provenance.rs::{hosted_clone_age_not_source_age_shape_is_pinned,
  server_clone_age_anchor_skips_upstream_between_source_and_clone}`). The server's
  own-build anchor reproduces this, so no speculative source-epoch validation is
  added (hosted does not do one). (b) The namespace-transition sub-case was NOT
  actually hardened earlier — `find_source_ns_logic_hash` still filtered
  namespace BEFORE the ORDER BY, leaking a stale same-namespace logic hash onto
  the clone-origin row. NOW FIXED (latest-row-first CTE, then post-filter;
  `clone_history_provenance.rs::clone_source_logic_inherit_must_reflect_current_namespace`).
  Deeper multi-step chain limits remain OPEN.

- **Finding 7 (lag/graph/ordering, physical identity across accounts).** Already
  documented open / characterization-only in §C. No new genuine bug.
- **Finding 8 (recording-proxy fidelity).** A test-oracle improvement (forward
  redacted client metadata, capture errors/trailers/raw bytes, fail on capture
  loss), not a server reuse-safety or conformance defect. Tracked as harness
  work; it does not change any decision.
- **Finding 9 (wire/client compatibility holes, auth = trusted-header scoping).**
  Explicitly deployment requirements and known documented gaps (see §D).

### C. Lag tolerance and template comparison (review 11, 12)

**Lag/data freshness — RESOLVED (live-verified 2026-10-11).** The freshness
rule was characterized against the real service (api.state.dbt.com, dbt Fusion
client, Snowflake `prod_demo`) via the recording proxy. A model with
`lag_tolerance=5m` over an upstream whose physical table was recreated in place
with identical content (advancing its modification epoch WITHOUT any dbt SQL
change, isolating pure data freshness from the upstream-logic-change bypass)
rebuilds iff:

    max_upstream_epoch > own_last_build_epoch

That is: an upstream newer than the object we last built forces a rebuild. The
comparison anchor is the node's OWN last-build epoch — NOT the upstream epoch
recorded at that build (the previous implementation's anchor), and NOT a
wall-clock build-age gate (the lag_tolerance doc's prose and the pre-hardening
review both hypothesized the latter; neither matched the observed behavior).
Critically, `freshness_tolerance_seconds` did NOT absorb the gap: a 300s
tolerance still rebuilt on an upstream only tens of seconds newer than the
build, and `is_stale=false` on every captured entry (staleness is a separate
axis). The previous `current_input > recorded_input + tolerance` rule could SKIP
when the upstream-delta was within tolerance even though the hosted service
rebuilt — the UNSAFE (stale-serving) direction.

`decision.rs::is_stale` now anchors on `prev.last_modified_epoch` (own build
epoch), with ANY/ALL preserved and `freshness_tolerance_seconds` intentionally
NOT applied (applying it would widen the skip window and reintroduce the unsafe
skip; the only safe direction when uncertain is to execute more). Data-test
nodes (`execution_type=8`) have no own object and no build epoch; they are
governed by identity/logic match, not this axis. Pinned by the unit tests in
`decision.rs` (`freshness_*`), the `decision_proptest.rs` oracle, the golden
fixture `golden/fixtures/lag_freshness_own_build.jsonl`, and the characterization
test `crates/harness/tests/lag_freshness.rs`.

Remaining lag questions (not blocking):

- **`require_fresh_data_from` ANY/ALL — VERIFIED LIVE 2026-10-11 (multi-upstream).**
  A model cross-joining two independent upstreams, each recreated in place to
  advance only its epoch, was characterized under both policies (fixture
  `golden/fixtures/lag_multi_upstream_policy.jsonl`,
  `experiments/lag_experiment.sh`). ALL rebuilds only when EVERY direct parent
  drifted; with one-of-two drifted it SKIPS. This holds even at
  `lag_tolerance=0`, proving the ALL skip is PURE POLICY (not lag absorption).
  Matches the engine's `drifted == considered` rule. Pinned by
  `crates/harness/tests/lag_policy.rs` and `decision.rs` unit tests.

- **`is_stale` reporting — VERIFIED LIVE + FIXED 2026-10-11.** The hosted service
  reports `is_stale=false` on EVERY execute (including an upstream-drift
  rebuild — lag fixture entry 5 and the ANY captures), and `is_stale=TRUE` only
  on a tolerated-stale SKIP (ALL policy with some parents fresh: the reused
  result knowingly serves slightly stale data). The engine previously hardcoded
  `is_stale=false` on skips and set `is_stale=true` on drift executes — both
  wrong. `decision.rs::evaluate_freshness` now returns `{should_rebuild,
  drift_present}`; `Verdict::Skip` carries `is_stale = drift_present` and every
  execute reports false. Pinned by `decision.rs` (`*_is_stale_*` tests) and
  `lag_policy.rs`.

- **`lag_tolerance` gating — NOW PROVEN TO EXIST (open: exact rule).** The prior
  conclusion that `lag_tolerance` NEVER gates was overturned. The multi-upstream
  capture contains ANY-policy SKIPS with `is_stale=true` whose
  `decision_description` is *"model was a no-op because its query is up to date
  and its data is within lag tolerance"*, where an upstream is newer than the
  own build by LESS than `lag_tolerance` (e.g. +53.7s under a 300s tolerance).
  So `lag_tolerance` CAN suppress a rebuild under ANY policy. However, multiple
  controlled single-run experiments (`experiments/lag_isolate.sh`,
  `lag_buildage.sh`) could NOT reproduce it: a fresh build followed by an
  upstream bump always rebuilt on the first drifted submit regardless of
  tolerance or build-age, because each drifted submit immediately re-advances
  the build epoch past the upstream. The confounding variable (which clock the
  tolerance is measured against — wall-clock since upstream change, wall-clock
  build-age, or a server-side "last rebuild" ledger that persists across submits
  without a client confirm) is NOT yet isolated. The engine therefore STILL uses
  the own-build anchor (rebuild on any ANY-drift) — the SAFE direction (it
  executes more, never serves staler data than hosted). DO NOT implement a lag
  gate until the exact trigger is isolated from a clean, un-confounded capture;
  a wrong gate reintroduces unsafe skips. The evidence is pinned by the
  `hosted_policy_is_stale_truth_table` assertion in `lag_policy.rs` (the
  ANY-within-lag-tolerance skip must remain in the corpus).

- Direct-parent vs leaf propagation of the upstream-logic-change bypass remains
  uncharacterized.

The own-build anchor itself remains verified for the ANY path:
`golden/fixtures/lag_freshness_own_build.jsonl`, `decision.rs` `freshness_*`
tests, `decision_proptest.rs` oracle, `crates/harness/tests/lag_freshness.rs`.

`compare_unrendered_code=true` is now DUAL-IDENTITY and LIVE-VERIFIED (review #2
finding 6, 2026-10-12): the server computes BOTH the rendered logic hash
(`node_sql_hash`, always) and the unrendered-template hash (`node_template_hash`,
migration 0011), and `find_confirmed` reuses when EITHER matches the recorded
row. This reproduces the documented contract — a rebuild occurs ONLY when BOTH
the rendered SQL AND the template change — and both cells are captured: template
same + rendered changed → SKIP (`compare_unrendered_code.jsonl`) and template
changed + rendered same → SKIP (`template_compare_unrendered.jsonl`, a
Jinja-comment-only edit;  `experiments/template_comparison.sh`). Both hashes fold
in the same dependency/context/extras components, so a genuine dependency or
context change still invalidates both and rebuilds. When `compare_unrendered_code`
is false only the rendered hash is matched, as before. Previously the server used
`template = node_body_hash` EXCLUSIVELY in cuc mode, so a template-only change
(identical rendered SQL) wrongly EXECUTED. Pinned by
`crates/harness/tests/template_comparison.rs` and
`golden/fixtures/template_compare_unrendered.jsonl`.

Still open on templates: the four-cell matrix with the flag BOTH ways and
`compare_unrendered_code=false` transitions, macro/var changes, and actual
Python/Rust clients. `SQLExecution` in Record has no comparison flag: it records
rendered mode (`node_template_hash` NULL), so a cuc submit after a Record-only
history reuses on the rendered identity alone — acceptable but uncharacterized;
decide how Record should seed the template identity once hosted behavior is
captured.

### D. Full public client feature set (review 8, 9 and coverage matrix)

- **State selectors — LIVE-VERIFIED + IMPLEMENTED (2026-10-11).**
  `SelectorService.GetStateSelection` (dbt State-powered `state:*` selectors) is
  implemented and conformant. Characterized against the real service
  (`api.state.dbt.com`, jaffle-shop, Snowflake `prod_demo` baseline, selector
  queried from `dev_clone` whose `defer_to_target: prod_demo`) via the recording
  proxy (`experiments/selector_experiment.sh`); fixture
  `golden/fixtures/selector_state_selection.jsonl` (67 distinct queries across
  all 10 criteria + every change type), pinned by
  `crates/harness/tests/selector_conformance.rs` (set-equal replay) and
  `selector_offline.rs` (record-path + org/project/target scoping). Verified
  semantics (see protocol.md): the server records the client-supplied
  `DbtNodeData` hashes verbatim on every Submit/Record (migration 0009
  `node_states`, keyed by org/project_id/`target` env/node_unique_id) and does a
  pure hash comparison. `new`/`old` and `modified`/`unmodified` partition the
  sent set; `modified = new ∪ body ∪ configs ∪ persisted_descriptions ∪ macros ∪
  contract` with **`relation` excluded**; a new node matches every `modified.*`;
  untracked node types are always new/modified; `node_body_hash` is a raw
  (whitespace-sensitive) file hash. Pure engine in `crates/server/src/selector.rs`
  (`classify`, 13 unit tests). Open: the client batches ≤10,000 nodes/request —
  the server handles a single large request; multi-batch union is client-side.
  **Baseline is LAST-WRITE-WINS — LIVE-VERIFIED (2026-10-12,
  `experiments/selector_last_write_wins.sh`).** Re-recording a node in the
  defer-to env moves the comparison baseline to the NEWEST recorded execution: a
  node rebuilt there is then reported `modified` against its own previous
  content. This confirms `store::upsert_node_state`'s latest-wins
  `ON CONFLICT DO UPDATE` is hosted-faithful for sequential semantics. Fixture
  `golden/fixtures/selector_last_write_wins.jsonl`; pinned by
  `selector_offline.rs::selector_baseline_is_last_write_wins` and
  `selector_conformance.rs::last_write_wins_baseline_reproduces_hosted`.
  Concurrent/racing-record ordering is NOT establishable from a capture and is
  intentionally not implemented as a guard (would be speculative); the
  last-arriving write wins by design. **Best-effort write is now OBSERVABLE:**
  `record_node_state` increments `AppState::selector_write_failures` on a store
  error (still never failing the decision path), so a silent baseline-staleness
  divergence is a surfaceable metric. Pinned by
  `selector_offline.rs::selector_state_write_failures_are_observable`.
  **Deferral** (`ResolveDeferredRelations`) is now IMPLEMENTED + LIVE-VERIFIED
  (2026-10-12, `experiments/deferral_experiment.sh`). Building from `dev_clone`
  (`defer_to_target: prod_demo`) and selecting only a downstream node defers its
  upstreams; the hosted service answers
  `ResolveDeferredRelations(profile, target=prod_demo, project, [uids])` with a
  map `node_unique_id -> "CATALOG"."SCHEMA"."OBJECT"` for each node built in that
  env (the submit `target_table`). The earlier empty-map captures were simply
  the not-yet-built case. The server records `target_table` in `node_states`
  (migration 0010) and resolves (org, project_id, target env, uids) → FQN,
  omitting unbuilt nodes. Pinned by `crates/harness/tests/deferral.rs` and the
  fixture `golden/fixtures/deferral_resolve.jsonl`.
- Implement volatile-function evaluation and `transformed_nodes_by_query`,
  including client execution/confirmation of transformed expressions. Current
  BUILD fallback does not reproduce the transformation protocol.
- SQL-submit `ready_to_clone` is now IMPLEMENTED for the cross-name equivalence
  case (see §A: `clone_required_last_modified_epoch`, TARGET_TABLE_MISMATCH
  rejection, source runtime echo). Still remaining: `query_hash_metadata_info`
  on `ready_to_execute`, `transformed_nodes_by_query`, and the complete rejection
  reason / description matrix across every submit branch (we populate the
  verified ones; others are the conservative BUILD/SKIP/CLONE set).
- Explain RPCs still return empty defaults; ClientValidation accepts every
  version; speculative SQL always returns Undecided. Establish exact supported
  client versions and behavior instead of advertising general coverage.
- ClientTelemetry is not served; investigate whether supported clients require
  its RPCs (including session methods). Health reports serving but is not a
  database-readiness check. Treat telemetry/observability as lower priority
  unless required for client compatibility.
- Header-based org selection is isolation, **not authentication**. Auth/TLS/
  trusted identity mapping remain deployment work. UI uses one configured org,
  not authenticated multi-user tenant selection. Bound asynchronous capture
  work/retention before enabling it under heavy load.
  **Hardening ADDED (2026-10-12):** `DBT_STATE_REQUIRE_ORG_HEADER=true` makes the
  server REJECT (`UNAUTHENTICATED`) any request lacking a non-empty
  `x-organization-id` header instead of silently bucketing it into the shared
  `local` tenant. Off by default (preserves the insecure local-dev flow where
  the client omits the header). This enforces org SCOPING PRESENCE — it still
  does NOT authenticate the claimed org against a trusted bearer token, which
  remains the deployment-side work (OAuth/TLS/trusted-identity). Pinned by
  `crates/harness/tests/auth.rs` and a `config.rs` unit test.

### E. Independent testing foundation

The pure freshness properties encode current policy assumptions and only test
an already selected candidate. They do not prove store selection, physical
validity or official behavior. Build an independent state machine at the gRPC
boundary with modelled physical contents/version, scopes, builds, confirms,
records, failures and clone transitions. Generate operation sequences; assert
that SKIP preserves the independently expected contents/results. Include
metadata permutations, restart, malformed requests, nulls, future/extreme
epochs, and migration from old confirmed rows. Extend arbitrary UTF-8 testing to
the public gRPC path and numeric/cast negative cases.

Replay now loads all fixture files and preserves outcome results/table type/
failed-clone/runtime fields, correlating confirms by request ID. It never
manufactures pre-capture history. Leading SKIPs and uncorrelated confirms are
reported as unknown baseline; clone/deferral gaps are counted. Complete causal
SKIP responses are compared after stripping only opaque IDs. Rebuild variants
are always checked, including after previous confirms. Organization metadata is preserved for stateful submit/confirm calls. Full rebuild response
fields still need equivalent initial preconditions; first-build descriptions
cannot be compared to a hosted prior-history rebuild without that history.
Six current causal mismatches are pinned in [golden/replay_gaps.json](../golden/replay_gaps.json)
(zero-based entry indices): c1_config_vs_logic entries 2/3 change dependency
SELECT text to view DDL (and use lag-specific descriptions); the second general
corpus entries 13–16 reuse data tests across environments. They must currently
BUILD locally; the replay checks that fallback and fails if an exemption becomes
unused or resolved. They are not counted as hosted agreement. Keep any explicit known mismatch narrowly keyed to a fixture entry with its
reason, never exempt a whole response family silently.

The opt-in differential fuzz tool remains **empty-history discovery**, comparing
variants only. It does not establish stateful conformance, even at zero
reported divergences. Build equivalent scenario state in both services,
compare complete responses/errors after ID remapping, save seeds and outcomes,
shrink stateful divergences, and distinguish history mismatch from genuine
policy mismatch. Transport/decode failures now fail the run. Do not run hosted
fuzzing while Snowflake/hosted validation is unavailable.

## Research sources for the next agent

Re-read primary sources when changing behavior:

- [dbt State overview](https://docs.getdbt.com/docs/deploy/dbt-state-about),
  [dbt State configs](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs)
  (the reuse → clone → build ladder; `lag_tolerance`, `require_fresh_data_from`,
  `pre_clone`, `allow_clones`, `defer_to_target`, `compare_unrendered_code`,
  `evaluate_volatile_sql`), and
  [deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).
- [Public Rust client at reviewed commit](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac):
  `crates/dbt-tasks-core/src/run_cache/run_cache_service.rs` (metadata, view
  traversal, transformations, cached tests, clone fallback), `run_cache_request.rs`,
  `crates/dbt-state/src/run_cache_defer.rs`, and the vendored protobuf definitions.
- [Public Python client at reviewed commit](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae):
  `clients/dbt_state/src/dbt_state/run_cache.py` (label identity, target metadata,
  seed semantic extras and test outcomes).
- Live capture workflow: [harness.md](harness.md) + the `experiments/` drivers
  (`clone_experiment.sh`, `clone_logic_sensitivity.sh`, `lag_experiment.sh`,
  `lag_isolate.sh`). Each isolates one behavior and only mutates `CDO_DEMO_DB`.

Wire schemas match the reviewed Rust client exactly; older Python schemas and
identity representations differ. Pin the supported client versions in a future
compatibility matrix, not just the proto commit.

## Validation

The offline suite needs only Postgres at DATABASE_URL (default in
[development.md](development.md)); none of it requires Snowflake or hosted access:

```bash
cargo test --workspace
DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid cargo test -p dbt-state-server --lib
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
```

Last run (Rust 1.98, disposable Postgres 17; 2026-10-13):

- Workspace: **275 tests passed** across the workspace (adds this session's
  review-#2 5TH FOLLOW-UP fixes on top of the earlier work: the reparse-invariant
  identifier canonicalization (quoted reserved words keep their quotes) with the
  fingerprint `v3` bump; the dotted/escaped quoted object-name parser; the
  parse-based volatility detection over the whole dependency graph; the
  replay-state-gated EXECUTE comparison + the `target_has_prior_build` rebuild
  rationale server fix with its entry-7 field-scoped gap; and the event-ordered
  trace validator). The 15 warehouse-free `trace_validate_test.py` checks are run
  separately (`python3 experiments/trace_validate_test.py`). (Env note: the
  shared Postgres test DB accumulates `difftest_` schemas — tests do not
  auto-drop their isolated schema; drop them if the volume fills.)
- Server library: **118 tests passed** with DATABASE_URL pointing to an
  unreachable port, verifying the engine needs no database (adds the
  reserved-word, dotted-name and parse-based-volatility `sql_norm` tests).
- Formatting and Clippy (all targets, fuzz feature, `-D warnings`): passed.
- Conformance replay: **26 fixtures causally replayed** with REAL CLONE gating
  (the `ready_to_clone` arm now compares the local clone payload and replays its
  confirmation — review #2 2nd follow-up P1 #3). Exact-replay captures promoted:
  `clone_fallback_swapped_bindings`, `clone_fallback_dep_binding_swap`,
  `data_test_compare_unrendered`, `clone_fallback_stale_source`,
  `clone_chain_upstream_drift`, `template_compare_unrendered`, and
  `data_test_upstream_freshness` (whose test-node assertions replay, with the
  externally-replaced upstream model isolated as a MISSING-HISTORY gap).
  Captures that clone from a source built in a PRIOR session are keyed as
  missing-source-history gaps in [golden/replay_gaps.json](../golden/replay_gaps.json)
  (clone_chain entries, clone_failed_fallback, dep_binding_swap,
  clone_submit_equivalent_fallback) rather than silently counted. The remaining
  characterization-only captures (`lag_multi_upstream_policy`,
  `selector_state_selection`, `clone_fallback_over_existing_target`,
  `selector_last_write_wins`, `deferral_resolve`,
  `clone_submit_allow_clones_false`, `clone_rebuild_execution_type`,
  `clone_fallback_xenv_fresh`, `clone_chain_source_age`) are excluded and
  validated by their dedicated tests.

Live verification (clone + lag paths, §A and §C) was performed against the real
service and Snowflake demo warehouse via the recording proxy; those captures are
promoted into `golden/fixtures/`. The offline suite above does **not** make
hosted/Snowflake requests. Re-run the live `experiments/` drivers only against a
disposable demo schema (`CDO_DEMO_DB`) when re-characterizing.

Re-run the commands above against a new local Postgres
instance before extending behavior; do not use the removed temporary database
or untracked `/tmp` research clones as required inputs.
