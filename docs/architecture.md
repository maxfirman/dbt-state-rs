# Architecture

Cargo workspace with six crates. The server depends on the proto, auth and
broker crates; the harness depends on them to drive replay and differential
tests.

```
crates/proto     generated tonic stubs (server + client) from the .proto files
crates/auth      auth core: scope format, TOML auth config, RS256 tokens and JWKS, mapping rules, authenticated context
crates/broker    Token broker: OIDC discovery, JWKS, browser sign-in through the Upstream IdP, client_credentials pass-through
crates/server    the dbt State server (decision engine + Postgres store + gRPC)
crates/harness   recording proxy, state probe, golden fixtures, differential + fuzz tooling, tests
crates/ui        optional read-only console, scoped to one configured organization
```

## proto crate

`build.rs` compiles the vendored `.proto` files with `tonic-prost-build`,
generating **both** server and client stubs (server for us, client for the proxy
and differential tests). Two notable build steps:

- **serde derives** are added to every message (`type_attribute`) so the proxy
  can emit JSON golden files that are human-diffable and replayable. The one
  `google.protobuf.Duration` field (`SessionEndRequest.session_duration`) uses
  a lossless optional seconds/nanoseconds serializer. Older captures omitted
  it; the omission still deserializes as absent.
- **`Clone` service collision patch.** The proto has a service literally named
  `Clone`; tonic generates a trait `clone_server::Clone` that shadows
  `std::clone::Clone` and makes the std-derive impl fail to compile. `build.rs`
  rewrites that one generated impl to `impl<T> std::clone::Clone for
  CloneServer<T>`, preserving the on-wire service name
  `com.fivetran.query_cache.Clone`.

Modules: `dbt_state_proto::query_cache` and `dbt_state_proto::grpc_health`.

## server crate

| Module | Responsibility |
|---|---|
| `config.rs` | Env config: `DBT_STATE_LISTEN` (default `127.0.0.1:50051`), `DATABASE_URL`, `DBT_STATE_AUTH` (`none` or `oidc`), `DBT_STATE_AUTH_CONFIG` (`oidc`: TOML auth config, loaded here), `DBT_STATE_ALLOW_INSECURE` (`none` on a non-loopback address), `DBT_STATE_REQUIRE_ORG_HEADER` (`none`: reject requests without `x-organization-id`; a startup error in `oidc`), `DBT_STATE_BROKER_LISTEN` (`oidc`: run the Token broker in-process on this HTTP address; a startup error in `none` or when the config cannot run a broker). Every startup refusal happens before connecting. |
| `main.rs`, `admin.rs` | The binary. No arguments serves gRPC; `revoke-sessions --issuer <i> --subject <s>` (`admin.rs`, needs only `DATABASE_URL`) revokes every Token broker session of a Principal (`dbt_state_broker::revoke_sessions`), prints the count and logs on stderr; an unknown Principal is an error. Any other argument is refused, so a misspelled command never serves. |
| `lib.rs` | `AppState` (`store`, `require_org_header`, `clock`, the `oidc` authenticator), `Clock` (`System`, test-only `Replay`), `build_router()`. `AppState::connect` opens the pool and runs the migrations; `with_oidc` selects `oidc` mode (`with_oidc_clock` with the clock the person session check reads; tests move it). |
| `main.rs` | Binary: tracing setup, `Config::from_env`, `AppState::connect`, serve. |
| `grpc_auth.rs` | The authentication layer in front of every service except `grpc.health.v1`. In `oidc` mode it verifies the token and the Organization (`dbt_state_auth::RequestAuthenticator`), puts the `AuthenticatedContext` in the request extensions, and refuses with `UNAUTHENTICATED` / `PERMISSION_DENIED` (logged at `warn`). Without `[signing]` it checks against the Token broker's keys (`dbt_state_broker::RemoteKeys`), refreshed when stale or after a signature miss. In `none` mode it passes calls through. A person token also needs an active Token broker session (`dbt_state_broker::SessionCheck`: not revoked, not past its 12-hour cap; each status cached on the replica for at most 30 s): an inactive one is `UNAUTHENTICATED`, an unreadable status `UNAVAILABLE`. Machine tokens (no `sid`) are not checked. |
| `request_scope.rs` | One `RequestScope` per gRPC call: the Organization, the Principal (`oidc` mode; from the `AuthenticatedContext`), the dbt invocation id and the session id. `request_scope::organization` is the only place the Organization is resolved: in `oidc` mode from the `AuthenticatedContext`; in `none` mode from `x-organization-id`, default `local` (`UNAUTHENTICATED` when `require_org_header` is set and it is missing). |
| `services.rs` | gRPC handlers. Each one that scopes by Organization first resolves its `RequestScope`, then calls `submit_decision`, `copy_registration` or the store with its Organization and Principal, maps outcomes to the wire (both clone oneofs) and spawns capture from the same scope. Re-exports `sql_execution` for upgrade tests. |
| `submit_decision/` | How one SQL or values submit is decided (flow below), and the speculative submits that are decided through the same code. Returns a `SubmitOutcome` carrying its `DecisionRecord`; `submit_sql_explained` also returns the `ExplainEvidence` capture persists, in the decision's resolved naming. Pure internal seams: `copy_search.rs` (which equivalent the Result copy fallback copies) and `diagnostics.rs` (which refusal diagnostic asks State history what, in hosted's precedence order). |
| `copy_registration.rs` | How one RegisterClone request is decided and recorded: `result_copy::register_lookup`, the one matching State history read, `result_copy::plan_register`, and the pending copy row. Returns a `RegisterOutcome`. |
| `reuse_candidate.rs` | Pure Reuse candidate selection on the latest row (strict identity match or copy-origin adoption), then Reuse qualification. Reports why there is no candidate, and which build an execution type change is judged against. |
| `data_test_history.rs` | Snowflake data-test history by key (#200): which build each input of a data test stands for (`stand_in`, pure: an unchanged copy this server recorded stands for its source build) and the key recomputed over those readings (`read_through_copies`). |
| `decision.rs` | The pure decision engine (`decide`, `reusable`), per-upstream freshness (`upstream_freshness`), decision reasons and hosted descriptions. |
| `decision_proptest.rs` | Property tests for `decide` (a `#[cfg(test)]` submodule of `decision`). |
| `result_copy.rs` | Pure Result copy planning for the submit fallback, the Snowflake seed copy on submit (#91) and RegisterClone, and the rejection codes of a submit that is not copied. Asks the Warehouse rules which execution types a submit may copy, and for chain limits, relation checks, source evidence, the incremental contract and DDL. |
| `analyzed_workload/` | Pure shared Submit/Record analysis. `analyze(&SqlExecution, Purpose)` is the one entry for a workload: it returns the strict, template and copy identities its purpose publishes, the copy proof and direct provenance, genuine inputs and whether the SQL reads a relation. `identity.rs` holds the versioned hash functions (also used directly for counterfactual readings) and their regressions (`identity/tests.rs`, `identity/pinned_values.rs`); shared AST traversal and checked dependency bindings live alongside. Warehouse adapters retain dialect policy and characterized strict domains. |
| `copy_lifecycle.rs` | Candidate qualification, frozen copy sources/inputs/producers and subsequent adoption for submit copies and RegisterClone. Distinct legacy origin proofs preserve their characterized eligibility and age rules. |
| `databricks_submit_copy.rs` | Bounded Databricks FULL automatic-copy request policy; identity and provenance implementation live in shared analysis/lifecycle. |
| `execution.rs` | Execution evidence: identity construction, pending SQL/seed/copy rows, and atomic RecordExecutions publication with selector/deferral state. |
| `volatile_evaluation.rs` | Pure Volatile evaluation (#67): renders each Volatile function the Warehouse has a captured rule for (`Warehouse::volatile_rules`), in the cells it was captured rendering in (`Rules::cells`), from an injected clock and random source (`Sources`), for `transformed_nodes_by_query` and Explain. Snowflake and Databricks rules live in `warehouse/`. Called by `submit_decision` after the decision; never an input to identity, fingerprints, Upstream producers or Reuse qualification. |
| `explain.rs` | Pure re-derivation of `dbt state explain` trees from a persisted node decision and its matched execution ([explain-catalog.md](explain-catalog.md)). `ExplainNode::of_decision` maps a persisted row to the generator's input. A Warehouse's captured volatile forms come from `Warehouse::volatile_explain_forms`. |
| `selector.rs` | Pure engine for `state:*` selectors (`SelectorService.GetStateSelection`). |
| `capture.rs` | Best-effort, non-blocking write of the UI domain rows and one `node_decisions` row per decision. |
| `sql_identity.rs` | Checked DuckDB strict identity and relation-read proofs. |
| `sql_norm.rs` | Restricted Snowflake AST canonicalization; unsupported syntax stays exact ([sql-normalization.md](sql-normalization.md)). |
| `warehouse.rs`, `warehouse/` | Interprets the wire dialect. The Warehouse rules answer each warehouse-specific question (fingerprint version tags, namespace identity, Result copy eligibility, DDL, relation checks, chain and candidate limits, provenance walk, incremental contract, build time, Volatile evaluation cells, volatile Explain forms, the changed volatile rerun and producer revision retention), with `warehouse/snowflake.rs`, `warehouse/duckdb.rs` and `warehouse/databricks.rs`. Databricks identifier resolution and SQL equivalences are bounded to the characterized subset; other warehouses get conservative defaults (ADR 0004). |
| `store.rs` | Postgres via sqlx: pending inserts, confirm-once, Record batches with `node_states` in one transaction, `node_states` upsert/fetch for selectors, confirmed execution scope reads for deferral, and `fetch_explain_decisions` (node decisions with their matched execution) for Explain. |
| `store/history.rs` | Scoped State history reads: a Target relation's current physical state (one or many), its history, a data test's latest run under its history key or its latest scoped outcome by UID, copy candidates, copy-chain depth and copy-source lookups. Derives warehouse-family and canonical physical lookup keys through the centralized Warehouse naming rules to match generated persisted keys ([ADR 0010](adr/0010-preserve-raw-identifiers-with-physical-history-keys.md)); Reuse qualification, freshness assessment and Result copy policy remain outside storage. |

### Shared analyzed workload contract (#181–#186)

This is the staged implementation contract. The behavior-preserving #183
extraction implements the shared placement and lifecycle below; the broader
Databricks grammar and v2 proofs are implemented by #184/#185. [ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md)
records why it replaces the existing paths. #183 changes placement only;
#184/#185 introduce new Databricks copy semantics. The initial milestone kept
both paths default off; the [accepted rollout](databricks-copy-preview.md) now
enables them without server switches while preserving client copy controls.

The seam has one pure analysis entry, conceptually
`analyze(&SqlExecution) -> AnalyzedWorkload`, shared by Submit and Record.
The dialect selects its adapter internally. The result contains:

- Existing strict rendered/template identities and raw SQL/context for exact
  fallback. Analysis failure never substitutes a partially normalized identity.
- A structural query graph: ordered projections with output names, expression
  trees, filters, ordered joins and predicates, CTE scopes, grouping and HAVING.
  It preserves operator/literal/cast syntax wherever no equivalence is proven.
- Reference bindings: lexical relation occurrence → physical relation key or
  supplied dependency definition; dependency name → analyzed definition in its
  own catalog/schema context. Repeated/self-join occurrences remain distinct.
- Node kind and semantic configuration, context/function effects, and either a
  versioned copy identity with a complete binding proof or an explicit opaque
  reason (syntax, binding, context, function or configuration unknown).

These are conceptual fields, not a requirement to add unused public Rust types.
Keep structural details private to the shared analysis module. Callers need
identities, bound inputs and the complete/opaque result; they do not walk ASTs.
Reuse the current `result_copy` lookup/plan interface: a lookup describes the
scoped evidence needed, the caller reads it, and a pure plan returns a copy or
refusal. Submit and RegisterClone keep their different protocol requests and
response shapes. A registration without SQL consumes its source's persisted
analysis evidence; it cannot reconstruct missing equivalence from a clone grant.
`ExecutionRow` retains hashes, contexts/provenance and recorded inputs, not a
reconstructible SQL plan. Persist the versioned identity and binding/effect proof
needed for registration when the ordinary source is written. Do not hydrate old
rows from audit SQL, a catalog query or a newly submitted definition.
One shared copy lifecycle owns qualification, frozen provenance and adoption;
warehouse adapters supply typed policy/capability choices, not independent
booleans on every node. Publication checks are performed atomically by the
store against the plan's frozen source, rather than a second policy engine.

#### First complete grammar and binding rules

The new Databricks copy proof accepts one SELECT query with explicit projections,
WHERE, ordered INNER/LEFT JOIN ... ON, nonrecursive CTEs, GROUP BY expressions
and HAVING. CTEs may refer only to earlier definitions; duplicate/shadowed or
unresolved names refuse proof. Qualified aliases bind in their lexical scope;
CTE output aliases retain position and spelling under dialect identifier rules.
Base relations resolve from structured identifiers and supplied defaults, then
must agree with the genuine reported physical inputs, keeping the separate
Target relation observation out of that set unless the query actually reads it.
No suffix-name guesses, splitting
quoted dots, recursive dependency cycles or silent missing/extra inputs. Conflicting
duplicate observations/definitions refuse proof; repeated equal observations may
be deduplicated without losing separate relation occurrences. Supplied
query dependencies are analyzed recursively in their declared contexts and bound
to their reference names; exchanging two definitions changes identity. Their
physical leaf inputs must be covered by the observations. A dependency body is
client evidence, not a warehouse catalog or a verified view definition. If only
a view epoch is supplied and its physical leaves lack observations, copy analysis
is opaque; never invent leaf freshness from the definition or view epoch. Use
bounded dependency traversal (at most 32 definition levels and 1024 relation
visits for new analysis), refusing cycles/overflow rather than partial proof.

A qualified `alias.column` binds the column reference to that relation occurrence;
it does not assert the column exists or infer its type. An unqualified base
column can bind only with one visible relation; with multiple inputs it needs
known projected column names from CTE/dependency outputs proving a unique binding,
including proof that every other visible relation lacks that name. A base relation
with unknown columns cannot supply that negative proof. Otherwise it is opaque. No correlated subqueries, struct/field interpretation,
implicit column search through a warehouse catalog, or alias-based GROUP BY/HAVING
resolution unless the dialect rule proves it unambiguously. Use qualified column
references and explicit grouping expressions in the first multi-input forms.

Expressions initially include column references, NULL/boolean/numeric/string
literals, parentheses, explicit CAST, unary signs, `+ - * /`, comparisons
`= <> < <= > >=`, `AND OR NOT`, `IS [NOT] NULL`, and searched CASE. Approved casts
are explicit scalar numeric casts (INT, BIGINT, DECIMAL with precision/scale);
no temporal, locale or string-to-number equivalence is inferred. Checked builtins
are COUNT(*), COUNT(expr), SUM(expr), MIN(expr), MAX(expr), without DISTINCT,
FILTER, ORDER BY or windows. The adapter must positively identify builtin
resolution and the effects of each admitted form. This list is a syntax ceiling,
not evidence that arbitrary operand types or session settings are harmless.
Unknown types stay unknown: retain exact typed expression structure, never infer
numeric columns, commute arithmetic, fold constants, remove casts, reassociate
joins or change NULL/error/overflow behavior. An operation whose result depends
on an unestablished setting/type rule is opaque; a successful source build alone
does not establish that rule. Identity of the same bound physical column can
preserve its type without discovering it; no type-dependent rewrite follows.

Named physical-column references require presence evidence. Without a column
set, even `q.missing` may resolve to a session-variable struct field. A complete
SQL table-alias column list (`FROM c.raw.t AS q(id,v)`) establishes the exposed
names because Databricks requires its arity to match the relation; it does not
establish their types. Known CTE/dependency outputs also establish presence.
Otherwise keep named references opaque; pass-through `*` and qualified builtin
`COUNT(*)` do not assert a named column. An unqualified name across multiple
inputs is opaque if any input has an unknown column set. See the primary
[table alias rule](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names#table-alias)
and [name-resolution order](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-name-resolution).

Output column order, names, quoting and duplicate names are semantic. Require
explicit aliases for computed outputs unless the dialect proves the exact
warehouse-generated name; a simple column keeps its resolved output name.
Preserve bags (including duplicates), three-valued predicates, LEFT JOIN null
extension, grouping keys and aggregate empty/all-NULL behavior. Do not reorder
projections, joins, predicates or grouping. The existing single-input SELECT-star
shape remains representable as exact pass-through with unknown column expansion;
new joins never infer a star expansion or column order. Window/set/recursive/
dynamic SQL, table functions, correlated queries, unknown/UDF functions and
unreviewed AST fields initially produce opaque copy analysis. Parse success is
insufficient. Existing stricter dialect subsets remain unchanged until separately
characterized; this grammar does not enable new Snowflake/DuckDB transformations.

For example, this is an admissible multi-input/CTE/aggregate shape without
assuming the types of `id` in either physical input. Explicit builtin names
establish function binding without unavailable session search-path metadata:

```sql
WITH orders AS (
  SELECT system.builtin.count(*) AS n FROM c.raw.orders WHERE id IS NOT NULL
), payments AS (
  SELECT system.builtin.count(*) AS n FROM c.raw.payments WHERE id IS NOT NULL
)
SELECT orders.n AS order_count, payments.n AS payment_count
FROM orders LEFT JOIN payments ON orders.n = payments.n
```

The two inputs bind exactly; IS NOT NULL needs no inferred column type; COUNT(*)
has the adapter's checked integer output; the equality compares those known
integer outputs. Numeric literals, lossless in-range integer-literal casts and
COUNT comparisons also admit a checked GROUP BY/HAVING example without session
settings. Ordinary explicit projections/pass-through and null predicates retain
unknown column types symbolically. They need no blanket timezone/ANSI proof.
In contrast, SUM over an unknown column, string/numeric implicit coercion in a
join, string/temporal parsing casts and arithmetic with unknown overflow semantics
stay opaque unless the adapter establishes the needed operand/context rule.
Even explicit numeric CAST syntax is insufficient when conversion may depend on
unknown source types or error modes. These refusals do not prevent the example
or exact same-target reuse. #184 must test both admission and these refusals;
new effect rules require positive dialect evidence, not presumed session defaults.

Databricks SQL and Runtime 19+ resolve unqualified routines through a session
search path that can place temporary functions before builtins. A bare name such
as `count` therefore does not establish builtin identity. New copy proofs accept
explicit `system.builtin` qualification for the checked aggregates; otherwise
function resolution is opaque without supplied evidence. Older runtime builtin-first
precedence is not a universal adapter rule. Parameterless context functions such
as `current_user`, `current_date` and `current_timestamp` also cannot be assumed
to be ordinary columns when physical column metadata is unknown. See the primary
[function/name resolution rules](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-name-resolution).

#### Context, node kind and observations

Physical input identities always retain catalog/schema/relation components.
For new Databricks identities only the *output* schema can be omitted, and only
when every read, dependency, column and function is bound independently of that
schema. Retain output catalog/name and full UID. Unqualified reads resolved using
a default keep that resolved physical name; do not erase a default used by an
unresolved reference or function. Session timezone, collation, ANSI/error modes,
current-user/clock/random effects and function search paths are semantic when an
expression depends on them. Unavailable relevant context means unknown, not a
fabricated default. Known pure builtin effects may prove context irrelevant;
unknown functions cannot. Keep the existing nondeterminism policy and exact
same-target fallback; do not evaluate/rewrite volatile SQL. Snowflake's captured
relative-schema map and DuckDB's checked dependency substitutions remain their
own policies, never arbitrary schema stripping across dialects.

SQL is only one part of a workload. Node kind, execution strategy, exact semantic
extras and declared body/config/macro/contract signatures remain identity evidence.
Opaque config signatures can distinguish configurations but cannot explain their
semantics. FULL automatic copy remains models only, with the existing request,
relation-property, freshness and chain restrictions, including the current
rendered-only mode; no new template/seed/view/strategy permission. These full-UID
and client-signature restrictions belong to Databricks copy, not ordinary model
reuse or a new universal Snowflake/DuckDB requirement. Direct adoption broadens analysis for already-supported MERGE and
source-clock check snapshots only: retain the measured unique-key/schema policy
and `check_cols`/`updated_at`/hard-delete limits in
[pre-copy acceptance](databricks-precopy.md). Do not infer a whole snapshot or
incremental result from its current SELECT. Unsupported/missing node-kind or
configuration evidence refuses new adoption. The existing bounded native
configuration is the acceptance scope, not proof that every equal hash describes
one of those configurations.

State history is a separate input, never a field of the parsed query graph.
The resolved snapshot carries scoped latest rows, current target/source/input
observations, pending-source barriers, recorded input epochs and Upstream producers,
and server time/build-age anchors. Physical keys follow
[ADR 0010](adr/0010-preserve-raw-identifiers-with-physical-history-keys.md);
raw spellings survive for output. Select current physical history before identity
([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md)); never resurrect an
older matching build. Epochs establish only the dialect's observed ordering.
DuckDB ordinals are not wall-clock instants: use server confirmation receipt time
for build age. Unknown epochs cannot establish ordering or freshness, and must
never be filled from submit time, copy time or a guessed timestamp.

For new Databricks FULL copies, all physical leaf inputs must have matching known
current/recorded epochs and producers. Permit both known NoBuild and recorded
Build(logic identity), including multiple inputs; Unrecorded, changed producers,
missing evidence or altered bindings refuse. A current ordinary source must
match org/family/nonblank namespace, full UID, node kind/config and versioned
logic, with an unmodified positive source epoch and no pending/newer barrier.
A source copy never establishes a positive chain. Freeze source ID/key/epoch,
all genuine input epochs/producers, original build-age anchor and inherited
runtime before writing pending evidence. Confirm rechecks the frozen source and
pending barriers in its transaction. Failed cloning publishes ordinary fallback
only; invalid proof cannot gain adoption through strict SQL or namespace hashes.
A later copy cannot renew Lag tolerance. Adoption checks the same proof against
current inputs through `reuse_candidate` and the pure freshness engine.
Snowflake/DuckDB keep their existing copy-age/chain rules during extraction.
No request supplies a new physical source observation at Confirm: unversioned
DDL still requires quiescent sources and does not solve external physical races.

#### Replacement map and compatibility gate

| Existing responsibility / call sites | Shared destination; code that goes away |
|---|---|
| `execution::ExecutionEvidence::{sql_submit,sql_record}` → `analyzed_workload::identity::{sql_logic_hash,copy_logic_hash}` | One analysis result feeds both writes; keep length framing, seed hashing and legacy dialect serialization unchanged. Remove caller-specific repeated identity construction. |
| `sql_identity::{canonicalized,duckdb_copy_graph}`, `sql_norm`, `warehouse/databricks.rs` parsing | Shared traversal/scopes/binding; retain dialect transformations and identifier rules as adapters. Replace duplicated copy-only walks as their shared equivalent lands; unrelated audit/selector proofs are outside this migration. |
| Former `databricks_submit_copy::identity` and direct SELECT-star analyzer | Removed by #184/#185. Shared `analyzed_workload` traversal produces one `CopyProof`; `CopyProof::for_origin` frames automatic-v2 and direct-v2 origins. Strict fallback and legacy decoding retain their existing domains. |
| `result_copy::{submit_lookup,plan_submit,register_lookup,plan_register}`, `databricks_submit_copy::{source_admits,reusable}` | One qualification/provenance implementation behind the existing lookup/plan interface; remove duplicate source/input/origin checks. Preserve refusal diagnostics and distinct DDL-attempt versus adoption permission. |
| `execution::{submit_copied_from,copy_registration}`, `Store::confirm` | One frozen-copy evidence construction and atomic publication predicate, replacing parallel direct/automatic JSON publication branches; keep version/origin-specific compatibility decoding and confirm-once behavior. |
| `reuse_candidate::for_model`, `decision::{decide,upstream_freshness}` | Adoption consumes the shared proof, removing duplicated origin bypass logic. Keep latest-row selection, ordinary identity checks, freshness/lag policy and wire descriptions in their current modules. |
| `warehouse.rs` and dialect adapters | Keep naming, effects/context, observation/build-age semantics, capabilities/DDL and justified legacy policy differences. Remove delegated duplicate analysis/qualification, rather than adding another warehouse flag forest. |

#183 must preserve **all fingerprint bytes and decisions**, including current
Databricks v1 direct/automatic proofs. Do not bump a tag merely for extraction.
Preserve confirmed, NULL/unknown-version and pending rows confirmed after upgrade,
raw physical aliases, cached test payloads, rejection codes, both clone mirrors,
Explain, source/age anchoring and failed/duplicate-confirm behavior. Frozen bytes
in `golden/upgrade/` are independent evidence; a production hash helper invoked
with an older tag is not an old implementation.

#184 uses a distinct `databricks-submit-copy-v2` identity domain; #185 similarly
uses `databricks-direct-copy-v2` provenance. Version the persisted proof as well
as its identity framing, including origin, source and age evidence; do not reuse
v1 fields with v2 meaning. Ordinary strict rendered/template identity stays at its
current version unless its own semantics change. Legacy ordinary builds can
still reuse strictly, but cannot supply a v2 copy without persisted v2 evidence.
Old copy-origin rows lacking the required new proof cold-miss adoption, even if a
strict/namespace hash happens to match. Preserve their latest-row barrier. Pending
v1 rows confirmed after upgrade retain v1 bytes/version, never synthesize v2
proof. Test those cold misses and unknown/NULL/incompatible origins explicitly;
a new ordinary build is the recovery. These reviewed version transitions are
the only intended compatibility changes in #184/#185.

The unchanged integration base is `6aa72ef`; its
[CI run 43763522675](https://github.com/maxfirman/dbt-state-rs/actions/runs/43763522675)
passed the normal workspace and DuckDB gates. Reuse that baseline; this contract
change needs documentation/link checks, not another warehouse campaign.
Use this small comparison gate, not a second permanent engine:

| Responsibility | Existing baseline and old/new requirement |
|---|---|
| Complete public history / output fidelity | `conformance.rs::conformance_replay_all_corpora`, exact Databricks submit/pre-copy/native replay tests: same full responses with existing ID/clock handling, gaps unchanged. Compare complete histories from the same fresh store on the pre-extraction and extracted binaries. |
| Binding / legacy identity | `sql_norm`/`fingerprint` tests, `sql_normalization.rs`, `duckdb_identity.rs`, `duckdb_copy.rs`, `databricks_identity.rs`, `databricks_dependency_identity.rs` and frozen `golden/upgrade/`: identical old hash bytes, selected current rows and upgrade barriers at #183. |
| Qualification / publication / adoption | `clone_{eligibility,lifecycle,history_provenance,chain_provenance,submit_fallback}.rs`, `databricks_{lifecycle,submit_copy,precopy}.rs`: same selected source, pending/confirmed provenance, frozen inputs/age and outputs, including rollback/failure/retry/duplicate Confirm. Compare persisted evidence as well as public results. |
| Independent physical/model safety | Existing DuckDB acceptance and Databricks pre-copy/submit-copy native drivers retain complete typed bags/schema/properties/history and actual clock evidence; `decision_proptest.rs` retains pure freshness invariants. These do not independently prove the new grammar. #184/#185 add worked operator/binding negatives and #186 uses the existing native oracles for a small representative broader graph. |

Retain `conformance::databricks_copy_recovery_complete_hosted_boundary` with all
151 requests and its **nine full differing outputs** at entries
26, 30, 115, 119, 127, 128, 129, 133, 141. The exact payload snapshot in
[`databricks_copy_recovery_local_differences.json`](../crates/harness/tests/fixtures/databricks_copy_recovery_local_differences.json)
and [boundary explanation](databricks-copy-recovery.md#whole-hosted-boundary)
stay unchanged. In particular, do not imitate the hosted stale-source grant.
The separate submit-controls volatile/display differences and accepted
UID/namespace/unknown-freshness floors also remain explicit, not newly masked.
Temporary old/new comparisons are test tooling removed after extraction parity;
new semantic forms require independent expectations and version-transition tests.

### Registered services

`build_router()` registers, against one shared `AppState`:

| Service | Methods |
|---|---|
| `grpc.health.v1.Health` | reports `SERVING`; needs no token in `oidc` mode |
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` (`skip_execution` only for an unchanged data test without upstreams, otherwise `undecided`) |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` |
| `Clone` | `RegisterClone` |
| `ClientValidation` | `ValidateClientVersion` |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` (empty stub) |
| `SelectorService` | `GetStateSelection` |

The binary and the in-process integration tests share `build_router()`, so tests
exercise the production wiring. Wire semantics are in [protocol.md](protocol.md).

### Submit flow

`submit_decision::submit_sql`:

1. Record the node's selector state in `node_states` (best effort; a failure
   is counted, never fails the submit).
2. Build the execution evidence and resolve each input's Upstream producer
   from State history (`resolve_producers`). A Snowflake data test then reads
   each input that is an unchanged copy this server recorded as the build it
   was copied from, and recomputes its history key over those readings
   (`data_test_history::read_through_copies`, #200).
3. Read State history once: the Target relation's current physical state, or a
   data test's latest run under its history key (Snowflake; under
   `compare_unrendered_code` a key without a run falls back to the unique
   id's latest run) or by scoped UID (other Warehouses). `reuse_candidate` decides on that
   row alone whether it is a Reuse candidate. With Volatile evaluation
   requested, Reuse qualification refuses only SQL whose root or query
   dependency definitions call a Volatile function per the Warehouse's
   detector (`Warehouse::has_volatile_sql`); deterministic SQL qualifies
   (#191 cell 3, maintainer decision, #194). Databricks also qualifies the
   checked source-less FULL literal projection shape with an ordinary matching
   build, including `rand(42)` (#213). A Snowflake model under
   `compare_unrendered_code` whose template identity equals the row's is not
   refused for volatile SQL (maintainer decision 1 on #201, #206; ADR 0005
   "Amended by #201"). A model's rebuild refused at Reuse qualification keeps
   the refused build as its Explain anchor (`Selection::unqualified`,
   decision 3 on #201); it never becomes a Reuse candidate.
4. With no Reuse candidate (and no External modification of the Target
   relation), try the Result copy fallback. A copy overrides the verdict.
   Only the execution types the Warehouse rules copy are copied (FULL; DuckDB
   also delete+insert under its contract). Another kind with an equivalent
   keeps the ordinary verdict and reports the equivalent in its rejection
   codes (`with_refused_copy`).
5. Otherwise call `decide()`, with the build age measured on `AppState::clock`.
6. If the Reuse candidate must rebuild for upstream freshness, try a Result
   copy of a same-upstream equivalent over the existing Target relation.
7. For a rebuild, report an older build that would still qualify
   (`older_match`).
8. For a SQL rebuild, add the refusal diagnostics hosted reports
   (`diagnostics::decorate`). They only decorate codes and descriptions; none
   selects a reuse candidate, copy source or Explain anchor.
9. `settle`: mint the ids, write the pending row for every outcome except SKIP,
   and return the `SubmitOutcome`. A confirmed Databricks FULL build
   can publish the bounded producer revision from an externally modified input
   ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#databricks-volatile-producer-after-external-modification-213)).
10. Volatile evaluation, once decided (`volatile_evaluation::evaluate`, ADR
    0005 "Amended by #67"): for a build, a reuse or a Result copy, render
    the Volatile functions the Warehouse has captured rules for
    (`Warehouse::volatile_rules`) in the root SQL or the query dependency
    definitions, only in a cell the Warehouse was captured rendering in
    (`Rules::cells`: execution type, evaluation on or off, build reason,
    reuse or a copy into an absent Target relation, own SQL or
    dependencies). Any other submit, a speculative submit and the
    chain-limit build render nothing. The values are returned in
    `transformed_nodes_by_query` and persisted with the decision for
    Explain; the decision above never sees them, and the Warehouse executes
    the original SQL ([protocol.md](protocol.md#volatile-evaluation)).

`submit_values` (seeds) follows steps 1, 3, 5, 7 and 8: no producers, no Result
copy, no Lag tolerance, ANY policy.

`submit_decision::speculative_sql` (#103) runs the same code for a data test
with no Target relation and no reported tables, and keeps only a SKIP: it
stops, before anything is written, after step 3 when there is no Reuse
candidate and after step 5 when the verdict is not SKIP, and answers
`undecided`. It records the node state (step 1) only for the SKIP. Every
other speculative submit is `undecided` without a read.

### Physical scope rule

The physical execution scope is organization + warehouse family + client
`table_namespace` + exact Target relation name. dbt derives the namespace from
an adapter-specific warehouse identifier. Selecting the latest row *within that
scope* stops a same-named relation in another account, project or host from
shadowing this one; execution type, dialect options and logic are checked only
after the current row is selected, so an older row is never resurrected after a
later build. One exception for which row a rebuild is judged against: when the
current row has the submit's identity under another execution type and
recorded the submit's genuine upstream names, the latest
row of the submit's type is (`submit_decision::select_sql`, ADR 0007); it is
never reused. Other upstream names are another identity (#116). A client without a namespace gets an isolated absent-namespace
scope per family. Empty strings share that physical scope, including legacy
confirmed and pending rows. Source-less Databricks rejection/Explain reads can
return query diagnostics from outside a physical reuse scope; those reads
return only booleans or a name and never feed a reuse/copy candidate, producer
or freshness anchor. A hosted absent-to-named fallback conflicts with this
required self-hosted isolation and remains [an explicit incomplete
contract](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-isolation-2026-11-13.md). A data test has no Target relation: on Snowflake it selects the latest
confirmed run by org, warehouse family, namespace and history key (ADR 0007
"Data-test history key", #200); an older run under another key stays
reusable. Other Warehouses' data tests select by org, project, warehouse
scope and UID.
Every store query is org-scoped; neither this nor the UI's
`DBT_STATE_UI_ORG_ID` filter is authentication.

### Store tables

Schema is defined by `crates/server/migrations/` (applied on startup).

| Table | Contents |
|---|---|
| `executions` | State history: pending and confirmed rows with identity/context hashes, the Principal whose Submit, RecordExecutions or RegisterClone wrote the row (`principal_id`, migration 0032, #226; NULL in `none` mode and for older rows), generated `warehouse_family`, `table_namespace`, recorded inputs with producers, outcome, cached result bytes, copy provenance (`clone_source_table`, `node_namespace_logic_hash`; a RegisterClone copy also keeps its source build's runtime and recorded upstream names, `clone_source_inputs`; a Snowflake copy recorded since #200 keeps the build it was made from, `clone_source_execution_id`), a Snowflake data test's history key (`data_test_key`, migration 0029), and a Databricks build's producer revision (`producer_revision`, migration 0030, #213). |
| `node_states` | Latest client-supplied node hashes per (org, project, Target, node) for `state:*` selectors, plus retained `target_table` snapshot metadata. |
| `node_decisions` | One row per decision: the Principal whose request made it (`principal_id`, migration 0032, #226; NULL in `none` mode and for older rows), class, description, rejection codes, `is_stale`, `decision_reason`, `matched_execution_id`, Lag tolerance and build age, decision-time inputs; for a view, whether its SQL reads a relation (`reads_relation`, #117); the values Volatile evaluation rendered (`rendered_volatile_values`, migration 0027, #67) and whether the submit requested Volatile evaluation (`volatile_evaluation_requested`, migration 0028, #194). Read by Explain and the UI. |
| `organizations`, `projects`, `environments`, `invocations` | UI capture domain. An `environments` row is a dbt Target. |
| `principals` | Each Principal the Token broker issued a token to (migration 0032, #226): a machine before its token, a person with its session. Keyed by Upstream IdP `issuer` and `subject`; the opaque `id` tokens carry as `sub` (the `principal_id` of decisions and executions, with no foreign key, so a decision is recorded even when its Principal has no record), `kind` (`person` or `machine`), a display `email` when known. Not scoped by Organization. Replaces the reserved, never-written `actors`, `roles` and `role_bindings`. |
| `broker_pending_logins`, `broker_person_sessions`, `broker_authorization_codes`, `broker_refresh_tokens` | Token broker state (migration 0031, #220): browser sign-ins in progress, person sessions (Principal as upstream issuer, subject and `principal_id`; client id; last granted scope; 12 h cap; `revoked_at`; the upstream refresh token sealed; `upstream_renewed_at`), single-use codes and refresh-token family members. Bearer values only as SHA-256 hashes. Not State history and not org-scoped. |

### Lifecycle

1. A submit that returns SKIP writes no `executions` row. EXECUTE, CLONE
   (`ready_to_clone`) and the chain-limit EXECUTE write a `pending` row keyed
   by `request_id`; `RegisterClone` does too.
2. `ConfirmExecution(request_id, …)` flips the row to `confirmed` with the
   outcome and the server receipt time, exactly once. A repeat confirm returns
   INTERNAL (`Database operation failed`) without rewriting the outcome. A
   missing valid UUID returns NOT_FOUND; malformed IDs return INTERNAL.
3. `RecordExecutions` writes a batch of confirmed rows and their `node_states`
   in one transaction, after validating the complete batch. A physical target
   requires an outcome epoch; an absent target requires an absent epoch.
4. Later submits consider only confirmed rows. A SKIP echoes the recorded
   `execution_runtime_ms`. Physical current state uses the greatest completion
   epoch, with confirmation receipt order breaking equal epochs. A legacy
   confirmed row with an unknown epoch blocks physical reuse and copying even
   when external modifications are ignored; later delivery cannot prove recovery.
   Logical test/deferral histories and historical copy provenance retain their
   separate ordering rules.

## auth crate

`crates/auth` (`dbt-state-auth`) is shared by the gRPC server, the Token
broker and, later, the UI. It is pure apart from reading the config and key
files; fetching keys and talking to the Upstream IdP live in the broker
crate.

| Module | Responsibility |
|---|---|
| `organization.rs` | `OrganizationId` (non-empty visible ASCII, no `:`, not `*`), `Role` (`developer` < `admin`), declared `Organizations` (active or disabled). |
| `scope.rs` | The `scope` claim: `Scope::issue` (highest Role per declared Organization; active ones as `org` + `app` entries, disabled ones as `app` only) and the strict `Scope::parse`. `tests/scope_proptest.rs` checks both against ports of the pinned clients' scope rules (`tests/client_scope/`). |
| `config.rs` | `AuthConfig::load`: the TOML auth config (`[broker] issuer` and optional `jwks_url`, `max_pending_logins_per_ip` (default 20), `client_ip_header`; `[api] audience`; `[tokens]` lifetimes; optional `[signing]` keys; `[[organizations]]`; optional `[upstream]` (`issuer`, `api_audience`, `client_credentials_scope`, `subject_claim` (default `sub`), and for browser sign-in `client_id`, exactly one of `private_key_file` (+ optional `private_key_id`) or `client_secret_file`, `refresh_token_key_file`, `login_scope`); `[[rules]]`; optional `[clients.dbt] client_id` (default `DEFAULT_DBT_CLIENT_ID`, the dbt client's built-in id)), unknown keys refused. Builds the `TokenIssuer` (with `[signing]`), `TokenVerifier` and `RequestAuthenticator` over a `VerificationKeys`. |
| `keys.rs` | RS256 keys from PEM files (PKCS#8 or PKCS#1), several key ids, one active; never echoes key material. `VerificationKeys`: a shared, replaceable public key set, published and read as a JWKS. |
| `token.rs` | `TokenIssuer::issue` and `TokenVerifier::verify` (signature by any loaded key id, issuer, audience, expiry with 60 s skew, claim shape); `PrincipalId` (`PrincipalId::upstream(issuer, subject)`: an opaque hash of the Upstream IdP pair), `PrincipalKind` (person tokens carry `sid`). |
| `mapping.rs` | Mapping rules (`kind`, `claim`, `value`, Organization, Role): `MappingRules::scope` evaluates only the Principal kind's rules; a claim matches as a string or an array of strings; highest Role wins; no match is an empty scope. `tests/mapping_proptest.rs`. |
| `upstream.rs` | `UpstreamTokenVerifier`: an Upstream IdP access token must be an RS256 JWT signed by a published key, with the IdP's issuer, `aud` holding the upstream API audience, and current (`exp`, `nbf`, 60 s skew). Opaque tokens are refused. `verify_id_token` checks an ID token for the broker's client id: also `azp` (required with several audiences) and the `nonce` it sent. |
| `credential.rs` | `UpstreamCredential`: the broker's own credential at the Upstream IdP, from a mounted file. `ClientAssertionKey::assertion` signs a `private_key_jwt` assertion (RS256, `iss` = `sub` = client id, `aud` the token endpoint, random `jti`, 5 min); `ClientSecret::basic_authorization` (RFC 6749 §2.3.1). `Debug` redacts both. |
| `sealing.rs` | `RefreshTokenKey`: AES-256-GCM (`ring::aead`) under the `refresh_token_key_file` key (base64 of 32 bytes), a random nonce per value, associated data binding a value to its row. Layout: version byte, nonce, ciphertext and tag. |
| `context.rs` | `RequestAuthenticator::authenticate`: token, then Organization; an `AuthenticatedContext` (Principal, Principal kind, Organization, Role, client id, session) or a `Rejection` with its gRPC class, fixed message and log reason. |

## broker crate

`crates/broker` (`dbt-state-broker`, also a standalone binary) is the Token
broker (ADR 0012). It runs in the server process on its own HTTP/1.1
listener (`DBT_STATE_BROKER_LISTEN`), sharing the config's key set and the
server's Postgres pool, or alone (`dbt-state-broker`, env
`DBT_STATE_BROKER_LISTEN` default `127.0.0.1:8080`, `DBT_STATE_AUTH_CONFIG`
and `DATABASE_URL`). Its tables live in the server's schema: their migrations
are in `crates/server/migrations` (from `0031_broker_person_login.sql`), and
a standalone broker runs the same migrator at startup (`MIGRATOR`), so any
number of brokers and servers share one schema. It makes no request to the
Upstream IdP at startup.

| Module | Responsibility |
|---|---|
| `lib.rs` | `Broker::check(&AuthConfig)` (needs `[signing]` and `[upstream]`), `Broker::new(&AuthConfig, PgPool)` and `Broker::with_clock(.., Clock)`, `Broker::router()`: `GET /` (authorize), `GET /callback`, `GET /.well-known/openid-configuration`, `GET /jwks`, `POST /token`; `serve` supplies the peer address the pending-login cap needs. `http_client()`: timeouts, no redirects. |
| `sign_in.rs` | Browser sign-in. `GET /`: the dbt client registration (client id, exact redirect `http://127.0.0.1:29525/handler`; a mismatch is an error page), PKCE S256 required, a pending login stored (10 min, capped per client IP), then a redirect to the Upstream IdP with the broker's own `state`, `nonce` and PKCE. `GET /callback`: the pending login taken once by `state`, the code exchanged with the broker's credential, the ID token checked, the Principal named by the issuer and `subject_claim`, `kind = "person"` rules applied, a person session created with its Principal recorded (12 h cap, upstream refresh token sealed), and a 60 s code sent to the loopback handler. Refusals go back to the client as OAuth errors with fixed descriptions and are logged (`sign-in refused`). |
| `code_grant.rs` | `POST /token` `authorization_code`: the code used up on first presentation (a replay revokes its session), bound to the client id, redirect URI and PKCE challenge, valid 60 s; returns a person token (`sid`) and a refresh token whose hash is the session's first refresh-token family member. |
| `store.rs` | Postgres: pending logins, person sessions, authorization codes, refresh tokens, and `principals` (upserted by issuer and subject; a known email is kept when a later sign-in has none). Bearer values only as SHA-256 hashes. |
| `sessions.rs` | Person sessions outside the broker's endpoints. `SessionCheck::check(sid)`: the gRPC server's check that a person token's session is active (not revoked, before its 12-hour cap), each status cached per replica for at most `SESSION_STATUS_MAX_AGE` (30 s) from when its read began, on the given `Clock`; a failed read is `Unavailable`, never a pass. `revoke_sessions(pool, issuer, subject, at)`: sets `revoked_at` on every session of the Principal (the admin command), logged `person sessions revoked`. |
| `clock.rs`, `secret.rs` | The broker clock (tests move it); random bearer values, their hashes, PKCE S256. |
| `token_endpoint.rs` | `POST /token`. `client_credentials`: HTTP Basic forwarded verbatim to the Upstream IdP with `client_credentials_scope`; the access token is checked, the Principal named by the issuer and `subject_claim`, `kind = "machine"` rules applied, the Principal recorded (a failed record is `temporarily_unavailable`), and `id_token`, `scope`, `token_type`, `expires_in` returned. `authorization_code` goes to `code_grant.rs`. Token exchange and other grants (including `refresh_token`, until #221) are `unsupported_grant_type`. Every issue (`info`) and refusal (`warn`) is logged with grant, Principal, subject, client id and reason. |
| `upstream.rs` | The Upstream IdP: discovered on first use (retried until it succeeds), the `client_credentials` forward, the broker's own authorize URL and code exchange (`private_key_jwt` or client secret), and access- and ID-token checks with a key refresh on an unknown key. |
| `remote_keys.rs` | `RemoteKeys`: a JWKS fetched (from a URL or through discovery) and cached; refetched after `MAX_AGE` (5 min) or on a signature miss, at most once per `MIN_INTERVAL` (30 s); a failed fetch keeps the loaded keys. Used for the Upstream IdP's keys and, by a gRPC server without `[signing]`, the broker's. |
| `discovery.rs` | OIDC discovery fetch; the document's `issuer` must equal the one asked for, and its `token_endpoint`, `authorization_endpoint` and `jwks_uri` must be https (plain http only on loopback, `dbt_state_auth::secure_url`). |
| `oauth.rs` | OAuth error responses (RFC 6749 §5.2) with fixed descriptions; `Cache-Control: no-store`. |

## ui crate

`crates/ui` (bin `dbt-state-ui`) reads the capture tables read-only. See
[ui.md](ui.md).

## harness crate

The recording proxy (`record-proxy`), the single-RPC `state-probe` used by
live characterization drivers, the opt-in `diff-fuzz` tool (`--features fuzz`),
golden/auth/diff support modules and the integration tests. See
[harness.md](harness.md) and [testing.md](testing.md).

### Submit clock for Explain

Databricks SQL submits retain nullable `display_submitted_at_ms` on their
execution and node-decision rows (migration 0022). Explain uses this frozen
instant for temporal text, including identity-change rebuilds without a reuse
anchor. A captured FULL upstream with a missing current epoch displays the
current submit instant; a missing recorded epoch uses the original build
submit instant only when it was retained. Both raw epochs remain unknown.
These columns never participate in freshness, producer selection, physical
history ordering or fingerprints, and legacy rows are never backfilled.
