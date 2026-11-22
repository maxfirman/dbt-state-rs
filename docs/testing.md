# Testing

The offline suite needs only Postgres and makes no hosted or Snowflake calls;
it is the day-to-day gate. Hosted behavior is characterized live through the
recording proxy and the `experiments/` drivers ([harness.md](harness.md)):
capture first, promote a golden fixture, then assert. Do not invent hosted
behavior. Record open questions in [correctness-handoff.md](correctness-handoff.md).

## Validation

CI (`.github/workflows/ci.yml`) runs the first, third, fourth and fifth
commands below. Run all of them before a change that touches decision logic.

```bash
cargo test --workspace                                   # needs Postgres at $DATABASE_URL
DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid \
  cargo test -p dbt-state-server --lib                   # proves library tests need no database
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
./scripts/duckdb-ci.sh                                   # CI passes --jobs 4
python3 experiments/trace_validate_test.py               # warehouse-free experiment-oracle tests
python3 experiments/explain_census_test.py               # warehouse-free Explain census tests
python3 experiments/explain_capture_test.py              # warehouse-free capture driver tests
python3 experiments/data_test_history_test.py            # #200 echo attribution and crafted probe plans
python3 experiments/databricks_survey_test.py            # offline Databricks campaign guards/results
python3 experiments/databricks_isolation_test.py         # offline concurrent campaign ownership/physical-peer gates
python3 experiments/databricks_deferral_test.py          # offline deferral history/ownership gates
python3 experiments/databricks_baseline_test.py          # offline independent physical-result fault probes
python3 experiments/databricks_readonly_reference_test.py # no-object reference output/SQL fault probes
python3 experiments/databricks_shared_copy_test.py       # explicit typed bags/SCD history and bounded copy coordinator faults
python3 experiments/databricks_target_lifecycle_test.py   # same-lifetime continuation admission guards
python3 experiments/databricks_policy_test.py             # immutable BUILD vs fresh policy reference fault probes
python3 experiments/databricks_seeds_test.py             # seed rows/NULLs/duplicates/types/properties fault probes
python3 experiments/databricks_sql_equivalence_test.py   # typed oracle sensitivity and separate hosted-history plans
python3 experiments/databricks_dependency_identity_test.py # exact role ownership and complete-result fault probes
python3 experiments/databricks_cached_tests_test.py      # no-object cached-failure payload/SQL/exit fault probes
python3 experiments/databricks_cached_config_test.py     # severity/raw-flags and nonzero threshold/NULL/duplicate outcomes
python3 experiments/databricks_volatile_test.py          # #198 capture-boundary ownership and physical-result fault probes
```

`rust-toolchain.toml` pins the Rust version. `cargo test --workspace` does not
build the `fuzz` feature, so it never contacts the hosted service.

## Explain tree census

`experiments/explain_census.py` counts the Explain trees in the tracked
fixtures per node kind × decision class: trees by origin (real client and its
version, or crafted `state-probe`), empty trees, and each marker and badge. It
names the fixture and entry of any `INFO`, `EM_UNSPECIFIED`, `EB_UNSPECIFIED`
or unmapped tree. Method and output:
[explain-catalog.md](explain-catalog.md#explain-tree-census).

```bash
python3 experiments/explain_census.py                    # tracked fixtures only
python3 experiments/explain_census.py "$GOLDEN_DIR" more.jsonl   # plus capture files or directories
python3 experiments/explain_census.py --no-fixtures "$GOLDEN_DIR"  # one campaign alone
```

Every #40 capture ticket reruns it after promoting its fixtures. It then
updates, in `docs/explain-catalog.md`, the census table, the vocabulary line
counts, the "not observed across N trees (date)" entries and the evidence
column of the coverage table. A finding (an `INFO` line, an explicit
unspecified value, or a tree no submit minted) needs a catalog entry before the
fixture is promoted.

## Evidence rules and release gate

The [experimental Databricks preview](databricks-preview.md) uses the explicitly
revised [landing plan](databricks-landing.md). Full completion of the original
Databricks feature inventory is deferred; no full-conformance claim is made.
Safety floors and exact retained regression assertions still apply.
The maintainer subsequently accepted documented conservative differences for
feature-ticket completion: extra BUILDs/undecided responses preserve safety
when hosted would reuse across UID/namespace or unknown freshness boundaries.
Missing implementation and untested behavior are not waived. Classify these
limits in the [preview matrix](databricks-preview.md); keep exact captured
regressions and the stricter full-conformance claim distinct.

Two questions need different evidence (GLOSSARY: Hosted conformance, Reuse safety):

- **Reuse safety**: can a SKIP or CLONE serve the wrong physical result? Offline
  tests with an independent oracle can show this. The oracle must not call the
  production fingerprint, freshness or decision code to decide what result
  should exist.
- **Hosted conformance**: does the server return the same gRPC status and full
  response as dbt State for the same starting state and request? Only a
  causally complete hosted capture for a pinned client and warehouse shows this.

Conformance is claimed per **client/version × warehouse × feature slice**, never
for the product as a whole. A slice needs: no wrong-result SKIP or CLONE in the
fixed, generated and upgrade safety tests; zero unclassified mismatches in the
captured-traffic replay; and a tested, documented fallback for every
intentional gap. Offline evidence never promotes an item to hosted-verified.
Fix wrong-result reuse and cross-tenant or cross-warehouse contamination first,
then false rebuilds and missing features, then response fidelity.

## Test support

Integration tests share `crates/harness/tests/support.rs` and its modules in
`crates/harness/tests/support/`.

- `resources.rs` owns `TestServer` and `TestDatabase`. Each creates an isolated
  Postgres schema, runs all migrations and removes the schema on drop, including
  after a panic or failed server startup. `TestServer` also stops and joins its
  server runtime. Tests that call server modules directly (such as
  `submit_decision.rs`) take state from `TestDatabase::state`; the database
  owner must outlive that state.
- `scenario.rs` builds requests that fill in the reuse identity (`model`,
  `view`, `data_test`, `duckdb_model`, `seed`, `clone_request`), outcomes
  (`outcome`, `built_at`, `test_results`), and a `Scenario` client with
  `expect_execute`/`expect_skip`/`expect_clone`, `verdict`, `speculative`,
  `build` and
  `record`. `Submit::body` changes the rendered SQL as well as the body hash,
  because reuse ignores `node_body_hash`. Seed State history through the
  protocol with `Scenario::build` or `Scenario::record`.
  - `captured_submit(&entry.request)` builds a `Submit` from a captured
    fixture request, exactly as the client sent it.
  - `Scenario::at(now_ms)` sends the test replay clock header
    (`x-dbt-state-replay-now-ms`) on every request of that scenario, so it
    decides at a captured time.
  - `Scenario::explain(&[ids])` calls GetExplainMessages scoped to the
    scenario's org.
  - `volatile_values` (a response, on any branch), `volatile_values_of` (a
    `transformed_nodes_by_query` map) and `hosted_volatile_values` (a
    captured response) read Volatile evaluation values as query → function
    → value; `volatile_map_shape` drops the values.
- `oidc.rs` supports `oidc` mode. `TestAuth::new(&[(org, disabled)])`
  writes and loads a real auth config over the test-only keys in
  `crates/auth/testdata/`, and `start_server_oidc(&auth)` serves it. A
  `TestSigner` (`auth.signer()`, or `auth.signer_with(SignerOptions { issuer,
  audience, key, organizations })` for wrong issuers, audiences, keys or a
  stale view of the Organizations) issues person (`.person(sub, sid)`) and
  machine (`.machine(sub)`) tokens with `.grant(org, role)` and
  `.issued_ago(d)`. The gRPC server refuses a person token whose `sid`
  names no active Token broker session, so a test that needs one accepted
  records the session first: `person_session(&server, principal).await`
  returns its `sid`. `Scenario::bearer(token)` / `Scenario::authorization(raw)`
  send them. `capture_logs()` / `captured_logs()` capture the process's log
  lines. `TestAuth::builder(&orgs)` extends the config: `.upstream(&idp)`
  (which also registers the broker for browser sign-in with
  `BROKER_CLIENT_SECRET` from a file and a refresh-token key file),
  `.broker_credential(BrokerCredential::PrivateKeyJwt)`,
  `.subject_claim(c)`, `.dbt_client_id(id)`,
  `.max_pending_logins_per_ip(n)`, `.client_ip_header(h)`,
  `.without_browser_sign_in()` (no `[upstream] client_id`),
  `.rule(kind, claim, value, org, role)`, `.person_lifetime_minutes(m)`,
  `.machine_lifetime_minutes(m)`, `.issuer(url)` and `.without_signing()`.
- `start_server_with_broker(&auth)` runs the gRPC server and the Token
  broker in one process on separate listeners (`server.broker_url()`), as
  `DBT_STATE_BROKER_LISTEN` does; `start_broker(builder).await` runs a
  standalone broker on its own migrated schema whose issuer is its own URL
  (`TestBroker::url()`, `.auth`, `.clock`), and
  `start_broker_beside(&broker, builder)` another one on the same schema and
  clock (a replica, or the deployment restarted with another config;
  `tests/shared_logins.rs` renews through two replicas). `server.broker_clock` (a
  `TestClock`) moves the broker's clock forward (`.advance(d)`) to expire
  codes, pending logins and sessions; the gRPC server's person session
  check reads the same clock, so advancing it ages cached session statuses
  (30 s) and brings sessions to their 12-hour cap
  (`tests/session_revocation.rs`). `start_server_tls(builder).await` is
  `start_server_with_broker` with native TLS on both listeners, as
  `DBT_STATE_TLS_*` and `DBT_STATE_BROKER_TLS_*` run them: the broker's
  issuer is its own `https://127.0.0.1:<port>` URL and `server.tls` is
  set. `tls.rs` loads the test certificate (`crates/auth/testdata/tls-*`,
  for `localhost`, `127.0.0.1` and `::1`) and names its CA (`ca_file()`).
- `python_client.rs` runs the pinned Python client driver
  (`crates/harness/python-client/driver.py`, environment locked in its
  `uv.lock`, `uv run --locked`) against a TLS test server:
  `PythonClient::available(&server)` points the client's own settings at
  it (`RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE`, `RUN_CACHE_AUTH_URL`,
  `RUN_CACHE_TOKEN_URL`, `RUN_CACHE_CA_BUNDLE` and `REQUESTS_CA_BUNDLE` for
  the test CA, a temp `RUN_CACHE_HOME`) in an otherwise cleared
  environment; `.machine(id, secret)` adds the client id and secret
  (`DBT_ENGINE_STATE_OAUTH_CLIENT_ID`,
  `DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET`) and `CI=true`;
  `.run(&[scenario, steps..]).await` returns the exit status, output and
  the driver's JSON result. `.person().await` is a person at a terminal
  (the driver gives the client a pseudo-terminal) whose client signs in
  through the browser flow and caches its token in its `RUN_CACHE_HOME`
  (`.home()`): the driver names the harness browser
  (`python-client/browser.py`) in `BROWSER`, so the client's own
  `webbrowser.open` runs it, and it follows the redirects through the
  Token broker and the fake Upstream IdP to the client's loopback handler;
  `.browser_visits()` lists each visit (URL opened, redirects, landing,
  status). The person scenario is `["person", steps..]` with steps `rpc`
  (one RPC, one credential lookup) and `wait`: `.start(args)` returns a
  `Driver`, `.paused().await` waits for its next `wait`, `.resume(d).await`
  moves the driver's wall clock `d` on (move `server.broker_clock` as far
  first) and `.finish().await` runs it to its end. The client's loopback
  handler binds the fixed port 29525, so `.person()` holds an exclusive
  lock on `$TMPDIR/dbt-state-python-client-loopback-29525.lock` until the
  client is dropped: person tests run one at a time, across test binaries
  and worktrees. Without `uv` it skips with a message unless
  `DBT_STATE_REQUIRE_PYTHON_CLIENT` is set, which CI sets. The lock pins
  public PyPI, and `UV_*` variables pass through (for example
  `UV_NO_CONFIG=1` under a uv config naming another index). Tests:
  `tests/python_client.rs`.
- `fake_idp.rs` is the fake OIDC Upstream IdP (no Docker): `FakeIdp::start()`
  serves discovery, `/jwks` and `/token` (`client_credentials` by HTTP
  Basic) at `idp.issuer()`. `idp.machine(UpstreamMachine::new(id, secret)
  .claim(name, value))` registers an app registration; `.audience(aud)`,
  `.issuer(iss)`, `.opaque()`, `.signed_by_impostor()` and `.expired()`
  spoil its access token. `idp.sign_in(UpstreamPerson::new(sub)
  .claim(name, value))` signs the browser in as a person, whom the
  auto-approving `/authorize` approves at once; `.wrong_nonce()`,
  `.signed_by_impostor()`, `.audience(aud)`, `.issuer(iss)`,
  `.without_id_token()` and `.without_refresh_token()` spoil its token
  response. The `authorization_code` grant authenticates the broker by
  `BROKER_CLIENT_SECRET` or a `private_key_jwt` assertion
  (`upstream-rp-key.pem`) and checks redirect URI and PKCE. The
  `refresh_token` grant renews a person with their current claims, rotating
  the refresh token, and `/userinfo` answers for a person's access token:
  `idp.update(person)` changes a person's claims, `idp.disable(sub)` /
  `idp.enable(sub)` a person, `idp.fail_refresh(status, error)` /
  `idp.restore_refresh()` every refresh, `idp.delay_refresh(d)` slows every
  refresh answer by `d` (a renewal then holds its session's row lock that
  long); `UpstreamPerson`
  `.without_refresh_id_token()`, `.without_refresh_access_token()`,
  `.keeps_refresh_token()`, `.renewed_as(sub)` and `.userinfo_without(claim)`
  shape renewal, and `idp.follow(&server.broker_clock)` issues person tokens
  at the broker's test clock. `idp.token_requests()`,
  `idp.authorize_requests()`, `idp.issued_refresh_tokens()`,
  `idp.issued_access_tokens()`, `idp.userinfo_requests()` and
  `idp.jwks_fetches()` report what the broker sent and got.
- `broker_client.rs` sends token requests exactly as the pinned dbt clients
  do (`client_credentials`, `token_exchange`, and the browser sign-in's
  `authorization_code` as Fusion and as the Python client post it, and
  `refresh_token`, whose form both clients share), and
  `claims(jwt)` reads a token's claims unverified, as the clients do.
  `SignIn::new()` is one browser sign-in as the clients start it (client id
  `DBT_CLIENT_ID`, redirect `LOOPBACK_REDIRECT_URI`, PKCE S256, `state`);
  `.client_id(id)`, `.redirect_uri(uri)`, `.without_pkce()`,
  `.plain_pkce()` vary it, and `.authorize_url(auth_url)` is the URL the
  client opens.
- `browser.rs`: `Browser::new().route(ISSUER, &server.broker_url())`
  follows redirects (`visit(url, LOOPBACK_REDIRECT_URI)`) until the dbt
  client's loopback handler (`Landing::Loopback(query)`) or a page
  (`Landing::Page { status, body }`). Tests: `tests/person_login.rs`, and
  renewal in `tests/person_renewal.rs` and, across replicas,
  `tests/shared_logins.rs`.
- `http.rs` runs an axum router on its own thread and runtime (`HttpServer`).
- `rows.rs` reads `executions` and `node_states` back for assertions,
  `decisions(&server, n)` the `node_decisions` rows once `n` are captured,
  `principals(&server)` the Principal records and `tables(&server)` the
  schema's tables;
  `every_row_as_text(&server)` dumps the schema, to prove a value was never
  stored.
- `legacy.rs` (legacy fingerprints, frozen rows, corrupt encodings) and
  `fault.rs` (fault-injection DDL) are the only raw-SQL State writers.
  `legacy::forget_rendered_volatile_values` and
  `legacy::forget_volatile_evaluation_flag` make a captured decision look
  recorded before migration 0027 or 0028, and
  `legacy::forget_data_test_history_keys` makes State history look recorded
  before migration 0029 (#200).
  `legacy::record_history_before_principals(&database, org)` migrates a
  `TestDatabase` only to the migration before `principals` and records a
  decision and an execution there, for upgrade tests of migration 0032
  (#226);
  `fault::fail_principal_records` makes every `principals` write fail;
  `fault::fail_session_status` makes every person session status read
  fail.
- `python.rs` provides `python_project(...)` for real-dbt cases. Successful
  projects are removed; a failed one is kept and its path reported.

`scenario_guard.rs` fails when a file in its `MIGRATED` list builds requests by
hand, destructures decisions, runs SQL, or when raw State writes appear outside
`legacy.rs`/`fault.rs`. The capture database test lives in
`crates/server/tests/capture.rs`, so server library tests stay independent of
Postgres.

## Offline regressions

- `offline_regressions.rs` and `correctness.rs`: public gRPC decisions and
  persistent state, including overwritten history, target relation existence
  and External modifications, scoped cached test outcomes, dependency and
  context fingerprints, unknown freshness, seeds, duplicate confirms,
  Record/Submit parity, corruption, old fingerprints and unsupported RPCs.
- `warehouse_portability.rs`: independent physical histories across connection
  namespaces and conservative same-target reuse for unverified warehouses. It
  does not show hosted or warehouse parity.
- `data_test_history.rs`: the Snowflake data-test history key (#200, ADR
  0007): alternating Targets through a copy this server recorded, a copy
  changed after the copy, a newer failing run in the key (no
  resurrection), an older run under another key, two unique ids sharing a
  key (the later-confirmed run is served), severity, namespace and other
  result-setting config separating keys, organization isolation, a copy of
  an older source build, a RegisterClone copy, a relation never copied,
  rows recorded before the key (`legacy::forget_data_test_history_keys`)
  and unchanged per-unique-id history on Databricks and DuckDB.
- `speculative_submit.rs`: `SubmitEnrichedSQLSpeculative` SKIPs only an
  unchanged data test without upstreams, with the ordinary SKIP's payload and
  an explainable decision id, and stays `undecided` for a data test with an
  upstream, incomplete cached results, a model without upstreams and another
  organization's history (#103).
- `crates/ui/src/db.rs::every_read_is_scoped_to_the_configured_organization`:
  organization isolation for every UI read.
- `crates/server/src/decision_proptest.rs`: properties of the pure freshness
  policy for an already selected candidate. They are a regression oracle, not
  an independent model of store selection or hosted lag semantics.
- `crates/server/src/sql_norm.rs` tests and `sql_normalization.rs`: captured
  positive equivalences, negative cases and an arbitrary-UTF-8 no-panic
  property ([sql-normalization.md](sql-normalization.md)).
- `lag_tolerance.rs`: pins the hosted Lag tolerance rule (ADR 0007) by fixture
  inspection, including `seed_upstream_arms` (seed upstreams) and
  `copy_candidate_arms` (which build hosted reuses or copies when the latest
  build no longer qualifies). Most of its fixtures also replay in
  `conformance.rs`.
- `data_test_explain.rs`: pins what the data-test Explain captures show by
  fixture inspection: passing and failing tests render the same tree, the
  EXECUTE tree does not depend on the rebuild reason, a `store_failures` test
  reports its audit relation (a table or a view, passing or failing) and
  hosted does not list it (#88), the client names a separate audit schema
  as `default_schema` and carries a configured alias nowhere (#106), and
  hosted serves incomplete cached results
  (#87). All five fixtures also replay in `conformance.rs`.
  `data_test_freshness.rs` checks offline that only the test's own audit
  relation is dropped from its inputs: a new upstream next to it, or a
  table that only resembles it, still rebuilds; an audit relation in a
  separate audit schema is dropped, and so is an aliased one as the only
  table in the test's schema its SQL never mentions, while a new upstream
  next to it (named by the SQL, or a second unnamed table) still rebuilds
  (#123). It also checks that the Explain tree of a test without upstreams
  never lists its audit relation.

## DuckDB acceptance gate

The maintainer deferred slow local real-dbt DuckDB acceptance during the
Databricks effort (2026-11-15). The original preview landed in PR #164; the
remaining Databricks implementation continues on `integrate/databricks-followups`
with the same lean validation workflow. Do not run `scripts/duckdb-ci.sh` or
ignored real-dbt DuckDB cases for each Databricks slice. Both Databricks
integration branches have the temporary PR CI exception; main, other PRs and
manual/scheduled runs retain their normal coverage. Keep the deferral explicit
in validation notes and revisit DuckDB acceptance when this effort concludes.
Targeted Rust/protocol tests and ordinary workspace regression checks continue.

`./scripts/duckdb-ci.sh` installs the locked Python environment, builds all
targets once, and runs every fixed real-dbt case plus bounded generated traces
with independent-oracle mutation probes. Each case gets its own Postgres schema
and DuckDB file; logs land in `artifacts/duckdb/`. A scheduled or manual CI job
runs a larger generated set (`--generated-only`). DuckDB checks establish local
Reuse safety only; Snowflake captures remain the hosted-conformance evidence.
See [duckdb.md](duckdb.md).

## Captured traffic replay

`crates/harness/tests/conformance.rs::conformance_replay_all_corpora` loads
every `golden/fixtures/*.jsonl` not named in its `CHARACTERIZATION_ONLY` list
and replays it through the gRPC server against an empty store. The replayed
corpus size is pinned there. Invalid trace lines fail with a line number.

- Confirms are correlated by mapping hosted request IDs to local ones, and keep
  the captured outcome. A confirm with no local pending request is counted as
  an unknown baseline.
- Hosted `ready_to_execute` responses are compared in full after removing
  opaque IDs. A first-build-versus-rebuild rationale difference is tolerated
  only when the local store has no confirmed history for that workload, and
  only for the exact (fixture, entry) set listed in the test.
- A hosted SKIP is compared in full when the replay has confirmed history for
  that workload. A data test's workload is its unique id, but a Snowflake
  data test reuses the latest run of its history key under any unique id
  (#200), so a data-test SKIP the local server also SKIPs is compared in
  full too. Otherwise it depends on unknown hosted history and is counted
  as an unknown baseline, never hydrated with guessed epochs.
- A hosted SQL-submit `ready_to_clone` is compared in full and its confirm is
  correlated. A `RegisterClone` is driven and its full response compared
  after removing opaque IDs (both the top-level copy and the deprecated
  `ready_to_clone_v1` mirror). A granted copy's confirm is correlated, so the
  copy is State history for the next submit, only when the copy's source was
  confirmed in the same fixture; otherwise the confirm is an unknown baseline.
  `RecordExecutions` (sent by client 2.0.11 instead of a submit for an
  incremental run under `--full-refresh`) is driven and its response compared;
  each record counts as confirmed history for its workload.
  `ResolveDeferredRelations` executes through the same production router and
  compares the complete mapping, success status and application metadata.
  Fresh `databricks_deferral{,_scopes}.jsonl` histories have zero unknown
  baselines/gaps; `deferral.rs` also pins organization and lifecycle safety.
- `ValidateClientVersion`, speculative submits and `GetExplainMessages` are
  compared too. A speculative submit is compared in full after removing
  opaque IDs, and a speculative SKIP's decision id is correlated for the
  Explain that follows, like a submit's (#103).

`CHARACTERIZATION_ONLY` holds interleaved multi-run captures whose decisions
depend on warehouse state or history outside the capture. Each entry has a
comment naming the dedicated test that pins it.

### `golden/replay_gaps.json`

Each entry names a `fixture` and `entry` index, the `local_variant` the server
returns, and a `reason`. Kinds:

- **Whole-response gap** (no `ignore_fields`): the local response differs, for
  example missing source history for a hosted clone or a deliberate
  safety-floor divergence. For a submit, the local variant must equal
  `local_variant`. A hosted rebuild that the server decides as another class
  (a decision divergence, spec #40) needs one; its hosted confirm then has no
  local counterpart.
- **Field-scoped gap** (`ignore_fields`): the named fields are stripped from
  both responses and the rest must match exactly. The named fields must still
  differ.

Rendered Volatile values are the only runtime values the replay normalizes
(`conformance.rs::normalize_volatile_values`, #193). In every submit and copy
response, a `transformed_nodes_by_query` value whose function text is
registered in `VOLATILE_VALUE_FORMATS` must have that format and is then
compared as the format's name; map keys and function text are compared
exactly, and values of other functions are compared as they are. Explain
`«function» -> rendered with '«value»'` lines are compared the same way
(`conformance.rs::normalize_volatile_line`). A malformed value fails the
replay. Field-scoped gaps are judged after this normalization, so a map that
differs only in its runtime values needs no gap. The only Volatile evaluation
gap is `explain_volatile_cells.jsonl` entry 146, a field-scoped
`execution_runtime_ms` gap keyed to #200 (maintainer decision 2 on #201,
[correctness-handoff.md](correctness-handoff.md#known-divergences)). A
field-scoped gap can also cover a submit `ready_to_clone` payload (#202
entry 207 had one until #207).
- **Line-scoped Explain gap** (`ignore_explain_lines`): hosted Explain lines
  whose text is exactly one of the named texts are removed, with their
  children, from the hosted tree only. The decision, the description and the
  rest of the tree must then match exactly, and at least one named line must
  be present. The #191 and #202 volatile lines it was used for are rendered
  since #194, #195 and #203–#207; no gap uses it now.
- **Empty-tree Explain gap** (`local_variant: "empty_tree"`): a hosted
  decision this server makes differently (spec #40). The local Explain tree
  for it must be empty, and must differ from hosted's.

The replay fails if any gap is unused, if a whole-response gap now matches
hosted ("gap is resolved; remove its exemption"), or if a field-scoped gap's
fields no longer differ. Divergences are explained in
[correctness-handoff.md](correctness-handoff.md), keyed by these entries.

Passing the replay is evidence for the captured cases only. It does not show
that every feature or response field is implemented.

### `golden/upgrade/`

Frozen rows captured from an old server revision. `clone_submit_fallback.rs`
loads them (`upgrade_rejects_frozen_366be09_*`) and asserts the current server
does not reuse them. Do not regenerate them with current hashing code.

## Hosted discovery (differential fuzz)

```bash
cargo run -p dbt-state-harness --features fuzz --bin diff-fuzz -- \
  --seeds golden/fixtures/<file>.jsonl --iterations 200
```

Needs `~/.dbt/dbt_cloud.yml` and uses dbt State metering. Its local side starts
with empty history and compares decision variants only, so it is discovery,
not a conformance gate. Failed comparisons and divergences fail the process.
Promote useful controlled cases into `golden/fixtures/`.

### Native supplementary replay clocks

`DBT_STATE_EXTRA_REPLAY_CAPTURE=/path/databricks_native.jsonl` adds a complete
native capture to the hosted conformance gate. Native UUIDv7 IDs are minted
*after* the handler samples its clock: their timestamps do not establish the
instant saved for relative-age Explain text. Retain that actual clock with
`support::native_clock::retain(&server, capture, evidence)` in the native test
wrapper after the proxy drains and before the temporary State schema is dropped.
The collector reads exact organization-scoped `node_decisions` audit rows; it
never changes the live request clock or the proxy capture.

Set `DBT_STATE_EXTRA_REPLAY_CLOCKS=/path/native-clocks.json` with the extra
capture to replay those saved instants. The evidence must match the entire
capture's SHA-256, cover every ordinary Submit decision once with its original
organization and ID, and put each sampled clock inside the captured request
interval. Missing, duplicate, foreign or out-of-interval rows fail validation.
The sidecar records its persisted-audit schema or reviewed WAL-recovery
provenance. Its provenance hashes identify independently retained evidence;
they do not replace review of that evidence. Hosted fixtures keep their existing
UUID clock path. Without a sidecar, supplemental replay retains that old path
and cannot establish exact native relative-age fidelity.

The exact five-row regression and its independent clock evidence are documented
in `crates/harness/tests/fixtures/native-clock147.md`. Native supplementary
results remain local replay evidence, separate from hosted conformance.

For the bounded #148 predicate replacement seam, run
`python3 experiments/databricks_replace_where_test.py`,
`python3 experiments/databricks_predicate_recovery_test.py` and
`cargo test -p dbt-state-harness --test databricks_replace_where`. The ordinary
tests check independent cumulative history, duplicate/NULL retention, typed
schema/reference and native-error admission faults, exact scope/wire/SQL guards,
and the complete hosted
Record11→ordinary11 SQL transition. The real-client test is ignored and needs
an exact separate grant, fresh owned pair and frozen tools/ledger. Its native
capture must retain actual audit clocks before State cleanup and pass complete
supplemental replay with `DBT_STATE_EXTRA_REPLAY_CLOCKS`; no live replay clock
header is used. See the [bounded report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-replace-where-2026-11-14.md).

For #150's moving-window supplement, run
`python3 experiments/databricks_moving_windows_test.py` and
`cargo test -p dbt-state-harness --test databricks_microbatch --test databricks_150_description`.
These check independent cumulative bags and guarded no-history BUILD display.
The ordinary conformance gate includes the whole hosted77 history; the ignored
native test requires pinned tools and exact owned names. Original native33 and
public-hydrated/native50 retain distinct actual-clock evidence and are replayed
independently. [Scope and attribution](databricks-microbatch.md).

`cargo test -p dbt-state-harness --test databricks_generated` is an ordinary,
credential-free generated **modeled Reuse safety** gate. It uses public
SubmitEnrichedSQL, ConfirmExecution and GetExplainMessages with isolated
Postgres, four fixed semantic seeds and eight bounded source episodes per seed.
Its fixed spine covers genuine source INSERT/UPDATE/DELETE, changed literal/k,
unchanged required reuse, A→B→A latest physical history and source-based org,
named namespace and Target relation isolation controls. Epochs and request
metadata are explicitly synthetic; changed data always advances a positive
epoch. The grammar is FULL=1, zero Lag tolerance, ANY=0, literal cast/VALUES
and direct source projection with bounded INT addition and NULL propagation.

The virtual managed result R changes only on an effective BUILD. A separate
fresh reference F is computed from the source ledger and symbolic intent before
every response; typed row multisets retain duplicates and NULLs. SQL rendering
has fixed worked controls against the restricted templates. This is a tiny
relational interpreter, **not Databricks SQL execution**; MANAGED/Delta tags are
modeled expectations, not measured properties. Missing/equal/delayed changed-data
epochs, no-op DML, Lag/ALL, copies and other node kinds are excluded from this
bounded grammar and retain their separate unresolved coverage requirements.

Boundary mutants preserve the raw server response, force wrong SKIP after
10→17, k or literal changes, and must fail exactly `wrong-skip:rows`. A forced
BUILD at an unchanged checkpoint leaves R=F but fails
`required-reuse:decision`; no server IDs or confirmations are fabricated.
Checker controls reject duplicate loss, NULL substitution, schema type/order,
absent results and changed modeled properties, and prove BUILD snapshots do not
alias the mutable source ledger. Transport errors cannot satisfy mutant tests.

Set `DBT_STATE_DATABRICKS_GENERATED_ARTIFACTS=/path` to retain prewritten input
plans and JSONL request/response/Confirm/Explain, source and binary-source pins,
R/F, epoch/clock and violation ledgers. Default: a run-nonced directory below
`$TMPDIR/dbt-state-databricks-generated`. Replay the same semantic plan with the
same seed using the test command; each invocation uses fresh org/namespace scope.
CI uploads this gate's artifacts for 14 days even on failure, independently of
DuckDB's conditions. Artifacts say `generated-modeled-safety`,
`hosted_verified=false`, `actual_client=false`, `real_warehouse_execution=false`.
The complete hosted fixture replay remains a separate unchanged gate.

An [opt-in live replay](databricks-generated.md) exports the same bounded plan
by explicit seed and executes it through pinned dbt, a recording proxy and
hosted or local Rust State. It verifies complete actual typed results and
MANAGED/Delta properties against the independent ledger and a State-disabled
read-only dbt Reference execution. Ordinary CI remains credential-free; no
cloud schedule is added. Broader grammar and a general shrinker remain future
extensions. Neither generated mode resolves #140's unknown-evidence policy or
by itself establishes complete Databricks hosted conformance.

For the bounded #149 DELETE_INSERT seam, run
`python3 experiments/databricks_delete_insert_test.py` and
`cargo test -p dbt-state-harness --test databricks_delete_insert`. The ordinary
test checks independent multiset key replacement, selected/absent NULL keys,
empty input, duplicates, exact schema/reference/SQL/scope guards, actual nested
fresh-SKIP flags and a full-refresh kind locked from prior hosted evidence.
The real-client test is ignored and requires an exact separate grant and fresh
owned pair. Its native wrapper retains actual audit clocks before State cleanup;
whole supplemental replay supplies that sidecar. See the
[dated bounded report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-delete-insert-2026-11-15.md).

For #144's genuine-source table/view audits, run
`python3 experiments/databricks_audit_upstream_test.py`,
`cargo test -p dbt-state-harness --test databricks_audit_upstream` and the fast
`conformance::databricks_audit_upstream_exact_public_history` test. The ordinary
corpus includes its complete hosted81 history. The ignored native test needs
fresh exact owned aliases and an observed contract; retain its actual clocks,
then use `databricks_audit_native_exact_public_history` with
`DBT_STATE_EXTRA_REPLAY_CAPTURE` and `DBT_STATE_EXTRA_REPLAY_CLOCKS` for exact
whole-native replay. [Scope, ownership and evidence](databricks-audit-tests.md).

For #151's source-clock snapshot supplement, run
`python3 experiments/databricks_snapshot_modes_test.py`,
`cargo test -p dbt-state-harness --test databricks_snapshots`, and
`conformance::databricks_snapshot_modes_exact_public_history`.
The complete hosted source-clock/check and timestamp precision/adoption history
joins the ordinary fixture gate. The ignored native snapshot-modes test requires
fresh exact owned UUID objects and the frozen operation/ownership grant. Retain
its actual audit clocks, then run
`databricks_snapshot_modes_native_exact_public_history` with the extra capture
and clock sidecar for complete native public replay.
[Snapshot scope and compatibility limits](databricks-snapshots.md).
