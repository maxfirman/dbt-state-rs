# Databricks pinned-client auxiliary RPC evidence — 2026-11-12

#157 covers the auxiliary RPCs actually observed from official dbt **2.0.11**
(SHA-256 `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`)
with the #132 Databricks Free Edition/serverless Unity Catalog profile. The
production router now acknowledges its telemetry. This is an auxiliary RPC
compatibility claim, not full Databricks decision or physical-result conformance.

## Actual inventory and historical capture limit

The [machine-readable index](databricks-client-auxiliary-2026-11-12.json)
inventories every certified capture pinned by the [#134 survey
index](databricks-capabilities-2026-11-12.json), checking capture SHA-256 and
integrity accounting before counting. All **533 auxiliary calls** returned OK:

| RPC | Calls | Complete response |
| --- | ---: | --- |
| `ClientValidation.ValidateClientVersion` | 150 | `{"is_supported":true}` |
| `ClientTelemetry.SubmitTelemetryBatch` | 383 | `{"success":true}` |

The batches contain 150 `session_start`, 223 `enriched_sql_prepared` and 150
`session_end` events. Session-end results are 145 SUCCESS and five FAILURE.
No standalone deprecated `RegisterSessionStart` or `client_selector_event`
appears in those campaigns. Absence from this inventory does not establish
unreachability across all pinned-client commands.

The historical serializer skipped `SessionEndRequest.session_duration`, so
all 150 archived session-end durations are missing. Their durable RPC counts
remain valid, but their duration payloads are incomplete. No historical
duration was reconstructed or promoted as complete evidence. The fix records
optional duration as `null` or exact `{"seconds":int64,"nanos":int32}`; legacy
fixtures with an omitted field remain readable.

## Fresh real-client hosted and Rust checks

[The auxiliary runner](../../experiments/databricks_auxiliary.py) creates its
own empty project and runs two actual pinned-client invocations:

1. `compile --select __auxiliary_no_nodes__`: successful empty compilation.
2. `compile --select auxiliary_fail`: a model's execute-time Jinja raises the
   explicit expected compilation error, after State initialization.

Each role reaches **two version validations and three telemetry batches**.
Both sessions send `session_start` and `session_end`, including nonzero
nanosecond durations; the failure sends `result:1` and `result_description:
"completed with errors"`. Every auxiliary RPC is OK with the exact hosted
acknowledgement shown above. The unsuccessful dbt invocation exits 1 for the
expected compilation error, not for a State error. Both roles retain empty
`query_log.sql` files: **zero warehouse statements and zero warehouse writes**.
No model decision is requested, so there is no relevant Explain tree or
physical output to compare in this auxiliary-only check.

Hosted and Rust were run separately through recording proxies. The Rust proxy
uses #135's explicit literal-loopback mode against the production router and
an isolated, migrated Postgres schema; no hosted credential is loaded by that
proxy mode. The schema and owned server process were removed after validation.
The runner still loads the existing Databricks profile/token internally for
the real client. Ordinary replay tests need neither those credentials nor the
pinned executable.

Raw secret-free evidence is retained at
`/home/maxfirman/.local/state/dbt-state-rs/databricks-157-20261112`, including
commands/return codes, client logs, empty warehouse query logs, JSONL, graceful
shutdown integrity sidecars, summaries, runner and a SHA-256 artifact manifest.
The linked index pins final runner-verified hosted/Rust captures and the
promoted fixture's earlier hosted source; every original session is retained.

```bash
# Build once; hosted credentials use the existing internal configuration.
cargo build -p dbt-state-harness --bin record-proxy
python experiments/databricks_auxiliary.py \
  --dbt /path/to/dbt-2.0.11 --proxy target/debug/record-proxy \
  --profiles-dir /path/to/databricks/profiles --profile databricks \
  --work-dir /path/to/new-hosted-output

# Repeat against a running Rust production server, retaining a separate output.
# Add: --local-upstream http://127.0.0.1:PORT
```

## Credential-free replay and safety checks

`golden/fixtures/databricks_client_auxiliary.jsonl` contains the full five-call
fresh hosted auxiliary session, followed by one independent stateless batch
of eight actual `enriched_sql_prepared` events from #134. That extra batch has
both present and absent target names and optional dependency fields, and no
session-end event with an omitted duration. Auxiliary acknowledgements have no
State-history prerequisites, correlated IDs or nondeterministic payload fields.

`client_auxiliary` replays all six calls through generated gRPC clients and
the production router against isolated Postgres. It compares full response
fields/presence, status code/message/details and safe application metadata.
Mutation checks reject missing response fields, changed status and changed
application metadata. The existing five observed ingress headers are retained
in captures and omitted from protocol comparison; no new normalization or
Databricks exception was added. Generic conformance now replays these auxiliary
calls and requires zero unknown baselines, gaps and partial-history exceptions
for Databricks fixtures. The duration regression independently exercises real
protobuf forwarding, proxy capture, JSON loading and gRPC replay, and failed
before the serializer fix.

Validation performed:

- Public auxiliary regression: red before router support, green afterward.
- Public proxy duration regression: red before serialization fix, green afterward.
- All 10 conformance tests, including 70 replay corpora, passed with the new fixture.
- All 17 targeted tests passed (four proxy tests, one auxiliary replay, ten
  conformance tests and two capture/replay tests).
- `cargo fmt --all --check` and workspace Clippy with all targets, `fuzz` and
  warnings denied passed.
- The actual pinned-client hosted and Rust campaigns above both passed.

The server acknowledges telemetry without implementing hosted metering,
accounts, OAuth or billing. Existing self-hosted authentication is unchanged.
The obsolete standalone session RPC remains unimplemented; no behavior for
older client paths is inferred. There is no observed auxiliary gRPC rejection
in this inventory; client failure telemetry is covered without inventing an
error status. New event variants or newly called methods need fresh evidence
before expanding this dated compatibility claim.
