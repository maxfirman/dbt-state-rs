# Development

## Toolchain

- **Rust** 1.98 (pinned by `rust-toolchain.toml`; server/proto/harness use
  edition 2021, the UI edition 2024).
- **protoc** (protobuf compiler), required by `tonic-prost-build`. Install it
  and put it on `PATH` (e.g. `brew install protobuf`, `apt-get install
  protobuf-compiler`), or set `PROTOC`.
- **Postgres** reachable at `DATABASE_URL`. CI uses `postgres:17-alpine`.
- **uv** and **Python 3.12** for the DuckDB acceptance cases
  (`scripts/duckdb-ci.sh`).

## Build, lint and test

```bash
cargo build --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings   # same as CI
cargo test --workspace                     # unit + integration (needs Postgres)
cargo test -p dbt-state-server --lib       # server library tests (no DB)
./scripts/duckdb-ci.sh                     # every fixed and generated DuckDB case
```

Integration tests create a unique, isolated Postgres schema per test and run the
migrations into it, so they are independent and repeatable. CI
(`.github/workflows/ci.yml`) runs the same commands; `duckdb-ci.sh` runs there
with `--jobs 4`. Validation strategy is in [testing.md](testing.md).
The Databricks preview and remaining implementation tickets retain the
maintainer-approved [DuckDB acceptance deferral](testing.md#duckdb-acceptance-gate).

## Pre-commit hook

A tracked hook, [`.githooks/pre-commit`](../.githooks/pre-commit), runs
`cargo fmt --all --check` when at least one staged file ends in `.rs`, and
exits immediately otherwise. Enable it once per clone:

```bash
git config core.hooksPath .githooks
```

It checks formatting only; clippy and tests stay in CI. Bypass once with
`git commit --no-verify`.

## Postgres

Any reachable instance works. A disposable container:

```bash
docker run -d --name dbt-state-pg \
  -e POSTGRES_USER=dbtstate -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate \
  -p 55441:5432 postgres:17-alpine
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
```

The server applies the migrations (`crates/server/migrations/`) on startup.

## Running the server

```bash
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
export DBT_STATE_LISTEN=127.0.0.1:50051        # optional, this is the default
export DBT_STATE_REQUIRE_ORG_HEADER=false      # optional; true rejects requests
                                               # without x-organization-id
RUST_LOG=info cargo run -p dbt-state-server
```

That is `DBT_STATE_AUTH=none` (the default): no tokens. It refuses to start
on a non-loopback address (for example `0.0.0.0:50051`) unless
`DBT_STATE_ALLOW_INSECURE=true`.

`DBT_STATE_AUTH=oidc` requires a Token-broker-signed token on every RPC
except health, for an Organization in the TOML auth config at
`DBT_STATE_AUTH_CONFIG` ([example](../examples/auth/auth.toml),
[wire contract](protocol.md#authentication-oidc-mode)).
`DBT_STATE_REQUIRE_ORG_HEADER` is a startup error in this mode.

The Token broker issues those tokens. Set `DBT_STATE_BROKER_LISTEN` (for
example `127.0.0.1:8080`) to run it in the server process on its own HTTP/1.1
listener, sharing the config's `[signing]` keys; the gRPC listener stays
HTTP/2. It needs `[signing]` and `[upstream]` in the auth config, and is a
startup error in `none` mode. To run it on its own instead:

```bash
DBT_STATE_AUTH_CONFIG=auth.toml DBT_STATE_BROKER_LISTEN=127.0.0.1:8080 \
  cargo run -p dbt-state-broker
```

A gRPC server whose auth config has no `[signing]` then checks tokens
against that broker's published keys: fetched from `broker.jwks_url`, or by
OIDC discovery at `broker.issuer`, and cached (refetched after 5 minutes, or
when a token's key is unknown, at most every 30 s).

A CI machine authenticates with its Upstream IdP app registration: point the
dbt client's token URL (`RUN_CACHE_TOKEN_URL`) at `<broker>/token` and set
its client id and secret (`DBT_ENGINE_STATE_OAUTH_CLIENT_ID`,
`DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET`). The broker forwards the
credentials to the IdP's `client_credentials` grant, checks the access token
it returns, and applies the `kind = "machine"` mapping rules.

## Running the UI

```bash
HOST=127.0.0.1 PORT=4000 DBT_STATE_UI_ORG_ID=local cargo run -p dbt-state-ui
```

It reads the same `DATABASE_URL`. `HOST`/`PORT` default to `127.0.0.1:3000`;
`DBT_STATE_UI_ORG_ID` defaults to `local`. See [ui.md](ui.md).

## Pointing the dbt client at the server

The dbt Fusion client (dbt v2) uses an insecure channel, and therefore sends
**no OAuth**, when `RUN_CACHE_API_SECURE=false`:

```bash
export DBT_ENGINE_MANAGE_STATE=true        # enable dbt State
export RUN_CACHE_API_URL=127.0.0.1:50051   # our server
export RUN_CACHE_API_SECURE=false          # insecure => no OAuth
dbt build --profile <profile> --target <target> --skip-semantic-manifest-validation
```

Client auth, metadata headers and configuration are described in
[protocol.md](protocol.md).

### Warehouse note

Hosted dbt State does not engage for DuckDB. Live validation against the
hosted service uses Snowflake or [Databricks](databricks-preview.md); local
DuckDB runs use the [DuckDB workflow](duckdb.md).

### Databricks live setup

Databricks characterization uses **dbt 2.0.11**, Unity Catalog and managed
Delta tables on the tested Free Edition serverless workspace. The
[support matrix](databricks-preview.md) and [acceptance report](databricks-acceptance.md)
record the verified cases and remaining limits.

Use a `databricks` base profile in `~/.dbt/profiles.yml`. Replace the host,
warehouse ID and schema below with existing demo resources:

```yaml
databricks:
  target: prod_demo
  outputs:
    prod_demo:
      type: databricks
      host: <workspace-host>
      http_path: /sql/1.0/warehouses/<warehouse-id>
      catalog: cdo_demo_db
      schema: <existing-demo-schema>
      token: "{{ env_var('DBT_ENV_SECRET_DATABRICKS_TOKEN') }}"
      threads: 1
```

Keep the token in the Databricks CLI's `DEFAULT` profile in
`~/.databrickscfg` (mode `0600`). The campaign runners read it and set
`DBT_ENV_SECRET_DATABRICKS_TOKEN` for their child processes. The CLI host must
match the dbt profile's host, and the profile's HTTP path must name the same
warehouse used by the runner's `--warehouse` option.

The local demo at `~/projects/jaffle-shop-databricks` has a `dbt-local` wrapper
that reads the same token, sets the secret environment variable and invokes
`dbt` from the project directory. With the pinned binary on `PATH`, check its
connection with:

```bash
cd ~/projects/jaffle-shop-databricks
./dbt-local debug --target prod_demo
```

For new captures, use the [Databricks experiment drivers](harness.md#databricks-capability-campaign)
and the relevant feature's documented ownership and schema-lease requirements.
Keep all Warehouse mutations in `cdo_demo_db` and preserve existing experiment
objects. The [volatile evaluation runner](databricks-volatile.md#reproduce)
is one example using fresh UUID-named objects in evidenced existing schemas.

## Capturing real traffic

See [harness.md](harness.md) (recording proxy) and [testing.md](testing.md).
Those flows need a dbt Cloud credential in `~/.dbt/dbt_cloud.yml` and consume
dbt State metering.

Server internals are in [architecture.md](architecture.md).
