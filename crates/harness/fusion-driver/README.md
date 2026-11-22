# Pinned Fusion driver

A small program that runs pinned dbt Fusion's own dbt State client code,
`dbt-run-cache` at
[`dbt-labs/dbt-fusion@9977b6c`](https://github.com/dbt-labs/dbt-fusion/tree/9977b6cbb1b761065536300037560d8e3c037011/crates/dbt-run-cache),
against a dbt State deployment and its Token broker over TLS: Fusion's
config loader, token source (`client_credentials`, the browser flow, the
token on disk, refresh) and gRPC client. The harness suite
`crates/harness/tests/fusion_client.rs` drives it (see
[docs/testing.md](../../../docs/testing.md#pinned-fusion-driver)).

## License constraint

dbt-fusion is licensed under the
[Elastic License 2.0](https://github.com/dbt-labs/dbt-fusion/blob/9977b6cbb1b761065536300037560d8e3c037011/LICENSES.md)
(ELv2), not Apache-2.0 like this project. This driver only compiles
against it: Cargo fetches `dbt-run-cache` from dbt-labs' public repository
at the pinned commit when the driver is built. The rules:

- Never vendor, copy or redistribute dbt-fusion source in this repository
  (no checkout, submodule or copied file). `Cargo.lock` pins versions only.
- Never publish or ship the built driver, and never upload it, its
  `target` directory or Cargo's checkout of dbt-fusion as a CI artifact or
  cache. The CI job builds them in the runner's temp directory.
- The driver is a test tool, outside the Cargo workspace (the root
  `Cargo.toml` excludes it), so `cargo build` and `cargo test` never fetch
  or build dbt-fusion.

The driver's own code (`src/`) is Apache-2.0. The `[patch.crates-io]`
section repeats dbt-fusion's patches to its public `arrow-rs`, `datafusion`
and `sqlparser` forks, because a git dependency does not inherit its
workspace's patches.

## Build and run

Needs `protoc` on `PATH`, network access to GitHub and crates.io, about
2.5 GB of disk for the build, and Postgres at `$DATABASE_URL` for the
suite.

```bash
CARGO_TARGET_DIR=/tmp/fusion-driver \
  cargo build --locked --manifest-path crates/harness/fusion-driver/Cargo.toml
DBT_STATE_FUSION_DRIVER=/tmp/fusion-driver/debug/fusion-driver \
  cargo test -p dbt-state-harness --test fusion_client
```

Without `DBT_STATE_FUSION_DRIVER`, every test in the suite skips.

## How it runs Fusion

The driver calls `RunCacheServiceConfig::from_env`,
`GrpcRunCacheServiceClient::connect` and the client's RPC methods, as a
`dbt build` does; each RPC looks up a credential through Fusion's
`OAuthTokenSource`. The harness configures Fusion only through its
supported settings: `RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE=true`,
`RUN_CACHE_AUTH_URL`, `RUN_CACHE_TOKEN_URL`, a machine's
`DBT_ENGINE_STATE_OAUTH_CLIENT_ID` and
`DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET`, `HOME` (Fusion keeps a
person's token in `~/.dbt/state_auth.json`), `SSL_CERT_FILE` and `PATH`,
in an otherwise cleared environment.

- **Crypto provider.** The dbt binary installs aws-lc-rs as rustls's
  process-wide provider when it starts (`dbt-main`'s `do_execute_fs`); the
  driver does the same before calling Fusion.
- **Browser.** On Linux Fusion opens its sign-in URL with the `open`
  crate, which runs the desktop's URL opener, `xdg-open`, from `PATH`
  (`open` ignores `BROWSER`). The harness puts an `xdg-open` first on
  `PATH` that runs `fusion-driver browser <url>`: it follows the redirects
  through the Token broker and the Upstream IdP to Fusion's loopback
  handler (`http://127.0.0.1:29525/handler`), and logs each visit.
- **Time.** Fusion stamps a token's expiry from the wall clock when it
  receives it and treats it as fresh while more than 300 s remain. To age
  tokens without waiting, the driver binary defines the C library's
  `clock_gettime`, so the Rust standard library's wall clock reads
  (`CLOCK_REALTIME`) return the kernel's time plus an offset the harness
  sets at each `wait` step. Monotonic clocks (tokio's timers, Fusion's
  timeouts) are untouched, and only the driver's process is affected. The
  Token broker and the fake Upstream IdP move on the harness's test clock
  as far. No lifetime validation is relaxed: tests use the real minimum
  (15 minutes) and default (60 minutes) person token lifetimes, and the
  broker's test clock is reachable only through the library API the
  harness calls (`Broker::with_clock`, `AppState::with_oidc_clock`), not
  through any setting of a deployment.
- **Trusting the test CA.** Fusion's gRPC channel uses native roots
  (`ClientTlsConfig::with_native_roots`), which read `SSL_CERT_FILE`. Its
  token requests use reqwest 0.12 with only the `rustls-tls` feature,
  which trusts only the public Mozilla roots compiled in (webpki-roots); no
  setting adds a CA. Built that way, every token request to the test
  broker fails with `invalid peer certificate: UnknownIssuer`. The driver
  therefore enables reqwest's `rustls-tls-native-roots` feature, so Cargo
  builds the same reqwest trusting the native roots as well. This is the
  driver's one change to Fusion's behavior: a build feature, not a source
  change. It means a deployment's Token broker must present a certificate
  from a publicly trusted CA for a client built from this source, while its
  gRPC listener may use a private CA that the host trusts. The released
  dbt 2.0.11 binary also trusts `SSL_CERT_FILE` for its token requests
  (#232, [docs/testing.md](../../../docs/testing.md#live-fusion-acceptance-on-snowflake)).
