# Token broker delegates identity to the Upstream IdP

## Status

Accepted (2026-11-21, spec #215, revised after its
[review](https://github.com/maxfirman/dbt-state-rs/issues/215#issuecomment-7069495787)).
The child tickets of #215 implement it. Terms are defined in the
[glossary](../../GLOSSARY.md#access).

## Context

A self-hosted deployment has no authentication. The gRPC server trusts the
`x-organization-id` header, falls back to the shared `local` Organization, and
the UI has no login. Enterprises need single sign-on through their corporate
identity provider and a way to offboard people and machines. dbt users need
their unmodified dbt client to work as it does against hosted dbt State.

The client's token contract is fixed and not standard OAuth
([hosted auth report](../reports/hosted-auth-2026-11-21.md), sections 2 to 4):

- every token response must contain an `id_token`, including the response to
  `client_credentials`;
- the client never requests `openid`; it sends only `runcache:scope:orgs`;
- it reads Organization access from a string claim literally named `scope`, in
  the `runcache:scope:{org|app}:<ORG>:<role>` grammar;
- without a refresh token, an expired person token sends the person back
  through the browser;
- a client that finds a dbt platform credential always uses
  `subject_token_type=dbt` token exchange and never falls back to the browser.

## Decision

dbt-state-rs ships a **Token broker** that speaks the dbt client's token
protocol and owns no identity. It delegates every authentication to the one
**Upstream IdP** the operator configures through OIDC discovery (Entra ID,
Okta, Ping, Keycloak and others).

- **People** sign in by browser. The broker acts as a standard OIDC relying
  party of the Upstream IdP. Each renewal requires fresh upstream evidence
  naming the same Principal, and the mapping is applied again.
- **Machines** use an app registration of the Upstream IdP. The broker passes
  their `client_credentials` through to the IdP and validates the JWT access
  token the IdP returns.
- **Mapping**: operator config maps IdP claims to an Organization and a Role.
  Each rule applies to one Principal kind, which comes from the validated
  flow. The highest Role wins, and a Principal with no matching rule is
  denied.
- **Tokens**: the broker issues RS256 tokens in the shape the client expects,
  for one dbt State API audience. gRPC accepts only that audience.

The broker holds no passwords, MFA factors, user accounts or machine secrets.
Users, MFA, SSO sessions and offboarding stay with the customer's IdP.

## Why

The [build-versus-adopt report](../reports/auth-build-vs-adopt-2026-11-21.md)
tested the contract against the available products. Every way of meeting it
needs custom code. A thin broker puts that code in one place we can test, and
it adds no system the customer must run beyond the server and its Postgres.
Cross-Organization isolation is then decided by a pure mapping function with
property tests, not by scripts or settings inside another product.

Self-hosted products with the same problem do the same: Pinniped, HashiCorp
Vault and Teleport each run a small issuer that federates to the customer's
IdP, and Harbor mints its own credential for clients that cannot do OIDC.

## Considered Options

- **Bundle an IdP.** Rejected: no candidate meets the contract unchanged, and
  each one becomes a second IdP the customer must operate behind the corporate
  one (admin console, user store, upgrades, CVEs, HA, backups).
  - **Keycloak 26.8.2**, tested live in a disposable container, fails:
    - It issues an `id_token` only when the raw `scope` parameter contains
      `openid`. Making `openid` a default client scope does not change this.
    - It refuses any mapper that writes the `scope` claim ("Claim 'scope' is
      non-modifiable in IDToken").
    - It serializes multi-valued group claims as JSON arrays, which the client
      cannot read.
    - It rejects `subject_token_type=dbt` exchange.

    Making it work needs a Java SPI or a re-minting proxy, which is custom
    code anyway. It also idled at 588 to 850 MiB and took about 26 s to become
    ready.
  - **authentik** is the runner-up. A Python scope mapping can produce the
    `id_token` and `scope` claim. That mapping lives in authentik's admin
    database, outside our code and tests. Refresh tokens require
    `offline_access`, which the client never requests, so people would sign
    in by browser at every token expiry. Token exchange stays unsupported.
  - Zitadel, Ory Hydra, Dex, Authelia and Kanidm each fail at least one
    requirement (report table).
- **Point the client straight at the customer's IdP.** Rejected: CI cannot
  work. None of Entra ID, Okta, PingFederate or Google issues an `id_token`
  from `client_credentials`. Without `openid` in the request, a compliant
  provider issues no `id_token` to people either. Entra ID restricts the
  `scope` and `scp` claims, and no IdP produces the runcache grammar unaided.

A customer without an OIDC IdP can still put Dex, Keycloak or authentik
upstream of the broker, as Argo CD's optional bundled Dex does. The broker
needs no extra code for that.

## Consequences

- **Machine secrets are passed through and never stored.** The broker
  forwards a machine's client id and secret to the Upstream IdP's
  `client_credentials` grant and never persists them. It accepts only a
  signed JWT access token with the configured upstream API audience, and
  refuses opaque tokens. Machines are not registered with the broker, so a
  database leak exposes no CI secret, and machine credentials are created,
  rotated and revoked at the IdP. The cost: the IdP must issue JWT access
  tokens for an API audience. Token introspection and a local machine
  registry are out of scope.
- **dbt platform token exchange is rejected in v1.** Accepting dbt platform
  tokens would make dbt platform a second identity source the customer does
  not manage. The token endpoint refuses the exchange, the client reports a
  generic 4xx, and the dbt run fails. The client gives no opt-out, so a
  person whose environment exposes a dbt platform credential
  (`DBT_CLOUD_TOKEN`, a `dbt login` session or `~/.dbt/dbt_cloud.yml`)
  cannot use the browser flow against a self-hosted deployment. A later
  opt-in with an allowlist of dbt platform hosts remains possible.
- **Person token lifetime is driven by Fusion's re-acquisition behavior.**
  Fusion reads the on-disk token once per process and then never uses its
  in-memory refresh token. Once the token comes within 300 s of expiry, it
  re-acquires: by browser for a person, or a failure when no terminal is
  attached (current `dbt-labs/dbt`). Hosted's 15-minute lifetime would prompt
  mid-run in any run longer than about 10 minutes. Person tokens therefore
  default to 60 minutes (configurable 15 to 240), so runs of up to about 55
  minutes don't prompt. The cost is offboarding latency: a person disabled at
  the IdP keeps gRPC access until the issued token expires, at most the
  lifetime plus 60 s skew. To bound admin action separately, gRPC checks the
  person's broker session on every request with a cache of at most 30 s, and
  every person session is capped at 12 hours. A machine re-acquires through
  `client_credentials` without a prompt, so machine tokens keep a 15-minute
  default (configurable 15 to 60). The 15-minute floor for both leaves 10
  minutes of use above the client's 300 s freshness window.
- The broker is security-critical code we own. It follows
  [RFC 9700](https://www.rfc-editor.org/rfc/rfc9700): single-use 60 s
  authorization codes, mandatory PKCE S256, exact redirect URIs, and rotated
  refresh tokens with replay detection.
- The client contract can drift. Changes under the `dbt-labs/dbt`
  `crates/dbt-state/src/auth/` directory must be tracked, and pinned-client
  drivers test the real client code.
- v1 supports one Upstream IdP per deployment and no IdP-specific APIs, SCIM,
  SAML or LDAP in the broker.
- Local and DuckDB workflows keep an explicit no-auth mode, limited to
  loopback addresses.

The build-versus-adopt report predates the spec review. Two of its
recommendations were replaced: 15-minute person tokens became the 60-minute
default above, and its fallback registry of argon2-hashed machine secrets was
dropped in favor of pass-through only.

## Operating it

The [operator guide](../operator-guide.md) describes the deployment as
built: configuration, Upstream IdP recipes, TLS, client settings, and the
[revocation bounds](../operator-guide.md#revocation-bounds) with the test
that verifies each. Two bounds differ from the consequences above. The
60 s refresh grace window can reissue a token from evidence up to 60 s old,
so a person disabled at the IdP keeps gRPC access for at most the lifetime
plus 2 minutes. A person token is refused within 30 s of its session's
12-hour cap, even before it expires.
