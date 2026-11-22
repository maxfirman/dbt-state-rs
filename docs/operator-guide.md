# Operator guide: authentication (`oidc` mode)

How to deploy dbt-state-rs so that every dbt client and console user signs in
through your own identity provider. It covers the deployment shapes, the
configuration, recipes for Keycloak, Microsoft Entra ID and Okta, TLS and
load balancers, the dbt client's settings, when people sign in again, the
revocation bounds, and the audit trail.

Terms (Organization, Role, Principal, Token broker, Upstream IdP) are
defined in the [glossary](../GLOSSARY.md#access); the design is
[ADR 0012](adr/0012-token-broker-delegates-identity-to-upstream-idp.md). The
wire contract (token claims, gRPC refusals, every broker error and log
reason) is in [protocol.md](protocol.md#authentication-oidc-mode).

Everything here describes the code on `main`. Where it differs from spec
#215, the difference is called out.

**What is tested.** Every `cargo test` runs the gRPC server and Token broker
against a fake Upstream IdP in the harness. An opt-in suite runs them against
Keycloak 26.8.2 ([testing.md](testing.md#keycloak-upstream-suite)). CI runs
the pinned Python client's own auth code (`dbt-state==2.62.6`) over TLS, and
an opt-in job runs pinned Fusion's (`dbt-fusion@9977b6c`,
[testing.md](testing.md#pinned-fusion-driver)). The Entra ID and Okta
recipes follow the vendors' documentation; no test in this repository runs
against either.

## Contents

- [How it fits together](#how-it-fits-together)
- [Deployment shapes](#deployment-shapes)
- [Configuration](#configuration)
- [Upstream IdP recipes](#upstream-idp-recipes): [requirements](#what-the-broker-needs-from-an-upstream-idp), [Keycloak](#keycloak), [Entra ID](#entra-id), [Okta](#okta)
- [TLS, HTTP/2 and load balancers](#tls-http2-and-load-balancers)
- [Pointing dbt clients at the deployment](#pointing-dbt-clients-at-the-deployment)
- [When people sign in again](#when-people-sign-in-again)
- [Revocation bounds](#revocation-bounds)
- [Offboarding and incidents](#offboarding-and-incidents)
- [Rotating keys and secrets](#rotating-keys-and-secrets)
- [Logging and audit](#logging-and-audit)
- [No-auth mode](#no-auth-mode)
- [Known limits](#known-limits)

## How it fits together

| Component | Speaks | Does |
|---|---|---|
| gRPC server (`dbt-state-server`) | gRPC over HTTP/2 | Decides and records. In `oidc` mode every RPC except `grpc.health.v1` needs a Token broker token for the dbt State API audience, and `x-organization-id` must name an active Organization in that token. |
| Token broker (in `dbt-state-server`, or `dbt-state-broker`) | HTTP/1.1 | Speaks the dbt client's token protocol. People sign in through the Upstream IdP in a browser; CI passes its Upstream IdP client id and secret through. Issues RS256 tokens in the shape the client expects. |
| Postgres | | State history, the broker's sessions, codes and refresh tokens, and the Principals. |
| Console (`dbt-state-ui`, optional) | HTTP/1.1 | Read-only. People sign in as a separate client of the broker and see only their Organizations. |
| Upstream IdP | OIDC | Your Entra ID, Okta, Keycloak or other OIDC provider. It authenticates every person and machine. |

The broker holds no passwords, MFA factors or machine secrets. It stores
people's upstream refresh tokens encrypted, and keeps refresh tokens and
codes it issues only as SHA-256 hashes.

A person's flow: the dbt client opens a browser at the broker, which sends
the person to the Upstream IdP. The broker maps the person's IdP claims to
Organizations and Roles with the `kind = "person"` rules, starts a broker
session (12-hour cap), and returns a token and a refresh token. Every
renewal asks the Upstream IdP again and re-applies the rules. Every RPC
with a person token also checks the session.

A machine's flow: the dbt client sends its client id and secret to the
broker's token endpoint as HTTP Basic. The broker forwards them unchanged
to the Upstream IdP's `client_credentials` grant, checks the JWT access
token the IdP returns, applies the `kind = "machine"` rules, and returns a
token without a refresh token or session.

## Deployment shapes

### One binary, two listeners

The smallest deployment is one `dbt-state-server` process with the broker
on its own listener:

```bash
DATABASE_URL=postgres://... \
DBT_STATE_AUTH=oidc DBT_STATE_AUTH_CONFIG=/etc/dbt-state/auth.toml \
DBT_STATE_LISTEN=0.0.0.0:50051 \
DBT_STATE_BROKER_LISTEN=0.0.0.0:8080 \
  dbt-state-server
```

- The gRPC listener serves HTTP/2 only; the broker listener serves
  HTTP/1.1. Give them separate load balancer listeners or routes
  ([below](#tls-http2-and-load-balancers)).
- The broker uses the config's `[signing]` keys and the server's Postgres
  pool. The gRPC side verifies with the same in-memory key set, so it fetches
  no keys.
- The config needs `[signing]` and `[upstream]`, or the server refuses to
  start.
- The process has one Postgres pool of 10 connections. A token renewal holds
  a connection, and a row lock on the person's session, while it calls the
  Upstream IdP (each call times out after 10 s). Under heavy sign-in load,
  run standalone brokers instead.

### Standalone broker

```bash
DATABASE_URL=postgres://... \
DBT_STATE_AUTH_CONFIG=/etc/dbt-state/auth.toml \
DBT_STATE_BROKER_LISTEN=0.0.0.0:8080 \
  dbt-state-broker
```

- The broker needs `DATABASE_URL`, `DBT_STATE_AUTH_CONFIG` (with `[signing]`
  and `[upstream]`) and optionally `DBT_STATE_BROKER_LISTEN` (default
  `127.0.0.1:8080`). It runs the migrations at startup.
- **It must use the same Postgres database and schema as the gRPC servers.**
  The gRPC server reads `broker_person_sessions` there on every RPC with a
  person token, and the broker writes `principals` there. Person tokens from
  a broker on another schema are refused
  (`bearer token session is not active`).
- A gRPC server whose config has no `[signing]` verifies tokens with the
  broker's published keys: from `[broker] jwks_url`, or by OIDC discovery at
  `[broker] issuer`. It caches them, refetches after 5 minutes or when a
  token names an unknown key (at most every 30 s), and keeps the loaded keys
  when a fetch fails. The key URL must be https or plain http on a loopback
  address, and its certificate must chain to a public root
  ([TLS](#tls-http2-and-load-balancers)).

### Replicas

Any number of gRPC servers and brokers can share one Postgres:

- Every broker replica needs the same config and the same `[signing]` key
  files, and every gRPC replica the same `[broker] issuer`, `[api] audience`
  and `[[organizations]]`.
- Pending sign-ins, codes, sessions and refresh tokens live in Postgres, so a
  sign-in can start on one replica and finish on another, and a restart or a
  lost node signs nobody out.
- The cap on pending sign-ins per client IP is counted in Postgres, across
  replicas.
- Renewals of one session serialize on its row lock. Two dbt processes that
  share a login and refresh at once both succeed: a refresh token rotated
  less than 60 s ago still renews (the grace window), and the second
  renewal reuses the first one's upstream evidence, so the Upstream IdP sees
  each of its refresh tokens once. A renewal waiting on another one's lock
  can wait for that renewal's IdP calls.
- Each gRPC replica caches a session's status for at most 30 s, and the
  console does the same.
- Config is read at startup. Apply a change with a rolling restart. Until
  every replica has restarted, replicas disagree (for example about a
  disabled Organization).

### The console

The console is optional and runs as its own process with its own
environment ([ui.md](ui.md#organization-scope)):

```bash
DATABASE_URL=postgres://... HOST=0.0.0.0 PORT=3000 \
DBT_STATE_AUTH=oidc DBT_STATE_AUTH_CONFIG=/etc/dbt-state/ui-auth.toml \
DBT_STATE_UI_PRIVATE_KEY_FILE=/run/secrets/ui-client.pem \
DBT_STATE_UI_COOKIE_KEY_FILE=/run/secrets/ui-cookie.key \
  dbt-state-ui
```

- Register it in the broker's config as `[clients.ui]`; its `redirect_uri`
  must be `<console URL>/auth/callback`.
- Give it a config without `[signing]` and `[upstream]`, so it holds neither
  the broker's signing keys nor the broker's IdP credential. It then fetches
  the broker's published keys. It reads `[broker]`, `[[organizations]]` and
  `[clients.ui]`.
- It reaches the broker's token endpoint at `[broker] issuer`, or at
  `DBT_STATE_UI_BROKER_URL`. That URL must be https with a publicly trusted
  certificate, or plain http on a loopback address (for example a sidecar).
- Serve it over https: its cookies are `Secure` and `__Host-` prefixed.
- Its database user needs read access to the capture tables and to
  `broker_person_sessions`. Every replica needs the same cookie key.
- A console token cannot call gRPC: its `aud` is the console's client id.

### Database access

| Process | Needs |
|---|---|
| `dbt-state-server` | Owner of the schema: it runs the migrations at startup. |
| `dbt-state-broker` | The same: it runs the same migrations at startup. |
| `dbt-state-ui` | Read access to the capture tables and `broker_person_sessions`. |
| `dbt-state-server revoke-sessions` | Read `principals`, update `broker_person_sessions`. It reads only `DATABASE_URL`. |

## Configuration

Configuration is environment variables plus one TOML auth config file. The
complete annotated config is
[`examples/auth/auth.toml`](../examples/auth/auth.toml); per-IdP configs are
[`entra-id.toml`](../examples/auth/entra-id.toml),
[`okta.toml`](../examples/auth/okta.toml) and
[`keycloak.toml`](../examples/auth/keycloak.toml). Tests load all four
(`crates/auth/tests/config.rs::the_example_config_loads`,
`::the_upstream_idp_recipes_load_and_map_as_documented`).

### Environment variables

`dbt-state-server`:

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | Postgres. |
| `DBT_STATE_LISTEN` | gRPC listen address, default `127.0.0.1:50051`. |
| `DBT_STATE_AUTH` | `none` (default) or `oidc`. |
| `DBT_STATE_AUTH_CONFIG` | `oidc`: path of the auth config. Required. |
| `DBT_STATE_BROKER_LISTEN` | `oidc`: also run the Token broker on this address. A startup error in `none` mode, or when the config lacks `[signing]` or `[upstream]`. |
| `DBT_STATE_TLS_CERT_FILE`, `DBT_STATE_TLS_KEY_FILE` | Serve gRPC over TLS: PEM certificate chain (leaf first) and private key (PKCS#8, PKCS#1 or SEC1). Both or neither. |
| `DBT_STATE_BROKER_TLS_CERT_FILE`, `DBT_STATE_BROKER_TLS_KEY_FILE` | The same for the broker listener. A startup error without `DBT_STATE_BROKER_LISTEN`. |
| `DBT_STATE_REQUIRE_ORG_HEADER` | `none` mode only. Setting it at all in `oidc` mode is a startup error, since `oidc` always requires and checks the header. |
| `DBT_STATE_ALLOW_INSECURE` | `none` mode only ([No-auth mode](#no-auth-mode)). |
| `RUST_LOG` | Log filter, default `info`. |

`dbt-state-broker`: `DATABASE_URL` and `DBT_STATE_AUTH_CONFIG` (both
required), `DBT_STATE_BROKER_LISTEN` (default `127.0.0.1:8080`),
`DBT_STATE_BROKER_TLS_CERT_FILE` and `DBT_STATE_BROKER_TLS_KEY_FILE`,
`RUST_LOG`.

`dbt-state-ui` in `oidc` mode: `DATABASE_URL`, `HOST` and `PORT` (default
`127.0.0.1:3000`), `DBT_STATE_AUTH=oidc`, `DBT_STATE_AUTH_CONFIG`,
`DBT_STATE_UI_PRIVATE_KEY_FILE` (the private half of `[clients.ui]
public_key_file`; startup checks they match), optional
`DBT_STATE_UI_PRIVATE_KEY_ID` (the assertions' `kid`),
`DBT_STATE_UI_COOKIE_KEY_FILE` (base64 of 32 random bytes) and optional
`DBT_STATE_UI_BROKER_URL`. `DBT_STATE_UI_ORG_ID` is a startup error in
`oidc` mode, and the `oidc`-only variables are startup errors in `none`
mode.

Every startup refusal happens before the process connects to anything.
Files are read once at startup; a changed file applies on restart.

### Auth config reference

Unknown keys are refused, so a misspelled key never goes unnoticed. Relative
paths resolve from the config file's directory. Errors never print key or
secret contents.

| Key | Required | Default and bounds | Meaning |
|---|---|---|---|
| `[broker] issuer` | yes | http(s) URL | The broker's public URL: the `iss` of every token, discovery at `<issuer>/.well-known/openid-configuration`, the authorize endpoint `<issuer>/` (the client's `RUN_CACHE_AUTH_URL`), `<issuer>/token` and `<issuer>/callback`. Without `[signing]` and `jwks_url`, it must be https (or http on loopback). |
| `[broker] jwks_url` | no | https, or http on loopback | Where a gRPC server or console without `[signing]` fetches the broker's keys. Unset: discovery at `issuer`. |
| `[broker] max_pending_logins_per_ip` | no | 20; 1 to 10000 | Browser sign-ins in progress per client IP. A pending sign-in lasts 10 minutes. |
| `[broker] client_ip_header` | no | | Behind a proxy that sets it on every request: the header whose last comma-separated entry is the client IP. Unset: the peer address. |
| `[api] audience` | yes | non-empty | The dbt State API audience: the `aud` of every dbt token and the only one gRPC accepts. |
| `[tokens] person_lifetime_minutes` | no | 60; 15 to 240 | Person token lifetime. |
| `[tokens] machine_lifetime_minutes` | no | 15; 15 to 60 | Machine token lifetime. |
| `[signing] active_key_id` | with `[signing]` | | The key new tokens are signed with. |
| `[[signing.keys]] id`, `private_key_file` | with `[signing]` | RSA 2048 to 4096 bits, PKCS#8 or PKCS#1 PEM | Every listed key verifies tokens and is published at `/jwks`. The broker needs `[signing]`; a gRPC server or console may leave it out. |
| `[[organizations]] id` | | visible ASCII, no `:` or whitespace, not `*`, unique | Organization ids you choose. |
| `[[organizations]] disabled` | no | `false` | Refused on every request; issued only as a disabled (`app`) entry. |
| `[upstream] issuer` | for the broker | https (http on loopback) | The Upstream IdP: discovery at `<issuer>/.well-known/openid-configuration`, whose `issuer` must equal this value exactly. |
| `[upstream] api_audience` | with `[upstream]` | non-empty | The `aud` a machine's upstream access token must hold. |
| `[upstream] client_credentials_scope` | no | | The `scope` sent on the IdP's `client_credentials` grant. The scope the dbt client asks for is ignored. |
| `[upstream] subject_claim` | no | `sub` | The claim that, with the IdP issuer, identifies a Principal, for people and machines. Changing it later changes every Principal. |
| `[upstream] client_id` | for browser sign-in | | The broker's own client registration at the IdP. Without it only machines are served, and the keys below are refused. |
| `[upstream] private_key_file` | one of these two | RSA PEM | The broker's credential: `private_key_jwt` (RS256 assertion, `iss` = `sub` = `client_id`, `aud` the IdP's token endpoint, random `jti`, 5 minutes). |
| `[upstream] private_key_id` | no | | The assertion's `kid`. Only with `private_key_file`. |
| `[upstream] client_secret_file` | one of these two | non-empty file | The broker's credential as a client secret, sent as HTTP Basic. |
| `[upstream] refresh_token_key_file` | for browser sign-in | base64 of 32 bytes | Encrypts stored upstream refresh tokens (AES-256-GCM). |
| `[upstream] login_scope` | no | `openid offline_access email profile`; must contain `openid` | The scope requested at the IdP's authorize endpoint. |
| `[clients.dbt] client_id` | no | `2fd87cd5-69a6-4c5f-9097-747a58f0edf6` | The dbt client registration. The default is the dbt clients' built-in id; change it only if your clients set another. Its redirect URI is always `http://127.0.0.1:29525/handler`. |
| `[clients.ui] client_id` | with `[clients.ui]` | differs from the dbt client id and `[api] audience` | The console registration; its tokens carry this `aud`. |
| `[clients.ui] redirect_uri` | with `[clients.ui]` | https (http on loopback), no fragment; path `/auth/callback` | The console's one callback. |
| `[clients.ui] public_key_file` | with `[clients.ui]` | PEM `BEGIN PUBLIC KEY`, RSA 2048 to 4096 bits | Verifies the console's `private_key_jwt` assertions. One key only. |
| `[[rules]] kind` | yes | `person` or `machine` | The Principal kind the rule applies to. The kind comes from the flow (browser sign-in and renewal: person; `client_credentials`: machine), never from a claim. |
| `[[rules]] claim`, `value` | yes | non-empty | Matches when the top-level `claim` equals `value`, or is an array of strings holding it. Exact and case-sensitive; nested claims (for example Keycloak's `realm_access.roles`) cannot match. |
| `[[rules]] organization`, `role` | yes | a declared Organization; `developer` or `admin` | What a match grants. The highest Role wins per Organization. A Principal no rule matches is refused. Any Role may call any RPC. |

### Keys and secrets

Mount these as files, readable only by the process that needs them:

| File | Generate | Used by |
|---|---|---|
| Signing keys | `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out signing-2026-11.pem` | Broker (and an in-process gRPC server). |
| Broker's IdP credential | a private key as above, or the IdP's client secret | Broker only. |
| Refresh token key | `openssl rand -base64 32 > refresh-token.key` | Broker only. |
| Console key pair | `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out ui-client.pem` and `openssl pkey -in ui-client.pem -pubout -out ui-client.pub.pem` | Private half: console. Public half: broker (`[clients.ui] public_key_file`). |
| Console cookie key | `openssl rand -base64 32 > ui-cookie.key` | Console only. |
| TLS certificate and key | your CA | Whichever listener serves TLS. |

## Upstream IdP recipes

### What the broker needs from an Upstream IdP

- **Discovery.** `<issuer>/.well-known/openid-configuration` whose `issuer`
  equals `[upstream] issuer` exactly, and whose `authorization_endpoint`,
  `token_endpoint`, `jwks_uri` and (if any) `userinfo_endpoint` are https.
  The broker makes no request at startup; it discovers on first use and
  retries until discovery succeeds.
- **A publicly trusted certificate.** The broker's HTTP client trusts only
  the public Mozilla root store (webpki roots); no setting adds a CA. An IdP
  behind a private CA needs a TLS-terminating proxy with a public
  certificate in front of it. Plain http is accepted only on loopback.
- **RS256-signed JWTs with a `kid`**, for ID tokens and access tokens, under
  keys the `jwks_uri` publishes. Opaque access tokens are refused.
- **For people:** an OIDC confidential client for the broker with redirect
  URI `<broker issuer>/callback`, the authorization code grant with PKCE
  S256, and refresh tokens. The ID token must carry the broker's client id
  in `aud` (and in `azp`, when present), the `nonce` the broker sent, the
  `[upstream] subject_claim`, and the claims the person rules read. The code
  exchange must return a refresh token, or sign-in is refused
  (`upstream_missing_refresh_token`). On refresh the IdP should return a
  fresh ID token; otherwise the broker calls `userinfo` with the new access
  token, which must return the subject and the rule claims.
- **For machines:** one app registration per CI pipeline with a client
  secret the IdP accepts as HTTP Basic (`client_secret_basic`): the dbt
  client sends Basic and the broker forwards that header unchanged. The
  `client_credentials` grant must return an RS256 JWT access token with
  `iss` = `[upstream] issuer`, `aud` holding `[upstream] api_audience`, the
  subject claim, and the claims the machine rules read.
- **The broker's own credential** at the token endpoint: HTTP Basic client
  secret, or `private_key_jwt` with an RS256 assertion (optional `kid`).

When a person or machine is refused, the broker's `sign-in refused` or
`token request refused` log line names the reason. These are the usual
configuration mistakes:

| Log `reason` | Usual cause |
|---|---|
| `no_matching_rule` with `missing_claims` | The IdP leaves the claim out: no mapper, no groups claim configured, or Entra ID group overage. |
| `no_matching_rule` without `missing_claims` | The claim is there, but no rule's value matches (for example a full group path `/data-eng`, or a group name where the IdP sends an object id). |
| `upstream_token_wrong_audience` | The machine's access token is for another API, or lacks the audience mapper. |
| `upstream_token_wrong_issuer` | The token's `iss` differs from `[upstream] issuer` (Entra v1.0 tokens; a Keycloak hostname mismatch). |
| `upstream_token_not_a_signed_jwt` | The IdP issued an opaque access token. |
| `upstream_token_missing_subject` | The token lacks `[upstream] subject_claim` (Entra `oid` without the `profile` scope). |
| `upstream_rejected_client` | The IdP refused the broker's own credential. |
| `upstream_missing_refresh_token` | No refresh token at sign-in (`offline_access` not requested or not allowed). |
| `upstream_unavailable` | Discovery, keys or the token endpoint unreachable, failing, or plain http off loopback. |

### Keycloak

Tested by the opt-in suite against Keycloak 26.8.2
([`crates/harness/tests/keycloak.rs`](../crates/harness/tests/keycloak.rs);
the realm and every lesson are in
[`crates/harness/keycloak/README.md`](../crates/harness/keycloak/README.md)).
Config: [`examples/auth/keycloak.toml`](../examples/auth/keycloak.toml).

1. **Issuer.** `[upstream] issuer = "https://<host>/realms/<realm>"`.
   Keycloak builds the issuer from the request's host, so set Keycloak's
   `hostname` option: the broker and every CI runner must reach Keycloak
   under the same name.
2. **Broker client.** A confidential OpenID Connect client with only the
   standard flow enabled, redirect URI exactly `<broker issuer>/callback`,
   and the advanced setting "Proof Key for Code Exchange Code Challenge
   Method" `S256`. Client authenticator "Client Id and Secret"
   (`client_secret_file`) or "Signed JWT" with RS256 and the broker's public
   key as a JWKS (`private_key_file`; no `private_key_id` is needed when the
   client has one key).
3. **Groups claim.** Add a "Group Membership" mapper to the broker client
   (or a default client scope of it) with token claim name `groups`, "Full
   group path" off, and "Add to ID token" and "Add to userinfo" on. Rules
   match the group name: `claim = "groups"`, `value = "data-eng"`. Realm
   roles live in the nested `realm_access.roles`, which no rule can match;
   map them to a top-level claim with a "User Realm Role" mapper if rules
   should use roles. Keycloak omits `groups` for a person in no group, and
   the refusal then logs `missing_claims="groups"`.
4. **Refresh tokens.** The default `login_scope` asks for `offline_access`.
   Keycloak then issues an offline refresh token, bounded by the realm's
   "Offline Session Idle" (30 days by default) rather than "SSO Session
   Idle" (30 minutes by default). Only people with the `offline_access`
   realm role (in the realm's default roles) can sign in; others get
   `not_allowed` at the code exchange. Keycloak returns a fresh ID token on
   every refresh, so a group change or a disabled person applies at the next
   renewal.
5. **CI clients.** Each pipeline is a confidential client with only
   "Service account roles" on. Add an "Audience" mapper (included custom
   audience = `[upstream] api_audience`, "Add to access token" on) to each
   CI client or to a default client scope of them. Machine rules use
   `claim = "azp"` (the client id). The Principal's subject is the service
   account's user id. Leave `[upstream] client_credentials_scope` unset:
   Keycloak answers a scope the client was not given with `invalid_scope`.
6. **Subject.** The default `subject_claim = "sub"` is Keycloak's user id,
   stable across username and email changes.

A realm import that lists `clientScopes` loses Keycloak's built-in client
scopes; put mappers on the clients instead (README "Realm import").

### Entra ID

From Microsoft's documentation; not run by any test here. Config:
[`examples/auth/entra-id.toml`](../examples/auth/entra-id.toml).

1. **Issuer.** `https://login.microsoftonline.com/<tenant id>/v2.0`, with the
   tenant id GUID. Discovery for a tenant-specific URL returns the issuer
   with the GUID, so a domain name in `[upstream] issuer` does not match.
   ([Validate the issuer](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#validate-the-issuer).)
2. **Broker app registration.** Platform "Web", redirect URI
   `<broker issuer>/callback`. Add a client secret and mount it as
   `client_secret_file`. Grant admin consent for the delegated permissions
   `openid`, `profile`, `email` and `offline_access`, so people see no
   consent prompt. The default `login_scope` requests exactly these;
   `profile` is what puts `oid` in the ID token.
3. **Subject.** Set `subject_claim = "oid"`: it is the same for a user in
   every app of the tenant, and for a machine it is its service principal's
   object id. Entra's `sub` is pairwise per application
   ([ID token claims](https://learn.microsoft.com/en-us/entra/identity-platform/id-token-claims-reference)).
4. **People's access: app roles (recommended).** Define app roles on the
   broker's registration (allowed member types: users/groups), for example
   `dbt-state.analytics.developer`, and assign users or groups to them under
   Enterprise applications. The ID token then carries them in `roles`;
   rules use `claim = "roles"`
   ([app roles](https://learn.microsoft.com/en-us/entra/identity-platform/howto-add-app-roles-in-apps)).
5. **Or groups.** Under Token configuration, add a groups claim for the ID
   token. Values are group object ids, so a rule's `value` is the object id.
   A person in more than 200 groups gets no `groups` claim (group overage:
   Entra sends a pointer to Microsoft Graph instead), and the broker, which
   calls no IdP-specific API, refuses them with `no_matching_rule` and logs
   `missing_claims="groups"`. Avoid overage with app roles, or by emitting
   only groups assigned to the application (`groupMembershipClaims =
   "ApplicationGroup"`)
   ([group claims](https://learn.microsoft.com/en-us/entra/identity/hybrid/connect/how-to-connect-fed-group-claims),
   [access token claims](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference)).
6. **dbt State API registration (for CI).** A separate app registration:
   Expose an API with an Application ID URI (for example `api://dbt-state`),
   define app roles with allowed member type Applications (for example
   `dbt-state.analytics.ci`), and set `requestedAccessTokenVersion` to `2` in
   its manifest. With the default (`null`), Entra issues v1.0 access tokens,
   whose `iss` is `https://sts.windows.net/<tenant id>/` and which the
   broker refuses (`upstream_token_wrong_issuer`). In v2.0 tokens `aud` is
   the API's Application (client) ID, so set `api_audience` to that GUID, and
   `client_credentials_scope = "api://dbt-state/.default"`
   ([access token versions](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#token-formats),
   [client credentials](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow)).
7. **CI app registrations.** One per pipeline, with a client secret. Under
   API permissions, add the dbt State API's application permission (the app
   role) and grant admin consent. Its tokens carry the role in `roles` and
   its client id in `azp`; rules use either.

Two points are unverified against Entra ID:

- **`private_key_jwt` for the broker.** Entra documents an `x5t#S256`
  certificate thumbprint in the assertion header
  ([certificate credentials](https://learn.microsoft.com/en-us/entra/identity-platform/certificate-credentials)).
  The broker sends `alg`, `typ` and optionally `kid` only, so use a client
  secret with Entra ID.
- **The refreshed ID token.** Microsoft's authorization code flow page
  describes the `id_token` of a refresh response as unsigned
  ([refresh the access token](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow#refresh-the-access-token)).
  The broker requires a signed one, and an untrusted refreshed ID token
  ends the session (`upstream_token_*`). Test a renewal in a test tenant
  before rollout: sign in, wait for the token to come within 5 minutes of
  expiry (or use a 15-minute `person_lifetime_minutes`), and run dbt again.

Conditional Access sign-in frequency and Entra's "revoke sessions" both make
the next refresh fail, which ends the broker session; the person then signs
in again.

### Okta

From Okta's documentation; not run by any test here. Config:
[`examples/auth/okta.toml`](../examples/auth/okta.toml).

1. **A custom authorization server.** The org authorization server
   (`https://<your Okta domain>`) issues access tokens only for Okta's own
   APIs, which you cannot validate and whose audience you cannot set, so
   `client_credentials` cannot work against it. Use a custom authorization
   server (Okta's API Access Management; orgs that have it come with one
   named `default`). `[upstream] issuer =
   "https://<your Okta domain>/oauth2/<authorization server id>"`, and
   `api_audience` = that server's Audience
   ([authorization servers](https://developer.okta.com/docs/concepts/auth-servers/)).
2. **Broker app integration.** An OIDC Web Application with grant types
   Authorization Code and Refresh Token, sign-in redirect URI
   `<broker issuer>/callback`. Client authentication: "Public key / Private
   key" with the broker's public key (`private_key_file`, and
   `private_key_id` set to the key's id in Okta), or a client secret
   (`client_secret_file`). Okta's assertion `aud` is the token endpoint
   URL, which is what the broker sends
   ([client authentication](https://developer.okta.com/docs/api/openapi/okta-oauth/guides/client-auth/)).
3. **Refresh tokens.** The default `login_scope` requests `offline_access`,
   which Okta requires on the authorize request for a refresh token. Allow
   the Refresh Token grant in the authorization server's access policy
   rule; its refresh token lifetime and idle settings decide how long a
   person stays signed in between runs, up to the broker's 12-hour cap.
   With `openid` in the scope, Okta returns a fresh ID token on refresh, so
   group changes apply at the next renewal. Web apps keep a persistent
   refresh token by default; the broker handles that and rotation alike
   ([refresh tokens](https://developer.okta.com/docs/guides/refresh-tokens/main/)).
4. **Groups claim.** On the authorization server's Claims tab, add a claim
   named `groups`, included in the ID token "Always", value type Groups,
   with a filter that matches your dbt State groups. Rules match group
   names. Okta fails the request when more than 100 groups match the
   filter, so keep the filter narrow
   ([groups claim](https://developer.okta.com/docs/guides/customize-tokens-groups-claim/main/),
   [ID token claims](https://developer.okta.com/docs/api/openapi/okta-oauth/guides/overview/)).
5. **Subject.** The default `subject_claim = "sub"` is a person's Okta user
   id (`00u…`).
6. **CI.** One API Services app integration per pipeline, with a client
   secret. Client Credentials cannot request OpenID scopes, so define a
   custom scope on the authorization server (for example `dbt_state.ci`),
   allow the Client Credentials grant and that scope in an access policy
   rule, and set `client_credentials_scope` to it. Okta puts the client id
   in `cid`; machine rules use `claim = "cid"`
   ([client credentials](https://developer.okta.com/docs/guides/implement-grant-type/clientcreds/main/)).
   Check a decoded token from your org for the claim your
   `subject_claim` names before you go live.

## TLS, HTTP/2 and load balancers

### What the clients require

- The dbt client sends credentials only over TLS. It uses TLS when the API
  URL starts with `https://` (Fusion) or ends in `:443`, or when
  `RUN_CACHE_API_SECURE=true`. Over a plain channel it sends no token and no
  `x-organization-id`, and an `oidc` server refuses every RPC.
- `RUN_CACHE_AUTH_URL` and `RUN_CACHE_TOKEN_URL` must be https.
- **Pinned Fusion's token requests trust only the public Mozilla roots
  built into it**, and no setting adds a CA (`dbt-fusion@9977b6c`, found by
  the pinned Fusion driver, #223). The broker's token URL must therefore
  present a certificate from a publicly trusted CA, at the broker or at a
  TLS-terminating proxy. Fusion's gRPC channel uses the host's roots, so a
  private CA works there (`SSL_CERT_FILE`). Whether an official dbt release
  is built the same way is unknown.
- The pinned Python client trusts a private CA for gRPC through
  `RUN_CACHE_CA_BUNDLE`, and for token requests (Python `requests`) through
  `REQUESTS_CA_BUNDLE`.
- The broker, a gRPC server fetching the broker's keys, and the console
  trust only the public Mozilla roots for their own outbound requests (to
  the Upstream IdP, `[broker] jwks_url`, `DBT_STATE_UI_BROKER_URL`).

### Native TLS or a TLS-terminating proxy

Both listeners serve TLS (rustls) from mounted PEM files
(`DBT_STATE_TLS_*`, `DBT_STATE_BROKER_TLS_*`), or plain HTTP behind a proxy
that terminates TLS. Plain HTTP on a non-loopback address is allowed in
`oidc` mode for that reason; keep that hop inside a network you trust,
since tokens cross it. A renewed certificate applies on restart. A TLS
handshake that takes longer than 10 s is dropped.

### gRPC needs HTTP/2 end to end

- The gRPC listener speaks only HTTP/2: over TLS it negotiates `h2` by
  ALPN, and in plain text it expects HTTP/2 with prior knowledge (h2c). A
  proxy that forwards HTTP/1.1 to it fails every call.
- Configure the load balancer to forward gRPC as HTTP/2. On an AWS
  Application Load Balancer that is a target group with protocol version
  `gRPC` (HTTPS listener only)
  ([target group protocol version](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html#target-group-protocol-version));
  on nginx, `grpc_pass`; on Envoy, an HTTP/2 upstream.
- Health check `/grpc.health.v1.Health/Check`. It needs no token and always
  reports `SERVING`; it does not check Postgres.

### The broker is HTTP/1.1

- Its TLS listener offers only `http/1.1` by ALPN. Route it as an ordinary
  HTTPS site, on its own host name or listener: `[broker] issuer` must be
  exactly the URL browsers, dbt clients and the console reach, because the
  broker builds `<issuer>/callback`, `<issuer>/token` and its discovery
  document from it.
- Behind a proxy, set `[broker] client_ip_header` (for example
  `x-forwarded-for`, whose last entry is the address the proxy saw) so the
  pending sign-in cap counts clients rather than the proxy. Set it only if
  the proxy sets that header on every request.
- Allow request timeouts of at least 60 s on `/token` and `/callback`: each
  Upstream IdP call can take up to 10 s, and a renewal can wait for a
  concurrent renewal of the same session.
- A health check can use `GET /.well-known/openid-configuration` or
  `GET /jwks`; neither touches Postgres or the Upstream IdP.
- The broker limits only pending sign-ins per IP. Rate-limit `/`,
  `/callback` and `/token` at the ingress.

## Pointing dbt clients at the deployment

Both pinned clients read these settings. The Python client also accepts
each `RUN_CACHE_` name with a `DBT_ENGINE_STATE_` prefix (which wins) or a
`DBT_RUN_CACHE_` prefix.

| Setting | Value |
|---|---|
| `RUN_CACHE_API_URL` | The gRPC address, `state.example.com:443`. |
| `RUN_CACHE_API_SECURE` | `true` (implied by `:443`). |
| `RUN_CACHE_AUTH_URL` | `[broker] issuer`, for example `https://auth.state.example.com`. The client uses it verbatim as the authorize endpoint. |
| `RUN_CACHE_TOKEN_URL` | `<broker issuer>/token`. |
| `DBT_ENGINE_STATE_OAUTH_CLIENT_ID`, `DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET` | CI only: the pipeline's Upstream IdP client id and secret. Also `RUN_CACHE_OAUTH_CLIENT_ID` and `RUN_CACHE_OAUTH_CLIENT_SECRET`. People set neither. |
| `RUN_CACHE_ORG_ID` | The Organization id, when the Principal has more than one active Organization. |
| `DBT_ENGINE_MANAGE_STATE=true` | Fusion: enables dbt State, as against hosted. |

Sources: `dbt-state==2.62.6` `dbt_state/config.py`, `auth/sso.py` and
`grpc/client.py`; `dbt-fusion@9977b6c` `crates/dbt-run-cache/src/service_config.rs`
and `auth/`; the [hosted auth report](reports/hosted-auth-2026-11-21.md).

**People** set no client id or secret. The first run opens a browser
(Fusion through `xdg-open`, `open` and the like; Python through its URL
opener) and listens on `http://127.0.0.1:29525/handler`, so port 29525 must
be free on the person's machine. The token is cached at
`~/.dbt/state_auth.json`; `DBT_ENGINE_STATE_HOME` names another directory
(the Python client also reads `RUN_CACHE_HOME`).

**CI** sets the client id and secret of the pipeline's Upstream IdP app
registration. A configured secret always selects `client_credentials`, and
no browser is involved. In CI or without a terminal, the Python client keeps
tokens in memory only.

**Organization.** Without a configured Organization, the token must hold
exactly one active Organization; otherwise the client stops with an error
listing them. Set `RUN_CACHE_ORG_ID`, or `state-org-id` in the `dbt-cloud`
block of `dbt_project.yml` (Python 2.62.6; current `dbt-labs/dbt` also reads
`DBT_CLOUD_STATE_ORG_ID`). A configured Organization the token lists only
as disabled produces the client's "access to the requested organization …
has been disabled" message.

**dbt platform credentials break sign-in.** When no client secret is set,
both clients use any dbt platform credential they find
(`DBT_CLOUD_TOKEN` with `DBT_CLOUD_ACCOUNT_HOST`, `~/.dbt/dbt_cloud.yml`,
and for Fusion a `dbt login` session) for token exchange, and never fall
back to the browser. The broker refuses token exchange
(`unsupported_grant_type`, `token_exchange_unsupported`), and the run fails.
People must remove those credentials from the environment that runs dbt
against this deployment.

**Headless people.** The Python client opens no browser in CI (a `CI` or
similar variable) or without a terminal; current `dbt-labs/dbt` fails fast
without one. Pipelines use machine credentials.

**Failures.** A 4xx from the token endpoint fails the dbt run. A gRPC
`UNAUTHENTICATED` or `PERMISSION_DENIED` makes the client disable dbt State
for the rest of the process and build every node normally, so an auth
problem costs warehouse time, not correctness. Current `dbt-labs/dbt`
retries only `UNAVAILABLE`
([hosted auth report](reports/hosted-auth-2026-11-21.md#4-how-the-client-authenticates)).

## When people sign in again

| Situation | Python `dbt-state` 2.62.6 | Fusion `9977b6c` |
|---|---|---|
| First run on a machine | Browser | Browser |
| A later invocation, session still active | No prompt: refreshes from the token on disk when it has 5 minutes or less left | Same |
| One long run | No prompt: refreshes in memory | Reads the token on disk once per process. Once that token is within 5 minutes of expiry, it signs in through the browser again. A run longer than the remaining lifetime of the token it started with, minus 5 minutes, prompts: with the 60-minute default and a just-renewed token, runs up to about 55 minutes. |
| The broker session ends | Browser at the next refresh | Same |

A broker session ends, and the next refresh sends the person to the browser,
when:

- it reaches its 12-hour cap (counted from sign-in; renewals do not extend
  it), so a daily user signs in about once a day;
- an operator revokes it, or a refresh token or code is replayed;
- the Upstream IdP refuses the refresh: the person was disabled, their IdP
  session or refresh token expired (Keycloak's offline session idle, Okta's
  refresh token policy, Entra's sign-in frequency or a session revocation);
- the person no longer matches any person rule, or the IdP's evidence names
  another subject;
- the config changes `[upstream] issuer` (`upstream_issuer_changed`),
  `[upstream] subject_claim` (`upstream_subject_mismatch`; every Principal
  also changes), or `refresh_token_key_file` (`upstream_refresh_token_unreadable`).

When the broker cannot reach the Upstream IdP, it answers 503 and keeps the
session (a deviation from spec #215, which ended the session on any failed
upstream refresh). Both pinned clients, however, discard their stored token
when the refresh of an expired token fails, and then open the browser.

The console signs people in again when its broker session ends, after 12
hours, and after any failed renewal, including an unreachable broker or IdP
(spec #215 did not specify this). Changing `DBT_STATE_UI_COOKIE_KEY_FILE`
signs everyone out of the console.

A browser sign-in while the person's IdP session is still active usually
returns without a password prompt. Restarting or redeploying the server, the
broker or the console signs nobody out.

## Revocation bounds

How long access survives each kind of revocation, as implemented. Test paths
are under `crates/`; `harness/tests/keycloak.rs` runs only with
`DBT_STATE_KEYCLOAK=1`.

| Event | Effect on new tokens | Effect on gRPC access | Verified by |
|---|---|---|---|
| Person disabled, or removed from the mapped groups or app roles, at the Upstream IdP | The next renewal is denied and ends the session (`upstream_refused_grant`, `no_matching_rule`). A new sign-in fails at the IdP, or finds no matching rule. | The token they hold works until `exp` plus 60 s skew. A renewal through the 60 s grace window reuses the previous renewal's evidence, so access ends at most person token lifetime + 2 minutes after the change (default 62 minutes). | `harness/tests/person_renewal.rs::renewal_is_denied_and_the_session_ends_without_fresh_proof_of_the_same_mapped_person`, `harness/tests/keycloak.rs::renewal_rechecks_the_person_at_keycloak_and_a_disabled_person_is_denied`, `harness/tests/keycloak.rs::a_person_removed_from_the_mapped_group_is_denied_renewal`, `harness/tests/person_login.rs::only_person_rules_apply_and_unmapped_people_are_denied`, `harness/tests/shared_logins.rs::a_retry_after_a_lost_response_within_the_window_succeeds`, `harness/tests/person_login.rs::the_person_token_lifetime_is_configurable`, `harness/tests/oidc.rs::token_problems_are_unauthenticated_with_fixed_messages` |
| A person's Role or Organizations change at the Upstream IdP | The next renewal carries the new grant | The old grant until `exp` plus 60 s | `harness/tests/person_renewal.rs::role_and_organization_changes_at_the_idp_take_effect_on_renewal` |
| Upstream IdP unreachable | No new tokens (503); sessions are kept | Unchanged: tokens expire on schedule, so an outage never extends access | `harness/tests/person_renewal.rs::renewal_waits_out_an_unavailable_idp_without_ending_the_session` |
| Operator runs `revoke-sessions` | No session of the Principal renews, through the grace window or not (`session_revoked`) | Person tokens refused on every replica within 30 s (session status cache) | `harness/tests/session_revocation.rs::revoking_a_principal_s_sessions_refuses_their_rpcs_within_30_seconds`, `harness/tests/session_revocation.rs::a_revoked_session_issues_no_more_tokens`, `harness/tests/shared_logins.rs::a_revoked_session_does_not_renew_through_the_window`, `server/tests/admin.rs::revoke_sessions_revokes_every_session_of_a_principal` |
| Person session reaches its 12-hour cap | Renewal denied (`session_expired`) | Refused within 30 s of the cap, even when the token's `exp` is later. Spec #215 bounded this by token expiry. | `harness/tests/person_renewal.rs::renewal_is_denied_once_the_session_reaches_the_12_hour_cap`, `harness/tests/session_revocation.rs::a_person_token_is_refused_once_its_session_reaches_the_12_hour_cap` |
| A rotated refresh token presented 60 s or more after rotation (replay) | The whole session is revoked | As for `revoke-sessions`: within 30 s | `harness/tests/shared_logins.rs::a_rotated_refresh_token_presented_after_the_window_revokes_the_session`, `harness/tests/person_renewal.rs::a_rotated_refresh_token_presented_again_revokes_the_session` |
| An authorization code presented twice | The session it started is revoked | As for `revoke-sessions` | `harness/tests/person_login.rs::an_authorization_code_is_single_use` |
| Machine app disabled, its secret rotated, or its role removed, at the Upstream IdP | The next `client_credentials` is refused: 401 `invalid_client` when the IdP rejects the credentials, otherwise 400 `unauthorized_client` (including `no_matching_rule`) | Until `exp` plus 60 s: at most machine token lifetime + 60 s (default 16 minutes). Machines have no session; `revoke-sessions` does not affect them. | `harness/tests/broker.rs::bad_client_credentials_are_invalid_client`, `harness/tests/broker.rs::an_unmapped_machine_is_refused`, `harness/tests/keycloak.rs::a_ci_client_signs_in_with_client_credentials_passed_through_to_keycloak`, `harness/tests/broker.rs::the_machine_token_lifetime_is_configurable`, `harness/tests/oidc.rs::token_problems_are_unauthenticated_with_fixed_messages` |
| Organization disabled in config | Each broker replica issues it only as a disabled (`app`) entry once it restarts with the config | Each gRPC replica refuses it on every request once restarted (`PERMISSION_DENIED`), including for tokens issued before | `harness/tests/oidc.rs::organization_problems_are_permission_denied`, `auth/tests/mapping_proptest.rs::a_disabled_organization_is_granted_only_as_disabled` |
| Organization removed from config | Remove its rules too: config load refuses a rule naming an undeclared Organization | `PERMISSION_DENIED` on each replica once restarted | `harness/tests/oidc.rs::organization_problems_are_permission_denied`, `auth/tests/config.rs::invalid_upstream_and_rules_are_refused` |
| Console user disabled or removed from groups at the IdP | The console's renewal, on the first request with less than 5 minutes of the token left, is denied | The console session ends at that request: within the person token lifetime | `harness/tests/ui_sign_in.rs::the_console_renews_at_the_broker_within_the_person_token_lifetime`, `harness/tests/ui_sign_in.rs::a_failed_renewal_ends_the_console_session` |
| Console user's sessions revoked | | The console session ends within 30 s | `harness/tests/ui_sign_in.rs::a_revoked_session_ends_the_console_session_within_30_seconds` |
| Organization disabled, for the console | | Refused once the console restarts with the config | `harness/tests/ui_sign_in.rs::an_organization_disabled_in_config_is_refused_after_the_console_restarts` |

The 30 s bound is `dbt_state_broker::SESSION_STATUS_MAX_AGE`, measured from
when the cached read began; the 60 s skew and grace window are fixed. A
console token never authorizes gRPC
(`harness/tests/ui_client.rs::ui_tokens_are_refused_by_every_rpc`).

The bound for a person disabled at the IdP follows from the person token
lifetime. Lowering `person_lifetime_minutes` tightens it at the cost of
Fusion prompts in long runs ([above](#when-people-sign-in-again)).

## Offboarding and incidents

- **A person leaves.** Disable them at the Upstream IdP. Their next
  renewal fails and ends the session; their current token lasts at most the
  person token lifetime + 2 minutes. To cut access within 30 s, also run
  `revoke-sessions`.
- **A person's laptop is lost, or a token leaks.** Run `revoke-sessions`.
  It does not stop a new sign-in; disable the person at the IdP (or reset
  their credentials there) to keep them out.
- **A CI secret leaks.** Rotate or delete the secret at the Upstream IdP.
  Tokens already issued last at most the machine token lifetime + 60 s.
- **An Organization must be cut off.** Set `disabled = true` and roll every
  broker, gRPC server and console replica.

`revoke-sessions` takes the Upstream IdP issuer and the person's value of
`subject_claim`, which the broker's `token issued` and `sign-in completed`
logs show as `subject`, or which `principals` holds:

```bash
psql "$DATABASE_URL" -c "SELECT issuer, subject, id FROM principals WHERE email = 'alice@example.com'"
DATABASE_URL=postgres://... dbt-state-server revoke-sessions \
  --issuer https://login.microsoftonline.com/<tenant id>/v2.0 --subject <oid>
# revoked 2 sessions of Principal Zt9...
```

It reads only `DATABASE_URL`, prints the result on stdout, logs `person
sessions revoked` on stderr, and fails when no Principal with that issuer
and subject is known.

## Rotating keys and secrets

- **Signing keys** (no downtime). Add the new key to `[[signing.keys]]`
  everywhere and roll every replica, keeping the old key active, so every
  replica verifies it and the brokers publish it. Wait at least 5 minutes,
  so gRPC servers that fetch the broker's keys have reloaded them. Then set
  `active_key_id` to the new key and roll again. After the longest token
  lifetime plus 60 s, remove the old key and roll a third time. Removing a
  key early makes gRPC refuse the tokens it signed (`invalid bearer token
  signature`); the clients then build without dbt State until their token
  comes within 5 minutes of expiry.
- **The broker's Upstream IdP credential.** Add the new secret or key at
  the IdP, point the file at it, roll the brokers, then remove the old one
  at the IdP.
- **The refresh token key** cannot be rotated: replacing it ends every
  person session at its next renewal (`upstream_refresh_token_unreadable`).
- **The console key pair** has one registered public key, so a change
  needs the broker and the console restarted together; console sign-ins
  fail in between.
- **The console cookie key**: replacing it signs everyone out of the
  console.
- **TLS certificates** are read at startup; roll the replicas.

## Logging and audit

All processes log as text to stdout (the admin command to stderr), filtered
by `RUST_LOG` (default `info`). Token values, refresh tokens, codes, client
secrets and keys are never logged.

| Message | Level | Process | Fields (when known) |
|---|---|---|---|
| `gRPC request refused` | warn | gRPC server | `rpc`, `reason`, `principal`, `client_id`, `session`, `organization` |
| `token issued` | info | broker | `grant`, `principal`, `kind`, `subject`, `client_id`, `scope`, `expires_in`; people also `session`; renewals also `evidence` (`id_token`, `userinfo` or `recent_renewal`) and `grace_window` |
| `token request refused` | warn | broker | `grant`, `reason`, `principal`, `subject`, `client_id`, `upstream` (the IdP's error), `missing_claims`; code exchanges and renewals also `session`, renewals `session_ended` |
| `sign-in completed` | info | broker | `principal`, `kind`, `subject`, `client_id`, `session`, `scope` |
| `sign-in refused` | warn | broker | `step`, `reason`, `principal`, `subject`, `client_id`, `upstream`, `missing_claims` |
| `person sessions revoked` | info | admin command | `issuer`, `subject`, `principal`, `sessions` |
| `console sign-in completed`, `console session renewed` | info | console | `principal` (and `client_id`) |
| `console sign-in refused`, `console session ended`, `console request refused`, `console Organization switch refused`, `the Token broker refused the console's token request` | warn or info | console | `reason`, `principal`, or the broker's `status` and `error` |

The gRPC `reason` is one of `missing_token`, `malformed_token`,
`bad_signature`, `wrong_issuer`, `wrong_audience`, `expired`,
`session_unknown`, `session_revoked`, `session_expired`,
`session_status_unavailable`, `missing_organization`,
`unknown_organization`, `disabled_organization` or
`organization_not_granted`. The client sees only a fixed message
([protocol.md](protocol.md#authentication-oidc-mode)). The broker's reasons
are listed with its error responses
([protocol.md](protocol.md#token-broker-cratesbroker)).

`principal` is the opaque id the broker puts in `sub`: a hash of the
Upstream IdP issuer and subject. In Postgres:

- `principals` maps it to `issuer`, `subject`, `kind` and the last known
  `email`, with `first_seen_at` and `last_seen_at`;
- every decision (`node_decisions.principal_id`) and every execution row a
  Submit, RecordExecutions or RegisterClone writes (`executions.principal_id`)
  records the Principal of the request. Rows written before the upgrade,
  and rows written in `none` mode, have none.

## No-auth mode

`DBT_STATE_AUTH=none` (the default) is for local and DuckDB work:

- No tokens. The Organization is `x-organization-id`, or `local` without it.
  The dbt client on a plain channel sends no `x-organization-id`, so its
  requests use `local`. `DBT_STATE_REQUIRE_ORG_HEADER=true` refuses requests
  without the header (`UNAUTHENTICATED`); it checks presence only and
  authenticates nothing.
- The server refuses a non-loopback `DBT_STATE_LISTEN` unless
  `DBT_STATE_ALLOW_INSECURE` is `true`, `1` or `yes`, and then logs a
  warning at startup.
- `DBT_STATE_BROKER_LISTEN` is a startup error.
- The console shows one Organization, `DBT_STATE_UI_ORG_ID` (default
  `local`), without sign-in, and refuses the `oidc`-only variables.
- No decision or execution records a Principal.
- The DuckDB extension supports only this mode ([duckdb.md](duckdb.md)).

## Known limits

Remaining deployment gaps are tracked in
[correctness-handoff.md](correctness-handoff.md#deployment-gaps). In short:

- one Upstream IdP per deployment; no SCIM, SAML, LDAP or IdP-specific APIs
  (so no Entra group overage resolution);
- no hot reload; no refresh token key rotation; one console public key;
- no logout endpoint for the dbt client, and no sign-out in the console;
- a revoked person can sign in again until they are disabled at the IdP;
- `broker_person_sessions` and `broker_refresh_tokens` rows are never
  deleted;
- no rate limiting beyond the pending sign-in cap;
- the broker, a key-fetching gRPC server and the console trust only public
  roots for outbound https, and pinned Fusion's token requests do too;
- dbt platform token exchange is refused;
- hosted dbt State's own status codes and messages for auth failures are
  not characterized.
