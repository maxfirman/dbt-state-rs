# Hosted dbt State authentication and authorization — 2026-11-21

How the hosted service (`api.state.dbt.com`, `auth.state.dbt.com`) authenticates
clients and scopes them to an organization, and what an unmodified dbt client
requires from a self-hosted server. Research only; no server code changed. No
gRPC call was made and no dbt State metering was used. Each finding is marked
**Observed** (read from a primary source) or **Inferred**.

## Sources

| Ref | Source |
|---|---|
| F | `dbt-labs/dbt-fusion` @ `9977b6cbb1b761065536300037560d8e3c037011` (2026-06-04, archived), `crates/dbt-run-cache/src/` and `crates/dbt-platform-auth/src/` |
| D | `dbt-labs/dbt` @ `ef3cbabc4e408a64b99de83278be4ae5543b4d4f` (2026-11-21), `crates/dbt-state/src/` (the renamed client crate) and `CHANGELOG.md` |
| P | `dbt-labs/dbt-state` (Python client) @ `b0f3030c90dd0766f2d1625b3305829d881fc2ae`, `clients/dbt_state/src/dbt_state/` |
| M | Unauthenticated GETs on 2026-11-21: `https://auth.state.dbt.com/.well-known/openid-configuration`, `/jwks`, `/clients`, `/openapi.json`, `/` |
| T | A cached hosted `id_token` in `~/.dbt/state_auth.json`, decoded locally (expired; identity claims redacted) |
| Docs | docs.getdbt.com: [dbt-state-about](https://docs.getdbt.com/docs/deploy/dbt-state-about), [dbt-state-setup](https://docs.getdbt.com/docs/deploy/dbt-state-setup), [dbt-state-cicd](https://docs.getdbt.com/docs/deploy/dbt-state-cicd), [dbt-state-oauth](https://docs.getdbt.com/docs/deploy/dbt-state-oauth), [dbt-state-deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral) |

## 1. Hosted topology

**Observed**
- The authorization server is separate from the gRPC API. Its issuer is
  `https://auth.state.dbt.com`. The OIDC metadata lists the authorization
  endpoint as the root URL, plus `/token`, `/userinfo`, `/device` and `/jwks`.
  It supports `authorization_code` (PKCE `S256` only), `refresh_token`,
  `client_credentials`, `urn:ietf:params:oauth:grant-type:token-exchange` and
  `device_code`, and signs id_tokens with RS256 only. `/jwks` publishes two RSA
  signing keys (M).
- `/openapi.json` is titled "FastAPI" and lists `/login/{provider_name}`,
  `/saml/metadata/{name}`, `/saml/login/{name}`, `/saml/callback/{name}`,
  `/register`, `/invite/{token}`, `/clients` and `/impersonate/undo` (M).
- `GET /` redirects to dbt platform OAuth
  (`us1.dbt.com/register?...scope=identity:read+offline_access&redirect_uri=https://auth.state.dbt.com/handler/dbt`)
  (M).
- `GET /clients` lists the public clients `runcache`
  (`2fd87cd5-69a6-4c5f-9097-747a58f0edf6`, the client's default id) and
  `conway-auth` (M; F `service_config.rs:10`).
- Every dbt State user needs a dbt platform account. The standalone app
  (app.state.dbt.com) and its M2M clients are deprecated (Docs: about, oauth).

**Inferred**
- The authorization server is custom and brokers identity. It federates human
  login to dbt platform OAuth, keeps per-tenant SAML and social-login routes,
  and issues its own tokens. The previous issuer was `auth.conway.fivetran.com`,
  because P `auth/sso.py:527-542` deletes cached tokens from that issuer.

## 2. Token shape

**Observed** (T; the claim list in M is a subset)
- Header: `alg: RS256`, `typ: JWT`, and a `kid` that matches a `/jwks` key.
- Claims: `iss: https://auth.state.dbt.com`, `aud: <client_id>`, `sub`, `iat`,
  `exp` (`exp - iat = 900`, so a 15-minute lifetime), `email`, `name`,
  `picture`, `dbt: true`, `personal: false`, and
  `scope: "runcache:scope:app:act_<id>:developer runcache:scope:org:act_<id>:developer"`.
- Hosted organization ids look like `act_` followed by 27 base62 characters.
- The token response must contain `id_token`. `access_token`, `refresh_token`,
  `scope`, `token_type`, `expires_in` and `expires_at` are optional
  (F `auth/browser_flow.rs` `TokenResponse`). The hosted OpenAPI marks
  `id_token`, `scope` and `expires_in` as required (M).
- The client sends the **id_token** as the Bearer credential, not an
  access_token (F `service_client.rs` `with_auth_metadata`).

## 3. Scope grammar and organization selection

**Observed** (F `auth/scope.rs`; D `auth/scope.rs`; P `query_cache_common/auth.py`)
- Each scope token is `runcache:scope:{org|app}:<ORG_ID>:<level>`. A token with
  fewer than 5 colon-separated parts, or an empty org, is rejected.
- `org` scopes are active organizations. An org that appears only in an `app`
  scope is **disabled**, and the client raises `OrgDisabled`. `*` is a wildcard
  org.
- Clients request the scope `runcache:scope:orgs` (F `browser_flow.rs:205`).
- The client reads `scope` from the id_token payload **without verifying the
  signature** (F `scope.rs::jwt_claims`). It then picks the org:
  - If an org is configured (`RUN_CACHE_ORG_ID`; in D also
    `DBT_CLOUD_STATE_ORG_ID` or `state-org-id` in the `dbt-cloud` block), it
    must be in scope.
  - Otherwise the scope must hold exactly one non-wildcard org.
- The level values observed are `admin` and `developer`. No client interprets
  them.

**Inferred**
- Level enforcement, if any exists, happens on the server. The docs describe
  roles only for the deprecated standalone UI: Owner and Admin see the Usage,
  Users, Billing and Clients tabs; Developer sees Usage only and is the
  default. No source says any gRPC RPC is restricted by level.

## 4. How the client authenticates

**Observed**
- **TLS gates auth.** The client attaches credentials only when
  `config.secure` is true. `secure` defaults to true when the API URL is
  `https://...` or ends in `:443`, and `RUN_CACHE_API_SECURE` overrides it. An
  insecure channel sends no `authorization` and no `x-organization-id`
  (F `service_client.rs` `RunCacheAuth::from_config`; D `service_client.rs:279`).
- **Every endpoint is configurable** (F `service_config.rs`):

  | Setting | Default |
  |---|---|
  | `RUN_CACHE_API_URL` | `api.state.dbt.com:443` |
  | `RUN_CACHE_TOKEN_URL` | `https://auth.state.dbt.com/token` |
  | `RUN_CACHE_AUTH_URL` | `https://auth.state.dbt.com`, used verbatim as the authorize endpoint |
  | `DBT_ENGINE_STATE_OAUTH_CLIENT_ID` / `RUN_CACHE_OAUTH_CLIENT_ID` | `2fd87cd5-…` |
  | `DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET` / `RUN_CACHE_OAUTH_CLIENT_SECRET` | none |
  | `DBT_ENGINE_STATE_HOME` | `~/.dbt`, which holds `state_auth.json` |

- **Grant precedence** (F `oauth.rs::acquire_fresh_token`; unchanged in D):
  1. A configured client secret selects `client_credentials`. The client sends
     HTTP Basic `client_id:secret` with `scope=runcache:scope:orgs`.
  2. Otherwise, a dbt platform credential found by `AuthChain` (`DBT_CLOUD_TOKEN`
     + `DBT_CLOUD_ACCOUNT_HOST` + `DBT_CLOUD_ACCOUNT_ID`, a `dbt login` session,
     or `~/.dbt/dbt_cloud.yml`) selects `token-exchange`. The form sends
     `subject_token_type=dbt`, `subject_token=<platform token>`,
     `dbt_hostname` and `client_id`. If the exchange fails, the client does
     **not** fall back to the browser.
  3. Otherwise, the client runs a browser `authorization_code` flow with PKCE
     S256. The redirect URI is `http://127.0.0.1:29525/handler`, the timeout is
     300 s, and the client validates `state`. D fails fast with
     `NoInteractiveTerminal` when no TTY is attached.
  4. `refresh_token` is used when the stored token carries one.
- **Caching.** The token is reused until 300 s before expiry (F `oauth.rs`
  `TOKEN_REFRESH_WINDOW`). D does not persist re-derivable tokens in headless
  runs, and records `platform_account_id` with the stored token.
- **Headers.** `authorization: Bearer <id_token>` and `x-organization-id: <org>`,
  plus `x-request-id`, `x-session-id`, `x-submitted-at-epoch`,
  `x-system-user-id` and `x-os-name` (F `service_client.rs`; P
  `grpc/interceptors.py`).
- **Reaction to auth errors** (D `service_client.rs:160-170`):
  `PERMISSION_DENIED`, `UNAUTHENTICATED`, `OrgDisabled` and retryable token
  endpoint failures disable dbt State for the rest of the process. dbt then
  runs every node normally and logs "dbt State service disabled: …; executing
  normally". The client never refreshes and retries after `UNAUTHENTICATED`.
  Only `UNAVAILABLE` is retried, up to `RUN_CACHE_API_CLIENT_MAX_ATTEMPTS`
  (default 3).
- **Platform credentials for CI.** CI uses a dbt platform service token with
  Job Runner or higher, or Developer on Enterprise plans (Docs: cicd). Fusion
  treats `dbtu_` tokens as PATs and other tokens as service tokens
  (F `dbt-platform-auth/src/credential.rs:59-61`).

## 5. Unknowns (not answerable offline)

No primary source documents the hosted gRPC responses to:
1. a missing `authorization` header, a malformed or badly signed token, or an
   expired token (status code and message);
2. a valid token whose `x-organization-id` is outside its `org` scopes, or is
   only in an `app` scope;
3. a valid token with no `x-organization-id`;
4. whether `grpc.health.v1` and `ClientValidation.ValidateClientVersion` need a
   token;
5. whether any RPC behaves differently for `developer` and `admin`.

D's tests use `Status::permission_denied("locked")` and
`Status::unauthenticated("locked")` as fixtures, which suggests (but does not
prove) the codes hosted uses for locked accounts.

`state-probe validate` (`docs/harness.md`) creates no State history, so a probe
of cases 1–4 through a variant of the recording proxy would settle them. It
needs the maintainer's go-ahead and a dbt Cloud credential. It cannot settle
case 5 without a second account role.

## 6. Implications for dbt-state-rs

- An unmodified client can authenticate to a self-hosted authorization server:
  set `RUN_CACHE_AUTH_URL`, `RUN_CACHE_TOKEN_URL` and the client id/secret
  variables, and serve gRPC over TLS. No client patch is needed.
- A stock enterprise IdP cannot be the client's token endpoint directly:
  - the client never requests `openid`, so a compliant OIDC provider issues no
    id_token;
  - `client_credentials` normally returns only an access_token;
  - the client requires the proprietary `runcache:scope:*` grammar inside the
    id_token;
  - `subject_token_type=dbt` is not a registered token type.

  Parity therefore needs a dbt State authorization server that brokers to the
  enterprise IdP, as hosted does.
- Users who also hold dbt platform credentials (`dbt_cloud.yml`,
  `DBT_CLOUD_TOKEN`) always take the token-exchange path. A self-hosted token
  endpoint receives their dbt platform token, so it must either implement the
  exchange or reject it with a clear message. It must never log that token.
- Because `PERMISSION_DENIED` and `UNAUTHENTICATED` make dbt build everything
  rather than fail the run, an auth outage costs warehouse spend, not
  correctness. The server should log rejections at a level operators notice.
