# Auth: build or adopt — 2026-11-21

This report follows [hosted-auth-2026-11-21.md](hosted-auth-2026-11-21.md). It
asks which auth server should issue tokens to an unmodified dbt client for a
customer-run (not SaaS) dbt-state-rs deployment. No server code changed.

## The fixed client contract

The contract comes from the dbt client source (`dbt-labs/dbt-fusion` @ `9977b6c`
and `dbt-labs/dbt` @ `ef3cbab`, `auth/`):

1. Every token response, including the one from `client_credentials`, must
   contain an `id_token`.
2. The client never sends `openid` in `scope`. It sends only
   `runcache:scope:orgs`.
3. The id_token must carry a string claim literally named `scope`, holding the
   runcache grammar (`runcache:scope:org:<ORG>:<level>`).
4. Without a `refresh_token`, an expired token sends a human back through the
   browser login (`oauth.rs::get_or_refresh_from_disk`).
5. A user who has a dbt platform credential always takes the
   `subject_token_type=dbt` token-exchange path and never falls back to the
   browser.

## Products tested against the contract

Keycloak was tested live: version 26.8.2 in a disposable container. Every other
product was checked against its documentation and source code at pinned
commits.

| Product | 1 + 2 (machine) | 1 + 2 (human) | 3 (`scope` claim) | 4 (refresh) | 5 (`dbt` exchange) |
|---|---|---|---|---|---|
| Keycloak | **No**, observed | **No**, observed | **No**, observed: `scope` is non-modifiable | Yes, but without an id_token | **No**, observed |
| authentik | Yes | Yes | Yes, through a Python scope mapping | **No**: requires `offline_access` | No |
| Zitadel | No | No | Yes, through Actions | Yes | No |
| Ory Hydra | No | Yes, through our own consent app | Yes, through our own consent app | Yes | No |
| Dex | No | No | No | Yes | No |
| Authelia | No | Not verified | No: `scope` is reserved | Not verified | No |
| Kanidm | No | Yes, through a scope map | Static values per group | Not verified | No |
| Entra ID | No | No | No: `scope` and `scp` are restricted | — | No |
| Okta | No | No | Custom auth server claims | — | No |

### Keycloak evidence

- `id_token` is generated only when the raw `scope` request parameter contains
  `openid` (`OAuth2GrantTypeBase.java:146`, `TokenUtil.java:91`). Making
  `openid` a default client scope did not change this.
- Keycloak refuses a mapper that writes to `scope`. It logs `Claim 'scope' is
  non-modifiable in IDToken` (`OIDCAttributeMapperHelper.java:131-140`).
- Multi-valued group claims serialize as JSON arrays. The client cannot read
  an array.
- Token exchange of `subject_token_type=dbt` fails with `Parameter
  'subject_token' supports access tokens only`.
- Idle memory: 850 MiB unbounded, 588 MiB under a 768 MiB limit. Readiness
  took about 26 s.

### Notes on the other products

- **authentik.** The Python scope mapping lives in authentik's admin
  database, not in our code (`authentik/providers/oauth2/id_token.py`
  L126-172). With no refresh token, every token expiry sends users back
  through the browser.
- **Ory Hydra.** The human flow works only if we write the login/consent app.
  `client_credentials` still issues no id_token.
- **Customer enterprise IdPs.** None of Entra ID, Okta, PingFederate or Google
  issues an id_token from `client_credentials`. Pointing the client straight
  at a customer IdP therefore cannot work for CI.

## Precedents

Self-hosted products with clients they cannot change, or a token shape of
their own, handle it in one of three ways:

- **Embed a small token issuer that federates to the customer IdP:**
  - [Pinniped](https://pinniped.dev/docs/background/architecture/)
  - [Vault](https://developer.hashicorp.com/vault/docs/auth/jwt)
  - [Teleport](https://goteleport.com/docs/reference/architecture/authentication/)
- **Mint their own credential for the clients that cannot do OIDC:**
  [Harbor](https://goharbor.io/docs/main/administration/configure-authentication/oidc-auth/)
  (Docker CLI secrets).
- **Bundle a third-party broker as an optional upstream only:**
  [Argo CD](https://argo-cd.readthedocs.io/en/stable/operator-manual/user-management/)
  (Dex).

## Options

| | A. Bundle an IdP (authentik or Keycloak) | B. Customer IdP directly | C. Thin token broker in dbt-state-rs |
|---|---|---|---|
| Meets the contract | authentik: partly (no refresh, no exchange). Keycloak: only with a Java SPI or a re-minting proxy | No (machine flow) | Yes, by construction |
| Custom code still needed | Yes: a mapping script, or an SPI, or a proxy | — | Yes: the broker |
| Who owns identity (users, MFA, SSO, offboarding) | The bundled IdP, federated to the corporate IdP | The customer IdP | The customer IdP |
| Extra systems the customer runs | One more IdP: admin console, user store, CVE stream, upgrades, HA, backups | None | None beyond the existing server and Postgres |
| Security review story | Well-known product, but it sits as a second IdP behind the corporate one | — | Small and auditable, but ours |
| Cross-organization isolation is decided in | IdP config or mapping scripts | — | Our mapping code, which we can test |

## Recommendation

Choose **C**: a thin broker that adapts the client protocol and owns no
identity.

**We build the protocol adapter only:**
- `/.well-known/openid-configuration` and `/jwks`.
- `/authorize`: exact `redirect_uri` match and mandatory PKCE S256. It
  redirects to the customer IdP as a standard OIDC relying party.
- `/token` handling:
  - `authorization_code`: single-use codes, 60 s.
  - `refresh_token`: rotating and hashed, with family revocation. Each refresh
    re-checks the user at the upstream IdP, so offboarding at the IdP takes
    effect within one token lifetime.
  - `client_credentials`: either passed through to the customer IdP's own
    `client_credentials`, so the IdP owns machine secrets, or checked against
    a local registry of argon2-hashed secrets where the IdP cannot do it.
  - Token exchange: a clear rejection until it is designed.
- Signed 15-minute id_tokens.
- An explicit, deny-by-default mapping from IdP groups or app roles to
  (org, level).

**We do not build:** user accounts, passwords, MFA, recovery, SSO sessions,
SAML/LDAP parsing, SCIM or conditional access. A customer without an OIDC IdP
can put Dex, Keycloak or authentik upstream, following the Argo CD pattern.
The broker needs no extra code for that.

**Implementation:**
- Use reviewed crates: `openidconnect` for the upstream side, and
  `jsonwebtoken` or `josekit` for signing and verification. Rust has no mature
  OAuth authorization-server framework, so the issuing endpoints are assembled
  from these primitives. Follow
  [RFC 9700](https://www.rfc-editor.org/rfc/rfc9700) (the OAuth 2.0 Security
  BCP).
- Ship the broker as its own crate. It should run either in the server
  process or as a separate binary, using the existing Postgres for codes and
  refresh tokens.
- The UI becomes a standard OIDC client of the broker, so group mapping lives
  in one place.

**Risks of C, and their mitigations:**
- **A mapping bug could grant cross-organization access.** Make the mapping a
  pure function and give it property tests; deny by default.
- **Signing keys can leak or need rotation.** Support keys from file or KMS,
  with overlapping `kid`s.
- **The OAuth details are subtle:** code replay, open redirects, CSRF on the
  callback, refresh token reuse. Follow the RFC 9700 checklist, run fuzz and
  negative tests, and get an external review before 1.0.
- **The client contract may drift.** Track the `dbt-labs/dbt` `crates/dbt-state/src/auth/`
  directory.

**Runner-up: authentik.** Choose it only if the requirement becomes "bundle a
known IdP". Users would then get a browser login at every token expiry, and
the platform token exchange would remain unsupported.
