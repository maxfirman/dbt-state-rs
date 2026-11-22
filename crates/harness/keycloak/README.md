# Keycloak test realm

`dbt-state-realm.json` is the realm the opt-in Keycloak suite
(`crates/harness/tests/keycloak.rs`) runs against. `scripts/keycloak.sh start`
imports it into Keycloak 26.8.2 (`quay.io/keycloak/keycloak:26.8.2`) in dev
mode on `http://127.0.0.1:18080`. Every secret and password in it is
test-only.

| Realm object | Purpose |
|---|---|
| client `dbt-state-broker` | the Token broker as a confidential OIDC relying party (client secret); redirect URI `https://broker.dbt-state.test/callback`, PKCE S256 required, group membership mapper `groups` |
| client `dbt-state-broker-jwt` | the same with `private_key_jwt`: its `jwks.string` holds the public key of `crates/auth/testdata/upstream-rp-key.pem` |
| client `dbt-state-ci` | a CI pipeline's app registration: service account, audience mapper `dbt-state-api` |
| client `other-service` | an app registration for another API: audience mapper `other-api` only |
| client `harness-admin` | the harness's user administration: service account with `realm-management` user roles and the `kc-admin-api` scope |
| groups `data-eng`, `marketing` | `data-eng` is mapped by the suite's person rule; `marketing` is not |
| user `alice` / `alice-password` | in `data-eng` |

Other people are created per test through the admin REST API
(`support/keycloak.rs`), so a test that disables a person or changes their
groups never affects another.

## Configuration lessons

Each of these was observed against Keycloak 26.8.2 with this realm; the
suite's test that shows it is named where there is one. They are the inputs
for the operator guide's Keycloak recipe (#231).

**Broker client (browser sign-in)**

- `[upstream] issuer` is the realm URL, `https://<host>/realms/<realm>`.
  Keycloak builds it from the request's host, so the broker, the browser
  and the CI runners must all reach Keycloak under the same hostname (set
  Keycloak's `hostname` option in production).
- Make the broker a confidential client with the standard flow only (no
  direct access grants, no implicit flow), its redirect URI exactly
  `<broker issuer>/callback`, and the advanced setting "Proof Key for Code
  Exchange Code Challenge Method" `S256`. Keycloak then refuses an
  authorization request without `code_challenge_method`.
- Keycloak accepts the broker's client secret by HTTP Basic (client
  authenticator "Client Id and Secret"). `private_key_jwt` also works:
  client authenticator "Signed JWT", signature algorithm RS256, and the
  broker's public key as a JWKS (here the client attributes
  `use.jwks.string` and `jwks.string`; a JWKS URL also works). The broker's
  assertion needs no `kid` (`[upstream] private_key_id`) when the client
  has one key (`the_broker_authenticates_at_keycloak_with_private_key_jwt`).
- `groups` is not in Keycloak's tokens by default, and the broker's rules
  match only top-level claims. Add a "Group Membership" mapper to the
  broker client (or a client scope assigned to it as a default scope) with
  token claim name `groups`, "Full group path" off (otherwise the values
  are `/data-eng`), and "Add to ID token" and "Add to userinfo" on. Rules
  then match the group name: `claim = "groups"`, `value = "data-eng"`.
  Keycloak's realm roles live in the nested `realm_access.roles`, which a
  rule cannot match: map roles to a top-level claim with a "User Realm
  Role" mapper if rules should use them.
- Keycloak leaves the `groups` claim out entirely for a person in no group.
  The broker then refuses sign-in with `no_matching_rule` and logs
  `missing_claims="groups"`; a person only in unmapped groups is refused
  without `missing_claims` (`a_person_in_no_mapped_group_is_refused_sign_in`).
- The default `[upstream] subject_claim = "sub"` is Keycloak's user id (a
  UUID), stable across username and email changes. The broker's `name` is
  Keycloak's "First name Last name" and `email` its email, from the
  default `profile` and `email` scopes.
- The broker's default `login_scope` asks for `offline_access`. Keycloak
  then issues an offline refresh token (`typ` `Offline`), whose lifetime is
  the realm's "Offline Session Idle" (30 days by default) rather than the
  SSO session's ("SSO Session Idle", 30 minutes by default), so a person's
  renewals keep working across idle periods up to the broker's 12-hour
  session cap. Keycloak allows it only to people holding the
  `offline_access` realm role, which the realm's default roles include; a
  person without it cannot sign in at all (Keycloak answers the code
  exchange with `not_allowed`, "Offline tokens not allowed for the user or
  client"). This realm lists `offline_access` among the broker client's
  optional client scopes. With the client's "Full scope allowed" off, no
  role scope mapping for `offline_access` was needed.
- Keycloak returns a fresh ID token on every refresh (no `nonce`), with
  current claims, so the broker's renewal evidence comes from it, not from
  userinfo. A group change in Keycloak applies at the person's next renewal
  (`a_person_removed_from_the_mapped_group_is_denied_renewal`: denied with
  `no_matching_rule`).
- A person disabled in Keycloak is refused at their next renewal: Keycloak
  answers the refresh with HTTP 400 `invalid_grant`, "User disabled", and the broker ends the session with
  `upstream_refused_grant`
  (`renewal_rechecks_the_person_at_keycloak_and_a_disabled_person_is_denied`).
  Their current dbt token stays valid until it expires, as for any
  Upstream IdP.

**CI clients (`client_credentials`)**

- Each CI pipeline is a confidential client with "Service account roles"
  on and every other flow off. Its Principal subject is the service
  account's user id (user `service-account-<client id>`); the client id is
  in `azp` (and `client_id`), so machine rules use `claim = "azp"`.
- Keycloak's `client_credentials` access tokens carry no `aud` for an API
  unless a mapper adds one. Add an "Audience" mapper (included custom
  audience = the configured `[upstream] api_audience`, "Add to access
  token" on) to each CI client, or to a client scope assigned to the CI
  clients as a default scope. A client whose tokens lack that audience is
  refused with `upstream_token_wrong_audience`, even when a machine rule
  names it (`a_keycloak_token_for_another_audience_is_refused`).
- Leave `[upstream] client_credentials_scope` unset. Keycloak answers a
  request for a scope the client was not given with `invalid_scope`, and
  the client's default scopes (with the audience mapper) apply without a
  `scope` parameter.
- A wrong CI secret is HTTP 401 `invalid_client` from Keycloak and from the
  broker
  (`a_ci_client_signs_in_with_client_credentials_passed_through_to_keycloak`).

**Realm import**

- A realm file that lists `clientScopes` gets none of Keycloak's built-in
  client scopes (`basic`, `profile`, `email`, `roles`, `service_account`
  and the others are missing after import; service-account tokens then
  lacked `client_id`). This realm therefore puts its mappers on the
  clients and lists no client scopes.
- Keycloak 26 logs a deprecation warning for clients with "Full scope
  allowed" on; every client here has it off, and `harness-admin` gets its
  `realm-management` roles through a client scope mapping.
- Keycloak 26.8's admin REST API answered 401 to `harness-admin`'s token
  until the client had the `kc-admin-api` scope, so it is one of its
  default scopes.
