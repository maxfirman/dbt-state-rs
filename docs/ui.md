# UI console

A read-only web console for the dbt State server, built with
[Topcoat](https://tokio.rs/blog/2026-07-22-announcing-topcoat) (server-rendered
Rust, no WASM). It shows what dbt State decided across projects and Targets. It
never executes dbt and never writes to the database.

A Target is the dbt profile output name the client sends (`target_name`). The
UI shows it literally rather than inventing a server-side environment model;
the underlying table is still called `environments`.

## Pages

Routes come from the module tree under `crates/ui/src/app/` (`module_router!`).

| Route | Source | Shows |
|---|---|---|
| `/` | `app.rs::home` | Reuse rate, built/reused/cloned totals, recent invocations. |
| `/projects` | `app/projects.rs::index` | Every project with Target counts and totals. |
| `/projects/{project_id}` | `app/projects.rs::project_id::detail` | The project's Targets with dbt State flags and per-Target totals. |
| `/invocations` | `app/invocations.rs::index` | Recent runs. |
| `/invocations/{invocation_id}` | `app/invocations.rs::invocation_id::detail` | Every node decision of a run with its description, hashes, timing and generated copy SQL; `?decision=build\|skip\|clone` filters it. |
| `/environments/{environment_id}` | `app/environments.rs::environment_id::catalog` | Target lineage: the latest decision per node as a server-rendered SVG graph (`dagre-rs`) plus a text node list. |

Sign-in routes (`oidc` mode only; 404 in `none` mode):

| Route | Source | Does |
|---|---|---|
| `GET /auth/login?next=<path>` | `app/auth.rs::login::start` | Starts a sign-in at the Token broker, returning to the local path `next`. |
| `GET /auth/callback` | `app/auth.rs::callback::finish` | The UI's registered callback: completes the sign-in, or explains with fixed text why not. |
| `POST /organization` | `app/organization.rs::switch` | The Organization switcher (form fields `organization`, `csrf`). |

## Running it

The UI reads the database the server writes to.

```bash
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
cargo run -p dbt-state-server                   # captures decisions
HOST=127.0.0.1 PORT=4000 cargo run -p dbt-state-ui
# open http://127.0.0.1:4000
```

`HOST` and `PORT` default to `127.0.0.1` and `3000`. During development,
`topcoat dev` (from `crates/ui`) rebuilds and reloads automatically.

## Organization scope

`DBT_STATE_AUTH` selects the mode, as for the server (`crates/ui/src/settings.rs`).

**`none` (default).** Every UI query is filtered by `DBT_STATE_UI_ORG_ID`
(default `local`). Set it to the Organization ID the gRPC requests use. It is
a single-organization filter, not authentication or tenant selection. The
`oidc`-only variables below are refused, so a console meant to require
sign-in never starts without it.

**`oidc`.** People sign in through the Token broker
([ADR 0012](adr/0012-token-broker-delegates-identity-to-upstream-idp.md)),
and each sees only their own Organizations (`crates/ui/src/sign_in.rs`).
`DBT_STATE_UI_ORG_ID` is a startup error in this mode, like
`DBT_STATE_REQUIRE_ORG_HEADER` for the server: an operator who sets it
would believe it limits the console, while the signed-in person's
Organizations decide instead.

| Variable | Meaning |
|---|---|
| `DBT_STATE_AUTH_CONFIG` | The auth config. It must register the UI (`[clients.ui]`), and its `redirect_uri` must be `<console URL>/auth/callback`. The console reads `[broker] issuer` (and `jwks_url`), `[[organizations]]`, `[clients.ui]` and, if present, `[signing]`. Give it a copy without `[signing]` and `[upstream]` so it never holds the broker's private keys: it then fetches the broker's published keys. |
| `DBT_STATE_UI_PRIVATE_KEY_FILE` | The PEM private key whose public half is `[clients.ui] public_key_file`. The console signs a fresh `private_key_jwt` assertion with it for every token request. Startup checks that the two keys match. |
| `DBT_STATE_UI_PRIVATE_KEY_ID` | Optional `kid` in the assertions' header. |
| `DBT_STATE_UI_COOKIE_KEY_FILE` | Base64 of 32 random bytes (`openssl rand -base64 32`) that encrypt the console's cookies. Every replica needs the same key; changing it signs everyone out. |
| `DBT_STATE_UI_BROKER_URL` | Optional. Where the console reaches the broker's token endpoint, and its `/jwks` without `[signing]` and `jwks_url`. Default `[broker] issuer`. Set it when the console reaches the broker at an internal URL. It must be https, or plain http to a loopback address. Browsers are always sent to the issuer. |

The console's database user also needs to read `broker_person_sessions`.

### Sign-in

A request without a session is redirected (303) to the broker's authorize
endpoint (`<issuer>/`) as the UI's client, with `scope=openid email profile`,
a fresh `state` and `nonce`, and a PKCE S256 challenge. The broker signs the
person in at the Upstream IdP and returns the browser to `/auth/callback`.
The callback then:

1. checks `state` against the pending sign-in, which lives in its own
   cookie for at most 10 minutes and is used once;
2. exchanges the code at the token endpoint with the PKCE verifier and a
   fresh client assertion;
3. verifies the `id_token`: signature by the broker's keys, issuer, `aud`
   and `azp` the UI's client id, expiry with 60 s skew, the nonce, and a
   `sid`.

The verified claims (Principal, session, email, name, `scope`, expiry) and
the refresh token become the session, and the browser returns to the page
it opened. A person the broker denies (no person mapping rule) gets a 403
page with fixed text. A callback without its pending sign-in, for example
one opened in another browser, gets a 400 page and its code is never used.
The access token is not kept: the console calls no API with it.

### Cookies

Both cookies, `__Host-dbt-state-ui-sign-in` (pending sign-in) and
`__Host-dbt-state-ui-session` (12 h `Max-Age`, the broker's session cap), are
encrypted and authenticated with the cookie key (AES-256-GCM, the cookie
crate's private jar). Both are `Secure`, `HttpOnly`, `SameSite=Lax` and
`Path=/`, with no `Domain`. Page scripts cannot read them, and the browser
sends them to no other host.

- **The session is in the cookie, not in Postgres**, so the console stays
  read-only on the database and any replica with the same key serves any
  request. The cost: a session ends only through its broker session (below)
  or the cookie's expiry. There is no sign-out button yet.
- **`SameSite=Lax`, not `Strict`.** The broker's redirect to the callback is
  a cross-site navigation, so a `Strict` pending-sign-in cookie would not
  arrive with it. Neither would a `Strict` session cookie on the redirect
  that follows, or on a link to the console from another site. Lax still
  withholds both cookies from cross-site POSTs.
- **CSRF.** The only state-changing request, the Organization switch, is a
  POST that must carry the session's CSRF token. It also passes Topcoat's
  default origin check, which refuses cross-origin POSTs.
- Signed-in pages are sent with `Cache-Control: no-store`.
- Behind a proxy, serve the console over https. Browsers accept `Secure`
  cookies over plain http only on `localhost` and loopback addresses.

### Each request

Before a page renders (`app.rs::access`):

1. **Session status.** The broker session (`sid`) must be active, checked
   with `dbt_state_broker::SessionCheck` against the shared Postgres, which
   caches each status for at most 30 s. A session revoked with
   `dbt-state-server revoke-sessions`, or past its 12-hour cap, ends the
   console's session within 30 s. If the status cannot be read, the request
   gets 503.
2. **Renewal.** With less than 5 minutes of the person token left, the
   console renews at the broker: the `refresh_token` grant with a fresh
   assertion. The broker re-checks the person at the Upstream IdP, so a
   person disabled there, or removed from their mapped groups, loses the
   console within one person token lifetime (60 minutes by default). The
   renewed token's Principal and session must match the old one's.
3. **Organizations.** The console shows the token's active Organizations
   (`runcache:scope:org:` entries) that its own config also declares
   active. An Organization disabled in config is therefore refused once the
   console restarts with that config, even while a token still lists it.
   With none left, the request gets 403. The selected Organization defaults
   to the first by id, and every query on the page is scoped to it.

A failed renewal or an inactive session clears the session cookie and
redirects to sign in again. The browser comes back to the same page after a
GET, or to the overview after a POST. That includes a broker or Upstream IdP
that is unreachable at renewal time. If the browser never receives a renewal's
response (and so keeps a rotated refresh token), the next renewal after the
broker's 60 s grace window counts as a replay. The broker then revokes the
session, and the person signs in again.

### Organizations and Roles

With more than one Organization, the header shows a switcher listing each one
with the person's Role. `POST /organization` refuses (403) an Organization
outside that list (one not granted, disabled in config, or unknown) and a form
without the session's CSRF token. A successful switch goes to the overview,
since ids on the previous page belong to the previous Organization. Every
page is open to any Role (`developer` or `admin`). An id from another
Organization in a URL shows nothing, because queries are scoped by
Organization, not by id.

### Tests

`crates/harness/tests/ui_sign_in.rs` drives the console as a deployment
runs it (`crates/harness/tests/support/console.rs`: the operator's
environment variables, the test server's schema and broker clock), with the
fake Upstream IdP and the in-process broker. It covers the login redirect
(state, nonce, PKCE), cookie attributes, filtering, switching and its
refusals, renewal, a failed renewal, revocation within 30 s, an Organization
disabled after a restart, signing in with the broker's published keys, and
`none` mode. `crates/ui/tests/startup.rs` runs the binary for the startup
refusals.

## Data

The server's best-effort capture (`crates/server/src/capture.rs`) writes
`organizations`, `projects`, `environments`, `invocations` and
`node_decisions`. The UI reads `projects`, `environments`, `invocations` and
`node_decisions` (`crates/ui/src/db.rs`).

## Known limitation

The UI shows each decision's `decision_description`, not the persisted Decision
reason (`node_decisions.decision_reason`).
