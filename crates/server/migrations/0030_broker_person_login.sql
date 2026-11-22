-- Token broker: browser sign-in of people (#220, spec #215 "Schema").
--
-- The Token broker keeps all its state here, in the server's schema, so an
-- in-process broker and any number of standalone `dbt-state-broker`
-- replicas share it. These tables hold no State history and are not scoped
-- by Organization: a person session may grant several Organizations, and
-- its grant is recomputed from the Upstream IdP's claims on every renewal.
--
-- Secrets never appear in plain text: login `state`, authorization codes and
-- refresh tokens are stored as SHA-256 hashes, and the Upstream IdP's
-- refresh token is sealed with AES-256-GCM under a key from a mounted file
-- (`[upstream] refresh_token_key_file`). All times come from the broker's
-- clock.

-- A browser sign-in between the dbt client's authorize request and the
-- Upstream IdP's callback. Single use; expires after ten minutes.
CREATE TABLE broker_pending_logins (
    -- SHA-256 of the `state` the broker sent to the Upstream IdP.
    state_hash             BYTEA       PRIMARY KEY,
    -- The dbt client's authorize request.
    client_id              TEXT        NOT NULL,
    redirect_uri           TEXT        NOT NULL,
    client_state           TEXT,
    code_challenge         TEXT        NOT NULL,
    -- What the broker sent the Upstream IdP: its ID token must carry the
    -- nonce, and the code exchange the PKCE verifier.
    upstream_nonce         TEXT        NOT NULL,
    upstream_code_verifier TEXT        NOT NULL,
    -- The pending-login cap is per client IP.
    client_ip              TEXT        NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    expires_at             TIMESTAMPTZ NOT NULL
);
CREATE INDEX broker_pending_logins_ip ON broker_pending_logins (client_ip);
CREATE INDEX broker_pending_logins_expiry ON broker_pending_logins (expires_at);

-- A person's broker session: the `sid` of every person token issued in it.
-- Capped at 12 hours from creation. Renewal (#221) re-checks the person at
-- the Upstream IdP with the sealed refresh token, under a row lock on the
-- session (#224), and records when it last did.
CREATE TABLE broker_person_sessions (
    id                     UUID        PRIMARY KEY,
    -- The Principal: the opaque id in token `sub`, and the Upstream IdP
    -- issuer and subject (the configured `subject_claim`) it derives from.
    principal_id           TEXT        NOT NULL,
    upstream_issuer        TEXT        NOT NULL,
    upstream_subject       TEXT        NOT NULL,
    client_id              TEXT        NOT NULL,
    -- The last upstream evidence: the granted `scope` and, for display only,
    -- email and name.
    scope                  TEXT        NOT NULL,
    email                  TEXT,
    name                   TEXT,
    -- The Upstream IdP's refresh token, sealed (version, nonce, ciphertext
    -- and tag), bound to this session's id.
    upstream_refresh_token BYTEA       NOT NULL,
    upstream_renewed_at    TIMESTAMPTZ NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    -- The 12-hour cap.
    expires_at             TIMESTAMPTZ NOT NULL,
    revoked_at             TIMESTAMPTZ
);
CREATE INDEX broker_person_sessions_principal
    ON broker_person_sessions (upstream_issuer, upstream_subject);

-- An authorization code: single use, valid for 60 seconds, bound to the
-- client id, redirect URI and PKCE challenge of the authorize request. A
-- used code stays until well past expiry so a replay is recognized, and
-- revokes its session.
CREATE TABLE broker_authorization_codes (
    code_hash      BYTEA       PRIMARY KEY,
    session_id     UUID        NOT NULL REFERENCES broker_person_sessions (id) ON DELETE CASCADE,
    client_id      TEXT        NOT NULL,
    redirect_uri   TEXT        NOT NULL,
    code_challenge TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    used_at        TIMESTAMPTZ
);
CREATE INDEX broker_authorization_codes_expiry ON broker_authorization_codes (expires_at);

-- The refresh tokens issued in a session (its family). Each is rotated on
-- use (#221); one rotated less than 60 s ago is still honored (#224).
CREATE TABLE broker_refresh_tokens (
    token_hash BYTEA       PRIMARY KEY,
    session_id UUID        NOT NULL REFERENCES broker_person_sessions (id) ON DELETE CASCADE,
    issued_at  TIMESTAMPTZ NOT NULL,
    rotated_at TIMESTAMPTZ
);
CREATE INDEX broker_refresh_tokens_session ON broker_refresh_tokens (session_id);
