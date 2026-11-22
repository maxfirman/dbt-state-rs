-- Token broker: the UI's own client registration (#228, spec #215
-- "Registered clients").
--
-- The UI signs people in with the standard OIDC code flow, so its authorize
-- request carries a nonce that the code, and then the code exchange's
-- `id_token`, must echo. The dbt client sends none: both columns stay NULL
-- for its sign-ins.
ALTER TABLE broker_pending_logins ADD COLUMN client_nonce TEXT;
ALTER TABLE broker_authorization_codes ADD COLUMN nonce TEXT;

-- The `jti` of every `private_key_jwt` client assertion the broker accepted,
-- kept until the assertion could no longer be accepted (its `exp` plus the
-- clock skew), so a replayed assertion is refused on every replica
-- (RFC 7523 §3, OIDC Core §9). Stored as SHA-256 hashes like other bearer
-- values; not scoped by Organization.
CREATE TABLE broker_client_assertions (
    client_id      TEXT        NOT NULL,
    jti_hash       BYTEA       NOT NULL,
    remember_until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client_id, jti_hash)
);
CREATE INDEX broker_client_assertions_expiry ON broker_client_assertions (remember_until);
