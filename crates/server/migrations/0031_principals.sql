-- Principals (#226): each decision and execution records the Principal (a
-- person or a machine the Upstream IdP authenticated) whose request made it.
--
-- Replaces the auth seams reserved in 0003 and never written: `actors`,
-- `roles` (`owner|admin|member|read_only`), `role_bindings`, and the
-- nullable `actor_id` audit columns. Roles are not stored: a Principal's
-- Role in an Organization comes from the Token broker's mapping rules and is
-- carried in its token.

DROP TABLE IF EXISTS role_bindings;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS actors;
ALTER TABLE invocations DROP COLUMN IF EXISTS actor_id;
ALTER TABLE node_decisions DROP COLUMN IF EXISTS actor_id;

-- A Principal the Token broker issued a token to, keyed by the Upstream
-- IdP's issuer and subject. `id` is the opaque Principal id the broker's
-- tokens carry as `sub` (`PrincipalId::upstream(issuer, subject)`). Not
-- scoped by Organization: one Principal may hold Roles in several.
CREATE TABLE principals (
    issuer        TEXT        NOT NULL,
    subject       TEXT        NOT NULL,
    id            TEXT        NOT NULL UNIQUE,
    kind          TEXT        NOT NULL CHECK (kind IN ('person', 'machine')),
    -- For display only, when the Upstream IdP provides it; never identity.
    email         TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, subject)
);

-- The Principal whose request made the decision, or wrote the execution
-- row (its Submit, RecordExecutions or RegisterClone): `principals.id`.
-- NULL for rows recorded before this migration and in `none` mode. No
-- foreign key: a decision is recorded even when its Principal has no record
-- (for example, a token from a Token broker without this database).
ALTER TABLE node_decisions ADD COLUMN principal_id TEXT;
ALTER TABLE executions ADD COLUMN principal_id TEXT;
