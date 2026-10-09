-- dbt State server — UI domain model (Phase 1)
--
-- Additive to the existing `executions` table. These tables model the dbt
-- Platform concepts the UI mirrors (organization -> project -> environment ->
-- invocation -> node decision) and are populated best-effort on each decision.
--
-- Forwards-compatibility notes:
--   * Every table carries org_id from day one. Single-tenant today (one org,
--     derived from the x-organization-id header, default 'local'); real
--     multi-tenancy later needs no reshape.
--   * Auth is deferred, but the seams exist now: actors/roles/role_bindings and
--     a nullable actor_id on audit rows. They are unused until the auth phase.
--   * All identity/time columns use stable types so later migrations are purely
--     additive.

-- ---------------------------------------------------------------------------
-- Organization (tenant). One row in single-tenant deployments.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organizations (
    id          BIGSERIAL PRIMARY KEY,
    -- The external org id as seen on the wire (x-organization-id). 'local' for
    -- insecure local runs.
    external_id TEXT        NOT NULL UNIQUE,
    name        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Project. Mirrors dbt Platform projects.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projects (
    id          BIGSERIAL PRIMARY KEY,
    org_id      TEXT        NOT NULL,
    -- project_id from dbt_node_state (may be absent for some clients).
    external_id TEXT,
    name        TEXT        NOT NULL,
    dialect     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A project is identified within an org by its name (external_id may be null).
    UNIQUE (org_id, name)
);
CREATE INDEX IF NOT EXISTS idx_projects_org ON projects (org_id);

-- ---------------------------------------------------------------------------
-- Environment. Mirrors dbt Platform environments (dev/deployment targets).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS environments (
    id                 BIGSERIAL PRIMARY KEY,
    org_id             TEXT        NOT NULL,
    project_id         BIGINT      NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    -- target_name from the client (e.g. "prod", "dev").
    name               TEXT        NOT NULL,
    profile_name       TEXT,
    dialect            TEXT,
    -- Inferred: an environment that submits to dbt State has it enabled.
    dbt_state_enabled  BOOLEAN     NOT NULL DEFAULT TRUE,
    is_deferrable      BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS idx_environments_org ON environments (org_id);
CREATE INDEX IF NOT EXISTS idx_environments_project ON environments (project_id);

-- ---------------------------------------------------------------------------
-- Invocation (run). Grouped from the client's x-dbt-invocation-id metadata.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invocations (
    id                     BIGSERIAL PRIMARY KEY,
    org_id                 TEXT        NOT NULL,
    environment_id         BIGINT      NOT NULL REFERENCES environments (id) ON DELETE CASCADE,
    external_invocation_id TEXT,       -- x-dbt-invocation-id
    session_id             TEXT,       -- x-session-id
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    built_count            INTEGER     NOT NULL DEFAULT 0,
    reused_count           INTEGER     NOT NULL DEFAULT 0,
    cloned_count           INTEGER     NOT NULL DEFAULT 0,
    -- Auth seam (nullable until auth exists): which actor ran this.
    actor_id               BIGINT,
    UNIQUE (org_id, external_invocation_id)
);
CREATE INDEX IF NOT EXISTS idx_invocations_env ON invocations (environment_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_invocations_org_time ON invocations (org_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- Node decision — the append-only audit log. One row per decision returned.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS node_decisions (
    id                    BIGSERIAL PRIMARY KEY,
    org_id                TEXT        NOT NULL,
    invocation_id         BIGINT      REFERENCES invocations (id) ON DELETE CASCADE,
    environment_id        BIGINT      NOT NULL REFERENCES environments (id) ON DELETE CASCADE,
    node_unique_id        TEXT,
    node_name             TEXT,
    node_fqn              TEXT,
    resource_type         TEXT,
    execution_type        INTEGER     NOT NULL,
    -- 'build' | 'skip' | 'clone'
    decision              TEXT        NOT NULL,
    is_stale              BOOLEAN     NOT NULL DEFAULT FALSE,
    decision_description  TEXT,
    request_id            TEXT,
    execution_decision_id TEXT,
    node_body_hash        TEXT,
    values_hash           TEXT,
    table_namespace       TEXT,
    target_table          TEXT,
    default_schema        TEXT,
    dialect               TEXT,
    -- clone specifics
    clone_source          TEXT,
    clone_sqls            JSONB,
    -- upstream freshness snapshot at decision time
    input_tables          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    execution_runtime_ms  BIGINT,
    -- Auth seam.
    actor_id              BIGINT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_node_decisions_invocation ON node_decisions (invocation_id);
CREATE INDEX IF NOT EXISTS idx_node_decisions_env_time ON node_decisions (environment_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_node_decisions_node ON node_decisions (org_id, node_unique_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_node_decisions_org_time ON node_decisions (org_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Auth seams. Created now, unused until the auth phase. Kept minimal; the auth
-- phase will add columns additively (e.g. SSO subject details, scopes).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS actors (
    id               BIGSERIAL PRIMARY KEY,
    org_id           TEXT        NOT NULL,
    -- 'user' | 'service' | 'system'
    kind             TEXT        NOT NULL,
    external_subject TEXT,       -- SSO subject / token subject, later
    display_name     TEXT,
    email            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, kind, external_subject)
);

CREATE TABLE IF NOT EXISTS roles (
    id             BIGSERIAL PRIMARY KEY,
    org_id         TEXT        NOT NULL,
    name           TEXT        NOT NULL,
    -- Permission set name, mirrors dbt (owner|admin|member|read_only).
    permission_set TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

CREATE TABLE IF NOT EXISTS role_bindings (
    id         BIGSERIAL PRIMARY KEY,
    org_id     TEXT        NOT NULL,
    role_id    BIGINT      NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    actor_id   BIGINT      NOT NULL REFERENCES actors (id) ON DELETE CASCADE,
    -- 'all' | 'project'
    scope      TEXT        NOT NULL DEFAULT 'all',
    project_id BIGINT      REFERENCES projects (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_role_bindings_actor ON role_bindings (actor_id);
