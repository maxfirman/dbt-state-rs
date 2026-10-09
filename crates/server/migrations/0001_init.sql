-- dbt State server — Postgres schema (v1)
--
-- The server stores, per organization + target table, a history of confirmed
-- executions keyed by the client-supplied semantic hashes. On SubmitEnrichedSQL
-- the server looks for a matching confirmed record whose upstream data is not
-- newer than what was recorded (freshness), and decides SKIP vs EXECUTE.

CREATE TABLE IF NOT EXISTS executions (
    id                   BIGSERIAL PRIMARY KEY,
    org_id               TEXT        NOT NULL,
    target_table         TEXT        NOT NULL,
    execution_type       INTEGER     NOT NULL,
    -- client-supplied node hashes (from dbt_node_state)
    node_hash            TEXT,
    node_body_hash       TEXT,
    node_configs_hash    TEXT,
    node_contract_hash   TEXT,
    node_unique_id       TEXT,
    table_namespace      TEXT,
    dialect              TEXT        NOT NULL,
    -- recorded outcome from ConfirmExecution
    last_modified_epoch  BIGINT,
    table_type           TEXT,
    execution_runtime_ms BIGINT,
    -- upstream input freshness recorded at submit time (JSON array of {name,last_modified_epoch})
    input_tables         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    -- lifecycle: 'pending' after a ready_to_execute verdict, 'confirmed' after ConfirmExecution
    status               TEXT        NOT NULL DEFAULT 'pending',
    request_id           TEXT        NOT NULL,
    execution_decision_id TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at         TIMESTAMPTZ
);

-- Fast lookup of the most recent confirmed execution for a node fingerprint.
CREATE INDEX IF NOT EXISTS idx_executions_lookup
    ON executions (org_id, target_table, execution_type, node_body_hash, status);

-- Correlate ConfirmExecution by request_id.
CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_request_id
    ON executions (request_id);

-- Freshness lookups by node unique id (upstream propagation).
CREATE INDEX IF NOT EXISTS idx_executions_node_uid
    ON executions (org_id, node_unique_id, status);
