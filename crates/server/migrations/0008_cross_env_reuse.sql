-- dbt State server — cross-environment reuse + clone provenance (v8).
--
-- The hosted service reuses an object across environments (e.g. a dev clone of
-- a prod table) when the submit's logic matches an execution in the SAME
-- `table_namespace`, abstracting the physical schema qualifier, and the data is
-- fresh. Verified live (api.state.dbt.com, Fusion client, Snowflake); see
-- experiments/clone_logic_sensitivity.sh and docs/correctness-handoff.md.
--
-- `node_namespace_logic_hash` is the schema-abstracted, environment-independent
-- logic identity (see fingerprint::namespace_logic_hash). It is recorded on
-- every SQL execution so a later cross-environment submit can match it. The
-- strict `node_sql_hash` remains the primary SAME-target identity; this hash is
-- only consulted for namespace-grouped cross-environment reuse.
ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS node_namespace_logic_hash TEXT;

-- Clone provenance: when a row originates from a RegisterClone (rather than a
-- SQL build), record the source it was cloned from. A clone-origin row has NO
-- SQL of its own, so its logic identity is inherited from a confirmed execution
-- of the clone SOURCE within the same namespace.
ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS clone_source_table TEXT;

-- Cross-environment reuse lookup: newest confirmed execution in a namespace
-- carrying a given schema-abstracted logic identity.
CREATE INDEX IF NOT EXISTS idx_executions_ns_logic
    ON executions (org_id, table_namespace, node_namespace_logic_hash, confirmed_at DESC, id DESC)
    WHERE status = 'confirmed';
