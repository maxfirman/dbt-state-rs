-- dbt State server — add node_sql_hash (rendered SQL fingerprint).
--
-- The hosted service compares the RENDERED SQL (default
-- compare_unrendered_code=false), not just the client's node_body_hash — which
-- is the UNRENDERED template hash and so stays identical when only an env_var /
-- non-deterministic value changes. Matching on node_body_hash alone can
-- therefore wrongly SKIP a model whose rendered SQL changed (serving stale
-- output). We store a hash of the raw `sql` the client sends and require it to
-- match, forcing a rebuild when the rendered SQL differs. See
-- crates/harness/tests/c2_rendered_sql.rs.

ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS node_sql_hash TEXT;

-- Extend the primary fingerprint lookup to include the rendered-SQL hash.
CREATE INDEX IF NOT EXISTS idx_executions_sql_lookup
    ON executions (org_id, target_table, execution_type, node_body_hash, node_sql_hash, status);
