-- Fresh hosted Snowflake + Databricks histories pin confirmed deferral by
-- exact profile plus project ID, or profile + project name when ID is absent.
-- Preserve selector state separately; pending/failed submits are not mappings.
-- Legacy executions lack this evidence and cannot be guessed into a scope.
ALTER TABLE executions ADD COLUMN node_profile_name TEXT;
ALTER TABLE executions ADD COLUMN node_project_name TEXT;
ALTER TABLE executions ADD COLUMN node_target_name TEXT;
CREATE INDEX idx_executions_named_deferral
    ON executions (org_id, node_profile_name, node_project_name, node_target_name,
                   node_unique_id, confirmed_at DESC, id DESC)
    WHERE status = 'confirmed';
CREATE INDEX idx_executions_id_deferral
    ON executions (org_id, node_profile_name, project_id, node_target_name,
                   node_unique_id, confirmed_at DESC, id DESC)
    WHERE status = 'confirmed';
