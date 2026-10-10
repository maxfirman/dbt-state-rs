-- Preserve protobuf outcomes losslessly (including int64 and non-finite doubles).
ALTER TABLE executions ADD COLUMN execution_results BYTEA;
ALTER TABLE executions ADD COLUMN project_id TEXT;

-- Current physical state must be selected before filtering by logic or kind.
CREATE INDEX idx_executions_current_target
    ON executions (org_id, target_table, confirmed_at DESC, id DESC)
    WHERE status = 'confirmed';
