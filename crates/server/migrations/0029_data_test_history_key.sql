-- #200 (ADR 0007 "Data-test history key"): a Snowflake data test's State
-- history is keyed by its versioned history key (normalized SQL with each
-- input relation resolved through copies this server recorded, plus its
-- result-setting configuration), within the organization, warehouse family
-- and connection namespace. NULL on every other row and on every data test
-- recorded before this migration, which therefore cold-misses: no existing
-- fingerprint is reinterpreted.
ALTER TABLE executions ADD COLUMN data_test_key TEXT;

-- The confirmed build a Snowflake copy (a Result copy on submit or a
-- RegisterClone) was made from. A data test reads a copy, unchanged since
-- it was made, as this build. NULL for ordinary builds and for copies
-- recorded before this migration, which then stand only for themselves.
ALTER TABLE executions
    ADD COLUMN clone_source_execution_id BIGINT REFERENCES executions(id) ON DELETE SET NULL;

CREATE INDEX executions_data_test_key_idx
    ON executions (org_id, warehouse_family, table_namespace, data_test_key,
                   confirmed_at DESC NULLS LAST, id DESC)
    WHERE status = 'confirmed' AND data_test_key IS NOT NULL;
