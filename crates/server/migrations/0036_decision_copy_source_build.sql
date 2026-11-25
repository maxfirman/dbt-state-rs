-- The build held by the Snowflake copy source, recorded for Explain.
ALTER TABLE node_decisions
    ADD COLUMN copy_source_build_id BIGINT REFERENCES executions(id) ON DELETE SET NULL;
