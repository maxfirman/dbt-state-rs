-- The client namespace identifies the warehouse connection (Snowflake account,
-- Redshift/Databricks host, BigQuery project). A relation with the same textual
-- FQN on another connection is a different physical object. The dialect family
-- is separate from dialect options such as Redshift identifier normalization.
ALTER TABLE executions
    ADD COLUMN warehouse_family TEXT GENERATED ALWAYS AS (
        CASE lower(btrim(split_part(dialect, ',', 1)))
            WHEN 'spark' THEN 'databricks'
            ELSE lower(btrim(split_part(dialect, ',', 1)))
        END
    ) STORED;

CREATE INDEX idx_executions_physical_current
    ON executions (org_id, warehouse_family, table_namespace, target_table,
                   confirmed_at DESC, id DESC)
    WHERE status = 'confirmed';
