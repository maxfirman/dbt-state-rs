-- Preserve raw relation spelling while grouping the demonstrated Databricks
-- metadata aliases into one physical history. Generated storage covers existing
-- confirmed rows and old pending rows confirmed after an upgrade alike.
-- Quoted dots, escaped backticks and all other uncharacterized forms stay exact.
ALTER TABLE executions ADD COLUMN physical_target_key TEXT COLLATE "C" GENERATED ALWAYS AS (
    CASE WHEN translate(btrim(split_part(dialect, ',', 1)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') COLLATE "C" IN ('databricks', 'spark')
        AND target_table COLLATE "C" ~ '^(`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)[.](`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)[.](`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)$'
        THEN translate(replace(target_table, '`', ''), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
        ELSE target_table
    END
) STORED;
CREATE INDEX executions_physical_target_key_idx
    ON executions (org_id, warehouse_family, table_namespace, physical_target_key,
                   confirmed_at DESC NULLS LAST, id DESC)
    WHERE status = 'confirmed';
