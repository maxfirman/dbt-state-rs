-- dbt State server — store upstream dependency identities for lineage.
--
-- `input_tables` holds physical-read freshness (often raw sources seen through
-- views), which is too sparse to reconstruct the model DAG. `query_dependencies`
-- carries the actual upstream relations the client resolved for the node
-- (e.g. a model's upstream models), which is what lineage should be built from.

ALTER TABLE node_decisions
    ADD COLUMN IF NOT EXISTS query_dependencies JSONB NOT NULL DEFAULT '[]'::jsonb;
