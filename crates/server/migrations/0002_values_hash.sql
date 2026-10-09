-- dbt State server — add seed (SubmitValues) support.
--
-- Seeds are matched on a client-computed md5 of the seed file bytes
-- (values_hash) instead of the SQL body fingerprint (node_body_hash). Store it
-- in a nullable column so model executions (which have no values_hash) are
-- unaffected, and index it alongside the existing fingerprint lookup.

ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS values_hash TEXT;

-- Fast lookup of the most recent confirmed seed execution for a target table.
CREATE INDEX IF NOT EXISTS idx_executions_values_lookup
    ON executions (org_id, target_table, execution_type, values_hash, status);
