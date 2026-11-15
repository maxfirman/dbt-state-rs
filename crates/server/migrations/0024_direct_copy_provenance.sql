-- Nullable versioned evidence: legacy Databricks copies remain cold misses.
ALTER TABLE executions ADD COLUMN direct_copy_provenance JSONB;
