-- Captured Databricks template evidence. Nullable for legacy decisions; no
-- backfill or reinterpretation of old fingerprints/metadata.
ALTER TABLE executions ADD COLUMN node_macros_hash TEXT;
ALTER TABLE node_decisions ADD COLUMN template_comparison JSONB;
