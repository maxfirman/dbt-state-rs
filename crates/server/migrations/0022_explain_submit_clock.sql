-- Explain-only display defaults. Never backfill historical rows from receipt
-- times or completion epochs: neither establishes the original submit instant.
ALTER TABLE executions ADD COLUMN display_submitted_at_ms BIGINT;
ALTER TABLE node_decisions ADD COLUMN display_submitted_at_ms BIGINT;
