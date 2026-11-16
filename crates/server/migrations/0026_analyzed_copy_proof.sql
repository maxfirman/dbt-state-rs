-- New proof domain; no legacy source SQL exists to backfill safely.
ALTER TABLE executions ADD COLUMN analyzed_copy_proof JSONB;
