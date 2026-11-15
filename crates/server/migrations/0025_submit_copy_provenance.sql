-- New automatic-copy proofs never reinterpret RegisterClone v1 history.
ALTER TABLE executions ADD COLUMN submit_copy_provenance jsonb;
