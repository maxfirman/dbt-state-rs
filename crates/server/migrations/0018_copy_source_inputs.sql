-- Record, on a client-requested copy (RegisterClone), the upstream relations
-- its source build recorded, by physical name (a JSON array of strings). When
-- hosted reuses such a copy, `dbt state explain` gives a FRESH verdict (and the
-- `no updates since … last executed` line) only to an upstream the source
-- build recorded; an upstream resolved per target under another name is
-- listed with its `last updated` line only (VERIFIED LIVE 2026-11-11,
-- explain_post_copy_identity.jsonl, #93).
--
-- Explain evidence only: decisions never read it. NULLABLE: every other row
-- has none, and so has a copy registered before this migration, which Explain
-- then renders as before.
ALTER TABLE executions
    ADD COLUMN clone_source_inputs JSONB;
