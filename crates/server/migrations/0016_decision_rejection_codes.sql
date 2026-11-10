-- Persist a node decision's wire rejection codes, so `dbt state explain` can
-- tell a rebuild whose target's older build still matched the submit
-- (LATEST_QUERY_HASH_NOT_MATCH, 8) from one where nothing matched. A seed's
-- Explain says "the seed data has not changed" in the first case.
--
-- NULLABLE: rows written before this migration, and skips, have none.
ALTER TABLE node_decisions
    ADD COLUMN skip_rejection_reason INT,
    ADD COLUMN clone_rejection_reason INT;
