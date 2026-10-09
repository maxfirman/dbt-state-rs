-- dbt State server — honest-log fields (Phase 2 revised).
--
-- We do NOT model authoritative environments. The `environments` table is the
-- dbt profile *target* (a per-developer alias), surfaced in the UI as "Target".
-- Record the real relation coordinates the decision touched so the log is
-- precise about what a target actually is: a database + schema (+ the profile
-- target name), not a governed environment.

ALTER TABLE environments ADD COLUMN IF NOT EXISTS database TEXT;
ALTER TABLE environments ADD COLUMN IF NOT EXISTS "schema" TEXT;

-- Each decision row is self-describing: it already has target_table + schema;
-- add the database (default_catalog) too.
ALTER TABLE node_decisions ADD COLUMN IF NOT EXISTS default_catalog TEXT;
