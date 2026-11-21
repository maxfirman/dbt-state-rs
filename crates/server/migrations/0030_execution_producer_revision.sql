-- #213: distinguish an ordinary Databricks FULL build after a proven
-- external modification of its single managed input. This revision belongs
-- to the producing execution, independent of its SQL fingerprint.
ALTER TABLE executions ADD COLUMN producer_revision BIGINT;
