-- Persist, for a rebuild without a matched build whose latest build in scope
-- did not have the workload's identity (a logic change), whether every
-- genuine upstream's epoch is unchanged since that build. `dbt state explain`
-- ends the SHAPE 7 freshness listing with `upstream data is up to date` only
-- then (hosted, explain_incremental_append_merge_history.jsonl, #73).
--
-- NULLABLE: rows written before this migration, and every other decision,
-- have none.
ALTER TABLE node_decisions
    ADD COLUMN unmatched_upstreams_unchanged BOOLEAN;
