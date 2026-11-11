-- Record, for a rebuild without a matched build, the latest build in scope
-- whose identity did not match the workload (a logic change). `dbt state
-- explain` judges the SHAPE 7 freshness listing against that build: hosted
-- marks each upstream FRESH or OUTDATED (with the `updated N seconds after …`
-- line) and ends with `upstream data is up to date` / `upstream data is
-- outdated` as for any anchored listing (explain_incremental_*.jsonl,
-- explain_snapshot_history.jsonl, explain_rebuild_reason_gaps.jsonl; #73,
-- #40 code review). Like `matched_execution_id` (0013, ADR 0005) it is an id,
-- and the listing is re-derived from it, never persisted.
--
-- NULLABLE: every other decision has none, and so has a rebuild recorded
-- before this migration. Unknown evidence never proves freshness, so Explain
-- then omits the freshness summary line. The pointer is evidence for
-- Explain only, so deleting the build it names clears it (ON DELETE SET
-- NULL) instead of blocking the delete.
ALTER TABLE node_decisions
    ADD COLUMN unmatched_execution_id BIGINT REFERENCES executions(id) ON DELETE SET NULL;
