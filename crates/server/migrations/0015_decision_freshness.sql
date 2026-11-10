-- Persist the freshness context a node decision used (ADR 0007), so
-- `dbt state explain` can re-derive the Lag tolerance line: the node's
-- tolerance and the reuse candidate's build age on the server clock at
-- decision time. `node_decisions.input_tables` now also carries each input's
-- resolved producer.
--
-- NULLABLE: rows written before this migration render no tolerance line.
ALTER TABLE node_decisions
    ADD COLUMN freshness_tolerance_ms BIGINT,
    ADD COLUMN build_age_ms BIGINT;
