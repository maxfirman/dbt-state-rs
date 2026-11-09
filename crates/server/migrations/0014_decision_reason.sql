-- Persist why each node decision was made (ADR 0006, amending ADR 0005), so
-- `dbt state explain` selects its tree shape from what the decision knew
-- instead of from `decision_description` wording.
--
-- The value is the decision reason's stable lowercase string
-- (`decision::DecisionReason::as_str`), e.g. `first_build`,
-- `no_reuse_candidate.own_target_changed_or_missing`, `reused.fresh`.
-- Persisted state: never reword an existing value.
--
-- NULLABLE only for rows written before this migration; every capture writes
-- it. dbt-state-rs is pre-production (ADR 0005), so there is NO backfill. Explain
-- renders an empty tree for a NULL (or unknown) reason.
ALTER TABLE node_decisions
    ADD COLUMN decision_reason TEXT;
