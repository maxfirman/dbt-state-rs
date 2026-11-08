-- Anchor a SKIP/reuse decision to the EXACT prior `executions` row it matched,
-- so `dbt state explain` (ADR 0005) can re-derive the per-upstream FRESH/OUTDATED
-- freshness subtree against the same row the decision used, rather than
-- re-selecting "latest confirmed for target" (which drifts as the append-only
-- ledger grows and could pick a different row at explain time).
--
-- NULLABLE because the match is INTRINSICALLY OPTIONAL, not for backward
-- compatibility: dbt-state-rs is pre-production, so there is no historical data
-- to preserve — this is a plain additive `ADD COLUMN` with NO backfill and NO
-- dual-write. The column is NULL whenever the decision matched no prior row:
--   * a genuine first-build / fingerprint-miss EXECUTE (`decide` ran with
--     confirmed = None — there is no `executions.id` to point at);
--   * a cross-name CLONE adoption (its match is a `CloneEquivalent`, which
--     carries no `executions.id`);
--   * the best-effort `node_decisions` capture, which is separate from the
--     `executions` write and may legitimately lack the id.
-- So NULL means "no matched prior build" — a large fraction of rows — NOT
-- "legacy row". A future reader MUST NOT tighten this to NOT NULL.
ALTER TABLE node_decisions
    ADD COLUMN matched_execution_id BIGINT REFERENCES executions(id);
