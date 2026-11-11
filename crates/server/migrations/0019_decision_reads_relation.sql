-- Record, for a view decision, whether the view's SQL reads a relation: a
-- table or view it names in a FROM or JOIN, other than a CTE of the query.
-- Client 2.0.11 reports no upstreams for a view, so `dbt state explain`
-- cannot tell the two apart from the inputs. Hosted says `no upstream
-- dependencies` for a view that reads no relation (no FROM, a CTE without
-- FROM, VALUES, a table function) and lists `upstream model queries have not
-- changed` / `upstream data is up to date` for a view over a `ref`
-- (VERIFIED LIVE 2026-11-11, explain_view_reads_no_relation.jsonl,
-- explain_reason_kind_cells.jsonl `vc0`, `cv0`; #117, ADR 0005).
--
-- Explain evidence only: decisions never read it, and no tree or verdict is
-- persisted. NULLABLE: every other node kind has none, and so has a view
-- whose SQL does not parse as one query or that was decided before this
-- migration. Explain then renders nothing for a view without reported
-- upstreams, because unknown evidence does not choose a wording.
ALTER TABLE node_decisions
    ADD COLUMN reads_relation BOOLEAN;
