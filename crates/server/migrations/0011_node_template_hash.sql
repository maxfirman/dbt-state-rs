-- dbt State server — template (unrendered) logic identity (v11).
--
-- With `compare_unrendered_code = true`, the hosted service reuses a node when
-- EITHER the rendered SQL OR the unrendered Jinja template is unchanged — a
-- rebuild occurs ONLY when BOTH have changed (documented contract, and verified
-- live: a Jinja-comment-only edit that changes the body hash while leaving the
-- rendered SQL identical SKIPS — see experiments/template_comparison.sh and
-- golden/fixtures/template_compare_unrendered.jsonl; the complementary
-- rendered-changed/template-same SKIP is in compare_unrendered_code.jsonl).
--
-- To express "reuse iff rendered matches OR template matches" we persist BOTH
-- identities: `node_sql_hash` ALWAYS carries the rendered logic hash (as for
-- `compare_unrendered_code = false`), and `node_template_hash` carries the
-- template (unrendered-body) logic hash, set only in unrendered-comparison
-- mode. Both hashes fold in the same dependency/context/extras components, so a
-- genuine dependency or context change still invalidates BOTH and rebuilds.
ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS node_template_hash TEXT;

-- Reuse lookup for the template identity (newest confirmed per target).
CREATE INDEX IF NOT EXISTS idx_executions_template_hash
    ON executions (org_id, target_table, execution_type, node_template_hash, status);
