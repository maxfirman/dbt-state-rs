-- dbt State server — node-state comparison store for state:* selectors (v9).
--
-- Powers SelectorService.GetStateSelection (dbt State-powered `state:*`
-- selectors). CHARACTERIZED LIVE against api.state.dbt.com (jaffle-shop,
-- Snowflake prod_demo baseline, selector queried from dev_clone whose
-- defer_to_target=prod_demo); see experiments/selector_experiment.sh,
-- golden/fixtures/selector_state_selection.jsonl, docs/correctness-handoff.md.
--
-- The selector compares each submitted node's hashes against the node's OWN
-- last recorded execution in the `defer_to_target` ENV (not the physical
-- target_table), scoped by project_id. This is a SEPARATE store from the
-- `executions` decision ledger:
--   * it is keyed by the logical (project_id, target env name, node_unique_id),
--     which is exactly the SelectorRequest scope — the executions table is keyed
--     by physical target_table and does not carry the env name or the
--     persisted_descriptions / macros / database_representation hashes;
--   * it is a pure latest-wins snapshot of the client-supplied DbtNodeData
--     hashes (the hosted service records them verbatim and does a pure hash
--     comparison — no recompute).
--
-- Populated on every SubmitEnrichedSQL / SubmitValues / RecordExecutions that
-- carries a dbt_node_state (which supplies target_name + all six hashes +
-- node_database_representation). A real `dbt build` in the defer-to env thus
-- seeds the comparison state a later `state:*` selector reads.
CREATE TABLE IF NOT EXISTS node_states (
    org_id                           TEXT NOT NULL,
    project_id                       TEXT NOT NULL,
    target_name                      TEXT NOT NULL,
    node_unique_id                   TEXT NOT NULL,
    node_body_hash                   TEXT,
    node_configs_hash                TEXT,
    node_persisted_descriptions_hash TEXT,
    node_macros_hash                 TEXT,
    node_contract_hash               TEXT,
    node_database_representation     TEXT,
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, project_id, target_name, node_unique_id)
);
