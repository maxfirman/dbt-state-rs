# Databricks source-less data-test UID boundary — 2026-11-14

**#138 remains INCOMPLETE.** The actual pinned client reused a source-less true
comparison test across different authoritative full UIDs. The required full-UID
isolation floor remains unchanged. This is a structural contract conflict; equal
literal results do not waive it, and no wrong-row outcome is claimed. There is no
new replay gap, mask, fixture exclusion or dependency promotion.

The original H project has a complete 17-RPC history: initial speculative
UNDECIDED, ordinary BUILD, successful Confirm with complete passing results and
runtime 3498 ms, then within-UID required reuses. A fresh D project's first actual
dbt invocation receives speculative SKIP with those complete results and runtime
3498 ms. No earlier D publication appears in the retained campaign histories.
The runtime echo strongly suggests H supplied the result, but hosted does not
expose its source row ID; that attribution is an inference. Fresh D reuse itself
contradicts the required full-UID boundary.

| Field | Original H | Fresh D |
| --- | --- | --- |
| `dbt_node_state.node_unique_id` | `test.survey_20261114c138cd.template_identity` | `test.survey_20261114d138df.template_identity` |
| Fallback label `dbt_node_unique_id` | same H full UID | same D full UID |
| `project_name` | `survey_20261114c138cd` | `survey_20261114d138df` |
| `node_hash` | `5165c3cc642d1fb125877096df190098` | `4bc307b21e8d28bd10158d53c64d8f05` |
| `project_id` | null | null |
| `table_namespace` | `f72415cee49eac9b2b28d9ad8e588507` | same |
| `node_body_hash` | `06de1c9e0d2406ad5213d18d12646ed7` | same |
| `node_configs_hash` | `b952ee812ea5c523f9f5ef0031d89eae` | same |

Both use the same warehouse family, catalog/schema defaults, profile/target,
rendered SQL `select cast(1 as int) as id where 0 = 1`, empty inputs and null
physical target. These are actual emitted requests, not selector metadata or a
crafted claim of reachability. The fresh run fails its first-build assertion;
its complete certified four RPCs remain outside the accepted matching fixtures.
The earlier complete H history is accepted for its within-UID identity behavior.

The current server continues to require exact scoped UID identity. A public
regression confirms H's cached result, requires fresh D to be speculative
UNDECIDED and ordinary BUILD, and then requires H to reuse positively. The root's
independent read-only merger verified the authoritative UID fields and causal
history. No code was widened to reproduce the conflicting hosted reuse.

The later no-DDL within-UID acceptance adds a run nonce to the body to establish
a genuinely fresh declaration history. That avoids a hidden hosted baseline;
it does not repair, hide or waive this cross-UID conflict. The baseline, unchanged,
pure-comment and same-body compiled-divergence cases all retain independent
State-disabled test/SELECT references and complete cached results.

Evidence is retained under
`~/.local/state/dbt-state-rs/databricks-138-20261114/`:

- `hosted-tests/`: complete initial H history, SHA-256
  `88cfae3f79e2444e0c8ecea095cb0ee24b53a92df4c3e29d55f9b6ec8fb0f3dd`.
- `hosted-test-comments/`: fresh D's first invocation and failure, SHA-256
  `34b3959998da155c91e47510334b8ec3407bcd7ea6a96293efcdab59e4885fe7`.
- `hosted-cross-uid-explain/`: subsequent read-only Explain with its original
  decision ID and causal source pointers.
- `hosted-test-comments-fresh-body/`: successful new, complete within-UID
  declaration history; no warehouse DDL or relation writes.

All warehouse inventories are preserved. These data-test diagnostics allocate no
warehouse objects. Parent copy/snapshot/freshness/logical-target/namespace
blockers remain unchanged, and #138 cannot unblock native dependents.
