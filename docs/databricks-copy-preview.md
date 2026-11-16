# Databricks Result copy and client controls

Databricks Result copy is enabled in the normal server configuration. There are
no Databricks-specific server environment switches or embedded-server toggles.
Automatic FULL copying and direct MERGE/check-snapshot copying retain their
[shared eligibility and provenance rules](databricks-shared-copy.md); enabling
them does not admit arbitrary SQL, new materializations or positive copy chains.
Seeds are not automatically copied on Databricks.

## Client controls

Hosted-compatible client controls remain available. To suppress direct pre-copy
for a model or snapshot, merge this into its existing `state` mapping:

```jinja
{{ config(state={'pre_clone': 'never'}) }}
```

To disable ordinary submit-time cloning for a profile output, use its existing
`allow_clones` setting:

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      # Keep the existing Databricks connection settings here.
      allow_clones: false
```

These are distinct client paths: `pre_clone: never` suppresses RegisterClone;
ordinary SQL submissions still carry their own copy permissions. The pinned
client's [configuration captures](../golden/fixtures/explain_copy_logic_mismatch.jsonl)
show `allow_clones: false` and `clone_chain_depth_limit: 0` on ordinary submits.
The public regressions verify default-on automatic/direct grants and explicit
client refusal without relaxing source integrity or adoption checks.

## Accepted limitations

The maintainer accepted these limitations on 2026-11-16; neither is a
Databricks cloning acceptance blocker:

- Unity Catalog shallow clones cannot be overwritten by `CREATE OR REPLACE`.
  The pinned dbt 2.0.11 FULL rebuild uses that statement and can fail with
  `CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR`. The same failure is observed
  with hosted State and in State-disabled controls. MERGE repairs update the
  clone rather than replacing it and pass the retained physical checks.
  Databricks documents DROP followed by CREATE, or a new name, as alternatives;
  this service does not automatically drop user relations or rewrite the
  client's ordinary materialization. See the [recovery evidence](databricks-copy-recovery.md)
  and [warehouse restriction](https://docs.databricks.com/aws/en/tables/operations/clone-unity-catalog#limitations).
- Clone SQL remains unpinned, matching captured hosted behavior. A concurrent
  source change between validation and clone execution is a shared physical
  race, including Snowflake; general concurrency safety is not claimed.

Externally modified sources visible to validation are still refused. Unknown
bindings/epochs/producers, incompatible identities and invalid publication
remain ineligible. No persistence or fingerprint versions change in this rollout;
existing valid v2 history remains reusable and incompatible legacy origins still
cold-miss. The older experiments used explicit server opt-ins; their retained
captures and source attribution are historical evidence, not current configuration.
