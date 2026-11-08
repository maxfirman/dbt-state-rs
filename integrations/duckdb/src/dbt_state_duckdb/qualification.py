"""Compose DuckDB reuse qualification before crossing the State transport.

Request adapters share this module's SQL, seed and initialization preparation.
Workload admission uses the same copy/configuration rules. Specialized SQL,
hook and incremental validators retain their own supported subsets; unknown
inputs and versioned contract bytes are preserved in the outgoing evidence.
"""

from sqlglot import exp

from .incremental import incremental_relation_contract, supported_incremental
from .views import SQL_CONTRACT, supported_execution_context


def ctas_satisfies_requirements(node):
    config = node.config
    # Hooks may create additional physical properties; rebuilding preserves the
    # normal materialization contract. Unknown adapter configs are conservative.
    allowed = {
        "enabled",
        "alias",
        "schema",
        "database",
        "tags",
        "meta",
        "group",
        "materialized",
        "persist_docs",
        "quoting",
        "full_refresh",
        "packages",
        "docs",
        "access",
        "static_analysis",
        "event_time",
        "batch_size",
        "begin",
        "lookback",
        "concurrent_batches",
    }
    properties = config.to_dict()
    from .incremental import supported_incremental

    if config.materialized != "table" and not supported_incremental(node):
        return False
    if properties.get("contract", {}).get("enforced"):
        return False
    for key in (
        "constraints",
        "indexes",
        "pre_hook",
        "post_hook",
        "sql_header",
        "grants",
        "defaults",
        "column_types",
        "partition_by",
        "cluster_by",
        "persist_docs",
    ):
        if config.get(key):
            return False
    if any(getattr(column, "constraints", None) for column in node.columns.values()):
        return False
    # Client settings prefixed run_cache control shared decision policy.
    for key, value in properties.items():
        if (
            value
            and key not in allowed
            and not key.startswith("run_cache_")
            and key
            not in {
                "contract",
                "on_schema_change",
                "incremental_strategy",
                "unique_key",
                "on_configuration_change",
            }
        ):
            return False
    return True


def qualify_sql(builder, node, sql, request):
    """Refresh SQL evidence, then qualify copying and incremental identity."""
    from query_cache_common.models import shared_models

    from .hooks import semantic_contract, supported_hooks
    from .incremental import reusable_self_read

    request.semantic_extras.update(semantic_contract(node))
    # Earlier outcomes did not prove this SQL/context subset.
    # Do not reinterpret that history under the stricter contract.
    request.semantic_extras["duckdb_sql_contract"] = SQL_CONTRACT
    request.default_schema = builder._adapter_ext.adapter.config.credentials.schema
    traversal = builder._adapter_ext.traverse_view_definitions(sql)
    # State excludes the target from upstream freshness. Its latest
    # output certifies reuse only when another execution is a no-op.
    unsafe_self_read = (
        node.config.materialized == "incremental"
        and request.target_table in traversal.seen_tables
        and (
            traversal.view_definitions
            or not reusable_self_read(sql, request.target_table)
        )
    )
    if (
        not supported_execution_context(builder._adapter_ext)
        or not supported_hooks(node)
        or unsafe_self_read
    ):
        marker = builder._adapter_ext._sql(
            exp.table_(
                "unsupported_execution_context",
                db="_dbt_state",
                catalog=builder._adapter_ext.default_catalog,
                quoted=True,
            )
        )
        traversal.seen_tables.add(marker)
        traversal.unresolvable_tables.add(marker)
    names = traversal.seen_tables | (
        {request.target_table} if request.target_table else set()
    )
    epochs = builder._adapter_ext.get_last_modified_epoch(names)
    request.tables = [
        shared_models.TableModifiedInfo(
            name=name,
            last_modified_epoch=(
                None if name in traversal.unresolvable_tables else epoch
            ),
        )
        for name, epoch in epochs.items()
    ]
    request.query_dependencies = [
        shared_models.QueryDependency(
            name=name,
            query=view.definition,
            default_catalog=view.default_catalog,
            default_schema=view.default_schema,
        )
        for name, view in traversal.view_definitions.items()
    ]

    if ctas_satisfies_requirements(node):
        request.semantic_extras["duckdb_copy_contract"] = "rows-schema-v1"
        if node.config.materialized == "incremental":
            from .incremental import initialization_identity

            identity = initialization_identity(builder, node)
            request.dbt_node_state.node_hash = (
                identity or "duckdb-incremental-no-init"
            )
            request.semantic_extras["duckdb_incremental_contract"] = (
                "delete-insert-fixed-v1"
            )
            # The cold branch can render identically across different
            # incremental templates. Bind the initialization contract to
            # automatic SQL copying too, so a rejected precompile copy
            # cannot re-enter through the rendered-SQL fallback.
            request.semantic_extras["duckdb_incremental_identity"] = (
                identity or "unavailable"
            )
            if identity is None:
                request.clone_chain_depth_limit = 0
    else:
        # Even an existing compatible source may not satisfy this node.
        request.clone_chain_depth_limit = 0


def qualify_values(builder, node, request):
    """Seeds retain their narrower hook/unknown-evidence qualification."""
    from .hooks import hook_config, semantic_contract

    request.semantic_extras.update(semantic_contract(node))
    # Seed hooks have no validated local execution subset yet. Retain
    # ordinary BUILD and never certify their current target for reuse.
    if (
        any(hook_config(node))
        or builder._adapter_ext.traverse_view_definitions(
            "SELECT 1"
        ).unresolvable_tables
    ):
        request.last_modified_epoch = None


def qualify_clone(builder, node, request):
    """Qualify initialization while no compiled SQL is available."""
    from .incremental import supported_incremental

    if supported_incremental(node) and ctas_satisfies_requirements(node):
        from .incremental import initialization_identity

        identity = initialization_identity(builder, node)
        if identity:
            request.labels["duckdb_incremental_contract"] = (
                "delete-insert-fixed-v1"
            )
            request.labels["duckdb_incremental_identity"] = identity
        else:
            request.clone_chain_depth_limit = 0
    else:
        request.clone_chain_depth_limit = 0


def state_workload(node):
    """Unsupported work still executes through the ordinary dbt runner."""
    kind = str(node.resource_type)
    if kind in {"test", "seed"}:
        return True
    if kind != "model" or getattr(node, "language", None) != "sql":
        return False
    materialized = node.config.materialized
    if materialized in {"table", "view"}:
        return True
    return supported_incremental(node) and ctas_satisfies_requirements(node)


def allows_workload(cache, node, phase):
    """Admit the workload at each pinned client callback, before its effects."""
    if not state_workload(node):
        return False
    if (
        node.config.materialized == "incremental"
        and phase == "on_execute"
    ):
        import duckdb

        name = cache._adapter_ext._sql(
            cache._node_resolution.node_to_table(node)
        )
        with duckdb.connect(str(cache._adapter_ext.path)) as db:
            if not incremental_relation_contract(db, name, node):
                return False
    if node.config.materialized == "incremental" and any(
        m.package_name not in {"dbt", "dbt_duckdb"}
        and (
            "delete_insert" in m.name
            or m.name.startswith("materialization_incremental")
        )
        for m in cache._manifest.macros.values()
    ):
        return False
    return True
