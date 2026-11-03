"""Local project scope and completed data-test invocation handling."""

import hashlib
from pathlib import Path


def completed_invocation(result):
    """A completed failing test is an outcome, unlike an execution error.

    dbt returns success=False for severity=error failures even though every test
    query completed. Preserve its confirmed outcomes across ordinary restarts.
    Any model failure, SQL error, or absent result still leaves recovery dirty.
    """
    if result[1]:
        return True
    results = getattr(result[0], "results", None)
    if not results:
        return False
    return all(
        str(item.status) in {"success", "pass", "warn", "skipped"}
        or (str(item.node.resource_type) == "test" and str(item.status) == "fail")
        for item in results
    )


def install(adapter_class):
    from dbt_state.request_builder import RequestBuilder
    from dbt_state.run_cache import RunCache

    original = RequestBuilder.build_dbt_node_state

    def node_state(builder, node):
        state = original(builder, node)
        if isinstance(builder._adapter_ext, adapter_class):
            # Cloud project IDs are a separate authority. Local project identity
            # binds the canonical project root and name, independently of file /
            # generation namespaces shared by models in the same warehouse.
            root = Path(builder._config.project_root).expanduser().resolve()
            scope = (
                "duckdb-local-project-v1\0"
                + str(root)
                + "\0"
                + builder._config.project_name
            )
            state.project_id = (
                "duckdb-local-v1-" + hashlib.sha256(scope.encode()).hexdigest()
            )
        return state

    RequestBuilder.build_dbt_node_state = node_state

    original_proxy = RunCache.data_test_adapter_proxy

    def adapter_proxy(cache, node):
        if isinstance(cache._adapter_ext, adapter_class) and (
            node.should_store_failures or node.config.store_failures_as
        ):
            # Stored failure relations have a separate write lifecycle; until
            # proven, let dbt execute all DDL and aggregate SQL ordinarily.
            return cache._adapter
        return original_proxy(cache, node)

    RunCache.data_test_adapter_proxy = adapter_proxy
