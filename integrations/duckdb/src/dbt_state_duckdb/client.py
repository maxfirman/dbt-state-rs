"""Compatibility with the pinned dbt-state Python client.

Own all upstream patches and their ordering here. Reuse qualification, completed
outcomes, evidence recovery and Result copy policy remain in their own modules.
In particular, qualify callbacks before guarding outcomes, and record warehouse
completion before upstream confirmation, including its transport-failure bypass.
"""

import hashlib
import os
from pathlib import Path


def install(extension_type):
    """Apply a process-local extension only with explicit, insecure loopback mode."""
    if os.environ.get("DBT_STATE_LOCAL") != "1":
        return
    from dbt_state.config import get_env

    address = get_env("API_URL")
    host, separator, port = address.rpartition(":")
    if (
        host not in {"localhost", "127.0.0.1", "[::1]"}
        or not separator
        or not port.isdigit()
        or get_env("API_SECURE").lower() != "false"
    ):
        raise ValueError(
            "DBT_STATE_LOCAL=1 requires a loopback RUN_CACHE_API_URL and RUN_CACHE_API_SECURE=false"
        )
    from dbt_state import runner
    from dbt_state.adapters import ADAPTER_EXTENSION_MAPPING
    from dbt_state.request_builder import RequestBuilder

    from .data_tests import completed_invocation
    from dbt_state.run_cache import RunCache

    original_node_state = RequestBuilder.build_dbt_node_state

    def node_state(builder, node):
        state = original_node_state(builder, node)
        if isinstance(builder._adapter_ext, extension_type):
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

    original_data_test_proxy = RunCache.data_test_adapter_proxy

    def adapter_proxy(cache, node):
        if isinstance(cache._adapter_ext, extension_type) and (
            node.should_store_failures or node.config.store_failures_as
        ):
            # Stored failure relations have a separate write lifecycle; until
            # proven, let dbt execute all DDL and aggregate SQL ordinarily.
            return cache._adapter
        return original_data_test_proxy(cache, node)

    RunCache.data_test_adapter_proxy = adapter_proxy
    ADAPTER_EXTENSION_MAPPING["duckdb"] = extension_type
    from .qualification import (
        allows_workload,
        qualify_clone,
        qualify_sql,
        qualify_values,
    )


    original_sql = RequestBuilder.build_submit_enriched_sql_request

    def sql(
        self,
        node,
        sql,
        execution_type,
        traversal_result,
        microbatch_window=None,
        speculative=False,
    ):
        local = isinstance(self._adapter_ext, extension_type)
        if local:
            speculative = False
        request, duration = original_sql(
            self,
            node,
            sql,
            execution_type,
            traversal_result,
            microbatch_window,
            speculative,
        )
        if local:
            qualify_sql(self, node, sql, request)
        return request, duration

    RequestBuilder.build_submit_enriched_sql_request = sql
    original_values = RequestBuilder.build_submit_values_request

    def values(self, node):
        request = original_values(self, node)
        if isinstance(self._adapter_ext, extension_type):
            qualify_values(self, node, request)
        return request

    RequestBuilder.build_submit_values_request = values
    original_clone = RequestBuilder.build_clone_request

    def clone(self, node, *args, **kwargs):
        request = original_clone(self, node, *args, **kwargs)
        if isinstance(self._adapter_ext, extension_type):
            qualify_clone(self, node, request)
        return request

    RequestBuilder.build_clone_request = clone

    for method in ("on_compile", "on_execute", "on_run_result"):
        original_callback = getattr(RunCache, method)

        def gated(self, node, *args, _original=original_callback, _method=method, **kwargs):
            if isinstance(self._adapter_ext, extension_type):
                if not allows_workload(self, node, _method):
                    return None
            return _original(self, node, *args, **kwargs)

        setattr(RunCache, method, gated)
    from .result_copy import ResultCopy

    original_process = RunCache._process_query_cache_response

    def process(self, node, response):
        if isinstance(self._adapter_ext, extension_type):
            return ResultCopy(self, node).process(response, original_process)
        return original_process(self, node, response)

    RunCache._process_query_cache_response = process
    original_try_clone = RunCache._try_clone

    def try_clone(self, node):
        if isinstance(self._adapter_ext, extension_type):
            return ResultCopy(self, node).initialize()
        return original_try_clone(self, node)

    RunCache._try_clone = try_clone
    # Upstream's early noninteractive guard predates its insecure transport path.
    # Avoid constructing the hosted auth helper solely in this explicit mode;
    # QueryCacheGrpcClient still chooses the configured insecure channel itself.
    from contextvars import ContextVar

    local_duckdb = ContextVar("local_duckdb", default=False)
    # Enter before dbt's on-run hooks, including invocations without model work.
    # Keep the file lock until the CLI has joined workers and closed adapters.
    from dbt.cli.main import cli

    from .recovery import begin, finish, guard_outcome

    original_config = runner.RunnerOverride.set_runtime_config
    original_invoke = cli.invoke

    def configure(self, config):
        if config.credentials.type == "duckdb":
            begin(config.credentials.path)
        return original_config(self, config)

    def invoke(ctx):
        success = False
        # Click closes adapter resources when the outermost context exits.
        # Register first so resource callbacks run before evidence is marked clean.
        ctx.call_on_close(lambda: finish(success))
        with ctx:
            from dbt.cli.exceptions import ResultExit

            try:
                result = original_invoke(ctx)
            except ResultExit as outcome:
                success = completed_invocation((outcome.result, False))
                raise
            success = completed_invocation(result)
            return result

    runner.RunnerOverride.set_runtime_config = configure
    cli.invoke = invoke
    from dbt_state.grpc.client import QueryCacheGrpcClient

    for method in (
        "confirm_execution",
        "record_executions",
        "update_execution_metadata",
    ):
        setattr(
            QueryCacheGrpcClient,
            method,
            guard_outcome(getattr(QueryCacheGrpcClient, method)),
        )

    original_auth = runner.sso_auth
    original_create = runner.RunnerOverride._run_cache_get_or_create

    def create(self, config, adapter, manifest):
        token = local_duckdb.set(adapter.type() == "duckdb")
        try:
            return original_create(self, config, adapter, manifest)
        finally:
            local_duckdb.reset(token)

    runner.RunnerOverride._run_cache_get_or_create = create
    runner.sso_auth = lambda **kwargs: (
        None if local_duckdb.get() else original_auth(**kwargs)
    )
    original_namespace = RequestBuilder.table_namespace.fget

    def namespace(builder):
        if isinstance(builder._adapter_ext, extension_type):
            return builder._adapter_ext.table_namespace
        return original_namespace(builder)

    RequestBuilder.table_namespace = property(namespace)

    # Materializations return after commit and both inside/outside post-hooks.
    # Wrap the original execution, including upstream's transport-failure bypass,
    # so completion evidence exists before on_run_result and confirmation.
    from .completion import completed_result, may_confirm


    original_on_result = RunCache.on_run_result

    def on_result(cache, node, result):
        # Data-test aggregate outcomes use their separate complete-result route;
        # an ordinary failed materialization is never a successful Values/SQL row.
        if (
            isinstance(cache._adapter_ext, extension_type)
            and str(result.status) != "success"
        ):
            return
        return original_on_result(cache, node, result)

    RunCache.on_run_result = on_result
    original_execute = runner.RunnerOverride._execute_with_cache

    def execute(self, dbt_runner, node, manifest, *, original, submit, should_confirm):
        def completed(*args, **kwargs):
            result = original(*args, **kwargs)
            cache = self._run_cache_get_or_create(
                dbt_runner.config, dbt_runner.adapter, manifest
            )
            if cache and isinstance(cache._adapter_ext, extension_type):
                completed_result(cache, node, result)
            return result

        return original_execute(
            self,
            dbt_runner,
            node,
            manifest,
            original=completed,
            submit=submit,
            should_confirm=lambda result: may_confirm(
                self._run_cache, extension_type, result, should_confirm
            ),
        )

    runner.RunnerOverride._execute_with_cache = execute
