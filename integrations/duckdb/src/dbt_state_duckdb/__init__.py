"""Pinned extension to the maintained Python State client for explicit local use."""

import os
import sys
import uuid
from pathlib import Path

import duckdb
from dbt_state.adapters.base import BaseAdapterExtension
from .views import ViewMetadata
from .copy import TableCopy


class DuckDBAdapterExtension(TableCopy, ViewMetadata, BaseAdapterExtension):
    """Local table observations; unknown evidence never receives invented epochs."""

    CLONE_CHAIN_DEPTH_LIMIT = 2
    SYSTEM_METADATA_SCHEMAS = ("information_schema", "_dbt_state")

    def __init__(self, adapter, **kwargs):
        super().__init__(adapter, **kwargs)
        path = Path(adapter.config.credentials.path).expanduser()
        if str(path) == ":memory:" or "://" in str(path):
            raise ValueError("State local mode requires a persistent local DuckDB file")
        self.path = path.resolve()
        self.evidence = None
        self._uncertain_writes = set()
        try:
            from .recovery import begin

            self.invocation = begin(self.path)
            self.table_namespace = self.invocation.namespace
            self.invocation.drainers.append(lambda: self._executor.shutdown(wait=True))
            from .evidence import EvidenceStore

            self.evidence = EvidenceStore(self.path)
        except (duckdb.Error, OSError, ValueError, TypeError) as exc:
            # Unknown/incompatible metadata must keep ordinary dbt execution
            # available, with no chance of matching earlier physical history.
            from dbt_state import events

            events.fire_warn_event("DuckDB State metadata unavailable: {}", str(exc))
            self.table_namespace = "duckdb-unknown-" + str(uuid.uuid4())

    def _observe(self, table, *, completed_write=False):
        if self.evidence is None or (
            table in self._uncertain_writes and not completed_write
        ):
            return None
        try:
            return self.evidence.observe(table, completed_write=completed_write)
        except (duckdb.Error, OSError, ValueError, TypeError):
            from .recovery import uncertain

            uncertain()
            return None

    def get_last_modified_epoch(self, tables, table_overrides=None):
        # Bypass upstream epoch futures/caches and loaded_at wall-clock overrides.
        return {
            self._sql(self._to_fqn(table)): (
                self._observe(self._sql(self._to_fqn(table)))
                if not table_overrides
                else None
            )
            for table in tables
        }

    def complete_write(self, table):
        name = self._sql(self._to_fqn(table))
        # An uncertain completion must stay unknown even if a later scan works.
        self._uncertain_writes.add(name)
        self.clear_cache([table])
        if self._observe(name, completed_write=True) is not None:
            self._uncertain_writes.discard(name)
        else:
            from .recovery import uncertain

            uncertain()

    def current_timestamp_utc(self):
        # A wall clock is not valid freshness evidence for this warehouse.
        raise NotImplementedError("DuckDB freshness requires observed-change evidence")

    def _fetch_last_modified_epochs(self, table_batch):
        return {self._sql(table): None for table in table_batch}

    def _fetch_view_definitions(self, table_batch):
        # Unused by DuckDB: ViewMetadata traverses one fresh physical snapshot.
        # Keep the asynchronous upstream hook conservative if called separately.
        from dbt_state.adapters.common import ViewFetchResult

        return ViewFetchResult(
            definitions=[], unresolvable={self._sql(t) for t in table_batch}
        )


def install():
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

    from .data_tests import completed_invocation, install as install_data_tests

    install_data_tests(DuckDBAdapterExtension)
    ADAPTER_EXTENSION_MAPPING["duckdb"] = DuckDBAdapterExtension
    from .views import install_view_requests

    install_view_requests(DuckDBAdapterExtension)
    from .copy import install_copy

    install_copy(DuckDBAdapterExtension)
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
        if isinstance(builder._adapter_ext, DuckDBAdapterExtension):
            return builder._adapter_ext.table_namespace
        return original_namespace(builder)

    RequestBuilder.table_namespace = property(namespace)

    # Materializations return after commit and both inside/outside post-hooks.
    # Wrap the original execution, including upstream's transport-failure bypass,
    # so completion evidence exists before on_run_result and confirmation.
    from .completion import completed_result, install_result_guard, may_confirm

    install_result_guard(DuckDBAdapterExtension)
    original_execute = runner.RunnerOverride._execute_with_cache

    def execute(self, dbt_runner, node, manifest, *, original, submit, should_confirm):
        def completed(*args, **kwargs):
            result = original(*args, **kwargs)
            cache = self._run_cache_get_or_create(
                dbt_runner.config, dbt_runner.adapter, manifest
            )
            if cache and isinstance(cache._adapter_ext, DuckDBAdapterExtension):
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
                self._run_cache, DuckDBAdapterExtension, result, should_confirm
            ),
        )

    runner.RunnerOverride._execute_with_cache = execute


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        import argparse

        from .recovery import reset

        parser = argparse.ArgumentParser(
            description="Reset State evidence without deleting user relations"
        )
        parser.add_argument("--database", required=True)
        reset(parser.parse_args(sys.argv[2:]).database)
        return
    install()
    from dbt.cli.main import cli

    cli(args=sys.argv[1:])
