"""Pinned extension to the maintained Python State client for explicit local use."""

import hashlib
import os
import sys
import uuid
from pathlib import Path

import duckdb
from dbt_state.adapters.base import BaseAdapterExtension


class DuckDBAdapterExtension(BaseAdapterExtension):
    """Cold execution support. Unknown evidence never receives invented epochs."""

    CLONE_CHAIN_DEPTH_LIMIT = 0
    SYSTEM_METADATA_SCHEMAS = ("information_schema", "_dbt_state")

    def __init__(self, adapter, **kwargs):
        super().__init__(adapter, **kwargs)
        path = Path(adapter.config.credentials.path).expanduser()
        if str(path) == ":memory:" or "://" in str(path):
            raise ValueError("State local mode requires a persistent local DuckDB file")
        self.path = path.resolve()
        try:
            self.table_namespace = self._file_namespace()
        except (duckdb.Error, OSError, ValueError, TypeError) as exc:
            # Unknown/incompatible metadata must keep ordinary dbt execution
            # available, with no chance of matching earlier physical history.
            from dbt_state import events

            events.fire_warn_event("DuckDB State metadata unavailable: {}", str(exc))
            self.table_namespace = "duckdb-unknown-" + str(uuid.uuid4())

    def _file_namespace(self):
        with duckdb.connect(str(self.path)) as metadata:
            metadata.execute("CREATE SCHEMA IF NOT EXISTS _dbt_state")
            metadata.execute(
                "CREATE TABLE IF NOT EXISTS _dbt_state.database_identity "
                "(id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL, generation VARCHAR NOT NULL)"
            )
            metadata.execute(
                "INSERT INTO _dbt_state.database_identity VALUES (1, 1, ?) ON CONFLICT DO NOTHING",
                [str(uuid.uuid4())],
            )
            rows = metadata.execute(
                "SELECT version, generation FROM _dbt_state.database_identity"
            ).fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                raise ValueError("Unsupported State metadata version")
            generation = str(uuid.UUID(rows[0][1]))
        stat = self.path.stat()
        # Generation handles recreation; path and inode distinguish copied files.
        scope = (
            f"duckdb-file-v1\0{self.path}\0{stat.st_dev}\0{stat.st_ino}\0{generation}"
        )
        return hashlib.sha256(scope.encode()).hexdigest()

    def get_last_modified_epoch(self, tables, table_overrides=None):
        # Do not let the upstream missing-view or loaded_at fallbacks manufacture
        # wall-clock evidence. Durable relation evidence is a separate milestone.
        return {self._sql(self._to_fqn(table)): None for table in tables}

    def current_timestamp_utc(self):
        # A wall clock is not valid freshness evidence for this warehouse.
        raise NotImplementedError("DuckDB freshness requires observed-change evidence")

    def _fetch_last_modified_epochs(self, table_batch):
        return {self._sql(table): None for table in table_batch}

    def _fetch_view_definitions(self, table_batch):
        # View reuse is unsupported until durable evidence and traversal are added.
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

    ADAPTER_EXTENSION_MAPPING["duckdb"] = DuckDBAdapterExtension
    # Upstream's early noninteractive guard predates its insecure transport path.
    # Avoid constructing the hosted auth helper solely in this explicit mode;
    # QueryCacheGrpcClient still chooses the configured insecure channel itself.
    from contextvars import ContextVar

    local_duckdb = ContextVar("local_duckdb", default=False)
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


def main():
    install()
    from dbt.cli.main import cli

    cli(args=sys.argv[1:])
