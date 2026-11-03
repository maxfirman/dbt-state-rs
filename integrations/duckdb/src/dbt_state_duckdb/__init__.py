"""Pinned extension to the maintained Python State client for explicit local use."""

import hashlib
import os
import sys
import uuid
from pathlib import Path

import duckdb
from dbt_state.adapters.base import BaseAdapterExtension


class DuckDBAdapterExtension(BaseAdapterExtension):
    """Local table observations; unknown evidence never receives invented epochs."""

    CLONE_CHAIN_DEPTH_LIMIT = 0
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
            self.table_namespace = self._file_namespace()
            from .evidence import EvidenceStore

            self.evidence = EvidenceStore(self.path)
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
            evidence_tables = metadata.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='_dbt_state' AND table_name IN ('evidence_clock','relation_evidence')"
            ).fetchall()
            if not evidence_tables:
                # Missing evidence cannot restart its sequence in an old scope.
                metadata.execute(
                    "UPDATE _dbt_state.database_identity SET generation=? WHERE id=1",
                    [str(uuid.uuid4())],
                )
            elif len(evidence_tables) != 2:
                raise ValueError("Incomplete State evidence metadata")
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

    def _observe(self, table, *, completed_write=False):
        if self.evidence is None or (
            table in self._uncertain_writes and not completed_write
        ):
            return None
        try:
            return self.evidence.observe(table, completed_write=completed_write)
        except (duckdb.Error, OSError, ValueError, TypeError):
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

    # Materializations return after commit and both inside/outside post-hooks.
    # Wrap the original execution, including upstream's transport-failure bypass,
    # so completion evidence exists before on_run_result and confirmation.
    original_execute = runner.RunnerOverride._execute_with_cache

    def execute(self, dbt_runner, node, manifest, *, original, submit, should_confirm):
        def completed(*args, **kwargs):
            result = original(*args, **kwargs)
            cache = self._run_cache_get_or_create(
                dbt_runner.config, dbt_runner.adapter, manifest
            )
            if (
                cache
                and isinstance(cache._adapter_ext, DuckDBAdapterExtension)
                and str(result.status) == "success"
            ):
                cache._adapter_ext.complete_write(
                    cache._node_resolution.node_to_table(node)
                )
            return result

        return original_execute(
            self,
            dbt_runner,
            node,
            manifest,
            original=completed,
            submit=submit,
            should_confirm=should_confirm,
        )

    runner.RunnerOverride._execute_with_cache = execute


def main():
    install()
    from dbt.cli.main import cli

    cli(args=sys.argv[1:])
