"""Pinned extension to the maintained Python State client for explicit local use."""

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
    """Install the pinned client integration for this DuckDB extension."""
    from .client import install as install_client

    install_client(DuckDBAdapterExtension)


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
