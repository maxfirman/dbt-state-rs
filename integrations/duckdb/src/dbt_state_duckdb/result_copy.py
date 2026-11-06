"""Result copy lifecycle shared by SQL submission and initialization.

The entry path determines completion and failure policy. SQL submission keeps
its pinned client hooks and actual-build fallback; initialization leaves failed
registrations pending and repairs metadata visibility before compilation resumes.
The context variable is private glue for the pinned adapter clone callback.
"""

from contextvars import ContextVar
from enum import Enum, auto

import duckdb
from sqlglot import exp, parse_one
from dbt_state.run_cache import NoRunResult
from query_cache_common.models.services.clone_service_models import ReadyToCloneResponse

from .copy import ctas_satisfies_requirements, inspected_table

_SOURCE = ContextVar("duckdb_copy_source", default=None)


class _Path(Enum):
    SQL = auto()
    INITIALIZATION = auto()


class ResultCopy:
    """Own validation, scoped provenance and path-specific completion/recovery."""

    def __init__(self, cache, node):
        self.cache = cache
        self.node = node
        self.extension = cache._adapter_ext

    def process(self, response, original):
        if not isinstance(response, ReadyToCloneResponse):
            return original(self.cache, self.node, response)
        return self._attempt(response, _Path.SQL, original)

    def initialize(self):
        if (
            not ctas_satisfies_requirements(self.node)
            or self.cache._dev_cloner is None
            or self.cache.is_write_only
        ):
            return False
        response = self.cache._submit_clone_request(self.node)
        if not isinstance(response, ReadyToCloneResponse):
            return False
        return self._attempt(response, _Path.INITIALIZATION)

    def _attempt(self, response, path, original=None):
        failed = (
            NoRunResult(response.request_id, failed_to_clone=True)
            if path is _Path.SQL
            else False
        )
        expected = response.clone_required_last_modified_epoch
        if path is _Path.SQL:
            actual = self.extension.get_last_modified_epoch([response.clone_source])
            if expected is None or actual.get(response.clone_source) != expected:
                return failed
        target = self.extension._sql(self.cache._node_resolution.node_to_table(self.node))
        if expected is None or response.clone_target != target:
            return failed
        token = _SOURCE.set((response.clone_source, target, expected))
        try:
            if path is _Path.SQL:
                # Upstream owns hooks, relation caching, confirmation and its
                # failed-to-copy outcome for the subsequent ordinary execution.
                return original(self.cache, self.node, response)
            self.cache._dev_cloner.clone(
                self.cache._adapter,
                self.node,
                response.clone_sqls,
                response.clone_source,
                target,
            )
            self.extension.commit_if_open()
            # Preserve the pinned client's non-raising None result: transport
            # guards/evidence recovery own uncertainty, not the return value.
            self.cache.confirm_execution(self.node, response.request_id)
            self.cache._decision_logger.log_dev_clone(
                self.node.name, response.clone_source, target
            )
            return True
        except Exception:
            if path is _Path.SQL:
                # Do not broaden upstream's catch scope (e.g. pre-hook errors).
                raise
            self._recover_initialization(target)
            return False
        finally:
            _SOURCE.reset(token)

    def _recover_initialization(self, target):
        # Leave registration pending. A later SQL submission records the actual
        # build rather than certifying fallback execution as the source's logic.
        from .recovery import uncertain

        uncertain()
        # CTAS may have committed while dbt retains an older metadata snapshot.
        connection = self.cache._adapter.connections.get_thread_connection()
        if connection.transaction_open:
            # Pinned dbt-duckdb's wrapper has no rollback() method; the inherited
            # rollback_if_open silently leaves SQL active.
            self.cache._adapter.execute("ROLLBACK", auto_begin=False)
            connection.transaction_open = False
        with duckdb.connect(str(self.extension.path)) as db:
            if inspected_table(db, target):
                self.extension.cache_node_relation(self.node)
        self.extension.clear_cache([target])


def execute_copy(extension, clone_sqls, clone_source, clone_target):
    source = parse_one(clone_source, into=exp.Table, dialect="duckdb")
    target = parse_one(clone_target, into=exp.Table, dialect="duckdb")
    if source.catalog != extension.default_catalog or target.catalog != source.catalog:
        raise ValueError("Copy requires one local database")
    if (
        source.db not in {"prod", "dev"}
        or target.db not in {"prod", "dev"}
        or source.db == target.db
        or target.db != extension.adapter.config.credentials.schema
    ):
        raise ValueError("Unsupported copy environment")
    expected = (
        f"CREATE OR REPLACE TABLE {clone_target} AS SELECT * FROM {clone_source}"
    )
    if list(clone_sqls) != [expected]:
        raise ValueError("Unsupported copy plan")
    from .evidence import _LOCK

    provenance = _SOURCE.get()
    if provenance is None or provenance[:2] != (clone_source, clone_target):
        raise ValueError("Copy requires correlated source provenance")
    with _LOCK, duckdb.connect(str(extension.path)) as db:
        db.execute("BEGIN TRANSACTION")
        if (
            not inspected_table(db, clone_source)
            or extension.evidence is None
            or extension.evidence.observe_snapshot(db, clone_source) != provenance[2]
        ):
            db.execute("ROLLBACK")
            raise ValueError("Source no longer matches recorded copy provenance")
        db.execute(expected)
        db.execute("COMMIT")
    # CTAS committed before the ordinary client's confirmation runs.
    extension.complete_write(target)
