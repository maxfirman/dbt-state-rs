"""Conservative local CTAS capability and actual-outcome lifecycle.

Only table materializations without extra property requirements qualify. The
server keeps physical base names; this client inspects the actual source rather
than treating a table label as proof that CTAS preserves its contract.
"""

from contextvars import ContextVar

import duckdb
from sqlglot import exp, parse_one

_SOURCE = ContextVar("duckdb_copy_source", default=None)


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


def inspected_table(db, name):
    table = parse_one(name, into=exp.Table, dialect="duckdb")
    if not all((table.catalog, table.db, table.name)):
        return False
    identity = [table.catalog, table.db, table.name]
    kinds = db.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?",
        identity,
    ).fetchall()
    if kinds != [("BASE TABLE",)]:
        return False
    if db.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE database_name=? AND schema_name=? AND table_name=?",
        identity,
    ).fetchone()[0]:
        return False
    if db.execute(
        "SELECT count(*) FROM duckdb_indexes() WHERE database_name=? AND schema_name=? AND table_name=?",
        identity,
    ).fetchone()[0]:
        return False
    columns = db.execute("DESCRIBE " + name).fetchall()
    # CTAS preserves only data and column names/types, not defaults or NOT NULL.
    return bool(columns) and all(
        col[2] == "YES" and col[3] is None and col[4] is None for col in columns
    )


class TableCopy:
    IMPLEMENTS_CUSTOM_CLONE = True

    def commit_if_open(self):
        # Upstream forces transaction_open=True for Snowflake's autocommit DDL.
        # DuckDB rejects COMMIT without BEGIN; our CTAS connection committed on
        # close and must not invent a transaction on dbt's worker connection.
        connection = self.adapter.connections.get_thread_connection()
        if connection.transaction_open:
            self.adapter.commit_if_has_connection()

    def get_relation_table_type(self, node, relation):
        name = self._sql(self._to_fqn(relation.render()))
        with duckdb.connect(str(self.path)) as db:
            from .incremental import incremental_relation_contract

            if (
                ctas_satisfies_requirements(node)
                and inspected_table(db, name)
                and incremental_relation_contract(db, name, node)
            ):
                return "DUCKDB CTAS TABLE"
        return None

    def clone(self, clone_sqls, clone_source, clone_target):
        source = parse_one(clone_source, into=exp.Table, dialect="duckdb")
        target = parse_one(clone_target, into=exp.Table, dialect="duckdb")
        if source.catalog != self.default_catalog or target.catalog != source.catalog:
            raise ValueError("Copy requires one local database")
        if (
            source.db not in {"prod", "dev"}
            or target.db not in {"prod", "dev"}
            or source.db == target.db
            or target.db != self.adapter.config.credentials.schema
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
        with _LOCK, duckdb.connect(str(self.path)) as db:
            db.execute("BEGIN TRANSACTION")
            if (
                not inspected_table(db, clone_source)
                or self.evidence is None
                or self.evidence.observe_snapshot(db, clone_source) != provenance[2]
            ):
                db.execute("ROLLBACK")
                raise ValueError("Source no longer matches recorded copy provenance")
            db.execute(expected)
            db.execute("COMMIT")
        # CTAS committed before the ordinary client's confirmation runs.
        self.complete_write(target)


def install_copy(adapter_type):
    from dbt_state.request_builder import RequestBuilder
    from dbt_state.run_cache import NoRunResult, RunCache
    from query_cache_common.models.services.clone_service_models import (
        ReadyToCloneResponse,
    )

    original_build = RequestBuilder.build_submit_enriched_sql_request

    def build(self, node, *args, **kwargs):
        request, duration = original_build(self, node, *args, **kwargs)
        if isinstance(self._adapter_ext, adapter_type):
            if ctas_satisfies_requirements(node):
                request.semantic_extras["duckdb_copy_contract"] = "rows-schema-v1"
                if node.config.materialized == "incremental":
                    from .incremental import initialization_identity

                    identity = initialization_identity(self, node)
                    request.dbt_node_state.node_hash = (
                        identity or "duckdb-incremental-no-init"
                    )
                    request.semantic_extras["duckdb_incremental_contract"] = (
                        "delete-insert-fixed-v1"
                    )
            else:
                # Even an existing compatible source may not satisfy this node.
                request.clone_chain_depth_limit = 0
        return request, duration

    RequestBuilder.build_submit_enriched_sql_request = build
    original_clone_request = RequestBuilder.build_clone_request

    def clone_request(self, node, *args, **kwargs):
        request = original_clone_request(self, node, *args, **kwargs)
        if isinstance(self._adapter_ext, adapter_type):
            from .incremental import supported_incremental

            if supported_incremental(node) and ctas_satisfies_requirements(node):
                from .incremental import initialization_identity

                identity = initialization_identity(self, node)
                if identity:
                    request.labels["duckdb_incremental_contract"] = (
                        "delete-insert-fixed-v1"
                    )
                    request.labels["duckdb_incremental_identity"] = identity
                else:
                    request.clone_chain_depth_limit = 0
            else:
                request.clone_chain_depth_limit = 0
        return request

    RequestBuilder.build_clone_request = clone_request
    original_process = RunCache._process_query_cache_response

    def process(self, node, response):
        if isinstance(self._adapter_ext, adapter_type) and isinstance(
            response, ReadyToCloneResponse
        ):
            expected = response.clone_required_last_modified_epoch
            actual = self._adapter_ext.get_last_modified_epoch([response.clone_source])
            if expected is None or actual.get(response.clone_source) != expected:
                return NoRunResult(response.request_id, failed_to_clone=True)
            if response.clone_target != self._adapter_ext._sql(
                self._node_resolution.node_to_table(node)
            ):
                return NoRunResult(response.request_id, failed_to_clone=True)
            token = _SOURCE.set(
                (response.clone_source, response.clone_target, expected)
            )
            try:
                return original_process(self, node, response)
            finally:
                _SOURCE.reset(token)
        return original_process(self, node, response)

    RunCache._process_query_cache_response = process

    # Profile-based initialization happens during compilation, before SQL exists.
    # It bypasses _process_query_cache_response in the upstream client.
    original_try_clone = RunCache._try_clone

    def try_clone(self, node):
        if not isinstance(self._adapter_ext, adapter_type):
            return original_try_clone(self, node)
        if (
            not ctas_satisfies_requirements(node)
            or self._dev_cloner is None
            or self.is_write_only
        ):
            return False
        response = self._submit_clone_request(node)
        if not isinstance(response, ReadyToCloneResponse):
            return False
        expected = response.clone_required_last_modified_epoch
        target = self._adapter_ext._sql(self._node_resolution.node_to_table(node))
        if expected is None or response.clone_target != target:
            return False
        token = _SOURCE.set((response.clone_source, response.clone_target, expected))
        try:
            self._dev_cloner.clone(
                self._adapter,
                node,
                response.clone_sqls,
                response.clone_source,
                response.clone_target,
            )
            self._adapter_ext.commit_if_open()
            self.confirm_execution(node, response.request_id)
            self._decision_logger.log_dev_clone(
                node.name, response.clone_source, response.clone_target
            )
            return True
        except Exception:
            # Leave registration pending: it has no submitted transformation
            # that could certify an ordinary fallback as the source's logic.
            # The following SQL submission records the actual build instead.
            from .recovery import uncertain

            uncertain()
            return False
        finally:
            _SOURCE.reset(token)

    RunCache._try_clone = try_clone
