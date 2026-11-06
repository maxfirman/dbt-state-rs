"""Conservative local CTAS capability and pinned client entry adapters.

Only table materializations without extra property requirements qualify. The
server keeps physical base names; this client inspects the actual source rather
than treating a table label as proof that CTAS preserves its contract.
"""

import duckdb
from sqlglot import exp, parse_one


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
    if (
        db.execute(
            "SELECT 1 FROM system.main.duckdb_constraints() WHERE database_name=? AND schema_name=? AND table_name=? LIMIT 1",
            identity,
        ).fetchone()
        is not None
    ):
        return False
    if (
        db.execute(
            "SELECT 1 FROM system.main.duckdb_indexes() WHERE database_name=? AND schema_name=? AND table_name=? LIMIT 1",
            identity,
        ).fetchone()
        is not None
    ):
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
        from .result_copy import execute_copy

        return execute_copy(self, clone_sqls, clone_source, clone_target)


def install_copy(adapter_type):
    from dbt_state.request_builder import RequestBuilder
    from dbt_state.run_cache import RunCache
    from .result_copy import ResultCopy

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
        if isinstance(self._adapter_ext, adapter_type):
            return ResultCopy(self, node).process(response, original_process)
        return original_process(self, node, response)

    RunCache._process_query_cache_response = process
    original_try_clone = RunCache._try_clone

    def try_clone(self, node):
        if isinstance(self._adapter_ext, adapter_type):
            return ResultCopy(self, node).initialize()
        return original_try_clone(self, node)

    RunCache._try_clone = try_clone
