"""Physical CTAS capability and pinned Result copy entry adapters.

Only table materializations without extra property requirements qualify. The
server keeps physical base names; this client inspects the actual source rather
than treating a table label as proof that CTAS preserves its contract.
"""

import duckdb
from sqlglot import exp, parse_one

from .qualification import ctas_satisfies_requirements


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
    from dbt_state.run_cache import RunCache
    from .result_copy import ResultCopy

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
