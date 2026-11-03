"""Physical view provenance and uncached traversal for the local DuckDB subset.

View ordinals describe observed definition/schema changes and known DDL writes;
they never claim to timestamp the data returned by querying a view.
"""

import hashlib

import duckdb
from dbt_state.adapters.common import ViewDefinition, ViewTraversalResult
from dbt_state.errors import AdapterExtensionError
from dbt_state.utils import find_tables
from sqlglot import exp, parse, parse_one
from sqlglot.errors import SqlglotError


def references(sql):
    try:
        statements = parse(sql, dialect="duckdb")
    except SqlglotError as exc:
        raise ValueError("Unsupported view syntax") from exc
    if len(statements) != 1:
        raise ValueError("Expected one view query")
    query = statements[0]
    if isinstance(query, exp.Create):
        query = query.expression
    if not isinstance(query, (exp.Select, exp.Union)):
        raise TypeError("Unsupported view query")
    # Table-valued functions, remote inputs and unqualified search-path lookup
    # require resolution semantics beyond this initial local-file contract.
    tables = find_tables(query)
    for table in tables:
        if not isinstance(table.this, exp.Identifier) or not table.db:
            raise ValueError("Unsupported or ambiguous view reference")
    safe_functions = {
        "CAST",
        "TRY_CAST",
        "SUM",
        "COUNT",
        "AVG",
        "MIN",
        "MAX",
        "CONCAT",
        "CHR",
        "UNHEX",
        "COALESCE",
        "NULLIF",
        "LOWER",
        "UPPER",
    }
    if any(
        isinstance(node, exp.Func) and node.sql_name() not in safe_functions
        for node in query.walk()
    ):
        raise ValueError("Unsupported function in view query")
    return tables


def physical_view(db, table):
    relation = parse_one(table, into=exp.Table, dialect="duckdb")
    rows = db.execute(
        "SELECT sql FROM duckdb_views() WHERE database_name=? AND schema_name=? AND view_name=?",
        [relation.catalog, relation.db, relation.name],
    ).fetchall()
    if len(rows) != 1 or not rows[0][0]:
        raise ValueError("Missing physical view definition")
    sql = rows[0][0]
    references(sql)
    schema = db.execute("DESCRIBE SELECT * FROM " + table).fetchall()
    return sql, schema


def view_fingerprint(db, table):
    from .evidence import frame

    sql, schema = physical_view(db, table)
    schema_bytes = b"".join(
        frame(str(value).encode()) for col in schema for value in col
    )
    return (
        hashlib.sha256(b"duckdb-view-schema-v1" + schema_bytes).hexdigest(),
        hashlib.sha256(b"duckdb-view-definition-v1" + sql.encode()).hexdigest(),
    )


class ViewMetadata:
    def cache_view_definition(self, *args, **kwargs):
        # Compiled SQL is not evidence of what committed to the warehouse.
        pass

    def get_available_last_modified_epochs(self, tables):
        # Speculative callers must still observe current committed evidence.
        return self.get_last_modified_epoch(tables)

    def traverse_view_definitions(self, sql_or_tables, terminal_fqns=None):
        definitions, leaves, unknown = {}, set(), set()
        marker = self._sql(
            exp.table_(
                "unresolved_view",
                db="_dbt_state",
                catalog=self.default_catalog,
                quoted=True,
            )
        )
        try:
            tables = (
                references(sql_or_tables)
                if isinstance(sql_or_tables, str)
                else sql_or_tables
            )
            with duckdb.connect(str(self.path)) as db:
                db.execute("BEGIN TRANSACTION")
                active, visited = set(), set()

                def visit(raw):
                    table = self._to_fqn(raw)
                    name = self._sql(table)
                    if name in active:
                        raise ValueError("Cyclic view dependencies")
                    if name in visited:
                        return
                    visited.add(name)
                    kind = db.execute(
                        "SELECT table_type FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?",
                        [table.catalog, table.db, table.name],
                    ).fetchall()
                    if kind == [("BASE TABLE",)]:
                        leaves.add(name)
                    elif kind == [("VIEW",)]:
                        sql, _ = physical_view(db, name)
                        active.add(name)
                        for dependency in references(sql):
                            visit(dependency)
                        active.remove(name)
                        definitions[name] = ViewDefinition(
                            fqn=name,
                            definition=sql,
                            dialect="duckdb",
                            default_catalog=table.catalog,
                            default_schema=table.db,
                        )
                    else:
                        leaves.add(name)
                        unknown.add(name)

                for table in tables:
                    visit(table)
                db.execute("COMMIT")
        except (
            duckdb.Error,
            SqlglotError,
            AdapterExtensionError,
            ValueError,
            TypeError,
        ):
            # Preserve an explicit unknown input even when parsing yielded no
            # usable relation. No empty-input request may certify this query.
            leaves.add(marker)
            unknown.add(marker)
        return ViewTraversalResult(
            view_definitions=definitions,
            seen_tables=leaves,
            unresolvable_tables=unknown,
        )


def install_view_requests(adapter_type):
    from dbt_state.request_builder import RequestBuilder
    from query_cache_common.models import shared_models

    original = RequestBuilder.build_submit_enriched_sql_request

    def build(
        self,
        node,
        sql,
        execution_type,
        traversal_result,
        microbatch_window=None,
        speculative=False,
    ):
        local = isinstance(self._adapter_ext, adapter_type)
        if local:
            speculative = False
        request, duration = original(
            self,
            node,
            sql,
            execution_type,
            traversal_result,
            microbatch_window,
            speculative,
        )
        if local:
            request.default_schema = self._adapter_ext.adapter.config.credentials.schema
            traversal = self._adapter_ext.traverse_view_definitions(sql)
            names = traversal.seen_tables | (
                {request.target_table} if request.target_table else set()
            )
            epochs = self._adapter_ext.get_last_modified_epoch(names)
            request.tables = [
                shared_models.TableModifiedInfo(
                    name=name,
                    last_modified_epoch=None
                    if name in traversal.unresolvable_tables
                    else epoch,
                )
                for name, epoch in epochs.items()
            ]
            request.query_dependencies = [
                shared_models.QueryDependency(
                    name=name,
                    query=view.definition,
                    default_catalog=view.default_catalog,
                    default_schema=view.default_schema,
                )
                for name, view in traversal.view_definitions.items()
            ]
        return request, duration

    RequestBuilder.build_submit_enriched_sql_request = build
