"""Physical view provenance and uncached traversal for the local DuckDB subset.

View ordinals describe observed definition/schema changes and known DDL writes;
they never claim to timestamp the data returned by querying a view.
"""

import hashlib

import duckdb
from dbt_common.exceptions import DbtRuntimeError
from dbt_state.adapters.common import ViewDefinition, ViewTraversalResult
from dbt_state.errors import AdapterExtensionError
from dbt_state.utils import find_tables
from sqlglot import exp, parse, parse_one
from sqlglot.errors import SqlglotError

FUNCTION_CATALOG_SQL = (
    "SELECT function_name, internal FROM system.main.duckdb_functions()"
)


def references(sql, function_catalog=None):
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
    # Hidden rowid exposes physical position, which unordered visible-column
    # fingerprints deliberately do not track. Reject qualified/quoted reads
    # too; aliases and string literals called rowid are not column reads.
    if any(
        isinstance(node, exp.Column) and node.name.lower() == "rowid"
        for node in query.walk()
    ):
        raise ValueError("Unsupported physical rowid reference")
    # Table-valued functions, remote inputs and unqualified search-path lookup
    # require resolution semantics beyond this initial local-file contract.
    if any(
        isinstance(
            node, (exp.Limit, exp.Offset, exp.TableSample, exp.Window, exp.Collate)
        )
        for node in query.walk()
    ):
        raise ValueError("Unsupported order-sensitive view query")
    if any(node.args.get("on") for node in query.find_all(exp.Distinct)):
        raise ValueError("Unsupported DISTINCT ON view query")
    # Approximate intermediates can make an aggregate depend on scan order,
    # even if every physical leaf column and the final result are exact types.
    for node in query.walk():
        if isinstance(node, exp.Div):
            raise TypeError("Unsupported approximate arithmetic")
        if isinstance(node, exp.Cast) and node.args["to"].this in {
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DOUBLE,
        }:
            raise ValueError("Unsupported approximate cast")
        if (
            isinstance(node, exp.Literal)
            and node.is_number
            and "e" in node.this.lower()
        ):
            raise ValueError("Unsupported approximate numeric literal")
    if any(join.args.get("method") for join in query.find_all(exp.Join)):
        raise ValueError("Unsupported order-dependent join method")
    tables = find_tables(query)
    for table in tables:
        if not isinstance(table.this, exp.Identifier) or not table.db:
            raise ValueError("Unsupported or ambiguous view reference")
    safe_functions = {
        "CAST",
        "TRY_CAST",
        "SUM",
        "COUNT",
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
    functions = {node.sql_name() for node in query.walk() if isinstance(node, exp.Func)}
    if not functions <= safe_functions:
        raise ValueError("Unsupported function in view query")
    if function_catalog is None:
        raise ValueError("Missing function binding evidence")
    internal, uncertain = set(), set()
    for name, builtin in function_catalog:
        (internal if builtin else uncertain).add(name.upper())
    # DuckDB resolves operators (including + and ||) through mutable function
    # bindings too. The AST's Func nodes do not enumerate every implicit call.
    # Conservatively reject every query if any built-in name is overridden,
    # regardless of current search path, overload signature or apparent usage.
    if internal & uncertain:
        raise ValueError("Overridden built-in function or operator")
    # CAST/TRY_CAST/COALESCE are parser constructs, absent from the function
    # catalog. Every callable name needs current built-in evidence.
    intrinsic = {"CAST", "TRY_CAST", "COALESCE"}
    if functions & uncertain or not functions <= internal | intrinsic:
        raise ValueError("Uncertain function binding in view query")
    return tables


def physical_view(db, table):
    relation = parse_one(table, into=exp.Table, dialect="duckdb")
    rows = db.execute(
        "SELECT sql FROM system.main.duckdb_views() WHERE database_name=? AND schema_name=? AND view_name=?",
        [relation.catalog, relation.db, relation.name],
    ).fetchall()
    if len(rows) != 1 or not rows[0][0]:
        raise ValueError("Missing physical view definition")
    sql = rows[0][0]
    references(sql, db.execute(FUNCTION_CATALOG_SQL).fetchall())
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
            # The real worker cursor includes temporary macros and plugin UDFs
            # invisible to a separately opened evidence connection.
            _, session_functions = self.adapter.execute(
                FUNCTION_CATALOG_SQL, auto_begin=False, fetch=True
            )
            with duckdb.connect(str(self.path)) as db:
                db.execute("BEGIN TRANSACTION")
                function_catalog = (
                    list(session_functions.rows)
                    + db.execute(FUNCTION_CATALOG_SQL).fetchall()
                )
                tables = (
                    references(sql_or_tables, function_catalog)
                    if isinstance(sql_or_tables, str)
                    else sql_or_tables
                )
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
                        for dependency in references(sql, function_catalog):
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
            DbtRuntimeError,
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
            from .hooks import semantic_contract, supported_hooks
            from .incremental import reusable_self_read

            request.semantic_extras.update(semantic_contract(node))
            request.default_schema = self._adapter_ext.adapter.config.credentials.schema
            traversal = self._adapter_ext.traverse_view_definitions(sql)
            settings = self._adapter_ext.adapter.config.credentials.settings or {}
            # State excludes the target from upstream freshness. Its latest
            # output certifies reuse only when another execution is a no-op.
            unsafe_self_read = (
                node.config.materialized == "incremental"
                and request.target_table in traversal.seen_tables
                and (
                    traversal.view_definitions
                    or not reusable_self_read(sql, request.target_table)
                )
            )
            if any(
                str(key).lower() == "default_collation" for key in settings
            ) or not supported_hooks(node) or unsafe_self_read:
                marker = self._adapter_ext._sql(
                    exp.table_(
                        "unsupported_execution_context",
                        db="_dbt_state",
                        catalog=self._adapter_ext.default_catalog,
                        quoted=True,
                    )
                )
                traversal.seen_tables.add(marker)
                traversal.unresolvable_tables.add(marker)
            names = traversal.seen_tables | (
                {request.target_table} if request.target_table else set()
            )
            epochs = self._adapter_ext.get_last_modified_epoch(names)
            request.tables = [
                shared_models.TableModifiedInfo(
                    name=name,
                    last_modified_epoch=(
                        None if name in traversal.unresolvable_tables else epoch
                    ),
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

    original_values = RequestBuilder.build_submit_values_request

    def values(self, node):
        request = original_values(self, node)
        if isinstance(self._adapter_ext, adapter_type):
            from .hooks import hook_config, semantic_contract

            request.semantic_extras.update(semantic_contract(node))
            # Seed hooks have no validated local execution subset yet. Retain
            # ordinary BUILD and never certify their current target for reuse.
            if (
                any(hook_config(node))
                or self._adapter_ext.traverse_view_definitions(
                    "SELECT 1"
                ).unresolvable_tables
            ):
                request.last_modified_epoch = None
        return request

    RequestBuilder.build_submit_values_request = values
