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
SQL_CONTRACT = "types-scalars-v5"


def supported_execution_context(adapter_ext):
    settings = adapter_ext.adapter.config.credentials.settings or {}
    if any(str(key).lower() == "default_collation" for key in settings):
        return False
    try:
        # Inspect the actual worker cursor, including plugin session settings.
        # Collation changes can alter literal comparisons with no table inputs.
        # Permissive scalar subqueries can choose by physical scan order, which
        # the unordered visible-column evidence deliberately omits.
        _, result = adapter_ext.adapter.execute(
            "SELECT system.main.current_setting('scalar_subquery_error_on_multiple_rows'), "
            "system.main.current_setting('default_collation')",
            auto_begin=False,
            fetch=True,
        )
        return (
            len(result.rows) == 1
            and result.rows[0][0] is True
            and result.rows[0][1] == ""
        )
    except DbtRuntimeError:
        return False


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
    # Inspect intermediate expressions too: supported leaf/output types do not
    # prove that evaluation ignores mutable type bindings or physical row order.
    for node in query.walk():
        # A named type can be redefined without changing leaf/output evidence
        # (e.g. ENUM ordering used by an intermediate comparison). Check every
        # nested type too, including TRY_CAST and PostgreSQL-style :: casts.
        if (
            isinstance(node, exp.DataType)
            and node.this == exp.DataType.Type.USERDEFINED
        ):
            raise ValueError("Unsupported named type binding")
        # Intermediate timezone-aware casts can produce ordinary VARCHAR/BOOL
        # results whose values depend on worker TimeZone/Calendar settings.
        # Reject the type anywhere in the tree, including nested/TRY_CAST forms.
        if isinstance(node, exp.DataType) and node.this in {
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMETZ,
        }:
            raise ValueError("Unsupported timezone-sensitive cast")
        # Approximate intermediates can make aggregates depend on scan order.
        # // can resolve through DOUBLE for decimal operands. Without resolved
        # operand types, its name does not prove integral arithmetic.
        if isinstance(node, (exp.Div, exp.IntDiv)):
            raise TypeError("Unsupported approximate arithmetic")
        if isinstance(node, exp.DataType) and node.this in {
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
