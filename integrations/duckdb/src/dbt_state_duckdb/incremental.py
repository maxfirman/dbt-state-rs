"""The explicit first-release incremental workload contract."""


def supported_incremental(node):
    import re

    config = node.config
    return (
        str(node.resource_type) == "model"
        and getattr(node, "language", None) == "sql"
        and config.materialized == "incremental"
        and config.get("incremental_strategy") == "delete+insert"
        and isinstance(config.get("unique_key"), str)
        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", config.get("unique_key"))
        is not None
        and config.get("on_schema_change") == "fail"
        and not config.get("predicates")
        and not config.get("incremental_predicates")
    )


def state_workload(node):
    """Unsupported work still executes through the ordinary dbt runner."""
    kind = str(node.resource_type)
    if kind in {"test", "seed"}:
        return True
    if kind != "model" or getattr(node, "language", None) != "sql":
        return False
    materialized = node.config.materialized
    if materialized in {"table", "view"}:
        return True
    from .copy import ctas_satisfies_requirements

    return supported_incremental(node) and ctas_satisfies_requirements(node)


def install_workload_gate(adapter_type):
    from dbt_state.run_cache import RunCache

    for method in ("on_compile", "on_execute", "on_run_result"):
        original = getattr(RunCache, method)

        def gated(self, node, *args, _original=original, _method=method, **kwargs):
            if isinstance(self._adapter_ext, adapter_type):
                if not state_workload(node):
                    return None
                if (
                    node.config.materialized == "incremental"
                    and _method == "on_execute"
                ):
                    import duckdb

                    name = self._adapter_ext._sql(
                        self._node_resolution.node_to_table(node)
                    )
                    with duckdb.connect(str(self._adapter_ext.path)) as db:
                        if not incremental_relation_contract(db, name, node):
                            return None
                if node.config.materialized == "incremental" and any(
                    m.package_name not in {"dbt", "dbt_duckdb"}
                    and (
                        "delete_insert" in m.name
                        or m.name.startswith("materialization_incremental")
                    )
                    for m in self._manifest.macros.values()
                ):
                    return None
            return _original(self, node, *args, **kwargs)

        setattr(RunCache, method, gated)


def initialization_identity(builder, node):
    """Versioned template/config identity available before compilation.

    Dynamic context beyond the supported dbt incremental template primitives is
    not proven equivalent between profiles, even when the template text matches.
    """
    import hashlib
    import json
    import jinja2

    try:
        ast = jinja2.Environment().parse(node.raw_code)
    except jinja2.TemplateError:
        return None
    allowed = {"config", "is_incremental", "this"}
    if any(name.name not in allowed for name in ast.find_all(jinja2.nodes.Name)):
        return None
    if any(ast.find_all((jinja2.nodes.Getattr, jinja2.nodes.Getitem))):
        return None
    # Validate both branches before any profile-based CTAS. An unchanged raw
    # template is insufficient for random(), LIMIT, remote inputs or unknown
    # metadata. Self reads are intentionally separate from external freshness.
    from .views import FUNCTION_CATALOG_SQL, references
    from dbt_common.exceptions import DbtRuntimeError
    import duckdb
    from sqlglot import exp, parse_one

    target = parse_one(node.relation_name, into=exp.Table, dialect="duckdb")
    try:
        _, session_functions = builder._adapter_ext.adapter.execute(
            FUNCTION_CATALOG_SQL, auto_begin=False, fetch=True
        )
        with duckdb.connect(str(builder._adapter_ext.path)) as db:
            function_catalog = (
                list(session_functions.rows)
                + db.execute(FUNCTION_CATALOG_SQL).fetchall()
            )
            for incremental in (False, True):
                sql = (
                    jinja2.Environment()
                    .from_string(node.raw_code)
                    .render(
                        config=lambda *args, **kwargs: "",
                        is_incremental=lambda: incremental,
                        this=node.relation_name,
                    )
                )
                for table in references(sql, function_catalog):
                    if table == target:
                        continue
                    if (
                        table.catalog != builder._adapter_ext.default_catalog
                        or table.db in {"prod", "dev"}
                    ):
                        return None
                    identity = [table.catalog, table.db, table.name]
                    if db.execute(
                        "select table_type from information_schema.tables where table_catalog=? and table_schema=? and table_name=?",
                        identity,
                    ).fetchall() != [("BASE TABLE",)]:
                        return None
                    if (
                        builder._adapter_ext._observe(table.sql(dialect="duckdb"))
                        is None
                    ):
                        return None
    except (ValueError, TypeError, duckdb.Error, jinja2.TemplateError, DbtRuntimeError):
        return None
    config = node.config.to_dict()
    for key in ("alias", "schema", "database", "tags", "group"):
        config.pop(key, None)
    # Hash every supplied macro too: materialization strategy dispatch can invoke
    # macros not listed in the model's direct dependency set before compilation.
    macros = sorted(
        (m.unique_id, m.macro_sql) for m in builder._manifest.macros.values()
    )
    data = [node.unique_id, node.raw_code, config, macros]
    return (
        "duckdb-incremental-init-v2:"
        + hashlib.sha256(
            json.dumps(data, sort_keys=True, default=str).encode()
        ).hexdigest()
    )


def incremental_relation_contract(db, name, node):
    """NULL keys accumulate under delete+insert and are outside this contract.

    Missing targets are ordinary cold builds; inspect existing results before
    allowing State to skip an operation that would otherwise append NULL keys.
    """
    if node.config.materialized != "incremental":
        return True
    from sqlglot import exp, parse_one

    table = parse_one(name, into=exp.Table, dialect="duckdb")
    exists = db.execute(
        "select 1 from information_schema.tables where table_catalog=? and table_schema=? and table_name=? limit 1",
        [table.catalog, table.db, table.name],
    ).fetchone()
    if exists is None:
        return True
    key = node.config.get("unique_key")
    if not supported_incremental(node):
        return False
    try:
        return (
            db.execute(f'SELECT 1 FROM {name} WHERE "{key}" IS NULL LIMIT 1').fetchone()
            is None
        )
    except Exception:
        return False
