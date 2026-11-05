"""Versioned local execution contract and a narrow own-result post-hook subset."""

import json

import jinja2
from sqlglot import exp, parse, parse_one
from sqlglot.errors import SqlglotError

LOCAL_CONTRACT = "functions-hooks-v1"


def hook_config(node):
    config = node.config.to_dict()
    return [config.get("pre-hook", []), config.get("post-hook", [])]


def semantic_contract(node):
    # Include empty hooks too: an old outcome may have used a now-removed hook
    # or macro even when current SQL and warehouse evidence look unchanged.
    return {
        "duckdb_local_contract": LOCAL_CONTRACT,
        "duckdb_hooks_v1": json.dumps(hook_config(node), sort_keys=True),
    }


def supported_hooks(node):
    pre, post = hook_config(node)
    if pre:
        # These run after State's inspection and can change session bindings.
        return False
    if not post:
        return True
    try:
        target = parse_one(node.relation_name, into=exp.Table, dialect="duckdb")
        for hook in post:
            template = hook["sql"]
            ast = jinja2.Environment().parse(template)
            # Only {{ this }} is a supported late-rendered context value.
            if any(n.name != "this" for n in ast.find_all(jinja2.nodes.Name)):
                return False
            if any(
                not isinstance(
                    n,
                    (jinja2.nodes.Output, jinja2.nodes.TemplateData, jinja2.nodes.Name),
                )
                for n in ast.find_all(jinja2.nodes.Node)
            ):
                return False
            rendered = (
                jinja2.Environment()
                .from_string(template)
                .render(this=node.relation_name)
            )
            statements = parse(rendered, dialect="duckdb")
            if len(statements) != 1 or not isinstance(statements[0], exp.Update):
                return False
            update = statements[0]
            if update.this != target or any(
                value is not None and key not in {"this", "expressions"}
                for key, value in update.args.items()
            ):
                return False
            for assignment in update.expressions:
                if not isinstance(assignment, exp.EQ) or not isinstance(
                    assignment.this, exp.Column
                ):
                    return False
                # No functions, subqueries, additional relations, context changes
                # or order-sensitive expressions. Exact literal/column arithmetic
                # retains the previously tested own-target +1 post-hook.
                allowed = (
                    exp.Column,
                    exp.Identifier,
                    exp.Literal,
                    exp.Null,
                    exp.Boolean,
                    exp.Add,
                    exp.Sub,
                    exp.Mul,
                    exp.Paren,
                    exp.Neg,
                )
                for expression in assignment.walk():
                    if expression is assignment:
                        continue
                    if not isinstance(expression, allowed):
                        return False
                    if isinstance(expression, exp.Column) and (
                        expression.table or expression.name.lower() == "rowid"
                    ):
                        # Hidden physical row position is not column evidence.
                        return False
                    if isinstance(expression, exp.Literal) and expression.is_number:
                        if not expression.this.isdigit():
                            return False
        return True
    except (ValueError, TypeError, KeyError, SqlglotError, jinja2.TemplateError):
        return False
