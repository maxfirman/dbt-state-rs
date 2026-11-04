"""Independent controlled-fixture oracle: no State, evidence, or hash imports.

The workload is integer addition over this declared multiset. Expected rows
are calculated from its declaration, never from the warehouse under test.
"""
import duckdb


class OracleViolation(AssertionError):
    pass


def inputs(revision):
    return [(1, 10 + revision), (2, 20), (2, 20), (3, None)]


def expected(revision, increment):
    return {
        'schema': [['id', 'INTEGER'], ['amount', 'INTEGER']],
        'rows': [[key, None if value is None else value + increment]
                 for key, value in inputs(revision)],
    }


def snapshot(path, relation):
    with duckdb.connect(str(path)) as db:
        try:
            schema = [[row[0], row[1]] for row in db.execute(
                'DESCRIBE SELECT * FROM ' + relation).fetchall()]
            rows = [list(row) for row in db.execute('SELECT * FROM ' + relation).fetchall()]
        except duckdb.CatalogException:
            return None
    return {'schema': schema, 'rows': rows}


def canonical(rows):
    return sorted(rows, key=repr)


def assert_snapshot(actual, allowed, context):
    if actual is None:
        raise OracleViolation(context + ':missing')
    schemas = [item for item in allowed if item['schema'] == actual['schema']]
    if not schemas:
        raise OracleViolation(context + ':schema')
    if not any(canonical(item['rows']) == canonical(actual['rows']) for item in schemas):
        raise OracleViolation(context + ':rows')
