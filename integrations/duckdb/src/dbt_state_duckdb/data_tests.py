"""Completed data-test invocation handling."""


def completed_invocation(result):
    """A completed failing test is an outcome, unlike an execution error.

    dbt returns success=False for severity=error failures even though every test
    query completed. Preserve its confirmed outcomes across ordinary restarts.
    Any model failure, SQL error, or absent result still leaves recovery dirty.
    """
    if result[1]:
        return True
    results = getattr(result[0], "results", None)
    if not results:
        return False
    return all(
        str(item.status) in {"success", "pass", "warn", "skipped"}
        or (str(item.node.resource_type) == "test" and str(item.status) == "fail")
        for item in results
    )
