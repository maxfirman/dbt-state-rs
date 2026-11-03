"""Observe the real SQL/confirmation boundary while running ordinary dbt."""

import json
import os
import sys
from pathlib import Path

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.clone_service_models import ReadyToCloneResponse
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
if os.environ.get("COPY_FAILURE") == "race":
    from dbt_state_duckdb import DuckDBAdapterExtension

    original_clone = DuckDBAdapterExtension.clone

    def racing_clone(self, statements, source, target):
        # Simulate another invocation worker writing after the early source
        # check, before the transaction that consumes its contents.
        import duckdb

        with duckdb.connect(os.environ["WAREHOUSE_PATH"]) as db:
            db.execute("UPDATE " + source + " SET amount=999")
        return original_clone(self, statements, source, target)

    DuckDBAdapterExtension.clone = racing_clone
observed = []
confirmations = []
submit = QueryCacheGrpcClient.submit_sql
confirm = QueryCacheGrpcClient.confirm_execution


def observe(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    observed.append(response)
    if isinstance(response, ReadyToCloneResponse):
        assert response.clone_target == request.target_table
        assert response.clone_source != response.clone_target
        assert response.clone_required_last_modified_epoch is not None
        if os.environ.get("COPY_FAILURE"):
            # Fault at the warehouse boundary, after selection and before copy.
            import duckdb

            source = response.clone_source
            with duckdb.connect(os.environ["WAREHOUSE_PATH"]) as db:
                if os.environ["COPY_FAILURE"] == "drop":
                    db.execute("DROP TABLE " + source)
                elif os.environ["COPY_FAILURE"] == "edit":
                    db.execute("UPDATE " + source + " SET amount = 999")
                elif os.environ["COPY_FAILURE"] == "property":
                    db.execute("CREATE INDEX copy_source_index ON " + source + "(id)")
                elif os.environ["COPY_FAILURE"] == "default":
                    db.execute(
                        "ALTER TABLE " + source + " ALTER amount SET DEFAULT 999"
                    )
                elif os.environ["COPY_FAILURE"] == "constraint":
                    db.execute("ALTER TABLE " + source + " ALTER id SET NOT NULL")
                elif os.environ["COPY_FAILURE"] == "sql":
                    response.clone_sqls = ["deliberately unsupported copy"]
    return response


def confirmation(self, request, *args, **kwargs):
    response = confirm(self, request, *args, **kwargs)
    assert response.success
    confirmations.append(request)
    return response


QueryCacheGrpcClient.submit_sql = observe
QueryCacheGrpcClient.confirm_execution = confirmation
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as result:
    if result.code:
        raise
assert len(observed) == 1, observed
kind = {
    ReadyToExecuteResponse: "build",
    SkipExecutionResponse: "skip",
    ReadyToCloneResponse: "copy",
}[type(observed[0])]
assert kind == os.environ["EXPECT_DECISION"], (kind, observed)
assert len(confirmations) == (0 if kind == "skip" else 1), confirmations
if confirmations:
    assert confirmations[0].failed_to_clone == bool(os.environ.get("COPY_FAILURE"))
    assert confirmations[0].last_modified_epoch is not None
with Path(os.environ["COPY_OBSERVATIONS"]).open("a") as out:
    out.write(
        json.dumps({"decision": kind, "fallback": bool(os.environ.get("COPY_FAILURE"))})
        + "\n"
    )
