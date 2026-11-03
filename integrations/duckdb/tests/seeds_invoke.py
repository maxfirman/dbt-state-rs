"""Trace the public values/SQL lifecycle without replacing warehouse execution."""

import json
import os
import sys

# Fault at the real dbt runner boundary: warehouse execution completes, but the
# adapter reports an unsuccessful result. Existing target evidence must not be
# mistaken for a successful outcome by either confirm or write-only recording.
if os.environ.get("SEED_RETURN_ERROR") == "1":
    from dbt.task.seed import SeedRunner
    from dbt.task.run import ModelRunner
    from dbt.contracts.results import RunStatus

    execute = ModelRunner.execute

    def unsuccessful(self, *args, **kwargs):
        result = execute(self, *args, **kwargs)
        if isinstance(self, SeedRunner):
            result.status = RunStatus.Error
            result.message = (
                "Injected unsuccessful seed result after warehouse execution"
            )
        return result

    ModelRunner.execute = unsuccessful
from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
expected = json.loads(os.environ["SEED_EXPECT"])
observed, pending, confirmed = {}, {}, set()
for method in ("submit_values", "submit_sql"):
    original = getattr(QueryCacheGrpcClient, method)

    def observe(self, request, *args, _original=original, **kwargs):
        response = _original(self, request, *args, **kwargs)
        name = request.dbt_node_state.node_unique_id.rsplit(".", 1)[-1]
        if name in expected:
            assert isinstance(
                response, (ReadyToExecuteResponse, SkipExecutionResponse)
            ), response
            observed[name] = (
                "skip" if isinstance(response, SkipExecutionResponse) else "build"
            )
            if isinstance(response, ReadyToExecuteResponse):
                pending[response.request_id] = name
        return response

    setattr(QueryCacheGrpcClient, method, observe)
published = []
original_record = QueryCacheGrpcClient.record_executions


def record(self, request, *args, **kwargs):
    published.extend(request.records)
    return original_record(self, request, *args, **kwargs)


QueryCacheGrpcClient.record_executions = record
original_confirm = QueryCacheGrpcClient.confirm_execution


def confirm(self, request, *args, **kwargs):
    response = original_confirm(self, request, *args, **kwargs)
    if request.request_id in pending:
        assert response.success and request.last_modified_epoch is not None
        confirmed.add(pending[request.request_id])
    return response


QueryCacheGrpcClient.confirm_execution = confirm
from dbt.cli.main import cli

exit_code = 0
try:
    cli(args=sys.argv[1:])
except SystemExit as exc:
    exit_code = exc.code or 0
assert bool(exit_code) == bool(int(os.environ.get("SEED_FAIL", "0"))), exit_code
assert observed == expected, (observed, expected)
assert confirmed == {n for n, v in expected.items() if v == "build"} - set(
    json.loads(os.environ.get("SEED_UNCONFIRMED", "[]"))
), confirmed

assert not published, published
