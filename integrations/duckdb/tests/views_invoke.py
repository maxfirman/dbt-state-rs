"""Observe public State decisions during an otherwise ordinary dbt command."""

import json
import os
import sys

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
expected = json.loads(os.environ["VIEW_EXPECT"])
observed = {}
requests = {}
confirmed = set()
submit = QueryCacheGrpcClient.submit_sql
confirm = QueryCacheGrpcClient.confirm_execution


def observe(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    name = request.dbt_node_state.node_unique_id.rsplit(".", 1)[-1]
    if name in expected:
        if os.environ.get("VIEW_UNKNOWN") == "1":
            assert any(
                t.last_modified_epoch is None and t.name != request.target_table
                for t in request.tables
            ), request
        observed[name] = (
            "skip" if isinstance(response, SkipExecutionResponse) else "build"
        )
        assert isinstance(response, (SkipExecutionResponse, ReadyToExecuteResponse)), (
            response
        )
        if isinstance(response, ReadyToExecuteResponse):
            requests[response.request_id] = name
    return response


def confirmation(self, request, *args, **kwargs):
    response = confirm(self, request, *args, **kwargs)
    if request.request_id in requests:
        assert response.success and request.last_modified_epoch is not None
        confirmed.add(requests[request.request_id])
    return response


QueryCacheGrpcClient.submit_sql = observe
QueryCacheGrpcClient.confirm_execution = confirmation
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as exc:
    if exc.code:
        raise
assert observed == expected, (observed, expected)
assert confirmed == {
    name
    for name, outcome in expected.items()
    if outcome == "build"
    and name not in json.loads(os.environ.get("VIEW_UNCONFIRMED", "[]"))
}, confirmed
