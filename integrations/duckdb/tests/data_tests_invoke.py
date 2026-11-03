"""Capture data-test decisions and outcomes through the public State client."""

import json
import os
import sys
from pathlib import Path

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
requests = {}
events = []
submit = QueryCacheGrpcClient.submit_sql
confirm = QueryCacheGrpcClient.confirm_execution


def observe(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    if request.execution_type.value == "DBT_DATA_TEST":
        assert request.target_table is None
        node = request.dbt_node_state
        event = dict(
            uid=node.node_unique_id,
            project=node.project_id,
            namespace=request.table_namespace,
            decision="skip" if isinstance(response, SkipExecutionResponse) else "build",
            results=getattr(response, "execution_results", None),
            confirmed=False,
            epochs=[t.last_modified_epoch for t in request.tables],
        )
        assert isinstance(response, (ReadyToExecuteResponse, SkipExecutionResponse))
        if isinstance(response, ReadyToExecuteResponse):
            requests[response.request_id] = event
        events.append(event)
    return response


def completed(self, request, *args, **kwargs):
    response = confirm(self, request, *args, **kwargs)
    if request.request_id in requests:
        assert request.last_modified_epoch is None and request.table_type is None
        event = requests[request.request_id]
        event["confirmed"] = response.success
        event["results"] = request.execution_results
    return response


QueryCacheGrpcClient.submit_sql = observe
QueryCacheGrpcClient.confirm_execution = completed
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as outcome:
    code = outcome.code or 0
else:
    code = 0
Path(os.environ["TEST_OBSERVATIONS"]).write_text(json.dumps(events, default=str))
sys.exit(code)
