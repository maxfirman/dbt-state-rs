"""Observe public decisions without substituting execution or decision logic."""
import json
import os
import sys
from pathlib import Path

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.clone_service_models import ReadyToCloneResponse
from query_cache_common.models.services.sql_service_models import ReadyToExecuteResponse, SkipExecutionResponse

install()
events, pending, confirmations = [], {}, set()
for method in ('submit_sql', 'submit_values', 'register_clone'):
    original = getattr(QueryCacheGrpcClient, method)

    def observe(self, request, *args, _original=original, _method=method, **kwargs):
        response = _original(self, request, *args, **kwargs)
        kind = {ReadyToCloneResponse: 'copy', ReadyToExecuteResponse: 'build', SkipExecutionResponse: 'skip'}.get(type(response), 'rejected')
        node = getattr(request, 'dbt_node_state', None)
        event = dict(method=_method, uid=node.node_unique_id if node else None,
                     target=getattr(request, 'target_table', None), decision=kind,
                     namespace=request.table_namespace)
        events.append(event)
        if kind in ('build', 'copy'):
            pending[response.request_id] = event
        return response

    setattr(QueryCacheGrpcClient, method, observe)
original_confirm = QueryCacheGrpcClient.confirm_execution


def confirm(self, request, *args, **kwargs):
    result = original_confirm(self, request, *args, **kwargs)
    assert result.success
    if request.request_id in pending:
        assert not request.failed_to_clone
        confirmations.add(request.request_id)
        pending[request.request_id]['confirmed'] = True
    return result


QueryCacheGrpcClient.confirm_execution = confirm
from dbt.cli.main import cli
try:
    cli(args=sys.argv[1:])
except SystemExit as exc:
    if exc.code:
        raise
assert set(pending) == confirmations, (pending, confirmations)
Path(os.environ['WORKFLOW_EVENTS']).write_text(json.dumps(events))
