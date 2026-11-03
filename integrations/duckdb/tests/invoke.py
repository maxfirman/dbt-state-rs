"""Observe real public client responses while running the normal dbt CLI."""

import sys

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import ReadyToExecuteResponse

install()
observed = []
submit = QueryCacheGrpcClient.submit_sql


def observe(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    if request.dbt_node_state.node_unique_id == "model.duckdb_smoke.totals":
        observed.append(response)
        assert request.table_namespace
        assert all(table.last_modified_epoch is None for table in request.tables)
    return response


QueryCacheGrpcClient.submit_sql = observe
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as result:
    if result.code:
        raise
assert len(observed) == 1, "State must return a model decision through its public RPC"
assert isinstance(observed[0], ReadyToExecuteResponse), observed
