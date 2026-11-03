"""Fault injection only; decisions and warehouse execution use real dbt/RPCs."""

import json
import os
import runpy
from pathlib import Path

import duckdb
from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb.evidence import EvidenceStore
from query_cache_common.models.services.sql_service_models import ReadyToExecuteResponse

phase = os.environ["INTERRUPT_PHASE"]
marker = Path(os.environ["INTERRUPT_MARKER"])
submit = QueryCacheGrpcClient.submit_sql


def submitted(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    if request.dbt_node_state.node_unique_id == "model.duckdb_smoke.totals":
        assert isinstance(response, ReadyToExecuteResponse), response
        marker.write_text(
            json.dumps(
                {
                    "namespace": request.table_namespace,
                    "request_id": response.request_id,
                }
            )
        )
        if phase == "before-execution":
            os._exit(74)
    return response


QueryCacheGrpcClient.submit_sql = submitted
observe = EvidenceStore.observe
connect = duckdb.connect
persisting_completion = False


class InterruptedConnection:
    """Delegate real DuckDB work, then interrupt its open metadata transaction."""

    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.connection.__enter__()
        return self

    def __exit__(self, *args):
        return self.connection.__exit__(*args)

    def execute(self, sql, *args, **kwargs):
        result = self.connection.execute(sql, *args, **kwargs)
        if sql.startswith("INSERT OR REPLACE INTO _dbt_state.relation_evidence"):
            os._exit(74)
        return result


def connecting(*args, **kwargs):
    connection = connect(*args, **kwargs)
    if phase == "during-evidence" and persisting_completion:
        return InterruptedConnection(connection)
    return connection


duckdb.connect = connecting


def evidence(self, table, *, completed_write=False):
    if completed_write and phase == "before-evidence":
        os._exit(74)
    global persisting_completion
    persisting_completion = completed_write
    try:
        result = observe(self, table, completed_write=completed_write)
    finally:
        persisting_completion = False
    if completed_write and phase == "after-evidence":
        os._exit(74)
    return result


EvidenceStore.observe = evidence
if phase == "before-confirm":

    def interrupted_confirm(*args, **kwargs):
        os._exit(74)

    QueryCacheGrpcClient.confirm_execution = interrupted_confirm

runpy.run_path(str(Path(__file__).with_name("invoke.py")), run_name="__main__")
