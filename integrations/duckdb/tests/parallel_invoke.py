"""Observe correlated production RPCs while dbt schedules four real workers."""

import inspect
import json
import os
import sys
import threading
import time
from pathlib import Path

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
# A frozen wall clock at the evidence boundary must not collapse known writes.
real_time = time.time


def frozen_time():
    caller = inspect.currentframe().f_back.f_code.co_filename
    return 1000.0 if caller.endswith("dbt_state_duckdb/evidence.py") else real_time()


time.time = frozen_time
expected = json.loads(os.environ["PARALLEL_EXPECT"])
failed = os.environ.get("PARALLEL_FAIL") == "1"
barrier = threading.Barrier(3)
observed = {}
requests = {}
confirmations = {}
workers = set()
submit = QueryCacheGrpcClient.submit_sql
confirm = QueryCacheGrpcClient.confirm_execution


def submitted(self, request, *args, **kwargs):
    response = submit(self, request, *args, **kwargs)
    name = request.dbt_node_state.node_unique_id.rsplit(".", 1)[-1]
    assert isinstance(response, (ReadyToExecuteResponse, SkipExecutionResponse)), (
        response
    )
    observed[name] = {
        "decision": "skip" if isinstance(response, SkipExecutionResponse) else "build",
        "namespace": request.table_namespace,
        "inputs": {
            t.name.rsplit('"', 2)[-2]: t.last_modified_epoch for t in request.tables
        },
    }
    if isinstance(response, ReadyToExecuteResponse):
        requests[response.request_id] = name
    if name.startswith("branch_"):
        workers.add(threading.get_ident())
        # Delay only the public responses: dbt must schedule all independent
        # branches concurrently before any is allowed to execute/confirm.
        barrier.wait(timeout=30)
    return response


def confirmed(self, request, *args, **kwargs):
    response = confirm(self, request, *args, **kwargs)
    name = requests[request.request_id]
    assert response.success and request.last_modified_epoch is not None
    assert request.last_modified_epoch > max(
        (v or 0 for v in observed[name]["inputs"].values()), default=0
    ), (name, observed[name], request)
    confirmations[name] = request.last_modified_epoch
    return response


QueryCacheGrpcClient.submit_sql = submitted
QueryCacheGrpcClient.confirm_execution = confirmed
from dbt.cli.main import cli

code = 0
try:
    cli(args=sys.argv[1:])
except SystemExit as exc:
    code = exc.code or 0
assert code == (1 if failed else 0), code
assert {n: r["decision"] for n, r in observed.items()} == expected, observed
assert len(workers) == 3, workers
assert set(confirmations) == {
    name
    for name, outcome in expected.items()
    if outcome == "build" and not (failed and name == "branch_b")
}, confirmations
assert len(set(confirmations.values())) == len(confirmations), confirmations
# DAG children must observe exactly the evidence published by their committed
# parents, not a cached pre-build value. Confirmation must follow those inputs.
for name, record in observed.items():
    for parent, epoch in record["inputs"].items():
        if parent != name and parent in confirmations:
            assert epoch == confirmations[parent], (name, parent, record, confirmations)
Path(os.environ["PARALLEL_OBSERVATIONS"]).write_text(
    json.dumps({"nodes": observed, "confirmed": confirmations})
)
