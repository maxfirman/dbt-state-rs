"""Observe real public client responses while running the normal dbt CLI."""

import inspect
import json
import os
import sys
import time
from pathlib import Path

from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state_duckdb import install
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
# Deliberately freeze/regress wall time: evidence must remain ordinal.
if os.environ.get("EVIDENCE_TEST_CLOCK"):
    real_time = time.time

    def evidence_time():
        caller = inspect.currentframe().f_back.f_code.co_filename
        if caller.endswith("dbt_state_duckdb/evidence.py"):
            return float(os.environ["EVIDENCE_TEST_CLOCK"])
        return real_time()

    time.time = evidence_time
observed = []
requests = []
confirmed = []
confirm = QueryCacheGrpcClient.confirm_execution


def observe_confirmation(self, request, *args, **kwargs):
    if os.environ.get("FAIL_CONFIRM") == "1":
        rpc = self.execution_stub.ConfirmExecution

        def unavailable(*args, **kwargs):
            raise RuntimeError("Injected confirmation transport failure")

        self.execution_stub.ConfirmExecution = unavailable
        try:
            return confirm(self, request, *args, **kwargs)
        finally:
            self.execution_stub.ConfirmExecution = rpc
    response = confirm(self, request, *args, **kwargs)
    if os.environ.get("CRASH_AFTER_CONFIRM") == "1":
        os._exit(73)
    if observed and request.request_id == getattr(observed[0], "request_id", None):
        assert response.success
        assert request.last_modified_epoch > max(
            (table.last_modified_epoch or 0 for table in requests[0].tables),
            default=0,
        ), "completion must follow all request evidence in one ordering domain"
        confirmed.append(request)
    return response


QueryCacheGrpcClient.confirm_execution = observe_confirmation
submit = QueryCacheGrpcClient.submit_sql


def observe(self, request, *args, **kwargs):
    if (
        os.environ.get("FAIL_STATE") == "1"
        and request.dbt_node_state.node_unique_id == "model.duckdb_smoke.totals"
    ):
        raise RuntimeError("Injected transport failure before submit")
    if os.environ.get("PAUSE_STATE_PATH"):
        marker = Path(os.environ["PAUSE_STATE_PATH"])
        marker.write_text("ready")
        while not marker.with_suffix(".release").exists():
            time.sleep(0.05)
    response = submit(self, request, *args, **kwargs)
    if request.dbt_node_state.node_unique_id == "model.duckdb_smoke.totals":
        observed.append(response)
        requests.append(request)
        assert request.table_namespace

    return response


QueryCacheGrpcClient.submit_sql = observe

if os.environ.get("EXPECT_NO_COPY") == "1":
    from query_cache_common.models.services.clone_service_models import (
        ReadyToCloneResponse,
    )

    register = QueryCacheGrpcClient.register_clone

    def reject_copy(self, request, *args, **kwargs):
        response = register(self, request, *args, **kwargs)
        assert not isinstance(response, ReadyToCloneResponse), response
        return response

    QueryCacheGrpcClient.register_clone = reject_copy
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as result:
    if result.code:
        raise
if os.environ.get("FAIL_STATE") == "1":
    assert not observed
    sys.exit(0)
assert len(observed) == 1, "State must return a model decision through its public RPC"
expected_response = (
    ReadyToExecuteResponse
    if os.environ.get("EXPECT_DECISION", "build") == "build"
    else SkipExecutionResponse
)
assert isinstance(observed[0], expected_response), observed
if os.environ.get("EXPECT_CONFIRM", "1") == "1" and isinstance(
    observed[0], ReadyToExecuteResponse
):
    assert len(confirmed) == 1, (
        "successful BUILD must confirm with post-completion evidence"
    )
if os.environ.get("EXPECT_CONFIRM") == "0":
    assert not confirmed, "unknown evidence must not confirm"
print(
    "STATE_DECISION="
    + ("build" if isinstance(observed[0], ReadyToExecuteResponse) else "skip")
)

if os.environ.get("STATE_OBSERVATION_PATH"):
    with Path(os.environ["STATE_OBSERVATION_PATH"]).open("a") as artifact:
        artifact.write(
            json.dumps(
                {
                    "namespace": requests[0].table_namespace,
                    "decision": "build"
                    if isinstance(observed[0], ReadyToExecuteResponse)
                    else "skip",
                    "target_epoch": next(
                        (
                            t.last_modified_epoch
                            for t in requests[0].tables
                            if t.name == requests[0].target_table
                        ),
                        None,
                    ),
                    "confirmed_epochs": [r.last_modified_epoch for r in confirmed],
                }
            )
            + "\n"
        )
