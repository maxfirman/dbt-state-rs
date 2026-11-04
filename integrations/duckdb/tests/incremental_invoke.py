"""Public client observations around real dbt incremental execution."""

import os
import sys
from dbt_state_duckdb import install
from dbt_state.grpc.client import QueryCacheGrpcClient
from query_cache_common.models.services.clone_service_models import ReadyToCloneResponse
from query_cache_common.models.services.sql_service_models import (
    ReadyToExecuteResponse,
    SkipExecutionResponse,
)

install()
if os.environ.get("INCREMENTAL_COPY_FAILURE") == "after_commit":
    from dbt_state_duckdb import DuckDBAdapterExtension

    original_clone = DuckDBAdapterExtension.clone

    def uncertain_clone(self, *args, **kwargs):
        original_clone(self, *args, **kwargs)
        raise RuntimeError("Injected failure after committed CTAS")

    DuckDBAdapterExtension.clone = uncertain_clone
submissions, registrations, confirmations = [], [], []
submit = QueryCacheGrpcClient.submit_sql
register = QueryCacheGrpcClient.register_clone
confirm = QueryCacheGrpcClient.confirm_execution


def observe_submit(self, request, *args, **kwargs):
    if os.environ.get("INCREMENTAL_COPY_FAILURE"):
        # Exercise the ordinary SQL fallback after the precompile copy fault,
        # rather than allowing a second independent automatic copy attempt.
        request.clone_chain_depth_limit = 0
    response = submit(self, request, *args, **kwargs)
    submissions.append((request, response))
    return response


def observe_register(self, request, *args, **kwargs):
    assert request.execution_type.value == "DELETE_INSERT"
    assert request.clone_source_last_modified_epoch is not None
    assert request.clone_source_table_type == "DUCKDB CTAS TABLE"
    response = register(self, request, *args, **kwargs)
    registrations.append((request, response))
    if isinstance(response, ReadyToCloneResponse):
        assert (
            response.clone_required_last_modified_epoch
            == request.clone_source_last_modified_epoch
        )
        if os.environ.get("INCREMENTAL_COPY_FAILURE") == "plan":
            response.clone_sqls = ["deliberately invalid CTAS plan"]
    return response


def observe_confirm(self, request, *args, **kwargs):
    response = confirm(self, request, *args, **kwargs)
    assert response.success and request.last_modified_epoch is not None
    for registration, proposal in registrations:
        if (
            isinstance(proposal, ReadyToCloneResponse)
            and proposal.request_id == request.request_id
        ):
            import duckdb

            with duckdb.connect(os.environ["WAREHOUSE_PATH"]) as db:
                assert (
                    db.execute(
                        "select * from " + proposal.clone_source + " order by all"
                    ).fetchall()
                    == db.execute(
                        "select * from " + proposal.clone_target + " order by all"
                    ).fetchall()
                )
                assert [
                    (r[0], r[1])
                    for r in db.execute("describe " + proposal.clone_target).fetchall()
                ] == [("id", "INTEGER"), ("amount", "INTEGER")]
            assert (
                request.last_modified_epoch
                > registration.clone_source_last_modified_epoch
            )
    confirmations.append(request)
    return response


QueryCacheGrpcClient.submit_sql = observe_submit
QueryCacheGrpcClient.register_clone = observe_register
QueryCacheGrpcClient.confirm_execution = observe_confirm
from dbt.cli.main import cli

try:
    cli(args=sys.argv[1:])
except SystemExit as result:
    if os.environ.get("EXPECT_FAILURE"):
        assert result.code and not confirmations, confirmations
        sys.exit(0)
    if result.code:
        raise
if os.environ.get("EXPECT_FAILURE"):
    raise AssertionError("schema fail mode must reject incompatible columns")
expected = os.environ["EXPECT_DECISION"]
if expected == "ordinary":
    assert not submissions and not registrations and not confirmations, (
        submissions,
        registrations,
        confirmations,
    )
else:
    assert len(submissions) == 1, submissions
    response = submissions[0][1]
    kind = {
        ReadyToExecuteResponse: "build",
        SkipExecutionResponse: "skip",
        ReadyToCloneResponse: "copy",
    }[type(response)]
    assert kind == expected, (kind, expected)
    assert len(registrations) == int(os.environ.get("EXPECT_REGISTER", "0")), (
        registrations
    )
    for request, clone in registrations:
        if isinstance(clone, ReadyToCloneResponse):
            matches = [c for c in confirmations if c.request_id == clone.request_id]
            assert len(matches) == (
                0 if os.environ.get("INCREMENTAL_COPY_FAILURE") else 1
            ), confirmations
    if kind == "build":
        assert any(c.request_id == response.request_id for c in confirmations), (
            confirmations
        )

if expected == "copy":
    actual = [c for c in confirmations if c.request_id == response.request_id]
    assert len(actual) == 1 and actual[0].failed_to_clone, confirmations
