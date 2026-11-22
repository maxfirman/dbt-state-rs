"""Drive the pinned dbt-state Python client's own auth and gRPC code.

The harness (`crates/harness/tests/python_client.rs`) runs this against a
dbt State deployment and its Token broker. Nothing here reimplements or
patches the client: the driver builds the client's config from the
environment with the client's own loader, then calls the client's own
`QueryCacheGrpcClient.create`, which acquires the token (`SsoAuth`), resolves
the Organization from the token's scope and opens the TLS channel, and the
client's own RPC methods.

The client reads only its supported settings from the environment:

- `RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE`: the gRPC listener, over TLS;
- `RUN_CACHE_AUTH_URL`, `RUN_CACHE_TOKEN_URL`: the Token broker;
- `DBT_ENGINE_STATE_OAUTH_CLIENT_ID`, `DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET`:
  the machine's Upstream IdP app registration;
- `RUN_CACHE_CA_BUNDLE`: the CA the client's gRPC channel trusts;
- `RUN_CACHE_HOME`: where the client would cache a token on disk.

The token request goes through `requests`, which trusts the CA in
`REQUESTS_CA_BUNDLE`.

Usage: `driver.py machine`. Prints one JSON object on stdout: the outcome
of each step, or the error the client raised. Exits 1 when the client
raised.
"""

import json
import sys
import uuid

from dbt_state.config import RunCacheConfig
from dbt_state.errors import AuthenticationError
from dbt_state.grpc.client import QueryCacheGrpcClient
from query_cache_common.models import shared_models
from query_cache_common.models.services import sql_service_models

TARGET = '"DB"."S"."T"'


def config() -> RunCacheConfig:
    """The client's config from environment variables only: the env step
    of `RunCacheConfig.from_runtime_config`, without a dbt project."""
    return RunCacheConfig(**RunCacheConfig._load_from_env(RunCacheConfig._fields_and_types()))


def machine() -> dict:
    """A CI machine's run: `client_credentials` login, then the RPCs a dbt
    run starts with."""
    session_id = str(uuid.uuid4())
    with QueryCacheGrpcClient.create(
        config(), session_id=session_id, invocation_id=str(uuid.uuid4())
    ) as client:
        supported = client.is_client_version_supported()
        decision = client.submit_sql(
            sql_service_models.SubmitEnrichedSQLRequest(
                target_table=TARGET,
                dialect="snowflake",
                default_catalog="DB",
                execution_type=shared_models.ModelExecutionType.FULL,
                sql="select 1 as id",
                ignore_external_modifications=False,
                tolerate_nondeterminism=True,
            )
        )
    return {
        "outcome": "ok",
        "client_version_supported": supported,
        "decision": type(decision).__name__,
        "session_id": session_id,
    }


def main() -> int:
    scenarios = {"machine": machine}
    if len(sys.argv) != 2 or sys.argv[1] not in scenarios:
        print(f"usage: driver.py {{{'|'.join(scenarios)}}}", file=sys.stderr)
        return 2
    try:
        result = scenarios[sys.argv[1]]()
    except AuthenticationError as error:
        # The client's own error, and the cause it chained (for a refused
        # token request, the OAuth error from the token endpoint).
        result = {
            "outcome": "error",
            "error": type(error).__name__,
            "message": str(error),
            "cause": str(error.__cause__) if error.__cause__ else None,
        }
    print(json.dumps(result), flush=True)
    return 0 if result["outcome"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
