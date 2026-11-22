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
- `RUN_CACHE_HOME`: where the client caches a person's token on disk.

The token request goes through `requests`, which trusts the CA in
`REQUESTS_CA_BUNDLE`.

Scenarios:

- `driver.py machine`: a CI machine (`CI` set) logs in with
  `client_credentials` and makes RPCs.
- `driver.py person STEP...`: a person at a terminal opens the client, which
  signs in through the browser (or refreshes the token it cached on disk),
  then runs each step: `rpc` makes one RPC (one credential lookup); `wait`
  pauses for the harness (see below).

A person runs dbt from an attached terminal, which the client requires for
the browser flow (and for caching the token on disk). The driver gives the
client one: its stdin and stdout become a pseudo-terminal, whose output the
driver copies to stderr. The driver keeps its own stdin and stdout as the
harness's channel. The client's browser is the harness browser
(`browser.py`), registered with Python's `webbrowser` through `BROWSER`,
as a person's environment names their browser; the client's own
`webbrowser.open` runs it.

`wait` prints `{"waiting": true, "rpcs": <made so far>}` and reads one JSON
line from stdin, `{"advance_seconds": N}`: the harness has moved the
deployment's clock N seconds on, and the driver moves the process's wall
clock (`time.time`, which the client's token freshness check and its
token expiry read) as far, as if that long had passed.

Prints one JSON object on stdout at the end: the outcome of each step, or
the error the client raised. Exits 1 when the client raised.
"""

import fcntl
import json
import os
import shlex
import struct
import sys
import termios
import threading
import time
import uuid

import grpc

from dbt_state.config import RunCacheConfig
from dbt_state.errors import AuthenticationError
from dbt_state.grpc.client import QueryCacheGrpcClient
from query_cache_common.models import shared_models
from query_cache_common.models.services import sql_service_models

TARGET = '"DB"."S"."T"'

BROWSER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser.py")


def config() -> RunCacheConfig:
    """The client's config from environment variables only: the env step
    of `RunCacheConfig.from_runtime_config`, without a dbt project."""
    return RunCacheConfig(**RunCacheConfig._load_from_env(RunCacheConfig._fields_and_types()))


def submit(client: QueryCacheGrpcClient) -> str:
    """One model submit: the decision's response type."""
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
    return type(decision).__name__


def machine(out, _control, steps: list[str]) -> dict:
    """A CI machine's run: `client_credentials` login, then the RPCs a dbt
    run starts with."""
    if steps:
        raise Usage("machine takes no steps")
    session_id = str(uuid.uuid4())
    with QueryCacheGrpcClient.create(
        config(), session_id=session_id, invocation_id=str(uuid.uuid4())
    ) as client:
        supported = client.is_client_version_supported()
        decision = submit(client)
    return {
        "outcome": "ok",
        "client_version_supported": supported,
        "decision": decision,
        "session_id": session_id,
    }


class WallClock:
    """The process's wall clock, `advance`d by the harness."""

    def __init__(self) -> None:
        self.offset = 0.0
        host = time.time
        time.time = lambda: host() + self.offset

    def advance(self, seconds: float) -> None:
        self.offset += seconds


def attach_terminal() -> None:
    """Make stdin and stdout a pseudo-terminal (80x24) and copy what is
    written to it to stderr."""
    terminal, client_side = os.openpty()
    fcntl.ioctl(client_side, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    os.dup2(client_side, 0)
    os.dup2(client_side, 1)
    os.close(client_side)

    def copy() -> None:
        while True:
            try:
                chunk = os.read(terminal, 4096)
            except OSError:
                return
            if not chunk:
                return
            sys.stderr.buffer.write(chunk)
            sys.stderr.buffer.flush()

    threading.Thread(target=copy, daemon=True).start()


def person(out, control, steps: list[str]) -> dict:
    """A person's long-running dbt process at a terminal: open the client
    (browser sign-in, or refresh from the token cached on disk), then run
    `steps`."""
    unknown = sorted(set(steps) - {"rpc", "wait"})
    if unknown:
        raise Usage(f"unknown person steps: {unknown}")
    clock = WallClock()
    attach_terminal()
    os.environ["BROWSER"] = f"{shlex.join([sys.executable, BROWSER])} %s"
    decisions = []
    session_id = str(uuid.uuid4())
    with QueryCacheGrpcClient.create(
        config(), session_id=session_id, invocation_id=str(uuid.uuid4())
    ) as client:
        for step in steps:
            if step == "rpc":
                decisions.append(submit(client))
                continue
            print(json.dumps({"waiting": True, "rpcs": len(decisions)}), file=out, flush=True)
            line = control.readline()
            if not line:
                raise Usage("the harness ended the wait without resuming")
            clock.advance(json.loads(line)["advance_seconds"])
    return {"outcome": "ok", "decisions": decisions, "session_id": session_id}


class Usage(Exception):
    pass


def main() -> int:
    scenarios = {"machine": machine, "person": person}
    if len(sys.argv) < 2 or sys.argv[1] not in scenarios:
        print(f"usage: driver.py {{{'|'.join(scenarios)}}} [STEP...]", file=sys.stderr)
        return 2
    # The harness's channel, whatever the scenario does with stdin and
    # stdout.
    out = os.fdopen(os.dup(1), "w")
    control = os.fdopen(os.dup(0), "r")
    try:
        result = scenarios[sys.argv[1]](out, control, sys.argv[2:])
    except Usage as error:
        print(f"driver.py: {error}", file=sys.stderr)
        return 2
    except grpc.RpcError as error:
        # An RPC the deployment refused (or could not answer).
        result = {
            "outcome": "error",
            "error": "RpcError",
            "code": error.code().name,
            "message": error.details(),
        }
    except AuthenticationError as error:
        # The client's own error, and the cause it chained (for a refused
        # token request, the OAuth error from the token endpoint).
        result = {
            "outcome": "error",
            "error": type(error).__name__,
            "message": str(error),
            "cause": str(error.__cause__) if error.__cause__ else None,
        }
    print(json.dumps(result), file=out, flush=True)
    return 0 if result["outcome"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
