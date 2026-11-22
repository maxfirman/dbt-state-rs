#!/usr/bin/env python3
"""Send the crafted Databricks source-choice histories from #240 through PROBE_URL."""

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
NATIVE = ROOT / "golden/fixtures/databricks_submit_copy.jsonl"
PROBE = ROOT / "target/debug/state-probe"

# Each pair gives a source role and its physical completion offset in ms.
# The sequence of pairs gives the independent confirmation order.
OLD = ("old", 10_000)
MIDDLE = ("middle", 20_000)
STALE = ("stale", 30_000)
PEER = ("peer", 30_000)
HISTORIES = {
    "old_only": (OLD,),
    "middle_only": (MIDDLE,),
    "stale_only": (STALE,),
    "old_stale": (OLD, STALE),
    "old_middle_stale": (OLD, MIDDLE, STALE),
    "three_reversed": (STALE, MIDDLE, OLD),
    "two_fresh": (OLD, PEER),
    "two_fresh_reversed": (PEER, OLD),
    "two_fresh_swapped_epochs": (("old", 30_000), ("peer", 10_000)),
}


def rpc(operation, request, invocation):
    result = subprocess.run(
        [PROBE, operation], input=json.dumps(request), text=True,
        stdout=subprocess.PIPE, check=True, timeout=60,
        env=dict(os.environ, PROBE_INVOCATION=invocation, PROBE_SESSION_ID=invocation),
    )
    return json.loads(result.stdout)


def submit_request(template, role, model, namespace, base_epoch):
    request = deepcopy(template)
    schema = "cc_" + role
    input_schema = "cc_middle" if role == "middle" else "cc_old"
    input_name = model + "_inputs"
    uid = "model.copy_candidates." + model
    request.update(
        target_table=f"`cdo_demo_db`.`{schema}`.`{model}`",
        default_schema=schema,
        table_namespace=namespace,
        sql=f"\nselect * from cdo_demo_db.{input_schema}.{input_name}",
        tables=[{
            "name": f"`cdo_demo_db`.`{input_schema}`.`{input_name}`",
            "last_modified_epoch": base_epoch - 10_000 if role == "stale" else base_epoch,
        }],
        labels={
            "dbt_node_unique_id": uid,
            "dbt_node_fqn": "copy_candidates." + model,
            "dbt_node_name": model,
        },
    )
    request["dbt_node_state"].update(node_unique_id=uid, project_name="copy_candidates")
    return request


def run_history(name, sources, build, confirm, destination, base_epoch):
    namespace = uuid.uuid4().hex
    model = "cc_" + namespace
    pending = {}
    offsets = dict(sources)
    # Overlap the builds: stale read the shared input before it changed, even
    # though its physical result finishes last. All epochs are in the past.
    for role in ("stale", "old", "middle", "peer"):
        if role not in offsets:
            continue
        request = submit_request(build, role, model, namespace, base_epoch)
        invocation = str(uuid.uuid4())
        result = rpc("submit", request, invocation)["response"]
        if "ready_to_execute" not in result:
            raise ValueError(f"{name}: {role} setup did not BUILD: {result}")
        confirmation = dict(
            confirm,
            request_id=result["ready_to_execute"]["request_id"],
            labels=request["labels"],
            last_modified_epoch=base_epoch + offsets[role],
        )
        pending[role] = invocation, confirmation

    confirmations = []
    for role in offsets:
        invocation, confirmation = pending[role]
        if not rpc("confirm", confirmation, invocation)["success"]:
            raise ValueError(f"{name}: {role} confirmation failed")
        confirmations.append((role, confirmation["last_modified_epoch"]))

    request = submit_request(destination, "dev", model, namespace, base_epoch)
    result = rpc("submit", request, str(uuid.uuid4()))["response"]
    ((variant, body),) = result.items()
    print(json.dumps({
        "history": name, "model": model, "variant": variant,
        "clone_source": body.get("clone_source"),
        "confirmations": confirmations,
    }), flush=True)


def main():
    with NATIVE.open() as source:
        rows = [json.loads(line) for line in source]
    # Native FULL build, successful build confirmation, absent-destination submit.
    build, confirm, destination = (rows[i]["request"] for i in (2, 4, 9))
    base_epoch = (int(time.time()) - 120) * 1000
    for name, sources in HISTORIES.items():
        run_history(name, sources, build, confirm, destination, base_epoch)


if __name__ == "__main__":
    main()
