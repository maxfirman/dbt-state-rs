"""Wire names and decision-row helpers shared by the experiment tools.

Imported by the capture driver (`explain_capture.py`) and the census
(`explain_census.py`). A "row" is one golden-log record
(`{"method", "request", "response", ...}`).

Self-tests: python3 experiments/explain_capture_test.py and
python3 experiments/explain_census_test.py.
"""

# shared.proto ModelExecutionType (the node kind)
KIND = {
    0: "UNSPECIFIED", 1: "FULL", 2: "APPEND", 3: "MERGE", 4: "INSERT_OVERWRITE",
    5: "DELETE_INSERT", 6: "MICROBATCH", 7: "SNAPSHOT", 8: "DBT_DATA_TEST",
    9: "VALUES", 10: "VIEW", 11: "DBT_CUSTOM",
}
# shared.proto SubmitSQLResultType, named as in docs/explain-catalog.md
DECISION = {0: "SKIP", 1: "EXECUTE", 3: "CLONE", 4: "UNKNOWN"}

# Methods whose answer can mint an execution_decision_id. A speculative submit
# mints one when hosted decides on it (explain_rebuild_reason_gaps.jsonl, #103).
MINTING_METHODS = (
    "SubmitEnrichedSQL", "SubmitEnrichedSQLSpeculative", "SubmitValues", "RegisterClone",
)
VALUES = 9  # a SubmitValues request has no execution_type: a seed is VALUES


def variant(row):
    """(variant name, payload) of a decision row.

    A submit answers through its `response` oneof; a RegisterClone through its
    top-level `ready_to_clone` / `unable_to_clone`. (None, {}) when neither.
    """
    resp = row.get("response") or {}
    if row.get("method") == "RegisterClone":
        for v in ("ready_to_clone", "unable_to_clone"):
            if resp.get(v):
                return v, resp[v]
        return None, {}
    inner = resp.get("response") or {}
    v = next(iter(inner), None)
    return v, (inner.get(v) or {})


def minted(row):
    """(execution_decision_id, execution_type) when the row mints a decision,
    else (None, None)."""
    if row.get("method") not in MINTING_METHODS:
        return None, None
    _, payload = variant(row)
    edid = payload.get("execution_decision_id")
    if not edid:
        return None, None
    if row.get("method") == "SubmitValues":
        return edid, VALUES
    return edid, (row.get("request") or {}).get("execution_type")

