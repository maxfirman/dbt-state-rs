"""#191 (spec #67) follow-up: one Volatile function per model, so each map key
can be attributed to the SQL that produced it.

`volatile_evaluation.py` puts every detector function in one model. Hosted
returned 9 keys for 21 calls there, so which calls fold into a shared key
(LOCALTIMESTAMP() into CURRENT_TIMESTAMP()?) and which render nothing (ZIPF,
SEQn) stays an inference. Here every model is a first build with evaluation on
(`state: {evaluate_volatile_sql: true}`), calling one form:

  fl  localtimestamp()          flb  localtimestamp   (parenless)
  fs  systimestamp()            fg   getdate()
  ft  current_time()            fli  localtime()
  fcb current_date  (parenless) fcs  current_timestamp (parenless)
  fu  uniform(1, 100, random())  (RANDOM() only nested)
  fz  zipf(1, 10, random())      (ZIPF over a nested RANDOM())
  fq  seq4()
  fx  function names only in a string literal, a quoted identifier and a block
      comment (no call)

1 build (12 submits), 12 `state-probe` explains, 12 `dbt show` reads.
"""
from explain_capture import Build, Explain, Read, Scenario, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
ON = "materialized='table', state={'evaluate_volatile_sql': true}"

BODIES = {
    "fl": "localtimestamp() as v",
    "flb": "localtimestamp as v",
    "fs": "systimestamp() as v",
    "fg": "getdate() as v",
    "ft": "current_time() as v",
    "fli": "localtime() as v",
    "fcb": "current_date as v",
    "fcs": "current_timestamp as v",
    "fu": "uniform(1, 100, random()) as v",
    "fz": "zipf(1, 10, random()) as v",
    "fq": "seq4() as v",
    "fx": "'current_timestamp() random()' as v, 1 as \"CURRENT_DATE\" /* uuid_string() */",
}


def model(name, body):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config({ON}) }}}}\nselect 1 as id, '{name}_${{run}}' as tag, {body}\n")


SCENARIO = Scenario(
    name="volatile_evaluation_functions",
    summary="Volatile evaluation on Snowflake: one Volatile function form per model, plus names "
            "only in a literal, a quoted identifier and a comment (#191)",
    names=tuple(BODIES),
    steps=[
        *[model(n, b) for n, b in BODIES.items()],
        Build(*(f"${{{n}}}" for n in BODIES)),
        *[Explain(f"${{{n}}}", intent=f"first build: {BODIES[n]}", decision="EXECUTE",
                  execution_type=1, history=False, via="probe") for n in BODIES],
        *[Read(f"{DEMO}.${{{n}}}") for n in BODIES],
    ],
)
