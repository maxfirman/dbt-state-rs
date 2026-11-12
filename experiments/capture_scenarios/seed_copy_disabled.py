"""Seeds whose copy on submit is disallowed (`allow_clones: false`).

Hosted copies a seed on submit from an equivalent load in another schema of
its namespace (#91: `explain_copy_probes.jsonl` entry 12,
`explain_seed_copy_existing.jsonl` entries 6 and 14). This run asks what
hosted answers when the dev target disallows the copy, as the FULL
`copy_disabled` cell (`copy_logic_mismatch` `cf4`, 1 / 10, "cloning is
disabled") asked for a table. One attempt, no retries.

Nodes (per run; each seed yml puts it in target.schema, so prod and dev are
different relations):
  sda  loaded in prod_demo, then built in dev_noclone with the same values:
       its dev table is absent and prod holds an equivalent load.
  sdb  loaded in prod_demo with values Y, first loaded in dev_noclone with
       values X (no equivalent: an ordinary load), then built in dev_noclone
       with values Y: its dev table exists with other values and prod holds
       an equivalent load.

`dev_noclone` is not in the shared `~/.dbt/profiles.yml`. Run with
`--profiles` naming a private copy (mode 600, outside the repo) that adds it
as `dev_clone` with `allow_clones: false`; the driver checks that every
target resolves to CDO_DEMO_DB. Delete the copy after the run.

dbt invocations: 3 builds, 1 state explain = 4. The explain of sda is a
`state-probe` GetExplainMessages call through this run's proxy (no dbt
invocation).
"""
from explain_capture import Build, Explain, Scenario, Write

SEEDS = ("${sda}", "${sdb}")


def seed_yml(name):
    return f"""seeds:
  - name: ${{{name}}}
    config:
      schema: "{{{{ target.schema }}}}"
"""


SCENARIO = Scenario(
    name="seed_copy_disabled",
    summary="seed with an equivalent load in prod_demo built under allow_clones: false: "
            "dev table absent (sda), dev table loaded with other values (sdb)",
    names=("sda", "sdb"),
    steps=[
        Write("seeds/${sda}.yml", seed_yml("sda")),
        Write("seeds/${sdb}.yml", seed_yml("sdb")),
        Write("seeds/${sda}.csv", "id,v\n1,a_${run}\n2,a_${run}\n"),
        Write("seeds/${sdb}.csv", "id,v\n1,y_${run}\n2,y_${run}\n"),

        # prod_demo: the equivalent loads.
        Build(*SEEDS),

        # dev_noclone: sda into an absent table (prod equivalent, copies
        # disallowed); sdb first loaded with other values (no equivalent).
        Write("seeds/${sdb}.csv", "id,v\n1,x_${run}\n2,x_${run}\n"),
        Build(*SEEDS, target="dev_noclone"),
        Explain("${sda}", intent="dev table absent, prod equivalent, allow_clones: false",
                execution_type=9, via="probe"),

        # dev_noclone: sdb now has prod's values over its existing dev table.
        Write("seeds/${sdb}.csv", "id,v\n1,y_${run}\n2,y_${run}\n"),
        Build("${sdb}", target="dev_noclone"),
        Explain("${sdb}", intent="dev table with other values, prod equivalent, "
                "allow_clones: false", execution_type=9, target="dev_noclone"),
    ],
)
