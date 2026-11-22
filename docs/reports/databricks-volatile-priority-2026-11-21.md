# Databricks Volatile evaluation priority captures (#213)

Observed on 2026-11-21 with pinned dbt 2.0.11, the Databricks Free Edition
Serverless Starter Warehouse (`dbsql_version: 2026.45`), Unity Catalog, and
managed Delta tables. The runner used an existing schema lease in
`cdo_demo_db`, fresh UUID-prefixed relations, live ownership and collision
checks, and independent typed SQL API readbacks. It created no schemas and
preserved the existing inventory. The only manual UPDATEs and DROP targeted
relations created by their own run.

## Promoted histories

| Fixture | RPCs | Ordinary submits | Successful confirms | Explain messages | SHA-256 |
|---|---:|---:|---:|---:|---|
| `databricks_volatile_priority.jsonl` | 98 | 27 | 24 | 27 | `6550d89c1f122cbebd07bd2731616e39813b518a231572bbbf78fcca8c9e2001` |
| `databricks_volatile_function_builds.jsonl` | 23 | 6 | 6 | 6 | `f095c8db1998927664248169c3eea9ba7671cce6552d1e51815c7c2c20882cbb` |
| `databricks_volatile_checked_reuse.jsonl` | 19 | 4 | 2 | 4 | `0022b4e07a53d65be8f7eb992ed31ef3e121f5bf4c97d5585ca414f39d713772` |
| `databricks_volatile_producer.jsonl` | 50 | 14 | 11 | 14 | `061682e50e65ee26f5e2ce84a5f2506663e9eec35fd6688683c060b7799b404d` |

The priority history established the following behavior.

- The FULL model built with and without evaluation. With evaluation off,
  `rand()` and `current_timestamp()` each returned a value on BUILD and SKIP.
  The SKIP Explain printed the rendered value and said the configuration
  tolerated non-determinism. The subsequent SQL API read found the first
  build's value still stored.
- `random()` rendered as `RAND()`, `now()` as `CURRENT_TIMESTAMP()`, and
  `uuid()` as `UUID()`. With evaluation on, these and
  `current_timestamp()` rendered on the first build and unchanged rerun.
- APPEND, MERGE and DELETE_INSERT over unchanged source-less SQL returned
  BUILD and a `RAND()` map both times. The APPEND kept the original row and
  added a new one. MERGE and DELETE_INSERT replaced the row by key.
- The FULL model over a managed Delta upstream rendered on its unchanged
  rerun and after its upstream table changed externally. Explain listed the
  upstream at its recorded epoch on the unchanged rerun and as outdated
  after the external update. It rendered again after an external edit to
  its own Target relation, and after the relation was dropped. The latter
  Explain had only the absent-table analysis.
- The zero-tolerance consumer built after the initial producer build and
  rebuilt after the producer's ordinary volatile rerun. Its map was empty.
  Explain reported unchanged upstream query logic and outdated data.
- Every promoted BUILD returned rejection codes 6/6. All first builds
  had only the ordinary absent-table Explain tree.

In the function-build history, both date spellings rendered
`CURRENT_DATE`. Bare `current_timestamp` rendered `CURRENT_TIMESTAMP()`.
The three literal seeds `0`, `-7` and `42` returned the respective values
below. Their State values differ from the Warehouse values. The supported
seed forms use exactly those captured constants. An algorithm for other
seeds and Reuse across a date boundary remain unverified.

## Warehouse readback and State values

Values below come from the independent SQL API read following each native
invocation. For the input control the value is its revision. For consumers
over that control's downstream model the value is the inherited random
column. The append rerun shows both stored rows, newest value first. An
empty map is shown as `{}`.

### Priority history

| Phase | Model | Submit / Explain | Decision | State value | Warehouse value |
|---|---|---|---|---|---|
| first | `random_alias` | 21 / 39 | BUILD | `RAND()` = `0.40814211219617913` | `0.08277302183580015` |
| first | `uuid_function` | 16 / 39 | BUILD | `UUID()` = `cc9e34cf-2d74-4818-bd08-abac710d08a4` | `d036763a-390a-4feb-9696-a8ad8996088a` |
| first | `current_ts` | 15 / 39 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:42:29.374475` | `2026-11-21T19:42:31.422Z` |
| first | `now_alias` | 19 / 39 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:42:29.446223` | `2026-11-21T19:42:33.993Z` |
| first | `rand_off` | 20 / 39 | BUILD | `RAND()` = `0.027717169402322384` | `0.7066187667957926` |
| first | `ts_off` | 22 / 39 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:42:29.446899` | `2026-11-21T19:42:36.382Z` |
| first | `inc_append` | 18 / 39 | BUILD | `RAND()` = `0.5938603248500086` | `0.13021595166064437` |
| first | `inc_merge` | 13 / 39 | BUILD | `RAND()` = `0.8739511315496913` | `0.6675260797616905` |
| first | `inc_delete` | 14 / 39 | BUILD | `RAND()` = `0.06808395752558793` | `0.25506538202015017` |
| first | `input_row` | 17 / 39 | BUILD | `{}` | `1` |
| first | `with_input` | 31 / 39 | BUILD | `RAND()` = `0.4956270134211823` | `0.11153462780518075` |
| first | `strict_consumer` | 35 / 39 | BUILD | `{}` | `0.11153462780518075` |
| rerun | `random_alias` | 57 / 75 | BUILD | `RAND()` = `0.736876265923948` | `0.32683730625141294` |
| rerun | `uuid_function` | 52 / 75 | BUILD | `UUID()` = `f0419fc4-e29a-4dc3-bf46-78694836fcc3` | `5819d13e-4017-430c-8ff9-762bbf1e9025` |
| rerun | `current_ts` | 53 / 75 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:43:15.630284` | `2026-11-21T19:43:17.279Z` |
| rerun | `now_alias` | 54 / 75 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:43:15.642853` | `2026-11-21T19:43:17.359Z` |
| rerun | `rand_off` | 56 / 75 | SKIP | `RAND()` = `0.11298166518946795` | `0.7066187667957926` |
| rerun | `ts_off` | 58 / 75 | SKIP | `CURRENT_TIMESTAMP()` = `2026-11-21 19:43:15.717360` | `2026-11-21T19:42:36.382Z` |
| rerun | `inc_append` | 59 / 75 | BUILD | `RAND()` = `0.4018550888457125` | `0.739699730607559`, `0.13021595166064437` |
| rerun | `inc_merge` | 61 / 75 | BUILD | `RAND()` = `0.5592062128259658` | `0.3763141497401521` |
| rerun | `inc_delete` | 60 / 75 | BUILD | `RAND()` = `0.9108464406866952` | `0.1642283185158716` |
| rerun | `input_row` | 55 / 75 | SKIP | `{}` | `1` |
| rerun | `with_input` | 62 / 75 | BUILD | `RAND()` = `0.9282766138849758` | `0.20193150075899557` |
| rerun | `strict_consumer` | 69 / 75 | BUILD | `{}` | `0.20193150075899557` |
| upstream changed | `with_input` | 79 / 82 | BUILD | `RAND()` = `0.025261221059531547` | `0.04161688539172914` |
| target modified | `with_input` | 86 / 89 | BUILD | `RAND()` = `0.791026066958671` | `0.8618447852234703` |
| target dropped | `with_input` | 93 / 97 | BUILD | `RAND()` = `0.7512611418513031` | `0.7843525982507275` |

### Function first builds

| Phase | Model | Submit / Explain | Decision | State value | Warehouse value |
|---|---|---|---|---|---|
| first | `rand_seeded` | 12 / 22 | BUILD | `RAND(42)` = `0.6394267984578837` | `0.08575559529546095` |
| first | `today` | 9 / 22 | BUILD | `CURRENT_DATE` = `2026-11-21` | `2026-11-21` |
| first | `rand_zero_seed` | 10 / 22 | BUILD | `RAND(0)` = `0.8444218515250481` | `0.8541927863674711` |
| first | `rand_negative_seed` | 11 / 22 | BUILD | `RAND(-7)` = `0.32383276483316237` | `0.5817384287232591` |
| first | `bare_date` | 8 / 22 | BUILD | `CURRENT_DATE` = `2026-11-21` | `2026-11-21` |
| first | `bare_timestamp` | 13 / 22 | BUILD | `CURRENT_TIMESTAMP()` = `2026-11-21 19:40:57.982227` | `2026-11-21T19:41:02.466Z` |

## Maintainer decisions

**Observed with evaluation requested.** The literal-only control, `rand(42)`
and `current_date()` each returned fresh SKIP on the unchanged rerun in the
exploratory history. The seed and date had filled maps and a detector line
in Explain without a configuration-tolerance line. The maintainer approved
Reuse for the verified source-less literal or `rand(42)` shape. A separate
complete capture, below, reproduced both approved cases. In the exploratory
date cell the first build wrote `2026-11-21`. Its rerun returned a map with
`CURRENT_DATE` = `2026-11-21` and retained that value in the Warehouse
(entries 22 and 67, Explain 48 and 91). Date-boundary behavior is unknown, so
the Rust server retains the conservative rebuild.

| Phase | Model | Submit / Explain | Decision | State value | Warehouse value |
|---|---|---|---|---|---|
| first | `rand_seeded` | 4 / 10 | BUILD | `RAND(42)` = `0.6394267984578837` | `0.08575559529546095` |
| first | `deterministic_on` | 5 / 10 | BUILD | `{}` | `1` |
| rerun | `rand_seeded` | 15 / 18 | SKIP | `RAND(42)` = `0.6394267984578837` | `0.08575559529546095` |
| rerun | `deterministic_on` | 16 / 18 | SKIP | `{}` | `1` |

**Observed in the downstream producer history.** The one-hour consumer reused
the first build's value after the volatile model rebuilt over unchanged
inputs. After an External modification to the producer's sole confirmed
input, both consumers rebuilt and native Explain reported an upstream query
change. The edited input was at revision `2`. On the next volatile rebuild
over that same input, the one-hour consumer resumed stale Reuse. The
producer and consumers sent unchanged compiled SQL through all four phases.

**Inference and approved scope.** The paired tolerances support treating the
external input edit as a change of producer for this graph. They leave
broader transitive behavior unresolved. The maintainer approved the single
observed transition and its implementation records the revision with the
confirmed build, independently of rendered values.
[ADR 0007](../adr/0007-lag-tolerance-and-upstream-producers.md#databricks-volatile-producer-after-external-modification-213).

| Phase | Model | Submit / Explain | Decision | State value | Warehouse value |
|---|---|---|---|---|---|
| first | `input_row` | 3 / 14 | BUILD | `{}` | `1` |
| first | `with_input` | 6 / 14 | BUILD | `RAND()` = `0.3249195076469661` | `0.5471752433306221` |
| first | `strict_consumer` | 10 / 14 | BUILD | `{}` | `0.5471752433306221` |
| first | `tolerant_consumer` | 9 / 14 | BUILD | `{}` | `0.5471752433306221` |
| rerun | `input_row` | 18 / 26 | SKIP | `{}` | `1` |
| rerun | `with_input` | 19 / 26 | BUILD | `RAND()` = `0.9610550707768568` | `0.08450539957799219` |
| rerun | `strict_consumer` | 23 / 26 | BUILD | `{}` | `0.08450539957799219` |
| rerun | `tolerant_consumer` | 22 / 26 | SKIP | `{}` | `0.5471752433306221` |
| upstream changed | `with_input` | 30 / 38 | BUILD | `RAND()` = `0.2864913846863978` | `0.3714388086166881` |
| upstream changed | `strict_consumer` | 32 / 38 | BUILD | `{}` | `0.3714388086166881` |
| upstream changed | `tolerant_consumer` | 33 / 38 | BUILD | `{}` | `0.3714388086166881` |
| after transition | `with_input` | 42 / 49 | BUILD | `RAND()` = `0.12021961695680894` | `0.8666542273388743` |
| after transition | `strict_consumer` | 45 / 49 | BUILD | `{}` | `0.8666542273388743` |
| after transition | `tolerant_consumer` | 44 / 49 | SKIP | `{}` | `0.3714388086166881` |

## Reproduce

```bash
cargo build -p dbt-state-harness --bin record-proxy
python3 experiments/databricks_volatile_priority.py \
  --batch response --dbt /path/to/dbt-2.0.11 --proxy target/debug/record-proxy \
  --schema-lease /path/to/lease.json --work-dir /path/to/fresh-capture
```

Use a fresh work directory for each batch. The promoted capture modes are
`response`, `function-builds`, `checked-reuse` and `producer`. Raw artifacts
are under `~/projects/databricks-volatile-213-response-e/`,
`~/projects/databricks-volatile-213-forms-d/`,
`~/projects/databricks-volatile-213-checked-reuse-f/` and
`~/projects/databricks-volatile-213-producer-g/`, respectively. Exploratory
runs `priority-a`, `response-b` and `producer-c` retain the earlier discovery
evidence. Use `--batch reuse-controls` to reproduce the exploratory date
cell. Each directory preserves the executed runner, SQL responses, typed
row and table-property checks, manifests, native Explain outputs,
inventories and the certified capture.
