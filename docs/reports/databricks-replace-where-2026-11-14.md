# Databricks predicate replacement: bounded #148 evidence, 2026-11-14

The pinned dbt 2.0.11 client and fresh Rust acceptance both preserve the declared
predicate-replacement history across thirteen managed invocations, with thirteen
independent State-disabled references per backend. The complete hosted 90-RPC
history passes exact public replay. The complete fresh native 90-RPC capture also
passes supplementary replay using its twelve actual persisted audit clocks.
This is a validated partial. **Whole #148 and dependencies #138/#140 remain
incomplete.** No production decision, identity, freshness, persistence or
warehouse policy changed in this slice.

## Scope and native contract

The campaign uses the selected Databricks serverless Unity Catalog workspace,
SQL models and ordinary MANAGED Delta tables. dbt 2.0.11 SHA256 is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The existing connection is preserved; no client feature, adapter macro or
warehouse setting is overridden. Native configuration is incremental
`replace_where`, Delta, `unique_key='id'`, `on_schema_change='ignore'`, rendered
comparison, zero lag/ANY and `state.pre_clone='never'`.

Actual ordinary submissions use **DBT_CUSTOM 11**, rather than the enum of
another replacement strategy. The first invocation executes managed CTAS with
an unfiltered SELECT. Existing-target SQL creates one unqualified session-local
`<consumer>__dbt_tmp` view and executes `INSERT INTO <target> BY NAME REPLACE WHERE
<predicate> TABLE <temporary>`. The target's native clustering alteration is
retained in the SQL/property/history evidence. No persistent auxiliary was
observed. Predicate configuration controls the destination independently of the
model's incoming SELECT filter; unique_key does not deduplicate the rows.

P1 is `p = DATE '2026-11-01' OR p IS NULL`. The model applies this fixed filter
only when is_incremental is true. P2 is
`p IN (DATE '2026-11-01', DATE '2026-11-02') OR p IS NULL`; only destination
configuration changes when moving from P1 to P2. Complete rows have ordered
BIGINT/STRING/DATE/TIMESTAMP columns. The source starts with duplicated A rows,
one outside B row and duplicated NULL rows.

## Independent cumulative result

R is the complete target after the declared operation history. F is the fresh
selected result at that cell. Immutable literal snapshots are frozen outside
the repository before warehouse use; they are not derived from actual output,
State decisions, adapter SQL or the most recent source alone. A successful
replacement retains every old row for which the destination predicate is not
TRUE, including UNKNOWN, and multiset-unions the incoming rows. Initial and
full-refresh CTAS instead replace R with the full selected result.

| Cell | Native outcome on both backends | R rows | F rows |
|---|---|---:|---:|
| first | type 11 BUILD/Confirm, unfiltered CTAS | 5 | 5 |
| steady | type 11 BUILD/Confirm, rendered SQL transition | 5 | 4 |
| steady_repeat | fresh SKIP, own prior runtime | 5 | 4 |
| inside | A update plus duplicate NULL insertion, BUILD/Confirm | 6 | 5 |
| inside_repeat | fresh SKIP | 6 | 5 |
| outside | B update then source deletion, BUILD/Confirm | 6 | 5 |
| outside_repeat | fresh SKIP | 6 | 5 |
| empty | empty incoming replacement, BUILD/Confirm | 1 | 0 |
| predicate | P2 destination-only change, BUILD/Confirm | 0 | 0 |
| predicate_repeat | fresh SKIP | 0 | 0 |
| full_refresh | unfiltered CTAS, accepted Record input type 11 | 4 | 4 |
| next_ordinary | filtered SELECT, type 11 BUILD/Confirm | 3 | 3 |
| next_repeat | fresh SKIP | 3 | 3 |

Outside deletion deliberately leaves the original B row in R while the complete
source and F omit it. Empty replacement removes matching A/NULL rows while
retaining B. Destination-only widening then removes B without changing the
rendered SELECT. Full refresh refills R from four fresh rows; the following P2
replacement removes the new B row and retains both duplicate A rows and NULL.
These differences reject a fresh-source substitute for cumulative R.

All 26 cells independently verify complete typed source/R/F multisets, exact
ordered schema, MANAGED Delta properties and full consecutive Delta histories.
Each literal R also executes through State-disabled dbt show with a separate
recorder certifying zero State RPCs. Typed API results independently check that
reference. Pinned show omits zero fractional seconds; only its evidenced UTC
whole-second spelling maps to the identical `.000Z` in a typed TIMESTAMP
column. Original #147 stdout/API/literal proof was revalidated offline; no extra
format probe or unknown temporal normalization was used.

## Preserved helper stop and complete history

The first hosted phase completed ten checked cells and successfully executed
full-refresh CTAS and RecordExecutions. Its helper then stopped because it
expected FULL 1 from other strategies. Actual replace_where Record input is
**type 11**, with the unfiltered SQL, accepted count 1, positive epoch and
runtime 3160 ms. This was a helper assumption, not a warehouse capability failure
or hosted rejection. The original 76 RPCs, false helper summary,626 source pins
and 1,713-file evidence snapshot remain unchanged.

A separately authorized read-only preflight verified exact object identities,
actor, four-row source/R/F, ordered schema, MANAGED Delta, complete histories,
protected eight-object metadata and the exact CTAS query linked to the Record
outcome. A separate continuation completed only missing refresh checks/reference
and the final two managed invocations/two references. It performed no constructor,
version, parse, source DML, CTAS or successful-cell rerun. Its 14 certified RPCs
append byte for byte to the original 76. No prefix, response edit, mask, gap,
unknown baseline or fabricated State history is accepted.

Full-refresh Record and the following ordinary invocation both remain 11. The
rebuild follows the **unfiltered Record SQL → filtered steady SQL identity
transition**, not an execution type change. The helper retains default
expected_type=1 for its general seam; #148 passes literal expected_type 11 bound
to independent hosted evidence, and tests reject wrong types.

## Fresh native acceptance and clocks

Fresh Rust acceptance runs against a new exact source/target pair and isolated
State TestServer, without importing hosted history. Its thirteen managed cells,
five fresh own-runtime SKIPs, complete statuses, physical R/F/schema/history,
thirteen zero-RPC references and twelve Explain trees pass in 262.83s. The
runtime executable is pinned by actual PID/argv/executable SHA, with all 636
source inputs unchanged through child exit. No live replay clock header is used.

After the recorder drains and before State schema cleanup,
`support::native_clock::retain` preserves all 12 organization-scoped audit clocks.
The sidecar binds to the entire native capture hash and has persisted-audit
provenance. Supplementary replay passes that actual sidecar, never a UUID-time
approximation. A byte-identical `databricks_` alias satisfies the replay filename
guard; the raw proxy capture remains unchanged.

| Evidence | SHA256 |
|---|---|
| complete hosted 90 | `ec58c539f4d2c515c943fb008e34d98418bcaaeb9d6239bc5f20a813c1ca0142` |
| complete native 90 | `34ec2fb2b2fa04658c268f95125cf631fe1bfc8e48a27ccf6ef594c8a0446e8b` |
| actual native clock sidecar | `ff25d3c79f3d75877cd3b5f4a22a0ad4e2f680985e334f2e4a169e19e92d34b7` |
| immutable operation ledger V2 | `3354c0bb2a9933cafda54b49de3c28f249406c90179ee9ac10a417287c3ed1e7` |

## Validation and footprint

Fresh validation consists of one whole-corpus test over 127 existing hosted
corpora plus the whole hosted 90 trace (240.35s); after promotion, one whole-corpus
test over 128 hosted corpora plus the whole native 90/actual-clock sidecar
(241.18s). Each new trace asserts 78 non-Explain responses and 12 full Explain
trees with zero gaps, unknown baselines or partial history. A separate run passes
the other 18 conformance guards in 9.87s; the whole-corpus log alone reports
one passed test with 18 filtered out. Strict workspace/all-targets/fuzz Clippy
passes in 84s, fmt/whitespace checks pass, focused Python tests pass 37 checks,
and the #148 wrapper tests pass 2 with the opt-in live test separately exercised.
An offline audit rechecks every retained cell and the original 1,713 hashes.

The earlier root validation of 681 workspace tests, 310 database-free library
tests and 25 DuckDB cases is **reused, not rerun for #148**. Exact input-hash proof
matches 68 production source and 57 DuckDB integration/script inputs to that
uniform gate manifest; resolved package versions/sources/checksums are unchanged.
The only earlier lockfile difference is the existing sha2 harness dev-dependency
edge for the canonical test-only clock helper. New #148 tests, replay, lint and
native results are fresh as stated above.

The existing root-owned container holds four protected #146 and four protected
#147 objects. The exact new pairs are
`rw_20261114{a148aa,b148bb}_{inputs,consumer}`. Separate hosted/native grants
verify the container identity, absence and capacity before use. The native
postflight inventory has 12 permanent objects. One persistent auxiliary slot was
reserved (peak 13), but observed persistent auxiliary usage is 0; serialized SQL
uses one session-local temporary view. No old object/source is written or read
as model data, no new schema or permanent reference is created, and no cleanup
is performed. Both windows and recorders are finished.

The machine-readable [report](databricks-replace-where-2026-11-14.json) pins the
private evidence index, source/executable grants, clock evidence and input proof.
The archive is `~/.local/state/dbt-state-rs/databricks-148-20261114`.

## Remaining required work

Schema evolution; alternate, aliased, scalar/list/reordered, empty or invalid
predicate forms; predicate constraint/settings and exact warehouse errors;
failure/retry publication; broader strategy/node-kind transitions and generated
lifecycle controls remain required. No generic full-refresh mapping, unresolved
hosted policy, unknown baseline or native graph promotion is inferred. #138/#140
and the full #132 scope remain incomplete.
