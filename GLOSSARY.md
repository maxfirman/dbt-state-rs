# dbt State

Language for deciding when a dbt workload can reuse recorded results across
dbt targets.

## Language

**Warehouse**:
The database system in which dbt sources and model results reside and model
queries execute.
_Avoid_: State store

**Target**:
A named dbt profile output a run writes its results to, such as `prod` or
`dev`.
_Avoid_: Environment

**Target relation**:
The physical relation a workload's result is written to.
_Avoid_: Target, own target, target table

**Result copy**:
A new target relation populated from an eligible existing result rather than
by executing the model's transformation again; required relation properties
must also be satisfied.
_Avoid_: Zero-copy clone (unless specifically describing that warehouse capability)

**State history**:
Recorded evidence about earlier executions and their outcomes, used to assess
whether a result can be reused.
_Avoid_: Warehouse contents

**Observed change**:
A difference in a relation's contents or structure between observations;
it does not imply that every intervening write is known.
_Avoid_: Write event

**External modification**:
A change to a dbt-managed relation that was not made by a build recorded in
State history.
_Avoid_: Unregistered change, out-of-band write

**Freshness evidence**:
Information about the timing or ordering of observed changes and known writes
used to assess whether a recorded result is fresh enough for reuse.
_Avoid_: Proof of current contents

**Lag tolerance**:
The time after a workload's last build during which its recorded result may
still be reused although upstream data has changed since that build. It does
not cover upstream logic changes or external modifications of an upstream
result the workload consumed.
_Avoid_: Freshness tolerance, build_after, freshness_tolerance_seconds

**Build age**:
The time since a workload's recorded result was built, as measured by the
State service's clock. Lag tolerance is compared against it.
_Avoid_: Staleness, upstream lag

**Upstream producer**:
The recorded build whose result an upstream relation held when a workload
read it, or none, for a source or after an External modification. When the
current producer's logic differs from the one the workload consumed, the
upstream's query has changed, which Lag tolerance does not cover.
_Avoid_: Upstream build, writer

**Hosted conformance**:
Agreement with the official dbt State service's observable behavior for a
specified client, warehouse and feature scope.
_Avoid_: Reuse safety

**Reuse safety**:
The property that reusing a result satisfies the workload's declared logic,
freshness and isolation requirements.
_Avoid_: Hosted conformance

**Reuse qualification**:
Assessment of whether a workload and its available evidence meet the
requirements for considering a recorded result for reuse.
_Avoid_: Reuse decision (qualification alone does not select a result)

**Reuse candidate**:
The latest recorded result for a workload's output, when it matches the
workload's identity and passes reuse qualification. Freshness is assessed
afterwards, so a reuse candidate is not yet a decision to reuse it.
_Avoid_: Match, confirmed row

**Decision reason**:
The one cause a decision records for building, reusing or copying a workload's
result, such as a first build, no reuse candidate, an outdated upstream or a
result copy.
_Avoid_: Decision description, rejection reason, rationale

**Reference execution**:
An independent execution of a test workload used to establish its expected
result, with any permitted stale results specified separately.
_Avoid_: Hosted oracle
