# dbt State

Language for deciding when a dbt workload can reuse recorded results across
warehouse environments.

## Language

**Warehouse**:
The database system in which dbt sources and model results reside and model
queries execute.
_Avoid_: State store

**Environment**:
A named context in which a dbt project produces its results, such as production
or development.
_Avoid_: Warehouse

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

**Freshness evidence**:
Information about the timing or ordering of observed changes and known writes
used to assess whether a recorded result is fresh enough for reuse.
_Avoid_: Proof of current contents

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

**Reference execution**:
An independent execution of a test workload used to establish its expected
result, with any permitted stale results specified separately.
_Avoid_: Hosted oracle
