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

**Node kind**:
The category of dbt node a workload is, such as model, view, seed, snapshot,
incremental model or data test. Each incremental strategy is a distinct node
kind.
_Avoid_: Execution type (except when naming the wire field), materialization

**Analyzed workload**:
A workload's query structure, bound inputs, node kind and configuration, together
with what is known or unknown about the context affecting its result.
_Avoid_: Logical plan (when implying a complete warehouse analysis)

**Logical copy identity**:
The identity of a workload's transformation and result semantics used to compare
results across Target relations; it preserves the bindings of inputs to definitions.
_Avoid_: SQL identity (when omitting node kind, configuration or bindings)

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
read it, or none, for a source or after an External modification. A change
of producer under the Warehouse's rules counts as an upstream query change,
which Lag tolerance does not cover.
_Avoid_: Upstream build, writer

**Volatile function**:
A function in a workload's SQL whose result can differ between executions of
the same query, such as the current time or a random number.
_Avoid_: Nondeterminism (except when naming the `tolerate_nondeterminism` wire field)

**Volatile evaluation**:
The State service's rendering of each Volatile function in a workload's SQL
to a value when it decides, reported with that decision. The Warehouse still
executes the original SQL; rendered values are not substituted into it.
_Avoid_: Substitution, SQL transformation

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

**Data-test history key**:
What a data test's State history is grouped by on Snowflake: its normalized
SQL over the relations it reads, each Stand-in read as its source, and its
result-setting configuration, within the organization, warehouse family and
connection namespace. Only the latest confirmed run under a key is reused.
_Avoid_: Test identity, test UID scope

**Stand-in**:
A relation a workload reads that is a Result copy this server recorded,
unchanged since the copy, read as the build it was copied from: a data test
is keyed and judged for freshness as if it read that build, and an
equivalent that read that build may be copied for the workload. Such an
input is said to be read through a copy.
_Avoid_: Alias, equivalent relation

**Physical binding**:
Which physical relation each relation reference of a workload reads, each
resolved in the context of the SQL that names it and a view through its
definition. Two workloads with the same Logical copy identity can bind
their references differently.
_Avoid_: Input set, reported tables

**Decision reason**:
The one cause a decision records for building, reusing or copying a workload's
result, such as a first build, no reuse candidate, an outdated upstream or a
result copy.
_Avoid_: Decision description, rejection reason, rationale

**Explain tree**:
The structured explanation of one recorded decision that the State service
returns for `dbt state explain`: nested lines, each with an optional marker
and badge.
_Avoid_: Explain messages, decision description

**Reference execution**:
An independent execution of a test workload used to establish its expected
result, with any permitted stale results specified separately.
_Avoid_: Hosted oracle

## Access

**Organization**:
The isolation boundary for all State history and access. A deployment hosts
one or more Organizations, each with an operator-chosen id.
_Avoid_: Tenant, account, org scope

**Role**:
The access a Principal holds in one Organization: `developer` or
`admin`.
_Avoid_: Level, permission set

**Principal**:
A person or machine the upstream IdP authenticated, identified by the IdP's
issuer and subject.
_Avoid_: Actor, user, service account, caller

**Token broker**:
The dbt State component that issues the tokens dbt clients present, after the
upstream IdP has authenticated the Principal. It holds no credentials for
the people or machines it serves.
_Avoid_: Auth server, IdP, identity service

**Upstream IdP**:
The customer's own identity provider, which authenticates every person and
machine that uses dbt State.
_Avoid_: SSO provider, auth provider
