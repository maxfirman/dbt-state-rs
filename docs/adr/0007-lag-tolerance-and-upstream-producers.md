# Freshness against recorded upstream epochs and producers, gated by lag tolerance

## Status

Accepted (2026-11-10). Supersedes the own-build freshness anchor recorded in
`docs/correctness-handoff.md` (finding 12), which was characterized only with
External modifications of dbt-managed upstreams.

## Decision

A model's reuse candidate is assessed upstream by upstream against what its
last confirmed build recorded:

- **Drift**: an upstream drifted when its current epoch is later than the epoch
  recorded for it at that build. The node's own build epoch is not the
  comparison point.
- **Producer**: each recorded input stores the logic identity of the build in
  State history that produced the relation's contents when it was read, or that
  no recorded build did (a source, or an External modification). If the current
  producer differs from the recorded one, the upstream counts as an upstream
  query change and the node rebuilds regardless of the stale-upstream policy
  and Lag tolerance.
- **Lag tolerance**: when the policy requires a rebuild, the node is reused
  instead while its build age is under the tolerance. Build age is the server
  clock minus the build's ConfirmExecution epoch; on DuckDB, whose epochs are
  ordinals, it is the server clock minus the time the server received the
  confirmation.

## Why

The hosted service behaves this way (live characterization 2026-11-10:
`experiments/lag_matrix.py`, the docs' worked example through the real client,
fixtures `lag_tolerance_*.jsonl`, scored by `experiments/lag_rule_check.py`).
The own-build anchor missed upstream changes made between a node's submit and
its confirmation, and could not express why an External modification of a
consumed build rebuilds under ALL while a recorded rebuild of that upstream is
reused within tolerance.

## Considered Options

- **Store the producing execution id** instead of its logic identity. Rejected:
  hosted treats a same-logic rebuild of an upstream as a data change (reused
  within tolerance) and a changed-logic rebuild as an upstream query change, so
  the comparison must be on logic identity.
- **A ledger of what earlier skips saw.** Rejected: hosted rebuilds under ALL
  once every upstream has drifted since the build, even if an earlier skip
  already saw one of them.
- **Derive producers at decision time from history ordering.** Rejected: it
  needs the upstream's build history as of the node's submit, which later
  confirms can obscure; recording it at submit time is exact.

## Consequences

- Inputs recorded before producers were stored read as unrecorded and never
  count as an upstream query change; they keep the drift and tolerance rules.
- Copies registered with RegisterClone record no inputs. Their freshness stays
  anchored on the copy's own build epoch, so a copy is never treated as
  fresher than when it was made.
- A seed's producer identity includes its values, so a reloaded seed counts
  as an upstream query change. Hosted behavior for seeds is uncharacterized;
  this is the conservative choice.
