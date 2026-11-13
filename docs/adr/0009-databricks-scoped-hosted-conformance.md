# Scope Databricks hosted conformance and require zero known mismatches

Accepted during the Databricks planning interview (2026-11-12).

The initial Databricks conformance claim covers dbt 2.0.11 on the available
Free Edition serverless workspace with Unity Catalog and managed Delta tables.
It must cover every State feature reachable through that client within this
warehouse scope. Other client versions, deployment types and storage formats
are not implicitly covered. This bounds a live-verifiable contract instead of
extrapolating Snowflake behavior or claiming untested Databricks deployments.

Within this scope, completion requires zero known hosted mismatches, including
response shape, rejection codes, descriptions, Explain and error statuses.
Unlike the repository's general allowance for documented replay exceptions,
a Databricks development exception remains unfinished work and cannot satisfy
the final claim. Existing Snowflake exceptions are not retrospectively changed.
Official State captures establish expected policy and responses; live Databricks
and independent Reference executions establish physical behavior and Reuse
safety. If hosted policy conflicts with Reuse safety, preserve safety and leave
the affected slice unresolved rather than calling it fully conformant.

This accepts a potentially longer characterization campaign and a narrower
release claim in exchange for evidence that can be reproduced. Fingerprint
changes still follow ADR 0004; this decision does not authorize weakening
organization/warehouse isolation or importing Snowflake rules unchanged.


Validation and telemetry RPCs actually used by the pinned client are included
in the compatibility inventory. Hosted OAuth, account management and billing
are outside warehouse support; the existing self-hosted authentication model
remains. Ordinary PR gates are credential-free; live acceptance is explicit
and required before release. Capability limitations remain blockers until
resolved or addressed by an explicit scope decision.

Publish the tested feature matrix, exact environment and evidence date, and
reopen an affected feature when a new mismatch is discovered. The zero-known-
mismatch standard is bounded by that documented scope and evidence; it is not
an exhaustive proof for every possible SQL input.
