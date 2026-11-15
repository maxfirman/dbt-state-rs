# Databricks preview landing plan

Revised on 2026-11-15 after the maintainer authorized consolidation and a
smaller initial goal. This plan replaces full completion of every #132 child
as the prerequisite for landing PR #164. The original acceptance criteria
remain the backlog for a future full-conformance claim.

## Release scope

Land experimental Databricks support, pinned to dbt 2.0.11, the tested Free
Edition serverless workspace, Unity Catalog and managed Delta. Publish a
single feature matrix with links to existing evidence. Only the exact
characterized cases receive a tested claim; feature headings alone do not
imply complete coverage. Preserve the implemented same-target behavior,
ordinary seeds, deferral and tested incremental/snapshot subsets.

Document conservative BUILD and speculative undecided responses as accepted
preview limitations when hosted would reuse. Formatting, explanation and
error-status parity outside retained regression cases can be follow-up work.
Keep existing fixtures exact; do not loosen assertions to manufacture parity.
ADR 0009 still governs any future full scoped-conformance claim; this preview
explicitly makes a smaller claim.

Databricks Result copy is outside the preview. Disable Databricks copy grants
by default on both RegisterClone and ordinary submit; automatic submit copy
is already disabled. Retain narrow direct-copy implementation/provenance only
where it can remain clearly isolated and tested. Do not launch another live
copy campaign to make this release complete. #152-#154 remain follow-ups.
The unsupported response/fallback must be explicit and tested against the
pinned client contract; if it prevents an otherwise supported ordinary run,
resolve that boundary or document the required no-copy client configuration
before landing.

## Preserve these safety requirements

Organization, warehouse, namespace, full-UID and latest-physical-history
isolation remain required. Unknown freshness and incompatible identities must
not become reuse. Keep original upstream epoch/producer evidence. A known
wrong-result path attributable to this server is a landing blocker: fix it or
disable the optimization. Do not reproduce unsafe hosted SKIPs for parity.

The State-disabled snapshot new_record delete behavior and shallow-clone
FULL/seed replacement failures remain documented client/warehouse limitations;
those workflows are excluded from the preview. Namespace-crossing, cross-UID
speculative reuse and unresolved targetless reuse remain conservatively
restricted. Lack of full compatibility is acceptable; silently claiming safe
support for an unresolved path is not.

## Consolidation pass

Freeze new feature development and live campaigns during landing preparation.
Do one bounded cleanup pass, avoiding a broad framework rewrite:

1. Preserve a named archive ref for the pre-cleanup evidence, with a compact
   manifest identifying the commit, original paths and hashes. Preserve
   existing local evidence too; the archive ref must be available remotely
   before removing files from the integration tree.
2. Replace redundant JSON report outputs with one concise feature/status
   matrix and evidence index. The current docs JSON outputs add 11,517 lines;
   these are the first cleanup candidates, subject to dependency checks.
   Keep structured files actually consumed by tests or reproduction tools.
3. Move unexecuted MERGE settings/schema campaign scaffolds and their
   scaffold-only wrappers/tests out of this landing diff into the archived
   follow-up work. Update #145 with exact pointers; do not call them accepted.
4. Consolidate repeated report narrative into the matrix. Preserve useful
   investigations only where they explain a production decision or an open
   defect. Fix references to removed paths.
5. Keep captured golden histories, legacy database fixtures, production
   regression tests and the drivers needed to reproduce accepted behavior.
   Simplify Python helpers only where duplication is obvious. Do not rewrite
   all drivers or merge all tests merely to reduce the line count.

Expected first reduction: roughly 10,000-15,000 added lines, chiefly generated
reports and unfinished scaffolds. This is an estimate, not a deletion quota.
A smaller, accurate diff matters more than the count.

## Consolidation checkpoint

The pre-cleanup state is published as `archive/databricks-132-evidence` at
`d4ac553cdd09becb77c7555b143e5f4bedaeb2fb`. The first pass archived67 report and
unexecuted-scaffold files (19,935 lines), repaired local links, and retained
golden/upgrade fixtures, production regressions and accepted campaign drivers.
The [preview matrix](databricks-preview.md) replaces the report inventory;
[original paths and hashes](databricks-evidence.tsv) preserve attribution.

## Remaining landing gates

- Finish the cleanup and one user-facing preview matrix, including deployment
  configuration, unsupported copy behavior and known limitations.
- Verify the copy default and unsupported-client fallback without expanding
  feature scope. Reuse existing hosted/warehouse evidence; fresh cloud
  campaigns and exhaustive independent-reference matrices are not prerequisites.
- Run focused tests for changed behavior. Run one final workspace/protocol
  cycle and strict CI lint after material cleanup/guard changes. The baseline
  at 45e94c3 passed 709 workspace tests and CI; 131 hosted replay corpora are
  retained. Explain exact exclusions and test provenance.
- Local and integration-PR real-dbt DuckDB acceptance stay deferred until after
  landing, as explicitly requested. Existing Rust regression coverage remains.
- Review the resulting production/migration changes and consolidated evidence
  once. Mark PR #164 ready, address valid automated Codex findings, and merge
  when required checks pass. Do not restart characterization for non-blocking
  compatibility differences found during review.
- Close #132 against this explicitly revised landing scope when merged. Keep
  incomplete children open with follow-up scope and evidence links; remove
  automatic closure references to incomplete issues from the PR.

No broad support claim, every-feature acceptance campaign, new fuzz grammar,
all selectors, complete upgrade matrix or additional incremental/snapshot
strategy matrices are required to land the preview. Their original tickets
remain the follow-up backlog.
