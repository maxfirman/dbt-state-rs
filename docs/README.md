# Documentation index

Documentation for **dbt-state-rs**, an open-source reimplementation of the dbt
State ("query cache") gRPC decision service.

| Doc | What it covers |
|---|---|
| [correctness-handoff.md](correctness-handoff.md) | Current correctness status, completed offline fixes, remaining tasks and validation limits. |
| [correctness-review-2026-10-10.md](correctness-review-2026-10-10.md) | Original review of the pre-hardening commit, research and reproduced defects. |
| [overview.md](overview.md) | What dbt State is, how the client/server split works, project goals. |
| [protocol.md](protocol.md) | The gRPC protocol reference: services, messages, decision semantics, observed wire shapes. |
| [architecture.md](architecture.md) | Server internals: decision engine, Postgres store/schema, service wiring. |
| [development.md](development.md) | Toolchain, build/run, Postgres, pointing the dbt client at the server. |
| [harness.md](harness.md) | The recording proxy + how real golden traffic is captured. |
| [testing.md](testing.md) | Testing strategy: conformance replay, property tests, differential fuzzing. |
| [ui.md](ui.md) | The read-only web console (Topcoat): what it shows, how to run it, design. |
| [ui-plan.md](ui-plan.md) | UI plan, dbt Platform research, domain model, roadmap. |
| [research.md](research.md) | Reverse-engineering notes and findings from real traffic. |

Reading order for a newcomer: **overview → protocol → architecture → testing**.
For hands-on work: **development → harness**.
