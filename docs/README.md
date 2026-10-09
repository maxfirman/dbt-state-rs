# Documentation index

Documentation for **dbt-state-rs**, an open-source reimplementation of the dbt
State ("query cache") gRPC decision service.

| Doc | What it covers |
|---|---|
| [overview.md](overview.md) | What dbt State is, how the client/server split works, project goals. |
| [protocol.md](protocol.md) | The gRPC protocol reference: services, messages, decision semantics, observed wire shapes. |
| [architecture.md](architecture.md) | Server internals: decision engine, Postgres store/schema, service wiring. |
| [development.md](development.md) | Toolchain, build/run, Postgres, pointing the dbt client at the server. |
| [harness.md](harness.md) | The recording proxy + how real golden traffic is captured. |
| [testing.md](testing.md) | Testing strategy: conformance replay, property tests, differential fuzzing. |
| [research.md](research.md) | Reverse-engineering notes and findings from real traffic. |

Reading order for a newcomer: **overview → protocol → architecture → testing**.
For hands-on work: **development → harness**.
