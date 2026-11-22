# Databricks copy source choice, 2026-11-21

Hosted State granted a copy from the oldest fresh build after a newer stale
build and a middle build reading a different physical input were added. Each
of those competitors produced BUILD in isolation. With two fresh builds of the
same input, the grant followed the later physical completion epoch in all three
ordering controls. [Issue #240](https://github.com/maxfirman/dbt-state-rs/issues/240)

## Matched histories

The driver used the ordinary Databricks FULL submit and confirmation shapes
from dbt 2.0.11. Each history had a fresh model UID, output object name, input
object name and connection namespace. Within a history all candidates and the
destination shared the UID, output object name, catalog (`cdo_demo_db`),
namespace and node hashes. Each occupied a distinct output schema. The
destination's first submit reported only its input, representing an absent
output. [Native template, zero-based entries 2, 4 and 9](../../golden/fixtures/databricks_submit_copy.jsonl)

Every submit used zero Lag tolerance, ANY policy and external modification
checking. Source submits used `allow_clones=false`, chain limit 0 and
`is_defer_to_profile=true`. The destination used `allow_clones=true`, chain
limit 1 and `is_defer_to_profile=false`. Both used `defer_enabled=true`.
Every source received BUILD and a successful ConfirmExecution. Each history
ends at the destination decision.

| Source schema | Input relative to destination | Input epoch (ms) | Reported physical completion epoch (ms) |
|---|---|---:|---:|
| `cc_old` | Same physical input in `cc_old` | 1795304153000 | 1795304163000 |
| `cc_middle` | Same input object name in `cc_middle` | 1795304153000 | 1795304173000 |
| `cc_stale` | Same physical input in `cc_old`, read before it changed | 1795304143000 | 1795304183000 |
| `cc_peer` | Same physical input in `cc_old` | 1795304153000 | 1795304183000 |

All destination submits read the shared input at 1795304153000. Source
submits preceded all confirmations. Where present, the stale source submitted
first, followed by old, middle and peer. This models an overlapping build
that read an older input and finished later. The final control exchanged
only the two physical completion epochs within the same relative setup.

## Destination decisions

Entries are zero-based in the [complete hosted capture](../../golden/fixtures/databricks_copy_candidates.jsonl).
The confirmation column lists source schemas in RPC order with the `cc_`
prefix omitted. Every grant named the destination in `cc_dev` and required
the selected source's confirmed epoch.

| History | Destination entry | Confirmation order | Decision and exact granted source |
|---|---:|---|---|
| Oldest fresh alone | 2 | old | CLONE `cdo_demo_db.cc_old.cc_90e377ac7623475eb586c4c22b25c2ee` |
| Different input alone | 5 | middle | BUILD |
| Stale alone | 8 | stale | BUILD |
| Add stale | 13 | old → stale | CLONE `cdo_demo_db.cc_old.cc_a2aa20957564428198eccb24020fbc2a` |
| Insert middle | 20 | old → middle → stale | CLONE `cdo_demo_db.cc_old.cc_c6cafa01009f40adbe9555e46b325856` |
| Reverse the three confirmations | 27 | stale → middle → old | CLONE `cdo_demo_db.cc_old.cc_1370962dddc2462dbd724b87ca8a2f57` |
| Two fresh sources | 32 | old → peer | CLONE `cdo_demo_db.cc_peer.cc_b678f1e918014da3a8e69ca36ec22679` |
| Reverse the two confirmations | 37 | peer → old | CLONE `cdo_demo_db.cc_peer.cc_5b2df5a377214427af639729b7a8f981` |
| Exchange old and peer completion epochs | 42 | old → peer | CLONE `cdo_demo_db.cc_old.cc_00fa85d81d3a4eada9ae2a28816e3797` |

Adding the stale source, then inserting the different-input source, preserved
the grant from the oldest fresh source. The two-fresh-source controls distinguish
observed source choice from last-confirmed and last-submitted order. The experiment leaves
the service's internal candidate traversal and equal-epoch tie breaking open.
All nine histories reached a decision. Physical rows and client fallback
require the separate native experiment.

## Evidence and reproduction

The recording proxy forwarded 26 SubmitEnrichedSQL and 17 ConfirmExecution
RPCs to hosted State between 23:37:54 and 23:38:39 UTC. All returned gRPC OK.
The proxy certified 43 started and 43 recorded, complete, with zero failures.
The JSONL preserves every request, response, status and recorded metadata in
order. SHA-256 is `c27a235897b3aed09d61a9e886650c0ebbde0bcdef098fdd693390c05726354e`.
The credential-free replay compares all 43 RPCs through the Rust server.

```sh
cargo test -p dbt-state-harness --test conformance databricks_copy_candidates_exact_public_history
```

Build the probe and recording proxy, then start the proxy in a fresh capture
directory. The driver uses new UUIDs and synthetic epochs on each run. It
sends State RPCs only. [Running a capture](../harness.md#running-a-capture)

```sh
PROBE_URL=http://127.0.0.1:50099 python3 experiments/databricks_copy_candidates.py
```

Stop the proxy with Ctrl-C after the ninth result. The integrity check reads
the JSONL and its completion sidecar and should print 43.

```sh
PYTHONPATH=experiments python3 -c 'import sys; from capture_integrity import verify_capture; print(len(verify_capture(sys.argv[1])))' /path/to/golden_session.jsonl
```
